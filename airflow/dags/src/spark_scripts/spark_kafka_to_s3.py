import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace
from pyspark.sql.window import Window

def main():
    input_file = str(sys.argv[1])
    output_file = str(sys.argv[2])

    conf = SparkConf().setAppName(f'enrich_geo_data_with_tz')
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    # Пропишем в схему типы данных, с которыми надо прочитать csv
    csv_schema = StructType() \
        .add("id",IntegerType(),False) \
        .add("city",StringType(),False) \
        .add("lat",StringType(),False) \
        .add("lng",StringType(),False)

    # Список городов, по которым Spark распознаёт таймзоны
    timezone_cities = ["Darwin", "Perth", "Eucla", "Brisbane", "Lindeman", "Canberra", "Queensland",
               "Adelaide", "Hobart", "Melbourne", "Sydney", "Broken_Hill", "Lord_Howe"]

    geo = sql.read.csv(f"{input_file}", sep = ";", header = True, schema = csv_schema) \
        .withColumn("lat", regexp_replace("lat", ",", ".")) \
        .withColumn("lng", regexp_replace("lng", ",", ".")) \
        .withColumn("lat", F.col("lat").cast("double")) \
        .withColumn("lng", F.col("lng").cast("double")) \
        .withColumn("tz_city", F.when(F.col("city").isin(timezone_cities), F.col("city"))) \
        .withColumn("tz", F.concat(F.lit("Australia/"),F.col("tz_city"))) \
        .withColumn("lat_rad", F.col("lat")*3.14/180) \
        .withColumn("lng_rad", F.col("lng")*3.14/180)

    # Для городов без своей таймзоны, определим таймзону по ближайшему городу с таймзоной
    geo_tz = geo.where(F.col("tz_city").isNotNull())
    geo_no_tz = geo.where(F.col("tz_city").isNull()) \
        .selectExpr(["id", "city", "lat_rad as latitude", "lng_rad as longitude"])

    window = Window().partitionBy("city").orderBy([F.col("distance").asc()])

    geo_no_tz = geo_no_tz.crossJoin(geo_tz.select("lat", "lng", "lat_rad", "lng_rad", "tz_city", "tz").hint("broadcast")) \
        .withColumn("distance", F.lit(2*6371)*F.asin(
        (F.sin((F.col("lat_rad")-F.col("latitude"))/2)**2 +
         F.cos(F.col("lat_rad"))*F.cos(F.col("latitude"))*
         (F.sin((F.col("lng_rad") - F.col("longitude"))/2)**2)
         )** (1/2))) \
        .withColumn('rank', F.rank().over(window)) \
        .where(F.col("rank") == 1) \
        .selectExpr(["id", "city","latitude as lat_rad", "longitude as lng_rad", "tz"])

    # Объединим обратно два дата фрейма в один
    geo = geo_tz.select("id", "city", "lat_rad", "lng_rad", "tz") \
        .unionByName(geo_no_tz)

    # Сохраним результат в parquet
    geo.write.parquet(f"{output_file}", mode = "overwrite")

if __name__ == "__main__":
    main()