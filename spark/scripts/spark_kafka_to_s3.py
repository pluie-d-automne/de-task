from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, struct
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType
from datetime import datetime

TOPIC = 'user_data' 

target_subcolumns = ["gender", "name", "location", "email", "dob", "registered", "extracted", "phone", "cell", "id"]
target_columns = ["results."+ colname for colname in target_subcolumns]

drop_columns =  [
    "location.street", "location.coordinates", "location.timezone", "login", "picture", "nat"]
           
spark = SparkSession.builder \
    .appName("spark-user-data-etl") \
    .master('local') \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("security.protocol", "PLAINTEXT") \
  .option('group.id', 'spark-user-reader3') \
  .option("subscribe", TOPIC) \
  .option("startingOffsets", "earliest") \
  .load()
  
dstream = df.selectExpr("CAST(value AS STRING)")

user_schema = StructType([
    StructField('gender',StringType(), True),
    StructField('name', StructType([
             StructField('title', StringType(), True),
             StructField('first', StringType(), True),
             StructField('last', StringType(), True)
             ])),
    StructField('location', StructType([
             StructField('street', StructType([
                        StructField('number', IntegerType(), True),
                        StructField('name', StringType(), True)
                        ])),
             StructField('city', StringType(), True),
             StructField('state', StringType(), True),
             StructField('country', StringType(), True),
             StructField('postcode', StringType(), True),
             StructField('coordinates', StructType([
                        StructField('latitude', StringType(), True),
                        StructField('longitude', StringType(), True)
                        ])),
             StructField('timezone', StructType([
                        StructField('offset', StringType(), True),
                        StructField('description', StringType(), True)
                        ]))
             ])),
    StructField('email',StringType(), True),
    StructField('login', StructType([
             StructField('uuid', StringType(), True),
             StructField('username', StringType(), True),
             StructField('password', StringType(), True),
             StructField('salt', StringType(), True),
             StructField('md5', StringType(), True),
             StructField('sha1', StringType(), True),
             StructField('sha256', StringType(), True),
             ])),
    StructField('dob', StructType([
             StructField('date', StringType(), True),
             StructField('age', IntegerType(), True)
             ])),
    StructField('registered', StructType([
             StructField('date', StringType(), True),
             StructField('age', IntegerType(), True)
             ])),
    StructField('extracted', StructType([
             StructField('date', StringType(), True)
             ])),
    StructField('phone',StringType(), True),
    StructField('cell',StringType(), True),
    StructField('id', StructType([
             StructField('name', StringType(), True),
             StructField('value', StringType(), True)
             ])),
    StructField('picture', StructType([
             StructField('large', StringType(), True),
             StructField('mediun', StringType(), True),
             StructField('thumbnail', StringType(), True)
             ])),
    StructField('nat',StringType(), True)
  ])

cover_schema = StructType([
  StructField("results", ArrayType(user_schema))
])


dstream = dstream \
     .withColumn('json', from_json(col('value'), cover_schema)) \
     .withColumn('results', col('json.results')) \
     .selectExpr('results') \
     .selectExpr(target_columns) \
     .withColumn("extracted",
                 struct(lit(datetime.now().strftime("%Y-%m-%d")).alias("date"))) \
     .withColumn("location",
                 struct(
                   col("location.city")[0].alias("city"),
                   col("location.state")[0].alias("state"),
                   col("location.country")[0].alias("country"),
                   col("location.postcode")[0].alias("postcode")                         
                                              ))
     
query = dstream.writeStream.format("json") \
      .option("path", "s3a://myminio/user-data") \
      .option("spark.hadoop.fs.s3a.proxy.host", "minio") \
      .option("spark.hadoop.fs.s3a.endpoint", "minio") \
      .option("spark.hadoop.fs.s3a.proxy.port", "9000") \
      .option("spark.hadoop.fs.s3a.path.style.access", "true") \
      .option("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
      .option("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .start() \
            .awaitTermination()

# query = dstream.writeStream.format("console") \
#             .option("truncate", "false") \
#             .outputMode("append") \
#             .option("checkpointLocation", "checkpoints") \
#             .start() \
#             .awaitTermination()
# query = dstream.format("avro") \
#     .option("path", "s3a://datalake/test")
#         #.option("checkpointLocation", self.join_path + "/applicationHistory").partitionBy("year", "month", "day", "hour") \
        