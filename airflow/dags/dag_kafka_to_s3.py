from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
import findspark
findspark.init()
findspark.find()

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow'
}

with DAG(
    dag_id = "friends_recommendation_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    end_date=datetime(2022, 6, 21),
    catchup=True,
    max_active_runs=1,
) as dag_spark:

    enrich_geo = SparkSubmitOperator(
        task_id='enrich_geo',
        dag=dag_spark,
        application ='/lessons/enrich_geo.py',
        conn_id= 'yarn_spark',
        application_args = ["/user/pdkudrya/data/geo/geo.csv",
                            "/user/pdkudrya/data/ods/geo/geo.parquet"
        ],
        conf={
            "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 2,
        executor_memory = '1g'
    )