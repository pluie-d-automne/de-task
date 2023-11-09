from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
import logging

API_URL = "https://randomuser.me/api/"
USERS_CNT_TO_LOAD = 500 # Сколько случайных пользователей будет загружено в Кафку каждый запуск
KAFKA_TOPIC_NAME = "user_data"

default_args = {
    'owner': 'airflow'
}

@dag(
    dag_id="dag_kafka_to_s3",
    default_args=default_args,
    schedule_interval="0 * * * *", # Запуск раз в час
    start_date=datetime(2023, 11, 8),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1
)

def dag():
    
    @task
    def load_user_data_to_kafka():
        import requests
        from confluent_kafka import Producer
        import json
        
        logger = logging.getLogger('load_user_data_to_kafka')
        
        conn = BaseHook.get_connection('KAFKA_USER_DATA_CONN')
        conf_producer = json.loads(conn.get_extra())
        producer = Producer(conf_producer)
        logger.info("Kafka producer seccesfully created")
        
        def delivery_report(errmsg, msg):
            if errmsg is not None:
                logger.warning("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
            return
        
        for i in range(USERS_CNT_TO_LOAD):
            r = requests.get(API_URL)
            if r.status_code == 200:
                message_json = json.dumps(r.json()).encode()
                producer.produce(topic=KAFKA_TOPIC_NAME, value=message_json, on_delivery=delivery_report)
                producer.poll(0)
                if i % 20==0: # После каждых 20 асинхронно отправленных сообщений, убеждаемся, что они дошли
                    producer.flush()
            else:
                logger.warning("Received {} status_code from API".format(r.status_code))
        producer.flush()
        
    load_user_data_to_kafka()
    
dag = dag()