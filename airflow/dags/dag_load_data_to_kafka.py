from datetime import datetime, timedelta
from airflow.decorators import dag, task
import logging

API_URL = "https://randomuser.me/api/"
USERS_CNT_TO_LOAD = 10 # Сколько случайных пользователей будет загружено в Кафку каждый запуск

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
        
        KAFKA_TOPIC_NAME = "user_data"
        conf_producer = {
		'bootstrap.servers' : 'kafka:29092',
		'security.protocol' : 'PLAINTEXT',
		}
        
        producer = Producer(conf_producer)
        logger.info("Kafka producer seccesfully created")
        def delivery_report(errmsg, msg):
            if errmsg is not None:
                logger.warning("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
            return
        
        for i in range(USERS_CNT_TO_LOAD):
            r = requests.get(API_URL)
            if r.status_code == 200:
                logger.info("Status Code: {}".format(r.status_code))
                message_json = json.dumps(r.json()).encode()
                logger.info("Message: {}".format(message_json[:10]))
                producer.produce(topic=KAFKA_TOPIC_NAME, value=message_json, on_delivery=delivery_report)
        producer.poll(0)
        producer.flush()
        
    load_user_data_to_kafka()
    
dag = dag()