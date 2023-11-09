# de-task
Условие задачи: см ./de-task.pdf

## Как запустить решение
1) Поднять сервисы в докере. Для этого в директории проекта выполинть
```bash
docker-compose up -d --build
```
2) Зайти в UI Airflow:
* URL: localhost:8080
* user: airflow
* password: airflow
3) Создать подключения в Airflow: в Admin -> Connections -> +
```text
Connection Id: KAFKA_USER_DATA_CONN
Connection Type: Apache Kafka
Config Dict {"bootstrap.servers":"kafka:9092", "security.protocol":"PLAINTEXT"}

```

4) Запустить DAG:
* dag_kafka_to_s3

## Описание решения
Оркестратор: Airflow
Брокер сообщений: Kafka
В качестве s3-like хранилища выбрано MinIO
Формат хранения данных - avro, так как он:
* хорошо поддерживает вложенность
* поддерживает эфолюцию схемы: данные по клиентам могут меняться как в сторону увеличения (например, появится потребность хранить новый атрибут), так и в сторону уменьшения (например, по требованию службы безопасности), типы данных для каких-то полей тоже вполне могут меняться.