from airflow.decorators import task

@task(task_id="fetch_sqlite_data_to_kafka")
def fetch_sqlite_data_to_kafka():
    import json
    from kafka import KafkaProducer
    
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    return