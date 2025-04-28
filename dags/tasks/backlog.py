from airflow.decorators import task


@task(task_id="validate_backlog")
def validate_backlog():
    import json
    from kafka import KafkaConsumer
    
    consumer = KafkaConsumer(
        "backlog",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    
    return

@task(task_id="generate_backlog_features")
def generate_backlog_features():
    
    
    return


