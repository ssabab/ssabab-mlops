from datetime import datetime
from tasks import *

from airflow import DAG

with DAG(
    dag_id="SSABAB_pipeline_dag",
    schedule="@daily",
    start_date=datetime(2025, 4, 28),
    end_date=datetime(2025, 5, 31),
    catchup=False,
) as dag:
    pass
    