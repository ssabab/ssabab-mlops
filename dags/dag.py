from datetime import datetime
from tasks import init, backlog, score, food, vote, menu

from airflow import DAG

with DAG(
    dag_id="SSABAB_legacy_system_dag",
    schedule="@daily",
    start_date=datetime(2025, 4, 28),
    end_date=datetime(2025, 5, 31),
    catchup=False,
) as dag:
    
    fetch_data = init.fetch_sqlite_data_to_kafka()
    
    valid_backlog = backlog.validate_backlog()
    gen_backlog_feature = backlog.generate_backlog_features()
    valid_backlog >> gen_backlog_feature
    
    valid_food = food.validate()
    prprcss_food = food.preprocessing()
    valid_food >> prprcss_food
    
    valid_vote = vote.validate()
    prprcss_vote = vote.preprocessing()
    valid_vote >> prprcss_vote
    
    valid_menu = menu.validate()
    prprcss_menu = menu.preprocessing()
    valid_menu >> prprcss_menu
    
    valid_score = score.validate()
    prprcss_score = score.preprocessing()
    valid_score >> prprcss_score
    
    fetch_data >> [valid_backlog, valid_food, valid_vote, valid_menu, valid_score]
    