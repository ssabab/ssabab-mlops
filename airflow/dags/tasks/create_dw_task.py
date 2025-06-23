import os
import pandas as pd
from datetime import datetime
from airflow.decorators import task
from airflow.utils.log.logging_mixin import LoggingMixin
from utils.db import get_mysql_connection
from common.env_loader import load_env

load_env()

SQL_PATH = os.getenv("SQL_PATH")
ALLOWED_TABLES = {
    "ssabab_dw.dim_user",
    "ssabab_dw.dim_food",
    "ssabab_dw.dim_category",
    "ssabab_dw.dim_tag",
    "ssabab_dw.fact_user_ratings",
    "ssabab_dw.fact_user_tags",
    "ssabab_dw.fact_user_pre_votes",
}

log = LoggingMixin().log

def fetch_and_insert(query, target_table, column_order, params=None, insert_strategy="append"):
    if target_table not in ALLOWED_TABLES:
        raise ValueError(f"Invalid table name: {target_table}")

    with get_mysql_connection() as conn:
        df = pd.read_sql(query, conn, params=params)
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                values = tuple(row[col] for col in column_order)
                placeholders = ', '.join(['%s'] * len(values))
                columns = ', '.join(column_order)

                if insert_strategy == "ignore":
                    sql = f"INSERT IGNORE INTO {target_table} ({columns}) VALUES ({placeholders})"
                elif insert_strategy == "overwrite":
                    update_clause = ', '.join([f"{col}=VALUES({col})" for col in column_order])
                    sql = f"INSERT INTO {target_table} ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
                else:
                    sql = f"INSERT INTO {target_table} ({columns}) VALUES ({placeholders})"

                cur.execute(sql, values)
        conn.commit()
        log.info(f"Inserted {len(df)} rows into `{target_table}` using strategy '{insert_strategy}'.")

@task
def create_tables_from_sql_files():
    with get_mysql_connection() as conn:
        with conn.cursor() as cur:
            for item in sorted(os.listdir(SQL_PATH)):
                folder_path = os.path.join(SQL_PATH, item)
                if not os.path.isdir(folder_path):
                    continue
                for filename in sorted(os.listdir(folder_path)):
                    if filename.endswith(".sql"):
                        file_path = os.path.join(folder_path, filename)
                        try:
                            with open(file_path, "r", encoding="utf-8") as f:
                                sql = f.read()
                                log.info(f"Executing SQL file: {file_path}")
                                cur.execute(sql)
                        except Exception as e:
                            log.error(f"Failed to execute {filename}: {e}")
            conn.commit()
            log.info("All SQL files executed successfully.")

@task
def insert_dim_user_data():
    query = """
        SELECT 
            user_id,
            gender,
            YEAR(birth_date) AS birth_year,
            CONCAT(ssafy_year, '-', class_num) AS ssafy_class,
            ssafy_region AS region
        FROM account
    """
    column_order = ["user_id", "gender", "birth_year", "ssafy_class", "region"]
    fetch_and_insert(query, "ssabab_dw.dim_user", column_order, insert_strategy="ignore")

@task
def insert_dim_food_data():
    query = """
        SELECT food_id, food_name, category AS category_id
        FROM food
    """
    column_order = ["food_id", "food_name", "category_id"]
    fetch_and_insert(query, "ssabab_dw.dim_food", column_order, insert_strategy="ignore")

@task
def insert_dim_category_data():
    query = """
        SELECT DISTINCT category AS category_name
        FROM food
    """
    column_order = ["category_name"]
    fetch_and_insert(query, "ssabab_dw.dim_category", column_order, insert_strategy="ignore")

@task
def insert_dim_tag_data():
    query = """
        SELECT DISTINCT tag AS tag_name
        FROM food_tag
    """
    column_order = ["tag_name"]
    fetch_and_insert(query, "ssabab_dw.dim_tag", column_order, insert_strategy="ignore")

@task
def insert_fact_user_ratings_data(target_date: str = datetime.today().strftime('%Y-%m-%d')):
    query = """
        SELECT user_id, food_id, food_score, DATE(timestamp) AS created_date
        FROM food_review
        WHERE DATE(timestamp) = %s
    """
    column_order = ["user_id", "food_id", "food_score", "created_date"]
    fetch_and_insert(query, "ssabab_dw.fact_user_ratings", column_order, params=[target_date])

@task
def insert_fact_user_tags_data(target_date: str = datetime.today().strftime('%Y-%m-%d')):
    query = """
        SELECT user_id, food_id, tag_id, DATE(created_at) AS created_date
        FROM food_tag_log
        WHERE DATE(created_at) = %s
    """
    column_order = ["user_id", "food_id", "tag_id", "created_date"]
    fetch_and_insert(query, "ssabab_dw.fact_user_tags", column_order, params=[target_date])

@task
def insert_fact_user_pre_votes_data(target_date: str = datetime.today().strftime('%Y-%m-%d')):
    query = """
        SELECT user_id, food_id, DATE(created_at) AS vote_date
        FROM food_vote
        WHERE DATE(created_at) = %s
    """
    column_order = ["user_id", "food_id", "vote_date"]
    fetch_and_insert(query, "ssabab_dw.fact_user_pre_votes", column_order, params=[target_date])
