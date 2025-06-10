import os
from datetime import datetime
import pandas as pd
from airflow.decorators import task
from airflow.utils.log.logging_mixin import LoggingMixin
from utils.db import get_mysql_connection
from common.env_loader import load_env

load_env()

SQL_PATH = os.getenv("SQL_PATH")
ALLOWED_TABLES = {"dim_food", "dim_user", "dim_user_group", "fact_user_ratings"}

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
            for subfolder in os.listdir(SQL_PATH):
                folder_path = os.path.join(SQL_PATH, subfolder)
                for filename in sorted(os.listdir(folder_path)):
                    if filename.endswith(".sql"):
                        file_path = os.path.join(folder_path, filename)
                        try:
                            with open(file_path, "r", encoding="utf-8") as f:
                                sql = f.read()
                                log.info(f"Executing: {filename}")
                                cur.execute(sql)
                        except Exception as e:
                            log.error(f"Failed to execute {filename}: {e}")
        conn.commit()
        log.info("All SQL files executed successfully.")


@task
def insert_dim_food_data():
    query = """
        SELECT food_id, food_name, category, tag
        FROM food
    """
    column_order = ["food_id", "food_name", "category", "tag"]
    fetch_and_insert(query, "dim_food", column_order, insert_strategy="ignore")


@task
def insert_dim_user_group_data():
    query = """
        SELECT group_id, user_id, ord_num, class
        FROM account
    """
    column_order = ["group_id", "user_id", "ord_num", "class"]
    fetch_and_insert(query, "dim_user_group", column_order, insert_strategy="ignore")


@task
def insert_dim_user_data():
    query = """
        SELECT user_id, birth_year, gender
        FROM account
    """
    column_order = ["user_id", "birth_year", "gender"]
    fetch_and_insert(query, "dim_user", column_order, insert_strategy="ignore")


@task
def insert_fact_user_ratings_data(target_date: str = datetime.today().strftime('%Y-%m-%d')):
    query = """
        SELECT user_id, group_id, food_id, rating, created_date, updated_date
        FROM raw_user_ratings_data 
        WHERE DATE(created_date) = %s
    """
    column_order = ["user_id", "group_id", "food_id", "rating", "created_date", "updated_date"]
    fetch_and_insert(query, "fact_user_ratings", column_order, params=[target_date])
