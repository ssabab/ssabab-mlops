import os
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python import get_current_context
from utils.db import get_mysql_connection
from common.env_loader import load_env

load_env()

SQL_PATH = os.getenv("SQL_PATH")
ALLOWED_TABLES = {
    "ssabab_dw.dim_user",
    "ssabab_dw.dim_menu_food_combined",
    "ssabab_dw.fact_user_ratings",
    "ssabab_dw.fact_user_votings",
    "ssabab_dw.fact_user_comments",
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
def insert_dim_user():
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
def insert_dim_menu_food_combined():
    query = """
        SELECT 
            mf.food_id,
            f.food_name,
            f.main_sub,
            f.category AS category_name,
            f.tag AS tag_name,
            mf.menu_id,
            m.date AS menu_date
        FROM menu_food mf
        JOIN food f ON mf.food_id = f.food_id
        JOIN menu m ON mf.menu_id = m.menu_id
    """
    column_order = ["food_id", "food_name", "main_sub", "category_name", "tag_name", "menu_id", "menu_date"]
    fetch_and_insert(query, "ssabab_dw.dim_menu_food_combined", column_order, insert_strategy="ignore")

@task
def insert_fact_user_ratings():
    context = get_current_context()
    execution_date = context["execution_date"]
    target_date = execution_date.format("YYYY-MM-DD")
    query = """
        SELECT user_id, food_id, food_score, DATE(timestamp) AS rating_date
        FROM food_review
        WHERE DATE(timestamp) = %s
    """
    column_order = ["user_id", "food_id", "food_score", "rating_date"]
    fetch_and_insert(query, "ssabab_dw.fact_user_ratings", column_order, params=[target_date])

@task
def insert_fact_user_votings():
    context = get_current_context()
    execution_date = context["execution_date"]
    target_date = execution_date.format("YYYY-MM-DD")
    query = """
        SELECT 
            pv.user_id,
            mf.food_id,
            DATE(m.date) AS vote_date
        FROM pre_vote pv
        JOIN menu m ON pv.menu_id = m.menu_id
        JOIN menu_food mf ON pv.menu_id = mf.menu_id
        WHERE DATE(m.date) = %s
    """
    column_order = ["user_id", "food_id", "vote_date"]
    fetch_and_insert(query, "ssabab_dw.fact_user_votings", column_order, params=[target_date])

@task
def insert_fact_user_comments():
    query = """
        SELECT 
            user_id,
            menu_id,
            menu_comment,
            DATE(timestamp) AS comment_date
        FROM menu_review
    """
    column_order = ["user_id", "menu_id", "menu_comment", "comment_date"]
    fetch_and_insert(query, "ssabab_dw.fact_user_comments", column_order)
