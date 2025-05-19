import os
from datetime import datetime
import pandas as pd
from airflow.decorators import task
from utils.db import *
from common.env_loader import load_env

load_env()

SQL_PATH = os.getenv("SQL_PATH")


def fetch_and_insert(query, target_table, column_order, params=None):
    with get_mysql_connection() as mysql_conn, get_postgres_connection() as pg_conn:
        df = pd.read_sql(query, mysql_conn, params=params)

        with pg_conn.cursor() as cur:
            for _, row in df.iterrows():
                values = tuple(row[col] for col in column_order)
                placeholders = ', '.join(['%s'] * len(values))
                columns = ', '.join(column_order)
                cur.execute(f"""
                    INSERT INTO {target_table} ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT DO NOTHING;
                """, values)

        pg_conn.commit()


@task
def create_tables_from_sql_files():
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            for subfolder in os.listdir(SQL_PATH):
                folder_path = os.path.join(SQL_PATH, subfolder)
                for filename in sorted(os.listdir(folder_path)):
                    if filename.endswith(".sql"):
                        file_path = os.path.join(folder_path, filename)
                        with open(file_path, "r", encoding="utf-8") as f:
                            sql = f.read()
                            print(f"Executing: {filename}")
                            cur.execute(sql)
        conn.commit()

@task
def insert_raw_account():
    query = """SELECT id, gender, birth_year, ord_num, class, provider FROM account"""
    column_order = ["id", "gender", "birth_year", "ord_num", "class", "provider"]
    fetch_and_insert(query, "raw_account", column_order)

@task
def insert_raw_food():
    query = """SELECT food_id, food_name, main_sub, category, tag FROM food"""
    column_order = ["food_id", "food_name", "main_sub", "category", "tag"]
    fetch_and_insert(query, "raw_food", column_order)

@task
def insert_raw_menu():
    query = """SELECT menu_id, date FROM menu"""
    column_order = ["menu_id", "date"]
    fetch_and_insert(query, "raw_menu", column_order)

@task
def insert_raw_menu_food():
    query = """SELECT menu_id, food_id FROM menu_food"""
    column_order = ["menu_id", "food_id"]
    fetch_and_insert(query, "raw_menu_food", column_order)

@task
def insert_raw_food_review():
    query = """SELECT id, user_id, food_id, food_score, create_at FROM food_review"""
    column_order = ["id", "user_id", "food_id", "food_score", "create_at"]
    fetch_and_insert(query, "raw_food_review", column_order)

@task
def insert_raw_menu_review():
    query = """SELECT id, user_id, menu_id, menu_comment, timestamp FROM menu_review"""
    column_order = ["id", "user_id", "menu_id", "menu_comment", "timestamp"]
    fetch_and_insert(query, "raw_menu_review", column_order)

@task
def insert_raw_pre_vote():
    query = """SELECT user_id, menu_id, food_id FROM pre_vote"""
    column_order = ["user_id", "menu_id", "food_id"]
    fetch_and_insert(query, "raw_pre_vote", column_order)

@task
def insert_raw_friend():
    query = """SELECT user_id, friend_user_id FROM friend"""
    column_order = ["user_id", "friend_user_id"]
    fetch_and_insert(query, "raw_friend", column_order)


@task
def insert_fact_user_food_score_data(target_date: str = datetime.today().strftime('%Y-%m-%d')):
    query = """
        SELECT user_id, food_id, menu_id,
               TO_CHAR(date, 'YYYYMMDD')::INT AS date_id,
               food_score,
               TRUE AS has_review,
               'user_rating' AS rating_source,
               CURRENT_DATE AS snapshot_date
        FROM raw_food_review
        JOIN raw_menu_food USING (food_id)
        JOIN raw_menu USING (menu_id)
        WHERE DATE(create_at) = %s
    """
    column_order = ["user_id", "food_id", "menu_id", "date_id", "food_score", "has_review", "rating_source", "snapshot_date"]
    fetch_and_insert(query, "fact_user_food_score", column_order, params=[target_date])

@task
def insert_fact_user_menu_review_data(target_date: str = datetime.today().strftime('%Y-%m-%d')):
    query = """
        SELECT id AS review_id, user_id, menu_id, menu_comment, timestamp,
               CURRENT_DATE AS snapshot_date
        FROM raw_menu_review
        WHERE DATE(timestamp) = %s
    """
    column_order = ["review_id", "user_id", "menu_id", "menu_comment", "timestamp", "snapshot_date"]
    fetch_and_insert(query, "fact_user_menu_review", column_order, params=[target_date])
