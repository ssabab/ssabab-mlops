import os
from datetime import datetime
import pandas as pd
from airflow.decorators import task
from utils.db import *
from common.env_loader import load_env

load_env()

SQL_PATH = os.getenv("SQL_PATH")


def fetch_and_insert(query, target_table, column_order, params=None, is_dim=False):
    with get_mysql_connection() as conn:
        df = pd.read_sql(query, conn, params=params)

        with conn.cursor() as cur:
            for _, row in df.iterrows():
                values = tuple(row[col] for col in column_order)
                placeholders = ', '.join(['%s'] * len(values))
                columns = ', '.join(column_order)
                
                if is_dim:
                    cur.execute(f"""
                        INSERT INTO {target_table} ({columns})
                        VALUES ({placeholders})
                        ON CONFLICT DO NOTHING;
                    """, values)
                else:
                    cur.execute(f"""
                        INSERT INTO {target_table} ({columns})
                        VALUES ({placeholders})
                    """, values)

        conn.commit()


@task
def create_tables_from_sql_files():
    with get_mysql_connection() as conn:
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
def insert_dim_food_data():
    query = """
        SELECT food_id, food_name, category, tag
        FROM food
    """
    column_order = ["food_id", "food_name", "category", "tag"]
    fetch_and_insert(query, "dim_food", column_order, is_dim=True)


@task
def insert_dim_user_group_data():
    query = """
        SELECT group_id, user_id, ord_num, class
        FROM account
    """
    column_order = ["group_id", "user_id", "ord_num", "class"]
    fetch_and_insert(query, "dim_user_group", column_order, is_dim=True)


@task
def insert_dim_user_data():
    query = """
        SELECT user_id, birth_year, gender
        FROM account
    """
    column_order = ["user_id", "birth_year", "gender"]
    fetch_and_insert(query, "dim_user", column_order, is_dim=True)


@task
def insert_fact_user_ratings_data(target_date: str = datetime.today().strftime('%Y-%m-%d')):
    query = """
        SELECT user_id, group_id, food_id, rating, created_date, updated_date
        FROM raw_user_ratings_data 
        WHERE DATE(created_date) = %s
    """
    column_order = ["user_id", "group_id", "food_id", "rating", "created_date", "updated_date"]
    fetch_and_insert(query, "fact_user_ratings", column_order, params=[target_date])
    