import os
import pandas as pd
from airflow.decorators import task
from airflow.operators.python import get_current_context
from utils.db import get_mysql_connection
from common.env_loader import load_env

load_env()

SQL_PATH = os.getenv("SQL_PATH")
ALLOWED_TABLES = {
    "ssabab_dw.dim_user",
    "ssabab_dw.dim_menu_food_combined",
    "ssabab_dw.fact_user_food_feedback",
    "ssabab_dw.fact_user_menu_feedback",
}


@task
def print_execution_date():
    context = get_current_context()
    execution_date = context["execution_date"].format("YYYY-MM-DD")
    print(f"execution_date: {execution_date}")


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
        print(f"Inserted {len(df)} rows into `{target_table}` using strategy '{insert_strategy}'.")


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
                                print(f"Executing SQL file: {file_path}")
                                cur.execute(sql)
                        except Exception as e:
                            print(f"Failed to execute {filename}: {e}")
            conn.commit()
            print("All SQL files executed successfully.")


@task
def insert_dim_user():
    query = """
        SELECT 
            user_id,
            LOWER(gender) AS gender,
            YEAR(birth_date) AS birth_year,
            CAST(ssafy_year AS UNSIGNED) AS ssafy_year,
            CAST(class_num AS UNSIGNED) AS ssafy_class,
            ssafy_region
        FROM account
    """
    column_order = ["user_id", "gender", "birth_year", "ssafy_year", "ssafy_class", "ssafy_region"]
    fetch_and_insert(query, "ssabab_dw.dim_user", column_order, insert_strategy="ignore")


@task
def insert_dim_menu_food_combined():
    query = """
        SELECT 
            mf.menu_id,
            m.date AS menu_date,
            mf.food_id,
            f.food_name,
            CASE 
                WHEN f.main_sub = '주메뉴' THEN 'main'
                WHEN f.main_sub = '서브메뉴' THEN 'sub'
                ELSE 'etc'
            END AS main_sub,
            f.category AS category_name,
            f.tag AS tag_name
        FROM menu_food mf
        JOIN food f ON mf.food_id = f.food_id
        JOIN menu m ON mf.menu_id = m.menu_id
    """
    column_order = ["menu_id", "menu_date", "food_id", "food_name", "main_sub", "category_name", "tag_name"]
    fetch_and_insert(query, "ssabab_dw.dim_menu_food_combined", column_order, insert_strategy="ignore")


@task
def insert_fact_user_food_feedback():
    context = get_current_context()
    execution_date = context["execution_date"].format("YYYY-MM-DD")
    query = """
        SELECT user_id, food_id, food_score, DATE(timestamp) AS rating_date
        FROM food_review
        WHERE DATE(timestamp) = %s
    """
    column_order = ["user_id", "food_id", "food_score", "rating_date"]
    fetch_and_insert(query, "ssabab_dw.fact_user_food_feedback", column_order, params=[execution_date])


@task
def insert_fact_user_menu_feedback():
    context = get_current_context()
    execution_date = context["execution_date"].format("YYYY-MM-DD")
    query = """
        SELECT 
            mr.user_id,
            mr.menu_id,
            mr.menu_score,
            mr.menu_regret,
            mr.menu_comment,
            CASE 
                WHEN pv.pre_vote_id IS NOT NULL THEN 1
                ELSE 0
            END AS pre_vote,
            DATE(mr.timestamp) AS comment_date
        FROM menu_review mr
        LEFT JOIN pre_vote pv
            ON mr.user_id = pv.user_id AND mr.menu_id = pv.menu_id
        WHERE DATE(mr.timestamp) = %s
    """
    column_order = ["user_id", "menu_id", "menu_score", "menu_regret", "menu_comment", "pre_vote", "comment_date"]
    fetch_and_insert(query, "ssabab_dw.fact_user_menu_feedback", column_order, params=[execution_date])
