from datetime import date
import pymysql.cursors
from utils.db import get_mysql_connection
from utils.model import load_llm_model
from airflow.decorators import task


def get_all_user_ids():
    query = "SELECT DISTINCT user_id FROM food_review"
    conn = get_mysql_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return [row[0] for row in rows]
    finally:
        conn.close()

@task
def fetch_user_ids():
    return get_all_user_ids()


def load_user_data_task(user_id):
    query = """
        SELECT fr.food_id, f.food_name, fr.food_score
        FROM food_review fr
        JOIN food f ON fr.food_id = f.food_id
        WHERE fr.user_id = %s
        ORDER BY fr.timestamp DESC
        LIMIT 5
    """
    conn = get_mysql_connection()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(query, (user_id,))
            return cursor.fetchall()
    finally:
        conn.close()


def generate_insight_chunk(user_id, user_data, tokenizer, model):
    review_lines = "\n".join([
        f"- {row['food_name']} ({row['food_score']}점)"
        for row in user_data
    ])

    prompt = f"""
        당신은 사용자 음식 평가 데이터를 기반으로 인사이트를 제공하는 AI입니다.
        다음은 user_id={user_id} 사용자의 최근 음식 평가 내역입니다:

        {review_lines}

        이 사용자의 취향과 선호를 분석하여 1~2문장으로 요약된 인사이트를 생성하세요.
    """

    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    inputs.pop("token_type_ids", None)

    outputs = model.generate(
        **inputs,
        max_new_tokens=300,
        do_sample=True,
        top_k=50,
        top_p=0.95,
        temperature=0.7
    )
    result = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return result.strip()

def save_user_insight(user_id, insight_text, insight_date):
    conn = get_mysql_connection()
    query = """
        INSERT INTO dm_user_insight (user_id, insight_date, insight_text)
        VALUES (%s, %s, %s)
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (user_id, insight_date, insight_text))
        conn.commit()
    finally:
        conn.close()

@task
def generate_user_insight_task(user_ids):
    tokenizer, model = load_llm_model()

    for user_id in user_ids:
        try:
            user_data = load_user_data_task(user_id=user_id)
            insight = generate_insight_chunk(user_id=user_id, user_data=user_data, tokenizer=tokenizer, model=model)
            if insight:
                save_user_insight(user_id, insight, date.today())
                print(f"user_id={user_id} insight saved.")
        except Exception as e:
            print(f"Failed for user_id={user_id}: {e}")