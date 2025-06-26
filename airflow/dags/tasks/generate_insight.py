import os
from string import Template
import pandas as pd
import pymysql.cursors
from airflow.decorators import task
from airflow.operators.python import get_current_context
from utils.db import get_mysql_connection
from utils.model import load_llm_model
from common.env_loader import load_env

load_env()

PROMPT_PATH = os.getenv("PROMPT_PATH")


@task
def fetch_user_ids():
    query = "SELECT DISTINCT user_id FROM ssabab_dw.fact_user_food_feedback"
    conn = get_mysql_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return [row[0] for row in rows]
    finally:
        conn.close()

def load_user_data_task(user_id):
    conn = get_mysql_connection()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("""
                SELECT food_name, food_score
                FROM ssabab_dm.dm_user_food_rating_rank
                WHERE user_id = %s AND score_type = 'best'
                ORDER BY rank_order ASC
                LIMIT 3
            """, (user_id,))
            top_foods = cursor.fetchall()

            cursor.execute("""
                SELECT tag, count
                FROM ssabab_dm.dm_user_tag_stats
                WHERE user_id = %s
                ORDER BY count DESC
                LIMIT 3
            """, (user_id,))
            top_tag = cursor.fetchone()

            cursor.execute("""
                SELECT category, count
                FROM ssabab_dm.dm_user_category_stats
                WHERE user_id = %s
                ORDER BY count DESC
                LIMIT 3
            """, (user_id,))
            top_category = cursor.fetchone()

            cursor.execute("""
                SELECT user_avg_score, group_avg_score, user_diversity_score, group_diversity_score
                FROM ssabab_dm.dm_user_group_comparison
                WHERE user_id = %s AND group_type = 'all'
            """, (user_id,))
            group_comparison = cursor.fetchone()

        return {
            "top_foods": top_foods,
            "top_tag": top_tag,
            "top_category": top_category,
            "group_comparison": group_comparison
        }

    finally:
        conn.close()


def generate_insight_chunk(user_id, user_data, tokenizer, model):
    top_foods = user_data.get("top_foods", [])
    top_tag = user_data.get("top_tag", {})
    top_category = user_data.get("top_category", [])
    group = user_data.get("group_comparison", {})

    top_food_lines = "\n".join(
        [f"- {row['food_name']} ({row['food_score']}점)" for row in top_foods]
    ) or "- 없음"

    with open(os.path.join(PROMPT_PATH, "user_insight_prompt.txt"), "r", encoding="utf-8") as f:
        template = Template(f.read())

    prompt = template.substitute(
        user_id=user_id,
        top_food_lines=top_food_lines,
        top_tag=top_tag.get("tag", "없음"),
        top_tag_count=top_tag.get("count", "N/A"),
        top_category=top_category.get("category", "없음"),
        top_category_count=top_category.get("count", "N/A"),
        user_avg_score=group.get("user_avg_score", "N/A"),
        group_avg_score=group.get("group_avg_score", "N/A"),
        user_diversity_score=group.get("user_diversity_score", "N/A"),
        group_diversity_score=group.get("group_diversity_score", "N/A"),
    )

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

    input_len = inputs["input_ids"].shape[-1]
    generated_text = outputs[0][input_len:]
    return tokenizer.decode(generated_text, skip_special_tokens=True).strip()


def save_user_insight(user_id, insight):
    conn = get_mysql_connection()
    query = """
        INSERT INTO ssabab_dm.dm_user_insight (user_id, insight)
        VALUES (%s, %s, %s)
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (user_id, insight))
        conn.commit()
    finally:
        conn.close()


@task
def generate_user_insight_task(user_ids):
    tokenizer, model = load_llm_model()
    insights = {}
    for user_id in user_ids:
        try:
            user_data = load_user_data_task(user_id=user_id)
            insight = generate_insight_chunk(user_id=user_id, user_data=user_data, tokenizer=tokenizer, model=model)
            if insight:
                insights[user_id] = insight
        except Exception as e:
            print(f"Failed for user_id={user_id}: {e}")
    return insights


@task
def insert_raw_user_insight(insights):
    if not insights:
        print("insights is empty or None, skipping insert.")
        return
    
    context = get_current_context()
    insight_date = context["execution_date"].format("YYYY-MM-DD")

    rows = [(int(uid), insight_date, content) for uid, content in insights.items()]
    df = pd.DataFrame(rows, columns=["user_id", "insight_date", "insight"])

    with get_mysql_connection() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO ssabab_dw.raw_user_insight (user_id, insight_date, insight)
                    VALUES (%s, %s, %s)
                """, tuple(row))
        conn.commit()
