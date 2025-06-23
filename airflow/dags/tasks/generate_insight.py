import os
from string import Template
from datetime import date
import pymysql.cursors
from utils.db import get_mysql_connection
from utils.model import load_llm_model
from airflow.decorators import task


@task
def fetch_user_ids():
    query = "SELECT DISTINCT user_id FROM ssabab_dw.fact_user_ratings"
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
                SELECT avg_score, total_reviews, pre_vote_count
                FROM ssabab_dm.dm_user_summary
                WHERE user_id = %s
            """, (user_id,))
            summary = cursor.fetchone()

            cursor.execute("""
                SELECT food_name, food_score
                FROM ssabab_dm.dm_user_food_rating_rank
                WHERE user_id = %s AND score_type = 'best'
                ORDER BY rank_order ASC
                LIMIT 3
            """, (user_id,))
            top_foods = cursor.fetchall()

            cursor.execute("""
                SELECT tag, ratio
                FROM ssabab_dm.dm_user_tag_stats
                WHERE user_id = %s
                ORDER BY ratio DESC
                LIMIT 1
            """, (user_id,))
            top_tag = cursor.fetchone()

            cursor.execute("""
                SELECT user_avg_score, group_avg_score, user_diversity_score, group_diversity_score
                FROM ssabab_dm.dm_user_group_comparison
                WHERE user_id = %s AND group_type = 'age'
            """, (user_id,))
            group_comparison = cursor.fetchone()

        return {
            "summary": summary,
            "top_foods": top_foods,
            "top_tag": top_tag,
            "group_comparison": group_comparison
        }

    finally:
        conn.close()


def generate_insight_chunk(user_id, user_data, tokenizer, model):
    summary = user_data.get("summary", {})
    top_foods = user_data.get("top_foods", [])
    top_tag = user_data.get("top_tag", {})
    group = user_data.get("group_comparison", {})

    top_food_lines = "\n".join(
        [f"- {row['food_name']} ({row['food_score']}점)" for row in top_foods]
    ) or "- 없음"

    with open("prompts/user_insight_prompt.txt", "r", encoding="utf-8") as f:
        template = Template(f.read())

    prompt = template.substitute(
        user_id=user_id,
        avg_score=summary.get("avg_score", "N/A"),
        total_reviews=summary.get("total_reviews", "N/A"),
        pre_vote_count=summary.get("pre_vote_count", "N/A"),
        top_food_lines=top_food_lines,
        top_tag=top_tag.get("tag", "없음"),
        top_tag_ratio=round(top_tag.get("ratio", 0) * 100),
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
    return tokenizer.decode(outputs[0], skip_special_tokens=True).strip()


def save_user_insight(user_id, insight_text, insight_date):
    conn = get_mysql_connection()
    query = """
        INSERT INTO ssabab_dm.dm_user_insight (user_id, insight_date, insight_text)
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
