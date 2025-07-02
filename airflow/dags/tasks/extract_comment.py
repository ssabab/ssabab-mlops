from collections import Counter, defaultdict
import pandas as pd
from konlpy.tag import Okt
from airflow.decorators import task
from utils.db import get_mysql_connection
import json

STOPWORDS = {
    "정말", "진짜", "너무", "매우", "그리고", "그냥", "또", "더", "더욱",
    "거", "것", "근데", "그건", "이건", "저건", "그게", "이게", "저게",
    "해서", "했는데", "했어요", "입니다", "있어요", "있는", "없는", "하기",
    "처럼", "같이", "때문", "하면서", "보다", "보다도", "까지",
    "맛", "밥", "음식", "요리", "식사", "메뉴", "이다"
}


@task
def fetch_review_df():
    conn = get_mysql_connection()
    query = """
        SELECT user_id, menu_comment, comment_date 
        FROM ssabab_dw.fact_user_menu_feedback
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df.to_json(orient="records", force_ascii=False)


@task
def parse_nouns(json_str: str):
    df = pd.read_json(json_str)
    okt = Okt()
    
    merged_counter = defaultdict(int)

    for _, row in df.iterrows():
        if pd.isnull(row["menu_comment"]):
            continue

        text = row["menu_comment"]
        pos_result = okt.pos(text, norm=True, stem=True)

        words = [
            word for word, tag in pos_result
            if tag in ("Noun", "Adjective") and word not in STOPWORDS and len(word) > 1
        ]

        counter = Counter(words)
        for word, count in counter.items():
            key = (row["user_id"], word, row["comment_date"])
            merged_counter[key] += count

    records = [
        {
            "user_id": user_id,
            "word": word,
            "comment_date": comment_date,
            "count": count
        }
        for (user_id, word, comment_date), count in merged_counter.items()
    ]

    return json.dumps(records, ensure_ascii=False)


@task
def insert_review_keywords(json_str: str):
    records = json.loads(json_str)

    if not records:
        print("No records to insert.")
        return

    df = pd.DataFrame(records)

    conn = get_mysql_connection()
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO ssabab_dw.raw_user_review_word (user_id, word, comment_date, count)
            VALUES (%s, %s, %s, %s)
        """, df[["user_id", "word", "comment_date", "count"]].values.tolist())
    conn.commit()
    conn.close()
