from collections import Counter
import pandas as pd
from konlpy.tag import Okt
from tqdm import tqdm
from airflow.decorators import task
from utils.db import get_mysql_connection

STOPWORDS = {
    "정말", "진짜", "너무", "매우", "그리고", "그냥", "또", "더", "더욱",
    "거", "것", "근데", "그건", "이건", "저건", "그게", "이게", "저게",
    "해서", "했는데", "했어요", "입니다", "있어요", "있는", "없는", "하기",
    "처럼", "같이", "때문", "하면서", "보다", "보다도", "까지",
    "맛", "밥", "음식", "요리", "식사", "메뉴"
}

@task
def extract_raw_user_review_word():
    conn = get_mysql_connection()
    query = """
        SELECT user_id, menu_comment, comment_date 
        FROM ssabab_dw.fact_user_comments
    """
    df = pd.read_sql(query, conn)
    conn.close()

    okt = Okt()
    records = []

    for _, row in tqdm(df.iterrows(), total=len(df)):
        if pd.isnull(row["menu_comment"]):
            continue

        text = row["menu_comment"]
        result = okt.pos(text, norm=True, stem=True)
        print(f"POS result: {result}")

        words = [
            word for word, tag in result
            if tag in ("Noun", "Adjective") and word not in STOPWORDS
        ]
        print(f"comment: {text} → words: {words}")

        counter = Counter(words)

        for word, count in counter.items():
            records.append((
                row["user_id"],
                word,
                row["comment_date"],
                count
            ))

    if records:
        result_df = pd.DataFrame(records, columns=["user_id", "word", "comment_date", "count"])

        conn = get_mysql_connection()
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO ssabab_dw.raw_user_review_word (user_id, word, comment_date, count)
                VALUES (%s, %s, %s, %s)
            """, result_df.values.tolist())
        conn.commit()
        conn.close()
