from datetime import date
from tasks.load_user_data import user_chunks
from utils.llm import call_llm_and_generate_insights 


insight_results = []

def generate_insight_chunk(chunk_id):
    chunk_df = user_chunks[chunk_id]
    results = []

    for _, row in chunk_df.iterrows():
        user_id = row["user_id"]
        prompt = format_prompt(row)
        insight = call_llm_and_generate_insights(prompt)
        results.append({
            "user_id": user_id,
            "insight_date": date.today().isoformat(),
            "insight_text": insight
        })

    global insight_results
    insight_results += results


def format_prompt(user_row):
    return f"""
        사용자 정보:
        - 성별: {user_row['gender']}
        - 출생년도: {user_row['birth_year']}
        - 친구 수: {len(user_row['friends'].split(','))}

        최근 메뉴:
        {user_row['menu_history']}

        위 사용자의 식사 기록과 평가를 바탕으로 따뜻한 말투로 인사이트를 2~3줄 생성해주세요.
    """
