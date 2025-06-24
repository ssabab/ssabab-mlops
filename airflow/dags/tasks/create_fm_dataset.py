from airflow.decorators import task
import pandas as pd
from sklearn.preprocessing import OneHotEncoder
import json
import sys
import os

# Add the project root to the Python path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from utils.db import get_mysql_connection

@task
def generate_fm_input(target_date):
    # 데이터베이스 연결 설정 (pymysql 직접 사용)
    conn = get_mysql_connection()
    
    # 1. 데이터 로딩 (날짜별로 쿼리)
    query = f"SELECT * FROM ssabab_dm.ml_menu_comparison WHERE date = '{target_date}'"
    df = pd.read_sql(query, con=conn)
    
    # 데이터가 없으면 빈 파일 생성하고 종료
    if df.empty:
        print(f"No data found for date: {target_date}")
        with open(f"/opt/mlops/data/train_{target_date}.libfm", "w") as f:
            f.write("")  # 빈 파일 생성
        conn.close()
        return

    # 2. 사용자 특성 인코딩
    user_ohe = OneHotEncoder()
    user_feat = user_ohe.fit_transform(df[['user_id', 'gender', 'region', 'class_num']])

    # 3. 음식 태그 벡터 (JSON → 펼침)
    def json_to_dict_list(col):
        return pd.json_normalize([json.loads(x) if x else {} for x in col])

    tags_a = json_to_dict_list(df['tag_vector_a'])
    tags_b = json_to_dict_list(df['tag_vector_b'])

    # 4. 평점 정보 등 추가
    df_features = pd.concat([
        pd.DataFrame(user_feat.toarray()),
        tags_a.add_prefix("tag_a_"),
        tags_b.add_prefix("tag_b_"),
        df[['avg_food_score_a', 'avg_food_score_b']],
        df['label']
    ], axis=1)

    # 5. libFM 포맷으로 변환
    def to_libfm(df):
        libfm_lines = []
        for _, row in df.iterrows():
            label = int(row['label'])
            features = [f"{i}:{1}" for i in range(len(row)-1) if row.iloc[i] != 0]
            line = f"{label} " + " ".join(features)
            libfm_lines.append(line)
        return libfm_lines

    libfm_data = to_libfm(df_features)
    with open(f"/opt/mlops/data/train_{target_date}.libfm", "w") as f:
        f.writelines("\n".join(libfm_data))
    
    # 연결 종료
    conn.close()