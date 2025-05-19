CREATE TABLE IF NOT EXISTS dm_food_ranking_monthly (
    month DATE NOT NULL,                  -- 월 단위 (예: 2024-05-01)
    food_id INT NOT NULL,                 -- 음식 ID
    food_name TEXT,                       -- 음식 이름 (조회용)
    avg_score NUMERIC(3,2),               -- 월간 평균 평점
    rank_type TEXT NOT NULL,              -- 랭킹 유형 (예: best, worst, trending)
    rank INT,                             -- 해당 음식의 랭킹
    PRIMARY KEY (month, food_id, rank_type)
);
