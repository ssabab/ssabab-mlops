CREATE TABLE IF NOT EXISTS dm_user_rating_score (
    user_id INT PRIMARY KEY,
    score FLOAT,
    total_iqr_json JSONB,
    ord_iqr_json JSONB,
    gender_iqr_json JSONB
    -- 예: {"q1": 1.5, "q2": 2.0, "q3": 2.5}, {"ord_num": 13, ...}, {"gender": True, ...}
);
