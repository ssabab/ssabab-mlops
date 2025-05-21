CREATE TABLE IF NOT EXISTS dm_user_rating_top_bottom (
    user_id INT PRIMARY KEY,
    best_foods_json JSONB,
    worst_foods_json JSONB
    -- 예: [{"food_name": "돈까스", "score": 5.0}, {"food_name": "김치찌개", "score": 4.8}]
);
