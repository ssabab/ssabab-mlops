CREATE TABLE IF NOT EXISTS dm_user_category_tag_preference (
    user_id INT PRIMARY KEY,
    category_json JSONB, -- 예: [{"category": "한식", "count": 12}, {"category": "중식", "count": 9}]
    tag_json JSONB  -- 예: [{"tag": "고기", "count": 7}, {"tag": "밥", "count": 5}]
);
