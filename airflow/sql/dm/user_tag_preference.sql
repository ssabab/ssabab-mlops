CREATE TABLE IF NOT EXISTS dm_user_tag_preference (
    user_id INT PRIMARY KEY,
    tag_json JSONB
    -- 예: [{"category": "한식", "count": 10}, {"tag": "맵", "count": 7}]
);
