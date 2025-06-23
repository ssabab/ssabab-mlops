CREATE TABLE IF NOT EXISTS ssabab_dm.dm_user_summary (
    user_id BIGINT PRIMARY KEY,
    avg_score FLOAT,
    total_reviews INT,
    pre_vote_count INT
);