CREATE TABLE IF NOT EXISTS ssabab_dm.dm_user_insight (
    user_id BIGINT,
    insight_date DATE,
    insight_text TEXT,
    PRIMARY KEY (user_id, insight_date)
);
