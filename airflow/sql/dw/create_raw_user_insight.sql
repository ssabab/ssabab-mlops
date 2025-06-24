CREATE TABLE IF NOT EXISTS ssabab_dw.raw_user_insight (
    user_id BIGINT,
    insight_date DATE,
    insight TEXT,
    PRIMARY KEY (user_id, insight_date)
);
