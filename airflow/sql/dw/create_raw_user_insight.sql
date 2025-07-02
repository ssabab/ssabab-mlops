CREATE TABLE IF NOT EXISTS ssabab_dw.raw_user_insight (
    user_id BIGINT,
    insight_date DATE,
    insight TEXT,
    PRIMARY KEY (user_id, insight_date),
    FOREIGN KEY (user_id) REFERENCES ssabab_dw.dim_user(user_id)
);
