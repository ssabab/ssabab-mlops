CREATE TABLE dm_user_insight (
    user_id         BIGINT,
    insight_date    DATE NOT NULL,
    insight_text    TEXT,
    PRIMARY KEY (user_id, insight_date)
);