CREATE TABLE IF NOT EXISTS ssabab_dm.dm_user_category_stats (
    user_id BIGINT,
    category VARCHAR(50),
    count INT,
    ratio FLOAT,
    PRIMARY KEY (user_id, category)
);
