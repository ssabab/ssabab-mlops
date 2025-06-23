CREATE TABLE IF NOT EXISTS ssabab_dm.dm_user_tag_stats (
    user_id BIGINT,
    tag VARCHAR(50),
    count INT,
    ratio FLOAT,
    PRIMARY KEY (user_id, tag)
);
