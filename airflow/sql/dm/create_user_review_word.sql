CREATE TABLE IF NOT EXISTS ssabab_dm.dm_user_review_word (
    user_id BIGINT,
    word VARCHAR(30),
    count INT,
    PRIMARY KEY (user_id, word)
);
