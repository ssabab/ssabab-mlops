CREATE TABLE IF NOT EXISTS ssabab_dw.raw_user_review_word (
    user_id BIGINT,
    word VARCHAR(50),
    comment_date DATE,
    count INT,
    created_at, TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, word, comment_date),
);
