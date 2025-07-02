CREATE TABLE IF NOT EXISTS ssabab_dw.raw_user_review_word (
    user_id BIGINT,
    word VARCHAR(50),
    comment_date DATE,
    count INT,
    PRIMARY KEY (user_id, word, comment_date),
    FOREIGN KEY (user_id) REFERENCES ssabab_dw.dim_user(user_id)
);
