CREATE TABLE IF NOT EXISTS ssabab_dw.fact_user_food_feedback (
    user_id BIGINT,
    food_id BIGINT,
    food_score FLOAT,
    rating_date DATE,
    PRIMARY KEY (user_id, food_id, rating_date),
    FOREIGN KEY (user_id) REFERENCES ssabab_dw.dim_user(user_id)
);
