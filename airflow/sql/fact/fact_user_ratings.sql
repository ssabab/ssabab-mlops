CREATE TABLE IF NOT EXISTS fact_user_ratings (
    user_id INT,
    food_id INT,
    score FLOAT,
    created_date TIMESTAMP, -- 최초 평점 입력 날짜
    PRIMARY KEY (user_id, food_id, created_date),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id) ON DELETE CASCADE,
    FOREIGN KEY (food_id) REFERENCES dim_food(food_id) ON DELETE CASCADE
);
