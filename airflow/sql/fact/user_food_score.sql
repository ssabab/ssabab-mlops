CREATE TABLE IF NOT EXISTS fact_user_food_score (
    user_id INT REFERENCES dim_user(user_id),
    food_id INT REFERENCES dim_food(food_id),
    date DATE,
    food_score FLOAT,
    snapshot_date DATE DEFAULT CURRENT_DATE
);
