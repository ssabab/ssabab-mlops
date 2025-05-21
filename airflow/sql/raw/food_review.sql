CREATE TABLE IF NOT EXISTS food_review (
    id INT PRIMARY KEY,
    food_score INT NOT NULL,
    create_at TIMESTAMP NOT NULL,
    food_id INT REFERENCES food(food_id),
    user_id INT REFERENCES account(user_id)
);
