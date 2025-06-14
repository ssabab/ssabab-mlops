CREATE TABLE IF NOT EXISTS dim_food (
    food_id     INT PRIMARY KEY,
    food_name   VARCHAR(255),
    category    ENUM('한식', '중식', '양식', '일식', '기타'),
    tag         ENUM('밥', '국', '면', '고기', '야채', '생선', '기타')
);
