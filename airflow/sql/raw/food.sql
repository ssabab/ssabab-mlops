-- dim_food
CREATE TABLE IF NOT EXISTS food (
    food_id INT PRIMARY KEY,
    food_name VARCHAR(100) NOT NULL,
    main_sub VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    tag VARCHAR(50) NOT NULL
);
