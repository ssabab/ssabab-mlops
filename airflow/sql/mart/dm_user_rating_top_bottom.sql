CREATE TABLE IF NOT EXISTS dm_user_rating_top_bottom (
    user_id INT PRIMARY KEY,

    best_food1_name VARCHAR(50),
    best_food1_score FLOAT,

    best_food2_name VARCHAR(50),
    best_food2_score FLOAT,

    best_food3_name VARCHAR(50),
    best_food3_score FLOAT,

    best_food4_name VARCHAR(50),
    best_food4_score FLOAT,

    best_food5_name VARCHAR(50),
    best_food5_score FLOAT,

    worst_food1_name VARCHAR(50),
    worst_food1_score FLOAT,

    worst_food2_name VARCHAR(50),
    worst_food2_score FLOAT,

    worst_food3_name VARCHAR(50),
    worst_food3_score FLOAT,

    worst_food4_name VARCHAR(50),
    worst_food4_score FLOAT,

    worst_food5_name VARCHAR(50),
    worst_food5_score FLOAT
);
