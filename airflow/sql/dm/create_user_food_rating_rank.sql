CREATE TABLE IF NOT EXISTS ssabab_dm.dm_user_food_rating_rank (
    user_id BIGINT,
    food_name VARCHAR(50),
    food_score FLOAT,
    rank_order INT,
    score_type ENUM('best', 'worst'),
    PRIMARY KEY (user_id, score_type, rank_order)
);
