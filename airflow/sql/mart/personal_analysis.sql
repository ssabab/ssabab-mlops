CREATE TABLE dm_user_summary (
    user_id BIGINT PRIMARY KEY,
    avg_score FLOAT,
    total_reviews INT,
    pre_vote_count INT
);

CREATE TABLE dm_user_food_rating_rank (
    user_id BIGINT,
    food_name VARCHAR(50),
    food_score FLOAT,
    rank_order INT,
    score_type ENUM('best', 'worst'),
    PRIMARY KEY (user_id, score_type, rank_order)
);

CREATE TABLE dm_user_category_stats (
    user_id BIGINT,
    category VARCHAR(50),
    count INT,
    ratio FLOAT,
    PRIMARY KEY (user_id, category)
);

CREATE TABLE dm_user_tag_stats (
    user_id BIGINT,
    tag VARCHAR(50),
    count INT,
    ratio FLOAT,
    PRIMARY KEY (user_id, tag)
);

CREATE TABLE dm_user_diversity_comparison (
    user_id BIGINT,
    group_type ENUM('all', 'gender', 'age'),
    user_avg_score FLOAT,
    group_avg_score FLOAT,
    user_diversity_score FLOAT,
    group_diversity_score FLOAT,
    PRIMARY KEY (user_id, group_type)
);

CREATE TABLE dm_user_review_word (
    user_id BIGINT,
    word VARCHAR(30),
    count INT,
    sentiment_score FLOAT,
    PRIMARY KEY (user_id, word)
);

CREATE TABLE dm_user_insight (
    user_id BIGINT PRIMARY KEY,
    insight TEXT
);
