-- 스키마 생성
CREATE SCHEMA IF NOT EXISTS ssabab_dw;
USE ssabab_dw;

-- 사용자 차원 테이블
CREATE TABLE dim_user (
    user_id BIGINT PRIMARY KEY,
    gender ENUM('M', 'F'),
    birth_year INT,
    ssafy_class VARCHAR(10),
    region VARCHAR(50)
);

-- 카테고리 차원 테이블
CREATE TABLE dim_category (
    category_id BIGINT PRIMARY KEY,
    category_name VARCHAR(50)
);

-- 음식 차원 테이블
CREATE TABLE dim_food (
    food_id BIGINT PRIMARY KEY,
    food_name VARCHAR(50),
    category_id BIGINT,
    FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
);

-- 태그 차원 테이블
CREATE TABLE dim_tag (
    tag_id BIGINT PRIMARY KEY,
    tag_name VARCHAR(50)
);

-- 사용자 음식 평가 사실 테이블
CREATE TABLE fact_user_ratings (
    user_id BIGINT,
    food_id BIGINT,
    food_score FLOAT,
    created_date DATE,
    PRIMARY KEY (user_id, food_id, created_date),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    FOREIGN KEY (food_id) REFERENCES dim_food(food_id)
);

-- 사용자 태그 기록 테이블
CREATE TABLE fact_user_tags (
    user_id BIGINT,
    food_id BIGINT,
    tag_id BIGINT,
    created_date DATE,
    PRIMARY KEY (user_id, food_id, tag_id),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    FOREIGN KEY (tag_id) REFERENCES dim_tag(tag_id)
);

-- 사용자 사전 투표 기록 테이블
CREATE TABLE fact_user_pre_votes (
    user_id BIGINT,
    food_id BIGINT,
    vote_date DATE,
    PRIMARY KEY (user_id, food_id, vote_date),
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id),
    FOREIGN KEY (food_id) REFERENCES dim_food(food_id)
);
