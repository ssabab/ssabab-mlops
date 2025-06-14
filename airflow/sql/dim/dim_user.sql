CREATE TABLE dim_user (
    user_id     BIGINT PRIMARY KEY,
    birth_date  DATE,
    gender      VARCHAR(10),
    ssafy_year  VARCHAR(4),
    class_num   VARCHAR(10),
    region      VARCHAR(20)
);