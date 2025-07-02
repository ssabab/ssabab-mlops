CREATE TABLE IF NOT EXISTS ssabab_dw.dim_user (
    user_id BIGINT PRIMARY KEY,
    gender ENUM('m', 'f'),
    birth_year INT,
    ssafy_year INT,
    ssafy_class INT,
    ssafy_region VARCHAR(50)
);
