CREATE TABLE IF NOT EXISTS ssabab_dw.dim_user (
    user_id BIGINT PRIMARY KEY,
    gender ENUM('M', 'F'),
    birth_year INT,
    ssafy_class VARCHAR(10),
    region VARCHAR(50)
);
