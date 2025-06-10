CREATE TABLE IF NOT EXISTS dim_user (
    user_id INT PRIMARY KEY,
    birth_year INT,
    gender CHAR(1),
    ord_num INT,
    class INT
);
