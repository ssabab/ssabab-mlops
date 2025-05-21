CREATE TABLE IF NOT EXISTS dim_user_group (
    group_id INT PRIMARY KEY,
    user_id INT,
    ord_num INT,
    class INT,
    FOREIGN KEY (user_id) REFERENCES dim_user(user_id) ON DELETE CASCADE
);