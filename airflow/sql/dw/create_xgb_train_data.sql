CREATE TABLE IF NOT EXISTS ssabab_dw.xgb_train_data (
    user_id BIGINT,
    user_gender ENUM('m', 'f'),
    user_age INT,

    A_tag_overlap INT,
    B_tag_overlap INT,
    A_tag_ratio FLOAT,
    B_tag_ratio FLOAT,

    A_category_overlap INT,
    B_category_overlap INT,
    A_category_ratio FLOAT,
    B_category_ratio FLOAT,

    A_avg_score_on_tags FLOAT,
    B_avg_score_on_tags FLOAT,
    A_avg_score_on_categories FLOAT,
    B_avg_score_on_categories FLOAT,

    menu_regret TINYINT,

    label TINYINT,
    train_date DATE,

    PRIMARY KEY (user_id, train_date),
    FOREIGN KEY (user_id) REFERENCES ssabab_dw.dim_user(user_id)
);
