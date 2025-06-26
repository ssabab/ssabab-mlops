CREATE TABLE IF NOT EXISTS ssabab_dw.xgb_train_data (
    user_id BIGINT,
    train_date DATE,

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

    user_gender ENUM('M', 'F'),
    user_age INT,

    menu_regret BOOLEAN,

    label TINYINT,

    PRIMARY KEY (user_id, train_date)
);
