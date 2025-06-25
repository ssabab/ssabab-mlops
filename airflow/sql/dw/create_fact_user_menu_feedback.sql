CREATE TABLE IF NOT EXISTS ssabab_dw.fact_user_menu_feedback (
    user_id BIGINT,
    menu_id BIGINT,
    menu_score FLOAT,
    menu_regret BOOLEAN,
    menu_comment VARCHAR(255),
    pre_voted BOOLEAN,
    comment_date DATE,
    PRIMARY KEY (user_id, menu_id, comment_date),
    FOREIGN KEY (user_id) REFERENCES ssabab_dw.dim_user(user_id),
    FOREIGN KEY (menu_id) REFERENCES ssabab_dw.dim_menu_food_combined(menu_id)
);
