CREATE TABLE IF NOT EXISTS ssabab_dw.dim_menu_food_combined (
    menu_id        BIGINT,
    menu_date      DATE,
    food_id        BIGINT,
    food_name      VARCHAR(50),
    main_sub       ENUM('MAIN', 'SUB'),
    category_name  VARCHAR(50),
    tag_name       VARCHAR(50),
    PRIMARY KEY (food_id, menu_id)
);
