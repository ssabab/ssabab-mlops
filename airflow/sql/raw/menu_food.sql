CREATE TABLE IF NOT EXISTS menu_food (
    menu_id INT REFERENCES menu(menu_id),
    food_id INT REFERENCES food(food_id),
    PRIMARY KEY (menu_id, food_id)
);
