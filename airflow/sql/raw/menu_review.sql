CREATE TABLE IF NOT EXISTS menu_review (
    id INT PRIMARY KEY,
    menu_comment TEXT,
    timestamp TIMESTAMP NOT NULL,
    user_id INT REFERENCES account(user_id),
    menu_id INT REFERENCES menu(menu_id)
);
