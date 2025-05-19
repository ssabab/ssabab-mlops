CREATE TABLE IF NOT EXISTS pre_vote (
    pre_vote_id INT PRIMARY KEY,
    menu_id INT REFERENCES menu(menu_id),
    user_id INT REFERENCES account(user_id)
);
