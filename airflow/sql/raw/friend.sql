CREATE TABLE IF NOT EXISTS friend (
    user_id INT REFERENCES account(user_id),
    friend_user_id INT REFERENCES account(user_id),
    PRIMARY KEY (user_id, friend_user_id)
);
