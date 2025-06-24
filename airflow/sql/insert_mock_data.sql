INSERT INTO menu_review (id, user_id, menu_id, menu_score, menu_comment, timestamp) VALUES
(4, 9, 1, 4.2, '기대했던 것보다 맛있었어요', CURRENT_TIMESTAMP);


INSERT INTO food_review (id, user_id, food_id, food_score, timestamp) VALUES
(11, 9, 1, 4.5, CURRENT_TIMESTAMP),
(12, 9, 2, 5, CURRENT_TIMESTAMP),
(13, 9, 3, 3, CURRENT_TIMESTAMP),
(14, 9, 4, 2, CURRENT_TIMESTAMP),
(15, 9, 5, 4, CURRENT_TIMESTAMP),
(16, 9, 6, 3.5, CURRENT_TIMESTAMP);

INSERT INTO pre_vote (pre_vote_id, menu_id, user_id) VALUES
(4, 1, 9);

INSERT INTO ssabab_dw.fact_user_ratings (user_id, food_id, food_score, rating_date) VALUES
(9, 7, 4.5, '2025-06-23'),
(9, 8, 5, '2025-06-23'),
(9, 9, 2.5, '2025-06-23'),
(9, 10, 1, '2025-06-23'),
(9, 11, 1.5, '2025-06-23'),
(9, 12, 3, '2025-06-23');

