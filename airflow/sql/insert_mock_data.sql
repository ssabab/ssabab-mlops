INSERT INTO menu (menu_id, date) VALUES
(3, '2025-06-25'), (4, '2025-06-25'), (5, '2025-06-26'), (6, '2025-06-26');

INSERT INTO menu_food (menu_id, food_id) VALUES
(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6),
(2, 7), (2, 8), (2, 9), (2, 10), (2, 11), (2, 12),
(3, 13), (3, 14), (3, 15), (3, 16), (3, 17), (3, 18),
(4, 19), (4, 20), (4, 21), (4, 22), (4, 23), (4, 24),
(5, 25), (5, 26), (5, 27), (5, 28), (5, 29), (5, 30),
(6, 31), (6, 32), (6, 33), (6, 34), (6, 35), (6, 36);

INSERT INTO menu_review (id, user_id, menu_id, menu_score, menu_regret, menu_comment, timestamp) VALUES
(1, 9, 1, 3.5, 0, '기대했던 것보다 맛있었어요', '2025-06-25'),
(2, 9, 4, 4.0, 0, '개 맛도리', '2025-06-26'),
(3, 9, 5, 1.0, 1, '우웩, 너무 별로입니다.', CURRENT_TIMESTAMP);


INSERT INTO food_review (id, user_id, food_id, food_score, timestamp) VALUES
(1, 9, 25, 2, CURRENT_TIMESTAMP),
(2, 9, 26, 1, CURRENT_TIMESTAMP),
(3, 9, 27, 1.5, CURRENT_TIMESTAMP),
(4, 9, 28, 2.5, CURRENT_TIMESTAMP),
(5, 9, 29, 2, CURRENT_TIMESTAMP),
(6, 9, 30, 3, CURRENT_TIMESTAMP);

INSERT INTO pre_vote (pre_vote_id, menu_id, user_id) VALUES
(4, 1, 9), (5, 4, 9), (6, 5, 9);

INSERT INTO ssabab_dw.fact_user_food_feedback (user_id, food_id, food_score, rating_date) VALUES
(9, 1, 4.5, '2025-06-25'),
(9, 2, 5, '2025-06-25'),
(9, 3, 2.5, '2025-06-25'),
(9, 4, 1, '2025-06-25'),
(9, 5, 1.5, '2025-06-25'),
(9, 6, 3, '2025-06-25'),
(9, 19, 4.5, '2025-06-26'),
(9, 20, 5, '2025-06-26'),
(9, 21, 4, '2025-06-26'),
(9, 22, 5, '2025-06-26'),
(9, 23, 3.5, '2025-06-26'),
(9, 24, 3.5, '2025-06-26');
