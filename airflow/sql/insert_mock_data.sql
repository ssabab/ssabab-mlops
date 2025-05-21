INSERT INTO account (user_id, password, username, email, role, created_at, updated_at, active, ord_num, gender, birth_year, class) VALUES
(1, 'password123', '홍길동', 'hong@example.com', 'USER', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, true, 9, true, 1995, 1),
(2, 'password456', '김철수', 'kim@example.com', 'USER', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, true, 9, false, 1996, 2),
(3, 'password789', '이영희', 'lee@example.com', 'USER', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, true, 9, false, 1997, 1),
(4, 'password012', '박지성', 'park@example.com', 'USER', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, true, 9, true, 1998, 2),
(5, 'password345', '최유진', 'choi@example.com', 'USER', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, true, 9, false, 1999, 1);

INSERT INTO food (food_id, food_name, main_sub, category, tag) VALUES
(1, '제육볶음', 'MAIN', 'KOREAN', 'MEAT'),
(2, '김치찌개', 'MAIN', 'KOREAN', 'SOUP'),
(3, '스테이크', 'MAIN', 'WESTERN', 'MEAT'),
(4, '파스타', 'MAIN', 'WESTERN', 'NOODLE'),
(5, '비빔밥', 'MAIN', 'KOREAN', 'RICE'),
(6, '삼겹살', 'MAIN', 'KOREAN', 'MEAT'),
(7, '치킨', 'MAIN', 'KOREAN', 'MEAT'),
(8, '피자', 'MAIN', 'WESTERN', 'MEAT'),
(9, '햄버거', 'MAIN', 'WESTERN', 'MEAT'),
(10, '샐러드', 'SUB', 'WESTERN', 'VEGETABLE');

INSERT INTO menu_review (id, user_id, menu_id, menu_comment, timestamp) VALUES
(1, 1, 1, '맛있었어요!', CURRENT_TIMESTAMP),
(2, 2, 2, '좋았습니다.', CURRENT_TIMESTAMP),
(3, 3, 3, '괜찮았어요', CURRENT_TIMESTAMP),
(4, 4, 4, '기대했던 것보다 맛있었어요', CURRENT_TIMESTAMP),
(5, 5, 5, '다음에도 주문할게요', CURRENT_TIMESTAMP);

INSERT INTO food_review (id, user_id, food_id, food_score, create_at) VALUES
(1, 1, 1, 5, CURRENT_TIMESTAMP),
(2, 2, 2, 4, CURRENT_TIMESTAMP),
(3, 3, 3, 5, CURRENT_TIMESTAMP),
(4, 4, 4, 3, CURRENT_TIMESTAMP),
(5, 5, 5, 5, CURRENT_TIMESTAMP),
(6, 1, 6, 4, CURRENT_TIMESTAMP),
(7, 2, 7, 5, CURRENT_TIMESTAMP),
(8, 3, 8, 2, CURRENT_TIMESTAMP),
(9, 4, 9, 3, CURRENT_TIMESTAMP),
(10, 5, 10, 4, CURRENT_TIMESTAMP),
(11, 1, 3, 5, CURRENT_TIMESTAMP),
(12, 2, 4, 4, CURRENT_TIMESTAMP),
(13, 3, 5, 5, CURRENT_TIMESTAMP),
(14, 4, 6, 3, CURRENT_TIMESTAMP),
(15, 5, 7, 4, CURRENT_TIMESTAMP);
