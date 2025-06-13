-- account
INSERT INTO account (
    user_id, password, provider, provider_id, username, email, role,
    created_at, updated_at, active, ssafy_year, class_num,
    ssafy_region, gender, age, profile_image_url, refresh_token
) VALUES
(1, 'password123', 'kakao', 'kakao_1', '홍길동', 'hong@example.com', 'USER',
 CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), 1, '9', '1',
 '서울', 'M', 25, 'https://example.com/profiles/1.jpg', 'token_1'),

(2, 'password456', 'kakao', 'kakao_2', '김철수', 'kim@example.com', 'USER',
 CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), 1, '9', '2',
 '서울', 'M', 24, 'https://example.com/profiles/2.jpg', 'token_2'),

(3, 'password789', 'naver', 'naver_3', '이영희', 'lee@example.com', 'USER',
 CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), 1, '9', '1',
 '대전', 'F', 23, 'https://example.com/profiles/3.jpg', 'token_3'),

(4, 'password012', 'naver', 'naver_4', '박지성', 'park@example.com', 'USER',
 CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), 1, '9', '2',
 '광주', 'M', 24, 'https://example.com/profiles/4.jpg', 'token_4'),

(5, 'password345', 'google', 'google_5', '최유진', 'choi@example.com', 'USER',
 CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), 1, '9', '1',
 '구미', 'F', 22, 'https://example.com/profiles/5.jpg', 'token_5');


-- food
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

-- menu
INSERT INTO menu (menu_id, date) VALUES
(1, '2025-06-10'),
(2, '2025-06-11'),
(3, '2025-06-12'),
(4, '2025-06-13'),
(5, '2025-06-14');

-- menu_food
INSERT INTO menu_food (menu_id, food_id) VALUES
(1, 1),
(1, 10),
(2, 2),
(2, 9),
(3, 3),
(3, 8),
(4, 4),
(4, 7),
(5, 5),
(5, 6);

-- menu_review
INSERT INTO menu_review (id, user_id, menu_id, menu_score, menu_comment, timestamp) VALUES
(1, 1, 1, 4.5, '맛있었어요!', CURRENT_TIMESTAMP),
(2, 2, 2, 4.0, '좋았습니다.', CURRENT_TIMESTAMP),
(3, 3, 3, 3.5, '괜찮았어요', CURRENT_TIMESTAMP),
(4, 4, 4, 4.2, '기대했던 것보다 맛있었어요', CURRENT_TIMESTAMP),
(5, 5, 5, 5.0, '다음에도 주문할게요', CURRENT_TIMESTAMP);

-- food_review
INSERT INTO food_review (id, user_id, food_id, food_score, timestamp) VALUES
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

-- pre_vote
INSERT INTO pre_vote (pre_vote_id, menu_id, user_id) VALUES
(1, 1, 1),
(2, 2, 2),
(3, 3, 3),
(4, 4, 4),
(5, 5, 5);

-- friend
INSERT INTO friend (user_id, user_id) VALUES
(1, 2),
(1, 3),
(2, 3),
(3, 4),
(4, 5);
