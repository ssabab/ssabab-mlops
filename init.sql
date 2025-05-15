-- -- 기존 ENUM 타입, 테이블 삭제
-- DROP TABLE IF EXISTS dm_food_ranking_monthly CASCADE;
-- DROP TABLE IF EXISTS dm_class_engagement_monthly CASCADE;
-- DROP TABLE IF EXISTS food_review CASCADE;
-- DROP TABLE IF EXISTS menu_review CASCADE;
-- DROP TABLE IF EXISTS food CASCADE;
-- DROP TABLE IF EXISTS account CASCADE;
-- DROP TYPE IF EXISTS main_sub_type CASCADE;
-- DROP TYPE IF EXISTS category_type CASCADE;
-- DROP TYPE IF EXISTS tag_type CASCADE;

-- -- 데이터베이스 생성
-- -- CREATE DATABASE ssabab;

-- -- 데이터베이스 연결
-- -- \c ssabab;

-- -- ENUM 타입 생성
-- CREATE TYPE main_sub_type AS ENUM ('MAIN', 'SUB');
-- CREATE TYPE category_type AS ENUM ('KOREAN', 'CHINESE', 'JAPANESE', 'WESTERN', 'SNACK', 'DESSERT');
-- CREATE TYPE tag_type AS ENUM ('RICE', 'NOODLE', 'MEAT', 'VEGETABLE', 'FISH', 'SOUP', 'SIDE');

-- -- DW Tables
-- CREATE TABLE account
-- (
--   user_id           INT       NOT NULL,
--   password          VARCHAR   NOT NULL,
--   username          VARCHAR   NOT NULL,
--   email             VARCHAR   NOT NULL,
--   profile_image_url TEXT      NULL    ,
--   role              VARCHAR   NOT NULL,
--   created_at        TIMESTAMP NOT NULL,
--   updated_at        TIMESTAMP NOT NULL,
--   active            BOOLEAN   NOT NULL,
--   ord_num           INT       NOT NULL,
--   gender            BOOLEAN   NOT NULL,
--   birth_year        INT       NOT NULL,
--   class             INT      NOT NULL,
--   PRIMARY KEY (user_id)
-- );

-- CREATE TABLE food
-- (
--   food_id   INT     NOT NULL,
--   food_name VARCHAR NOT NULL,
--   main_sub  main_sub_type NOT NULL,
--   category  category_type NOT NULL,
--   tag       tag_type NOT NULL,
--   PRIMARY KEY (food_id)
-- );

-- CREATE TABLE menu_review
-- (
--   id           INT      NOT NULL,
--   user_id      INT      NOT NULL,
--   menu_id      INT      NOT NULL,
--   menu_comment TEXT     NULL    ,
--   timestamp    TIMESTAMP NOT NULL,
--   PRIMARY KEY (id)
-- );

-- CREATE TABLE food_review
-- (
--   id         INT       NOT NULL,
--   user_id    INT       NOT NULL,
--   food_id    INT       NOT NULL,
--   food_score INT       NOT NULL,
--   create_at  TIMESTAMP NOT NULL,
--   PRIMARY KEY (id)
-- );

-- -- DM Tables
-- CREATE TABLE dm_class_engagement_monthly
-- (
--   month        DATE    NOT NULL,
--   class        TEXT    NOT NULL,
--   generation   TEXT    NOT NULL,
--   review_count INTEGER DEFAULT 0,
--   PRIMARY KEY (month, class, generation)
-- );

-- CREATE TABLE dm_food_ranking_monthly
-- (
--   month     DATE         NOT NULL,
--   food_id   INTEGER      NOT NULL,
--   food_name TEXT        ,
--   avg_score NUMERIC(3,2),
--   rank_type TEXT         NOT NULL,
--   rank      INTEGER     ,
--   PRIMARY KEY (month, food_id, rank_type)
-- );

-- 테스트 데이터 삽입
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