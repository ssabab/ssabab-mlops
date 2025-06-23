CREATE TABLE IF NOT EXISTS dm_class_engagement_monthly (
    month DATE NOT NULL,             -- 월 단위 (예: 2024-05-01)
    class TEXT NOT NULL,             -- 반 (예: A, B, C)
    generation TEXT NOT NULL,        -- 기수 (예: 10기)
    review_count INTEGER DEFAULT 0,  -- 해당 월 리뷰 수
    PRIMARY KEY (month, class, generation)
);
