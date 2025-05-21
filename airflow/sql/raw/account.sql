-- dim_user
CREATE TABLE IF NOT EXISTS account (
    user_id INT PRIMARY KEY,
    password TEXT NOT NULL,
    provider VARCHAR(50),
    provider_id VARCHAR(255),
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    profile_image_url TEXT,
    role VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    active BOOLEAN NOT NULL,
    ord_num INT NOT NULL,
    class INT,
    gender BOOLEAN NOT NULL,
    birth_year INT NOT NULL
);
