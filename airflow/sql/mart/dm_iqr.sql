CREATE TABLE IF NOT EXISTS dm_iqrs (
    iqr_type ENUM('total', 'ord', 'gender') PRIMARY KEY,
    q1 FLOAT,
    q2 FLOAT,
    q3 FLOAT
);
