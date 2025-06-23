CREATE TABLE IF NOT EXISTS ssabab_dm.dm_user_group_comparison (
    user_id BIGINT,
    group_type ENUM('all', 'gender', 'age'),
    user_avg_score FLOAT,
    group_avg_score FLOAT,
    user_diversity_score FLOAT,
    group_diversity_score FLOAT,
    PRIMARY KEY (user_id, group_type)
);
