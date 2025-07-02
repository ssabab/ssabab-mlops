import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, when, udf, lit, avg
from pyspark.sql.types import IntegerType
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

train_date = sys.argv[1] if len(sys.argv) > 1 else datetime.today().date()

spark = SparkSession.builder.appName("generate_xgb_train_data").getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_props = get_mysql_jdbc_properties()

# Load all required tables
menu = spark.read.jdbc(mysql_url, "ssabab_dw.dim_menu_food_combined", properties=mysql_props)
users = spark.read.jdbc(mysql_url, "ssabab_dw.dim_user", properties=mysql_props)
food_feed = spark.read.jdbc(mysql_url, "ssabab_dw.fact_user_food_feedback", properties=mysql_props)
menu_feed = spark.read.jdbc(mysql_url, "ssabab_dw.fact_user_menu_feedback", properties=mysql_props)
user_summary = spark.read.jdbc(mysql_url, "ssabab_dm.dm_user_summary", properties=mysql_props)

# 오늘의 메뉴에서 두 가지 menu_id 선택
today_menu = menu.filter(col("menu_date") == lit(train_date))
A_menu_id, B_menu_id = today_menu.select("menu_id").distinct().rdd.map(lambda r: r.menu_id).collect()[:2]

# 메뉴별 음식 분리
a_food = today_menu.filter(col("menu_id") == A_menu_id)
b_food = today_menu.filter(col("menu_id") == B_menu_id)

# 고유 태그/카테고리 추출
def collect_unique(df, col_name):
    return df.select(col_name).distinct().rdd.flatMap(lambda x: x).collect()

a_tags = collect_unique(a_food, "tag_name")
b_tags = collect_unique(b_food, "tag_name")
a_cats = collect_unique(a_food, "category_name")
b_cats = collect_unique(b_food, "category_name")

# 평균 이상 리뷰만 필터링
food_feed = food_feed.join(user_summary.select("user_id", "avg_score"), on="user_id", how="left")
high_scores = food_feed.filter(col("food_score") >= col("avg_score"))

# 사용자 선호 태그/카테고리
joined = high_scores.join(menu, "food_id")
user_pref = joined.groupBy("user_id").agg(
    collect_set("tag_name").alias("user_tags"),
    collect_set("category_name").alias("user_cats")
)

# 태그/카테고리별 평점
tag_scores = joined.groupBy("user_id", "tag_name").agg(avg("food_score").alias("tag_score"))
cat_scores = joined.groupBy("user_id", "category_name").agg(avg("food_score").alias("cat_score"))

# 중첩도 계산 UDF
def overlap(set1, set2):
    return len(set(set1) & set(set2))

def make_udf(ref):
    return udf(lambda x: overlap(x, ref) if x else 0, IntegerType())

# 사용자별 feature 생성
user_pref = user_pref \
    .withColumn("A_tag_overlap", make_udf(a_tags)("user_tags")) \
    .withColumn("B_tag_overlap", make_udf(b_tags)("user_tags")) \
    .withColumn("A_category_overlap", make_udf(a_cats)("user_cats")) \
    .withColumn("B_category_overlap", make_udf(b_cats)("user_cats")) \
    .withColumn("A_tag_ratio", col("A_tag_overlap") / lit(len(a_tags))) \
    .withColumn("B_tag_ratio", col("B_tag_overlap") / lit(len(b_tags))) \
    .withColumn("A_category_ratio", col("A_category_overlap") / lit(len(a_cats))) \
    .withColumn("B_category_ratio", col("B_category_overlap") / lit(len(b_cats)))

# 평균 점수 계산
def avg_on_overlap(ref_set, key_col, score_df, score_col, alias_name):
    return score_df.filter(col(key_col).isin(ref_set)) \
        .groupBy("user_id") \
        .agg(avg(score_col).alias(alias_name))

a_tag_avg = avg_on_overlap(a_tags, "tag_name", tag_scores, "tag_score", "A_avg_score_on_tags")
b_tag_avg = avg_on_overlap(b_tags, "tag_name", tag_scores, "tag_score", "B_avg_score_on_tags")
a_cat_avg = avg_on_overlap(a_cats, "category_name", cat_scores, "cat_score", "A_avg_score_on_categories")
b_cat_avg = avg_on_overlap(b_cats, "category_name", cat_scores, "cat_score", "B_avg_score_on_categories")

# 피처 통합
df = user_pref \
    .join(a_tag_avg, "user_id", "left") \
    .join(b_tag_avg, "user_id", "left") \
    .join(a_cat_avg, "user_id", "left") \
    .join(b_cat_avg, "user_id", "left")

# 사용자 기본 정보 추가
df = df.join(users.select("user_id", "gender", "birth_year"), "user_id", "left")
df = df.withColumn("user_gender", col("gender")) \
       .withColumn("user_age", lit(2025) - col("birth_year"))

# 라벨 정의 (선택, 후회 여부, 사전투표 여부)
label_df = menu_feed.filter(
    col("menu_score").isNotNull() &
    col("menu_id").isin([A_menu_id, B_menu_id])
).withColumn("user_choice", when(col("menu_id") == B_menu_id, 1).otherwise(0)) \
 .select("user_id", "user_choice", "menu_regret", "pre_vote")

# 통합 및 최종 컬럼 정리
df = df.join(label_df, "user_id", "inner") \
       .withColumn("train_date", lit(train_date)) \
       .drop("gender", "birth_year", "user_tags", "user_cats")

# 저장
df.write.jdbc(
    url=mysql_url, 
    table="ssabab_dw.xgb_train_data", 
    mode="append", 
    properties=mysql_props
)
