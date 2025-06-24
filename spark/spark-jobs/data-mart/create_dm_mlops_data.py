from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, collect_list, to_json, struct, when, lit, explode, first
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties
import sys
from datetime import datetime

spark = SparkSession.builder.appName("CreateMLMenuComparison").getOrCreate()

# 파라미터로 날짜 받기
if len(sys.argv) > 1:
    target_date = sys.argv[1]
else:
    target_date = datetime.now().strftime("%Y-%m-%d")

# DB 연결
jdbc_url = get_mysql_jdbc_url()
jdbc_props = get_mysql_jdbc_properties()

# 테이블 로드
account = spark.read.jdbc(jdbc_url, "account", properties=jdbc_props)
menu = spark.read.jdbc(jdbc_url, "menu", properties=jdbc_props).filter(col("date") == target_date)
menu_review = spark.read.jdbc(jdbc_url, "menu_review", properties=jdbc_props).filter(col("timestamp").cast("date") == target_date)
menu_food = spark.read.jdbc(jdbc_url, "menu_food", properties=jdbc_props)
food = spark.read.jdbc(jdbc_url, "food", properties=jdbc_props)
food_review = spark.read.jdbc(jdbc_url, "food_review", properties=jdbc_props)

# 1. 날짜별 menu_id 쌍 생성 (menu 테이블 기준)
menu_pairs = (
    menu.groupBy("date")
    .agg(
        first("menu_id", ignorenulls=True).alias("menu_id_a"),
        first("menu_id", ignorenulls=True).alias("menu_id_b")  # 나중에 정렬 바꿔서 두 개 추출
    )
)

# 2. 사용자 리뷰 (menu_review) → chosen_menu_id = 해당 날짜에 유저가 리뷰 남긴 메뉴
user_chosen = (
    menu_review
    .withColumnRenamed("menu_id", "chosen_menu_id")
    .select("user_id", "chosen_menu_id", "menu_score", col("timestamp").cast("date").alias("date"))
)

# 3. 사용자 정보
user_features = (
    account.select(
        col("user_id"),
        col("gender"),
        col("age"),
        col("ssafy_region").alias("region"),
        col("class_num")
    )
)

# 4. menu-food 관계
menu_food_map = menu_food.join(food, "food_id")

# 5. food_review 집계 (음식 특성)
food_review_agg = food_review.groupBy("food_id").agg(
    avg("food_score").alias("avg_food_score"),
    count("id").alias("review_count")
)

# 6. food 특성 벡터화 (tag, category, main_sub)
# → 여기선 단순 JSON 집계로 처리 (실제 운영에선 one-hot or word2vec 등 적용 가능)
food_feature = (
    food
    .join(food_review_agg, "food_id", "left")
    .select(
        "food_id",
        "category", "tag", "main_sub",
        "avg_food_score", "review_count"
    )
)

# 7. 메뉴별 food 특성 집계
menu_feature = (
    menu_food
    .join(food_feature, "food_id")
    .groupBy("menu_id")
    .agg(
        collect_list("food_id").alias("food_ids"),
        avg("avg_food_score").alias("avg_food_score"),
        count("review_count").alias("review_count")
        # tag/category/main_sub도 집계 가능
    )
)

# 8. 조인하여 최종 테이블 구성
ml = (
    user_chosen
    .join(menu_pairs, "date")
    .join(user_features, "user_id")
    .join(menu_feature.alias("a"), col("menu_id_a") == col("a.menu_id"))
    .join(menu_feature.alias("b"), col("menu_id_b") == col("b.menu_id"))
    .withColumn("label", when(col("chosen_menu_id") == col("menu_id_a"), lit(0)).otherwise(lit(1)))
    .select(
        "user_id", "date", "menu_id_a", "menu_id_b", "chosen_menu_id", "label",
        "gender", "age", "region", "class_num",
        col("a.food_ids").alias("food_ids_a"),
        col("a.avg_food_score").alias("avg_food_score_a"),
        col("a.review_count").alias("review_count_a"),
        col("b.food_ids").alias("food_ids_b"),
        col("b.avg_food_score").alias("avg_food_score_b"),
        col("b.review_count").alias("review_count_b"),
    )
)

# 저장
ml.write.jdbc(
    url=jdbc_url,
    table="ssabab_dm.ml_menu_comparison",
    mode="append",
    properties=jdbc_props
)
