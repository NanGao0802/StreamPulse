from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_PATH = "/data/public-opinion/delta/single_topic_comments"
BRONZE_PATH = f"{BASE_PATH}/bronze_comments_raw"
SILVER_PATH = f"{BASE_PATH}/silver_comments_enriched"
GOLD_HOURLY_PATH = f"{BASE_PATH}/gold_hourly_metrics"
GOLD_DAILY_PATH = f"{BASE_PATH}/gold_daily_metrics"
GOLD_TOP10_PATH = f"{BASE_PATH}/gold_daily_top10_comments"

spark = (
    SparkSession.builder
    .appName("check-multi-topic-delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.session.timeZone", "Asia/Shanghai")
    .getOrCreate()
)

print("===== bronze count =====")
print(spark.read.format("delta").load(BRONZE_PATH).count())

print("===== silver count =====")
print(spark.read.format("delta").load(SILVER_PATH).count())

print("===== daily topics =====")
daily_df = spark.read.format("delta").load(GOLD_DAILY_PATH)
daily_df.select("topic").distinct().orderBy("topic").show(200, truncate=False)

print("===== daily metrics =====")
daily_df.orderBy("topic", "stat_date").show(200, truncate=False)

print("===== hourly metrics =====")
spark.read.format("delta").load(GOLD_HOURLY_PATH).orderBy("topic", "stat_hour").show(200, truncate=False)

print("===== top10 =====")
spark.read.format("delta").load(GOLD_TOP10_PATH).orderBy("topic", "stat_date", "rank_no").show(200, truncate=False)

spark.stop()
