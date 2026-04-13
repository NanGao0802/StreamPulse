from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

spark = (
    SparkSession.builder
    .appName("hello-public-opinion")
    .getOrCreate()
)

data = [
    ("dy:7613792636118516522", "dy", "26考研人数下降", "2026-03-05 23:24:05", "省时间高效够用，重庆地区", 4, 0),
    ("dy:7613821495457891078", "dy", "26考研人数下降", "2026-03-06 01:16:04", "跟着你192小时，62涨了10分", 0, 0)
]

df = spark.createDataFrame(
    data,
    ["event_id", "platform", "topic", "publish_time", "text", "like_count", "repost_count"]
)

df = df.withColumn("publish_ts", to_timestamp("publish_time", "yyyy-MM-dd HH:mm:ss"))

print("===== row_count =====")
print(df.count())

print("===== schema =====")
df.printSchema()

print("===== preview =====")
df.show(truncate=False)

spark.stop()
