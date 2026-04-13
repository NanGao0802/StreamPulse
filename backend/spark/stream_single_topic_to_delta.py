import json
import os
from datetime import datetime
from typing import Any, Dict, List

from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from alert_mailer import AlertMailer
from structbert_sentiment import StructBERTSentimentService

load_dotenv("/opt/pipeline/conf/alert.env")

APP_NAME = "multi-topic-comment-pipeline-structbert-alert"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "public_opinion_raw")

BASE_PATH = "/data/public-opinion/delta/single_topic_comments"
BRONZE_PATH = f"{BASE_PATH}/bronze_comments_raw"
SILVER_PATH = f"{BASE_PATH}/silver_comments_enriched"
GOLD_HOURLY_PATH = f"{BASE_PATH}/gold_hourly_metrics"
GOLD_DAILY_PATH = f"{BASE_PATH}/gold_daily_metrics"
GOLD_TOP10_PATH = f"{BASE_PATH}/gold_daily_top10_comments"
ALERT_PATH = f"{BASE_PATH}/gold_alert_events"

CHECKPOINT_PATH = "/data/public-opinion/checkpoints/single_topic_comment_pipeline"

ALERT_MIN_COMMENTS = int(os.getenv("ALERT_MIN_COMMENTS", "10"))
ALERT_NEG_RATIO_THRESHOLD = float(os.getenv("ALERT_NEG_RATIO_THRESHOLD", "0.40"))
ALERT_TOPN_TEXTS = int(os.getenv("ALERT_TOPN_TEXTS", "3"))


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_schema() -> T.StructType:
    return T.StructType([
        T.StructField("event_id", T.StringType(), True),
        T.StructField("platform", T.StringType(), True),
        T.StructField("topic", T.StringType(), True),
        T.StructField("publish_time", T.StringType(), True),
        T.StructField("time_id", T.StringType(), True),
        T.StructField("text", T.StringType(), True),
        T.StructField("like_count", T.LongType(), True),
        T.StructField("repost_count", T.LongType(), True),
        T.StructField("_src", T.StringType(), True),
        T.StructField("ingest_time", T.StringType(), True),
        T.StructField("source_file", T.StringType(), True),
    ])


def merge_to_silver(spark: SparkSession, batch_df: DataFrame) -> None:
    dedup_df = batch_df.dropDuplicates(["event_id"])

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        (
            dedup_df.write
            .format("delta")
            .mode("append")
            .partitionBy("event_date")
            .save(SILVER_PATH)
        )
        return

    target = DeltaTable.forPath(spark, SILVER_PATH)
    (
        target.alias("t")
        .merge(
            dedup_df.alias("s"),
            "t.event_id = s.event_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def rebuild_gold_tables(spark: SparkSession) -> None:
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        return

    silver_df = (
        spark.read.format("delta").load(SILVER_PATH)
        .filter(F.col("publish_ts").isNotNull())
        .filter(F.col("topic").isNotNull())
        .filter(F.length(F.trim(F.col("topic"))) > 0)
    )

    hourly_df = (
        silver_df
        .groupBy(
            F.col("topic"),
            F.date_trunc("hour", F.col("publish_ts")).alias("stat_hour")
        )
        .agg(
            F.count("*").alias("comment_count"),
            F.sum("like_count").alias("like_count_sum"),
            F.sum("repost_count").alias("repost_count_sum"),
            F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_count"),
            F.round(F.sum("heat_score"), 2).alias("heat_sum"),
            F.round(F.avg("heat_score"), 2).alias("heat_avg"),
        )
        .withColumn(
            "negative_ratio",
            F.round(
                F.when(F.col("comment_count") > 0, F.col("negative_count") / F.col("comment_count")).otherwise(F.lit(0.0)),
                4
            )
        )
        .orderBy("topic", "stat_hour")
    )

    (
        hourly_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_HOURLY_PATH)
    )

    daily_df = (
        silver_df
        .groupBy(
            F.col("topic"),
            F.to_date("publish_ts").alias("stat_date")
        )
        .agg(
            F.count("*").alias("comment_count"),
            F.sum("like_count").alias("like_count_sum"),
            F.sum("repost_count").alias("repost_count_sum"),
            F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_count"),
            F.round(F.sum("heat_score"), 2).alias("heat_sum"),
            F.round(F.avg("heat_score"), 2).alias("heat_avg"),
        )
        .withColumn(
            "negative_ratio",
            F.round(
                F.when(F.col("comment_count") > 0, F.col("negative_count") / F.col("comment_count")).otherwise(F.lit(0.0)),
                4
            )
        )
        .orderBy("topic", "stat_date")
    )

    (
        daily_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_DAILY_PATH)
    )

    win = Window.partitionBy("topic", "event_date").orderBy(
        F.desc("heat_score"),
        F.desc("like_count"),
        F.desc("repost_count"),
        F.asc("event_id"),
    )

    top10_df = (
        silver_df
        .withColumn("rank_no", F.row_number().over(win))
        .filter(F.col("rank_no") <= 10)
        .select(
            F.col("event_date").alias("stat_date"),
            "topic",
            "rank_no",
            "event_id",
            "platform",
            "publish_time",
            "publish_ts",
            "text",
            "like_count",
            "repost_count",
            "sentiment_label",
            "sentiment_score",
            "sentiment_confidence",
            "sentiment_source",
            "sentiment_model",
            "heat_score",
            "risk_level",
        )
        .orderBy("topic", "stat_date", "rank_no")
    )

    (
        top10_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_TOP10_PATH)
    )


def predict_sentiment(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    service = StructBERTSentimentService.get_instance()
    texts = [r.get("text", "") for r in records]
    preds = service.predict_texts(texts)

    enriched = []
    for rec, pred in zip(records, preds):
        item = dict(rec)
        item.update(pred)
        enriched.append(item)
    return enriched


def build_enriched_df(spark: SparkSession, records: List[Dict[str, Any]]) -> DataFrame:
    pred_records = predict_sentiment(records)
    df = spark.createDataFrame(pred_records)

    df = (
        df
        .withColumn("like_count", F.coalesce(F.col("like_count").cast("long"), F.lit(0)))
        .withColumn("repost_count", F.coalesce(F.col("repost_count").cast("long"), F.lit(0)))
        .withColumn("publish_ts", F.to_timestamp("publish_time", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("text_len", F.length("text"))
        .withColumn("is_negative", F.when(F.col("sentiment_label") == "negative", F.lit(1)).otherwise(F.lit(0)))
        .withColumn(
            "heat_score",
            F.round(
                F.lit(1.0)
                + F.col("like_count") * F.lit(0.6)
                + F.col("repost_count") * F.lit(1.0)
                + F.when(F.col("sentiment_label") == "negative", F.lit(1.5))
                   .when(F.col("sentiment_label") == "neutral", F.lit(0.5))
                   .otherwise(F.lit(0.2)),
                2
            )
        )
        .withColumn(
            "risk_level",
            F.when((F.col("sentiment_label") == "negative") & (F.col("heat_score") >= 12), F.lit("high"))
             .when((F.col("sentiment_label") == "negative") & (F.col("heat_score") >= 5), F.lit("medium"))
             .otherwise(F.lit("low"))
        )
        .withColumn("event_hour", F.date_trunc("hour", F.col("publish_ts")))
        .withColumn("event_date", F.to_date("publish_ts"))
    )
    return df


def append_alert_records(spark: SparkSession, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    df = spark.createDataFrame(rows)
    (
        df.write
        .format("delta")
        .mode("append")
        .save(ALERT_PATH)
    )


def get_existing_alert_keys(spark: SparkSession) -> set:
    if not DeltaTable.isDeltaTable(spark, ALERT_PATH):
        return set()
    rows = spark.read.format("delta").load(ALERT_PATH).select("alert_key").distinct().collect()
    return set(r["alert_key"] for r in rows)


def check_and_send_alerts(spark: SparkSession, batch_enriched_df: DataFrame) -> None:
    if batch_enriched_df.rdd.isEmpty():
        return

    if not DeltaTable.isDeltaTable(spark, GOLD_HOURLY_PATH):
        return

    batch_keys_df = (
        batch_enriched_df
        .select("topic", F.col("event_hour").alias("stat_hour"))
        .distinct()
    )

    hourly_df = spark.read.format("delta").load(GOLD_HOURLY_PATH)

    candidate_df = (
        hourly_df
        .join(batch_keys_df, on=["topic", "stat_hour"], how="inner")
        .filter(F.col("comment_count") > ALERT_MIN_COMMENTS)
        .filter(F.col("negative_ratio") > ALERT_NEG_RATIO_THRESHOLD)
    )

    candidates = candidate_df.collect()
    if not candidates:
        return

    existing_keys = get_existing_alert_keys(spark)
    mailer = AlertMailer.get_instance()

    silver_df = spark.read.format("delta").load(SILVER_PATH)

    new_alert_rows = []

    for row in candidates:
        topic = row["topic"]
        stat_hour = row["stat_hour"]
        comment_count = int(row["comment_count"])
        negative_count = int(row["negative_count"])
        negative_ratio = float(row["negative_ratio"])

        alert_key = f"{topic}__{str(stat_hour)}"
        if alert_key in existing_keys:
            continue

        sample_df = (
            silver_df
            .filter(F.col("topic") == topic)
            .filter(F.col("event_hour") == stat_hour)
            .filter(F.col("sentiment_label") == "negative")
            .orderBy(F.desc("heat_score"), F.desc("like_count"), F.desc("repost_count"))
            .select("publish_time", "heat_score", "text")
            .limit(ALERT_TOPN_TEXTS)
        )

        top_negative_comments = []
        for r in sample_df.collect():
            top_negative_comments.append({
                "publish_time": r["publish_time"],
                "heat_score": float(r["heat_score"]) if r["heat_score"] is not None else 0.0,
                "text": r["text"] if r["text"] is not None else "",
            })

        mail_result = mailer.send_alert(
            topic=topic,
            stat_hour=str(stat_hour),
            comment_count=comment_count,
            negative_count=negative_count,
            negative_ratio=negative_ratio,
            top_negative_comments=top_negative_comments
        )

        new_alert_rows.append({
            "alert_key": alert_key,
            "topic": topic,
            "stat_hour": str(stat_hour),
            "comment_count": comment_count,
            "negative_count": negative_count,
            "negative_ratio": round(negative_ratio, 4),
            "top_negative_comments_json": json.dumps(top_negative_comments, ensure_ascii=False),
            "email_status": mail_result.get("status", "unknown"),
            "email_message": mail_result.get("message", ""),
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    if new_alert_rows:
        append_alert_records(spark, new_alert_rows)


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    spark = batch_df.sparkSession

    records = [r.asDict(recursive=True) for r in batch_df.collect()]
    incoming_count = len(records)

    if incoming_count == 0:
        print(f"[batch={batch_id}] empty batch, skip")
        return

    print(f"[batch={batch_id}] incoming rows = {incoming_count}")

    enriched_df = build_enriched_df(spark, records)

    bronze_df = enriched_df.select(
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_json",
        "event_id",
        "platform",
        "topic",
        "publish_time",
        "publish_ts",
        "time_id",
        "text",
        "text_len",
        "like_count",
        "repost_count",
        "_src",
        "ingest_time",
        "source_file",
        "sentiment_raw_label",
        "sentiment_confidence",
        "sentiment_label",
        "sentiment_score",
        "sentiment_source",
        "sentiment_model",
        "is_negative",
        "heat_score",
        "risk_level",
        "event_hour",
        "event_date",
    )

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .partitionBy("event_date")
        .save(BRONZE_PATH)
    )

    silver_df = enriched_df.select(
        "event_id",
        "platform",
        "topic",
        "publish_time",
        "publish_ts",
        "time_id",
        "text",
        "text_len",
        "like_count",
        "repost_count",
        "_src",
        "ingest_time",
        "source_file",
        "sentiment_raw_label",
        "sentiment_confidence",
        "sentiment_label",
        "sentiment_score",
        "sentiment_source",
        "sentiment_model",
        "is_negative",
        "heat_score",
        "risk_level",
        "event_hour",
        "event_date",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )

    merge_to_silver(spark, silver_df)
    rebuild_gold_tables(spark)
    check_and_send_alerts(spark, enriched_df)

    print(f"[batch={batch_id}] write bronze/silver/gold/alerts done")


def main() -> None:
    spark = build_spark()
    json_schema = build_schema()

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "50")
        .load()
    )

    parsed_df = (
        kafka_df
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("json_obj", F.from_json(F.col("raw_json"), json_schema))
        .select(
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "raw_json",
            F.col("json_obj.event_id").alias("event_id"),
            F.col("json_obj.platform").alias("platform"),
            F.trim(F.col("json_obj.topic")).alias("topic"),
            F.col("json_obj.publish_time").alias("publish_time"),
            F.col("json_obj.time_id").alias("time_id"),
            F.col("json_obj.text").alias("text"),
            F.coalesce(F.col("json_obj.like_count"), F.lit(0)).cast("long").alias("like_count"),
            F.coalesce(F.col("json_obj.repost_count"), F.lit(0)).cast("long").alias("repost_count"),
            F.col("json_obj._src").alias("_src"),
            F.col("json_obj.ingest_time").alias("ingest_time"),
            F.col("json_obj.source_file").alias("source_file"),
        )
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("text").isNotNull())
        .filter(F.col("publish_time").isNotNull())
        .filter(F.col("topic").isNotNull())
        .filter(F.length(F.col("topic")) > 0)
    )

    query = (
        parsed_df.writeStream
        .foreachBatch(process_batch)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="60 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
