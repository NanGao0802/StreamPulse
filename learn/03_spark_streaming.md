# 第二层：流计算层（backend/spark/）

> 对应架构层级：**流计算层**  
> 核心技术：PySpark 3.5 · Spark Structured Streaming · Delta Lake · StructBERT · ModelScope

---

## 待学习内容清单

- [x] Spark Structured Streaming 与普通批处理的区别
- [x] `readStream` / `writeStream` / `foreachBatch` 的用法
- [x] 微批次（Micro-Batch）触发模式：`processingTime="60 seconds"` 的含义
- [x] `process_batch()` 函数内部的完整五步流水线
- [x] StructBERT 单例模式：为什么要用单例，`get_instance()` 如何实现
- [x] 情感三分类逻辑：置信度阈值 0.72 的作用
- [x] 热度分数公式的设计理由
- [x] 风险等级的判定规则
- [x] `merge_to_silver()` 如何实现幂等写入
- [x] `rebuild_gold_tables()` 为什么每批次全量重算（Overwrite）
- [x] `check_and_send_alerts()` 告警检测的完整流程
- [x] `build_spark()` 中的 Delta Lake 配置项含义
- [x] Spark Driver 端推理 vs Executor 端推理的区别

---

## 核心文件

| 文件 | 职责 |
|------|------|
| `stream_single_topic_to_delta.py` | 主任务：Kafka Source → foreachBatch → Delta写入 |
| `structbert_sentiment.py` | StructBERT单例推理服务 |
| `alert_mailer.py` | SMTP告警邮件，含去重逻辑 |

---

## foreachBatch 五步流水线

```
Kafka读取(50条/批)
    │
    ▼
JSON解析（from_json，提取Kafka元数据）
    │
    ▼
字段过滤（丢弃event_id/text/publish_time/topic任一为空的记录）
    │
    ▼
StructBERT情感推理（.collect()拉到Driver端，批量推理）
    │
    ▼
热度计算（生成 heat_score / risk_level / event_hour / event_date）
    │
    ├──► Bronze追加写入（保留Kafka元数据，按日期分区）
    │
    ├──► Silver Merge去重（按event_id，幂等写入）
    │
    ├──► Gold全量重算（Overwrite：小时/日/Top10三张表）
    │
    └──► 告警检测（查Gold小时表，超阈值则发邮件+写告警表）
```

---

## 关键代码片段

### SparkSession 初始化

```python
spark = (
    SparkSession.builder
    .appName("multi-topic-comment-pipeline-structbert-alert")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.session.timeZone", "Asia/Shanghai")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
```

### Kafka Source 配置

```python
spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "50")
    .load()
```

### 热度计算

```python
heat_score = round(
    1.0
    + like_count  × 0.6
    + repost_count × 1.0
    + (1.5 if negative else 0.5 if neutral else 0.2),
    2
)
```

### 风险等级

```python
risk_level =
    "high"   if (sentiment == "negative") and (heat_score >= 12) else
    "medium" if (sentiment == "negative") and (heat_score >= 5)  else
    "low"
```

### Silver Merge（幂等写入）

```python
target.alias("t")
    .merge(dedup_df.alias("s"), "t.event_id = s.event_id")
    .whenNotMatchedInsertAll()
    .execute()
```

---

## 1. Spark Structured Streaming 是什么

普通 Spark 是**批处理**：读一批数据 → 处理 → 写结果 → 结束。

Structured Streaming 是**无限增量处理**：把流数据抽象成一张不断追加新行的**无界表**，用与批处理相同的 DataFrame API 来写流处理逻辑。

```
普通批处理：
  [读数据] ──► [处理] ──► [写结果] ──► 结束

Structured Streaming（Micro-Batch模式）：
  [等待60s] ──► [读新增数据] ──► [foreachBatch处理] ──► [写Delta]
      ▲_____________________________________________________|
                        无限循环
```

**本项目选择 Micro-Batch（微批次）而非 Continuous（连续）模式**，原因是 StructBERT 推理本身就有延迟，微批次在延迟和吞吐量之间取得平衡，60 秒对舆情监测场景足够。

---

## 2. SparkSession 初始化（build_spark）

```python
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("multi-topic-comment-pipeline-structbert-alert")
        # 启用 Delta Lake SQL 扩展（支持 MERGE、OPTIMIZE 等 Delta SQL 语法）
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # 替换 Spark 默认的数据目录，让 Delta Lake 接管表管理
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # 统一时区（防止 publish_time 解析出现8小时偏差）
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        # 本地模式下降低 shuffle 分区数，减少小文件碎片
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
```

---

## 3. writeStream + foreachBatch 主循环

```python
query = (
    parsed_df.writeStream
    .foreachBatch(process_batch)          # 每个微批次调用此函数
    .outputMode("update")                 # 增量模式
    .option("checkpointLocation", CHECKPOINT_PATH)  # Offset检查点
    .trigger(processingTime="60 seconds") # 每60秒触发一次
    .start()
)
query.awaitTermination()   # 阻塞主线程，保持流任务运行
```

`foreachBatch` 是关键：它将每个微批次的数据作为一个普通 DataFrame 传给 `process_batch(batch_df, batch_id)`，在函数内部可以自由使用批处理 API（包括 `DeltaTable.merge()` 等复杂操作）。

---

## 4. process_batch 五步流水线（核心函数）

```python
def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    spark = batch_df.sparkSession

    # ① 将 DataFrame 数据拉到 Driver 端（因为StructBERT在Driver端）
    records = [r.asDict(recursive=True) for r in batch_df.collect()]
    if len(records) == 0:
        return  # 空批次直接跳过

    # ② StructBERT 情感推理 + 热度计算，生成 enriched_df
    enriched_df = build_enriched_df(spark, records)

    # ③ Bronze 层：追加写入（按 event_date 分区）
    bronze_df.write.format("delta").mode("append")
             .partitionBy("event_date").save(BRONZE_PATH)

    # ④ Silver 层：Merge 去重写入（按 event_id）
    merge_to_silver(spark, silver_df)

    # ⑤ Gold 层：全量重算（Overwrite）
    rebuild_gold_tables(spark)

    # ⑥ 告警检测：检查是否触发告警阈值，发邮件
    check_and_send_alerts(spark, enriched_df)
```

---

## 5. build_enriched_df：情感推理 + 衍生字段计算

```python
def build_enriched_df(spark, records):
    # Step 1: 调用 StructBERT 批量推理，为每条记录追加6个情感字段
    pred_records = predict_sentiment(records)
    df = spark.createDataFrame(pred_records)

    # Step 2: 计算衍生字段
    df = (
        df
        .withColumn("like_count",    F.coalesce(F.col("like_count").cast("long"), F.lit(0)))
        .withColumn("repost_count",  F.coalesce(F.col("repost_count").cast("long"), F.lit(0)))
        .withColumn("publish_ts",    F.to_timestamp("publish_time", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("text_len",      F.length("text"))
        .withColumn("is_negative",   F.when(F.col("sentiment_label") == "negative", 1).otherwise(0))
        # 热度公式
        .withColumn("heat_score",
            F.round(
                F.lit(1.0)
                + F.col("like_count")   * F.lit(0.6)
                + F.col("repost_count") * F.lit(1.0)
                + F.when(F.col("sentiment_label") == "negative", F.lit(1.5))
                   .when(F.col("sentiment_label") == "neutral",  F.lit(0.5))
                   .otherwise(F.lit(0.2)),
                2   # 保留2位小数
            )
        )
        # 风险等级
        .withColumn("risk_level",
            F.when((F.col("sentiment_label") == "negative") & (F.col("heat_score") >= 12), F.lit("high"))
             .when((F.col("sentiment_label") == "negative") & (F.col("heat_score") >= 5),  F.lit("medium"))
             .otherwise(F.lit("low"))
        )
        .withColumn("event_hour", F.date_trunc("hour", F.col("publish_ts")))  # 整点小时
        .withColumn("event_date", F.to_date("publish_ts"))                     # 日期
    )
    return df
```

### 热度公式设计理由

| 分量 | 权重 | 理由 |
|------|------|------|
| 基础值 `1.0` | — | 保证每条评论至少有非零热度 |
| `like_count × 0.6` | 0.6 | 点赞影响热度，但权重低于转发 |
| `repost_count × 1.0` | 1.0 | 转发代表更强的传播意愿 |
| 负向情感加成 `+1.5` | — | 负面内容在社交媒体传播更广 |
| 中性情感加成 `+0.5` | — | 中性内容传播力一般 |
| 正向情感加成 `+0.2` | — | 正面内容传播力最弱 |

### 风险等级含义

```
high   = 负向情感 AND 热度 ≥ 12  → 传播力强的负面评论，需立即关注
medium = 负向情感 AND 热度 ≥ 5   → 有一定传播的负面评论
low    = 其他所有情况              → 无明显风险
```

---

## 6. StructBERT 情感推理服务（structbert_sentiment.py）

### 单例模式：为什么只初始化一次

```python
class StructBERTSentimentService:
    _instance = None       # 类级别变量，整个进程只有一份

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = StructBERTSentimentService()  # 首次调用时才初始化
        return cls._instance

    def __init__(self):
        self.pipe = ms_pipeline(
            task=Tasks.text_classification,
            model="iic/nlp_structbert_sentiment-classification_chinese-base"
        )  # 加载约400MB模型，只做一次
```

**为什么在 Driver 端而不是 Executor 端推理：**
- 本项目使用 `local[*]` 模式，Driver 和 Executor 都在同一个 JVM 进程内
- 若在 Executor 端加载模型，多 Worker 场景下每个 Worker 都要加载一份 400MB 模型
- Driver 端单例：整个 Spark 进程只加载一次，效率最高

### 批量推理流程

```python
def predict_texts(self, texts: List[Any]) -> List[Dict]:
    clean_texts = [self._normalize_text(t) for t in texts]  # 清洗：去换行、截断到256字符

    results = []
    for i in range(0, len(clean_texts), self.batch_size):   # 按batch_size=32分块
        chunk = clean_texts[i:i + self.batch_size]
        chunk_result = self._predict_chunk(chunk)            # 批量推理
        results.extend(chunk_result)
    return results

def _predict_chunk(self, texts):
    try:
        result = self.pipe(texts)           # 批量调用ModelScope Pipeline
        return [self._parse_output(x) for x in result]
    except Exception:
        # 批量失败时，降级为逐条推理（容错）
        outputs = []
        for text in texts:
            try:
                result = self.pipe(text)
                outputs.append(self._parse_output(result))
            except Exception:
                outputs.append({    # 单条也失败，归为neutral
                    "sentiment_label": "neutral",
                    "sentiment_score": 0.0,
                    "sentiment_confidence": 0.0, ...
                })
        return outputs
```

### 三分类逻辑：置信度阈值

```python
def _parse_output(self, output):
    # 从模型输出解析正/负概率
    pos_prob = ...   # 正向概率
    neg_prob = ...   # 负向概率

    # 取较大概率的标签作为 raw_label
    if pos_prob >= neg_prob:
        raw_label = "positive"
        confidence = pos_prob
    else:
        raw_label = "negative"
        confidence = neg_prob

    # 三分类核心逻辑：低置信度 → 强制归中性
    if confidence < self.neutral_threshold:   # 默认0.72
        final_label = "neutral"
        score_value = 0.0                     # 中性时score强制为0
    else:
        final_label = raw_label               # positive 或 negative
        score_value = round(pos_prob - neg_prob, 4)  # 范围[-1, 1]

    return {
        "sentiment_raw_label":  raw_label,     # 模型原始预测（不含neutral）
        "sentiment_confidence": confidence,    # 置信度
        "sentiment_label":      final_label,   # 最终三分类结果
        "sentiment_score":      score_value,   # 正负概率差
        "sentiment_source":     "structbert",
        "sentiment_model":      self.model_id,
    }
```

**为什么选 0.72 作为阈值：**
- 低于 0.72 说明模型对正/负的判断不够确定（两类概率接近 50/50）
- 强制归中性可有效减少误报，特别是对模糊表达的评论
- 该值通过 `STRUCTBERT_NEUTRAL_THRESHOLD` 环境变量可调整

---

## 7. merge_to_silver：幂等写入

```python
def merge_to_silver(spark, batch_df):
    dedup_df = batch_df.dropDuplicates(["event_id"])   # 批次内先去重

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        # 表不存在时直接创建（首次运行）
        dedup_df.write.format("delta").mode("append")
                .partitionBy("event_date").save(SILVER_PATH)
        return

    # 表已存在：MERGE 操作
    target = DeltaTable.forPath(spark, SILVER_PATH)
    (
        target.alias("t")
        .merge(
            dedup_df.alias("s"),
            "t.event_id = s.event_id"   # 匹配条件：event_id 相同视为同一条
        )
        .whenNotMatchedInsertAll()       # 只有不存在时才插入，已存在的跳过
        .execute()
    )
```

`whenNotMatchedInsertAll()` 是关键：**只插入新记录，已有的 `event_id` 不更新**。这实现了幂等性——无论同一条消息被消费几次，Silver 层只会有一条记录。

---

## 8. rebuild_gold_tables：全量重算

```python
def rebuild_gold_tables(spark):
    silver_df = spark.read.format("delta").load(SILVER_PATH)

    # Gold 小时聚合
    hourly_df = (
        silver_df
        .groupBy("topic", F.date_trunc("hour", F.col("publish_ts")).alias("stat_hour"))
        .agg(
            F.count("*").alias("comment_count"),
            F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_count"),
            F.round(F.sum("heat_score"), 2).alias("heat_sum"),
            F.round(F.avg("heat_score"), 2).alias("heat_avg"),
        )
        .withColumn("negative_ratio",
            F.round(F.col("negative_count") / F.col("comment_count"), 4)
        )
    )

    hourly_df.write.format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")   # 允许Schema变更
             .save(GOLD_HOURLY_PATH)
    # Gold 日聚合、Top10 同理...
```

**为什么每批次全量 Overwrite 而不是增量更新：**
- Silver 层的 Merge 可能改变历史数据（虽然很少），增量更新难以保证历史 Gold 数据一致
- Gold 表数据量较小（聚合后通常几千行），Overwrite 重算耗时可接受（通常 < 5 秒）
- 逻辑简单，不易出 Bug

---

## 9. check_and_send_alerts：告警检测

```python
def check_and_send_alerts(spark, batch_enriched_df):
    # Step 1: 取当前批次涉及的 (topic, event_hour) 组合
    batch_keys_df = (
        batch_enriched_df
        .select("topic", F.col("event_hour").alias("stat_hour"))
        .distinct()
    )

    # Step 2: 关联最新的 Gold 小时聚合表，筛选超阈值的行
    hourly_df = spark.read.format("delta").load(GOLD_HOURLY_PATH)
    candidate_df = (
        hourly_df
        .join(batch_keys_df, on=["topic", "stat_hour"], how="inner")  # 只检查本批涉及的小时
        .filter(F.col("comment_count") > ALERT_MIN_COMMENTS)           # 评论数 > 10
        .filter(F.col("negative_ratio") > ALERT_NEG_RATIO_THRESHOLD)   # 负面占比 > 40%
    )

    # Step 3: 去重 - 已发送过的告警不重复发
    existing_keys = get_existing_alert_keys(spark)  # 从 gold_alert_events 读已有key

    for row in candidate_df.collect():
        alert_key = f"{row['topic']}__{str(row['stat_hour'])}"
        if alert_key in existing_keys:
            continue  # 跳过，本小时已经告警过了

        # Step 4: 从 Silver 层取该话题该小时 Top3 负面评论（热度降序）
        sample_df = (
            silver_df
            .filter(F.col("topic") == row["topic"])
            .filter(F.col("event_hour") == row["stat_hour"])
            .filter(F.col("sentiment_label") == "negative")
            .orderBy(F.desc("heat_score"))
            .limit(3)
        )

        # Step 5: 调用 AlertMailer 发邮件
        mail_result = mailer.send_alert(topic, stat_hour, ...)

        # Step 6: 写入告警记录表
        append_alert_records(spark, [{
            "alert_key": alert_key,
            "email_status": mail_result["status"],
            ...
        }])
```

---

## 10. AlertMailer：SMTP 邮件告警（alert_mailer.py）

```python
class AlertMailer:
    _instance = None   # 同样是单例

    def is_ready(self) -> bool:
        # 检查：enabled=True 且 SMTP配置完整 且 非默认占位符
        return self.enabled and self.host and self.user and self.password \
               and "YOUR_QQ_SMTP_AUTH_CODE" not in self.password

    def send_alert(self, topic, stat_hour, comment_count, negative_count,
                   negative_ratio, top_negative_comments):
        if not self.is_ready():
            return {"status": "disabled", "message": "..."}

        # 构建邮件内容
        subject = f"[舆情预警] {topic} 在 {stat_hour} 负面占比异常"
        body = f"""
话题：{topic}
统计小时：{stat_hour}
该小时评论总数：{comment_count}
该小时负面占比：{negative_ratio:.2%}
热度最高的负面评论：...
"""
        # 发送（支持 SSL/TLS 两种模式）
        if self.use_ssl:
            server = smtplib.SMTP_SSL(self.host, self.port)
        else:
            server = smtplib.SMTP(self.host, self.port)
        server.login(self.user, self.password)
        server.sendmail(self.mail_from, self.mail_to, msg.as_string())
```

**邮件是否发送成功会记录在 `gold_alert_events` 表的 `email_status` 字段**（`sent` / `failed` / `disabled`），便于排查邮件发送问题。

---

## 11. 配置项（从 alert.env 读取）

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka Broker 地址 |
| `KAFKA_TOPIC` | `public_opinion_raw` | 消费 Topic |
| `STRUCTBERT_MODEL_ID` | `iic/nlp_structbert_...` | ModelScope 模型 ID |
| `STRUCTBERT_BATCH_SIZE` | `32` | 批量推理大小（内存不足可调小） |
| `STRUCTBERT_NEUTRAL_THRESHOLD` | `0.72` | 中性判定置信度阈值 |
| `STRUCTBERT_MAX_TEXT_LENGTH` | `256` | 输入最大字符数（超出截断） |
| `ALERT_MIN_COMMENTS` | `10` | 触发告警的最少评论数 |
| `ALERT_NEG_RATIO_THRESHOLD` | `0.40` | 触发告警的负面占比阈值 |
| `ALERT_TOPN_TEXTS` | `3` | 告警邮件附带 Top N 负面评论 |
| `ALERT_EMAIL_ENABLED` | `false` | 是否启用邮件告警 |
| `SMTP_HOST` | `smtp.qq.com` | SMTP 服务器 |
| `SMTP_PORT` | `465` | SMTP 端口（SSL） |
| `SMTP_USER` | — | 发件邮箱 |
| `SMTP_PASSWORD` | — | SMTP 授权码（非登录密码） |

---

## 12. 启动命令

```bash
bash /opt/pipeline/scripts/run_stream_single_topic.sh
```

spark-submit 关键参数：
```bash
spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --executor-memory 2g \
  --packages io.delta:delta-spark_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  stream_single_topic_to_delta.py
```

---

## 学习笔记

<!-- 在这里补充你的学习理解 -->
