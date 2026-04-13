# 数据管道详解

## 1. Kafka 消息 Schema

爬虫将每条评论序列化为如下 JSON 结构，推送至 Kafka Topic `public_opinion_raw`：

```json
{
  "event_id":     "dy:7613792636118516522",
  "platform":     "dy",
  "topic":        "26考研人数下降",
  "publish_time": "2026-03-05 23:24:05",
  "time_id":      "20260305",
  "text":         "评论正文内容",
  "like_count":   4,
  "repost_count": 0,
  "ingest_time":  "2026-03-06 00:01:12",
  "source_file":  "/opt/pipeline/data/dy_20260305.jsonl"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `event_id` | string | `平台前缀:平台原始ID`，全局唯一，Silver 层用于去重 |
| `platform` | string | `bili` / `wb` / `xhs` / `dy` / `zhihu` / `ks` / `tieba` |
| `topic` | string | 话题关键词，与爬虫搜索词一致 |
| `publish_time` | string | 格式 `yyyy-MM-dd HH:mm:ss` |
| `time_id` | string | 日期桶，格式 `yyyyMMdd` |
| `text` | string | 评论/帖子正文，Spark 过滤空值 |
| `like_count` | long | 点赞数，缺失时默认 0 |
| `repost_count` | long | 转发/分享数，缺失时默认 0 |
| `ingest_time` | string | 写入 Kafka 的时间戳 |
| `source_file` | string | 来源 JSONL 文件路径，用于溯源 |

---

## 2. foreachBatch 五步处理流水线

每个 60 秒批次由 `process_batch(batch_df, batch_id)` 函数依次执行五步：

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ ① Kafka读取  │───►│ ② JSON解析   │───►│ ③ 字段过滤   │
└──────────────┘    └──────────────┘    └──────────────┘
                                                │
                                                ▼
                                       ┌──────────────────┐
                                       │ ④ StructBERT推理  │
                                       │  (Driver端批量)   │
                                       └──────────────────┘
                                                │
                                                ▼
                                       ┌──────────────────┐
                                       │  ⑤ 热度计算       │
                                       │  → enriched_df   │
                                       └──────────────────┘
                                                │
                          ┌─────────────────────┼───────────────────┐
                          ▼                     ▼                   ▼
                    Bronze 写入          Silver Merge          Gold 重算
                    (append)           (by event_id)         (overwrite)
                                                                     │
                                                              告警检测
                                                          (Alert Events)
```

### 步骤详解

**① Kafka Source**

```python
spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "50")   # 每批最多 50 条
```

**② JSON 解析**

```python
kafka_df.select(
    F.col("key").cast("string").alias("kafka_key"),
    F.col("value").cast("string").alias("raw_json"),
    ...
).withColumn("json_obj", F.from_json(F.col("raw_json"), json_schema))
```

Kafka 的 `value` 字段（bytes）cast 为 string，再用预定义 Schema 解析 JSON，同时保留 `kafka_key / topic / partition / offset / timestamp` 五个元数据字段写入 Bronze。

**③ 字段过滤**

```python
.filter(F.col("event_id").isNotNull())
.filter(F.col("text").isNotNull())
.filter(F.col("publish_time").isNotNull())
.filter(F.col("topic").isNotNull())
.filter(F.length(F.col("topic")) > 0)
```

任一关键字段为空的记录直接丢弃，不进入推理。

**④ StructBERT 情感推理**

`batch_df.collect()` 将数据拉到 Driver 端，调用 `StructBERTSentimentService.predict_texts()` 批量推理。详见 [§ 3 情感推理服务](#3-structbert-情感推理服务)。

**⑤ 热度计算**

在 Driver 端完成推理后，重新创建 Spark DataFrame，计算衍生字段：

```python
heat_score = round(
    1.0
    + like_count  × 0.6
    + repost_count × 1.0
    + (1.5 if negative else 0.5 if neutral else 0.2),   # 情感加成
    2
)
```

---

## 3. StructBERT 情感推理服务

### 模型信息

| 项目 | 值 |
|------|----|
| 模型 ID | `iic/nlp_structbert_sentiment-classification_chinese-base` |
| 来源 | 阿里达摩院 ModelScope |
| 任务 | 中文文本二分类（正向 / 负向） |
| 输入 | 最长 256 个字符的评论文本 |
| 输出 | 每个类别的置信度分数 |

### 单例模式

```python
class StructBERTSentimentService:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = StructBERTSentimentService()
        return cls._instance
```

模型在 Driver 进程中只初始化一次，避免每批次重复加载（约 400 MB）。

### 批量推理流程

```python
def predict_texts(self, texts):
    # 按 batch_size(32) 分块推理
    for i in range(0, len(texts), self.batch_size):
        chunk = texts[i:i+self.batch_size]
        results = self._predict_chunk(chunk)   # 批量
        # 若批量失败，降级为逐条推理
```

### 中性判定逻辑

模型原生只输出 `positive` / `negative`，系统引入置信度阈值实现三分类：

```python
if confidence < STRUCTBERT_NEUTRAL_THRESHOLD:  # 默认 0.72
    final_label = "neutral"
```

即：模型对正/负分类的把握不足 72% 时，归为中性，避免低置信度预测带来的误告警。

### 输出字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `sentiment_raw_label` | string | 模型原始预测（`positive` / `negative`） |
| `sentiment_confidence` | float | 预测类别的置信度 `[0, 1]` |
| `sentiment_label` | string | 最终标签（含 `neutral` 三分类） |
| `sentiment_score` | float | `pos_prob - neg_prob`，范围 `[-1, 1]`，中性时强制为 0 |
| `sentiment_source` | string | 固定为 `"structbert"` |
| `sentiment_model` | string | 使用的模型 ID |

---

## 4. 热度与风险等级

### 热度分数公式

```
heat_score = 1.0
           + like_count    × 0.6
           + repost_count  × 1.0
           + sentiment_bonus
```

情感加成（`sentiment_bonus`）：

| 情感标签 | 加成值 | 理由 |
|---------|--------|------|
| `negative` | +1.5 | 负面内容传播力更强 |
| `neutral` | +0.5 | 中性内容传播力一般 |
| `positive` | +0.2 | 正面内容传播力最弱 |

基础值 `1.0` 保证每条评论至少有非零热度。

### 风险等级

```python
risk_level =
    "high"   if (sentiment == "negative") and (heat_score >= 12) else
    "medium" if (sentiment == "negative") and (heat_score >= 5)  else
    "low"
```

---

## 5. Delta Lake 三层数据模型

根路径：`/data/public-opinion/delta/single_topic_comments/`

### Bronze 层：`bronze_comments_raw`

**写入模式**：Append，按 `event_date` 分区

保留 Kafka 元数据 + 原始 JSON + 情感推理结果，是**不可变的原始记录**，支持数据溯源与 Time Travel 回溯。

关键字段：

```
kafka_key / kafka_topic / kafka_partition / kafka_offset / kafka_timestamp
raw_json / event_id / platform / topic / publish_time / publish_ts
text / text_len / like_count / repost_count
sentiment_* (6个) / is_negative / heat_score / risk_level
event_hour / event_date
```

### Silver 层：`silver_comments_enriched`

**写入模式**：Merge（按 `event_id` 去重，`whenNotMatchedInsertAll`）

去除了 Kafka 原始元数据，保留业务字段。**同一 `event_id` 的消息只插入一次**，实现幂等写入（at-least-once → exactly-once 语义）。

相比 Bronze 额外保留 `kafka_topic / kafka_partition / kafka_offset`，便于消费位移追溯。

### Gold 层（4张表）

| 表名 | 写入 | 分组 | 主要指标 |
|------|------|------|---------|
| `gold_hourly_metrics` | Overwrite | `topic + stat_hour` | `comment_count` / `negative_count` / `negative_ratio` / `heat_sum` / `heat_avg` |
| `gold_daily_metrics` | Overwrite | `topic + stat_date` | 同上（按天） |
| `gold_daily_top10_comments` | Overwrite | `topic + event_date + rank_no` | 每话题每日 Top10 评论（按热度降序） |
| `gold_alert_events` | Append + Merge | `alert_key` | 触发的小时级预警记录 |

Gold 层每批次全量重算（Overwrite），保证历史数据与当前批次的聚合结果始终一致。

---

## 6. 告警逻辑

### 触发条件

```python
comment_count > ALERT_MIN_COMMENTS       # 默认 10
and negative_ratio > ALERT_NEG_RATIO_THRESHOLD  # 默认 0.40
```

即：某话题在某小时内有超过 10 条评论，且负面占比超过 40%。

### 去重机制

```python
alert_key = f"{topic}__{stat_hour}"   # e.g. "26考研人数下降__2026-03-05 23:00:00"
```

每次告警前查询 `gold_alert_events` 表中已有的 `alert_key`，同一话题+小时只发送一次邮件。

### 邮件内容

- 话题名称
- 统计小时
- 该小时评论总数、负面数、负面占比
- Top 3 负面评论（含发布时间、热度、正文）
