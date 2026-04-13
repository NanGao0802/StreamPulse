# 消息队列层（Apache Kafka）

> 对应架构层级：**消息队列层**（位于采集层与流计算层之间）  
> 核心技术：Apache Kafka 3.x

---

## 待学习内容清单

- [x] Kafka 是什么，为什么要用消息队列做中间层
- [x] Topic / Partition / Offset / Consumer Group 基本概念
- [x] At-Least-Once 消费语义是什么意思，与 Exactly-Once 的区别
- [x] 为什么所有话题用同一个 Topic，用消息体内的 `topic` 字段区分
- [x] Offset 检查点（Checkpoint）如何保证消费不丢失、不重复
- [x] `startingOffsets: latest` 与 `earliest` 的区别
- [x] `maxOffsetsPerTrigger: 50` 的作用是什么

---

## 本项目的 Kafka 设计

| 项目 | 值 |
|------|----|
| Broker 地址 | `172.16.156.151:9092`（环境变量 `KAFKA_BOOTSTRAP` 可覆盖） |
| Topic 名称 | `public_opinion_raw` |
| 消费语义 | At-Least-Once |
| 检查点路径 | `/data/public-opinion/checkpoints/single_topic_comment_pipeline` |
| 每批最大消费量 | 50 条（`maxOffsetsPerTrigger`） |
| 触发间隔 | 60 秒（`processingTime="60 seconds"`） |

---

## 1. Kafka 是什么，为什么要用它

Kafka 是一个**分布式消息队列**，核心思想是把"生产数据的人"（Producer）和"消费数据的人"（Consumer）解耦。

**本项目不用 Kafka 会怎样？**

```
# 没有Kafka的架构（直连方案）
爬虫 ──直接写─► Spark Streaming

问题：
- 爬虫崩溃时 Spark 无数据可读
- Spark 重启时爬虫数据丢失
- 爬虫速度 ≠ Spark 消费速度，容易堆积或饿死
```

**有 Kafka 的架构：**

```
爬虫（Producer） ──写入─► Kafka Topic ──读取─► Spark（Consumer）

优势：
- 解耦：爬虫和 Spark 各自独立扩展、重启，互不影响
- 缓冲：Kafka 持久化日志，Spark 宕机重启后从检查点位置继续消费
- At-Least-Once：消息不会丢失（最多重复，Silver 层 Merge 去重弥补）
```

---

## 2. 核心概念

### Topic / Partition / Offset

```
Topic: public_opinion_raw
  └── Partition 0
        ├── Offset 0: {"event_id":"dy:001","topic":"考研",...}
        ├── Offset 1: {"event_id":"bili:002","topic":"考研",...}
        ├── Offset 2: {"event_id":"wb:003","topic":"全红婵",...}
        └── Offset N: ...
```

- **Topic**：消息的逻辑分类，本项目用单 Topic `public_opinion_raw` 承接所有话题
- **Partition**：Topic 的物理分片，本项目单分区即可（数据量不大）
- **Offset**：消息在 Partition 中的序号，从 0 递增，永远不变

### 为什么所有话题用同一个 Topic

本项目选择**单 Topic + 消息体内 `topic` 字段区分**，原因：
- 简化运维：不需要为每个新话题创建新 Topic
- 灵活性：爬虫随时可以抓新话题，无需预配置 Kafka
- Spark 端通过 `F.trim(F.col("json_obj.topic"))` 提取话题字段，在代码层面分组处理

---

## 3. At-Least-Once 消费语义

### 什么是 At-Least-Once

Kafka 消费时，Offset 的提交时机决定了消费语义：

```
At-Most-Once:  先提交Offset，再处理  → 处理失败时数据丢失（≤1次）
At-Least-Once: 先处理，再提交Offset  → 处理失败重启后重复消费（≥1次）
Exactly-Once:  事务保证              → 精确一次（复杂，有性能代价）
```

Spark Structured Streaming 使用 **At-Least-Once**：只有当 `foreachBatch` 成功完成后，才更新检查点中记录的 Offset。

### 本项目如何处理重复

At-Least-Once 可能导致同一条消息被消费两次（Spark 重启场景），解决方案：

```
Kafka 重复消费
    │
    ▼
Bronze 层：Append，允许重复（保留原始记录，用于溯源）
    │
    ▼
Silver 层：Merge（按 event_id）
    "t.event_id = s.event_id" + whenNotMatchedInsertAll
    → 同一 event_id 只插入一次
    → 实现 At-Least-Once → Exactly-Once 的语义转换
```

---

## 4. Offset 检查点（Checkpoint）

检查点目录：`/data/public-opinion/checkpoints/single_topic_comment_pipeline`

Spark 在每批次成功处理后，将当前消费到的 Offset 写入检查点目录。结构大致如下：

```
checkpoints/
├── offsets/
│   ├── 0          # 批次0的offset记录
│   ├── 1          # 批次1的offset记录
│   └── ...
├── commits/
│   ├── 0          # 批次0已提交标记
│   └── ...
└── metadata       # 流查询元数据
```

**Spark 重启后的行为：**
1. 读取检查点目录，找到上次成功提交的 Offset
2. 从该 Offset 位置继续消费 Kafka，不会漏掉消息

---

## 5. Spark 端的 Kafka Source 配置

代码位置：`backend/spark/stream_single_topic_to_delta.py` 的 `main()` 函数

```python
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)  # Broker地址
    .option("subscribe", KAFKA_TOPIC)                    # 订阅Topic
    .option("startingOffsets", "latest")                 # 首次启动从最新位置开始
    .option("failOnDataLoss", "false")                   # Offset丢失时不报错（容错）
    .option("maxOffsetsPerTrigger", "50")                # 每批最多消费50条
    .load()
)
```

### 参数说明

| 参数 | 值 | 说明 |
|------|----|------|
| `startingOffsets` | `"latest"` | 首次无检查点时从最新消息开始，有检查点则从检查点继续 |
| `failOnDataLoss` | `"false"` | Kafka 消息过期（retention 到期）时不报错，容忍数据丢失 |
| `maxOffsetsPerTrigger` | `"50"` | 每个 60s 批次最多读取 50 条，控制 StructBERT 推理时间，防止单批次过长 |

### 为什么限制 maxOffsetsPerTrigger=50

StructBERT 推理每条约需几十毫秒，50 条约需 1-2 秒，加上 Bronze/Silver/Gold 写入，60 秒批次内完全可以处理完。若不限制，Kafka 积压时可能一批几千条，导致推理超时。

---

## 6. Kafka 消息 Schema 详解

```json
{
  "event_id":     "dy:7613792636118516522",
  "platform":     "dy",
  "topic":        "26考研人数下降",
  "publish_time": "2026-03-05 23:24:05",
  "time_id":      "202603052324",
  "text":         "评论正文内容",
  "like_count":   4,
  "repost_count": 0,
  "ingest_time":  "2026-03-06 00:01:12",
  "source_file":  "/opt/pipeline/data/dy_20260305.jsonl"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `event_id` | string | `平台:原始ID`，全局唯一，Silver 层 Merge 去重键 |
| `platform` | string | 来源平台标识 |
| `topic` | string | 话题关键词，与爬虫搜索词一致 |
| `publish_time` | string | 格式 `yyyy-MM-dd HH:mm:ss` |
| `time_id` | string | 日期桶 `yyyyMMddHHMM`，由 `event_schema.make_time_id()` 生成 |
| `text` | string | 评论正文，Spark 会过滤空值 |
| `like_count` | long | 点赞数，缺失时默认 0 |
| `repost_count` | long | 转发数，缺失时默认 0 |
| `ingest_time` | string | 写入 Kafka 的时间戳 |
| `source_file` | string | 来源 JSONL 路径，用于溯源 |

---

## 7. Spark 解析 Kafka 消息的流程

```python
# Kafka value 字段是 bytes，先 cast 为 string
# 再用 from_json + 预定义 Schema 解析
parsed_df = kafka_df.select(
    F.col("key").cast("string").alias("kafka_key"),
    F.col("value").cast("string").alias("raw_json"),     # 原始JSON字符串
    F.col("topic").alias("kafka_topic"),                 # Kafka元数据
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_timestamp"),
)
.withColumn("json_obj", F.from_json(F.col("raw_json"), json_schema))
.select(
    "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
    "raw_json",
    F.col("json_obj.event_id").alias("event_id"),
    F.trim(F.col("json_obj.topic")).alias("topic"),      # 去除话题首尾空格
    ...
)
.filter(F.col("event_id").isNotNull())   # 丢弃event_id为空的脏数据
.filter(F.col("text").isNotNull())       # 丢弃text为空的脏数据
.filter(F.col("publish_time").isNotNull())
.filter(F.col("topic").isNotNull())
.filter(F.length(F.col("topic")) > 0)
```

原始 Kafka 消息的 `value` 字段是二进制，Spark 先将其 cast 为 string，再用 `from_json` 按预定义 Schema 解析，同时保留 5 个 Kafka 元数据字段写入 Bronze 层，便于后续数据溯源。

---

## 学习笔记

<!-- 在这里补充你的学习理解 -->
