# 第三层：数据存储层（Delta Lake）

> 对应架构层级：**数据存储层**  
> 核心技术：Delta Lake 3.3.2 · Parquet · 本地文件系统

---

## 待学习内容清单

- [x] Delta Lake 是什么，相比普通 Parquet 文件有什么优势
- [x] ACID 事务在 Delta Lake 中是如何实现的（事务日志 `_delta_log`）
- [x] Time Travel（时间旅行）是什么，如何查看历史版本数据
- [x] Bronze/Silver/Gold 三层数据湖架构的设计理念
- [x] Append / Merge / Overwrite 三种写入模式的区别和适用场景
- [x] `DeltaTable.merge()` 的 `whenNotMatchedInsertAll` 语义
- [x] `event_id` 为何能作为全局唯一键
- [x] Gold层为什么每批次全量重算（Overwrite）而不是增量更新
- [x] `gold_alert_events` 为什么用 Append 而不是 Overwrite
- [x] `deltalake` Python库（读）与 `delta-spark`（写）的关系

---

## 存储路径

```
/data/public-opinion/delta/single_topic_comments/
├── bronze_comments_raw/          # Bronze层
├── silver_comments_enriched/     # Silver层
├── gold_hourly_metrics/          # Gold - 小时聚合
├── gold_daily_metrics/           # Gold - 日聚合
├── gold_daily_top10_comments/    # Gold - 每日Top10评论
└── gold_alert_events/            # 告警事件记录
```

---

## 六张表详解

### Bronze：`bronze_comments_raw`

| 属性 | 值 |
|------|----|
| 写入模式 | **Append**（只追加，永不修改） |
| 分区字段 | `event_date` |
| 作用 | 保留原始数据，支持溯源和Time Travel |

**关键字段：**
```
kafka_key / kafka_topic / kafka_partition / kafka_offset / kafka_timestamp
raw_json / event_id / platform / topic / publish_time / publish_ts
text / text_len / like_count / repost_count
sentiment_* (6个字段) / is_negative / heat_score / risk_level
event_hour / event_date
```

---

### Silver：`silver_comments_enriched`

| 属性 | 值 |
|------|----|
| 写入模式 | **Merge**（按 `event_id` 去重） |
| Merge条件 | `t.event_id = s.event_id`，`whenNotMatchedInsertAll` |
| 作用 | 精炼数据，实现幂等写入（at-least-once → exactly-once） |

**相比Bronze去掉的字段：** `kafka_key`、`raw_json`  
**保留的Kafka字段：** `kafka_topic / kafka_partition / kafka_offset`（用于消费位移追溯）

---

### Gold小时：`gold_hourly_metrics`

| 属性 | 值 |
|------|----|
| 写入模式 | **Overwrite**（每批次全量重算） |
| 分组维度 | `topic + stat_hour`（整点小时） |

**聚合字段：**

| 字段 | 说明 |
|------|------|
| `comment_count` | 该话题该小时评论总数 |
| `negative_count` | 负面评论数 |
| `negative_ratio` | 负面占比（保留4位小数） |
| `heat_sum` | 热度总和 |
| `heat_avg` | 平均热度 |
| `like_count_sum` | 点赞总数 |
| `repost_count_sum` | 转发总数 |

---

### Gold日：`gold_daily_metrics`

与小时表结构相同，分组维度为 `topic + stat_date`（日期）。

---

### Gold Top10：`gold_daily_top10_comments`

| 属性 | 值 |
|------|----|
| 写入模式 | **Overwrite** |
| 分区维度 | `topic + event_date` |
| 排名规则 | 按 `heat_score DESC, like_count DESC, repost_count DESC` |

每话题每日保留热度前10条评论，用于评论明细展示。

---

### 告警事件：`gold_alert_events`

| 属性 | 值 |
|------|----|
| 写入模式 | **Append**（每次新告警追加） |
| 唯一键 | `alert_key = f"{topic}__{stat_hour}"` |

**字段说明：**

| 字段 | 说明 |
|------|------|
| `alert_key` | 去重键，格式 `话题__小时` |
| `topic` | 话题名称 |
| `stat_hour` | 统计小时 |
| `comment_count` | 该小时评论总数 |
| `negative_count` | 负面评论数 |
| `negative_ratio` | 负面占比 |
| `top_negative_comments_json` | Top3负面评论（JSON字符串） |
| `email_status` | 邮件发送状态（sent/failed/disabled） |
| `created_at` | 告警触发时间 |

---

## 三层设计理念

```
Bronze（原始）→ Silver（精炼）→ Gold（预聚合）

Bronze: 不可变，保留所有原始信息，支持回溯
Silver: 去重后的干净数据，业务分析基础
Gold:   预计算好的汇总指标，API直接读取，无需全表扫描
```

**API层只读 Gold 和 Silver**，Bronze仅用于数据溯源和排查问题。

---

## 1. Delta Lake 是什么

Delta Lake 是建立在 **Parquet 文件**之上的**开源存储格式增强层**，核心就是一个 `_delta_log/` 目录——事务日志。

```
delta表目录/
├── _delta_log/              # 事务日志（Delta的核心）
│   ├── 00000000000000000000.json   # 版本0：建表操作
│   ├── 00000000000000000001.json   # 版本1：第1次写入
│   ├── 00000000000000000002.json   # 版本2：第2次写入
│   └── ...
├── event_date=2026-03-05/         # 分区目录
│   ├── part-00000-xxx.snappy.parquet
│   └── ...
└── event_date=2026-03-06/
    └── ...
```

每次写操作（Append/Overwrite/Merge）都会在 `_delta_log/` 中追加一个 JSON 文件，记录：
- 新增了哪些 Parquet 文件（`add`）
- 删除了哪些 Parquet 文件（`remove`）
- Schema 信息
- 操作类型和时间戳

**读取时**，Delta Lake 先读 `_delta_log/` 重建出"当前表的最新状态"（哪些文件有效），再读对应的 Parquet 文件。

---

## 2. Delta Lake vs 普通 Parquet

| 特性 | 普通 Parquet 目录 | Delta Lake |
|------|-----------------|-----------|
| ACID 事务 | ✗ 并发写可能损坏数据 | ✓ 乐观锁并发控制 |
| Upsert/Merge | ✗ 需要重写整个分区 | ✓ `DeltaTable.merge()` 原生支持 |
| Time Travel | ✗ | ✓ 按版本号或时间戳查询历史 |
| Schema 演化 | 手动维护 | `overwriteSchema=true` 自动更新 |
| 流批一体 | ✗ | ✓ 同一张表可同时被流写入和批读取 |
| 小文件合并 | 手动 | `OPTIMIZE` + `VACUUM` 命令 |
| 回滚 | ✗ | ✓ `RESTORE TABLE TO VERSION AS OF N` |

---

## 3. ACID 事务：_delta_log 是如何工作的

```
Spark 写入一批数据时：

Step 1: 将新数据写到临时 Parquet 文件
Step 2: 在 _delta_log/ 追加一个 JSON 提交记录
        {
          "add": [{"path": "part-xxx.parquet", ...}],
          "commitInfo": {"operation": "WRITE", "timestamp": ...}
        }
Step 3: 提交成功 → 读者立即可见新数据

如果 Step 2 失败（崩溃）：
  → Parquet文件存在但没有对应的 _delta_log 条目
  → 读者不会看到这批"孤儿文件"
  → 原子性保证：要么全看见，要么全看不见
```

---

## 4. Time Travel 时间旅行

Delta Lake 保留所有历史版本，可以查询任意时间点的数据：

```python
# 查询版本 5 的数据（Spark）
df = spark.read.format("delta").option("versionAsOf", 5).load(SILVER_PATH)

# 查询某时间点的数据
df = spark.read.format("delta").option("timestampAsOf", "2026-03-01").load(SILVER_PATH)

# 使用 deltalake Python 库查看历史（API 服务中用）
from deltalake import DeltaTable
dt = DeltaTable(SILVER_PATH)
print(dt.history())   # 返回所有版本历史
```

**实际价值：**
- 如果 StructBERT 模型升级后重新推理，可对比新旧版本的情感标签变化
- 误删数据后可用 Time Travel 恢复
- 答辩时可演示：`dt.history()` 列出 Bronze 层所有写入版本

---

## 5. 三层数据湖架构（Medallion Architecture）

```
Raw Data                   Refined Data             Aggregated Data
    │                           │                         │
    ▼                           ▼                         ▼
┌─────────┐   Merge去重    ┌─────────┐  全量重算     ┌─────────┐
│  Bronze │ ──────────►   │  Silver │ ─────────►   │  Gold   │
│  原始层  │               │  精炼层  │              │  汇总层  │
└─────────┘               └─────────┘              └─────────┘
 Append写入                Exactly-Once             预聚合指标
 保留所有原始字段           按event_id去重            API直接读取
 包含Kafka元数据           去掉Kafka元数据            <100ms响应
```

### 为什么需要三层？

- **Bronze 保什么？**  保存所有原始字段（包括 `raw_json`、Kafka Offset），支持数据溯源和模型重跑。即使后续处理出错，Bronze 永远有原始数据兜底。
- **Silver 解决什么？**  解决 Kafka At-Least-Once 带来的重复消费问题，通过 `event_id` Merge 实现 Exactly-Once 写入。
- **Gold 目的是什么？**  预聚合好的 KPI 数据（几千行），FastAPI 直接读取，无需 Spark 也无需全表扫描，响应速度极快。

---

## 6. 六张表完整 Schema

### Bronze：`bronze_comments_raw`

**写入方式：** `mode("append").partitionBy("event_date")`

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `kafka_key` | string | Kafka消息key |
| `kafka_topic` | string | Kafka topic名 |
| `kafka_partition` | int | Kafka分区号 |
| `kafka_offset` | long | Kafka offset（溯源关键字段） |
| `kafka_timestamp` | timestamp | Kafka写入时间戳 |
| `raw_json` | string | 原始JSON字符串（完整保留） |
| `event_id` | string | `平台:原始ID` |
| `platform` | string | 平台标识 |
| `topic` | string | 话题关键词 |
| `publish_time` | string | 发布时间字符串 |
| `publish_ts` | timestamp | 发布时间（转换为timestamp类型） |
| `time_id` | string | `yyyyMMddHHMM` |
| `text` | string | 评论正文 |
| `text_len` | int | 正文字符数 |
| `like_count` | long | 点赞数 |
| `repost_count` | long | 转发数 |
| `sentiment_raw_label` | string | 模型原始预测 |
| `sentiment_confidence` | double | 置信度 |
| `sentiment_label` | string | 最终三分类标签 |
| `sentiment_score` | double | 正负概率差 [-1,1] |
| `sentiment_source` | string | 固定 "structbert" |
| `sentiment_model` | string | 模型ID |
| `is_negative` | int | 1=负向，0=其他 |
| `heat_score` | double | 热度分数 |
| `risk_level` | string | high/medium/low |
| `event_hour` | timestamp | 整点小时 |
| `event_date` | date | **分区字段** |
| `ingest_time` | string | 爬虫写入时间 |
| `source_file` | string | 来源JSONL路径 |

---

### Silver：`silver_comments_enriched`

**写入方式：** `DeltaTable.merge()` + `whenNotMatchedInsertAll()`

与 Bronze 相比的差异：
- **去掉** `kafka_key`（Kafka内部字段，无业务价值）
- **去掉** `raw_json`（原始JSON已在Bronze保留）
- **其余字段相同**，多保留 `kafka_partition / kafka_offset`（消费位移追溯）

---

### Gold 小时：`gold_hourly_metrics`

**写入方式：** `mode("overwrite").option("overwriteSchema", "true")`  
**分组维度：** `topic` + `stat_hour`（整点小时，由 `date_trunc("hour", publish_ts)` 生成）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `topic` | string | 话题关键词 |
| `stat_hour` | timestamp | 统计小时（整点） |
| `comment_count` | long | 该话题该小时评论总数 |
| `negative_count` | long | 负面评论数 |
| `negative_ratio` | double | 负面占比（4位小数） |
| `heat_sum` | double | 热度总和（2位小数） |
| `heat_avg` | double | 平均热度（2位小数） |
| `like_count_sum` | long | 点赞总数 |
| `repost_count_sum` | long | 转发总数 |

---

### Gold 日：`gold_daily_metrics`

结构与小时表完全一致，分组维度换为 `topic` + `stat_date`（`date(publish_ts)` 生成，类型 date）。

---

### Gold Top10：`gold_daily_top10_comments`

**写入方式：** `mode("overwrite")`  
**内容：** 每话题每天热度前10条评论（Silver层取出后按 `heat_score DESC, like_count DESC, repost_count DESC` 排序取前10）

字段同 Silver，额外多一个 `rank`（排名序号 1-10）。

---

### 告警事件：`gold_alert_events`

**写入方式：** `mode("append")`（每次新告警追加，永不覆盖）

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `alert_key` | string | 去重键：`{topic}__{stat_hour}` |
| `topic` | string | 话题关键词 |
| `stat_hour` | string | 统计小时 |
| `comment_count` | int | 该小时评论总数 |
| `negative_count` | int | 负面评论数 |
| `negative_ratio` | double | 负面占比 |
| `top_negative_comments_json` | string | Top3负面评论（JSON字符串） |
| `email_status` | string | `sent` / `failed` / `disabled` |
| `email_message` | string | 邮件发送结果描述 |
| `created_at` | string | 告警触发时间 |

**为什么用 Append 而不是 Overwrite？**  
告警是历史事件记录，需要完整保留所有历史告警，不能覆盖。用 `alert_key` 在应用层去重，而不是在 Delta 层去重。

---

## 7. `deltalake` Python 库 vs `delta-spark`

| | `delta-spark` | `deltalake`（Python） |
|--|--------------|----------------------|
| **用途** | Spark 任务中读写 Delta 表 | 纯 Python 环境读取 Delta 表 |
| **依赖** | 需要 JVM + Spark | 只需 Python，基于 Rust 实现 |
| **支持写入** | ✓ | 有限（主要用于读） |
| **支持 Merge** | ✓ | 有限 |
| **使用场景** | `stream_single_topic_to_delta.py` | `backend/api/app.py` |

FastAPI 服务使用 `deltalake` 库，这样 API 进程启动无需 JVM，响应更快，部署更简单：

```python
# backend/api/app.py 中的用法
from deltalake import DeltaTable

dt = DeltaTable("/data/public-opinion/delta/.../gold_hourly_metrics")
df = dt.to_pandas()          # 直接转 Pandas DataFrame，延迟 <100ms
version = dt.version()       # 获取当前版本号（用于 /api/health 端点）
history = dt.history()       # 查看历史版本（Time Travel）
```

---

## 8. 三种写入模式对比

| 模式 | 语义 | 本项目使用场景 |
|------|------|--------------|
| `Append` | 只追加新数据，不修改已有数据 | Bronze（原始记录）、告警事件 |
| `Merge` | 按条件匹配：匹配则更新/忽略，不匹配则插入 | Silver（按 event_id 去重） |
| `Overwrite` | 删除所有现有数据，写入新数据 | Gold 三张表（全量重算） |

**Merge 的完整语义：**

```python
target.merge(source, condition)
    .whenMatchedUpdate(set={...})          # 匹配时：更新指定字段
    .whenMatchedDelete()                   # 匹配时：删除
    .whenMatchedUpdateAll()                # 匹配时：用source所有字段覆盖
    .whenNotMatchedInsert(values={...})    # 不匹配时：插入指定字段
    .whenNotMatchedInsertAll()             # 不匹配时：插入source所有字段 ← 本项目用这个
    .execute()
```

本项目只用 `whenNotMatchedInsertAll()`，等效于：如果 `event_id` 不存在则插入，已存在则跳过（不更新）。

---

## 学习笔记

<!-- 在这里补充你的学习理解 -->
