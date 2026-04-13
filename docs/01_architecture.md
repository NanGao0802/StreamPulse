# 系统架构与技术选型

## 1. 整体设计思路

StreamPulse 采用**流式处理 + 湖仓一体**的架构范式，解决传统批处理延迟高、实时系统难以支撑复杂分析的矛盾。核心设计原则：

- **解耦采集与处理**：爬虫写 JSONL 文件，Producer 进程异步推送 Kafka，两者独立扩展
- **准实时优先**：60 秒微批次在延迟与吞吐之间取得平衡，对舆情监测场景足够
- **Driver 端推理**：StructBERT 模型仅在 Driver 端加载一次（单例），避免 Executor 反复序列化模型的开销
- **湖仓分层**：Bronze 保留原始数据，Silver 去重精炼，Gold 预聚合，API 层直接读 Gold，查询无需全表扫描

---

## 2. 五层分布式架构

```
┌─────────────────────────────────────────────────────────────────┐
│  第五层：展示层                                                   │
│  Vue 3 + Vite · Apache ECharts 6 · 原生 CSS Glassmorphism       │
│  单页应用，话题切换 + 粒度切换 + 趋势图 + 评论表 + 告警面板        │
│                            ▲  HTTP GET / JSON                   │
├─────────────────────────────────────────────────────────────────┤
│  第四层：数据服务层                                               │
│  FastAPI 3.x · uvicorn · deltalake (Python) · pandas            │
│  9 个 REST 端点，直接读取 Delta Lake 文件，无额外数据库依赖        │
│                            ▲  deltalake.DeltaTable.to_pandas()  │
├─────────────────────────────────────────────────────────────────┤
│  第三层：数据存储层                                               │
│  Delta Lake 3.3（本地文件系统 /data/public-opinion/delta/）      │
│  6 张表：Bronze · Silver · Gold×4（小时/日/Top10/告警）           │
│                            ▲  foreachBatch 写入                 │
├─────────────────────────────────────────────────────────────────┤
│  第二层：流计算层                                                 │
│  Apache Spark 3.5 Structured Streaming                          │
│  60s 微批次 · StructBERT 情感推理 · 热度计算 · 告警检测           │
│                            ▲  readStream（消息流）               │
├─────────────────────────────────────────────────────────────────┤
│  消息队列层                                                       │
│  Apache Kafka · Topic: public_opinion_raw                       │
│  At-Least-Once 语义 · Offset 检查点 · 持久化日志                  │
│                            ▲  Kafka Producer（Python）          │
├─────────────────────────────────────────────────────────────────┤
│  第一层：数据采集层                                               │
│  MediaCrawler · Playwright CDP · httpx · asyncio                │
│  微博 · B站 · 抖音 · 小红书 · 知乎 · 快手 · 贴吧                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. 各层职责详解

### 第一层：数据采集层（`crawler/`）

基于 MediaCrawler 框架二次开发，核心改动：

- 新增 **JSONL 输出模式**：每条评论序列化为一行 JSON，追加写入本地文件
- 新增 **Kafka Producer 进程**：监听输出目录，逐行读取 JSONL 并 `produce` 到 Topic
- 保留 CDP 反检测机制（通过 Chrome DevTools Protocol 控制真实浏览器）

输出的 Kafka 消息格式见 [02_data_pipeline.md § Kafka 消息 Schema](./02_data_pipeline.md#1-kafka-消息-schema)。

### 消息队列层（Apache Kafka）

- 单 Topic `public_opinion_raw` 承接所有平台所有话题
- 通过 `topic` 字段在消息体内区分话题，无需多 Topic
- Spark 使用 `startingOffsets=latest` 和 Offset 检查点保证 At-Least-Once

### 第二层：流计算层（`backend/spark/`）

核心文件：

| 文件 | 职责 |
|------|------|
| `stream_single_topic_to_delta.py` | 主任务：Kafka Source → foreachBatch 处理 → Delta 写入 |
| `structbert_sentiment.py` | StructBERT 单例推理服务，批量推理 + 异常降级 |
| `alert_mailer.py` | SMTP 告警邮件发送，含去重逻辑 |

处理逻辑详见 [02_data_pipeline.md](./02_data_pipeline.md)。

### 第三层：数据存储层（Delta Lake）

Delta Lake 以**本地文件系统**存储 Parquet + 事务日志，无需独立服务进程。  
路径根目录：`/data/public-opinion/delta/single_topic_comments/`

### 第四层：数据服务层（`backend/api/`）

- 使用 `deltalake` Python 库直接读取 Delta 文件，**无需 Spark 运行时**
- `read_delta_as_pandas()` → `df.to_pandas()` → 序列化为 JSON 返回
- 对 `NaN`、`pd.Timestamp`、numpy 标量等做统一 `normalize_value()` 处理

### 第五层：展示层（`frontend/`）

- Vue 3 组合式 API + Vite 构建
- ECharts 5 折线图，支持 DataZoom 缩放
- 通过 `VITE_API_BASE` 环境变量指向 API 服务地址

---

## 4. 技术选型说明

### 为什么选 Spark Structured Streaming 而非 Flink？

Spark 与 Delta Lake 原生深度集成（`delta-spark` 包），`DeltaTable.merge()` 可在 foreachBatch 内直接调用，无需额外 connector。Flink 接入 Delta Lake 需要第三方 connector，且维护成本更高。本项目数据量在千条/分钟量级，Spark 本地模式完全胜任。

### 为什么选 Delta Lake 而非 Hudi / Iceberg？

Delta Lake 对 Python 支持最好：`delta-spark`（PySpark 写入） + `deltalake`（纯 Python 读取）两个包分别对应读写场景，API 层无需启动 Spark 进程即可读取数据。

### 为什么 StructBERT 在 Driver 端推理？

模型体积约 400 MB，若在每个 Executor 上加载，多 Worker 场景内存开销倍增。本项目使用 `local[*]` 模式（单机），Driver 即主进程，单例模式保证模型只加载一次。

### 为什么 FastAPI 直接读 Delta 文件而非查数据库？

Gold 层表为预聚合结果，数据量小（通常数千行），`deltalake` 读取延迟在 100ms 以内，满足 API 响应需求，省去同步数据到关系型数据库的运维复杂度。

---

## 5. 组件关系图

```
crawler/ ──JSONL──► Kafka Producer ──► Kafka Topic
                                            │
                                            ▼
                              Spark Structured Streaming
                              (stream_single_topic_to_delta.py)
                                            │
                              ┌─────────────┼─────────────┐
                              ▼             ▼             ▼
                           Bronze        Silver     Gold Tables
                           (append)    (merge)    (overwrite)
                                                       │
                                               FastAPI (app.py)
                                               deltalake 读取
                                                       │
                                               Vue 3 前端
                                               ECharts 展示
```
