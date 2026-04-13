# StreamPulse 项目总览

> 天津工业大学 · 2026届毕业设计 · 大数据2201 · 高楠  
> 项目名称：基于 Structured Streaming 的实时舆情分析与热度趋势监测系统

---

## 项目是什么

**StreamPulse** 是一套端到端的实时舆情分析平台。核心功能链路：

```
爬取社交媒体评论 → Kafka消息队列 → Spark流式处理 → StructBERT情感分类
→ 热度/风险计算 → Delta Lake存储 → FastAPI服务 → Vue可视化大屏 + 邮件告警
```

---

## 五层分布式架构（答辩必背）

```
┌─────────────────────────────────────────────┐
│  第五层：展示层                               │
│  Vue 3 + Vite + ECharts 5                   │
│                    ▲ HTTP REST / JSON        │
├─────────────────────────────────────────────┤
│  第四层：数据服务层                           │
│  FastAPI · 端口18000 · 9个端点               │
│                    ▲ deltalake 直接读取      │
├─────────────────────────────────────────────┤
│  第三层：数据存储层                           │
│  Delta Lake · Bronze/Silver/Gold · 6张表    │
│                    ▲ foreachBatch 写入       │
├─────────────────────────────────────────────┤
│  第二层：流计算层                             │
│  Spark Structured Streaming · 60s微批次     │
│                    ▲ readStream             │
├─────────────────────────────────────────────┤
│  消息队列层                                   │
│  Apache Kafka · Topic: public_opinion_raw   │
│                    ▲ Kafka Producer         │
├─────────────────────────────────────────────┤
│  第一层：数据采集层                           │
│  MediaCrawler · Playwright CDP · 7平台      │
└─────────────────────────────────────────────┘
```

---

## 项目目录结构

```
graduation project/
├── crawler/              # 第一层：数据采集层（爬虫）
├── backend/
│   ├── spark/            # 第二层：流计算层
│   │   ├── stream_single_topic_to_delta.py   # 主Spark任务
│   │   ├── structbert_sentiment.py           # 情感推理服务
│   │   └── alert_mailer.py                   # 告警邮件
│   ├── api/              # 第四层：数据服务层
│   │   └── app.py                            # FastAPI REST服务
│   ├── tests/            # 测试脚本
│   └── scripts/          # 部署启动脚本
├── frontend/             # 第五层：展示层（Vue大屏）
├── docs/                 # 技术文档
├── learn/                # 学习笔记（本目录）
└── thesis/               # 毕业论文LaTeX源码
```

---

## 核心技术栈速查

| 层次 | 技术 | 关键版本 |
|------|------|---------|
| 数据采集 | Python + Playwright (CDP) | Python 3.12 |
| 消息队列 | Apache Kafka | 3.x |
| 流计算 | Spark Structured Streaming | 3.5.x |
| 湖仓存储 | Delta Lake | 3.3.2 |
| 情感模型 | StructBERT (阿里ModelScope) | 中文base版 |
| 后端API | FastAPI + uvicorn + deltalake + pandas | Python 3.12 |
| 前端 | Vue 3 + Vite + ECharts | Vue 3.5 / Vite 6 |
| 告警 | Python smtplib SMTP over TLS | — |
| 部署 | 阿里云ECS Linux `/opt/pipeline/` | — |

---

## 三个核心算法（答辩必考）

### 1. StructBERT 情感三分类

模型原生只输出正/负二分类，系统通过置信度阈值扩展为三分类：

```
置信度 < 0.72  →  强制归为 neutral（中性）
置信度 ≥ 0.72  →  使用模型原始预测 positive / negative
```

### 2. 热度分数公式

```
heat_score = 1.0
           + like_count    × 0.6
           + repost_count  × 1.0
           + 情感加成（负向:+1.5 / 中性:+0.5 / 正向:+0.2）

risk_level:
  high   → 负向 且 heat_score ≥ 12
  medium → 负向 且 heat_score ≥ 5
  low    → 其余
```

### 3. 告警去重机制

```
alert_key = f"{topic}__{stat_hour}"
触发条件：comment_count > 10  AND  negative_ratio > 0.40
去重：同一 alert_key 只发一封邮件，写入 gold_alert_events 表
```

---

## 答辩高频问答

**Q: 为什么选Spark而不是Flink？**  
Spark与Delta Lake原生深度集成，`DeltaTable.merge()` 可在foreachBatch内直接调用，无需第三方connector。项目数据量千条/分钟量级，Spark本地模式完全胜任。

**Q: 为什么用Delta Lake而不是MySQL？**  
Gold层是预聚合结果，数据量小（数千行），`deltalake` Python库直接读取延迟<100ms，省去同步数据库的运维复杂度。且Delta Lake支持ACID事务和Time Travel回溯历史数据。

**Q: StructBERT为什么在Driver端推理而不是Executor端？**  
模型约400MB，若每个Executor都加载，多Worker场景内存开销倍增。本项目使用`local[*]`单机模式，Driver即主进程，单例模式保证模型只加载一次。

**Q: Bronze/Silver/Gold三层分层意义是什么？**  
- **Bronze**：保存原始数据，支持数据溯源，不可变
- **Silver**：按event_id做Merge去重，实现幂等写入（at-least-once → exactly-once语义）
- **Gold**：预聚合，API直接读Gold，无需全表扫描，响应快

**Q: 如何保证同一告警不重复发送？**  
用 `topic__stat_hour` 作唯一键 `alert_key`，每次触发前查询 `gold_alert_events` 中已有的key集合，已存在则跳过。

---

## 各层详细学习文档索引

| 文件 | 内容 |
|------|------|
| [01_crawler.md](./01_crawler.md) | 数据采集层：MediaCrawler、Playwright CDP、JSONL输出、Kafka Producer |
| [02_kafka.md](./02_kafka.md) | 消息队列层：Kafka概念、Topic设计、消费语义、消息Schema |
| [03_spark_streaming.md](./03_spark_streaming.md) | 流计算层：Structured Streaming原理、foreachBatch流水线、情感推理、热度计算 |
| [04_delta_lake.md](./04_delta_lake.md) | 数据存储层：Delta Lake原理、三层数据模型、Merge操作、6张表详解 |
| [05_api.md](./05_api.md) | 数据服务层：FastAPI设计、9个端点详解、deltalake读取、数据序列化 |
| [06_frontend.md](./06_frontend.md) | 展示层：Vue3组合式API、ECharts图表、告警面板、API调用层 |
