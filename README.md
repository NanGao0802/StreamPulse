# StreamPulse — 基于 Structured Streaming 的实时舆情分析系统

> 天津工业大学 · 2026 届毕业设计 · 大数据 2201 · 高楠

**StreamPulse** 是一套端到端的**实时舆情分析与热度趋势监测**平台，以 Apache Kafka + Spark Structured Streaming 为核心流处理引擎，集成 StructBERT 中文情感模型与 Delta Lake 湖仓架构，实现从多平台社交媒体评论采集、准实时流处理、情感极性分类、热度计算到可视化大屏展示的完整闭环。

---

## ✨ 系统特色

| 特性 | 说明 |
|------|------|
| **准实时处理** | 60 秒微批次触发，端到端延迟满足准实时监测需求 |
| **多平台接入** | 支持微博、抖音、B站、小红书、知乎、快手、贴吧 7 大平台 |
| **中文情感分析** | 集成阿里达摩院 StructBERT，正/负/中性三分类，批量推理 batch_size=32 |
| **湖仓一体存储** | Delta Lake Bronze-Silver-Gold 三层渐进式数据精炼，支持 ACID 与 Time Travel |
| **自动告警** | 负面舆情占比超阈值时自动发送 SMTP 告警邮件，同一小时去重 |
| **可视化大屏** | Vue 3 + ECharts 单页应用，多话题切换、按天/小时粒度趋势图、评论明细与告警面板 |

---

## 系统架构

系统采用**五层分布式架构**，由下至上依次为：

```
┌─────────────────────────────────────────────────────────────┐
│  展示层（前端）                                               │
│  Vue 3 + Vite · Apache ECharts 6                            │
│  多话题切换 · KPI 卡 · 趋势折线图 · 评论明细表 · 告警面板     │
│                         ▲ HTTP REST / JSON                  │
├─────────────────────────────────────────────────────────────┤
│  数据服务层（API）                                            │
│  FastAPI · 监听端口 18000                                    │
│  /api/dashboard · /api/comments · /api/alerts 等 9 个端点   │
│                       ▲ deltalake 直接读取                  │
├─────────────────────────────────────────────────────────────┤
│  数据存储层（湖仓）                                           │
│  Delta Lake（本地文件系统）                                   │
│  Bronze · Silver · Gold Hourly/Daily/Top10 · Alert Events   │
│                       ▲ foreachBatch 写入                   │
├─────────────────────────────────────────────────────────────┤
│  流计算层（Spark Structured Streaming）                       │
│  60s 微批次触发 · StructBERT 情感推理 · 热度计算             │
│  foreachBatch: Bronze写入→Silver Merge→Gold重算→告警检测     │
│                       ▲ readStream（消息流）                 │
├─────────────────────────────────────────────────────────────┤
│  消息队列层（Apache Kafka）                                   │
│  Topic: public_opinion_raw  Broker: 172.16.156.151:9092     │
│  持久化日志 · At-Least-Once 消费语义 · Offset 检查点管理      │
│                       ▲ JSONL / Kafka Producer              │
├─────────────────────────────────────────────────────────────┤
│  数据采集层（爬虫）                                           │
│  MediaCrawler 多平台异步爬虫 · CDP 反检测                    │
│  微博 · B站 · 抖音 · 小红书 · 知乎 · 快手 · 贴吧             │
└─────────────────────────────────────────────────────────────┘
```

### foreachBatch 五步处理流水线

每个 60 秒批次按以下顺序执行：

```
① Kafka Source  →  ② JSON 解析  →  ③ 字段过滤  →  ④ StructBERT 推理  →  ⑤ 热度计算
                                                                              │ enriched_df
                                                                              ▼
                    Bronze 写入  ←  Silver Merge  ←  Gold 重算  ←  告警检测
```

| 步骤 | 操作 | 说明 |
|------|------|------|
| ① | Kafka Source | 按 Offset 读取最新消息，每批最多 50 条 |
| ② | JSON 解析 | `from_json` 按预定义 Schema 解析，追加 Kafka 元数据 |
| ③ | 字段过滤 | 过滤 `event_id` / `text` / `publish_time` / `topic` 任一为空的记录 |
| ④ | StructBERT 推理 | 批量调用情感 Pipeline，追加 5 个情感字段 |
| ⑤ | 热度计算 | 计算 `heat_score` / `risk_level` / `event_hour` / `event_date` 等衍生字段 |

---

## 目录结构

```
graduation project/
├── crawler/              # 爬虫模块（多平台社交媒体数据采集）
├── backend/              # 后端模块（流处理管道 + REST API）
│   ├── spark/            # Spark Structured Streaming 核心任务
│   ├── api/              # FastAPI 数据服务
│   ├── tests/            # 测试与调试脚本
│   └── scripts/          # 部署与运维脚本
├── frontend/             # 前端模块（Vue 3 可视化大屏）
└── thesis/               # 毕业论文（LaTeX 源码 + Markdown 草稿）
    └── latex/
        ├── main.tex
        ├── chapters/
        ├── figures/
        └── references.bib
```

---

## 模块说明

### 一、爬虫模块 (`crawler/`)

基于开源 MediaCrawler 框架二次开发，支持多平台关键词搜索爬取与轮询模式。

**支持平台：**

| 平台 | 标识 |
|------|------|
| 小红书 | `xhs` |
| 抖音 | `dy` |
| B 站 | `bili` |
| 微博 | `wb` |
| 快手 | `ks` |
| 贴吧 | `tieba` |
| 知乎 | `zhihu` |

**核心功能：**
- **CDP 模式反检测**：通过 Chrome DevTools Protocol 控制用户真实浏览器，降低被平台检测风险
- **轮询抓取**：`ENABLE_POLL_LATEST=True` 时持续轮询抓取最新评论，支持自定义间隔
- **JSONL 输出**：爬取结果以 JSONL 格式写入指定目录，供 Kafka Producer 消费
- **多存储后端**：支持 PostgreSQL、SQLite、CSV、JSON 等存储方式
- **词云生成**：基于 jieba 分词和 wordcloud 库，支持评论词云图生成

**主要依赖：** `playwright`, `httpx`, `pandas`, `jieba`, `wordcloud`, `sqlalchemy`, `pydantic`

**启动方式：**
```bash
# 设置环境变量后运行，或交互输入
python main.py --platform bili --lt qrcode --type search
```

---

### 二、后端模块 (`backend/`)

部署于云服务器，包含流处理管道和 REST API 两个子系统。

#### 2.1 Spark 流处理管道 (`spark/stream_single_topic_to_delta.py`)

**技术栈：** PySpark 3.5 + Delta Lake 3.3 + Kafka + ModelScope StructBERT

**数据流程（Bronze → Silver → Gold）：**

| 层级 | 路径 | 内容 |
|------|------|------|
| Bronze | `bronze_comments_raw` | Kafka 原始消息 + 情感分析结果，按日期分区，仅追加 |
| Silver | `silver_comments_enriched` | 按 `event_id` 去重的精炼评论，Merge 写入 |
| Gold Hourly | `gold_hourly_metrics` | 按话题+小时聚合：评论数、负面数、负面占比、热度 |
| Gold Daily | `gold_daily_metrics` | 按话题+日期聚合：同上 |
| Gold Top10 | `gold_daily_top10_comments` | 每话题每日按热度排名的 Top10 评论 |
| Alert Events | `gold_alert_events` | 已触发的预警事件记录 |

**情感分析（`structbert_sentiment.py`）：**
- 模型：`iic/nlp_structbert_sentiment-classification_chinese-base`（阿里 ModelScope）
- 输出：`positive` / `negative` / `neutral`（置信度 < 0.72 时归为中性）
- 批量推理：默认 batch_size=32，支持 fallback 单条推理

**热度分数公式：**
```
heat_score = 1.0
           + like_count × 0.6
           + repost_count × 1.0
           + sentiment_bonus  # 负向:+1.5 / 中性:+0.5 / 正向:+0.2
```

**风险等级：**
- `high`：负向 且 热度 ≥ 12
- `medium`：负向 且 热度 ≥ 5
- `low`：其他

**告警逻辑（`alert_mailer.py`）：**
- 触发条件：某话题某小时内评论数 > 10 且负面占比 > 40%
- 去重机制：同一 `topic + stat_hour` 只告警一次
- 通知内容：话题、统计小时、评论数、负面数、负面占比、Top3 负面评论样例
- 发送方式：SMTP 邮件（兼容 QQ 邮箱等）

**启动命令：**
```bash
bash /opt/pipeline/scripts/run_stream_single_topic.sh
```

Spark-submit 参数包含：
- `--packages io.delta:delta-spark_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8`
- `--master local[*]`，driver/executor 各 2g 内存

---

#### 2.2 REST API 服务 (`api/app.py`)

**技术栈：** FastAPI + `deltalake`（Python Delta Lake 读取库）+ pandas

**监听端口：** `18000`（由 `scripts/run_api.sh` 启动，`uvicorn` 绑定 `0.0.0.0:18000`）

**API 端点列表：**

| 端点 | 说明 |
|------|------|
| `GET /api/health` | 健康检查，返回各 Delta 表版本和存在状态 |
| `GET /api/topics` | 获取全部话题列表（按评论总量排序） |
| `GET /api/meta` | 获取指定话题元信息（数据范围、表版本等） |
| `GET /api/summary` | 获取话题汇总指标（总评论数、总热度、最新负面占比、告警数） |
| `GET /api/hourly` | 获取按小时粒度的趋势数据（默认最近 720 小时） |
| `GET /api/daily` | 获取按天粒度的趋势数据（默认最近 365 天） |
| `GET /api/comments` | 获取评论明细（按天 Top30 或按小时全量） |
| `GET /api/alerts` | 获取告警事件记录（最近 50 条） |
| `GET /api/dashboard` | 聚合接口，一次返回 meta + summary + daily + hourly + alerts |

**启动命令：**
```bash
bash /opt/pipeline/scripts/run_api.sh
```

---

### 三、前端模块 (`frontend/`)

**技术栈：** Vue 3 + Vite + ECharts + 原生 CSS（Glassmorphism 风格）

**核心功能页面（单页应用）：**

**顶部控制栏：**
- 话题选择下拉框（自动加载所有可用话题）
- 分析粒度切换（按天 / 按小时）
- 时间区间选择（日期 / 小时桶）
- 刷新数据按钮

**KPI 指标卡（6 个）：**
- 当前话题、累计评论数、累计热度、最新日评论数、最新小时负面占比、累计告警数

**趋势图表区（ECharts）：**
- 评论数与负面评论数折线图（双系列）
- 负面占比趋势折线图
- 热度趋势折线图（全宽展示）
- 所有图表支持 DataZoom 缩放与拖拽

**评论明细表：**
- 按天模式：当日 Top30（按热度降序）
- 按小时模式：该小时全量评论（最多 2000 条）
- 展示字段：发布时间、情绪标签（正向/负向/中性）、情绪分数、热度、点赞数、转发数、评论内容

**告警记录面板：**
- 展示当前话题最近触发的预警事件
- 每条告警含：触发时间、统计小时、评论数、负面占比、Top3 负面评论样例

**本地开发：**
```bash
cd frontend
npm install
npm run dev    # 默认访问 http://localhost:5173
```

API 地址通过 `.env.development` 中的 `VITE_API_BASE` 配置（默认 `http://127.0.0.1:18000`）。

---

## 核心技术栈

| 层次 | 技术 | 版本 |
|------|------|------|
| 数据采集 | Python + Playwright (CDP) + httpx | Python 3.12 |
| 消息队列 | Apache Kafka | 3.x |
| 流计算引擎 | Apache Spark Structured Streaming | 3.5.x |
| Delta Lake | delta-spark + delta-kernel | 3.3.2 |
| 情感分析 | StructBERT（阿里 ModelScope 中文预训练）| `iic/nlp_structbert_sentiment-classification_chinese-base` |
| 深度学习 | PyTorch + ModelScope | — |
| 后端 API | FastAPI + uvicorn + deltalake + pandas | Python 3.12 |
| 前端框架 | Vue 3 + Vite | Vue 3.5 / Vite 6 |
| 数据可视化 | Apache ECharts | 5.x |
| 告警通知 | Python smtplib（SMTP over TLS）| — |
| 部署环境 | Linux 云服务器 `/opt/pipeline/` | Alibaba Cloud ECS |

---

## 数据模型

### Kafka 消息 Schema

```json
{
  "event_id":     "唯一事件 ID（用于 Silver 层去重）",
  "platform":     "来源平台（bili / wb / xhs / dy / zhihu / ks / tieba）",
  "topic":        "话题关键词",
  "publish_time": "发布时间（yyyy-MM-dd HH:mm:ss）",
  "text":         "评论正文",
  "like_count":   "点赞数（整型）",
  "repost_count": "转发数（整型）",
  "ingest_time":  "入库时间戳",
  "source_file":  "来源 JSONL 文件路径"
}
```

### Delta Lake 三层数据模型

| 层级 | 表名 | 写入模式 | 内容 |
|------|------|----------|------|
| **Bronze** | `bronze_comments_raw` | Append（按日期分区）| Kafka 原始消息 + StructBERT 情感推理结果，保留全量历史 |
| **Silver** | `silver_comments_enriched` | Merge（按 `event_id` 去重）| 精炼评论，新增 `heat_score` / `risk_level` / `event_hour` / `event_date` |
| **Gold Hourly** | `gold_hourly_metrics` | Overwrite | 按 `topic + event_hour` 聚合：评论数、负面数、负面占比、平均热度 |
| **Gold Daily** | `gold_daily_metrics` | Overwrite | 按 `topic + event_date` 聚合：同上 |
| **Gold Top10** | `gold_daily_top10_comments` | Overwrite | 每话题每日热度 Top10 评论 |
| **Alert Events** | `gold_alert_events` | Append（Merge 去重）| 已触发的小时级负面舆情预警记录 |

### Silver 层新增字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `sentiment_label` | string | `positive` / `negative` / `neutral` |
| `sentiment_score` | float | 正负概率差值，范围 `[-1, 1]` |
| `sentiment_confidence` | float | 模型置信度；< 0.72 时强制归为 `neutral` |
| `heat_score` | float | `1.0 + like×0.6 + repost×1.0 + 情感加成`（负:+1.5 / 中:+0.5 / 正:+0.2）|
| `risk_level` | string | `high`（负向且热度≥12）/ `medium`（负向且热度≥5）/ `low` |
| `event_hour` | string | `yyyy-MM-dd HH:00:00`，用于小时聚合 |
| `event_date` | string | `yyyy-MM-dd`，用于日聚合 |

---

## 部署说明

### 云服务器端（Kafka + Spark + API）

1. 确保 Kafka 服务运行，topic `public_opinion_raw` 已创建
2. 配置 `/opt/pipeline/conf/alert.env`（SMTP 邮件、告警阈值等环境变量）
3. 启动 Spark 流任务：
   ```bash
   bash /opt/pipeline/scripts/run_stream_single_topic.sh
   ```
4. 启动 API 服务：
   ```bash
   bash /opt/pipeline/scripts/run_api.sh
   ```

### 本地爬虫端（Windows）

```powershell
cd crawler
pip install -r requirements.txt
playwright install chromium
python main.py
# 按提示输入关键词、轮询间隔、JSONL 输出目录
```

### 本地前端（Windows）

```powershell
cd frontend
npm install
npm run dev
```

---

## 关键配置项

| 配置项 | 位置 | 说明 |
|--------|------|------|
| `KAFKA_BOOTSTRAP` | `spark/stream_single_topic_to_delta.py` | Kafka 地址，默认 `172.16.156.151:9092` |
| `KAFKA_TOPIC` | `spark/stream_single_topic_to_delta.py` | 消费 Topic，默认 `public_opinion_raw` |
| `ALERT_NEG_RATIO_THRESHOLD` | `alert.env` | 告警负面占比阈值，默认 0.40 |
| `ALERT_MIN_COMMENTS` | `alert.env` | 告警最少评论数，默认 10 |
| `STRUCTBERT_NEUTRAL_THRESHOLD` | `alert.env` | 中性判定置信度阈值，默认 0.72 |
| `VITE_API_BASE` | `frontend/.env.development` | 前端 API 地址，默认 `http://127.0.0.1:18000` |
| `PLATFORM` | `crawler/config/base_config.py` | 爬取平台选择 |
| `SAVE_DATA_OPTION` | 同上 | 数据存储方式（postgresql / csv / json 等） |

---

## License

本仓库代码以 [MIT License](LICENSE) 开源。

`crawler/` 目录基于 [MediaCrawler](https://github.com/NanGao0802/MediaCrawler) 二次开发，遵循其原始 License。

---

## 论文引用

本项目为以下毕业设计论文的配套代码：

> 高楠. 基于 Structured Streaming 的实时舆情分析与热度趋势监测系统设计 [D].  
> 天津：天津工业大学，2026.
