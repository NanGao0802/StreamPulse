# 基于 Structured Streaming 的实时舆情分析与热度趋势监测系统

> 毕业设计项目 · 面向社交媒体评论流的实时舆情监测平台

---

## 项目简介

本系统是一套端到端的**实时舆情分析与热度趋势监测**解决方案，以 Apache Kafka + Spark Structured Streaming 为核心流处理引擎，结合 StructBERT 中文情感分类模型、Delta Lake 数据湖架构，构建从社交媒体数据采集、实时流处理、情感分析、热度计算到可视化展示的完整链路。系统支持多话题同时监测、按天/按小时趋势分析、负面舆情自动告警与邮件通知。

---

## 系统架构

```
社交媒体平台
  (微博 / 抖音 / B站 / 小红书 / 知乎 / 快手 / 贴吧)
         │
         ▼
  ┌─────────────┐
  │   爬虫模块   │  轮询抓取评论，输出 JSONL 文件
  └──────┬──────┘
         │ JSONL
         ▼
  ┌─────────────┐
  │    Kafka    │  Topic: public_opinion_raw
  └──────┬──────┘
         │ 消息流
         ▼
  ┌────────────────────────────────────┐
  │   Spark Structured Streaming        │
  │   (60s micro-batch)                │
  │                                    │
  │   ┌──────────────────────────┐     │
  │   │  StructBERT 情感分类      │     │
  │   │  正向 / 中性 / 负向       │     │
  │   └──────────────────────────┘     │
  │                                    │
  │   热度分数 = 1.0                   │
  │           + 点赞数 × 0.6           │
  │           + 转发数 × 1.0           │
  │           + 情绪加成(负:1.5/中:0.5/正:0.2) │
  └──────┬─────────────────────────────┘
         │
         ▼
  ┌─────────────────────────────────────────┐
  │            Delta Lake 数据湖             │
  │                                         │
  │  Bronze  →  Silver  →  Gold (Hourly)    │
  │  (原始+全量)  (去重精炼)  (Gold Daily)   │
  │                          (Gold Top10)   │
  │                          (Alert Events) │
  └──────┬──────────────────────────────────┘
         │
         ▼
  ┌─────────────┐        ┌──────────────────┐
  │  FastAPI    │ ◄────► │  Vue 3 前端大屏   │
  │  REST API   │        │  ECharts 可视化   │
  └─────────────┘        └──────────────────┘
         │
         ▼
  ┌─────────────┐
  │  邮件告警    │  负面占比 > 40% 时自动触发
  └─────────────┘
```

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

## 核心技术栈总览

| 层次 | 技术 |
|------|------|
| 数据采集 | Python + Playwright (CDP) + httpx |
| 消息队列 | Apache Kafka |
| 流计算引擎 | Apache Spark 3.5 Structured Streaming |
| 情感分析模型 | StructBERT（ModelScope, 中文二分类） |
| 数据存储 | Delta Lake（Bronze / Silver / Gold 三层） |
| 后端 API | Python FastAPI + deltalake + pandas |
| 前端 | Vue 3 + Vite + ECharts |
| 告警通知 | SMTP 邮件（Python smtplib） |
| 部署环境 | Linux 云服务器（`/opt/pipeline/`） |

---

## 数据流关键字段说明

评论数据经爬虫采集后，以如下 JSON Schema 写入 Kafka：

```json
{
  "event_id":     "唯一事件 ID（用于去重）",
  "platform":     "来源平台（bili / wb / xhs 等）",
  "topic":        "话题关键词",
  "publish_time": "发布时间（yyyy-MM-dd HH:mm:ss）",
  "time_id":      "时间桶 ID",
  "text":         "评论正文",
  "like_count":   "点赞数",
  "repost_count": "转发数",
  "ingest_time":  "入库时间",
  "source_file":  "来源文件路径"
}
```

经 Spark 处理后，Silver 层额外新增字段：

```
sentiment_label      # positive / negative / neutral
sentiment_score      # 正负概率差值 [-1, 1]
sentiment_confidence # 模型置信度
heat_score           # 热度分数（自定义公式）
risk_level           # high / medium / low
event_hour           # 所属小时（用于小时聚合）
event_date           # 所属日期（用于日聚合）
```

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
