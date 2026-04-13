# 第四层：数据服务层（backend/api/）

> 对应架构层级：**数据服务层**  
> 核心技术：FastAPI · uvicorn · deltalake (Python) · pandas  
> 核心文件：`backend/api/app.py`

---

## 待学习内容清单

- [x] FastAPI 与 Flask/Django 的区别，为什么选 FastAPI
- [x] `deltalake` Python库如何直接读取 Delta 文件（不需要启动Spark）
- [x] `DeltaTable.to_pandas()` 的工作原理
- [x] `normalize_value()` 函数解决了什么问题（NaN/Timestamp序列化）
- [x] `resolve_topic()` 的逻辑：未指定话题时默认返回什么
- [x] CORS 中间件配置的作用
- [x] `/api/dashboard` 聚合接口如何减少前端请求次数
- [x] `filter_by_topic()` 为什么在 Python 层过滤而不是 Delta 查询层
- [x] 各端点的参数和响应格式

---

## 服务配置

| 项目 | 值 |
|------|----|
| 监听端口 | `18000` |
| 绑定地址 | `0.0.0.0`（允许外部访问） |
| 启动命令 | `bash /opt/pipeline/scripts/run_api.sh` |
| CORS | 允许所有来源（`allow_origins=["*"]`） |

---

## 9个 API 端点

### 基础端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/health` | GET | 健康检查，返回各表版本和存在状态 |
| `/api/topics` | GET | 获取全部话题列表（按评论总量降序） |

### 话题相关端点（均支持 `?topic=xxx` 参数）

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/meta` | GET | 话题元信息：数据范围、表版本 |
| `/api/summary` | GET | 汇总指标：总评论数、总热度、最新负面占比、告警数 |
| `/api/hourly` | GET | 按小时粒度趋势（默认最近720小时） |
| `/api/daily` | GET | 按天粒度趋势（默认最近365天） |
| `/api/comments` | GET | 评论明细（按天Top30或按小时全量最多2000条） |
| `/api/alerts` | GET | 告警记录（最近50条） |
| `/api/dashboard` | GET | **聚合接口**，一次返回 meta+summary+daily+hourly+alerts |

---

## 关键代码片段

### Delta Lake 读取

```python
def read_delta_as_pandas(path: str, columns=None) -> pd.DataFrame:
    if not path_exists(path):
        return pd.DataFrame(columns=columns or [])
    dt = DeltaTable(path)
    return dt.to_pandas(columns=columns)
```

### 数据类型序列化修复

```python
def normalize_value(v):
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, float) and math.isnan(v):
        return None
    if hasattr(v, "item"):      # numpy标量转Python原生类型
        return v.item()
    return v
```

### 话题解析逻辑

```python
def resolve_topic(topic):
    topics_df = get_topics_df()
    topic_list = topics_df["topic"].tolist()
    if topic and topic in topic_list:
        return topic
    return topic_list[0]        # 未指定则返回评论最多的话题
```

---

## `/api/comments` 端点参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `topic` | string | 话题名称 |
| `view` | string | `daily`（按天）或 `hourly`（按小时） |
| `date` | string | 按天时指定日期，格式 `yyyy-MM-dd` |
| `hour` | string | 按小时时指定小时，格式 `yyyy-MM-dd HH:00:00` |
| `limit` | int | 返回条数上限（daily默认30，hourly默认2000） |

---

## `/api/dashboard` 响应结构

```json
{
  "meta": {
    "topic": "话题名",
    "available_topics": [...],
    "hourly_range": {"min": "...", "max": "..."},
    "daily_range": {"min": "...", "max": "..."}
  },
  "summary": {
    "total_comment_count": 1234,
    "total_heat_sum": 5678.9,
    "latest_daily_negative_ratio": 0.32,
    "alert_count": 3
  },
  "daily": { "rows": 30, "data": [...] },
  "hourly": { "rows": 720, "data": [...] },
  "alerts": { "rows": 5, "data": [...] }
}
```

---

## 路径常量

```python
BASE_PATH    = "/data/public-opinion/delta/single_topic_comments"
SILVER_PATH  = f"{BASE_PATH}/silver_comments_enriched"
GOLD_HOURLY  = f"{BASE_PATH}/gold_hourly_metrics"
GOLD_DAILY   = f"{BASE_PATH}/gold_daily_metrics"
GOLD_TOP10   = f"{BASE_PATH}/gold_daily_top10_comments"
ALERT_PATH   = f"{BASE_PATH}/gold_alert_events"
```

---

## 1. 为什么选 FastAPI

| | Flask | Django | FastAPI |
|-|-------|--------|---------|
| 异步支持 | 需插件 | 有限 | ✓ 原生 async/await |
| 自动文档 | 需插件 | 需插件 | ✓ 内置 Swagger UI (`/docs`) |
| 类型提示 | ✗ | ✗ | ✓ Pydantic 自动校验 |
| 性能 | 中 | 低 | 高（基于 Starlette） |
| 启动速度 | 快 | 慢 | 快 |

本项目选 FastAPI 的核心原因：**内置 Swagger UI**（可在浏览器直接测试所有端点，答辩演示方便），以及轻量、启动无需 JVM。

---

## 2. 应用初始化与 CORS

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="StreamPulse API", version="1.0.0")

# 跨域配置：允许前端（localhost:5173）访问后端（localhost:18000）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # 允许所有来源（生产环境可限制为前端域名）
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

**CORS 是什么？** 浏览器的同源策略禁止跨域请求。前端运行在 `localhost:5173`，后端在 `localhost:18000`，端口不同即跨域。CORS 中间件通过设置响应头 `Access-Control-Allow-Origin: *` 告诉浏览器"允许跨域"。

---

## 3. 核心工具函数

### path_exists() — 判断 Delta 表是否存在

```python
def path_exists(path: str) -> bool:
    return os.path.exists(path) and os.path.isdir(path)
```

Delta 表本质是目录，目录存在才能读取。每个端点在读表前都会先检查，表不存在时返回空数据而不是报错。

---

### read_delta_as_pandas() — 读取 Delta 表为 DataFrame

```python
def read_delta_as_pandas(path: str, columns=None) -> pd.DataFrame:
    if not path_exists(path):
        return pd.DataFrame(columns=columns or [])   # 表不存在返回空DataFrame
    dt = DeltaTable(path)                             # 打开Delta表
    return dt.to_pandas(columns=columns)             # 转为Pandas，columns可指定只读部分列
```

`DeltaTable(path)` 读取 `_delta_log/` 重建最新版本状态，`to_pandas()` 将有效 Parquet 文件读入内存。由于 Gold 表数据量小（几千行），整个过程通常 < 100ms。

---

### normalize_value() — 序列化安全转换

```python
import math

def normalize_value(v):
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d %H:%M:%S")   # Timestamp → 字符串
    if isinstance(v, float) and math.isnan(v):
        return None                                # NaN → JSON null
    if isinstance(v, float) and math.isinf(v):
        return None                                # Inf → JSON null
    if hasattr(v, "item"):
        return v.item()                            # numpy标量 → Python原生类型
    return v
```

**为什么必须做这个转换？**  
Pandas/NumPy 的数据类型（`numpy.int64`、`pd.Timestamp`、`float('nan')`）无法直接被 Python 标准库的 `json.dumps()` 序列化，FastAPI 返回响应时会报 `TypeError`。`normalize_value()` 统一做安全转换。

---

### get_table_version() — 获取 Delta 表版本号

```python
def get_table_version(path: str) -> int:
    if not path_exists(path):
        return -1
    try:
        return DeltaTable(path).version()   # _delta_log/ 中最新提交的版本号
    except Exception:
        return -1
```

用于 `/api/health` 和 `/api/meta` 端点，监控表是否有更新。

---

### resolve_topic() — 话题参数解析

```python
def resolve_topic(topic: str = None) -> str:
    topics_df = get_topics_df()       # 从 Gold 小时表聚合所有话题
    topic_list = topics_df["topic"].tolist()   # 按评论总量降序排列

    if topic and topic in topic_list:
        return topic                  # 有效话题名：直接返回
    return topic_list[0]              # 未指定或无效：返回评论最多的话题
```

这个逻辑决定了**前端首次加载时默认展示哪个话题**：评论最多的话题（即最热门的）。

---

### filter_by_topic() — 按话题过滤 DataFrame

```python
def filter_by_topic(df: pd.DataFrame, topic: str) -> pd.DataFrame:
    if "topic" not in df.columns or df.empty:
        return df
    return df[df["topic"] == topic]
```

为什么在 Python 层（Pandas）过滤而不是 Delta 查询层？因为 Gold 表数据量小（全部话题的聚合数据也只有几千行），读全表再用 Pandas 过滤比 Delta 谓词下推更简单且性能差异可忽略不计。

---

## 4. 九个端点详解

### GET /api/health — 健康检查

```python
@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "silver_exists":       path_exists(SILVER_PATH),
        "hourly_table_exists": path_exists(GOLD_HOURLY),
        "daily_table_exists":  path_exists(GOLD_DAILY),
        "hourly_version":      get_table_version(GOLD_HOURLY),
        "daily_version":       get_table_version(GOLD_DAILY),
        "topic_count":         len(get_topics_df()),
        "server_time":         datetime.now().strftime("..."),
    }
```

**用途：** 部署后第一个访问的端点，确认各表均存在、Spark 任务已写入过数据。

---

### GET /api/topics — 话题列表

从 Gold 小时表聚合所有话题，按累计评论总数降序返回：

```python
@app.get("/api/topics")
def topics():
    df = read_delta_as_pandas(GOLD_HOURLY)
    topics_df = (
        df.groupby("topic")
          .agg(
              total_comment_count=("comment_count", "sum"),
              total_heat_sum=("heat_sum", "sum"),
              latest_date=("stat_hour", "max"),
          )
          .sort_values("total_comment_count", ascending=False)
          .reset_index()
    )
    return {"rows": len(topics_df), "data": df_to_records(topics_df)}
```

---

### GET /api/summary — KPI 汇总指标

前端 6 个 KPI 卡的数据来源：

| 返回字段 | 计算逻辑 |
|---------|---------|
| `total_comment_count` | Gold日表该话题 `comment_count` 求和 |
| `total_heat_sum` | Gold日表该话题 `heat_sum` 求和 |
| `latest_daily_negative_ratio` | Gold日表该话题最新一天的 `negative_ratio` |
| `latest_hourly_negative_ratio` | Gold小时表该话题最新一小时的 `negative_ratio` |
| `latest_daily_comment_count` | Gold日表最新一天的 `comment_count` |
| `latest_hourly_comment_count` | Gold小时表最新一小时的 `comment_count` |
| `alert_count` | 告警表该话题的记录总数 |

---

### GET /api/comments — 评论明细（两种视图）

```python
@app.get("/api/comments")
def comments(
    topic: str = None,
    view: str = "daily",      # "daily" 或 "hourly"
    date: str = None,         # view=daily 时：指定日期 yyyy-MM-dd
    hour: str = None,         # view=hourly 时：指定小时 yyyy-MM-dd HH:00:00
    limit: int = None,
):
    df = read_delta_as_pandas(GOLD_TOP10)   # 读 Top10 表（按天视图）
    # 或
    df = read_delta_as_pandas(SILVER_PATH)  # 读 Silver 表（按小时视图）
    ...
```

- **`view=daily`**：从 `gold_daily_top10_comments` 读当天 Top30，按 `heat_score DESC`
- **`view=hourly`**：从 `silver_comments_enriched` 读该小时全量，按 `publish_time DESC`，最多2000条

**`decode_alert_comments()` 函数：**  
告警表的 `top_negative_comments_json` 字段存储 JSON 字符串，API 返回前用此函数解析为对象列表。

---

### GET /api/dashboard — 聚合接口（前端核心接口）

```python
@app.get("/api/dashboard")
def dashboard(
    topic: str = None,
    daily_limit: int = 365,
    hourly_limit: int = 720,
    alerts_limit: int = 20,
):
    resolved = resolve_topic(topic)
    return {
        "meta":    _build_meta(resolved),
        "summary": _build_summary(resolved),
        "daily":   _build_daily(resolved, daily_limit),
        "hourly":  _build_hourly(resolved, hourly_limit),
        "alerts":  _build_alerts(resolved, alerts_limit),
    }
```

**设计目的：** 前端初始化时只需发一个请求，拿到所有数据。否则需要分别调 `/meta`、`/summary`、`/daily`、`/hourly`、`/alerts` 共5个请求，增加网络延迟和前端复杂度。

---

## 5. 启动方式

```bash
# backend/scripts/run_api.sh
uvicorn api.app:app \
    --host 0.0.0.0 \
    --port 18000 \
    --reload          # 开发模式：代码变更自动重启
```

交互式文档：访问 `http://<server>:18000/docs` 可在浏览器中直接测试所有端点（答辩演示利器）。

---

## 6. 各端点响应示例

完整的响应格式和字段说明参见 `docs/03_api_reference.md`，以下是快速速查：

| 端点 | 响应顶层字段 |
|------|------------|
| `/api/health` | `status, silver_exists, hourly_version, topic_count, server_time` |
| `/api/topics` | `rows, data[]` |
| `/api/meta` | `topic, available_topics, hourly_version, hourly_range, daily_range` |
| `/api/summary` | `topic, total_comment_count, total_heat_sum, latest_daily_negative_ratio, alert_count` |
| `/api/hourly` | `topic, rows, data[{stat_hour, comment_count, negative_count, negative_ratio, heat_sum}]` |
| `/api/daily` | `topic, rows, data[{stat_date, comment_count, negative_count, negative_ratio, heat_sum}]` |
| `/api/comments` | `topic, view, selected_bucket, bucket_options, rows, data[{event_id, text, sentiment_label, heat_score, risk_level}]` |
| `/api/alerts` | `topic, rows, data[{alert_key, stat_hour, negative_ratio, email_status, top_negative_comments}]` |
| `/api/dashboard` | `{meta, summary, daily, hourly, alerts}` 合并体 |

---

## 学习笔记

<!-- 在这里补充你的学习理解 -->
