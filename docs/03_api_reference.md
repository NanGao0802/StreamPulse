# REST API 参考文档

API 由 FastAPI 提供，监听 `0.0.0.0:18000`，所有端点均为 `GET` 请求，返回 JSON。

交互式文档（Swagger UI）：`http://<server>:18000/docs`

---

## 通用说明

- 所有接受 `topic` 参数的端点：若不传，自动选择评论数最多的话题
- 时间字段统一格式：`yyyy-MM-dd HH:mm:ss`（datetime）或 `yyyy-MM-dd`（date）
- `NaN` 和 `null` 均序列化为 JSON `null`

---

## 端点列表

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/health` | 健康检查 |
| GET | `/api/topics` | 话题列表 |
| GET | `/api/meta` | 话题元信息 |
| GET | `/api/summary` | 话题汇总指标 |
| GET | `/api/hourly` | 小时粒度趋势 |
| GET | `/api/daily` | 天粒度趋势 |
| GET | `/api/comments` | 评论明细 |
| GET | `/api/alerts` | 告警事件 |
| GET | `/api/dashboard` | 聚合接口（一次返回全部） |

---

## GET `/api/health`

健康检查，返回各 Delta 表的存在状态与版本号。

**无参数**

**响应示例**

```json
{
  "status": "ok",
  "silver_exists": true,
  "hourly_table_exists": true,
  "daily_table_exists": true,
  "top10_table_exists": true,
  "alert_table_exists": true,
  "hourly_version": 42,
  "daily_version": 42,
  "top10_version": 42,
  "alert_version": 7,
  "topic_count": 3,
  "server_time": "2026-03-06 01:23:45"
}
```

---

## GET `/api/topics`

返回所有话题列表，按累计评论数降序。

**无参数**

**响应示例**

```json
{
  "rows": 3,
  "data": [
    {
      "topic": "26考研人数下降",
      "total_comment_count": 4821,
      "total_heat_sum": 9234.5,
      "latest_date": "2026-03-06"
    },
    {
      "topic": "全红婵的减肥计划精确到克",
      "total_comment_count": 3102,
      "total_heat_sum": 5891.2,
      "latest_date": "2026-03-05"
    }
  ]
}
```

---

## GET `/api/meta`

返回指定话题的元信息，包括数据时间范围与各表版本号。

**参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `topic` | string | 否 | 话题名称，缺省时自动选择评论最多的话题 |

**响应示例**

```json
{
  "topic": "26考研人数下降",
  "available_topics": [...],
  "topic_count": 3,
  "hourly_version": 42,
  "daily_version": 42,
  "top10_version": 42,
  "alert_version": 7,
  "hourly_range": {
    "min": "2026-03-01 08:00:00",
    "max": "2026-03-06 01:00:00"
  },
  "daily_range": {
    "min": "2026-03-01",
    "max": "2026-03-06"
  }
}
```

---

## GET `/api/summary`

返回指定话题的 KPI 汇总指标（前端 KPI 卡数据来源）。

**参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `topic` | string | 否 | 话题名称 |

**响应示例**

```json
{
  "topic": "26考研人数下降",
  "latest_date": "2026-03-06",
  "latest_hour": "2026-03-06 01:00:00",
  "total_comment_count": 4821,
  "total_heat_sum": 9234.5,
  "latest_daily_negative_ratio": 0.3821,
  "latest_hourly_negative_ratio": 0.4103,
  "latest_daily_comment_count": 312,
  "latest_hourly_comment_count": 18,
  "alert_count": 3
}
```

---

## GET `/api/hourly`

返回指定话题按小时粒度的趋势数据（用于折线图）。

**参数**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `topic` | string | 否 | — | 话题名称 |
| `limit` | int | 否 | 720 | 返回最近 N 小时，范围 `[1, 5000]` |

**响应示例**

```json
{
  "topic": "26考研人数下降",
  "rows": 120,
  "data": [
    {
      "topic": "26考研人数下降",
      "stat_hour": "2026-03-01 08:00:00",
      "comment_count": 23,
      "like_count_sum": 156,
      "repost_count_sum": 12,
      "negative_count": 9,
      "heat_sum": 187.4,
      "heat_avg": 8.15,
      "negative_ratio": 0.3913
    }
  ]
}
```

---

## GET `/api/daily`

返回指定话题按天粒度的趋势数据。

**参数**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `topic` | string | 否 | — | 话题名称 |
| `limit` | int | 否 | 365 | 返回最近 N 天，范围 `[1, 5000]` |

**响应结构**同 `/api/hourly`，`stat_hour` 换为 `stat_date`（格式 `yyyy-MM-dd`）。

---

## GET `/api/comments`

返回指定话题的评论明细，支持按天/按小时两种视图。

**参数**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `topic` | string | 否 | — | 话题名称 |
| `view` | string | 否 | `"daily"` | `"daily"` 或 `"hourly"` |
| `date` | string | 否 | 最新日期 | `view=daily` 时指定日期，格式 `yyyy-MM-dd` |
| `hour` | string | 否 | 最新小时 | `view=hourly` 时指定小时，格式 `yyyy-MM-dd HH:00:00` |
| `limit` | int | 否 | 30(daily)/2000(hourly) | 返回条数上限 |

**说明**

- `view=daily`：返回该天热度 Top N 评论，按 `heat_score DESC` 排序
- `view=hourly`：返回该小时全量评论，按 `publish_time DESC` 排序

**响应示例**

```json
{
  "topic": "26考研人数下降",
  "view": "daily",
  "selected_bucket": "2026-03-06",
  "bucket_options": ["2026-03-06", "2026-03-05", "2026-03-04"],
  "rows": 30,
  "data": [
    {
      "event_id": "dy:7613792636118516522",
      "topic": "26考研人数下降",
      "publish_time": "2026-03-05 23:24:05",
      "text": "评论内容...",
      "sentiment_label": "negative",
      "sentiment_score": -0.8234,
      "sentiment_confidence": 0.9102,
      "sentiment_source": "structbert",
      "heat_score": 14.2,
      "risk_level": "high",
      "like_count": 21,
      "repost_count": 3
    }
  ]
}
```

---

## GET `/api/alerts`

返回指定话题的告警事件记录，按触发时间倒序。

**参数**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `topic` | string | 否 | — | 话题名称 |
| `limit` | int | 否 | 50 | 返回条数，范围 `[1, 500]` |

**响应示例**

```json
{
  "topic": "26考研人数下降",
  "rows": 2,
  "data": [
    {
      "alert_key": "26考研人数下降__2026-03-05 23:00:00",
      "topic": "26考研人数下降",
      "stat_hour": "2026-03-05 23:00:00",
      "comment_count": 18,
      "negative_count": 9,
      "negative_ratio": 0.5,
      "top_negative_comments": [
        {
          "publish_time": "2026-03-05 23:24:05",
          "heat_score": 14.2,
          "text": "负面评论内容..."
        }
      ],
      "email_status": "sent",
      "email_message": "ok",
      "created_at": "2026-03-05 23:01:03"
    }
  ]
}
```

---

## GET `/api/dashboard`

聚合接口，一次调用返回 `meta + summary + daily + hourly + alerts`，减少前端请求次数。

**参数**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `topic` | string | 否 | — | 话题名称 |
| `daily_limit` | int | 否 | 365 | 天粒度数据条数 |
| `hourly_limit` | int | 否 | 720 | 小时粒度数据条数 |
| `alerts_limit` | int | 否 | 20 | 告警记录条数 |

**响应结构**

```json
{
  "meta":    { /* /api/meta 响应 */ },
  "summary": { /* /api/summary 响应 */ },
  "daily":   { /* /api/daily 响应 */ },
  "hourly":  { /* /api/hourly 响应 */ },
  "alerts":  { /* /api/alerts 响应 */ }
}
```

前端初始化加载时调用此接口，后续用户切换话题/粒度时再按需调用单独端点。
