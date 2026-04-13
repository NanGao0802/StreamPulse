from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import math

import pandas as pd
from deltalake import DeltaTable
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

BASE_PATH = "/data/public-opinion/delta/single_topic_comments"
SILVER_PATH = f"{BASE_PATH}/silver_comments_enriched"
GOLD_HOURLY_PATH = f"{BASE_PATH}/gold_hourly_metrics"
GOLD_DAILY_PATH = f"{BASE_PATH}/gold_daily_metrics"
GOLD_TOP10_PATH = f"{BASE_PATH}/gold_daily_top10_comments"
ALERT_PATH = f"{BASE_PATH}/gold_alert_events"

app = FastAPI(
    title="Multi Topic Public Opinion API",
    version="3.0.0",
    description="多话题舆情监测系统 API"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def path_exists(path: str) -> bool:
    return Path(path).exists()


def get_table_version(path: str) -> Optional[int]:
    if not path_exists(path):
        return None
    dt = DeltaTable(path)
    return dt.version()


def read_delta_as_pandas(path: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
    if not path_exists(path):
        return pd.DataFrame(columns=columns or [])
    dt = DeltaTable(path)
    return dt.to_pandas(columns=columns)


def normalize_value(v: Any) -> Any:
    if v is None:
        return None

    if isinstance(v, (list, dict)):
        return v

    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")

    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            pass

    if isinstance(v, float):
        if math.isnan(v):
            return None
        return float(v)

    return v


def df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    records = df.to_dict(orient="records")
    normalized = []
    for row in records:
        normalized.append({k: normalize_value(v) for k, v in row.items()})
    return normalized


def get_topics_df() -> pd.DataFrame:
    df = read_delta_as_pandas(GOLD_DAILY_PATH)
    if df.empty or "topic" not in df.columns:
        return pd.DataFrame(columns=["topic", "total_comment_count", "total_heat_sum", "latest_date"])

    df = df.copy()
    df["stat_date"] = pd.to_datetime(df["stat_date"])

    agg = (
        df.groupby("topic", dropna=True)
        .agg(
            total_comment_count=("comment_count", "sum"),
            total_heat_sum=("heat_sum", "sum"),
            latest_date=("stat_date", "max"),
        )
        .reset_index()
        .sort_values(["total_comment_count", "latest_date", "topic"], ascending=[False, False, True])
    )
    return agg


def resolve_topic(topic: Optional[str]) -> Optional[str]:
    topics_df = get_topics_df()
    if topics_df.empty:
        return None

    topic_list = topics_df["topic"].astype(str).tolist()
    if topic and topic in topic_list:
        return topic
    return topic_list[0]


def filter_by_topic(df: pd.DataFrame, topic: Optional[str]) -> pd.DataFrame:
    if df.empty or "topic" not in df.columns:
        return df
    if not topic:
        return df.iloc[0:0].copy()
    return df[df["topic"] == topic].copy()


def decode_alert_comments(value: Any) -> Any:
    if value is None or value == "":
        return []
    try:
        return json.loads(value)
    except Exception:
        return []


def _topics() -> Dict[str, Any]:
    topics_df = get_topics_df().copy()
    if not topics_df.empty:
        topics_df["latest_date"] = pd.to_datetime(topics_df["latest_date"]).dt.strftime("%Y-%m-%d")
    data = df_to_records(topics_df)
    return {"rows": len(data), "data": data}


def _alerts(topic: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
    selected_topic = resolve_topic(topic)
    df = read_delta_as_pandas(ALERT_PATH)

    if df.empty:
        return {"topic": selected_topic, "rows": 0, "data": []}

    if "topic" in df.columns and selected_topic:
        df = df[df["topic"] == selected_topic].copy()

    if df.empty:
        return {"topic": selected_topic, "rows": 0, "data": []}

    if "top_negative_comments_json" in df.columns:
        df["top_negative_comments"] = df["top_negative_comments_json"].apply(decode_alert_comments)

    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"])
        df = df.sort_values("created_at", ascending=False)

    df = df.head(limit)

    return {"topic": selected_topic, "rows": len(df), "data": df_to_records(df)}


def _meta(topic: Optional[str] = None) -> Dict[str, Any]:
    selected_topic = resolve_topic(topic)

    hourly_df = read_delta_as_pandas(GOLD_HOURLY_PATH)
    daily_df = read_delta_as_pandas(GOLD_DAILY_PATH)
    topics_data = _topics()["data"]

    hourly_df = filter_by_topic(hourly_df, selected_topic)
    daily_df = filter_by_topic(daily_df, selected_topic)

    hourly_min = hourly_max = None
    if not hourly_df.empty and "stat_hour" in hourly_df.columns:
        hourly_df["stat_hour"] = pd.to_datetime(hourly_df["stat_hour"])
        hourly_min = hourly_df["stat_hour"].min().strftime("%Y-%m-%d %H:%M:%S")
        hourly_max = hourly_df["stat_hour"].max().strftime("%Y-%m-%d %H:%M:%S")

    daily_min = daily_max = None
    if not daily_df.empty and "stat_date" in daily_df.columns:
        daily_df["stat_date"] = pd.to_datetime(daily_df["stat_date"])
        daily_min = daily_df["stat_date"].min().strftime("%Y-%m-%d")
        daily_max = daily_df["stat_date"].max().strftime("%Y-%m-%d")

    return {
        "topic": selected_topic,
        "available_topics": topics_data,
        "topic_count": len(topics_data),
        "hourly_version": get_table_version(GOLD_HOURLY_PATH),
        "daily_version": get_table_version(GOLD_DAILY_PATH),
        "top10_version": get_table_version(GOLD_TOP10_PATH),
        "alert_version": get_table_version(ALERT_PATH),
        "hourly_range": {"min": hourly_min, "max": hourly_max},
        "daily_range": {"min": daily_min, "max": daily_max},
    }


def _summary(topic: Optional[str] = None) -> Dict[str, Any]:
    selected_topic = resolve_topic(topic)
    if not selected_topic:
        return {
            "topic": None,
            "latest_date": None,
            "latest_hour": None,
            "total_comment_count": 0,
            "total_heat_sum": 0,
            "latest_daily_negative_ratio": 0,
            "latest_hourly_negative_ratio": 0,
            "latest_daily_comment_count": 0,
            "latest_hourly_comment_count": 0,
            "alert_count": 0,
        }

    hourly_df = filter_by_topic(read_delta_as_pandas(GOLD_HOURLY_PATH), selected_topic)
    daily_df = filter_by_topic(read_delta_as_pandas(GOLD_DAILY_PATH), selected_topic)
    alerts_df = filter_by_topic(read_delta_as_pandas(ALERT_PATH), selected_topic)

    if not daily_df.empty:
        daily_df["stat_date"] = pd.to_datetime(daily_df["stat_date"])
        daily_df = daily_df.sort_values("stat_date")

    if not hourly_df.empty:
        hourly_df["stat_hour"] = pd.to_datetime(hourly_df["stat_hour"])
        hourly_df = hourly_df.sort_values("stat_hour")

    if daily_df.empty:
        return {
            "topic": selected_topic,
            "latest_date": None,
            "latest_hour": None,
            "total_comment_count": 0,
            "total_heat_sum": 0,
            "latest_daily_negative_ratio": 0,
            "latest_hourly_negative_ratio": 0,
            "latest_daily_comment_count": 0,
            "latest_hourly_comment_count": 0,
            "alert_count": 0,
        }

    latest_day = daily_df.iloc[-1].copy()
    latest_hour = hourly_df.iloc[-1].copy() if not hourly_df.empty else None

    total_comment_count = int(daily_df["comment_count"].sum()) if "comment_count" in daily_df.columns else 0
    total_heat_sum = float(daily_df["heat_sum"].sum()) if "heat_sum" in daily_df.columns else 0.0
    alert_count = len(alerts_df) if not alerts_df.empty else 0

    return {
        "topic": selected_topic,
        "latest_date": normalize_value(latest_day.get("stat_date")),
        "latest_hour": normalize_value(latest_hour.get("stat_hour")) if latest_hour is not None else None,
        "total_comment_count": total_comment_count,
        "total_heat_sum": round(total_heat_sum, 2),
        "latest_daily_negative_ratio": float(latest_day.get("negative_ratio", 0) or 0),
        "latest_hourly_negative_ratio": float(latest_hour.get("negative_ratio", 0) or 0) if latest_hour is not None else 0,
        "latest_daily_comment_count": int(latest_day.get("comment_count", 0) or 0),
        "latest_hourly_comment_count": int(latest_hour.get("comment_count", 0) or 0) if latest_hour is not None else 0,
        "alert_count": alert_count,
    }


def _hourly(topic: Optional[str] = None, limit: int = 720) -> Dict[str, Any]:
    selected_topic = resolve_topic(topic)
    df = filter_by_topic(read_delta_as_pandas(GOLD_HOURLY_PATH), selected_topic)

    if not df.empty:
        df["stat_hour"] = pd.to_datetime(df["stat_hour"])
        df = df.sort_values("stat_hour").tail(limit)

    return {"topic": selected_topic, "rows": len(df), "data": df_to_records(df)}


def _daily(topic: Optional[str] = None, limit: int = 365) -> Dict[str, Any]:
    selected_topic = resolve_topic(topic)
    df = filter_by_topic(read_delta_as_pandas(GOLD_DAILY_PATH), selected_topic)

    if not df.empty:
        df["stat_date"] = pd.to_datetime(df["stat_date"])
        df = df.sort_values("stat_date").tail(limit)

    return {"topic": selected_topic, "rows": len(df), "data": df_to_records(df)}


def _comments(
    topic: Optional[str] = None,
    view: str = "daily",
    date: Optional[str] = None,
    hour: Optional[str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    selected_topic = resolve_topic(topic)
    df = filter_by_topic(read_delta_as_pandas(SILVER_PATH), selected_topic)

    if df.empty:
        return {
            "topic": selected_topic,
            "view": view,
            "selected_bucket": None,
            "bucket_options": [],
            "rows": 0,
            "data": [],
        }

    df = df.copy()

    if "publish_ts" in df.columns:
        df["publish_ts"] = pd.to_datetime(df["publish_ts"])
    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"]).dt.strftime("%Y-%m-%d")
    if "event_hour" in df.columns:
        df["event_hour"] = pd.to_datetime(df["event_hour"]).dt.strftime("%Y-%m-%d %H:00:00")

    cols = [c for c in [
        "event_id",
        "topic",
        "publish_time",
        "text",
        "sentiment_label",
        "sentiment_score",
        "sentiment_confidence",
        "sentiment_source",
        "heat_score",
        "risk_level",
        "like_count",
        "repost_count",
    ] if c in df.columns]

    if view == "daily":
        bucket_options = sorted(df["event_date"].dropna().unique().tolist(), reverse=True)
        selected_bucket = date if date in bucket_options else (bucket_options[0] if bucket_options else None)

        if not selected_bucket:
            return {
                "topic": selected_topic,
                "view": view,
                "selected_bucket": None,
                "bucket_options": bucket_options,
                "rows": 0,
                "data": [],
            }

        filtered = df[df["event_date"] == selected_bucket].copy()
        filtered = filtered.sort_values(
            ["heat_score", "like_count", "repost_count", "publish_time"],
            ascending=[False, False, False, False]
        )

        real_limit = 30 if limit is None else limit
        filtered = filtered.head(real_limit)

        return {
            "topic": selected_topic,
            "view": view,
            "selected_bucket": selected_bucket,
            "bucket_options": bucket_options,
            "rows": len(filtered),
            "data": df_to_records(filtered[cols]),
        }

    bucket_options = sorted(df["event_hour"].dropna().unique().tolist(), reverse=True)
    selected_bucket = hour if hour in bucket_options else (bucket_options[0] if bucket_options else None)

    if not selected_bucket:
        return {
            "topic": selected_topic,
            "view": view,
            "selected_bucket": None,
            "bucket_options": bucket_options,
            "rows": 0,
            "data": [],
        }

    filtered = df[df["event_hour"] == selected_bucket].copy()
    filtered = filtered.sort_values(
        ["publish_time", "heat_score", "like_count", "repost_count"],
        ascending=[False, False, False, False]
    )

    real_limit = 2000 if limit is None else limit
    filtered = filtered.head(real_limit)

    return {
        "topic": selected_topic,
        "view": view,
        "selected_bucket": selected_bucket,
        "bucket_options": bucket_options,
        "rows": len(filtered),
        "data": df_to_records(filtered[cols]),
    }


@app.get("/api/health")
def health() -> Dict[str, Any]:
    topics_info = _topics()
    return {
        "status": "ok",
        "silver_exists": path_exists(SILVER_PATH),
        "hourly_table_exists": path_exists(GOLD_HOURLY_PATH),
        "daily_table_exists": path_exists(GOLD_DAILY_PATH),
        "top10_table_exists": path_exists(GOLD_TOP10_PATH),
        "alert_table_exists": path_exists(ALERT_PATH),
        "hourly_version": get_table_version(GOLD_HOURLY_PATH),
        "daily_version": get_table_version(GOLD_DAILY_PATH),
        "top10_version": get_table_version(GOLD_TOP10_PATH),
        "alert_version": get_table_version(ALERT_PATH),
        "topic_count": topics_info["rows"],
        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


@app.get("/api/topics")
def topics() -> Dict[str, Any]:
    return _topics()


@app.get("/api/meta")
def meta(topic: Optional[str] = Query(None)) -> Dict[str, Any]:
    return _meta(topic=topic)


@app.get("/api/summary")
def summary(topic: Optional[str] = Query(None)) -> Dict[str, Any]:
    return _summary(topic=topic)


@app.get("/api/hourly")
def hourly(
    topic: Optional[str] = Query(None),
    limit: int = Query(720, ge=1, le=5000),
) -> Dict[str, Any]:
    return _hourly(topic=topic, limit=limit)


@app.get("/api/daily")
def daily(
    topic: Optional[str] = Query(None),
    limit: int = Query(365, ge=1, le=5000),
) -> Dict[str, Any]:
    return _daily(topic=topic, limit=limit)


@app.get("/api/comments")
def comments(
    topic: Optional[str] = Query(None),
    view: str = Query("daily"),
    date: Optional[str] = Query(None),
    hour: Optional[str] = Query(None),
    limit: Optional[int] = Query(None),
) -> Dict[str, Any]:
    return _comments(topic=topic, view=view, date=date, hour=hour, limit=limit)


@app.get("/api/alerts")
def alerts(
    topic: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
) -> Dict[str, Any]:
    return _alerts(topic=topic, limit=limit)


@app.get("/api/dashboard")
def dashboard(
    topic: Optional[str] = Query(None),
    daily_limit: int = Query(365, ge=1, le=3650),
    hourly_limit: int = Query(720, ge=1, le=5000),
    alerts_limit: int = Query(20, ge=1, le=200),
) -> Dict[str, Any]:
    selected_topic = resolve_topic(topic)
    return {
        "meta": _meta(topic=selected_topic),
        "summary": _summary(topic=selected_topic),
        "daily": _daily(topic=selected_topic, limit=daily_limit),
        "hourly": _hourly(topic=selected_topic, limit=hourly_limit),
        "alerts": _alerts(topic=selected_topic, limit=alerts_limit),
    }
