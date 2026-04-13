from datetime import datetime
from tools.time_util import get_time_str_from_unix_time

def make_time_id(publish_time: str) -> str:
    # 'YYYY-MM-DD HH:MM:SS' -> 'YYYYMMDDHHMM'
    try:
        dt = datetime.strptime(publish_time, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y%m%d%H%M")
    except Exception:
        digits = "".join(ch for ch in (publish_time or "") if ch.isdigit())
        return (digits[:12]).ljust(12, "0")

def dy_comment_to_event(comment_item: dict, topic: str = "") -> dict:
    # comment_item 是你 store/douyin 里保存的那种 dict
    publish_time = get_time_str_from_unix_time(comment_item.get("create_time", 0)) if comment_item.get("create_time") else ""
    return {
        "event_id": f"dy:{comment_item.get('comment_id', '')}",
        "platform": "dy",
        "topic": topic or "",
        "publish_time": publish_time,
        "time_id": make_time_id(publish_time),
        "text": comment_item.get("content", ""),
        "like_count": int(comment_item.get("like_count", 0) or 0),
        "repost_count": 0,
    }