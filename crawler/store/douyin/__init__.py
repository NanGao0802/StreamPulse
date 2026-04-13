# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。

# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2024/1/14 18:46
# @Desc    :
from typing import List
from tools.event_schema import dy_comment_to_event
import config
from var import source_keyword_var

from ._store_impl import *
from .douyin_store_media import *
from tools.time_util import get_time_str_from_unix_time
AWEME_TITLE_CACHE = {}
class DouyinStoreFactory:
    STORES = {
        "csv": DouyinCsvStoreImplement,
        "xlsx": DouyinCsvStoreImplement, 
        "db": DouyinDbStoreImplement,
        "json": DouyinJsonStoreImplement,
        "sqlite": DouyinSqliteStoreImplement,
        "postgresql": DouyinDbStoreImplement,
    }
    AWEME_TITLE_CACHE = {}
    @staticmethod
    def create_store() -> AbstractStore:
        store_class = DouyinStoreFactory.STORES.get(config.SAVE_DATA_OPTION)
        if not store_class:
            raise ValueError("[DouyinStoreFactory.create_store] Invalid save option only supported csv or db or json or sqlite or postgresql ...")
        return store_class()


def _extract_note_image_list(aweme_detail: Dict) -> List[str]:
    """
    提取笔记图片列表

    Args:
        aweme_detail (Dict): 抖音内容详情

    Returns:
        List[str]: 笔记图片列表
    """
    images_res: List[str] = []
    images: List[Dict] = aweme_detail.get("images", [])

    if not images:
        return []

    for image in images:
        image_url_list = image.get("url_list", [])  # download_url_list 为带水印的图片，url_list 为无水印的图片
        if image_url_list:
            images_res.append(image_url_list[-1])

    return images_res


def _extract_comment_image_list(comment_item: Dict) -> List[str]:
    """
    提取评论图片列表

    Args:
        comment_item (Dict): 抖音评论

    Returns:
        List[str]: 评论图片列表
    """
    images_res: List[str] = []
    image_list: List[Dict] = comment_item.get("image_list", [])

    if not image_list:
        return []

    for image in image_list:
        image_url_list = image.get("origin_url", {}).get("url_list", [])
        if image_url_list and len(image_url_list) > 1:
            images_res.append(image_url_list[1])

    return images_res


def _extract_content_cover_url(aweme_detail: Dict) -> str:
    """
    提取视频封面地址

    Args:
        aweme_detail (Dict): 抖音内容详情

    Returns:
        str: 视频封面地址
    """
    res_cover_url = ""

    video_item = aweme_detail.get("video", {})
    raw_cover_url_list = (video_item.get("raw_cover", {}) or video_item.get("origin_cover", {})).get("url_list", [])
    if raw_cover_url_list and len(raw_cover_url_list) > 1:
        res_cover_url = raw_cover_url_list[1]

    return res_cover_url


def _extract_video_download_url(aweme_detail: Dict) -> str:
    """
    提取视频下载地址

    Args:
        aweme_detail (Dict): 抖音视频

    Returns:
        str: 视频下载地址
    """
    video_item = aweme_detail.get("video", {})
    url_h264_list = video_item.get("play_addr_h264", {}).get("url_list", [])
    url_256_list = video_item.get("play_addr_256", {}).get("url_list", [])
    url_list = video_item.get("play_addr", {}).get("url_list", [])
    actual_url_list = url_h264_list or url_256_list or url_list
    if not actual_url_list or len(actual_url_list) < 2:
        return ""
    return actual_url_list[-1]


def _extract_music_download_url(aweme_detail: Dict) -> str:
    """
    提取音乐下载地址

    Args:
        aweme_detail (Dict): 抖音视频

    Returns:
        str: 音乐下载地址
    """
    music_item = aweme_detail.get("music", {})
    play_url = music_item.get("play_url", {})
    music_url = play_url.get("uri", "")
    return music_url


async def update_douyin_aweme(aweme_item: Dict):
    aweme_id = aweme_item.get("aweme_id")
    user_info = aweme_item.get("author", {})
    interact_info = aweme_item.get("statistics", {})
    save_content_item = {
        "aweme_id": aweme_id,
        "aweme_type": str(aweme_item.get("aweme_type")),
        "title": aweme_item.get("desc", ""),
        "desc": aweme_item.get("desc", ""),
        "create_time": aweme_item.get("create_time"),
        "user_id": user_info.get("uid"),
        "sec_uid": user_info.get("sec_uid"),
        "short_user_id": user_info.get("short_id"),
        "user_unique_id": user_info.get("unique_id"),
        "user_signature": user_info.get("signature"),
        "nickname": user_info.get("nickname"),
        "avatar": user_info.get("avatar_thumb", {}).get("url_list", [""])[0],
        "liked_count": str(interact_info.get("digg_count")),
        "collected_count": str(interact_info.get("collect_count")),
        "comment_count": str(interact_info.get("comment_count")),
        "share_count": str(interact_info.get("share_count")),
        "ip_location": aweme_item.get("ip_label", ""),
        "last_modify_ts": utils.get_current_timestamp(),
        "aweme_url": f"https://www.douyin.com/video/{aweme_id}",
        "cover_url": _extract_content_cover_url(aweme_item),
        "video_download_url": _extract_video_download_url(aweme_item),
        "music_download_url": _extract_music_download_url(aweme_item),
        "note_download_url": ",".join(_extract_note_image_list(aweme_item)),
        "source_keyword": source_keyword_var.get(),
    }
    AWEME_TITLE_CACHE[aweme_id] = save_content_item.get("title", "") or save_content_item.get("desc", "")
    utils.logger.info(f"[store.douyin.update_douyin_aweme] douyin aweme id:{aweme_id}, title:{save_content_item.get('title')}")
    await DouyinStoreFactory.create_store().store_content(content_item=save_content_item)


async def batch_update_dy_aweme_comments(aweme_id: str, comments: List[Dict]):
    if not comments:
        return
    for comment_item in comments:
        await update_dy_aweme_comment(aweme_id, comment_item)


async def update_dy_aweme_comment(aweme_id: str, comment_item: Dict):
    """
    抖音评论 -> 统一事件 schema（对齐小红书）：
    {
      event_id: "dy:<comment_id>",
      platform: "dy",
      topic: "<视频标题/desc 或关键词>",
      publish_time: "YYYY-MM-DD HH:MM:SS",
      time_id: "YYYYMMDDHHMM",
      text: "<评论内容>",
      like_count: <int>,
      repost_count: 0
    }

    同时：
    - 仍然通过 store_comment() 输出到 csv/xlsx（由 --save_data_option 控制）
    - 如配置了 config.KAFKA_JSON_DIR，可额外写入 JSONL 供 Kafka 消费（可选）
    """

    comment_aweme_id = comment_item.get("aweme_id")
    if aweme_id != comment_aweme_id:
        utils.logger.error(
            f"[store.douyin.update_dy_aweme_comment] comment_aweme_id: {comment_aweme_id} != aweme_id: {aweme_id}"
        )
        return

    # ====== 1) 取字段 ======
    comment_id = comment_item.get("cid")
    create_ts = comment_item.get("create_time") or 0
    text = comment_item.get("text") or ""
    like_count = comment_item.get("digg_count") or 0

    # ====== 2) topic：标题优先，其次关键词 ======
    topic = (source_keyword_var.get() or "").strip()

    # ====== 3) publish_time / time_id ======
    # create_ts 是 unix 秒时间戳
    publish_time = get_time_str_from_unix_time(create_ts) if create_ts else ""
    # time_id: YYYYMMDDHHMM
    digits = "".join(ch for ch in publish_time if ch.isdigit())
    time_id = (digits[:12]).ljust(12, "0")

    # ====== 4) 统一 event schema（对齐小红书） ======
    event_item = {
        "event_id": f"dy:{comment_id}",
        "platform": "dy",
        "topic": topic,
        "publish_time": publish_time,
        "time_id": time_id,
        "text": text,
        "like_count": int(like_count),
        "repost_count": 0,
    }

    # ====== 5) 输出到 csv/xlsx（你原有 store 体系） ======
    await DouyinStoreFactory.create_store().store_comment(comment_item=event_item)

    # ====== 6) （可选）额外输出 Kafka JSONL：dy_events.jsonl ======
    # 需要你已经实现 AsyncFileWriter.append_to_jsonl 并在 config 里配置 KAFKA_JSON_DIR
    kafka_dir = getattr(config, "KAFKA_JSON_DIR", "") or ""
    if kafka_dir:
        try:
            from tools.async_file_writer import AsyncFileWriter
            from var import crawler_type_var  # 你项目里用来区分 search/detail 的变量

            fw = AsyncFileWriter(platform="douyin", crawler_type=crawler_type_var.get())
            kafka_path = os.path.join(kafka_dir, "dy_events.jsonl")
            await fw.append_to_jsonl(event_item, kafka_path)
        except Exception as e:
            utils.logger.warning(f"[store.douyin.update_dy_aweme_comment] kafka jsonl write failed: {e}")

async def save_creator(user_id: str, creator: Dict):
    user_info = creator.get("user", {})
    gender_map = {0: "未知", 1: "男", 2: "女"}
    avatar_uri = user_info.get("avatar_300x300", {}).get("uri")
    local_db_item = {
        "user_id": user_id,
        "nickname": user_info.get("nickname"),
        "gender": gender_map.get(user_info.get("gender"), "未知"),
        "avatar": f"https://p3-pc.douyinpic.com/img/{avatar_uri}" + r"~c5_300x300.jpeg?from=2956013662",
        "desc": user_info.get("signature"),
        "ip_location": user_info.get("ip_location"),
        "follows": user_info.get("following_count", 0),
        "fans": user_info.get("max_follower_count", 0),
        "interaction": user_info.get("total_favorited", 0),
        "videos_count": user_info.get("aweme_count", 0),
        "last_modify_ts": utils.get_current_timestamp(),
    }
    utils.logger.info(f"[store.douyin.save_creator] creator:{local_db_item}")
    await DouyinStoreFactory.create_store().store_creator(local_db_item)


async def update_dy_aweme_image(aweme_id, pic_content, extension_file_name):
    """
    更新抖音笔记图片
    Args:
        aweme_id:
        pic_content:
        extension_file_name:

    Returns:

    """

    await DouYinImage().store_image({"aweme_id": aweme_id, "pic_content": pic_content, "extension_file_name": extension_file_name})


async def update_dy_aweme_video(aweme_id, video_content, extension_file_name):
    """
    更新抖音短视频
    Args:
        aweme_id:
        video_content:
        extension_file_name:

    Returns:

    """

    await DouYinVideo().store_video({"aweme_id": aweme_id, "video_content": video_content, "extension_file_name": extension_file_name})
