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
# @Time    : 2024/1/14 19:34
# @Desc    :

from typing import List

import config
from var import source_keyword_var

from ._store_impl import *
from .bilibilli_store_media import *
VIDEO_TITLE_CACHE = {}

class BiliStoreFactory:
    STORES = {
        "csv": BiliCsvStoreImplement,
        "xlsx": BiliCsvStoreImplement, 
        "db": BiliDbStoreImplement,
        "json": BiliJsonStoreImplement,
        "sqlite": BiliSqliteStoreImplement,
        "postgresql": BiliDbStoreImplement,
    }

    @staticmethod
    def create_store() -> AbstractStore:
        store_class = BiliStoreFactory.STORES.get(config.SAVE_DATA_OPTION)
        if not store_class:
            raise ValueError("[BiliStoreFactory.create_store] Invalid save option only supported csv or db or json or sqlite or postgresql ...")
        return store_class()


async def update_bilibili_video(video_item: Dict):
    video_item_view: Dict = video_item.get("View")
    video_user_info: Dict = video_item_view.get("owner")
    video_item_stat: Dict = video_item_view.get("stat")
    # 保持 video_id 为整数类型，匹配数据库 BigInteger 字段
    video_id = int(video_item_view.get("aid"))
    save_content_item = {
        "video_id": video_id,
        "video_type": "video",
        "title": video_item_view.get("title", "")[:500],
        "desc": video_item_view.get("desc", "")[:500],
        "create_time": video_item_view.get("pubdate"),
        # user_id 和 liked_count 需要保持为整数类型，匹配数据库 BigInteger/Integer 字段
        "user_id": int(video_user_info.get("mid")) if video_user_info.get("mid") else None,
        "nickname": video_user_info.get("name"),
        "avatar": video_user_info.get("face", ""),
        "liked_count": int(video_item_stat.get("like", 0)) if video_item_stat.get("like") else None,
        "disliked_count": str(video_item_stat.get("dislike", "")),
        "video_play_count": str(video_item_stat.get("view", "")),
        "video_favorite_count": str(video_item_stat.get("favorite", "")),
        "video_share_count": str(video_item_stat.get("share", "")),
        "video_coin_count": str(video_item_stat.get("coin", "")),
        "video_danmaku": str(video_item_stat.get("danmaku", "")),
        "video_comment": str(video_item_stat.get("reply", "")),
        "last_modify_ts": utils.get_current_timestamp(),
        "video_url": f"https://www.bilibili.com/video/av{video_id}",
        "video_cover_url": video_item_view.get("pic", ""),
        "source_keyword": source_keyword_var.get(),
    }
    VIDEO_TITLE_CACHE[video_id] = save_content_item.get("title", "") or save_content_item.get("desc", "")
    utils.logger.info(f"[store.bilibili.update_bilibili_video] bilibili video id:{video_id}, title:{save_content_item.get('title')}")
    await BiliStoreFactory.create_store().store_content(content_item=save_content_item)


async def update_up_info(video_item: Dict):
    video_item_card_list: Dict = video_item.get("Card")
    video_item_card: Dict = video_item_card_list.get("card")
    # user_id 需要保持为整数类型，匹配数据库 BigInteger 字段
    mid_value = video_item_card.get("mid")
    saver_up_info = {
        "user_id": int(mid_value) if mid_value else None,
        "nickname": video_item_card.get("name"),
        "sex": video_item_card.get("sex"),
        "sign": video_item_card.get("sign"),
        "avatar": video_item_card.get("face"),
        "last_modify_ts": utils.get_current_timestamp(),
        "total_fans": video_item_card.get("fans"),
        "total_liked": video_item_card_list.get("like_num"),
        "user_rank": video_item_card.get("level_info").get("current_level"),
        "is_official": video_item_card.get("official_verify").get("type"),
    }
    utils.logger.info(f"[store.bilibili.update_up_info] bilibili user_id:{video_item_card.get('mid')}")
    await BiliStoreFactory.create_store().store_creator(creator=saver_up_info)


async def batch_update_bilibili_video_comments(video_id: str, comments: List[Dict]):
    if not comments:
        return
    for comment_item in comments:
        await update_bilibili_video_comment(video_id, comment_item)

import os
from tools.time_util import get_time_str_from_unix_time
from tools.async_file_writer import AsyncFileWriter
from var import crawler_type_var, source_keyword_var

async def update_bilibili_video_comment(video_id: str, comment_item: Dict):
    # rpid 是评论ID
    rpid_value = comment_item.get("rpid")
    comment_id = int(rpid_value) if rpid_value else None
    if not comment_id:
        return

    content: Dict = comment_item.get("content") or {}
    message = content.get("message", "")

    ctime = comment_item.get("ctime", 0)
    publish_time = get_time_str_from_unix_time(ctime) if ctime else ""
    time_id = "".join(ch for ch in publish_time if ch.isdigit())[:12].ljust(12, "0")

    # topic：优先视频标题，其次关键词
    # topic：固定使用输入关键词
    topic = (source_keyword_var.get() or "").strip()

    event_item = {
        "event_id": f"bili:{comment_id}",
        "platform": "bili",
        "topic": topic,
        "publish_time": publish_time,
        "time_id": time_id,
        "text": message,
        "like_count": int(comment_item.get("like") or 0),
        "repost_count": 0,   # B站评论无转发数，先固定 0
    }

    utils.logger.info(f"[store.bilibili.update_bilibili_video_comment] bili comment event_id={event_item['event_id']}")

    # 1) CSV / xlsx：继续走原 store_comment（你 save_data_option=csv/xlsx 不会变）
    await BiliStoreFactory.create_store().store_comment(comment_item=event_item)

    # 2) Kafka JSONL：如果你输入了目录，就追加写入
    if getattr(config, "KAFKA_JSON_DIR", ""):
        try:
            fw = AsyncFileWriter(platform="bilibili", crawler_type=crawler_type_var.get())
            kafka_path = os.path.join(config.KAFKA_JSON_DIR, "bili_events.jsonl")
            await fw.append_to_jsonl(event_item, kafka_path)
        except Exception as e:
            utils.logger.warning(f"[BiliKafkaJson] write failed: {e}")



async def store_video(aid, video_content, extension_file_name):
    """
    video video storage implementation
    Args:
        aid:
        video_content:
        extension_file_name:
    """
    await BilibiliVideo().store_video({
        "aid": aid,
        "video_content": video_content,
        "extension_file_name": extension_file_name,
    })


async def batch_update_bilibili_creator_fans(creator_info: Dict, fans_list: List[Dict]):
    if not fans_list:
        return
    for fan_item in fans_list:
        fan_info: Dict = {
            "id": fan_item.get("mid"),
            "name": fan_item.get("uname"),
            "sign": fan_item.get("sign"),
            "avatar": fan_item.get("face"),
        }
        await update_bilibili_creator_contact(creator_info=creator_info, fan_info=fan_info)


async def batch_update_bilibili_creator_followings(creator_info: Dict, followings_list: List[Dict]):
    if not followings_list:
        return
    for following_item in followings_list:
        following_info: Dict = {
            "id": following_item.get("mid"),
            "name": following_item.get("uname"),
            "sign": following_item.get("sign"),
            "avatar": following_item.get("face"),
        }
        await update_bilibili_creator_contact(creator_info=following_info, fan_info=creator_info)


async def batch_update_bilibili_creator_dynamics(creator_info: Dict, dynamics_list: List[Dict]):
    if not dynamics_list:
        return
    for dynamic_item in dynamics_list:
        dynamic_id: str = dynamic_item["id_str"]
        dynamic_text: str = ""
        if dynamic_item["modules"]["module_dynamic"].get("desc"):
            dynamic_text = dynamic_item["modules"]["module_dynamic"]["desc"]["text"]
        dynamic_type: str = dynamic_item["type"].split("_")[-1]
        dynamic_pub_ts: str = dynamic_item["modules"]["module_author"]["pub_ts"]
        dynamic_stat: Dict = dynamic_item["modules"]["module_stat"]
        dynamic_comment: int = dynamic_stat["comment"]["count"]
        dynamic_forward: int = dynamic_stat["forward"]["count"]
        dynamic_like: int = dynamic_stat["like"]["count"]
        dynamic_info: Dict = {
            "dynamic_id": dynamic_id,
            "text": dynamic_text,
            "type": dynamic_type,
            "pub_ts": dynamic_pub_ts,
            "total_comments": dynamic_comment,
            "total_forwards": dynamic_forward,
            "total_liked": dynamic_like,
        }
        await update_bilibili_creator_dynamic(creator_info=creator_info, dynamic_info=dynamic_info)


async def update_bilibili_creator_contact(creator_info: Dict, fan_info: Dict):
    # up_id 和 fan_id 需要保持为整数类型，匹配数据库 BigInteger 字段
    up_id_value = creator_info.get("id")
    fan_id_value = fan_info.get("id")
    save_contact_item = {
        "up_id": int(up_id_value) if up_id_value else None,
        "fan_id": int(fan_id_value) if fan_id_value else None,
        "up_name": creator_info.get("name"),
        "fan_name": fan_info.get("name"),
        "up_sign": creator_info.get("sign"),
        "fan_sign": fan_info.get("sign"),
        "up_avatar": creator_info.get("avatar"),
        "fan_avatar": fan_info.get("avatar"),
        "last_modify_ts": utils.get_current_timestamp(),
    }

    await BiliStoreFactory.create_store().store_contact(contact_item=save_contact_item)


async def update_bilibili_creator_dynamic(creator_info: Dict, dynamic_info: Dict):
    save_dynamic_item = {
        "dynamic_id": dynamic_info["dynamic_id"],
        "user_id": creator_info["id"],
        "user_name": creator_info["name"],
        "text": dynamic_info["text"],
        "type": dynamic_info["type"],
        "pub_ts": dynamic_info["pub_ts"],
        "total_comments": dynamic_info["total_comments"],
        "total_forwards": dynamic_info["total_forwards"],
        "total_liked": dynamic_info["total_liked"],
        "last_modify_ts": utils.get_current_timestamp(),
    }

    await BiliStoreFactory.create_store().store_dynamic(dynamic_item=save_dynamic_item)
