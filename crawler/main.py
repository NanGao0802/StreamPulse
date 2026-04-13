# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。


import asyncio
import sys
from typing import Optional

import cmd_arg
import config
from database import db
from base.base_crawler import AbstractCrawler
from media_platform.bilibili import BilibiliCrawler
from media_platform.douyin import DouYinCrawler
from media_platform.kuaishou import KuaishouCrawler
from media_platform.tieba import TieBaCrawler
from media_platform.weibo import WeiboCrawler
from media_platform.xhs import XiaoHongShuCrawler
from media_platform.zhihu import ZhihuCrawler
from tools.async_file_writer import AsyncFileWriter
from var import crawler_type_var
import os
import pathlib
import pandas as pd
from tools.time_util import get_current_date

class CrawlerFactory:
    CRAWLERS = {
        "xhs": XiaoHongShuCrawler,
        "dy": DouYinCrawler,
        "ks": KuaishouCrawler,
        "bili": BilibiliCrawler,
        "wb": WeiboCrawler,
        "tieba": TieBaCrawler,
        "zhihu": ZhihuCrawler,
    }

    @staticmethod
    def create_crawler(platform: str) -> AbstractCrawler:
        crawler_class = CrawlerFactory.CRAWLERS.get(platform)
        if not crawler_class:
            raise ValueError(
                "Invalid Media Platform Currently only supported xhs or dy or ks or bili ..."
            )
        return crawler_class()


crawler: Optional[AbstractCrawler] = None


# persist-1<persist1@126.com>
# 原因：增加 --init_db 功能，用于数据库初始化。
# 副作用：无
# 回滚策略：还原此文件。
def export_csvs_to_xlsx(platform: str, crawler_type: str, date_str: str) -> str | None:
        csv_dir = f"data/{platform}/csv"
        if not os.path.isdir(csv_dir):
            return None

        xlsx_dir = f"data/{platform}/xlsx"
        pathlib.Path(xlsx_dir).mkdir(parents=True, exist_ok=True)

    # 选出“本轮/当天”的 CSV：{crawler_type}_{item_type}_{date}.csv
        suffix = f"_{date_str}.csv"
        prefix = f"{crawler_type}_"
        csv_files = [
            fn for fn in os.listdir(csv_dir)
            if fn.startswith(prefix) and fn.endswith(suffix)
    ]
        if not csv_files:
            return None

        out_path = f"{xlsx_dir}/{crawler_type}_{date_str}.xlsx"

        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            for fn in sorted(csv_files):
            # item_type：contents / comments / creators
                item_type = fn[len(prefix):]
                item_type = item_type[: -len(suffix)]
                sheet = (item_type[:31] or "sheet")  # Excel sheet 名最长 31

                df = pd.read_csv(os.path.join(csv_dir, fn), dtype=str, encoding="utf-8-sig")
                df.to_excel(writer, sheet_name=sheet, index=False)

        return out_path
async def main():
    # Init crawler
    global crawler

    # parse cmd（先解析命令行，命令行有值就优先生效）
    args = await cmd_arg.parse_cmd()

    # init db
    if args.init_db:
        await db.init_db(args.init_db)
        print(f"Database {args.init_db} initialized successfully.")
        return

    # ====== 一键启动友好：先读环境变量，没有才交互输入 ======
    # 统一环境变量名（start_all.ps1 会设置）
    env_kw = os.getenv("CRAWL_KEYWORDS", "").strip()
    env_interval = os.getenv("CRAWL_INTERVAL", "").strip()
    env_json_dir = os.getenv("CRAWL_JSON_DIR", "").strip()

    # 1) 关键词：屏蔽默认测试例子（无论 config 原来是什么，都不自动用）
    config.KEYWORDS = ""
    while True:
        kw = env_kw or input("请输入关键词（多个用逗号/中文逗号分隔，不能为空）：").strip()
        if kw:
            config.KEYWORDS = kw.replace("，", ",").strip()
            break
        # 如果是环境变量空，会走 input；input 也空就继续提示
        env_kw = ""  # 防止死循环反复用空 env
        print("[WARN] 关键词不能为空，请重新输入。")

    # 2) 轮询间隔：优先环境变量，其次 config，其次 input 默认30
    if env_interval.isdigit():
        config.POLL_INTERVAL_SEC = int(env_interval)
    if int(getattr(config, "POLL_INTERVAL_SEC", 0) or 0) <= 0:
        interval_str = input("请输入轮询间隔秒数 interval（默认30）：").strip()
        if interval_str.isdigit():
            config.POLL_INTERVAL_SEC = int(interval_str)
        else:
            config.POLL_INTERVAL_SEC = 30

    # 3) Kafka JSONL 输出目录：优先环境变量，其次 config，其次 input
    if env_json_dir:
        config.KAFKA_JSON_DIR = env_json_dir
    if not getattr(config, "KAFKA_JSON_DIR", "").strip():
        json_dir = input("请输入 jsonl 输出目录（例如 D:\\kafka_out，不要文件名）：").strip()
        if json_dir:
            config.KAFKA_JSON_DIR = json_dir.strip()
        else:
            config.KAFKA_JSON_DIR = ""

    if config.KAFKA_JSON_DIR:
        os.makedirs(config.KAFKA_JSON_DIR, exist_ok=True)
        print(f"[OK] jsonl 输出目录：{os.path.abspath(config.KAFKA_JSON_DIR)}")
    else:
        print("[WARN] 未设置 jsonl 输出目录，将不会输出 Kafka jsonl。")

    # ====== 启动 crawler ======
    crawler = CrawlerFactory.create_crawler(platform=config.PLATFORM)
    await crawler.start()

    # 爬完后：csv -> xlsx 合并（保持你原逻辑）
    if config.SAVE_DATA_OPTION in ("csv", "xlsx"):
        date_str = get_current_date()
        out = export_csvs_to_xlsx(config.PLATFORM, config.CRAWLER_TYPE, date_str)
        if out:
            print(f"[Main] Excel saved: {out}")

    # Generate wordcloud after crawling is complete
    if config.SAVE_DATA_OPTION == "json" and config.ENABLE_GET_WORDCLOUD:
        try:
            file_writer = AsyncFileWriter(
                platform=config.PLATFORM,
                crawler_type=crawler_type_var.get()
            )
            await file_writer.generate_wordcloud_from_comments()
        except Exception as e:
            print(f"Error generating wordcloud: {e}")


def cleanup():
    if crawler:
        # asyncio.run(crawler.close())
        pass
    if config.SAVE_DATA_OPTION in ["db", "sqlite"]:
        asyncio.run(db.close())


if __name__ == "__main__":
    try:
        asyncio.get_event_loop().run_until_complete(main())
    finally:
        cleanup()
