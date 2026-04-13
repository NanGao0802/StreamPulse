import asyncio
import csv
import json
import os
import pathlib
from typing import Dict, List
import aiofiles
import config
from tools.utils import utils
from tools.words import AsyncWordCloudGenerator

class AsyncFileWriter:
    def __init__(self, platform: str, crawler_type: str):
        self.lock = asyncio.Lock()
        self.platform = platform
        self.crawler_type = crawler_type
        self.wordcloud_generator = AsyncWordCloudGenerator() if config.ENABLE_GET_WORDCLOUD else None
        self._dedup = {}  # file_path -> set(event_id)
    def _get_file_path(self, file_type: str, item_type: str) -> str:
        base_path = f"data/{self.platform}/{file_type}"
        pathlib.Path(base_path).mkdir(parents=True, exist_ok=True)
        file_name = f"{self.crawler_type}_{item_type}_{utils.get_current_date()}.{file_type}"
        return f"{base_path}/{file_name}"

    async def write_to_csv(self, item: Dict, item_type: str):
        file_path = self._get_file_path('csv', item_type)

        async with self.lock:
            file_exists = os.path.exists(file_path)

            # ✅ 去重：按 event_id
            eid = item.get("event_id")
            if eid:
                if file_path not in self._dedup:
                    self._dedup[file_path] = set()
                    # 启动时扫一遍已有 event_id（文件很大时会慢，但稳定；后面可优化）
                    if file_exists and os.path.getsize(file_path) > 0:
                        try:
                            async with aiofiles.open(file_path, 'r', encoding='utf-8-sig') as rf:
                                content = await rf.read()
                            # 简单按行解析第一列 event_id（假设第一行是表头）
                            lines = [ln for ln in content.splitlines() if ln.strip()]
                            if len(lines) >= 2:
                                headers = lines[0].split(",")
                                if "event_id" in headers:
                                    idx = headers.index("event_id")
                                    for ln in lines[1:]:
                                        cols = ln.split(",")
                                        if idx < len(cols):
                                            self._dedup[file_path].add(cols[idx].strip())
                        except Exception:
                            pass

                if str(eid) in self._dedup[file_path]:
                    return
                self._dedup[file_path].add(str(eid))

            # ✅ 追加写
            async with aiofiles.open(file_path, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=item.keys())
                if not file_exists or await f.tell() == 0:
                    await writer.writeheader()
                await writer.writerow(item)

    async def write_single_item_to_json(self, item: Dict, item_type: str):
        file_path = self._get_file_path('json', item_type)
        async with self.lock:
            existing_data = []
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                    try:
                        content = await f.read()
                        if content:
                            existing_data = json.loads(content)
                        if not isinstance(existing_data, list):
                            existing_data = [existing_data]
                    except json.JSONDecodeError:
                        existing_data = []
            
            existing_data.append(item)

            async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(existing_data, ensure_ascii=False, indent=4))


    async def append_to_jsonl(self, item: Dict, file_path: str, dedup_field: str = "event_id"):
        """
        追加写 JSONL（Kafka 友好），并按 event_id 去重（仅本次运行内；如要跨重启去重可再扩展）
        """
        if not file_path:
            return

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # 懒加载去重集合
        if not hasattr(self, "_dedup_jsonl"):
            self._dedup_jsonl = {}  # file_path -> set()

        async with self.lock:
            s = self._dedup_jsonl.setdefault(file_path, set())
            eid = item.get(dedup_field)
            if eid and str(eid) in s:
                return
            if eid:
                s.add(str(eid))

            async with aiofiles.open(file_path, "a", encoding="utf-8") as f:
                await f.write(json.dumps(item, ensure_ascii=False) + "\n")




    async def generate_wordcloud_from_comments(self):
        """
        Generate wordcloud from comments data
        Only works when ENABLE_GET_WORDCLOUD and ENABLE_GET_COMMENTS are True
        """
        if not config.ENABLE_GET_WORDCLOUD or not config.ENABLE_GET_COMMENTS:
            return

        if not self.wordcloud_generator:
            return

        try:
            # Read comments from JSON file
            comments_file_path = self._get_file_path('json', 'comments')
            if not os.path.exists(comments_file_path) or os.path.getsize(comments_file_path) == 0:
                utils.logger.info(f"[AsyncFileWriter.generate_wordcloud_from_comments] No comments file found at {comments_file_path}")
                return

            async with aiofiles.open(comments_file_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                if not content:
                    utils.logger.info(f"[AsyncFileWriter.generate_wordcloud_from_comments] Comments file is empty")
                    return

                comments_data = json.loads(content)
                if not isinstance(comments_data, list):
                    comments_data = [comments_data]

            # Filter comments data to only include 'content' field
            # Handle different comment data structures across platforms
            filtered_data = []
            for comment in comments_data:
                if isinstance(comment, dict):
                    # Try different possible content field names
                    content_text = comment.get('content') or comment.get('comment_text') or comment.get('text') or ''
                    if content_text:
                        filtered_data.append({'content': content_text})

            if not filtered_data:
                utils.logger.info(f"[AsyncFileWriter.generate_wordcloud_from_comments] No valid comment content found")
                return

            # Generate wordcloud
            words_base_path = f"data/{self.platform}/words"
            pathlib.Path(words_base_path).mkdir(parents=True, exist_ok=True)
            words_file_prefix = f"{words_base_path}/{self.crawler_type}_comments_{utils.get_current_date()}"

            utils.logger.info(f"[AsyncFileWriter.generate_wordcloud_from_comments] Generating wordcloud from {len(filtered_data)} comments")
            await self.wordcloud_generator.generate_word_frequency_and_cloud(filtered_data, words_file_prefix)
            utils.logger.info(f"[AsyncFileWriter.generate_wordcloud_from_comments] Wordcloud generated successfully at {words_file_prefix}")

        except Exception as e:
            utils.logger.error(f"[AsyncFileWriter.generate_wordcloud_from_comments] Error generating wordcloud: {e}")