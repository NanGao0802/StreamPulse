# 第一层：数据采集层（crawler/）

> 对应架构层级：**数据采集层**  
> 核心技术：Python 3.12 · Playwright CDP · MediaCrawler（开源框架二次开发）· asyncio · aiofiles

---

## 待学习内容清单

- [x] MediaCrawler 框架是什么，我们做了哪些二次开发
- [x] Playwright CDP 反检测原理
- [x] 爬虫如何输出 JSONL 文件
- [x] Kafka Producer 进程如何将 JSONL 推送到 Kafka Topic
- [x] 支持的7个平台各自的爬取方式有何差异
- [x] 轮询模式（ENABLE_POLL_LATEST）如何工作

---

## 模块目录结构

```
crawler/
├── main.py                  # 程序入口：参数解析、CrawlerFactory 分发、启动
├── base/
│   └── base_crawler.py      # 抽象基类：AbstractCrawler / AbstractLogin / AbstractStore
├── config/
│   ├── base_config.py       # 全局配置（PLATFORM、KEYWORDS、CDP、KAFKA_JSON_DIR 等）
│   ├── bilibili_config.py   # B站专属配置
│   ├── dy_config.py         # 抖音专属配置
│   └── ...（其余5个平台配置）
├── media_platform/          # 各平台爬虫实现（bilibili / douyin / weibo / xhs / ...）
├── store/                   # 各平台存储实现（csv / json / postgresql / sqlite）
├── tools/
│   ├── async_file_writer.py # JSONL异步写入 + 去重（供Kafka输出）
│   ├── event_schema.py      # 评论→统一Event格式转换（构造 event_id 等字段）
│   ├── cdp_browser.py       # CDP 浏览器控制（检测Chrome路径、附加进程）
│   ├── browser_launcher.py  # 浏览器启动器（标准模式 / CDP模式 自动切换）
│   └── crawler_util.py      # 通用工具函数
├── cache/                   # 去重缓存（内存/文件）
└── var.py                   # contextvars 全局变量（crawler_type_var）
```

---

## 1. MediaCrawler 是什么

MediaCrawler 是一个开源的多平台社交媒体爬虫框架，支持小红书、抖音、B站、微博、知乎、快手、贴吧。

**我们做的二次开发主要有三点：**

1. **新增 JSONL 输出模式**：在 `tools/async_file_writer.py` 中新增 `append_to_jsonl()` 方法，每条评论序列化为一行JSON追加写入本地文件，供后续 Kafka Producer 消费。

2. **新增统一 Event Schema**：在 `tools/event_schema.py` 中将各平台不同格式的评论字段统一转换为标准格式（`event_id`、`platform`、`topic`、`publish_time`、`text`、`like_count`、`repost_count`）。

3. **新增交互式启动流程**：`main.py` 支持通过环境变量（`CRAWL_KEYWORDS`、`CRAWL_INTERVAL`、`CRAWL_JSON_DIR`）或交互式输入配置关键词、轮询间隔和输出目录，无需修改配置文件。

---

## 2. 抽象基类设计（base/base_crawler.py）

```python
class AbstractCrawler(ABC):
    @abstractmethod
    async def start(self): ...          # 启动爬虫主流程

    @abstractmethod
    async def search(self): ...         # 关键词搜索

    @abstractmethod
    async def launch_browser(...): ...  # 标准模式启动浏览器

    async def launch_browser_with_cdp(...): ...  # CDP模式（可选覆写）
```

框架使用**工厂模式**分发各平台爬虫：

```python
class CrawlerFactory:
    CRAWLERS = {
        "xhs":   XiaoHongShuCrawler,
        "dy":    DouYinCrawler,
        "ks":    KuaishouCrawler,
        "bili":  BilibiliCrawler,
        "wb":    WeiboCrawler,
        "tieba": TieBaCrawler,
        "zhihu": ZhihuCrawler,
    }

    @staticmethod
    def create_crawler(platform: str) -> AbstractCrawler:
        crawler_class = CrawlerFactory.CRAWLERS.get(platform)
        return crawler_class()
```

各平台爬虫都继承 `AbstractCrawler`，实现同一套接口，`main.py` 只需调用 `crawler.start()`。

---

## 3. CDP 反检测原理

### 普通 Headless 浏览器的问题

普通 Playwright 启动的无头浏览器（`headless=True`）会在多个特征上暴露自己：
- `navigator.webdriver = true`
- 缺少真实用户的 Cookie、浏览历史、扩展插件
- 浏览器指纹（Canvas、WebGL 等）与普通用户不一致

### CDP 模式的原理

CDP（Chrome DevTools Protocol）是 Chrome 提供的调试协议，Playwright 可以通过它**连接并控制用户已安装的真实 Chrome/Edge 浏览器**：

```python
# tools/cdp_browser.py 的核心逻辑
# 1. 检测系统中 Chrome/Edge 的安装路径
# 2. 以调试模式启动真实浏览器（--remote-debugging-port=9222）
# 3. Playwright 通过 CDP 协议连接该端口接管控制
browser = await playwright.chromium.connect_over_cdp(
    f"http://localhost:{CDP_DEBUG_PORT}"
)
```

**关键优势：**
- 使用真实浏览器，指纹与普通用户完全一致
- 保留用户已有的 Cookie 和登录状态（`SAVE_LOGIN_STATE=True`）
- 扩展插件正常运行，平台检测机制难以识别

**配置项（`config/base_config.py`）：**

```python
ENABLE_CDP_MODE = True       # 是否启用CDP模式
CDP_DEBUG_PORT = 9222        # CDP通信端口（被占用时自动找下一个）
CUSTOM_BROWSER_PATH = ""     # 自定义浏览器路径（空则自动检测）
CDP_HEADLESS = False         # CDP模式下是否无头（建议False）
BROWSER_LAUNCH_TIMEOUT = 30  # 浏览器启动超时秒数
```

---

## 4. 数据输出：JSONL 文件（tools/async_file_writer.py）

### append_to_jsonl()：Kafka友好的异步写入

```python
async def append_to_jsonl(self, item: Dict, file_path: str, dedup_field: str = "event_id"):
    """
    追加写 JSONL（Kafka 友好），并按 event_id 去重（仅本次运行内）
    """
    async with self.lock:                         # asyncio.Lock 保证并发安全
        s = self._dedup_jsonl.setdefault(file_path, set())
        eid = item.get(dedup_field)
        if eid and str(eid) in s:
            return                                # 本次运行内已写过，跳过
        if eid:
            s.add(str(eid))
        async with aiofiles.open(file_path, "a", encoding="utf-8") as f:
            await f.write(json.dumps(item, ensure_ascii=False) + "\n")  # 一行一条JSON
```

**JSONL 格式示例：**
```
{"event_id":"dy:123","platform":"dy","topic":"26考研","publish_time":"2026-03-05 23:24:05","text":"...","like_count":4,"repost_count":0}
{"event_id":"dy:124","platform":"dy","topic":"26考研","publish_time":"2026-03-05 23:25:10","text":"...","like_count":0,"repost_count":0}
```

每行是一个完整的 JSON 对象，这种格式对 Kafka Producer 非常友好——逐行读取、逐条发送。

### event_schema.py：统一字段格式

不同平台的原始字段名差异很大（抖音叫 `comment_id`，B站叫 `rpid` 等），`event_schema.py` 负责统一转换：

```python
def dy_comment_to_event(comment_item: dict, topic: str = "") -> dict:
    publish_time = get_time_str_from_unix_time(comment_item.get("create_time", 0))
    return {
        "event_id":     f"dy:{comment_item.get('comment_id', '')}",  # 平台前缀:原始ID
        "platform":     "dy",
        "topic":        topic,
        "publish_time": publish_time,
        "time_id":      make_time_id(publish_time),  # 格式 yyyyMMddHHMM
        "text":         comment_item.get("content", ""),
        "like_count":   int(comment_item.get("like_count", 0) or 0),
        "repost_count": 0,
    }
```

`event_id` 的格式是 `平台前缀:平台原始ID`，保证全局唯一，这是 Silver 层 Merge 去重的关键字段。

---

## 5. 轮询模式（ENABLE_POLL_LATEST）

```python
# config/base_config.py
ENABLE_POLL_LATEST = False   # True=无限轮询抓最新
POLL_INTERVAL_SEC = 30       # 每轮间隔秒数
SEARCH_PAGES = 2             # 每轮抓前 N 页
KAFKA_JSON_DIR = ""          # JSONL 输出目录
```

开启后，爬虫会无限循环：抓前N页 → 等待间隔 → 再抓前N页，持续将最新评论追加到 JSONL 文件，实现"准实时"的持续数据采集。

---

## 6. 支持平台一览

| 平台 | 标识 | 配置文件 | 登录方式 |
|------|------|----------|---------|
| 小红书 | `xhs` | `xhs_config.py` | 二维码/Cookie |
| 抖音 | `dy` | `dy_config.py` | 二维码/Cookie |
| B站 | `bili` | `bilibili_config.py` | 二维码/Cookie |
| 微博 | `wb` | `weibo_config.py` | 二维码/Cookie |
| 快手 | `ks` | `ks_config.py` | 二维码 |
| 贴吧 | `tieba` | `tieba_config.py` | Cookie |
| 知乎 | `zhihu` | `zhihu_config.py` | Cookie |

---

## 7. main.py 启动流程

```python
async def main():
    args = await cmd_arg.parse_cmd()    # 解析命令行参数

    # 优先读环境变量，没有才交互输入
    env_kw       = os.getenv("CRAWL_KEYWORDS", "")   # 关键词
    env_interval = os.getenv("CRAWL_INTERVAL", "")   # 轮询间隔
    env_json_dir = os.getenv("CRAWL_JSON_DIR",  "")  # JSONL输出目录

    # 1. 设置关键词（必填）
    config.KEYWORDS = kw  # 用逗号分隔多个关键词

    # 2. 设置轮询间隔（默认30秒）
    config.POLL_INTERVAL_SEC = int(env_interval) or 30

    # 3. 设置JSONL输出目录
    config.KAFKA_JSON_DIR = env_json_dir

    # 4. 创建对应平台爬虫并启动
    crawler = CrawlerFactory.create_crawler(platform=config.PLATFORM)
    await crawler.start()

    # 5. 爬完后：csv → xlsx 合并（可选）
    # 6. 爬完后：生成词云（ENABLE_GET_WORDCLOUD=True时）
```

---

## 8. 关键配置项全表（config/base_config.py）

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `PLATFORM` | `"bili"` | 爬取平台：`xhs/dy/ks/bili/wb/tieba/zhihu` |
| `KEYWORDS` | 示例词 | 搜索关键词，英文逗号分隔 |
| `LOGIN_TYPE` | `"qrcode"` | 登录方式：`qrcode/phone/cookie` |
| `CRAWLER_TYPE` | `"search"` | 爬取类型：`search/detail/creator` |
| `ENABLE_CDP_MODE` | `True` | 是否启用CDP反检测模式 |
| `CDP_DEBUG_PORT` | `9222` | CDP通信端口 |
| `HEADLESS` | `False` | 是否无头（建议False防检测） |
| `SAVE_LOGIN_STATE` | `True` | 是否保存登录状态（Cookie持久化） |
| `SAVE_DATA_OPTION` | `"postgresql"` | 存储方式：`csv/json/sqlite/postgresql` |
| `ENABLE_POLL_LATEST` | `False` | 是否持续轮询 |
| `POLL_INTERVAL_SEC` | `30` | 轮询间隔（秒） |
| `KAFKA_JSON_DIR` | `""` | JSONL输出目录（空则不输出） |
| `CRAWLER_MAX_NOTES_COUNT` | `5` | 最多爬取帖子数 |
| `MAX_CONCURRENCY_NUM` | `1` | 并发爬虫数 |
| `ENABLE_GET_COMMENTS` | `True` | 是否抓取评论 |
| `CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES` | `20` | 每帖最多评论数 |
| `ENABLE_GET_SUB_COMMENTS` | `False` | 是否抓取二级评论 |
| `ENABLE_GET_WORDCLOUD` | `False` | 是否生成词云图 |

---

## 9. 启动方式

```powershell
# 安装依赖
cd crawler
pip install -r requirements.txt
playwright install chromium

# 方式一：交互式启动（程序提示输入）
python main.py

# 方式二：命令行参数启动
python main.py --platform bili --lt qrcode --type search

# 方式三：环境变量启动（适合脚本化）
$env:CRAWL_KEYWORDS="26考研人数下降"
$env:CRAWL_INTERVAL="60"
$env:CRAWL_JSON_DIR="D:\kafka_out"
python main.py
```

---

## 学习笔记

<!-- 在这里补充你的学习理解 -->
