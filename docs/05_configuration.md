# 配置参考手册

所有运行时配置均通过**环境变量**注入，由 `python-dotenv` 从 `/opt/pipeline/conf/alert.env` 加载。  
本文档列出全部可配置项，含类型、默认值和说明。

---

## 1. alert.env 完整模板

```ini
# ── Kafka ──────────────────────────────────────────────────────
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC=public_opinion_raw

# ── StructBERT 情感模型 ──────────────────────────────────────────
STRUCTBERT_MODEL_ID=iic/nlp_structbert_sentiment-classification_chinese-base
STRUCTBERT_BATCH_SIZE=32
STRUCTBERT_NEUTRAL_THRESHOLD=0.72
STRUCTBERT_MAX_TEXT_LENGTH=256

# ── 告警触发阈值 ─────────────────────────────────────────────────
ALERT_MIN_COMMENTS=10
ALERT_NEG_RATIO_THRESHOLD=0.40
ALERT_TOPN_TEXTS=3

# ── 邮件告警（SMTP）──────────────────────────────────────────────
ALERT_EMAIL_ENABLED=false
SMTP_HOST=smtp.qq.com
SMTP_PORT=465
SMTP_USE_SSL=true
SMTP_USE_TLS=false
SMTP_USER=your_qq@qq.com
SMTP_PASSWORD=your_smtp_auth_code
SMTP_FROM=your_qq@qq.com
SMTP_TO=recipient1@example.com,recipient2@example.com
```

---

## 2. Kafka 配置

| 变量 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `KAFKA_BOOTSTRAP` | string | `localhost:9092` | Kafka Broker 地址，多个用逗号分隔 |
| `KAFKA_TOPIC` | string | `public_opinion_raw` | 订阅的 Topic 名称 |

---

## 3. StructBERT 模型配置

| 变量 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `STRUCTBERT_MODEL_ID` | string | `iic/nlp_structbert_sentiment-classification_chinese-base` | ModelScope 模型 ID |
| `STRUCTBERT_BATCH_SIZE` | int | `32` | 批量推理的 batch 大小，内存受限时可调小 |
| `STRUCTBERT_NEUTRAL_THRESHOLD` | float | `0.72` | 置信度低于此值时归为中性，范围 `(0, 1)` |
| `STRUCTBERT_MAX_TEXT_LENGTH` | int | `256` | 输入文本最大字符数，超出截断 |

**调优建议**

- 内存 < 6 GB：将 `STRUCTBERT_BATCH_SIZE` 调小至 `8` 或 `16`
- 误报率偏高：适当提高 `STRUCTBERT_NEUTRAL_THRESHOLD`（如 `0.80`）
- 漏报率偏高：适当降低 `STRUCTBERT_NEUTRAL_THRESHOLD`（如 `0.60`）

---

## 4. 告警触发阈值

| 变量 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `ALERT_MIN_COMMENTS` | int | `10` | 触发告警所需的最少评论数（低流量话题防误报）|
| `ALERT_NEG_RATIO_THRESHOLD` | float | `0.40` | 负面占比阈值，超过时触发，范围 `(0, 1)` |
| `ALERT_TOPN_TEXTS` | int | `3` | 告警邮件中附带的 Top N 负面评论样例数 |

---

## 5. SMTP 邮件告警配置

| 变量 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `ALERT_EMAIL_ENABLED` | bool | `false` | 是否启用邮件告警，`true` / `false` |
| `SMTP_HOST` | string | `""` | SMTP 服务器地址 |
| `SMTP_PORT` | int | `465` | SMTP 端口（SSL: 465，TLS: 587）|
| `SMTP_USE_SSL` | bool | `true` | 是否使用 SSL 加密连接 |
| `SMTP_USE_TLS` | bool | `false` | 是否使用 STARTTLS（与 SSL 二选一）|
| `SMTP_USER` | string | `""` | SMTP 登录用户名（通常为邮箱地址）|
| `SMTP_PASSWORD` | string | `""` | SMTP 授权码（非邮箱登录密码）|
| `SMTP_FROM` | string | `""` | 发件人地址 |
| `SMTP_TO` | string | `""` | 收件人地址，多个用英文逗号分隔 |

### QQ 邮箱配置示例

```ini
ALERT_EMAIL_ENABLED=true
SMTP_HOST=smtp.qq.com
SMTP_PORT=465
SMTP_USE_SSL=true
SMTP_USER=123456789@qq.com
SMTP_PASSWORD=abcdefghijklmnop   # QQ邮箱的SMTP授权码，非登录密码
SMTP_FROM=123456789@qq.com
SMTP_TO=recipient@example.com
```

> QQ 邮箱需在设置 → 帐户 → POP3/SMTP 服务中开启，生成专用授权码。

### 163 邮箱配置示例

```ini
SMTP_HOST=smtp.163.com
SMTP_PORT=465
SMTP_USE_SSL=true
SMTP_USER=yourname@163.com
SMTP_PASSWORD=your_client_auth_code
```

---

## 6. 前端环境变量

文件路径：`frontend/.env.development`（开发）/ `frontend/.env.production`（生产）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `VITE_API_BASE` | `http://127.0.0.1:18000` | API 服务地址，不含路径，结尾不加 `/` |

```ini
# frontend/.env.development
VITE_API_BASE=http://127.0.0.1:18000

# frontend/.env.production（部署到公网时）
VITE_API_BASE=http://your-server-ip:18000
```

---

## 7. 爬虫关键配置

文件路径：`crawler/config/base_config.py`

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `PLATFORM` | `"bili"` | 爬取平台：`xhs / dy / ks / bili / wb / tieba / zhihu` |
| `KEYWORDS` | （示例关键词）| 搜索关键词，英文逗号分隔 |
| `ENABLE_POLL_LATEST` | `False` | 是否持续轮询抓取最新评论 |
| `POLL_SLEEP_INTERVAL` | `300` | 轮询间隔（秒）|
| `SAVE_DATA_OPTION` | `"json"` | 数据存储方式：`json / csv / postgresql / sqlite` |
| `MAX_COMMENTS_PER_POST` | `20` | 每条帖子最多抓取评论数 |

---

## 8. Spark 提交参数

脚本 `backend/scripts/run_stream_single_topic.sh` 中的关键 spark-submit 参数：

| 参数 | 值 | 说明 |
|------|----|------|
| `--master` | `local[*]` | 使用本机所有核心 |
| `--driver-memory` | `2g` | Driver 内存（需容纳 StructBERT 模型）|
| `--executor-memory` | `2g` | Executor 内存 |
| `--packages` | `io.delta:delta-spark_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8` | Delta Lake + Kafka connector |
| `spark.sql.shuffle.partitions` | `2` | 本地模式下降低 shuffle 分区数，减少小文件 |
| `maxOffsetsPerTrigger` | `50` | 每批次最多消费 50 条消息，控制推理延迟 |
| `processingTime` | `"60 seconds"` | 微批次触发间隔 |
