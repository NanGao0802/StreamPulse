# 部署指南

## 部署架构概览

```
┌─────────────────────────────────────────┐
│  云服务器（Linux / Alibaba Cloud ECS）   │
│                                         │
│  ● Apache Kafka（消息队列）              │
│  ● Spark Structured Streaming（流任务） │
│  ● FastAPI（REST API，端口 18000）       │
│  ● Python venv（/opt/pipeline/.venv/）  │
│  ● Delta Lake 数据（/data/public-opinion/）│
└─────────────────────────────────────────┘
         ▲ HTTP API
┌────────┘
│  本地开发机（Windows）
│  ● 爬虫（crawler/）
│  ● 前端开发（frontend/）
└─────────────────────────────────────────
```

---

## 一、云服务器部署

### 1.1 环境要求

| 软件 | 版本 | 说明 |
|------|------|------|
| JDK | 11 或 17 | Spark 运行依赖 |
| Python | 3.10 ~ 3.12 | 推荐 3.12 |
| Apache Spark | 3.5.x | 需与 delta-spark 版本匹配 |
| Apache Kafka | 3.x | 含 Zookeeper 或 KRaft 模式 |
| 内存 | ≥ 8 GB | StructBERT 模型约 400 MB，Spark Driver 建议 2 GB |
| 磁盘 | ≥ 20 GB | Delta Lake 数据存储 |

### 1.2 目录结构约定

```
/opt/pipeline/
├── .venv/              # Python 虚拟环境
├── spark/              # 本项目 backend/spark/ 同步至此
├── api/                # 本项目 backend/api/ 同步至此
├── tests/              # 本项目 backend/tests/ 同步至此
├── scripts/            # 本项目 backend/scripts/ 同步至此
└── conf/
    └── alert.env       # 告警配置（SMTP + 阈值，不入 Git）

/data/public-opinion/
├── delta/              # Delta Lake 数据目录
│   └── single_topic_comments/
│       ├── bronze_comments_raw/
│       ├── silver_comments_enriched/
│       ├── gold_hourly_metrics/
│       ├── gold_daily_metrics/
│       ├── gold_daily_top10_comments/
│       └── gold_alert_events/
└── checkpoints/        # Spark Checkpoint 目录
    └── single_topic_comment_pipeline/
```

### 1.3 Python 环境安装

```bash
python3 -m venv /opt/pipeline/.venv
source /opt/pipeline/.venv/bin/activate

pip install --upgrade pip
pip install \
    pyspark==3.5.3 \
    delta-spark==3.3.2 \
    deltalake==0.25.3 \
    modelscope \
    torch \
    fastapi \
    uvicorn \
    pandas \
    python-dotenv \
    kafka-python
```

> **注意**：`delta-spark` 与 `pyspark` 版本需严格匹配。  
> delta-spark 3.3.x 对应 pyspark 3.5.x。

### 1.4 配置 alert.env

在 `/opt/pipeline/conf/alert.env` 创建配置文件（参见 [05_configuration.md](./05_configuration.md)）：

```bash
mkdir -p /opt/pipeline/conf
cat > /opt/pipeline/conf/alert.env << 'EOF'
KAFKA_BOOTSTRAP=<Kafka地址:端口>
KAFKA_TOPIC=public_opinion_raw

ALERT_EMAIL_ENABLED=true
SMTP_HOST=smtp.qq.com
SMTP_PORT=465
SMTP_USE_SSL=true
SMTP_USER=your_qq@qq.com
SMTP_PASSWORD=your_smtp_auth_code
SMTP_FROM=your_qq@qq.com
SMTP_TO=recipient@example.com

ALERT_MIN_COMMENTS=10
ALERT_NEG_RATIO_THRESHOLD=0.40
ALERT_TOPN_TEXTS=3
STRUCTBERT_NEUTRAL_THRESHOLD=0.72
EOF
```

### 1.5 同步代码至服务器

```bash
# 在本地执行，将 backend/ 同步至服务器
rsync -avz --delete backend/spark/ user@server:/opt/pipeline/spark/
rsync -avz --delete backend/api/   user@server:/opt/pipeline/api/
rsync -avz --delete backend/tests/ user@server:/opt/pipeline/tests/
rsync -avz --delete backend/scripts/ user@server:/opt/pipeline/scripts/
```

### 1.6 创建数据目录

```bash
mkdir -p /data/public-opinion/delta/single_topic_comments
mkdir -p /data/public-opinion/checkpoints/single_topic_comment_pipeline
```

### 1.7 启动 Kafka

```bash
# 以 KRaft 模式为例
kafka-server-start.sh /opt/kafka/config/server.properties

# 创建 Topic（若不存在）
kafka-topics.sh --create \
  --topic public_opinion_raw \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### 1.8 启动 Spark 流任务

```bash
bash /opt/pipeline/scripts/run_stream_single_topic.sh
```

脚本内容要点：
- 设置 `PYSPARK_PYTHON` 和 `PYSPARK_DRIVER_PYTHON` 指向 venv Python
- `--packages` 下载 Delta Lake 和 Kafka connector（首次运行需联网）
- `--master local[*]` 单机模式

首次启动会下载 Maven 依赖包（约 200 MB），之后从本地缓存加载。

### 1.9 启动 API 服务

```bash
bash /opt/pipeline/scripts/run_api.sh
```

验证：

```bash
curl http://localhost:18000/api/health
```

### 1.10 环境变量配置（可选）

如需通过系统环境变量而非 `alert.env` 文件注入配置，可在 `/etc/profile.d/public-opinion.sh` 中添加：

```bash
export KAFKA_BOOTSTRAP="172.16.x.x:9092"
export KAFKA_TOPIC="public_opinion_raw"
```

---

## 二、本地爬虫部署（Windows）

### 2.1 安装依赖

```powershell
cd crawler
pip install -r requirements.txt
playwright install chromium
```

### 2.2 配置爬取参数

编辑 `crawler/config/base_config.py`：

```python
PLATFORM = "bili"        # 选择平台
KEYWORDS = "关键词1,关键词2"
ENABLE_POLL_LATEST = True  # 是否轮询最新评论
POLL_SLEEP_INTERVAL = 300  # 轮询间隔（秒）
SAVE_DATA_OPTION = "json"  # 输出方式：json / csv / postgresql
```

### 2.3 启动爬虫

```powershell
python main.py --platform bili --lt qrcode --type search
```

首次运行会弹出二维码，扫码登录后 Cookie 自动保存，后续无需重复登录。

### 2.4 启动 Kafka Producer

爬虫输出 JSONL 文件后，需运行 Producer 进程将数据推送至 Kafka：

```bash
# 在服务器端运行，或在爬虫机器上安装 kafka-python
python crawler/tools/event_schema.py   # 验证消息格式
# 具体 Producer 脚本根据实际部署调整
```

---

## 三、前端部署

### 3.1 本地开发

```powershell
cd frontend
npm install
npm run dev    # 访问 http://localhost:5173
```

配置 `frontend/.env.development`：

```
VITE_API_BASE=http://<服务器IP>:18000
```

### 3.2 生产构建

```powershell
npm run build
# 产物在 frontend/dist/，部署到任意静态服务器（Nginx / GitHub Pages 等）
```

---

## 四、验证部署

### 逐步验证清单

```bash
# 1. Kafka 是否收到消息
kafka-console-consumer.sh --topic public_opinion_raw \
  --bootstrap-server localhost:9092 --from-beginning --max-messages 5

# 2. Spark 任务是否正常运行
tail -f /var/log/spark-stream.log

# 3. Delta 表是否有数据
bash /opt/pipeline/scripts/check_single_topic_delta.sh

# 4. API 是否正常响应
curl http://localhost:18000/api/topics

# 5. 检查运行环境
bash /opt/pipeline/scripts/check_env.sh
```
