# StreamPulse 项目文档

本目录包含 StreamPulse 实时舆情分析系统的完整技术文档，适合开发者从零开始理解、部署和二次开发本项目。

---

## 文档目录

| 文档 | 内容简介 |
|------|----------|
| [01_architecture.md](./01_architecture.md) | 系统五层架构设计、模块关系、技术选型说明 |
| [02_data_pipeline.md](./02_data_pipeline.md) | Kafka 消息格式、StructBERT 情感推理、热度计算、Delta Lake 数据模型 |
| [03_api_reference.md](./03_api_reference.md) | REST API 全部端点的参数说明、响应格式与请求示例 |
| [04_deployment.md](./04_deployment.md) | 云服务器 / 本地环境完整部署步骤 |
| [05_configuration.md](./05_configuration.md) | 所有环境变量、配置文件模板与默认值说明 |

---

## 快速导航

**我想了解系统整体设计** → [01_architecture.md](./01_architecture.md)

**我想了解数据如何流动和处理** → [02_data_pipeline.md](./02_data_pipeline.md)

**我想调用 API 或开发前端** → [03_api_reference.md](./03_api_reference.md)

**我想在服务器上部署这套系统** → [04_deployment.md](./04_deployment.md)

**我想修改告警阈值 / SMTP 邮件 / Kafka 地址** → [05_configuration.md](./05_configuration.md)

---

## 项目结构速览

```
graduation project/
├── crawler/          # MediaCrawler 多平台爬虫（数据采集层）
├── backend/
│   ├── spark/        # Spark Structured Streaming 核心任务（流计算层）
│   ├── api/          # FastAPI REST 服务（数据服务层）
│   ├── tests/        # 调试与验证脚本
│   └── scripts/      # 云服务器启动脚本
├── frontend/         # Vue 3 + ECharts 可视化大屏（展示层）
├── docs/             # 本目录：项目技术文档
└── README.md         # 项目总览
```
