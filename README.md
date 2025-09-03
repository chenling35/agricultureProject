# 农产品价格信息系统设计 (Agricultural Price Information System)


这是一个集**数据采集、实时处理、存储分析与可视化**于一体的全链路农产品价格监测平台。旨在为市场分析提供实时、直观的数据支持，并将复杂数据转化为有价值的市场洞察。

---

## ✨ 项目亮点

*   **全链路实现**: 涵盖了从数据源（分布式爬虫）到底层数据处理（实时流计算），再到后端服务和前端可视化的完整数据产品流程。
*   **实时性**: 搭建了以 `Kafka` + `Spark Streaming` 为核心的实时数据管道，数据从采集到可供分析的延迟控制在**秒级**。
*   **高并发与稳定性**: 采用 `Scrapy-Redis` 实现分布式爬虫，并通过多种反爬策略保证了数据源的稳定接入。
*   **高性能服务**: `MySQL + Redis` 的混合存储架构，通过缓存热点数据将核心查询接口的**响应速度提升了60%**。
*   **丰富可视化**: 基于 `ECharts` 实现了动态、可交互的数据可视化界面，提供了多维度的数据分析视角。

---

## 🏛️ 系统架构

本项目采用分层解耦设计，确保了各模块的职责明确与协同工作。


1.  **数据采集层 (Data Collection Layer)**:
    *   **技术**: `Python`, `Scrapy`, `Scrapy-Redis`
    *   **职责**: 负责从农业部官网等多个数据源定时、高并发地抓取农产品价格数据。

2.  **数据管道层 (Data Pipeline Layer)**:
    *   **技术**: `Apache Kafka`
    *   **职责**: 作为消息总线，负责缓冲、削峰填谷，并将原始数据安全、可靠地传输给处理层。

3.  **实时处理层 (Real-time Processing Layer)**:
    *   **技术**: `Apache Spark Streaming`
    *   **职责**: 订阅 Kafka 中的数据流，进行实时的清洗、转换、聚合，并计算关键指标（如价格指数、环比涨跌幅）。

4.  **数据存储层 (Data Storage Layer)**:
    *   **技术**: `MySQL`, `Redis`
    *   **职责**: `MySQL` 用于持久化存储结构化的历史价格数据，`Redis` 作为高性能缓存，存储热点数据以加速查询。

5.  **后端服务层 (Backend Service Layer)**:
    *   **技术**: `Python`, `Flask`
    *   **职责**: 提供一套RESTful API接口，负责处理前端的业务请求，并从存储层获取数据返回。

6.  **前端展示层 (Frontend Presentation Layer)**:
    *   **技术**: `HTML`, `CSS`, `JavaScript`, `ECharts`
    *   **职责**: 调用后端API，以图表（热力图、折线图等）的形式动态、直观地展示数据分析结果。

---

## 🚀 如何运行

<!-- 提示：这是README中非常重要的部分。请根据你的项目实际情况，尽可能详细地填写。如果项目不能直接运行，可以说明原因或只展示核心代码。 -->

### 1. 环境依赖

*   Python 3.8+
*   Java 1.8+ (用于 Kafka & Spark)
*   Scrapy
*   Spark 3.x
*   Kafka 2.x
*   MySQL 5.7+
*   Redis
*   ... (其他Python库，如 Flask, pandas, sqlalchemy 等)

建议使用 `pip` 安装 Python 依赖：
```bash
pip install -r requirements.txt 
```
<!-- 提示：在你的项目中执行 `pip freeze > requirements.txt` 可以自动生成这个文件。 -->

### 2. 配置说明

*   **爬虫配置**: 修改 `vegetable_price/settings.py` 中的数据库和Redis连接信息。
*   **后端配置**: 修改 `demo2/app.py` 中的数据库连接信息。
*   ...

### 3. 启动步骤

1.  **启动依赖服务**:
    *   启动 Zookeeper
    *   启动 Kafka
    *   启动 Redis
    *   启动 MySQL

2.  **启动爬虫**:
    ```bash
    # 在 vegetable_price 目录下
    scrapy crawl mofcom
    ```

3.  **启动Spark Streaming任务**:
    ```bash
    # 提交Spark任务
    spark-submit kafka/kafka_streaming.py
    ```

4.  **启动后端Flask服务**:
    ```bash
    python demo2/app.py
    ```

5.  **访问前端页面**:
    在浏览器中打开 `http://127.0.0.1:5000` 即可查看。

