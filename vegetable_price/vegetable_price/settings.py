# Scrapy settings for vegetable_price project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "vegetable_price"

SPIDER_MODULES = ["vegetable_price.spiders"]
NEWSPIDER_MODULE = "vegetable_price.spiders"

# ADDONS = {} # 这个通常不需要，可以移除或注释掉

# -----------------------------------------------------------------------------
# Scrapy-Redis 分布式爬取核心设置
# -----------------------------------------------------------------------------

# 启用 Scrapy-Redis 调度器。所有爬虫实例将从 Redis 队列中获取请求。
SCHEDULER = "scrapy_redis.scheduler.Scheduler"

# 调度器是否持久化请求队列。如果为 True，Redis 队列在爬虫停止后不会被清空，
# 允许暂停和恢复爬取，也允许多个爬虫实例共享队列，实现断点续爬。
SCHEDULER_PERSIST = True

# 指定 Redis 服务器的连接 URL。
# **非常重要：请将 '192.168.88.101' 替换为您集群中运行 Redis 服务的 node1 的实际IP地址。**
REDIS_URL = 'redis://192.168.88.101:6379'

# >>>>>>> 新增或修改的编码设置，解决 UnicodeDecodeError <<<<<<<
REDIS_ENCODING = 'latin1' # 明确设置 Redis 编码为 latin1，以处理非 UTF-8 字节

# 启用 Redis-based 的去重过滤器。
# 这确保了在集群中的所有爬虫实例之间，相同的请求只会被处理一次，避免重复抓取。
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"

# (可选) 指定 Redis Key 的前缀。如果您在同一个 Redis 实例上运行多个 Scrapy-Redis 项目，
# 这可以帮助您区分不同项目的数据。
# REDIS_KEY = 'vegetable_price:requests'
# REDIS_DUPEFILTER_KEY = 'vegetable_price:dupefilter'

# -----------------------------------------------------------------------------
# 数据库配置 (您已决定不再直接写入MySQL，这些配置可以注释掉或移除)
# -----------------------------------------------------------------------------
# MYSQL_HOST = '192.168.88.101'
# MYSQL_USER = 'root'
# MYSQL_PASSWORD = '123456'
# MYSQL_DATABASE = 'vegetable_market'

# -----------------------------------------------------------------------------
# Redis 配置 (用于您的 RedisCacheDownloaderMiddleware)
# -----------------------------------------------------------------------------
# **这个设置必须与 REDIS_URL 中指向的 Redis 服务器一致。**
REDIS_HOST = '192.168.88.101'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_DECODE_RESPONSES = False # 保持 False，因为您在管道中处理编码
REDIS_CACHE_EXPIRE_SECONDS = 3600 # 缓存过期时间，单位秒 (例如1小时)

# -----------------------------------------------------------------------------
# Kafka 配置 (核心修改，用于将数据写入Kafka)
# -----------------------------------------------------------------------------
# **非常重要：请将这里的IP地址和端口替换为您实际 Kafka Broker 的 IP 地址和端口。**
# 例如，如果您的Kafka Broker在三台机器上，IP分别是 192.168.88.101, 192.168.88.102, 192.168.88.103，
# 并且都监听默认端口 9092，那么就配置为列表形式。
KAFKA_BROKERS = ['192.168.88.101:9092', '192.168.88.102:9092', '192.168.88.103:9092']
KAFKA_TOPIC = 'vegetable_prices_raw' # 您希望数据发送到的 Kafka Topic 名称 (建议使用更具描述性的名称)

# -----------------------------------------------------------------------------
# 并发与延迟控制
# -----------------------------------------------------------------------------
CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1.0
RANDOMIZE_DOWNLOAD_DELAY = True

# -----------------------------------------------------------------------------
# Autothrottle 自动限速
# -----------------------------------------------------------------------------
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 2.0
AUTOTHROTTLE_MAX_DELAY = 10.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
AUTOTHROTTLE_DEBUG = False

# -----------------------------------------------------------------------------
# 重试设置
# -----------------------------------------------------------------------------
RETRY_ENABLED = True
RETRY_TIMES = 5
RETRY_HTTP_CODES = [500, 502, 503, 504, 400, 403, 404, 408, 429]

# -----------------------------------------------------------------------------
# 下载器中间件
# -----------------------------------------------------------------------------
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
    'vegetable_price.middlewares.RedisCacheDownloaderMiddleware': 100,
}

# -----------------------------------------------------------------------------
# 日志设置
# -----------------------------------------------------------------------------
LOG_LEVEL = 'INFO'
LOG_FILE = '/export/python/vegetable_price/vegetable_crawler.log'
LOG_STDOUT = True
LOG_FORMAT = '%(levelname)s: %(asctime)s - %(name)s - %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

# -----------------------------------------------------------------------------
# 遵守robots.txt 规则
# -----------------------------------------------------------------------------
ROBOTSTXT_OBEY = True

# -----------------------------------------------------------------------------
# 管道配置 (核心修改，启用 KafkaPipeline 并禁用 MySQLPipeline)
# -----------------------------------------------------------------------------
ITEM_PIPELINES = {
    'vegetable_price.pipelines.KafkaPipeline': 300, # 启用 Kafka 管道，优先级设置为300
    # 'vegetable_price.pipelines.MySQLPipeline': None, # 显式禁用 MySQL 管道 (如果管道文件中还存在该类，请保留此行)
}

# -----------------------------------------------------------------------------
# 其他通用Scrapy默认设置
# -----------------------------------------------------------------------------
FEED_EXPORT_ENCODING = "utf-8"