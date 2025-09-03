# Scrapy settings for vegetable_price project
# ... (您的现有设置，例如 BOT_NAME, SPIDER_MODULES, NEWSPIDER_MODULE, ROBOTSTXT_OBEY 等保持不变) ...

# -----------------------------------------------------------------------------
# Scrapy-Redis 分布式爬取设置
# -----------------------------------------------------------------------------

# 启用 Scrapy-Redis 调度器。所有爬虫实例将从 Redis 队列中获取请求。
# 这是实现分布式爬取的关键一步。
SCHEDULER = "scrapy_redis.scheduler.Scheduler"

# 调度器是否持久化请求队列。如果为 True，Redis 队列在爬虫停止后不会被清空，
# 允许暂停和恢复爬取，也允许多个爬虫实例共享队列，实现断点续爬。
SCHEDULER_PERSIST = True

# 指定 Redis 服务器的连接 URL。
# **非常重要：请确保 '192.168.88.101' 是您集群中运行 Redis 服务的 node1 的实际IP地址。**
# 默认 Redis 端口是 6379。
REDIS_URL = 'redis://192.168.88.101:6379'

# 启用 Redis-based 的去重过滤器。
# 这确保了在集群中的所有爬虫实例之间，相同的请求只会被处理一次，避免重复抓取。
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"

# (可选) 指定 Redis Key 的前缀。如果您在同一个 Redis 实例上运行多个 Scrapy-Redis 项目，
# 这可以帮助您区分不同项目的数据。如果不需要，可以不设置或注释掉。
# REDIS_KEY = 'vegetable_price:requests'

# (可选) 指定 Redis 去重 Key 的前缀。
# REDIS_DUPEFILTER_KEY = 'vegetable_price:dupefilter'


# -----------------------------------------------------------------------------
# 您现有的 Redis 缓存下载中间件配置 (根据您的代码，您有一个 RedisCacheDownloaderMiddleware)
# -----------------------------------------------------------------------------
# **确保这里的 REDIS_HOST 也指向您 Redis 服务器的 IP 地址。**
REDIS_HOST = '192.168.88.101'
REDIS_PORT = 6379
REDIS_DB = 0 # 默认Redis数据库0
REDIS_DECODE_RESPONSES = True
REDIS_CACHE_EXPIRE_SECONDS = 3600 # 缓存过期时间（秒），例如1小时

# -----------------------------------------------------------------------------
# 下载中间件配置
# -----------------------------------------------------------------------------
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None, # 禁用Scrapy默认UserAgent中间件
    'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,    # 启用随机UserAgent
    'vegetable_price.middlewares.RedisCacheDownloaderMiddleware': 100, # 启用您的Redis缓存中间件，确保优先级较高
    # 'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,     # 如果使用代理池，保持启用
    # 'rotating_proxies.middlewares.BanDetectionMiddleware': 620,      # 如果使用代理池，保持启用
}

# -----------------------------------------------------------------------------
# 管道配置
# -----------------------------------------------------------------------------
# 根据您之前说的“先不考虑Kafka”，这里暂时注释掉KafkaPipeline。
# MySQLPipeline 保持启用，它将把数据写入您的MySQL数据库。
ITEM_PIPELINES = {
    # 'vegetable_price.pipelines.KafkaPipeline': 200, # 暂时注释掉 Kafka 管道
    'vegetable_price.pipelines.MySQLPipeline': 300, # 保持 MySQL 管道
}

# -----------------------------------------------------------------------------
# 其他通用Scrapy设置 (保持或根据需要调整)
# -----------------------------------------------------------------------------
# 并发请求数，在分布式环境下可以适当提高，但要注意目标网站承受能力和自身网络/CPU资源。
# 例如，如果您有3个节点，每个节点运行一个爬虫，总并发可以更高。
# CONCURRENT_REQUESTS = 32

# 下载延迟，如果抓取速度过快被封，可以增加。
# DOWNLOAD_DELAY = 1

# 启用自动限速，Scrapy会根据负载自动调整下载延迟。
# AUTOTHROTTLE_ENABLED = True
# AUTOTHROTTLE_START_DELAY = 5
# AUTOTHROTTLE_MAX_DELAY = 60
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# AUTOTHROTTLE_DEBUG = False

# 重试策略
# RETRY_ENABLED = True
# RETRY_TIMES = 3 # 失败的HTTP请求重试次数

# ... (您的其他现有设置) ...