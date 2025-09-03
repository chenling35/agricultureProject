# 定义 Scrapy 爬虫中间件模块
# 包含多个中间件类，其中 RedisCacheDownloaderMiddleware 用于实现基于 Redis 的响应缓存机制

from scrapy import signals
from itemadapter import ItemAdapter
import redis  # 引入 Redis 模块，用于缓存 HTTP 响应
import logging  # 引入日志模块，记录运行信息
from scrapy.exceptions import IgnoreRequest  # 忽略请求异常
from twisted.internet.defer import Deferred, succeed  # Twisted 相关（异步处理）
from twisted.internet import reactor
from scrapy.http import HtmlResponse  # 用于构造 HTML 响应对象

logger = logging.getLogger(__name__)  # 初始化日志器

# Redis 默认配置
# 注意：这里的 decode_responses 默认值很重要，它决定了从 Redis 取出的数据类型
REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'decode_responses': False  # <--- 将默认值改为 False，这样更符合 Scrapy Response.body 的预期
}

class RedisCacheDownloaderMiddleware:
    def __init__(self, host, port, db, decode_responses, expire_seconds):
        """
        初始化 Redis 客户端连接
        :param host: Redis 主机地址
        :param port: Redis 端口
        :param db: 数据库编号
        :param decode_responses: 是否自动解码响应数据 (从 settings 获取，或使用 REDIS_CONFIG 的新默认值)
        :param expire_seconds: 缓存过期时间（秒）
        """
        self.decode_responses = decode_responses # 保存这个设置，用于后续判断
        self.expire_seconds = expire_seconds
        self.redis_client = None
        try:
            # 确保这里的 decode_responses 参数和从 settings 读取的配置一致
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                decode_responses=self.decode_responses # 使用从 settings 读取或默认的 decode_responses
            )
            self.redis_client.ping()  # 测试连接是否成功
            logger.info("Redis连接成功 (RedisCacheDownloaderMiddleware)")
        except redis.ConnectionError as e:
            logger.error(f"Redis连接失败 (RedisCacheDownloaderMiddleware): {e}")
            self.redis_client = None  # 连接失败时设为 None

    @classmethod
    def from_crawler(cls, crawler):
        """
        Scrapy 使用该方法创建中间件实例
        :param crawler: Scrapy 的爬虫控制器
        """
        settings = crawler.settings
        # 确保从 settings 读取 REDIS_DECODE_RESPONSES，如果 settings 中没有，则使用 REDIS_CONFIG 的默认值 (False)
        decode_responses_setting = settings.getbool('REDIS_DECODE_RESPONSES', REDIS_CONFIG['decode_responses'])
        return cls(
            host=settings.get('REDIS_HOST', REDIS_CONFIG['host']),
            port=settings.getint('REDIS_PORT', REDIS_CONFIG['port']),
            db=settings.getint('REDIS_DB', REDIS_CONFIG['db']),
            decode_responses=decode_responses_setting, # 使用从 settings 读取的配置
            expire_seconds=settings.getint('REDIS_CACHE_EXPIRE_SECONDS', 3600)  # 默认缓存1小时
        )

    def process_request(self, request, spider):
        """
        在请求发送前尝试从 Redis 中获取缓存响应
        :param request: 当前请求对象
        :param spider: 爬虫实例
        :return: 返回缓存的 Response 或 None
        """
        if not self.redis_client:
            logger.warning("Redis连接未建立，跳过缓存处理")
            return None

        cache_key = request.meta.get('request_cache_key')
        if not cache_key:
            return None

        cached_response_body = self.redis_client.get(cache_key)
        if cached_response_body:
            logger.info(f"命中Redis缓存: {cache_key}")

            # >>> 核心修改在这里 <<<
            # 根据 self.decode_responses (从 settings.py 获取) 的值来处理 cached_response_body
            if self.decode_responses:
                # 如果 Redis 客户端自动解码了，那么 cached_response_body 是字符串 (str)
                # HtmlResponse 的 body 参数需要 bytes，所以需要编码
                body_bytes = cached_response_body.encode('utf-8')
            else:
                # 如果 Redis 客户端没有自动解码，那么 cached_response_body 已经是字节 (bytes)
                # 直接使用即可，不需要再次编码
                body_bytes = cached_response_body

            response = HtmlResponse(
                url=request.url,
                status=200, # 假设缓存响应总是成功
                body=body_bytes, # 使用处理后的字节数据
                encoding='utf-8', # 根据目标网站的实际编码设置，通常是 utf-8
                request=request
            )
            response.flags.append('cached')  # 标记为缓存响应
            return response

        return None

    def process_response(self, request, response, spider):
        """
        请求完成后，若状态码200且非缓存响应，则将结果写入 Redis 缓存
        :param request: 请求对象
        :param response: 响应对象
        :param spider: 爬虫实例
        :return: 原始或修改后的响应
        """
        if not self.redis_client:
            return response

        cache_key = request.meta.get('request_cache_key')
        if cache_key and response.status == 200 and 'cached' not in response.flags:
            # 存储到 Redis 的总是原始的响应体字节数据 (response.body)
            # 这样在读取时，无论 REDIS_DECODE_RESPONSES 是 True 还是 False，
            # 都能通过 process_request 中的逻辑正确处理。
            if "没有找到相关数据" not in response.text: # response.text 是字符串，可以正常判断
                # setex 期望 value 是 bytes 或 str。如果 decode_responses=True，它会再次编码。
                # 如果 decode_responses=False，它会直接存储 bytes。
                # 保持 response.body 写入是最安全的，它始终是 bytes。
                self.redis_client.setex(cache_key, self.expire_seconds, response.body)
                logger.debug(f"已将响应存入Redis缓存: {cache_key}")
            else:
                logger.debug(f"页面无数据，不缓存: {cache_key}")
        return response

    def process_exception(self, request, exception, spider):
        """
        处理请求异常，记录错误信息
        :param request: 出现异常的请求
        :param exception: 异常对象
        :param spider: 爬虫实例
        :return: None，表示继续由其他中间件处理异常
        """
        logger.error(f"下载请求异常: {exception}, URL: {request.url}")
        return None