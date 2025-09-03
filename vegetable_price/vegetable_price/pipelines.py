# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


import pymysql # 如果您不再需要MySQL功能，可以移除此导入
import logging
import json # 用于Kafka的JSON序列化
from kafka import KafkaProducer # 导入Kafka生产者
from kafka.errors import KafkaError # 导入Kafka错误类型
from datetime import datetime

# 获取日志记录器，方便调试
logger = logging.getLogger(__name__)

# --- KafkaPipeline：主要修改和优化在这里 ---
class KafkaPipeline:
    def __init__(self, kafka_brokers, kafka_topic):
        # 确保 kafka_brokers 是一个列表，直接赋值
        self.kafka_brokers = kafka_brokers
        self.kafka_topic = kafka_topic
        self.producer = None
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        # 使用 getlist() 方法从 settings 获取 KAFKA_BROKERS，确保它是一个列表
        return cls(
            kafka_brokers=crawler.settings.getlist('KAFKA_BROKERS'),
            kafka_topic=crawler.settings.get('KAFKA_TOPIC', 'vegetable-prices') # 默认值'vegetable-prices'
        )

    def open_spider(self, spider):
        """Spider启动时初始化Kafka生产者"""
        if not self.kafka_brokers:
            self.logger.error("KAFKA_BROKERS 未配置或为空。Kafka 生产者将不会被初始化。")
            self.producer = None # 确保连接不成功时生产者为None
            return

        try:
            # 这里的 bootstrap_servers 直接使用从 settings 传入的列表
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_brokers,
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False, default=str).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                # Kafka生产者配置：为了高可靠性通常这样设置
                acks='all',  # 确保所有ISR（in-sync replicas）都确认消息，提供最高可靠性
                retries=9999999,   # 几乎无限重试，以防瞬时网络问题导致消息丢失 (可能会导致重复消息)
                # 以下参数可以根据您的吞吐量和延迟需求进行调整
                batch_size=16384,  # 默认值 16KB，如果您的消息很小且数量大，可以适当减小
                linger_ms=10,      # 默认0，生产者会等待10毫秒，以聚合更多消息再发送，降低请求次数
                request_timeout_ms=30000, # 客户端请求超时时间，默认30秒
                api_version=(0, 10, 1) # 根据您的Kafka版本调整，确保兼容性
            )
            # 尝试发送一个空消息来验证连接（这是一个非阻塞的发送，等待其结果）
            # future = self.producer.send(self.kafka_topic, b'{}') # 发送一个空JSON对象验证
            # future.get(timeout=5) # 等待5秒确认发送结果，如果失败会抛出异常
            self.logger.info(f"Kafka生产者连接成功: {self.kafka_brokers}")
        except KafkaError as e: # 捕获Kafka特定的错误
            self.logger.error(f"Kafka生产者初始化失败 (KafkaError): {e}", exc_info=True)
            self.producer = None
        except Exception as e: # 捕获其他所有可能的异常
            self.logger.error(f"Kafka生产者初始化失败 (通用错误): {e}", exc_info=True)
            self.producer = None

    def close_spider(self, spider):
        """Spider关闭时清理资源"""
        if self.producer:
            self.producer.flush()  # 确保所有在缓冲区中的消息都发送出去
            self.producer.close()  # 关闭生产者
            self.logger.info("Kafka生产者已关闭")

    def process_item(self, item, spider):
        """处理每个item，将其发送到Kafka"""
        if not self.producer:
            self.logger.warning("Kafka生产者未初始化或连接失败，此Item将被丢弃。")
            from scrapy.exceptions import DropItem # 确保导入 DropItem
            raise DropItem("Kafka producer not available.") # 如果生产者不可用，则丢弃Item

        try:
            # 转换item为字典，确保所有数据都是Python原生类型
            item_dict = ItemAdapter(item).asdict() # 使用ItemAdapter.asdict() 转换为字典更规范

            # 添加时间戳和爬虫名称，便于后续处理和追溯
            item_dict['scraped_at'] = datetime.now().isoformat()
            item_dict['spider_name'] = spider.name

            # 创建消息key（用于Kafka分区，确保相同key的消息进入同一分区，保证顺序性）
            # 确保组合Key的各部分都是字符串
            message_key = f"{item_dict.get('variety', 'unknown')}_{item_dict.get('area', 'unknown')}_{item_dict.get('time_price', 'unknown')}"

            # 发送到Kafka
            future = self.producer.send(
                self.kafka_topic,
                value=item_dict, # 已通过 value_serializer 转换为 JSON 字节
                key=message_key # 已通过 key_serializer 转换为字节
            )

            # 异步处理发送结果，避免阻塞
            future.add_callback(self.on_send_success)
            future.add_errback(self.on_send_error)

            self.logger.debug(f"已将 Item 添加到 Kafka 发送队列: {item_dict.get('variety', '')} - {item_dict.get('area', '')} - {item_dict.get('time_price', '')}")

        except Exception as e:
            self.logger.error(f"处理或发送Item到Kafka时发生错误: {e}. Item: {item}", exc_info=True)
            from scrapy.exceptions import DropItem # 确保导入 DropItem
            raise DropItem(f"Error processing/sending item to Kafka: {e}") # 发生异常时丢弃Item

        return item

    def on_send_success(self, record_metadata):
        """发送成功回调函数"""
        self.logger.debug(
            f"Kafka消息发送成功: topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")

    def on_send_error(self, excp):
        """发送失败回调函数"""
        self.logger.error(f"Kafka消息发送失败: {excp}", exc_info=True)