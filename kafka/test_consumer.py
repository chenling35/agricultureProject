import json
import pymysql
import logging
from kafka import KafkaConsumer
from datetime import datetime


class VegetableKafkaConsumer:
    def __init__(self):
        # 数据库配置
        self.db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': '135781012zhu',
            'database': 'vegetable_market',
            'charset': 'utf8mb4'
        }

        # Kafka消费者配置
        self.consumer = KafkaConsumer(
            'vegetable-prices',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='vegetable-consumer-group',
            auto_offset_reset='latest',
            enable_auto_commit=True
        )

        self.conn = None
        self.cursor = None

    def connect_db(self):
        """连接数据库"""
        try:
            self.conn = pymysql.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("✅ 数据库连接成功")
            return True
        except Exception as e:
            print(f"❌ 数据库连接失败: {e}")
            return False

    def insert_single_item(self, item):
        """实时插入单条数据"""
        try:
            # --- 核心修改：更新SQL语句中的列名以匹配数据库表 structure ---
            sql = """
            INSERT INTO vegetable_prices1
            (vegetable, region, market, price, previous_price, change_percentage, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            price = VALUES(price),
            previous_price = VALUES(previous_price),
            change_percentage = VALUES(change_percentage)
            """

            # --- 核心修改：更新 values 的映射，使其与 Item 的键和 SQL 列名对应 ---
            values = (
                item['vegetable_name'],    # 对应数据库列 `vegetable`
                item['region'],            # 对应数据库列 `region`
                item['market'],            # 对应数据库列 `market`
                item['current_price'],     # 对应数据库列 `price`
                item['previous_price'],    # 对应数据库列 `previous_price`
                item['mom'],               # 对应数据库列 `change_percentage`
                item['date']               # 对应数据库列 `date`
            )
            # --- 修改结束 ---

            self.cursor.execute(sql, values)
            self.conn.commit()
            return True

        except Exception as e:
            print(f"❌ 插入数据失败: {e}")
            if self.conn:
                self.conn.rollback()
            return False

    def start_consuming(self):
        """开始消费Kafka消息"""
        if not self.connect_db():
            return

        print("🎯 开始实时消费Kafka消息...")
        print("📊 等待数据中...")

        try:
            for message in self.consumer:
                try:
                    data = message.value

                    # 实时插入数据库
                    if self.insert_single_item(data):
                        print(f"✅ 已存储: {data['vegetable_name']} - {data['region']} - ¥{data['current_price']}")
                    else:
                        print(f"❌ 存储失败: {data['vegetable_name']} - {data['region']}")

                except Exception as e:
                    print(f"❌ 处理消息失败: {e}")

        except KeyboardInterrupt:
            print("\n🛑 停止消费...")
        except Exception as e:
            print(f"❌ 消费过程出错: {e}")
        finally:
            if self.conn:
                self.conn.close()
                print("✅ 数据库连接已关闭")


if __name__ == "__main__":
    consumer = VegetableKafkaConsumer()
    consumer.start_consuming()