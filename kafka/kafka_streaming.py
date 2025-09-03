import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, trim, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
import pymysql
import datetime # 导入 datetime 模块，用于处理可能为 None 的日期

# --- 配置变量 (请根据您的环境修改) ---
# 阿里云 RDS MySQL 连接信息
DB_CONFIG = {
    'host': 'rm-cn-ow74bdsj90001x5o.rwlb.rds.aliyuncs.com', # 替换为您的RDS真正的外网连接地址
    'port': 3306,
    'user': 'adook',
    'password': 'Ykq123999',
    'database': 'vegetable_market',
    'charset': 'utf8mb4'
}

# Kafka 集群的启动服务器列表
KAFKA_BOOTSTRAP_SERVERS = "192.168.88.101:9092" # Kafka broker 地址
# Kafka 主题名称
KAFKA_TOPIC = "vegetable_prices_raw"

# --- 创建 SparkSession ---
# 注意：已移除对 MySQL JDBC 驱动 JAR 包的 extraClassPath 配置，
# 因为我们将完全依赖 Python 的 pymysql 进行数据库操作。
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingToAliyunMySQLUpsert") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # 设置日志级别，减少不必要的INFO日志

# --- 1. 定义数据 Schema (数据结构) ---
# 这是从 Kafka 读取的 JSON 消息的预期结构
input_data_schema = StructType([
    StructField("id", IntegerType(), True), # Kafka数据中的id
    StructField("variety", StringType(), True),
    StructField("area", StringType(), True),
    StructField("market", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("previous_price", DoubleType(), True),
    StructField("mom", DoubleType(), True),
    # time_price 类型为 DateType
    StructField("time_price", DateType(), True)
])

# --- 2. 从 Kafka 读取数据流 ---
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# --- 3. 解析 Kafka 消息中的 JSON 字符串 ---
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value", "timestamp AS event_time") \
                        .select(from_json(col("json_value"), input_data_schema).alias("data"), col("event_time")) \
                        .select("data.*", "event_time")

# --- 4. 数据清洗阶段 ---
cleaned_df = parsed_df.withColumn("processing_time", current_timestamp())

# 常见的清洗操作示例：
# 确保 price 和 time_price 不为空
cleaned_df = cleaned_df.na.drop(subset=["price", "time_price"])
# 过滤 price 和 previous_price 大于 0 的记录
cleaned_df = cleaned_df.filter(col("price").isNotNull() & (col("price") > 0))
cleaned_df = cleaned_df.filter(col("previous_price").isNotNull() & (col("previous_price") > 0))

# 去除字符串字段的前后空格
cleaned_df = cleaned_df.withColumn("variety", trim(col("variety")))
cleaned_df = cleaned_df.withColumn("area", trim(col("area")))
cleaned_df = cleaned_df.withColumn("market", trim(col("market")))


# --- 5. 定义数据写入 MySQL 的函数 (实现 Upsert 逻辑) ---
def write_db_batch(df, table_name, db_config, epoch_id): # 添加 db_config 参数
    """
    将 Spark DataFrame 批次写入 MySQL，实现 UPSERT (插入或更新) 逻辑。
    此函数在 Spark Driver 上运行，并使用纯 Python 的 pymysql。
    """
    if df.isEmpty(): # 使用 isEmpty() 检查 DataFrame 是否为空
        print(f"Batch {epoch_id} for table {table_name} is empty, skipping write to MySQL.")
        return

    print(f"Writing batch {epoch_id} to MySQL table: {table_name} with upsert logic using pymysql...")
    conn = None
    cursor = None
    try:
        # 选择与 MySQL 表匹配的列。id 是 AUTO_INCREMENT，所以我们不插入它
        df_to_write = df.select(
            col("variety"),
            col("area"),
            col("market"),
            col("price"),
            col("previous_price"),
            col("mom"),
            col("time_price")
        )

        # 将 Spark DataFrame 转换为 Pandas DataFrame
        # 注意：toPandas() 会将批次数据收集到 Driver 内存中，适合中小型批次。
        # 如果批次非常大，可以考虑使用 df.foreachPartition() 来分布式写入，但这会使代码更复杂。
        pandas_df = df_to_write.toPandas()

        # 建立 MySQL 连接 (使用 pymysql)
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'], # 明确指定端口
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset=db_config['charset']
        )
        cursor = conn.cursor()

        # SQL 模板：INSERT ... ON DUPLICATE KEY UPDATE
        # 根据您提供的 CREATE TABLE 语句，(time_price, market, area, variety) 是唯一键
        sql_template = f"""
        INSERT INTO {table_name} (variety, area, market, price, previous_price, mom, time_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            price = VALUES(price),
            previous_price = VALUES(previous_price),
            mom = VALUES(mom),
            time_price = VALUES(time_price);
        """

        # 准备数据以批量执行
        data_to_insert = []
        for index, row in pandas_df.iterrows():
            # time_price 是 DateType，转换为 'YYYY-MM-DD' 格式字符串，因为 pymysql 需要字符串或 datetime 对象
            time_price_val = row['time_price']
            time_price_formatted = None
            if isinstance(time_price_val, datetime.date):
                time_price_formatted = time_price_val.strftime('%Y-%m-%d')
            # 如果 time_price_val 是 None，time_price_formatted 仍为 None，pymysql 可以处理 None

            data_to_insert.append((
                row['variety'],
                row['area'],
                row['market'],
                row['price'],
                row['previous_price'],
                row['mom'],
                time_price_formatted # 使用格式化后的日期或 None
            ))

        if data_to_insert:
            # 使用 executemany 批量执行 INSERT ... ON DUPLICATE KEY UPDATE
            cursor.executemany(sql_template, data_to_insert)
            conn.commit() # 提交事务
            print(f"Batch {epoch_id} written to MySQL table {table_name} successfully with upsert logic.")
        else:
            print(f"Batch {epoch_id} for table {table_name} has no valid data rows to write after processing.")

    except pymysql.Error as err:
        print(f"MySQL Error during batch {epoch_id} write to {table_name}: {err}")
        if conn:
            conn.rollback() # 回滚事务
        # 抛出异常，让 Spark Structured Streaming 知道这个批次处理失败。
        # Spark 将会根据配置进行重试。
        raise
    except Exception as e:
        print(f"General Error processing batch {epoch_id} for table {table_name}: {e}")
        raise # 抛出异常

    finally:
        # 确保连接和游标被关闭
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# --- 6. 启动 StreamingQuery ---
query_cleaned = cleaned_df \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .foreachBatch(lambda batch_df, batch_id: (
        write_db_batch(batch_df, "vegetable_prices1", DB_CONFIG, batch_id) # 传入 DB_CONFIG
    )) \
    .start()

print("Spark Streaming 应用程序已启动。正在监听 Kafka 并处理数据...")
print(f"清洗后的原始数据将通过 Upsert 逻辑写入 MySQL 表 '{DB_CONFIG['database']}.vegetable_prices1'")

# 等待流查询终止。
query_cleaned.awaitTermination()