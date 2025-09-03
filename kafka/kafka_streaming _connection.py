from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, trim, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
import pymysql # <-- 核心修改点1: 导入 pymysql
import datetime # 导入 datetime 模块，用于处理可能为 None 的日期

# --- 配置变量 (请根据您的环境修改) ---
# MySQL 数据库主机 IP 地址
LOCAL_MYSQL_HOST = "192.168.88.101" # <-- 确保这是 node1 的正确 IP
# MySQL 数据库名称
MYSQL_DATABASE = "vegetable_market" # <-- 目标数据库名称
# 使用 root 用户 (安全警告：生产环境慎用)
MYSQL_USER = "root"
MYSQL_PASSWORD = "123456" # 请替换为您的实际密码！

# Kafka 集群的启动服务器列表
KAFKA_BOOTSTRAP_SERVERS = "node1:9092"
# Kafka 主题名称
KAFKA_TOPIC = "vegetable_prices_raw"

# --- 创建 SparkSession ---
# 注意：已移除对 MySQL JDBC 驱动 JAR 包的 extraClassPath 配置，
# 因为我们将完全依赖 Python 的 pymysql 进行数据库操作。
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingToNode1MySQLUpsert") \
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
cleaned_df = cleaned_df.na.drop(subset=["price", "time_price"])
cleaned_df = cleaned_df.filter(col("price").isNotNull() & (col("price") > 0))
cleaned_df = cleaned_df.filter(col("previous_price").isNotNull() & (col("previous_price") > 0))

cleaned_df = cleaned_df.withColumn("variety", trim(col("variety")))
cleaned_df = cleaned_df.withColumn("area", trim(col("area")))
cleaned_df = cleaned_df.withColumn("market", trim(col("market")))


# --- 5. 定义数据写入 MySQL 的函数 (实现 Upsert 逻辑) ---
def write_db_batch(df, table_name, epoch_id): # 函数名改为 write_db_batch 更通用
    """
    将 Spark DataFrame 批次写入 MySQL，实现 UPSERT (插入或更新) 逻辑。
    此函数在 Spark Driver 上运行，并使用纯 Python 的 pymysql。
    """
    if df.count() == 0:
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
        # 注意：这会将批次数据收集到 Driver 内存中，适合中小型批次。
        # 如果批次非常大，需要考虑 df.foreachPartition()。
        pandas_df = df_to_write.toPandas()

        # 建立 MySQL 连接 (使用 pymysql)
        conn = pymysql.connect( # <-- 核心修改点2: 使用 pymysql.connect
            host=LOCAL_MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            charset='utf8mb4' # <-- pymysql 推荐指定字符集
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
            mom = VALUES(mom);
        """

        # 准备数据以批量执行
        data_to_insert = []
        for index, row in pandas_df.iterrows():
            # time_price 是 DateType，转换为 'YYYY-MM-DD' 格式字符串
            time_price_val = row['time_price']
            time_price_formatted = None
            if isinstance(time_price_val, datetime.date):
                time_price_formatted = time_price_val.strftime('%Y-%m-%d')
            # 确保其他字段也转换成适合 pymysql 的类型，一般直接传值即可

            data_to_insert.append((
                row['variety'],
                row['area'],
                row['market'],
                row['price'],
                row['previous_price'],
                row['mom'],
                time_price_formatted
            ))

        if data_to_insert:
            # 使用 executemany 批量执行 INSERT ... ON DUPLICATE KEY UPDATE
            cursor.executemany(sql_template, data_to_insert)
            conn.commit() # 提交事务
            print(f"Batch {epoch_id} written to MySQL table {table_name} successfully with upsert logic.")
        else:
            print(f"Batch {epoch_id} for table {table_name} has no data to write after conversion.")

    except pymysql.Error as err: # <-- 核心修改点3: 捕获 pymysql 的错误
        # 捕获 MySQL 相关的特定错误
        print(f"MySQL Error during batch {epoch_id} write to {table_name}: {err}")
        if conn:
            conn.rollback() # 回滚事务
        # 抛出异常，让 Spark Structured Streaming 知道这个批次处理失败。
        # Spark 将会根据配置进行重试。
        raise
    except Exception as e:
        # 捕获其他任何非 MySQL 相关的通用错误
        print(f"General Error processing batch {epoch_id} for table {table_name}: {e}")
        raise # 抛出异常

    finally:
        # 确保连接和游标被关闭
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# --- 6. 启动 StreamingQuery (清洗后的原始数据只输出到 MySQL) ---
query_cleaned = cleaned_df \
    .writeStream \
    .outputMode("append").trigger(processingTime="30 seconds").foreachBatch(lambda batch_df, batch_id: (
        write_db_batch(batch_df, "vegetable_prices1", batch_id) # <-- 调用修改后的函数名
    )) \
    .start()

print("Spark Streaming 应用程序已启动。正在监听 Kafka 并处理数据...")
print(f"清洗后的原始数据将通过 Upsert 逻辑写入 MySQL 表 '{MYSQL_DATABASE}.vegetable_prices1'")

# 等待流查询终止。
query_cleaned.awaitTermination()