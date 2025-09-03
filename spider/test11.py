import requests
from bs4 import BeautifulSoup
import pymysql
import datetime
import time
import random
import logging
import os

# 日志配置
if not os.path.exists("vegetable_crawler.log"):
    open("vegetable_crawler.log", "w", encoding="utf-8").close()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("vegetable_crawler.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("vegetable_crawler")

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '135781012zhu',
    'database': 'vegetable_market',
    'charset': 'utf8mb4'
}

BASE_URL = "https://cif.mofcom.gov.cn/cif/"
SEARCH_URL = "https://cif.mofcom.gov.cn/cif/seach.fhtml"

VEGETABLES = {
    "油菜": "170020",
    # 其他蔬菜...
}

class VegetablePriceCrawler:
    """蔬菜价格爬虫类，负责数据爬取、解析和存储"""

    def __init__(self):
        self.conn = None
        self.session = self._init_session()
        self._setup_database()
        self.processed_requests = set()  # 替代 Redis 的简单本地去重

    def _init_session(self):
        """初始化带随机请求头的会话"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': random.choice([
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15'
            ]),
            'Accept': 'text/html,application/xhtml+xml',
            'DNT': '1',
            'Referer': BASE_URL
        })
        return session

    def _setup_database(self):
        """数据库连接和表创建"""
        try:
            self.conn = pymysql.connect(**DB_CONFIG)
            with self.conn.cursor() as cursor:
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS vegetable_prices3 (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    种类 VARCHAR(50) NOT NULL,
                    地区 VARCHAR(100) NOT NULL,
                    市场 VARCHAR(200) NOT NULL,
                    当日价格 DECIMAL(10,2) NOT NULL,
                    前一日价格 DECIMAL(10,2) NOT NULL,
                    环比 DECIMAL(10,2) NOT NULL,
                    时间 DATE NOT NULL,
                    UNIQUE KEY (种类, 地区, 市场, 时间)
                ) CHARSET=utf8mb4
                """)
            self.conn.commit()
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise

    def fetch_data(self, vegetable_name: str, date: str):
        """获取并验证数据"""
        try:
            datetime.datetime.strptime(date, "%Y-%m-%d")
            veg_id = VEGETABLES[vegetable_name]
        except (ValueError, KeyError) as e:
            logger.error(f"参数错误: {str(e)}")
            return []

        logger.info(f"请求参数: {vegetable_name}({veg_id}) @ {date}")

        # 检查是否已处理该请求
        request_key = f"{veg_id}_{date}"
        if request_key in self.processed_requests:
            logger.info(f"跳过重复请求: {request_key}")
            return []
        self.processed_requests.add(request_key)

        # 发起请求
        params = {'commdityid': veg_id, 'date': date}
        for attempt in range(3):  # 最多尝试3次
            try:
                response = self.session.get(SEARCH_URL, params=params, timeout=30)
                response.raise_for_status()
                response.encoding = 'utf-8'

                if "没有找到相关数据" in response.text or not response.text.strip():
                    logger.warning("服务器返回无数据响应")
                    time.sleep(random.uniform(5, 10))
                    continue

                return self._parse_html(response.text, date, vegetable_name)
            except requests.RequestException as e:
                logger.error(f"请求失败: {str(e)}")
                time.sleep(random.uniform(5, 10))
        return []

    def _parse_html(self, html: str, date: str, veg_name: str):
        """增强的HTML解析方法"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find('table')
            if not table:
                logger.warning("未找到数据表格")
                return []

            rows = table.find_all('tr')[1:]  # 跳过表头
            logger.info(f"解析到{len(rows)}行数据")

            results = []
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 5:
                    data = (
                        veg_name,
                        cols[0].text.strip(),
                        cols[1].text.strip(),
                        self._parse_float(cols[2].text),
                        self._parse_float(cols[3].text),
                        self._parse_float(cols[4].text.replace('%', '')),
                        date
                    )
                    # 验证数据有效性
                    if data[3] > 0:
                        results.append(data)
                    else:
                        logger.warning(f"无效价格数据: {data}")

            # 检查数据多样性
            if len(results) > 1 and all(r[3] == results[0][3] for r in results):
                logger.warning("所有价格数据相同，可能存在问题")

            return results
        except Exception as e:
            logger.error(f"解析失败: {str(e)}")
            return []

    def _parse_float(self, text: str) -> float:
        """安全的浮点数转换"""
        try:
            return float(text.strip())
        except (ValueError, AttributeError):
            return 0.0

    def save_data(self, data):
        """数据存储"""
        if not data:
            return False

        try:
            with self.conn.cursor() as cursor:
                sql = """
                INSERT INTO vegetable_prices3
                (种类, 地区, 市场, 当日价格, 前一日价格, 环比, 时间)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                当日价格 = VALUES(当日价格),
                前一日价格 = VALUES(前一日价格),
                环比 = VALUES(环比)
                """
                cursor.executemany(sql, data)
            self.conn.commit()
            logger.info(f"成功保存{len(data)}条记录")
            return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"保存失败: {str(e)}")
            return False

    def crawl(self, days=7):
        """执行爬取任务"""
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=days)

        date_range = [
            (start_date + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((end_date - start_date).days + 1)
        ]

        for veg_name in VEGETABLES:
            logger.info(f"开始爬取 {veg_name}...")

            for date in date_range:
                data = self.fetch_data(veg_name, date)
                if data:
                    self.save_data(data)

                # 随机延迟
                delay = random.uniform(3, 8)
                time.sleep(delay)

            # 蔬菜间增加延迟
            time.sleep(random.uniform(10, 15))

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")


def main():
    logger.info("=== 蔬菜价格爬虫启动 ===")
    crawler = VegetablePriceCrawler()

    try:
        crawler.crawl(days=3)  # 爬取最近3天数据
    except KeyboardInterrupt:
        logger.info("用户中断操作")
    except Exception as e:
        logger.error(f"程序异常: {str(e)}")
    finally:
        crawler.close()
        logger.info("=== 爬虫运行结束 ===")


if __name__ == "__main__":
    main()
