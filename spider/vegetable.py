import requests
from bs4 import BeautifulSoup
import pymysql
import datetime
import time
import random
import logging
from urllib.parse import urlencode
from typing import List, Tuple

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("vegetable_crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("vegetable_crawler")

# 数据库连接配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'vegetable_market',
    'charset': 'utf8mb4'
}

# 蔬菜列表及对应的ID（精简版）
VEGETABLES = {
    "油菜": "170020",
    "芹菜": "170040",
    "生菜": "170050",
    "韭菜": "170480",
    "洋葱": "170090",
    "大白菜": "170060",
    "圆白菜": "170010",
    "白萝卜": "170070",
    "大葱": "170250",
    "胡萝卜": "170260",
    "土豆": "170080",
    "莲藕": "170270",
    "莴笋": "170280",
    "绿豆芽": "170290",
    "蒜头": "170100",
    "西红柿": "170120",
    "生姜": "170110",
    "茄子": "170140",
    "尖椒": "170150",
    "青椒": "170160",
    "黄瓜": "170130",
    "冬瓜": "170180",
    "苦瓜": "170200",
    "西葫芦": "170330",
    "西兰花": "170340"
}

# 用户代理池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
]


class VegetablePriceCrawler:
    """优化的蔬菜价格爬虫"""

    def __init__(self):
        """初始化爬虫"""
        self.conn = None
        self.session = requests.Session()
        self.session.headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Referer': 'https://cif.mofcom.gov.cn/cif/index.fhtml'
        }

    def connect_database(self):
        """连接数据库"""
        try:
            self.conn = pymysql.connect(**DB_CONFIG)
            logger.info("成功连接到数据库")
            return True
        except pymysql.Error as e:
            logger.error(f"数据库连接失败: {e}")
            return False

    def generate_date_range(self, start_date: str, end_date: str) -> List[str]:
        """生成日期范围列表"""
        try:
            start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.datetime.strptime(end_date, "%Y-%m-%d")

            date_range = []
            current_date = start
            while current_date <= end:
                date_range.append(current_date.strftime("%Y-%m-%d"))
                current_date += datetime.timedelta(days=1)

            logger.info(f"生成日期范围: {start_date} 到 {end_date}, 共 {len(date_range)} 天")
            return date_range
        except Exception as e:
            logger.error(f"生成日期范围失败: {e}")
            return []

    def fetch_vegetable_data(self, vegetable_name: str, date: str, retry=3) -> List[Tuple]:
        """获取指定蔬菜在指定日期的价格数据"""
        vegetable_id = VEGETABLES.get(vegetable_name)
        if not vegetable_id:
            logger.error(f"未找到蔬菜ID: {vegetable_name}")
            return []

        params = {
            'commdityid': vegetable_id,
            'date': date
        }
        url = f"https://cif.mofcom.gov.cn/cif/seach.fhtml?{urlencode(params)}"

        # 随机切换User-Agent
        self.session.headers['User-Agent'] = random.choice(USER_AGENTS)

        logger.info(f"正在爬取 {vegetable_name} - {date}")

        for attempt in range(retry):
            try:
                response = self.session.get(
                    url,
                    timeout=15,
                    verify=False
                )
                response.raise_for_status()
                response.encoding = 'utf-8'

                soup = BeautifulSoup(response.text, 'html.parser')
                return self.parse_html(soup, date, vegetable_name)
            except requests.RequestException as e:
                logger.error(f"请求错误 [{vegetable_name} {date}][尝试 {attempt + 1}/{retry}]: {e}")
                if attempt < retry - 1:
                    wait_time = random.uniform(3, 8)  # 缩短重试等待时间
                    logger.info(f"等待 {wait_time:.2f} 秒后重试...")
                    time.sleep(wait_time)
            except Exception as e:
                logger.error(f"解析错误 [{vegetable_name} {date}][尝试 {attempt + 1}/{retry}]: {e}")

        logger.error(f"爬取失败: {vegetable_name} {date}")
        return []

    def parse_html(self, soup: BeautifulSoup, date: str, vegetable_name: str) -> List[Tuple]:
        """解析HTML获取价格数据"""
        data = []
        table = soup.find('table')

        if not table:
            logger.warning(f"未找到表格数据 [{vegetable_name} {date}]")
            return data

        rows = table.find_all('tr')[1:]  # 跳过表头行

        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 5:  # 确保有足够的列
                region = cols[0].text.strip()
                market = cols[1].text.strip()

                # 处理价格数据
                current_price = self._parse_price(cols[2].text.strip())
                previous_price = self._parse_price(cols[3].text.strip())

                # 处理环比数据
                mom = self._parse_mom(cols[4].text.strip())

                # 按指定字段顺序构建数据行
                data.append((
                    vegetable_name,
                    region,
                    market,
                    current_price,
                    previous_price,
                    mom,
                    date
                ))

        logger.info(f"成功解析 {len(data)} 条 {vegetable_name} 数据: {date}")
        return data

    def _parse_price(self, price_str: str) -> float:
        """解析价格字符串为浮点数"""
        try:
            return float(price_str) if price_str else None
        except ValueError:
            logger.warning(f"无法解析价格: {price_str}")
            return None

    def _parse_mom(self, mom_str: str) -> float:
        """解析环比字符串为浮点数"""
        if not mom_str:
            return None

        # 去除百分号
        mom_str = mom_str.replace('%', '')
        try:
            return float(mom_str)
        except ValueError:
            logger.warning(f"无法解析环比: {mom_str}")
            return None

    def save_to_database(self, data: List[Tuple]):
        """将数据保存到数据库"""
        if not data:
            return False

        if not self.conn:
            if not self.connect_database():
                return False

        try:
            with self.conn.cursor() as cursor:
                sql = """
                INSERT INTO vegetable_prices 
                (vegetable_name, region, market, current_price, previous_price, mom, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                current_price = VALUES(current_price),
                previous_price = VALUES(previous_price),
                mom = VALUES(mom)
                """
                cursor.executemany(sql, data)

            self.conn.commit()
            logger.info(f"成功保存 {len(data)} 条数据到数据库")
            return True
        except pymysql.Error as e:
            self.conn.rollback()
            logger.error(f"保存数据到数据库失败: {e}")
            return False

    def crawl_all_vegetables(self, start_date: str, end_date: str):
        """爬取所有蔬菜在指定日期范围内的数据"""
        if not self.connect_database():
            return False

        date_range = self.generate_date_range(start_date, end_date)
        if not date_range:
            return False

        # 遍历所有蔬菜
        for veg_name in VEGETABLES:
            logger.info(f"开始爬取蔬菜: {veg_name}")

            # 遍历所有日期
            for date in date_range:
                # 爬取数据
                veg_data = self.fetch_vegetable_data(veg_name, date)

                # 保存数据
                if veg_data:
                    self.save_to_database(veg_data)

                # 随机延时，避免频繁请求
                delay = random.uniform(3, 8)  # 缩短等待时间
                time.sleep(delay)

            # 蔬菜之间增加延时
            long_delay = random.uniform(15, 30)
            logger.info(f"完成一种蔬菜爬取，等待 {long_delay:.2f} 秒后继续...")
            time.sleep(long_delay)

        logger.info("所有蔬菜价格数据爬取完成")
        return True

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")


def main():
    """主函数"""
    logger.info("===== 蔬菜价格爬虫程序启动 =====")

    # 设置爬取日期范围 (2025年5月1日到2025年5月31日)
    start_date = "2025-05-01"
    end_date = "2025-05-31"

    crawler = VegetablePriceCrawler()
    try:
        crawler.crawl_all_vegetables(start_date, end_date)
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"爬取过程中发生错误: {e}")
    finally:
        crawler.close()
        logger.info("===== 蔬菜价格爬虫程序结束 =====")


if __name__ == "__main__":
    main()