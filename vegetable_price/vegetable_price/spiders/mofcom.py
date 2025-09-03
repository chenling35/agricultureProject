# vegetable_price/spiders/mofcom.py

import scrapy  # 导入 Scrapy 库，用于构建爬虫
from urllib.parse import urlencode  # 导入 urlencode，用于将字典转换为 URL 查询参数
from vegetable_price.items import VegetablePriceItem  # 导入自定义的 Item 类，用于存储爬取到的数据
import datetime  # 导入 datetime 模块，用于处理日期和时间
from bs4 import BeautifulSoup  # 导入 BeautifulSoup，用于解析 HTML
import hashlib  # 导入 hashlib，用于生成哈希值（这里用于请求缓存键）

# 蔬菜列表及对应的ID
# 这是一个字典，存储了蔬菜的名称和对应的商品ID。
# 爬虫会遍历这个字典，为每种蔬菜和每个指定日期生成请求。
VEGETABLES = {
    "油菜": "170020", "芹菜": "170040", "生菜": "170050", "韭菜": "170480",
    "洋葱": "170090", "大白菜": "170060", "圆白菜": "170010", "白萝卜": "170070",
    "大葱": "170250", "胡萝卜": "170260", "土豆": "170080", "莲藕": "170270",
    "莴笋": "170280", "绿豆芽": "170290", "蒜头": "170100", "西红柿": "170120",
    "生姜": "170110", "茄子": "170140", "尖椒": "170150", "青椒": "170160",
    "黄瓜": "170130", "冬瓜": "170180", "苦瓜": "170200", "西葫芦": "170330",
    "西兰花": "170340"
}


class MofcomSpider(scrapy.Spider):
    """
    MofcomSpider 类继承自 scrapy.Spider，用于爬取商务部网站的蔬菜价格数据。
    """
    name = 'mofcom'  # 爬虫的唯一名称
    allowed_domains = ['cif.mofcom.gov.cn']  # 允许爬取的域名，防止爬虫离开指定网站
    SEARCH_URL = "https://cif.mofcom.gov.cn/cif/seach.fhtml"  # 搜索数据的URL
    BASE_URL = "https://cif.mofcom.gov.cn/cif/"  # 网站的基URL，用于设置Referer头

    def __init__(self, start_date=None, end_date=None, *args, **kwargs):
        """
        爬虫的初始化方法。
        Args:
            start_date (str, optional): 开始日期，格式为 'YYYY-MM-DD'。如果未提供，则默认为三天前。
            end_date (str, optional): 结束日期，格式为 'YYYY-MM-DD'。如果未提供，则默认为开始日期。
        """
        super(MofcomSpider, self).__init__(*args, **kwargs)

        today = datetime.date.today()  # 获取今天的日期
        # 默认爬取三天前的数据，只爬取一天
        default_date = (today - datetime.timedelta(days=3)).strftime('%Y-%m-%d')

        if start_date and end_date:
            # 如果同时传入了开始和结束日期
            self.start_date_str = start_date
            self.end_date_str = end_date
        elif start_date:
            # 如果只传入了开始日期，默认结束日期为开始日期
            self.start_date_str = start_date
            self.end_date_str = start_date
        else:
            # 如果没有传入任何日期参数，则默认爬取三天前的一天数据
            self.start_date_str = default_date
            self.end_date_str = default_date

        # 记录爬取日期范围的日志
        self.logger.info(f"爬取日期范围: {self.start_date_str} 至 {self.end_date_str}")

    def start_requests(self):
        """
        生成初始请求。
        这个方法在爬虫启动时被 Scrapy 调用。
        """
        # 调用 _generate_date_range 方法，根据 self.start_date_str 和 self.end_date_str 生成日期列表
        date_range = self._generate_date_range(self.start_date_str, self.end_date_str)

        for veg_name, veg_id in VEGETABLES.items():  # 遍历每种蔬菜
            for date in date_range:  # 遍历每个日期
                params = {
                    'commdityid': veg_id,  # 商品ID
                    'searchDate': date  # 搜索日期
                }
                url = f"{self.SEARCH_URL}?{urlencode(params)}"  # 构造完整的请求URL
                # 生成请求缓存键，用于避免重复请求相同的数据
                param_str = hashlib.md5(urlencode(sorted(params.items())).encode()).hexdigest()
                request_cache_key = f"veg:{veg_id}:{param_str}"

                yield scrapy.Request(
                    url,  # 请求的URL
                    callback=self.parse,  # 指定回调函数，用于处理响应
                    meta={
                        'vegetable_name': veg_name,  # 将蔬菜名称传递给回调函数
                        'date': date,  # 将日期传递给回调函数
                        'request_cache_key': request_cache_key  # 将缓存键传递给回调函数
                    },
                    headers={
                        'Referer': self.BASE_URL,  # 设置Referer头，模拟浏览器访问
                    },
                    dont_filter=True  # 不过滤重复的URL（尽管这里有缓存键，但通常用于POST请求或动态URL）
                )

    def _generate_date_range(self, start_date_str, end_date_str):
        """
        根据开始日期和结束日期字符串生成日期范围列表。
        Args:
            start_date_str (str): 开始日期字符串，格式为 'YYYY-MM-DD'。
            end_date_str (str): 结束日期字符串，格式为 'YYYY-MM-DD'。
        Returns:
            list: 包含日期字符串的列表，如 ['2023-01-01', '2023-01-02']。
        """
        # 将日期字符串解析为 date 对象以便进行比较和计算
        start = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
        date_range = []
        current_date = start
        while current_date <= end:
            date_range.append(current_date.strftime("%Y-%m-%d"))  # 将日期格式化为字符串并添加到列表中
            current_date += datetime.timedelta(days=1)  # 移动到下一天
        return date_range

    def parse(self, response):
        """
        解析响应页面，提取蔬菜价格数据。
        Args:
            response (scrapy.http.Response): Scrapy 的响应对象。
        """
        vegetable_name = response.meta['vegetable_name']  # 从 meta 中获取蔬菜名称
        date = response.meta['date']  # 从 meta 中获取日期

        # 解码响应文本，确保能正确处理中文
        html_content = response.text if isinstance(response.text, str) else response.body.decode('utf-8')

        if "没有找到相关数据" in html_content:
            # 如果页面显示“没有找到相关数据”，则记录警告并返回
            self.logger.warning(f"服务器返回无数据响应 [{vegetable_name} {date}]")
            return

        soup = BeautifulSoup(html_content, 'html.parser')  # 使用 BeautifulSoup 解析 HTML 内容
        table = soup.find('table')  # 查找页面中的第一个 <table> 标签

        if not table:
            # 如果未找到表格，则记录警告并返回
            self.logger.warning(f"未找到表格数据 [{vegetable_name} {date}]")
            return

        rows = table.find_all('tr')[1:]  # 找到表格中所有的行，并跳过第一行（通常是表头）

        for row in rows:  # 遍历每一行数据
            cols = row.find_all('td')  # 找到当前行中所有的单元格
            if len(cols) < 5:
                continue  # 如果单元格数量少于5个，则跳过该行（数据不完整）

            item = VegetablePriceItem()  # 创建一个 VegetablePriceItem 实例
            item['variety'] = vegetable_name  # 蔬菜品种
            item['time_price'] = date  # 价格日期
            item['area'] = cols[0].text.strip()  # 地区
            item['market'] = cols[1].text.strip()  # 市场

            item['price'] = self._parse_float(cols[2].text)  # 今日价格
            item['previous_price'] = self._parse_float(cols[3].text)  # 昨日价格
            item['mom'] = self._parse_float(cols[4].text.replace('%', ''))  # 环比（去除百分号）

            if item['price'] is None or item['price'] <= 0:
                # 如果价格无效或为零，则记录警告并跳过该条数据
                self.logger.warning(
                    f"无效或零价格数据，跳过: {item['variety']} - {item['market']} - {item['time_price']} - 价格: {item['price']}")
                continue

            yield item  # 生成（yield）Item 对象，Scrapy 会自动处理它（例如，写入管道）

    def _parse_float(self, text: str):
        """
        尝试将文本转换为浮点数。
        Args:
            text (str): 需要转换的文本。
        Returns:
            float or None: 转换后的浮点数，如果转换失败则返回 None。
        """
        try:
            return float(text.strip())  # 尝试将去除空白字符的文本转换为浮点数
        except (ValueError, AttributeError, TypeError):
            return None  # 如果转换失败（例如，文本不是有效的数字），则返回 None