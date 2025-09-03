import requests
from bs4 import BeautifulSoup

def get_body_text(url):
    try:
        # 发送 HTTP 请求
        response = requests.get(url)
        response.raise_for_status()  # 检查请求是否成功

        # 解析网页内容
        soup = BeautifulSoup(response.text, 'html.parser')
        body = soup.find('body')

        if body:
            return body.get_text(strip=True)  # 提取 <body> 标签内的文本
        else:
            return "页面中未找到 <body> 标签"

    except requests.exceptions.RequestException as e:
        return f"请求失败，错误代码: {e}"
    except Exception as e:
        return f"发生错误: {e}"

# 示例用法
url = "https://price.21food.cn/market/137-p1.html"  # 替换为你要爬取的网站
result = get_body_text(url)
print(result)
