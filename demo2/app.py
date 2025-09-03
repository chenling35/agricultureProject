from flask import Flask, render_template, request, jsonify, send_file
import pymysql
from datetime import datetime, timedelta, date # 导入 date 类型
import csv
from io import StringIO
import json

app = Flask(__name__)

# 数据库配置 - 请根据您的实际情况修改
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '135781012zhu',
    'database': 'vegetable_market', # 确保这是你的数据库名
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def get_db_connection():
    """获取数据库连接"""
    return pymysql.connect(**DB_CONFIG)

@app.route('/')
def index():
    """渲染主页"""
    return render_template('index.html')

@app.route('/api/vegetables')
def get_vegetables():
    """获取所有蔬菜种类"""
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                # --- 修改表名为 prices ---
                sql = "SELECT DISTINCT vegetable FROM prices ORDER BY vegetable"
                cursor.execute(sql)
                result = cursor.fetchall()
                vegetables = [item['vegetable'] for item in result]
                return jsonify({'vegetables': vegetables})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/regions')
def get_regions():
    """获取所有地区"""
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                # --- 修改表名为 prices ---
                sql = "SELECT DISTINCT region FROM prices ORDER BY region"
                cursor.execute(sql)
                result = cursor.fetchall()
                regions = [item['region'] for item in result]
                return jsonify({'regions': regions})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/markets')
def get_markets_by_region():
    """获取指定地区的所有市场"""
    region = request.args.get('region')
    if not region:
        return jsonify({'markets': []})

    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                # --- 修改表名为 prices ---
                sql = "SELECT DISTINCT market FROM prices WHERE region = %s ORDER BY market"
                cursor.execute(sql, (region,))
                result = cursor.fetchall()
                markets = [item['market'] for item in result]
                return jsonify({'markets': markets})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 新增的 API 路由：根据蔬菜和日期获取各地区平均价格
@app.route('/api/average_prices_by_region')
def get_average_prices_by_region():
    vegetable = request.args.get('vegetable')
    start_date_str = request.args.get('startDate')
    end_date_str = request.args.get('endDate')

    if not vegetable or not start_date_str or not end_date_str:
        # 如果没有提供日期范围，默认使用近7天
        # 注意：这里直接修改start_date_str和end_date_str来保持一致性
        days = max(1, min(365, int(request.args.get('days', 7)))) # 默认7天
        end_date_obj = datetime.now().date()
        start_date_obj = end_date_obj - timedelta(days=days)
        start_date_str = start_date_obj.strftime('%Y-%m-%d')
        end_date_str = end_date_obj.strftime('%Y-%m-%d')
    else:
        # 确保传入的日期字符串是正确的日期对象
        start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d').date()


    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                # --- 修改表名为 prices ---
                sql = """
                    SELECT region, AVG(price) AS average_price
                    FROM prices
                    WHERE vegetable = %s
                    AND date BETWEEN %s AND %s
                    GROUP BY region
                    ORDER BY region
                """
                cursor.execute(sql, (vegetable, start_date_str, end_date_str)) # 使用字符串形式传递日期
                result = cursor.fetchall()

                average_prices = []
                for row in result:
                    average_prices.append({
                        'region': row['region'],
                        'average_price': float(row['average_price']) if row['average_price'] is not None else 0.0
                    })
                return jsonify({'average_prices': average_prices})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/prices')
def get_prices():
    """根据筛选条件获取价格数据"""
    vegetable = request.args.get('vegetable', 'all')
    region = request.args.get('region', 'all')
    market = request.args.get('market', 'all')
    start_date_str = request.args.get('startDate') # 保持为字符串变量名
    end_date_str = request.args.get('endDate')     # 保持为字符串变量名

    try:
        # 构建SQL查询条件
        conditions = []
        params = []

        if vegetable != 'all':
            conditions.append("vegetable = %s")
            params.append(vegetable)

        if region != 'all':
            conditions.append("region = %s")
            params.append(region)

        if market != 'all':
            conditions.append("market = %s")
            params.append(market)

        # 处理日期范围
        if start_date_str and end_date_str:
            conditions.append("date BETWEEN %s AND %s")
            params.extend([start_date_str, end_date_str])
        else:
            # 如果没有提供日期范围，默认使用近7天
            days = max(1, min(365, int(request.args.get('days', 7))))
            end_date_obj = datetime.now().date()
            start_date_obj = end_date_obj - timedelta(days=days)
            conditions.append("date BETWEEN %s AND %s")
            params.extend([start_date_obj.strftime('%Y-%m-%d'), end_date_obj.strftime('%Y-%m-%d')])

        # 构建完整SQL
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        # --- 修改表名为 prices ---
        sql = f"""
            SELECT
                p.vegetable,
                p.region,
                p.market,
                p.price,
                p.change_percentage AS `change`,
                p.date
            FROM prices p
            WHERE {where_clause}
            ORDER BY p.date DESC
        """

        # 打印SQL查询和参数，用于调试
        print(f"执行SQL查询: {sql}")
        print(f"查询参数: {params}")

        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                result = cursor.fetchall()

                # 转换为字典列表
                prices = []
                for row in result:
                    # --- 核心修改：确保 date 是 datetime.date 对象 ---
                    row_date = row['date']
                    if isinstance(row_date, str):
                        try:
                            row_date = datetime.strptime(row_date, '%Y-%m-%d').date()
                        except ValueError:
                            # 如果字符串日期格式不正确，可以记录警告或跳过
                            print(f"警告: 无法解析日期字符串: {row_date}")
                            continue # 或者设置为 None，根据你的业务需求处理
                    # --- 修改结束 ---

                    prices.append({
                        'vegetable': row['vegetable'],
                        'region': row['region'],
                        'market': row['market'],
                        'price': float(row['price']),
                        'change': float(row['change']),
                        'date': row_date.strftime('%Y-%m-%d') # 现在可以安全地调用 strftime
                    })

                return jsonify({'prices': prices})
    except Exception as e:
        # 打印详细的错误堆栈信息
        import traceback
        traceback.print_exc()

        # 返回更详细的错误信息
        error_info = {
            'error': str(e),
            'details': {
                'vegetable': vegetable,
                'region': region,
                'market': market,
                'startDate': start_date_str, # 使用字符串变量名
                'endDate': end_date_str      # 使用字符串变量名
            }
        }
        return jsonify(error_info), 500

@app.route('/api/export')
def export_data():
    """导出价格数据为CSV文件"""
    vegetable = request.args.get('vegetable', 'all')
    region = request.args.get('region', 'all')
    market = request.args.get('market', 'all')
    start_date_str = request.args.get('startDate') # 保持为字符串变量名
    end_date_str = request.args.get('endDate')     # 保持为字符串变量名

    try:
        # 构建SQL查询条件
        conditions = []
        params = []

        if vegetable != 'all':
            conditions.append("vegetable = %s")
            params.append(vegetable)

        if region != 'all':
            conditions.append("region = %s")
            params.append(region)

        if market != 'all':
            conditions.append("market = %s")
            params.append(market)

        # 处理日期范围
        if start_date_str and end_date_str:
            conditions.append("date BETWEEN %s AND %s")
            params.extend([start_date_str, end_date_str])
        else:
            # 如果没有提供日期范围，默认使用近7天
            days = max(1, min(365, int(request.args.get('days', 7))))
            end_date_obj = datetime.now().date()
            start_date_obj = end_date_obj - timedelta(days=days)
            conditions.append("date BETWEEN %s AND %s")
            params.extend([start_date_obj.strftime('%Y-%m-%d'), end_date_obj.strftime('%Y-%m-%d')])

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        # --- 修改表名为 prices ---
        sql = f"""
            SELECT
                p.vegetable,
                p.region,
                p.market,
                p.price,
                p.change_percentage AS `change`,
                p.date
            FROM prices p
            WHERE {where_clause}
            ORDER BY p.date DESC
        """

        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                result = cursor.fetchall()

                # 创建CSV文件
                output = StringIO()
                writer = csv.writer(output)
                writer.writerow(['蔬菜', '地区', '市场', '价格(元/公斤)', '涨跌(%)', '日期'])

                for row in result:
                    # --- 核心修改：确保 date 是 datetime.date 对象 ---
                    row_date = row['date']
                    if isinstance(row_date, str):
                        try:
                            row_date = datetime.strptime(row_date, '%Y-%m-%d').date()
                        except ValueError:
                            print(f"警告: 无法解析日期字符串: {row_date}")
                            continue # 或者设置为 None
                    # --- 修改结束 ---

                    writer.writerow([
                        row['vegetable'],
                        row['region'],
                        row['market'],
                        row['price'],
                        row['change'],
                        row_date.strftime('%Y-%m-%d') # 现在可以安全地调用 strftime
                    ])

                output.seek(0)

                # 设置响应头，让浏览器下载文件
                return send_file(
                    StringIO(output.getvalue()),
                    as_attachment=True,
                    download_name='农产品价格数据.csv',
                    mimetype='text/csv'
                )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # 建议在开发环境中使用 debug=True，方便调试和自动重载
    # app.run(debug=True, use_reloader=True)
    app.run(debug=False, use_reloader=False)

