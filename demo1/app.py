from flask import Flask, render_template, request, jsonify, send_file
import pymysql # 导入 PyMySQL 连接库，替代 snowflake.connector
from datetime import datetime, timedelta, date
import csv
from io import StringIO
import json

# =========================================================
# 导入您新的 Blueprint 模块
#from market_report_bp import market_report_bp
# =========================================================

app = Flask(__name__)

# 阿里云 DMS (MySQL) 数据库配置 - 请根据您的实际情况修改以下参数
# ！！！重要提示：
# ！！！您当前的“外网地址” 'rm-cn-ow74bdsj90001x5o.rwlb.rds.aliyuncs.com' 实际上是一个内网读写分离地址。
# ！！！如果您在本地运行应用，或不在同一个阿里云VPC内，您必须在阿里云RDS控制台为您的实例开通并使用真正的公共网络连接地址。
# ！！！请将 'host' 替换为开通后获得的外网连接地址（格式通常是 *.rds.aliyuncs.com，不含 rwlb）。
# ！！！同时，请务必将您运行Flask应用的设备的公网IP地址 (您的是 120.194.123.254) 添加到RDS实例的IP白名单中。
# ！！！最后，请务必替换 'database' 为您在 DMS 上使用的实际数据库名称！！！
DB_CONFIG = {
    'host': 'rm-cn-ow74bdsj90001x5o.rwlb.rds.aliyuncs.com', # <<<<<<< 请务必将此占位符替换为您的RDS真正的外网连接地址 >>>>>>>
    'port': 3306,                               # 端口通常是 3306
    'user': 'adook',                            # 替换为您的 DMS 数据库用户名
    'password': 'Ykq123999',                     # 替换为您的 DMS 数据库密码
    'database': 'vegetable_market',             # 替换为您在 DMS 上使用的实际数据库名称
    'charset': 'utf8mb4',                       # 推荐使用 utf8mb4 编码以支持更广泛的字符集，包括中文
    'cursorclass': pymysql.cursors.DictCursor   # 使用 DictCursor 获取字典形式的结果
}

def get_db_connection():
    """获取 DMS (MySQL) 数据库连接"""
    try:
        # 使用 pymysql.connect 方法建立连接
        conn = pymysql.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"连接 DMS 数据库失败: {e}")
        raise # 重新抛出异常，让调用者处理

@app.route('/')
def index():
    """渲染主页"""
    return render_template('index.html')

# =========================================================
# 在这里注册您的 Blueprints
# 这是关键步骤，将 market_report_bp 的路由和逻辑添加到主应用中
#app.register_blueprint(market_report_bp)
# =========================================================

# 以下是您原有的数据探索的路由，它们现在与新的Blueprint并行工作
@app.route('/api/varieties')
def get_varieties():
    """获取所有蔬菜种类 (VARIETY)"""
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor: # DictCursor 已在 DB_CONFIG 中设置
                # 修正：移除列名周围的双引号
                sql = "SELECT DISTINCT VARIETY FROM PRICES ORDER BY VARIETY"
                cursor.execute(sql)
                result = cursor.fetchall()
                # 访问结果时使用大写键名，因为MySQL的DictCursor默认可能会保留原始大小写，或转为大写
                varieties = [item['VARIETY'] for item in result]
                return jsonify({'varieties': varieties})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/areas')
def get_areas():
    """获取所有区域 (AREA)"""
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor: # DictCursor 已在 DB_CONFIG 中设置
                # 修正：移除列名周围的双引号
                sql = "SELECT DISTINCT AREA FROM PRICES ORDER BY AREA"
                cursor.execute(sql)
                result = cursor.fetchall()
                # 访问结果时使用大写键名
                areas = [item['AREA'] for item in result]
                return jsonify({'areas': areas})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/markets')
def get_markets_by_area():
    """获取指定区域 (AREA) 的所有市场"""
    area = request.args.get('area')
    if not area:
        return jsonify({'markets': []})

    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor: # DictCursor 已在 DB_CONFIG 中设置
                # 修正：移除列名周围的双引号
                sql = "SELECT DISTINCT MARKET FROM PRICES WHERE AREA = %s ORDER BY MARKET"
                cursor.execute(sql, (area,))
                result = cursor.fetchall()
                # 访问结果时使用大写键名
                markets = [item['MARKET'] for item in result]
                return jsonify({'markets': markets})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/average_prices_by_area')
def get_average_prices_by_area():
    variety = request.args.get('variety')
    start_date_str = request.args.get('startDate')
    end_date_str = request.args.get('endDate')

    if not variety or not start_date_str or not end_date_str:
        days = max(1, min(365, int(request.args.get('days', 7))))
        end_date_obj = datetime.now().date()
        start_date_obj = end_date_obj - timedelta(days=days)
        start_date_str = start_date_obj.strftime('%Y-%m-%d')
        end_date_str = end_date_obj.strftime('%Y-%m-%d')
    else:
        start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d').date()

    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor: # DictCursor 已在 DB_CONFIG 中设置
                # 修正：移除列名周围的双引号
                sql = """
                    SELECT AREA, AVG(PRICE) AS AVERAGE_PRICE
                    FROM PRICES
                    WHERE VARIETY = %s
                    AND TIME_PRICE BETWEEN %s AND %s
                    GROUP BY AREA
                    ORDER BY AREA
                """
                cursor.execute(sql, (variety, start_date_str, end_date_str))
                result = cursor.fetchall()

                average_prices = []
                for row in result:
                    # 访问结果时使用大写键名
                    average_prices.append({
                        'area': row['AREA'],
                        'average_price': float(row['AVERAGE_PRICE']) if row['AVERAGE_PRICE'] is not None else 0.0
                    })
                return jsonify({'average_prices': average_prices})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/prices')
def get_prices():
    """根据筛选条件获取价格数据"""
    variety = request.args.get('variety', 'all')
    area = request.args.get('area', 'all')
    market = request.args.get('market', 'all')
    start_date_str = request.args.get('startDate')
    end_date_str = request.args.get('endDate')

    try:
        conditions = []
        params = []

        if variety != 'all':
            # 修正：移除列名周围的双引号
            conditions.append("VARIETY = %s")
            params.append(variety)

        if area != 'all':
            # 修正：移除列名周围的双引号
            conditions.append("AREA = %s")
            params.append(area)

        if market != 'all':
            # 修正：移除列名周围的双引号
            conditions.append("MARKET = %s")
            params.append(market)

        if start_date_str and end_date_str:
            # 修正：移除列名周围的双引号
            conditions.append("TIME_PRICE BETWEEN %s AND %s")
            params.extend([start_date_str, end_date_str])
        else:
            days = max(1, min(365, int(request.args.get('days', 7))))
            end_date_obj = datetime.now().date()
            start_date_obj = end_date_obj - timedelta(days=days)
            # 修正：移除列名周围的双引号
            conditions.append("TIME_PRICE BETWEEN %s AND %s")
            params.extend([start_date_obj.strftime('%Y-%m-%d'), end_date_obj.strftime('%Y-%m-%d')])

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        # 修正：移除列名周围的双引号
        sql = f"""
            SELECT
                p.VARIETY,
                p.AREA,
                p.MARKET,
                p.PRICE,
                p.MOM AS `CHANGE`,
                p.TIME_PRICE
            FROM PRICES p
            WHERE {where_clause}
            ORDER BY p.TIME_PRICE DESC
        """

        print(f"执行SQL查询: {sql}")
        print(f"查询参数: {params}")

        with get_db_connection() as connection:
            with connection.cursor() as cursor: # DictCursor 已在 DB_CONFIG 中设置
                cursor.execute(sql, tuple(params))
                result = cursor.fetchall()

                prices = []
                for row in result:
                    # 访问结果时使用大写键名
                    row_time_price = row['TIME_PRICE']

                    prices.append({
                        'variety': row['VARIETY'],
                        'area': row['AREA'],
                        'market': row['MARKET'],
                        'price': float(row['PRICE']),
                        'change': float(row['CHANGE']),
                        'time_price': row_time_price.strftime('%Y-%m-%d')
                    })

                return jsonify({'prices': prices})
    except Exception as e:
        import traceback
        traceback.print_exc()
        error_info = {
            'error': str(e),
            'details': {
                'variety': variety,
                'area': area,
                'market': market,
                'startDate': start_date_str,
            'endDate': end_date_str
            }
        }
        return jsonify(error_info), 500

@app.route('/api/export')
def export_data():
    """导出价格数据为CSV文件"""
    variety = request.args.get('variety', 'all')
    area = request.args.get('area', 'all')
    market = request.args.get('market', 'all')
    start_date_str = request.args.get('startDate')
    end_date_str = request.args.get('endDate')

    try:
        conditions = []
        params = []

        if variety != 'all':
            # 修正：移除列名周围的双引号
            conditions.append("VARIETY = %s")
            params.append(variety)

        if area != 'all':
            # 修正：移除列名周围的双引号
            conditions.append("AREA = %s")
            params.append(area)

        if market != 'all':
            # 修正：移除列名周围的双引号
            conditions.append("MARKET = %s")
            params.append(market)

        if start_date_str and end_date_str:
            # 修正：移除列名周围的双引号
            conditions.append("TIME_PRICE BETWEEN %s AND %s")
            params.extend([start_date_str, end_date_str])
        else:
            days = max(1, min(365, int(request.args.get('days', 7))))
            end_date_obj = datetime.now().date()
            start_date_obj = end_date_obj - timedelta(days=days)
            # 修正：移除列名周围的双引号
            conditions.append("TIME_PRICE BETWEEN %s AND %s")
            params.extend([start_date_obj.strftime('%Y-%m-%d'), end_date_obj.strftime('%Y-%m-%d')])

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        # 修正：移除列名周围的双引号
        sql = f"""
            SELECT
                p.VARIETY,
                p.AREA,
                p.MARKET,
                p.PRICE,
                p.MOM AS `CHANGE`,
                p.TIME_PRICE
            FROM PRICES p
            WHERE {where_clause}
            ORDER BY p.TIME_PRICE DESC
        """

        with get_db_connection() as connection:
            with connection.cursor() as cursor: # DictCursor 已在 DB_CONFIG 中设置
                cursor.execute(sql, tuple(params))
                result = cursor.fetchall()

                output = StringIO()
                writer = csv.writer(output)
                writer.writerow(['蔬菜种类', '区域', '市场', '价格(元/公斤)', '环比涨跌(%)', '日期'])

                for row in result:
                    # 访问结果时使用大写键名
                    row_time_price = row['TIME_PRICE']

                    writer.writerow([
                        row['VARIETY'],
                        row['AREA'],
                        row['MARKET'],
                        row['PRICE'],
                        row['CHANGE'],
                        row_time_price.strftime('%Y-%m-%d')
                    ])

                output.seek(0)

                return send_file(
                    StringIO(output.getvalue()),
                    as_attachment=True,
                    download_name='农产品价格数据.csv',
                    mimetype='text/csv'
                )
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False, use_reloader=False)