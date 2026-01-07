from flask import Flask, render_template, request, jsonify, send_file
import sqlite3
from datetime import datetime
import pandas as pd
from io import BytesIO

app = Flask(__name__)

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect('fcra_data.db')
    conn.row_factory = sqlite3.Row
    return conn

def _parse_process_date(date_str: str) -> datetime | None:
    """将CSV/DB中的日期字符串(YYYY/M/D 或 YYYY/MM/DD)解析为datetime。"""
    if not date_str:
        return None
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            y = int(parts[0])
            m = int(parts[1])
            d = int(parts[2])
            return datetime(y, m, d)
        return None
    except Exception:
        return None

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')

@app.route('/api/summary_stats')
def get_summary_stats():
    """获取Summary页面的统计数据"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 总实例数
    cursor.execute('''
        SELECT portfolio, COUNT(*) as count 
        FROM fcra_records 
        GROUP BY portfolio
    ''')
    instances = cursor.fetchall()
    instances_dict = {row['portfolio']: row['count'] for row in instances}
    
    # 异常数（所有非 Nonexceptions 皆计为异常，包括 Resolved）
    cursor.execute('''
        SELECT portfolio, COUNT(*) as count
        FROM fcra_records
        WHERE remediation_status <> 'Nonexceptions'
        GROUP BY portfolio
    ''')
    exceptions = cursor.fetchall()
    exceptions_dict = {row['portfolio']: row['count'] for row in exceptions}
    
    # 待修复-未完成（Incomplete）
    cursor.execute('''
        SELECT portfolio, COUNT(*) as count 
        FROM fcra_records 
        WHERE remediation_status = 'Incomplete'
        GROUP BY portfolio
    ''')
    remediation_incomplete = cursor.fetchall()
    remediation_incomplete_dict = {row['portfolio']: row['count'] for row in remediation_incomplete}
    
    # 按 remediation_category 分类统计 Incomplete（用于第二张红卡及其子项）
    cursor.execute('''
        SELECT remediation_category, COUNT(*) as count
        FROM fcra_records
        WHERE remediation_status = 'Incomplete'
        AND remediation_category != ''
        GROUP BY remediation_category
    ''')
    category_incomplete = cursor.fetchall()
    category_dict = {row['remediation_category']: row['count'] for row in category_incomplete}
    category_total = sum(category_dict.values())
    
    # LOB engagement 按 portfolio 统计（用于图表第四个维度）
    cursor.execute('''
        SELECT portfolio, COUNT(*) as count
        FROM fcra_records
        WHERE remediation_status = 'Incomplete' AND remediation_category = 'LOB engagement'
        GROUP BY portfolio
    ''')
    lob_rows = cursor.fetchall()
    lob_by_portfolio = {row['portfolio']: row['count'] for row in lob_rows}

    conn.close()
    
    portfolios = ['Credit Cards', 'TDAF', 'Consumers']
    
    return jsonify({
        'instances': {
            'total': sum(instances_dict.get(p, 0) for p in portfolios),
            'Credit Cards': instances_dict.get('Credit Cards', 0),
            'TDAF': instances_dict.get('TDAF', 0),
            'Consumer': instances_dict.get('Consumers', 0)
        },
        'exceptions': {
            'total': sum(exceptions_dict.get(p, 0) for p in portfolios),
            'Credit Cards': exceptions_dict.get('Credit Cards', 0),
            'TDAF': exceptions_dict.get('TDAF', 0),
            'Consumer': exceptions_dict.get('Consumers', 0)
        },
        'remediation_incomplete': {
            'total': sum(remediation_incomplete_dict.get(p, 0) for p in portfolios),
            'Credit Cards': remediation_incomplete_dict.get('Credit Cards', 0),
            'TDAF': remediation_incomplete_dict.get('TDAF', 0),
            'Consumer': remediation_incomplete_dict.get('Consumers', 0)
        },
        # 为了与现有前端字段兼容，这里 lob_incomplete.total 等于 Incomplete 的按分类之和
        'lob_incomplete': {
            'total': category_total
        },
        'lob_incomplete_by_portfolio': {
            'Credit Cards': lob_by_portfolio.get('Credit Cards', 0),
            'TDAF': lob_by_portfolio.get('TDAF', 0),
            'Consumer': lob_by_portfolio.get('Consumers', 0)
        },
        'category_incomplete': {
            'Internal': category_dict.get('Internal', 0),
            'LOB engagement': category_dict.get('LOB engagement', 0),
            'Technology': category_dict.get('Technology', 0)
        }
    })

@app.route('/api/summary_table')
def get_summary_table():
    """获取Summary页面的表格数据"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 汇总（Overall）
    cursor.execute('''
        SELECT COUNT(*) as instances,
               SUM(CASE WHEN remediation_status <> 'Nonexceptions' THEN 1 ELSE 0 END) as exceptions,
               SUM(CASE WHEN remediation_status = 'Incomplete' THEN 1 ELSE 0 END) as remediation_incomplete,
               SUM(CASE WHEN remediation_status = 'Incomplete' AND remediation_category = 'LOB engagement' THEN 1 ELSE 0 END) as lob_incomplete,
               SUM(CASE WHEN remediation_status = 'Incomplete' AND remediation_category = 'Internal' THEN 1 ELSE 0 END) as internal_incomplete,
               SUM(CASE WHEN remediation_status = 'Incomplete' AND remediation_category = 'Technology' THEN 1 ELSE 0 END) as technology_incomplete,
               SUM(CASE WHEN remediation_status = 'Incomplete' AND CAST(aging AS INTEGER) >= 90 THEN 1 ELSE 0 END) as ninety_days_incomplete
        FROM fcra_records
    ''')
    total_row = cursor.fetchone()

    # 分组明细
    cursor.execute('''
        SELECT portfolio, 
               COUNT(*) as instances,
               SUM(CASE WHEN remediation_status <> 'Nonexceptions' THEN 1 ELSE 0 END) as exceptions,
               SUM(CASE WHEN remediation_status = 'Incomplete' THEN 1 ELSE 0 END) as remediation_incomplete,
               SUM(CASE WHEN remediation_status = 'Incomplete' AND remediation_category = 'LOB engagement' THEN 1 ELSE 0 END) as lob_incomplete,
               SUM(CASE WHEN remediation_status = 'Incomplete' AND remediation_category = 'Internal' THEN 1 ELSE 0 END) as internal_incomplete,
               SUM(CASE WHEN remediation_status = 'Incomplete' AND remediation_category = 'Technology' THEN 1 ELSE 0 END) as technology_incomplete,
               SUM(CASE WHEN remediation_status = 'Incomplete' AND CAST(aging AS INTEGER) >= 90 THEN 1 ELSE 0 END) as ninety_days_incomplete
        FROM fcra_records
        GROUP BY portfolio
    ''')
    rows = cursor.fetchall()
    conn.close()
    
    data = []
    # 先添加Overall行
    data.append({
        'portfolio': 'Overall',
        'instances': total_row['instances'],
        'exceptions': total_row['exceptions'],
        'remediation_incomplete': total_row['remediation_incomplete'],
        'lob_incomplete': total_row['lob_incomplete'],
        'internal_incomplete': total_row['internal_incomplete'],
        'technology_incomplete': total_row['technology_incomplete'],
        'ninety_days_incomplete': total_row['ninety_days_incomplete']
    })
    
    for row in rows:
        portfolio = row['portfolio']
        if portfolio == 'Consumers':
            portfolio = 'Consumer'
        data.append({
            'portfolio': portfolio,
            'instances': row['instances'],
            'exceptions': row['exceptions'],
            'remediation_incomplete': row['remediation_incomplete'],
            'lob_incomplete': row['lob_incomplete'],
            'internal_incomplete': row['internal_incomplete'],
            'technology_incomplete': row['technology_incomplete'],
            'ninety_days_incomplete': row['ninety_days_incomplete']
        })
    
    return jsonify(data)

@app.route('/api/as_of_date')
def get_as_of_date():
    """返回数据库中最新的process_date（最大日期）。"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT process_date FROM fcra_records WHERE process_date != ""')
    rows = cursor.fetchall()
    conn.close()

    latest: datetime | None = None
    for row in rows:
        dt = _parse_process_date(row['process_date'])
        if dt and (latest is None or dt > latest):
            latest = dt

    if latest is None:
        return jsonify({'as_of': ''})

    return jsonify({'as_of': latest.strftime('%Y-%m-%d')})

def _parse_month(date_str: str) -> str | None:
    """将 'YYYY/M/D' 转为 'YYYY-MM' 月份字符串。"""
    try:
        parts = date_str.split('/')
        if len(parts) != 3:
            return None
        y = int(parts[0])
        m = int(parts[1])
        return f"{y:04d}-{m:02d}"
    except Exception:
        return None

@app.route('/api/trend')
def get_trend():
    """根据Date of Info聚合各月份的趋势数据。

    参数 metric:
      - instances: 所有记录数
      - exceptions: remediation_status <> 'Nonexceptions'
      - remediation: remediation_status = 'Incomplete'
      - lob: remediation_status = 'Incomplete' AND remediation_category = 'LOB engagement'
    返回: { labels: [YYYY-MM...], series: { 'Credit Cards': [...], 'TDAF': [...], 'Consumer': [...] } }
    """
    metric = request.args.get('metric', 'instances')
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT portfolio, date_of_info, remediation_status, remediation_category
        FROM fcra_records
        WHERE date_of_info != ''
    ''')
    rows = cursor.fetchall()
    conn.close()

    # 收集月份
    months_set = set()
    records = []
    for r in rows:
        month = _parse_month(r['date_of_info'])
        if not month:
            continue
        months_set.add(month)
        records.append({
            'portfolio': r['portfolio'],
            'month': month,
            'status': r['remediation_status'],
            'category': r['remediation_category'] or ''
        })

    labels = sorted(months_set)
    portfolios = ['Credit Cards', 'TDAF', 'Consumers']
    series = { 'Credit Cards': [0]*len(labels), 'TDAF': [0]*len(labels), 'Consumer': [0]*len(labels) }

    idx = {m:i for i,m in enumerate(labels)}
    for rec in records:
        p = rec['portfolio']
        # 映射Consumers -> Consumer
        key = 'Consumer' if p == 'Consumers' else p
        if key not in series:
            continue
        i = idx[rec['month']]
        if metric == 'instances':
            series[key][i] += 1
        elif metric == 'exceptions':
            if rec['status'] != 'Nonexceptions':
                series[key][i] += 1
        elif metric == 'remediation':
            if rec['status'] == 'Incomplete':
                series[key][i] += 1
        elif metric == 'lob':
            if rec['status'] == 'Incomplete' and rec['category'] == 'LOB engagement':
                series[key][i] += 1

    return jsonify({ 'labels': labels, 'series': series })

@app.route('/api/portfolio_stats/<portfolio>')
def get_portfolio_stats(portfolio):
    """获取特定Portfolio的统计数据"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 调整portfolio名称以匹配数据库
    db_portfolio = 'Consumers' if portfolio == 'Consumer' else portfolio
    
    # 总实例数
    cursor.execute('''
        SELECT COUNT(*) as count 
        FROM fcra_records 
        WHERE portfolio = ?
    ''', (db_portfolio,))
    total_instances = cursor.fetchone()['count']
    
    # 异常数（所有非 Nonexceptions 皆计为异常）
    cursor.execute('''
        SELECT COUNT(*) as count 
        FROM fcra_records 
        WHERE portfolio = ? AND remediation_status <> 'Nonexceptions'
    ''', (db_portfolio,))
    total_exceptions = cursor.fetchone()['count']
    
    # 待修复-未完成总数（仅 Incomplete）
    cursor.execute('''
        SELECT COUNT(*) as count 
        FROM fcra_records 
        WHERE portfolio = ? AND remediation_status = 'Incomplete'
    ''', (db_portfolio,))
    remediation_incomplete = cursor.fetchone()['count']
    
    # 按 remediation_category 分类统计 Incomplete
    cursor.execute('''
        SELECT remediation_category, COUNT(*) as count 
        FROM fcra_records 
        WHERE portfolio = ? 
        AND remediation_status = 'Incomplete'
        AND remediation_category != ''
        GROUP BY remediation_category
    ''', (db_portfolio,))
    category_stats = cursor.fetchall()
    category_dict = {row['remediation_category']: row['count'] for row in category_stats}

    # 按 remediation_status 分类统计（用于第二张卡片的维度展示）
    cursor.execute('''
        SELECT remediation_status, COUNT(*) as count
        FROM fcra_records
        WHERE portfolio = ?
        GROUP BY remediation_status
    ''', (db_portfolio,))
    status_rows = cursor.fetchall()
    status_counts = {row['remediation_status']: row['count'] for row in status_rows}
    
    conn.close()
    
    return jsonify({
        'total_instances': total_instances,
        'total_exceptions': total_exceptions,
        'remediation_incomplete': remediation_incomplete,
        'category_incomplete': {
            'Internal': category_dict.get('Internal', 0),
            'LOB engagement': category_dict.get('LOB engagement', 0),
            'Technology': category_dict.get('Technology', 0)
        },
        'status_counts': {
            'Resolved': status_counts.get('Resolved', 0),
            'Incomplete': status_counts.get('Incomplete', 0),
            'Unsolved': status_counts.get('Unsolved', 0),
            'Nonexceptions': status_counts.get('Nonexceptions', 0)
        }
    })

@app.route('/api/portfolio_data/<portfolio>')
def get_portfolio_data(portfolio):
    """获取特定Portfolio的表格数据"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 调整portfolio名称以匹配数据库
    db_portfolio = 'Consumers' if portfolio == 'Consumer' else portfolio
    
    # 获取过滤参数
    remediation_status = request.args.get('remediation_status', '')
    remediation_category = request.args.get('remediation_category', '')
    
    query = '''
        SELECT * FROM fcra_records 
        WHERE portfolio = ?
    '''
    params = [db_portfolio]
    
    if remediation_status:
        query += ' AND remediation_status = ?'
        params.append(remediation_status)
    
    if remediation_category:
        query += ' AND remediation_category = ?'
        params.append(remediation_category)
    
    query += ' ORDER BY id'
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    data = []
    for row in rows:
        data.append({
            'id': row['id'],
            'acct_number': row['acct_number'],
            'portfolio': row['portfolio'],
            'rule_id': row['rule_id'],
            'rule_category': row['rule_category'],
            'severity': row['severity'],
            'dqs_status': row['dqs_status'],
            'date_of_info': row['date_of_info'],
            'aging': row['aging'],
            'process_date': row['process_date'],
            'remediation_status': row['remediation_status'],
            'action_taken_by': row['action_taken_by'] or '',
            'action_notes': row['action_notes'] or '',
            'action_date': row['action_date'] or '',
            'assigned_to': row['assigned_to'] or '',
            'remediation_category': row['remediation_category'] or ''
        })
    
    return jsonify(data)

@app.route('/api/update_record', methods=['POST'])
def update_record():
    """更新记录"""
    data = request.json
    record_id = data.get('id')
    field = data.get('field')
    value = data.get('value')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 如果更新action_notes，同时更新action_date
    if field == 'action_notes':
        current_date = datetime.now().strftime('%Y/%m/%d')
        cursor.execute(f'''
            UPDATE fcra_records 
            SET {field} = ?, action_date = ?
            WHERE id = ?
        ''', (value, current_date, record_id))
    else:
        cursor.execute(f'''
            UPDATE fcra_records 
            SET {field} = ?
            WHERE id = ?
        ''', (value, record_id))
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})

@app.route('/api/filter_options/<portfolio>')
def get_filter_options(portfolio):
    """获取过滤器选项"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    db_portfolio = 'Consumers' if portfolio == 'Consumer' else portfolio
    
    # 获取Remediation Status选项
    cursor.execute('''
        SELECT DISTINCT remediation_status 
        FROM fcra_records 
        WHERE portfolio = ? AND remediation_status != ''
        ORDER BY remediation_status
    ''', (db_portfolio,))
    remediation_statuses = [row['remediation_status'] for row in cursor.fetchall()]
    
    # 获取Remediation Category选项
    cursor.execute('''
        SELECT DISTINCT remediation_category 
        FROM fcra_records 
        WHERE portfolio = ? AND remediation_category != ''
        ORDER BY remediation_category
    ''', (db_portfolio,))
    remediation_categories = [row['remediation_category'] for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({
        'remediation_statuses': remediation_statuses,
        'remediation_categories': remediation_categories
    })

@app.route('/api/get_incomplete_count/<assignee>')
def get_incomplete_count(assignee):
    """获取某人未完成的任务数"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT COUNT(*) as count 
        FROM fcra_records 
        WHERE assigned_to = ? AND remediation_status IN ('Incomplete', 'Unsolved')
    ''', (assignee,))
    
    count = cursor.fetchone()['count']
    conn.close()
    
    return jsonify({'count': count})

@app.route('/api/export/<portfolio>')
def export_data(portfolio):
    """导出数据到Excel"""
    conn = get_db_connection()
    
    if portfolio == 'summary':
        # 导出Summary表格数据（含 Overall）
        total_q = '''
            SELECT 'Overall' as portfolio,
                   COUNT(*) as instances,
                   SUM(CASE WHEN remediation_status <> 'Nonexceptions' THEN 1 ELSE 0 END) as exceptions,
                   SUM(CASE WHEN remediation_status = 'Incomplete' THEN 1 ELSE 0 END) as remediation_incomplete,
                   SUM(CASE WHEN remediation_status = 'Incomplete' AND remediation_category = 'LOB engagement' THEN 1 ELSE 0 END) as lob_incomplete,
                   SUM(CASE WHEN remediation_status = 'Incomplete' AND remediation_category = 'Internal' THEN 1 ELSE 0 END) as internal_incomplete,
                   SUM(CASE WHEN remediation_status = 'Incomplete' AND remediation_category = 'Technology' THEN 1 ELSE 0 END) as technology_incomplete,
                   SUM(CASE WHEN remediation_status = 'Incomplete' AND CAST(aging AS INTEGER) >= 90 THEN 1 ELSE 0 END) as ninety_days_incomplete
            FROM fcra_records
        '''
        detail_q = '''
            SELECT CASE WHEN portfolio = 'Consumers' THEN 'Consumer' ELSE portfolio END as portfolio,
                   COUNT(*) as instances,
                   SUM(CASE WHEN remediation_status <> 'Nonexceptions' THEN 1 ELSE 0 END) as exceptions,
                   SUM(CASE WHEN remediation_status = 'Incomplete' THEN 1 ELSE 0 END) as remediation_incomplete,
                   SUM(CASE WHEN remediation_status = 'Incomplete' AND remediation_category = 'LOB engagement' THEN 1 ELSE 0 END) as lob_incomplete,
                   SUM(CASE WHEN remediation_status = 'Incomplete' AND remediation_category = 'Internal' THEN 1 ELSE 0 END) as internal_incomplete,
                   SUM(CASE WHEN remediation_status = 'Incomplete' AND remediation_category = 'Technology' THEN 1 ELSE 0 END) as technology_incomplete,
                   SUM(CASE WHEN remediation_status = 'Incomplete' AND CAST(aging AS INTEGER) >= 90 THEN 1 ELSE 0 END) as ninety_days_incomplete
            FROM fcra_records
            GROUP BY portfolio
        '''
        import pandas as _pd
        df_total = _pd.read_sql_query(total_q, conn)
        df_detail = _pd.read_sql_query(detail_q, conn)
        df = _pd.concat([df_total, df_detail], ignore_index=True)
    else:
        # 导出Portfolio数据
        db_portfolio = 'Consumers' if portfolio == 'Consumer' else portfolio
        query = '''
            SELECT acct_number, portfolio, rule_id, rule_category, severity,
                   dqs_status, date_of_info, aging, process_date, remediation_status,
                   action_taken_by, action_notes, action_date, assigned_to, remediation_category
            FROM fcra_records 
            WHERE portfolio = ?
        '''
        df = pd.read_sql_query(query, conn, params=(db_portfolio,))
    
    conn.close()
    
    # 创建Excel文件
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    output.seek(0)
    
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'{portfolio}_export.xlsx'
    )

if __name__ == '__main__':
    app.run(debug=True, port=5000)

