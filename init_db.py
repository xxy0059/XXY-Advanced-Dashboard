import sqlite3
import csv
from datetime import datetime

def init_database():
    """初始化数据库并导入CSV数据"""
    conn = sqlite3.connect('fcra_data.db')
    cursor = conn.cursor()
    
    # 创建数据表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fcra_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            acct_number INTEGER,
            portfolio TEXT,
            rule_id TEXT,
            rule_category TEXT,
            severity TEXT,
            dqs_status TEXT,
            date_of_info TEXT,
            aging INTEGER,
            process_date TEXT,
            remediation_status TEXT,
            action_taken_by TEXT,
            action_notes TEXT,
            action_date TEXT,
            assigned_to TEXT,
            remediation_category TEXT
        )
    ''')
    
    # 清空现有数据
    cursor.execute('DELETE FROM fcra_records')
    
    # 读取CSV文件并插入数据
    count = 0
    with open('FCRA data.csv', 'r', encoding='utf-8-sig') as file:  # 使用utf-8-sig处理BOM
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            # 跳过空行
            acct_num = row.get('Acct Number', '').strip()
            if not acct_num:
                continue
            
            try:
                cursor.execute('''
                    INSERT INTO fcra_records (
                        acct_number, portfolio, rule_id, rule_category, severity,
                        dqs_status, date_of_info, aging, process_date, remediation_status,
                        action_taken_by, action_notes, action_date, assigned_to, remediation_category
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    int(acct_num),
                    row.get('Portfolio', '').strip(),
                    row.get('Rule ID', '').strip(),
                    row.get('Rule Category', '').strip(),
                    row.get('Serverity ', '').strip(),
                    row.get('DQS status', '').strip(),
                    row.get('Date of Info', '').strip(),
                    int(row.get('Aging', 0)) if row.get('Aging', '').strip() else 0,
                    row.get('Process Date', '').strip(),
                    row.get('Remediation Status', '').strip(),
                    row.get('Action taken By', '').strip() if row.get('Action taken By', '').strip() not in ['None', ''] else '',
                    row.get('Action Notes', '').strip() if row.get('Action Notes', '').strip() not in ['None', ''] else '',
                    row.get('Action Date', '').strip() if row.get('Action Date', '').strip() not in ['None', ''] else '',
                    row.get('Assigned to', '').strip() if row.get('Assigned to', '').strip() not in ['None', ''] else '',
                    row.get('Remediation Category', '').strip() if row.get('Remediation Category', '').strip() not in ['None', ''] else ''
                ))
                count += 1
            except Exception as e:
                print(f"导入数据时出错: {e}")
                print(f"问题行: {row}")
    
    conn.commit()
    
    # 验证数据
    cursor.execute('SELECT COUNT(*) FROM fcra_records')
    total = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"数据库初始化完成！")
    print(f"成功导入 {count} 条记录")
    print(f"数据库中共有 {total} 条记录")

def seed_more_data():
    """在现有数据库中插入不同process_date的示例数据，便于演示趋势与As Of日期。"""
    conn = sqlite3.connect('fcra_data.db')
    cursor = conn.cursor()
    samples = [
        (300001, 'Credit Cards', '119x', 'CCcardRule', 'Medium', 'New', '2025/5/31', 120, '2025/10/15', 'Incomplete', 'Rob', 'Need more', '2025/10/15', 'Rob', 'LOB engagement'),
        (300002, 'TDAF', '118x', 'TDAF rule', 'High', 'Existing', '2025/6/30', 90, '2025/10/10', 'Resolved', 'Anoop', 'Fixed', '2025/10/10', 'Anoop', 'Internal'),
        (300003, 'Consumers', '120x', 'ConsumerRule', 'Low', 'New', '2025/7/31', 60, '2025/10/05', 'Unsolved', 'Juanita', 'Pending', '2025/10/05', 'Juanita', 'Technology'),
        (300004, 'Credit Cards', '119y', 'CCcardRule', 'High', 'New', '2025/8/31', 30, '2025/09/15', 'Nonexceptions', '', '', '', '', ''),
        (300005, 'TDAF', '118y', 'TDAF rule', 'Medium', 'New', '2025/9/30', 10, '2025/10/18', 'Incomplete', 'Rob', 'Need LOB', '2025/10/18', 'Rob', 'LOB engagement'),
    ]
    cursor.executemany('''
        INSERT INTO fcra_records (
            acct_number, portfolio, rule_id, rule_category, severity,
            dqs_status, date_of_info, aging, process_date, remediation_status,
            action_taken_by, action_notes, action_date, assigned_to, remediation_category
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', samples)
    conn.commit()
    conn.close()
    print('已插入示例数据，共', len(samples), '条。')

if __name__ == '__main__':
    init_database()
    seed_more_data()

