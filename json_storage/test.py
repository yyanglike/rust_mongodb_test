import sqlite3
import json

def flatten_json(json_obj, parent_key='', sep='_'):
    """
    将嵌套的 JSON 对象扁平化，并标记主键和索引
    """
    items = {}
    for k, v in json_obj.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_json(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items

def table_exists(conn, table_name):
    """
    检查表是否存在
    """
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cursor.fetchone() is not None

def create_table(conn, table_name, columns, primary_keys, indexed_columns):
    """
    创建表，并设置主键和索引
    """
    if not table_exists(conn, table_name):
        # 创建列定义
        col_defs = []
        for col in columns:
            if col in primary_keys:
                col_defs.append(f"{col} TEXT PRIMARY KEY")  # 设为主键
            else:
                col_defs.append(f"{col} TEXT")
        create_query = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
        conn.execute(create_query)

        # 创建索引
        for col in indexed_columns:
            if col not in primary_keys:  # 主键已自动创建索引，无需重复创建
                index_query = f"CREATE INDEX idx_{table_name}_{col} ON {table_name}({col})"
                conn.execute(index_query)

def insert_data(conn, table_name, data):
    """
    插入数据，如果主键冲突则替换
    """
    placeholders = ', '.join(['?' for _ in data])
    columns = ', '.join(data.keys())
    insert_query = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
    conn.execute(insert_query, tuple(data.values()))

def update_data(conn, table_name, data, where_condition):
    """
    更新数据
    """
    set_clause = ', '.join([f"{key} = ?" for key in data.keys()])
    update_query = f"UPDATE {table_name} SET {set_clause} WHERE {where_condition}"
    conn.execute(update_query, tuple(data.values()))

def delete_data(conn, table_name, where_condition):
    """
    删除数据
    """
    delete_query = f"DELETE FROM {table_name} WHERE {where_condition}"
    conn.execute(delete_query)

def query_with_pagination(conn, table_name, order_by_column, order_direction='ASC', page=1, page_size=10):
    """
    分页查询数据并支持排序
    """
    offset = (page - 1) * page_size
    query = f"SELECT * FROM {table_name} ORDER BY {order_by_column} {order_direction} LIMIT ? OFFSET ?"
    cursor = conn.execute(query, (page_size, offset))
    rows = cursor.fetchall()
    return rows

def restore_json(conn, table_name):
    """
    从数据库中恢复 JSON 数据
    """
    cursor = conn.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    json_data = []
    for row in rows:
        json_entry = {}
        for col, value in zip(columns, row):
            keys = col.split('_')
            temp = json_entry
            for key in keys[:-1]:
                temp = temp.setdefault(key, {})
            temp[keys[-1]] = value
        json_data.append(json_entry)
    return json_data

def process_json(conn, path, content):
    """
    处理 JSON 数据并插入到数据库
    """
    table_name = path.replace('/', '_')
    flat_data = flatten_json(content)

    # 提取主键和索引列
    primary_keys = [col for col in flat_data.keys() if '_pri' in col]
    indexed_columns = [col for col in flat_data.keys() if '_ind' in col]

    # 创建表并设置主键和索引
    create_table(conn, table_name, flat_data.keys(), primary_keys, indexed_columns)

    # 插入数据
    insert_data(conn, table_name, flat_data)

def main():
    # 连接数据库
    conn = sqlite3.connect('data.db')

    # 测试数据
    test_cases = [
        {"name_pri": "Test1", "details": {"age_ind": "25", "city": "Shanghai"}},
        {"name_pri": "Test2", "details": {"age_ind": "28", "city": "Guangzhou"}},
        {"name_pri": "Test3", "details": {"age_ind": "35", "city": "Shenzhen"}},
        {"name_pri": "Test4", "details": {"age_ind": "22", "city": "Beijing"}},
        {"name_pri": "Test5", "details": {"age_ind": "40", "city": "Chengdu"}},
        {"name_pri": "Test6", "details": {"age_ind": "31", "city": "Hangzhou"}},
    ]
    path = "first_level"

    # 插入测试数据
    for case in test_cases:
        process_json(conn, path, case)
    conn.commit()

    # 测试更新操作
    update_data(conn, path.replace('/', '_'), {"details_age_ind": "30"}, "name_pri = 'Test1'")
    conn.commit()

    # 测试删除操作
    delete_data(conn, path.replace('/', '_'), "name_pri = 'Test2'")
    conn.commit()

    # 测试排序和分页查询
    table_name = path.replace('/', '_')
    order_by_column = "details_age_ind"  # 按照 age 列排序
    order_direction = "ASC"              # 升序
    page = 2                             # 第 2 页
    page_size = 2                        # 每页 2 条数据

    # 查询数据
    rows = query_with_pagination(conn, table_name, order_by_column, order_direction, page, page_size)
    print(f"Page {page} (Page Size: {page_size}) Sorted by {order_by_column} {order_direction}:")
    for row in rows:
        print(row)

    # 恢复 JSON 数据并打印
    restored_data = restore_json(conn, path)
    print("Restored JSON:", json.dumps(restored_data, indent=4))

    # 关闭数据库连接
    conn.close()

if __name__ == "__main__":
    main()