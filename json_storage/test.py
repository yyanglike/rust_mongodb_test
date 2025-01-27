import sqlite3
import json

def flatten_json(json_obj, parent_key='', sep='_'):
    items = {}
    for k, v in json_obj.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_json(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items

def table_exists(conn, table_name):
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cursor.fetchone() is not None

def create_table(conn, table_name, columns):
    if not table_exists(conn, table_name):
        col_defs = ', '.join([f'{col} TEXT' for col in columns])
        create_query = f"CREATE TABLE {table_name} ({col_defs})"
        conn.execute(create_query)

def insert_data(conn, table_name, data):
    placeholders = ', '.join(['?' for _ in data])
    columns = ', '.join(data.keys())
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    conn.execute(insert_query, tuple(data.values()))

def update_data(conn, table_name, data, where_condition):
    set_clause = ', '.join([f"{key} = ?" for key in data.keys()])
    update_query = f"UPDATE {table_name} SET {set_clause} WHERE {where_condition}"
    conn.execute(update_query, tuple(data.values()))

def delete_data(conn, table_name, where_condition):
    delete_query = f"DELETE FROM {table_name} WHERE {where_condition}"
    conn.execute(delete_query)

def process_json(conn, path, content):
    table_name = path.replace('/', '_')
    flat_data = flatten_json(content)
    create_table(conn, table_name, flat_data.keys())
    insert_data(conn, table_name, flat_data)

def restore_json(conn, path):
    table_name = path.replace('/', '_')
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

def main():
    conn = sqlite3.connect('data.db')
    test_cases = [
        {"name": "Test1", "details": {"age": "25", "city": "Shanghai"}},
        {"name": "Test2", "details": {"age": "28", "city": "Guangzhou"}},
        {"name": "Test3", "details": {"age": "35", "city": "Shenzhen"}}
    ]
    path = "first_level"
    for case in test_cases:
        process_json(conn, path, case)
    conn.commit()

    # 测试更新操作
    update_data(conn, path.replace('/', '_'), {"details_age": "30"}, "name = 'Test1'")
    conn.commit()

    # 测试删除操作
    delete_data(conn, path.replace('/', '_'), "name = 'Test2'")
    conn.commit()

    # 恢复并打印更新后的数据
    restored_data = restore_json(conn, path)
    print("Restored JSON after update and delete:", json.dumps(restored_data, indent=4))
    conn.close()

if __name__ == "__main__":
    main()