from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlite3
import json

app = FastAPI()
conn = sqlite3.connect(':memory:', check_same_thread=False)

# 动态创建表
def create_table(table_name: str, data: dict):
    cursor = conn.cursor()
    fields = []
    for key, value in data.items():
        field_type = "TEXT"
        if isinstance(value, int):
            field_type = "INTEGER"
        elif isinstance(value, float):
            field_type = "REAL"
        elif isinstance(value, bool):
            field_type = "BOOLEAN"
        elif isinstance(value, dict):
            field_type = "TEXT"  # 嵌套对象存储为 JSON 字符串
        fields.append(f"{key} {field_type}")

    query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {", ".join(fields)}
        )
    """
    cursor.execute(query)
    conn.commit()

# 插入 JSON 数据
@app.post("/{uri}")
def insert_json(uri: str, json_data: dict):
    table_name = uri.replace("/", "_")
    create_table(table_name, json_data)

    cursor = conn.cursor()
    fields = ", ".join(json_data.keys())
    values = ", ".join([f"'{json.dumps(value)}'" if isinstance(value, dict) else f"'{value}'" if isinstance(value, str) else str(value) for value in json_data.values()])
    query = f"""
        INSERT INTO {table_name} ({fields}) VALUES ({values})
    """
    cursor.execute(query)
    conn.commit()
    return {"message": "Data inserted successfully"}

# 查询所有 JSON 数据
@app.get("/{uri}")
def get_all_json(uri: str):
    table_name = uri.replace("/", "_")
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    result = []
    for row in rows:
        row_data = {}
        for i, column in enumerate(columns):
            if column == "id":
                row_data[column] = row[i]
            else:
                try:
                    row_data[column] = json.loads(row[i])  # 尝试解析 JSON 字符串
                except:
                    row_data[column] = row[i]
        result.append(row_data)
    return result

# 查询特定 JSON 数据
@app.get("/{uri}/{id}")
def get_json_by_id(uri: str, id: int):
    table_name = uri.replace("/", "_")
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} WHERE id = ?", (id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Data not found")
    columns = [column[0] for column in cursor.description]
    result = {}
    for i, column in enumerate(columns):
        if column == "id":
            result[column] = row[i]
        else:
            try:
                result[column] = json.loads(row[i])  # 尝试解析 JSON 字符串
            except:
                result[column] = row[i]
    return result

# 更新 JSON 数据
@app.put("/{uri}/{id}")
def update_json(uri: str, id: int, json_data: dict):
    table_name = uri.replace("/", "_")
    cursor = conn.cursor()
    set_clause = ", ".join([f"{key} = ?" for key in json_data.keys()])
    values = [json.dumps(value) if isinstance(value, dict) else value for value in json_data.values()]
    values.append(id)
    query = f"""
        UPDATE {table_name} SET {set_clause} WHERE id = ?
    """
    cursor.execute(query, values)
    conn.commit()
    return {"message": "Data updated successfully"}

# 删除 JSON 数据
@app.delete("/{uri}/{id}")
def delete_json(uri: str, id: int):
    table_name = uri.replace("/", "_")
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM {table_name} WHERE id = ?", (id,))
    conn.commit()
    return {"message": "Data deleted successfully"}