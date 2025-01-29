import sqlite3
import json
import hashlib
from typing import Dict, List, Any, Tuple

# 系统配置
MAPPING_TABLE = "__column_mappings__"
COLUMN_SUFFIX = {
    "PRIMARY": "_pri",  # 主键后缀
    "INDEX": "_ind"     # 索引后缀
}

class ColumnMappingManager:
    """列名映射管理器（持久化版）"""
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.mapping: Dict[str, str] = {}
        self._init_table()
        self._load_mappings()

    def _init_table(self):
        """初始化映射表"""
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {MAPPING_TABLE} (
                hashed_name TEXT PRIMARY KEY,
                original_name TEXT NOT NULL UNIQUE
            )
        """)

    def _load_mappings(self):
        """加载已有映射"""
        cursor = self.conn.execute(
            f"SELECT hashed_name, original_name FROM {MAPPING_TABLE}"
        )
        self.mapping = {row[0]: row[1] for row in cursor.fetchall()}

    def get_original_name(self, hashed: str) -> str:
        """获取原始列名"""
        return self.mapping.get(hashed, hashed)

    def add_mapping(self, hashed: str, original: str):
        """新增映射关系"""
        if hashed not in self.mapping:
            self.conn.execute(
                f"INSERT OR IGNORE INTO {MAPPING_TABLE} VALUES (?, ?)",
                (hashed, original)
            )
            self.mapping[hashed] = original

def hash_column_name(original: str, mapper: ColumnMappingManager) -> str:
    """生成带持久化的哈希列名"""
    hashed = hashlib.md5(original.encode()).hexdigest()
    prefixed = f"col_{hashed}"
    mapper.add_mapping(prefixed, original)
    return prefixed

def flatten_json(
    data: Dict, 
    mapper: ColumnMappingManager,
    parent_key: str = '', 
    sep: str = '/'
) -> Dict[str, Any]:
    """扁平化JSON并记录列名映射"""
    items = {}
    for key, value in data.items():
        current_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_json(value, mapper, current_key, sep))
        else:
            hashed = hash_column_name(current_key, mapper)
            items[hashed] = value
    return items

def parse_condition(
    condition: str, 
    mapper: ColumnMappingManager
) -> Tuple[str, List[Any]]:
    """
    解析查询条件字符串
    返回：(处理后的SQL条件语句, 参数列表)
    """
    parts = []
    params = []
    for clause in condition.split(' AND '):
        key, value = map(str.strip, clause.split('=', 1))
        hashed_key = hash_column_name(key, mapper)
        
        # 处理带引号的字符串值
        if value.startswith(("'", '"')) and value.endswith(("'", '"')):
            clean_value = value[1:-1]
            parts.append(f"{hashed_key} = ?")
            params.append(clean_value)
        else:
            parts.append(f"{hashed_key} = ?")
            params.append(value)
    
    return ' AND '.join(parts), params

def update_data(
    conn: sqlite3.Connection,
    mapper: ColumnMappingManager,
    table_name: str,
    data: Dict[str, Any],
    where_condition: str
) -> None:
    """更新数据（安全参数化版本）"""
    # 转换数据键名
    hashed_data = {
        hash_column_name(k, mapper): v 
        for k, v in data.items()
    }
    
    # 解析条件
    where_clause, where_params = parse_condition(where_condition, mapper)
    
    # 构建SQL
    set_clause = ', '.join([f"{k} = ?" for k in hashed_data])
    params = list(hashed_data.values()) + where_params
    
    query = f"""
        UPDATE {table_name}
        SET {set_clause}
        WHERE {where_clause}
    """
    conn.execute(query, params)

def delete_data(
    conn: sqlite3.Connection,
    mapper: ColumnMappingManager,
    table_name: str,
    where_condition: str
) -> None:
    """删除数据（安全参数化版本）"""
    where_clause, params = parse_condition(where_condition, mapper)
    query = f"DELETE FROM {table_name} WHERE {where_clause}"
    conn.execute(query, params)

def unflatten_json(flat_data: List[Dict[str, Any]], mapper: ColumnMappingManager, sep: str = '/') -> List[Dict[str, Any]]:
    """恢复扁平化的 JSON 数据"""
    def set_nested_item(d: Dict, keys: List[str], value: Any):
        """设置嵌套数据项"""
        for key in keys[:-1]:
            d = d.setdefault(key, {})
        d[keys[-1]] = value

    result = []
    for item in flat_data:
        restored_item = {}
        for flat_key, value in item.items():
            # 获取原始列名
            original_key = mapper.get_original_name(flat_key)
            # 根据原始列名分割成嵌套的键
            keys = original_key.split(sep)
            # 设置恢复的数据
            set_nested_item(restored_item, keys, value)
        result.append(restored_item)
    return result

def query_with_pagination1(
    conn: sqlite3.Connection,
    mapper: ColumnMappingManager,
    table_name: str,
    order_field: str,
    order_dir: str,
    page: int,
    page_size: int
) -> List[Dict[str, Any]]:
    """分页查询数据"""
    # 扁平化排序列名
    hashed_order_field = hash_column_name(order_field, mapper)
    
    # 计算偏移量
    offset = (page - 1) * page_size
    
    query = f"""
        SELECT * FROM {table_name}
        ORDER BY {hashed_order_field} {order_dir}
        LIMIT ? OFFSET ?
    """
    cursor = conn.execute(query, (page_size, offset))
    rows = cursor.fetchall()

    # 恢复为JSON格式
    column_names = [description[0] for description in cursor.description]
    flat_data = [{col: row[i] for i, col in enumerate(column_names)} for row in rows]
    return unflatten_json(flat_data, mapper)

def query_with_pagination(
    conn: sqlite3.Connection,
    mapper: ColumnMappingManager,
    table_name: str,
    order_field: str,
    order_dir: str,
    page: int,
    page_size: int
) -> List[Dict[str, Any]]:
    """分页查询数据"""
    # 扁平化排序列名
    hashed_order_field = hash_column_name(order_field, mapper)
    
    # 计算偏移量
    offset = (page - 1) * page_size
    
    # 使用 COALESCE 处理缺失的字段
    query = f"""
        SELECT * FROM {table_name}
        ORDER BY COALESCE({hashed_order_field}, 0) {order_dir}
        LIMIT ? OFFSET ?
    """
    cursor = conn.execute(query, (page_size, offset))
    rows = cursor.fetchall()

    # 恢复为JSON格式
    column_names = [description[0] for description in cursor.description]
    flat_data = [{col: row[i] for i, col in enumerate(column_names)} for row in rows]
    return unflatten_json(flat_data, mapper)

# 这个的效率可能低一些，暂时不用了。
# def query_with_pagination(
#     conn: sqlite3.Connection,
#     mapper: ColumnMappingManager,
#     table_name: str,
#     order_field: str,
#     order_dir: str,
#     page: int,
#     page_size: int,
#     filters: Dict[str, Any] = None  # 可以传入额外的过滤条件
# ) -> List[Dict[str, Any]]:
#     """分页查询数据，跳过NULL值的列"""
#     # 处理排序字段
#     hashed_order_field = hash_column_name(order_field, mapper)
    
#     # 计算偏移量
#     offset = (page - 1) * page_size

#     # 生成查询条件
#     where_clause = ""
#     params = []
    
#     if filters:
#         conditions = []
#         for key, value in filters.items():
#             if value is not None:  # 过滤掉NULL值的字段
#                 hashed_key = hash_column_name(key, mapper)
#                 conditions.append(f"{hashed_key} = ?")
#                 params.append(value)
#         if conditions:
#             where_clause = "WHERE " + " AND ".join(conditions)

#     # 构建SQL查询语句
#     query = f"""
#         SELECT * FROM {table_name}
#         {where_clause}
#         ORDER BY {hashed_order_field} {order_dir}
#         LIMIT ? OFFSET ?
#     """
#     cursor = conn.execute(query, params + [page_size, offset])
#     rows = cursor.fetchall()

#     # 恢复为JSON格式
#     column_names = [description[0] for description in cursor.description]
#     flat_data = [{col: row[i] for i, col in enumerate(column_names)} for row in rows]
    
#     return unflatten_json(flat_data, mapper)



class DatabaseManager:
    """数据库上下文管理器（集成映射）"""
    def __init__(self, db_name: str):
        self.db_name = db_name
    
    def __enter__(self) -> Tuple[sqlite3.Connection, ColumnMappingManager]:
        self.conn = sqlite3.connect(self.db_name)
        self.mapper = ColumnMappingManager(self.conn)
        return self.conn, self.mapper
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.conn.close()

# 使用示例
def main():
    with DatabaseManager('test.db') as (conn, mapper):
        # 测试数据
        test_data_1 = {
            "user_pri": "U1",
            "details": {
                "age_ind": 25,
                "address": {"city": "Shanghai"}
            }
        }

        test_data_2 = {
            "user_pri": "U2",
            "details": {
                "age2_ind": 30,
                "address": {"city": "Beijing"}
            }
        }

        # 扁平化数据
        flat = flatten_json(test_data_1, mapper)
        flat_2 = flatten_json(test_data_2, mapper)
        
        # 动态识别主键和索引
        primary_keys = [
            col for col in flat 
            if mapper.get_original_name(col).endswith(COLUMN_SUFFIX["PRIMARY"])
        ]
        indexed_columns = [
            col for col in flat 
            if mapper.get_original_name(col).endswith(COLUMN_SUFFIX["INDEX"])
        ]
        
        # 创建表
        table_name = "user_data"
        if not table_exists(conn, table_name):
            create_table(conn, table_name, flat.keys(), primary_keys, indexed_columns)
            
        # 插入数据
        insert_data(conn, table_name, flat)
        insert_data(conn, table_name, flat_2)
        
        # 测试分页查询
        print("-- 第1页数据 --")
        page_data = query_with_pagination(
            conn, mapper,
            table_name=table_name,
            order_field="details/age_ind",
            order_dir="DESC",
            page=1,
            page_size=2
        )
        
        # 恢复数据并过滤null值
    restored_data = restore_data_with_filter(page_data)            
    print(json.dumps(restored_data, indent=2))

def table_exists(conn, table_name):
    """检查表是否存在"""
    cursor = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cursor.fetchone() is not None

def create_table1(conn, table_name, columns, primary_keys, indexed_columns):
    """创建表结构"""
    # 列定义（处理主键）
    col_defs = []
    for col in columns:
        if col in primary_keys:
            col_defs.append(f"{col} TEXT PRIMARY KEY")
        else:
            col_defs.append(f"{col} TEXT")
    
    # 创建表
    conn.execute(f"CREATE TABLE {table_name} ({', '.join(col_defs)})")
    
    # 创建索引
    for col in set(indexed_columns) - set(primary_keys):
        conn.execute(f"CREATE INDEX idx_{table_name}_{col} ON {table_name}({col})")
        
def create_table(conn, table_name, columns, primary_keys, indexed_columns):
    """创建表结构"""
    # 列定义（处理主键）
    col_defs = []
    for col in columns:
        if col in primary_keys:
            col_defs.append(f"{col} TEXT PRIMARY KEY")
        else:
            col_defs.append(f"{col} TEXT")

    # 创建表
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})")

    # 动态增加缺失的列
    for col in columns:
        try:
            conn.execute(f"SELECT {col} FROM {table_name} LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT")
    
    # 创建索引
    for col in set(indexed_columns) - set(primary_keys):
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{col} ON {table_name}({col})")
        
        

def insert_data(conn, table_name, data):
    """插入数据前，确保表包含所有需要的列"""
    # 获取表的所有列
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    existing_columns = [row[1] for row in cursor.fetchall()]
    
    # 获取数据中所有的列
    data_columns = list(data.keys())
    
    # 如果数据中包含未存在的列，则添加这些列
    for col in data_columns:
        if col not in existing_columns:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT")
    
    # 插入数据
    cols = ', '.join(data.keys())
    placeholders = ', '.join(['?'] * len(data))
    query = f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})"
    conn.execute(query, tuple(data.values()))
    
def restore_data_with_filter(data):
    """递归还原数据，并过滤掉值为null的键"""
    if isinstance(data, dict):
        # 遍历字典并过滤掉值为null的字段
        return {k: restore_data_with_filter(v) for k, v in data.items() if v is not None}
    elif isinstance(data, list):
        # 对列表进行递归处理
        return [restore_data_with_filter(item) for item in data]
    else:
        # 其他类型的数据直接返回
        return data    
    

# 测试代码
if __name__ == "__main__":
    main()
