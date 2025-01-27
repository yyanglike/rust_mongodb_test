import sqlite3

def init_db():
    conn = sqlite3.connect("json_storage.db", check_same_thread=False)
    return conn