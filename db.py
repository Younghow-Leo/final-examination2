# db.py
import pymysql
from pymysql.cursors import DictCursor
import pandas as pd
import streamlit as st

def get_db_connection():
    return pymysql.connect(
        host='mysql.sqlpub.com',
        user='younghowleo',
        password='9ZzxzHCtZKSJhYh8',
        db='mysqlexamination',
        port=3306,
        charset='utf8mb4',
        cursorclass=DictCursor
    )

def clean_table_name(filename):
    return ''.join(c for c in filename.lower().replace('.csv', '') 
                  if c.isalnum() or c == '_')

def get_table_schema(filename):
    base_schema = """
        id INT AUTO_INCREMENT PRIMARY KEY,
        confirmedCount INT, confirmedIncr INT,
        curedCount INT, curedIncr INT,
        currentConfirmedCount INT, currentConfirmedIncr INT,
        dateId DATE, deadCount INT, deadIncr INT,
        suspectedCount INT, suspectedCountIncr INT
    """
    
    if 'provincedata' in filename.lower():
        return f"{base_schema}, province VARCHAR(50)"
    elif 'countrydata' in filename.lower():
        return f"{base_schema}, country VARCHAR(100)"
    return base_schema

def create_table_for_file(filename):
    table_name = clean_table_name(filename)
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({get_table_schema(filename)})")
        conn.commit()
    return table_name

def save_data_to_table(df, table_name):
    numeric_cols = ['confirmedCount', 'confirmedIncr', 'curedCount', 
                   'curedIncr', 'currentConfirmedCount', 'currentConfirmedIncr',
                   'deadCount', 'deadIncr', 'suspectedCount', 'suspectedCountIncr']
    
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
    df['dateId'] = pd.to_datetime(df['dateId']).dt.date
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            columns = [col for col in df.columns if col in get_table_schema(table_name)]
            values = [tuple(row[col] for col in columns) for _, row in df.iterrows()]
            sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(['%s']*len(columns))})"
            cursor.executemany(sql, values)
        conn.commit()

def get_data_from_table(table_name, date_range=None):
    """从指定表获取数据"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if date_range:
                sql = f"""
                SELECT * FROM {table_name} 
                WHERE dateId BETWEEN %s AND %s 
                ORDER BY dateId
                """
                cursor.execute(sql, date_range)
            else:
                cursor.execute(f"SELECT * FROM {table_name} ORDER BY dateId")
            data = cursor.fetchall()
            
            # 转换为DataFrame
            df = pd.DataFrame(data)
            
            # 确保dateId列存在且格式正确
            if 'dateId' in df.columns:
                # 尝试将日期字符串转换为datetime格式
                df['dateId'] = pd.to_datetime(df['dateId'], format='%Y-%m-%d', errors='coerce')
            return df
    finally:
        conn.close()