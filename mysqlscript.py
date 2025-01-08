import mysql.connector
from mysql.connector import Error

# GCP MySQL 資料庫連線設定
config = {
    'host': '34.81.176.203',  # 替換為 GCP MySQL 實例的外部 IP 地址
    'user': 'frank',
    'password': 'Apollore100',
    'database': 'franktest',
    'port': 3306  # 通常為 3306
}

try:
    # 建立與 MySQL 的連線
    connection = mysql.connector.connect(**config)
    if connection.is_connected():
        print("成功連線到 GCP MySQL 資料庫!")
        
        # 建立游標並執行查詢
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES;")
        
        print("資料庫中的表格：")
        for table in cursor.fetchall():
            print(table)
        
except Error as e:
    print(f"連線失敗，錯誤訊息: {e}")
finally:
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("連線已關閉。")
