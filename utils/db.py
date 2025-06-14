import os
import pymysql
from common.env_loader import load_env

load_env()

def get_mysql_connection():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
        port=int(os.getenv("MYSQL_PORT", 3306))
    )

def get_mysql_jdbc_url():
    host = os.getenv("MYSQL_HOST")
    port = int(os.getenv("MYSQL_PORT", 3306))
    database = os.getenv("MYSQL_DATABASE")
    return f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&serverTimezone=UTC"

def get_mysql_jdbc_properties():
    return {
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver"
    }
