import os
import psycopg2
from psycopg2.extras import RealDictCursor
from common.env_loader import load_env

load_env()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        cursor_factory=RealDictCursor
    )
