# db/connection.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging

load_dotenv()  

logger = logging.getLogger(__name__)

def get_db_connection():
    """Получить подключение к PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'railway_db'),
            user=os.getenv('DB_USER', 'railway_user'),
            password=os.getenv('DB_PASSWORD', 'railway_password')
        )
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения: {e}")
        return None