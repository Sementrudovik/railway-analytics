# db/connection.py
"""
Модуль для работы с подключением к PostgreSQL
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv
import logging

# Загружаем переменные окружения
load_dotenv()

logger = logging.getLogger(__name__)

# Глобальный пул соединений (опционально)
connection_pool = None


def get_db_connection():
    """
    Получить подключение к PostgreSQL
    
    Returns:
        psycopg2.connection: Объект подключения или None при ошибке
    """
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
        logger.error(f"Ошибка подключения к базе данных: {e}")
        return None


def get_db_connection_dict():
    """
    Получить подключение с курсором, возвращающим словари
    """
    conn = get_db_connection()
    if conn:
        conn.cursor_factory = RealDictCursor
    return conn


def init_connection_pool(min_conn=1, max_conn=10):
    """
    Инициализировать пул соединений
    
    Args:
        min_conn: Минимальное количество соединений
        max_conn: Максимальное количество соединений
    """
    global connection_pool
    
    try:
        connection_pool = SimpleConnectionPool(
            min_conn,
            max_conn,
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'railway_db'),
            user=os.getenv('DB_USER', 'railway_user'),
            password=os.getenv('DB_PASSWORD', 'railway_password')
        )
        logger.info("Пул соединений инициализирован")
        return connection_pool
    except Exception as e:
        logger.error(f"Ошибка инициализации пула соединений: {e}")
        return None


def get_connection_from_pool():
    """
    Получить соединение из пула
    """
    global connection_pool
    if connection_pool is None:
        init_connection_pool()
    
    if connection_pool:
        return connection_pool.getconn()
    return None


def return_connection_to_pool(conn):
    """
    Вернуть соединение обратно в пул
    """
    global connection_pool
    if connection_pool and conn:
        connection_pool.putconn(conn)


def close_all_connections():
    """
    Закрыть все соединения в пуле
    """
    global connection_pool
    if connection_pool:
        connection_pool.closeall()
        logger.info("Все соединения закрыты")


def test_connection():
    """
    Тестирование подключения к БД
    """
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                print(f"✅ Подключено к PostgreSQL: {version[0][:50]}...")
                
                cur.execute("SELECT current_database();")
                db_name = cur.fetchone()
                print(f"✅ База данных: {db_name[0]}")
                
                cur.execute("SELECT current_user;")
                user = cur.fetchone()
                print(f"✅ Пользователь: {user[0]}")
                
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Ошибка при выполнении запроса: {e}")
            return False
    else:
        print("❌ Не удалось подключиться к базе данных")
        return False


if __name__ == "__main__":
    # Тестируем подключение
    test_connection()
