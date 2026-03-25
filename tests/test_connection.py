# db/connection.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
P
load_dotenv()

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
        print(f"Error connecting to database: {e}")
        return None

def test_connection():
    """Тест подключения"""
    conn = get_db_connection()
    if conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            print(f"Connected to: {version['version']}")
            
            cur.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'railway'
            """)
            tables = cur.fetchall()
            print("\nRailway schema tables:")
            for table in tables:
                print(f"  - {table['table_name']}")
        
        conn.close()
        return True
    return False

if __name__ == "__main__":
    test_connection()