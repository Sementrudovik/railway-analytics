# tests/test_connection.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

cur = conn.cursor()
cur.execute("SELECT version();")
print(f"Connected to: {cur.fetchone()[0][:50]}...")

cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'railway';")
tables = cur.fetchall()
print(f"\nRailway schema tables ({len(tables)}):")
for table in tables:
    print(f"  - {table[0]}")

cur.close()
conn.close()
