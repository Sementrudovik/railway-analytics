"""
Тест загрузки первых 100 строк из XLSB
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline.load_data import StreamingLoader
from config.columns.columns_config import COLUMNS_TO_DROP, COLUMN_MAPPING
import uuid
import logging

logging.basicConfig(level=logging.INFO)

def test_small_load():
    """Загрузка первых 100 строк из файла"""
    
    # Путь к файлу
    test_file = Path('/Users/semenanin/Documents/Python/University/railway-analytics/data/raw/2026') / "КТК 01.2026.xlsb"
    
    if not test_file.exists():
        print(f"❌ Файл не найден: {test_file}")
        return
    
    print(f"📁 Файл: {test_file.name}")
    #print(f"🗑️  Колонки для удаления: {COLUMNS_TO_DROP}")
    #print(f"🔄 Маппинг колонок: {list(COLUMN_MAPPING.keys())}")
    #print("=" * 50)
    
    # Создаем загрузчик
    loader = StreamingLoader(batch_size=100)
    batch_id = uuid.uuid4()
    
    # Загружаем
    success = loader.process_file(test_file, batch_id)
    
    if success:
        print("\n✅ Тест пройден! Данные загружены.")
        
        # Проверяем количество записей
        from db.connection import get_db_connection
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM railway.staging_transport")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        
        print(f"📊 Всего записей в таблице: {count}")
    else:
        print("\n❌ Тест провален")

if __name__ == "__main__":
    test_small_load()