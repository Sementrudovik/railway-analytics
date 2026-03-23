# tests/test_load_data.py
"""
Тестовый скрипт для проверки загрузки данных
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline.load_data import process_file
from config.columns.columns_config import COLUMNS_TO_DROP, COLUMN_MAPPING
import uuid
import logging

logging.basicConfig(level=logging.INFO)

def test_single_file():
    """
    Тест загрузки одного файла
    """
    test_file = Path("/Users/semenanin/Downloads") / "your_file.xlsb"  # Замените на реальное имя
    
    if not test_file.exists():
        print(f"Файл {test_file} не найден")
        return
    
    batch_id = uuid.uuid4()
    success = process_file(test_file, COLUMNS_TO_DROP, COLUMN_MAPPING, batch_id)
    
    if success:
        print("Тест пройден успешно")
    else:
        print("Тест провален")

if __name__ == "__main__":
    test_single_file()