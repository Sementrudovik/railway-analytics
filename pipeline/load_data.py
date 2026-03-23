# pipeline/load_data.py
"""
Скрипт для загрузки данных из XLSB файлов в PostgreSQL
"""
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os
import uuid
import logging
from datetime import datetime
import argparse

# Добавляем корень проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from db.connection import get_db_connection
from config.columns.columns_config import COLUMNS_TO_DROP, COLUMN_MAPPING

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Пути
INPUT_DIR = Path("/Users/semenanin/Downloads")
OUTPUT_DIR = Path("/Users/semenanin/Documents/Python/University/railway-analytics/data/processed")
ERROR_DIR = Path("/Users/semenanin/Documents/Python/University/railway-analytics/data/errors")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Очистка данных перед загрузкой
    """
    logger.info("Очистка данных...")
    
    # Заменяем пустые строки на None
    df = df.replace({pd.NA: None, np.nan: None, '': None, 'nan': None})
    
    # Обработка даты отправления
    if 'Дата отправления' in df.columns:
        df['Дата отправления'] = pd.to_datetime(
            df['Дата отправления'], 
            errors='coerce',
            format='%Y-%m-%d',  # подстройте под ваш формат
            dayfirst=False
        )
    
    # Очистка числовых полей
    numeric_fields = ['Номер вагона', 'Номер контейнера', 'Код груза', 'Количество контейнеров']
    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')
            # Заменяем NaN на None
            df[field] = df[field].where(pd.notna(df[field]), None)
    
    return df


def add_metadata_columns(df: pd.DataFrame, source_file: str, batch_id: uuid.UUID) -> pd.DataFrame:
    """
    Добавление метаданных в датафрейм
    """
    df['source_file'] = source_file
    df['batch_id'] = str(batch_id)
    df['loaded_at'] = datetime.now()
    return df


def rename_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """
    Переименование колонок согласно маппингу
    """
    # Оставляем только те колонки, которые есть в маппинге
    existing_columns = {k: v for k, v in mapping.items() if k in df.columns}
    df = df.rename(columns=existing_columns)
    
    # Добавляем недостающие колонки с None значениями
    for target_col in mapping.values():
        if target_col not in df.columns:
            df[target_col] = None
    
    return df


def save_error_file(file_path: Path, error: Exception, df: pd.DataFrame = None):
    """
    Сохранение информации об ошибке
    """
    ERROR_DIR.mkdir(parents=True, exist_ok=True)
    error_file = ERROR_DIR / f"{file_path.stem}_error.txt"
    
    with open(error_file, 'w', encoding='utf-8') as f:
        f.write(f"File: {file_path.name}\n")
        f.write(f"Error: {str(error)}\n")
        f.write(f"Time: {datetime.now()}\n\n")
        
        if df is not None:
            f.write(f"DataFrame info:\n")
            f.write(f"Shape: {df.shape}\n")
            f.write(f"Columns: {list(df.columns)}\n")
            f.write(f"Sample:\n{df.head(3).to_string()}\n")
    
    logger.error(f"Error saved to {error_file}")


def load_to_postgresql(df: pd.DataFrame, batch_id: uuid.UUID):
    """
    Загрузка данных в PostgreSQL
    """
    conn = get_db_connection()
    if not conn:
        logger.error("Не удалось подключиться к PostgreSQL")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Подготавливаем данные для вставки
        records = []
        for _, row in df.iterrows():
            # Заменяем все NaN на None
            row_data = [None if pd.isna(x) else x for x in row]
            records.append(row_data)
        
        # SQL запрос для вставки
        insert_query = """
            INSERT INTO railway.staging_transport (
                wagon_number,
                container_number,
                departure_date,
                cargo_code,
                departure_country,
                departure_region,
                departure_station,
                destination_country,
                destination_region,
                destination_station,
                destination_station_sng,
                number_of_containers,
                source_file,
                batch_id,
                loaded_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Вставляем батчами по 1000 записей
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            conn.commit()
            logger.info(f"Загружено {min(i + batch_size, len(records))} из {len(records)} записей")
        
        logger.info(f"Успешно загружено {len(records)} записей в staging_transport")
        return True
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при загрузке в PostgreSQL: {e}")
        return False
    finally:
        conn.close()


def process_file(file_path: Path, columns_to_drop: list, mapping: dict, batch_id: uuid.UUID) -> bool:
    """
    Обработка одного файла
    """
    try:
        logger.info(f"Начинаем обработку файла: {file_path.name}")
        
        # Читаем файл
        df = pd.read_excel(file_path, engine="pyxlsb")
        logger.info(f"Прочитано строк: {len(df)}, колонок: {len(df.columns)}")
        
        # Извлекаем дату из имени файла
        file_name_without_ext = file_path.stem
        date_from_filename = file_name_without_ext[-7:] if len(file_name_without_ext) >= 7 else None
        
        # Добавляем количество контейнеров
        df["Количество контейнеров"] = 1
        
        # Удаляем ненужные колонки
        columns_to_drop_existing = [col for col in columns_to_drop if col in df.columns]
        if columns_to_drop_existing:
            df = df.drop(columns=columns_to_drop_existing)
            logger.info(f"Удалено колонок: {len(columns_to_drop_existing)}")
        
        # Очистка данных
        df = clean_dataframe(df)
        
        # Переименовываем колонки
        df = rename_columns(df, mapping)
        
        # Добавляем метаданные
        df = add_metadata_columns(df, file_path.name, batch_id)
        
        # Загружаем в PostgreSQL
        success = load_to_postgresql(df, batch_id)
        
        if success:
            # Сохраняем CSV копию
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            output_path = OUTPUT_DIR / f"{file_path.stem}_processed.csv"
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"Сохранена CSV копия: {output_path}")
        
        logger.info(f"Файл {file_path.name} обработан успешно")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при обработке файла {file_path.name}: {e}")
        save_error_file(file_path, e)
        return False


def process_files(
    input_dir: Path, 
    columns_to_drop: list, 
    mapping: dict,
    limit: int = None
):
    """
    Обработка всех файлов в директории
    """
    # Проверяем существование директории
    if not input_dir.exists():
        logger.error(f"Директория {input_dir} не существует")
        return
    
    # Получаем список файлов
    xlsb_files = list(input_dir.glob("*.xlsb"))
    if not xlsb_files:
        logger.warning(f"Не найдено XLSB файлов в {input_dir}")
        return
    
    logger.info(f"Найдено файлов: {len(xlsb_files)}")
    
    # Ограничиваем количество файлов для теста
    if limit:
        xlsb_files = xlsb_files[:limit]
        logger.info(f"Обрабатываем только {limit} файлов")
    
    # Создаем общий batch_id для всей загрузки
    global_batch_id = uuid.uuid4()
    logger.info(f"Batch ID: {global_batch_id}")
    
    # Статистика
    successful = 0
    failed = 0
    
    for file_path in xlsb_files:
        success = process_file(file_path, columns_to_drop, mapping, global_batch_id)
        if success:
            successful += 1
        else:
            failed += 1
    
    logger.info(f"\n=== Статистика загрузки ===")
    logger.info(f"Успешно: {successful}")
    logger.info(f"Ошибок: {failed}")
    logger.info(f"Всего: {successful + failed}")


def main():
    """
    Основная функция
    """
    parser = argparse.ArgumentParser(description='Загрузка XLSB данных в PostgreSQL')
    parser.add_argument('--limit', type=int, help='Ограничить количество файлов для обработки')
    parser.add_argument('--input-dir', type=str, help='Директория с исходными файлами')
    parser.add_argument('--output-dir', type=str, help='Директория для сохранения CSV')
    
    args = parser.parse_args()
    
    # Определяем директории
    input_dir = Path(args.input_dir) if args.input_dir else INPUT_DIR
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    
    logger.info(f"Начало загрузки данных")
    logger.info(f"Входная директория: {input_dir}")
    logger.info(f"Выходная директория: {output_dir}")
    
    # Запускаем обработку
    process_files(
        input_dir=input_dir,
        columns_to_drop=COLUMNS_TO_DROP,
        mapping=COLUMN_MAPPING,
        limit=args.limit
    )
    
    logger.info("Загрузка данных завершена")


if __name__ == "__main__":
    main()