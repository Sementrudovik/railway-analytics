# pipeline/load_data.py

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import uuid
import logging
from datetime import datetime
import argparse
from typing import List, Tuple, Iterator, Any
import time

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from db.connection import get_db_connection
from config.columns.columns_config import COLUMNS_TO_KEEP, COLUMN_MAPPING

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
INPUT_DIR = Path("/Users/semenanin/Downloads")
OUTPUT_DIR = Path("/Users/semenanin/Documents/Python/University/railway-analytics/data/processed")
ERROR_DIR = Path("/Users/semenanin/Documents/Python/University/railway-analytics/data/errors")
BATCH_SIZE = 50000  # Размер батча для execute_values


class StreamingLoader:
    """
    Загрузчик данных с потоковым чтением XLSB
    Не держит весь файл в памяти
    """
    
    def __init__(self, batch_size: int = BATCH_SIZE):
        self.batch_size = batch_size
        self.stats = {
            'total_rows': 0,
            'total_files': 0,
            'successful_files': 0,
            'failed_files': 0,
            'start_time': None,
            'end_time': None
        }
    
    def read_xlsb_streaming(self, file_path: Path) -> Iterator[Tuple[List[str], List[Any]]]:
        """
        Истинно потоковое чтение XLSB файла.
        Читает строку за строкой, не загружая весь файл в память.
        
        Yields:
            Tuple[headers, row_values] - заголовки и значения строки
        """
        from pyxlsb import open_workbook
        
        logger.debug(f"Открываем файл: {file_path.name}")
        
        with open_workbook(file_path) as wb:
            with wb.get_sheet(1) as sheet:
                rows_iter = sheet.rows()
                
                # Читаем заголовки (первая строка)
                headers_row = next(rows_iter)
                headers = [item.v for item in headers_row]
                logger.debug(f"Заголовки: {headers[:5]}...")
                
                # Отдаем заголовки один раз
                headers_yielded = False
                
                # Читаем данные строку за строкой
                for row in rows_iter:
                    if not headers_yielded:
                        yield headers, None  # Сигнал: заголовки готовы
                        headers_yielded = True
                    
                    row_values = [item.v for item in row]
                    yield None, row_values  # Сигнал: строка данных
    
    def read_xlsb_chunked_streaming(self, file_path: Path, chunk_size: int = 10000) -> Iterator[pd.DataFrame]:
        """
        Потоковое чтение XLSB файла чанками.
        Накопляет строки в чанк и отдает DataFrame.
        
        Args:
            file_path: путь к файлу
            chunk_size: количество строк в чанке
        
        Yields:
            pd.DataFrame: чанк данных
        """
        chunk = []
        headers = None
        
        for h, row in self.read_xlsb_streaming(file_path):
            if h is not None and headers is None:
                headers = h
                continue
            
            if row is not None:
                chunk.append(row)
                
                if len(chunk) >= chunk_size:
                    df = pd.DataFrame(chunk, columns=headers)
                    yield df
                    chunk = []
        
        # Отдаем остаток
        if chunk:
            df = pd.DataFrame(chunk, columns=headers)
            yield df
    
    def extract_date_from_filename(self, filename: str) -> str:
        """
        Извлечение и парсинг даты из имени файла.
        Использует pd.to_datetime с правильным форматом.
        
        Ожидаемый формат: ...MM.YYYY.xlsb
        """
        name_without_ext = Path(filename).stem
        
        if len(name_without_ext) >= 7:
            date_str = name_without_ext[-7:]
            try:
                # Правильный парсинг даты
                parsed_date = pd.to_datetime(date_str, format="%m.%Y")
                # Возвращаем в формате YYYY-MM-DD (первое число месяца)
                return parsed_date.strftime("%Y-%m-%d")
            except Exception as e:
                logger.debug(f"Не удалось распарсить дату '{date_str}' из {filename}: {e}")
        
        return None
    
    def transform_row(self, row: List[Any], headers: List[str], file_name: str, 
                      batch_id: uuid.UUID, date_from_filename: str = None) -> Tuple:
        """
        Трансформация одной строки в кортеж для загрузки.
        Работает с одной строкой, не создавая DataFrame.
        """
        # Создаем словарь из строки
        row_dict = dict(zip(headers, row))
        
        # Собираем значения в порядке COLUMN_MAPPING
        result = []
        
        for source_col, target_col in COLUMN_MAPPING.items():
            value = row_dict.get(source_col)
            
            # Обработка даты отправления
            if target_col == 'departure_date':
                if value is None and date_from_filename:
                    value = date_from_filename
                elif value is not None:
                    try:
                        # Парсим дату если это строка
                        if isinstance(value, str):
                            value = pd.to_datetime(value, errors='coerce')
                            if pd.notna(value):
                                value = value.strftime("%Y-%m-%d")
                    except:
                        value = None
            
            # Очистка NaN
            if pd.isna(value) or (isinstance(value, float) and np.isnan(value)):
                value = None
            
            result.append(value)
        
        # Добавляем number_of_containers
        result.append(1)  # number_of_containers
        
        # Добавляем метаданные
        result.append(file_name)  # source_file
        result.append(str(batch_id))  # batch_id
        result.append(datetime.now())  # loaded_at
        
        return tuple(result)
    
    def bulk_insert(self, records: List[Tuple], table_name: str) -> bool:
        """
        Массовая вставка через execute_values
        """
        if not records:
            return True
        
        from psycopg2.extras import execute_values
        
        conn = get_db_connection()
        if not conn:
            logger.error("Нет подключения к БД")
            return False
        
        try:
            cursor = conn.cursor()
            
            insert_query = f"""
                INSERT INTO {table_name} (
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
                ) VALUES %s
            """
            
            execute_values(
                cursor,
                insert_query,
                records,
                page_size=10000
            )
            
            conn.commit()
            logger.debug(f"Вставлено {len(records)} записей")
            return True
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка вставки: {e}")
            return False
        finally:
            conn.close()
    
    def process_file(self, file_path: Path, batch_id: uuid.UUID) -> bool:
        """
        Обработка одного файла с потоковым чтением
        """
        file_start_time = time.time()
        logger.info(f"Обработка файла: {file_path.name}")
        
        try:
            # Извлекаем дату из имени файла
            date_from_filename = self.extract_date_from_filename(file_path.name)
            if date_from_filename:
                logger.info(f"Дата из имени файла: {date_from_filename}")
            
            records_buffer = []
            total_rows = 0
            headers = None
            
            # Потоковое чтение строк
            for h, row in self.read_xlsb_streaming(file_path):
                # Получаем заголовки
                if h is not None and headers is None:
                    headers = h
                    logger.debug(f"Заголовки получены: {len(headers)} колонок")
                    continue
                
                # Обрабатываем строку данных
                if row is not None and headers:
                    # Проверяем, есть ли нужные колонки
                    needed_cols = [col for col in COLUMNS_TO_KEEP if col in headers]
                    if not needed_cols:
                        logger.warning(f"Нет нужных колонок в файле")
                        return False
                    
                    # Трансформируем строку
                    record = self.transform_row(
                        row, headers, file_path.name, batch_id, date_from_filename
                    )
                    records_buffer.append(record)
                    total_rows += 1
                    
                    # Вставляем батч
                    if len(records_buffer) >= self.batch_size:
                        logger.info(f"Вставка батча {len(records_buffer)} записей")
                        if not self.bulk_insert(records_buffer, "railway.staging_transport"):
                            return False
                        records_buffer = []
            
            # Вставляем остатки
            if records_buffer:
                logger.info(f"Вставка остатка {len(records_buffer)} записей")
                if not self.bulk_insert(records_buffer, "railway.staging_transport"):
                    return False
            
            file_elapsed = time.time() - file_start_time
            throughput = total_rows / file_elapsed if file_elapsed > 0 else 0
            
            logger.info(
                f"Файл {file_path.name}: {total_rows} строк, "
                f"время: {file_elapsed:.2f} сек, "
                f"скорость: {throughput:.0f} rows/sec"
            )
            
            # Обновляем статистику
            self.stats['total_rows'] += total_rows
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка обработки {file_path.name}: {e}", exc_info=True)
            
            # Сохраняем ошибку
            ERROR_DIR.mkdir(parents=True, exist_ok=True)
            error_file = ERROR_DIR / f"{file_path.stem}_error.txt"
            with open(error_file, 'w') as f:
                f.write(f"File: {file_path.name}\n")
                f.write(f"Error: {e}\n")
                f.write(f"Time: {datetime.now()}\n")
            
            return False
    
    def process_all_files(self, input_dir: Path, limit: int = None):
        """
        Обработка всех файлов в директории
        """
        if not input_dir.exists():
            logger.error(f"Директория {input_dir} не существует")
            return
        
        files = sorted(list(input_dir.glob("*.xlsb")))
        if not files:
            logger.warning("XLSB файлы не найдены")
            return
        
        if limit:
            files = files[:limit]
        
        logger.info("=" * 60)
        logger.info(f"Найдено файлов: {len(files)}")
        logger.info(f"Размер батча: {self.batch_size} записей")
        logger.info("Режим: потоковое чтение (True streaming)")
        logger.info("=" * 60)
        
        global_batch_id = uuid.uuid4()
        logger.info(f"Global Batch ID: {global_batch_id}")
        
        self.stats['start_time'] = time.time()
        self.stats['total_files'] = len(files)
        
        for file_path in files:
            if self.process_file(file_path, global_batch_id):
                self.stats['successful_files'] += 1
            else:
                self.stats['failed_files'] += 1
        
        self.stats['end_time'] = time.time()
        self._print_summary()
    
    def _print_summary(self):
        """
        Вывод статистики загрузки
        """
        elapsed = self.stats['end_time'] - self.stats['start_time']
        
        print("\n" + "=" * 60)
        print("СТАТИСТИКА ЗАГРУЗКИ")
        print("=" * 60)
        print(f"Всего файлов: {self.stats['total_files']}")
        print(f"Успешно: {self.stats['successful_files']}")
        print(f"Ошибок: {self.stats['failed_files']}")
        print(f"Всего строк: {self.stats['total_rows']:,}")
        print(f"Общее время: {elapsed:.2f} сек")
        print(f"Средняя скорость: {self.stats['total_rows']/elapsed:.0f} rows/sec")
        print("=" * 60)


def main():
    """
    Основная функция
    """
    parser = argparse.ArgumentParser(
        description='Потоковая загрузка XLSB в PostgreSQL (True streaming)'
    )
    parser.add_argument('--limit', type=int, help='Ограничить количество файлов')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE, 
                       help=f'Размер батча (default: {BATCH_SIZE})')
    parser.add_argument('--input-dir', type=str, help='Директория с исходными файлами')
    
    args = parser.parse_args()
    
    # Определяем директории
    input_dir = Path(args.input_dir) if args.input_dir else INPUT_DIR
    
    # Создаем директории
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ERROR_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info("=" * 60)
    logger.info("ЗАПУСК ПОТОКОВОЙ ЗАГРУЗКИ XLSB")
    logger.info("=" * 60)
    logger.info(f"Входная директория: {input_dir}")
    logger.info(f"Размер батча: {args.batch_size or BATCH_SIZE}")
    logger.info("Потоковое чтение: ВКЛЮЧЕНО (строки не накапливаются в памяти)")
    
    loader = StreamingLoader(batch_size=args.batch_size or BATCH_SIZE)
    loader.process_all_files(input_dir, args.limit)
    
    logger.info("Загрузка завершена")


if __name__ == "__main__":
    main()