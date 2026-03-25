# pipeline/extract_1

import pandas as pd
from pathlib import Path
from pyxlsb import open_workbook
import sys
import logging
from datetime import datetime
from typing import Iterator, Dict, List, Any

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.columns.columns_config import COLUMNS_TO_DROP

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Пути
INPUT_DIR = Path("/Users/semenanin/Downloads")
OUTPUT_DIR = Path("/Users/semenanin/Documents/Python/University/railway-analytics/data/processed")
ERROR_DIR = Path("/Users/semenanin/Documents/Python/University/railway-analytics/data/errors")


def read_xlsb_streaming(file_path: Path, chunk_size: int = 10000) -> Iterator[pd.DataFrame]:
    """
    Потоковое чтение XLSB файла чанками
    
    Args:
        file_path: путь к XLSB файлу
        chunk_size: количество строк в чанке
    
    Yields:
        pd.DataFrame: чанк данных
    """
    logger.debug(f"Открываем файл: {file_path.name}")
    
    with open_workbook(file_path) as wb:
        # Получаем первый лист
        with wb.get_sheet(1) as sheet:
            rows_iter = sheet.rows()
            
            # Читаем заголовки (первая строка)
            headers_row = next(rows_iter)
            headers = [item.v for item in headers_row]
            logger.debug(f"Заголовки: {headers[:5]}...")  # Показываем первые 5
            
            # Читаем данные чанками
            chunk = []
            chunk_count = 0
            total_rows = 0
            
            for row in rows_iter:
                # Преобразуем строку в значения
                row_values = [item.v for item in row]
                chunk.append(row_values)
                total_rows += 1
                
                # Если накопили чанк - отдаем
                if len(chunk) >= chunk_size:
                    df_chunk = pd.DataFrame(chunk, columns=headers)
                    chunk_count += 1
                    logger.debug(f"Чанк {chunk_count}: {len(df_chunk)} строк")
                    yield df_chunk
                    chunk = []
            
            # Отдаем остаток
            if chunk:
                df_chunk = pd.DataFrame(chunk, columns=headers)
                logger.debug(f"Финальный чанк: {len(df_chunk)} строк")
                yield df_chunk
            
            logger.info(f"Файл {file_path.name}: всего {total_rows} строк, {chunk_count + 1} чанков")


def extract_date_from_filename(filename: str) -> str:
    """
    Извлечение даты из имени файла
    
    Предполагает формат: ..._MM.YYYY.xlsb
    """
    name_without_ext = Path(filename).stem
    
    if len(name_without_ext) >= 7:
        last_7 = name_without_ext[-7:]
        try:
            # Проверяем формат MM.YYYY
            pd.to_datetime(last_7, format="%m.%Y")
            return last_7
        except:
            pass
    
    return None


def clean_dataframe_chunk(df: pd.DataFrame, file_name: str, date_from_filename: str = None) -> pd.DataFrame:
    """
    Очистка и трансформация чанка данных
    """
    # Добавляем дату из имени файла если нет в данных
    if date_from_filename and 'Дата отправления' not in df.columns:
        df['Дата отправления'] = date_from_filename
    
    # Добавляем количество контейнеров
    df['Количество контейнеров'] = 1
    
    # Удаляем ненужные колонки
    columns_to_drop_existing = [col for col in COLUMNS_TO_DROP if col in df.columns]
    if columns_to_drop_existing:
        df = df.drop(columns=columns_to_drop_existing)
        logger.debug(f"Удалено колонок: {len(columns_to_drop_existing)}")
    
    # Очистка даты отправления
    if 'Дата отправления' in df.columns:
        df['Дата отправления'] = pd.to_datetime(
            df['Дата отправления'],
            errors='coerce',
            format='%m.%Y' if date_from_filename else None
        )
    
    return df


def process_file_streaming(file_path: Path, output_dir: Path, save_csv: bool = True) -> Dict[str, Any]:
    """
    Обработка одного файла с потоковым чтением
    
    Returns:
        Dict со статистикой обработки
    """
    start_time = datetime.now()
    stats = {
        'file_name': file_path.name,
        'total_rows': 0,
        'chunks_processed': 0,
        'start_time': start_time,
        'end_time': None,
        'status': 'processing'
    }
    
    logger.info(f"Обрабатываем файл: {file_path.name}")
    
    try:
        # Извлекаем дату из имени файла
        date_from_filename = extract_date_from_filename(file_path.name)
        if date_from_filename:
            logger.info(f"Дата из имени файла: {date_from_filename}")
        
        # Список для хранения чанков (если нужно сохранить)
        all_chunks = [] if save_csv else None
        
        # Потоковое чтение и обработка
        for i, chunk in enumerate(read_xlsb_streaming(file_path), 1):
            stats['chunks_processed'] += 1
            
            # Очищаем чанк
            chunk_clean = clean_dataframe_chunk(chunk, file_path.name, date_from_filename)
            stats['total_rows'] += len(chunk_clean)
            
            # Сохраняем чанк если нужно
            if save_csv:
                all_chunks.append(chunk_clean)
            
            # Логируем прогресс
            if i % 5 == 0:  # Каждые 5 чанков
                logger.info(f"  Чанк {i}: {len(chunk_clean)} строк, всего {stats['total_rows']}")
        
        # Объединяем все чанки если нужно сохранить CSV
        if save_csv and all_chunks:
            df_final = pd.concat(all_chunks, ignore_index=True)
            
            # Сохраняем в CSV
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"{file_path.stem}_processed.csv"
            df_final.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"CSV сохранен: {output_path}")
            stats['csv_path'] = str(output_path)
        
        stats['end_time'] = datetime.now()
        stats['duration_seconds'] = (stats['end_time'] - stats['start_time']).total_seconds()
        stats['status'] = 'success'
        
        logger.info(
            f"Файл {file_path.name}: "
            f"{stats['total_rows']} строк, "
            f"{stats['chunks_processed']} чанков, "
            f"время: {stats['duration_seconds']:.2f} сек"
        )
        
    except Exception as e:
        stats['status'] = 'failed'
        stats['error'] = str(e)
        logger.error(f"Ошибка обработки {file_path.name}: {e}")
        
        # Сохраняем информацию об ошибке
        ERROR_DIR.mkdir(parents=True, exist_ok=True)
        error_file = ERROR_DIR / f"{file_path.stem}_error.txt"
        with open(error_file, 'w') as f:
            f.write(f"File: {file_path.name}\n")
            f.write(f"Error: {e}\n")
            f.write(f"Time: {datetime.now()}\n")
    
    return stats


def process_all_files_streaming(
    input_dir: Path, 
    output_dir: Path, 
    limit: int = None,
    save_csv: bool = True
) -> List[Dict[str, Any]]:
    """
    Обработка всех XLSB файлов в директории с потоковым чтением
    """
    # Проверяем директорию
    if not input_dir.exists():
        logger.error(f"Директория {input_dir} не существует")
        return []
    
    # Получаем список файлов
    xlsb_files = list(input_dir.glob("*.xlsb"))
    if not xlsb_files:
        logger.warning(f"XLSB файлы не найдены в {input_dir}")
        return []
    
    # Ограничиваем количество
    if limit:
        xlsb_files = xlsb_files[:limit]
    
    logger.info(f"Найдено файлов: {len(xlsb_files)}")
    logger.info(f"Режим потокового чтения: включен")
    logger.info(f"Сохранение CSV: {'да' if save_csv else 'нет'}")
    
    all_stats = []
    
    for file_path in xlsb_files:
        stats = process_file_streaming(file_path, output_dir, save_csv)
        all_stats.append(stats)
    
    # Выводим общую статистику
    successful = [s for s in all_stats if s['status'] == 'success']
    failed = [s for s in all_stats if s['status'] == 'failed']
    
    logger.info("\n" + "=" * 60)
    logger.info("ИТОГОВАЯ СТАТИСТИКА")
    logger.info("=" * 60)
    logger.info(f"Всего файлов: {len(all_stats)}")
    logger.info(f"Успешно: {len(successful)}")
    logger.info(f"Ошибок: {len(failed)}")
    
    if successful:
        total_rows = sum(s['total_rows'] for s in successful)
        total_time = sum(s['duration_seconds'] for s in successful)
        logger.info(f"Всего строк: {total_rows:,}")
        logger.info(f"Общее время: {total_time:.2f} сек")
        logger.info(f"Средняя скорость: {total_rows/total_time:.0f} rows/sec")
    
    if failed:
        logger.info("\nОшибки:")
        for f in failed:
            logger.info(f"  - {f['file_name']}: {f.get('error', 'unknown')}")
    
    return all_stats


def main():
    """
    Основная функция
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Потоковая обработка XLSB файлов')
    parser.add_argument('--limit', type=int, help='Ограничить количество файлов')
    parser.add_argument('--no-csv', action='store_true', help='Не сохранять CSV')
    parser.add_argument('--input-dir', type=str, help='Директория с XLSB файлами')
    parser.add_argument('--output-dir', type=str, help='Директория для CSV')
    
    args = parser.parse_args()
    
    # Определяем директории
    input_dir = Path(args.input_dir) if args.input_dir else INPUT_DIR
    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    
    logger.info("=" * 60)
    logger.info("ЗАПУСК ПОТОКОВОЙ ОБРАБОТКИ XLSB")
    logger.info("=" * 60)
    logger.info(f"Входная директория: {input_dir}")
    logger.info(f"Выходная директория: {output_dir}")
    
    # Запускаем обработку
    stats = process_all_files_streaming(
        input_dir=input_dir,
        output_dir=output_dir,
        limit=args.limit,
        save_csv=not args.no_csv
    )
    
    logger.info("\nГотово!")


if __name__ == "__main__":
    main()