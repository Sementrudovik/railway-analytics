# import neccary libraries
import pandas as pd
from pyxlsb import open_workbook
import logging
from pathlib import Path

# set up logging 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = Path("/Users/semenanin/Documents/Python/University/railway-analytics/data/raw")


def read_xlsb_sample(file_path: str, n_rows: int = 100) -> pd.DataFrame:
    """
    Читает сэмпл из XLSB файла для проверки структуры
    """
    logger.info(f"Reading {file_path}...")
    
    data = []
    with open_workbook(file_path) as wb:
        with wb.get_sheet(1) as sheet:
            for i, row in enumerate(sheet.rows()):
                if i == 0:
                    headers = [item.v for item in row]
                else:
                    data.append([item.v for item in row])
                if i >= n_rows:  # Ограничиваем для теста
                    break
    
    df = pd.DataFrame(data, columns=headers)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df

if __name__ == "__main__":
    # Проверяем существование директории
    logger.info(f"Looking for XLSB files in: {DATA_DIR}")
    
    if not DATA_DIR.exists():
        logger.error(f"Directory does not exist: {DATA_DIR}")
        logger.info("Please create the directory and add XLSB files:")
        logger.info(f"mkdir -p {DATA_DIR}")
    else:
        # Ищем все XLSB файлы в директории
        sample_files = list(DATA_DIR.glob("*.xlsb"))
        
        if not sample_files:
            logger.warning(f"No XLSB files found in {DATA_DIR}")
            # Показываем содержимое директории
            logger.info(f"Contents of {DATA_DIR}:")
            for item in DATA_DIR.iterdir():
                if item.is_dir():
                    logger.info(f"  {item.name}/")
                else:
                    logger.info(f"   {item.name}")
        else:
            logger.info(f"Found {len(sample_files)} XLSB file(s)")
            # Читаем первый файл
            df = read_xlsb_sample(str(sample_files[0]))
            print("\n" + "="*50)
            print("FIRST 5 ROWS:")
            print("="*50)
            print(df.head())
            print("\n" + "="*50)
            print("DATA TYPES:")
            print("="*50)
            print(df.dtypes)
            print("\n" + "="*50)
            print(f"DataFrame shape: {df.shape}")
            print("="*50)