import pandas as pd
from pathlib import Path
from pyxlsb import open_workbook

# Пути
INPUT_DIR = Path("/Users/semenanin/Downloads")
OUTPUT_DIR = Path("/Users/semenanin/Documents/Python/University/railway-analytics/data/processed")
COLUMNS_TO_DROP = [ 'Номер документа', 'Наименование груза', 'Дор отпр', 'Код станц отпр РФ',
                   'Грузоотправитель полное наименование', 'Грузоотправитель-ОКПО', 'Дор наз',
                   'Код станц назн РФ', 'Грузополучатель полное наименование', 'Грузополучатель-ОКПО',
                   'Арендатор', 'Объем*перевозок (тн)', 'Провозная*плата', 'КЛАСС ГРУЗА', 'Код станц отпр СНГ',
                   'Код станц назн СНГ', 'Категория отпр', 'Подрод вагона', 'Тип вагона', 'Вагоно-км', 'Вид перевозки',
                   'Тоннажность', 'Модель вагона', 'Тип парка', 'Оператор', 'Собственник по ЕГРПО', 'Плательщик', 
                   'Тип контейнера', 'Плательщик', 'Собственник по ЕГРПО', 'Оператор', 'Станц отпр СНГ', 'Станц назн СНГ'
                   'Номер вагона', 'Грузоотправитель краткое наименование', 'Грузоотправитель-ИНН'
                ]
ч

def process_xlsb_files(input_dir: Path, output_dir: Path, columns_to_drop: list):
    output_dir.mkdir(parents=True, exist_ok=True)
    
    xlsb_files = list(input_dir.glob("*.xlsb"))
    print(f"Найдено файлов: {len(xlsb_files)}")
    
    for file_path in xlsb_files:
        print(f"\nОбрабатываем: {file_path.name}")
        
        # 
        file_name_without_ext = file_path.stem
        last_7_chars = file_name_without_ext[-7:] if len(file_name_without_ext) >= 7 else file_name_without_ext
        print(f"Последние 7 символов имени файла: {last_7_chars}")

        # Reading file
        df = pd.read_excel(file_path, engine='pyxlsb')
        print(f" Размер до: {df.shape}")
        
        # Cleanup file 
        df['Дата отправления'] = pd.to_datetime(df['Дата отправления'], errors='coerce')
        df['Количество контейнеров'] = 1
                                               
        # Deleted columns
        columns_to_drop_existing = [col for col in columns_to_drop if col in df.columns]
        if columns_to_drop_existing:
            df = df.drop(columns=columns_to_drop_existing)
            # print(f"  Удалены столбцы: {columns_to_drop_existing}")

        '''        df_grouped = df.groupby('Станц отпр РФ', as_index=False).agg({
                'Количество контейнеров': 'sum',
                })
        '''

        print(f" Размер после: {df.shape}")
        
        
        # Save in CSV 
        output_path_csv = output_dir / f"{file_path.stem}_processed.csv"
        df.to_csv(output_path_csv, index=False, encoding='utf-8-sig')
        # print(f"  Сохранено как CSV: {output_path_csv}")
        '''
        output_path_csv = output_dir / f"{file_path.stem}_grouped.csv"
        df_grouped.to_csv(output_path_csv, index=False, encoding='utf-8-sig')
        '''

if __name__ == "__main__":
    if not INPUT_DIR.exists():
        print(f"Ошибка: Директория {INPUT_DIR} не существует")
    else:
        process_xlsb_files(INPUT_DIR, OUTPUT_DIR, COLUMNS_TO_DROP)
        print(f"\nГотово! Обработанные файлы в: {OUTPUT_DIR}")