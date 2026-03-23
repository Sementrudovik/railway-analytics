"""
Модуль для обработки XLSB файлов с данными железнодорожных перевозок.
Преобразует файлы, очищает от ненужных колонок и сохраняет в CSV формате.
"""

import logging
from pathlib import Path
from typing import List, Optional, Set
from dataclasses import dataclass, field

import pandas as pd
from pyxlsb import open_workbook

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProcessingConfig:
    """Конфигурация обработки файлов."""
    input_dir: Path
    output_dir: Path
    columns_to_drop: Set[str] = field(default_factory=set)
    filename_suffix_length: int = 7
    container_column_name: str = "Количество контейнеров"
    date_column_name: str = "Дата отправления"
    encoding: str = "utf-8-sig"
    
    def __post_init__(self):
        """Валидация конфигурации после инициализации."""
        if self.filename_suffix_length <= 0:
            raise ValueError("filename_suffix_length должен быть положительным числом")
        if not self.input_dir.exists():
            raise ValueError(f"Директория {self.input_dir} не существует")


class ExcelProcessor:
    """Обработчик Excel файлов."""
    
    def __init__(self, config: ProcessingConfig):
        """
        Инициализация процессора.
        
        Args:
            config: Конфигурация обработки
        """
        self.config = config
        self._ensure_output_dir()
    
    def _ensure_output_dir(self) -> None:
        """Создание выходной директории если её нет."""
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_filename_suffix(self, file_path: Path) -> str:
        """
        Получение суффикса из имени файла.
        
        Args:
            file_path: Путь к файлу
            
        Returns:
            Последние N символов имени файла без расширения
        """
        filename = file_path.stem
        length = self.config.filename_suffix_length
        return filename[-length:] if len(filename) >= length else filename
    
    def _clean_dataframe(self, df: pd.DataFrame, filename_suffix: str) -> pd.DataFrame:
        """
        Очистка и подготовка DataFrame.
        
        Args:
            df: Исходный DataFrame
            filename_suffix: Суффикс для подстановки в дату
            
        Returns:
            Очищенный DataFrame
        """
        # Создаем копию чтобы избежать модификации оригинала
        df = df.copy()
        
        # Замена даты на суффикс из имени файла
        if self.config.date_column_name in df.columns:
            df[self.config.date_column_name] = filename_suffix
        else:
            logger.warning(f"Колонка '{self.config.date_column_name}' не найдена")
        
        # Добавление колонки с контейнерами
        df[self.config.container_column_name] = 1
        
        # Удаление ненужных колонок
        columns_to_drop = [
            col for col in self.config.columns_to_drop 
            if col in df.columns
        ]
        
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
            logger.debug(f"Удалены колонки: {columns_to_drop}")
        
        return df
    
    def _save_dataframe(self, df: pd.DataFrame, file_path: Path) -> Path:
        """
        Сохранение DataFrame в CSV.
        
        Args:
            df: DataFrame для сохранения
            file_path: Исходный путь к файлу
            
        Returns:
            Путь к сохраненному CSV файлу
        """
        output_path = self.config.output_dir / f"{file_path.stem}_processed.csv"
        df.to_csv(output_path, index=False, encoding=self.config.encoding)
        logger.info(f"Файл сохранен: {output_path}")
        return output_path
    
    def process_file(self, file_path: Path) -> Optional[Path]:
        """
        Обработка одного файла.
        
        Args:
            file_path: Путь к файлу для обработки
            
        Returns:
            Путь к обработанному файлу или None в случае ошибки
        """
        try:
            logger.info(f"Начало обработки: {file_path.name}")
            
            # Получение суффикса из имени файла
            filename_suffix = self._get_filename_suffix(file_path)
            logger.info(f"Суффикс для подстановки: {filename_suffix}")
            
            # Чтение файла
            df = pd.read_excel(file_path, engine="pyxlsb")
            logger.info(f"Размер исходных данных: {df.shape}")
            
            # Очистка данных
            df_clean = self._clean_dataframe(df, filename_suffix)
            logger.info(f"Размер после очистки: {df_clean.shape}")
            
            # Сохранение результата
            return self._save_dataframe(df_clean, file_path)
            
        except FileNotFoundError:
            logger.error(f"Файл не найден: {file_path}")
        except pd.errors.EmptyDataError:
            logger.error(f"Файл пуст: {file_path}")
        except Exception as e:
            logger.error(f"Ошибка при обработке {file_path.name}: {e}", exc_info=True)
        
        return None
    
    def process_all(self) -> List[Path]:
        """
        Обработка всех XLSB файлов в директории.
        
        Returns:
            Список путей к успешно обработанным файлам
        """
        # Поиск всех xlsb файлов
        xlsb_files = list(self.config.input_dir.glob("*.xlsb"))
        logger.info(f"Найдено XLSB файлов для обработки: {len(xlsb_files)}")
        
        if not xlsb_files:
            logger.warning(f"XLSB файлы не найдены в {self.config.input_dir}")
            return []
        
        # Обработка каждого файла
        processed_files = []
        for file_path in xlsb_files:
            result = self.process_file(file_path)
            if result:
                processed_files.append(result)
        
        logger.info(f"Успешно обработано файлов: {len(processed_files)}/{len(xlsb_files)}")
        return processed_files


def create_default_config() -> ProcessingConfig:
    """Создание конфигурации с настройками по умолчанию."""
    return ProcessingConfig(
        input_dir=Path("/Users/semenanin/Downloads"),
        output_dir=Path("/Users/semenanin/Documents/Python/University/railway-analytics/data/processed"),
        columns_to_drop={
            "Номер документа",
            "Наименование груза",
            "Дор отпр",
            "Код станц отпр РФ",
            "Грузоотправитель полное наименование",
            "Грузоотправитель-ОКПО",
            "Дор наз",
            "Код станц назн РФ",
            "Грузополучатель полное наименование",
            "Грузополучатель-ОКПО",
            "Арендатор",
            "Объем*перевозок (тн)",
            "Провозная*плата",
            "КЛАСС ГРУЗА",
            "Код станц отпр СНГ",
            "Код станц назн СНГ",
            "Категория отпр",
            "Подрод вагона",
            "Тип вагона",
            "Вагоно-км",
            "Вид перевозки",
            "Тоннажность",
            "Модель вагона",
            "Тип парка",
            "Оператор",
            "Собственник по ЕГРПО",
            "Плательщик",
            "Тип контейнера",
            "Станц отпр СНГ",
            "Станц назн СНГ",
            "Номер вагона",
            "Грузоотправитель краткое наименование",
            "Грузоотправитель-ИНН",
            "Грузополучатель краткое наименование",
            "Грузополучатель-ИНН",
        }
    )


def main() -> None:
    """Основная функция запуска обработки."""
    try:
        # Создание конфигурации
        config = create_default_config()
        
        # Инициализация процессора
        processor = ExcelProcessor(config)
        
        # Запуск обработки
        processed_files = processor.process_all()
        
        # Вывод итогов
        if processed_files:
            print(f"\n✅ Обработка завершена успешно!")
            print(f"📁 Обработанные файлы сохранены в: {config.output_dir}")
            print(f"📊 Всего файлов: {len(processed_files)}")
        else:
            print("\n❌ Не удалось обработать ни одного файла")
            
    except ValueError as e:
        logger.error(f"Ошибка конфигурации: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()