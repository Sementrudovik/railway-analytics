# Railway Transportation Analytics (2023-2025)

## О проекте

 Короткий аналитика для сводного анализа, используемого в статье. 
 
## Архитектура

- **Источник данных**: XLSB-файлы (2023-2025, помесячно)
- **Хранилище**: PostgreSQL (звездообразная схема)
- **ETL**: Python (pyxlsb, pandas)
- **Аналитика**: Python + SQL
- **Визуализация**: Matplotlib/Seaborn
- **CI/CD**: GitHub Actions

## Быстрый старт

### Предварительные требования

- Python 3.11+
- PostgreSQL 15+
- Docker (опционально)

### Установка

```bash
# Клонирование репозитория
git clone https://github.com/yourusername/railway-analytics.git
cd railway-analytics

# Создание виртуального окружения
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate  # Windows

# Установка зависимостей
make install 
