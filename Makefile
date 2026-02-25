# Makefile для railway-analytics

.PHONY: help venv install install-dev clean clean-all run test lint format precommit jupyter docker-up docker-down

# Цвета для вывода
GREEN := \033[0;32m
BLUE := \033[0;34m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Показать эту справку
	@echo "$(BLUE)Доступные команды:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

venv: ## Создать виртуальное окружение
	@echo "$(BLUE)Создаю виртуальное окружение...$(NC)"
	python3.11 -m venv venv
	@echo "$(GREEN)✅ Готово! Активируйте: source venv/bin/activate$(NC)"

install: venv ## Установить основные зависимости
	@echo "$(BLUE)Устанавливаю зависимости...$(NC)"
	. venv/bin/activate && pip install --upgrade pip
	. venv/bin/activate && pip install -r requirements.txt
	@echo "$(GREEN)✅ Зависимости установлены$(NC)"

install-dev: install ## Установить зависимости для разработки
	@echo "$(BLUE)Устанавливаю dev-зависимости...$(NC)"
	. venv/bin/activate && pip install -r requirements-dev.txt
	@echo "$(GREEN)✅ Dev-зависимости установлены$(NC)"

clean: ## Очистить кэш Python
	@echo "$(BLUE)Очищаю кэш Python...$(NC)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type f -name "*.pyd" -delete 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "build" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "dist" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ipynb_checkpoints" -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)✅ Кэш очищен$(NC)"

clean-all: clean ## Полностью очистить проект (включая venv)
	@echo "$(RED)Удаляю виртуальное окружение...$(NC)"
	rm -rf venv
	rm -rf .env
	@echo "$(GREEN)✅ Проект очищен$(NC)"

run: ## Запустить ETL пайплайн
	@echo "$(BLUE)Запускаю ETL пайплайн...$(NC)"
	. venv/bin/activate && python pipeline/run_pipeline.py

run-month: ## Запустить для конкретного месяца (make run-month MONTH=01 YEAR=2024)
	@echo "$(BLUE)Запускаю ETL для $(YEAR)-$(MONTH)...$(NC)"
	. venv/bin/activate && python pipeline/run_pipeline.py --month $(MONTH) --year $(YEAR)

test: ## Запустить тесты
	@echo "$(BLUE)Запускаю тесты...$(NC)"
	. venv/bin/activate && pytest tests/ -v

test-cov: ## Запустить тесты с покрытием
	@echo "$(BLUE)Запускаю тесты с покрытием...$(NC)"
	. venv/bin/activate && pytest tests/ -v --cov=./ --cov-report=term-missing --cov-report=html

lint: ## Проверить код линтером
	@echo "$(BLUE)Проверяю код flake8...$(NC)"
	. venv/bin/activate && flake8 pipeline/ analytics/ --max-line-length=120

format: ## Отформатировать код black и isort
	@echo "$(BLUE)Форматирую код black...$(NC)"
	. venv/bin/activate && black pipeline/ analytics/ tests/
	@echo "$(BLUE)Сортирую импорты isort...$(NC)"
	. venv/bin/activate && isort pipeline/ analytics/ tests/

precommit: ## Запустить pre-commit хуки
	@echo "$(BLUE)Запускаю pre-commit...$(NC)"
	. venv/bin/activate && pre-commit run --all-files

jupyter: ## Запустить Jupyter notebook
	@echo "$(BLUE)Запускаю Jupyter...$(NC)"
	. venv/bin/activate && jupyter notebook notebooks/

docker-up: ## Запустить PostgreSQL в Docker
	@echo "$(BLUE)Запускаю PostgreSQL...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)✅ PostgreSQL запущен на localhost:5432$(NC)"

docker-down: ## Остановить PostgreSQL
	@echo "$(BLUE)Останавливаю PostgreSQL...$(NC)"
	docker-compose down

docker-logs: ## Показать логи PostgreSQL
	docker-compose logs -f

db-init: ## Инициализировать базу данных
	@echo "$(BLUE)Создаю таблицы в БД...$(NC)"
	. venv/bin/activate && python db/init_db.py

db-backup: ## Сделать бэкап БД
	@echo "$(BLUE)Делаю бэкап БД...$(NC)"
	. venv/bin/activate && python scripts/backup_db.py

check: lint test ## Проверить код (линтер + тесты)

freeze: ## Заморозить текущие зависимости
	. venv/bin/activate && pip freeze > requirements.lock

# Значения по умолчанию для run-month
MONTH ?= 01
YEAR ?= 2024