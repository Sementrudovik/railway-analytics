# Загружаем переменные из .env
include .env
export $(shell sed 's/=.*//' .env)

# Docker команды
docker-up:
	@echo "Starting PostgreSQL container..."
	docker-compose up -d
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 5
	@make docker-status

docker-down:
	@echo "Stopping PostgreSQL container..."
	docker-compose down
	@echo "PostgreSQL stopped"

docker-restart: docker-down docker-up

docker-logs:
	docker-compose logs -f postgres

docker-shell:
	docker exec -it railway_postgres psql -U $(DB_USER) -d $(DB_NAME)

docker-backup:
	@echo "Creating backup..."
	docker exec railway_postgres pg_dump -U $(DB_USER) -d $(DB_NAME) > db/backups/backup_$$(date +%Y%m%d_%H%M%S).sql
	@echo "Backup created in db/backups/"

docker-restore:
	@read -p "Enter backup filename: " filename; \
	docker exec -i railway_postgres psql -U $(DB_USER) -d $(DB_NAME) < db/backups/$$filename

docker-status:
	@echo "Container status:"
	docker-compose ps
	@echo "\nPostgreSQL connection details:"
	@echo "Host: localhost"
	@echo "Port: $(DB_PORT)"
	@echo "Database: $(DB_NAME)"
	@echo "User: $(DB_USER)"
	@echo "Password: $(DB_PASSWORD)"
	@echo "\npgAdmin: http://localhost:$(PGADMIN_PORT)"
	@echo "pgAdmin email: $(PGADMIN_EMAIL)"
	@echo "pgAdmin password: $(PGADMIN_PASSWORD)"

docker-init: docker-up
	@echo "Initializing database schema..."
	@sleep 10
	docker exec -i railway_postgres psql -U $(DB_USER) -d $(DB_NAME) < db/init/01_create_extensions.sql
	docker exec -i railway_postgres psql -U $(DB_USER) -d $(DB_NAME) < db/init/02_create_schema.sql
	docker exec -i railway_postgres psql -U $(DB_USER) -d $(DB_NAME) < db/init/03_create_tables.sql
	docker exec -i railway_postgres psql -U $(DB_USER) -d $(DB_NAME) < db/init/04_create_functions.sql
	@echo "Database initialized"


load-data:
	@echo "Loading data from XLSB files..."
	source venv/bin/activate && python pipeline/load_data.py

load-data-limit:
	@read -p "Limit number of files: " limit; \
	source venv/bin/activate && python pipeline/load_data.py --limit $$limit

test-load:
	@echo "Testing data load..."
	source venv/bin/activate && python tests/test_load_data.py