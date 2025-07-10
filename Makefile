.PHONY: help install test lint format clean docker-build docker-up docker-down

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	poetry install --with dev
	poetry run pre-commit install

test: ## Run tests
	poetry run pytest --cov=flight_server --cov=scripts --cov-report=term-missing

test-watch: ## Run tests in watch mode
	poetry run pytest-watch

lint: ## Run linting
	poetry run ruff check .
	poetry run black --check .
	poetry run isort --check-only .
	poetry run mypy flight_server scripts

format: ## Format code
	poetry run black .
	poetry run isort .
	poetry run ruff check . --fix

security: ## Run security checks
	poetry run bandit -r flight_server scripts
	poetry run safety check

quality: lint security test ## Run all quality checks

clean: ## Clean up temporary files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name ".coverage" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +

docker-build: ## Build Docker images
	docker-compose build

docker-up: ## Start Docker services
	docker-compose up -d

docker-down: ## Stop Docker services
	docker-compose down

docker-logs: ## View Docker logs
	docker-compose logs -f

docker-restart: ## Restart Docker services
	docker-compose down && docker-compose up -d

create-tenant: ## Create a new tenant (usage: make create-tenant TENANT_ID=my_tenant)
	poetry run python scripts/create_tenant.py --id $(TENANT_ID)

delete-tenant: ## Delete a tenant (usage: make delete-tenant TENANT_ID=my_tenant)
	poetry run python scripts/delete_tenant.py --id $(TENANT_ID)

ingest-data: ## Ingest data via Flight server
	poetry run python scripts/ingest_flight.py

query-data: ## Query data from DuckDB
	poetry run python scripts/query_duckdb.py

dbt-run: ## Run dbt transformations
	cd transform/dbt_project && poetry run dbt run

dbt-test: ## Run dbt tests
	cd transform/dbt_project && poetry run dbt test

setup-iceberg: ## Setup Iceberg tables through Trino
	poetry run python scripts/setup_iceberg_tables.py

query-trino: ## Query Trino (usage: make query-trino QUERY="SELECT * FROM iceberg.default.patient_data LIMIT 5")
	poetry run python scripts/query_trino.py --query "$(QUERY)"

query-trino-list: ## List Trino catalogs, schemas, and tables
	poetry run python scripts/query_trino.py --list

query-trino-table: ## Query specific Trino table (usage: make query-trino-table TABLE=patient_data)
	poetry run python scripts/query_trino.py --table $(TABLE)

query-trino-info: ## Get table info from Trino (usage: make query-trino-info TABLE=patient_data)
	poetry run python scripts/query_trino.py --table $(TABLE) --info

dbt-run-dev: ## Run dbt in development mode (DuckDB)
	cd transform/dbt_project && poetry run dbt run --profiles-dir config --target dev

dbt-run-prod: ## Run dbt in production mode (Trino)
	cd transform/dbt_project && poetry run dbt run --profiles-dir config --target prod

dbt-test-dev: ## Run dbt tests in development mode
	cd transform/dbt_project && poetry run dbt test --profiles-dir config --target dev

dbt-test-prod: ## Run dbt tests in production mode
	cd transform/dbt_project && poetry run dbt test --profiles-dir config --target prod

full-stack-test: ## Run full stack integration test
	@echo "Running full stack integration test..."
	make docker-up
	@echo "Waiting for services to be ready..."
	sleep 30
	make setup-iceberg
	make ingest-data
	make dbt-run-dev
	make dbt-test-dev
	make query-trino-list
	@echo "Full stack test completed successfully!"

setup: install docker-up ## Complete setup for new development environment
	@echo "Setup complete! Services are running:"
	@echo "  - MinIO Console: http://localhost:9001"
	@echo "  - Trino: http://localhost:8080"
	@echo "  - Flight Server: grpc://localhost:8815"

validate-env: ## Validate environment configuration
	@echo "Validating environment configuration..."
	@python -c "from flight_server.app.utils import validate_environment; validate_environment()"
	@echo "Environment validation passed!"

demo-architecture: ## Demonstrate the corrected architecture
	poetry run python scripts/demonstrate_architecture.py

query-orchestrator: ## Test the query orchestrator (usage: make query-orchestrator QUERY="SELECT COUNT(*) FROM local_patients")
	poetry run python -c "from flight_server.app.query_orchestrator import get_query_orchestrator; o = get_query_orchestrator(); print(o.execute_query('$(QUERY)')); o.close()"

dev-server: ## Start development server
	poetry run python -m flight_server.app.app_xorq

benchmark: ## Run performance benchmarks
	@echo "Running performance benchmarks..."
	poetry run python -m pytest tests/benchmarks/ -v

docs: ## Generate documentation
	poetry run sphinx-build -b html docs docs/_build/html

serve-docs: ## Serve documentation locally
	poetry run python -m http.server 8000 --directory docs/_build/html
