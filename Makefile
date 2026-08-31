.PHONY: help env install lock sync test test-watch lint format security quality clean \
	docker-build docker-up docker-down docker-logs docker-restart

UV ?= uv
RUN := $(UV) run
COMPOSE := docker compose

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

env: ## Create .env from the template if it does not exist
	@test -f .env || (cp .devcontainer/.env.example .env && echo "Created .env — review the secrets before 'make docker-up'")

install: sync ## Install dependencies + git hooks
	$(RUN) pre-commit install

lock: ## Refresh uv.lock
	$(UV) lock

sync: ## Install the locked dependency set (incl. dev)
	$(UV) sync --frozen

test: ## Run tests
	$(RUN) pytest --cov=flight_server --cov=scripts --cov=datahut_duckhouse --cov-report=term-missing

test-watch: ## Run tests in watch mode
	$(RUN) pytest-watch

lint: ## Run linting
	$(RUN) ruff check .
	$(RUN) black --check .
	$(RUN) mypy flight_server scripts datahut_duckhouse

format: ## Format code
	$(RUN) ruff check . --fix
	$(RUN) black .

security: ## Run security checks
	$(RUN) bandit -r flight_server scripts
	$(RUN) safety check

quality: lint security test ## Run all quality checks

clean: ## Clean up temporary files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name ".coverage" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +

docker-build: ## Build Docker images
	$(COMPOSE) build

docker-up: env ## Start Docker services
	$(COMPOSE) up -d

docker-down: ## Stop Docker services
	$(COMPOSE) down

docker-logs: ## View Docker logs
	$(COMPOSE) logs -f

docker-restart: ## Restart Docker services
	$(COMPOSE) down && $(COMPOSE) up -d

create-tenant: ## Create a new tenant (usage: make create-tenant TENANT_ID=my_tenant)
	$(RUN) python scripts/create_tenant.py --id $(TENANT_ID)

delete-tenant: ## Delete a tenant (usage: make delete-tenant TENANT_ID=my_tenant)
	$(RUN) python scripts/delete_tenant.py --id $(TENANT_ID)

ingest-data: ## Ingest data via Flight server
	$(RUN) python scripts/ingest_flight.py

query-data: ## Query data from DuckDB
	$(RUN) python scripts/query_duckdb.py

dbt-run: ## Run dbt transformations
	cd transform/dbt_project && $(RUN) dbt run

dbt-test: ## Run dbt tests
	cd transform/dbt_project && $(RUN) dbt test

setup-iceberg: ## Setup Iceberg tables through Trino
	$(RUN) python scripts/setup_iceberg_tables.py

query-trino: ## Query Trino (usage: make query-trino QUERY="SELECT ...")
	$(RUN) python scripts/query_trino.py --query "$(QUERY)"

query-trino-list: ## List Trino catalogs, schemas, and tables
	$(RUN) python scripts/query_trino.py --list

query-trino-table: ## Query specific Trino table (usage: make query-trino-table TABLE=patient_data)
	$(RUN) python scripts/query_trino.py --table $(TABLE)

query-trino-info: ## Get table info from Trino (usage: make query-trino-info TABLE=patient_data)
	$(RUN) python scripts/query_trino.py --table $(TABLE) --info

dbt-run-dev: ## Run dbt in development mode (DuckDB)
	cd transform/dbt_project && $(RUN) dbt run --profiles-dir config --target dev

dbt-run-prod: ## Run dbt in production mode (Trino)
	cd transform/dbt_project && $(RUN) dbt run --profiles-dir config --target prod

dbt-test-dev: ## Run dbt tests in development mode
	cd transform/dbt_project && $(RUN) dbt test --profiles-dir config --target dev

dbt-test-prod: ## Run dbt tests in production mode
	cd transform/dbt_project && $(RUN) dbt test --profiles-dir config --target prod

full-stack-test: ## Run full stack integration test
	@echo "Running full stack integration test..."
	$(MAKE) docker-up
	@echo "Waiting for services to be ready..."
	sleep 30
	$(MAKE) setup-iceberg
	$(MAKE) ingest-data
	$(MAKE) dbt-run-dev
	$(MAKE) dbt-test-dev
	$(MAKE) query-trino-list
	@echo "Full stack test completed successfully!"

setup: install docker-up ## Complete setup for new development environment
	@echo "Setup complete! Services are running:"
	@echo "  - MinIO Console: http://localhost:9001"
	@echo "  - Trino: http://localhost:8080"
	@echo "  - Flight Server: grpc://localhost:8815"

validate-env: ## Validate environment configuration
	@echo "Validating environment configuration..."
	@$(RUN) python -c "from flight_server.app.utils import validate_environment; validate_environment()"
	@echo "Environment validation passed!"

demo-architecture: ## Demonstrate the corrected architecture
	$(RUN) python scripts/demonstrate_architecture.py

query-diagnostics: ## Analyze a query's shape/complexity (usage: make query-diagnostics QUERY="SELECT ...")
	$(RUN) python -c "from flight_server.app.query_diagnostics import analyze_query; print(analyze_query('$(QUERY)'))"

dev-server: ## Start development server
	$(RUN) python -m flight_server.app.app_xorq

benchmark: ## Run performance benchmarks
	$(RUN) python -m pytest tests/benchmarks/ -v

docs: ## Generate documentation
	$(RUN) sphinx-build -b html docs docs/_build/html

serve-docs: ## Serve documentation locally
	$(RUN) python -m http.server 8000 --directory docs/_build/html
