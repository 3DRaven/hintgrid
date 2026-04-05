.PHONY: install install-dev test test-integration test-unit lint format type-check clean help docker-up docker-down docker-logs docker-test-up docker-test-down docker-test-logs

# Default Python interpreter
PYTHON := python

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)HintGrid Development Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

install: ## Install package in production mode
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e .

install-dev: ## Install package in development mode with all dev dependencies
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e ".[dev]"
	@echo "$(GREEN)Development environment ready!$(NC)"

test: ## Run all tests
	pytest -v

test-unit: ## Run unit tests only (fast)
	pytest -v -m "not integration"

test-unit-parallel: ## Run unit tests in parallel (4 workers)
	pytest tests/unit/ -n 4 -v

test-integration: ## Run integration tests (requires Docker)
	pytest -v -m integration

test-smoke: ## Run smoke tests (quick sanity check)
	pytest -v -m smoke

test-fast: ## Run fast tests: unit parallel + smoke
	pytest tests/unit/ -n auto -v
	pytest -v -m smoke

test-parallel: ## Run all tests in parallel (requires pytest-xdist)
	pytest tests/unit/ -n auto -v
	@echo "Integration tests run sequentially for container isolation"
	pytest tests/integration/ -v

lint: ## Run linter (ruff)
	ruff check .

lint-fix: ## Run linter and auto-fix issues
	ruff check --fix .

format: ## Format code with ruff
	ruff format .

type-check: ## Run type checkers (mypy + pyright)
	mypy src/hintgrid
	pyright

check: lint type-check ## Run all checks (lint + type-check)

qa: format lint-fix type-check test ## Full QA: format, lint, type-check, test

clean: ## Remove build artifacts and cache files
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf src/*.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "$(GREEN)Cleaned!$(NC)"

run: ## Run HintGrid pipeline
	$(PYTHON) -m hintgrid.app run

run-dry: ## Run HintGrid pipeline in dry-run mode (no Redis writes)
	$(PYTHON) -m hintgrid.app run --dry-run

export-state: ## Export current state to Markdown (usage: make export-state FILE=state.md)
	$(PYTHON) -m hintgrid.app export $(FILE)

docker-up: ## Start development containers (Neo4j, Redis, PostgreSQL)
	docker compose -f docker-compose.dev.yml up -d

docker-down: ## Stop development containers
	docker compose -f docker-compose.dev.yml down

docker-logs: ## View container logs
	docker compose -f docker-compose.dev.yml logs -f

docker-test-up: ## Start test infrastructure containers
	docker compose -f docker-compose.test.yml up -d --wait

docker-test-down: ## Stop and remove test containers
	docker compose -f docker-compose.test.yml down -v --remove-orphans

docker-test-logs: ## View test container logs
	docker compose -f docker-compose.test.yml logs -f
