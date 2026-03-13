# =============================================================================
# Makefile — Common Development Commands
# =============================================================================

.PHONY: help install test lint format clean run clean-data

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -r requirements.txt
	pip install -e ".[dev]"

test: ## Run unit tests with coverage
	python -m pytest tests/ -v --tb=short --cov=src --cov-report=term-missing --cov-fail-under=80

test-quick: ## Run tests without coverage (faster)
	python -m pytest tests/ -v --tb=short

lint: ## Run linter (ruff)
	ruff check src/ tests/

format: ## Auto-format code (black + ruff fix)
	black src/ tests/
	ruff check src/ tests/ --fix

clean: ## Remove build artifacts and caches
	rm -rf __pycache__ .pytest_cache .coverage htmlcov
	rm -rf src/__pycache__ tests/__pycache__
	rm -rf *.egg-info build dist
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

check: lint test ## Run lint + tests (CI equivalent)

run: ## Run full pipeline locally (no Azure needed)
	python scripts/run_pipeline_local.py

run-debug: ## Run pipeline locally, skip quality gate
	python scripts/run_pipeline_local.py --skip-quality-gate

clean-data: ## Remove local Delta Lake data
	rm -rf /tmp/lakehouse /tmp/test_scd
