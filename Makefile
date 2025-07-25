# Makefile
.PHONY: install test lint format docker-build docker-run clean help

# Variables
PYTHON = python3
PIP = pip3
DOCKER_IMAGE = snowflake-validator
CONFIG_FILE = config/config.yaml

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

install-dev: ## Install development dependencies
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements-dev.txt
	$(PIP) install -e .

test: ## Run tests
	pytest tests/ -v

test-coverage: ## Run tests with coverage
	pytest tests/ --cov=snowflake_validator --cov-report=html --cov-report=term

lint: ## Run linting
	flake8 snowflake_validator/
	pylint snowflake_validator/
	mypy snowflake_validator/

format: ## Format code
	black snowflake_validator/
	isort snowflake_validator/

validate-config: ## Validate configuration
	$(PYTHON) snowflake_validator.py --dry-run --config $(CONFIG_FILE)

run: ## Run validation with default config
	$(PYTHON) snowflake_validator.py --config $(CONFIG_FILE)

run-dev: ## Run validation with development config
	$(PYTHON) snowflake_validator.py --config config/development.yaml

run-prod: ## Run validation with production config
	$(PYTHON) snowflake_validator.py --config config/production.yaml

resume: ## Resume from last execution
	$(PYTHON) snowflake_validator.py --resume --config $(CONFIG_FILE)

retry-failed: ## Retry only failed queries
	$(PYTHON) snowflake_validator.py --retry-failed --config $(CONFIG_FILE)

docker-build: ## Build Docker image
	docker build -t $(DOCKER_IMAGE) .

docker-run: ## Run in Docker container
	docker run -v $(PWD)/config:/app/config:ro -v $(PWD)/output:/app/output $(DOCKER_IMAGE) --config /app/config/config.yaml

docker-compose-up: ## Start with docker-compose
	docker-compose up -d

docker-compose-down: ## Stop docker-compose services
	docker-compose down

clean: ## Clean temporary files
	rm -rf build/ dist/ *.egg-info/
	rm -rf output/tmp/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

clean-output: ## Clean output directories
	rm -rf output/exports/ output/reports/ output/state/
	mkdir -p output/exports output/reports output/state

setup-dirs: ## Create necessary directories
	mkdir -p output/{exports/{source_data,target_data,differences},reports/{json/{individual,summary},html/{individual,summary}},state,logs}
	mkdir -p config queries tmp

init: setup-dirs install ## Initialize project (create dirs and install deps)

check-deps: ## Check if all dependencies are installed
	$(PYTHON) -c "import snowflake.connector, duckdb, pandas, yaml, jinja2, click, rich; print('All dependencies installed successfully')"