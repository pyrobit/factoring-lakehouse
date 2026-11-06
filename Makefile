# VENV is overridable so the same targets run inside the Docker image (VENV=/opt/venv).
VENV ?= .venv
BIN := $(VENV)/bin
PY := $(BIN)/python
PIP := $(BIN)/pip
AS_OF_DAY1 := 2026-10-31
AS_OF_DAY2 := 2026-11-01

.PHONY: setup lint test bdd test-all data demo snowflake-dry-run dbt-parse up docker-build docker-demo docker-test airflow-up airflow-down clean

setup:
	python3 -m venv .venv
	$(PIP) install -q --upgrade pip
	$(PIP) install -q -e ".[dev]"

lint:
	$(BIN)/ruff check src tests airflow databricks
	$(BIN)/sqlfluff lint snowflake/sql --dialect snowflake

test:
	$(BIN)/pytest

data:
	$(PY) -m factoring_lakehouse.datagen --out data/landing --seed 42

demo: data
	$(PY) -m factoring_lakehouse.pipeline --batch day1 --as-of $(AS_OF_DAY1)
	$(PY) -m factoring_lakehouse.pipeline --batch day2 --as-of $(AS_OF_DAY2)
	$(PY) -m factoring_lakehouse.pipeline --report --as-of $(AS_OF_DAY2)

snowflake-dry-run:
	$(PY) -m factoring_lakehouse.snowflake_io.loader --as-of $(AS_OF_DAY2) --dry-run

dbt-parse:
	cd dbt && $(abspath $(BIN))/dbt parse --profiles-dir ci --no-partial-parse

bdd:
	$(BIN)/behave tests/features -f progress

test-all: test bdd

# ---- containers -------------------------------------------------------------------------
up:                 ## Postgres + MongoDB sources only
	docker compose up -d postgres mongo

docker-build:       ## build the pipeline image and the Airflow image
	docker compose --profile pipeline --profile airflow build

docker-demo:        ## run the full demo inside the pipeline image (needs only Docker)
	docker compose --profile pipeline run --rm pipeline make demo

docker-test:        ## run pytest + behave inside the pipeline image
	docker compose --profile pipeline run --rm pipeline make test-all

airflow-up:         ## sources + Airflow (UI on http://localhost:8080, admin/admin)
	docker compose --profile airflow up -d

airflow-down:
	docker compose --profile airflow --profile pipeline down -v

clean:
	rm -rf data spark-warehouse metastore_db derby.log .pytest_cache
