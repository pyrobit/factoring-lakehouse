PY := .venv/bin/python
PIP := .venv/bin/pip
AS_OF_DAY1 := 2026-10-31
AS_OF_DAY2 := 2026-11-01

.PHONY: setup lint test bdd test-all data demo snowflake-dry-run dbt-parse up down clean

setup:
	python3 -m venv .venv
	$(PIP) install -q --upgrade pip
	$(PIP) install -q -e ".[dev]"

lint:
	.venv/bin/ruff check src tests airflow databricks
	.venv/bin/sqlfluff lint snowflake/sql --dialect snowflake

test:
	.venv/bin/pytest

data:
	$(PY) -m factoring_lakehouse.datagen --out data/landing --seed 42

demo: data
	$(PY) -m factoring_lakehouse.pipeline --batch day1 --as-of $(AS_OF_DAY1)
	$(PY) -m factoring_lakehouse.pipeline --batch day2 --as-of $(AS_OF_DAY2)
	$(PY) -m factoring_lakehouse.pipeline --report --as-of $(AS_OF_DAY2)

snowflake-dry-run:
	$(PY) -m factoring_lakehouse.snowflake_io.loader --as-of $(AS_OF_DAY2) --dry-run

dbt-parse:
	cd dbt && ../.venv/bin/dbt parse --profiles-dir ci --no-partial-parse

bdd:
	.venv/bin/behave tests/features -f progress

test-all: test bdd

up:
	docker compose up -d

down:
	docker compose down -v

clean:
	rm -rf data spark-warehouse metastore_db derby.log .pytest_cache
