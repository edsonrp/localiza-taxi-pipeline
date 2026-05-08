PYTHON ?= python
VENV := .venv
CONFIG := configs/local.yaml

.PHONY: setup install download ingest test clean-data

setup:
	$(PYTHON) -m venv $(VENV)

install:
	pip install --upgrade pip
	pip install -r requirements.txt
	pip install -e .

download:
	python -m taxi_pipeline.jobs.download_data --config $(CONFIG)

ingest:
	python -m taxi_pipeline.jobs.ingest_raw --config $(CONFIG)

test:
	pytest -q

clean-data:
	rm -rf data/bronze data/silver data/gold data/audit
	mkdir -p data/bronze data/silver data/gold data/audit
