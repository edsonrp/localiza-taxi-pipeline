PYTHON ?= python
VENV := .venv
CONFIG := configs/local.yaml

.PHONY: setup install download ingest silver gold run test clean-data

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

ingest:
	PYTHONPATH=src $(PYTHON) -m taxi_pipeline.jobs.ingest_raw --config $(CONFIG)

silver:
	PYTHONPATH=src $(PYTHON) -m taxi_pipeline.jobs.build_silver --config $(CONFIG)

gold:
	PYTHONPATH=src $(PYTHON) -m taxi_pipeline.jobs.build_gold --config $(CONFIG)

run: download ingest silver gold

test:
	PYTHONPATH=src $(PYTHON) -m pytest -q
	
clean-data:
	rm -rf data/bronze data/silver data/gold data/audit
	mkdir -p data/bronze data/silver data/gold data/audit
