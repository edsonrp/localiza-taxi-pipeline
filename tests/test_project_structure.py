from pathlib import Path


def test_expected_project_files_exist():
    root = Path(__file__).resolve().parents[1]
    expected_files = [
        "README.md",
        "configs/local.yaml",
        "src/taxi_pipeline/jobs/download_data.py",
        "src/taxi_pipeline/jobs/ingest_raw.py",
        "docs/architecture.md",
    ]
    for file_name in expected_files:
        assert (root / file_name).exists(), f"Missing {file_name}"
