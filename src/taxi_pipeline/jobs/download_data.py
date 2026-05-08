from __future__ import annotations

import argparse
from pathlib import Path

import requests

from taxi_pipeline.settings import load_config, project_path
from taxi_pipeline.utils.filesystem import ensure_dir


def download_file(url: str, destination: Path) -> None:
    """Download a file with streaming to avoid loading it fully into memory."""
    if destination.exists() and destination.stat().st_size > 0:
        print(f"[SKIP] File already exists: {destination}")
        return

    print(f"[DOWNLOAD] {url}")
    destination.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with destination.open("wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)

    print(f"[OK] Saved: {destination}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download NYC TLC input files.")
    parser.add_argument("--config", default="configs/local.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)

    green_dir = ensure_dir(project_path(cfg["paths"]["landing_green_taxi"]))
    zone_dir = ensure_dir(project_path(cfg["paths"]["landing_taxi_zone_lookup"]))

    for month in cfg["sources"]["months"]:
        url = cfg["sources"]["green_taxi_url_template"].format(month=month)
        destination = green_dir / f"green_tripdata_{month}.parquet"
        download_file(url, destination)

    zone_url = cfg["sources"]["taxi_zone_lookup_url"]
    download_file(zone_url, zone_dir / "taxi_zone_lookup.csv")


if __name__ == "__main__":
    main()
