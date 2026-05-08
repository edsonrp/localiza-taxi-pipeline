from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_config(config_path: str | Path) -> dict[str, Any]:
    """Load YAML config and return a dictionary."""
    path = Path(config_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def project_path(relative_path: str | Path) -> Path:
    """Resolve a project-relative path."""
    path = Path(relative_path)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path
