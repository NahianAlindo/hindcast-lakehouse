"""Environment + location config for the Hindcast extractor.

Phase-2 MVP: flat module layout so scripts run directly (python
ingestion/hindcast_extract/run_current.py) without needing -m or a package
install. Revisit as a proper installed package once Phase 1/8 bakes this into
a Docker image with a real entrypoint.
"""

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(REPO_ROOT / ".env")

OWM_API_KEY = os.environ["OWM_API_KEY"]
OWM_BASE_URL = os.environ.get("OWM_BASE_URL", "https://api.openweathermap.org")
EXTRACTOR_VERSION = "0.2.0-dev"

AZURE_STORAGE_CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
BLOB_CONTAINER_BRONZE = "bronze"


def load_locations() -> list[dict]:
    path = REPO_ROOT / "ingestion" / "config" / "locations.yml"
    return yaml.safe_load(path.read_text())["locations"]
