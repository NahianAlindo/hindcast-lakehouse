"""Small state blobs in ADLS -- currently just the forecast dedup tracker.

Kept in the same storage system as bronze itself (not a local file) so it
stays consistent regardless of whether a run happens on GitHub Actions'
ephemeral runners or the persistent VM.
"""

import json

from azure.storage.blob import BlobServiceClient
from config import AZURE_STORAGE_CONNECTION_STRING, BLOB_CONTAINER_BRONZE

_STATE_BLOB_PATH = "_state/forecast_last_hash.json"

_blob_service_client: BlobServiceClient | None = None


def _client() -> BlobServiceClient:
    global _blob_service_client
    if _blob_service_client is None:
        _blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )
    return _blob_service_client


def read_last_forecast_hashes() -> dict[str, str]:
    """Returns {location_id: last_payload_sha256}. Empty dict if no state yet."""
    container = _client().get_container_client(BLOB_CONTAINER_BRONZE)
    blob = container.get_blob_client(_STATE_BLOB_PATH)
    if not blob.exists():
        return {}
    return json.loads(blob.download_blob().readall())


def write_last_forecast_hashes(hashes: dict[str, str]) -> None:
    container = _client().get_container_client(BLOB_CONTAINER_BRONZE)
    container.upload_blob(name=_STATE_BLOB_PATH, data=json.dumps(hashes), overwrite=True)
