"""Bronze landing: immutable, lossless raw envelopes, written to ADLS Gen2.

Layout: bronze/endpoint={e}/dt={YYYY-MM-DD}/hh={HH}/{location_id}_{run_id}.json.gz
`requested_at` on the envelope IS `issued_at` for forecast rows -- minted here,
at request time, never inferred later from a DAG schedule (docs/PLAN.md's
single most load-bearing extractor rule).

Was local disk / git-committed via GitHub Actions as an interim stopgap before
Phase 1 provisioned ADLS. That stopgap is retired now that ADLS exists.
"""

import gzip
import hashlib
import json
from datetime import datetime
from io import BytesIO

from azure.storage.blob import BlobServiceClient
from config import AZURE_STORAGE_CONNECTION_STRING, BLOB_CONTAINER_BRONZE, EXTRACTOR_VERSION

_blob_service_client: BlobServiceClient | None = None


def _client() -> BlobServiceClient:
    global _blob_service_client
    if _blob_service_client is None:
        _blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_STORAGE_CONNECTION_STRING
        )
    return _blob_service_client


def land_bronze(
    *,
    endpoint: str,
    location_id: str,
    requested_at: datetime,
    http_status: int,
    url_redacted: str,
    payload: dict,
    run_id: str,
    is_new_model_run: bool | None = None,
    validation_error: str | None = None,
) -> str:
    payload_bytes = json.dumps(payload).encode()
    payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()

    envelope = {
        "run_id": run_id,
        "endpoint": endpoint,
        "location_id": location_id,
        "requested_at": requested_at.isoformat(),
        "http_status": http_status,
        "url_redacted": url_redacted,
        "payload_sha256": payload_sha256,
        "extractor_version": EXTRACTOR_VERSION,
        "image_digest": None,  # no Docker image yet -- arrives Phase 8
        "is_new_model_run": is_new_model_run,
        "validation_error": validation_error,
        "payload": payload,
    }

    blob_path = (
        f"endpoint={endpoint}"
        f"/dt={requested_at:%Y-%m-%d}"
        f"/hh={requested_at:%H}"
        f"/{location_id}_{run_id}.json.gz"
    )

    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(json.dumps(envelope).encode())

    container = _client().get_container_client(BLOB_CONTAINER_BRONZE)
    container.upload_blob(name=blob_path, data=buf.getvalue(), overwrite=True)

    return blob_path
