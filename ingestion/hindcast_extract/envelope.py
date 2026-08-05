"""Bronze landing: immutable, lossless raw envelopes. Never transform here.

Layout: data/bronze/endpoint={e}/dt={YYYY-MM-DD}/hh={HH}/{location_id}_{run_id}.json.gz
`requested_at` on the envelope IS `issued_at` for forecast rows — minted here,
at request time, never inferred later from a DAG schedule (docs/PLAN.md's
single most load-bearing extractor rule).
"""

import gzip
import hashlib
import json
from datetime import datetime
from pathlib import Path

from config import EXTRACTOR_VERSION, REPO_ROOT


def land_bronze(
    *,
    endpoint: str,
    location_id: str,
    requested_at: datetime,
    http_status: int,
    url_redacted: str,
    payload: dict,
    run_id: str,
) -> Path:
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
        "image_digest": None,  # no Docker image yet — arrives Phase 1/8
        "payload": payload,
    }

    out_dir = (
        REPO_ROOT
        / "data"
        / "bronze"
        / f"endpoint={endpoint}"
        / f"dt={requested_at:%Y-%m-%d}"
        / f"hh={requested_at:%H}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{location_id}_{run_id}.json.gz"
    with gzip.open(out_path, "wt", encoding="utf-8") as f:
        json.dump(envelope, f)
    return out_path
