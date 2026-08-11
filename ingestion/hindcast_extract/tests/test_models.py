"""models.validate()'s contract (docs/PLAN.md: bronze is lossless -- a
validation failure is logged, never a reason to drop the payload):
never raises, always returns (bool, error-or-None).
"""

import json
from pathlib import Path

from models import validate

FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def test_accepts_well_formed_current_response():
    is_valid, error = validate("current", _load("current_200.json"))
    assert is_valid
    assert error is None


def test_accepts_well_formed_forecast_response():
    is_valid, error = validate("forecast", _load("forecast_200.json"))
    assert is_valid
    assert error is None


def test_accepts_well_formed_air_quality_response():
    is_valid, error = validate("air_quality", _load("air_quality_200.json"))
    assert is_valid
    assert error is None


def test_rejects_malformed_payload_without_raising():
    is_valid, error = validate("current", _load("current_malformed.json"))
    assert not is_valid
    assert error is not None
    assert "main" in error


def test_rejects_a_401_error_body_without_raising():
    # A 401 error body isn't a CurrentWeatherResponse shape at all --
    # validate() must report it invalid, never crash, since bronze lands it
    # either way.
    is_valid, error = validate("current", _load("current_401.json"))
    assert not is_valid
    assert error is not None


def test_returns_false_for_an_unregistered_endpoint():
    is_valid, error = validate("geocode", {})
    assert not is_valid
    assert "no validation model registered" in error
