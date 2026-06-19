# tests/test_data_engineering.py
"""Tests for data engineering calculations."""

import importlib

_derived = importlib.import_module("streaming.data_engineering.derived_fields-ag")
enrich_message = _derived.enrich_message


def test_enrich_message_adds_vessel_category_and_risk_level() -> None:
    """Enriched messages should include threat ranges and risk levels."""
    row = {
        "mmsi": "412123456",
        "ship_type": "30",
    }

    enriched = enrich_message(row)

    assert "vessel_category" in enriched
    assert "threat_range_km" in enriched
    assert "risk_level" in enriched
    assert "threat_description" in enriched


def test_enrich_message_keeps_original_fields() -> None:
    """Enriched messages should preserve original message fields."""
    row = {
        "mmsi": "412123456",
        "ship_name": "TEST SHIP",
        "latitude": 30.0,
        "longitude": 120.0,
    }

    enriched = enrich_message(row)

    assert enriched["mmsi"] == row["mmsi"]
    assert enriched["ship_name"] == row["ship_name"]
    assert enriched["latitude"] == row["latitude"]
    assert enriched["longitude"] == row["longitude"]
