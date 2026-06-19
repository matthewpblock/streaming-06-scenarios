"""src/streaming/data_engineering/derived_fields-ag.py.

Enriches AIS messages for Chinese military/government vessels with threat ranges
and risk levels loaded from the threat_ranges_critical-section-ag.json library.
"""

import json
import logging
from pathlib import Path
from typing import Any

# Configure logger
LOG = logging.getLogger(__name__)

# Resolve path to the JSON file in the same directory
_CURRENT_DIR = Path(__file__).parent
_THREAT_RANGES_JSON = _CURRENT_DIR / "threat_ranges_critical-section-ag.json"

# Load the static library
try:
    with _THREAT_RANGES_JSON.open(encoding="utf-8") as f:
        _THREAT_DATA = json.load(f)
    LOG.info(f"Loaded threat ranges from {_THREAT_RANGES_JSON.name}")
except Exception as error:
    LOG.error(f"Failed to load threat ranges library: {error}")
    _THREAT_DATA = {
        "vessel_types": {},
        "default": {
            "category": "Government / Military (Other)",
            "threat_range_km": 15.0,
            "risk_level": "LOW",
            "description": "Unclassified Chinese Govt/Military Vessel (Default Profile)",
        },
    }

__all__ = ["enrich_message"]


def enrich_message(
    row: dict[str, Any],
    *args: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    """Add threat ranges and risk levels to a raw Chinese military/government vessel record.

    Looks up the ship_type of the vessel against the static threat ranges library.

    Arguments:
        row: A validated raw AIS message dictionary.
        *args: Ignored, for signature compatibility.
        **kwargs: Ignored, for signature compatibility.

    Returns:
        A new dict containing all original fields plus threat derived fields.
    """
    enriched = dict(row)

    # Get ship type and look up in static library
    ship_type = row.get("ship_type")
    ship_type_str = str(ship_type) if ship_type is not None else ""

    vessel_types = _THREAT_DATA.get("vessel_types", {})
    if ship_type_str in vessel_types:
        profile = vessel_types[ship_type_str]
    else:
        profile = _THREAT_DATA.get("default", {})

    # Add enriched fields
    enriched["vessel_category"] = profile.get(
        "category", "Government / Military (Other)"
    )
    enriched["threat_range_km"] = float(profile.get("threat_range_km", 15.0))
    enriched["risk_level"] = profile.get("risk_level", "LOW")
    enriched["threat_description"] = profile.get(
        "description", "Unclassified Chinese Govt/Military Vessel"
    )

    return enriched
