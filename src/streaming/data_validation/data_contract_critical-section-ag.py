"""src/streaming/data_validation/data_contract_critical-section-ag.py.

Defines the data contract for AIS telemetry data, including required fields,
field ordering, and validation schemas.
"""

from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.data_validation.types import ValidationResult
from datafun_streaming.data_validation.validation_utils import (
    validate_required_fields,
)

# === REQUIRED AND OPTIONAL FIELDS ===

AIS_REQUIRED_FIELDS: Final[list[str]] = [
    "mmsi",
    "ship_name",
    "latitude",
    "longitude",
    "timestamp",
    "message_type",
]

AIS_OPTIONAL_FIELDS: Final[list[str]] = [
    "ship_type",
    "cog",
    "sog",
    "heading",
    "destination",
    "call_sign",
    "imo_number",
]

VALID_AIS_FIELDNAMES: Final[list[str]] = [
    *AIS_REQUIRED_FIELDS,
    *AIS_OPTIONAL_FIELDS,
]

# === CONSUMED FIELDNAMES (including enrichment & kafka metadata) ===

CONSUMED_FIELDNAMES: Final[list[str]] = [
    *VALID_AIS_FIELDNAMES,
    "vessel_category",
    "threat_range_km",
    "risk_level",
    "threat_description",
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]

REJECTED_AIS_FIELDNAMES: Final[list[str]] = [
    *AIS_REQUIRED_FIELDS,
    "validation_errors",
]


def validate_ais_record(
    *,
    record: DataRecordDict,
    **kwargs: Any,
) -> ValidationResult:
    """Validate one AIS record against the data contract.

    Checks required fields and parses coordinates.

    Arguments:
        record: The AIS telemetry message to validate.
        **kwargs: Unused parameters for signature compatibility.

    Returns:
        A ValidationResult indicating validation status and list of errors.
    """
    errors: list[str] = []

    # 1. Validate required fields
    str_record = {k: str(v) if v is not None else "" for k, v in record.items()}
    errors.extend(
        validate_required_fields(record=str_record, required_fields=AIS_REQUIRED_FIELDS)
    )

    if errors:
        return ValidationResult(is_valid=False, errors=errors)

    # 2. Validate coordinates range
    try:
        lat = float(record["latitude"])
        if not (-90.0 <= lat <= 90.0):
            errors.append(f"Latitude out of bounds [-90, 90]: {lat}")
    except ValueError, TypeError:
        errors.append(f"Invalid latitude: {record['latitude']!r}")

    try:
        lon = float(record["longitude"])
        if not (-180.0 <= lon <= 180.0):
            errors.append(f"Longitude out of bounds [-180, 180]: {lon}")
    except ValueError, TypeError:
        errors.append(f"Invalid longitude: {record['longitude']!r}")

    # 3. Validate Chinese MMSI prefix (China Maritime Identification Digits)
    mmsi = str(record["mmsi"]).strip()
    if not (mmsi.startswith("412") or mmsi.startswith("413") or mmsi.startswith("414")):
        errors.append(
            f"MMSI {mmsi!r} does not have a Chinese maritime prefix (412, 413, 414)"
        )

    has_errors = bool(errors)
    return ValidationResult(is_valid=not has_errors, errors=errors)


def keep_ais_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Return only standard AIS fields.

    Arguments:
        row: The original message dictionary.

    Returns:
        A new dict containing only fields in VALID_AIS_FIELDNAMES.
    """
    return {field: row.get(field, "") for field in VALID_AIS_FIELDNAMES}
