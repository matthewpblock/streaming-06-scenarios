"""src/streaming/storage/storage_critical-section-ag.py.

Handles storage of consumed AIS telemetry into a DuckDB database.
Differentiates between valid enriched records and rejected records.
"""

import importlib
from pathlib import Path
from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_toolkit.logger import get_logger
import duckdb

# === DYNAMIC IMPORTS due to hyphens in names ===
_contract = importlib.import_module(
    "streaming.data_validation.data_contract_critical-section-ag"
)
CONSUMED_FIELDNAMES = _contract.CONSUMED_FIELDNAMES
REJECTED_AIS_FIELDNAMES = _contract.REJECTED_AIS_FIELDNAMES

_validation = importlib.import_module(
    "streaming.data_validation.data_validation_critical-section-ag"
)
add_validation_errors = _validation.add_validation_errors

# === DECLARE EXPORTS ===
__all__ = [
    "clear_storage_tables",
    "create_storage_tables",
    "init_db",
    "log_storage_summary",
    "write_rejected_record",
    "write_valid_record",
    "VALID_TABLE_NAME",
    "REJECTED_TABLE_NAME",
]

# === CONFIGURE LOGGER ===
LOG = get_logger("C06-STORAGE", level="DEBUG")

# === TABLE NAMES ===
VALID_TABLE_NAME: Final[str] = "consumed_valid_ais"
REJECTED_TABLE_NAME: Final[str] = "consumed_rejected_ais"

CONSUMED_VALID_FIELDNAMES: Final[list[str]] = CONSUMED_FIELDNAMES
CONSUMED_REJECTED_FIELDNAMES: Final[list[str]] = [
    *REJECTED_AIS_FIELDNAMES,
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]


def clean_valid_consumed_record(record: dict[str, Any]) -> dict[str, Any]:
    """Filter fields for the valid consumed table."""
    return {field: record.get(field) for field in CONSUMED_VALID_FIELDNAMES}


def clean_rejected_consumed_record(record: dict[str, Any]) -> dict[str, Any]:
    """Filter fields for the rejected consumed table."""
    return {field: record.get(field) for field in CONSUMED_REJECTED_FIELDNAMES}


def create_storage_tables(db_path: Path) -> None:
    """Create consumed AIS tables with appropriate data types if they do not exist."""
    with duckdb.connect(str(db_path)) as conn:
        # Create valid table with precise data types
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {VALID_TABLE_NAME} (
                mmsi VARCHAR,
                ship_name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                timestamp VARCHAR,
                message_type VARCHAR,
                ship_type INTEGER,
                cog DOUBLE,
                sog DOUBLE,
                heading DOUBLE,
                destination VARCHAR,
                call_sign VARCHAR,
                imo_number INTEGER,
                vessel_category VARCHAR,
                threat_range_km DOUBLE,
                risk_level VARCHAR,
                threat_description VARCHAR,
                _kafka_key VARCHAR,
                _kafka_partition INTEGER,
                _kafka_offset BIGINT
            )
        """)

        # Create rejected table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {REJECTED_TABLE_NAME} (
                mmsi VARCHAR,
                ship_name VARCHAR,
                latitude VARCHAR,
                longitude VARCHAR,
                timestamp VARCHAR,
                message_type VARCHAR,
                validation_errors VARCHAR,
                _kafka_key VARCHAR,
                _kafka_partition INTEGER,
                _kafka_offset BIGINT
            )
        """)
    LOG.info(
        f"Initialized tables {VALID_TABLE_NAME} and {REJECTED_TABLE_NAME} in DuckDB."
    )


def clear_storage_tables(db_path: Path) -> None:
    """Truncate tables for a fresh run."""
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(f"DELETE FROM {VALID_TABLE_NAME}")  # noqa: S608
        conn.execute(f"DELETE FROM {REJECTED_TABLE_NAME}")  # noqa: S608
    LOG.info("Cleared storage tables.")


def init_db(db_path: Path) -> None:
    """Initialize database by ensuring tables exist and clearing prior run data."""
    create_storage_tables(db_path)
    clear_storage_tables(db_path)


def write_valid_record(db_path: Path, record: DataRecordDict) -> None:
    """Write one valid enriched AIS telemetry record to DuckDB."""
    clean_record = clean_valid_consumed_record(record)
    placeholders = ", ".join(["?"] * len(CONSUMED_VALID_FIELDNAMES))
    insert_sql = f"INSERT INTO {VALID_TABLE_NAME} VALUES ({placeholders})"  # noqa: S608

    # Order values to match schema
    insert_values = [clean_record[field] for field in CONSUMED_VALID_FIELDNAMES]

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(insert_sql, insert_values)


def write_rejected_record(
    db_path: Path, record: DataRecordDict, errors: list[str]
) -> None:
    """Write one rejected AIS telemetry record to DuckDB with error details."""
    rejected_record = add_validation_errors(record=record, errors=errors)
    clean_record = clean_rejected_consumed_record(rejected_record)

    placeholders = ", ".join(["?"] * len(CONSUMED_REJECTED_FIELDNAMES))
    insert_sql = f"INSERT INTO {REJECTED_TABLE_NAME} VALUES ({placeholders})"  # noqa: S608

    insert_values = [clean_record[field] for field in CONSUMED_REJECTED_FIELDNAMES]

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(insert_sql, insert_values)


def log_storage_summary(db_path: Path) -> None:
    """Log summary statistics from DuckDB tables."""
    sql_valid_count = f"SELECT COUNT(*) FROM {VALID_TABLE_NAME}"  # noqa: S608
    sql_rejected_count = f"SELECT COUNT(*) FROM {REJECTED_TABLE_NAME}"  # noqa: S608
    sql_by_risk = f"""
        SELECT risk_level, COUNT(*) AS count
        FROM {VALID_TABLE_NAME}
        GROUP BY risk_level
        ORDER BY count DESC
        """  # noqa: S608
    sql_by_category = f"""
        SELECT vessel_category, COUNT(*) AS count
        FROM {VALID_TABLE_NAME}
        GROUP BY vessel_category
        ORDER BY count DESC
        """  # noqa: S608

    with duckdb.connect(str(db_path)) as conn:
        valid_result = conn.execute(sql_valid_count).fetchone()
        valid_count = valid_result[0] if valid_result else 0

        rejected_result = conn.execute(sql_rejected_count).fetchone()
        rejected_count = rejected_result[0] if rejected_result else 0

        risk_rows = conn.execute(sql_by_risk).fetchall()
        category_rows = conn.execute(sql_by_category).fetchall()

    LOG.info(f"DuckDB Valid AIS Telemetry Rows: {valid_count}")
    LOG.info(f"DuckDB Rejected Telemetry Rows: {rejected_count}")

    if valid_count > 0:
        LOG.info("DuckDB Count by Risk Level:")
        for risk, count in risk_rows:
            LOG.info(f"  {risk}: {count}")

        LOG.info("DuckDB Count by Vessel Category:")
        for category, count in category_rows:
            LOG.info(f"  {category}: {count}")
