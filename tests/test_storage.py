"""Tests for streaming.storage.storage_critical-section-ag."""

import importlib
from pathlib import Path

import duckdb

_storage = importlib.import_module("streaming.storage.storage_critical-section-ag")
CONSUMED_REJECTED_FIELDNAMES = _storage.CONSUMED_REJECTED_FIELDNAMES
CONSUMED_VALID_FIELDNAMES = _storage.CONSUMED_VALID_FIELDNAMES
REJECTED_TABLE_NAME = _storage.REJECTED_TABLE_NAME
VALID_TABLE_NAME = _storage.VALID_TABLE_NAME
clean_rejected_consumed_record = _storage.clean_rejected_consumed_record
clean_valid_consumed_record = _storage.clean_valid_consumed_record
clear_storage_tables = _storage.clear_storage_tables
create_storage_tables = _storage.create_storage_tables
write_rejected_record = _storage.write_rejected_record
write_valid_record = _storage.write_valid_record


# === FIXTURES ===

SAMPLE_VALID_RECORD = {
    "mmsi": "412123456",
    "ship_name": "TEST SHIP",
    "latitude": "30.0",
    "longitude": "120.0",
    "timestamp": "2026-05-08T10:00:00",
    "message_type": "PositionReport",
    "ship_type": "30",
    "cog": "90.0",
    "sog": "15.0",
    "heading": "90.0",
    "destination": "SHANGHAI",
    "call_sign": "TEST",
    "imo_number": "1234567",
    "vessel_category": "Test Category",
    "threat_range_km": "15.0",
    "risk_level": "LOW",
    "threat_description": "Test Description",
    "_kafka_key": "412123456",
    "_kafka_partition": "0",
    "_kafka_offset": "42",
}


# === clean_valid_consumed_record ===


def test_clean_valid_consumed_record_keeps_expected_fields() -> None:
    result = clean_valid_consumed_record(SAMPLE_VALID_RECORD)
    assert set(result.keys()) == set(CONSUMED_VALID_FIELDNAMES)


def test_clean_valid_consumed_record_fills_missing_with_empty() -> None:
    result = clean_valid_consumed_record({"mmsi": "412123456"})
    assert result["ship_name"] is None


def test_clean_rejected_consumed_record_keeps_expected_fields() -> None:
    record = {**SAMPLE_VALID_RECORD, "validation_errors": "Missing field"}
    result = clean_rejected_consumed_record(record)
    assert set(result.keys()) == set(CONSUMED_REJECTED_FIELDNAMES)


# === create_storage_tables / clear_storage_tables ===


def test_create_storage_tables_creates_both_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    with duckdb.connect(str(db_path)) as conn:
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
    assert VALID_TABLE_NAME in tables
    assert REJECTED_TABLE_NAME in tables


def test_clear_storage_tables_removes_all_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    write_valid_record(db_path, SAMPLE_VALID_RECORD)
    clear_storage_tables(db_path)
    with duckdb.connect(str(db_path)) as conn:
        sql = f"SELECT COUNT(*) FROM {VALID_TABLE_NAME}"  # noqa: S608
        row = conn.execute(sql).fetchone()
        count = row[0] if row is not None else 0
    assert count == 0


# === write_valid_record / write_rejected_record ===


def test_write_valid_record_inserts_row(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    write_valid_record(db_path, SAMPLE_VALID_RECORD)
    with duckdb.connect(str(db_path)) as conn:
        sql = f"SELECT COUNT(*) FROM {VALID_TABLE_NAME}"  # noqa: S608
        row = conn.execute(sql).fetchone()
        count = row[0] if row is not None else 0
    assert count == 1


def test_write_valid_record_stores_correct_values(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    write_valid_record(db_path, SAMPLE_VALID_RECORD)
    with duckdb.connect(str(db_path)) as conn:
        sql = f"SELECT mmsi, ship_name FROM {VALID_TABLE_NAME}"  # noqa: S608
        row = conn.execute(sql).fetchone()
    assert row is not None
    assert row[0] == "412123456"
    assert row[1] == "TEST SHIP"


def test_write_rejected_record_inserts_with_errors(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    write_rejected_record(db_path, SAMPLE_VALID_RECORD, ["Missing field: mmsi"])
    with duckdb.connect(str(db_path)) as conn:
        sql = f"SELECT COUNT(*) FROM {REJECTED_TABLE_NAME}"  # noqa: S608
        row = conn.execute(sql).fetchone()
        count = row[0] if row is not None else 0
    assert count == 1


def test_write_multiple_records(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    record2 = {**SAMPLE_VALID_RECORD, "mmsi": "412123457"}
    write_valid_record(db_path, SAMPLE_VALID_RECORD)
    write_valid_record(db_path, record2)
    with duckdb.connect(str(db_path)) as conn:
        sql = f"SELECT COUNT(*) FROM {VALID_TABLE_NAME}"  # noqa: S608
        row = conn.execute(sql).fetchone()
        count = row[0] if row is not None else 0
    assert count == 2
