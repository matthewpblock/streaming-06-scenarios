"""tests/test_smoke.py - Smoke test for the example.

WHY: Professional Python projects include tests to verify that code runs
     correctly and to catch problems early when changes are made.
     Running tests is part of the standard workflow in every module.

OBS: You do not need to read or modify this file.
     It exists so that `uv run python -m pytest` passes.
"""

import importlib


def test_consumer_module_imports() -> None:
    """Consumer module should import without running Kafka operations."""
    consumer_module = importlib.import_module(
        "streaming.kafka_consumer_critical-section-ag"
    )
    assert consumer_module is not None


def test_producer_module_imports() -> None:
    """Producer module should import without running Kafka operations."""
    producer_module = importlib.import_module(
        "streaming.kafka_producer_critical-section-ag"
    )
    assert producer_module is not None
