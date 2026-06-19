"""src/streaming/kafka_consumer_critical-section-ag.py.

Kafka consumer for Maritime Domain Awareness pipeline:
- Consumes AIS messages from Kafka topic.
- Validates them against the AIS data contract (Chinese MMSI prefix check).
- Enriches them with threat ranges and risk levels.
- Stores valid and invalid records in DuckDB.
"""

import importlib
import os
from pathlib import Path
from typing import Any, Final

from confluent_kafka.cimpl import OFFSET_BEGINNING, TopicPartition
from datafun_streaming.io.io_utils import append_csv_row
from datafun_streaming.kafka.kafka_admin_utils import (
    create_admin_client,
    get_topic_message_count,
    topic_exists,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_consumer_utils import (
    consume_kafka_message,
    create_consumer,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_streaming.stats.stats_utils import RunningStats
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars

# === DYNAMIC IMPORTS due to hyphens in names ===
_derived = importlib.import_module("streaming.data_engineering.derived_fields-ag")
enrich_message = _derived.enrich_message

_contract = importlib.import_module(
    "streaming.data_validation.data_contract_critical-section-ag"
)
CONSUMED_FIELDNAMES = _contract.CONSUMED_FIELDNAMES
validate_ais_record = _contract.validate_ais_record

_storage = importlib.import_module("streaming.storage.storage_critical-section-ag")
init_db = _storage.init_db
log_storage_summary = _storage.log_storage_summary
write_rejected_record = _storage.write_rejected_record
write_valid_record = _storage.write_valid_record

_viz = importlib.import_module(
    "streaming.visualizations.live_visualizations_critical-section-ag"
)
init_live_chart = _viz.init_live_chart
update_live_chart = _viz.update_live_chart
save_live_chart = _viz.save_live_chart
close_live_chart = _viz.close_live_chart

# === CONFIGURE LOGGER ===
LOG = get_logger("C06-CONSUMER", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===
load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===
TIMEOUT_SECONDS: Final[float] = float(os.getenv("CONSUMER_TIMEOUT_SECONDS", "10.0"))
MAX_MESSAGES: Final[int] = int(os.getenv("CONSUMER_MAX_MESSAGES", "10000"))

# === DECLARE CONSTANT PATHS ===
ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

OUTPUT_CSV: Final[Path] = OUTPUT_DIR / "consumed_ais.csv"
OUTPUT_DB: Final[Path] = OUTPUT_DIR / "ais.duckdb"
OUTPUT_CHART: Final[Path] = OUTPUT_DIR / "ais_chart_case.png"


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "C06-AIS-CONSUMER")
    LOG.info("========================")
    LOG.info("START AIS consumer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "OUTPUT_DB", OUTPUT_DB)


def load_settings() -> KafkaSettings:
    """Load settings from .env and customize the topic for MDA."""
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()

    # Override topic to separate AIS data from sales data
    import dataclasses

    settings = dataclasses.replace(settings, topic="mda-vessels-topic-ag")

    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS  = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC              = {settings.topic}")
    LOG.info(f"KAFKA_GROUP_ID           = {settings.group_id}")
    LOG.info(f"CONSUMER_TIMEOUT_SECONDS = {TIMEOUT_SECONDS}")
    LOG.info(f"CONSUMER_MAX_MESSAGES    = {MAX_MESSAGES}")
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify Kafka is reachable before doing anything else."""
    LOG.info("Verifying Kafka connection...")
    try:
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(str(error))
        raise SystemExit(1) from error


def verify_topic(settings: KafkaSettings) -> None:
    """Verify the topic exists."""
    LOG.info("Verifying Kafka topic...")
    admin = create_admin_client(settings)

    if not topic_exists(admin, settings.topic):
        LOG.error(f"Topic {settings.topic!r} does not exist.")
        LOG.error("Run the producer first to create the topic and send messages.")
        raise SystemExit(1)

    message_count = get_topic_message_count(admin, settings.topic, settings)
    LOG.info(f"Topic {settings.topic!r} exists.")
    LOG.info(f"Found {message_count} message(s) available in topic.")


def get_kafka_consumer(settings: KafkaSettings) -> Any:
    """Create a Kafka consumer subscribed to the topic and reading from beginning."""
    LOG.info("Creating Kafka consumer...")
    consumer = create_consumer(settings)
    consumer.subscribe(
        [settings.topic],
        on_assign=lambda c, partitions: c.assign(
            [
                TopicPartition(
                    partition.topic,
                    partition.partition,
                    OFFSET_BEGINNING,
                )
                for partition in partitions
            ]
        ),
    )
    LOG.info(f"Subscribed to topic: {settings.topic!r} (reading from beginning)")
    return consumer


def initialize_output() -> tuple[Any, Any, list[int], list[float], RunningStats]:
    """Initialize output database, clear CSV files, and prepare stats."""
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()
    LOG.info(f"Output CSV cleared: {OUTPUT_CSV.name}")

    # Initialize DuckDB database
    init_db(OUTPUT_DB)
    LOG.info(f"Database initialized: {OUTPUT_DB.name}")

    # No-op live chart init
    figure, axis, x_values, y_values = init_live_chart()
    LOG.info("Live chart initialized.")

    stats = RunningStats()
    return figure, axis, x_values, y_values, stats


def process_message(
    row: dict[str, Any],
    *,
    stats: RunningStats,
    figure: Any,
    axis: Any,
    x_values: list[int],
    y_values: list[float],
) -> dict[str, Any] | None:
    """Process and enrich one consumed message.

    Steps:
      - Validate required fields and Chinese MMSI prefix
      - Enrich with threat ranges and risk levels
      - Store telemetry in DuckDB
    """
    # 1. Validate
    result = validate_ais_record(record=row)
    if not result.is_valid:
        LOG.warning(f"Validation failed for vessel MMSI {row.get('mmsi', '?')}")
        LOG.warning(f"Errors: {result.errors}")
        write_rejected_record(OUTPUT_DB, row, result.errors)
        return None

    # 2. Enrich
    enriched = enrich_message(row)

    # 3. Update stats and viz
    speed = float(enriched["sog"]) if enriched.get("sog") is not None else 0.0
    stats.update(speed)

    update_live_chart(
        figure=figure,
        axis=axis,
        x_values=x_values,
        y_values=y_values,
        message=enriched,
    )

    return enriched


def consume_messages(
    consumer: Any,
    *,
    stats: RunningStats,
    figure: Any,
    axis: Any,
    x_values: list[int],
    y_values: list[float],
) -> tuple[int, int]:
    """Consume and process messages from the Kafka topic."""
    LOG.info("Consuming messages...")
    LOG.info(f"Waiting for up to {MAX_MESSAGES} message(s).")
    LOG.info("Press CTRL+C to stop early.\n")

    consumed_count = 0
    skipped_count = 0

    while consumed_count + skipped_count < MAX_MESSAGES:
        row = consume_kafka_message(
            consumer=consumer,
            timeout_seconds=TIMEOUT_SECONDS,
        )

        if row is None:
            LOG.info(f"No message received within {TIMEOUT_SECONDS}s timeout.")
            LOG.info("Producer finished or paused. Stopping consumer.")
            break

        enriched = process_message(
            row,
            stats=stats,
            figure=figure,
            axis=axis,
            x_values=x_values,
            y_values=y_values,
        )

        if enriched is None:
            skipped_count += 1
            continue

        # Write valid record to DuckDB
        write_valid_record(OUTPUT_DB, enriched)

        # Write to output CSV
        csv_row = {field: enriched.get(field, "") for field in CONSUMED_FIELDNAMES}
        # Convert list/dict fields to strings for CSV
        for k, v in csv_row.items():
            if v is None:
                csv_row[k] = ""

        append_csv_row(
            path=OUTPUT_CSV,
            row=csv_row,
            fieldnames=CONSUMED_FIELDNAMES,
        )

        consumed_count += 1
        LOG.info(
            f"Forwarded & Logged: MMSI={enriched['mmsi']} "
            f"Name='{enriched['ship_name']}' Risk={enriched['risk_level']} "
            f"SOG={enriched['sog']} knots"
        )

    return consumed_count, skipped_count


def save_artifacts(figure: Any) -> None:
    """Save outputs and log final resource locations."""
    LOG.info("Saving artifacts...")
    save_live_chart(figure=figure, chart_path=OUTPUT_CHART)

    log_path(LOG, "WROTE OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "WROTE OUTPUT_DB", OUTPUT_DB)


def log_summary(
    consumed_count: int,
    skipped_count: int,
    stats: RunningStats,
    settings: KafkaSettings,
) -> None:
    """Log final summary statistics."""
    LOG.info("Summary:")
    LOG.info(f"Consumed {consumed_count} message(s) from topic {settings.topic!r}.")
    LOG.info(f"Skipped  {skipped_count} message(s) due to validation errors.")

    if stats.count > 0:
        LOG.info(f"  Total messages processed: {stats.count}")
        LOG.info(f"  Average operational speed: {stats.mean:.2f} knots")
        LOG.info(f"  Maximum speed tracked:    {stats.maximum:.2f} knots")

    # Log database statistics
    log_storage_summary(OUTPUT_DB)

    LOG.info("========================")
    LOG.info("AIS Consumer executed successfully!")
    LOG.info("========================")


def main() -> None:
    """Main entry point for the Kafka consumer."""
    log_paths()

    settings = load_settings()
    verify_connection(settings)
    verify_topic(settings)
    consumer = get_kafka_consumer(settings)

    figure, axis, x_values, y_values, stats = initialize_output()

    consumed_count = 0
    skipped_count = 0

    try:
        try:
            consumed_count, skipped_count = consume_messages(
                consumer,
                stats=stats,
                figure=figure,
                axis=axis,
                x_values=x_values,
                y_values=y_values,
            )
        finally:
            consumer.close()
            LOG.info("Kafka consumer closed.")

        save_artifacts(figure)

    finally:
        close_live_chart()

    log_summary(consumed_count, skipped_count, stats, settings)


if __name__ == "__main__":
    main()
