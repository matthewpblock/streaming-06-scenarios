"""src/streaming/kafka_producer_critical-section-ag.py - Kafka Producer for AISStream.

Connects directly to the live AISStream WebSocket, filters for Chinese vessels
(MMSI starts with 412, 413, or 414), flattens the telemetry data, and publishes
vessel information to a Kafka topic.
"""

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Final

from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_producer_utils import (
    create_producer,
    prepare_producer_topic,
    produce_kafka_message,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv
import websockets

from streaming.core.utils import log_env_vars

# === CONFIGURE LOGGER ===
LOG = get_logger("P06-PRODUCER", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===
load_dotenv(override=True)
log_env_vars(LOG)

# === CONSTANT PATHS ===
ROOT_DIR: Final[Path] = Path.cwd()

# === AISSTREAM CONFIGURATION ===
AISSTREAM_URL: Final[str] = "wss://stream.aisstream.io/v0/stream"
AISSTREAM_API_KEY: Final[str] = os.getenv("AISSTREAM_API_KEY", "")

# Default bounding box covering China Seas (East China Sea, South China Sea, Yellow Sea, Taiwan Strait)
# Coordinates: [[[lat_min, lon_min], [lat_max, lon_max]]]
DEFAULT_BOUNDING_BOX: Final[list[list[list[float]]]] = [[[0.0, 100.0], [45.0, 140.0]]]


def log_paths() -> None:
    """Log run header and directory information."""
    log_header(LOG, "P06-AIS-PRODUCER")
    LOG.info("=================================")
    LOG.info("START AISStream Kafka Producer")
    LOG.info("=================================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)


def load_settings() -> KafkaSettings:
    """Load settings from .env and customize the topic for MDA."""
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()

    # Override topic to separate AIS data from sales data
    import dataclasses

    settings = dataclasses.replace(settings, topic="mda-vessels-topic-ag")

    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS    = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC                = {settings.topic}")
    LOG.info(
        f"AISSTREAM_API_KEY Presence = {'Present (len=' + str(len(AISSTREAM_API_KEY)) + ')' if AISSTREAM_API_KEY else 'MISSING'}"
    )
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify Kafka is reachable before starting."""
    LOG.info("Verifying Kafka connection...")
    try:
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(f"Kafka connection failed: {error}")
        raise SystemExit(1) from error


async def stream_ais_to_kafka(producer: Any, settings: KafkaSettings) -> None:
    """Connect to AISStream WebSocket and stream filtered messages to Kafka.

    Handles WebSocket exceptions and connection issues gracefully with detailed logging.
    """
    if not AISSTREAM_API_KEY:
        LOG.critical("AISSTREAM_API_KEY is not set in the environment or is empty.")
        LOG.critical("Please add 'AISSTREAM_API_KEY=your_key' to your .env file.")
        raise SystemExit(1)

    # Subscription payload
    subscription_payload = {
        "APIKey": AISSTREAM_API_KEY,
        "BoundingBoxes": DEFAULT_BOUNDING_BOX,
    }

    LOG.info(f"Connecting to AISStream WebSocket at {AISSTREAM_URL}...")
    LOG.info(f"Monitoring Bounding Box: {DEFAULT_BOUNDING_BOX}")

    try:
        async with websockets.connect(AISSTREAM_URL) as websocket:
            LOG.info(
                "WebSocket connection established. Sending subscription payload..."
            )
            await websocket.send(json.dumps(subscription_payload))
            LOG.info(
                "Subscription sent successfully. Listening for live AIS messages..."
            )

            message_count = 0
            async for raw_message in websocket:
                try:
                    data = json.loads(raw_message)
                except json.JSONDecodeError as err:
                    LOG.warning(f"Failed to decode WebSocket JSON message: {err}")
                    continue

                message_type = data.get("MessageType")
                metadata = data.get("MetaData", {})
                message_payload = data.get("Message", {})

                # Extract MMSI for filtering
                mmsi_val = metadata.get("MMSI")
                if not mmsi_val:
                    continue

                mmsi_str = str(mmsi_val).strip()

                # Filter for Chinese vessels (Maritime Identification Digits: 412, 413, 414)
                if not (
                    mmsi_str.startswith("412")
                    or mmsi_str.startswith("413")
                    or mmsi_str.startswith("414")
                ):
                    continue

                # Build flat record
                flat_record = {
                    "mmsi": mmsi_str,
                    "ship_name": metadata.get("ShipName", "").strip(),
                    "latitude": metadata.get("latitude"),
                    "longitude": metadata.get("longitude"),
                    "timestamp": metadata.get("time_utc", ""),
                    "message_type": message_type,
                    "ship_type": None,
                    "cog": None,
                    "sog": None,
                    "heading": None,
                    "destination": None,
                    "call_sign": None,
                    "imo_number": None,
                }

                # Extract message-specific telemetry or details
                if message_type in ("PositionReport", "StandardClassBPositionReport"):
                    pos_report = message_payload.get(message_type, {})
                    flat_record["cog"] = pos_report.get("Cog")
                    flat_record["sog"] = pos_report.get("Sog")
                    flat_record["heading"] = pos_report.get("TrueHeading")

                elif message_type == "ShipStaticData":
                    static_data = message_payload.get("ShipStaticData", {})
                    flat_record["ship_type"] = static_data.get("Type")
                    flat_record["call_sign"] = static_data.get("CallSign", "").strip()
                    flat_record["imo_number"] = static_data.get("ImoNumber")
                    flat_record["destination"] = static_data.get(
                        "Destination", ""
                    ).strip()

                # Validate coordinates
                if flat_record["latitude"] is None or flat_record["longitude"] is None:
                    continue

                # Produce message to Kafka
                key = mmsi_str
                produce_kafka_message(
                    producer=producer,
                    topic=settings.topic,
                    key=key,
                    message=flat_record,
                )

                # Poll to handle callbacks
                producer.poll(0)

                message_count += 1
                if message_count % 10 == 0 or message_count == 1:
                    LOG.info(
                        f"Forwarded {message_count} Chinese vessel telemetry messages. "
                        f"Latest: MMSI={mmsi_str} Name='{flat_record['ship_name']}' type={message_type}"
                    )

    except websockets.exceptions.InvalidStatusCode as err:
        LOG.error("=== WEBSOCKET AUTHENTICATION / REQUEST ERROR ===")
        LOG.error(
            f"AISStream WebSocket rejected connection with HTTP status: {err.status_code}."
        )
        if err.status_code == 403 or err.status_code == 401:
            LOG.error(
                "This indicates your AISSTREAM_API_KEY is invalid, expired, or unauthorized."
            )
        LOG.error("Check your credentials in the .env file and try again.")
        raise SystemExit(1) from err

    except websockets.exceptions.ConnectionClosed as err:
        LOG.error("=== WEBSOCKET CONNECTION CLOSED ===")
        LOG.error(
            f"WebSocket closed unexpectedly. Code: {err.code}, Reason: {err.reason}"
        )
        LOG.error(
            "Please verify network connection and check if AISStream services are online."
        )
        raise SystemExit(1) from err

    except Exception as err:
        LOG.error("=== UNEXPECTED WEBSOCKET ERROR ===")
        LOG.error(
            f"An unexpected error occurred during WebSocket communication: {err}",
            exc_info=True,
        )
        raise SystemExit(1) from err


def main() -> None:
    """Main entry point for the AISStream Kafka Producer."""
    log_paths()

    settings = load_settings()
    verify_connection(settings)

    # Ensure topic exists
    prepare_producer_topic(settings)

    # Create the synchronous confluent-kafka producer
    producer = create_producer(settings)

    try:
        # Run the asynchronous WebSocket loop
        asyncio.run(stream_ais_to_kafka(producer, settings))
    except KeyboardInterrupt:
        LOG.info("Producer stopped by user (CTRL+C).")
    finally:
        LOG.info("Flushing Kafka producer messages...")
        producer.flush()
        LOG.info("Producer shutdown complete.")


if __name__ == "__main__":
    main()
