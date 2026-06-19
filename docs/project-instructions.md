# Project Instructions

## Topic

Integrated streaming analytics using Kafka, validation, storage, and visualization for
Maritime Domain Awareness (MDA).

This project combines techniques introduced throughout the course into a complete
streaming analytics pipeline for tracking real-time vessel telemetry.

The project:

- connects to a live WebSocket (AISStream) to ingest telemetry data
- produces filtered telemetry messages to a Kafka topic
- consumes messages from Kafka
- validates each message against a stringent data contract
  (e.g., verifying Chinese MMSI prefixes)
- enriches valid messages with derived fields such as threat risk levels
- stores processed records in a resilient DuckDB database
- visualizes fleet locations and risk levels dynamically using a Streamlit dashboard
  with Pydeck mapping

## Architecture Files

Review these core pipeline files:

<!-- markdownlint-disable MD013 -->
| File | Purpose |
| ---- | ------- |
| `src/streaming/kafka_producer_critical-section-ag.py` | Produces filtered live AIS WebSocket messages to Kafka |
| `src/streaming/kafka_consumer_critical-section-ag.py` | Consumes, validates, enriches, and triggers storage of messages |
| `src/streaming/data_validation/data_contract_critical-section-ag.py` | Defines the MDA data contract and validation logic |
| `src/streaming/data_engineering/derived_fields-ag.py` | Computes derived fields such as threat categories and ranges |
| `src/streaming/storage/storage_critical-section-ag.py` | Inserts validated and rejected records into DuckDB with retry logic |
| `src/streaming/visualizations/dashboard-ag.py` | Interactive Streamlit dashboard for real-time fleet visualization |
<!-- markdownlint-enable MD013 -->

## Running the Project

Run commands are found in the `README.md`. Ensure that your `.env` file contains
your `AISSTREAM_API_KEY` before attempting to start the producer.

## Modifying the Project

To customize or extend the MDA pipeline, consider the following technical modifications:

- Change the **KAFKA_TOPIC** name in `.env` to track different datasets.
- Adjust the bounding box in the producer to focus on a different geographical
  region (e.g., the Mediterranean or the Baltic Sea).
- Add new validation rules to track specific vessel types (e.g., only military vessels).
- Add new derived fields (e.g., distance to a specific contested shoal or port).
- Enhance the Streamlit dashboard by adding a new analytical chart or altering
  the Pydeck map layers.

Keep your changes incremental and ensure you restart the producer and consumer
to verify the pipeline remains stable.
