---
type: overview
description: Project documentation homepage
---
# Maritime Domain Awareness Pipeline

This site provides documentation for the real-time Maritime Domain Awareness (MDA)
streaming data project. Use the navigation to explore module-specific materials.

## How-To Guide

To get the pipeline running, refer to the instructions in the main project `README.md`.

## Custom Project

### Dataset

Describe the dataset used by your Kafka producer.

- **Name of the dataset file:** Live AIS Telemetry Data stream
  (`wss://stream.aisstream.io/v0/stream`)
- **What kind of records it contains:** Live Automatic Identification System (AIS) telemetry from
  ships operating in the designated bounding box (equator up to Japan). The stream emits various
  messages such as `PositionReport`, `StandardClassBPositionReport`, and `ShipStaticData`.
- **Which fields are included in each record:** Raw WebSocket JSON including metadata
  (`MMSI`, `ShipName`, `latitude`, `longitude`, `time_utc`) and payload-specific fields like Speed
  Over Ground (`Sog`), Course Over Ground (`Cog`), `TrueHeading`, `CallSign`, and `Destination`.
- **Whether you used the original sales dataset or modified it:** We completely replaced the
  static sales dataset with a live streaming WebSocket API to track naval/maritime activity
  in real-time.

### Kafka Messages

Describe the messages sent through Kafka.

- **What your producer sends:** The producer flattens the nested WebSocket JSON and isolates the
  core telemetry fields needed for tracking. It explicitly filters for Chinese maritime forces
  using MMSI prefixes (412, 413, 414) before pushing messages to Kafka.
- **Which Kafka topic you used:** `mda-vessels-topic-ag`
- **What message key you used, if any:** The vessel's `MMSI` (Maritime Mobile Service Identity)
  string is used as the Kafka message key to ensure all telemetry for a given ship goes to the
  same partition.
- **Whether you changed the message fields:** Yes. The producer standardizes fields into a flat
  dictionary, mapping payload specifics like `Cog` to `cog` and extracting metadata fields uniformly.

### Consumer Processing

Describe what your consumer receives and does with each message.

- **What your consumer receives from Kafka:** Flattened JSON strings of Chinese vessel
  AIS telemetry.
- **How many messages it consumes:** It consumes up to a predefined `MAX_MESSAGES` limit
  (e.g., 10,000) or until stopped manually.
- **What it logs or prints:** Logs details about validation errors (e.g., out-of-bounds
  coordinates or missing fields). For valid vessels, it logs the MMSI, Name, assigned Risk Level,
  and Speed. It also prints out summary statistics (average operational speed) at the end.
- **If it writes records to a CSV file:** Yes, it writes flattened, enriched records to
  `data/output/consumed_ais.csv` for backup and quick inspection.
- **If it processes or filters selected fields (be specific):**
  - **Validates:** Checks required fields (`mmsi`, `ship_name`, coordinates, timestamp)
    and enforces MMSI prefixes (412, 413, 414).
  - **Enriches:** Derives `threat_range_km`, `vessel_category`, `risk_level` (LOW, MEDIUM,
    HIGH, CRITICAL), and `threat_description` by inspecting the `ship_type` code.
  - **Stores:** Connects to an embedded DuckDB instance with robust retry logic and inserts
    validated records into `consumed_valid_ais` and rejected records into `consumed_rejected_ais`.

### Experiments

Describe the small technical changes you made.

- **Phase 4 change:** Implemented a new, dedicated Kafka topic (`mda-vessels-topic-ag`)
  specifically for AIS telemetry, effectively isolating this project from the original sales data.
  We also implemented a retry loop in the `storage_critical-section-ag.py` DuckDB context manager
  to handle Windows file-lock contention seamlessly.
- **Phase 5 application:** Completely transformed the workflow from static batch analysis to an
  open-source intelligence (OSINT) real-time streaming pipeline. We added the `websockets`
  dependency for the producer to ingest live data and built an interactive map overlay in the
  Streamlit dashboard using `pydeck` with CartoDB basemaps instead of generic matplotlib charts.

### Results

Describe what happened when you ran the producer and consumer.

The producer successfully opened a persistent WebSocket connection, subscribed to a large East Asia
bounding box, filtered for Chinese vessels, and published messages to Kafka. The consumer
instantly picked up these messages, successfully appended threat assessments based on vessel
classification, circumvented database locks via our robust connection pooler, and stored them in
DuckDB. Concurrently, the Streamlit dashboard queried DuckDB read-only and visualized the live
tactical picture without any crashes or concurrency issues.

### Interpretation

Explain what the Kafka streaming workflow showed you.

- **What changed from the original example:** We moved from a static, pre-collected dataset to a
  dynamic, live-streaming data feed, demonstrating Kafka's ability to act as a buffer and
  distributor for unpredictable real-time data rates.
- **What you learned from watching messages move through Kafka:** Kafka decouples data ingestion
  from heavy processing and storage. If the consumer goes down, Kafka retains the vessel tracking
  data, and the consumer can resume exactly where it left off, ensuring no telemetry is lost.
- **What the stream could tell a business or organization:** For security, defense, or open-source
  intelligence (OSINT) organizations, this stream provides a "Tactical Radar" overlay of fleet
  movements in a contested region in real-time.
- **What business intelligence was gained from the consumed messages:** We were able to categorize
  vessels instantly into Risk Levels (CRITICAL, HIGH, MEDIUM, LOW) based on their `ship_type`
  metadata. This allows stakeholders to focus entirely on high-threat assets (like military or
  law-enforcement vessels) while filtering out benign traffic (like fishing or passenger ships).
