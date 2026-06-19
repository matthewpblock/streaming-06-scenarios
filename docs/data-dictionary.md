---
type: dictionary
description: Canonical schema, business logic, and derived metrics for the MDA pipeline
---

# Data Dictionary

This document serves as the canonical reference for the Maritime Domain Awareness (MDA) pipeline's
data assets. It provides the necessary context for both human analysts and AI agents querying the
Kafka stream or the DuckDB database.

## 1. Core Data Assets

### Kafka Stream

- **Topic:** `mda-vessels-topic-ag`
- **Payload:** Flattened JSON strings of live Automatic Identification System (AIS) telemetry.
- **Key:** The vessel's MMSI (Maritime Mobile Service Identity) string.

### DuckDB Database

- **Database File:** `data/output/ais.duckdb`
- **Primary Table:** `consumed_valid_ais` (Contains clean, enriched telemetry)
- **Dead-Letter Table:** `consumed_rejected_ais` (Contains telemetry that failed validation)

---

## 2. Business Rules & Ingestion Logic

The pipeline does not ingest all global maritime traffic. It applies strict filtering at the edge
(Producer) and during validation (Consumer) to track specific fleets of interest.

### Bounding Box

Telemetry is only ingested if the vessel is broadcasting from within the primary East Asia
bounding box (Approx. 0° to 45° N Latitude, 100° to 140° E Longitude).

### MMSI Filtering

To focus the intelligence gathering, the pipeline strictly tracks vessels registered to China.
The consumer enforces this by ensuring the `mmsi` field begins with one of the following
Maritime Identification Digits (MID):

- **412**
- **413**
- **414**

---

## 3. Schema & Fields

### Raw Ingested Fields

<!-- markdownlint-disable MD013 -->
| Field Name | Type | Description |
| :--- | :--- | :--- |
| `mmsi` | VARCHAR | **Primary Key**. 9-digit unique maritime identification number. |
| `ship_name` | VARCHAR | The registered name of the vessel. |
| `latitude` | DOUBLE | Current Y coordinate (WGS84 / EPSG:4326). |
| `longitude` | DOUBLE | Current X coordinate (WGS84 / EPSG:4326). |
| `timestamp` | VARCHAR | UTC timestamp of the telemetry report. |
| `message_type` | VARCHAR | The AIS message class (e.g., `PositionReport`, `ShipStaticData`). |
| `ship_type` | INTEGER | Standardized AIS vessel classification code. |
| `sog` | DOUBLE | Speed Over Ground (in knots). |
| `cog` | DOUBLE | Course Over Ground (in degrees). |
| `heading` | DOUBLE | True heading of the vessel (in degrees). |
<!-- markdownlint-enable MD013 -->

### Derived Metrics (Enrichment)

During the Consumer processing phase, the raw `ship_type` integer is used to append several
derived intelligence fields to the payload before it is written to the database:

<!-- markdownlint-disable MD013 -->
| Field Name | Type | Description / Logic |
| :--- | :--- | :--- |
| `vessel_category` | VARCHAR | Human-readable translation of the `ship_type` code. |
| `risk_level` | VARCHAR | Categorical threat assessment assigned via the mapping rules below. |
| `threat_description` | VARCHAR | Contextual text explaining the rationale behind the risk level. |
| `threat_range_km` | DOUBLE | An estimated operational or radar range based on the vessel's size. |
<!-- markdownlint-enable MD013 -->

#### Risk Level Mapping

- **CRITICAL:** Military, Law Enforcement, or Search & Rescue vessels.
- **HIGH:** Unclassified vessels or ships with suspicious/missing classification data operating
  in contested zones.
- **MEDIUM:** Large commercial assets like Cargo ships and Tankers.
- **LOW:** Passenger ferries, pleasure craft, and standard fishing vessels.

---

## 4. Validation & Rejection Criteria

Before any record is written to `consumed_valid_ais`, it must pass the data contract validation.
Records are routed to `consumed_rejected_ais` (with a `validation_errors` field explaining the
failure) if they violate any of the following rules:

1. **Missing Required Fields:** `mmsi`, `ship_name`, `latitude`, `longitude`, and `timestamp`
   cannot be null or empty.
2. **Invalid Coordinates:** `latitude` must be between -90 and 90. `longitude` must be between
   -180 and 180.
3. **Invalid Origin:** The `mmsi` does not start with an approved tracking prefix (412, 413, 414).
