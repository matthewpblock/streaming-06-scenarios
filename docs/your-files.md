---
type: reference
description: Details on directly editing project files
---

# Your Files

For this custom Maritime Domain Awareness (MDA) project, we have departed from the original course
structure of keeping `_case` and personal alias copies side-by-side.

## Direct Editing

We now edit the pipeline files directly to build a professional, unified application.
All core logic is contained within the `src/streaming/` directory, and our specialized modules
utilize the `-ag` suffix (e.g., `kafka_consumer_critical-section-ag.py`) to denote our custom
integrations and differentiate them from the original boilerplate.

## 1. Python Files

The core files you will interact with are:

```text
src/streaming/kafka_producer_critical-section-ag.py
src/streaming/kafka_consumer_critical-section-ag.py
src/streaming/visualizations/dashboard-ag.py
```

## 2. Python File Execution Command

Run the pipeline using the commands outlined in the main `README.md`. For example:

<!-- markdownlint-disable MD013 -->
```shell
uv run --with websockets python -m src.streaming.kafka_producer_critical-section-ag
uv run python -m src.streaming.kafka_consumer_critical-section-ag
uv run --with streamlit --with duckdb python -m streamlit run src/streaming/visualizations/dashboard-ag.py
```
<!-- markdownlint-enable MD013 -->

## 3. Data Files

The `data/output` directory is dynamically populated by the consumer with a DuckDB database
(`ais.duckdb`) and an ongoing CSV log (`consumed_ais.csv`). You do not need to manually manage
these files, as they are generated and updated automatically by the pipeline.
