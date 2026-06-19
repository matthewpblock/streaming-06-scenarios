# Maritime Domain Awareness (MDA) Pipeline

[![API Reference](https://img.shields.io/badge/API--Utils-datafun--streaming-purple)](https://denisecase.github.io/datafun-streaming/api/)
[![Workflow Guide](https://img.shields.io/badge/Pro--Guide-pro--analytics--02-green)](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
[![Python 3.14](https://img.shields.io/badge/python-3.14%2B-blue?logo=python)](./pyproject.toml)
[![MIT](https://img.shields.io/badge/license-see%20LICENSE-yellow.svg)](./LICENSE)

> Streaming data analytics: complete Maritime Domain Awareness pipeline.

Streaming analytics requires working with data in motion
and distributed, scalable systems. This project refactors the original sales pipeline
into a real-time tracking solution for Chinese maritime forces.

## This Project

This project brings the full streaming analytics workflow together to track vessels using live AIS data.

The project uses Kafka to move AIS messages from a producer to a consumer.

- The **Producer** connects to the live `wss://stream.aisstream.io/v0/stream` WebSocket,
  filters for Chinese vessels, and sends them to a Kafka topic.
- The **Consumer** reads each message, validates MMSI requirements, computes threat ranges
  based on vessel classification, updates a running statistic,
  and stores results in a DuckDB database.
- The **Streamlit Dashboard** visualizes the database contents with an interactive
  Pydeck map and KPI metrics.

## Working Files

You'll work with these areas:

- **data/** - input data, output CSV files, and the DuckDB database
- **docs/** - the project narrative and documentation
- **src/streaming/** - producer, consumer, and supporting code
- **pyproject.toml** - dependencies and environment settings

## Instructions

Follow the instructions below to run the complete MDA pipeline.

### 1. Get an AISStream API Key

To access live maritime data, you need a free API key from AISStream:

1. Go to [aisstream.io](https://aisstream.io/) and create a free account.
2. Once logged in, generate a new API key from your dashboard.
3. In your project directory, copy `.env.example` to `.env` (if you haven't already).
4. Add your key to the `.env` file: `AISSTREAM_API_KEY=your_key_here`

> [!IMPORTANT]
> Make sure your `.env` file contains your `AISSTREAM_API_KEY` before starting the producer.

## Success

Use five named terminals:

1. **kafka** (WSL) - keep the Kafka message broker running
2. **topics** (WSL) - create, list, or reset Kafka topics
3. **producer** (PowerShell) - run the producer to stream WebSocket data into Kafka
4. **consumer** (PowerShell) - run the consumer to enrich and store data in DuckDB
5. **dashboard** (PowerShell) - run the Streamlit dashboard

After the producer and consumer run successfully, processed data will appear in
`data/output/ais.duckdb`, and the dashboard will display live tracking data.

## Command Reference

<details>
<summary>Show command reference</summary>

### In a machine terminal (open in your `Repos` folder)

```bash
git clone https://github.com/matthewpblock/streaming-06-scenarios
cd streaming-06-scenarios
code .
```

### In VS Code Terminal 1: Start Kafka (kafka)

Open a new VS Code terminal. Rename it `kafka`.
If running Windows, specify the terminal type as **wsl** or type `wsl`.
Run the commands one at a time.

Step 1. Verify Java and PATH

```bash
echo "$JAVA_HOME"
"$JAVA_HOME/bin/java" --version
```

Step 2. Rebuild ClusterID (as needed)

```bash
cd ~/kafka
rm -rf /tmp/kraft-combined-logs
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
echo "Cluster ID: $KAFKA_CLUSTER_ID"
bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
```

Step 3. Start kafka server (keep running)

```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

### In VS Code terminal 2: Create Topic (topics)

Open another VS Code terminal. Rename it `topics`.
If running Windows, specify the terminal type as **wsl** or type `wsl`.
Run the commands one at a time.

```bash
cd ~/kafka

bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --topic mda-vessels-topic-ag
```

### In VS Code Terminal 3: Run Producer (producer)

Open another VS Code terminal. Rename it `producer`.
If running Windows, use **PowerShell**.

```shell
uv self update
uv python pin 3.14
uv sync --extra dev --extra docs --upgrade

uvx pre-commit install

# run the custom MDA producer with websockets dependency
clear
uv run --with websockets python -m src.streaming.kafka_producer_critical-section-ag
```

### In VS Code Terminal 4: Run Consumer (consumer)

Open another VS Code terminal. Rename it `consumer`.
If running Windows, use **PowerShell**.

```shell
clear
uv run python -m src.streaming.kafka_consumer_critical-section-ag
```

### In VS Code Terminal 5: Run Dashboard (dashboard)

Open another VS Code terminal. Rename it `dashboard`.
If running Windows, use **PowerShell**.

```shell
clear
uv run --with streamlit --with duckdb python -m streamlit run src/streaming/visualizations/dashboard-ag.py
```

</details>

## Notes

- Use the **UP ARROW** and **DOWN ARROW** in the terminal to scroll through past commands.
- Use `CTRL+c` to gracefully stop a running process.
- All new files follow the `-ag` suffix and `critical-section` naming convention
  to strictly preserve original files.

## Troubleshooting >>> or

If you see something like this in your terminal: `>>>` or `...`
You accidentally started Python interactive mode.
Press `Ctrl+c` (both keys together) or `Ctrl+Z` then `Enter` on Windows.
