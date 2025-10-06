# data-streaming-flink

# Flink IoT Data Pipeline

This project implements a real-time data pipeline for IoT device and sensor data using Apache Flink, Kafka, Delta Lake, and Apache Iceberg (on MinIO). It is designed to ingest change data capture (CDC) streams from MongoDB (via Kafka), process and join device and sensor readings, and store both raw and aggregated data in Delta Lake or Iceberg tables for analytics and downstream consumption.

## Features
- **Kafka CDC Sources**: Consumes CDC streams for devices and sensor readings from Kafka topics.
- **Delta Lake Sinks**: Writes raw and processed data to Delta Lake tables stored in MinIO (S3-compatible storage).
- **Iceberg Sinks**: Writes data to Apache Iceberg tables with upsert support for CDC-aware storage.
- **Streaming Joins & Aggregations**: Joins device and sensor data, computes severity, and aggregates temperature readings per minute.
- **Fault Tolerance**: Uses Flink checkpointing with externalized checkpoints to MinIO.

## Project Structure
```
├── docker-compose.yml         # Docker Compose (Flink, Kafka, MongoDB, MinIO, etc.)
├── Makefile                   # Common commands for build/run
├── requirements.txt           # Python dependencies
├── flink-conf/                # Flink and Hadoop configuration files
│   ├── core-site.xml
│   ├── flink-config.yml
│   └── flink.Dockerfile
├── mongo-conf/                # MongoDB initialization scripts
│   └── mongo-init.sh
├── src/
│   ├── cdc_to_kafka.py        # MongoDB CDC to Kafka 
│   ├── kafka_to_delta.py      # Kafka CDC to Delta Lake
│   ├── kafka_to_iceberg.py    # Kafka CDC to Apache Iceberg
│   └── kafka_to_es.py         # Kafka CDC to Elasticsearch
└── README.md                  
```




## Main Pipelines

This project provides three main Flink streaming pipelines, each serving a core role in the IoT data platform:

<img width="1055" height="454" alt="Screenshot 2025-10-01 114934" src="https://github.com/user-attachments/assets/ff03635d-2cc0-4004-8d3c-3ab657b59ac7" />

### 1. `cdc_to_kafka.py`
Ingests change data capture (CDC) streams directly from MongoDB collections and writes them to Kafka topics in Debezium JSON format. This is typically the first stage, making MongoDB changes available in Kafka for downstream processing.
- **MongoDB CDC Sources**: Reads from MongoDB collections (`devices`, `sensor_readings`) using the Flink MongoDB CDC connector.
- **Kafka Sinks**: Writes CDC events to Kafka topics (`devices-cdc`, `sensor-readings-cdc`).
- **Streaming**: Designed for continuous, low-latency CDC ingestion.

### 2. `kafka_to_delta.py`
Consumes CDC data from Kafka topics, processes and joins device and sensor readings, computes severity, aggregates temperature readings, and writes both raw and processed data to Delta Lake tables in MinIO for analytics and historical storage.
- **Kafka Sources**: Reads CDC data from Kafka topics (`devices-cdc`, `sensor-readings-cdc`).
- **Views & Aggregations**: Cleans, joins, and aggregates IoT data.
- **Delta Sinks**: Writes to partitioned Delta tables in MinIO:
   - `devices_raw`
   - `sensor_readings_raw`
   - `devices_readings` (joined)
   - `temperature_readings_minute` (aggregated)

### 3. `kafka_to_es.py`
Reads CDC data from Kafka topics and writes both raw and joined/processed IoT data to Elasticsearch for search, dashboarding, and alerting use cases.
- **Kafka Sources**: Reads CDC data from Kafka topics (`devices-cdc`, `sensor-readings-cdc`) in Debezium JSON format.
- **Views**: Joins device and sensor readings, computes severity, and prepares data for Elasticsearch.
- **Elasticsearch Sinks**: Writes to Elasticsearch indices:
   - `iot_devices`
   - `iot_sensor_readings`
   - `iot_devices_readings` (joined)




## Additional Pipeline: `kafka_to_iceberg.py`

If you want to store IoT data in Apache Iceberg tables with **upsert support** (instead of append-only Delta Lake), you can use the `kafka_to_iceberg.py` pipeline.

- **Kafka Sources**: Reads CDC data from Kafka topics (`devices-cdc`, `sensor-readings-cdc`) in Debezium JSON format.
- **Iceberg Catalog**: Uses Hive Metastore as the catalog with MinIO (S3) as the warehouse.
- **Upsert Support**: Unlike Delta Lake, Iceberg tables support automatic CREATE/UPDATE/DELETE operations based on CDC events.
- **Iceberg Sinks**: Writes to partitioned Iceberg tables:
   - `iceberg_devices` (partitioned by date, primary key: device_id)
   - `iceberg_sensor_readings` (partitioned by date, primary key: reading_id)
   - `iceberg_devices_readings_enriched` (joined and enriched data)
- **Use Case**: Provides CDC-aware storage with upsert capabilities, ideal for maintaining current state while keeping historical versions.

To run this pipeline:
```bash
make iceberg
```

**Key Difference**: While `kafka_to_delta.py` uses append-only writes, `kafka_to_iceberg.py` automatically handles upserts, updates, and deletes from CDC streams, making it suitable for maintaining current-state views.


## Additional Pipeline: `cdc_to_es.py`

If you want to run a Flink pipeline that ingests change data capture (CDC) streams directly from MongoDB and writes them to Elasticsearch (without using Kafka or Delta Lake), you can use the optional `cdc_to_es.py` script.

- **MongoDB CDC Sources**: Reads from MongoDB collections (`devices`, `sensor_readings`) using the Flink MongoDB CDC connector.
- **Elasticsearch Sinks**: Writes both raw and joined/processed IoT data to Elasticsearch indices (`devices`, `sensor_readings`, `devices_readings`).
- **Use Case**: Enables direct CDC from MongoDB to Elasticsearch for search and analytics, bypassing Kafka and Delta Lake.

To run this pipeline:
```bash
make cdc-es
```

## Getting Started
1. **Clone the repository**
2. **Start the stack**:
   ```bash
   make up
   ```
3. **Install Python dependencies** (for local development):
   ```bash
   pip install -r requirements.txt
   ```
4. **Run a pipeline using Makefile targets**:
   - To ingest MongoDB CDC to Kafka:
      ```bash
      make cdc-kafka
      ```
   - To process Kafka CDC to Delta Lake:
      ```bash
      make delta
      ```
   - To process Kafka CDC to Apache Iceberg:
      ```bash
      make iceberg
      ```
   - To process Kafka CDC to Elasticsearch:
      ```bash
      make es
      ```

## Configuration
- **Flink & Hadoop**: See `flink-conf/` for custom configuration (e.g., S3/MinIO access).
- **Kafka**: Topics and brokers are configured in the pipeline scripts.
- **Delta Lake**: Tables are written to MinIO buckets (see `table-path` in `kafka_to_delta.py`).
- **Iceberg**: Uses Hive Metastore catalog with MinIO warehouse at `s3a://iceberg-warehouse/`.

## Notes
- **Flink writes to Delta Lake in append-only mode:** All Delta Lake tables in this project are append-only; Flink does not support upserts or deletes to Delta tables here. Data is only added, not updated or removed.
- **Iceberg supports full CDC operations:** The Iceberg pipeline (`kafka_to_iceberg.py`) uses format version 2 with upsert-enabled tables, automatically handling CREATE, UPDATE, and DELETE operations from CDC streams.


