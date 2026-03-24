# AIS MSK Flink Demo — Job 1: Parser & Validator (Bronze Layer)

A production-grade **Apache Flink 1.19** streaming job that ingests raw NMEA/AIS sentences from **AWS MSK (Kafka)**, decodes all 27 AIS message types, validates field ranges and enumerations, and sinks clean records to:

- **MSK** → `ais.ingest` topic (JSON per record)
- **S3 / MinIO** → Parquet files partitioned by `year/month/day/hour` (Bronze layer)

Designed to run identically on a local Docker stack (Redpanda + MinIO) or in production on AWS (MSK + S3 + KDA).

---

## Architecture

```
Vendors (Spire / Orbcomm / Kpler)
        │  TCP/TLS
   [Go AIS Receiver]
        │  NMEA lines + tag blocks
        ▼
  MSK Topics: ais-terrestrial / ais-satellite / ais-roaming
        │
  ┌─────────────────────────────────────────────────────┐
  │              Flink Job 1 (this repo)                │
  │                                                     │
  │  NMEANormalizer          (Kpler line fix)           │
  │       │                                             │
  │  NMEAFragmentAssembler   (multi-part buffer)        │
  │       │                                             │
  │  NMEAParser              (AisLib decode)  ──► DLQ   │
  │       │                                             │
  │  ASMDecoder              (Type 6/8 binary)          │
  │       │                                             │
  │  Type24Joiner            (Part A + B join)          │
  │       │                                             │
  │  ValidationEngine        (rules.yml checks)         │
  └─────────────┬───────────────────────────────────────┘
                │
        ┌───────┴────────┐
        ▼                ▼
  MSK ais.ingest    S3 Parquet
  (JSON records)   (Bronze layer)
```

Diagram files: [`docs/architecture.svg`](docs/architecture.svg) · [`docs/architecture-considerations.svg`](docs/architecture-considerations.svg)

---

## Pipeline Operators

| Operator | Type | What it does |
|---|---|---|
| `NMEANormalizer` | `MapFunction` | Strips Kpler proprietary line prefix before the `\` tag block |
| `NMEAFragmentAssembler` | `KeyedProcessFunction` | Buffers multi-part NMEA (e.g. Type 5, 24) keyed by `topic:channel:seqId`; TTL-expires stale fragments |
| `NMEAParser` | `ProcessFunction` | Decodes complete NMEA via DMA AisLib 2.8.6; extracts `c:` timestamp and `s:` source from tag block; emits failures to DLQ side output |
| `ASMDecoder` | `MapFunction` | Decodes Type 6/8 binary Application-Specific Messages (DAC/FID/data) |
| `Type24Joiner` | `KeyedProcessFunction` | Joins Class B Type 24 Part A (name) + Part B (dimensions/callsign) keyed by MMSI; 5-minute TTL |
| `ValidationEngine` | `MapFunction` | Applies `rules.yml` range and enum checks; flags records with `validationErrors` list |

---

## AIS Message Types Supported

Types 1, 2, 3 (Position Class A) · 4 (Base Station) · 5 (Static/Voyage) · 6, 8, 12 (Binary/ASM) · 9 (SAR Aircraft) · 14 (Safety Text) · 17 (DGNSS) · 18 (Position Class B) · 19 (Extended Class B) · 21 (Aid-to-Navigation) · 24 (Class B Static, Part A+B) · 27 (Long Range)

---

## Local Development

### Prerequisites

- Docker + Docker Compose
- Java 11 + Maven 3.8+
- Python 3 + `pip install kafka-python-ng` (for flood test)

### Start the Stack

```bash
docker compose up --build
```

| Service | URL |
|---|---|
| Flink UI | http://localhost:8081 |
| Redpanda Console | http://localhost:8090 |
| MinIO UI | http://localhost:9001 (minioadmin / minioadmin) |
| Kafka broker | localhost:19092 |

### Build and Deploy

```bash
./deploy-local.sh
```

This builds the fat JAR with Maven and submits it to the local Flink cluster.

### Flood Test

Send a mix of valid and malformed NMEA messages to verify end-to-end flow:

```bash
python3 flood_test.py --count 10000 --bad-pct 5
```

After ~60 seconds, Parquet files appear in MinIO under `ais-bronze-local/ais-bronze-ingest/`.

---

## Configuration

All config is driven by environment variables — no code changes needed between local and production.

| Variable | Default | Description |
|---|---|---|
| `MSK_BROKERS` | — | Kafka bootstrap servers |
| `SOURCE_TOPICS` | — | Comma-separated source topics |
| `CONSUMER_GROUP` | — | Kafka consumer group ID |
| `INGEST_TOPIC` | `ais.ingest` | Output topic for parsed records |
| `DLQ_TOPIC` | `ais.parse.dlq` | Dead letter queue topic |
| `S3_BUCKET` | — | Target S3 bucket name |
| `S3_PREFIX` | `ais-bronze-ingest` | S3 key prefix |
| `AWS_REGION` | `us-east-1` | AWS region |
| `KAFKA_AUTH` | — | Set to `NONE` for local; omit for MSK IAM |
| `CHECKPOINT_DIR` | — | S3/HDFS path for Flink checkpoints |
| `FLINK_PARALLELISM` | `1` | Job parallelism |
| `FRAGMENT_TTL_SECONDS` | `30` | Stale multi-part fragment expiry |
| `TYPE24_JOIN_TTL_SECONDS` | `300` | Type 24 Part A/B join window |
| `RESTART_ATTEMPTS` | `3` | Fixed-delay restart attempts |
| `RESTART_DELAY_MS` | `10000` | Delay between restart attempts |
| `MIN_PAUSE_BETWEEN_CHECKPOINTS_MS` | `30000` | Minimum gap between checkpoints |

---

## Production (AWS)

Use `docker-compose.msk.yml` as a reference for MSK endpoint and IAM auth settings. The job uses `AWS_MSK_IAM` SASL/SSL when `KAFKA_AUTH` is not set to `NONE`. Deploy the fat JAR to **Kinesis Data Analytics (KDA)** or a self-managed Flink cluster on EKS/EC2.

---

## Validation Rules

Rules are defined in [`src/main/resources/rules.yml`](src/main/resources/rules.yml):

- **Range checks**: MMSI (9 digits), lat/lon, SOG, COG, heading, draught, IMO number
- **Enum checks**: nav status, ship type, position fix type, maneuver indicator
- **Error violations** (hard fail flags): `MMSI_FORMAT`, `MMSI_RANGE`, `LAT_RANGE`, `LON_RANGE`, `MSG_ID_CONST`

Records with violations are flagged in `validationErrors` and still flow downstream — filtering happens in Job 3.

---

## Dead Letter Queue

Parse failures are emitted as JSON to `ais.parse.dlq`:

```json
{
  "ts": 1711276900123,
  "topic": "ais-terrestrial",
  "error": "SomeException: message detail",
  "lines": ["\\c:1711276900,s:SPIRE-T\\!AIVDM,1,1,,A,TRUNCATED,0*00"]
}
```

Monitor with:
```bash
docker exec redpanda rpk topic consume ais.parse.dlq --brokers redpanda:9092 -n 10
```

---

## Project Structure

```
.
├── src/main/java/com/polestar/ais/
│   ├── Job1ParserValidator.java        # Main entry point / pipeline wiring
│   ├── config/JobConfig.java           # Env var config loader
│   ├── model/AISRecord.java            # Unified output record (Parquet + JSON)
│   ├── operators/
│   │   ├── NMEANormalizer.java
│   │   ├── NMEAFragmentAssembler.java
│   │   ├── NMEAParser.java
│   │   ├── ASMDecoder.java
│   │   ├── Type24Joiner.java
│   │   └── ValidationEngine.java
│   └── sink/
│       ├── MSKSinkBuilder.java
│       ├── S3ParquetSinkBuilder.java
│       ├── AISRecordSerializer.java
│       └── HiveStyleDateBucketAssigner.java
├── src/main/resources/rules.yml        # Validation rules
├── docker/
│   ├── Dockerfile.flink-base           # Flink image with S3/Hadoop plugins
│   └── submit-job.sh                   # Job submission script
├── docs/
│   ├── architecture.svg                # Full 3-job pipeline diagram
│   └── architecture-considerations.svg # Scalability/fault-tolerance analysis
├── docker-compose.yml                  # Local dev stack
├── docker-compose.msk.yml              # AWS MSK reference config
├── Dockerfile                          # Fat JAR build image
├── deploy-local.sh                     # One-command local deploy
├── flood_test.py                       # Load test producer
└── pom.xml
```

---

## Tech Stack

| Component | Version |
|---|---|
| Apache Flink | 1.19.1 |
| Flink Kafka Connector | 3.2.0-1.19 |
| DMA AisLib | 2.8.6-rc1 |
| Apache Parquet / Avro | via Flink FileSystem connector |
| Jackson | 2.15.2 |
| AWS MSK IAM Auth | 2.x |
| Java | 11 |
| Redpanda (local Kafka) | v24.1.1 |
| MinIO (local S3) | 2024-03-30 |
