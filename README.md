# AIS MSK Flink Demo — Job 1: Parser & Validator (Bronze Layer)

A production-grade **Apache Flink 1.19** streaming job that ingests raw NMEA/AIS sentences from **AWS MSK (Kafka)**, parses all 27 AIS message types, validates field ranges and enumerations, and sinks clean records to MSK and S3 Parquet (Bronze layer).

> Full design document with all diagrams: [`docs/AIS_Design_Document.pdf`](docs/AIS_Design_Document.pdf)

---

## End-to-End Architecture

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                          DATA SOURCES                                           ║
║   Spire (Terrestrial + Satellite)  ·  Orbcomm (Terrestrial + Satellite)         ║
║   Kpler (Terrestrial)                                                            ║
╚══════════════╤═══════════════════════════════════════════════════════════════════╝
               │  TCP/TLS  (NMEA stream, tag blocks c:/s:)
               ▼
╔══════════════════════════════════════════════════════════════════════════════════╗
║                     Go AIS RECEIVER  (psd-ais-receiver)                         ║
║                                                                                  ║
║  • TLS dial per vendor config        • Tag block parsing  (c:, s:)               ║
║  • Fragment TTL buffer (30 s)        • Source station attribution                ║
║  • Dual-write: S3 raw + Kafka        • Reconnect with backoff                    ║
╚══════════════╤═══════════════════════════════════════════════════════════════════╝
               │  Kafka produce  (acks=1, lz4, linger 10 ms)
               │  Individual NMEA lines, key = null
               ▼
╔══════════════════════════════════════════════════════════════════════════════════╗
║                  AWS MSK  (3 brokers, replication factor 3)                      ║
║                                                                                  ║
║   ais-terrestrial (4 partitions)                                                 ║
║   ais-satellite   (4 partitions)          retention: 7 days                      ║
║   ais-roaming     (4 partitions)                                                 ║
╚══════════════╤═══════════════════════════════════════════════════════════════════╝
               │  KafkaSource (SASL_SSL, IAM, committed offsets)
               │  Tuple2<sourceTopic, rawLine>
               ▼
╔══════════════════════════════════════════════════════════════════════════════════╗
║                   FLINK JOB 1  —  Parser + Validator                             ║
║                   Parallelism: 4  |  Exactly-Once Checkpointing                  ║
║                                                                                  ║
║  1. NMEANormalizer  [MapFunction]                                                ║
║     ├─ Strips Kpler proprietary prefix before \ tag block                        ║
║     └─ Returns null on blank/null → filtered out upstream                        ║
║                         │                                                        ║
║  2. NMEAFragmentAssembler  [KeyedProcessFunction]                                ║
║     ├─ Key: "{sourceTopic}:{channel}:{seqId}"                                    ║
║     ├─ State: ListState<String> fragments + ValueState total + timer             ║
║     ├─ Emits List<String> when all N parts arrive (sorted by fragment#)          ║
║     ├─ Single-part messages emitted immediately (no state)                       ║
║     └─ TTL timer (30 s) → silently drops stale incomplete fragments              ║
║                         │                                                        ║
║  3. NMEAParser  [ProcessFunction]                                                ║
║     ├─ Builds AisPacket from line list via DMA AisLib 2.8.6                      ║
║     ├─ Extracts c: → receivedTimestamp,  s: → sourceStation                     ║
║     ├─ Dispatches to populateByType() for all 27 AIS message types               ║
║     ├─ SUCCESS → AISRecord on main stream                                        ║
║     └─ FAILURE → JSON on DLQ side output (OutputTag "parse-dlq")                 ║
║                         │                                  │                    ║
║  4. ASMDecoder  [MapFunction]                              │ DLQ side output     ║
║     ├─ Detects Type 6/8/12 binary ASM messages            ▼                     ║
║     └─ Decodes DAC/FID/data into extras map   ais.parse.dlq (KafkaSink<String>) ║
║                         │                                                        ║
║  5. Type24Joiner  [KeyedProcessFunction]                                         ║
║     ├─ Key: MMSI (Long)                                                          ║
║     ├─ State: ValueState<AISRecord> partA + partB + timer                        ║
║     ├─ Emits each part immediately (real-time partial data)                      ║
║     ├─ Emits merged record when both parts arrive (type24Joined=true)            ║
║     └─ TTL timer (5 min) → drops unmatched partial silently                      ║
║                         │                                                        ║
║  6. ValidationEngine  [MapFunction]                                              ║
║     ├─ Loads rules.yml at startup (ranges + enums)                               ║
║     ├─ Checks: MMSI range, lat/lon, SOG, COG, heading, draught, IMO             ║
║     ├─ Checks: nav_status, ship_type, maneuver_indicator enums                   ║
║     └─ Appends validationErrors[] — records flow regardless                      ║
╚═════════════════╤══════════════════════════════════════════════════════════════╝
                  │
         ┌────────┴──────────┐
         ▼                   ▼
╔═════════════════╗  ╔══════════════════════════════════════════════╗
║  MSK KafkaSink  ║  ║           S3 / MinIO FileSink                ║
║                 ║  ║                                              ║
║  ais.ingest     ║  ║  Format: Parquet (Avro reflection)           ║
║  JSON per       ║  ║  Partitioned: year=/month=/day=/hour=        ║
║  record         ║  ║  Roll policy: on checkpoint (60 s)           ║
║  acks=all       ║  ║  Path: s3://bucket/ais-bronze-ingest/        ║
║  idempotent     ║  ║  Compaction: per-checkpoint file             ║
║  lz4            ║  ╚══════════════════════════════════════════════╝
╚═════════════════╝
         │
         ▼  (Job 2 — future)
  MSK Router → ais.position / ais.static / ais.voyage / ais.binary
         │
         ▼  (Job 3 — future)
  Quality Filter → Iceberg Gold tables (raw_position, raw_static …)
```

---

## Scalability

| Layer | Mechanism | Details |
|---|---|---|
| **Source parallelism** | Kafka partitions | 4 partitions per topic × 3 topics = 12 source tasks at p=4 |
| **Horizontal scale** | `FLINK_PARALLELISM` env var | Raise parallelism → Flink auto-allocates more task slots; no code change |
| **Fragment key distribution** | `topic:channel:seqId` | Evenly distributes multi-part assembly across all TaskManagers |
| **Type24 key distribution** | MMSI (Long) | Natural hash distribution; no hot-key risk at scale |
| **Kafka sink batching** | `linger_ms=20`, `batch.size=256KB` | Reduces broker round-trips; lz4 cuts network bytes ~60% |
| **Parquet sink** | Columnar + checkpoint-aligned rolling | Downstream query engines (Athena, Spark) scan only needed columns |
| **MSK broker scaling** | 3 brokers, RF=3 | Can add brokers and re-partition without consumer restart |
| **State backend** | Filesystem (RocksDB drop-in for large state) | Swap to RocksDB by changing `state.backend: rocksdb` in Flink config |

---

## Fault Tolerance & Durability

```
Checkpoint cycle (every 60 s):
  Flink snapshots operator state ──► s3://bucket/flink-checkpoints/job1/
                                     (externalized, retained on cancel)

Failure recovery path:
  TaskManager crash  → Flink restarts from last checkpoint automatically
  JobManager crash   → Restart from latest externalized checkpoint
  MSK broker down    → Kafka consumer retries internally; Flink pauses source
  S3 transient error → Flink retries write within checkpoint barrier

Checkpoint config:
  mode:           EXACTLY_ONCE
  interval:       60 s
  min pause:      30 s  (prevents checkpoint storms during GC)
  max concurrent: 1     (no overlapping checkpoints)
  on cancel:      RETAIN_ON_CANCELLATION  (manual recovery possible)

Restart strategy:
  type:     fixed-delay
  attempts: 3  (RESTART_ATTEMPTS)
  delay:    10 s  (RESTART_DELAY_MS)
```

---

## Error Handling & Retry

```
 Input error path:
 ┌──────────────────────────────────────────────────────────┐
 │  Raw NMEA line arrives                                    │
 │       │                                                   │
 │  NMEANormalizer → returns null?  ──► filter() drops      │
 │       │                                                   │
 │  NMEAFragmentAssembler                                    │
 │    fragment arrives incomplete + TTL expires?             │
 │       └──► clearState() silently drops stale fragments   │
 │       │                                                   │
 │  NMEAParser parse attempt                                 │
 │    AisLib throws / msg == null?                           │
 │       └──► emitDlq() → DLQ side output                   │
 │              JSON: {ts, topic, error, lines[]}            │
 │              sinks to: ais.parse.dlq                      │
 │       │                                                   │
 │  Type24Joiner                                             │
 │    only Part A or Part B arrives + TTL (5 min) expires?   │
 │       └──► clearState() drops unmatched partial          │
 │       │                                                   │
 │  ValidationEngine                                         │
 │    field out of range or invalid enum?                    │
 │       └──► appends to validationErrors[], record flows   │
 └──────────────────────────────────────────────────────────┘

 DLQ record format (ais.parse.dlq):
   {
     "ts":    1711276900123,          ← wall-clock ms
     "topic": "ais-terrestrial",      ← source Kafka topic
     "error": "NullPointerException: ...",
     "lines": ["\\c:...,s:...\\!AIVDM,..."]
   }

 Kafka producer retry (MSK / ingest sink):
   acks=all, enable.idempotence=true
   retries=3, retry.backoff.ms=500
   max.in.flight.requests.per.connection=5
```

---

## Pipeline Operator Reference

| # | Operator | Flink Type | Key | State | TTL | Output |
|---|---|---|---|---|---|---|
| 1 | `NMEANormalizer` | `MapFunction` | — | none | — | `Tuple2<topic, line>` |
| 2 | `NMEAFragmentAssembler` | `KeyedProcessFunction` | `topic:channel:seqId` | `ListState`, `ValueState` ×2 | 30 s | `Tuple2<topic, List<line>>` |
| 3 | `NMEAParser` | `ProcessFunction` | — | none | — | `AISRecord` + DLQ side |
| 4 | `ASMDecoder` | `MapFunction` | — | none | — | `AISRecord` |
| 5 | `Type24Joiner` | `KeyedProcessFunction` | MMSI | `ValueState` ×3 | 5 min | `AISRecord` |
| 6 | `ValidationEngine` | `MapFunction` | — | none | — | `AISRecord` |

---

## AIS Message Types Supported

| Types | Description |
|---|---|
| 1, 2, 3 | Position Report Class A (nav status, ROT, SOG, lat/lon, COG, heading) |
| 4 | Base Station Report (position) |
| 5 | Static and Voyage Data (IMO, callsign, name, ship type, dimensions, ETA, draught) |
| 6, 8, 12 | Binary / Application-Specific Messages (DAC, FID, raw data) |
| 9 | SAR Aircraft Position |
| 14 | Safety-Related Text |
| 17 | DGNSS Broadcast (lon/lat in 1/10000 min resolution) |
| 18 | Class B Position Report (SOG, lat/lon, COG, heading, CS flags) |
| 19 | Extended Class B (position + name + ship type) |
| 21 | Aid-to-Navigation Report (name, position, ATon type) |
| 24 | Class B Static (Part A: name / Part B: type, callsign, dimensions) — joined |
| 27 | Long Range AIS Broadcast |

---

## Local Development

### Prerequisites

- Docker + Docker Compose
- Java 11 + Maven 3.8+
- Python 3 + `pip install kafka-python-ng` (flood test only)

### Start the Stack

```bash
docker compose up --build
```

| Service | URL | Credentials |
|---|---|---|
| Flink UI | http://localhost:8081 | — |
| Redpanda Console | http://localhost:8090 | — |
| MinIO UI | http://localhost:9001 | minioadmin / minioadmin |
| Kafka broker (external) | localhost:19092 | — |

### Build and Deploy

```bash
./deploy-local.sh
```

Builds the fat JAR with Maven and submits it to the local Flink cluster via REST API.

### Flood Test

```bash
python3 flood_test.py --count 10000 --bad-pct 5
```

Sends 9,500 valid + 500 malformed NMEA messages. After ~60 seconds:
- Parquet files appear in MinIO → `ais-bronze-local/ais-bronze-ingest/`
- DLQ messages visible in Redpanda Console → `ais.parse.dlq`

Monitor:
```bash
# Check parsed output
docker exec redpanda rpk topic consume ais.ingest --brokers redpanda:9092 -n 5

# Check DLQ
docker exec redpanda rpk topic consume ais.parse.dlq --brokers redpanda:9092 -n 5
```

---

## Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `MSK_BROKERS` | *(see JobConfig)* | Kafka bootstrap servers |
| `SOURCE_TOPICS` | `ais-terrestrial,ais-satellite,ais-roaming` | Comma-separated source topics |
| `CONSUMER_GROUP` | `ais-flink-parser-job1` | Kafka consumer group |
| `INGEST_TOPIC` | `ais.ingest` | Parsed records output topic |
| `DLQ_TOPIC` | `ais.parse.dlq` | Dead letter queue topic |
| `S3_BUCKET` | — | Target S3 bucket |
| `S3_PREFIX` | `ais-bronze-ingest` | S3 key prefix |
| `AWS_REGION` | `us-east-1` | AWS region |
| `KAFKA_AUTH` | *(IAM)* | Set `NONE` for local Redpanda |
| `CHECKPOINT_DIR` | `s3://bucket/flink-checkpoints/job1` | Checkpoint storage path |
| `CHECKPOINT_INTERVAL_MS` | `60000` | Checkpoint interval |
| `MIN_PAUSE_BETWEEN_CHECKPOINTS_MS` | `30000` | Min gap between checkpoints |
| `FLINK_PARALLELISM` | `4` | Job parallelism |
| `FRAGMENT_TTL_SECONDS` | `30` | Multi-part fragment expiry |
| `TYPE24_JOIN_TTL_SECONDS` | `300` | Type 24 A+B join window |
| `RESTART_ATTEMPTS` | `3` | Restart attempts on failure |
| `RESTART_DELAY_MS` | `10000` | Delay between restart attempts |
| `S3_TARGET_FILE_SIZE_MB` | `64` | Target Parquet file size |

---

## Validation Rules (`rules.yml`)

**Range checks** — values outside bounds appended to `validationErrors[]`:

| Field | Min | Max | Reserved |
|---|---|---|---|
| MMSI | 100,000,000 | 999,999,999 | — |
| Latitude | −90.0 | 90.0 | — |
| Longitude | −180.0 | 180.0 | — |
| SOG | 0.0 | 102.2 | 102.3 |
| COG | 0.0 | 359.9 | 360.0 |
| True Heading | 0 | 359 | 511 |
| Draught | 0.0 | 25.5 | — |
| IMO Number | 1,000,000 | 9,999,999 | — |

**Error violations** (hard-flagged in `validationErrors`): `MSG_ID_CONST` · `MMSI_FORMAT` · `MMSI_RANGE` · `LAT_RANGE` · `LON_RANGE`

Records with validation errors still flow to sinks — filtering is Job 3's responsibility.

---

## Project Structure

```
.
├── src/main/java/com/polestar/ais/
│   ├── Job1ParserValidator.java        # Entry point — pipeline wiring
│   ├── config/JobConfig.java           # Env var config
│   ├── model/AISRecord.java            # Output schema (Parquet + JSON)
│   ├── operators/
│   │   ├── NMEANormalizer.java         # Kpler line fix
│   │   ├── NMEAFragmentAssembler.java  # Multi-part NMEA buffer (keyed state)
│   │   ├── NMEAParser.java             # AisLib decode + DLQ side output
│   │   ├── ASMDecoder.java             # Type 6/8 binary decoder
│   │   ├── Type24Joiner.java           # Class B static join (keyed state)
│   │   └── ValidationEngine.java       # rules.yml field checks
│   └── sink/
│       ├── MSKSinkBuilder.java         # Ingest + DLQ Kafka sinks
│       ├── S3ParquetSinkBuilder.java   # Parquet FileSink to S3
│       ├── AISRecordSerializer.java    # JSON serializer for Kafka
│       └── HiveStyleDateBucketAssigner.java  # year=/month=/day=/hour= partitioning
├── src/main/resources/rules.yml        # Validation rules
├── docker/
│   ├── Dockerfile.flink-base           # Flink + S3/Hadoop plugins
│   └── submit-job.sh                   # REST submission script
├── docs/
│   ├── AIS_Design_Document.pdf         # Full design doc (this README + diagrams)
│   ├── architecture.svg                # Pipeline overview diagram
│   └── architecture-considerations.svg # Scalability/fault-tolerance analysis
├── docker-compose.yml                  # Local dev stack
├── docker-compose.msk.yml              # AWS MSK reference
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

---

## Known Gaps / Future Work

| Item | Priority | Notes |
|---|---|---|
| S3 lifecycle rules (expire raw after 90 days) | Medium | Terraform scope |
| `processedTimestamp` field in AISRecord | Medium | Needed for Job 2 lag metrics |
| RocksDB state backend for large-scale deployment | Medium | Switch `state.backend` config only |
| Flink Job 2 — MSK Router | High | Routes by message type to typed topics |
| Flink Job 3 — Quality Filter → Iceberg Gold | High | Filters `validationErrors`, writes Iceberg |
| Checkpoint retention limit | Low | KDA manages automatically in prod |
