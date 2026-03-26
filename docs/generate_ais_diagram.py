#!/usr/bin/env python3
"""
Generate draw.io XML for AIS Flink Architecture Diagram.

Import into Lucidchart:  File → Import → Lucidchart / draw.io XML
Import into draw.io:     File → Import from → XML
"""

cells = []
_cid = [10]

def _nid():
    _cid[0] += 1
    return _cid[0]

def _esc(s):
    return (str(s)
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;'))

def shape(label, x, y, w, h, style):
    i = _nid()
    cells.append(
        f'<mxCell id="{i}" value="{_esc(label)}" style="{style}" '
        f'vertex="1" parent="1">'
        f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/>'
        f'</mxCell>'
    )
    return i

def conn(src, tgt, label="", color='#555555', dashed=False):
    i = _nid()
    d = 'dashed=1;' if dashed else ''
    style = (f'edgeStyle=orthogonalEdgeStyle;rounded=1;exitX=0.5;exitY=1;'
             f'exitDx=0;exitDy=0;entryX=0.5;entryY=0;entryDx=0;entryDy=0;'
             f'strokeColor={color};fontColor={color};fontSize=9;{d}')
    cells.append(
        f'<mxCell id="{i}" value="{_esc(label)}" style="{style}" '
        f'edge="1" source="{src}" target="{tgt}" parent="1">'
        f'<mxGeometry relative="1" as="geometry"/>'
        f'</mxCell>'
    )
    return i

def conn_h(src, tgt, label="", color='#555555'):
    """Horizontal edge — exits right, enters left."""
    i = _nid()
    style = (f'edgeStyle=orthogonalEdgeStyle;rounded=1;exitX=1;exitY=0.5;'
             f'exitDx=0;exitDy=0;entryX=0;entryY=0.5;entryDx=0;entryDy=0;'
             f'strokeColor={color};fontColor={color};fontSize=9;')
    cells.append(
        f'<mxCell id="{i}" value="{_esc(label)}" style="{style}" '
        f'edge="1" source="{src}" target="{tgt}" parent="1">'
        f'<mxGeometry relative="1" as="geometry"/>'
        f'</mxCell>'
    )
    return i

# ── Reusable styles ────────────────────────────────────────────────────────────
def S_BG(fill, stroke):
    return (f'rounded=1;whiteSpace=wrap;html=1;fillColor={fill};strokeColor={stroke};'
            f'fontSize=11;fontStyle=1;verticalAlign=top;align=left;spacingLeft=6;spacingTop=4;')

def S_BOX(fill, stroke):
    return f'rounded=1;whiteSpace=wrap;html=1;fillColor={fill};strokeColor={stroke};fontSize=10;'

def S_OP():
    return 'rounded=1;whiteSpace=wrap;html=1;fillColor=#d5e8d4;strokeColor=#82b366;fontSize=9;fontStyle=1;'

def S_NOTE():
    return ('text;html=1;strokeColor=none;fillColor=none;align=left;'
            'verticalAlign=top;whiteSpace=wrap;fontSize=8;fontStyle=2;')

def S_BADGE(fill='#fff2cc', stroke='#d6b656'):
    return f'rounded=1;whiteSpace=wrap;html=1;fillColor={fill};strokeColor={stroke};fontSize=8;'

def S_TITLE():
    return ('text;html=1;strokeColor=none;fillColor=none;align=center;'
            'verticalAlign=middle;whiteSpace=wrap;fontSize=17;fontStyle=1;')

def S_HEADER():
    return ('text;html=1;strokeColor=none;fillColor=none;align=center;'
            'verticalAlign=middle;whiteSpace=wrap;fontSize=12;fontStyle=1;')

# ═══════════════════════════════════════════════════════════════════════════════
# LAYOUT
#   Main pipeline:  x=30 … 1000   (width ~970)
#   Right panel:    x=1060 … 1660  (width ~600)
#   Total canvas:   1700 × 1430
# ═══════════════════════════════════════════════════════════════════════════════

MW = 970   # main width
MX = 30    # main left
RX = 1060  # right panel left
RW = 610   # right panel width

# ── TITLE ─────────────────────────────────────────────────────────────────────
shape('AIS Data Platform — Kappa Architecture on AWS\n'
      'Flink Job 1 (Bronze)  →  Job 2 (Silver)  →  Job 3 (Gold)',
      MX, 8, MW + RW + 30, 48, S_TITLE())

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — DATA SOURCES  (y=62)
# ─────────────────────────────────────────────────────────────────────────────
shape('Data Sources', MX, 62, MW, 108, S_BG('#dbeafe', '#3b82f6'))
s_terr = shape('Terrestrial Receivers\n(NMEA / TCP · UDP)', 60,  92, 175, 60, S_BOX('#bfdbfe', '#3b82f6'))
s_sat  = shape('Satellite Receivers\n(Orbcomm / SpaceQuest)',  250, 92, 175, 60, S_BOX('#bfdbfe', '#3b82f6'))
s_roam = shape('Roaming Receivers\n(Cross-provider)', 440, 92, 175, 60, S_BOX('#bfdbfe', '#3b82f6'))
shape('• 10 K+ msgs/sec peak\n• Raw NMEA 0183 + tag blocks (s:, c:, t:)\n• UDP / TCP transport from global stations', 640, 88, 330, 72, S_NOTE())

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — GO AIS RECEIVER  (y=178)
# ─────────────────────────────────────────────────────────────────────────────
shape('Ingestion Layer — Go AIS Receiver  (Amazon EKS)', MX, 178, MW, 108, S_BG('#fef3c7', '#d97706'))
recv = shape('Go AIS Receiver\n(EKS / Kubernetes)\n2–20 pods per topic  (HPA)', 140, 206, 260, 68, S_BOX('#fde68a', '#d97706'))
shape('• Multi-part NMEA fragment assembly (source attribution)\n'
      '• Source tagging: copies s: header to all fragment parts\n'
      '• Exactly-once publish via Kafka transactions\n'
      '• IAM-authenticated MSK producer (SASL/SSL port 9098)\n'
      '• HPA autoscales on CloudWatch consumer-group lag metric',
      430, 202, 548, 78, S_NOTE())

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — MSK SOURCE TOPICS  (y=294)
# ─────────────────────────────────────────────────────────────────────────────
shape('Amazon MSK — Source Topics  '
      '(3 brokers · 3 AZs · replication factor = 3)',
      MX, 294, MW, 108, S_BG('#fef9c3', '#ca8a04'))
msk_terr = shape('ais-terrestrial\n32 partitions', 80,  322, 155, 58, S_BOX('#fef08a', '#ca8a04'))
msk_sat  = shape('ais-satellite\n32 partitions',   250, 322, 155, 58, S_BOX('#fef08a', '#ca8a04'))
msk_roam = shape('ais-roaming\n32 partitions',     420, 322, 155, 58, S_BOX('#fef08a', '#ca8a04'))
shape('• SASL/SSL + AWS IAM auth (port 9098)\n'
      '• 7-day retention → full replay on consumer reset\n'
      '• Replication = 3 → survives single broker or AZ failure\n'
      '• Scalability: add partitions + brokers with zero downtime\n'
      '• Durability: committed offsets survive full MSK restart',
      600, 318, 378, 78, S_NOTE())

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 — FLINK JOB 1  (y=410)
# ─────────────────────────────────────────────────────────────────────────────
shape('Flink Job 1 — Parser / Validator  (Bronze Layer)   '
      '|  Parallelism: 4   |  Checkpoint: EXACTLY_ONCE every 60 s   |  Restart: 3× / 10 s delay',
      MX, 410, MW, 220, S_BG('#dcfce7', '#16a34a'))

op1 = shape('NMEANormalizer\n(MapFunction)\nKpler line normalization', 55,  455, 138, 58, S_OP())
op2 = shape('Fragment\nAssembler\n(KeyedProcessFn)\nkey: topic:chan:seqId', 205, 455, 138, 58, S_OP())
op3 = shape('NMEAParser\n(FlatMapFn)\nAisLib decode\n+ tag block extract', 355, 455, 138, 58, S_OP())
op4 = shape('ASMDecoder\n(MapFunction)\nType 6 / 8\nbinary ASM payloads', 505, 455, 138, 58, S_OP())
op5 = shape('Type24Joiner\n(KeyedProcessFn)\nPart A + Part B\nkey: MMSI · TTL 5 min', 655, 455, 138, 58, S_OP())
op6 = shape('Validation\nEngine\n(MapFunction)\nrules.yml driven', 805, 455, 138, 58, S_OP())

# operator chain arrows (horizontal)
for src, tgt in [(op1,op2),(op2,op3),(op3,op4),(op4,op5),(op5,op6)]:
    conn_h(src, tgt, '', '#16a34a')

# badges row 1
shape('Side output → DLQ (unparseable lines)', 355, 520, 230, 20, S_BADGE('#fecaca', '#dc2626'))
shape('5-min state TTL (Type24 join window)',   655, 520, 230, 20, S_BADGE('#bbf7d0', '#16a34a'))

# badge rows 2–4 (fault tolerance, checkpointing, scalability)
shape('Exactly-Once Checkpointing  |  State backend: RocksDB (incremental snapshots)  |  Back-pressure: Flink credit-based flow control',
      40, 550, MW - 50, 22, S_BADGE('#bbf7d0', '#16a34a'))
shape('Fault tolerance: restore from last checkpoint (< 60 s RPO)  |  Flink restart = 3 fixed-delay attempts  |  MSK offsets committed at checkpoint',
      40, 578, MW - 50, 22, S_BADGE('#fef9c3', '#ca8a04'))
shape('Scalability: parallelism ≤ MSK partition count (32)  |  Add TaskManagers → Flink rebalances automatically  |  Max ~50 K msgs/sec at p=32',
      40, 606, MW - 50, 22, S_BADGE('#dbeafe', '#3b82f6'))

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — JOB 1 OUTPUTS  (y=638)
# ─────────────────────────────────────────────────────────────────────────────
shape('Job 1 Outputs', MX, 638, MW, 108, S_BG('#f3e8ff', '#9333ea'))
s3_bronze  = shape('S3 Bronze — Parquet\ns3://…/ais-bronze-ingest/\nyear=Y/month=M/day=D/\nSnappy compressed', 70,  666, 200, 70, S_BOX('#e9d5ff', '#9333ea'))
msk_ingest = shape('MSK  ais.ingest\n(JSON per record)\n32 partitions', 300, 666, 165, 70, S_BOX('#fef08a', '#ca8a04'))
msk_dlq1   = shape('MSK  ais.parse.dlq\n(DLQ — side output)\nunparseable NMEA', 500, 666, 170, 70, S_BOX('#fecaca', '#dc2626'))
shape('• Parquet: columnar, 10:1 compression vs JSON\n'
      '• Hive partitions → date-pruning in Athena queries\n'
      '• S3: 11-nines durability, lifecycle → Glacier 90 d\n'
      '• ais.ingest consumed by Job 2 downstream\n'
      '• DLQ: 7-day retention, operator can replay & fix',
      695, 662, 302, 78, S_NOTE())

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — FLINK JOB 2  (y=754)
# ─────────────────────────────────────────────────────────────────────────────
shape('Flink Job 2 — MSK Router  (Silver Layer)   '
      '|  Parallelism: 4   |  Checkpoint: EXACTLY_ONCE every 60 s',
      MX, 754, MW, 138, S_BG('#dcfce7', '#16a34a'))
job2 = shape('Route by msgType\n'
             '1 / 2 / 3  →  ais.type1  (Position Reports)\n'
             '5          →  ais.type5  (Voyage & Static)\n'
             '18 / 19    →  ais.type18 (Class B Position)\n'
             '24         →  ais.type24 (Class B Static)\n'
             '27         →  ais.type27 (Long Range AIS)\n'
             'unknown    →  ais.router.dlq',
             80, 782, 360, 98, S_BOX('#86efac', '#16a34a'))
shape('• Fan-out: 1 input record → 1 typed output topic\n'
      '• Exactly-once with MSK transactional producer\n'
      '• Type-sharding → each consumer sees only relevant msgs\n'
      '• Fault: job restarts from checkpoint; MSK txn aborted\n'
      '• Scalability: each typed topic independently partitioned\n'
      '• DLQ: unknown/malformed msgType records preserved',
      470, 778, 500, 98, S_NOTE())

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7 — MSK TYPED TOPICS  (y=900)
# ─────────────────────────────────────────────────────────────────────────────
shape('Amazon MSK — Typed Topics  (Silver · replication = 3)',
      MX, 900, MW, 95, S_BG('#fef9c3', '#ca8a04'))
t1 = shape('ais.type1\nPosition', 55,  928, 122, 52, S_BOX('#fef08a', '#ca8a04'))
t2 = shape('ais.type5\nVoyage',   190, 928, 122, 52, S_BOX('#fef08a', '#ca8a04'))
t3 = shape('ais.type18\nClass B', 325, 928, 122, 52, S_BOX('#fef08a', '#ca8a04'))
t4 = shape('ais.type24\nStatic',  460, 928, 122, 52, S_BOX('#fef08a', '#ca8a04'))
t5 = shape('ais.type27\nLong Rng',595, 928, 122, 52, S_BOX('#fef08a', '#ca8a04'))
t6 = shape('ais.router\n.dlq',    730, 928, 108, 52, S_BOX('#fecaca', '#dc2626'))

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 8 — FLINK JOB 3  (y=1003)
# ─────────────────────────────────────────────────────────────────────────────
shape('Flink Job 3 — Quality Filter  (Gold Layer)   '
      '|  Parallelism: 4   |  Checkpoint: EXACTLY_ONCE every 60 s',
      MX, 1003, MW, 148, S_BG('#dcfce7', '#16a34a'))
job3 = shape('1. Dedup: MMSI + timestamp (30 s keyed window)\n'
             '2. Coord validation: lat ∈ [−90, 90]  lon ∈ [−180, 180]\n'
             '3. Quality score: field completeness (0.0 – 1.0)\n'
             '4. Threshold filter: score ≥ 0.7 (configurable)\n'
             '5. Iceberg upsert: keyed by MMSI + event time',
             80, 1030, 380, 108, S_BOX('#86efac', '#16a34a'))
shape('• Iceberg: ACID transactions, time travel, schema evolution\n'
      '• Upsert semantics: late-arriving data corrects prior records\n'
      '• Exactly-once via Iceberg commit protocol (2-phase commit)\n'
      '• Dedup prevents double-counting from replay / retries\n'
      '• Fault: Iceberg rolls back incomplete txn on restart\n'
      '• Scalability: Iceberg table sharding by MMSI hash range',
      490, 1026, 490, 110, S_NOTE())

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 9 — GOLD OUTPUTS  (y=1159)
# ─────────────────────────────────────────────────────────────────────────────
shape('Gold Layer — Analytics-Ready', MX, 1159, MW, 108, S_BG('#f3e8ff', '#9333ea'))
s3_gold    = shape('S3 Iceberg Gold\ns3://…/ais-gold/\nMMSI partitioned\ntime-travel enabled', 60,  1187, 195, 68, S_BOX('#e9d5ff', '#9333ea'))
glue       = shape('AWS Glue\nData Catalog\n(Iceberg schema\n& table registry)', 278, 1187, 165, 68, S_BOX('#e9d5ff', '#9333ea'))
athena     = shape('Amazon Athena\n(serverless SQL\npartition pruning\nPay-per-scan)', 466, 1187, 165, 68, S_BOX('#bfdbfe', '#3b82f6'))
quicksight = shape('Amazon\nQuickSight\n(Fleet dashboards\nreal-time)', 654, 1187, 165, 68, S_BOX('#bfdbfe', '#3b82f6'))
shape('• Time travel: SELECT … AS OF TIMESTAMP\n'
      '• Schema evolution: add fields without full rewrite\n'
      '• Lifecycle: 1 yr hot S3 → Glacier → expire\n'
      '• Athena: predicate pushdown on Iceberg metadata',
      840, 1183, 148, 78, S_NOTE())

# ═══════════════════════════════════════════════════════════════════════════════
# RIGHT PANEL — CROSS-CUTTING CONCERNS
# ═══════════════════════════════════════════════════════════════════════════════

# ── FAULT TOLERANCE & DURABILITY  (y=62)
shape('Fault Tolerance & Durability', RX, 62, RW, 348, S_BG('#fefce8', '#ca8a04'))

shape('S3 Checkpoints  (all 3 Flink jobs)\n'
      's3://bucket/flink-checkpoints/{job1, job2, job3}/\n'
      'Retained on cancellation  |  Incremental (RocksDB delta)',
      RX+18, 100, RW-36, 58, S_BOX('#fef9c3', '#ca8a04'))

shape('Checkpoint config (per job)\n'
      '• Interval: 60 s   |   Min pause between: 30 s\n'
      '• Max concurrent: 1   |   Mode: EXACTLY_ONCE (barriers)\n'
      '• Storage: S3 via Flink S3 filesystem plugin\n'
      '• Incremental RocksDB → only delta written each cycle',
      RX+18, 170, RW-36, 80, S_BOX('#fef9c3', '#ca8a04'))

shape('MSK Durability\n'
      '• Replication factor 3 across 3 AZs\n'
      '• Retention 7 days → replay from any offset\n'
      '• Survives single broker failure (no data loss)\n'
      '• Consumer group offset committed at checkpoint',
      RX+18, 262, RW-36, 78, S_BOX('#fef9c3', '#ca8a04'))

shape('S3 durability: 99.999999999 % (11 nines)\n'
      'RPO < 60 s  |  RTO ~ 2 min (restart + warmup)\n'
      'Flink restore: replay MSK from last checkpoint offset',
      RX+18, 352, RW-36, 48, S_NOTE())

# ── RETRY & ERROR HANDLING  (y=418)
shape('Retry & Error Handling', RX, 418, RW, 290, S_BG('#fff1f2', '#e11d48'))

shape('Flink Restart Strategy\n'
      'Type: Fixed Delay   |   Attempts: 3   |   Delay: 10 s\n'
      'Trigger: task failure, OOM, uncaught exception\n'
      'After 3 failures → job FAILED → CloudWatch alarm',
      RX+18, 452, RW-36, 72, S_BOX('#fecdd3', '#e11d48'))

shape('Error Routing (DLQ strategy per job)\n'
      '• Job 1: unparseable NMEA → ais.parse.dlq (side output)\n'
      '• Job 2: unknown msgType → ais.router.dlq\n'
      '• Job 3: invalid coords / low quality → logged + skipped\n'
      '• DLQ retention: 7 days  |  replayable after operator fix',
      RX+18, 536, RW-36, 95, S_BOX('#fecdd3', '#e11d48'))

shape('Back-pressure: Flink credit-based flow control\n'
      'Slow consumers automatically slow MSK readers\n'
      'No data loss — source re-reads from committed offset\n'
      'CloudWatch alarm: DLQ rate > 1 % of ingest rate',
      RX+18, 642, RW-36, 56, S_NOTE())

# ── SCALABILITY  (y=716)
shape('Scalability', RX, 716, RW, 242, S_BG('#f0fdf4', '#16a34a'))

shape('Horizontal scaling levers\n'
      '• MSK: add partitions / brokers live (zero downtime)\n'
      '• Flink: add TaskManager pods → auto rebalance operators\n'
      '• Go Receiver: KEDA HPA on consumer-group lag metric\n'
      '• Max Flink parallelism = MSK partition count (32)',
      RX+18, 752, RW-36, 88, S_BOX('#bbf7d0', '#16a34a'))

shape('Throughput estimates (p=32, 4 TMs)\n'
      '• Ingest: ~50 K msgs/sec  (source topics)\n'
      '• After Job 1 parsing: ~42 K/sec (DLQ ~2 %)\n'
      '• Gold Iceberg writes: ~35 K/sec (after dedup)\n'
      'Scale-out: double partitions → double throughput',
      RX+18, 852, RW-36, 96, S_NOTE())

# ── SECURITY  (y=966)
shape('Security', RX, 966, RW, 198, S_BG('#eff6ff', '#3b82f6'))

shape('Identity & Access\n'
      '• MSK: SASL/SSL + AWS IAM (port 9098, TLS 1.2+)\n'
      '• S3: IAM role per Flink job (least privilege)\n'
      '• EKS: IRSA — pod-level IAM role (no node credentials)\n'
      '• MSK in VPC private subnets, no public endpoint\n'
      '• No credentials in code — K8s Secrets / Param Store',
      RX+18, 1000, RW-36, 118, S_BOX('#bfdbfe', '#3b82f6'))

shape('VPC layout: MSK brokers on 10.30.x.x\n'
      'Flink TaskManagers in same VPC / SG\n'
      'All traffic stays within AWS network',
      RX+18, 1130, RW-36, 24, S_NOTE())

# ── MONITORING  (y=1172)
shape('Monitoring & Alerting', RX, 1172, RW, 198, S_BG('#fdf4ff', '#9333ea'))

shape('Amazon CloudWatch\n'
      '• MSK consumer-group lag per topic → HPA trigger\n'
      '• Flink job health via REST API (metrics scrape)\n'
      '• S3 Bronze object count / size (data gap detection)\n'
      '• DLQ message rate alarm: > 1 % → PagerDuty\n'
      '• Flink job FAILED alarm → SNS → on-call rotation',
      RX+18, 1206, RW-36, 118, S_BOX('#e9d5ff', '#9333ea'))

shape('Optional: Flink metrics → Prometheus + Grafana\n'
      'Iceberg table metrics via Glue / Athena metadata',
      RX+18, 1336, RW-36, 24, S_NOTE())

# ── PERFORMANCE  (y=1378)
shape('Performance', RX, 1378, RW, 130, S_BG('#fff7ed', '#c2410c'))
shape('• Parquet + Snappy: ~10:1 vs JSON; predicate pushdown\n'
      '• Hive-style date partitions → Athena cost reduction\n'
      '• Flink pipelined execution: no shuffle between operators\n'
      '• RocksDB incremental checkpoints: delta only (~3 MB @ 50 K/s)\n'
      '• Iceberg metadata caching: fast split planning\n'
      '• MSK: large batch fetching reduces round-trips',
      RX+18, 1410, RW-36, 88, S_NOTE())

# ═══════════════════════════════════════════════════════════════════════════════
# EDGES
# ═══════════════════════════════════════════════════════════════════════════════
BLK  = '#374151'
GRN  = '#16a34a'
ORG  = '#d97706'
RED  = '#dc2626'
PRP  = '#9333ea'
BLU  = '#3b82f6'

# Sources → Receiver
conn(s_terr, recv, '',             BLK)
conn(s_sat,  recv, '',             BLK)
conn(s_roam, recv, '',             BLK)

# Receiver → MSK source topics
conn(recv, msk_terr, 'SASL/SSL · IAM', ORG)
conn(recv, msk_sat,  '',               ORG)
conn(recv, msk_roam, '',               ORG)

# MSK source → Job 1 (all three feed op1)
conn(msk_terr, op1, 'Tuple2<topic, rawLine>', BLK)
conn(msk_sat,  op1, '',                       BLK)
conn(msk_roam, op1, '',                       BLK)

# Job 1 outputs
conn(op6, s3_bronze,  'Parquet write', PRP)
conn(op6, msk_ingest, 'JSON publish',  ORG)
conn(op3, msk_dlq1,   'side output\n(parse fail)', RED, dashed=True)

# MSK ingest → Job 2
conn(msk_ingest, job2, 'consume · SASL/SSL', BLK)

# Job 2 → Typed topics
conn(job2, t1, '',       GRN)
conn(job2, t2, '',       GRN)
conn(job2, t3, '',       GRN)
conn(job2, t4, '',       GRN)
conn(job2, t5, '',       GRN)
conn(job2, t6, 'errors', RED, dashed=True)

# Typed topics → Job 3
conn(t1, job3, '', BLK)
conn(t2, job3, '', BLK)
conn(t3, job3, '', BLK)
conn(t4, job3, '', BLK)
conn(t5, job3, '', BLK)

# Job 3 → Gold
conn(job3,  s3_gold,    'Iceberg write', PRP)
conn_h(s3_gold,    glue,       'register', PRP)
conn_h(glue,       athena,     'catalog',  BLU)
conn_h(athena,     quicksight, 'query',    BLU)

# ═══════════════════════════════════════════════════════════════════════════════
# WRITE XML
# ═══════════════════════════════════════════════════════════════════════════════
lines = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<mxGraphModel dx="1422" dy="762" grid="1" gridSize="10" guides="1" '
    'tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" '
    'pageWidth="1800" pageHeight="1550" math="0" shadow="0">',
    '  <root>',
    '    <mxCell id="0"/>',
    '    <mxCell id="1" parent="0"/>',
]
for c in cells:
    lines.append('    ' + c)
lines += ['  </root>', '</mxGraphModel>']

out = '/tmp/ais-flink-job1-java/docs/ais_architecture.drawio'
with open(out, 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))

print(f"✓  {len(cells)} elements generated")
print(f"✓  Saved to: {out}")
print()
print("How to open:")
print("  Lucidchart : File → Import → Lucidchart / draw.io XML → select the .drawio file")
print("  draw.io    : File → Import from → XML (or drag-drop the file)")
