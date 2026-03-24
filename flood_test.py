#!/usr/bin/env python3
"""
flood_test.py — AIS Pipeline Load Test
Produces a mix of valid and malformed NMEA messages to Redpanda.

Usage:
  python3 flood_test.py [--count N] [--bad-pct P] [--topic T]

  --count   total messages to send   (default: 5000)
  --bad-pct % of malformed messages  (default: 5)
  --topic   source topic to target   (default: all three source topics)
"""

import argparse
import random
import time
from kafka import KafkaProducer

# ── Known-good NMEA sentences ──────────────────────────────────────────────
# A variety of real message types for realistic testing.
VALID_NMEA = [
    # Type 1 — Position Report Class A (various MMSIs + positions)
    r"\c:1711276800,s:SPIRE-T\!AIVDM,1,1,,A,15M67N0P01G?Uf6E`Bm2h4n0L00,0*73",
    r"\c:1711276801,s:SPIRE-T\!AIVDM,1,1,,B,15RtBV0Oh;G=oHRES`3e84?v0<0O,0*2B",
    r"\c:1711276802,s:ORBCOMM-T\!AIVDM,1,1,,A,13HOI:0P0000VOHLCnHQKwvL05Ip,0*23",
    r"\c:1711276803,s:ORBCOMM-T\!AIVDM,1,1,,B,139S1h0Oh3G?UlNESV0<h4?v0D00,0*7E",
    r"\c:1711276804,s:KPLER-T\!AIVDM,1,1,,A,15NTES0Pho;:d<HESC=v;4?v0<1E,0*41",
    r"\c:1711276805,s:KPLER-T\!AIVDM,1,1,,A,15NTES0Ph<;:d<HESC=v;4?v00000,0*45",
    r"\c:1711276806,s:SPIRE-S\!AIVDM,1,1,,B,13u?etPv2;0n:dDPwUM1U1Cb069D,0*23",
    r"\c:1711276807,s:ORBCOMM-S\!AIVDM,1,1,,A,15>h8>PP01G?Vf:E`Cg2h4?v0<0O,0*66",

    # Type 3 — Position Report (response to interrogation)
    r"\c:1711276810,s:SPIRE-T\!AIVDM,1,1,,A,35NSH5`P00G?Tp<EWb2;h8?v0<00,0*36",

    # Type 18 — Class B position report
    r"\c:1711276820,s:KPLER-T\!AIVDM,1,1,,B,B5NJ;PP0:8V1PMo76<6;Bsb0l0HR,0*54",
    r"\c:1711276821,s:SPIRE-T\!AIVDM,1,1,,B,B5NT>h`006Vcnh<6;L;Bsb0ND@R,0*55",

    # Type 4 — Base Station Report
    r"\c:1711276830,s:ORBCOMM-T\!AIVDM,1,1,,A,403OviQuMGCqWrRO9>E6fE700@GO,0*4D",

    # Type 5 — Static and Voyage Data (2-part)
    r"\c:1711276840,s:SPIRE-T\!AIVDM,2,1,3,A,55?P5oP00001L@CO220l4E84j0Dh4pB222220l0`A886430000000000,0*5E",
    r"\c:1711276840,s:SPIRE-T\!AIVDM,2,2,3,A,00000000000,2*25",

    # Type 21 — Aid-to-Navigation Report
    r"\c:1711276850,s:KPLER-T\!AIVDM,1,1,,A,ENk`sR1`1`0l09@?;l=TS000000,4*4A",

    # Type 24 Part A — Class B Static (name only)
    r"\c:1711276860,s:ORBCOMM-T\!AIVDM,1,1,,A,H42O55lti4hhhilD3nink000?050,2*36",

    # Type 27 — Long Range AIS
    r"\c:1711276870,s:SPIRE-S\!AIVDM,1,1,,A,KC5E2b@U19PFdLbMuc5=ROv62<7m,0*16",
]

# ── Malformed messages (should trigger DLQ) ────────────────────────────────
BAD_NMEA = [
    # Truncated payload
    r"\c:1711276900,s:SPIRE-T\!AIVDM,1,1,,A,15M67,0*00",
    # Wrong checksum
    r"\c:1711276901,s:SPIRE-T\!AIVDM,1,1,,A,15M67N0P01G?Uf6E`Bm2h4n0L00,0*FF",
    # Completely garbled
    "NOT_NMEA_AT_ALL this is garbage data !!@@##",
    # Empty payload field
    r"\c:1711276903,s:ORBCOMM-T\!AIVDM,1,1,,A,,0*00",
    # Wrong sentence start
    r"\c:1711276904,s:KPLER-T\$AIVDM,1,1,,A,15M67N0P01G?Uf6E`Bm2h4n0L00,0*73",
    # Missing checksum entirely
    r"\c:1711276905,s:SPIRE-T\!AIVDM,1,1,,A,15M67N0P01G?Uf6E`Bm2h4n0L00,0",
    # Binary noise
    "\x00\x01\x02\x03\x04CORRUPT\xff\xfe",
]

TOPICS = ["ais-terrestrial", "ais-satellite", "ais-roaming"]


def run(count: int, bad_pct: float, target_topics: list):
    producer = KafkaProducer(
        bootstrap_servers="localhost:19092",
        value_serializer=lambda v: v.encode("utf-8", errors="replace"),
        acks=1,
        linger_ms=10,
        batch_size=64 * 1024,
    )

    bad_count   = int(count * bad_pct / 100)
    good_count  = count - bad_count
    sent        = 0
    errors      = 0
    start       = time.time()

    print(f"Producing {count:,} messages  ({good_count:,} valid + {bad_count:,} malformed)")
    print(f"Topics : {target_topics}")
    print(f"Broker : localhost:19092")
    print("-" * 55)

    # Build shuffled message list
    messages = (
        [("good", random.choice(VALID_NMEA)) for _ in range(good_count)] +
        [("bad",  random.choice(BAD_NMEA))   for _ in range(bad_count)]
    )
    random.shuffle(messages)

    for kind, msg in messages:
        topic = random.choice(target_topics)
        try:
            producer.send(topic, value=msg)
            sent += 1
        except Exception as e:
            errors += 1
            print(f"  producer error: {e}")

        if sent % 500 == 0:
            elapsed = time.time() - start
            rate    = sent / elapsed if elapsed > 0 else 0
            print(f"  sent {sent:>6,} / {count:,}  |  {rate:>7,.0f} msg/s  |  elapsed {elapsed:.1f}s")

    producer.flush()
    elapsed = time.time() - start

    print("-" * 55)
    print(f"Done.  Sent: {sent:,}  Errors: {errors}  Time: {elapsed:.2f}s  Rate: {sent/elapsed:,.0f} msg/s")
    print()
    print("Now monitor with:")
    print("  # Kafka consumer lag")
    print("  docker exec redpanda rpk topic consume ais.ingest --brokers redpanda:9092 -n 5")
    print("  docker exec redpanda rpk topic consume ais.parse.dlq --brokers redpanda:9092 -n 5")
    print("  # Flink UI")
    print("  http://localhost:8081")
    print("  # MinIO (Parquet files appear after first checkpoint ~60s)")
    print("  http://localhost:9001  →  ais-bronze-local/ais-bronze-ingest/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count",   type=int,   default=5000, help="total messages")
    parser.add_argument("--bad-pct", type=float, default=5.0,  help="% malformed messages")
    parser.add_argument("--topic",   type=str,   default=None, help="specific topic (default: all 3)")
    args = parser.parse_args()

    topics = [args.topic] if args.topic else TOPICS
    run(args.count, args.bad_pct, topics)
