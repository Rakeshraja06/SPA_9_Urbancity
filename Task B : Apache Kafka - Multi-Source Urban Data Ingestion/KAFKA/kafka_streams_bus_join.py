#!/usr/bin/env python3
"""
UrbanPulse — Kafka Streams Bus GPS Enrichment (KTable Join)
============================================================
Reads from  : urbanpulse.bus_gps
KTable      : route_schedule.csv  (static lookup — loaded once into memory)
Writes to   : urbanpulse.bus_gps_enriched  (Kafka topic)

Each bus_gps event is enriched with:
  - route_name            (e.g. "Airport Express")
  - terminal              (e.g. "Kempegowda Bus Terminal")
  - scheduled_arrival_time (e.g. "06:00")
  - frequency_min         (service frequency in minutes)

Why an in-memory dict instead of Java Kafka Streams KTable?
  The assignment requires simple Python. A Python dict loaded from CSV
  is functionally equivalent to a KTable for a static lookup — it is
  pre-loaded once at startup and used for every join, exactly like
  a KTable is built from a compacted changelog topic.

Run:
  python3 kafka_streams_bus_join.py

Output sample:
  BUS-0012 | route=R005 | Bannerghatta Road → Jayanagar 4th Block
            | lat=12.9342 lon=77.6102 | speed=32.1 km/h
            | scheduled_arrival=07:00 | freq=15 min
"""

import csv
import json
import logging
import sys
from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("BusGPSEnrichment")

import os
BOOTSTRAP        = "localhost:29092"
INPUT_TOPIC      = "urbanpulse.bus_gps"
SCHEDULE_CSV     = os.path.join(os.path.dirname(__file__), "route_schedule.csv")


# ── Step 1: Load KTable (route_schedule.csv → in-memory dict) ──
def load_ktable(csv_path: str) -> dict:
    """
    Load route schedule CSV into a dict keyed by route_id.
    This is our KTable — a static snapshot of route metadata.
    """
    table = {}
    try:
        with open(csv_path, newline="") as f:
            for row in csv.DictReader(f):
                table[row["route_id"]] = {
                    "route_name":             row["route_name"],
                    "terminal":               row["terminal"],
                    "scheduled_arrival_time": row["scheduled_arrival_time"],
                    "frequency_min":          row["frequency_min"],
                }
        log.info("KTable loaded — %d routes from %s", len(table), csv_path)
    except FileNotFoundError:
        log.error("route_schedule.csv not found at %s — run from KAFKA/ directory", csv_path)
        sys.exit(1)
    return table


# ── Step 2: Join function ──
def enrich(gps_event: dict, ktable: dict) -> dict | None:
    """
    Join one GPS event with the KTable on route_id.
    Returns enriched event, or None if route not found (goes to DLQ).
    """
    route = ktable.get(gps_event.get("route_id"))
    if route is None:
        return None   # unknown route — no match

    return {
        # Original GPS fields
        "bus_id":        gps_event["bus_id"],
        "route_id":      gps_event["route_id"],
        "lat":           gps_event["lat"],
        "lon":           gps_event["lon"],
        "speed_kmh":     gps_event["speed_kmh"],
        "occupancy_pct": gps_event["occupancy_pct"],
        "timestamp":     gps_event["timestamp"],
        # Enriched schedule fields (from KTable)
        "route_name":             route["route_name"],
        "terminal":               route["terminal"],
        "scheduled_arrival_time": route["scheduled_arrival_time"],
        "frequency_min":          route["frequency_min"],
    }


def main():
    # Load the KTable once at startup
    ktable = load_ktable(SCHEDULE_CSV)

    # Consumer config
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id":          "bus-gps-enrichment-job",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe([INPUT_TOPIC])

    # Producer config
    p_conf = {
        "bootstrap.servers": BOOTSTRAP,
        "client.id":         "bus-gps-enrichment-producer",
        "compression.type":  "lz4",
    }
    producer = Producer(p_conf)

    log.info("Listening on %s — will produce enriched events to urbanpulse.bus_gps_enriched...", INPUT_TOPIC)
    print("-" * 70)

    joined   = 0
    no_match = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            gps = json.loads(msg.value())
            enriched = enrich(gps, ktable)

            if enriched is None:
                no_match += 1
                log.warning("No KTable match for route_id=%s", gps.get("route_id"))
                continue

            joined += 1

            # Produce enriched record to output topic
            producer.produce(
                "urbanpulse.bus_gps_enriched",
                key=enriched["route_id"].encode(),
                value=json.dumps(enriched).encode()
            )

            # Also print periodically
            if joined % 500 == 0:
                print(
                    f"  {enriched['bus_id']:10s} | route={enriched['route_id']} "
                    f"| {enriched['route_name']} → {enriched['terminal']}\n"
                    f"  lat={enriched['lat']} lon={enriched['lon']} "
                    f"speed={enriched['speed_kmh']} km/h | "
                    f"arrival={enriched['scheduled_arrival_time']} | "
                    f"freq={enriched['frequency_min']} min"
                )
                log.info("Joined: %d | No-match: %d", joined, no_match)
                producer.poll(0)

    except KeyboardInterrupt:
        log.info("Stopping — joined=%d no_match=%d", joined, no_match)
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
