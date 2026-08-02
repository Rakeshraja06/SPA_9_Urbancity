#!/usr/bin/env python3
"""
UrbanPulse — Bus GPS Producer
Topic : urbanpulse.bus_gps
Rate  : ~2,400 events/sec  (600 buses, 40 routes)

Key features (assignment requirements):
  - Key = route_id  → all buses on the same route land in the
    same partition → per-route ordering is guaranteed by Kafka
  - At-least-once semantics (acks=all, retries=10, idempotence)
  - Logging (structured, both file and stdout)
"""

import json
import random
import time
import sys
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaException

# ── Logging setup ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bus_gps_producer.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("BusGPSProducer")

# ── Config ──
BOOTSTRAP   = "localhost:29092"
TOPIC       = "urbanpulse.bus_gps"
RATE        = 2400
NUM_BUSES   = 600
NUM_ROUTES  = 40

# Bengaluru bounding box
LAT_MIN, LAT_MAX = 12.88, 13.12
LON_MIN, LON_MAX = 77.48, 77.75

# ── Initial bus state ──
buses = {
    f"BUS-{i:04d}": {
        "route_id":  f"R{random.randint(1, NUM_ROUTES):03d}",
        "lat":       random.uniform(LAT_MIN, LAT_MAX),
        "lon":       random.uniform(LON_MIN, LON_MAX),
        "speed":     random.uniform(10, 45),
        "occupancy": random.randint(5, 90),
    }
    for i in range(NUM_BUSES)
}
bus_ids = list(buses.keys())


def update_bus(b: dict) -> None:
    """Random-walk bus position and speed."""
    b["speed"]     = max(0, min(b["speed"] + random.uniform(-3, 3), 80))
    b["lat"]       = max(LAT_MIN, min(b["lat"] + random.uniform(-0.00015, 0.00015), LAT_MAX))
    b["lon"]       = max(LON_MIN, min(b["lon"] + random.uniform(-0.00015, 0.00015), LON_MAX))
    b["occupancy"] = max(0, min(b["occupancy"] + random.randint(-2, 2), 100))


def make_event() -> dict:
    bid = random.choice(bus_ids)
    b   = buses[bid]
    update_bus(b)
    return {
        "bus_id":       bid,
        "route_id":     b["route_id"],
        "lat":          round(b["lat"], 6),
        "lon":          round(b["lon"], 6),
        "speed_kmh":    round(b["speed"], 1),
        "occupancy_pct": b["occupancy"],
        "timestamp":    datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


def delivery_cb(err, msg):
    if err:
        log.error("Delivery FAILED — route=%s err=%s", msg.key(), err)


def produce_with_retry(producer: Producer, event: dict, max_retries: int = 5) -> None:
    """At-least-once: retry with exponential backoff on transient errors."""
    payload = json.dumps(event).encode()
    # KEY = route_id — guarantees all buses on same route go to same partition
    key = event["route_id"].encode()

    for attempt in range(1, max_retries + 1):
        try:
            producer.produce(TOPIC, key=key, value=payload, callback=delivery_cb)
            return
        except KafkaException as exc:
            wait = 0.1 * (2 ** (attempt - 1))
            log.warning("Produce failed attempt %d/%d — %s — retry in %.1fs",
                        attempt, max_retries, exc, wait)
            time.sleep(wait)
        except BufferError:
            producer.poll(0.5)   # drain queue then retry

    log.error("Gave up after %d attempts for bus %s", max_retries, event["bus_id"])


def main():
    conf = {
        "bootstrap.servers":   BOOTSTRAP,
        "client.id":           "bus-gps-producer",
        # At-least-once
        "enable.idempotence":  True,
        "acks":                "all",
        "retries":             10,
        "retry.backoff.ms":    500,
        # Throughput (high rate stream)
        "linger.ms":           10,
        "batch.num.messages":  1000,
        "compression.type":    "lz4",
    }

    producer = Producer(conf)
    log.info("Starting — topic=%s rate=%d/s buses=%d routes=%d",
             TOPIC, RATE, NUM_BUSES, NUM_ROUTES)

    try:
        while True:
            t0 = time.perf_counter()

            for _ in range(RATE):
                produce_with_retry(producer, make_event())

            producer.poll(0)

            elapsed = time.perf_counter() - t0
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)

    except KeyboardInterrupt:
        log.info("Keyboard interrupt — flushing...")
    finally:
        producer.flush(timeout=10)
        log.info("Done.")


if __name__ == "__main__":
    main()
