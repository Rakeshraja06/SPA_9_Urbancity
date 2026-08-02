#!/usr/bin/env python3
"""
UrbanPulse — Air Quality Producer
Topic : urbanpulse.air_quality
Rate  : ~60 events/sec  (50 sensors across 5 zones)

Key features (assignment requirements):
  - At-least-once semantics  (enable.idempotence=True, acks=all, retries=10)
  - Exponential-backoff retry on produce failure
  - 5% of events have null AQI (sensor fault) — handled & logged, never crashes
  - Keyed by sensor_id so same sensor always hits same partition
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
        logging.FileHandler("air_quality_producer.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("AirQualityProducer")

# ── Config ──
BOOTSTRAP = "localhost:29092"
TOPIC     = "urbanpulse.air_quality"
RATE      = 60          # events per second
NUM_SENSORS = 50
ZONES = ["North", "South", "East", "West", "Central"]

# ── Sensor state (gradual walk simulation) ──
sensors = {
    f"AQ-{i:03d}": {
        "zone":  random.choice(ZONES),
        "pm25":  random.uniform(10, 80),
        "pm10":  random.uniform(20, 120),
        "no2":   random.uniform(10, 70),
    }
    for i in range(NUM_SENSORS)
}
sensor_ids = list(sensors.keys())


def calc_aqi(pm25: float) -> int:
    """Simplified AQI from PM2.5 (0–500)."""
    return max(0, min(int(pm25 * 2.5), 500))


def update_sensor(s: dict) -> None:
    """Random-walk each reading within realistic bounds."""
    s["pm25"] = max(0,   min(s["pm25"] + random.uniform(-2, 2),  250))
    s["pm10"] = max(0,   min(s["pm10"] + random.uniform(-3, 3),  400))
    s["no2"]  = max(0,   min(s["no2"]  + random.uniform(-2, 2),  200))


def make_event() -> dict:
    """Create one air-quality event; 5% chance of null AQI."""
    sid = random.choice(sensor_ids)
    s   = sensors[sid]
    update_sensor(s)

    aqi = calc_aqi(s["pm25"])
    if random.random() < 0.05:          # 5% sensor fault
        aqi = None
        log.warning("NULL AQI — sensor %s zone %s (fault simulated)", sid, s["zone"])

    return {
        "sensor_id": sid,
        "zone":      s["zone"],
        "pm25":      round(s["pm25"], 1),
        "pm10":      round(s["pm10"], 1),
        "no2":       round(s["no2"],  1),
        "aqi":       aqi,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


def delivery_cb(err, msg):
    """Called by librdkafka when a message is acknowledged (or fails permanently)."""
    if err:
        log.error("Delivery FAILED — topic=%s err=%s", msg.topic(), err)
    # on success we stay silent (60 msgs/sec would flood the log)


def produce_with_retry(producer: Producer, event: dict, max_retries: int = 5) -> None:
    """
    Produce one message with exponential-backoff retry.
    Handles transient KafkaException (broker unavailable, queue full, sensor timeout).
    """
    payload = json.dumps(event).encode()
    key     = event["sensor_id"].encode()

    for attempt in range(1, max_retries + 1):
        try:
            producer.produce(TOPIC, key=key, value=payload, callback=delivery_cb)
            return                                  # success — exit retry loop
        except KafkaException as exc:
            wait = 0.1 * (2 ** (attempt - 1))      # 0.1s, 0.2s, 0.4s, 0.8s, 1.6s
            log.warning(
                "Produce failed (attempt %d/%d) — %s — retrying in %.1fs",
                attempt, max_retries, exc, wait,
            )
            time.sleep(wait)
        except BufferError:
            # Internal queue full — poll to drain, then retry immediately
            producer.poll(0.5)

    log.error("Gave up after %d attempts for sensor %s", max_retries, event["sensor_id"])


def main():
    conf = {
        "bootstrap.servers":        BOOTSTRAP,
        "client.id":                "air-quality-producer",
        # At-least-once semantics
        "enable.idempotence":       True,   # prevents duplicates on retry
        "acks":                     "all",  # wait for all ISR replicas
        "retries":                  10,
        "retry.backoff.ms":         500,
        # Throughput tuning
        "linger.ms":                5,
        "batch.num.messages":       100,
    }

    producer = Producer(conf)
    log.info("Starting — topic=%s rate=%d/s", TOPIC, RATE)

    try:
        while True:
            t0 = time.perf_counter()

            for _ in range(RATE):
                produce_with_retry(producer, make_event())

            producer.poll(0)                        # serve delivery callbacks

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
