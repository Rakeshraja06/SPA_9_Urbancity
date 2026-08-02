#!/usr/bin/env python3

"""
Synthetic Traffic Signal Data Generator

Topic:
    urbanpulse.traffic_signals

Rate:
    ~380 events/sec

Output:
    JSON (one event per line)

Fields:
    junction_id
    zone
    vehicle_count
    avg_wait_sec
    signal_phase
    timestamp
"""

import json
import random
import time
import sys
from datetime import datetime, timezone
from confluent_kafka import Producer

# -----------------------
# Configuration
# -----------------------

EVENTS_PER_SECOND = 380
NUM_JUNCTIONS = 120
TOPIC = "urbanpulse.traffic_signals"

ZONES = [
    "North",
    "South",
    "East",
    "West",
    "Central"
]

SIGNAL_PHASES = [
    "GREEN",
    "YELLOW",
    "RED"
]

# -----------------------
# Initial Junction State
# -----------------------

junction_state = {}

for i in range(NUM_JUNCTIONS):
    junction_id = f"JUNC-{i:03d}"

    junction_state[junction_id] = {
        "zone": random.choice(ZONES),
        "vehicle_count": random.randint(10, 120),
        "avg_wait_sec": random.randint(5, 220),
        "signal_phase": random.choice(SIGNAL_PHASES),
    }

junction_ids = list(junction_state.keys())


def update_junction(junction):
    """Simulate changing traffic conditions."""

    # Vehicle count fluctuates
    junction["vehicle_count"] += random.randint(-8, 8)
    junction["vehicle_count"] = max(0, min(junction["vehicle_count"], 250))

    # Average wait depends on traffic
    wait_change = random.randint(-8, 8)
    junction["avg_wait_sec"] += wait_change
    junction["avg_wait_sec"] = max(0, min(junction["avg_wait_sec"], 300))

    # Occasionally change signal phase
    if random.random() < 0.12:
        current = junction["signal_phase"]

        if current == "GREEN":
            junction["signal_phase"] = "YELLOW"
        elif current == "YELLOW":
            junction["signal_phase"] = "RED"
        else:
            junction["signal_phase"] = "GREEN"


def create_event():
    junction_id = random.choice(junction_ids)
    junction = junction_state[junction_id]

    update_junction(junction)

    return {
        "junction_id": junction_id,
        "zone": junction["zone"],
        "vehicle_count": junction["vehicle_count"],
        "avg_wait_sec": junction["avg_wait_sec"],
        "signal_phase": junction["signal_phase"],
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}', file=sys.stderr)

def main():
    conf = {
        'bootstrap.servers': 'localhost:29092',
        'client.id': 'traffic-signals-producer',
        'enable.idempotence': True,
        'acks': 'all',
        'retries': 10,
        'retry.backoff.ms': 500,
        'linger.ms': 10,
        'batch.num.messages': 500,
        'compression.type': 'lz4'
    }

    producer = Producer(conf)
    interval = 1.0

    print(f"Starting traffic_signals_producer, publishing to {TOPIC} at ~{EVENTS_PER_SECOND} msgs/sec")
    
    try:
        while True:
            start = time.perf_counter()

            for _ in range(EVENTS_PER_SECOND):
                event = create_event()
                producer.produce(
                    TOPIC,
                    key=event["junction_id"].encode('utf-8'),
                    value=json.dumps(event).encode('utf-8'),
                    callback=delivery_report
                )

            producer.poll(0)

            elapsed = time.perf_counter() - start

            if elapsed < interval:
                time.sleep(interval - elapsed)
                
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        print("Flushing messages...")
        producer.flush()


if __name__ == "__main__":
    main()
