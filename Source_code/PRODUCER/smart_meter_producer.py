#!/usr/bin/env python3

"""
Synthetic Smart Meter Data Generator

Topic:
    urbanpulse.smart_meters

Rate:
    ~1100 events/sec

Output:
    JSON (one event per line)

Fields:
    meter_id
    ward_id
    kwh_reading
    voltage
    power_factor
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

EVENTS_PER_SECOND = 1100
NUM_METERS = 800
NUM_WARDS = 60
TOPIC = "urbanpulse.smart_meters"

# -----------------------
# Initial Meter State
# -----------------------

meter_state = {}

for i in range(NUM_METERS):
    meter_id = f"MTR-{i:05d}"

    meter_state[meter_id] = {
        "ward_id": f"WARD-{random.randint(1, NUM_WARDS):03d}",
        "kwh_reading": round(random.uniform(500, 50000), 2),
        "voltage": random.uniform(220, 240),
        "power_factor": random.uniform(0.88, 1.00),
    }

meter_ids = list(meter_state.keys())


def update_meter(meter):
    """Simulate realistic smart meter updates."""

    # Energy consumption always increases
    meter["kwh_reading"] += random.uniform(0.01, 0.20)

    # Voltage fluctuates slightly
    meter["voltage"] += random.uniform(-1.5, 1.5)
    meter["voltage"] = max(210.0, min(meter["voltage"], 250.0))

    # Power factor varies gradually
    meter["power_factor"] += random.uniform(-0.005, 0.005)
    meter["power_factor"] = max(0.80, min(meter["power_factor"], 1.00))


def create_event():
    meter_id = random.choice(meter_ids)
    meter = meter_state[meter_id]

    update_meter(meter)

    return {
        "meter_id": meter_id,
        "ward_id": meter["ward_id"],
        "kwh_reading": round(meter["kwh_reading"], 2),
        "voltage": round(meter["voltage"], 1),
        "power_factor": round(meter["power_factor"], 3),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}', file=sys.stderr)

def main():
    conf = {
        'bootstrap.servers': 'localhost:29092',
        'client.id': 'smart-meter-producer',
        'enable.idempotence': True,
        'acks': 'all',
        'retries': 10,
        'retry.backoff.ms': 500,
        'linger.ms': 10,
        'batch.num.messages': 1000,
        'compression.type': 'lz4'
    }

    producer = Producer(conf)
    interval = 1.0

    print(f"Starting smart_meter_producer, publishing to {TOPIC} at ~{EVENTS_PER_SECOND} msgs/sec")
    
    try:
        while True:
            start = time.perf_counter()

            for _ in range(EVENTS_PER_SECOND):
                event = create_event()
                producer.produce(
                    TOPIC,
                    key=event["meter_id"].encode('utf-8'),
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
