#!/usr/bin/env python3
"""
UrbanPulse — Traffic Signal Priority Consumer Demo
===================================================
Demonstrates TWO consumer groups on urbanpulse.traffic_signals:

  HIGH_PRIORITY   — 1 consumer, all 15 partitions assigned
                    simulates the real-time signal control system
                    → stays at near-zero lag always

  STANDARD_PRIORITY — 3 consumers, shares 15 partitions (5 each)
                      simulates the analytics dashboard
                      → has artificial 0.5s sleep per message to fall behind

Run in 3 separate terminals:

  Terminal 1 (producer):
    cd SPA_9/PRODUCER && python3 traffic_signals_producer.py

  Terminal 2 (HIGH priority):
    python3 traffic_priority_consumer.py high

  Terminal 3 (STANDARD priority — falls behind):
    python3 traffic_priority_consumer.py standard

  Terminal 4 (lag monitor — shows HIGH stays near zero):
    watch -n 2 "docker exec kafka1 kafka-consumer-groups \\
      --bootstrap-server localhost:29092 --describe \\
      --group HIGH_PRIORITY_GROUP"
"""

import sys
import time
import json
import logging
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("PriorityConsumer")

BOOTSTRAP = "localhost:29092"
TOPIC     = "urbanpulse.traffic_signals"

def run_high_priority():
    """
    HIGH_PRIORITY: single consumer, processes as fast as possible.
    Feeds the adaptive signal control system.
    """
    conf = {
        "bootstrap.servers":   BOOTSTRAP,
        "group.id":            "HIGH_PRIORITY_GROUP",
        "auto.offset.reset":   "latest",        # only care about NOW
        "enable.auto.commit":  True,
    }
    c = Consumer(conf)
    c.subscribe([TOPIC])
    log.info("HIGH_PRIORITY consumer started — reading all %s partitions", TOPIC)

    count = 0
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            data = json.loads(msg.value())
            count += 1
            # Simulate fast processing (signal control action)
            if count % 500 == 0:
                log.info("[HIGH] processed %d msgs | junction=%s wait=%ss",
                         count, data["junction_id"], data["avg_wait_sec"])
            # No sleep — runs at full speed

    except KeyboardInterrupt:
        log.info("[HIGH] stopping — %d messages processed", count)
    finally:
        c.close()


def run_standard_priority(instance_id: int = 1):
    """
    STANDARD_PRIORITY: 3 consumers share partitions.
    Has artificial sleep to simulate slow analytics processing.
    Will fall behind; HIGH_PRIORITY group stays unaffected.
    """
    conf = {
        "bootstrap.servers":  BOOTSTRAP,
        "group.id":           "STANDARD_PRIORITY_GROUP",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    }
    c = Consumer(conf)
    c.subscribe([TOPIC])
    log.info("STANDARD_PRIORITY consumer #%d started — will fall behind intentionally", instance_id)

    count = 0
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            data = json.loads(msg.value())
            count += 1

            # ← Simulated slowdown: analytics processing takes 0.5s per message
            # This causes the STANDARD group to fall far behind
            time.sleep(0.5)

            if count % 10 == 0:
                log.info("[STANDARD #%d] processed %d msgs | junction=%s",
                         instance_id, count, data["junction_id"])

    except KeyboardInterrupt:
        log.info("[STANDARD #%d] stopping — %d messages processed", instance_id, count)
    finally:
        c.close()


def show_instructions():
    print("""
========================================================
  UrbanPulse — Priority Consumer Demo
========================================================

Step 1 — Start producer in another terminal:
  cd SPA_9/PRODUCER
  python3 traffic_signals_producer.py

Step 2 — Run HIGH_PRIORITY consumer (this terminal):
  python3 traffic_priority_consumer.py high

Step 3 — Run STANDARD_PRIORITY in another terminal:
  python3 traffic_priority_consumer.py standard

Step 4 — Watch lag in another terminal (run every 2s):
  watch -n 2 "docker exec kafka1 kafka-consumer-groups \\
    --bootstrap-server localhost:29092 \\
    --describe --group HIGH_PRIORITY_GROUP"

  Expected result:
    HIGH_PRIORITY_GROUP  → LAG ≈ 0  (always near-zero)
    STANDARD_PRIORITY_GROUP → LAG grows (falling behind)

  This proves HIGH_PRIORITY is unaffected by STANDARD slowdown
  because they are SEPARATE consumer groups — each maintains
  its own independent offset pointer in Kafka.
========================================================
Usage:
  python3 traffic_priority_consumer.py high
  python3 traffic_priority_consumer.py standard
""")


if __name__ == "__main__":
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "help"

    if mode == "high":
        run_high_priority()
    elif mode == "standard":
        run_standard_priority(instance_id=1)
    else:
        show_instructions()
