#!/usr/bin/env python3
"""
UrbanPulse — Dead-Letter Queue (DLQ) Validator
================================================
Reads ALL four UrbanPulse streams and validates each message.
Invalid messages → urbanpulse.dlq  with an error_reason field.
Valid messages   → consumed and counted (no further action for demo).

Validation rules:
  air_quality  : aqi must not be None; aqi in [0, 500]; pm25 >= 0; pm10 >= 0
  bus_gps      : lat in [12.88, 13.12]; lon in [77.48, 77.75]; speed_kmh >= 0
  traffic_sig  : avg_wait_sec >= 0; vehicle_count >= 0
  smart_meters : voltage in [200, 260]; power_factor in [0.0, 1.0]

Run:
  python3 dlq_validator.py

After 5 minutes it prints an error-type distribution report, then continues.
Press Ctrl-C anytime to see the report immediately.
"""

import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("DLQValidator")

BOOTSTRAP  = "localhost:29092"
DLQ_TOPIC  = "urbanpulse.dlq"
REPORT_INTERVAL = 300   # print report every 5 minutes

TOPICS = [
    "urbanpulse.air_quality",
    "urbanpulse.bus_gps",
    "urbanpulse.traffic_signals",
    "urbanpulse.smart_meters",
]

# Bengaluru bounding box (same as producer)
LAT_MIN, LAT_MAX = 12.88, 13.12
LON_MIN, LON_MAX = 77.48, 77.75


# ── Validation functions ──

def validate_air_quality(msg: dict) -> list[str]:
    errors = []
    aqi = msg.get("aqi")
    if aqi is None:
        errors.append("null_aqi")
    elif not (0 <= aqi <= 500):
        errors.append(f"aqi_out_of_range:{aqi}")
    if (msg.get("pm25") or 0) < 0:
        errors.append("negative_pm25")
    if (msg.get("pm10") or 0) < 0:
        errors.append("negative_pm10")
    return errors


def validate_bus_gps(msg: dict) -> list[str]:
    errors = []
    lat = msg.get("lat", 0)
    lon = msg.get("lon", 0)
    spd = msg.get("speed_kmh", 0)
    if not (LAT_MIN <= lat <= LAT_MAX):
        errors.append(f"impossible_lat:{lat}")
    if not (LON_MIN <= lon <= LON_MAX):
        errors.append(f"impossible_lon:{lon}")
    if spd < 0:
        errors.append(f"negative_speed:{spd}")
    if spd > 120:
        errors.append(f"speed_too_high:{spd}")
    return errors


def validate_traffic_signals(msg: dict) -> list[str]:
    errors = []
    if (msg.get("avg_wait_sec") or 0) < 0:
        errors.append("negative_wait_sec")
    if (msg.get("vehicle_count") or 0) < 0:
        errors.append("negative_vehicle_count")
    if msg.get("junction_id") is None:
        errors.append("null_junction_id")
    return errors


def validate_smart_meters(msg: dict) -> list[str]:
    errors = []
    v  = msg.get("voltage", 230)
    pf = msg.get("power_factor", 1.0)
    if not (200 <= v <= 260):
        errors.append(f"voltage_out_of_range:{v}")
    if not (0.0 <= pf <= 1.0):
        errors.append(f"power_factor_invalid:{pf}")
    if msg.get("meter_id") is None:
        errors.append("null_meter_id")
    return errors


VALIDATORS = {
    "urbanpulse.air_quality":    validate_air_quality,
    "urbanpulse.bus_gps":        validate_bus_gps,
    "urbanpulse.traffic_signals": validate_traffic_signals,
    "urbanpulse.smart_meters":   validate_smart_meters,
}


def send_to_dlq(producer: Producer, original_topic: str,
                original_msg: dict, errors: list[str]) -> None:
    """Route a failed message to the DLQ with metadata."""
    dlq_payload = {
        "original_topic": original_topic,
        "error_reason":   errors[0],        # primary reason
        "all_errors":     errors,
        "original_data":  original_msg,
        "dlq_timestamp":  datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }
    producer.produce(
        DLQ_TOPIC,
        value=json.dumps(dlq_payload).encode(),
        key=original_topic.encode(),
    )
    producer.poll(0)


def print_report(stats: dict, total: dict, start_time: float) -> None:
    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"  DLQ Error Distribution Report — {elapsed/60:.1f} min window")
    print("=" * 60)
    grand_dlq   = 0
    grand_valid = 0
    for topic in TOPICS:
        short = topic.split(".")[-1]
        dlq   = stats[topic]
        valid = total[topic] - sum(dlq.values())
        grand_dlq   += sum(dlq.values())
        grand_valid += valid
        print(f"\n  [{short}]  total={total[topic]}  valid={valid}  dlq={sum(dlq.values())}")
        for err_type, cnt in sorted(dlq.items(), key=lambda x: -x[1]):
            print(f"    ├─ {err_type:<35s} : {cnt}")
    print(f"\n  TOTAL  valid={grand_valid}  dlq={grand_dlq}  "
          f"dlq_rate={grand_dlq/(grand_valid+grand_dlq)*100:.1f}%")
    print("=" * 60 + "\n")


def main():
    # Consumer
    c_conf = {
        "bootstrap.servers":  BOOTSTRAP,
        "group.id":           "dlq-validator",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(c_conf)
    consumer.subscribe(TOPICS)

    # DLQ Producer
    p_conf = {
        "bootstrap.servers": BOOTSTRAP,
        "client.id":         "dlq-producer",
        "acks":              "1",
    }
    producer = Producer(p_conf)

    # Counters
    error_stats = {t: defaultdict(int) for t in TOPICS}
    total_msgs  = {t: 0 for t in TOPICS}
    start_time  = time.time()
    last_report = start_time

    log.info("DLQ Validator started — monitoring %d topics", len(TOPICS))
    log.info("Report every %d seconds. Press Ctrl-C for immediate report.", REPORT_INTERVAL)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            # Periodic report
            if time.time() - last_report >= REPORT_INTERVAL:
                print_report(error_stats, total_msgs, start_time)
                last_report = time.time()

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            topic = msg.topic()
            total_msgs[topic] += 1

            try:
                data = json.loads(msg.value())
            except (json.JSONDecodeError, Exception) as e:
                error_stats[topic]["json_parse_error"] += 1
                send_to_dlq(producer, topic, {}, [f"json_parse_error:{e}"])
                continue

            errors = VALIDATORS[topic](data)
            if errors:
                for e in errors:
                    error_stats[topic][e] += 1
                send_to_dlq(producer, topic, data, errors)

    except KeyboardInterrupt:
        log.info("Interrupted — printing final report...")
        print_report(error_stats, total_msgs, start_time)
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
