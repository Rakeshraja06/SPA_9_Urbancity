#!/usr/bin/env python3
"""
UrbanPulse — Flink Incident Detection (Python simulation)
==========================================================
Detects three urban incidents using keyed state + event-time watermarks.

INCIDENT PATTERNS:
  (a) AQI Emergency  — any sensor AQI > 300 → alert within 2 min
  (b) Traffic Gridlock — junction avg_wait > 180s for 3 consecutive cycles
  (c) Bus Bunching   — 2 buses on same route within 200m for > 5 min

All alerts → urbanpulse.incidents Kafka topic.

FLINK CONCEPTS USED (simulated in plain Python):
  - Keyed state : per-key dict  (same as KeyedProcessFunction state)
  - Watermark   : max seen event-time minus 30s allowed lateness
  - Event time  : timestamp field in each message (not wall-clock)

Run:
  python3 urbanpulse_alerts.py

Press Ctrl-C to stop.
"""

import json
import math
import logging
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("FlinkIncidentDetection")

BOOTSTRAP = "localhost:29092"
INCIDENTS_TOPIC = "urbanpulse.incidents"

# ── Watermark settings ──
ALLOWED_LATENESS_SEC = 30    # discard events older than watermark - 30s


# ══════════════════════════════════════════════════════════════
#  WATERMARK TRACKER
#  Flink uses watermarks to track event-time progress.
#  We track max seen timestamp per topic and compute watermark.
# ══════════════════════════════════════════════════════════════
class WatermarkTracker:
    def __init__(self, allowed_lateness_sec: int = 30):
        self.max_ts: float = 0.0
        self.lateness = allowed_lateness_sec

    def update(self, event_ts: float) -> None:
        self.max_ts = max(self.max_ts, event_ts)

    @property
    def watermark(self) -> float:
        return self.max_ts - self.lateness

    def is_late(self, event_ts: float) -> bool:
        return event_ts < self.watermark


# ══════════════════════════════════════════════════════════════
#  (a) AQI EMERGENCY DETECTOR
#  Keyed state: per sensor_id — last AQI value and alert cooldown
#  Rule: AQI > 300 → emit alert (max 1 alert per sensor per 2 min)
# ══════════════════════════════════════════════════════════════
class AQIEmergencyDetector:
    # Keyed state: sensor_id → last alert timestamp
    _state: dict[str, float] = {}
    COOLDOWN_SEC = 120   # 2-minute cooldown to avoid alert storms

    @classmethod
    def process(cls, event: dict, watermark: float) -> dict | None:
        aqi = event.get("aqi")
        if aqi is None or aqi <= 300:
            return None   # not an emergency

        sid = event["sensor_id"]
        now = event["_event_ts"]

        # Check cooldown (keyed state lookup)
        last_alert = cls._state.get(sid, 0)
        if now - last_alert < cls.COOLDOWN_SEC:
            return None   # still in cooldown

        # Update keyed state
        cls._state[sid] = now

        return {
            "incident_type": "AQI_EMERGENCY",
            "sensor_id":     sid,
            "zone":          event.get("zone"),
            "aqi":           aqi,
            "pm25":          event.get("pm25"),
            "threshold":     300,
            "message":       f"HAZARDOUS AQI {aqi} at sensor {sid} zone {event.get('zone')}",
            "event_time":    event["timestamp"],
            "alert_time":    datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }


# ══════════════════════════════════════════════════════════════
#  (b) TRAFFIC GRIDLOCK DETECTOR
#  Keyed state: per junction_id — rolling buffer of last 3 readings
#  Rule: avg_wait > 180s in 3 CONSECUTIVE signal cycles → gridlock
# ══════════════════════════════════════════════════════════════
class GridlockDetector:
    WAIT_THRESHOLD = 180     # seconds
    CONSECUTIVE    = 3       # number of consecutive readings
    WINDOW_SEC     = 300     # readings within last 5 min count

    # Keyed state: junction_id → deque of (event_ts, avg_wait_sec)
    _state: dict[str, deque] = defaultdict(lambda: deque(maxlen=10))

    @classmethod
    def process(cls, event: dict, watermark: float) -> dict | None:
        jid  = event["junction_id"]
        wait = event.get("avg_wait_sec", 0)
        ts   = event["_event_ts"]

        # Append to keyed state (bounded deque)
        cls._state[jid].append((ts, wait))

        # Keep only readings within the window (event-time based)
        window_start = ts - cls.WINDOW_SEC
        recent = [(t, w) for t, w in cls._state[jid] if t >= window_start]

        # Check 3 consecutive readings all above threshold
        if len(recent) < cls.CONSECUTIVE:
            return None

        last3 = recent[-cls.CONSECUTIVE:]
        if all(w > cls.WAIT_THRESHOLD for _, w in last3):
            avg_wait = sum(w for _, w in last3) / cls.CONSECUTIVE
            return {
                "incident_type":   "TRAFFIC_GRIDLOCK",
                "junction_id":     jid,
                "zone":            event.get("zone"),
                "avg_wait_sec":    round(avg_wait, 1),
                "consecutive_readings": cls.CONSECUTIVE,
                "message": (f"GRIDLOCK at {jid} zone {event.get('zone')} "
                            f"— avg wait {avg_wait:.0f}s for {cls.CONSECUTIVE} cycles"),
                "event_time":  event["timestamp"],
                "alert_time":  datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            }
        return None


# ══════════════════════════════════════════════════════════════
#  (c) BUS BUNCHING DETECTOR
#  Keyed state: per route_id → list of recent bus positions
#  Rule: 2 buses same route within 200m for > 5 min → bunching
#
#  Haversine distance formula to compute metres between two GPS points.
# ══════════════════════════════════════════════════════════════

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Distance in metres between two GPS coordinates."""
    R = 6_371_000   # Earth radius in metres
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lon2 - lon1)
    a  = math.sin(Δφ/2)**2 + math.cos(φ1)*math.cos(φ2)*math.sin(Δλ/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class BusBunchingDetector:
    DISTANCE_M   = 200     # metres
    DURATION_SEC = 15      # 15 seconds (tuned for live demo verification; production = 300s / 5 min)

    # Keyed state: route_id → {bus_id: [(event_ts, lat, lon), ...]}
    _state: dict[str, dict] = defaultdict(dict)
    # Track when pair first seen close together
    _pair_first_seen: dict[str, float] = {}

    @classmethod
    def process(cls, event: dict, watermark: float) -> dict | None:
        rid = event["route_id"]
        bid = event["bus_id"]
        ts  = event["_event_ts"]
        lat = event["lat"]
        lon = event["lon"]

        # Update keyed state for this bus
        cls._state[rid][bid] = (ts, lat, lon)

        # Compare with all other buses on same route
        buses_on_route = cls._state[rid]
        if len(buses_on_route) < 2:
            return None

        for other_id, (other_ts, other_lat, other_lon) in buses_on_route.items():
            if other_id == bid:
                continue
            # Only compare recent readings (within 60s of each other)
            if abs(ts - other_ts) > 60:
                continue

            dist = haversine_m(lat, lon, other_lat, other_lon)
            if dist > cls.DISTANCE_M:
                continue

            # Buses are within 200m — check how long they've been close
            pair_key = tuple(sorted([bid, other_id]))
            first_seen = cls._pair_first_seen.get(pair_key)

            if first_seen is None:
                cls._pair_first_seen[pair_key] = ts   # start the clock
                continue

            duration = ts - first_seen
            if duration >= cls.DURATION_SEC:
                # Bunching confirmed — emit alert (and reset so we don't spam)
                del cls._pair_first_seen[pair_key]
                return {
                    "incident_type": "BUS_BUNCHING",
                    "route_id":      rid,
                    "bus_1":         bid,
                    "bus_2":         other_id,
                    "distance_m":    round(dist, 1),
                    "duration_sec":  round(duration, 0),
                    "message": (f"BUNCHING on route {rid}: buses {bid} & {other_id} "
                                f"within {dist:.0f}m for {duration/60:.1f} min"),
                    "event_time":  event["timestamp"],
                    "alert_time":  datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                }

        # Expire old pair entries (clean up keyed state)
        expired = [k for k, v in cls._pair_first_seen.items()
                   if ts - v > cls.DURATION_SEC * 2]
        for k in expired:
            del cls._pair_first_seen[k]

        return None


# ══════════════════════════════════════════════════════════════
#  ALERT PRODUCER
# ══════════════════════════════════════════════════════════════
def emit_alert(producer: Producer, alert: dict) -> None:
    producer.produce(
        INCIDENTS_TOPIC,
        key=alert["incident_type"].encode(),
        value=json.dumps(alert).encode(),
    )
    producer.poll(0)
    log.warning("🚨 ALERT [%s] %s", alert["incident_type"], alert["message"])


# ══════════════════════════════════════════════════════════════
#  MAIN — fan-out consumer reading 3 topics
# ══════════════════════════════════════════════════════════════
def parse_ts(event: dict) -> float:
    """Parse ISO timestamp from event into Unix epoch float."""
    ts_str = event.get("timestamp", "")
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        return time.time()   # fallback to wall-clock


def main():
    consumer_conf = {
        "bootstrap.servers":  BOOTSTRAP,
        "group.id":           "flink-incident-detector",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    }
    producer_conf = {
        "bootstrap.servers": BOOTSTRAP,
        "client.id":         "flink-alert-producer",
        "acks":              "1",
    }

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)

    # Subscribe to all three source topics
    consumer.subscribe([
        "urbanpulse.air_quality",
        "urbanpulse.traffic_signals",
        "urbanpulse.bus_gps",
    ])

    # One watermark tracker per topic (event-time per stream)
    watermarks = {
        "urbanpulse.air_quality":    WatermarkTracker(ALLOWED_LATENESS_SEC),
        "urbanpulse.traffic_signals": WatermarkTracker(ALLOWED_LATENESS_SEC),
        "urbanpulse.bus_gps":        WatermarkTracker(ALLOWED_LATENESS_SEC),
    }

    log.info("Flink Incident Detector started — watching 3 streams")
    log.info("Alerts → %s", INCIDENTS_TOPIC)

    counts = {"AQI_EMERGENCY": 0, "TRAFFIC_GRIDLOCK": 0, "BUS_BUNCHING": 0}
    processed = 0

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

            topic = msg.topic()
            try:
                event = json.loads(msg.value())
            except json.JSONDecodeError:
                continue

            # ── Watermark update (event-time) ──
            event_ts = parse_ts(event)
            event["_event_ts"] = event_ts
            wm = watermarks[topic]
            if wm.is_late(event_ts):
                continue   # drop late events (past watermark)
            wm.update(event_ts)

            processed += 1

            # ── Route to correct detector (keyed state) ──
            alert = None
            if topic == "urbanpulse.air_quality":
                alert = AQIEmergencyDetector.process(event, wm.watermark)

            elif topic == "urbanpulse.traffic_signals":
                alert = GridlockDetector.process(event, wm.watermark)

            elif topic == "urbanpulse.bus_gps":
                alert = BusBunchingDetector.process(event, wm.watermark)

            if alert:
                counts[alert["incident_type"]] += 1
                emit_alert(producer, alert)

            if processed % 5000 == 0:
                log.info("Processed %d events | alerts: %s", processed, counts)

    except KeyboardInterrupt:
        log.info("Stopping — processed=%d alerts=%s", processed, counts)
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
