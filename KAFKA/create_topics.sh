#!/bin/bash
# UrbanPulse — Topic Creation Script
# Run AFTER docker-compose up -d and brokers are healthy
#
# Usage:  chmod +x create_topics.sh && ./create_topics.sh
#
# All topics: replication-factor=3, min.insync.replicas=2 (set globally in broker config)

BROKER="localhost:29092"

echo "========================================"
echo " UrbanPulse — Creating Kafka Topics"
echo "========================================"

# ── Helper function ──
create_topic() {
  local NAME=$1
  local PARTITIONS=$2
  local RETENTION_MS=$3
  local REASON=$4

  echo ""
  echo "▸ Creating: $NAME"
  echo "  Partitions : $PARTITIONS"
  echo "  Retention  : $RETENTION_MS ms  ($REASON)"

  docker exec kafka1 kafka-topics \
    --bootstrap-server "$BROKER" \
    --create \
    --if-not-exists \
    --topic "$NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor 3 \
    --config retention.ms="$RETENTION_MS"
}

# ─────────────────────────────────────────────────────────
#  Topic 1: urbanpulse.bus_gps
#  Partitions: 40 — one per route (40 routes).
#              Keyed by route_id → all buses on same route
#              land in same partition → ordering guaranteed.
#  Retention: 24 hours (86400000 ms)
#  WHY 24h: GPS data is used for accident reconstruction
#            and ETA replay within the same operating day.
#            Older positions have no operational value.
# ─────────────────────────────────────────────────────────
create_topic "urbanpulse.bus_gps" 40 86400000 "24 h — accident replay window"

# ─────────────────────────────────────────────────────────
#  Topic 2: urbanpulse.traffic_signals
#  Partitions: 15 — 120 junctions / 8 per partition.
#              Balanced load; junction keying keeps
#              consecutive readings in order for Flink gridlock.
#  Retention: 7 days (604800000 ms)
#  WHY 7d: signal timing analysis for weekly optimisation.
# ─────────────────────────────────────────────────────────
create_topic "urbanpulse.traffic_signals" 15 604800000 "7 days — weekly signal optimisation"

# ─────────────────────────────────────────────────────────
#  Topic 3: urbanpulse.air_quality
#  Partitions: 10 — 50 sensors / 5 zones = 2 partitions/zone.
#              Parallel zone-level processing in Flink.
#  Retention: 90 days (7776000000 ms)
#  WHY 90d: CPCB pollution trend analysis; state-level
#            quarterly AQI reports to Pollution Control Board.
# ─────────────────────────────────────────────────────────
create_topic "urbanpulse.air_quality" 10 7776000000 "90 days — quarterly pollution trend"

# ─────────────────────────────────────────────────────────
#  Topic 4: urbanpulse.smart_meters
#  Partitions: 20 — 800 meters / 40 per partition.
#              Ward-level aggregation in Spark spread evenly.
#  Retention: 365 days (31536000000 ms)
#  WHY 365d: Electricity Act 2003 mandates energy audit
#             records kept for 1 year; state DISCOM submission.
# ─────────────────────────────────────────────────────────
create_topic "urbanpulse.smart_meters" 20 31536000000 "365 days — regulatory energy audit"

# ─────────────────────────────────────────────────────────
#  Topic 5: urbanpulse.incidents   (Flink alert output)
#  Partitions: 5 — low-volume alert stream
#  Retention: 30 days
# ─────────────────────────────────────────────────────────
create_topic "urbanpulse.incidents" 5 2592000000 "30 days — incident history"

# ─────────────────────────────────────────────────────────
#  Topic 6: urbanpulse.dlq   (Dead-Letter Queue)
#  Partitions: 3 — very low volume (only bad messages)
#  Retention: 7 days — enough time for ops team to investigate
# ─────────────────────────────────────────────────────────
create_topic "urbanpulse.dlq" 3 604800000 "7 days — ops investigation window"

# ─────────────────────────────────────────────────────────
#  Topic 7: urbanpulse.health_advisories   (Spark SQL output)
#  Partitions: 5
#  Retention: 30 days
# ─────────────────────────────────────────────────────────
create_topic "urbanpulse.health_advisories" 5 2592000000 "30 days — advisory history"

# ─────────────────────────────────────────────────────────
#  Topic 8: urbanpulse.ward_energy_summary   (Spark streaming output)
#  Partitions: 10 — one per 6 wards (60 wards total)
#  Retention: 30 days
# ─────────────────────────────────────────────────────────
create_topic "urbanpulse.ward_energy_summary" 10 2592000000 "30 days — dashboard feed"

# ─────────────────────────────────────────────────────────
#  Topic 9: urbanpulse.bus_gps_enriched   (Kafka Streams Join output)
#  Partitions: 40 — one per route (matching input)
#  Retention: 24 hours
# ─────────────────────────────────────────────────────────
create_topic "urbanpulse.bus_gps_enriched" 40 86400000 "24 h — enriched ETA feed"

echo ""
echo "========================================"
echo " All topics created. Listing:"
echo "========================================"
docker exec kafka1 kafka-topics --bootstrap-server "$BROKER" --list
