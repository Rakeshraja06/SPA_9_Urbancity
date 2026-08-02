#!/bin/bash
# ============================================================
#  UrbanPulse — Complete Automated End-to-End Demo Script
#  DSE ZG556 | Stream Processing and Analytics
# ============================================================
# Usage:
#   cd /Users/rakesh.r/Documents/SPA_9
#   source venv/bin/activate
#   ./run_all_demo.sh
#
# Prerequisites: Docker Desktop must be running with the
# Kafka cluster already started via:
#   cd KAFKA && docker-compose up -d && sleep 15 && ./create_topics.sh
# ============================================================

set -e

ROOT="/Users/rakesh.r/Documents/SPA_9"
cd "$ROOT"

# Activate venv if not already active
if [[ -z "$VIRTUAL_ENV" ]]; then
    source venv/bin/activate
fi

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'  # No Color

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║      UrbanPulse Smart City — End-to-End Demo             ║"
echo "║      DSE ZG556 | Stream Processing and Analytics         ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ── Pre-flight: Verify Kafka is reachable ─────────────────────
echo -e "${YELLOW}[PRE-FLIGHT] Checking Kafka broker connectivity...${NC}"
if ! docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo -e "${RED}❌ Kafka is not running! Please start it first:${NC}"
    echo "   cd KAFKA && docker-compose up -d && sleep 15 && ./create_topics.sh"
    exit 1
fi
echo -e "${GREEN}✅ Kafka cluster is reachable${NC}"
echo ""

# ── Clean old log files ───────────────────────────────────────
rm -f logs_aq_producer.txt logs_bus_producer.txt logs_traffic_producer.txt \
       logs_meter_producer.txt logs_bus_join.txt logs_flink.txt \
       logs_spark_energy.txt logs_spark_health.txt logs_dlq.txt

# ── PHASE 1: Start all 4 Producers ───────────────────────────
echo -e "${BLUE}🚀 [1/5] Starting all 4 Kafka Producers...${NC}"

python3 PRODUCER/air_quality_producer.py     > logs_aq_producer.txt 2>&1 &
P1=$!
echo "  ✓ Air Quality Producer (PID $P1) → urbanpulse.air_quality"

python3 PRODUCER/bus_gps_producer.py         > logs_bus_producer.txt 2>&1 &
P2=$!
echo "  ✓ Bus GPS Producer (PID $P2) → urbanpulse.bus_gps"

python3 PRODUCER/traffic_signals_producer.py > logs_traffic_producer.txt 2>&1 &
P3=$!
echo "  ✓ Traffic Signals Producer (PID $P3) → urbanpulse.traffic_signals"

python3 PRODUCER/smart_meter_producer.py     > logs_meter_producer.txt 2>&1 &
P4=$!
echo "  ✓ Smart Meter Producer (PID $P4) → urbanpulse.smart_meters"

sleep 3

# ── PHASE 2: Start Kafka Streams Enrichment & DLQ ───────────
echo ""
echo -e "${BLUE}🧩 [2/5] Starting Kafka Streams (KTable Join + DLQ Validator)...${NC}"

python3 KAFKA/kafka_streams_bus_join.py  > logs_bus_join.txt 2>&1 &
P5=$!
echo "  ✓ Bus GPS Enrichment (PID $P5) → urbanpulse.bus_gps_enriched"

python3 KAFKA/dlq_validator.py           > logs_dlq.txt 2>&1 &
P6=$!
echo "  ✓ DLQ Validator (PID $P6) → urbanpulse.dlq"

sleep 3

# ── PHASE 3: Start Speed Layer (Flink simulation) ───────────
echo ""
echo -e "${BLUE}⚡ [3/5] Starting Speed Layer — Flink Incident Detection...${NC}"

python3 FLINK/urbanpulse_alerts.py > logs_flink.txt 2>&1 &
P7=$!
echo "  ✓ Flink Incident Detector (PID $P7) → urbanpulse.incidents"

sleep 3

# ── PHASE 4: Start Batch Layer (Spark simulation) ────────────
echo ""
echo -e "${BLUE}📊 [4/5] Starting Batch Layer — Spark Analytics...${NC}"

python3 SPARK/ward_energy_streaming.py --simulate > logs_spark_energy.txt 2>&1 &
P8=$!
echo "  ✓ Spark Ward Energy (PID $P8) → urbanpulse.ward_energy_summary"

python3 SPARK/health_advisory_sql.py --simulate  > logs_spark_health.txt 2>&1 &
P9=$!
echo "  ✓ Spark Health Advisory SQL (PID $P9) → urbanpulse.health_advisories"

# ── PHASE 5: Let the smart city run ─────────────────────────
echo ""
echo -e "${YELLOW}⏳ [5/5] UrbanPulse is live! Running for 30 seconds...${NC}"
echo "   (Open http://localhost:8080 to see Kafka UI)"
echo ""
for i in {1..30}; do
    echo -n "▓"
    sleep 1
done
echo ""
echo ""

# ── Graceful shutdown ────────────────────────────────────────
echo -e "${YELLOW}🛑 Shutting down all components gracefully...${NC}"
kill -SIGINT $P1 $P2 $P3 $P4 $P5 $P6 $P7 $P8 $P9 2>/dev/null
sleep 4
kill -9 $P1 $P2 $P3 $P4 $P5 $P6 $P7 $P8 $P9 2>/dev/null
wait $P1 $P2 $P3 $P4 $P5 $P6 $P7 $P8 $P9 2>/dev/null

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                  ✅  DEMO RESULTS                        ║"
echo "╚══════════════════════════════════════════════════════════╝"

# ── Show Bus GPS Enrichment output ───────────────────────────
echo ""
echo -e "${GREEN}📡 [Kafka Streams] Bus GPS Enriched Events (sample):${NC}"
ENRICHED=$(grep "km/h" logs_bus_join.txt 2>/dev/null | head -n 3)
if [[ -n "$ENRICHED" ]]; then
    echo "$ENRICHED"
else
    echo "  (No enriched events yet — bus_gps may need more time)"
fi

# ── Show Flink Alerts ─────────────────────────────────────────
echo ""
echo -e "${GREEN}🚨 [Flink] Incident Alerts Detected:${NC}"
FLINK_ALERTS=$(grep "ALERT" logs_flink.txt 2>/dev/null | head -n 5)
if [[ -n "$FLINK_ALERTS" ]]; then
    echo "$FLINK_ALERTS"
else
    echo "  (No alerts in 30s — run for longer to see gridlock/bunching events)"
fi

# ── Show Spark Health Advisories ─────────────────────────────
echo ""
echo -e "${GREEN}🏥 [Spark] Health Advisories (AQI > 150):${NC}"
ADVISORIES=$(grep "ADVISORY" logs_spark_health.txt 2>/dev/null | head -n 3)
if [[ -n "$ADVISORIES" ]]; then
    echo "$ADVISORIES"
else
    echo "  (No advisories yet — AQI averages may be below 150 threshold)"
fi

# ── Show Spark Ward Energy ────────────────────────────────────
echo ""
echo -e "${GREEN}⚡ [Spark] Ward Energy Windows (sample):${NC}"
ENERGY=$(grep "WARD" logs_spark_energy.txt 2>/dev/null | head -n 3)
if [[ -n "$ENERGY" ]]; then
    echo "$ENERGY"
else
    echo "  (Ward energy windows flush after 45-min watermark — run longer)"
fi

# ── Show DLQ Stats ───────────────────────────────────────────
echo ""
echo -e "${GREEN}📋 [DLQ] Validation Report:${NC}"
DLQ_REPORT=$(grep -A 10 "DLQ Error Distribution" logs_dlq.txt 2>/dev/null | head -n 10)
if [[ -n "$DLQ_REPORT" ]]; then
    echo "$DLQ_REPORT"
else
    echo "  (DLQ report prints after 5 min — see logs_dlq.txt)"
fi

# ── Show Air Quality Null AQI Events ─────────────────────────
echo ""
echo -e "${GREEN}💨 [Air Quality] NULL AQI sensor faults (5% simulation):${NC}"
NULL_AQI=$(grep "NULL AQI" logs_aq_producer.txt 2>/dev/null | head -n 3)
if [[ -n "$NULL_AQI" ]]; then
    echo "$NULL_AQI"
else
    echo "  (Check logs_aq_producer.txt for NULL AQI warnings)"
fi

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  All log files saved in: $ROOT/"
echo "    logs_aq_producer.txt  logs_bus_producer.txt"
echo "    logs_traffic_producer.txt  logs_meter_producer.txt"
echo "    logs_bus_join.txt  logs_dlq.txt"
echo "    logs_flink.txt  logs_spark_energy.txt  logs_spark_health.txt"
echo "══════════════════════════════════════════════════════════"
