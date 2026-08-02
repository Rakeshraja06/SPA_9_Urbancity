# UrbanPulse - Topic Creation Script (PowerShell)
# Run AFTER docker-compose up -d and brokers are healthy
#
# Usage:  .\create_topics.ps1
#
# All topics: replication-factor=3, min.insync.replicas=2 (set globally in broker config)

$Broker = "kafka1:9092"  # INTERNAL listener - use this since kafka-topics runs INSIDE the kafka1 container via docker exec

Write-Host "========================================"
Write-Host " UrbanPulse - Creating Kafka Topics"
Write-Host "========================================"

# ── Helper function ──
function Create-Topic {
    param(
        [string]$Name,
        [int]$Partitions,
        [long]$RetentionMs,
        [string]$Reason
    )

    Write-Host ""
    Write-Host "> Creating: $Name"
    Write-Host "  Partitions : $Partitions"
    Write-Host "  Retention  : $RetentionMs ms  ($Reason)"

    docker exec kafka1 kafka-topics `
        --bootstrap-server $Broker `
        --create `
        --if-not-exists `
        --topic $Name `
        --partitions $Partitions `
        --replication-factor 3 `
        --config retention.ms=$RetentionMs
}

# ─────────────────────────────────────────────────────────
#  Topic 1: urbanpulse.bus_gps
#  Partitions: 40 - one per route (40 routes).
#              Keyed by route_id → all buses on same route
#              land in same partition → ordering guaranteed.
#  Retention: 24 hours (86400000 ms)
#  WHY 24h: GPS data is used for accident reconstruction
#            and ETA replay within the same operating day.
#            Older positions have no operational value.
# ─────────────────────────────────────────────────────────
Create-Topic -Name "urbanpulse.bus_gps" -Partitions 40 -RetentionMs 86400000 -Reason "24 h - accident replay window"

# ─────────────────────────────────────────────────────────
#  Topic 2: urbanpulse.traffic_signals
#  Partitions: 15 - 120 junctions / 8 per partition.
#              Balanced load; junction keying keeps
#              consecutive readings in order for Flink gridlock.
#  Retention: 7 days (604800000 ms)
#  WHY 7d: signal timing analysis for weekly optimisation.
# ─────────────────────────────────────────────────────────
Create-Topic -Name "urbanpulse.traffic_signals" -Partitions 15 -RetentionMs 604800000 -Reason "7 days - weekly signal optimisation"

# ─────────────────────────────────────────────────────────
#  Topic 3: urbanpulse.air_quality
#  Partitions: 10 - 50 sensors / 5 zones = 2 partitions/zone.
#              Parallel zone-level processing in Flink.
#  Retention: 90 days (7776000000 ms)
#  WHY 90d: CPCB pollution trend analysis; state-level
#            quarterly AQI reports to Pollution Control Board.
# ─────────────────────────────────────────────────────────
Create-Topic -Name "urbanpulse.air_quality" -Partitions 10 -RetentionMs 7776000000 -Reason "90 days - quarterly pollution trend"

# ─────────────────────────────────────────────────────────
#  Topic 4: urbanpulse.smart_meters
#  Partitions: 20 - 800 meters / 40 per partition.
#              Ward-level aggregation in Spark spread evenly.
#  Retention: 365 days (31536000000 ms)
#  WHY 365d: Electricity Act 2003 mandates energy audit
#             records kept for 1 year; state DISCOM submission.
# ─────────────────────────────────────────────────────────
Create-Topic -Name "urbanpulse.smart_meters" -Partitions 20 -RetentionMs 31536000000 -Reason "365 days - regulatory energy audit"

# ─────────────────────────────────────────────────────────
#  Topic 5: urbanpulse.incidents   (Flink alert output)
#  Partitions: 5 - low-volume alert stream
#  Retention: 30 days
# ─────────────────────────────────────────────────────────
Create-Topic -Name "urbanpulse.incidents" -Partitions 5 -RetentionMs 2592000000 -Reason "30 days - incident history"

# ─────────────────────────────────────────────────────────
#  Topic 6: urbanpulse.dlq   (Dead-Letter Queue)
#  Partitions: 3 - very low volume (only bad messages)
#  Retention: 7 days - enough time for ops team to investigate
# ─────────────────────────────────────────────────────────
Create-Topic -Name "urbanpulse.dlq" -Partitions 3 -RetentionMs 604800000 -Reason "7 days - ops investigation window"

# ─────────────────────────────────────────────────────────
#  Topic 7: urbanpulse.health_advisories   (Spark SQL output)
#  Partitions: 5
#  Retention: 30 days
# ─────────────────────────────────────────────────────────
Create-Topic -Name "urbanpulse.health_advisories" -Partitions 5 -RetentionMs 2592000000 -Reason "30 days - advisory history"

# ─────────────────────────────────────────────────────────
#  Topic 8: urbanpulse.ward_energy_summary   (Spark streaming output)
#  Partitions: 10 - one per 6 wards (60 wards total)
#  Retention: 30 days
# ─────────────────────────────────────────────────────────
Create-Topic -Name "urbanpulse.ward_energy_summary" -Partitions 10 -RetentionMs 2592000000 -Reason "30 days - dashboard feed"

# ─────────────────────────────────────────────────────────
#  Topic 9: urbanpulse.bus_gps_enriched   (Kafka Streams Join output)
#  Partitions: 40 - one per route (matching input)
#  Retention: 24 hours
# ─────────────────────────────────────────────────────────
Create-Topic -Name "urbanpulse.bus_gps_enriched" -Partitions 40 -RetentionMs 86400000 -Reason "24 h - enriched ETA feed"

Write-Host ""
Write-Host "========================================"
Write-Host " All topics created. Listing:"
Write-Host "========================================"
docker exec kafka1 kafka-topics --bootstrap-server $Broker --list