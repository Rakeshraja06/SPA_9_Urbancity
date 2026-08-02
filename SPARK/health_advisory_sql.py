#!/usr/bin/env python3
"""
UrbanPulse — Spark Streaming SQL: Health Advisory Generator
============================================================
(a) Computes 10-minute rolling average AQI per zone
(b) Joins with static zone_profile table (name, population, schools, hospitals)
(c) Filters rolling_avg_aqi > 150 (Unhealthy)
(d) Writes to urbanpulse.health_advisories  |  output mode: Update

Run with Spark:
  spark-submit --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    health_advisory_sql.py

Run in simulation mode (no Spark):
  python3 health_advisory_sql.py --simulate
"""

import sys, os, json, time, logging, shutil
from collections import defaultdict
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("HealthAdvisorySQL")

try:
    from pyspark.sql import SparkSession
    if shutil.which("java") is None:
        log.warning("Java runtime not found. PySpark requires Java. Falling back to simulation mode.")
        SPARK_AVAILABLE = False
    else:
        SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

ADVISORY_TOPIC = "urbanpulse.health_advisories"
AQI_THRESHOLD  = 150
ZONE_PROFILE_CSV = os.path.join(os.path.dirname(__file__), "zone_profile.csv")


# ══════════════════════════════════════════════════════════════
#  SPARK MODE (production path)
# ══════════════════════════════════════════════════════════════
def run_spark():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        from_json, col, window, avg as spark_avg,
        to_json, struct, lit, current_timestamp
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType,
        DoubleType, IntegerType, TimestampType
    )

    spark = (
        SparkSession.builder
        .appName("UrbanPulse-HealthAdvisory")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Schema of air_quality events ──
    schema = StructType([
        StructField("sensor_id", StringType(),    True),
        StructField("zone",      StringType(),    True),
        StructField("pm25",      DoubleType(),    True),
        StructField("pm10",      DoubleType(),    True),
        StructField("no2",       DoubleType(),    True),
        StructField("aqi",       IntegerType(),   True),
        StructField("timestamp", TimestampType(), True),
    ])

    # ── Load static zone_profile as in-memory table ──
    zone_df = spark.read.csv(ZONE_PROFILE_CSV, header=True, inferSchema=True)
    zone_df.createOrReplaceTempView("zone_profile")
    log.info("Zone profile loaded — %d zones", zone_df.count())

    # ── Read streaming AQI from Kafka ──
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("subscribe", "urbanpulse.air_quality")
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw.select(from_json(col("value").cast("string"), schema).alias("d"))
        .select("d.*")
        .filter(col("aqi").isNotNull())            # skip null AQI (faulty sensors)
        .withWatermark("timestamp", "10 minutes")  # 10-min allowed lateness
    )

    # Register as temp view for SQL
    events.createOrReplaceTempView("aqi_stream")

    # ── Streaming SQL: 10-min rolling avg AQI per zone ──
    # Note: Spark Streaming SQL uses the same window() function under the hood
    rolling_avg = spark.sql("""
        SELECT
            zone,
            window(timestamp, '10 minutes').start  AS window_start,
            window(timestamp, '10 minutes').end    AS window_end,
            AVG(aqi)                               AS rolling_avg_aqi,
            COUNT(*)                               AS sensor_readings
        FROM aqi_stream
        GROUP BY zone, window(timestamp, '10 minutes')
    """)

    # ── Join with static zone_profile ──
    enriched = (
        rolling_avg
        .join(zone_df, on="zone", how="left")
        .filter(col("rolling_avg_aqi") > AQI_THRESHOLD)   # only Unhealthy+
        .select(
            col("zone"),
            col("zone_name"),
            col("window_start"),
            col("window_end"),
            col("rolling_avg_aqi"),
            col("population"),
            col("num_schools"),
            col("num_hospitals"),
            col("sensor_readings"),
        )
    )

    # ── Write to Kafka — Update output mode ──
    query = (
        enriched
        .selectExpr("zone AS key", "to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("topic", ADVISORY_TOPIC)
        .option("checkpointLocation", "/tmp/checkpoint/health_advisory")
        .outputMode("update")   # ← assignment requirement
        .trigger(processingTime="5 seconds")
        .start()
    )

    print(f"Health Advisory SQL streaming started.")
    print(f"  → Kafka: {ADVISORY_TOPIC}")
    print(f"  Filter: rolling_avg_aqi > {AQI_THRESHOLD} (Unhealthy)")
    print(f"  Output mode: Update (5s trigger for demo)")
    query.awaitTermination()


# ══════════════════════════════════════════════════════════════
#  SIMULATION MODE — pure Python (no Spark required)
# ══════════════════════════════════════════════════════════════
def run_simulate():
    import csv
    from confluent_kafka import Consumer, Producer, KafkaError

    print("=" * 60)
    print("  Health Advisory SQL — SIMULATION MODE")
    print(f"  Window: 10-min rolling avg | Filter: AQI > {AQI_THRESHOLD}")
    print(f"  Output mode: Update (5s fast demo emit)")
    print("=" * 60)

    # Load zone_profile (static table)
    zone_profile = {}
    try:
        with open(ZONE_PROFILE_CSV) as f:
            for row in csv.DictReader(f):
                zone_profile[row["zone"]] = row
        log.info("Zone profile: %d zones loaded", len(zone_profile))
    except FileNotFoundError:
        log.error("zone_profile.csv not found — run from SPARK/ directory")
        sys.exit(1)

    consumer = Consumer({
        "bootstrap.servers":  "localhost:29092",
        "group.id":           "health-advisory-sql-sim",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    })
    producer = Producer({
        "bootstrap.servers": "localhost:29092",
        "client.id":         "health-advisory-producer",
    })
    consumer.subscribe(["urbanpulse.air_quality"])

    # Rolling 10-min window state: window_key → zone → {aqi_sum, count}
    WINDOW_SEC = 600   # 10 minutes
    window_state: dict = defaultdict(lambda: defaultdict(lambda: {"aqi_sum": 0.0, "count": 0}))
    max_ts = 0.0
    last_emit = time.time()
    EMIT_INTERVAL = 5   # emit updates every 5s for fast live demo

    def window_key(ts: float) -> str:
        bucket = int(ts // WINDOW_SEC) * WINDOW_SEC
        return datetime.fromtimestamp(bucket, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M")

    def emit_advisories():
        for wk, zones in list(window_state.items()):
            for zone, data in list(zones.items()):
                if data["count"] == 0:
                    continue
                avg_aqi = data["aqi_sum"] / data["count"]
                if avg_aqi <= AQI_THRESHOLD:
                    continue   # not Unhealthy — filter out

                zp = zone_profile.get(zone, {})
                advisory = {
                    "zone":            zone,
                    "zone_name":       zp.get("zone_name", zone),
                    "window_start":    wk,
                    "rolling_avg_aqi": round(avg_aqi, 1),
                    "population":      zp.get("population", "N/A"),
                    "num_schools":     zp.get("num_schools", "N/A"),
                    "num_hospitals":   zp.get("num_hospitals", "N/A"),
                    "sensor_readings": data["count"],
                    "advisory_level":  "HAZARDOUS" if avg_aqi > 300 else "UNHEALTHY",
                    "advisory_text": (
                        f"Air quality in {zp.get('zone_name', zone)} is {('HAZARDOUS' if avg_aqi > 300 else 'UNHEALTHY')}. "
                        f"Rolling 10-min AQI = {avg_aqi:.0f}. "
                        f"{zp.get('num_schools','?')} schools and "
                        f"{zp.get('num_hospitals','?')} hospitals affected. "
                        f"Population at risk: {int(zp.get('population',0)):,}."
                    ),
                    "output_mode":     "UPDATE",   # ← matches Spark Update mode
                    "emitted_at":      datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                }
                log.warning("🏥 ADVISORY [%s] zone=%s avg_aqi=%.1f pop=%s",
                            advisory["advisory_level"], zone, avg_aqi,
                            zp.get("population", "?"))
                producer.produce(
                    ADVISORY_TOPIC,
                    key=zone.encode(),
                    value=json.dumps(advisory).encode(),
                )
        producer.poll(0)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if time.time() - last_emit > EMIT_INTERVAL:
                emit_advisories()
                last_emit = time.time()

            if msg is None:
                continue
            if msg.error():
                continue

            try:
                event = json.loads(msg.value())
            except Exception:
                continue

            aqi = event.get("aqi")
            if aqi is None:
                continue   # skip null AQI (same as Spark .filter(col("aqi").isNotNull()))

            zone = event.get("zone", "Unknown")
            try:
                ts = datetime.fromisoformat(
                    event["timestamp"].replace("Z", "+00:00")
                ).timestamp()
            except Exception:
                continue

            max_ts = max(max_ts, ts)
            wk = window_key(ts)
            window_state[wk][zone]["aqi_sum"] += aqi
            window_state[wk][zone]["count"]   += 1

    except KeyboardInterrupt:
        log.info("Stopped. Emitting final advisories...")
        emit_advisories()
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    if "--simulate" in sys.argv or not SPARK_AVAILABLE:
        run_simulate()
    else:
        try:
            run_spark()
        except Exception as e:
            log.warning("PySpark failed to initialize (likely missing Java JRE).")
            log.warning(f"Error: {e}")
            log.info("Falling back to pure Python simulation mode...")
            run_simulate()
