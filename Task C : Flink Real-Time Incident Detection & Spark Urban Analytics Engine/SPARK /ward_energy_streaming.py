#!/usr/bin/env python3
"""
UrbanPulse — Spark Structured Streaming: Ward Energy Analytics
==============================================================
Source  : urbanpulse.smart_meters  (Kafka)
Window  : 15-minute TUMBLING window per ward_id
Outputs :
  1. urbanpulse.ward_energy_summary  (Kafka topic)
  2. output/ward_energy/             (Parquet, partitioned by ward_id + date)

Metrics per window per ward_id:
  - total_kwh_consumed  (sum of kwh readings)
  - avg_power_factor
  - peak_voltage

Watermark : 45 minutes  (handles late-arriving meter readings)

Run:
  spark-submit --master local[*] ward_energy_streaming.py

  Or without Spark installed (pure Python simulation mode):
  python3 ward_energy_streaming.py --simulate
"""

import sys
import os

# ─────────────────────────────────────────────────────────────────
#  SIMULATION MODE (no Spark required — for demo / development)
#  Reads from Kafka using confluent-kafka, aggregates in Python,
#  prints results and writes a sample Parquet via pandas.
# ─────────────────────────────────────────────────────────────────
import shutil
if "--simulate" in sys.argv:
    SPARK_AVAILABLE = False
else:
    try:
        from pyspark.sql import SparkSession
        if shutil.which("java") is None:
            print("WARNING: Java runtime not found. PySpark requires Java. Falling back to simulation mode.")
            SPARK_AVAILABLE = False
        else:
            SPARK_AVAILABLE = True
    except ImportError:
        SPARK_AVAILABLE = False

# ══════════════════════════════════════════════════════════════
#  SPARK MODE — PySpark Structured Streaming (production path)
# ══════════════════════════════════════════════════════════════
def run_spark():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        from_json, col, window, sum as spark_sum,
        avg, max as spark_max, to_date, lit
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType
    )

    spark = (
        SparkSession.builder
        .appName("UrbanPulse-WardEnergy")
        .master("local[*]")
        # Kafka package — include when running:
        # spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Schema of smart_meters events
    schema = StructType([
        StructField("meter_id",     StringType(),    True),
        StructField("ward_id",      StringType(),    True),
        StructField("kwh_reading",  DoubleType(),    True),
        StructField("voltage",      DoubleType(),    True),
        StructField("power_factor", DoubleType(),    True),
        StructField("timestamp",    TimestampType(), True),
    ])

    # ── Read from Kafka ──
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("subscribe", "urbanpulse.smart_meters")
        .option("startingOffsets", "latest")
        .load()
    )

    # ── Parse JSON ──
    events = (
        raw.select(from_json(col("value").cast("string"), schema).alias("d"))
        .select("d.*")
        .withWatermark("timestamp", "45 minutes")   # ← assignment requirement
    )

    # ── 15-minute tumbling window per ward_id ──
    aggregated = (
        events
        .groupBy(
            window(col("timestamp"), "15 minutes"),   # tumbling window
            col("ward_id")
        )
        .agg(
            spark_sum("kwh_reading").alias("total_kwh_consumed"),
            avg("power_factor").alias("avg_power_factor"),
            spark_max("voltage").alias("peak_voltage"),
        )
        .select(
            col("ward_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_kwh_consumed"),
            col("avg_power_factor"),
            col("peak_voltage"),
            to_date(col("window.start")).alias("date"),   # for Parquet partition
        )
    )

    # ── Output 1: Kafka topic (ward_energy_summary) ──
    # ── Output 1: Kafka topic (ward_energy_summary) ──
    kafka_query = (
        aggregated
        .selectExpr(
            "ward_id AS key",
            "to_json(struct(*)) AS value"
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("topic", "urbanpulse.ward_energy_summary")
        .option("checkpointLocation", "/tmp/checkpoint/ward_energy_kafka")
        .outputMode("update")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # ── Output 2: Parquet (partitioned by ward_id and date) ──
    parquet_query = (
        aggregated
        .writeStream
        .format("parquet")
        .option("path", "output/ward_energy")
        .option("checkpointLocation", "/tmp/checkpoint/ward_energy_parquet")
        .partitionBy("ward_id", "date")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("Spark Ward Energy Streaming started.")
    print("  → Kafka: urbanpulse.ward_energy_summary")
    print("  → Parquet: output/ward_energy/ (partitioned by ward_id, date)")
    print("  Watermark: 45 minutes | Window: 15 min tumbling (5s trigger for demo)")
    print("  Press Ctrl-C to stop.")

    spark.streams.awaitAnyTermination()


# ══════════════════════════════════════════════════════════════
#  SIMULATION MODE — pure Python + confluent-kafka + pandas
#  Produces the same aggregates without Spark installed.
# ══════════════════════════════════════════════════════════════
def run_simulate():
    import json, time, math
    from collections import defaultdict
    from datetime import datetime, timezone, timedelta
    from confluent_kafka import Consumer, Producer, KafkaError

    print("="*60)
    print("  Ward Energy — SIMULATION MODE (no Spark required)")
    print("  Window: 15-min tumbling | Output trigger: 5 seconds")
    print("="*60)

    consumer = Consumer({
        "bootstrap.servers":  "localhost:29092",
        "group.id":           "spark-ward-energy-sim",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    })
    producer = Producer({
        "bootstrap.servers": "localhost:29092",
        "client.id":         "ward-energy-producer",
    })
    consumer.subscribe(["urbanpulse.smart_meters"])

    # State: window_key → ward_id → {kwh_sum, pf_sum, peak_v, count}
    # window_key = floor(event_ts, 15min)
    windows: dict = defaultdict(lambda: defaultdict(lambda: {
        "kwh_sum": 0.0, "pf_sum": 0.0, "peak_v": 0.0, "count": 0
    }))
    max_event_ts = 0.0

    def window_key(ts: float) -> str:
        """Floor timestamp to nearest 15-min bucket."""
        bucket = int(ts // 900) * 900
        return datetime.fromtimestamp(bucket, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M")

    def emit_window(wk: str, ward_id: str, data: dict) -> None:
        """Write one window result to Kafka + Parquet."""
        total_kwh = round(data["kwh_sum"], 3)
        avg_pf    = round(data["pf_sum"] / data["count"], 4) if data["count"] else 0
        peak_v    = round(data["peak_v"], 1)
        date_str  = wk[:10]

        result = {
            "ward_id":             ward_id,
            "window_start":        wk,
            "total_kwh_consumed":  total_kwh,
            "avg_power_factor":    avg_pf,
            "peak_voltage":        peak_v,
            "date":                date_str,
        }
        print(f"  [WARD {ward_id}] window={wk} | kwh={total_kwh} "
              f"pf={avg_pf} peak_v={peak_v}V")

        # Kafka output
        producer.produce(
            "urbanpulse.ward_energy_summary",
            key=ward_id.encode(),
            value=json.dumps(result).encode(),
        )
        producer.poll(0)

        # Parquet output — generate real binary .parquet files
        out_dir = f"output/ward_energy/ward_id={ward_id}/date={date_str}"
        os.makedirs(out_dir, exist_ok=True)
        parquet_file = os.path.join(out_dir, f"window_{wk.replace(':', '-')}.parquet")

        written_parquet = False
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            table = pa.Table.from_pydict({
                "ward_id": [ward_id],
                "window_start": [wk],
                "total_kwh_consumed": [total_kwh],
                "avg_power_factor": [avg_pf],
                "peak_voltage": [peak_v],
                "date": [date_str],
            })
            pq.write_table(table, parquet_file)
            written_parquet = True
        except Exception:
            try:
                import pandas as pd
                df = pd.DataFrame([result])
                df.to_parquet(parquet_file, index=False)
                written_parquet = True
            except Exception:
                pass

        if not written_parquet:
            # Fallback to json if pyarrow/pandas parquet writer is unavailable
            json_file = os.path.join(out_dir, f"window_{wk.replace(':', '-')}.json")
            with open(json_file, "w") as f:
                json.dump(result, f)

    last_flush = time.time()
    FLUSH_INTERVAL = 5   # emit active window updates every 5s for fast live demo

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            # Periodic flush: emit active windows every 5 seconds for fast demo feedback
            now = time.time()
            if now - last_flush > FLUSH_INTERVAL:
                for wk, ward_dict in list(windows.items()):
                    for ward_id, data in list(ward_dict.items()):
                        if data["count"] > 0:
                            emit_window(wk, ward_id, data)
                last_flush = now

            if msg is None:
                continue
            if msg.error():
                continue

            try:
                event = json.loads(msg.value())
            except Exception:
                continue

            # Parse event time
            try:
                ts = datetime.fromisoformat(
                    event["timestamp"].replace("Z", "+00:00")
                ).timestamp()
            except Exception:
                continue

            max_event_ts = max(max_event_ts, ts)
            wk      = window_key(ts)
            ward_id = event.get("ward_id", "UNKNOWN")

            # Accumulate into tumbling window state
            w = windows[wk][ward_id]
            w["kwh_sum"] += event.get("kwh_reading", 0)
            w["pf_sum"]  += event.get("power_factor", 0)
            w["peak_v"]   = max(w["peak_v"], event.get("voltage", 0))
            w["count"]   += 1

    except KeyboardInterrupt:
        print("\nFlushing remaining windows...")
        for wk, wards in windows.items():
            for ward_id, data in wards.items():
                if data["count"] > 0:
                    emit_window(wk, ward_id, data)
        producer.flush()
        consumer.close()
        print("Done.")


# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if "--simulate" in sys.argv or not SPARK_AVAILABLE:
        run_simulate()
    else:
        try:
            run_spark()
        except Exception as e:
            print(f"WARNING: PySpark failed to initialize (likely missing Java JRE): {e}")
            print("Falling back to pure Python simulation mode...")
            run_simulate()
