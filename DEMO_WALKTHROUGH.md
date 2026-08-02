# UrbanPulse — Demo Walkthrough & Video Script

This document is your step-by-step guide for recording the end-to-end video walkthrough for the DSE ZG556 assignment. It includes the exact commands to run and a suggested voiceover script for each step.

## 🛠 Prerequisites & Setup (Do this before recording)

1. **Install Dependencies**:
   Ensure you have the required Python packages installed.
   ```bash
   # Make sure you are in the project root
   cd /Users/rakesh.r/Documents/SPA_9
   
   # Activate the virtual environment
   source venv/bin/activate
   
   # Install dependencies
   pip install -r requirements.txt
   ```

2. **Clean State**:
   Make sure Docker is running, and you have a clean Kafka cluster.
   ```bash
   cd /Users/rakesh.r/Documents/SPA_9/KAFKA
   docker-compose down -v  # Cleans up any old data
   docker-compose up -d
   sleep 15                # Wait for brokers to start
   ./create_topics.sh      # Create all required topics
   ```

---

## 🎬 Video Recording Steps

### Part 1: Architecture (Task A)

**Action**: Open `Architecture/urbanpulse_final_architecture.html` in your browser.

**Voiceover**:
> "Welcome to the UrbanPulse platform demo. Let's start with the architecture. We have four real-time data sources feeding into a 3-broker Kafka cluster. We use a Lambda hybrid architecture: Flink handles the speed layer for sub-2-minute incident detection, while Spark handles the batch layer for 15-minute ward aggregations. Storage is polyglot, including InfluxDB for time-series and Parquet for batch reports, feeding into our serving layer dashboards."

**Action**: Switch to `Architecture/urbanpulse_arch_evaluation.html`.

**Voiceover**:
> "We chose Lambda over a pure Kappa architecture primarily due to the government reporting mandate. While Kappa is simpler operationally, Lambda provides deterministic, auditable Parquet files for councillor reports, and ensures that heavy historical reprocessing doesn't starve the real-time alerting pipeline on our on-premise servers."

**Action**: Switch to `Architecture/urbanpulse_readiness_checklist.html`.

**Voiceover**:
> "Our architecture is fully compliant with the government readiness checklist, ensuring data sovereignty by keeping all data on-premise, using 100% open-source software, and achieving zero RPO with Kafka replication factor 3."

---

### Part 2: Kafka Ingestion & Producers (Task B)

**Action**: Open your terminal. Split it into multiple panes if possible. Let's start the producers.

```bash
# In each new terminal pane, make sure to activate the virtual environment FIRST:
# source venv/bin/activate

# Terminal 1
python3 PRODUCER/air_quality_producer.py

# Terminal 2
python3 PRODUCER/bus_gps_producer.py

# Terminal 3
python3 PRODUCER/traffic_signals_producer.py

# Terminal 4
python3 PRODUCER/smart_meter_producer.py
```

**Voiceover**:
> "Now I'm starting our four producers. The air quality producer implements at-least-once semantics with idempotence enabled and exponential backoff retries. It also intentionally injects null AQI values for 5% of events to simulate sensor faults, which it gracefully logs without crashing. The bus GPS producer keys its messages by `route_id`, guaranteeing strict ordering per route across Kafka partitions."

---

### Part 3: Priority Consumer & DLQ (Task B continued)

**Action**: Demonstrate the Priority Consumer.
```bash
# Remember to run `source venv/bin/activate` if opening a new terminal!

# Terminal 5 (High Priority)
python3 CONSUMER/traffic_priority_consumer.py high

# Terminal 6 (Standard Priority - will fall behind)
python3 CONSUMER/traffic_priority_consumer.py standard

# Terminal 7 (Watch Lag)
watch -n 2 "docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group HIGH_PRIORITY_GROUP"
```

**Voiceover**:
> "To demonstrate priority consumption on the traffic signals topic, I have two consumer groups. The HIGH_PRIORITY group feeds our adaptive signal controller and processes messages instantly. The STANDARD_PRIORITY group simulates a heavy analytics dashboard and is artificially slowed down. As we can see in the lag monitor, the HIGH_PRIORITY group maintains near-zero lag, completely unaffected by the slowdown in the standard group, because Kafka maintains independent consumer offsets."

**Action**: Stop the priority consumers (`Ctrl+C`). Run the DLQ Validator.
```bash
python3 KAFKA/dlq_validator.py
```
*(Wait a few seconds, then press `Ctrl+C` to force the report to print).*

**Voiceover**:
> "Next is our Dead-Letter Queue validator. It subscribes to all streams, applying validation rules like ensuring coordinates are within city limits and AQI is not null. Invalid messages are routed to the `urbanpulse.dlq` topic. Here is the 5-minute error distribution report, showing how many messages were rejected and why."

---

### Part 4: Kafka Streams Join (Task B continued)

**Action**: Run the Bus GPS enrichment.
```bash
python3 KAFKA/kafka_streams_bus_join.py
```
*(Let it run for a few seconds to show the joined output, then stop).*

**Voiceover**:
> "For our ETA service, we use a Kafka Streams-style KTable join. We load the static `route_schedule.csv` into memory and join it with the live `bus_gps` stream on `route_id`. The output enriches the raw GPS coordinates with the route name, terminal, and scheduled arrival time."

---

### Part 5: Flink Incident Detection (Task C)

**Action**: Run the Flink Alerts simulation.
```bash
# Make sure you are in the project root directory /Users/rakesh.r/Documents/SPA_9
python3 FLINK/urbanpulse_alerts.py
```
*(Optional: Open another terminal and inject an AQI > 300 to trigger an alert immediately).*
```bash
docker exec -it kafka1 kafka-console-producer --bootstrap-server localhost:29092 --topic urbanpulse.air_quality
# Paste this:
{"sensor_id":"AQ-001","zone":"Central","pm25":130,"pm10":200,"no2":80,"aqi":325,"timestamp":"2026-07-24T07:30:00.000+00:00"}
```

**Voiceover**:
> "Moving to the processing layer, here is our Flink incident detection application. It uses keyed state and event-time watermarks. We track watermarks to allow for 30 seconds of late-arriving data. It successfully detects AQI emergencies when readings exceed 300, traffic gridlock when a junction waits over 180 seconds for 3 cycles, and bus bunching when two buses on the same route are within 200 metres for 5 minutes, routing all alerts to the incidents topic."

---

### Part 6: Spark Analytics (Task C)

**Action**: Run the Spark Ward Energy job (Simulation mode is fine for demo speed, or use spark-submit if PySpark is fully configured).
```bash
python3 SPARK/ward_energy_streaming.py --simulate
```
*(Let it flush a window).*

**Voiceover**:
> "Finally, our Spark Structured Streaming engine handles the batch reporting. For ward energy, we apply a 15-minute tumbling window on the smart meters stream, grouped by ward. We use a 45-minute watermark to handle late readings. The output calculates total kWh, average power factor, and peak voltage, dual-writing the results to a Kafka topic for live dashboards and partitioned Parquet files for historical councillor reports."

**Action**: Run the Spark Health Advisory SQL.
```bash
python3 SPARK/health_advisory_sql.py --simulate
```

**Voiceover**:
> "Our Spark Streaming SQL query computes a 10-minute rolling average AQI per zone. It joins with a static zone profile to enrich the data with population and hospital counts. We filter for averages over 150 and use Update output mode to publish health advisories to Kafka."

---

### Wrap-Up & Submission Packaging

**Voiceover**:
> "This concludes the UrbanPulse architecture walkthrough. The system successfully integrates real-time Flink alerting with robust Spark batch processing over a resilient Kafka ingestion layer, fully satisfying MetroConnect's operational and government reporting requirements. Thank you."

**Final Submission Steps (After Recording)**:
1. **Convert HTML to PDF**: Open all three HTML files in the `Architecture/` folder in your web browser. Use `Ctrl+P` (or `Cmd+P`) and select **"Save as PDF"**.
2. **Convert Markdown to PDF**: Do the same for `DOCS/flink_vs_spark_comparison.md` (you can use a markdown viewer extension or an online converter to print it to PDF).
3. **Zip the Code**: Zip the entire `SPA_9` folder (you can delete the `KAFKA/kafka-logs` or Docker volumes if the zip is too large, but keep the code files).
4. **Submit**: Upload the zipped code and the four PDF reports to eLearn along with your video link.
