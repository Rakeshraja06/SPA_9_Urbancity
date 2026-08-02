# 🏙️ UrbanPulse — Smart Cities Real-Time Stream Processing Platform

> **Stream Processing and Analytics (DSE ZG556) : Group 09**  
> *Situated Learning — Domain 3: UrbanPulse Smart Cities (Bengaluru Urban Telemetry Engine)*

---

## 👥 Contributors

| Name | Role / Focus Area | Contribution |
|------|------------------|--------------|
| **Rakesh R** | 2024dc04070@wilp.bits-pilani.ac.in | 100% |
| **Sumit Mondal** | 2024dc04216@wilp.bits-pilani.ac.in | 100% |
| **Rahul Dombar** | 2024dc04081@wilp.bits-pilani.ac.in | 100% |
| **Rajeshwari M** | 2024dc04277@wilp.bits-pilani.ac.in | 100% |

---

## 🎯 Problem Statement & Key Challenges

MetroConnect, a city of 4.2 million residents, is implementing **UrbanPulse**, a real-time urban operations platform under the Smart Cities Mission. Despite having extensive data streams from buses, traffic signals, air quality monitors, and smart meters, the city faces critical operational inefficiencies:

1. **Static Traffic Signal Control:** Signal timings do not adapt to real-time congestion (34 min lost idling at signals). *Target: Adaptive signal control within 90s.*
2. **Stale Bus Arrival Predictions:** ETA predictions lag by 8–12 minutes causing an 18% drop in public transit ridership. *Target: Real-time ETA updates with <60s refresh intervals.*
3. **Delayed Air Quality Alerts:** AQI breaches are only identified in T+1 reports, missing critical emergencies. *Target: Issue AQI breach alerts within 2 minutes.*

---
## 📌 Overview

**UrbanPulse** is a high-throughput, enterprise-grade **Lambda Architecture** platform designed to ingest, process, enrich, and analyze real-time urban telemetry data for smart city management in Bengaluru.

The platform handles continuous multi-source IoT data streams at a combined rate of over **3,900 messages per second**, delivering:
* **⚡ Speed Layer (Apache Flink):** Sub-second emergency detection (AQI spikes, gridlocks, bus bunching).
* **📊 Batch & Analytical Layer (Apache Spark):** 15-minute windowed ward energy aggregation & 10-minute rolling AQI health advisories.
* **🔍 Stream Enrichment (Kafka Streams):** Low-latency KTable joins enriching raw GPS telemetry with static route metadata.
* **🛡️ Reliability (DLQ Validator):** Robust error routing and payload schema validation using Dead-Letter Queue (DLQ) patterns.
* **⚡ Consumer Isolation:** Independent High-Priority vs. Standard-Priority consumer groups ensuring mission-critical actions are unaffected by analytics processing loads.

---

## 🏛️ System Architecture

```
                                  ┌─────────────────────────────┐
                                  │   IoT Data Producers        │
                                  │  • Bus GPS (2,400 msg/s)    │
                                  │  • Smart Meters (1,100/s)   │
                                  │  • Traffic Signals (380/s)  │
                                  │  • Air Quality (60 msg/s)   │
                                  └──────────────┬──────────────┘
                                                 │
                                                 ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                Apache Kafka Cluster (3 Brokers)                        │
│                                                                                        │
│  [urbanpulse.bus_gps]    [urbanpulse.traffic_signals]  [urbanpulse.air_quality]  ...   │
└──────────────┬────────────────────────┬─────────────────────────┬──────────────────────┘
               │                        │                         │
               ▼                        ▼                         ▼
┌──────────────────────────┐ ┌────────────────────┐ ┌───────────────────────────────────┐
│   Kafka Streams          │ │ Priority Consumers │ │ Speed Layer (Apache Flink)        │
│   • KTable Join with     │ │ • High-Priority    │ │ • Gridlock Detection (>120s wait) │
│     Route Schedules      │ │   (Near 0 Lag)     │ │ • AQI Hazardous Alert (>300)      │
│   • Enriched Stream      │ │ • Standard-Priority│ │ • Bus Bunching Alert (<60s gap)   │
└──────────────┬───────────┘ │   (Analytics Lag)  │ └─────────────────┬─────────────────┘
               │             └────────────────────┘                   │
               ▼                                                      ▼
┌──────────────────────────┐                               ┌───────────────────────────────────┐
│ Output:                  │                               │ Output:                           │
│ urbanpulse.bus_gps_enrich│                               │ urbanpulse.incidents              │
└──────────────────────────┘                               └───────────────────────────────────┘
                                                 │
                                                 ▼
                                  ┌─────────────────────────────┐
                                  │  Batch Layer (Apache Spark) │
                                  │  • 15-min Ward Energy Agg   │
                                  │  • 10-min Rolling AQI SQL   │
                                  └─────────────────────────────┘
```

---

## 📁 Repository Structure

```
SPA_9/
├── KAFKA/
│   ├── docker-compose.yml       # 3-Broker Kafka Cluster + Zookeeper + Schema Registry + UI
│   ├── create_topics.sh         # Topic provisioner with custom partitioning & retention
│   ├── kafka_streams_bus_join.py# KTable-Stream enrichment join engine
│   ├── dlq_validator.py         # Payload validator & Dead Letter Queue producer
│   └── route_schedule.csv       # Static route metadata reference table
│
├── PRODUCER/
│   ├── bus_gps_producer.py      # High-rate GPS stream (key = route_id)
│   ├── traffic_signals_producer.py # Junction telemetry (key = junction_id)
│   ├── air_quality_producer.py  # Air Quality sensors (5% simulated fault rate)
│   └── smart_meter_producer.py  # Electricity grid readings (365d audit retention)
│
├── CONSUMER/
│   └── traffic_priority_consumer.py # High vs Standard priority consumer group demo
│
├── FLINK/
│   └── urbanpulse_alerts.py     # Stateful Flink CEP & streaming alert engine
│
├── SPARK/
│   ├── health_advisory_sql.py   # PySpark 10-min rolling window AQI SQL advisories
│   ├── ward_energy_streaming.py # PySpark 15-min sliding window energy aggregation
│   └── zone_profile.csv         # Static ward & zone metadata
│
├── DOCS/
│   ├── STEP_BY_STEP_DEMO.md     # Complete presentation & demo script
│   └── STUDY_GUIDE.md           # Technical viva & Q&A guide
│
├── run_all_demo.sh              # One-command full system demo script
├── requirements.txt             # Python dependencies
└── README.md                    # Project documentation
```

---

## ⚙️ Prerequisites & Setup

### Requirements
* **Python**: `3.9` or higher
* **Docker & Docker Compose**: For local Kafka cluster
* **Java**: JDK 11 or 17 (Required for PySpark and Apache Flink)

### Environment Setup
1. **Clone the repository & enter workspace:**
   ```bash
   cd SPA_9
   ```

2. **Create and activate Python virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

---

## 🚀 Quick Start Guide

### 1. Start Infrastructure (Kafka Cluster)
Launch the 3-broker Kafka cluster, Zookeeper, and Kafka UI:
```bash
cd KAFKA
docker compose up -d
```
Verify cluster health & topic initialization:
```bash
./create_topics.sh
```
*Access **Kafka UI** at http://localhost:8080*

---

### 2. Run Data Producers
Open separate terminal tabs for each telemetry stream:

```bash
# Bus GPS Stream (2,400 msg/s)
python3 PRODUCER/bus_gps_producer.py

# Traffic Signals Stream (380 msg/s)
python3 PRODUCER/traffic_signals_producer.py

# Air Quality Stream (60 msg/s, includes 5% fault rate)
python3 PRODUCER/air_quality_producer.py

# Smart Meter Stream (1,100 msg/s)
python3 PRODUCER/smart_meter_producer.py
```

---

### 3. Run Consumers & Stream Engines

* **Kafka Streams KTable Join:**
  ```bash
  python3 KAFKA/kafka_streams_bus_join.py
  ```

* **Priority Consumer Group Isolation:**
  ```bash
  # High Priority Consumer (0 Lag)
  python3 CONSUMER/traffic_priority_consumer.py high

  # Standard Priority Consumer (Simulated Lag)
  python3 CONSUMER/traffic_priority_consumer.py standard
  ```

* **DLQ Validator:**
  ```bash
  python3 KAFKA/dlq_validator.py
  ```

* **Apache Flink Speed Layer:**
  ```bash
  python3 FLINK/urbanpulse_alerts.py
  ```

* **Apache Spark Analytics Layer:**
  ```bash
  python3 SPARK/health_advisory_sql.py --simulate
  python3 SPARK/ward_energy_streaming.py --simulate
  ```

---

### 4. Automated Demo Run
To run the full demonstration pipeline automatically:
```bash
./run_all_demo.sh
```

---

## 📊 Topic Specifications & Retention Matrix

| Topic Name | Partitions | Retention | Key | Purpose |
|------------|------------|-----------|-----|---------|
| `urbanpulse.bus_gps` | 40 | 24 Hours | `route_id` | Raw bus GPS telemetry stream |
| `urbanpulse.traffic_signals` | 15 | 7 Days | `junction_id` | Signal phases & queue metrics |
| `urbanpulse.air_quality` | 10 | 90 Days | `sensor_id` | Environmental sensor readings |
| `urbanpulse.smart_meters` | 20 | 365 Days | `meter_id` | DISCOM regulatory grid readings |
| `urbanpulse.incidents` | 5 | 30 Days | `incident_id` | Low-latency Flink alerts |
| `urbanpulse.dlq` | 3 | 7 Days | `original_topic` | Malformed message dead-letter queue |
| `urbanpulse.health_advisories` | 5 | 30 Days | `zone_id` | Spark SQL AQI health alerts |
| `urbanpulse.ward_energy_summary`| 10 | 30 Days | `ward_id` | 15-min window energy metrics |
| `urbanpulse.bus_gps_enriched` | 40 | 24 Hours | `route_id` | KTable joined enriched telemetry |

---

<<<<<<< HEAD
## 👥 Contributors

| Name | Role / Focus Area |
|------|------------------|
| **Rakesh R** |  2024dc04070@wilp.bits-pilani.ac.in  |
| **Sumit Mondal** | 2024dc04216@wilp.bits-pilani.ac.in  |
| **Rahul Dombar** | 2024dc04081@wilp.bits-pilani.ac.in  |
| **Rajeshwari M** | 2024dc04277@wilp.bits-pilani.ac.in |

---
=======
>>>>>>> fc18fd8b6a4a4ab8ae745a140549a4f6cc51f11f

## 📜 License & Acknowledgments

* **Course**: Stream Processing and Analytics (DSE ZG556)
* **Domain**: Domain 3 — UrbanPulse Smart Cities
* Built using Apache Kafka, Apache Flink, Apache Spark, PySpark, and Docker.
