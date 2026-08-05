#!/usr/bin/env python3
"""
UrbanPulse — Spark Configuration & Optimization Settings
=========================================================
Centralized configuration for all Spark streaming jobs.
Provides optimized settings for both local development and production.

Usage:
    from spark_config import get_spark_session
    
    spark = get_spark_session("MyApp")
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str, mode: str = "local") -> SparkSession:
    """
    Create an optimized Spark session for UrbanPulse streaming jobs.
    
    Args:
        app_name: Name of the Spark application
        mode: "local" for development, "cluster" for production
    
    Returns:
        Configured SparkSession instance
    """
    builder = SparkSession.builder.appName(f"UrbanPulse-{app_name}")
    
    if mode == "local":
        builder = builder.master("local[*]")
        # Local development optimizations
        builder = (
            builder
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.streaming.kafka.maxRatePerPartition", "100")
            .config("spark.sql.streaming.metricsEnabled", "true")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        )
    else:
        # Production cluster optimizations
        builder = (
            builder
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.streaming.kafka.maxRatePerPartition", "1000")
            .config("spark.sql.streaming.metricsEnabled", "true")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
        )
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


# Kafka connection settings
KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:29092",
    "topics": {
        "smart_meters": "urbanpulse.smart_meters",
        "air_quality": "urbanpulse.air_quality",
        "bus_gps": "urbanpulse.bus_gps",
        "traffic_incidents": "urbanpulse.traffic_incidents",
        "ward_energy_summary": "urbanpulse.ward_energy_summary",
        "health_advisories": "urbanpulse.health_advisories",
    }
}

# Data quality thresholds
DATA_QUALITY = {
    "energy": {
        "voltage_min": 200,
        "voltage_max": 250,
        "power_factor_min": 0,
        "power_factor_max": 1,
        "kwh_min": 0,
    },
    "air_quality": {
        "aqi_min": 0,
        "aqi_max": 500,
        "pm25_min": 0,
        "pm10_min": 0,
        "no2_min": 0,
    },
    "traffic": {
        "speed_min": 0,
        "speed_max": 200,  # km/h
    }
}

# Watermark settings (in minutes)
WATERMARKS = {
    "smart_meters": 45,      # 45 minutes for energy data
    "air_quality": 10,       # 10 minutes for air quality
    "bus_gps": 5,            # 5 minutes for GPS data
    "traffic_incidents": 15, # 15 minutes for traffic incidents
}

# Window sizes (in minutes)
WINDOWS = {
    "energy_ward_summary": 15,     # 15-min tumbling window
    "health_advisory": 10,         # 10-min rolling average
    "traffic_congestion": 5,       # 5-min sliding window
}

# Checkpoint locations
CHECKPOINTS = {
    "ward_energy_kafka": "/tmp/checkpoint/ward_energy_kafka",
    "ward_energy_parquet": "/tmp/checkpoint/ward_energy_parquet",
    "health_advisory": "/tmp/checkpoint/health_advisory",
    "traffic_analysis": "/tmp/checkpoint/traffic_analysis",
}

# Output paths
OUTPUT_PATHS = {
    "ward_energy": "output/ward_energy",
    "health_advisories": "output/health_advisories",
    "traffic_analytics": "output/traffic_analytics",
}


if __name__ == "__main__":
    print("UrbanPulse Spark Configuration")
    print("=" * 60)
    print(f"\nKafka Topics:")
    for key, topic in KAFKA_CONFIG["topics"].items():
        print(f"  - {key:20s} → {topic}")
    
    print(f"\nWatermark Settings:")
    for source, minutes in WATERMARKS.items():
        print(f"  - {source:20s} → {minutes} minutes")
    
    print(f"\nWindow Sizes:")
    for window, minutes in WINDOWS.items():
        print(f"  - {window:25s} → {minutes} minutes")
    
    print(f"\nData Quality Thresholds:")
    for category, thresholds in DATA_QUALITY.items():
        print(f"  {category}:")
        for key, value in thresholds.items():
            print(f"    - {key:20s} → {value}")
