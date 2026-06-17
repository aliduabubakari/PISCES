"""
multi_asset_job.py — Multi-Asset Dagster Pipeline with Partitions
==================================================================

A more complex Dagster pipeline with multiple asset groups, partitioned
data, and dynamic output. Demonstrates fan-out patterns in Dagster's
software-defined asset model.

Expected eco-design profile:
  - LARGE_FAN_OUT (multiple downstream assets from single source)
  - Deep dependency chain across asset groups
"""

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetOut,
    DailyPartitionsDefinition,
    Definitions,
    MetadataValue,
    Output,
    asset,
    define_asset_job,
    multi_asset,
)

daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


@asset(
    description="Raw sensor readings from IoT devices",
    partitions_def=daily_partitions,
    group_name="ingestion",
    metadata={"source": "iot_gateway", "format": "json"},
)
def raw_sensor_readings(context: AssetExecutionContext) -> list[dict]:
    """Simulate IoT sensor data ingestion."""
    import random
    context.log.info(f"Fetching sensor data for partition: {context.partition_key}")
    devices = [f"device_{i:03d}" for i in range(20)]
    readings = []
    for device in devices:
        readings.append({
            "device_id": device,
            "temperature": round(random.uniform(18.0, 30.0), 2),
            "humidity": round(random.uniform(30.0, 80.0), 2),
            "co2_ppm": random.randint(400, 1200),
            "battery_pct": random.randint(10, 100),
        })
    context.add_output_metadata({
        "device_count": len(devices),
        "reading_count": len(readings),
        "partition": context.partition_key,
    })
    return readings


@asset(
    description="Cleaned and validated sensor readings",
    group_name="transformation",
    ins={"readings": AssetIn("raw_sensor_readings")},
)
def cleaned_sensor_data(
    context: AssetExecutionContext, readings: list[dict]
) -> list[dict]:
    """Clean and validate raw sensor readings."""
    valid = []
    outliers = 0

    for r in readings:
        # Flag outliers
        if r["temperature"] < 0 or r["temperature"] > 50:
            outliers += 1
            continue
        if r["co2_ppm"] < 300 or r["co2_ppm"] > 2000:
            outliers += 1
            continue
        valid.append({
            **r,
            "quality": "good" if r["battery_pct"] > 20 else "low_battery",
            "processed_at": context.partition_key,
        })

    context.log.info(f"Cleaned: {len(valid)} valid, {outliers} outliers removed")
    return valid


@asset(
    description="Temperature analytics from sensor data",
    group_name="analytics",
    ins={"data": AssetIn("cleaned_sensor_data")},
)
def temperature_analytics(
    context: AssetExecutionContext, data: list[dict]
) -> dict:
    """Compute temperature-specific analytics."""
    import statistics
    temps = [r["temperature"] for r in data]
    analytics = {
        "avg_temp": round(statistics.mean(temps), 2),
        "min_temp": min(temps),
        "max_temp": max(temps),
        "stdev_temp": round(statistics.stdev(temps) if len(temps) > 1 else 0, 2),
        "reading_count": len(temps),
    }
    context.log.info(f"Temperature analytics: {analytics}")
    return analytics


@asset(
    description="CO2 emissions analytics from sensor data",
    group_name="analytics",
    ins={"data": AssetIn("cleaned_sensor_data")},
)
def co2_analytics(
    context: AssetExecutionContext, data: list[dict]
) -> dict:
    """Compute CO2-specific analytics."""
    import statistics
    co2_values = [r["co2_ppm"] for r in data]
    analytics = {
        "avg_co2_ppm": round(statistics.mean(co2_values), 1),
        "min_co2_ppm": min(co2_values),
        "max_co2_ppm": max(co2_values),
        "quality_distribution": {
            "good": sum(1 for r in data if r["quality"] == "good"),
            "low_battery": sum(1 for r in data if r["quality"] == "low_battery"),
        },
    }
    context.log.info(f"CO2 analytics: {analytics}")
    return analytics


@asset(
    description="Combined environmental report",
    group_name="delivery",
    ins={
        "temp": AssetIn("temperature_analytics"),
        "co2": AssetIn("co2_analytics"),
    },
)
def environmental_report(
    context: AssetExecutionContext,
    temp: dict,
    co2: dict,
) -> dict:
    """Generate a combined environmental health report."""
    report = {
        "timestamp": context.partition_key,
        "temperature": temp,
        "co2": co2,
        "overall_health": (
            "good"
            if temp["avg_temp"] < 28 and co2["avg_co2_ppm"] < 1000
            else "needs_attention"
        ),
        "total_devices": temp["reading_count"],
    }
    context.log.info(f"Environmental report: overall_health={report['overall_health']}")
    context.add_output_metadata({
        "overall_health": report["overall_health"],
        "total_devices": report["total_devices"],
    })
    return report


# Define the job
sensor_analytics_job = define_asset_job(
    name="sensor_analytics_job",
    selection=[
        raw_sensor_readings,
        cleaned_sensor_data,
        temperature_analytics,
        co2_analytics,
        environmental_report,
    ],
)

defs = Definitions(
    assets=[
        raw_sensor_readings,
        cleaned_sensor_data,
        temperature_analytics,
        co2_analytics,
        environmental_report,
    ],
    jobs=[sensor_analytics_job],
)
