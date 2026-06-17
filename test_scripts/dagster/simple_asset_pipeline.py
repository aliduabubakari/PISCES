"""
simple_asset_pipeline.py — Basic Dagster Asset Pipeline
========================================================

A minimal Dagster asset-based pipeline for baseline energy benchmarking.
Uses software-defined assets with simple dependencies.

Expected eco-design profile:
  - Clean, minimal structure
  - No fan-out or heavy compute
  - Good baseline for Dagster energy measurement
"""

from dagster import (
    AssetExecutionContext,
    Definitions,
    AssetIn,
    asset,
    define_asset_job,
    DailyPartitionsDefinition,
)


@asset(
    description="Extract raw data from source system",
    group_name="ingestion",
)
def raw_weather_data(context: AssetExecutionContext) -> list[dict]:
    """Simulate extracting raw weather observations."""
    import random
    context.log.info("Extracting raw weather data...")
    stations = ["KBOS", "KORD", "KCLT"]
    data = []
    for station in stations:
        data.append({
            "station": station,
            "temperature_c": round(random.uniform(5.0, 35.0), 1),
            "humidity_pct": random.randint(30, 90),
            "wind_speed_kmh": round(random.uniform(0.0, 50.0), 1),
        })
    return data


@asset(
    description="Clean and normalize raw weather data",
    group_name="transformation",
    ins={"raw_data": AssetIn("raw_weather_data")},
)
def cleaned_weather_data(
    context: AssetExecutionContext, raw_data: list[dict]
) -> list[dict]:
    """Clean and standardize raw weather observations."""
    context.log.info(f"Cleaning {len(raw_data)} weather records...")
    cleaned = []
    for record in raw_data:
        cleaned.append({
            "station_id": record["station"],
            "temp_f": round(record["temperature_c"] * 9 / 5 + 32, 1),
            "temp_c": record["temperature_c"],
            "humidity_pct": record["humidity_pct"],
            "wind_speed_kmh": record["wind_speed_kmh"],
        })
    return cleaned


@asset(
    description="Compute daily weather statistics",
    group_name="analytics",
    ins={"cleaned": AssetIn("cleaned_weather_data")},
)
def weather_statistics(
    context: AssetExecutionContext, cleaned: list[dict]
) -> dict:
    """Compute aggregate statistics from cleaned data."""
    import statistics
    temps = [r["temp_c"] for r in cleaned]
    humidity = [r["humidity_pct"] for r in cleaned]
    wind = [r["wind_speed_kmh"] for r in cleaned]

    stats = {
        "station_count": len(cleaned),
        "avg_temp_c": round(statistics.mean(temps), 1),
        "min_temp_c": min(temps),
        "max_temp_c": max(temps),
        "avg_humidity": round(statistics.mean(humidity), 1),
        "avg_wind_kmh": round(statistics.mean(wind), 1),
    }
    context.log.info(f"Computed statistics: {stats}")
    return stats


@asset(
    description="Load processed weather data to warehouse",
    group_name="delivery",
    ins={
        "cleaned": AssetIn("cleaned_weather_data"),
        "stats": AssetIn("weather_statistics"),
    },
)
def weather_warehouse_load(
    context: AssetExecutionContext,
    cleaned: list[dict],
    stats: dict,
) -> int:
    """Simulate loading results to a data warehouse."""
    context.log.info(f"Loading {len(cleaned)} records + statistics to warehouse...")
    for record in cleaned:
        context.log.debug(f"  INSERT: {record['station_id']} "
                          f"temp={record['temp_f']}F")
    context.log.info(f"Statistics: {stats}")
    return len(cleaned)


# Define the job
weather_etl_job = define_asset_job(
    name="weather_etl_job",
    selection=[
        raw_weather_data,
        cleaned_weather_data,
        weather_statistics,
        weather_warehouse_load,
    ],
)

defs = Definitions(
    assets=[
        raw_weather_data,
        cleaned_weather_data,
        weather_statistics,
        weather_warehouse_load,
    ],
    jobs=[weather_etl_job],
)
