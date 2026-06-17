"""
simple_flow.py — Basic Prefect Flow
====================================

A minimal Prefect flow for baseline energy testing. Uses tasks with
retries and simple data passing.

Expected eco-design profile:
  - Clean sequential structure
  - Minimal resource usage
  - Good baseline for Prefect energy measurement
"""

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from typing import List, Dict


@task(
    retries=2,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=5),
)
def extract_weather_data(api_url: str = "https://api.weather.gov") -> List[Dict]:
    """Simulate extracting weather data from an external API."""
    import random

    print(f"Extracting data from {api_url}...")
    stations = ["KBOS", "KORD", "KCLT", "KSEA", "KDEN"]

    data = []
    for station in stations:
        data.append({
            "station": station,
            "temperature_c": round(random.uniform(-5.0, 40.0), 1),
            "humidity_pct": random.randint(20, 95),
            "wind_speed_ms": round(random.uniform(0.0, 30.0), 1),
            "pressure_hpa": round(random.uniform(980.0, 1040.0), 1),
        })

    print(f"Extracted {len(data)} station records")
    return data


@task(retries=1)
def transform_weather_data(raw_data: List[Dict]) -> List[Dict]:
    """Transform raw weather data with derived metrics."""
    print(f"Transforming {len(raw_data)} records...")

    transformed = []
    for record in raw_data:
        transformed.append({
            "station_id": record["station"],
            "temp_f": round(record["temperature_c"] * 9 / 5 + 32, 1),
            "temp_c": record["temperature_c"],
            "humidity_pct": record["humidity_pct"],
            "wind_speed_mph": round(record["wind_speed_ms"] * 2.237, 1),
            "pressure_inhg": round(record["pressure_hpa"] * 0.02953, 2),
            "comfort_index": round(
                0.5 * record["temperature_c"]
                + 0.3 * (100 - record["humidity_pct"])
                - 0.2 * record["wind_speed_ms"],
                1,
            ),
        })

    print(f"Transformed {len(transformed)} records")
    return transformed


@task(retries=1)
def validate_weather_data(transformed: List[Dict]) -> tuple[List[Dict], Dict]:
    """Validate transformed data quality."""
    print(f"Validating {len(transformed)} records...")

    valid = []
    stats = {"total": len(transformed), "valid": 0, "flagged": 0}

    for record in transformed:
        issues = []
        if record["temp_c"] < -20 or record["temp_c"] > 50:
            issues.append("extreme_temperature")
        if record["wind_speed_ms"] > 50:
            issues.append("extreme_wind")
        if record["humidity_pct"] < 0 or record["humidity_pct"] > 100:
            issues.append("invalid_humidity")

        record["validation_issues"] = issues
        if issues:
            stats["flagged"] += 1
        else:
            stats["valid"] += 1
        valid.append(record)

    print(f"Validation complete: {stats}")
    return valid, stats


@task
def load_to_database(validated_data: tuple[List[Dict], Dict], table: str = "weather") -> int:
    """Simulate loading validated data to a database."""
    data, stats = validated_data
    print(f"Loading {len(data)} records to {table}...")

    for record in data:
        print(f"  INSERT INTO {table}: {record['station_id']} "
              f"temp={record['temp_f']}F comfort={record['comfort_index']}")

    print(f"Loaded {stats['valid']} valid, {stats['flagged']} flagged records")
    return stats["valid"]


@flow(
    name="weather-etl-flow",
    description="Simple ETL flow for weather data processing",
    log_prints=True,
)
def weather_etl_flow(api_url: str = "https://api.weather.gov"):
    """Main flow: extract → transform → validate → load."""
    raw = extract_weather_data(api_url)
    transformed = transform_weather_data(raw)
    validated = validate_weather_data(transformed)
    loaded_count = load_to_database(validated)
    print(f"Flow complete: {loaded_count} records loaded")
    return loaded_count


if __name__ == "__main__":
    weather_etl_flow()
