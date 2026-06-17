"""
parameterized_etl.py — Parameterized Prefect ETL with Subflows
================================================================

A moderately complex Prefect pipeline with parameterized tasks,
subflows, and concurrent execution. Demonstrates fan-out patterns
using Prefect's task submission API.

Expected eco-design profile:
  - LARGE_FAN_OUT (concurrent city processing)
  - Moderate XCom-like data passing between tasks
  - Subflow abstraction
"""

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from typing import List, Dict, Any
import random


@task(retries=2, retry_delay_seconds=15)
def discover_cities(region: str = "us") -> List[Dict[str, Any]]:
    """Discover cities to process based on region."""
    cities_db = {
        "us": [
            {"name": "Boston", "state": "MA", "lat": 42.36, "lon": -71.06},
            {"name": "Chicago", "state": "IL", "lat": 41.88, "lon": -87.63},
            {"name": "Charlotte", "state": "NC", "lat": 35.23, "lon": -80.84},
            {"name": "Seattle", "state": "WA", "lat": 47.61, "lon": -122.33},
            {"name": "Denver", "state": "CO", "lat": 39.74, "lon": -104.99},
        ],
        "eu": [
            {"name": "London", "state": "UK", "lat": 51.51, "lon": -0.13},
            {"name": "Paris", "state": "FR", "lat": 48.86, "lon": 2.35},
            {"name": "Berlin", "state": "DE", "lat": 52.52, "lon": 13.40},
        ],
    }
    cities = cities_db.get(region, cities_db["us"])
    print(f"Discovered {len(cities)} cities in {region}")
    return cities


@task(retries=1)
def fetch_city_weather(city: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch current weather for a single city."""
    weather_conditions = ["sunny", "cloudy", "rainy", "partly_cloudy", "stormy"]

    data = {
        "city": city["name"],
        "state": city["state"],
        "lat": city["lat"],
        "lon": city["lon"],
        "temperature_c": round(random.uniform(-5.0, 40.0), 1),
        "humidity_pct": random.randint(20, 95),
        "wind_speed_ms": round(random.uniform(0.0, 25.0), 1),
        "condition": random.choice(weather_conditions),
        "uv_index": round(random.uniform(0.0, 11.0), 1),
    }

    # Simulate API call latency
    import time
    time.sleep(0.1 * random.random())

    print(f"  {data['city']}: {data['temperature_c']}°C, {data['condition']}, "
          f"UV={data['uv_index']}")
    return data


@task
def enrich_weather_data(weather: Dict[str, Any]) -> Dict[str, Any]:
    """Enrich weather data with derived comfort metrics."""
    enriched = {**weather}

    # Heat index approximation
    t = weather["temperature_c"]
    h = weather["humidity_pct"]
    enriched["heat_index_c"] = round(
        t + 0.5 * (h / 100) * t, 1
    )

    # Wind chill approximation
    w = weather["wind_speed_ms"]
    enriched["wind_chill_c"] = round(
        13.12 + 0.6215 * t - 11.37 * w**0.16 + 0.3965 * t * w**0.16, 1
    )

    # Comfort score (0-100)
    comfort = 100
    if t < 0 or t > 35:
        comfort -= 30
    elif t < 10 or t > 30:
        comfort -= 10
    if h > 80:
        comfort -= 15
    if w > 15:
        comfort -= 10
    if weather["uv_index"] > 7:
        comfort -= 10
    enriched["comfort_score"] = max(0, comfort)

    return enriched


@task
def aggregate_city_data(enriched_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate weather data across all cities."""
    import statistics

    temps = [d["temperature_c"] for d in enriched_data]
    comfort = [d["comfort_score"] for d in enriched_data]
    uv = [d["uv_index"] for d in enriched_data]

    conditions = {}
    for d in enriched_data:
        cond = d["condition"]
        conditions[cond] = conditions.get(cond, 0) + 1

    summary = {
        "city_count": len(enriched_data),
        "avg_temp_c": round(statistics.mean(temps), 1),
        "min_temp_c": min(temps),
        "max_temp_c": max(temps),
        "avg_comfort_score": round(statistics.mean(comfort), 1),
        "avg_uv_index": round(statistics.mean(uv), 1),
        "condition_distribution": conditions,
        "most_comfortable": max(enriched_data, key=lambda d: d["comfort_score"])["city"],
        "least_comfortable": min(enriched_data, key=lambda d: d["comfort_score"])["city"],
    }

    print(f"Aggregated {summary['city_count']} cities: "
          f"avg_temp={summary['avg_temp_c']}°C, "
          f"best={summary['most_comfortable']}, "
          f"worst={summary['least_comfortable']}")
    return summary


@task
def generate_report(summary: Dict[str, Any]) -> str:
    """Generate a human-readable weather report."""
    lines = [
        "=" * 50,
        "       WEATHER REPORT",
        "=" * 50,
        f"Cities analyzed: {summary['city_count']}",
        f"Average temperature: {summary['avg_temp_c']}°C",
        f"Temperature range: {summary['min_temp_c']}°C — {summary['max_temp_c']}°C",
        f"Average comfort score: {summary['avg_comfort_score']}/100",
        f"UV index: {summary['avg_uv_index']} (average)",
        "",
        "Conditions:",
    ]
    for cond, count in sorted(summary["condition_distribution"].items()):
        lines.append(f"  {cond}: {count} cities")
    lines.append("")
    lines.append(f"Most comfortable: {summary['most_comfortable']}")
    lines.append(f"Least comfortable: {summary['least_comfortable']}")
    lines.append("=" * 50)

    report = "\n".join(lines)
    print(report)
    return report


@flow(
    name="city-weather-flow",
    description="Parameterized multi-city weather processing with fan-out",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True,
)
def city_weather_flow(region: str = "us"):
    """Main flow: parallel city weather processing."""
    cities = discover_cities(region)

    # Fan-out: process each city concurrently
    weather_futures = []
    for city in cities:
        future = fetch_city_weather.submit(city)
        weather_futures.append(future)

    # Enrich each city's weather data
    enriched_futures = []
    for future in weather_futures:
        weather = future.result()
        enriched_futures.append(enrich_weather_data.submit(weather))

    # Collect results
    enriched_data = [f.result() for f in enriched_futures]

    # Aggregate and report
    summary = aggregate_city_data(enriched_data)
    report = generate_report(summary)

    print(f"\nFlow complete: processed {summary['city_count']} cities in {region}")
    return report


if __name__ == "__main__":
    city_weather_flow(region="us")
