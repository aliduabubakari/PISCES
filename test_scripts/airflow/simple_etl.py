"""
simple_etl.py — Basic Airflow ETL Pipeline
==========================================

A minimal extract-transform-load pipeline ideal for baseline energy testing.
Single-path sequential execution with no parallelism or sensors.

Expected eco-design profile:
  - No rules fired (simple, efficient structure)
  - Good baseline for comparing against more complex pipelines
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="simple_etl",
    default_args=default_args,
    description="A basic ETL pipeline for weather data",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "simple", "baseline"],
) as dag:

    def extract_data(api_endpoint="https://api.weather.gov/stations", **context):
        """Simulate extracting data from an external API."""
        import json
        import random

        stations = ["KBOS", "KORD", "KCLT"]
        data = []
        for station in stations:
            data.append({
                "station": station,
                "temperature_c": round(random.uniform(5.0, 35.0), 1),
                "humidity_pct": random.randint(30, 90),
                "wind_speed_kmh": round(random.uniform(0.0, 50.0), 1),
                "pressure_hpa": round(random.uniform(990.0, 1030.0), 1),
                "timestamp": context["execution_date"].isoformat(),
            })
        return data

    def transform_data(ti, **context):
        """Convert raw weather data to standardized format with derived fields."""
        raw_data = ti.xcom_pull(task_ids="extract_data")
        transformed = []
        for record in raw_data:
            transformed.append({
                "station_id": record["station"],
                "temp_f": round(record["temperature_c"] * 9 / 5 + 32, 1),
                "feels_like_f": round(
                    record["temperature_c"] * 9 / 5 + 32
                    - (0.2 * record["wind_speed_kmh"]),
                    1,
                ),
                "humidity_category": (
                    "low" if record["humidity_pct"] < 40
                    else "medium" if record["humidity_pct"] < 70
                    else "high"
                ),
                "observation_time": record["timestamp"],
            })
        return transformed

    def load_data(ti, table_name="weather_observations", **context):
        """Simulate loading transformed data to a database."""
        transformed = ti.xcom_pull(task_ids="transform_data")
        # Simulate DB write
        for row in transformed:
            print(f"INSERT INTO {table_name}: {row['station_id']} "
                  f"temp={row['temp_f']}F humidity={row['humidity_category']}")
        return len(transformed)

    extract_data = PythonOperator(task_id="extract_data", python_callable=extract_data)
    transform_data = PythonOperator(task_id="transform_data", python_callable=transform_data)
    load_data = PythonOperator(task_id="load_data", python_callable=load_data)

    extract_data >> transform_data >> load_data
