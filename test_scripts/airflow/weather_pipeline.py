"""
weather_pipeline.py — Multi-Branch Weather Data Pipeline
=========================================================

A moderately complex pipeline that processes weather data from multiple
cities in parallel branches. Demonstrates fan-out/fan-in patterns and
XCom data passing.

Expected eco-design profile:
  - LARGE_FAN_OUT (3 branches from extract)
  - HIGH_XCOM_INTENSITY (multiple push/pull across branches)
  - DEEP_SEQUENTIAL_CHAIN (each branch is sequential)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "weather-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    description="Multi-branch weather data processing with fan-out",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "fan-out", "multi-branch"],
) as dag:

    # --- Extract Phase ---
    def fetch_station_list(**context):
        """Fetch list of weather stations to process."""
        return [
            {"id": "KBOS", "city": "Boston", "lat": 42.36, "lon": -71.01},
            {"id": "KORD", "city": "Chicago", "lat": 41.88, "lon": -87.63},
            {"id": "KCLT", "city": "Charlotte", "lat": 35.23, "lon": -80.84},
        ]

    def fetch_weather_for_station(ti, station_id, **context):
        """Fetch current weather for a single station."""
        import random
        stations = ti.xcom_pull(task_ids="fetch_station_list")
        station = next(s for s in stations if s["id"] == station_id)
        return {
            **station,
            "temperature_c": round(random.uniform(5.0, 35.0), 1),
            "humidity_pct": random.randint(30, 90),
            "wind_speed_kmh": round(random.uniform(0.0, 50.0), 1),
            "pressure_hpa": round(random.uniform(990.0, 1030.0), 1),
        }

    # --- Transform Phase ---
    def enrich_weather_data(ti, station_id, **context):
        """Enrich raw weather with derived metrics."""
        raw = ti.xcom_pull(task_ids=f"fetch_{station_id}")
        return {
            **raw,
            "temp_f": round(raw["temperature_c"] * 9 / 5 + 32, 1),
            "heat_index": round(
                raw["temperature_c"]
                + 0.5 * raw["humidity_pct"] / 100 * raw["temperature_c"],
                1,
            ),
            "wind_chill": round(
                35.74 + 0.6215 * raw["temperature_c"]
                - 35.75 * raw["wind_speed_kmh"] ** 0.16
                + 0.4275 * raw["temperature_c"] * raw["wind_speed_kmh"] ** 0.16,
                1,
            ),
            "processed_at": context["execution_date"].isoformat(),
        }

    def validate_weather_data(ti, station_id, **context):
        """Validate transformed weather data quality."""
        data = ti.xcom_pull(task_ids=f"enrich_{station_id}")
        warnings = []
        if data["temperature_c"] < -10 or data["temperature_c"] > 50:
            warnings.append("extreme_temperature")
        if data["wind_speed_kmh"] > 100:
            warnings.append("extreme_wind")
        return {"data": data, "warnings": warnings, "valid": len(warnings) == 0}

    # --- Load Phase ---
    def merge_weather_results(ti, **context):
        """Merge results from all station branches."""
        all_data = []
        for sid in ["KBOS", "KORD", "KCLT"]:
            result = ti.xcom_pull(task_ids=f"validate_{sid}")
            if result:
                all_data.append(result)
        print(f"Merged {len(all_data)} station results")
        return all_data

    def load_to_warehouse(ti, **context):
        """Load merged weather data to warehouse."""
        merged = ti.xcom_pull(task_ids="merge_weather_results")
        for record in merged:
            print(f"Loading {record['data']['city']}: "
                  f"valid={record['valid']} temp={record['data']['temp_f']}F")
        return len(merged)

    def send_alerts(ti, **context):
        """Send alerts for invalid weather readings."""
        merged = ti.xcom_pull(task_ids="merge_weather_results")
        alert_count = 0
        for record in merged:
            if not record["valid"]:
                for warning in record["warnings"]:
                    print(f"ALERT: {record['data']['city']} - {warning}")
                    alert_count += 1
        return alert_count

    # --- DAG Structure ---
    fetch_stations = PythonOperator(
        task_id="fetch_station_list", python_callable=fetch_station_list
    )

    # Fan-out: one branch per station
    fetch_tasks = {}
    enrich_tasks = {}
    validate_tasks = {}

    for station_id in ["KBOS", "KORD", "KCLT"]:
        fetch_tasks[station_id] = PythonOperator(
            task_id=f"fetch_{station_id}",
            python_callable=fetch_weather_for_station,
            op_kwargs={"station_id": station_id},
        )
        enrich_tasks[station_id] = PythonOperator(
            task_id=f"enrich_{station_id}",
            python_callable=enrich_weather_data,
            op_kwargs={"station_id": station_id},
        )
        validate_tasks[station_id] = PythonOperator(
            task_id=f"validate_{station_id}",
            python_callable=validate_weather_data,
            op_kwargs={"station_id": station_id},
        )

        fetch_stations >> fetch_tasks[station_id] >> enrich_tasks[station_id] >> validate_tasks[station_id]

    # Fan-in
    merge_results = PythonOperator(
        task_id="merge_weather_results", python_callable=merge_weather_results
    )
    load_warehouse = PythonOperator(
        task_id="load_to_warehouse", python_callable=load_to_warehouse
    )
    send_alerts = PythonOperator(
        task_id="send_alerts", python_callable=send_alerts
    )

    for station_id in ["KBOS", "KORD", "KCLT"]:
        validate_tasks[station_id] >> merge_results

    merge_results >> [load_warehouse, send_alerts]
