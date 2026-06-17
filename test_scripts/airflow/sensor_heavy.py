"""
sensor_heavy.py — Sensor-Heavy Monitoring Pipeline
====================================================

A pipeline that uses Airflow sensors to wait for external conditions.
Demonstrates poke-mode sensor usage which wastes resources.

Expected eco-design profile:
  - SENSOR_POKE_MODE (poke-mode sensors detected — major rule)
  - HIGH_XCOM_INTENSITY (multiple data exchanges)
  - Deep sequential dependency chain
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.http_sensor import HttpSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "monitoring-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="sensor_heavy_monitoring",
    default_args=default_args,
    description="Monitoring pipeline demonstrating sensor-heavy patterns",
    schedule="*/30 * * * *",  # Every 30 minutes — HIGH FREQUENCY
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sensors", "monitoring", "high-frequency"],
) as dag:

    # POKE-MODE sensors — these wake up repeatedly!
    wait_for_data_file = FileSensor(
        task_id="wait_for_data_file",
        filepath="/data/incoming/weather_batch_{{ ds_nodash }}.csv",
        poke_interval=60,  # Check every 60 seconds — WASTEFUL
        timeout=600,
        mode="poke",  # <--- POKE MODE: the sensor holds a worker slot!
        soft_fail=True,
    )

    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_upstream_etl",
        external_dag_id="simple_etl",
        external_task_id="load_data",
        poke_interval=120,
        timeout=1800,
        mode="poke",  # <--- Another poke-mode sensor
        soft_fail=True,
    )

    check_api_health = HttpSensor(
        task_id="check_api_health",
        http_conn_id="weather_api",
        endpoint="/health",
        poke_interval=30,  # Very frequent poking!
        timeout=300,
        mode="poke",  # <--- Yet another poke-mode sensor
        soft_fail=True,
    )

    # --- Data Processing (sequential chain) ---
    def ingest_data(ti, **context):
        """Ingest data after all sensors pass."""
        import random
        records = []
        for i in range(50):  # Heavy processing
            records.append({
                "id": i,
                "sensor_reading": round(random.uniform(0, 100), 2),
                "quality": random.choice(["good", "fair", "poor"]),
            })
        ti.xcom_push(key="ingested_records", value=records)
        return len(records)

    def validate_quality(ti, **context):
        """Validate data quality metrics."""
        records = ti.xcom_pull(
            task_ids="ingest_data", key="ingested_records"
        )
        good = sum(1 for r in records if r["quality"] == "good")
        return {"total": len(records), "good": good, "ratio": good / len(records)}

    def compute_aggregates(ti, **context):
        """Compute statistical aggregates."""
        records = ti.xcom_pull(
            task_ids="ingest_data", key="ingested_records"
        )
        import statistics
        values = [r["sensor_reading"] for r in records]
        aggregates = {
            "mean": statistics.mean(values),
            "stdev": statistics.stdev(values),
            "min": min(values),
            "max": max(values),
        }
        ti.xcom_push(key="aggregates", value=aggregates)
        return aggregates

    def generate_report(ti, **context):
        """Generate monitoring report."""
        quality = ti.xcom_pull(task_ids="validate_quality")
        aggregates = ti.xcom_pull(task_ids="compute_aggregates", key="aggregates")
        report = {
            "timestamp": context["execution_date"].isoformat(),
            "quality": quality,
            "statistics": aggregates,
        }
        ti.xcom_push(key="report", value=report)
        print(f"Report: {report}")
        return report

    def archive_results(ti, **context):
        """Archive monitoring results."""
        report = ti.xcom_pull(task_ids="generate_report", key="report")
        # Simulate archiving
        print(f"Archiving report from {report['timestamp']}: "
              f"quality_ratio={report['quality']['ratio']:.2f}")
        return True

    # --- DAG Structure ---
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # All sensors run first
    all_sensors = [wait_for_data_file, wait_for_upstream, check_api_health]

    ingest = PythonOperator(
        task_id="ingest_data", python_callable=ingest_data
    )
    validate = PythonOperator(
        task_id="validate_quality", python_callable=validate_quality
    )
    compute = PythonOperator(
        task_id="compute_aggregates", python_callable=compute_aggregates
    )
    report = PythonOperator(
        task_id="generate_report", python_callable=generate_report
    )
    archive = PythonOperator(
        task_id="archive_results", python_callable=archive_results
    )

    # Wiring: sensors → processing → archive (deep sequential chain)
    start >> all_sensors >> ingest >> [validate, compute] >> report >> archive >> end
