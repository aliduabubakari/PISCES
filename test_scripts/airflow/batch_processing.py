"""
batch_processing.py — High-Frequency Batch Processing Pipeline
===============================================================

A heavy compute pipeline with large fan-out, scheduled every 5 minutes.
Demonstrates HIGH_FREQUENCY_HEAVY_COMPUTE and LARGE_FAN_OUT patterns.

Expected eco-design profile:
  - HIGH_FREQUENCY_HEAVY_COMPUTE (major — heavy compute every 5 min)
  - LARGE_FAN_OUT (6 parallel branches)
  - HIGH_XCOM_INTENSITY
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "batch-team",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="batch_processing",
    default_args=default_args,
    description="High-frequency batch processing with heavy compute",
    schedule="*/5 * * * *",  # Every 5 minutes — VERY HIGH FREQUENCY
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "heavy-compute", "fan-out", "high-frequency"],
) as dag:

    def fetch_batch_partitions(**context):
        """Fetch list of data partitions to process."""
        return [
            {"partition": "northeast", "files": 120, "size_gb": 4.5},
            {"partition": "southeast", "files": 95, "size_gb": 3.8},
            {"partition": "midwest", "files": 210, "size_gb": 7.2},
            {"partition": "southwest", "files": 80, "size_gb": 2.9},
            {"partition": "northwest", "files": 65, "size_gb": 2.1},
            {"partition": "central", "files": 150, "size_gb": 5.5},
        ]

    # --- Parallel Partition Processing (HEAVY COMPUTE) ---
    def process_partition(partition_name, file_count, **context):
        """Simulate heavy compute on a data partition."""
        import hashlib
        import json

        # Simulate CPU-heavy processing
        results = []
        for i in range(min(file_count, 200)):
            # Expensive hash computation
            data = f"{partition_name}_{i}_{context['execution_date']}"
            h = hashlib.sha256(data.encode()).hexdigest()
            results.append(h[:16])

        return {
            "partition": partition_name,
            "files_processed": len(results),
            "success": len(results) > 0,
        }

    def validate_partition(ti, partition_name, **context):
        """Validate processed partition results."""
        result = ti.xcom_pull(
            task_ids=f"process_{partition_name}"
        )
        # Simulate validation checks
        validation = {
            "partition": partition_name,
            "checksum_valid": True,
            "row_count_match": True,
            "schema_valid": True,
            "warnings": 0,
        }
        return validation

    # --- Merge Phase ---
    def merge_partition_results(ti, **context):
        """Merge results from all partitions."""
        partitions = [
            "northeast", "southeast", "midwest",
            "southwest", "northwest", "central",
        ]
        merged = []
        for p in partitions:
            result = ti.xcom_pull(task_ids=f"process_{p}")
            validation = ti.xcom_pull(task_ids=f"validate_{p}")
            if result and validation:
                merged.append({**result, **validation})

        success_count = sum(1 for r in merged if r["success"])
        print(f"Merged {len(merged)} partitions, {success_count} succeeded")
        return merged

    def generate_summary(ti, **context):
        """Generate processing summary report."""
        merged = ti.xcom_pull(task_ids="merge_partition_results")
        total_files = sum(r["files_processed"] for r in merged)
        total_warnings = sum(r["warnings"] for r in merged)

        summary = {
            "timestamp": context["execution_date"].isoformat(),
            "total_partitions": len(merged),
            "total_files_processed": total_files,
            "success_rate": sum(1 for r in merged if r["success"]) / max(len(merged), 1),
            "total_warnings": total_warnings,
        }
        print(f"Summary: {summary}")
        return summary

    def archive_and_notify(ti, **context):
        """Archive results and send notifications."""
        summary = ti.xcom_pull(task_ids="generate_summary")
        # Simulate archiving and notification
        print(f"Archiving batch results: {summary['total_files_processed']} files")
        return summary["total_files_processed"]

    # --- DAG Structure ---
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    fetch_partitions = PythonOperator(
        task_id="fetch_batch_partitions",
        python_callable=fetch_batch_partitions,
    )

    partition_names = ["northeast", "southeast", "midwest",
                       "southwest", "northwest", "central"]

    process_tasks = {}
    validate_tasks = {}

    for p in partition_names:
        process_tasks[p] = PythonOperator(
            task_id=f"process_{p}",
            python_callable=process_partition,
            op_kwargs={"partition_name": p},
        )
        validate_tasks[p] = PythonOperator(
            task_id=f"validate_{p}",
            python_callable=validate_partition,
            op_kwargs={"partition_name": p},
        )
        fetch_partitions >> process_tasks[p] >> validate_tasks[p]

    merge = PythonOperator(
        task_id="merge_partition_results",
        python_callable=merge_partition_results,
    )
    summarize = PythonOperator(
        task_id="generate_summary",
        python_callable=generate_summary,
    )
    archive = PythonOperator(
        task_id="archive_and_notify",
        python_callable=archive_and_notify,
    )

    start >> fetch_partitions
    for p in partition_names:
        validate_tasks[p] >> merge
    merge >> summarize >> archive >> end
