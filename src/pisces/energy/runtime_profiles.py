"""Explicit, bounded OCI engine profiles; not a declaration of production parity."""

PROFILES = {
    "airflow": {"distributions": {"apache-airflow": "3.4.0", "apache-airflow-providers-standard": "1.21.0"},
                "interface": "DAG.test", "control_plane": "temporary_sqlite"},
    "prefect": {"distributions": {"prefect": "3.6.18", "fastapi": "0.128.0", "sqlalchemy": "2.0.55"}, "interface": "Flow.__call__(return_state=True)",
                "control_plane": "local_test_harness_api_and_sqlite"},
    "dagster": {"distributions": {"dagster": "1.12.8", "sqlalchemy": "2.0.55"}, "interface": "JobDefinition.execute_in_process",
                "control_plane": "ephemeral_instance"},
}
