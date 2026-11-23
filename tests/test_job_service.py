from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient
from pydantic import ValidationError

from pisces.service.models import JobSubmission
from pisces.service.settings import Settings
from pisces.service.store import JobStore, QueueFull
from pisces.web import create_app

TOKEN = "test-only-token-" + "x" * 32


@pytest.fixture
def client(tmp_path):
    app = create_app(Settings(state_dir=tmp_path, token=TOKEN))
    app.state.store.heartbeat(
        [{"name": "python", "ready": True, "image_id": "sha256:" + "a" * 64}]
    )
    return TestClient(
        app, base_url="http://127.0.0.1", headers={"Authorization": "Bearer " + TOKEN}
    )


def submission(**kwargs):
    return {
        "filename": "script.py",
        "source": "raise RuntimeError('not executed by API')\n",
        "profile": "python",
        "timeout_s": 30,
        **kwargs,
    }


def test_authenticated_upload_queues_without_executing_source(client):
    response = client.post("/api/v1/jobs", json=submission())
    assert response.status_code == 202
    job = response.json()
    assert job["state"] == "queued"
    assert (
        job["artifact_sha256"]
        == hashlib.sha256(submission()["source"].encode()).hexdigest()
    )
    assert "source" not in job and "request" not in job
    assert client.get("/api/v1/jobs").json()[0] == job
    assert client.get(f"/api/v1/jobs/{job['id']}/report").status_code == 409
    assert client.delete(f"/api/v1/jobs/{job['id']}").status_code == 409
    assert (
        client.post(f"/api/v1/jobs/{job['id']}/cancel").json()["state"] == "cancelled"
    )
    assert client.get(f"/api/v1/jobs/{job['id']}/source").status_code == 409
    assert client.delete(f"/api/v1/jobs/{job['id']}").status_code == 204


def test_jobs_require_auth_and_ready_profile(client):
    assert (
        client.get(
            "/api/v1/jobs", headers={"Authorization": "Bearer wrong"}
        ).status_code
        == 401
    )
    assert client.get("/healthz", headers={"Authorization": ""}).status_code == 200
    assert (
        client.post(
            "/api/v1/jobs",
            json=submission(profile="airflow", contract="{}", binding="{}"),
        ).status_code
        == 409
    )
    assert (
        client.post(
            "/api/v1/jobs", json=submission(filename="../script.py")
        ).status_code
        == 422
    )
    assert (
        client.post("/api/v1/jobs", json=submission(timeout_s=10000)).status_code == 422
    )
    assert (
        client.post(
            "/api/v1/jobs", json=submission(image="arbitrary:latest")
        ).status_code
        == 422
    )
    assert client.get("/api/v1/jobs/../../etc/passwd").status_code != 200


@pytest.mark.parametrize(
    "path",
    [
        "../private.csv",
        "/etc/passwd",
        "a/../secret.csv",
        "a\\secret.csv",
        "contract.json",
        "script.py",
        "fixtures//events.csv",
        "contract.json/",
    ],
)
def test_native_fixture_paths_are_confined(path):
    with pytest.raises(ValidationError):
        JobSubmission.model_validate(
            submission(
                profile="dagster",
                contract="{}",
                binding="{}",
                files=[{"path": path, "content_base64": "eA=="}],
            )
        )


def test_claim_is_atomic_and_restart_is_not_replayed(tmp_path):
    store = JobStore(tmp_path)
    job = store.submit(submission())
    with ThreadPoolExecutor(max_workers=4) as pool:
        claimed = list(pool.map(lambda _: store.claim(), range(4)))
    assert sum(value is not None for value in claimed) == 1
    store.recover()
    assert store.get(job["id"])["state"] == "failed"
    assert store.get(job["id"])["error"] == "worker_restarted"
    assert store.claim() is None
    assert store.path.stat().st_mode & 0o777 == 0o660


def test_queue_capacity_and_cancel_race(tmp_path):
    store = JobStore(tmp_path)
    for _ in range(8):
        store.submit(submission())
    with pytest.raises(QueueFull):
        store.submit(submission())
    running = store.claim()
    store.cancel(running["id"])
    store.finish(running["id"], state="completed", report={})
    assert store.get(running["id"])["state"] == "cancelled"
    assert store.report(running["id"]) is None


def test_production_env_refuses_missing_token(monkeypatch):
    monkeypatch.setenv("PISCES_ENV", "production")
    monkeypatch.delenv("PISCES_API_TOKEN", raising=False)
    monkeypatch.delenv("PISCES_API_TOKEN_FILE", raising=False)
    with pytest.raises(ValueError, match="secret API token"):
        Settings.from_env()


def test_job_contract_generated_from_models():
    from scripts.export_job_schema import job_schema

    path = (
        Path(__file__).resolve().parents[1]
        / "src/pisces/schemas/jobs-1.0.0.schema.json"
    )
    assert json.loads(path.read_text()) == job_schema()


@pytest.mark.parametrize("status", ["failed", "unavailable", "skipped"])
def test_unfinished_evaluation_is_not_success(tmp_path, status):
    store = JobStore(tmp_path)
    job = store.submit(submission())
    store.claim()
    store.finish(
        job["id"],
        state="completed",
        report={"energy": {"measurements": {"runtime": {"status": status}}}},
    )
    assert store.get(job["id"])["has_report"]
    assert store.get(job["id"])["evaluation_succeeded"] is False


def test_non_ascii_auth_header_is_rejected(client):
    response = client.get("/api/v1/jobs", headers={"Authorization": b"Bearer \xc3\xa9"})
    assert response.status_code == 401


def test_oversized_report_is_not_marked_successful(tmp_path):
    store = JobStore(tmp_path)
    job = store.submit(submission())
    store.claim()
    store.finish(
        job["id"], state="completed", report={"payload": "x" * (8 * 1024 * 1024)}
    )
    result = store.get(job["id"])
    assert result["state"] == "failed" and result["error"] == "report_too_large"
    assert result["evaluation_succeeded"] is None and not result["has_report"]


def test_stdin_bundle_marker_does_not_enable_host_native_execution(
    tmp_path, monkeypatch
):
    import sys
    from pisces.energy import runtime_runner_cli as runner

    request, output = tmp_path / "request.json", tmp_path / "output.json"
    request.write_text(
        json.dumps(
            {
                "orchestrator": "dagster",
                "experiment_mode": True,
                "execution_adapter": "native",
                "mode": "zero_fill",
                "fixture_delivery": "private_stdin_bundle",
                "workload_contract": "unused",
                "artifact_binding": "unused",
            }
        )
    )
    monkeypatch.setenv("PISCES_OCI_TARGET", "dagster")
    monkeypatch.setenv("PISCES_OCI_INPUT_DELIVERY", "private_stdin_bundle")
    monkeypatch.setattr(runner.os, "geteuid", lambda: 1000)
    monkeypatch.setattr(
        sys, "argv", ["runner", "--in", str(request), "--out", str(output)]
    )
    runner.main()
    assert (
        json.loads(output.read_text())["error"]["message"]
        == "qualified_engine_requires_private_oci_worker"
    )
