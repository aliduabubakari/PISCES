"""Live API/worker/container smoke test; no Docker socket in this test client."""

import json
import os
from pathlib import Path
import time
import urllib.request

base = os.environ.get("PISCES_TEST_API_ORIGIN", "http://api:8000")
token = (
    Path(os.environ.get("PISCES_TEST_API_TOKEN_FILE", "/run/secrets/api_token"))
    .read_text()
    .strip()
)


def call(path, data=None, method=None):
    request = urllib.request.Request(
        base + path,
        data=json.dumps(data).encode() if data is not None else None,
        headers={
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response) if response.status != 204 else None


def run(profile, source, timeout=30):
    job = call(
        "/api/v1/jobs",
        {
            "filename": "smoke.py",
            "profile": profile,
            "source": source,
            "timeout_s": timeout,
        },
    )
    deadline = time.monotonic() + timeout + 30
    while job["state"] in ("queued", "running") and time.monotonic() < deadline:
        time.sleep(0.3)
        job = call("/api/v1/jobs/" + job["id"])
    return job


assert call("/api/v1/capabilities")["execution"] is True
static = run("static", "raise RuntimeError('must not execute')\n")
assert static["state"] == "completed" and static["evaluation_succeeded"]
python = run("python", "total = sum(i * i for i in range(1000000))\n")
assert python["state"] == "completed" and python["evaluation_succeeded"], python
report = call(f"/api/v1/jobs/{python['id']}/report")
runtime = report["energy"]["measurements"]["runtime"]
assert runtime["container_telemetry"]["cpu_s"] > 0
assert runtime["container_telemetry"]["memory_peak_bytes"] > 0
assert runtime["eligible_for_resource_comparison"] is False
view = call(f"/api/v1/jobs/{python['id']}/view")
assert view["identity"]["artifact_sha256"] == python["artifact_sha256"]
assert view["outputs"]["status"] == "not_evaluated"
timeout = run("python", "while True: pass\n", timeout=5)
assert timeout["state"] == "timed_out", timeout
cancel = call(
    "/api/v1/jobs",
    {
        "filename": "cancel.py",
        "profile": "python",
        "source": "while True: pass\n",
        "timeout_s": 30,
    },
)
call(f"/api/v1/jobs/{cancel['id']}/cancel", {}, method="POST")
deadline = time.monotonic() + 20
while time.monotonic() < deadline:
    cancel = call(f"/api/v1/jobs/{cancel['id']}")
    if cancel["state"] == "cancelled":
        break
    time.sleep(0.2)
assert cancel["state"] == "cancelled"
for job in (static, python, timeout, cancel):
    call(f"/api/v1/jobs/{job['id']}", method="DELETE")
print(
    "Compose smoke passed: static, actual Python execution, telemetry, timeout, cancellation, report views and deletion"
)
