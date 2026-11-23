"""Verify a prepared synthetic benchmark through the authenticated job API."""

import argparse
import base64
import hashlib
import json
from pathlib import Path
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "bundle", type=Path, help="Prepared benchmark pipeline directory"
    )
    parser.add_argument(
        "--profile", choices=("airflow", "prefect", "dagster"), required=True
    )
    parser.add_argument("--origin", default="http://127.0.0.1:18080")
    parser.add_argument(
        "--token-file", type=Path, default=Path("containers/service/test-token.txt")
    )
    args = parser.parse_args()
    token = args.token_file.read_text().strip()

    def call(path, data=None, method=None):
        request = urllib.request.Request(
            args.origin + path,
            data=json.dumps(data).encode() if data is not None else None,
            headers={
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json",
            },
            method=method,
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response) if response.status != 204 else None

    contract_bytes = (args.bundle / "contract.json").read_bytes()
    contract = json.loads(contract_bytes)
    paths = {item["fixture"] for item in contract["inputs"]}
    paths.update(
        item["reference"]["path"]
        for item in contract["outputs"]
        if item.get("reference")
    )
    files = []
    for path in sorted(paths):
        source = (args.bundle / path).resolve()
        if not source.is_relative_to(args.bundle.resolve()):
            raise ValueError("Fixture must stay inside the explicitly selected bundle")
        files.append(
            {
                "path": path,
                "content_base64": base64.b64encode(source.read_bytes()).decode("ascii"),
            }
        )
    for case, expected in (("valid", "passed"), ("same_shape_wrong", "failed")):
        source = (args.bundle / f"{case}.py").read_bytes()
        job = call(
            "/api/v1/jobs",
            {
                "filename": f"{case}.py",
                "profile": args.profile,
                "source": source.decode("utf-8"),
                "timeout_s": 120,
                "contract": contract_bytes.decode("utf-8"),
                "binding": (args.bundle / f"{case}.binding.json")
                .read_bytes()
                .decode("utf-8"),
                "files": files,
            },
        )
        identity = job["id"]
        try:
            deadline = time.monotonic() + 150
            while job["state"] in ("queued", "running") and time.monotonic() < deadline:
                time.sleep(0.5)
                job = call(f"/api/v1/jobs/{identity}")
            assert job["state"] == "completed", job
            report = call(f"/api/v1/jobs/{identity}/report")
            runtime = report["energy"]["measurements"]["runtime"]
            assert runtime["execution_adapter_used"] == "oci", (
                runtime.get("execution_outcome"),
                runtime.get("ineligibility_reasons"),
            )
            assert runtime["container_telemetry"]["cpu_s"] > 0
            assert runtime["container_telemetry"]["memory_peak_bytes"] > 0
            assert runtime["eligible_for_resource_comparison"] is False
            assert (
                report["metadata"]["artifact_sha256"]
                == hashlib.sha256(source).hexdigest()
            )
            assert runtime["output_assertions"]["status"] == expected
            view = call(f"/api/v1/jobs/{identity}/view")
            assert view["outputs"]["status"] == expected
            print(
                f"Native service smoke passed: {args.profile}/{case}, outputs={expected}, CPU/memory telemetry attached"
            )
        finally:
            call(f"/api/v1/jobs/{identity}/cancel", {}, "POST")
            call(f"/api/v1/jobs/{identity}", method="DELETE")


if __name__ == "__main__":
    main()
