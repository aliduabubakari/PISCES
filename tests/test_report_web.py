from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from pisces.report_views import MAX_REPORT_BYTES, build_report_view
from pisces.web import create_app


@pytest.fixture
def client():
    return TestClient(create_app(), base_url="http://127.0.0.1")


def test_capabilities_openapi_and_no_execution_routes(client):
    response = client.get("/api/v1/capabilities")
    assert response.json()["execution"] is False
    assert response.json()["persistence"] is False
    assert response.headers["cache-control"] == "no-store"
    assert client.post("/api/v1/evaluate").status_code == 404
    document = client.get("/openapi.json").json()
    assert document["paths"]["/api/v1/reports/view"]["post"]["requestBody"]["required"]

    def check_refs(value):
        if isinstance(value, dict):
            if "$ref" in value:
                target = document
                for key in value["$ref"].removeprefix("#/").split("/"):
                    target = target[key]
            for child in value.values():
                check_refs(child)
        elif isinstance(value, list):
            for child in value:
                check_refs(child)

    check_refs(document)


def test_library_http_parity_and_untrusted_source_paths_are_not_read(client):
    examples = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "frontend/src/test/fixtures/examples.json"
        ).read_text()
    )
    for example in examples:
        report = example["report"]
        report["workflow"] = {"path": "/etc/passwd"}
        response = client.post("/api/v1/reports/view", json=report)
        assert response.status_code == 200
        assert response.json() == build_report_view(report)
        assert "/etc/passwd" not in response.text


def test_upload_and_origin_controls(client):
    url = "/api/v1/reports/view"
    assert (
        client.post(
            url, content="{}", headers={"content-type": "text/plain"}
        ).status_code
        == 415
    )
    assert (
        client.post(
            url,
            content=b" " * (MAX_REPORT_BYTES + 1),
            headers={"content-type": "application/json"},
        ).status_code
        == 413
    )
    for body in ('{"x":NaN}', '{"x":1,"x":2}', "[]", "{"):
        assert (
            client.post(
                url, content=body, headers={"content-type": "application/json"}
            ).status_code
            == 422
        )
    assert (
        client.post(
            url, json={}, headers={"origin": "https://attacker.example"}
        ).status_code
        == 403
    )
    assert client.post(url, json={}, headers={"origin": "http://["}).status_code == 403
    assert (
        client.get(
            "/api/v1/capabilities", headers={"host": "attacker.example"}
        ).status_code
        == 400
    )
