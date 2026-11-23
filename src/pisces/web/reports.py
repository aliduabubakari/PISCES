from fastapi import APIRouter, HTTPException, Request
from jsonschema.exceptions import ValidationError

from pisces.report_views import (
    MAX_REPORT_BYTES,
    VIEW_SCHEMA_VERSION,
    build_workspace_view,
    load_report,
)

router = APIRouter()


async def json_body(request: Request, limit: int = MAX_REPORT_BYTES):
    if (
        request.headers.get("content-type", "").split(";", 1)[0].lower()
        != "application/json"
    ):
        raise HTTPException(415, "application_json_required")
    data = bytearray()
    async for chunk in request.stream():
        if len(data) + len(chunk) > limit:
            raise HTTPException(413, "report_too_large")
        data.extend(chunk)
    try:
        return load_report(bytes(data))
    except ValueError as exc:
        raise HTTPException(422, "invalid_report_evidence") from exc


@router.get("/api/v1/capabilities")
def capabilities(request: Request):
    enabled = request.app.state.store is not None
    return {
        "view_schema_version": VIEW_SCHEMA_VERSION,
        "report_types": [
            "unified",
            "suite_attempt",
            "suite_summary",
            "load_sweep",
            "scaling_sessions",
        ],
        "execution": enabled,
        "persistence": enabled,
        "max_report_bytes": MAX_REPORT_BYTES,
    }


@router.post(
    "/api/v1/reports/view",
    responses={
        200: {
            "content": {
                "application/json": {
                    "schema": {
                        "oneOf": [
                            {"$ref": "#/components/schemas/ReportView"},
                            {"$ref": "#/components/schemas/CampaignView"},
                        ]
                    }
                }
            }
        }
    },
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": {"type": "object"}}},
        }
    },
)
async def report_view(request: Request):
    report = await json_body(request)
    try:
        return build_workspace_view(report)
    except (ValueError, ValidationError, TypeError, KeyError) as exc:
        raise HTTPException(422, "invalid_report_evidence") from exc
