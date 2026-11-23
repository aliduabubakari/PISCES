from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, Request, Response
from pydantic import ValidationError

from pisces.report_views import build_workspace_view
from pisces.service.models import (
    JobSubmission,
    JobStatus,
    MAX_JOB_BYTES,
    PROFILES,
    ProfileReadiness,
)
from pisces.service.store import QueueFull
from .reports import json_body

router = APIRouter(prefix="/api/v1/jobs", tags=["Evaluation jobs"])
JobID = Annotated[str, Path(pattern="^[a-f0-9]{32}$")]


def store_for(request):
    if request.app.state.store is None:
        raise HTTPException(503, "execution_service_disabled")
    return request.app.state.store


@router.get("/profiles", response_model=list[ProfileReadiness])
def profiles(request: Request):
    store = store_for(request)
    ready = {item["name"]: item for item in store.readiness()}
    return [
        ready.get(
            name,
            {"name": name, "ready": False, "reason": "worker_or_profile_not_ready"},
        )
        for name in PROFILES
    ]


@router.post(
    "",
    response_model=JobStatus,
    status_code=202,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/JobSubmission"}
                }
            },
        }
    },
)
async def submit(request: Request):
    store = store_for(request)
    try:
        submission = JobSubmission.model_validate(
            await json_body(request, MAX_JOB_BYTES)
        )
    except ValidationError as exc:
        raise HTTPException(422, "invalid_job_submission") from exc
    available = {item["name"] for item in store.readiness() if item["ready"]}
    if submission.profile not in available:
        raise HTTPException(409, "profile_not_ready")
    try:
        return store.submit(submission.model_dump())
    except QueueFull as exc:
        raise HTTPException(429, "job_queue_or_storage_full") from exc


@router.get("", response_model=list[JobStatus])
def list_jobs(request: Request):
    return store_for(request).list()


@router.get("/{identity}", response_model=JobStatus)
def job(identity: JobID, request: Request):
    value = store_for(request).get(identity)
    if value is None:
        raise HTTPException(404, "job_not_found")
    return value


@router.post("/{identity}/cancel", response_model=JobStatus)
def cancel(identity: JobID, request: Request):
    job(identity, request)
    return store_for(request).cancel(identity)


@router.delete("/{identity}", status_code=204)
def delete(identity: JobID, request: Request):
    job(identity, request)
    try:
        store_for(request).delete(identity)
    except ValueError as exc:
        raise HTTPException(409, "job_still_active") from exc
    return Response(status_code=204)


@router.get("/{identity}/report")
def report(identity: JobID, request: Request):
    job(identity, request)
    value = store_for(request).report(identity)
    if value is None:
        raise HTTPException(409, "job_report_not_available")
    return value


@router.get("/{identity}/view")
def view(identity: JobID, request: Request):
    return build_workspace_view(report(identity, request))


@router.get("/{identity}/source")
def source(identity: JobID, request: Request):
    job(identity, request)
    value = store_for(request).source(identity)
    if value is None:
        raise HTTPException(409, "job_source_not_retained")
    return value
