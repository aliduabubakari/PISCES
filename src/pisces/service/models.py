from __future__ import annotations

import base64
import binascii
from pathlib import PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

PROFILES = ("static", "python", "airflow", "prefect", "dagster")
MAX_JOB_BYTES = 8 * 1024 * 1024


class FixtureFile(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    path: str = Field(max_length=160)
    content_base64: str = Field(max_length=8 * 1024 * 1024)

    @field_validator("path")
    @classmethod
    def confined(cls, value):
        path = PurePosixPath(value)
        if (
            not value
            or value.startswith(".")
            or "\\" in value
            or ":" in value
            or "\x00" in value
            or path.is_absolute()
            or str(path) != value
            or any(part in (".", "..") for part in value.split("/"))
            or len(path.parts) > 5
            or path.suffix.lower() not in (".csv", ".json", ".txt")
        ):
            raise ValueError(
                "Only confined relative CSV/JSON/text fixture paths are accepted"
            )
        return value

    def decoded(self):
        try:
            return base64.b64decode(self.content_base64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("Invalid base64 fixture") from exc


class JobSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    filename: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}\.py$")
    source: str = Field(min_length=1, max_length=1024 * 1024)
    profile: Literal["static", "python", "airflow", "prefect", "dagster"] = "static"
    timeout_s: int = Field(default=60, ge=5, le=120)
    contract: str | None = Field(default=None, max_length=1024 * 1024)
    binding: str | None = Field(default=None, max_length=1024 * 1024)
    files: list[FixtureFile] = Field(default_factory=list, max_length=16)

    @model_validator(mode="after")
    def validate_bundle(self):
        if "\x00" in self.source or len(self.source.encode("utf-8")) > 1024 * 1024:
            raise ValueError("Source must be UTF-8 Python text up to 1 MiB")
        native = self.profile in ("airflow", "prefect", "dagster")
        if native and (not self.contract or not self.binding):
            raise ValueError(
                "Native engine execution requires a v2 contract and artifact binding"
            )
        if not native and (
            self.contract is not None or self.binding is not None or self.files
        ):
            raise ValueError(
                "Fixtures are accepted only for contract-bound native profiles"
            )
        if len({file.path for file in self.files}) != len(self.files):
            raise ValueError("Duplicate fixture paths")
        if any(file.path == "contract.json" for file in self.files):
            raise ValueError("Fixture cannot replace the workload contract")
        if sum(len(file.decoded()) for file in self.files) > 6 * 1024 * 1024:
            raise ValueError("Decoded fixtures exceed 6 MiB")
        return self


class JobStatus(BaseModel):
    id: str
    filename: str
    profile: str
    artifact_sha256: str
    state: Literal["queued", "running", "completed", "failed", "cancelled", "timed_out"]
    created_at: float
    started_at: float | None
    finished_at: float | None
    cancel_requested: bool
    error: str | None
    has_report: bool
    evaluation_succeeded: bool | None


class ProfileReadiness(BaseModel):
    name: str
    ready: bool
    image_id: str | None = None
    reason: str | None = None
