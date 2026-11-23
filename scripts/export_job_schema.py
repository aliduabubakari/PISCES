"""Generate the frontend job contract from API-owned Pydantic models."""

import json
from pathlib import Path
from pisces.service.models import JobStatus, JobSubmission, ProfileReadiness


def job_schema():
    definitions = {}
    for model in (JobStatus, JobSubmission, ProfileReadiness):
        schema = model.model_json_schema()
        definitions.update(schema.pop("$defs", {}))
        definitions[model.__name__] = schema
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "JobContract",
        "type": "object",
        "properties": {
            "job": {"$ref": "#/$defs/JobStatus"},
            "submission": {"$ref": "#/$defs/JobSubmission"},
            "profile": {"$ref": "#/$defs/ProfileReadiness"},
        },
        "required": ["job", "submission", "profile"],
        "$defs": definitions,
    }


if __name__ == "__main__":
    Path("src/pisces/schemas/jobs-1.0.0.schema.json").write_text(
        json.dumps(job_schema(), indent=2) + "\n"
    )
