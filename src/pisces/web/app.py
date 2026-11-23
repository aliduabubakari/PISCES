from __future__ import annotations

from copy import deepcopy
import os

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from starlette.middleware.trustedhost import TrustedHostMiddleware

from pisces.report_views import view_schema
from pisces.report_views.campaigns import campaign_schema
from pisces.service.models import JobSubmission
from pisces.service.settings import Settings
from pisces.service.store import JobStore
from .jobs import router as jobs_router
from .reports import router as reports_router
from .security import configure_security


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    if settings.state_dir is not None and not settings.token:
        raise ValueError("Execution requires authentication")
    os.umask(0o007)
    app = FastAPI(
        title="PISCES evaluation API",
        version="0.2.0",
        docs_url=None if settings.production else "/docs",
        redoc_url=None,
    )
    app.state.store = JobStore(settings.state_dir) if settings.state_dir else None
    app.state.settings = settings
    app.add_middleware(
        TrustedHostMiddleware, allowed_hosts=list(settings.allowed_hosts)
    )
    configure_security(app, settings)
    app.include_router(reports_router)
    app.include_router(jobs_router)

    @app.get("/healthz", include_in_schema=False)
    def health():
        return {"status": "ok"}

    def openapi():
        if app.openapi_schema is None:
            document = get_openapi(
                title=app.title, version=app.version, routes=app.routes
            )

            def relocate(value, name):
                if isinstance(value, dict):
                    ref = value.get("$ref")
                    if isinstance(ref, str) and ref.startswith("#/$defs/"):
                        value["$ref"] = ref.replace(
                            "#/$defs/", f"#/components/schemas/{name}/$defs/", 1
                        )
                    for child in value.values():
                        relocate(child, name)
                elif isinstance(value, list):
                    for child in value:
                        relocate(child, name)

            for name, source in (
                ("ReportView", view_schema()),
                ("CampaignView", campaign_schema()),
                ("JobSubmission", JobSubmission.model_json_schema()),
            ):
                schema = deepcopy(source)
                relocate(schema, name)
                document.setdefault("components", {}).setdefault("schemas", {})[
                    name
                ] = schema
            if settings.token:
                document["components"].setdefault("securitySchemes", {})[
                    "BearerAuth"
                ] = {"type": "http", "scheme": "bearer"}
                document["security"] = [{"BearerAuth": []}]
            app.openapi_schema = document
        return app.openapi_schema

    app.openapi = openapi
    return app
