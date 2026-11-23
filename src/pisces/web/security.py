from __future__ import annotations

import hmac
from urllib.parse import urlparse

from starlette.responses import JSONResponse


def configure_security(app, settings):
    @app.middleware("http")
    async def security(request, call_next):
        origin = request.headers.get("origin")
        try:
            valid_origin = (
                not origin
                or origin in settings.allowed_origins
                or (
                    not settings.production
                    and urlparse(origin).hostname in ("localhost", "127.0.0.1", "::1")
                )
            )
        except ValueError:
            valid_origin = False
        if not valid_origin:
            return JSONResponse({"detail": "non_local_origin"}, status_code=403)
        if settings.token and request.url.path != "/healthz":
            provided = request.headers.get("authorization", "")
            if not hmac.compare_digest(
                provided.encode("utf-8"), ("Bearer " + settings.token).encode("utf-8")
            ):
                return JSONResponse(
                    {"detail": "authentication_required"},
                    status_code=401,
                    headers={"WWW-Authenticate": "Bearer"},
                )
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response
