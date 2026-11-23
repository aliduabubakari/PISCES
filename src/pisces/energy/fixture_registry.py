"""Deterministic in-process HTTP, urllib, and FTP fixture mocks."""

from __future__ import annotations

import contextlib
import ftplib
import io
import json
from pathlib import Path
from typing import Any, Dict, Iterator, List
import urllib.request

from .workload_contract import LoadedWorkloadContract, WorkloadContractError


class UnregisteredExternalAccess(WorkloadContractError):
    pass


class _Headers(dict):
    def get_content_charset(self, default: str | None = None) -> str | None:
        return default


class FixtureResponse(io.BytesIO):
    def __init__(self, content: bytes, *, url: str):
        super().__init__(content)
        self.content = content
        self.url = url
        self.status = 200
        self.status_code = 200
        self.headers = _Headers({"Content-Type": "application/octet-stream"})

    @property
    def text(self) -> str:
        return self.content.decode("utf-8")

    def raise_for_status(self) -> None:
        return None

    def getcode(self) -> int:
        return 200

    def json(self) -> Any:
        return json.loads(self.text)

    def iter_content(self, chunk_size: int = 8192) -> Iterator[bytes]:
        payload = self.content
        size = max(1, int(chunk_size))
        for offset in range(0, len(payload), size):
            yield payload[offset : offset + size]

    def __enter__(self) -> "FixtureResponse":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class FixtureRegistry:
    def __init__(
        self,
        workload: LoadedWorkloadContract,
        *,
        fixture_paths: Dict[str, Path] | None = None,
    ):
        self.workload = workload
        self.fixture_paths = fixture_paths or workload.fixtures
        bindings = workload.binding.get("integration_bindings") or {}
        self.locators = {str(key): str(value) for key, value in bindings.items()}
        self.access_log: List[Dict[str, Any]] = []

    def resolve(self, locator: str, *, method: str) -> bytes:
        integration_id = self.locators.get(locator)
        if integration_id is None:
            self.access_log.append(
                {"locator": locator, "method": method, "status": "blocked"}
            )
            raise UnregisteredExternalAccess(
                f"unregistered_external_access:{method}:{locator}"
            )
        fixture = self.fixture_paths[integration_id]
        payload = fixture.read_bytes()
        self.access_log.append(
            {
                "locator": locator,
                "method": method,
                "status": "served",
                "integration_id": integration_id,
                "bytes": len(payload),
            }
        )
        return payload


@contextlib.contextmanager
def patched_external_services(
    registry: FixtureRegistry,
) -> Iterator[FixtureRegistry]:
    original_urlopen = urllib.request.urlopen
    original_urlretrieve = urllib.request.urlretrieve
    original_ftp = ftplib.FTP
    requests_session_request = None
    requests_api_request = None

    def urlopen(request: Any, *args: Any, **kwargs: Any) -> FixtureResponse:
        locator = (
            request.full_url
            if isinstance(request, urllib.request.Request)
            else str(request)
        )
        return FixtureResponse(
            registry.resolve(locator, method="urllib"),
            url=locator,
        )

    def urlretrieve(
        url: str,
        filename: str | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        payload = registry.resolve(str(url), method="urllib:urlretrieve")
        destination = Path(filename or Path(str(url)).name)
        destination.write_bytes(payload)
        return str(destination), _Headers(
            {"Content-Type": "application/octet-stream"}
        )

    class MockFTP:
        def __init__(self, host: str, *args: Any, **kwargs: Any):
            self.host = host
            self.directory = "/"

        def __enter__(self) -> "MockFTP":
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def login(self, *args: Any, **kwargs: Any) -> str:
            return "fixture-login-ok"

        def cwd(self, dirname: str) -> str:
            if str(dirname).startswith("/"):
                self.directory = str(dirname)
            else:
                self.directory = f"{self.directory.rstrip('/')}/{dirname}"
            return "fixture-cwd-ok"

        def pwd(self) -> str:
            return self.directory

        def retrbinary(self, command: str, callback: Any, *args: Any, **kwargs: Any) -> str:
            remote_path = command.removeprefix("RETR").strip()
            if not remote_path.startswith("/"):
                remote_path = f"{self.directory.rstrip('/')}/{remote_path}"
            locator = f"ftp://{self.host}/{remote_path.lstrip('/')}"
            callback(registry.resolve(locator, method="ftp"))
            return "fixture-transfer-ok"

        def quit(self) -> str:
            return "fixture-quit-ok"

    try:
        urllib.request.urlopen = urlopen  # type: ignore[assignment]
        urllib.request.urlretrieve = urlretrieve  # type: ignore[assignment]
        ftplib.FTP = MockFTP  # type: ignore[assignment]
        try:
            import requests

            requests_session_request = requests.sessions.Session.request
            requests_api_request = requests.api.request

            def request(
                session_or_method: Any,
                method_or_url: str,
                url: str | None = None,
                *args: Any,
                **kwargs: Any,
            ) -> FixtureResponse:
                if url is None:
                    method = str(session_or_method)
                    locator = str(method_or_url)
                else:
                    method = str(method_or_url)
                    locator = str(url)
                return FixtureResponse(
                    registry.resolve(locator, method=f"requests:{method.lower()}"),
                    url=locator,
                )

            requests.sessions.Session.request = request  # type: ignore[assignment]

            def api_request(method: str, url: str, *args: Any, **kwargs: Any) -> FixtureResponse:
                return FixtureResponse(
                    registry.resolve(str(url), method=f"requests:{method.lower()}"),
                    url=str(url),
                )

            requests.api.request = api_request  # type: ignore[assignment]
        except ImportError:
            pass
        yield registry
    finally:
        urllib.request.urlopen = original_urlopen  # type: ignore[assignment]
        urllib.request.urlretrieve = original_urlretrieve  # type: ignore[assignment]
        ftplib.FTP = original_ftp  # type: ignore[assignment]
        if requests_session_request is not None and requests_api_request is not None:
            import requests

            requests.sessions.Session.request = requests_session_request  # type: ignore[assignment]
            requests.api.request = requests_api_request  # type: ignore[assignment]
