from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    state_dir: Path | None = None
    token: str | None = None
    allowed_hosts: tuple[str, ...] = ("localhost", "127.0.0.1", "[::1]")
    allowed_origins: tuple[str, ...] = ()
    production: bool = False

    def __post_init__(self):
        if self.production and (not self.token or len(self.token) < 32):
            raise ValueError(
                "Production requires a secret API token of at least 32 characters"
            )
        if "*" in self.allowed_hosts or "*" in self.allowed_origins:
            raise ValueError("Explicit allowed hosts and origins are required")

    @classmethod
    def from_env(cls):
        production = os.environ.get("PISCES_ENV") == "production"
        token_file = os.environ.get("PISCES_API_TOKEN_FILE")
        token = (
            Path(token_file).read_text().strip()
            if token_file
            else os.environ.get("PISCES_API_TOKEN")
        )
        if production and (not token or len(token) < 32):
            raise ValueError(
                "Production requires a secret API token of at least 32 characters"
            )
        directory = os.environ.get("PISCES_STATE_DIR")
        if directory and not token:
            raise ValueError(
                "Execution service requires PISCES_API_TOKEN or PISCES_API_TOKEN_FILE"
            )
        hosts = tuple(
            os.environ.get("PISCES_ALLOWED_HOSTS", "localhost,127.0.0.1,[::1]").split(
                ","
            )
        )
        origins = tuple(
            filter(None, os.environ.get("PISCES_ALLOWED_ORIGINS", "").split(","))
        )
        if "*" in hosts or "*" in origins:
            raise ValueError("Explicit allowed hosts and origins are required")
        return cls(
            Path(directory) if directory else None, token, hosts, origins, production
        )
