"""Optional HTTP transport; importing the CLI does not require this module."""

from .app import create_app

__all__ = ["create_app"]
