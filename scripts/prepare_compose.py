"""Create a private local deployment token once, without printing its value."""

from pathlib import Path
import secrets

path = Path(".secrets/api-token")
path.parent.mkdir(mode=0o700, exist_ok=True)
path.parent.chmod(0o700)
if not path.exists():
    with path.open("x") as stream:
        path.chmod(0o644)
        stream.write(secrets.token_urlsafe(48) + "\n")
print(f"API token file ready: {path}. Use it in the frontend's Connect API dialog.")
