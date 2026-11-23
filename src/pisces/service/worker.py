from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import time

from pisces.energy.oci_runtime import _local_engine
from .executor import DockerExecutor
from .models import PROFILES
from .store import JobStore


def main():
    os.umask(0o007)
    directory = Path(os.environ.get("PISCES_STATE_DIR", "/var/lib/pisces"))
    store = JobStore(directory)
    default_image = os.environ.get(
        "PISCES_DEFAULT_EXECUTOR_IMAGE", "pisces-executor:local"
    )
    images = (
        json.loads(os.environ["PISCES_EXECUTION_IMAGES"])
        if os.environ.get("PISCES_EXECUTION_IMAGES")
        else {"static": default_image, "python": default_image}
    )
    if (
        not isinstance(images, dict)
        or not images
        or any(
            key not in PROFILES or not isinstance(value, str)
            for key, value in images.items()
        )
    ):
        raise ValueError(
            "PISCES_EXECUTION_IMAGES must be an operator-controlled profile/image mapping"
        )
    with (directory / "worker.lock").open("a") as lock, _local_engine() as cli:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        executor = DockerExecutor(cli, store.deployment_id(), {})
        executor.recover()
        store.recover()
        executor = DockerExecutor(cli, store.deployment_id(), images)
        while True:
            store.heartbeat(executor.profiles)
            job = store.claim()
            if job:
                executor.execute(job, store)
            else:
                time.sleep(0.5)


if __name__ == "__main__":
    main()
