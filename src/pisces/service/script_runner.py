"""In-container script process; never used by the API or controller."""

from __future__ import annotations

import json
from pathlib import Path
import runpy
import sys

from pisces.energy.operational_metrics import (
    start_operational_measurement,
    stop_operational_measurement,
)


def main():
    source, output = map(Path, sys.argv[1:])
    started = start_operational_measurement()
    code = 0
    sys.argv = [str(source)]
    try:
        runpy.run_path(str(source), run_name="__main__")
    except SystemExit as exc:
        code = exc.code if type(exc.code) is int else 0 if exc.code is None else 1
    except BaseException:
        code = 1
    metrics = stop_operational_measurement(started, scope="bounded_execution_block")
    output.write_text(json.dumps(metrics, allow_nan=False), encoding="utf-8")
    raise SystemExit(code)


if __name__ == "__main__":
    main()
