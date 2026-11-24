"""Internal fresh-process importer used by the benchmark supervisor."""

import json
import logging
import os
import sys
from dataclasses import asdict
from pathlib import Path

from .measurements import ImportMeasurements
from .pipeline import run_pipeline


def main() -> int:
    """Read an owned config file; keep connection credentials only in the environment."""
    config = json.loads(Path(sys.argv[1]).read_text())
    run_id = config["run_id"]
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s run=%(run_id)s %(message)s", defaults={"run_id": run_id}))
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    measurements = ImportMeasurements(run_id)
    try:
        with measurements.measure("pipeline"):
            result = run_pipeline(
                Path(config["input"]), config["format"], os.environ["DATABASE_URL"],
                Path(config["rejects"]), 500, measurements=measurements, engine=config.get("engine", "sequential"),
            )
    except Exception as error:
        measurements.log_summary()
        print(f"Import failed: run={run_id} error_type={type(error).__name__}; inspect stage diagnostics before retry", file=sys.stderr)
        return 1
    measurements.log_summary()
    print(json.dumps({**asdict(result), **measurements.snapshot()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
