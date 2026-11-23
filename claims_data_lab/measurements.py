"""Small import measurements sharing Python logging and a benchmark run ID."""

import logging
from collections import defaultdict
from contextlib import contextmanager
from time import perf_counter


class ImportMeasurements:
    """Aggregate elapsed stage seconds, never one log message per input row.

    Timers measure instrumented wall time, including failed calls. Stages may
    nest: SQL is part of driver consumption, so do not sum every stage as total.
    Logging uses the project's standard logging framework and one run context.
    """

    def __init__(self, run_id: str):
        self.run_id = run_id
        self.seconds = defaultdict(float)
        self.calls = defaultdict(int)
        self.logger = logging.LoggerAdapter(logging.getLogger(__name__), {"run_id": run_id})

    def record(self, stage: str, seconds: float) -> None:
        """Accumulate one completed or failed operation in seconds."""
        self.seconds[stage] += seconds
        self.calls[stage] += 1

    @contextmanager
    def measure(self, stage: str):
        """Measure an operation without altering its return value or exception."""
        started = perf_counter()
        try:
            yield
        finally:
            self.record(stage, perf_counter() - started)

    def call(self, stage, function, *args, **kwargs):
        """Time an existing synchronous operation, preserving its original cause."""
        with self.measure(stage):
            return function(*args, **kwargs)

    def iterate(self, stage, records):
        """Measure incremental reads, including EOF, and close the owned reader."""
        try:
            while True:
                with self.measure(stage):
                    try:
                        record = next(records)
                    except StopIteration:
                        return
                yield record
        finally:
            records.close()

    def snapshot(self) -> dict:
        """Return JSON-compatible aggregate evidence correlated to this run."""
        return {"run_id": self.run_id, "stage_seconds": dict(self.seconds), "stage_calls": dict(self.calls)}

    def log_summary(self) -> None:
        """Emit one aggregate record through shared logging handlers."""
        self.logger.info("Import measurements: %s", self.snapshot())
