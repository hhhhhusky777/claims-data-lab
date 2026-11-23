"""Connect either transformation engine to one driver-owned PostgreSQL writer."""

import json
import logging
from contextlib import closing, contextmanager, nullcontext
from dataclasses import asdict
from pathlib import Path
from typing import Literal

import psycopg

from . import database
from .measurements import ImportMeasurements
from .models import ImportProgress, PipelineResult, RejectedRecord
from .readers import read_csv, read_jsonl
from .transform import convert_record


logger = logging.getLogger(__name__)


def _sequential_outcomes(reader, progress, measurements):
    """Keep incremental parsing/conversion in this process, with located errors."""
    if measurements is not None:
        reader = measurements.iterate("read", reader)
    with closing(reader):
        while True:
            progress.stage = "read"
            try:
                record = next(reader)
            except StopIteration:
                return
            progress.source_record = record.source_record
            if isinstance(record, RejectedRecord):
                yield record
            else:
                progress.stage = "transform"
                yield convert_record(record) if measurements is None else measurements.call("transform", convert_record, record)


@contextmanager
def _outcome_source(reader, engine, progress, measurements):
    """Choose a source without importing Spark for sequential callers."""
    if engine == "spark":
        from .spark_transform import spark_outcomes
        with spark_outcomes(reader, progress, measurements) as outcomes:
            yield outcomes
    else:
        with closing(_sequential_outcomes(reader, progress, measurements)) as outcomes:
            yield outcomes


def _write_outcomes(connection, outcomes, rejects, batch_rows, progress, measurements):
    """Load converted payouts/rejections; only the driver has these write handles.

    The caller owns the transaction. Counts here are staged until it commits;
    every accepted row is appended, including repeated IDs and file reimports.
    """
    inserted = rejected = 0
    batch = []
    insert = (lambda records: database.insert_batch(connection, records)) if measurements is None else (
        lambda records: measurements.call("insert", database.insert_batch, connection, records)
    )
    for record in outcomes:
        progress.source_record = record.source_record
        if isinstance(record, RejectedRecord):
            progress.stage = "write_rejection"
            rejects.write(json.dumps(asdict(record)) + "\n")
            rejected += 1
        else:
            batch.append(record)
            if len(batch) == batch_rows:
                progress.stage = "insert_batch"
                inserted += insert(batch)
                batch.clear()
    if batch:
        progress.stage = "insert_final_batch"
        inserted += insert(batch)
    return PipelineResult(inserted, rejected)


def run_pipeline(
    input_path: Path,
    input_format: Literal["csv", "jsonl"],
    database_url: str,
    reject_path: Path,
    batch_rows: int,
    *,
    measurements: ImportMeasurements | None = None,
    engine: Literal["sequential", "spark"] = "sequential",
) -> PipelineResult:
    """Import one real file through a common writer and one file transaction.

    Existing calls default to sequential and require no Spark. Spark stages the
    same reader outputs, converts pure partitions, materializes on disk, then
    returns payouts/rejections to this driver's bounded insert groups.

    Expected row errors append located JSONL diagnostics. Fatal source, Spark,
    output or database errors abort and preserve original causes/stage context.
    Reject output must be distinct from the input, including filesystem aliases;
    output/source cleanup finishes before commit. Diagnostics may survive a
    rollback. No automatic retry or engine fallback occurs. Inspect PostgreSQL
    before retrying an interrupted COMMIT; successful reimports append again.

    Sequential memory depends on largest record plus batch_rows payouts. Spark
    additionally has JVM/workers, staging/cache and largest-partition iteration
    overhead; neither engine promises a hard byte limit. Optional measurements
    use shared logging/run context. SQL time nests inside driver consumption.
    """
    if type(batch_rows) is not int or batch_rows <= 0:
        raise ValueError("batch_rows must be a positive integer")
    reader = {"csv": read_csv, "jsonl": read_jsonl}[input_format](input_path)
    progress = ImportProgress()
    log = logger if measurements is None else logging.LoggerAdapter(logger, {"run_id": measurements.run_id})
    timer = measurements.measure if measurements is not None else lambda stage: nullcontext()
    try:
        connect = lambda: psycopg.connect(database_url, autocommit=True)
        with closing(reader), (connect() if measurements is None else measurements.call("connect", connect)) as connection:
            transaction = database.transaction(connection) if measurements is None else database.transaction(connection, measurements=measurements)
            with transaction:
                progress.stage = "create_table"
                database.create_tables(connection)
                progress.stage = "open_reject_output"
                if input_path.resolve() == reject_path.resolve() or (
                    reject_path.exists() and reject_path.samefile(input_path)
                ):
                    raise ValueError("Rejection output must be distinct from the input file")
                with reject_path.open("a", encoding="utf-8") as rejects:
                    with _outcome_source(reader, engine, progress, measurements) as outcomes:
                        with timer("consume"):
                            result = _write_outcomes(connection, outcomes, rejects, batch_rows, progress, measurements)
                    progress.stage = "close_reject_output"
                progress.stage = "commit"
            progress.stage = "close_connection"
    except Exception as error:
        error.add_note(
            f"Import stage={progress.stage}, source={input_path}, last_record={progress.source_record}. "
            "A rejection artifact may exist even when the database import failed."
        )
        log.error("Import failed: stage=%s source=%s last_record=%s error_type=%s",
                  progress.stage, input_path, progress.source_record, type(error).__name__)
        raise
    log.info("Import committed: source=%s inserted=%d rejected=%d", input_path, result.inserted, result.rejected)
    return result
