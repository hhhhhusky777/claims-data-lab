"""Coordinate incremental file transformation and a PostgreSQL import."""

import json
import logging
from contextlib import closing
from dataclasses import asdict
from pathlib import Path
from typing import Literal

import psycopg

from . import database
from .errors import RowValidationError
from .models import ImportedPayout, PipelineResult, RejectedRecord
from .readers import read_csv, read_jsonl
from .transform import transform_record


logger = logging.getLogger(__name__)


def run_pipeline(
    input_path: Path,
    input_format: Literal["csv", "jsonl"],
    database_url: str,
    reject_path: Path,
    batch_rows: int,
) -> PipelineResult:
    """Import one file using bounded groups inside a writer-owned transaction.

    Supply an existing input file, its explicit format, a Psycopg connection
    string and a positive integer batch_rows. reject_path names a diagnostic
    file in an existing directory. Diagnostics append to it across invocations.
    The input file must be distinct from that output, including filesystem
    aliases, to preserve source bytes. No credentials are logged.

    Expected row errors stream to reject_path as located JSONL diagnostics.
    The artifact is diagnostic and may survive a rolled-back import. Its flush
    and close happen before database commit, so output failures abort the file.
    Each successful run appends rows, including reimports of the same file.

    Returns committed inserted/rejected counts only after successful commit.
    Fatal read, output and database exceptions propagate with stage/location
    notes and original causes, with no automatic retry. Confirm database state
    before retrying an interrupted COMMIT. Memory is O(largest input record +
    batch_rows transformed records), with no hard byte ceiling.
    """
    if type(batch_rows) is not int or batch_rows <= 0:
        raise ValueError("batch_rows must be a positive integer")
    readers = {"csv": read_csv, "jsonl": read_jsonl}
    reader = readers[input_format](input_path)
    inserted = rejected = 0
    batch: list[ImportedPayout] = []
    stage = "connect"
    source_record = None

    try:
        with closing(reader), psycopg.connect(database_url, autocommit=True) as connection:
            with database.transaction(connection):
                stage = "create_table"
                database.create_tables(connection)
                stage = "open_reject_output"
                if input_path.resolve() == reject_path.resolve() or (
                    reject_path.exists() and reject_path.samefile(input_path)
                ):
                    raise ValueError("Rejection output must be distinct from the input file")
                with reject_path.open("a", encoding="utf-8") as rejects:
                    stage = "read"
                    for record in reader:
                        source_record = record.source_record
                        if isinstance(record, RejectedRecord):
                            rejection = record
                        else:
                            stage = "transform"
                            try:
                                payout = transform_record(record)
                            except RowValidationError as error:
                                rejection = RejectedRecord(
                                    record.source_file, record.source_record, str(error)
                                )
                            else:
                                batch.append(payout)
                                if len(batch) == batch_rows:
                                    stage = "insert_batch"
                                    inserted += database.insert_batch(connection, batch)
                                    batch.clear()
                                stage = "read"
                                continue

                        stage = "write_rejection"
                        rejects.write(json.dumps(asdict(rejection)) + "\n")
                        rejected += 1
                        stage = "read"

                    if batch:
                        stage = "insert_final_batch"
                        inserted += database.insert_batch(connection, batch)
                    stage = "close_reject_output"
                stage = "commit"
            stage = "close_connection"
    except Exception as error:
        error.add_note(
            f"Import stage={stage}, source={input_path}, last_record={source_record}. "
            "A rejection artifact may exist even when the database import failed."
        )
        logger.error(
            "Import failed: stage=%s source=%s last_record=%s error_type=%s",
            stage, input_path, source_record, type(error).__name__,
        )
        raise

    logger.info("Import committed: source=%s inserted=%d rejected=%d", input_path, inserted, rejected)
    return PipelineResult(inserted=inserted, rejected=rejected)
