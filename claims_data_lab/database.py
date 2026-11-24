"""PostgreSQL schema, grouped inserts, and file-transaction ownership."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from time import perf_counter

from .measurements import ImportMeasurements

from psycopg import Connection

from .models import ImportedPayout


CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS imported_payouts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_record BIGINT NOT NULL,
    payout_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    policy_id TEXT NOT NULL,
    settled_on DATE NOT NULL,
    currency TEXT NOT NULL,
    amount_cents BIGINT NOT NULL
)
"""

INSERT_PAYOUT = """
INSERT INTO imported_payouts (
    source_file, source_record, payout_id, version,
    policy_id, settled_on, currency, amount_cents
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


@contextmanager
def transaction(connection: Connection, *, measurements: ImportMeasurements | None = None) -> Iterator[None]:
    """Own the transaction for one file on an idle PostgreSQL connection.

    The pipeline opens the connection with autocommit=True and enters this
    context before any SQL. Psycopg commits on normal exit and rolls back
    exceptions. Database helpers participate without committing themselves.

    Exceptions retain their identity and cause. A connection loss during COMMIT
    can leave the outcome unknown, so callers must inspect the database before
    retrying. This append-only import deliberately has no automatic retries.
    """
    started = perf_counter() if measurements is not None else None
    finishing = None
    finish_stage = "commit"
    try:
        with connection.transaction():
            if measurements is not None:
                measurements.record("begin", perf_counter() - started)
            try:
                yield
            except BaseException:
                finish_stage = "rollback"
                raise
            finally:
                if measurements is not None:
                    finishing = perf_counter()
    except BaseException as error:
        error.add_note(
            "File transaction failed. If COMMIT was interrupted, inspect PostgreSQL "
            "before retrying; a repeated successful import appends rows."
        )
        raise
    finally:
        if measurements is not None and finishing is not None:
            measurements.record(finish_stage, perf_counter() - finishing)


def create_tables(connection: Connection) -> None:
    """Create the fixed target table within the caller's file transaction.

    Existing tables are reused; this function performs no schema migration.
    PostgreSQL errors propagate to the transaction owner.
    """
    with connection.cursor() as cursor:
        cursor.execute(CREATE_TABLE)


def insert_batch(connection: Connection, records: Sequence[ImportedPayout]) -> int:
    """Insert a bounded group and return its staged row count.

    Uses SQL parameters so values are adapted by Psycopg, including date and
    signed integer fields. Every record is inserted, including repeated payout
    IDs and versions. This function never commits; its returned count becomes
    durable only when the containing file transaction commits.

    Empty groups perform no SQL. A database error propagates and causes the
    owning transaction to roll back. Parameter rows are generated lazily.
    """
    if not records:
        return 0
    parameters = (
        (
            row.source_file, row.source_record, row.payout_id, row.version,
            row.policy_id, row.settled_on, row.currency, row.amount_cents,
        )
        for row in records
    )
    with connection.cursor() as cursor:
        cursor.executemany(INSERT_PAYOUT, parameters)
    return len(records)
