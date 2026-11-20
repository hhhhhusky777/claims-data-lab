"""Incremental readers for real CSV and JSON Lines input files."""

import csv
import json
from collections.abc import Iterator
from pathlib import Path

from .models import PAYOUT_FIELDS, RejectedRecord, SourceRecord


def read_csv(path: Path) -> Iterator[SourceRecord | RejectedRecord]:
    """Yield located CSV records with original string values.

    Opens UTF-8 with optional BOM and newline='' so the csv module handles
    quoted newlines. Required columns may appear in any order. Extra columns
    and extra row values are ignored. Short rows retain missing fields for
    transformation to reject. Blank data records are also yielded for validation.

    Raises ValueError for an absent/incomplete header, csv.Error for malformed
    framing, and the original UnicodeError/OSError for decoding or I/O failure.
    These failures abort the file. The file closes on exhaustion or when the
    caller closes the generator after stopping early.
    """
    with path.open(encoding="utf-8-sig", newline="") as file:
        reader = csv.reader(file, strict=True)
        header = next(reader, [])
        missing = set(PAYOUT_FIELDS) - set(header)
        if missing:
            raise ValueError(f"{path}: missing CSV columns: {', '.join(sorted(missing))}")
        for number, fields in enumerate(reader, start=1):
            yield SourceRecord(str(path), number, dict(zip(header, fields)), "csv")


def read_jsonl(path: Path) -> Iterator[SourceRecord | RejectedRecord]:
    """Yield one located object or rejection per physical JSONL line.

    Opens UTF-8 with optional BOM. Malformed JSON, blank lines and non-object
    values yield RejectedRecord and allow subsequent lines to be read. JSON
    fields keep their native types so transformation can validate them.

    File decoding and I/O failures propagate as fatal errors. Memory depends
    on the largest line, not the number of lines. The file closes when the
    generator finishes or the caller explicitly closes it.
    """
    with path.open(encoding="utf-8-sig") as file:
        for number, line in enumerate(file, start=1):
            try:
                values = json.loads(line)
            except ValueError as error:
                # json.loads can also raise ValueError for an oversized integer.
                reason = error.msg if isinstance(error, json.JSONDecodeError) else str(error)
                yield RejectedRecord(str(path), number, f"Invalid JSON: {reason}")
                continue
            if not isinstance(values, dict):
                yield RejectedRecord(str(path), number, "Expected a JSON object")
                continue
            yield SourceRecord(str(path), number, values)
