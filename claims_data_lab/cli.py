"""Command-line entry point for the PostgreSQL file import."""

import argparse
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path

from .pipeline import run_pipeline


def _positive_integer(value: str) -> int:
    """Validate batch size before opening any files or database connection."""
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("batch rows must be a positive integer") from error
    if number <= 0:
        raise argparse.ArgumentTypeError("batch rows must be a positive integer")
    return number


def main(argv: Sequence[str] | None = None) -> int:
    """Parse import arguments, invoke the pipeline, and display its outcome.

    Credentials come only from DATABASE_URL. Successful imports print JSON
    committed counts and return 0, including imports with rejected rows.
    Fatal pipeline failures return 1. Argument/environment errors use argparse's
    exit status 2. No connection string, raw database error, or traceback is
    printed by this boundary; pipeline stage logs identify where it failed.

    The original exception remains available within the pipeline's Python API.
    An uncertain COMMIT requires database inspection before retrying, because
    repeated imports append records. See `python -m claims_data_lab --help`.
    """
    parser = argparse.ArgumentParser(description="Transform one CSV/JSONL file into PostgreSQL rows.")
    parser.add_argument("--input", type=Path, required=True, help="Source file path")
    parser.add_argument("--format", choices=("csv", "jsonl"), required=True, help="Input file format")
    parser.add_argument("--rejects", type=Path, required=True, help="Diagnostic JSONL path; appended across runs")
    parser.add_argument("--batch-rows", type=_positive_integer, required=True, help="Maximum records per insert group")
    parser.add_argument("--engine", choices=("sequential", "spark"), default="sequential", help="Transformation engine; default requires no Spark")
    args = parser.parse_args(argv)
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        parser.error("set DATABASE_URL to a local development PostgreSQL connection string")

    try:
        options = {"engine": "spark"} if args.engine == "spark" else {}
        result = run_pipeline(args.input, args.format, database_url, args.rejects, args.batch_rows, **options)
    except Exception as error:
        print(
            f"Import failed ({type(error).__name__}). Check the pipeline stage log. "
            "No success counts are available. If COMMIT was interrupted, inspect "
            "PostgreSQL before retrying; reimports append rows.",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(asdict(result)))
    return 0
