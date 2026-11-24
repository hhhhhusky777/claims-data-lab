"""Deterministic real CSV/JSONL workloads for the accepted Phase 2 profile."""

import csv
import hashlib
import json
from pathlib import Path

from .models import PAYOUT_FIELDS


SIZES = (100_000, 1_000_000)
SEED = 42


def payout_values(index: int, seed: int = SEED) -> dict:
    """Produce the accepted synthetic formula; index is zero-based.

    IDs intentionally repeat every 50,000 records. Negative amounts and repeated
    versions are ordinary payload. This generator does not infer update deltas.
    """
    return dict(zip(PAYOUT_FIELDS, (
        f"T{index % 50_000}", 1 + index % 5, f"P{index % 10_000}",
        "2026-11-18", "USD", ((index * 7919 + seed) % 200_001) - 100_000,
    )))


def file_evidence(path: Path) -> dict:
    """Hash a file incrementally; never load the entire workload into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while block := file.read(1024 * 1024):
            digest.update(block)
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def generate_workload(directory: Path, rows: int, seed: int = SEED) -> dict:
    """Write equivalent formats under an invocation-owned directory.

    The caller owns the output directory and its lifetime. Generate one record
    at a time; CSV uses the csv module and JSONL contains native integer values.
    Generation and hashing are outside the benchmark import interval.
    """
    directory.mkdir(parents=True, exist_ok=True)
    csv_path, jsonl_path = directory / f"payouts-{rows}.csv", directory / f"payouts-{rows}.jsonl"
    with csv_path.open("w", encoding="utf-8", newline="") as csv_file, jsonl_path.open("w", encoding="utf-8") as json_file:
        writer = csv.DictWriter(csv_file, fieldnames=PAYOUT_FIELDS)
        writer.writeheader()
        for index in range(rows):
            values = payout_values(index, seed)
            writer.writerow(values)
            json_file.write(json.dumps(values, separators=(",", ":")) + "\n")
    return {"rows": rows, "seed": seed, "csv": file_evidence(csv_path), "jsonl": file_evidence(jsonl_path)}
