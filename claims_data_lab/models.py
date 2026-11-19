"""Data passed between readers, transformation, and the database writer."""

from dataclasses import dataclass
from datetime import date
from typing import Literal


PAYOUT_FIELDS = (
    "payout_id", "version", "policy_id", "settled_on", "currency", "amount_cents"
)


@dataclass(frozen=True)
class SourceRecord:
    """One parsed record with its origin and format, before type conversion.

    source_record is a 1-based CSV data-record number or JSONL physical line.
    input_format lets conversion distinguish CSV integer text from JSON values.
    values may contain extra fields, which transformation ignores.
    """

    source_file: str
    source_record: int
    values: dict[str, object]
    input_format: Literal["csv", "jsonl"] = "jsonl"


@dataclass(frozen=True)
class ImportedPayout:
    """Converted database values; PostgreSQL supplies the technical row ID.

    version and policy_id are payload, without update or relationship semantics.
    amount_cents is signed, preserving negative input values.
    """

    source_file: str
    source_record: int
    payout_id: str
    version: int
    policy_id: str
    settled_on: date
    currency: str
    amount_cents: int


@dataclass(frozen=True)
class RejectedRecord:
    """A recoverable row rejection, retaining a location and actionable reason."""

    source_file: str
    source_record: int
    reason: str


@dataclass(frozen=True)
class PipelineResult:
    """Counts returned only after a whole input file commits successfully."""

    inserted: int
    rejected: int
