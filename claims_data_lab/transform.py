"""Fixed field mapping into PostgreSQL-compatible payout values."""

import re
from datetime import date

from .errors import RowValidationError
from .models import ImportedPayout, SourceRecord


def _required_text(values: dict[str, object], field: str) -> str:
    """Return a required nonempty string without altering the supplied value."""
    value = values.get(field)
    if not isinstance(value, str) or not value:
        raise RowValidationError(f"{field} must be a nonempty string")
    return value


def _bigint(record: SourceRecord, field: str) -> int:
    """Convert CSV signed integer text or check a native JSON integer.

    Reject booleans, floats and values outside PostgreSQL's signed BIGINT range.
    Numeric JSON strings are rejected; CSV numeric strings are required.
    """
    value = record.values.get(field)
    if record.input_format == "csv":
        if not isinstance(value, str) or re.fullmatch(r"[+-]?[0-9]+", value) is None:
            raise RowValidationError(f"{field} must be signed decimal integer text")
        # Strip leading zeros before conversion so Python's digit-count guard
        # does not reject a representable value such as a long string of zeros.
        digits = value.lstrip("+-").lstrip("0") or "0"
        if len(digits) > 19:
            raise RowValidationError(f"{field} is outside the PostgreSQL BIGINT range")
        value = int(digits) * (-1 if value.startswith("-") else 1)
    if type(value) is not int:
        raise RowValidationError(f"{field} must be an integer")
    if not -(2**63) <= value < 2**63:
        raise RowValidationError(f"{field} is outside the PostgreSQL BIGINT range")
    return value


def transform_record(record: SourceRecord) -> ImportedPayout:
    """Map required fields to typed columns, preserving source location.

    Returns ImportedPayout with datetime.date and signed integer values. Extra
    input fields are ignored. No duplicate, version ordering or policy lookup
    interpretation is performed, and the source dictionary is left unchanged.

    Raises RowValidationError for missing/null fields, invalid types, invalid
    YYYY-MM-DD dates or integer overflow. Conversion errors retain their cause.
    For example, a CSV amount_cents='-2000' becomes integer -2000.
    """
    values = record.values
    day = _required_text(values, "settled_on")
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", day) is None:
        raise RowValidationError("settled_on must use YYYY-MM-DD")
    try:
        settled_on = date.fromisoformat(day)
    except ValueError as error:
        raise RowValidationError("settled_on must be a valid calendar date") from error
    return ImportedPayout(
        source_file=record.source_file,
        source_record=record.source_record,
        payout_id=_required_text(values, "payout_id"),
        version=_bigint(record, "version"),
        policy_id=_required_text(values, "policy_id"),
        settled_on=settled_on,
        currency=_required_text(values, "currency"),
        amount_cents=_bigint(record, "amount_cents"),
    )
