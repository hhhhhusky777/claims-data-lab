"""T1 contracts exercised through actual CSV and JSONL files."""

import csv
import json
import tempfile
import unittest
from dataclasses import asdict
from datetime import date
from pathlib import Path

from claims_data_lab.errors import RowValidationError
from claims_data_lab.models import PAYOUT_FIELDS, RejectedRecord, SourceRecord
from claims_data_lab.readers import read_csv, read_jsonl
from claims_data_lab.transform import transform_record


ROOT = Path(__file__).resolve().parents[1]
PAYOUT = {
    "payout_id": "T1", "version": 1, "policy_id": "P1",
    "settled_on": "2026-11-18", "currency": "USD", "amount_cents": 10000,
}


class TransformationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def write(self, filename, content, encoding="utf-8"):
        path = self.root / filename
        path.write_text(content, encoding=encoding)
        return path

    def test_existing_jsonl_fixtures_preserve_all_payloads(self):
        for name, amounts in (
            ("2026-11-18.jsonl", [10000, 5000, 20000]),
            ("2026-11-19.jsonl", [4000, 7000, -2000, 3000]),
        ):
            with self.subTest(name=name):
                path = ROOT / "fixtures" / "payouts" / name
                rows = [transform_record(row) for row in read_jsonl(path)]
                self.assertEqual([row.amount_cents for row in rows], amounts)
                self.assertEqual([row.source_record for row in rows], list(range(1, len(rows) + 1)))
                self.assertEqual(rows[0].source_file, str(path))
                self.assertIsInstance(rows[0].settled_on, date)
        # P99 is retained: this exercise has no policy-reference validation.
        self.assertEqual(rows[-1].policy_id, "P99")

    def test_csv_and_jsonl_produce_equal_target_values(self):
        csv_path = self.root / "payout.csv"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=list(reversed(PAYOUT_FIELDS)) + ["extra"])
            writer.writeheader()
            writer.writerow({**PAYOUT, "amount_cents": -2000, "extra": "ignored"})
        json_path = self.write("payout.jsonl", json.dumps({**PAYOUT, "amount_cents": -2000}), "utf-8-sig")
        csv_row = asdict(transform_record(next(read_csv(csv_path))))
        json_row = asdict(transform_record(next(read_jsonl(json_path))))
        csv_row.pop("source_file")
        json_row.pop("source_file")
        self.assertEqual(csv_row, json_row)
        self.assertEqual(csv_row["amount_cents"], -2000)
        self.assertEqual(csv_row["settled_on"], date(2026, 11, 18))

    def test_csv_multiline_record_uses_logical_number(self):
        path = self.write("quoted.csv", ",".join(PAYOUT_FIELDS) + '\n"T\n1",1,P1,2026-11-18,USD,100\nT2,1,P1,2026-11-18,USD,200\n')
        rows = list(read_csv(path))
        self.assertEqual([row.source_record for row in rows], [1, 2])
        self.assertEqual(transform_record(rows[0]).payout_id, "T\n1")

    def test_csv_reader_yields_before_later_framing_error(self):
        path = self.write("broken.csv", ",".join(PAYOUT_FIELDS) + '\nT1,1,P1,2026-11-18,USD,100\n"unfinished')
        reader = read_csv(path)
        self.addCleanup(reader.close)
        self.assertEqual(transform_record(next(reader)).payout_id, "T1")
        with self.assertRaises(csv.Error):
            next(reader)

    def test_csv_missing_header_is_fatal(self):
        for content in ("", "payout_id,version\nT1,1\n"):
            with self.subTest(content=content):
                with self.assertRaisesRegex(ValueError, "missing CSV columns"):
                    list(read_csv(self.write("missing.csv", content)))

    def test_csv_header_only_is_empty_input(self):
        self.assertEqual(list(read_csv(self.write("empty.csv", ",".join(PAYOUT_FIELDS) + "\n"))), [])

    def test_short_and_blank_csv_records_reach_validation(self):
        path = self.write("short.csv", ",".join(PAYOUT_FIELDS) + "\nT1,1\n\n")
        rows = list(read_csv(path))
        self.assertEqual([row.source_record for row in rows], [1, 2])
        for row in rows:
            with self.assertRaises(RowValidationError):
                transform_record(row)

    def test_extra_csv_values_are_ignored(self):
        path = self.write("extra.csv", ",".join(PAYOUT_FIELDS) + "\nT1,1,P1,2026-11-18,USD,100,ignored\n")
        self.assertEqual(transform_record(next(read_csv(path))).amount_cents, 100)

    def test_jsonl_rejects_keep_physical_locations_and_continue(self):
        lines = [json.dumps(PAYOUT), "", "{bad", "null", "[]", "42", json.dumps(PAYOUT)]
        path = self.write("mixed.jsonl", "\n".join(lines) + "\n")
        rows = list(read_jsonl(path))
        self.assertIsInstance(rows[0], SourceRecord)
        self.assertIsInstance(rows[-1], SourceRecord)
        self.assertEqual(rows[-1].source_record, 7)
        self.assertEqual([row.source_record for row in rows if isinstance(row, RejectedRecord)], [2, 3, 4, 5, 6])
        self.assertTrue(all(row.reason for row in rows if isinstance(row, RejectedRecord)))

    def test_jsonl_reader_does_not_decode_later_lines_early(self):
        path = self.root / "invalid_encoding.jsonl"
        # A later physical line has invalid UTF-8 beyond the initial read buffer.
        path.write_bytes(json.dumps(PAYOUT).encode() + b"\n" + b" " * 20000 + b"\xff\n")
        reader = read_jsonl(path)
        self.addCleanup(reader.close)
        self.assertEqual(next(reader).source_record, 1)
        with self.assertRaises(UnicodeDecodeError):
            list(reader)

    def test_empty_jsonl_yields_nothing(self):
        self.assertEqual(list(read_jsonl(self.write("empty.jsonl", ""))), [])

    def test_extremely_large_json_integer_does_not_prevent_next_record(self):
        path = self.write("big.jsonl", '{"amount_cents":' + '9' * 5000 + '}\n' + json.dumps(PAYOUT))
        rows = list(read_jsonl(path))
        # With Python's digit guard this is a parser rejection. If the guard is
        # disabled, ordinary BIGINT validation rejects the parsed integer.
        if isinstance(rows[0], SourceRecord):
            with self.assertRaises(RowValidationError):
                transform_record(rows[0])
        else:
            self.assertIsInstance(rows[0], RejectedRecord)
        self.assertEqual(transform_record(rows[1]).amount_cents, 10000)

    def test_file_open_errors_propagate(self):
        for read in (read_csv, read_jsonl):
            with self.subTest(reader=read.__name__):
                with self.assertRaises(FileNotFoundError):
                    next(read(self.root / "absent"))

    def test_missing_null_empty_and_wrong_text_types(self):
        for field in PAYOUT_FIELDS:
            values = dict(PAYOUT)
            del values[field]
            with self.subTest(field=field, kind="missing"):
                with self.assertRaises(RowValidationError):
                    transform_record(SourceRecord("x", 1, values))
            with self.subTest(field=field, kind="null"):
                with self.assertRaises(RowValidationError):
                    transform_record(SourceRecord("x", 1, {**PAYOUT, field: None}))
        for field in ("payout_id", "policy_id", "currency"):
            for value in ("", 123, [], {}):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(RowValidationError):
                        transform_record(SourceRecord("x", 1, {**PAYOUT, field: value}))

    def test_json_integer_types_and_bigint_limits(self):
        for field in ("amount_cents", "version"):
            for value in (True, False, 1.0, "100", 2**63, -(2**63) - 1):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(RowValidationError):
                        transform_record(SourceRecord("x", 1, {**PAYOUT, field: value}))
            for value in (-(2**63), 0, 2**63 - 1):
                self.assertEqual(getattr(transform_record(SourceRecord("x", 1, {**PAYOUT, field: value})), field), value)

    def test_csv_integer_syntax_and_range(self):
        values = {key: str(value) for key, value in PAYOUT.items()}
        for value in ("1.0", "1e3", " 100", "100 ", "١", "", "--1", str(2**63)):
            with self.subTest(value=value):
                with self.assertRaises(RowValidationError):
                    transform_record(SourceRecord("x", 1, {**values, "amount_cents": value}, "csv"))
        for value, expected in (("+100", 100), ("-2000", -2000), ("0001", 1)):
            self.assertEqual(transform_record(SourceRecord("x", 1, {**values, "amount_cents": value}, "csv")).amount_cents, expected)
        self.assertEqual(transform_record(SourceRecord("x", 1, {**values, "amount_cents": "0" * 5000}, "csv")).amount_cents, 0)

    def test_dates_require_canonical_format_and_calendar_validity(self):
        for value in ("20261118", "2026-W47-3", "2026-1-18", "2026-02-30", 20261118):
            with self.subTest(value=value):
                with self.assertRaises(RowValidationError):
                    transform_record(SourceRecord("x", 1, {**PAYOUT, "settled_on": value}))
        with self.assertRaises(RowValidationError) as caught:
            transform_record(SourceRecord("x", 1, {**PAYOUT, "settled_on": "2026-02-30"}))
        self.assertIsInstance(caught.exception.__cause__, ValueError)

    def test_transformation_preserves_input_and_opaque_values(self):
        values = {**PAYOUT, "version": -1, "currency": "custom", "extra": "ignored"}
        before = dict(values)
        first = transform_record(SourceRecord("file", 3, values))
        second = transform_record(SourceRecord("file", 4, values))
        self.assertEqual(values, before)
        self.assertEqual((first.version, first.currency), (-1, "custom"))
        self.assertEqual((first.payout_id, second.payout_id), ("T1", "T1"))


if __name__ == "__main__":
    unittest.main()
