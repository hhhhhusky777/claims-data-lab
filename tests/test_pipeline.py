"""T2 integration tests use a dedicated PostgreSQL URL and private schemas.

Set TEST_DATABASE_URL to an isolated local test database. Each test creates and
removes its own schema. Without the variable, integration cases are skipped.
Unit cases can still exercise transaction failure propagation without a server.
"""

import csv
import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from claims_data_lab import database
from claims_data_lab.models import PAYOUT_FIELDS
from claims_data_lab.pipeline import run_pipeline


ROOT = Path(__file__).resolve().parents[1]
TEST_URL = os.environ.get("TEST_DATABASE_URL")
PAYOUT = {
    "payout_id": "T1", "version": 1, "policy_id": "P1",
    "settled_on": "2026-11-18", "currency": "USD", "amount_cents": 10000,
}


class TransactionTests(unittest.TestCase):
    def test_uncertain_commit_propagates_original_exception_without_retry(self):
        connection = MagicMock()
        error = psycopg.OperationalError("simulated interrupted COMMIT")
        connection.transaction.return_value.__exit__.side_effect = error
        with self.assertRaises(psycopg.OperationalError) as caught:
            with database.transaction(connection):
                pass
        self.assertIs(caught.exception, error)
        self.assertIn("inspect PostgreSQL", " ".join(error.__notes__))
        connection.transaction.assert_called_once()

    def test_empty_insert_group_performs_no_sql(self):
        connection = MagicMock()
        self.assertEqual(database.insert_batch(connection, []), 0)
        connection.cursor.assert_not_called()

    def test_invalid_batch_size_precedes_database_connection(self):
        for size in (0, -1, True, 1.5):
            with self.subTest(size=size), patch("claims_data_lab.pipeline.psycopg.connect") as connect:
                with self.assertRaisesRegex(ValueError, "positive integer"):
                    run_pipeline(Path("unused"), "jsonl", "unused", Path("unused-output"), size)
                connect.assert_not_called()


@unittest.skipUnless(TEST_URL, "set TEST_DATABASE_URL to an isolated local PostgreSQL instance")
class PostgreSQLPipelineTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.schema = "claims_test_" + uuid4().hex
        with psycopg.connect(TEST_URL, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
        self.addCleanup(self.drop_schema)
        self.url = make_conninfo(TEST_URL, options=f"-csearch_path={self.schema}")
        with psycopg.connect(self.url, autocommit=True) as connection:
            with database.transaction(connection):
                database.create_tables(connection)

    def drop_schema(self):
        with psycopg.connect(TEST_URL, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    def write_jsonl(self, lines):
        path = self.root / "input.jsonl"
        path.write_text("\n".join(json.dumps(row) if isinstance(row, dict) else row for row in lines), encoding="utf-8")
        return path

    def stored(self):
        with psycopg.connect(self.url) as connection:
            return connection.execute(
                "SELECT payout_id, version, policy_id, settled_on, currency, amount_cents, source_record "
                "FROM imported_payouts ORDER BY id"
            ).fetchall()

    def load(self, path, batch_rows=2, output="rejects.jsonl", input_format="jsonl"):
        return run_pipeline(path, input_format, self.url, self.root / output, batch_rows)

    def seed(self):
        self.load(self.write_jsonl([PAYOUT]), output="seed-rejects.jsonl")

    def test_real_fixture_inserts_groups_and_final_partial_group(self):
        path = ROOT / "fixtures/payouts/2026-11-18.jsonl"
        sizes = []
        real_insert = database.insert_batch

        def observe(connection, records):
            sizes.append(len(records))
            # Another connection must see no rows until the file commits.
            self.assertEqual(self.stored(), [])
            return real_insert(connection, records)

        with patch("claims_data_lab.pipeline.database.insert_batch", side_effect=observe):
            result = self.load(path)
        self.assertEqual((result.inserted, result.rejected), (3, 0))
        self.assertEqual(sizes, [2, 1])
        self.assertEqual(self.stored(), [
            ("T1", 1, "P1", date(2026, 11, 18), "USD", 10000, 1),
            ("T2", 1, "P1", date(2026, 11, 18), "USD", 5000, 2),
            ("T3", 1, "P2", date(2026, 11, 18), "USD", 20000, 3),
        ])
        self.assertEqual((self.root / "rejects.jsonl").read_text(), "")

    def test_csv_inserts_typed_parameter_values(self):
        path = self.root / "payout.csv"
        with path.open("w", encoding="utf-8-sig", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=list(PAYOUT_FIELDS))
            writer.writeheader()
            writer.writerow({**PAYOUT, "payout_id": "T'quoted", "amount_cents": -2000})
        result = self.load(path, input_format="csv")
        self.assertEqual(result.inserted, 1)
        self.assertEqual(self.stored()[0], ("T'quoted", 1, "P1", date(2026, 11, 18), "USD", -2000, 1))

    def test_repeat_ids_and_file_reimports_append(self):
        path = self.write_jsonl([PAYOUT, {**PAYOUT, "version": -1}])
        self.load(path)
        self.load(path)
        self.assertEqual([row[1] for row in self.stored()], [1, -1, 1, -1])

    def test_reader_and_conversion_rejections_continue_with_locations(self):
        path = self.write_jsonl([
            PAYOUT, "{bad", "[]", {**PAYOUT, "amount_cents": "abc"},
            {**PAYOUT, "settled_on": "2026-02-30"}, {**PAYOUT, "policy_id": "P99"},
        ])
        result = self.load(path, batch_rows=1)
        self.assertEqual((result.inserted, result.rejected), (2, 4))
        rejections = [json.loads(line) for line in (self.root / "rejects.jsonl").read_text().splitlines()]
        self.assertEqual([row["source_record"] for row in rejections], [2, 3, 4, 5])
        self.assertTrue(all(row["source_file"] == str(path) and row["reason"] for row in rejections))
        self.assertEqual(self.stored()[-1][2], "P99")

    def test_empty_jsonl_commits_zero_counts(self):
        result = self.load(self.write_jsonl([]))
        self.assertEqual((result.inserted, result.rejected), (0, 0))
        self.assertEqual(self.stored(), [])

    def test_fatal_csv_framing_rolls_back_earlier_group(self):
        self.seed()
        before = self.stored()
        path = self.root / "broken.csv"
        path.write_text(
            ",".join(PAYOUT_FIELDS) + '\nT2,1,P1,2026-11-18,USD,200\n"unfinished', encoding="utf-8"
        )
        with self.assertRaises(csv.Error):
            self.load(path, batch_rows=1, input_format="csv")
        self.assertEqual(self.stored(), before)
        path.write_text(",".join(PAYOUT_FIELDS) + "\nT2,1,P1,2026-11-18,USD,200\n", encoding="utf-8")
        self.assertEqual(self.load(path, output="retry-rejects.jsonl", input_format="csv").inserted, 1)

    def test_database_failure_rolls_back_previously_inserted_group(self):
        self.seed()
        before = self.stored()
        path = self.write_jsonl([PAYOUT, {**PAYOUT, "payout_id": "bad\x00value"}])
        # PostgreSQL text cannot contain NUL. This fails at the actual writer
        # boundary, after one accepted group has already been sent.
        with self.assertRaises(psycopg.DataError):
            self.load(path, batch_rows=1)
        self.assertEqual(self.stored(), before)

    def test_rejection_output_failure_rolls_back_earlier_group(self):
        self.seed()
        before = self.stored()
        path = self.write_jsonl([PAYOUT, "{bad"])
        output = self.root / "rejects.jsonl"
        original_open = Path.open

        @contextmanager
        def failing_output():
            with original_open(output, "a", encoding="utf-8") as file:
                writer = MagicMock(wraps=file)
                writer.write.side_effect = OSError("simulated full disk")
                yield writer

        def open_file(target, *args, **kwargs):
            if target == output:
                return failing_output()
            return original_open(target, *args, **kwargs)

        with patch.object(Path, "open", open_file):
            with self.assertRaisesRegex(OSError, "full disk"):
                self.load(path, batch_rows=1)
        self.assertEqual(self.stored(), before)

    def test_existing_reject_file_is_preserved_and_appended(self):
        path = self.write_jsonl([PAYOUT])
        output = self.root / "rejects.jsonl"
        previous = json.dumps({"source_file": "earlier", "source_record": 1, "reason": "earlier error"}) + "\n"
        output.write_text(previous, encoding="utf-8")
        self.assertEqual(self.load(path).inserted, 1)
        self.assertEqual(output.read_text(), previous)
        self.load(self.write_jsonl(["{bad"]))
        rows = [json.loads(line) for line in output.read_text().splitlines()]
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["reason"], "earlier error")
        self.assertEqual(len(self.stored()), 1)

    def test_source_cannot_be_overwritten_by_reject_output(self):
        path = self.write_jsonl([PAYOUT])
        before = path.read_bytes()
        with self.assertRaisesRegex(ValueError, "distinct"):
            run_pipeline(path, "jsonl", self.url, path, 1)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.stored(), [])

    def test_rejection_output_aliases_preserve_source(self):
        path = self.write_jsonl([PAYOUT])
        before = path.read_bytes()
        for alias_type in ("symlink", "hardlink"):
            alias = self.root / alias_type
            if alias_type == "symlink":
                alias.symlink_to(path)
            else:
                alias.hardlink_to(path)
            with self.subTest(alias=alias_type), self.assertRaisesRegex(ValueError, "distinct"):
                run_pipeline(path, "jsonl", self.url, alias, 1)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.stored(), [])


if __name__ == "__main__":
    unittest.main()
