"""CLI contracts and subprocess-to-PostgreSQL verification."""

import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import date
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from claims_data_lab.cli import main
from claims_data_lab.models import PipelineResult


ROOT = Path(__file__).resolve().parents[1]
TEST_URL = os.environ.get("TEST_DATABASE_URL")
ARGS = ["--input", "input.jsonl", "--format", "jsonl", "--batch-rows", "2", "--rejects", "rejects.jsonl"]


class CLIUnitTests(unittest.TestCase):
    def test_help_does_not_require_database_url(self):
        output = io.StringIO()
        with patch.dict(os.environ, {}, clear=True), redirect_stdout(output):
            with self.assertRaises(SystemExit) as caught:
                main(["--help"])
        self.assertEqual(caught.exception.code, 0)
        self.assertIn("--batch-rows", output.getvalue())

    def test_bad_arguments_fail_before_pipeline(self):
        for args in ([], [*ARGS[:-4], "--batch-rows", "0", "--rejects", "x"],
                     ["--input", "x", "--format", "xml", "--batch-rows", "2", "--rejects", "x"],
                     [*ARGS[:-4], "--batch-rows", "abc", "--rejects", "x"]):
            with self.subTest(args=args), patch("claims_data_lab.cli.run_pipeline") as run:
                with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
                    main(args)
                self.assertEqual(caught.exception.code, 2)
                run.assert_not_called()

    def test_missing_credentials_fail_before_pipeline(self):
        with patch.dict(os.environ, {}, clear=True), patch("claims_data_lab.cli.run_pipeline") as run:
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
                main(ARGS)
        self.assertEqual(caught.exception.code, 2)
        run.assert_not_called()

    def test_success_prints_committed_counts(self):
        output = io.StringIO()
        with patch.dict(os.environ, {"DATABASE_URL": "test-url"}):
            with patch("claims_data_lab.cli.run_pipeline", return_value=PipelineResult(3, 1)) as run:
                with redirect_stdout(output):
                    self.assertEqual(main(ARGS), 0)
        run.assert_called_once_with(Path("input.jsonl"), "jsonl", "test-url", Path("rejects.jsonl"), 2)
        self.assertEqual(json.loads(output.getvalue()), {"inserted": 3, "rejected": 1})

    def test_failure_redacts_connection_and_raw_exception(self):
        secret = "postgresql://demo:password_not_for_logs@localhost/private"
        error = psycopg.OperationalError("connection failed: " + secret)
        output, diagnostic = io.StringIO(), io.StringIO()
        with patch.dict(os.environ, {"DATABASE_URL": secret}):
            with patch("claims_data_lab.cli.run_pipeline", side_effect=error):
                with redirect_stdout(output), redirect_stderr(diagnostic):
                    self.assertEqual(main(ARGS), 1)
        self.assertEqual(output.getvalue(), "")
        self.assertIn("OperationalError", diagnostic.getvalue())
        self.assertIn("inspect PostgreSQL", diagnostic.getvalue())
        self.assertNotIn(secret, diagnostic.getvalue())
        self.assertNotIn("password_not_for_logs", diagnostic.getvalue())

    def test_subprocess_connection_error_does_not_expose_credentials(self):
        # Deliberately malformed conninfo fails before any network connection.
        secret = "password='secret_that_must_not_be_printed"
        result = subprocess.run(
            [sys.executable, "-m", "claims_data_lab", *ARGS], cwd=ROOT,
            env={**os.environ, "DATABASE_URL": secret, "PYTHONDONTWRITEBYTECODE": "1"},
            text=True, capture_output=True, timeout=10,
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertIn("stage=connect", result.stderr)
        self.assertNotIn("secret_that_must_not_be_printed", result.stderr)


@unittest.skipUnless(TEST_URL, "set TEST_DATABASE_URL to an isolated local PostgreSQL instance")
class CLIPostgreSQLTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.schema = "claims_cli_" + uuid4().hex
        with psycopg.connect(TEST_URL, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
        self.addCleanup(self.drop_schema)
        self.url = make_conninfo(TEST_URL, options=f"-csearch_path={self.schema}")

    def drop_schema(self):
        with psycopg.connect(TEST_URL, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    def invoke(self, path, input_format, batch_rows="2"):
        return subprocess.run(
            [sys.executable, "-m", "claims_data_lab", "--input", str(path),
             "--format", input_format, "--batch-rows", batch_rows,
             "--rejects", str(self.root / "rejects.jsonl")],
            cwd=ROOT, env={**os.environ, "DATABASE_URL": self.url, "PYTHONDONTWRITEBYTECODE": "1"},
            text=True, capture_output=True, timeout=15,
        )

    def stored(self):
        with psycopg.connect(self.url) as connection:
            return connection.execute(
                "SELECT payout_id, version, policy_id, settled_on, currency, amount_cents, source_record "
                "FROM imported_payouts ORDER BY id"
            ).fetchall()

    def test_both_fixture_formats_append_matching_values(self):
        expected = [
            ("T1", 1, "P1", date(2026, 11, 18), "USD", 10000, 1),
            ("T2", 1, "P1", date(2026, 11, 18), "USD", 5000, 2),
            ("T3", 1, "P2", date(2026, 11, 18), "USD", 20000, 3),
        ]
        for count, input_format in enumerate(("jsonl", "csv"), start=1):
            with self.subTest(input_format=input_format):
                result = self.invoke(ROOT / f"fixtures/payouts/2026-11-18.{input_format}", input_format)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(result.stdout), {"inserted": 3, "rejected": 0})
                self.assertEqual(self.stored(), expected * count)

    def test_cli_rejects_bad_row_and_reports_committed_counts(self):
        good = (ROOT / "fixtures/payouts/2026-11-18.jsonl").read_text().splitlines()[0]
        path = self.root / "mixed.jsonl"
        path.write_text(good + "\n{bad\n", encoding="utf-8")
        result = self.invoke(path, "jsonl", batch_rows="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {"inserted": 1, "rejected": 1})
        self.assertEqual(len(self.stored()), 1)
        reject = json.loads((self.root / "rejects.jsonl").read_text())
        self.assertEqual((reject["source_file"], reject["source_record"]), (str(path), 2))

    def test_cli_fatal_error_has_nonzero_status_and_rolls_back(self):
        fixture = ROOT / "fixtures/payouts/2026-11-18.jsonl"
        self.assertEqual(self.invoke(fixture, "jsonl").returncode, 0)
        before = self.stored()
        path = self.root / "broken.csv"
        header = (ROOT / "fixtures/payouts/2026-11-18.csv").read_text().splitlines()[0]
        path.write_text(header + '\nT4,1,P1,2026-11-18,USD,200\n"unfinished', encoding="utf-8")
        result = self.invoke(path, "csv", batch_rows="1")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertIn("stage=read", result.stderr)
        self.assertNotIn(self.url, result.stderr)
        self.assertEqual(self.stored(), before)

    def test_invalid_batch_size_does_not_create_table_or_output(self):
        result = self.invoke(ROOT / "fixtures/payouts/2026-11-18.jsonl", "jsonl", batch_rows="0")
        self.assertEqual(result.returncode, 2)
        self.assertFalse((self.root / "rejects.jsonl").exists())
        with psycopg.connect(self.url) as connection:
            self.assertIsNone(connection.execute("SELECT to_regclass('imported_payouts')").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
