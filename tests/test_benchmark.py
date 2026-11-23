"""Focused evidence for real workloads, measurements and the baseline runner."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import psycopg

from claims_data_lab.benchmark import (
    observe_import, private_schema, run_benchmark_import, sample_process_tree, summarize, verify_workload,
)
from claims_data_lab.measurements import ImportMeasurements
from claims_data_lab.pipeline import run_pipeline
from claims_data_lab.readers import read_csv, read_jsonl
from claims_data_lab.transform import transform_record
from claims_data_lab.workloads import file_evidence, generate_workload, payout_values


class WorkloadTests(unittest.TestCase):
    def test_known_values_and_intentional_repeated_ids(self):
        self.assertEqual(payout_values(0), {"payout_id": "T0", "version": 1, "policy_id": "P0",
            "settled_on": "2026-11-18", "currency": "USD", "amount_cents": -99958})
        self.assertEqual(payout_values(1)["amount_cents"], -92039)
        self.assertEqual(payout_values(50000)["payout_id"], "T0")
        self.assertEqual(payout_values(50000)["amount_cents"], 48063)

    def test_real_files_convert_to_same_known_values(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = generate_workload(Path(directory), 3)
            csv_rows = [transform_record(r) for r in read_csv(Path(evidence["csv"]["path"]))]
            json_rows = [transform_record(r) for r in read_jsonl(Path(evidence["jsonl"]["path"]))]
            self.assertEqual([r.amount_cents for r in csv_rows], [-99958, -92039, -84120])
            for left, right in zip(csv_rows, json_rows):
                self.assertEqual(left.source_record, right.source_record)
                self.assertEqual((left.payout_id, left.version, left.policy_id, left.settled_on, left.currency, left.amount_cents),
                                 (right.payout_id, right.version, right.policy_id, right.settled_on, right.currency, right.amount_cents))
            native = json.loads(Path(evidence["jsonl"]["path"]).read_text().splitlines()[0])
            self.assertIs(type(native["amount_cents"]), int)

    def test_generation_is_repeatable_and_evidence_matches_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            a = generate_workload(root / "a", 5)
            b = generate_workload(root / "b", 5)
            for format in ("csv", "jsonl"):
                self.assertEqual(a[format]["sha256"], b[format]["sha256"])
                self.assertEqual(a[format]["bytes"], Path(a[format]["path"]).stat().st_size)
                self.assertEqual(a[format], file_evidence(Path(a[format]["path"])))

    def test_empty_workload_has_csv_header_and_empty_jsonl(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = generate_workload(Path(directory), 0)
            self.assertEqual(list(read_csv(Path(evidence["csv"]["path"]))), [])
            self.assertEqual(list(read_jsonl(Path(evidence["jsonl"]["path"]))), [])


class MeasurementTests(unittest.TestCase):
    def test_failed_call_preserves_identity_and_records_seconds(self):
        measurements = ImportMeasurements("test-run")
        original = OSError("expected")
        def fail():
            raise original
        with patch("claims_data_lab.measurements.perf_counter", side_effect=[10.0, 10.25]):
            with self.assertRaises(OSError) as raised:
                measurements.call("read", fail)
        self.assertIs(raised.exception, original)
        self.assertEqual(measurements.snapshot()["stage_seconds"], {"read": 0.25})
        self.assertEqual(measurements.snapshot()["stage_calls"], {"read": 1})

    def test_timed_iterator_closes_source_on_early_stop(self):
        closed = []
        def source():
            try:
                yield 1
                yield 2
            finally:
                closed.append(True)
        measurements = ImportMeasurements("iteration")
        iterator = measurements.iterate("read", source())
        self.assertEqual(next(iterator), 1)
        iterator.close()
        self.assertEqual(closed, [True])
        self.assertEqual(measurements.calls["read"], 1)

    def test_failed_process_is_not_interpreted_as_success(self):
        with tempfile.TemporaryDirectory() as directory:
            result = observe_import([sys.executable, "-c", "import sys; print('no result'); sys.exit(7)"], dict(os.environ), Path(directory))
        self.assertEqual(result["returncode"], 7)
        self.assertNotIn("import", result)
        self.assertGreater(result["process_seconds"], 0)

    def test_process_result_and_sampling_notes_are_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch("claims_data_lab.benchmark.sample_process_tree", return_value=(12345, ["AccessDenied"])):
                result = observe_import([sys.executable, "-c", 'import time; time.sleep(0.15); print(\'{"inserted":3,"rejected":0}\')'], dict(os.environ), Path(directory))
        self.assertEqual(result["import"]["inserted"], 3)
        self.assertEqual(result["sampled_process_tree_rss_bytes"], 12345)
        self.assertIn("AccessDenied", result["rss_sample_notes"])

    def test_zero_exit_with_invalid_result_is_not_a_success_result(self):
        with tempfile.TemporaryDirectory() as directory:
            result = observe_import([sys.executable, "-c", "print('invalid json')"], dict(os.environ), Path(directory))
        self.assertNotIn("import", result)
        self.assertEqual(result["result_error_type"], "JSONDecodeError")

    def test_denied_process_discovery_is_disclosed(self):
        with patch("claims_data_lab.benchmark.psutil.Process") as process:
            process.return_value.children.side_effect = PermissionError("blocked")
            self.assertEqual(sample_process_tree(123), (None, ["PermissionError"]))

    def test_summary_preserves_failures_and_all_successful_rates(self):
        rows = [{"rows": 100000, "format": "csv", "status": "passed", "accepted_rows_per_second": n} for n in [10, 30]]
        rows.append({"rows": 100000, "format": "csv", "status": "failed", "accepted_rows_per_second": None})
        result = summarize(rows)[0]
        self.assertEqual((result["runs"], result["passed"]), (3, 2))
        self.assertEqual(result["median_rows_per_second"], 20)
        self.assertEqual(result["range_rows_per_second"], [10, 30])


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "requires owned PostgreSQL")
class BenchmarkPostgreSQLTests(unittest.TestCase):
    def test_sequential_worker_runs_and_schema_is_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workload = generate_workload(root / "input", 7)
            with patch.dict(os.environ, {"BENCHMARK_CONTAINER": ""}):
                result = run_benchmark_import(os.environ["TEST_DATABASE_URL"], workload, "csv", root / "test-run")
            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["verification"]["stored_rows"], 7)
            self.assertEqual(result["import"]["stage_calls"]["transform"], 7)
            self.assertEqual(result["import"]["stage_calls"]["read"], 8)
            self.assertEqual(result["import"]["stage_calls"]["insert"], 1)
            self.assertIn("commit", result["import"]["stage_seconds"])
            self.assertNotIn(os.environ["TEST_DATABASE_URL"], json.dumps(result))

    def test_full_value_verification_detects_wrong_column_and_duplicate_location(self):
        with tempfile.TemporaryDirectory() as directory, private_schema(os.environ["TEST_DATABASE_URL"]) as url:
            root = Path(directory)
            workload = generate_workload(root / "input", 2)
            path = Path(workload["jsonl"]["path"])
            run_pipeline(path, "jsonl", url, root / "rejects", 500)
            self.assertTrue(verify_workload(url, path, 2)["valid"])
            with psycopg.connect(url) as connection:
                connection.execute("UPDATE imported_payouts SET currency='WRONG' WHERE source_record=1")
            self.assertFalse(verify_workload(url, path, 2)["valid"])
            with psycopg.connect(url) as connection:
                connection.execute("UPDATE imported_payouts SET currency='USD', source_record=1")
            self.assertFalse(verify_workload(url, path, 2)["valid"])

    def test_measurements_preserve_rollback_after_inserted_group(self):
        with tempfile.TemporaryDirectory() as directory, private_schema(os.environ["TEST_DATABASE_URL"]) as url:
            root = Path(directory)
            path = root / "broken.csv"
            path.write_text('payout_id,version,policy_id,settled_on,currency,amount_cents\nT1,1,P1,2026-11-18,USD,10\n"unfinished')
            measurements = ImportMeasurements("rollback")
            with self.assertRaises(Exception):
                run_pipeline(path, "csv", url, root / "rejects", 1, measurements=measurements)
            self.assertEqual(measurements.calls["insert"], 1)
            self.assertEqual(measurements.calls["rollback"], 1)
            with psycopg.connect(url) as connection:
                self.assertIsNone(connection.execute("SELECT to_regclass('imported_payouts')").fetchone()[0])

    def test_owned_schema_cleanup_preserves_original_failure(self):
        original = RuntimeError("test primary")
        schema = None
        with self.assertRaises(RuntimeError) as raised:
            with private_schema(os.environ["TEST_DATABASE_URL"]) as url:
                with psycopg.connect(url) as connection:
                    schema = connection.execute("SELECT current_schema()").fetchone()[0]
                raise original
        self.assertIs(raised.exception, original)
        with psycopg.connect(os.environ["TEST_DATABASE_URL"]) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM pg_namespace WHERE nspname=%s", (schema,)).fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
