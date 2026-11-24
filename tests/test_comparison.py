"""Correctness evidence for paired imports and truthful comparison reports."""

import io
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import psycopg

from claims_data_lab.benchmark import private_schema, run_benchmark_import
from claims_data_lab.comparison import append_pair, compare_stored_rows, main, run_pair, summarize_pairs
from claims_data_lab.pipeline import run_pipeline
from claims_data_lab.workloads import generate_workload


TEST_URL = os.environ.get("TEST_DATABASE_URL")
RUN_SPARK = os.environ.get("RUN_SPARK_TESTS") == "1"


class ComparisonTests(unittest.TestCase):
    def test_summary_excludes_invalid_pairs_and_preserves_scheduled_count(self):
        pairs = []
        for valid, value in [(True, 1), (True, 3), (False, 100)]:
            pairs.append({"rows": 100000, "format": "csv", "status": "passed" if valid else "failed",
                "runs": [{"engine": engine, "status": "passed", "process_seconds": value,
                          "accepted_rows_per_second": 100000/value, "sampled_process_tree_rss_bytes": value*1024}
                         for engine in ("sequential", "spark")]})
        summary = summarize_pairs(pairs)
        for result in summary[:2]:
            self.assertEqual(result["scheduled_pairs"], 3)
            self.assertEqual(result["valid_pairs"], 2)
            self.assertEqual(result["median_process_seconds"], 2)
            self.assertEqual(result["range_process_seconds"], [1, 3])

    def test_main_preserves_partial_pair_before_stopping(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = OSError("pair verification unavailable")
            original.comparison_observation = {"pair_id": "failed-pair", "status": "failed", "failed_stage": "pair_multiset_comparison",
                "runs": [{"run_id": "completed", "status": "passed", "process_seconds": 1.25}]}
            with patch.dict(os.environ, {"DATABASE_URL": "unused"}), patch(
                "claims_data_lab.comparison.Path", return_value=root), patch(
                "claims_data_lab.comparison.environment_evidence", return_value={}), patch(
                "claims_data_lab.comparison.subprocess.run", return_value=subprocess.CompletedProcess([], 0, stdout="", stderr="test Java 17")), patch(
                "claims_data_lab.comparison.importlib.metadata.version", return_value="test"), patch(
                "claims_data_lab.comparison.generate_workload", return_value={"rows": 100000}), patch(
                "claims_data_lab.comparison.run_benchmark_import", return_value={"status": "passed"}), patch(
                "claims_data_lab.comparison.run_pair", side_effect=original):
                self.assertEqual(main(), 1)
            report = next(root.glob("*/runs.jsonl"))
            retained = json.loads(report.read_text())
            self.assertEqual(retained["process_seconds"], 1.25)
            self.assertEqual(retained["pair_status"], "failed")
            self.assertEqual(json.loads(next(root.glob("*/pairs.jsonl")).read_text()), original.comparison_observation)

    def test_pair_report_keeps_completed_and_failed_observations(self):
        pair = {"pair_id": "pair-test", "status": "failed", "runs": [
            {"run_id": "first", "status": "passed", "process_seconds": 1.25},
            {"run_id": "second", "status": "failed", "failed_stage": "verification"}]}
        pair_report, run_report = io.StringIO(), io.StringIO()
        append_pair(pair, pair_report, run_report)
        self.assertEqual(json.loads(pair_report.getvalue()), pair)
        records = [json.loads(line) for line in run_report.getvalue().splitlines()]
        self.assertEqual([r["run_id"] for r in records], ["first", "second"])
        self.assertTrue(all(r["pair_status"] == "failed" for r in records))
        self.assertEqual(records[0]["process_seconds"], 1.25)


@unittest.skipUnless(TEST_URL, "requires owned PostgreSQL")
class ComparisonPostgreSQLTests(unittest.TestCase):
    def test_multiset_check_detects_duplicate_multiplicity_and_changed_column(self):
        with tempfile.TemporaryDirectory() as directory, private_schema(TEST_URL) as left, private_schema(TEST_URL) as right:
            root = Path(directory)
            workload = generate_workload(root / "input", 3)
            path = Path(workload["jsonl"]["path"])
            for url, name in [(left, "a"), (right, "b")]:
                run_pipeline(path, "jsonl", url, root / name, 500)
            self.assertEqual(compare_stored_rows(TEST_URL, left, right),
                             {"left_only_rows": 0, "right_only_rows": 0, "identical": True})
            with psycopg.connect(right) as connection:
                connection.execute("INSERT INTO imported_payouts (source_file,source_record,payout_id,version,policy_id,settled_on,currency,amount_cents) SELECT source_file,source_record,payout_id,version,policy_id,settled_on,currency,amount_cents FROM imported_payouts WHERE source_record=1")
            difference = compare_stored_rows(TEST_URL, left, right)
            self.assertEqual((difference["left_only_rows"], difference["right_only_rows"]), (0, 1))
            with psycopg.connect(right) as connection:
                connection.execute("DELETE FROM imported_payouts WHERE id=(SELECT max(id) FROM imported_payouts)")
                connection.execute("UPDATE imported_payouts SET currency='WRONG' WHERE source_record=2")
            self.assertEqual(compare_stored_rows(TEST_URL, left, right),
                             {"left_only_rows": 1, "right_only_rows": 1, "identical": False})

    def test_caller_owned_schema_survives_until_pair_comparison(self):
        with tempfile.TemporaryDirectory() as directory, private_schema(TEST_URL) as url:
            root = Path(directory)
            workload = generate_workload(root / "input", 3)
            with patch.dict(os.environ, {"BENCHMARK_CONTAINER": ""}):
                result = run_benchmark_import(TEST_URL, workload, "csv", root / "retained", schema_url=url)
            self.assertEqual(result["status"], "passed")
            with psycopg.connect(url) as connection:
                self.assertEqual(connection.execute("SELECT count(*) FROM imported_payouts").fetchone()[0], 3)

    def test_second_engine_exception_preserves_first_measurement_and_original_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workload = generate_workload(root / "input", 3)
            original = OSError("second verification unavailable")
            original.benchmark_observation = {"run_id": "spark-csv-3-1", "status": "failed", "process_seconds": 2.5}
            first = {"run_id": "sequential-csv-3-1", "engine": "sequential", "status": "passed", "process_seconds": 1.25}
            with patch("claims_data_lab.comparison.run_benchmark_import", side_effect=[first, original]):
                with self.assertRaises(OSError) as raised:
                    run_pair(TEST_URL, workload, "csv", 1, root / "failed-pair")
            self.assertIs(raised.exception, original)
            retained = json.loads((root / "failed-pair" / "observation.json").read_text())
            self.assertEqual(retained["failed_stage"], "import_spark")
            self.assertEqual([r["process_seconds"] for r in retained["runs"]], [1.25, 2.5])
            self.assertEqual(retained, original.comparison_observation)

    def test_failed_observation_write_preserves_completed_measurement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workload = generate_workload(root / "input", 3)
            original = OSError("run report unavailable")
            native_write = Path.write_text
            def selected_write(path, *args, **kwargs):
                if path.name == "observation.json":
                    raise original
                return native_write(path, *args, **kwargs)
            with patch.dict(os.environ, {"BENCHMARK_CONTAINER": ""}), patch.object(Path, "write_text", selected_write):
                with self.assertRaises(OSError) as raised:
                    run_benchmark_import(TEST_URL, workload, "csv", root / "write-failure")
            self.assertIs(raised.exception, original)
            observation = original.benchmark_observation
            self.assertEqual(observation["failed_stage"], "observation_write")
            self.assertEqual(observation["import"]["inserted"], 3)
            self.assertGreater(observation["process_seconds"], 0)
            self.assertIsNone(observation["accepted_rows_per_second"])

    def test_engine_order_alternates_without_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workload = generate_workload(root / "input", 3)
            for repeat, expected in [(1, ["sequential", "spark"]), (2, ["spark", "sequential"]), (3, ["sequential", "spark"])]:
                seen = []
                def failed_import(*args, **kwargs):
                    seen.append(kwargs["engine"])
                    return {"run_id": str(len(seen)), "engine": kwargs["engine"], "status": "failed"}
                with patch("claims_data_lab.comparison.run_benchmark_import", side_effect=failed_import):
                    pair = run_pair(TEST_URL, workload, "csv", repeat, root / f"order-{repeat}")
                self.assertEqual(seen, expected)
                self.assertEqual(pair["status"], "failed")
                self.assertEqual(pair["comparison"]["state"], "not_compared")


@unittest.skipUnless(TEST_URL and RUN_SPARK, "requires real Spark and owned PostgreSQL")
class PairedSparkTests(unittest.TestCase):
    def test_real_pair_selects_both_engines_and_compares_complete_values(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workload = generate_workload(root / "input", 17)
            with patch.dict(os.environ, {"BENCHMARK_CONTAINER": ""}):
                result = run_pair(TEST_URL, workload, "jsonl", 1, root / "real-pair")
            self.assertEqual(result["status"], "passed")
            self.assertTrue(result["comparison"]["identical"])
            self.assertEqual([r["engine"] for r in result["runs"]], ["sequential", "spark"])
            self.assertTrue(all(r["import"]["inserted"] == 17 for r in result["runs"]))
            self.assertIn("spark_materialize", result["runs"][1]["import"]["stage_seconds"])
            self.assertIn("spark_cleanup", result["runs"][1]["import"]["stage_seconds"])
