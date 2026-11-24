"""Real Spark/PostgreSQL parity and failure boundaries for Phase 2 T2."""

import csv
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import psycopg

from claims_data_lab import database
from claims_data_lab.benchmark import private_schema
from claims_data_lab.cli import main
from claims_data_lab.measurements import ImportMeasurements
from claims_data_lab.models import ImportProgress
from claims_data_lab.pipeline import run_pipeline
from claims_data_lab.spark_transform import _worker_environment, spark_outcomes
from claims_data_lab.workloads import generate_workload


ROOT = Path(__file__).resolve().parents[1]
TEST_URL = os.environ.get("TEST_DATABASE_URL")
RUN_SPARK = os.environ.get("RUN_SPARK_TESTS") == "1"


def stored(url):
    with psycopg.connect(url) as connection:
        return connection.execute("SELECT source_file,source_record,payout_id,version,policy_id,settled_on,currency,amount_cents FROM imported_payouts ORDER BY source_record").fetchall()


def no_table(url):
    with psycopg.connect(url) as connection:
        return connection.execute("SELECT to_regclass('imported_payouts')").fetchone()[0] is None


class EngineUnitTests(unittest.TestCase):
    def test_engine_typo_fails_before_import(self):
        with patch.dict(os.environ, {"DATABASE_URL": "not-used"}), patch("claims_data_lab.cli.run_pipeline") as run:
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
                main(["--input", "x", "--format", "csv", "--batch-rows", "2", "--rejects", "y", "--engine", "sparkk"])
        self.assertEqual(caught.exception.code, 2)
        run.assert_not_called()

    def test_worker_environment_restores_driver_state_on_failure(self):
        original = dict(os.environ)
        with patch.dict(os.environ, {"DATABASE_URL": "do-not-ship", "TEST_DATABASE_URL": "also-private", "PYSPARK_PYTHON": "previous"}):
            with self.assertRaisesRegex(RuntimeError, "expected"):
                with _worker_environment():
                    self.assertNotIn("DATABASE_URL", os.environ)
                    self.assertNotIn("TEST_DATABASE_URL", os.environ)
                    self.assertEqual(os.environ["PYSPARK_PYTHON"], sys.executable)
                    raise RuntimeError("expected")
            self.assertEqual(os.environ["DATABASE_URL"], "do-not-ship")
            self.assertEqual(os.environ["PYSPARK_PYTHON"], "previous")
        self.assertEqual(dict(os.environ), original)


@unittest.skipUnless(TEST_URL and RUN_SPARK, "requires owned PostgreSQL, Java/PySpark and RUN_SPARK_TESTS=1")
class SparkPostgreSQLTests(unittest.TestCase):
    def compare_engines(self, path, input_format, root):
        outputs = []
        for engine in ("sequential", "spark"):
            with private_schema(TEST_URL) as url:
                reject = root / (engine + "-rejects.jsonl")
                result = run_pipeline(path, input_format, url, reject, 1, engine=engine)
                outputs.append((result, stored(url), [json.loads(line) for line in reject.read_text().splitlines()]))
        self.assertEqual(outputs[0], outputs[1])
        return outputs[1]

    def test_csv_multiline_bom_and_jsonl_native_type_rejection_parity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "multiline.csv"
            with path.open("w", encoding="utf-8-sig", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["payout_id", "version", "policy_id", "settled_on", "currency", "amount_cents", "extra"])
                writer.writerow(["T1", "1", "P1", "2026-11-18", "USD", "-2000", "ignored"])
                writer.writerow(["line\nbreak", "+2", "P99", "2026-11-18", "USD", "500", "ignored"])
                writer.writerow(["bad", "true", "P1", "2026-11-18", "USD", "500", "ignored"])
            result, rows, rejects = self.compare_engines(path, "csv", root)
            self.assertEqual((result.inserted, result.rejected), (2, 1))
            self.assertEqual([row[1] for row in rows], [1, 2])
            self.assertEqual(rows[1][2], "line\nbreak")
            self.assertEqual(rejects[0]["source_record"], 3)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            good = json.loads((ROOT / "fixtures/payouts/2026-11-18.jsonl").read_text().splitlines()[0])
            records = [good, {**good, "version": True}, {**good, "amount_cents": "10"},
                       {**good, "settled_on": "2026-02-30"}, {**good, "amount_cents": 2**63},
                       {**good, "amount_cents": -2000}, {}, []]
            path = root / "native.jsonl"
            path.write_text("\ufeff" + "\n".join(json.dumps(r) for r in records) + "\n{broken\n\n", encoding="utf-8")
            result, rows, rejects = self.compare_engines(path, "jsonl", root)
            self.assertEqual((result.inserted, result.rejected), (2, 8))
            self.assertEqual([r[1] for r in rows], [1, 6])
            self.assertEqual([r["source_record"] for r in rejects], [2, 3, 4, 5, 7, 8, 9, 10])

    def test_fixture_reimports_append_and_reject_diagnostics_append(self):
        with tempfile.TemporaryDirectory() as directory, private_schema(TEST_URL) as url:
            root = Path(directory)
            path = root / "input.jsonl"
            path.write_text((ROOT / "fixtures/payouts/2026-11-18.jsonl").read_text() + "{bad\n")
            reject = root / "rejects"
            reject.write_text('{"previous":true}\n')
            for _ in range(2):
                result = run_pipeline(path, "jsonl", url, reject, 2, engine="spark")
                self.assertEqual((result.inserted, result.rejected), (3, 1))
            self.assertEqual(len(stored(url)), 6)
            self.assertEqual(len(reject.read_text().splitlines()), 3)

    def test_cli_spark_prints_only_committed_counts(self):
        with tempfile.TemporaryDirectory() as directory, private_schema(TEST_URL) as url:
            result = subprocess.run([sys.executable, "-m", "claims_data_lab", "--input",
                str(ROOT / "fixtures/payouts/2026-11-18.jsonl"), "--format", "jsonl", "--batch-rows", "2",
                "--rejects", str(Path(directory) / "rejects"), "--engine", "spark"],
                cwd=ROOT, env={**os.environ, "DATABASE_URL": url}, capture_output=True, text=True, timeout=90)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout), {"inserted": 3, "rejected": 0})
            self.assertEqual([r[-1] for r in stored(url)], [10000, 5000, 20000])
            self.assertNotIn(url, result.stderr)
            self.assertNotIn("claims_local_test", result.stderr)

    def test_empty_input_and_alias_guard(self):
        with tempfile.TemporaryDirectory() as directory, private_schema(TEST_URL) as url:
            root = Path(directory)
            path = root / "empty.jsonl"
            path.write_text("")
            result = run_pipeline(path, "jsonl", url, root / "rejects", 2, engine="spark")
            self.assertEqual((result.inserted, result.rejected), (0, 0))
            self.assertEqual(stored(url), [])
            alias = root / "alias"
            alias.hardlink_to(path)
            with patch("claims_data_lab.spark_transform.spark_outcomes") as source:
                with self.assertRaises(ValueError):
                    run_pipeline(path, "jsonl", url, alias, 2, engine="spark")
                source.assert_not_called()
            self.assertEqual(path.read_bytes(), b"")

    def test_fatal_csv_before_startup_rolls_back_without_spark_fallback(self):
        with tempfile.TemporaryDirectory() as directory, private_schema(TEST_URL) as url:
            root = Path(directory)
            path = root / "broken.csv"
            path.write_text('payout_id,version,policy_id,settled_on,currency,amount_cents\nT1,1,P1,2026-11-18,USD,1\n"unfinished')
            with patch("pyspark.SparkContext") as context:
                with self.assertRaises(csv.Error) as raised:
                    run_pipeline(path, "csv", url, root / "rejects", 1, engine="spark")
                context.assert_not_called()
            self.assertTrue(no_table(url))
            self.assertIn("stage=spark_stage_input", " ".join(raised.exception.__notes__))

    def test_worker_error_is_fatal_and_owned_resources_are_cleaned(self):
        def broken_partition(lines):
            raise RuntimeError("intentional conversion defect")
            yield
        with tempfile.TemporaryDirectory() as directory, private_schema(TEST_URL) as url:
            root = Path(directory)
            temporary_paths = []
            original = tempfile.mkdtemp
            def capture(*args, **kwargs):
                path = original(*args, **kwargs)
                temporary_paths.append(Path(path))
                return path
            with patch("claims_data_lab.spark_transform._convert_partition", broken_partition), patch(
                "claims_data_lab.spark_transform.tempfile.mkdtemp", side_effect=capture):
                with self.assertRaises(Exception) as raised:
                    run_pipeline(ROOT / "fixtures/payouts/2026-11-18.jsonl", "jsonl", url, root / "rejects", 1, engine="spark")
            self.assertIn("intentional conversion defect", str(raised.exception))
            self.assertIn("stage=spark_materialize", " ".join(raised.exception.__notes__))
            self.assertTrue(no_table(url))
            self.assertTrue(temporary_paths)
            self.assertTrue(all(not path.exists() for path in temporary_paths))

    def test_late_iterator_failure_rolls_back_earlier_group(self):
        from pyspark import RDD
        native_iterator = RDD.toLocalIterator
        original = OSError("simulated driver transfer failure")
        def broken_iterator(rdd, prefetchPartitions=False):
            iterator = native_iterator(rdd, prefetchPartitions=prefetchPartitions)
            try:
                yield next(iterator)
                raise original
            finally:
                iterator.close()
        with tempfile.TemporaryDirectory() as directory, private_schema(TEST_URL) as url:
            with patch.object(RDD, "toLocalIterator", broken_iterator), patch(
                "claims_data_lab.database.insert_batch", wraps=database.insert_batch) as insert:
                with self.assertRaises(OSError) as raised:
                    run_pipeline(ROOT / "fixtures/payouts/2026-11-18.jsonl", "jsonl", url, Path(directory) / "rejects", 1, engine="spark")
                self.assertEqual(insert.call_count, 1)
            self.assertIs(raised.exception, original)
            self.assertIn("stage=spark_transfer", " ".join(raised.exception.__notes__))
            self.assertTrue(no_table(url))

    def test_database_failure_and_cleanup_do_not_replace_original_error(self):
        from pyspark import SparkContext
        native_stop = SparkContext.stop
        calls = []
        # Save the original insert before patching its module attribute.
        native_insert = database.insert_batch
        def staged_insert(connection, records):
            calls.append(1)
            if len(calls) == 1:
                return native_insert(connection, records)
            connection.execute("SELECT 1/0")
        def failed_stop(context):
            native_stop(context)
            raise OSError("simulated cleanup failure")
        with tempfile.TemporaryDirectory() as directory, private_schema(TEST_URL) as url:
            with patch("claims_data_lab.database.insert_batch", staged_insert), patch.object(SparkContext, "stop", failed_stop):
                with self.assertRaises(psycopg.errors.DivisionByZero) as raised:
                    run_pipeline(ROOT / "fixtures/payouts/2026-11-18.jsonl", "jsonl", url, Path(directory) / "rejects", 1, engine="spark")
            self.assertEqual(len(calls), 2)
            self.assertIn("Spark cleanup also failed: OSError", raised.exception.__notes__)
            self.assertTrue(no_table(url))

    def test_reject_output_failure_rolls_back_earlier_group(self):
        original = OSError("simulated diagnostic output failure")
        native_open = Path.open
        class BrokenOutput:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def write(self, value): raise original
        with tempfile.TemporaryDirectory() as directory, private_schema(TEST_URL) as url:
            root = Path(directory)
            path, rejects = root / "mixed.jsonl", root / "rejects"
            path.write_text((ROOT / "fixtures/payouts/2026-11-18.jsonl").read_text().splitlines()[0] + "\n{bad\n")
            def selected_open(path, *args, **kwargs):
                return BrokenOutput() if path == rejects else native_open(path, *args, **kwargs)
            with patch.object(Path, "open", selected_open), patch(
                "claims_data_lab.database.insert_batch", wraps=database.insert_batch) as insert:
                with self.assertRaises(OSError) as raised:
                    run_pipeline(path, "jsonl", url, rejects, 1, engine="spark")
                self.assertEqual(insert.call_count, 1)
            self.assertIs(raised.exception, original)
            self.assertIn("stage=write_rejection", " ".join(original.__notes__))
            self.assertTrue(no_table(url))

    def test_missing_spark_is_fatal_with_no_sequential_fallback(self):
        import builtins
        native_import = builtins.__import__
        def unavailable(name, *args, **kwargs):
            if name == "pyspark":
                raise ModuleNotFoundError("test runtime unavailable")
            return native_import(name, *args, **kwargs)
        with tempfile.TemporaryDirectory() as directory, private_schema(TEST_URL) as url:
            with patch("builtins.__import__", side_effect=unavailable):
                with self.assertRaises(ModuleNotFoundError) as raised:
                    run_pipeline(ROOT / "fixtures/payouts/2026-11-18.jsonl", "jsonl", url, Path(directory) / "rejects", 2, engine="spark")
            self.assertIn("stage=spark_startup", " ".join(raised.exception.__notes__))
            self.assertTrue(no_table(url))

    def test_sequential_engine_never_imports_spark(self):
        import builtins
        native_import = builtins.__import__
        def guarded(name, *args, **kwargs):
            if name.startswith("pyspark"):
                raise AssertionError("sequential attempted Spark import")
            return native_import(name, *args, **kwargs)
        with tempfile.TemporaryDirectory() as directory, private_schema(TEST_URL) as url:
            with patch("builtins.__import__", side_effect=guarded):
                result = run_pipeline(ROOT / "fixtures/payouts/2026-11-18.jsonl", "jsonl", url, Path(directory) / "rejects", 2)
            self.assertEqual(result.inserted, 3)

    def test_real_parallel_tasks_and_complete_generated_values(self):
        with tempfile.TemporaryDirectory() as directory, private_schema(TEST_URL) as url:
            root = Path(directory)
            workload = generate_workload(root / "input", 10000)
            measurements = ImportMeasurements("t2-parallel-witness")
            def guarded_partition(lines):
                import os
                from claims_data_lab.spark_transform import _convert_partition
                if any(key in os.environ for key in ("DATABASE_URL", "TEST_DATABASE_URL", "PGPASSWORD")):
                    raise RuntimeError("Database credentials reached a conversion worker")
                yield from _convert_partition(lines)
            with patch("claims_data_lab.spark_transform._convert_partition", guarded_partition):
                result = run_pipeline(Path(workload["jsonl"]["path"]), "jsonl", url, root / "rejects", 500,
                                      engine="spark", measurements=measurements)
            self.assertEqual((result.inserted, result.rejected), (10000, 0))
            from claims_data_lab.benchmark import verify_workload
            self.assertTrue(verify_workload(url, Path(workload["jsonl"]["path"]), 10000)["valid"])
            details = measurements.details["spark"]
            self.assertEqual(sum(p["rows"] for p in details["partitions"]), 10000)
            nonempty = {p["partition"] for p in details["partitions"] if p["rows"]}
            tasks = [t for t in details["transformation_tasks"] if t["partition"] in nonempty and t["successful"]]
            self.assertGreaterEqual(len(tasks), 2)
            self.assertTrue(any(a["partition"] != b["partition"] and max(a["launch_ms"], b["launch_ms"]) < min(a["finish_ms"], b["finish_ms"]) for a in tasks for b in tasks))
            self.assertEqual(details["prefetch"], False)
            evidence = ROOT / "artifacts/t2-parallel-witness.json"
            evidence.parent.mkdir(exist_ok=True)
            evidence.write_text(json.dumps(measurements.snapshot(), indent=2))
