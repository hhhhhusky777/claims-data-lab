"""Compare the two accepted engines without changing their processing strategies.

Run python -m claims_data_lab.comparison against an owned local PostgreSQL.
Generate once, alternate engine order, retain all observations and compare
complete stored multisets before dropping the pair's private schemas.
"""

import importlib.metadata
import json
import os
import statistics
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql

from .benchmark import environment_evidence, private_schema, run_benchmark_import
from .workloads import SIZES, generate_workload


ENGINES = ("sequential", "spark")


def compare_stored_rows(database_url: str, left_url: str, right_url: str) -> dict:
    """Compare every payload/source column and duplicate multiplicity in SQL.

    Schema names come from owned connections and use Identifier quoting. Ignore
    generated technical IDs. EXCEPT ALL retains duplicates; compare both ways.
    This check is outside both timed importer processes.
    """
    schemas = []
    for url in (left_url, right_url):
        with psycopg.connect(url) as connection:
            schemas.append(connection.execute("SELECT current_schema()").fetchone()[0])
    select = sql.SQL("SELECT source_file,source_record,payout_id,version,policy_id,settled_on,currency,amount_cents FROM {}.imported_payouts")
    left, right = [select.format(sql.Identifier(name)) for name in schemas]
    query = sql.SQL("""
        WITH left_only AS ({} EXCEPT ALL {}), right_only AS ({} EXCEPT ALL {})
        SELECT (SELECT count(*) FROM left_only), (SELECT count(*) FROM right_only)
    """).format(left, right, right, left)
    with psycopg.connect(database_url) as connection:
        left_only, right_only = connection.execute(query).fetchone()
    return {"left_only_rows": left_only, "right_only_rows": right_only,
            "identical": left_only == right_only == 0}


def run_pair(database_url: str, workload: dict, input_format: str, repeat: int, directory: Path) -> dict:
    """Retain two private tables through verification and preserve partial evidence.

    Scheduled repeats alternate AB/BA/AB; a failed observation is not retried.
    Original exceptions keep their identity and attach retained pair/run context.
    """
    directory.mkdir()
    order = ENGINES if repeat % 2 else tuple(reversed(ENGINES))
    pair = {"pair_id": directory.name, "format": input_format, "rows": workload["rows"],
            "repeat": repeat, "engine_order": list(order), "runs": [], "status": "failed"}
    stage = "pair_schema_setup"
    try:
        with private_schema(database_url) as sequential_url, private_schema(database_url) as spark_url:
            urls = {"sequential": sequential_url, "spark": spark_url}
            for engine in order:
                stage = f"import_{engine}"
                run = run_benchmark_import(database_url, workload, input_format,
                    directory / f"{engine}-{input_format}-{workload['rows']}-{repeat}",
                    engine=engine, schema_url=urls[engine])
                pair["runs"].append(run)
            if all(run["status"] == "passed" for run in pair["runs"]):
                stage = "pair_multiset_comparison"
                pair["comparison"] = compare_stored_rows(database_url, sequential_url, spark_url)
                pair["status"] = "passed" if pair["comparison"]["identical"] else "failed"
            else:
                pair["comparison"] = {"state": "not_compared", "reason": "import_failed"}
            stage = "pair_schema_cleanup"
        stage = "pair_observation_write"
        (directory / "observation.json").write_text(json.dumps(pair, indent=2))
    except BaseException as error:
        retained_run = getattr(error, "benchmark_observation", None)
        if retained_run is not None and not any(r["run_id"] == retained_run["run_id"] for r in pair["runs"]):
            pair["runs"].append(retained_run)
        pair.update({"status": "failed", "failed_stage": stage, "error_type": type(error).__name__})
        error.comparison_observation = pair
        error.add_note(f"Comparison pair={pair['pair_id']} stage={stage}; no automatic retry")
        try:
            (directory / "observation.json").write_text(json.dumps(pair, indent=2))
        except OSError as persistence_error:
            error.add_note(f"Pair observation persistence failed: {type(persistence_error).__name__}")
        raise
    return pair


def append_pair(pair: dict, pair_report, run_report) -> None:
    """Append the pair and every obtained run, marking comparison validity."""
    pair_report.write(json.dumps(pair) + "\n")
    pair_report.flush()
    for run in pair["runs"]:
        run_report.write(json.dumps({**run, "pair_id": pair["pair_id"], "pair_status": pair["status"]}) + "\n")
    run_report.flush()


def summarize_pairs(pairs: list[dict]) -> list[dict]:
    """Summarize complete valid pairs; failures remain in all-runs evidence."""
    results = []
    for rows in SIZES:
        for input_format in ("csv", "jsonl"):
            group = [pair for pair in pairs if pair["rows"] == rows and pair["format"] == input_format]
            for engine in ENGINES:
                selected = [run for pair in group if pair["status"] == "passed"
                            for run in pair["runs"] if run["engine"] == engine and run["status"] == "passed"]
                durations = [run["process_seconds"] for run in selected]
                rates = [run["accepted_rows_per_second"] for run in selected]
                memory = [run["sampled_process_tree_rss_bytes"] for run in selected
                          if run["sampled_process_tree_rss_bytes"] is not None]
                results.append({"rows": rows, "format": input_format, "engine": engine,
                    "scheduled_pairs": len(group), "valid_pairs": len(selected),
                    "median_process_seconds": statistics.median(durations) if durations else None,
                    "range_process_seconds": [min(durations), max(durations)] if durations else None,
                    "median_rows_per_second": statistics.median(rates) if rates else None,
                    "range_rows_per_second": [min(rates), max(rates)] if rates else None,
                    "median_sampled_rss_bytes": statistics.median(memory) if memory else None,
                    "range_sampled_rss_bytes": [min(memory), max(memory)] if memory else None})
    return results


def main() -> int:
    """Run all 24 observations and four small warm-ups on the accepted profile."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("Set DATABASE_URL to an owned disposable PostgreSQL database", file=sys.stderr)
        return 2
    directory = Path("artifacts/phase2-comparison") / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8])
    directory.mkdir(parents=True)
    print(f"Artifacts: {directory}", flush=True)
    try:
        metadata = environment_evidence(database_url)
        metadata["engines"] = list(ENGINES)
        metadata["pyspark"] = importlib.metadata.version("pyspark")
        metadata["py4j"] = importlib.metadata.version("py4j")
        metadata["java_home"] = os.environ.get("JAVA_HOME")
        java = str(Path(metadata["java_home"]) / "bin/java") if metadata["java_home"] else "java"
        version = subprocess.run([java, "-version"], capture_output=True, text=True)
        metadata["java_version"] = {"returncode": version.returncode, "output": version.stderr.strip()}
        metadata["ordering"] = "AB/BA/AB per format/size; fresh process; current materialized Spark; overlap issue#6 deferred"
        (directory / "environment.json").write_text(json.dumps(metadata, indent=2))
        workloads = [generate_workload(directory / "inputs", rows) for rows in SIZES]
        (directory / "workloads.json").write_text(json.dumps(workloads, indent=2))
        warmup = generate_workload(directory / "warmup", 100)
        for engine in ENGINES:
            for input_format in ("csv", "jsonl"):
                result = run_benchmark_import(database_url, warmup, input_format,
                    directory / f"warmup-{engine}-{input_format}", engine=engine)
                (directory / f"warmup-{engine}-{input_format}.json").write_text(json.dumps(result, indent=2))
                if result["status"] != "passed":
                    print("Warm-up failed; inspect retained diagnostics", file=sys.stderr)
                    return 1
        pairs = []
        with (directory / "pairs.jsonl").open("w") as pair_report, (directory / "runs.jsonl").open("w") as run_report:
            for repeat in range(1, 4):
                for workload in workloads:
                    for input_format in ("csv", "jsonl"):
                        try:
                            pair = run_pair(database_url, workload, input_format, repeat,
                                directory / f"pair-{input_format}-{workload['rows']}-{repeat}")
                        except BaseException as error:
                            retained = getattr(error, "comparison_observation", None)
                            if retained is not None:
                                try:
                                    append_pair(retained, pair_report, run_report)
                                except OSError as report_error:
                                    error.add_note(f"Comparison report persistence failed: {type(report_error).__name__}")
                            raise
                        pairs.append(pair)
                        append_pair(pair, pair_report, run_report)
                        print(f"{pair['pair_id']}: {pair['status']} " + ", ".join(
                            f"{run['engine']} {run['process_seconds']:.3f}s" for run in pair["runs"]), flush=True)
        (directory / "summary.json").write_text(json.dumps(summarize_pairs(pairs), indent=2))
        return 0 if all(pair["status"] == "passed" for pair in pairs) else 1
    except Exception as error:
        retained = getattr(error, "comparison_observation", {})
        print(f"Comparison failed: pair={retained.get('pair_id', 'setup')} stage={retained.get('failed_stage', 'setup')} "
              f"error_type={type(error).__name__}; inspect retained artifacts; no automatic retry", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
