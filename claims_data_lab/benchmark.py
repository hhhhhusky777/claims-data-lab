"""Reproduce the accepted sequential baseline on an owned local PostgreSQL.

Run with DATABASE_URL set, using python -m claims_data_lab.benchmark. Every
invocation owns its artifact directory and private database schemas. It never
starts/stops Docker, deletes external files, tunes SQL or retries an import.
"""

import json
import os
import platform
import statistics
import subprocess
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter, sleep

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import psutil

from .workloads import SEED, SIZES, generate_workload


@contextmanager
def private_schema(database_url: str):
    """Create/drop only this invocation's schema, preserving a primary failure."""
    schema = "claims_bench_" + uuid.uuid4().hex
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        failure = None
        try:
            yield make_conninfo(database_url, options=f"-csearch_path={schema}")
        except BaseException as error:
            failure = error
            raise
        finally:
            try:
                connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
            except Exception as error:
                if failure is None:
                    raise
                failure.add_note(f"Owned schema cleanup failed: {type(error).__name__}")


def sample_process_tree(pid: int) -> tuple[int | None, list[str]]:
    """Return observed summed RSS; disclose denied samples and normal exit races.

    Summing RSS can double-count shared pages. Short-lived children and peaks
    between samples can be missed. This is not unique physical memory or an
    exact high-water mark. Descendant discovery is owned by psutil.
    """
    errors = []
    try:
        root = psutil.Process(pid)
        processes = [root, *root.children(recursive=True)]
    except psutil.NoSuchProcess:
        return None, ["process_exited"]
    except (psutil.AccessDenied, PermissionError) as error:
        return None, [type(error).__name__]
    total = 0
    for process in processes:
        try:
            total += process.memory_info().rss
        except psutil.NoSuchProcess:
            errors.append("process_exited")
        except (psutil.AccessDenied, PermissionError) as error:
            errors.append(type(error).__name__)
    return total, errors


def observe_import(command: list[str], environment: dict, directory: Path) -> dict:
    """Launch one child and sample its process tree every 100 ms until exit.

    The interval includes process startup and shutdown; polling may overestimate
    elapsed time by up to one sampling interval. Output goes to files, avoiding
    pipe backpressure. On interruption only this owned child is terminated.
    """
    stdout_path, stderr_path = directory / "stdout.json", directory / "stderr.log"
    peak = None
    samples = 0
    errors = set()
    with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
        started = perf_counter()
        process = subprocess.Popen(command, env=environment, stdout=stdout, stderr=stderr)
        try:
            while process.poll() is None:
                rss, observed_errors = sample_process_tree(process.pid)
                errors.update(observed_errors)
                if rss is not None:
                    peak = max(peak or 0, rss)
                    samples += 1
                sleep(0.1)
            returncode = process.wait()
            elapsed = perf_counter() - started
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
    outcome = {
        "returncode": returncode, "process_seconds": elapsed,
        "sampled_process_tree_rss_bytes": peak, "rss_samples": samples,
        "rss_sample_notes": sorted(errors), "sample_interval_seconds": 0.1,
        "stdout": str(stdout_path), "stderr": str(stderr_path),
    }
    if returncode == 0:
        # A corrupt/missing worker result is a failed benchmark, not a success.
        try:
            outcome["import"] = json.loads(stdout_path.read_text())
            outcome["process_overhead_seconds"] = elapsed - outcome["import"].get("stage_seconds", {}).get("pipeline", 0)
        except (ValueError, OSError) as error:
            outcome["result_error_type"] = type(error).__name__
    return outcome


def verify_workload(database_url: str, input_path: Path, rows: int) -> dict:
    """Check every stored column against the accepted formula, outside timing.

    SQL is an independent implementation of the workload contract. Count,
    distinct located record numbers, range and full-column checks jointly prove
    exactly one expected row per source location, including repeated payout IDs.
    Generated identity IDs are intentionally excluded.
    """
    query = """
        SELECT count(*), count(DISTINCT source_record), count(*) FILTER (WHERE
            source_file IS DISTINCT FROM %(path)s OR
            source_record < 1 OR source_record > %(rows)s OR
            payout_id IS DISTINCT FROM ('T' || ((source_record-1) %% 50000)::text) OR
            version IS DISTINCT FROM (1 + (source_record-1) %% 5) OR
            policy_id IS DISTINCT FROM ('P' || ((source_record-1) %% 10000)::text) OR
            settled_on IS DISTINCT FROM DATE '2026-11-18' OR
            currency IS DISTINCT FROM 'USD' OR
            amount_cents IS DISTINCT FROM ((((source_record-1)*7919+42) %% 200001)-100000)
        ) FROM imported_payouts
    """
    with psycopg.connect(database_url) as connection:
        count, distinct, invalid = connection.execute(query, {"path": str(input_path), "rows": rows}).fetchone()
    return {"stored_rows": count, "distinct_source_records": distinct, "invalid_rows": invalid,
            "valid": count == distinct == rows and invalid == 0}


def container_memory_snapshot() -> dict:
    """Optional separate Docker memory snapshot, outside the import interval.

    BENCHMARK_CONTAINER names the caller-owned test container. Snapshots are
    neither per-run peaks nor importer RSS; Docker failures remain explicit.
    """
    container = os.environ.get("BENCHMARK_CONTAINER")
    if not container:
        return {"state": "not_collected"}
    try:
        result = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", container], capture_output=True, text=True)
        if result.returncode:
            return {"state": "unavailable", "returncode": result.returncode}
        data = json.loads(result.stdout)
        return {"state": "snapshot", "memory_usage": data["MemUsage"], "memory_percent": data["MemPerc"]}
    except (OSError, ValueError, KeyError, TypeError) as error:
        return {"state": "unavailable", "error_type": type(error).__name__}


def run_benchmark_import(database_url: str, workload: dict, input_format: str, directory: Path) -> dict:
    """Run and verify one import, retaining observations even if verification fails.

    A failed verification/cleanup never turns a committed child into successful
    benchmark evidence. Retain measured time/RSS and the failed stage, remove
    successful throughput, then propagate the original exception to the owner.
    """
    directory.mkdir()
    path = Path(workload[input_format]["path"])
    run_id = directory.name
    config = {"run_id": run_id, "input": str(path), "format": input_format,
              "rejects": str(directory / "rejects.jsonl")}
    config_path = directory / "config.json"
    config_path.write_text(json.dumps(config, indent=2))
    command = [sys.executable, "-m", "claims_data_lab.benchmark_worker", str(config_path)]
    observation = {"run_id": run_id, "engine": "sequential", "format": input_format,
                   "rows": workload["rows"], "input": workload[input_format], "command": command,
                   "status": "failed", "accepted_rows_per_second": None}
    stage = "schema_setup"
    try:
        observation["database_memory_before"] = container_memory_snapshot()
        with private_schema(database_url) as scoped_url:
            environment = dict(os.environ, DATABASE_URL=scoped_url, PYTHONDONTWRITEBYTECODE="1")
            stage = "import"
            observation.update(observe_import(command, environment, directory))
            result = observation.get("import", {})
            successful = observation["returncode"] == 0 and result.get("inserted") == workload["rows"] and result.get("rejected") == 0
            if successful:
                stage = "verification"
                observation["verification"] = verify_workload(scoped_url, path, workload["rows"])
                successful = observation["verification"]["valid"]
            stage = "schema_cleanup"
        observation["database_memory_after"] = container_memory_snapshot()
        observation["status"] = "passed" if successful else "failed"
        if successful:
            observation["accepted_rows_per_second"] = result["inserted"] / observation["process_seconds"]
    except BaseException as error:
        observation.update({"status": "failed", "accepted_rows_per_second": None,
                            "failed_stage": stage, "error_type": type(error).__name__})
        error.benchmark_observation = observation
        error.add_note(f"Benchmark run={run_id} stage={stage}; measured evidence remains in {directory}")
        try:
            (directory / "observation.json").write_text(json.dumps(observation, indent=2))
        except OSError as persistence_error:
            error.add_note(f"Observation persistence failed: {type(persistence_error).__name__}")
        raise
    (directory / "observation.json").write_text(json.dumps(observation, indent=2))
    return observation

def environment_evidence(database_url: str) -> dict:
    """Collect useful configuration without recording a connection string."""
    with psycopg.connect(database_url) as connection:
        settings = {name: {"value": setting, "unit": unit} for name, setting, unit in connection.execute(
            "SELECT name, setting, unit FROM pg_settings WHERE name = ANY(%s)",
            (["server_version", "shared_buffers", "work_mem", "max_connections", "max_wal_size", "fsync", "synchronous_commit"],)).fetchall()}
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], text=True))
    container = os.environ.get("BENCHMARK_CONTAINER")
    container_config = {"state": "not_collected"}
    if container:
        selected = subprocess.run(["docker", "inspect", "--format",
            '{{json .HostConfig.NanoCpus}} {{json .HostConfig.Memory}} {{json .Image}}', container],
            capture_output=True, text=True)
        container_config = {"state": "captured" if selected.returncode == 0 else "unavailable",
                            "nano_cpus_memory_bytes_image_id": selected.stdout.strip() if selected.returncode == 0 else None}
    docker_vm = subprocess.run(["docker", "info", "--format", "{{json .MemTotal}} {{json .NCPU}}"],
        capture_output=True, text=True) if container else None
    return {"container_config": container_config,
            "docker_vm_memory_bytes_cpus": docker_vm.stdout.strip() if docker_vm and docker_vm.returncode == 0 else None, "commit": commit, "working_tree_dirty": dirty, "python": platform.python_version(),
            "platform": platform.platform(), "logical_cpus": psutil.cpu_count(),
            "physical_memory_bytes": psutil.virtual_memory().total, "psycopg": psycopg.__version__,
            "psutil": psutil.__version__, "postgresql_settings": settings,
            "batch_rows": 500, "repeats": 3, "seed": SEED,
            "timing_note": "process launch through observed successful exit; polling may add up to100ms; generation/reset/verification excluded",
            "memory_note": "maximum sampled sum of importer and descendants RSS; shared pages may be counted twice; not exact peak; DB snapshots separate",
            "cache_note": "uncontrolled OS/DB cache warmth; fresh importer process per run; no cold-cache claim"}


def summarize(observations: list[dict]) -> list[dict]:
    """Retain every observation; summarize successful runs without hiding failures."""
    summaries = []
    for rows in SIZES:
        for input_format in ("csv", "jsonl"):
            group = [r for r in observations if r["rows"] == rows and r["format"] == input_format]
            passed = [r for r in group if r["status"] == "passed"]
            rates = [r["accepted_rows_per_second"] for r in passed]
            summaries.append({"rows": rows, "format": input_format, "runs": len(group), "passed": len(passed),
                "median_rows_per_second": statistics.median(rates) if rates else None,
                "range_rows_per_second": [min(rates), max(rates)] if rates else None})
    return summaries


def main() -> int:
    """Generate the fixed accepted profile, warm up once per format and run it."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("Set DATABASE_URL to an owned disposable PostgreSQL database", file=sys.stderr)
        return 2
    directory = Path("artifacts/phase2-baseline") / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8])
    directory.mkdir(parents=True)
    print(f"Artifacts: {directory}", flush=True)
    try:
        metadata = environment_evidence(database_url)
        (directory / "environment.json").write_text(json.dumps(metadata, indent=2))
        workloads = [generate_workload(directory / "inputs", rows) for rows in SIZES]
        (directory / "workloads.json").write_text(json.dumps(workloads, indent=2))
        warmup = generate_workload(directory / "warmup", 100)
        for input_format in ("csv", "jsonl"):
            warm_result = run_benchmark_import(database_url, warmup, input_format, directory / f"warmup-{input_format}")
            (directory / f"warmup-{input_format}.json").write_text(json.dumps(warm_result, indent=2))
            if warm_result["status"] != "passed":
                print("Warm-up failed; inspect retained diagnostics", file=sys.stderr)
                return 1
        observations = []
        with (directory / "runs.jsonl").open("w") as report:
            for repeat in range(1, 4):
                for workload in workloads:
                    for input_format in ("csv", "jsonl"):
                        run_id = f"sequential-{input_format}-{workload['rows']}-{repeat}"
                        try:
                            result = run_benchmark_import(database_url, workload, input_format, directory / run_id)
                        except BaseException as error:
                            retained = getattr(error, "benchmark_observation", None)
                            if retained is not None:
                                try:
                                    report.write(json.dumps(retained) + "\n")
                                    report.flush()
                                except OSError as report_error:
                                    error.add_note(f"Run report persistence failed: {type(report_error).__name__}")
                            raise
                        observations.append(result)
                        report.write(json.dumps(result) + "\n")
                        report.flush()
                        print(f"{run_id}: {result['status']} {result['process_seconds']:.3f}s", flush=True)
        (directory / "summary.json").write_text(json.dumps(summarize(observations), indent=2))
        return 0 if all(r["status"] == "passed" for r in observations) else 1
    except Exception as error:
        retained = getattr(error, "benchmark_observation", {})
        print(f"Benchmark failed: run={retained.get('run_id', 'setup')} stage={retained.get('failed_stage', 'setup')} "
              f"error_type={type(error).__name__}; inspect artifacts; no automatic retry", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
