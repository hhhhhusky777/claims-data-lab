"""Located disk staging and pure local Spark conversion; no database imports.

The file reader remains serial. Spark parallelizes the existing converter,
materializes outcomes on disk, then sends them to the driver iterator. This
module ships only synthetic record payloads and never receives a database URL.
"""

import json
import logging
import os
import pickle
import shutil
import sys
import tempfile
import uuid
import zipfile
from contextlib import contextmanager, nullcontext
from dataclasses import asdict
from pathlib import Path

from .models import RejectedRecord, SourceRecord
from .transform import convert_record


logger = logging.getLogger(__name__)


def _convert_partition(lines):
    """Pure worker operation: decode owned envelopes and reuse conversion."""
    for line in lines:
        envelope = json.loads(line)
        cls = SourceRecord if envelope["kind"] == "source" else RejectedRecord
        yield convert_record(cls(**envelope["record"]))


def _partition_sizes(index, records):
    """Emit one small metadata record per partition, never its full contents.

    Bytes sum individually pickled Python rows; Spark cache framing and resident
    object/JVM overhead are excluded, so this is not a memory bound.
    """
    count = size = 0
    for record in records:
        count += 1
        size += len(pickle.dumps(record, protocol=4))
    yield {"partition": index, "rows": count, "python_pickled_row_bytes": size}


@contextmanager
def _worker_environment():
    """Keep database credentials out of the launched JVM and Python workers.

    This local engine supports one importer per process. Restore the driver's
    original environment on every exit; use its pinned interpreter in workers.
    """
    keys = ("DATABASE_URL", "TEST_DATABASE_URL", "PGPASSWORD", "PGPASSFILE", "PGSERVICE",
            "PGSERVICEFILE", "PGSSLPASSWORD", "PGUSER", "PGHOST", "PGPORT", "PGDATABASE",
            "PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON")
    saved = {key: os.environ.get(key) for key in keys}
    try:
        for key in keys:
            os.environ.pop(key, None)
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def transformation_events(directory: Path) -> dict:
    """Read native event logs for the tagged materialization job and task timing."""
    stages = set()
    tasks = []
    for path in sorted(directory.rglob("*")):
        if not path.is_file() or not path.name.startswith(("local-", "events_")):
            continue
        with path.open(encoding="utf-8") as file:
            for line in file:
                event = json.loads(line)
                if event["Event"] == "SparkListenerJobStart" and (event.get("Properties") or {}).get("spark.jobGroup.id") == "transform-materialization":
                    stages.update(event["Stage IDs"])
                if event["Event"] == "SparkListenerTaskEnd":
                    info = event["Task Info"]
                    tasks.append({"stage": event["Stage ID"], "partition": info["Partition ID"],
                                  "task_id": info["Task ID"], "launch_ms": info["Launch Time"],
                                  "finish_ms": info["Finish Time"], "successful": event["Task End Reason"].get("Reason") == "Success"})
    return {"materialization_stage_ids": sorted(stages),
            "transformation_tasks": [task for task in tasks if task["stage"] in stages]}


@contextmanager
def spark_outcomes(reader, progress, measurements=None):
    """Own staging/cache/context until the driver finishes or aborts consumption.

    Called inside the file transaction, after source/reject alias checks. All
    conversion is materialized before loading. Cache iteration uses no prefetch,
    but may still consume memory as large as the largest result partition.
    Event logs remain under ignored artifacts; temporary envelopes/package/cache
    are removed. Cleanup preserves a primary exception instead of replacing it.
    """
    progress.stage = "spark_startup"
    from pyspark import SparkConf, SparkContext, StorageLevel

    progress.stage = "spark_stage_input"
    temporary = Path(tempfile.mkdtemp(prefix="claims-spark-"))
    events = Path("artifacts/spark-events").resolve() / uuid.uuid4().hex
    context = cached = local_iterator = None
    failure = None
    details = {"event_log_directory": str(events), "master": "local[2]", "requested_partitions": 8,
               "driver_heap": "1g", "prefetch": False}
    timer = measurements.measure if measurements is not None else lambda stage: nullcontext()
    try:
        progress.stage = "spark_stage_input"
        with timer("spark_stage_input"):
            staged = temporary / "located.jsonl"
            with staged.open("w", encoding="utf-8") as output:
                for record in reader:
                    progress.source_record = record.source_record
                    kind = "source" if isinstance(record, SourceRecord) else "rejection"
                    output.write(json.dumps({"kind": kind, "record": asdict(record)}) + "\n")
            # Package only the pure conversion modules and this worker entry.
            package = temporary / "claims-conversion.zip"
            with zipfile.ZipFile(package, "w") as archive:
                for name in ("__init__.py", "models.py", "errors.py", "transform.py", "spark_transform.py"):
                    archive.write(Path(__file__).with_name(name), f"claims_data_lab/{name}")
        progress.stage = "spark_startup"
        events.mkdir(parents=True)
        configuration = (SparkConf(loadDefaults=False).setMaster("local[2]").setAppName("ClaimsDataLabTransform")
            .set("spark.driver.memory", "1g").set("spark.driver.host", "127.0.0.1")
            .set("spark.driver.bindAddress", "127.0.0.1").set("spark.ui.enabled", "false")
            .set("spark.pyspark.python", sys.executable).set("spark.local.dir", str(temporary / "cache"))
            .set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", events.as_uri())
            .set("spark.eventLog.compress", "false").set("spark.eventLog.rolling.enabled", "false"))
        with _worker_environment():
            with timer("spark_startup"):
                context = SparkContext(conf=configuration)
                context.setLogLevel("WARN")
                context.addPyFile(str(package))
            progress.stage = "spark_materialize"
            with timer("spark_materialize"):
                context.setJobGroup("transform-materialization", "Pure conversion before PostgreSQL loading")
                cached = context.textFile(staged.as_uri(), minPartitions=8).mapPartitions(_convert_partition).persist(StorageLevel.DISK_ONLY)
                details["outcomes"] = cached.count()
                context.setLocalProperty("spark.jobGroup.id", None)
            progress.stage = "spark_partition_summary"
            with timer("spark_partition_summary"):
                # Collect ONLY one metadata object per partition, not input rows.
                details["partitions"] = cached.mapPartitionsWithIndex(_partition_sizes).collect()
            progress.stage = "spark_transfer"
            local_iterator = cached.toLocalIterator(prefetchPartitions=False)
            def located_iterator():
                while True:
                    progress.stage = "spark_transfer"
                    try:
                        record = next(local_iterator)
                    except StopIteration:
                        return
                    progress.source_record = record.source_record
                    yield record
            yield located_iterator()
    except BaseException as error:
        failure = error
        raise
    finally:
        cleanup_failure = None
        actions = []
        if local_iterator is not None and hasattr(local_iterator, "close"):
            actions.append(local_iterator.close)
        if cached is not None:
            actions.append(lambda: cached.unpersist(blocking=True))
        if context is not None:
            actions.append(context.stop)
        actions.append(lambda: shutil.rmtree(temporary))
        for action in actions:
            try:
                action()
            except Exception as error:
                if failure is not None:
                    failure.add_note(f"Spark cleanup also failed: {type(error).__name__}")
                elif cleanup_failure is None:
                    cleanup_failure = error
                else:
                    cleanup_failure.add_note(f"Additional cleanup failure: {type(error).__name__}")
        try:
            details.update(transformation_events(events))
        except (OSError, ValueError, KeyError) as error:
            details["event_summary_unavailable"] = type(error).__name__
        if measurements is not None:
            measurements.details["spark"] = details
        logger.info("Spark evidence: %s", details)
        if failure is None and cleanup_failure is not None:
            progress.stage = "spark_cleanup"
            raise cleanup_failure
