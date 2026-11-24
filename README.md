# Claims Data Lab

A small, readable data engineering project that transforms CSV and JSON Lines files into typed PostgreSQL rows. Build correctness first, then measure performance and introduce distributed processing and AI features incrementally. All fixtures are synthetic.

## Project overview

Claims Data Lab is a hands-on data and AI engineering learning system. It starts with reliable file-to-database ingestion, adds measured parallel transformation, and then extends toward evidence-grounded AI. It uses synthetic insurance-style payout data; it is not a production claims platform or a representation of any company's internal architecture.

| Phase | Business / engineering problem | Solution | Current status |
| --- | --- | --- | --- |
| **1. Reliable ingestion** | Convert different source formats into consistent, queryable records without loading an entire file into memory | Streaming CSV/JSONL readers, explicit typed conversion, located rejections, Psycopg batch inserts and one PostgreSQL transaction per file | **Implemented and merged** — [PR #2](https://github.com/hhhhhusky777/claims-data-lab/pull/2) |
| **2. Parallel transformation and measurement** | Learn where distributed processing helps, while preserving Phase 1 correctness | Optional local PySpark partition conversion, one shared driver-side writer, deterministic workloads and paired end-to-end benchmarks | **Implemented and merged** — [PR #5](https://github.com/hhhhhusky777/claims-data-lab/pull/5); sequential remains default |
| **3. AI engineering** | Turn documents and structured data into evidence that an assistant can retrieve and explain | Proposed document RAG with PostgreSQL + pgvector and OpenAI, followed by narrow read-only payout tools | **Design in progress** — [PR #9](https://github.com/hhhhhusky777/claims-data-lab/pull/9); no live embeddings or generated answers yet |

```mermaid
flowchart LR
    F["CSV / JSONL sources"] --> P1["Phase 1: parse and transform"]
    P1 --> DB[("PostgreSQL payout records")]
    F --> P2["Phase 2: optional Spark conversion"]
    P2 --> W["Shared driver-side batch writer"]
    W --> DB
    D["Synthetic policy documents"] -. "Phase 3 planned" .-> V["Chunking, embeddings and retrieval"]
    V -.-> AI["Grounded assistant with citations"]
    DB -. "Planned read-only tools" .-> AI
```

### Phase 1 solution — correctness first

Both file formats feed the same typed conversion and database writer. Readers yield records incrementally, so the application buffers a configurable number of accepted rows rather than the whole input. The mapping is explicit Python for a fixed schema, keeping the learning path easy to inspect.

```mermaid
flowchart TD
    CSV["CSV: logical records, including quoted newlines"] --> R["Located SourceRecord"]
    JSON["JSONL: physical line numbers and native JSON types"] --> R
    R --> T["Validate and convert fields"]
    T -->|"Valid"| B["ImportedPayout batches"]
    T -->|"Expected row error"| J["Located JSONL rejection diagnostics"]
    B --> PG[("PostgreSQL: parameterized inserts")]
    PG --> C["Commit once after complete successful import"]
    X["Fatal read / output / database error"] --> RB["Roll back all database changes for this file"]
```

- **Data contract:** every accepted row is appended. Repeated payout IDs, versions and reimports remain separate observations; there is no implicit deduplication or latest-version merge.
- **Reliability:** expected malformed rows are diagnosed; fatal failures roll back the file transaction. Rejection files are diagnostic and may survive rollback. An interrupted COMMIT requires inspection before retry.
- **Resource boundary:** memory depends on the largest record plus the configured batch; this is not a hard byte limit.
- **Code:** [readers](claims_data_lab/readers.py), [conversion](claims_data_lab/transform.py), [writer/transaction](claims_data_lab/database.py), [pipeline](claims_data_lab/pipeline.py).

### Phase 2 solution — parallel conversion with evidence

Spark is an optional transformation engine. The original sequential engine remains available and is selected by default. Both engines reuse the same conversion rules and driver-owned PostgreSQL writer; Spark workers perform no database writes.

```mermaid
flowchart TD
    F["Original CSV / JSONL"] --> R["Existing incremental reader"]
    R --> E{"Selected engine"}
    E -->|"sequential — default"| S["Convert records incrementally"]
    E -->|"spark — explicit"| D["Stage located envelopes on disk"]
    D --> P["Pure mapPartitions conversion: local two task slots"]
    P --> K["Materialize outcomes in DISK_ONLY cache"]
    K --> I["Driver iterates cached partitions without prefetch"]
    S --> W["Shared bounded Psycopg batch writer"]
    I --> W
    W --> PG[("One PostgreSQL transaction per input file")]
```

The serial reader/staging and full-materialization barrier deliberately preserve the original parser/conversion contract and provide observable parallel task execution. This is **parallel conversion on one machine**, not a distributed database or a proven multi-node deployment. Transformation and database loading do not overlap in the current Spark path.

**What we measured:** 100,000 and 1,000,000 rows, CSV and JSONL, 500-row batches, three observations per engine/format/size. Fresh importer processes and private schemas preserve isolation. Independent expected-value checks and bidirectional SQL `EXCEPT ALL` verify complete rows and duplicate multiplicity before each pair is accepted.

| 1,000,000-row input | Sequential median | Spark median |
| --- | ---: | ---: |
| CSV | 16.01 s | 29.53 s |
| JSONL | 16.31 s | 30.20 s |

All **24 measured imports / 12 pairs**, representing **13.2 million imported rows**, passed complete correctness checks. Spark tasks genuinely overlapped, but the current Spark path was slower end to end on this lightweight local workload. Serial staging, startup, materialization, statistics and transfer add costs; database loading remains shared and sequential. These measurements do not isolate a hardware bottleneck or prove Spark is generally slower.

**Engineering decision:** keep sequential as default; investigate removal of avoidable barriers and conversion/loading overlap in [issue #6](https://github.com/hhhhhusky777/claims-data-lab/issues/6). Report the complete result rather than assuming parallelism guarantees a speedup.

**Validation evidence:** the Phase 2 final candidate passed **84 tests with zero skips**, including real PostgreSQL and Spark integration; the merged source tree matched that validated candidate. Native Spark events proved overlapping conversion tasks. Known SDK socket warnings remain disclosed; sustained stability, production capacity and cluster scaling have not been established. Exact commits, commands and limitations are in [PR #5](https://github.com/hhhhhusky777/claims-data-lab/pull/5).

**Code:** [Spark adapter](claims_data_lab/spark_transform.py), [workloads](claims_data_lab/workloads.py), [measurement supervisor](claims_data_lab/benchmark.py), [paired comparison](claims_data_lab/comparison.py).

### Phase 3 solution — grounded AI (planned)

The next increment extends the pipeline toward a Claims Operations Assistant. RAG is the retrieval-and-generation workflow; PostgreSQL with the pgvector extension is the proposed document/vector store. The OpenAI API supplies embeddings and language generation. The API key is not ready, so preparation that does not require live calls comes first.

```mermaid
flowchart TD
    DOC["Synthetic Markdown / text policy documents"] --> PARSE["Parse, chunk and preserve source spans"]
    PARSE --> PREP[("PostgreSQL: prepared documents and chunks")]
    PREP -. "When API key is available" .-> EMB["OpenAI embeddings"]
    EMB -.-> V[("pgvector: chunk vectors and provenance")]
    Q["User question"] -.-> QE["Embed question"]
    QE -.-> SEARCH["Retrieve relevant chunks"]
    V -.-> SEARCH
    SEARCH -.-> PROMPT["Question plus source evidence"]
    PROMPT -.-> LLM["OpenAI: structured answer with citations"]
    PAY[("Existing payout records")] -. "Later fixed read-only SQL tools" .-> LLM
```

All Phase 3 components in this diagram are **planned**, not implemented. The current policy reference contains product labels only, so new explicitly synthetic policy documents are required; the system cannot infer insurance terms from those labels.

1. **First:** document parsing, chunking, provenance, PostgreSQL storage, retrieval plumbing and offline tests. Store absent embeddings as absent; fixed test vectors must not be presented as working semantic retrieval.
2. **When configured:** generate genuine embeddings, retrieve evidence, and produce cited answers or an explicit insufficient-evidence result. Citations must be checked; schema-valid output alone does not establish truth.
3. **Then:** narrow typed read-only tools retrieve payout records and compute exact explicitly scoped figures in SQL. The model explains supplied evidence; it does not execute arbitrary SQL or make payment decisions.
4. **Evaluate:** retrieval relevance separately from answer support, citation accuracy and abstention; record model/prompt/index versions, latency and token usage. Live quality remains untested until real model calls are performed.

**Learning coverage:** data preparation and lineage, vector retrieval, embeddings, prompt construction, structured output, tool calling, evaluation, observability and resource/cost awareness. Azure adapters, UI, CDC/streaming and cloud deployment remain later independent increments.

### Interview walkthrough

> I built this incrementally: first a reliable streaming CSV/JSONL-to-PostgreSQL importer, then an optional Spark transformation path with the same correctness and transaction contracts. I benchmarked both implementations rather than assuming parallel execution would be faster. The current local Spark version is slower for this lightweight workload, so I retained the sequential default and recorded an optimization follow-up. The next phase is a document-grounded assistant: prepare and retrieve policy evidence, add read-only tools for exact database facts, and evaluate the answers. That AI phase is currently in design, with live API integration still pending.

## Phase 1 — Transform files into PostgreSQL

```text
CSV / JSONL → reader → SourceRecord → conversion → ImportedPayout
                                                      ↓
                                                bounded batch
                                                      ↓
                                            PostgreSQL transaction
```

The input mapping is explicit Python code for one fixed table. Each successful invocation appends every accepted record. Repeated payout IDs and versions remain separate rows. The version field is ordinary payload. The policy lookup in `fixtures/policies.csv` is retained reference material outside this import.

## Quick start

Use Python 3.11 or newer and a local development PostgreSQL database. The examples use Docker for a disposable database, with credentials published here solely for that local demo. Run commands from the repository root.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt

docker run --name claims-data-lab-dev -d \
  -e POSTGRES_USER=claims \
  -e POSTGRES_PASSWORD=claims_local_demo \
  -e POSTGRES_DB=claims \
  -p 127.0.0.1:55432:5432 postgres:16-alpine

docker exec claims-data-lab-dev pg_isready -U claims -d claims
```

Wait until `pg_isready` reports that PostgreSQL is accepting connections. Docker may download the image if it is absent locally. Dependency/image downloads follow your environment's configured mirrors and download rules.

```sh
export DATABASE_URL='postgresql://claims:claims_local_demo@127.0.0.1:55432/claims'
mkdir -p artifacts

.venv/bin/python -m claims_data_lab \
  --input fixtures/payouts/2026-11-18.jsonl \
  --format jsonl \
  --batch-rows 2 \
  --rejects artifacts/rejected.jsonl
```

Expected stdout on a successful import:

```json
{"inserted": 3, "rejected": 0}
```

Inspect the actual rows:

```sh
docker exec claims-data-lab-dev psql -U claims -d claims -c \
  'SELECT payout_id, version, policy_id, settled_on, currency, amount_cents, source_record FROM imported_payouts ORDER BY id;'
```

| payout_id | version | policy_id | settled_on | currency | amount_cents | source_record |
| --- | ---: | --- | --- | --- | ---: | ---: |
| T1 | 1 | P1 | 2026-11-18 | USD | 10000 | 1 |
| T2 | 1 | P1 | 2026-11-18 | USD | 5000 | 2 |
| T3 | 1 | P2 | 2026-11-18 | USD | 20000 | 3 |

The matching CSV contains the same data. Try it with:

```sh
.venv/bin/python -m claims_data_lab \
  --input fixtures/payouts/2026-11-18.csv \
  --format csv \
  --batch-rows 2 \
  --rejects artifacts/rejected.jsonl
```

That second invocation appends three more rows, giving six rows if both examples ran against the same database. The other JSONL fixture, `2026-11-19.jsonl`, imports four rows, including policy P99 and a negative amount. It applies no policy lookup or update interpretation.

## Input contract

Supply one file and an explicit `csv` or `jsonl` format per invocation. CSV fields may be reordered and both formats may contain extra fields. The readers support UTF-8 with an optional BOM. A JSONL record is one object on one physical line. CSV handles quoted commas/newlines and reports logical data-record numbers after its header.

| Field | Required input | Target column type |
| --- | --- | --- |
| payout_id | Nonempty string | TEXT |
| version | CSV signed integer text or native JSON integer | BIGINT |
| policy_id | Nonempty string | TEXT |
| settled_on | Valid YYYY-MM-DD date string | DATE |
| currency | Nonempty string | TEXT |
| amount_cents | CSV signed integer text or native JSON integer | BIGINT |

Integers must fit PostgreSQL's signed 64-bit range. JSON booleans, floats and numeric strings fail integer validation. Extra fields are ignored. A date must have the exact format and be a real calendar date.

The target table is `imported_payouts`. PostgreSQL generates its `id` primary key. The pipeline adds `source_file` and `source_record` to each row. Table creation is automatic, while schema migrations remain future work.

## Transactions, rejects and retries

`--batch-rows` is required and positive. It limits records per insert group, with the final partial group also written. The whole file uses one database transaction, including table creation and all groups. Expected row errors go to the rejection file while accepted rows continue. Fatal read, output or database errors abort the file transaction. Previously committed imports remain intact.

The rejection output is an appended JSONL diagnostic file in an existing directory. Each diagnostic contains `source_file`, `source_record` and `reason`. It may survive a database rollback and can contain entries from multiple attempts. Input and output must be distinct, including symlink/hardlink aliases. Blank or malformed JSONL and missing/invalid row values are rejected. Missing required CSV headers, malformed CSV framing, decoding and file-access errors abort processing.

| CLI outcome | Exit status | Output |
| --- | ---: | --- |
| File committed, possibly with row rejections | 0 | JSON inserted/rejected counts on stdout |
| Fatal import failure | 1 | Stage/type diagnostics on stderr; no success counts |
| Invalid arguments or missing DATABASE_URL | 2 | Argument error and usage on stderr |

The CLI keeps connection strings and raw exception messages out of its output. The Python API preserves original exceptions and causes. A connection failure during COMMIT may leave the outcome unknown. Inspect PostgreSQL before repeating an ambiguous import. A confirmed rollback can be retried, but repeating a successful import appends its rows again. Rejection-output counts describe the current successful invocation, not the cumulative diagnostic file.

## Read the code

1. `claims_data_lab/models.py` defines the records passed between stages.
2. `claims_data_lab/readers.py` yields one located source record or rejection at a time.
3. `claims_data_lab/transform.py` converts fields into target types.
4. `claims_data_lab/database.py` owns schema, grouped inserts and the file transaction.
5. `claims_data_lab/pipeline.py` connects the stages and returns committed counts.
6. `claims_data_lab/cli.py` parses arguments and calls the pipeline. `__main__.py` provides `python -m claims_data_lab`.

For the default sequential engine, memory depends on the largest input record plus the configured batch of transformed records. There is no hard byte limit or producer queue. The optional Spark engine has additional partition/JVM/worker memory, described below.

## Tests

The standard-library unit tests use real temporary files. Database tests are enabled only with `TEST_DATABASE_URL` and create/drop their own private schemas. Use a disposable local database and an account allowed to create schemas.

```sh
# Unit and CLI checks run; database cases explicitly skip without TEST_DATABASE_URL.
.venv/bin/python -m unittest discover -s tests -v

# Enable actual PostgreSQL integration and subprocess CLI checks.
TEST_DATABASE_URL="$DATABASE_URL" .venv/bin/python -m unittest discover -s tests -v
```

Do not interpret skipped integration cases as PostgreSQL evidence. Review results and the current validation boundary in [PR #2](https://github.com/hhhhhusky777/claims-data-lab/pull/2).

## Phase 2 — Sequential benchmark (T1)

The optional benchmark tooling uses Python 3.12.9 and pinned dependencies in `requirements-benchmark.txt`. It keeps the sequential import algorithm and 500-row groups. Install with your configured package mirror and persistent cache:

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-benchmark.txt
```

Use a separate disposable PostgreSQL container. Reuse a cached image where available; preflight any required image download under your host's download rules. The accepted local profile limits this database to one CPU and 1 GiB memory. The benchmark creates and drops only its own private schemas, and never starts or stops containers itself.

```sh
docker run --pull=never --name claims-data-lab-benchmark -d \
  --label codex.project=claims-data-lab --label codex.task=phase2-baseline \
  --cpus 1 --memory 1g \
  -e POSTGRES_USER=claims -e POSTGRES_PASSWORD=claims_local_demo -e POSTGRES_DB=claims \
  -p 127.0.0.1:55433:5432 postgres:16-alpine

docker exec claims-data-lab-benchmark pg_isready -U claims -d claims
export DATABASE_URL='postgresql://claims:claims_local_demo@127.0.0.1:55433/claims'
export BENCHMARK_CONTAINER=claims-data-lab-benchmark
.venv/bin/python -m claims_data_lab.benchmark
```

Wait for database readiness before starting. One invocation generates equivalent 100,000- and 1,000,000-row CSV/JSONL files, performs one small unmeasured warm-up per format, then runs three measured imports per size/format (12 measured imports). Each import uses a fresh Python process and private schema. The workload repeats payout IDs every 50,000 rows and includes signed amounts. Its seed is 42; it injects no invalid records into throughput runs. Generation is incremental and uses real files.

The printed `artifacts/phase2-baseline/<invocation>/` directory contains:

| File | Meaning |
| --- | --- |
| `environment.json` | Commit/dirty state, Python/dependency versions, host capacity, database settings and optional Docker limits/image ID; no connection string |
| `workloads.json` | Seed, row counts, paths, exact byte sizes and SHA-256 hashes |
| `runs.jsonl` | Every measured result, including failures, commands, counts, full-column verification and sampled memory |
| `summary.json` | Successful-run median/range plus passed/total counts; failed imports have no successful throughput |
| Per-run directory | Nonsecret configuration, stdout JSON, correlated logs, rejection diagnostics and `observation.json`; measured evidence survives later verification/cleanup failures |

The supervisor measures process launch through observed exit. Generation, schema setup/cleanup and post-run correctness queries are outside that interval. Polling at 100 ms may add up to one sample interval to observed elapsed time. Worker timers expose read, transform, SQL insert, connection, transaction begin/commit/rollback and whole-pipeline seconds. The residual process interval includes interpreter/module startup, shutdown and polling; it is not pure startup time. Stage intervals can nest, so do not add all timers together.

Memory is the maximum sampled sum of importer/descendant RSS, not an exact peak or unique physical footprint: samples can miss short peaks/children and count shared pages twice. Permission failures and exit races are retained in sampling notes. `BENCHMARK_CONTAINER` adds separate before/after database memory snapshots outside the timed interval; these are not database peaks. OS/database cache warmth is uncontrolled and disclosed. Instruments add overhead, and results describe this instrumented workload and host only.

After each run, an independent SQL expression checks every stored column, source location and expected multiplicity against the accepted formula. Row counts alone are insufficient. Verification is outside the import timing. Generated datasets/results remain ignored; retain sanitized evidence in [PR #5](https://github.com/hhhhhusky777/claims-data-lab/pull/5). Spark and comparative measurements belong to later tasks.

Read the T1 code in this order: `workloads.py` generates real files; `benchmark.py` supervises runs and verifies stored values; `benchmark_worker.py` launches the measured import; `measurements.py` aggregates optional timers used by `pipeline.py` and `database.py`.

```sh
# Focused benchmark tests; database cases skip unless explicitly enabled.
TEST_DATABASE_URL="$DATABASE_URL" .venv/bin/python -m unittest discover -s tests -p test_benchmark.py -v

# After the owned benchmark environment is no longer needed:
docker stop claims-data-lab-benchmark
docker rm -v claims-data-lab-benchmark
```

## Definition of Done

- Both input formats produce the known column values and committed counts in PostgreSQL.
- Required types, rejection locations and append-on-repeat behavior are verified.
- Insert groups stay bounded and the last partial group is written.
- A fatal failure after an earlier insert group rolls back the file's new rows.
- CLI errors have truthful exit status and omit credentials.
- Setup and inspection commands can be reproduced from a clean checkout.

When finished with the disposable example database:

```sh
docker stop claims-data-lab-dev
# Remove this demo container and its anonymous data volume when its data is no longer needed.
docker rm -v claims-data-lab-dev
```

## Phase 2 — Measure and improve performance

Measure the working import baseline before changing it. Candidate experiments include PostgreSQL COPY, [bounded pipeline parallelism](https://github.com/hhhhhusky777/claims-data-lab/issues/3), and the same transformation in PySpark. Compare output correctness, end-to-end throughput, stage costs and memory on identical workloads. Spark can distribute transformation, while destination loading can remain a bottleneck.

## Spark transformation engine (T2)

The optional `--engine spark` path uses classic PySpark 4.0.4 with Python 3.12.9 and Java 17. The verified Java build for this experiment is Temurin 17.0.21.1+2 on macOS ARM64. Install from an existing persistent cache or a verified source following host download rules; PySpark's source distribution is about 434 MB. The ordinary sequential command needs neither Java nor PySpark.

```sh
.venv/bin/python -m pip install -r requirements-spark.txt
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
```

For an unpacked Java archive, set `JAVA_HOME` to its `Contents/Home` directory instead. This workspace keeps its checksum-verified runtime at `../claims-runtime-cache/java17/jdk-17.0.21.1+2/Contents/Home`. The official [Temurin archive](https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.21.1%2B2/OpenJDK17U-jdk_aarch64_mac_hotspot_17.0.21.1_2.tar.gz) SHA-256 is `2a88ccb044a8fb00957be93245da6357c11b55717511509d147cbcc5d93b2109`. The pinned PySpark source SHA-256 is `7e119366364d48e3cda1b1efd1ebc6cd672949500667c9cabbeef858daba700f`. Keep archives/caches outside Git; no host-wide Java installation is required when using an unpacked archive.

With the existing local PostgreSQL setup and DATABASE_URL:

```sh
.venv/bin/python -m claims_data_lab \
  --input fixtures/payouts/2026-11-18.jsonl --format jsonl \
  --batch-rows 500 --rejects artifacts/spark-rejects.jsonl --engine spark
```

Expected stdout is `{"inserted": 3, "rejected": 0}` after commit. Spark diagnostics use stderr. The Python API accepts `run_pipeline(..., engine="spark")`; existing calls and an omitted option remain sequential. A missing runtime is a fatal import error, with no automatic fallback or retry.

```text
Existing CSV/JSONL reader (serial)
  → temporary JSONL envelopes retaining source location and native values
  → Spark textFile partitions, local[2], eight requested partitions
  → pure mapPartitions calls to the existing converter
  → DISK_ONLY outcomes, fully materialized before database loading
  → driver iterator without prefetch
  → shared Psycopg groups and one file transaction
```

The reader still owns quoted multiline CSV, BOM handling and JSONL physical line numbers. This experiment parallelizes conversion, while input parsing/staging and database loading remain driver responsibilities. Workers never receive database URLs or write rejection files. The driver restores its environment after sanitizing credentials from the launched Spark processes. One importer per Python process is supported; each call creates/stops its own SparkContext, and the SDK rejects a second concurrent context. The driver and workers use the same Python executable.

The driver opens the file transaction before staging; fatal staging, worker, transfer, rejection-output or database errors roll it back. Expected row errors keep the same source locations and reasons. All cached outcomes and temporary envelope/package/cache files are cleaned before commit. Cleanup failures preserve an existing primary error. Diagnostic rejection order and generated identity IDs are not comparison keys; duplicate values remain separate appended rows.

Spark is configured for two local task slots, a 1 GiB driver heap and loopback driver RPC, with its UI disabled. The heap setting excludes Python workers/native memory. DISK_ONLY avoids keeping all outcomes in the Python driver, but iteration can require the largest partition, and a single large record can still consume substantial memory. This local setup does not prove multi-machine operation; a cluster would require shared storage and separately verified packaging/security.

Native event logs remain under ignored `artifacts/spark-events/<invocation>/`. Optional ImportMeasurements snapshots retain partition row counts, summed individually pickled-row bytes and native task launch/finish evidence. Those serialized-byte sums exclude Spark framing and object/JVM overhead. The implementation collects only one small metadata item per partition, never the full dataset. Startup, staging, materialization and partition-summary timing are separate; SQL time nests inside driver consumption. T3 reruns both engines for the formal performance comparison after this refactor.

```sh
# Run actual Spark/PostgreSQL task checks; runtime and database must be available.
RUN_SPARK_TESTS=1 TEST_DATABASE_URL="$DATABASE_URL" \
  .venv/bin/python -m unittest discover -s tests -p test_spark_transform.py -v
```

Start reading `pipeline.py` for the shared writer, `spark_transform.py` for staging/materialization/driver iteration, and `transform.py` for the pure conversion boundary. Worker/task failures are tested as fatal; tests also cover multiline/native-type parity, append behavior, owned cleanup and rollback after an earlier insert group. Native event timing proves overlapping local tasks; it is not a production throughput claim.

The pinned SDK can emit socket ResourceWarnings during collection/iteration. Its source relies on garbage collection for those sockets; see the related [Apache issue](https://issues.apache.org/jira/browse/SPARK-54247). Tests retain these warnings. Application-owned iterator/cache/context/temp cleanup is verified, while sustained resource stability is unmeasured.

## Performance comparison (T3)

After installing `requirements-spark.txt` and setting `JAVA_HOME`, use the same owned PostgreSQL profile as T1 (one CPU/1 GiB, cached image, loopback connection). Run:

```sh
export BENCHMARK_CONTAINER=claims-data-lab-benchmark
.venv/bin/python -m claims_data_lab.comparison
```

This generates identical CSV/JSONL inputs once, performs four small unmeasured warm-ups (each engine/format), then runs 24 measured imports: two sizes × two formats × two engines × three repeats. Each size/format pair alternates engine order AB/BA/AB. Every importer starts a fresh Python process; Spark includes a fresh JVM launch. Input hashes, PostgreSQL limits/settings and host/runtime details are retained. The earlier T1 results are historical context; this command reruns sequential and Spark at the same current code revision.

The current Spark engine fully materializes outcomes before loading. [Issue #6](https://github.com/hhhhhusky777/claims-data-lab/issues/6) records a future overlap experiment; it is not part of this comparison.

Under ignored `artifacts/phase2-comparison/<invocation>/`, `runs.jsonl` preserves every measured import, `pairs.jsonl` preserves each pair's comparison/validity and `summary.json` reports per-engine median/range for complete valid pairs, along with scheduled/valid pair counts. Partial observations and original errors survive verification/cleanup failures. Reports do not silently retry or label failed pairs as successful comparison evidence.

Both private tables remain until a bidirectional PostgreSQL `EXCEPT ALL` comparison checks all source/payload columns and duplicate multiplicity, excluding generated identity IDs. Per-run independent formula checks still run. Schema setup, complete-row verification, pair comparison and schema cleanup are outside importer timing; schemas are removed after comparison. Separate database memory snapshots describe conditions before/after each import, not peak memory.

Process elapsed time includes Python/JVM startup, staging, conversion/materialization, partition-statistics collection, transfer, insert groups, application cleanup and commit. Sampled importer/descendant RSS includes JVM/Python workers and may count shared pages twice or miss short peaks. SDK process teardown after the observed importer exit is not separately timed. SQL time nests inside consumption, so these intervals must not be summed. Spark materialization includes staged input, Python conversion, scheduling/serialization and cache writes; it is not isolated converter CPU time. OS/DB cache warmth and unrelated host activity remain uncontrolled and disclosed.

The standard `python -m claims_data_lab.benchmark` entry still runs sequential-only T1-style baselines. T3 adds an engine selection in the internal worker configuration without changing import defaults or adding runtime admission rules. All observations, medians/ranges and the reviewed interpretation are retained in [PR #5](https://github.com/hhhhhusky777/claims-data-lab/pull/5). They are local experiment evidence, with no speed target or production/cluster capacity claim. The comparison retains sequential as the default; future performance changes need their own evidence.

## Phase 3 — Delivery roadmap

See the [planned AI solution](#phase-3-solution--grounded-ai-planned) above and [design PR #9](https://github.com/hhhhhusky777/claims-data-lab/pull/9). Concrete task contracts follow the reviewed design. The existing importers remain unchanged; AI key configuration, live model calls and generated-answer quality are not delivered yet.

## Performance Goals and Evidence

The accepted starting synthetic benchmark profile is 100,000 and 1,000,000 rows, 500-row groups, three runs per size, one importer and one isolated PostgreSQL instance. Throughput and peak memory are measured before improvement targets are set. Dataset shape and runtime-resource configuration belong to the Phase 2 design. See [the Spark experiment](https://github.com/hhhhhusky777/claims-data-lab/issues/4).

The repository owner approves workload boundaries and performance goals. Small fixtures establish correctness and provide no capacity or latency claim. No numeric performance target has been accepted.

The accepted T1 workload uses the deterministic formula in `workloads.py`: T(i mod 50000), version 1+(i mod 5), P(i mod 10000), date 2026-11-18, USD and amount ((i*7919+42) mod 200001)-100000 for zero-based i. Run `python -m claims_data_lab.benchmark` using Python 3.12.9, Psycopg 3.3.2, psutil 7.2.2, one importer and a one-CPU/1-GiB PostgreSQL container. Generated file metadata supplies exact record/file sizes. The command, measurement intervals, correctness checks and interpretation limits are documented above. Revise the accepted profile before changing these workload/resource conditions. Record batch seconds, accepted rows per second and relevant resource measurements, together with commit and environment details, in the existing pull request. Compare repeated runs with unchanged inputs and verify identical stored rows. Document variability and unmeasured conditions. Revisit the contract when workload, resources or goals change.

## Delivery playbook setup

The [adoption manifest](.github/spec-driven-delivery/project-adoption-manifest.md) records the installation state and pin. Each clone generates its own ignored runtime. Obtain the installer from the exact revision and run it in the project root:

```sh
git clone https://github.com/hhhhhusky777/spec-driven-delivery-playbook.git ../claims-sdd-playbook
git -C ../claims-sdd-playbook checkout --detach d038cdb6878232583085231f57dcc4b872124242
cp ../claims-sdd-playbook/install-sdd.sh ./install-sdd.sh
./install-sdd.sh --revision d038cdb6878232583085231f57dcc4b872124242
```

Give the agent `.sdd-runtime/agent-guide.md` and follow its installed skill. See [AGENTS.md](AGENTS.md) for learning and project authority boundaries.
