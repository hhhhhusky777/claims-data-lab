# Claims Data Lab

A small, readable data engineering project that transforms CSV and JSON Lines files into typed PostgreSQL rows. Build correctness first, then measure performance and introduce distributed processing and AI features incrementally. All fixtures are synthetic.

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

Memory depends on the largest input record plus the configured batch of transformed records. There is no hard byte limit, queue or parallel worker in this version.

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

## Phase 3 — Extend coverage

Add Azure storage/orchestration, CDC/streaming, policy retrieval and cited AI answers, or an internal UI in independently designed increments. Each extension keeps its input contract and acceptance evidence explicit.

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
