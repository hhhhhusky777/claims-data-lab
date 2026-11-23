# Implementation Plan — Phase 1 transformation implementation

<!-- sdd: implementation-plan -->

## Delivery status

| Field | Value |
| --- | --- |
| State | `IN_PROGRESS` |
| Active tasks | `None` — stopped after T3 as requested |
| Next ready task | None; final readiness awaits owner continuation |
| Active blocker | None |
| Implementation mode | human-review-before-merge; Autopilot off; stop after every completed task |
| Delivery branch / target | `feature/phase1-transform` / `main` |
| Owner | Repository owner hhhhhusky777 |
| Primary need | [PR #2](https://github.com/hhhhhusky777/claims-data-lab/pull/2) |
| Concluded whiteboard | [Solution](solution-whiteboard.md), accepted A1 content `7b6d7cc`, frozen by `8f94dbe` |
| Required reviewers | Retained phase1_design_a and phase1_design_b |
| Last verified | 2026-11-19; owner accepted A1 and plan; both reviewers verified metadata-only conclusion transition |

## Governing inputs

The owner's narrowed transformation-only scope and explicit design acceptance govern delivery. The [whiteboard](solution-whiteboard.md) owns D1–D7, V1/V2 and FC1. [AGENTS.md](../../AGENTS.md) owns the learner-implementation boundary. Preserve fixture bytes and secrets. The owner explicitly requested fully implemented code for reading and learning. The assistant implements runtime code and focused tests with type annotations and English docstrings, then stops after each completed task. Performance tuning stays deferred. A1 supersedes the earlier stub-only boundary.

## Tasks and design mapping

All tasks use `feature/phase1-transform`, PR #2, required target `main`, under one final-PR integration model. There are no intermediate task merges. Only the final reviewed delivery may merge with separate owner authority. Assumptions consumed by each task: None.

| ID | State | Depends on | Consumed design | Implementation work and outcome | Boundaries | Error handling / validations | Acceptance evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| T1 | DONE | None | D1, D3, D4, D6 | Implement models, errors, incremental CSV/JSONL readers and fixed transformation with focused unit tests | File-to-typed-record slice; database and CLI follow later | V1 row errors versus fatal framing/header errors; preserve source locations | Actual fixture records convert to expected types; rejects, extra/reordered columns, BOM, strict dates/integers and iterator behavior tested |
| T2 | DONE | T1 | D1–D7 | Implement database schema, writer-owned transaction, batched inserts and pipeline; test actual PostgreSQL behavior | Plain append, one file transaction, bounded batches; no business merge or queue | V1/V2 as consumed, FC1 rollback and unknown commit; diagnostic output failures propagate | Real PostgreSQL inserts, final partial batch, append-on-rerun and rollback-after-earlier-group evidence |
| T3 | DONE | T1, T2 | D1, D2, D7; canonical updates | Implement CLI, add corresponding CSV fixture, setup/run instructions, reconcile README/manifest and complete end-to-end tests | Environment credentials; scope remains transformation only | CLI V2, redacted stage errors, committed counts | CLI-to-PostgreSQL execution for both formats; actual rows and rejects verified; closing docs reviewed |

After each task passes its focused checks and both retained reviewers approve its exact candidate, mark it DONE and stop. The owner chooses when to continue. Do not start a dependent task automatically.

## Module and callable inventory

Each module has a purpose docstring. Each function/method has Inputs, Returns, Effects, Raises and an example where useful, and a complete implementation. Data containers use dataclasses with annotated fields where suitable. Use Python 3.11+ annotations, Psycopg 3 connection typing, pathlib.Path, datetime.date and collections.abc interfaces. Annotations describe supported values; they do not validate them at runtime.

| Module | Declaration | Implementation responsibility |
| --- | --- | --- |
| `models.py` | `SourceRecord(source_file: str, source_record: int, values: dict[str, object], input_format: Literal['csv', 'jsonl'] = 'jsonl')` | Parsed raw object with source location and format; format distinguishes CSV integer text from native JSON integers |
| `models.py` | `ImportedPayout(source_file: str, source_record: int, payout_id: str, version: int, policy_id: str, settled_on: date, currency: str, amount_cents: int)` | Fixed target fields; database generates technical ID |
| `models.py` | `RejectedRecord(source_file: str, source_record: int, reason: str)` | Row rejection diagnostic |
| `models.py` | `PipelineResult(inserted: int, rejected: int)` | Counts for a successfully committed file; failures raise instead of returning success |
| `readers.py` | `read_csv(path: Path) -> Iterator[SourceRecord \| RejectedRecord]` | Incremental logical CSV rows, required headers and UTF-8 BOM handling; fatal framing errors propagate |
| `readers.py` | `read_jsonl(path: Path) -> Iterator[SourceRecord \| RejectedRecord]` | Incremental physical lines; malformed JSON and non-object values produce located rejects |
| `transform.py` | `transform_record(record: SourceRecord) -> ImportedPayout` | Fixed field mapping, required fields and strict integer/date conversion; raise RowValidationError for expected row errors |
| `errors.py` | `RowValidationError(ValueError)` | Type declaration for expected transformation errors; no custom methods |
| `database.py` | `transaction(connection: Connection) -> AbstractContextManager[None]` | Writer-owned context manager commits on success and rolls back fatal failure; pipeline enters it without duplicating transaction logic |
| `database.py` | `create_tables(connection: Connection) -> None` | Create the agreed fixed schema inside the writer-owned transaction |
| `database.py` | `insert_batch(connection: Connection, records: Sequence[ImportedPayout]) -> int` | Bind values and use executemany; plain inserts, return staged count, never commit here |
| `pipeline.py` | `run_pipeline(input_path: Path, input_format: Literal['csv', 'jsonl'], database_url: str, reject_path: Path, batch_rows: int) -> PipelineResult` | Own file/connection cleanup and coordinate processing inside database.transaction; flush partial final group and stream rejects; propagate fatal errors through writer rollback while preserving causes |
| `cli.py` | `main(argv: Sequence[str] \| None = None) -> int` | Parse arguments, read DATABASE_URL, validate batch_rows before inserts, invoke pipeline, redact secrets and display outcome |

The fixed field conversion in transform.py is the mapping. Avoid generic schema/mapping frameworks, queue classes, report/API modules and repository abstractions. Table creation is called inside the pipeline's setup/transaction boundary. Each successful invocation appends rows. Logging uses Python's logging framework with source/stage context, excluding database credentials.

## Acceptance inventory

| Owner | Scenario | Current coverage / boundary |
| --- | --- | --- |
| T1 | Real fixture JSONL conversion; equivalent CSV records; malformed/non-object/blank JSONL; missing/null fields; strict dates and BIGINT integers including booleans and overflow | Implemented in tests/test_transformation.py; 18 tests passed on Python 3.14.5 |
| T1 | Reordered/extra CSV columns, required headers, malformed framing, logical CSV record numbers, BOM and incremental iteration | Implemented in tests/test_transformation.py; 18 tests passed on Python 3.14.5 |
| T2 | Actual PostgreSQL schema and inserted values; repeated IDs/reimport append; opaque version values | Implemented in tests/test_pipeline.py; real PostgreSQL 16.14 integration passed |
| T2 | Bounded insert groups and final partial group; confirmed rollback after earlier insert; unknown commit truthfully propagated | Implemented in tests/test_pipeline.py; 14 T2 tests passed, including simulated unknown-COMMIT propagation |
| T2 | Rejection output remains diagnostic; output/read failure aborts file transaction | Implemented in tests/test_pipeline.py; real PostgreSQL 16.14 integration passed |
| T3 | CLI options, DATABASE_URL handling, invalid batch_rows before inserts, redacted errors, committed counts | Implemented in tests/test_cli.py; 10 focused T3 tests passed |
| T3 | Both real input formats through CLI into PostgreSQL, expected values and reject locations | Implemented subprocess-to-PostgreSQL cases in tests/test_cli.py; focused T3 checks passed |

## Validation, review and completion

Run task-focused tests before both retained reviewers inspect each completed task. Use an isolated local development PostgreSQL instance for database checks, verifying environment ownership first. Reuse available dependency caches and follow host download rules before fetching packages or images. If actual PostgreSQL evidence cannot be obtained, report the gap and leave T2 incomplete rather than substitute mocks for integration proof. Unit tests may isolate failure paths that cannot be safely reproduced with a real connection. Final-gate checks wait for the playbook implementation audit and human acceptance.

Before final tests, complete the playbook implementation audit, retained reviews and human implementation acceptance. Before final merge review, reconcile README and manifest, preserve the full concluded whiteboard and plan in one closing archive, reset the live whiteboard and remove this live plan according to the accepted closing workflow. Detailed approval, validation and merge evidence belongs in PR #2. No archive/reset occurs during planning.

| Delivery outcome | Definition of Done | Current result |
| --- | --- | --- |
| File transformation | Complete typed readers and conversion with focused tests | Implemented; 18 focused tests passed; both retained reviewers approved T1 candidate 42e52d6 |
| Transactional ingestion | PostgreSQL file transaction and bounded inserts verified | Implemented; 14 focused T2 tests passed against PostgreSQL; both reviewers approved 15bcc4a |
| Runnable learning project | CLI, fixtures and README/manifest reflect tested PostgreSQL behavior | Implemented; 10 focused T3 tests passed; both retained reviewers approved 524935a |
| Merge candidate | Required review, exact-head validation and closing archive/reset | Pending final readiness and owner authority |

## Task T1 result

Implemented claims_data_lab models, errors, readers and transformation with the Python standard library. SourceRecord includes input_format metadata to distinguish CSV numeric strings from strict JSON native integers. Both retained reviewers approved exact T1 candidate 42e52d6 with no findings and independently passed all 18 tests. Task T1 is DONE; execution stops until owner continuation. No database, queue or CLI behavior is introduced. Python 3.14.5 focused test command `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v` passed 18 tests. The tests use actual temporary files and tracked JSONL fixtures. PostgreSQL and end-to-end obligations remain owned by T2/T3.

## Task T2 result

Implemented database.py (fixed table schema, writer-owned transaction, parameterized executemany groups) and pipeline.py (incremental orchestration, rejection JSONL and committed counts). Dependencies are pinned in requirements.txt. run_pipeline takes an explicit batch_rows with no default; diagnostics append to existing rejection files so confirmed-rollback retries can reuse the same paths. The rejection path must be distinct from the input, including aliases, to preserve source bytes under the existing project content-protection boundary.

Fourteen T2 focused tests passed against a dedicated PostgreSQL 16-alpine container with private per-test schemas. Test entry point: `TEST_DATABASE_URL=<isolated-test-url> PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover -s tests -p test_pipeline.py -v`. Reuse the pinned requirements in a virtual environment. Integration tests are explicitly skipped when TEST_DATABASE_URL is absent. Confirm that the supplied database is a disposable local test instance before using it. Real database evidence covers typed CSV/JSONL inserts, bounded/final groups, append-on-repeat, rejection locations, confirmed rollback after framing/database/output failures and append-to-existing diagnostic output and input/alias preservation. Unknown-COMMIT identity and retry behavior are tested with an isolated simulated driver failure; no real network fault is claimed.

Both retained reviewers approved corrected T2 candidate 15bcc4a with no unresolved findings. Task T2 is DONE; execution stops before T3. The verification container is owned by this task with labels codex.project=claims-data-lab and codex.task=T2; it reuses the cached postgres:16-alpine image. Unrelated environments are outside its scope. After review, the owned test container and its anonymous data volume are removed; the virtual environment is retained for reuse. Product README and manifest contract reconciliation are delivered by T3, replacing the superseded reporting instructions.

## Human decision

A1 and the revised plan were accepted in chat. Stop after T1 completes its focused checks and retained reviews. T2 continuation was explicitly authorized in chat. T3 continuation was explicitly authorized in chat. Stop after its focused checks and retained reviews; final feature readiness and merge remain separate. Autopilot is off; final audit/validation and protected-target merge remain separate decisions.

### T2 review correction

Reviewer A found that exclusive diagnostic output creation introduced an unnecessary existing-path prohibition. Removed that prohibition: diagnostics append across runs. Tests now reuse the same paths and verify preserved earlier diagnostic rows. A direct/symlink/hardlink input-output collision is prevented to preserve the source, consuming the existing AGENTS.md protected-content obligation. No automatic replay, business-key handling or new pipeline stage was added.

## Task T3 result

Implemented cli.py and __main__.py, the equivalent CSV payout fixture, local setup/inspection/cleanup instructions and the PostgreSQL transformation contract in README. Reconciled stable manifest authority links and gaps. CLI requires explicit positive batch_rows and format; DATABASE_URL comes from the environment. It returns committed counts as JSON with exit 0, fatal pipeline failures with exit 1 and argument/environment errors with exit 2. Raw exception/connection strings are withheld from CLI output while the Python API retains the original errors.

Focused command: `TEST_DATABASE_URL=<isolated-test-url> PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover -s tests -p test_cli.py -v`. Both retained reviewers approved exact task candidate 524935a with no findings and independently reran all ten focused tests. Task T3 is DONE; stop before final feature readiness. Ten T3 tests passed on Python 3.14.5 with Psycopg 3.3.2 against a task-owned PostgreSQL 16-alpine container, including four subprocess-to-database cases. Unit cases verify parser behavior, missing credentials, committed-count output and redaction. Subprocess cases verify both actual fixture formats, rejects, fatal rollback and argument rejection before database effects. Relative links and whitespace checks passed. Final feature validation has not run; the owner’s requested task stop precedes final readiness and archive/reset.

The task-owned T3 verification container and anonymous data volume are removed after review. Dependencies remain in the existing virtual environment for reuse. No final audit, full-suite result, archive/reset or merge readiness is claimed at this task stop.

## Implementation review

| Information | Implementation summary / evidence |
| --- | --- |
| Implementation / design | D1–D7 implemented across models/readers/transform, database/pipeline and CLI. Fixed mapping, opaque version payload and plain append preserved. Writer owns one file transaction with parameterized executemany groups. T1/T2/T3 task reviews approved the exact task candidates recorded above. |
| Code-level flows | Business sequence, data flow and error activity below describe current cli.py, pipeline.py, readers.py, transform.py and database.py. |
| Review findings | Task review found the unnecessary exclusive rejection-output rule; it was removed in 15bcc4a and both reviewers approved append behavior plus source protection. Final combined audit findings belong in PR #2. |
| Risks / limits / unknowns | Largest-record size is unbounded; batch_rows limits record count. File transactions may be large. No schema migrations, deduplication, update semantics, parallel pipeline, Spark or performance target. Unknown COMMIT uses a driver simulation in tests, not a real network fault. Rejection artifacts can survive rollback. |
| Validation | Task-focused results: 18 T1, 14 T2 and 10 T3 cases passed at their recorded candidates. No combined final suite has run. Exact-head full validation waits for human implementation acceptance. No new product changes in this audit preparation. |
| Recommendation / decision | Review the combined implementation, then request human implementation acceptance to begin final validation. Protected-target merge and closure remain later explicit boundaries. |

### Business sequence

```mermaid
sequenceDiagram
    actor User
    participant CLI
    participant Pipeline
    participant ReaderTransform as Reader and transformer
    participant PG as PostgreSQL
    User->>CLI: Input, format, batch size and rejects path
    CLI->>Pipeline: run_pipeline with DATABASE_URL
    Pipeline->>PG: Begin file transaction and ensure table
    loop Incremental records and bounded groups
        Pipeline->>ReaderTransform: Read and convert next record
        ReaderTransform-->>Pipeline: Typed payout or row rejection
        Pipeline->>PG: Insert accepted full groups
    end
    Pipeline->>PG: Insert final group and commit
    Pipeline-->>CLI: Committed inserted/rejected counts
    CLI-->>User: JSON result and exit 0
```

### Data flow

```mermaid
flowchart LR
    F[CSV or JSONL file] --> R[read_csv or read_jsonl]
    R --> S[SourceRecord]
    S --> T[transform_record]
    T --> P[ImportedPayout batch]
    P --> W[insert_batch and SQL parameters]
    W --> DB[(imported_payouts)]
    R --> E[RejectedRecord]
    T --> E
    E --> J[Appended diagnostic JSONL]
```

### Error activity

```mermaid
flowchart TD
    N[Read next record] --> O{Outcome}
    O -->|Valid| B[Buffer and insert groups]
    B --> N
    O -->|Expected row error| R[Append located rejection]
    R --> N
    O -->|End of input| C[Flush final group and close output]
    C --> M[Commit file transaction]
    M --> S[Return committed counts]
    O -->|Fatal read failure| X[Abort and transaction rollback]
    B -->|Database error| X
    R -->|Output error| X
    C -->|Output close error| X
    X --> E[Preserve cause and report failure]
    M -->|Connection lost during commit| U[Report failure with unknown commit possibility]
    U --> Q[Operator inspects PostgreSQL before retry]
```

## Phase 2 handoff

The owner selected a starting benchmark profile on 2026-11-23: 100,000 and 1,000,000 synthetic rows, 500-row groups, three runs per size, one importer and one isolated local PostgreSQL instance, measuring throughput and peak memory before setting improvement targets. [Issue #3](https://github.com/hhhhhusky777/claims-data-lab/issues/3) records this agreement. Phase 2 starts with a reproducible sequential baseline; data-shape/resource details and a normal Phase 2 design remain to be recorded. No Phase 2 product code or benchmark run has begun. This handoff does not alter the frozen Phase 1 design.
