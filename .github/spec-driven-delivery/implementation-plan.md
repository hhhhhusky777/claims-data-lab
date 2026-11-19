# Implementation Plan — Phase 1 transformation scaffold

<!-- sdd: implementation-plan -->

## Delivery status

| Field | Value |
| --- | --- |
| State | `DRAFT` |
| Active tasks | `None` |
| Next ready task | T1 after plan acceptance |
| Active blocker | Plan review and owner acceptance |
| Implementation mode | human-review-before-merge; Autopilot off |
| Delivery branch / target | `feature/phase1-transform` / `main` |
| Owner | Repository owner hhhhhusky777 |
| Primary need | [PR #2](https://github.com/hhhhhusky777/claims-data-lab/pull/2) |
| Concluded whiteboard | [Solution](solution-whiteboard.md), accepted content `1b21d43`, frozen by `8c6305a` |
| Required reviewers | Retained phase1_design_a and phase1_design_b |
| Last verified | 2026-11-19; both reviewers verified metadata-only conclusion transition |

## Governing inputs

The owner's narrowed transformation-only scope and explicit design acceptance govern delivery. The [whiteboard](solution-whiteboard.md) owns D1–D7, V1/V2 and FC1. [AGENTS.md](../../AGENTS.md) owns the learner-implementation boundary. Preserve fixture bytes and secrets. The assistant delivers declarations, type annotations and English docstrings with NotImplementedError method/function bodies. Runtime code, SQL implementation, executable integration tests and performance tuning are learner work.

## Tasks and design mapping

All tasks use `feature/phase1-transform`, PR #2, required target `main`, under one final-PR integration model. There are no intermediate task merges. Only the final reviewed delivery may merge with separate owner authority. Assumptions consumed by each task: None.

| ID | State | Depends on | Consumed design | Scaffold work and outcome | Boundaries | Error handling / validations | Acceptance evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| T1 | PLANNED | None | D1–D7 | Define the complete typed module surface below, with English contracts and unimplemented bodies | Fixed mapping, sequential pipeline; no product behavior | Docstrings express D5/D6, FC1, V1 and V2 | Python syntax/import checks and inspection of annotations and NotImplementedError bodies |
| T2 | PLANNED | T1 | D1–D3, D7; canonical authority updates | Add a payout CSV equivalent and expected converted values; reconcile README and manifest to the accepted scope; document learner run/setup goals | Keep existing input fixtures; no financial report/version merge requirements | Describe row rejection, plain repeated inserts and ambiguous commit handling | Compare fixture correspondence and check documentation links/contracts against T1 |
| T3 | PLANNED | T1, T2 | D1; D2/D5/D6 evidence obligations | Complete scaffold inspection and provide a learner acceptance-scenario inventory in README | No executable product-test implementations or live database claims | Name V1/V2/FC1 scenarios and distinction between confirmed rollback and unknown commit | Annotated scaffold is importable; all runtime scenarios explicitly pending learner implementation |

## Module and callable inventory

Each module has a purpose docstring. Each function/method has Inputs, Returns, Effects, Raises and an example where useful. Body is `raise NotImplementedError`. Type-only containers may use dataclasses with annotated fields and no custom behavior. Use Python 3.11+ annotations, Psycopg 3 connection typing, pathlib.Path, datetime.date and collections.abc interfaces. Annotations describe supported values; they do not validate them at runtime.

| Module | Declaration | Learner responsibility described in annotation/docstring |
| --- | --- | --- |
| `models.py` | `SourceRecord(source_file: str, source_record: int, values: dict[str, object])` | Parsed raw object with source location |
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
| T1/T3 assistant | Declarations import, annotations resolve in the configured environment, bodies are unimplemented | Planned scaffold checks |
| T2 assistant | CSV/JSONL fixtures describe corresponding values, dates and negative amounts | Planned fixture/document inspection |
| Learner runtime | Query actual PostgreSQL columns after each supported input format | Pending implementation |
| Learner runtime | Repeat IDs are retained; reimport appends; version remains ordinary payload | Pending implementation |
| Learner runtime | Empty data input, reordered CSV columns, extra fields, final partial group | Pending implementation; use accepted required-header rule for CSV |
| Learner runtime | Malformed JSON, blank JSONL line, null/missing field, invalid integer/date and BIGINT overflow | Pending V1 tests |
| Learner runtime | Invalid batch_rows fails before insertion | Pending V2 test |
| Learner runtime | Fatal error after an earlier group leaves no file inserts; prior committed rows remain | Pending FC1 integration test |
| Learner runtime | Unknown commit outcome is reported honestly, without automatic replay | Pending diagnostic verification |

## Validation, review and completion

Focused scaffold checks use Python syntax/import inspection with declared dependencies available. No database/container or package download is needed just to review this plan. Dependency installation and runtime commands are documented only when chosen within accepted scope and download authority. Product correctness remains unproven until learner implementation and actual PostgreSQL tests.

Before final tests, complete the playbook implementation audit, retained reviews and human implementation acceptance. Before final merge review, reconcile README and manifest, preserve the full concluded whiteboard and plan in one closing archive, reset the live whiteboard and remove this live plan according to the accepted closing workflow. Detailed approval, validation and merge evidence belongs in PR #2. No archive/reset occurs during planning.

| Delivery outcome | Definition of Done | Current result |
| --- | --- | --- |
| Annotated starter | Complete callable inventory with no product implementation | Pending T1 |
| Canonical contract | README/manifest reflect PostgreSQL transformation and fixture expectations | Pending T2 |
| Learner handoff | Scaffold checks pass and unrun runtime acceptance is explicit | Pending T3 |
| Merge candidate | Required review, exact-head validation and closing archive/reset | Pending final readiness and owner authority |

## Human decision

Accept the scaffold-only task plan. Autopilot remains off unless explicitly enabled. With it enabled, authority is limited to in-scope scaffold preparation through required review and final merge readiness; human implementation acceptance and merge into main remain separate. No learner runtime implementation is authorized by this plan.
