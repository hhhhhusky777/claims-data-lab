# Implementation Plan — Phase 3 AI Engineering

<!-- sdd: implementation-plan -->

This is the only active-delivery state authority. Exact reviews, commands, run results and owner decisions belong in [PR #9](https://github.com/hhhhhusky777/claims-data-lab/pull/9).

## Delivery status

| Field | Value |
| --- | --- |
| State | `DRAFT` |
| Active tasks | Plan review; implementation not started |
| Next ready task | T1 after retained plan reviews and owner plan acceptance |
| Active blocker | Owner plan acceptance after reviews |
| Implementation mode | Human-controlled; human-review-before-merge; Autopilot OFF |
| Authorized scope / feature branch / protected target | Accepted D1–D7 offline foundations; feature/phase3-ai → main |
| Delivery branch / target | One feature PR #9, feature/phase3-ai → main; no task merges |
| Owner | Repository owner hhhhhusky777 |
| Primary issue / need | [Issue #8](https://github.com/hhhhhusky777/claims-data-lab/issues/8) |
| Concluded whiteboard | [Whiteboard](solution-whiteboard.md), accepted design1d58018; metadata-only conclusiona32f993 verified by both retained reviewers |
| Required reviewers | phase3_review_a and phase3_review_b, retained for this delivery |
| Last verified | 2026-11-24; owner approved design, workload and V/FC dispositions; no product changes; runtime pin d038cdb CURRENT; plan review pending |

## Governing inputs and delivery boundaries

Explicit owner instructions and [AGENTS](../../AGENTS.md) govern readable full implementation, English identifiers/docstrings and stopping after each completed task for code reading. The owner selected Phase3 task naming without lettered subphases. The frozen whiteboard owns architecture, file/chunk/revision/database/API/CLI/error contracts. [README](../../README.md) owns existing import semantics and performance authority; [manifest](project-adoption-manifest.md) indexes installed pin/project authorities. No new authority or behavior is introduced by this plan.

In scope: real local text preparation; isolated rag tables and pgvector setup; atomic document lifecycle; expected-revision complete vector publication; compatible exact cosine query; prepare/inspect CLI; offline evidence, docs and optional dependencies. Out of scope: OpenAI SDK/provider calls, model selection, question embedding, natural-language search, generated answers, prompt/output work, payout tools, API/UI, cloud, PDF/Word and Spark optimization. OPENAI_API_KEY remains blank/unread. Fixed numeric vectors are isolated test data, not generated semantic embeddings.

Accepted workload: three synthetic documents, each≤1MiB raw bytes, one operation at a time. No SLO, throughput, production capacity or semantic-quality target is assumed. Existing diagnostic timers can record operation/stage elapsed seconds and environment. Full-document memory is deliberate within D1; no unbounded corpus load or ANN tuning.

## Consumed design and contracts

D1 prepares normalized text, canonical hash and versioned1000-character source slices. D2 owns atomic replacement/invalidation and consistent reads. D3 publishes only a complete vector set for the locked current revision. D4 owns exact model/dimension-scoped cosine lookup and deterministic ties. D5 keeps separate prepare/inspect commands and original importer compatibility. D6 owns truthful real-file/database/fixed-vector evidence and learning stops. D7 owns existing-library reuse, shared diagnostics and preflighted pinned runtime setup. V1–V3 and FC1–FC3 are approved; this plan adds no admission rule or fail-closed behavior.

## Tasks — the single design-to-task mapping

Every row uses feature/phase3-ai, PR #9, required target main. Tasks are reviewed increments on that same branch, not independent merges.

| ID | State | Depends on | Consumed design | Implementation work | Outcome | Boundaries | Consumed error handling | Consumed validations | Acceptance evidence | Consumed assumptions | Branch / PR / required target |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| T1 | PLANNED | None | D1; D5/D6 readable pure preparation | Add rag package with typed preparation/chunk/revision models and real-file normalization/hash/chunk functions; add three synthetic policy/operations files and independent unit fixtures; add a short reading section in README | A developer can call prepare_document and inspect deterministic canonical text/chunks without a database or key | No DB imports/effects, provider, schema or CLI yet; preserve payout modules; follow exact D1 slice/hash/line rules | Fatal file/decode errors retain cause/source; validation occurs before any persistence | V1/FC1 input subset: nonblank ID, .md/.txt, UTF-8/BOM and≤1MiB; empty/whitespace input remains valid zero chunks | Real-file unit tests for canonical hash/newlines/BOM/Unicode, long-line splits, offsets/lines/IDs, whitespace/empty and failure/boundary cases; existing transformation tests; both task reviews and reading stop | Accepted small-file workload; Python3.12.9 present; no package download required | feature/phase3-ai / #9 / main |
| T2 | PLANNED | T1 | D2; D5 storage setup/inspection; D7 runtime ownership | Preflight and pin pgvector server/adapter requirements with existing Psycopg3.3.2; provision owned PG16 test runtime; add schema initialization plus save/load functions consuming T1 models; real integration tests; setup/reading docs | Prepared documents persist with source metadata and consistent snapshots; identical revision preserves vectors/chunks, changed revision atomically replaces chunks and invalidates vectors | Separate rag_* tables; no payout migration/writer changes; no vector publication/search yet; setup explicit, load/inspect never creates extension/tables | Psycopg transaction rollback, original cause/stage, uncertain COMMIT inspection; locks/upserts serialize same-document saves | Consume D2 revision lifecycle; V1/FC1 not-found lookup; no new runtime admission rule | Real PG+pgvector schema and save/load checks; no-op content with path refresh, changed-content invalidation, zero chunks, failure after earlier writes and rollback; consistent snapshot and distinct-schema/payout preservation; focused old DB behavior checks; reviews/stop | Accepted PG16/Psycopg3; exact extension/adapter/transitive pins/cache/direct route are setup outputs, not assumptions of readiness | feature/phase3-ai / #9 / main |
| T3 | PLANNED | T2 | D3/D4; D6 numeric evidence; D7 adapter reuse | Add complete expected-revision embedding-publication API and exact cosine retrieval API, reusing T2 ownership/schema; shared vector input conversion where the same contract applies; known-vector integration tests; reading docs | A developer can atomically publish compatible supplied vectors and retrieve correctly ranked, source-located hits with declared scope | No embedding generator/upload CLI, OpenAI SDK, question text search, rank threshold, ANN or fake production vectors; fixed vectors only in private tests | Preserve primary error and transaction state; typed stale-revision/publication validation at API owner; no automatic retries | V2/FC2 complete/current/model/dimension/finite/nonzero publication; V3/FC3 model/query/limit; pgvector owns supported type/dimension limits | Known cosine order/ties, model/dimension filtering, prepared exclusions/empty success; incomplete/stale/malformed/zero/float32 boundary inputs; rollback after partial writes; current-revision protection, whole-model replacement and snapshot provenance; reviews/stop | Small accepted corpus; fixed-vector correctness is not semantic relevance; exact server/adapter pins from T2 | feature/phase3-ai / #9 / main |
| T4 | PLANNED | T3 | D5 separate CLI; D6 learning/evidence; D7 shared diagnostics; canonical updates | Add python -m claims_data_lab.rag prepare/inspect composition and JSON output; RAG_DATABASE_URL configuration and blank-key example; integrate existing logging/run context/timing; real subprocess tests; reconcile README/manifest contracts, setup/tests/performance boundaries and code reading | User can prepare and inspect real documents without a key and see honest prepared/vectors-present metadata and source spans; old importer remains usable | Only two accepted commands; no init-db/search/ask command or live provider work; initialization stays documented setup; no duplicate telemetry or policy file | Existing exit conventions2for arguments/missing URL,1for operation failure,0success; Python preserves cause, CLI safe output; inspect missing ID fails explicitly | Compose V1/FC1 and consumed store invariants; no new checks beyond accepted D1–D7/V1–V3 | Real CLI prepare/inspect, blank/unset key, independent old importer without optional AI imports, source-safe file failures, not-found/DB errors and credential-safe messages; JSON readiness/offsets; focused suite and reviews; stop before combined audit | Accepted local workload; no dotenv dependency/provider account required; detailed coverage inventory below | feature/phase3-ai / #9 / main |

### Why these dependencies exist

T2 consumes T1's canonical document/revision/chunk values. T3 consumes T2's actual schema, transaction and snapshot owner. T4 consumes prepared store and its final readiness/retrieval contracts, keeping one coherent public interface/reading guide. Required independent reviewers can work concurrently; implementation remains sequential to preserve owner reading stops. No task's own runtime/setup output is a prerequisite for beginning that setup task.

## Reading anchors and implementation clarity

These are bounded code responsibilities/signature intentions, not scaffold-only output or another mapping. Full implementation is authorized by the owner's learning request; equivalent names/layout may be refined without changing accepted behavior.

- rag/models.py: typed document revision, source-located chunk, prepared/stored document snapshot and retrieval hit/scope values. No generic model/provider registry.
- rag/documents.py: prepare_document(path, document_id) and pure chunk_text(canonical_text), consuming D1's exact constants and offsets. Validate/read once and retain the canonical snapshot.
- rag/store.py: initialize_schema(database_url), save_document(database_url, prepared) and load_document(database_url, document_id); native Psycopg connection/transaction ownership. Keep schema and lifecycle SQL in this owner.
- rag/retrieval.py: publish_embeddings(database_url, expected_revision, embedding_model, vectors) and search_chunks(database_url, query_vector, embedding_model, limit); fixed parameterized SQL and pgvector adapter, not a second store framework.
- rag/cli.py and rag/__main__.py: accepted prepare/inspect arguments and service composition. Pure preparation runs before document writes; inspection is read-only. No changes to the existing import entrypoint.

## Test and acceptance contracts

This is the single coverage inventory, not a test-run log. Actual names/results are recorded by owning tasks in PR9. Non-focused missing cases, if any, must be explicitly assigned to final-gate work; no skipped integration case is a passing result.

| Owning task | Accepted contract / material risk | Test or scenario | Coverage | Work boundary |
| --- | --- | --- | --- | --- |
| T1 | D1 exact text/hash/source provenance | Independent temporary .md/.txt files; BOM/CRLF/CR/Unicode; manual expected chunks/offsets/line spans; newline and hard-cut boundaries; whitespace-only slices/empty document | Planned tests/test_documents.py | Focused T1 |
| T1 | V1 bounded real input | Exactly1MiB and1MiB+1 bytes; wrong suffix, blank ID, invalid decoding and file errors; no DB import or effect | Planned tests/test_documents.py | Focused T1 |
| T2 | D2 separate persisted document lifecycle | Real rag tables, correct rows/text/provenance, same-content preservation/path refresh, changed revision invalidates all vector metadata, empty prepared document | Planned tests/test_document_store.py | Focused T2 |
| T2 | D2 transaction and snapshot consistency | Inject write error after prior group/statement; verify old complete revision survives; concurrent committed replacement cannot mix inspected document/chunk revisions; lock/upsert same-document save invariants; payout rows preserved | Planned tests/test_document_store.py | Focused T2 |
| T2 | Setup/adapter and optional dependency boundary | Real CREATE EXTENSION/type registration in owned DB, pinned versions; no install/download at importer runtime; base import path remains independent | Planned setup receipt and relevant old pipeline tests | Focused T2 |
| T3 | D3/V2/FC2 complete-current publication | Known vectors, complete count, stale expected revision under row lock, wrong model/shape/dimension, nonfinite/boolean/nonzero/float32-representable bounds, zero chunks, rollback and whole-set replacement | Planned tests/test_retrieval.py | Focused T3 |
| T3 | D4/V3/FC3 exact retrieval | Independent cosine examples, deterministic tie order, model/dimension/prepared exclusions, limit, zero/nonfinite query, successful empty result, one-snapshot text/revision/spans and declared search scope | Planned tests/test_retrieval.py | Focused T3 |
| T4 | D5 key-free CLI and compatibility | Real subprocess prepare/inspect with key unset/blank; exact JSON/status/spans, missing doc, invalid args/files, absent URL, DB failure; sanitized public errors while API retains cause; old default CLI usable without optional AI imports | Planned tests/test_rag_cli.py plus existing test_cli.py | Focused T4 |
| T4 | D7 diagnostic and timing truth | Real operation receives run ID/stage and revision context; failed-operation cause preserved; no secrets/raw-document logging; existing measurement snapshot semantics reused | Planned relevant store/CLI scenarios | Focused T4 |
| Final delivery | All D1–D7 and unchanged import behavior | Reconcile inventory after tasks and add necessary missing cases only after combined implementation audit/owner acceptance; full unittest suite with real PostgreSQL, Spark and pgvector enabled; runtime validation/diff check | Unrun; commands/settings finalized with task setup | Final gate |

No semantic retrieval/LLM correctness is assigned to these tests. A future live delivery needs genuine embeddings and separate semantic/answer evaluation, not additional offline stub claims here.

## Integration, compatibility and operational readiness

| Concern | This delivery |
| --- | --- |
| Integration | Single feature PR9; tasks remain on feature/phase3-ai; protected target main; no per-task merges |
| Existing callers/data | Original CLI/API/schema/append/transaction/default engine unchanged; no SQL migration of imported_payouts |
| New schema/setup | Explicit documented initialization creates only owned rag tables and pgvector in an owned database; never initialize during inspect; exact setup API is not a new user CLI command |
| Runtime | T1 uses existing Python3.12.9 and standard-library tests. T2 creates worktree-local optional environment from persistent cached packages where suitable; pin every added dependency after metadata verification. Retained project-owned cache/Java is available for later full-suite checks |
| Downloads | Preflight exact extension image/source/adapter/transitive dependencies. Check shared daily proxy ledger and route/caches/domestic mirrors. Never infer DIRECT or silently fallback through proxy; GitHub traffic exempt. No package/image download performed during planning |
| Database ownership | Test only project-owned disposable PostgreSQL16+pgvector with labels and private schemas. Do not modify/restart unrelated stacks or existing payout DB; keep6060closed. Remove only owned test container/volumes when finished |
| Recovery | Existing transaction ownership, primary causes and diagnostic context preserved; ambiguousCOMMIT requires inspection of revision/readiness before retry; no generic retry or compensation service |
| Readability/learning | Stop after every completed reviewed task. Explain actual functions with a concrete fixture and implemented/planned distinction |
| Canonical updates | T1–T4 reading/setup/contracts in README; T4 reconciles full manifest inventory and performance/test links. AGENTS needs no change per accepted whiteboard |
| Acceptance | Author and both retained reviewers audit combined actual implementation/flows after tasks; then explicit owner implementation acceptance before final tests; separate main merge authority |

## Risks and readiness gaps

| Item | Handling / owner | State |
| --- | --- | --- |
| pgvector not present in cached PG images; exact compatible pins/download route unresolved | T2 preflight/install from verified cache/direct mirror under host allowance; failed setup is not integration success; do not mutate another stack | Expected task setup, not a starting blocker for T1 |
| Retained Phase2 environment contains historical activation/script paths | Use cached packages/runtime only through verified project-owned support; create/verify worktree-local environment before dependent tests; do not assume copied scripts work | Verify during runtime setup |
| Provider API key absent | No key read/call/dependency in scope; do not fabricate embeddings/answers | Accepted boundary |
| New requirement discovered during implementation | Pause affected scope and request exact design amendment; frozen whiteboard unchanged for routine task state | No open design gap |

## Plan validation and completion

### Implementation review

Pending until T1–T4 finish. The author will complete the actual implementation/design, business/data/error views, review findings, risks/limits, exact validation and recommendation table in this section before the combined audit. No product implementation or final test success is claimed by this plan.

### Delivery Definition of Done

| Outcome | Required evidence | Current result |
| --- | --- | --- |
| Accepted D1–D7 delivered | T1–T4 outcomes with real-file/database/fixed-vector evidence and retained reviews | Pending |
| Old interfaces/data preserved | Existing import/default behavior, no optional AI import requirement, no payout migration | Pending |
| Key-free preparation honest | Runnable prepare/inspect, blank/unset key, prepared vs vectors-present status; no semantic/LLM success claims | Pending |
| Durable authority updated | README carries actual document/vector lifecycle, commands/setup/pins/tests/performance meaning; manifest indexes same owners; AGENTS justified unchanged | Pending |
| Applicable validation | Focused evidence per task; required combined audit/owner implementation acceptance; exact-head full integration suite and runtime validation | Pending |
| Closing canonical state | Preserve complete whiteboard+plan in one combined archive, then remove live plan/reset whiteboard before final-candidate review, per installed closing contract | Pending |
| PR-owned merge outcome | Exact reviews/results, owner acceptance and separate merge authorization, target verification in PR9 | Pending |

### Cleanup inventory

| Item | Disposition | Ownership / boundary |
| --- | --- | --- |
| Whiteboard and this plan | Archive complete sources together, then reset/remove live delivery state at closure | Only after implementation acceptance and closure preparation; preserve all accepted design/task/outcome/cleanup content |
| Product/models/tests/synthetic fixtures/README/manifest/KB | Keep | Reusable project output/authority; never rewrite historical archives |
| Test schemas/containers/volumes | Remove when owned verification boundary ends | Record exact names/labels; preserve unrelated stacks and data |
| Cached packages/Java/downloads and useful ignored evidence | Keep | Persistent project-owned support; exclude from Git; preserve exact-candidate evidence |
| Worktree/branch | Retire only after authorized merge and exact-target verification | This delivery only; preserve reusable evidence/cache and uncommitted work |

## Human review brief

HUMAN_DECISION: accept this four-task plan and authorize T1 implementation. AGENT_ACTION: implement only the next authorized task, run focused checks, obtain retained reviews and stop for reading. DISCLOSE: all new functionality/tests are unimplemented; exact pgvector setup is T2 work and live AI stays deferred. NONE: new design decisions/validations/fail-closed behaviors. Main merge and final implementation acceptance remain separate.
