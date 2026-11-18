# Claims Data Lab

Claims Data Lab is a hands-on data engineering project for a fictional insurer. Finance needs a reliable daily report of net claim payouts by product and currency. A later increment will help support staff answer policy questions with citations to the applicable clause.

The first version runs locally. The project then progresses through measured performance improvements, distributed execution, and new features. All sample records are synthetic.

## Phase 1 — Build a working daily report

Build a Python batch command that loads the policy lookup and one payout batch at a time, validates each row, and publishes a report. Provide a read-only report endpoint. SQLite is a reasonable starting point for demonstrating repeatable imports and atomic publication.

| Input | Contract |
|---|---|
| `fixtures/policies.csv` | One row per `policy_id`, with its `product`. The lookup is fixed for this phase. |
| `fixtures/payouts/YYYY-MM-DD.jsonl` | One payout version per line. Fields are `payout_id`, `version`, `policy_id`, `settled_on`, `currency`, and signed integer `amount_cents`. The filename is the delivery date. `settled_on` controls the reporting date. |

Keep payout history keyed by `(payout_id, version)` and select the highest accepted version for each payout. A reversal has its own payout ID and a negative amount. Preserve the source inputs so batches can be replayed, and record rejected rows with a reason and their input location.

Publish `daily_claims_paid` with one row per `settled_on`, `product`, and `currency`. Each row has `transaction_count` and `net_paid_cents`. Expose the last successful batch ID or date with the report metadata. Keep currencies separate and calculate amounts in integer cents.

### Definition of Done

- A clean setup can process both fixture files using the documented command and read the report through `GET /reports/claims-paid`.
- After the first batch, November 18 Travel totals $150. The second batch corrects it to $140. Running the second batch again leaves the report unchanged.
- The second batch has one rejected row, T6, because its policy ID is unknown. The four published rows below total $390. Reconciliation checks compare valid current payout counts and signed amounts against the report by settlement date and currency.
- An older payout version arriving after a correction does not roll the correction back. A simulated failure before publication leaves the last successful report readable. A retry produces the expected result.
- Contributors can run the same acceptance checks from a clean checkout. Add setup and run instructions with the first implementation.

| Settlement date | Product | Currency | Transactions | Net paid in cents |
|---|---|---|---:|---:|
| 2026-11-18 | Home | USD | 1 | 20000 |
| 2026-11-18 | Travel | USD | 2 | 14000 |
| 2026-11-19 | Home | USD | 1 | 7000 |
| 2026-11-19 | Travel | USD | 1 | -2000 |

## Phase 2 — Measure and improve performance

Generate a larger, repeatable workload. Capture end-to-end batch time, rows processed per second, and report query latency. Use the query plan and measurements to pick one bottleneck. If rebuilding history dominates, try republishing only the affected dates. When a correction moves a payout to a different date, rebuild both the old and new dates. Record before-and-after results on the same inputs and verify identical report rows.

## Phase 3 — Scale and extend in small increments

| Increment | What it should demonstrate |
|---|---|
| Spark and Parquet | Run the same report in PySpark. Compare ordinary and broadcast join plans, inspect shuffle metrics, and verify output parity with the local baseline. |
| Azure operations | Run a scheduled job against ADLS and Delta tables. Verify run status, retry behavior, and published results. |
| Change data capture | Consume database changes and resume from a checkpoint. Reconcile results after duplicates and out-of-order delivery. |
| Policy Q&A | Compare keyword and embedding retrieval on labeled questions. Cite the applicable policy version and clause. Abstain when evidence is missing. |
| Internal UI | Show report filters and freshness, and display policy answers with citations. |

Keep ingestion, publication, and serving behind clear boundaries. Ingestion owns validation and payout versions. Reporting owns aggregation and publication. The API reads the published result. That keeps the report contract stable when the compute engine or data source changes.

## First contribution

Start with a test for `$150 → $140 → $140 after replay`. Implement the smallest pipeline that passes it, then add rejection, reconciliation, and the interrupted-publication test.

## Performance Goals and Evidence

The repository owner approves workload boundaries and performance goals. Correctness is the first milestone. The small tracked fixtures establish expected results and provide no capacity or latency claim. No numeric performance target has been accepted yet.

Before optimization, agree on generated row counts, key distribution and skew, batch sizes, query frequency, concurrency and the available machine resources. Add a reproducible benchmark command with its seed and parameters when the runnable pipeline exists. Record end-to-end batch seconds, accepted rows per second, and report query latency in milliseconds. Capture the commit, environment, dataset parameters and repeated-run results with the comparison in the pull request. Compare unchanged workloads and verify identical reports before interpreting improvement. Document variability and unmeasured conditions.

Revisit the contract with the owner when workload, resources, deployment or performance goals change. Missing benchmark inputs defer dependent performance choices while Phase 1 correctness work proceeds.

## Delivery playbook setup

The [adoption manifest](.github/spec-driven-delivery/project-adoption-manifest.md) records the installation state and pin. Each clone generates its own ignored runtime. Obtain the installer from the exact revision and run it in the project root:

```sh
git clone https://github.com/hhhhhusky777/spec-driven-delivery-playbook.git ../claims-sdd-playbook
git -C ../claims-sdd-playbook checkout --detach d038cdb6878232583085231f57dcc4b872124242
cp ../claims-sdd-playbook/install-sdd.sh ./install-sdd.sh
./install-sdd.sh --revision d038cdb6878232583085231f57dcc4b872124242
```

Give the agent `.sdd-runtime/agent-guide.md` and follow its installed skill. See [AGENTS.md](AGENTS.md) for learning and project authority boundaries.
