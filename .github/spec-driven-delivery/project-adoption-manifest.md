# Project Adoption Manifest — Claims Data Lab

<!-- sdd: manifest -->

## Installation

| Field | Value |
| --- | --- |
| Project / repository | https://github.com/hhhhhusky777/claims-data-lab |
| Adoption state | `INSTALLED` |
| State before block | `None` |
| Playbook source repository | `https://github.com/hhhhhusky777/spec-driven-delivery-playbook.git` |
| Playbook revision | `d038cdb6878232583085231f57dcc4b872124242` |
| Upgrade state | `NONE` |
| Upgrade candidate | `None` |
| Adoption scope | This repository and its learning deliveries |
| Project adoption root | `.github/spec-driven-delivery` |
| Knowledge Base root | `.github/spec-driven-delivery/knowledge-base` |
| Materialization mode | Installer-owned immutable checkout, local to each worktree |
| Target base revision | `26c219a905c2ea98cdf19210e9fa39928962abd9` |
| Adoption owner | Repository owner hhhhhusky777 |
| Required review | Two independent adoption reviews followed by human acceptance |
| Last verified | 2026-11-23; README and authority inventory reconciled with accepted PostgreSQL transformation design; installation acceptance remains in PR #1 |

## Canonical project authorities and discovery inventory

| Concern | Canonical source / evidence | Applicability and owner |
| --- | --- | --- |
| Contributor entry and business/data contracts | [README](../../README.md) | Owner-maintained Phase 1 contract and staged roadmap |
| Test strategy and commands | [README Definition of Done](../../README.md#definition-of-done) | Runtime and unit/integration test commands are documented; detailed run evidence belongs in the PR |
| Branch, review, and merge | [AGENTS.md](../../AGENTS.md) | Owner accepts adoption and authorizes target merge |
| Security and synthetic data | [README](../../README.md), [AGENTS.md](../../AGENTS.md) | Synthetic fixtures only; keep credentials and generated state out of Git |
| Documentation and learning boundaries | [AGENTS.md](../../AGENTS.md) | English identifiers/docstrings; implementation scope follows the owner’s explicit learning request |
| Data and concurrency | [README](../../README.md#input-contract) | Fixed types, append-only import, bounded groups and one transaction per file |
| CI, release, operations | [README](../../README.md#quick-start) | Local PostgreSQL setup and cleanup; CI and cloud release remain future work |
| Performance Goals and Evidence | [README](../../README.md#performance-goals-and-evidence) | Owner-defined staged approach and accepted starting synthetic workload; numeric improvement targets and detailed benchmark configuration remain for Phase 2 |
| Durable delivery decisions | [solution whiteboard](solution-whiteboard.md), [delivery archives](archive/), Git and pull requests | Whiteboard owns current design; an active implementation plan owns task state; closing archives preserve completed delivery sources |
| Project Experience | [Experience](experience.md) | Verified lessons maintained by contributors, subject to current authority |
| Reusable guidance | [Knowledge Base](knowledge-base/README.md) | Complete installer-managed tree at the pinned revision |

## Stable boundaries

Adoption prepares reusable guidance and project authority. Product implementation, performance tuning and cloud deployment follow separate feature decisions. Autopilot is off. Repository owner acceptance is required before changing the manifest to INSTALLED. Pull requests own detailed review and acceptance evidence. Future features reconcile the authority index with the current repository.

The installer and runtime are machine-local and ignored through Git's local exclude file. New clones bootstrap their own runtime using the pinned installer instructions in README. Preserve the tracked Knowledge Base during runtime cleanup. Abandon an unaccepted adoption by retaining main and discarding only this candidate's adoption changes after owner authorization.

## Validation and gaps

| Item | Evidence / disposition |
| --- | --- |
| Documentation | Check relative links and `git diff --check` for this adoption |
| Runtime | `./install-sdd.sh --revision d038cdb6878232583085231f57dcc4b872124242 --validate` |
| Product tests | [README tests](../../README.md#tests) documents unit and isolated PostgreSQL integration commands; review evidence remains in the PR |
| Performance | Starting row counts, group size and repeats are accepted in README; dataset shape, runtime resources and measurement details require feature-design acceptance before dependent benchmark work; numeric improvement targets remain unset |
| CI and environments | Local run instructions exist; automated CI and cloud deployment remain deferred |
| Playbook deviations | None proposed |
| Human decision | Owner accepted adoption of candidate 243c302 after both reviews; acceptance recorded in [PR #1](https://github.com/hhhhhusky777/claims-data-lab/pull/1). Protected-target merge remains separate. |
