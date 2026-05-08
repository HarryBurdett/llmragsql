# SAM Conventions — Project Context + Decided Answers

This document captures what we've decided about the SAM merge so the
adapter work has a single source of truth. It complements the
[`sam-team-handover.md`](./sam-team-handover.md) — that document is
the formal runbook; this one captures the working state.

## Project context (important to read first)

This is **not** a customer-rolling migration program. It's a
one-off engineering project to centralise development.

| | |
|---|---|
| **Stage** | Active development. No production users. |
| **Test data** | Developer's own working data (intsys, cloudsis, z_demo) |
| **Goal** | Move codebase + test data + future development into SAM |
| **Outcome** | All future work — code changes, tests, deployments — happens inside SAM. The standalone Mac/docker-compose dev environment is retired. |

There is no per-customer cutover plan, no tenant rollout schedule, no
production downtime to plan around. The "migration" is:

1. SAM team answers the 6 outstanding questions
2. We write SAM adapter code (~1 week)
3. We move the Git repo into SAM
4. We rsync the developer's test data into SAM volumes
5. Future development continues in SAM

## Decided answers (Q7-Q12)

These were settled during review and don't need further input.

### Q7 — Where does the code live?

**In SAM.** The current `HarryBurdett/llmragsql` GitHub repo becomes a
frozen archive at cutover. All future commits go to SAM's Git host.

**Mechanics:** `git push --mirror` from the GitHub repo to SAM's repo
preserves full history, branches, and tags. Old GitHub repo archived
read-only for reference.

### Q8 — Where does CI/CD run?

**In SAM.** The current `.github/workflows/*` (GitHub Actions) gets
ported to SAM's CI format. ~1-2 day port (most CI systems use similar
YAML).

### Q9 — Where do developers build / test / deploy?

**In SAM.** Local laptop development workflow is replaced by whatever
SAM provides (cloud IDE, VM, container, etc. — TBC by SAM team).

### Q10 — Rollback window

**~1 week.** Old environment kept warm for ~1 week post-merge, then
retired. Since there are no production users, this is a low-risk
window — primarily there to catch surprises while the new setup
beds in.

### Q11 — Code ownership

**Managed in SAM by named individuals with explicit access rights.**
SAM's access-control model decides who can read, modify, and deploy.

**Bus-factor note:** at least 2-3 individuals should have full access
from day one to avoid a single-person bottleneck if production needs
intervention.

### Q12 — Documentation handover

Already largely solved by existing convention:

- **Opera knowledge** (schema, business rules, query patterns,
  posting rules) — lives in two places already (mandatory rule per
  `CLAUDE.md`):
  - Local: `apps/core/docs/opera_knowledge_base.md`
  - **Central:** `https://github.com/jonathangintsys/aisam.git` —
    the shared knowledge base. **Stays as-is.** Already centralised,
    already platform-independent. SAM-merge doesn't disturb it.
- **App-specific docs** (`docs/`, `marketing/manuals/`) — travel with
  the code into SAM per Q7.

The mandatory "always update both local + central" rule carries over
unchanged.

## Outstanding (Q1-Q6) — awaiting Jonathan

Six questions still block adapter code. Each maps to one piece of code
we have to write. Detailed plain-English versions sent to Jonathan +
Charlie on 2026-05-08.

| # | Question | Unblocks |
|---|---|---|
| Q1 | Where will our containers live (same K8s cluster, separate cluster)? | Caching strategy in SAM clients |
| Q2 | How does SAM put secrets into our containers? | `apps/core/adapters/sam/secrets.py` |
| Q3 | Auth token format + verification key + claims? | `apps/core/adapters/sam/auth.py` |
| Q4 | How does SAM tell us the tenant ID? | Request middleware |
| Q5 | How do our apps authenticate to SAM? | HTTP client config in every SAM adapter |
| Q6 | SAM email service API contract? | `apps/core/adapters/sam/email_storage.py` + `smtp.py` |

## What happens after Jonathan replies

| Day | Action | Owner |
|---|---|---|
| 0 | Q1-Q6 answers received | SAM |
| 1-5 | Write 6 SAM adapter files | Us |
| 6 | Build + push container images to SAM's registry | Us |
| 7-8 | SAM creates deployment manifests | SAM |
| 9 | Deploy in SAM, provision volumes, rsync test data | Joint |
| 10 | Smoke test (developer runs through workflows) | Us |
| 11 | Move Git repo to SAM, switch development | Joint |
| 12-19 | Old environment kept warm for safety | — |
| 20 | Retire old environment | SAM |

**~3 weeks total.** No customer-visible downtime (no customers).

## Out of scope

These are NOT part of the merge — the user's clarification rules them out:

- ~~Per-customer migration runbook~~ — no customers
- ~~Tenant rollout schedule~~ — single developer
- ~~Production cutover windows~~ — no production traffic
- ~~Per-tenant migrator CLI tool~~ — would only be useful if there
  were many customers being migrated repeatedly
- ~~Long parallel-run period~~ — short safety window only
