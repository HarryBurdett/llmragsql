# Opera 3 for bank-reconcile & gocardless — what Jonathan needs to do to complete it

**Date:** 2026-07-30 · **From:** Harry / Charlie · **For:** Jonathan
**One-line:** The apps and the write agent are essentially done. Two SAM-platform pieces remain, both yours: **(1) merge a ready branch of Opera-3 read fixes, and (2) build multi-connection support so the live SAM can hold an Opera-3 connection alongside Opera SE.** Everything else on the critical path (agent verification, releases, production enable) is ours.

Repos referenced: `jonathangintsys/aisam` (the SAM platform — yours), `jonathangintsys/bank-rec`, `jonathangintsys/gocardless` (the apps — Charlie's), `jonathangintsys/opera3-write-agent` (Charlie's).

---

## Task 1 — Merge the ready read-path fix branch (small, ~30 min)

**Branch:** `port/opera3-app-read-fixes` (already pushed to `jonathangintsys/aisam`, based on current `main` / v1.17.1).

**Why:** without these, every bank-rec/gocardless *read* screen on an Opera-3 connection 500s. They were proven against the live sidecar during the 12-July end-to-end. Your recent facade work landed one of them (ISNULL recursion); this branch adds the remaining seven that the apps specifically need, cleanly rebased on your current facade (zero conflicts, 53/53 facade tests green).

**The 8 commits (all in `packages/backend/src/opera/`):**
| Commit | What it fixes |
|---|---|
| `91f294d` | `companyFolder=''` when `co_subdir` points at the Data root |
| `722ff7c` | strip MSSQL table hints (`WITH (NOLOCK)`/`UPDLOCK`/…) — bank-rec uses these everywhere |
| `0964e0e` | translate `TOP (n)` → `TOP n` for VFP (Knex `.first()` emits the parenthesised form) |
| `49054b3` | callable facade — Knex query-builder support on opera-3 (not just `.raw()`) |
| `2fc58db` | facade `raw()` is dual-purpose like Knex's (fragment *and* awaitable) |
| `200eb42` | hoist ORDER BY expressions into aliased select columns (VFP refuses TOP without ORDER BY) |
| `f67d5b1` | no-op MSSQL session `SET` statements (VFP 500s on them) |
| `93d4747` | agent client: expose `unmarkReconciled` (`/reconcile/unmark`) — needed for bank-rec reverse-reconcile on opera-3 |

**Action:** review the branch, merge to `main`. It's additive and test-covered. (Any pre-existing tsc noise about `ExternalAccessSurface`/`publicApiPaths` is from your external-access work and exists on clean `main` too — not from this branch.)

---

## Task 2 — Multi-connection support (the real work; the blocker)

**The problem, precisely.** The live Intsys UK SAM (1.17.1, the Docker container) can hold exactly **one** Opera connection today. Adding a second (the Opera-3 connection) fails with `HTTP 422 v1_plugins_block_multi_connection`, and — more fundamentally — even if the gate were removed, the runtime resolves a single connection **globally**:

- `packages/backend/src/plugins/loader.ts:439` and `:590`: `opera_connections.where({ is_active: true }).first()` — **no tenant filter, no ordering**
- `packages/backend/src/plugins/context.ts` (per-request): same single-connection resolution
- `packages/backend/src/routes/admin/connections.ts`: `checkV1PluginGate` blocks a 2nd connection while any loaded plugin is `samContextVersion < 2` (6 of the 11 live apps are: apautomation, eshop, finance-hub, hr-system, opera-copilot, router)

So with two active connections, `.first()` is non-deterministic for **every** app — finance-hub/apautomation (which write to Opera) could bind to the wrong system on a restart. The gate is currently protecting you from exactly that.

**What to build (three parts):**

**2A. Deterministic connection binding (safety-critical, small).**
Give the single-connection resolutions a stable order so legacy (v1) apps always bind to the *primary* connection. Simplest: `orderBy('created_at','asc')` (oldest = the established Opera SE Production connection), or add an explicit `is_primary` flag to `opera_connections`. Apply at `loader.ts:439` & `:590` and the equivalent spots in `context.ts`. After this, a second connection can exist without ever repointing the legacy apps.

**2B. Per-company connection resolution for v2 apps (the actual feature).**
`opera_companies` already carries `connection_id`. For `samContextVersion: 2` plugins, resolve the request's context **from the selected company's connection** (company → connection → type/agent/facade) instead of the global first-active row. This is what lets bank-rec/gocardless serve Opera SE companies via SQL and Opera-3 companies via the sidecar/agent **in the same instance**. (The plugin scheduler is already per-connection, so the request path is the only gap.)

**2C. Relax `checkV1PluginGate`.**
Once 2A is in, allow additional connections while v1 plugins are present (they're pinned to the primary). If you'd rather keep it strict until the 6 legacy apps are on `samContextVersion 2`, that's your call — but 2A+2B is the minimum that unblocks Charlie's apps.

**2D. Deploy.** Rebuild + restart the live SAM Docker container on the Intsys server. (The box is already on 1.17.1, so the deploy path is proven — the last upgrade went cleanly.)

---

## After Tasks 1 & 2: adding the connection is a 5-minute form (Charlie/Harry do this)

Once the platform supports it, the Opera-3 connection goes in via Admin → Connections on the live SAM. Parameters are all known and the .214 firewall is already opened. Nothing further from you for that step.

---

## What is NOT yours (so you know the full shape)

These are Charlie's, in flight, and do **not** block your two tasks:
- **Agent golden-master verification** — native-Opera-3 vs agent postings, per verb, against captured snapshots (the snapshot campaign completed 2026-07-30). Produces **agent 2.6.13**.
- **Two agent hardenings** found in a locking review (2026-07-30): lock-first counter allocation (nparm/atype) and a production guard against the SMB fallback backend.
- **Small gaps:** `cost_centre` on the agent's nominal-entry verb; the sidecar `ae_remove` LOGICAL-coercion duplicate-check fix; gocardless first end-to-end through the agent.
- **Agent version skew to flag for later:** the **.99** production Opera-3 host runs agent **2.6.0**; current is **2.6.12** (repo) heading to 2.6.13. Charlie will deploy the signed-off version to .99 as part of go-live — not now, and writes there stay disabled (`OPERA3_WRITES_ENABLED` unset) until Harry+Jonathan sign off the verification runbook. **.214** (demo) is on 2.6.12 with writes enabled for verification.

## Critical path to "done"

```
Task 1 (merge read fixes)  ─┐
                            ├─► Charlie: agent verification → 2.6.13 → runbook sign-off (Harry + Jonathan)
Task 2 (multi-connection) ─┘        │
                                    ▼
                        add Opera-3 connection on live SAM (5-min form)
                                    ▼
              deploy 2.6.13 to .99 + enable writes → Opera-3 apps live
```

Your two tasks are the only platform blockers. Everything downstream is ours.

— Charlie
