# Opera 3 for bank-reconcile & gocardless — what Jonathan needs to do to complete it

**Date:** 2026-07-30 · **From:** Harry / Charlie · **For:** Jonathan
**One-line (updated 2026-07-30):** Charlie has now done most of the platform work too. Your remaining tasks: **(1) merge the ready Opera-3 read-fix branch, (2) review the multi-connection branch — 2A+2C are DONE, implement the designed 2B, then deploy.** The agent is ours and its reconcile verb is now golden-verified live on .214. Branches waiting on `jonathangintsys/aisam`: `port/opera3-app-read-fixes` (Task 1) and `feat/opera-multi-connection` (Task 2).

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

**UPDATE 2026-07-30 — Charlie has already implemented 2A + 2C and designed 2B.** All on branch **`feat/opera-multi-connection`** (pushed to `jonathangintsys/aisam`, based on current main). Your job on this task is now **review + implement 2B + deploy**, not build from scratch.

**2A. Deterministic connection binding — DONE (commit `2a26f9c`).**
The three connection lookups (`loader.ts` ~439/~590, `context.ts`) now `.orderBy('created_at','asc').first()`, so legacy (v1) plugins always bind to the OLDEST = the established Opera SE Production connection. With one connection (every tenant today) this changes nothing; it only matters once a second exists. ~15 lines; 109/109 plugin tests pass.

**2C. Relax `checkV1PluginGate` — DONE (commit `2a26f9c`).**
The hard 422 block is now a **non-blocking advisory**: a second connection is allowed (safe because 2A pins legacy apps to the primary, and a new connection is always newer), and the create response carries an `advisory` naming the plugins pinned to primary. v1Gate tests updated to the new contract (11/11 pass). Review and confirm you're comfortable dropping the hard block.

**2B. Per-company connection resolution — DESIGNED, needs your implementation (commit `1ea7f61`).**
Full design in **`docs/opera3-multi-connection-2B-design.md`** on the branch. This is the actual feature (v2 apps serving SE companies via SQL AND Opera-3 companies via the sidecar/agent in one instance). Charlie did NOT implement it because it changes `AppContext` — the contract all 11 apps consume — so it wants your eyes on the contract + the per-company pool/agent lifecycle before it lands. The doc has the contract change, an implementation sketch (SAM side), migration notes (no schema change — `opera_companies.connection_id` already exists), and a test plan. Additive and back-compatible: apps adopt the per-company resolvers in their next release; until then they run against the primary exactly as today.

**2D. Deploy.** Merge the branch (after your 2B implementation) and rebuild + restart the live SAM Docker container on the Intsys server. (The box is already on 1.17.1, so the deploy path is proven — the last upgrade went cleanly.) This deploy is yours — Charlie can't reach that host.

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
