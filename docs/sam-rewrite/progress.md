# SAM Rewrite Progress

Live tracker. Each session updates this file before committing.

## Status

**Active app:** `balance-check` (foundation phase complete; remaining endpoints to port)
**Calendar week of project:** 1
**Sessions logged:** 1

## Per-app progress

### shared (utilities used by all plugins)

- [x] `package.json`, `tsconfig.json`, workspace setup
- [x] Opera SQL control-accounts lookup (`getControlAccounts` — port of `sql_rag/opera_config.py`)
- [x] Tests for control-accounts (8 passing)
- [ ] Period status helpers (port from `sql_rag/opera_config.py`)
- [ ] Opera 3 Agent client (HTTP wrapper)
- [ ] Common posting primitives (id allocation, VAT tracking — populated as gocardless/bank-rec rewrites progress)

### balance-check (first plugin)

- [x] Directory scaffolded: `apps-sam/balance-check/`
- [x] `manifest.json` — SAM plugin manifest matching `plugin-authoring.md` §8
- [x] `package.json`, `tsconfig.json`, `vitest.config.ts`
- [x] `src/app-context.ts` — local copy of SAM's AppContext shape
- [x] `src/index.ts` — factory function (default export)
- [x] `src/router.ts` — Express router with first endpoint mounted
- [x] `src/types.ts` — TypeScript types matching Python response contract
- [x] `src/services/reconcile-summary.ts` — port of `reconcile_summary()` (~250 lines Python → ~330 lines TS)
- [x] Endpoint port: `/api/reconcile/summary`
- [x] Tests for reconcile-summary (9 passing)
- [x] TypeScript builds cleanly
- [x] Vitest passes (9/9)
- [ ] Endpoint port: `/api/reconcile/creditors`
- [ ] Endpoint port: `/api/reconcile/debtors`
- [ ] Endpoint port: `/api/reconcile/vat`
- [ ] Endpoint port: `/api/reconcile/cashbook`
- [ ] Endpoint port: `/api/reconcile/trial-balance`
- [ ] Endpoint port: `/api/reconcile/vat/diagnostic`
- [ ] Endpoint port: `/api/reconcile/vat/variance-drilldown`
- [ ] Port `apps/balance_check/logic/sub_ledger_reconcile.py` (helpers used by creditors/debtors)
- [ ] Port `apps/balance_check/logic/vat_reconcile.py` (VAT helpers)
- [ ] Frontend plugin packaging
- [ ] Parity validated against Python version (joint test run)

### gocardless

- [ ] (queued — starts after balance-check)

### bank-reconcile

- [ ] (queued — starts after gocardless)

### suppliers

- [ ] (queued — finished in TypeScript directly)

### frontend plugins

- [ ] balance-check frontend
- [ ] gocardless frontend
- [ ] bank-reconcile frontend
- [ ] suppliers frontend

## Test counts

| Suite | Tests | Status |
|---|---|---|
| `apps-sam/shared/tests/control-accounts.test.ts` | 8 | ✅ passing |
| `apps-sam/balance-check/tests/reconcile-summary.test.ts` | 9 | ✅ passing |
| **Total TypeScript tests** | **17** | ✅ all passing |
| Python tests (existing, kept alive as reference) | 604 | ✅ all passing |

## Open questions / blockers

(None currently — proceeding autonomously)

Notes for future sessions:
- The `tsconfig.json` `noUncheckedIndexedAccess` strict mode forced explicit
  null-safety throughout the port. Worth keeping.
- The mock Knex builder in tests covers the chain shapes used by the port;
  expand it as new Knex patterns are introduced (`.union()`, `.with()`, etc.).
- SAM's plugin loader expects ESM with `package.json` `"type": "module"` —
  we're using that throughout. Imports must use `.js` extensions even from
  `.ts` source files (NodeNext module resolution).

## Session log

### Session 1 (2026-05-08)

**Goal:** Establish TypeScript foundation + ship first SAM plugin (`balance-check`)
with one endpoint ported and passing tests.

**Achieved:**

1. Investigated SAM's actual architecture by reading `~/opera-knowledge-ref/`
   - Confirmed: SAM is a TypeScript/Node.js plugin host (not Docker-friendly)
   - Confirmed: plugins are loaded in-process via `import()`, must export factory function
   - Confirmed: SAM provides email, auth, secrets via the `AppContext` parameter
2. Decided on full TypeScript rewrite (vs sidecar) — production targets 10s of sites
3. Set up `apps-sam/` workspace:
   - npm workspace at root
   - `shared/` package for cross-plugin utilities
   - `balance-check/` package as the first plugin
4. Wrote rewrite plan: `docs/sam-rewrite/README.md`
5. Wrote progress tracker: `docs/sam-rewrite/progress.md` (this file)
6. Ported `getControlAccounts()` from `sql_rag/opera_config.py`
   - Faithful translation: same fallback chain (sprfls → pprfls → nparm), same error messages, same caching semantics
   - 8 tests covering happy path, fallbacks, errors, caching
7. Built balance-check plugin scaffolding:
   - SAM plugin manifest (full-stack, opera-se database, no separate DB)
   - TypeScript ESM build with `noUncheckedIndexedAccess` strict mode
   - Vitest test setup
8. Ported first endpoint `/api/reconcile/summary`:
   - 4 sub-checks (debtors, creditors, cashbook, VAT) — all faithful
   - Each check independently runs and reports — failure in one doesn't break others
   - Variance comparisons exact to the penny (matches Python: no tolerance)
   - 9 tests covering all check shapes, error handling, structure
9. Both packages build clean (zero TypeScript errors)
10. All 17 TypeScript tests pass

**Decisions made (no user input needed — sensible defaults):**

- Used Knex for Opera SQL access (matches SAM's existing convention — see `~/opera-knowledge-ref/packages/backend/src/opera/`)
- Kept Python implementation alive at `apps/` as the reference; will retire per-app once parity is confirmed
- Used `WeakMap` for the control-accounts cache (keyed to Knex pool — auto-clears when the pool is GC'd)
- Used parameterised SQL via Knex builder rather than string interpolation (Python uses interpolation because pandas/pyodbc make parameters awkward; the SQL produced is identical)

**Next session priorities (in order):**

1. Port `apps/balance_check/logic/sub_ledger_reconcile.py` helpers into `shared/` (used by both creditors and debtors endpoints)
2. Port `/api/reconcile/creditors` endpoint
3. Port `/api/reconcile/debtors` endpoint
4. Port the corresponding tests from `tests/test_sub_ledger_reconcile_helpers.py`

**Lines committed:** ~1500 lines TypeScript across `apps-sam/` + ~600 lines docs.
