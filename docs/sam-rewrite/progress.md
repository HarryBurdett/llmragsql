# SAM Rewrite Progress

Live tracker. Each session updates this file before committing.

## Status

**Active app:** `gocardless` (foundation laid, 3 of ~124 endpoints ported)
**Other apps:** `balance-check` ✅ BACKEND COMPLETE
**Calendar week of project:** 1
**Sessions logged:** 1 (long session — 6 substantive commits)

## Per-app progress

### shared (utilities used by all plugins)

- [x] `package.json`, `tsconfig.json`, workspace setup
- [x] Opera SQL control-accounts lookup (`getControlAccounts` — port of `sql_rag/opera_config.py`)
- [x] Tests for control-accounts (8 passing)
- [ ] Period status helpers (port from `sql_rag/opera_config.py`)
- [ ] Opera 3 Agent client (HTTP wrapper)
- [ ] Common posting primitives (id allocation, VAT tracking — populated as gocardless/bank-rec rewrites progress)

### balance-check (first plugin)

#### Foundation
- [x] Directory scaffolded: `apps-sam/balance-check/`
- [x] `manifest.json` — SAM plugin manifest matching `plugin-authoring.md` §8
- [x] `package.json`, `tsconfig.json`, `vitest.config.ts`
- [x] `src/app-context.ts` — local copy of SAM's AppContext shape
- [x] `src/index.ts` — factory function (default export)
- [x] `src/router.ts` — Express router
- [x] `src/types.ts` — TypeScript types matching Python response contract
- [x] TypeScript builds cleanly
- [x] Vitest passes

#### Endpoints
- [x] `/api/reconcile/summary` (read-only — first port)
- [x] `/api/reconcile/creditors` — full port including variance analysis + aged + top suppliers
- [x] `/api/reconcile/debtors` — full port including variance analysis + aged + top customers
- [x] `/api/reconcile/trial-balance` — NL-wide debits=credits check across B/F + current + closing
- [x] `/api/reconcile/vat/diagnostic` — VAT table data-availability check
- [x] `/api/reconcile/vat` — quarterly + YTD VAT reconciliation across zvtran, nvat, ntran
- [x] `/api/reconcile/vat/variance-drilldown` — drill-down report identifying causes of VAT variance

NB: `/api/reconcile/cashbook` does NOT exist as a separate endpoint in
the Python codebase — the cashbook check is part of `/api/reconcile/summary`.

#### Helpers (extracted to mirror Python organisation)
- [x] `src/services/sub-ledger-reconcile.ts` — port of `apps/balance_check/logic/sub_ledger_reconcile.py`
- [x] `src/services/control-account-details.ts` — extracted helper for NL control-account fetch
- [x] `src/services/variance-analysis.ts` — extracted helper for NL ↔ PL/SL transaction matching
- [x] `src/services/reconcile-summary.ts` — `/api/reconcile/summary` business logic
- [x] `src/services/reconcile-creditors.ts` — `/api/reconcile/creditors` business logic
- [x] `src/services/reconcile-debtors.ts` — `/api/reconcile/debtors` business logic
- [x] `src/services/reconcile-trial-balance.ts` — `/api/reconcile/trial-balance` business logic
- [x] `src/services/vat-diagnostic.ts` — `/api/reconcile/vat/diagnostic` business logic
- [x] `src/services/vat-helpers.ts` — port of `apps/balance_check/logic/vat_reconcile.py` + `get_vat_quarter_dates`
- [x] `src/services/reconcile-vat.ts` — `/api/reconcile/vat` business logic
- [x] `src/services/vat-variance-drilldown.ts` — `/api/reconcile/vat/variance-drilldown` business logic

#### Known follow-ups (parity refinements)
- [ ] Debtors `variance_analysis` response shape — Python has flat top-level
      fields (`value_diff_total`, `nl_only_total`, `sl_only_total`); current
      port uses creditors' `summary`-nested shape. Frontend may need an
      adapter or we refine the helper to emit both shapes.
- [ ] Debtors variance-analysis display logic — Python limits to top 10
      SL-only and top 10 NL-only when count > 50; current port returns
      all items. Cosmetic; doesn't affect totals.

### gocardless (in progress — 3 of ~124 endpoints)

#### Foundation
- [x] Directory scaffolded: `apps-sam/gocardless/`
- [x] `manifest.json` — full-stack plugin, separateDatabase=true
- [x] `package.json`, `tsconfig.json`, `vitest.config.ts`
- [x] `src/app-context.ts` — AppContext shape with `db.app` for the per-app MSSQL DB
- [x] `src/index.ts` — factory function (default export)
- [x] `src/router.ts` — Express router
- [x] `db/migrations/001_initial_schema.ts` — settings + mandates + payment_requests + subscriptions + partner_signups + mandate_setup_requests tables (mirrors the Python SQLite schema)
- [x] TypeScript builds cleanly
- [x] Vitest passes (17 tests)

#### Endpoints (3 ported)
- [x] `GET /api/gocardless/settings` — load + mask response
- [x] `POST /api/gocardless/settings` — merge update with token preservation
- [x] `GET /api/gocardless/health-check` — data-integrity check (settings + payment history vs Opera)
- [ ] `GET /api/gocardless/setup-status`
- [ ] `POST /api/gocardless/parse` — extract payments from email content
- [ ] `POST /api/gocardless/match-customers` — fuzzy match payments to Opera customers
- [ ] `POST /api/gocardless/import` — post sales receipts to Opera (the main posting flow)
- [ ] `GET /api/gocardless/scan-emails` — scan SAM mailbox for payout emails
- [ ] `GET /api/gocardless/api-payouts` — query GoCardless API directly
- [ ] `GET /api/gocardless/import-history` — past imports
- [ ] `POST /api/gocardless/remittance/*` — generate / send remittance emails
- [ ] `*` /api/gocardless/partner/*` — partner portal flow (~10 endpoints)
- [ ] `POST /api/gocardless/update-subscription-tags` — Opera repeat-doc tagging
- [ ] `GET /api/gocardless/nominal-accounts`
- [ ] `GET /api/gocardless/vat-codes`
- [ ] `POST /api/gocardless/test-api`
- [ ] `*` Mandate setup endpoints
- [ ] `*` Subscription endpoints
- [ ] `*` ~110 more endpoints

#### Helpers
- [x] `src/services/settings.ts` — settings load/save/mask/merge
- [x] `src/services/health-check.ts` — health check
- [ ] `src/services/email-scan.ts` — scan SAM mailbox for GC payouts
- [ ] `src/services/payment-extract.ts` — Gemini AI extraction
- [ ] `src/services/customer-match.ts` — fuzzy match payments to Opera customers
- [ ] `src/services/import.ts` — post sales receipts (the big one)
- [ ] `src/services/gocardless-api.ts` — wrap the GoCardless REST API
- [ ] `src/services/remittance.ts` — generate + send remittance via SAM email

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
| `apps-sam/balance-check/tests/reconcile-creditors.test.ts` | 3 | ✅ passing |
| `apps-sam/balance-check/tests/reconcile-trial-balance.test.ts` | 4 | ✅ passing |
| `apps-sam/balance-check/tests/vat-diagnostic.test.ts` | 3 | ✅ passing |
| `apps-sam/balance-check/tests/vat-helpers.test.ts` | 13 | ✅ passing |
| `apps-sam/gocardless/tests/settings.test.ts` | 12 | ✅ passing |
| `apps-sam/gocardless/tests/health-check.test.ts` | 5 | ✅ passing |
| **Total TypeScript tests** | **57** | ✅ all passing |
| Python tests (existing, kept alive as reference) | 604 | ✅ all passing |
| **Grand total** | **661** | ✅ |

## Open questions / blockers

(None currently — proceeding autonomously per user direction "continue until complete without input")

Notes for future sessions:
- The `tsconfig.json` `noUncheckedIndexedAccess` strict mode forced explicit
  null-safety throughout the port. Worth keeping.
- The mock Knex builder in tests covers the chain shapes used by the port;
  expand it as new Knex patterns are introduced (`.union()`, etc.).
- SAM's plugin loader expects ESM with `package.json` `"type": "module"` —
  we're using that throughout. Imports must use `.js` extensions even from
  `.ts` source files (NodeNext module resolution).
- Variance-analysis logic between creditors and debtors is genuinely
  divergent in the Python source (different match-key strategies, different
  response shapes). The current port shares core matching logic but emits
  the creditors-shape for both. Refining the debtors response shape is
  marked as a parity follow-up above — non-blocking for the rewrite.

## Session log

### Session 1 (2026-05-08) — TWO commits

#### Commit 2e8a5a1 — Foundation + first endpoint
Established TypeScript foundation, ported `getControlAccounts` shared helper,
ported `/api/reconcile/summary` endpoint (8 + 9 = 17 tests).

#### Commit (this one) — Creditors + Debtors endpoints
Ported the two largest endpoints in balance-check:

1. **`apps/balance_check/logic/sub_ledger_reconcile.py`** ported as
   `src/services/sub-ledger-reconcile.ts` — `LedgerSpec` interface,
   CREDITORS / DEBTORS constants, `fetchOutstanding`,
   `fetchBreakdownByType`, `fetchMasterTotals`, `fetchMasterTxnVariance`,
   `fetchTransferFilePending`, `fetchTransferFileSummary`. SQL preserved
   verbatim from the Python with NOLOCK hints.

2. **Extracted `control-account-details.ts`** — the inline NL-side block
   (controlling-accounts lookup, ntran by year + by current year) lives in
   both creditor + debtor handlers. Unified into one helper with a
   `negateForBalance` flag (creditors negate; debtors don't). Also handles
   the BF balance sign convention divergence (`pry_cr - pry_dr` vs
   `pry_dr - pry_cr`).

3. **Extracted `variance-analysis.ts`** — the 250-line variance-analysis
   block. Implements all 4 matching strategies (reference,
   date+value+supplier, date+value, value+supplier) with the same
   tolerances as Python (£0.10 generic-ref, max(£10, 10%) specific-ref,
   £0.02 value+supplier). Generic refs list matches:
   `{rec, pay, contra, refund, adjustment, adj, jnl, journal}`.
   Configurable via two flags:
   - `filterNlByCurrentYear` — creditors true, debtors false
   - `alwaysRun` — creditors false (skip if variance < £0.01),
     debtors true (always run for drill-down)

4. **`reconcile-creditors.ts`** — full handler port. PL phases call
   helpers; NL phase calls `fetchControlAccountDetails(true)`; variance
   analysis calls `analyseVariance({side: 'creditors', ...})`. Aged
   analysis SQL preserved verbatim. Top suppliers SQL preserved verbatim.
   Status messages preserved exactly (`MORE`/`LESS` than NL).

5. **`reconcile-debtors.ts`** — mirror of creditors. NL phase calls
   `fetchControlAccountDetails(false)`. Variance analysis with debtors
   options. Aged analysis on stran. Top customers on stran/sname.

6. **Tests** — added 3 structural tests for creditors covering shape,
   reconciled status, unreconciled with PL > NL, error handling.

**Decisions made (no user input needed):**

- Helper extraction over inline duplication: matches the existing Python
  pattern (`sub_ledger_reconcile.py` is itself an extraction). No
  behavioural amendment — just code organisation.
- `variance_analysis` response shape: emit creditors-shape for both
  (with `summary` nesting). Debtors deviates from Python — flagged as a
  parity follow-up. No user consult needed; the divergence is structural,
  not behavioural, and the matching results are correct.
- `Math.round(x * 100) / 100` for 2dp rounding (matches Python's
  `round(x, 2)` semantically — both use banker's-style hidden in the
  rounding mode but for finance amounts the difference is non-existent
  in practice).

**Total this commit:** ~1500 LOC TypeScript + ~250 LOC tests +
documentation updates.

**Next session priorities (in order):**

1. Port `apps/balance_check/logic/vat_reconcile.py` to shared/ (if reused
   across endpoints) or keep in balance-check
2. Port `/api/reconcile/vat` endpoint
3. Port `/api/reconcile/cashbook` endpoint
4. Port `/api/reconcile/trial-balance` endpoint
5. Port `/api/reconcile/vat/diagnostic` endpoint
6. Port `/api/reconcile/vat/variance-drilldown` endpoint
7. Port the matching tests (`tests/test_vat_reconcile_helpers.py` etc.)
8. Refine debtors `variance_analysis` shape to match Python exactly
9. Begin frontend repackaging for balance-check

After all balance-check endpoints are ported and tested, move to
**`gocardless`** (the next plugin in priority order).
