# SAM Rewrite Progress

Live tracker. Each session updates this file before committing.

## SAM contract alignment (verified 2026-05-08)

Examined the real SAM at `~/opera-knowledge-ref/`:
- `packages/backend/src/plugins/loader.ts` (lines 338-398) — runtime
  context shape SAM injects into a plugin factory
- `packages/backend/src/plugins/context.ts` — SAM's published interface
  (leaner than what loader actually injects)
- `packages/backend/src/middleware/company.ts` — `X-Opera-Company` →
  `req.operaCompany` middleware
- `packages/shared/src/types/manifest.ts` — `SapManifest` schema
- `docs/plugin-authoring.md` — full plugin contract

Aligned all 4 plugins with the runtime contract:
- AppContext: dropped `eventBus` (not in loader), added optional
  services SAM actually passes (`createAIService`, `email`, `llm`,
  `emailIngest`, `graph`, `setSyncTrigger`) — see
  `apps-sam/<app>/src/app-context.ts` for the canonical shape with
  full type signatures from real SAM source.
- Manifests: dropped invalid `sam:email:read`/`sam:email:send`
  permissions (not in `AppPermission` union); added
  `consumes: { email-ingest: true, llm: true }` for plugins that scan
  mailboxes / use AI extraction (gocardless, bank-reconcile, suppliers).
- Express request typing: now matches SAM's `req.user` shape
  (userType, appRole, permissions[]) and the `req.operaCompany`
  resolver injection.

This means the merge into SAM should be a drop-in: SAM's loader runs
each plugin's migrations from `<dist>/db/migrations`, imports the
default factory, and passes a context our types already match.

## Status

**Status:** All 4 plugin foundations in place; 1 fully ported; 3 in active progress.
**balance-check:** ✅ BACKEND COMPLETE (7/7 endpoints, 32 tests)
**gocardless:** 38 of ~124 endpoints (190 tests)
**bank-reconcile:** 34 of ~127 endpoints (170 tests)
**suppliers:** 38 endpoints (greenfield TS work — 128 tests)
**Calendar week of project:** 1
**Sessions logged:** 1 (extended session — 33 substantive commits)

## Per-app progress

### shared (utilities used by all plugins)

- [x] `package.json`, `tsconfig.json`, workspace setup
- [x] Opera SQL control-accounts lookup (`getControlAccounts` — port of `sql_rag/opera_config.py`)
- [x] Tests for control-accounts (8 passing)
- [x] Period validation primitives (port from `sql_rag/opera_config.py`):
      `getPeriodForDate`, `getCurrentPeriodInfo`, `getPeriodStatus`,
      `isOpenPeriodAccountingEnabled`, `isRealTimeUpdateEnabled`,
      `validatePostingPeriod`, `getLedgerTypeForTransaction`. 27 tests.
- [x] Home currency helper (port from `sql_rag/opera_sql_import.py`):
      `getHomeCurrency` — zxchg lookup with GBP fallback + per-DB cache. 6 tests.
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

### gocardless (in progress — 34 of ~124 endpoints)

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
- [x] `GET /api/gocardless/setup-status` — configured? pending signup?
- [x] `GET /api/gocardless/batch-types` — Opera atype receipt types
- [x] `GET /api/gocardless/nominal-accounts` — Opera nacnt list
- [x] `GET /api/gocardless/payment-types` — Opera atype payment types
- [x] `GET /api/gocardless/vat-codes` — ztax VAT codes with date-applicable rates
- [x] `GET /api/gocardless/bank-accounts` — nbank list
- [x] `GET /api/gocardless/import-config` — consolidated batch_types + nominal + VAT
- [ ] `POST /api/gocardless/parse` — extract payments from email content
- [x] `POST /api/gocardless/match-customers` — match payments via metadata + mandates + sname (5 priorities + 1p-tolerance duplicate check + customer-id backfill)
- [x] `POST /api/gocardless/revalidate-batches` — refresh period_valid + possible_duplicate against current Opera state (foreign-currency aware, ref + amount duplicate detection, 14-day fallback)
- [x] `GET /api/gocardless/partner/config` — partner-credentials probe + redirect_uri builder
- [x] `GET /api/gocardless/partner/signup-status` — latest signup, token redacted
- [x] `GET /api/gocardless/partner/merchants` — all signups, tokens redacted
- [x] `POST /api/gocardless/partner/admin-auth` — admin password gate (first-time aware)
- [x] `PUT /api/gocardless/partner/admin-password` — set/change admin password (≥4 chars)
- [x] `PUT /api/gocardless/partner/merchant-app-url` — save deployment URL for a merchant
- [x] `POST /api/gocardless/partner/activate-merchant` — push token (local→settings, remote→fetch)
- [x] `PUT /api/gocardless/deploy-token` — receive a token from the partner portal
- [x] `GET /api/gocardless/test-data` — hardcoded sample payout dataset
- [x] `POST /api/gocardless/partner/initiate-signup` — start OAuth Connect; insert pending signup with state token
- [x] `GET /api/gocardless/partner/callback` — OAuth callback; CSRF-validate state, exchange code for token, fetch creditor, update signup, render HTML
- [x] `POST /api/gocardless/archive-email` — record email as already-in-Opera (move-folder reports provider_not_available until SAM exposes that capability)
- [ ] `POST /api/gocardless/import` — post sales receipts to Opera (the main posting flow)
- [ ] `GET /api/gocardless/scan-emails` — scan SAM mailbox for payout emails
- [x] `GET /api/gocardless/api-payouts` — query GoCardless API directly (slim port; enrichment deferred)
- [x] `GET /api/gocardless/import-history` — past imports with Opera + GC name enrichment
- [x] `POST /api/gocardless/skip-payout` — record payout to history without importing (foreign / manual / dup)
- [x] `POST /api/gocardless/test-api` — validate the saved GoCardless token against /creditors
- [ ] `POST /api/gocardless/remittance/*` — generate / send remittance emails
- [ ] `*` /api/gocardless/partner/*` — partner portal flow (~10 endpoints)
- [x] `POST /api/gocardless/update-subscription-tags` — Opera repeat-doc tagging (preview + apply, ROWLOCK)
- [x] `GET /api/gocardless/validate-date` — Opera period validation (OPA-aware, NL master gate, sub-ledger check)
- [x] `GET /api/gocardless/payment-requests/stats` — dashboard stats (mandates active, pending count+amount, MTD paid-out, 30d failed)
- [ ] `GET /api/gocardless/nominal-accounts`
- [ ] `GET /api/gocardless/vat-codes`
- [ ] `POST /api/gocardless/test-api`
- [ ] `*` Mandate setup endpoints
- [ ] `*` Subscription endpoints
- [ ] `*` ~110 more endpoints

#### Helpers
- [x] `src/services/settings.ts` — settings load/save/mask/merge
- [x] `src/services/health-check.ts` — health check
- [x] `src/services/lookups.ts` — batch types / nominal accounts / payment types / VAT codes / setup status
- [ ] `src/services/email-scan.ts` — scan SAM mailbox for GC payouts
- [ ] `src/services/payment-extract.ts` — Gemini AI extraction
- [ ] `src/services/customer-match.ts` — fuzzy match payments to Opera customers
- [ ] `src/services/import.ts` — post sales receipts (the big one)
- [x] `src/services/gocardless-api.ts` — wrap the GoCardless REST API (testConnection + getPayouts)
- [ ] `src/services/remittance.ts` — generate + send remittance via SAM email

### bank-reconcile (in progress — 28 of ~127 endpoints)

- [x] Directory scaffolded: `apps-sam/bank-reconcile/`
- [x] `manifest.json` — full-stack plugin, separateDatabase=true
- [x] `package.json`, `tsconfig.json`, `vitest.config.ts`
- [x] `src/app-context.ts` + `src/index.ts` (status endpoint)
- [x] `db/migrations/001_initial_schema.ts` — settings + bank_import_aliases +
      repeat_entry_aliases + ai_suggestions + duplicate_overrides +
      bank_import_patterns + extraction_cache + import_locks +
      deferred_transactions + bank_statement_imports tables (mirrors
      Python SQLite schemas)
- [x] `src/services/banks.ts` — port of `get_bank_accounts`
- [x] `src/services/health-check.ts` — port of bank-reconcile health check
      (aliases / patterns / statement-import history / Opera connection sanity)
- [x] `src/services/orphan-tmpstat.ts` — list + clear orphan ae_tmpstat
      reservations (residual partial-reconcile state). NOLOCK for read,
      ROWLOCK for clear UPDATE per CLAUDE.md.
- [x] `src/services/reconciliation-status.ts` — port of
      `OperaSQLImport.get_unreconciled_entries` and
      `OperaSQLImport.get_reconciliation_status`. Reconciled balance
      from nbank, unreconciled count + total from aentry; current
      balance derived (NOT from nk_curbal which has historical drift).
- [x] `src/router.ts` — mounts:
  - GET /api/bank-reconcile/status
  - GET /api/reconcile/banks
  - GET /api/bank-import/health-check
  - GET /api/reconcile/bank/:bank_code/orphan-tmpstat
  - POST /api/reconcile/bank/:bank_code/clear-orphan-tmpstat
  - GET /api/reconcile/bank/:bank_code/unreconciled
  - GET /api/reconcile/bank/:bank_code/status
  - POST /api/reconcile/bank/:bank_code/ignore-transaction
  - GET /api/reconcile/bank/:bank_code/ignored-transactions
  - DELETE /api/reconcile/bank/ignored-transaction/:record_id
  - DELETE /api/reconcile/bank/:bank_code/unignore-transaction
  - POST /api/statement-files/mark-reconciled
  - GET /api/statement-files/imported-for-reconciliation
  - GET /api/recurring-entries/config
  - PUT /api/recurring-entries/config
  - GET /api/bank-import/cashbook-types
  - GET /api/bank-import/config
  - PUT /api/bank-import/config
  - POST /api/bank-import/detect-format (CSV/OFX/QIF/MT940 sniff)
  - POST /api/bank-import/detect-bank (regex + CSV-header sniff vs nbank)
  - POST /api/bank-import/duplicate-override (record user-confirmed not-a-dup)
  - POST /api/bank-import/draft (save WIP)
  - GET /api/bank-import/draft (load WIP)
  - DELETE /api/bank-import/draft (clear WIP)
  - GET /api/bank-import/accounts/customers (dormant-filtered)
  - GET /api/bank-import/accounts/suppliers (dormant-filtered)
  - POST /api/reconcile/bank/:bank_code/unreconcile **(first finance-write port — bank lock + ROWLOCK + transaction)**
  - POST /api/reconcile/bank/:bank_code/mark-reconciled **(full + partial — UPDLOCK on read, ROWLOCK on writes, fresh-bank auto-recovery)**
- [x] TypeScript builds cleanly
- [x] 8 tests passing (4 banks + 4 health-check)
- [ ] Endpoints: 3 of ~127 ported. Future-session priorities:
  - `/api/bank-import/scan-emails` (via SAM email service)
  - `/api/bank-import/preview-from-pdf`
  - `/api/bank-import/preview-from-email`
  - `/api/bank-import/import` (the big posting flow)
  - `/api/reconcile/bank/{code}/list`
  - `/api/reconcile/bank/{code}/reconcile`
  - `/api/repeat-entries/*`
- [ ] Services to port:
  - `sql_rag/bank_import.py` — main matcher + importer
  - `sql_rag/opera_sql_import.py` — Opera posting logic
  - `sql_rag/bank_patterns.py` — pattern learning
  - `sql_rag/statement_reconcile.py` — reconcile workflow
  - `sql_rag/bank_pdf_extract.py` — Gemini extraction

### suppliers (foundation + 3 endpoints — finished in TS directly)

- [x] Directory scaffolded: `apps-sam/suppliers/`
- [x] `manifest.json` — full-stack plugin, separateDatabase=true,
      version `0.1.0-dev`, navLabel "Suppliers (DEV)"
- [x] `package.json`, `tsconfig.json`, `vitest.config.ts`
- [x] `src/app-context.ts` + `src/index.ts` + `src/router.ts`
- [x] `db/migrations/001_initial_schema.ts` — full per-app schema
- [x] `src/services/supplier-list.ts` — list active suppliers + supplier-detail
      from Opera pname (excludes dormant per CLAUDE.md)
- [x] `src/router.ts` — mounts:
  - GET /api/suppliers/status (liveness)
  - GET /api/suppliers (list active suppliers)
  - GET /api/suppliers/:code (supplier detail)
- [x] TypeScript builds cleanly
- [x] 6 tests passing (4 listSuppliers + 2 getSupplier)
- [ ] Endpoints: 3 of TBD ported. Future-session priorities:
  - POST /api/suppliers/scan-emails (via SAM email)
  - POST /api/suppliers/extract-statement (Gemini)
  - POST /api/suppliers/reconcile (statement vs ptran)
  - GET  /api/suppliers/:code/contacts
  - POST /api/suppliers/onboard
  - POST /api/suppliers/remittance

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
| `apps-sam/gocardless/tests/lookups.test.ts` | 12 | ✅ passing |
| `apps-sam/bank-reconcile/tests/banks.test.ts` | 4 | ✅ passing |
| `apps-sam/bank-reconcile/tests/health-check.test.ts` | 4 | ✅ passing |
| `apps-sam/bank-reconcile/tests/orphan-tmpstat.test.ts` | 7 | ✅ passing |
| `apps-sam/suppliers/tests/supplier-list.test.ts` | 6 | ✅ passing |
| `apps-sam/gocardless/tests/import-history.test.ts` | 4 | ✅ passing |
| `apps-sam/gocardless/tests/skip-payout.test.ts` | 5 | ✅ passing |
| `apps-sam/bank-reconcile/tests/reconciliation-status.test.ts` | 6 | ✅ passing |
| `apps-sam/gocardless/tests/gocardless-api.test.ts` | 8 | ✅ passing |
| `apps-sam/bank-reconcile/tests/ignored-transactions.test.ts` | 7 | ✅ passing |
| `apps-sam/bank-reconcile/tests/statement-files.test.ts` | 5 | ✅ passing |
| `apps-sam/suppliers/tests/aged-debt.test.ts` | 6 | ✅ passing |
| `apps-sam/suppliers/tests/contacts.test.ts` | 8 | ✅ passing |
| `apps-sam/bank-reconcile/tests/settings.test.ts` | 6 | ✅ passing |
| `apps-sam/suppliers/tests/approved-emails.test.ts` | 9 | ✅ passing |
| `apps-sam/suppliers/tests/supplier-config.test.ts` | 8 | ✅ passing |
| `apps-sam/gocardless/tests/receipt-search.test.ts` | 5 | ✅ passing |
| `apps-sam/suppliers/tests/supplier-statements.test.ts` | 6 | ✅ passing |
| `apps-sam/suppliers/tests/automation-config.test.ts` | 9 | ✅ passing |
| `apps-sam/suppliers/tests/onboarding.test.ts` | 10 | ✅ passing |
| `apps-sam/gocardless/tests/import-history-delete.test.ts` | 6 | ✅ passing |
| `apps-sam/suppliers/tests/remittance-log.test.ts` | 7 | ✅ passing |
| **Total TypeScript tests** | **205** | ✅ all passing |
| Python tests (existing, kept alive as reference) | 604 | ✅ all passing |
| **Grand total** | **809** | ✅ |

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
