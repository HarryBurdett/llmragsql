# SAM Team Handoff — apps-sam replication status

This document is the canonical handoff for the SAM team picking up the
ported plugins. Each app under `apps-sam/` has been replicated as a
SAM-compatible TypeScript plugin (backend + frontend bundle) following
the contract in `~/opera-knowledge-ref/packages/backend/src/plugins/`
and `~/opera-knowledge-ref/packages/frontend/src/plugins/`.

The replication is **engine-agnostic single-source TS** — the same
plugin serves both Opera SE (SQL Server) and Opera 3 (FoxPro) via SAM's
unified Knex client. Engine routing happens in SAM under the plugin;
the plugin code never branches on `ctx.operaType`.

## What's done

| Plugin | Backend tests | Backend endpoints | Frontend bundle |
|---|---|---|---|
| `gocardless` | 33 files / 490 tests | ~46 endpoints ported | UMD `__SAM_APPS__["gocardless"]` |
| `bank-reconcile` | 34 files / 315 tests | ~46 endpoints ported | UMD `__SAM_APPS__["bank-reconcile"]` |
| `suppliers` | 21 files / 184 tests | ~58 endpoints ported | UMD `__SAM_APPS__["suppliers"]` |
| `balance-check` | TBC tests | 7 endpoints ported | UMD `__SAM_APPS__["balance-check"]` |

All four plugins type-check clean (`npx tsc --noEmit`) and all backend
tests pass (`npx vitest run`). Frontend bundles build with
`vite build` (esbuild minify) at ~17–19 kB each.

## Posting / extraction adapters — SAM team to wire

The replication intentionally stops at the SQL-write boundary for the
two large posting flows. Each is exposed as a typed adapter on
`AppContext`. The plugin returns **HTTP 503** until the adapter is
wired.

### gocardless/import (POST /api/gocardless/import)
- `BatchPostingExecutor.postBatch(operaDb, ValidatedRequest)` — ~750
  LOC of aentry / atran / stran / ntran / anoml + balance updates
- `ImportLockAdapter` — bank-level lock (Python uses
  `sql_rag/import_lock.py`)

Source of truth for the posting body:
`sql_rag/opera_sql_import.py:6017-7017` —
`OperaSQLImport.import_gocardless_batch`.

The validation layer (idempotency, mandate→customer match, fees gate,
period gate, foreign currency, bank existence) is **fully ported** in
`apps-sam/gocardless/src/services/import-batch.ts`. Tests cover all
guards with mocks (`tests/import-batch.test.ts`).

### bank-reconcile/import-from-pdf (POST /api/bank-import/import-from-pdf)
- `PdfExtractor.extractFromPdf(...)` — ctx.llm Claude vision call
- `ImportPostingExecutor.postBankImport(...)` — ~750 LOC posting body
- `ImportLockAdapter`
- `PeriodOverlapChecker` — equivalent of
  `apps/bank_reconcile/logic/import_orchestration.check_statement_period_overlap`

Source of truth for the posting body:
`sql_rag/bank_import.py` (BankStatementImport class) and
`apps/bank_reconcile/api/routes.py:4031-4787`.

The orchestration shell (validation, overlap gate, lock, audit) is
fully ported in
`apps-sam/bank-reconcile/src/services/import-from-pdf.ts`.

## Email-ingest adapters — SAM team to wire

Three plugins consume `ctx.emailIngest`. Because SAM's
`SamEmailIngestService` is a streaming handler API rather than a
list/query API, each scan-emails endpoint takes a small adapter that
the SAM team builds against whatever cache strategy SAM settles on
(plugin-local cache table, host-side delta poll, etc.).

### gocardless/scan-emails (GET /api/gocardless/scan-emails)
Adapter: `EmailMailboxAdapter` with `sync()` + `list()`. Returns 503
until wired. Service: `apps-sam/gocardless/src/services/scan-emails.ts`.

### bank-import/scan-emails (GET /api/bank-import/scan-emails)
Two adapters: `BankMailboxAdapter` (list emails + get-by-id) and
`ReconciledKeyStore` (already-reconciled keys + filenames). Returns 503
until wired. Service:
`apps-sam/bank-reconcile/src/services/scan-emails.ts`.

### Suppliers
No mailbox-scan endpoint in Python — supplier statements arrive via
per-email handlers. The SAM team will likely add one as a SAM-managed
delta-poll outside the plugin.

## Frontend handoff

Each plugin ships a Vite-built UMD bundle at `frontend/dist/index.js`
that registers its entry component on `window.__SAM_APPS__`. The
component receives a `context: SamPluginContext` prop matching SAM's
`AppShell.tsx` contract (appId, user, token, currentCompany,
api.fetch).

**Scope of the scaffold:** each entry component renders a working tab
shell that exercises the new TS endpoints — enough to demo the apps
end-to-end after the SAM team wires the executors. The full UI port
lives in the legacy React pages under `frontend/src/pages/`:

| Plugin | Legacy reference page(s) | LOC |
|---|---|---|
| gocardless | `GoCardlessImport.tsx` | 2,501 |
| bank-reconcile | `BankStatementReconcile.tsx` | 5,436 |
| suppliers | `SupplierDashboard.tsx` + `SupplierQueries.tsx` + `SupplierStatementHistory.tsx` + `SupplierAccount.tsx` + `SupplierSettings.tsx` + `SupplierReconciliations.tsx` | ~3,000 |
| balance-check | `Reconcile.tsx` + `CashbookOptions.tsx` + `CreditorsReconcile.tsx` + `VATReconcile.tsx` | ~2,500 |

The scaffold component for each plugin lists the endpoints it already
calls and points at the legacy React file the SAM team translates.

## Building locally

```bash
# Backend type-check + tests for one plugin
cd apps-sam/gocardless
npx tsc --noEmit
npx vitest run

# Frontend bundle for one plugin
cd apps-sam/gocardless/frontend
npm install
npx tsc --noEmit
npx vite build       # → frontend/dist/index.js
```

## Opera 3 validation

Once the adapters are wired and the apps are deployable:

1. Run the snapshot tool against an Opera 3 install before posting a
   transaction (`scripts/snapshot_opera3.py before --data-path
   /path/to/install/DATA`).
2. Post the transaction through the new SAM plugin.
3. Run snapshot again (`scripts/snapshot_opera3.py after`).
4. Diff against the Opera SE capture in
   `~/opera-knowledge-ref/packages/opera-knowledge/transaction-library/`
   — same tables, same fields touched? Anything that diverges is a
   version-specific code path the Write Agent needs to handle.

The transaction checklist in
`~/opera-knowledge-ref/packages/opera-knowledge/transaction-library/INDEX.md`
is the definitive list of what needs Opera 3 validation.

## New since first handoff (this session)

- **Suppliers state-transition workflow** — process / acknowledge /
  approve / edit-response / bulk-approve, with the never_communicate
  policy gate enforced on every outbound email path. Adapters:
  `EmailSender`, `OperaSupplierLookup`, `PtranLookup`. Migrations
  003 + 004 add the columns the workflow needs.
- **Suppliers security** — list-alerts / verify / audit / scan-changes
  with bank-detail change detection and email alerts to
  `security_alert_recipients`. Adapters: `OperaPnameProvider`,
  `SecurityEmailSender`. First-time observations auto-verify as
  `scan_baseline` rows.
- **gocardless/import-from-email** — thin wrapper around the
  validated batch import that adds email-archive on success via the
  optional `EmailArchiveAdapter`. Best-effort archive does not roll
  back the import.
- **bank-import/check-duplicates** — all six EnhancedDuplicateDetector
  strategies ported (fingerprint, fit_id, exact, fuzzy_amount,
  reference, cross_period, bank_amount). Sign-aware: a +£X receipt
  and a -£X payment are not duplicates of each other; opposite-sign
  aentry rows are not flagged in bank_amount fallback.
- **reconcile/refresh-matches** — re-runs duplicate detection
  against Opera and updates is_duplicate / skip_reason / action
  flags without a full re-extract. Threshold-based (≥0.85 → posted).
- **bank-import/suggest-account** — three-tier customer/supplier
  picker for unmatched statement lines (substring → word-match →
  fuzzy). Promoted `sequenceMatcherRatio` to `@sqlrag/sam-shared`
  so gocardless suggest-match and bank-reconcile suggest-account
  share one CPython-faithful implementation.

## Known follow-ups

- `ctx.llm` integration for PDF extraction (bank-reconcile preview /
  import-from-pdf, suppliers extract-from-email).
- Frontend full-UI port of the four legacy React pages.
- Email ingestion glue (per-plugin or SAM-host cache).
- Write Agent — Opera 3 FoxPro write service, in development.
- Remaining Python endpoints in bank-reconcile: the big
  `/api/reconcile/bank/{bank_code}` dashboard endpoint and the
  `/api/archive/*` filesystem-bound endpoints (these need a
  storage adapter design decision from the SAM team).

## Contact

- Harry Burdett (harryb@intsysuk.com, mobile 07764 190507) for
  questions / async pings during deployment.
- Charlie Burdett (charlieb@intsysuk.com) for stakeholder demos.
