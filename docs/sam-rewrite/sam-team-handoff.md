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
| `gocardless` | 34 files / 495 tests | **100% parity** (74 endpoints) | UMD `__SAM_APPS__["gocardless"]` |
| `bank-reconcile` | 40 files / 351 tests | **100% parity** (86 endpoints) | UMD `__SAM_APPS__["bank-reconcile"]` |
| `suppliers` | 21 files / 184 tests | **100% parity** (94 endpoints) | UMD `__SAM_APPS__["suppliers"]` |
| `balance-check` | 32 tests | **100% parity** (7 endpoints) | UMD `__SAM_APPS__["balance-check"]` |

**Total: 1,062 tests passing across the four plugins. All four
backends type-check clean. Endpoint diff vs Python: 0 missing in
every plugin.**

All four plugins type-check clean (`npx tsc --noEmit`) and all backend
tests pass (`npx vitest run`). Frontend bundles build with
`vite build` (esbuild minify) at ~17–19 kB each.

## Posting / extraction adapters

Both posting flows now ship with **default executors** that handle
the dominant happy path. The SAM team can override any adapter via
`AppContext` for production deployments.

### gocardless/import (POST /api/gocardless/import)
- **Default**: `gocardlessBatchPostingExecutor` — ports the full
  inner posting body of `OperaSQLImport.import_gocardless_batch`:
  - Receipts: aentry header + atran/stran per payment +
    ntran/anoml/njmemo pairs (when completeBatch) + sname/nbank/
    nacnt updates.
  - **Fees split**: SEPARATE cashbook entry, DR fees expense + DR
    VAT input + CR bank, with split atran lines (net + VAT) when
    VAT > 0. VAT nominal looked up from ztax via
    `fetchVatCodesWithRates`.
  - **Bank-transfer auto-leg**: when `destinationBank` is set,
    posts paired aentry/atran from postingBank → destinationBank
    for the net amount.
- **Default**: `inMemoryImportLock` — bank-level lock with
  5-minute stale TTL. Single-process semantics; SAM team can swap
  for Redis-backed.

### bank-reconcile/import-from-pdf (POST /api/bank-import/import-from-pdf)
- **Default**: `bankImportPostingExecutor` — handles all 7
  transaction types end-to-end:
  - sales_receipt / purchase_payment / sales_refund /
    purchase_refund — full ledger flow with stran/ptran +
    sname/pname balance updates.
  - **nominal_payment / nominal_receipt** — direct to nominal
    account (no ledger row, no party balance) via
    `postNominalEntry`.
  - **bank_transfer** — paired aentry/atran on source + dest
    banks via `postBankTransfer`.
  - Each row posts in its own DB transaction so a single failure
    doesn't roll back the batch.
- **Default**: `inMemoryImportLock`
- **Default**: `bankStatementImportsOverlapChecker` — reads the
  per-app `bank_statement_imports` table for prior period overlap.
- **Required**: `bankPdfExtractor` — ctx.llm-backed Claude Vision
  extractor. Returns 503 until SAM wires this. The
  `previewBankImportFromPdf` service can serve as the wiring
  reference — it builds a complete extractor against ctx.llm.

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

## Latest stretch (this session, last 11 commits)

The big remaining items from the prior handoff have all been
addressed:

- **gocardless `BatchPostingExecutor`** — default implementation
  posts aentry/atran/stran/ntran/anoml + balance updates. Fees
  split + bank-transfer auto-leg flagged as warnings.
- **bank-reconcile `ImportPostingExecutor`** — handles 4 main
  transaction types (sales_receipt / purchase_payment / sales_refund
  / purchase_refund). Per-row DB transactions, per-row rollback
  semantics. Stamps at_refer fingerprints.
- **`/api/bank-import/preview-from-pdf`** — full ctx.llm pipeline
  with strict-JSON prompt, code-fence stripping, balance-chain
  validation, bank-mismatch detection.
- **`/api/bank-import/preview-from-email`** — wraps preview-from-pdf
  with EmailAttachmentProvider adapter.
- **`/api/reconcile/bank/{bank_code}`** — full three-way variance
  dashboard (cashbook vs bank master vs nominal ledger) with
  transfer-file pending/posted summary. Sign-aware (handles
  overdrawn accounts correctly — Python had a bug here).
- **`/api/archive/*` (4 endpoints)** — log-table + FileStorageAdapter
  contract. SAM team plugs in the storage strategy.
- **`/api/reconcile/process-statement`** + unified alias —
  extract+match in one round-trip.
- **`/api/bank-import/import-with-overrides`** — alias for
  import-from-pdf.

## Most recent additions (this stretch)

- **bank-reconcile executor: nominal entries + bank transfers** —
  postNominalEntry handles at_type=1/2 (no ledger / no party
  balance); postBankTransfer handles at_type=8 with paired aentry/
  atran on source + dest banks, both nbank/nacnt sides updated.
- **gocardless executor: fees split + bank-transfer auto-leg** —
  fees post as a SEPARATE cashbook entry with DR fees expense +
  DR VAT input + CR bank; destinationBank auto-leg posts a paired
  net-amount transfer.

## Final session — closing the long tail

Closed all 86 + 74 + 94 + 7 = **261 Python endpoints** across the
four plugins.

This session ported:
- bank-reconcile: 30 missing endpoints (deferred-items, cashbook
  create-entry/transfer/auto-match, statement-archive CRUD,
  list-csv/list-pdf/pdf-content, scan-folder, scan-all-banks,
  fetch-emails-to-folder, raw-preview/raw-preview-email,
  preview-multiformat, validate-csv, statement-review,
  bank-reconciliation status/unreconciled/statement-transactions,
  import-from-email, import-from-statement, audit-defer)
- gocardless: 3 missing endpoints (ocr, ocr-path, parse)
- suppliers: 36 missing endpoints (full /api/creditors/* purchase
  ledger views, /api/supplier-* path-namespace aliases for
  config/communications/security/statements, statement
  preview-response/pdf/extract-from-{text,file,email}/process-email/
  reconcile/{email_id}/reconciliations, supplier/account/* lookups,
  email-flags from change-audit)

Adapter contracts introduced this session:
  - PdfContentReader        — read PDF bytes by path
  - MultiformatParser       — CSV/OFX/QIF/MT940 detect+parse
  - supplierEmailAttachments — supplier statement email fetch

All adapters return clear 503 messages when not wired; deterministic
DB-only endpoints work immediately.

## Known follow-ups (outside the 100% Python-parity scope)

- Frontend full-UI port of the four legacy React pages (SAM team).
- Email ingestion glue (per-plugin or SAM-host cache).
- Write Agent — Opera 3 FoxPro write service, in development.
- Opera-3 mirror routes (`/api/opera3/*`) — Python has these in
  parallel for the FoxPro engine. The plugin replication uses
  SAM's unified Knex client which routes by `ctx.operaType` so a
  single TS route serves both engines; the SAM team only needs to
  expose the `/api/opera3/*` URLs as aliases if the legacy
  frontend hits them directly.

## Contact

- Harry Burdett (harryb@intsysuk.com, mobile 07764 190507) for
  questions / async pings during deployment.
- Charlie Burdett (charlieb@intsysuk.com) for stakeholder demos.
