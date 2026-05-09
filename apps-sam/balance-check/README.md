# balance-check (SAM plugin)

Internal Opera control-account reconciliation: cashbook, debtors,
creditors, VAT, trial balance. Read-only — never writes to Opera.

This is **not** the same thing as bank-reconciliation. The
distinction:

- **bank-reconcile**: reconciles a bank statement against Opera's
  cashbook (external → internal)
- **balance-check**: confirms Opera's own sub-ledgers agree with
  the nominal control accounts (internal consistency)

## What SAM provides

| ctx field | Required? | Purpose |
| --- | --- | --- |
| `db.getCompanyDb(code)` | yes | Knex pool for Opera (read-only) |
| `operaType` | yes | `'opera-se'` or `'opera-3'` |
| `logger` | yes | Standard logger interface |

No `db.app` — this plugin keeps no state. No `llm`, `emailIngest`,
or `email` — read-only reporting only.

## Routes

7 endpoints, all under `/api/reconcile/`:

- `summary` — at-a-glance status across all four checks
- `creditors` — purchase ledger vs creditors control
- `debtors` — sales ledger vs debtors control
- `cashbook` — cashbook vs bank master vs nominal
- `trial-balance` — full TB pull
- `vat` — VAT liability check
- `vat/diagnostic`, `vat/variance-drilldown` — VAT investigation

Every URL has an `/api/opera3/...` mirror via the same prefix-strip
middleware as the other plugins.

## Database

No migrations — `manifest.backend.separateDatabase = false`.

## Tests

```sh
npm test               # vitest run — 32 tests
npm run lint           # tsc --noEmit
```

## Frontend

Four ported pages from the legacy frontend (`CreditorsReconcile`,
`DebtorsReconcile`, `TrialBalanceCheck`, `CashbookReconcile`),
mounted via tab switcher in `BalanceCheck.tsx`. Tailwind scoped
to `.balance-check-app`.

```sh
cd frontend
npm install
npm run build
```
