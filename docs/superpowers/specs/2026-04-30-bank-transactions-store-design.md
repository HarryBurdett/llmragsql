# Bank Transactions Store — Design Spec (Future)

**Date**: 2026-04-30
**Status**: Captured for future implementation. Not in active development. Approved as a parking note.

## Problem

Today the app stores scanned bank statement data in `data/<company>/bank_reconcile/pdf_extraction_cache.db`:

```sql
extraction_cache (
    pdf_hash TEXT PRIMARY KEY,
    statement_info_json TEXT NOT NULL,
    transactions_json TEXT NOT NULL,
    transaction_count INTEGER NOT NULL,
    extracted_at TEXT NOT NULL,
    model_name TEXT,
    file_size INTEGER
)
```

Statement metadata and the full transaction list are stored as JSON blobs keyed by the PDF's SHA-256 hash. The cache works correctly for its primary purpose — preventing redundant Gemini calls — but it is **only queryable by hash**. To answer questions like:

- "What bank statement transactions for customer ABC have we seen across the last 12 months?"
- "Which transactions appear on bank statements but were never posted to Opera?"
- "Show all bank transactions for BC010 between Mar and Apr."

…the only way today is to load and parse every JSON row in the cache. That's not a tool, it's a workaround.

## Goal

Add a queryable, structured table — `bank_transactions` — that captures every transaction that has been *accepted into the books from a bank statement*, with normalised columns. The cache stays as the AI-extraction layer (raw text); the new table becomes the *processing / reporting* layer (curated data).

The two layers are complementary:

- **Cache** — what the AI extracted from a particular PDF. One row per unique PDF. Indefinite retention. Hash-keyed.
- **Bank transactions table** — what we've decided to act on. One row per imported / matched / deferred transaction. Linked to the originating statement. Queryable by bank, period, customer, account, action, status.

## Out of Scope

- Replacing the cache. The cache stays.
- Replacing Opera's `atran` / `aentry` tables. Those remain canonical for Opera's posted records. The new table is parallel — it captures a different perspective (bank-statement-centric) than Opera's ledger-centric view.
- Backfill of existing data on day one. Forward-only at first; backfill is its own follow-up.
- New dashboards or reports. Those come later, once data is accumulating.
- Cross-company aggregation. Each company has its own DB.

## Architecture

### New SQLite store

Path: `data/<company>/bank_reconcile/bank_transactions.db` (per company; existing pattern).

Schema:

```sql
CREATE TABLE IF NOT EXISTS bank_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_code TEXT NOT NULL,
    statement_period_start TEXT,           -- YYYY-MM-DD
    statement_period_end TEXT,             -- YYYY-MM-DD
    statement_pdf_hash TEXT,               -- links to pdf_extraction_cache
    transaction_date TEXT,                 -- YYYY-MM-DD
    description TEXT,
    reference TEXT,
    amount REAL,                           -- pounds, signed (positive = receipt)
    running_balance REAL,                  -- per the bank statement, may be NULL
    action TEXT,                           -- 'sales_receipt' | 'purchase_payment' | 'sales_refund' | 'purchase_refund' | 'nominal_payment' | 'nominal_receipt' | 'bank_transfer' | 'ignored' | 'deferred' | 'already_in_opera'
    matched_account TEXT,                  -- e.g. customer/supplier code, NULL for nominal
    matched_name TEXT,
    posted_atran_unique TEXT,              -- atran row identifier in Opera, NULL if not posted
    source TEXT,                           -- 'pdf' | 'email' | 'csv' | 'ofx'
    imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    imported_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_bt_bank_period
    ON bank_transactions(bank_code, statement_period_start, statement_period_end);
CREATE INDEX IF NOT EXISTS idx_bt_account
    ON bank_transactions(matched_account);
CREATE INDEX IF NOT EXISTS idx_bt_date
    ON bank_transactions(transaction_date);
```

### Wrapper module

`sql_rag/bank_transactions_db.py` — same audit-style pattern as `deferred_transactions_db.py`:

- `BankTransactionsDB(db_path)` — schema init on first use.
- `record(*, bank_code, statement_period_start, statement_period_end, statement_pdf_hash, transaction_date, description, reference, amount, running_balance, action, matched_account, matched_name, posted_atran_unique, source, imported_by)` — single-row insert. Wraps in try/except, logs warning on failure, never raises. Audit-only — never blocks the caller's business logic.
- `query_for_bank(bank_code, period_start=None, period_end=None)` — diagnostic / future-reporting helper.

### Write hooks

After every successful Opera post in:

- `apps/bank_reconcile/api/routes.py` — Opera SE per-row create-entry path, Opera SE batch import-with-overrides, Opera 3 import-from-pdf.

A single call:

```python
bank_transactions_db.record(
    bank_code=bank_code,
    statement_period_start=...,
    statement_period_end=...,
    statement_pdf_hash=...,           # if available from the source PDF
    transaction_date=txn.date.isoformat(),
    description=txn.memo or txn.name,
    reference=txn.reference,
    amount=float(txn.amount),
    running_balance=txn.balance,
    action=txn.action,                # 'sales_receipt' etc.
    matched_account=txn.matched_account,
    matched_name=txn.matched_name,
    posted_atran_unique=opera_post_result.atran_unique,
    source=source_kind,               # 'pdf' / 'email' / 'csv'
    imported_by="admin",              # placeholder until real auth wired
)
```

Wrapped in `try/except` — failure logs a warning, does not block the import.

### What gets recorded vs. not

| Statement line state | `bank_transactions` row written? |
|---|---|
| Matched + imported in this session | Yes, `action = 'sales_receipt' / 'purchase_payment' / etc.` and `posted_atran_unique` populated. |
| Matched against existing Opera atran ("In Opera" bucket) | Yes, `action = 'already_in_opera'`, `posted_atran_unique` = the matched atran's id. Useful for full statement-history reconstruction. |
| Ignored (permanent) | Yes, `action = 'ignored'`, `posted_atran_unique` NULL. |
| Deferred | Yes, `action = 'deferred'`, `posted_atran_unique` NULL. When later imported, a *new* row is added with `action = 'sales_refund'` (or whatever) and a link via `statement_pdf_hash` to the original deferred entry. |
| Skipped before reaching the operator (already_posted detection) | Yes, `action = 'already_in_opera'`. |

This means `bank_transactions` ends up with a complete record of *every line on every imported statement*, not just the lines that resulted in new Opera writes.

### Un-import / corrections

If a posted bank-import row is later removed in Opera (an existing un-import endpoint exists in the codebase), the matching `bank_transactions` row should be flagged or deleted. Single hook in the existing un-import endpoint handles it.

## Future Use Cases Unlocked

These are not built as part of this spec — just illustrating what the table enables:

- **"Bank vs Opera" report** — list every `bank_transactions` row where `posted_atran_unique IS NULL` and `action != 'ignored'`. Tells you what's been seen on the bank but never posted (i.e. all currently-deferred items + any orphans).
- **Customer transaction history** — query `bank_transactions WHERE matched_account = ?` across all banks and periods. Cross-bank visibility on customer payments.
- **Cash-flow trends** — sum `amount` grouped by month / bank / category.
- **Audit reconstruction** — given a statement period, list every line and what was done with it. Useful for end-of-year audits and HMRC enquiries.

These would be follow-up features once the table has accumulated data.

## Testing

`tests/test_bank_transactions_db.py`:

- Schema init creates the table on first use.
- Single insert writes a row.
- Multiple inserts grouped by bank + period work correctly.
- `query_for_bank` filters return the right rows.
- Insert with NULL optional fields succeeds.
- Re-opening the DB preserves rows (persistence).
- Failure to open the DB or insert logs a warning and does not raise.

## Effort Estimate

| Phase | Effort | Notes |
|---|---|---|
| Wrapper module + tests + schema | ~half day | Same pattern as `deferred_transactions_db.py`; mostly mechanical. |
| Write hooks in three import endpoints | ~half day | Three single-line additions inside existing try blocks. Tests for each endpoint to confirm row is written. |
| Un-import hook | ~hour | One additional call in the existing un-import endpoint. |
| Forward-only deployment (table empty at start, fills from next import onward) | implicit | No migration; data accumulates organically. |
| Backfill of historical statements (optional) | ~half day | Iterate `pdf_extraction_cache.db` and `bank_statement_imports`, reconstruct rows. Skip if not needed. |
| Knowledge base + manual updates | ~hour | Document the new table and its purpose. |

**Total for forward-only delivery: ~1.5 days.** Backfill and dashboards are separate downstream items.

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Write failure breaks the import flow | Wrap every write in try/except, log warning, never raise. Same audit-only pattern as Defer. |
| Row drift between `bank_transactions` and Opera atran (e.g. atran row deleted but bank_transactions row stays) | Add un-import hook to delete or flag. Acceptable risk while only-write hook is live; revisit before building reports. |
| Data privacy / retention | Per-company isolation as today. No new third-party data flows. Standard SQLite under `data/<company>/...`. |
| Schema migration concerns | All columns optional except primary key, bank_code, imported_at. Future schema additions can use `ALTER TABLE ADD COLUMN` safely. |
| User confusion ("which table is the truth?") | Document clearly: Opera atran is canonical; `bank_transactions` is the bank-statement-centric mirror used for cross-statement reporting. |

## Success Criteria

1. After deployment, every bank-statement import populates the `bank_transactions` table with one row per statement line.
2. The table is queryable with simple SQL — bank/period/account/date filters work via the indexes.
3. Existing import flows are unaffected — same posting behaviour, same UX, same Opera writes.
4. A row's failure to write does not block the import.
5. The table is forward-only on day one; historical backfill is captured as a separate follow-up.
6. Both Opera SE and Opera 3 paths populate the table identically.
7. The table is ready as the foundation for future reports and dashboards.
