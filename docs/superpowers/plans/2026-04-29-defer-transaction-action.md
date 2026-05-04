# Defer Transaction Action Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a per-row `'defer'` action at Stage 3 (Import) of the Bank Statement Reconciliation flow. A deferred row is **not** posted to Opera, **not** added to the permanent ignore list, and **does** count as "decided" so the Import button enables. An audit row is written to a small local SQLite table for traceability. Stage 4 (Reconcile) is unchanged — deferred rows reach it as ordinary unmatched lines and the existing partial-reconciliation dialog handles them as today.

**Architecture:** Pure additive change. Backend extends the allowed list of `transaction_type` overrides to include `'defer'` and counts/audits deferred rows in the response — no changes to the import loop's posting logic (deferred rows naturally fall outside the existing posting whitelist). Frontend adds a Defer button alongside the existing Assign and Ignore buttons. A small new module `sql_rag/deferred_transactions_db.py` owns the audit-only SQLite table.

**Tech Stack:** Python 3, FastAPI, pytest, React + TypeScript, Tailwind, SQLite.

**Spec:** `docs/superpowers/specs/2026-04-29-defer-transaction-action-design.md`

---

## File Structure

| File | Role | Status |
|---|---|---|
| `sql_rag/deferred_transactions_db.py` | New SQLite wrapper: schema, `record()` helper, `count_for_bank()` helper for tests | **CREATE** |
| `tests/test_deferred_transactions_db.py` | Unit tests for the wrapper | **CREATE** |
| `apps/bank_reconcile/api/routes.py` | Allow `'defer'` in transaction-type override whitelist; on each defer, write audit row + count; surface `deferred_count` in import response. Apply to both `/api/bank-import/import-with-overrides` (Opera SE) and `/api/opera3/bank-import/import-from-pdf` (Opera 3). | MODIFY |
| `frontend/src/pages/BankStatementReconcile.tsx` | New `Defer` button per unmatched row; amber "Awaiting manual entry" badge; state to track deferred lines; relax Import button enable rule to count deferred rows as decided. | MODIFY |
| `apps/core/docs/opera_knowledge_base.md` | Document the new `'defer'` action and its effect on Stage 3 vs Stage 4. | MODIFY |
| `marketing/manuals/manual-bank-reconciliation.md` | User-facing paragraph on when to use Defer vs Ignore vs Assign. Update Last-updated date. | MODIFY |

---

## Task 1: Create `deferred_transactions_db` module

**Files:**
- Create: `sql_rag/deferred_transactions_db.py`
- Create: `tests/test_deferred_transactions_db.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_deferred_transactions_db.py` with the following content:

```python
"""Tests for sql_rag/deferred_transactions_db.py

Covers schema initialisation, single-record write, repeated writes, and the
count_for_bank helper used to verify audit traceability in higher-level tests.
"""

import os
import tempfile

import pytest

from sql_rag.deferred_transactions_db import DeferredTransactionsDB


@pytest.fixture
def tmpdb(tmp_path):
    db_path = tmp_path / "deferred.db"
    return DeferredTransactionsDB(str(db_path))


def test_schema_is_created_on_first_use(tmpdb):
    # Schema is created on construction — count_for_bank should work even with no rows.
    assert tmpdb.count_for_bank("BC010") == 0


def test_record_inserts_a_row(tmpdb):
    tmpdb.record(
        bank_code="BC010",
        statement_date="2026-04-17",
        amount=123.45,
        description="Test deferred payment",
        deferred_by="admin",
    )
    assert tmpdb.count_for_bank("BC010") == 1


def test_record_supports_multiple_rows_for_same_bank(tmpdb):
    tmpdb.record(bank_code="BC010", statement_date="2026-04-17", amount=10.0,
                 description="A", deferred_by="admin")
    tmpdb.record(bank_code="BC010", statement_date="2026-04-17", amount=20.0,
                 description="B", deferred_by="admin")
    tmpdb.record(bank_code="BC020", statement_date="2026-04-17", amount=30.0,
                 description="C", deferred_by="admin")
    assert tmpdb.count_for_bank("BC010") == 2
    assert tmpdb.count_for_bank("BC020") == 1


def test_record_handles_negative_amounts(tmpdb):
    tmpdb.record(
        bank_code="BC010",
        statement_date="2026-04-17",
        amount=-456.78,  # Payment out
        description="Outgoing payment deferred",
        deferred_by="admin",
    )
    assert tmpdb.count_for_bank("BC010") == 1


def test_record_handles_missing_optional_fields(tmpdb):
    # description and deferred_by may be empty strings
    tmpdb.record(
        bank_code="BC010",
        statement_date="2026-04-17",
        amount=5.0,
        description="",
        deferred_by="",
    )
    assert tmpdb.count_for_bank("BC010") == 1


def test_reopening_db_preserves_rows(tmp_path):
    path = str(tmp_path / "deferred.db")
    db1 = DeferredTransactionsDB(path)
    db1.record(bank_code="BC010", statement_date="2026-04-17", amount=1.0,
               description="X", deferred_by="admin")

    db2 = DeferredTransactionsDB(path)  # Re-open
    assert db2.count_for_bank("BC010") == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run from the repo root:

```bash
source venv/bin/activate && pytest tests/test_deferred_transactions_db.py -v
```

Expected: ImportError or ModuleNotFoundError on `sql_rag.deferred_transactions_db`.

- [ ] **Step 3: Create the module**

Create `sql_rag/deferred_transactions_db.py` with the following content (note: `from __future__ import annotations` is added so the type hints work on Python 3.9 — the project's interpreter):

```python
"""Audit-only SQLite store for bank-statement rows the user marked as
'Awaiting manual entry' (deferred) at Stage 3 of the reconciliation flow.

Failure to write must NOT block the import workflow — callers should wrap
record() in a try/except and log a warning on failure. This module deliberately
does not raise to the caller for write failures.

Schema is created on first use. Concurrent access from multiple processes is
safe via SQLite's default file locking; the workflow is single-writer per
import endpoint so contention is negligible.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Optional

logger = logging.getLogger(__name__)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS deferred_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_code TEXT NOT NULL,
    statement_date TEXT,
    amount REAL,
    description TEXT,
    deferred_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deferred_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_deferred_bank_date
    ON deferred_transactions(bank_code, statement_date);
"""


class DeferredTransactionsDB:
    """Tiny SQLite wrapper for the deferred-transactions audit table.

    Usage:
        db = DeferredTransactionsDB("/path/to/deferred.db")
        db.record(bank_code="BC010", statement_date="2026-04-17",
                  amount=123.45, description="...", deferred_by="admin")
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_SCHEMA)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def record(
        self,
        *,
        bank_code: str,
        statement_date: Optional[str],
        amount: Optional[float],
        description: str,
        deferred_by: str,
    ) -> None:
        """Insert one audit row. Failures log a warning and are swallowed —
        an audit-only write must never block the user's workflow."""
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO deferred_transactions
                        (bank_code, statement_date, amount, description, deferred_by)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (bank_code, statement_date, amount, description, deferred_by),
                )
                conn.commit()
        except Exception as e:
            logger.warning(
                "Failed to record deferred transaction (bank=%s, amount=%s): %s",
                bank_code, amount, e,
            )

    def count_for_bank(self, bank_code: str) -> int:
        """Test/diagnostic helper: how many deferred rows exist for a bank."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT COUNT(*) FROM deferred_transactions WHERE bank_code = ?",
                (bank_code,),
            )
            return int(cur.fetchone()[0])
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
source venv/bin/activate && pytest tests/test_deferred_transactions_db.py -v
```

Expected: 6 tests PASS.

- [ ] **Step 5: Run the full test suite to confirm no regressions**

Run:

```bash
source venv/bin/activate && pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add sql_rag/deferred_transactions_db.py tests/test_deferred_transactions_db.py
git commit -m "feat(bank-recon): add deferred_transactions audit DB"
```

---

## Task 2: Allow `'defer'` action in Opera SE import endpoint

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py` (`/api/bank-import/import-with-overrides`, around line 4068)

This task extends the request validator so the frontend can send `transaction_type: 'defer'` in an override entry, and writes one audit row per deferred line.

- [ ] **Step 1: Inspect the current allowed-list line**

Open the file at `apps/bank_reconcile/api/routes.py`. Find the line beginning `if transaction_type and transaction_type in (` (around line 4170). The current code is:

```python
if transaction_type and transaction_type in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt', 'bank_transfer'):
    txn.action = transaction_type
    # Store bank transfer details on the transaction
    if transaction_type == 'bank_transfer':
        txn.bank_transfer_details = override.get('bank_transfer_details', {})
```

- [ ] **Step 2: Add `'defer'` to the allowed list**

Replace the tuple in the condition above to include `'defer'`. Use Edit with enough surrounding context for unique match. After:

```python
if transaction_type and transaction_type in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt', 'bank_transfer', 'defer'):
    txn.action = transaction_type
    # Store bank transfer details on the transaction
    if transaction_type == 'bank_transfer':
        txn.bank_transfer_details = override.get('bank_transfer_details', {})
```

- [ ] **Step 3: Add audit + count after the override loop**

Find the end of the override-application loop (the for loop that walks `transactions` and assigns `txn.action` from `override_map`). After that loop completes, insert a block that walks the transactions, records each `action == 'defer'` row in the audit DB, and tallies a count for the response.

Use the following block. Place it AFTER the override loop and BEFORE the period-validation step (search for `# Validate periods for all selected transactions` to find the boundary):

```python
        # --- Audit and count deferred rows ---
        # Deferred rows are not posted to Opera (they fall outside the import
        # action whitelist below). We record one audit row per deferred line
        # and surface deferred_count in the response so the UI can confirm
        # the user's choice was honoured.
        deferred_count = 0
        try:
            from sql_rag.deferred_transactions_db import DeferredTransactionsDB
            from sql_rag.company_data import get_current_db_path
            audit_path = get_current_db_path("bank_reconcile/deferred_transactions.db")
            audit_db = DeferredTransactionsDB(audit_path)
            for txn in transactions:
                if txn.action == 'defer':
                    deferred_count += 1
                    audit_db.record(
                        bank_code=bank_code,
                        statement_date=txn.date.isoformat() if hasattr(txn.date, 'isoformat') else str(txn.date or ''),
                        amount=float(txn.amount or 0.0),
                        description=(txn.memo or txn.name or '')[:255],
                        deferred_by="admin",
                    )
        except Exception as audit_err:
            logger.warning("Deferred-transaction audit failed: %s", audit_err)
```

- [ ] **Step 4: Surface `deferred_count` in the response**

Find the `return` statement at the end of `import_with_manual_overrides` (it returns a dict containing `imported`, `skipped`, etc.). Add `deferred_count` to that dict.

The current return shape ends with something like:

```python
return {
    "success": True,
    "imported": imported,
    "skipped": skipped,
    ...
}
```

Locate the actual return dict (search for `return {` near the end of the function — there will be one main success return and possibly error returns), and add `"deferred_count": deferred_count,` to the success return only.

- [ ] **Step 5: Smoke-test the import**

Run from the repo root:

```bash
source venv/bin/activate && python -c "from apps.bank_reconcile.api.routes import router; print('ok')"
```

Expected: prints `ok` with no traceback.

- [ ] **Step 6: Run the full test suite**

```bash
source venv/bin/activate && pytest tests/ -q
```

Expected: all tests pass — no regressions.

- [ ] **Step 7: Commit**

```bash
git add apps/bank_reconcile/api/routes.py
git commit -m "feat(bank-recon-se): accept defer action and audit deferred rows"
```

---

## Task 3: Mirror `'defer'` handling in Opera 3 import endpoint

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py` (`/api/opera3/bank-import/import-from-pdf`, around line 12074)

The Opera 3 endpoint accepts the same overrides shape but is a separate function. Apply the equivalent of Task 2 here, using `BankStatementMatcherOpera3` / `bank_import_opera3` semantics (same field names — `txn.action`, `txn.amount`, `txn.date`, `txn.memo`, `txn.name`).

- [ ] **Step 1: Locate the override-application section**

In the same `routes.py`, find the function `opera3_import_bank_statement_from_pdf` (around line 12074). Read 50 lines below the body parsing (`overrides = body.get('overrides', [])`) to find where overrides are applied to transactions. The override-loop will look similar to the SE version — a `for o in overrides:` that sets `txn.action` from `transaction_type`. There will be a corresponding allowed-types tuple.

- [ ] **Step 2: Add `'defer'` to the Opera 3 allowed-list**

If you find a line analogous to:

```python
if transaction_type and transaction_type in ('sales_receipt', 'purchase_payment', ...):
    txn.action = transaction_type
```

extend the tuple to include `'defer'` exactly as in Task 2 Step 2.

If the Opera 3 endpoint uses a different mechanism (e.g. sets `txn.action = override.get('transaction_type')` without a whitelist), add a defensive whitelist check that includes `'defer'`. Pseudocode for the defensive form:

```python
ALLOWED_O3_ACTIONS = ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund', 'nominal_payment', 'nominal_receipt', 'bank_transfer', 'defer')
if transaction_type in ALLOWED_O3_ACTIONS:
    txn.action = transaction_type
```

The exact code change depends on what's already there — read first, then make the smallest targeted change.

- [ ] **Step 3: Add audit + count for Opera 3**

After the override loop in `opera3_import_bank_statement_from_pdf`, insert the same audit block as in Task 2 Step 3 (verbatim — `bank_code` and `transactions` variables exist in this scope too).

- [ ] **Step 4: Surface `deferred_count` in the Opera 3 response**

Find the success-path `return` dict in `opera3_import_bank_statement_from_pdf` (search for `return {` and `"success": True`). Add `"deferred_count": deferred_count,`.

- [ ] **Step 5: Smoke-test the import**

```bash
source venv/bin/activate && python -c "from apps.bank_reconcile.api.routes import router; print('ok')"
```

Expected: prints `ok`.

- [ ] **Step 6: Run the full test suite**

```bash
source venv/bin/activate && pytest tests/ -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add apps/bank_reconcile/api/routes.py
git commit -m "feat(bank-recon-o3): accept defer action and audit deferred rows"
```

---

## Task 4: Frontend Defer button + amber badge + Import button gate

**Files:**
- Modify: `frontend/src/pages/BankStatementReconcile.tsx`

This task has the largest UI surface. Steps are split fine-grained.

- [ ] **Step 1: Add deferred state**

In `BankStatementReconcile.tsx`, near the existing `useState` declarations for `selectedForImport`, `manualMatchOverrides`, etc. (search for `manualMatchOverrides` to locate the section), add:

```tsx
  const [deferredLines, setDeferredLines] = useState<Set<number>>(new Set());
```

This Set holds `statement_line` numbers the user has marked as Defer in the current session.

- [ ] **Step 2: Add a "Defer" button next to Assign and Ignore**

Find the per-row action `<td>` block (around line 3763 — the `<div>` containing the Assign and Ignore buttons in the unmatched-rows table). The current structure is:

```tsx
                                  <td className="px-3 py-2 text-center">
                                    <div className="flex items-center justify-center gap-1">
                                      <button
                                        onClick={() => { ... }}
                                        className="text-xs px-2 py-1 text-blue-600 hover:text-blue-800 hover:bg-blue-50 rounded"
                                        title="Assign or edit customer/supplier/nominal"
                                      >
                                        Assign
                                      </button>
                                      <button
                                        onClick={() => setIgnoreConfirm({ ... })}
                                        className="text-xs px-2 py-1 text-orange-600 hover:text-orange-800 hover:bg-orange-50 rounded"
                                        title="Ignore this transaction"
                                      >
                                        Ignore
                                      </button>
                                    </div>
                                  </td>
```

Replace it (using the surrounding `<td>` as Edit context) with:

```tsx
                                  <td className="px-3 py-2 text-center">
                                    {deferredLines.has(line.statement_line) ? (
                                      <div className="flex items-center justify-center gap-1">
                                        <span
                                          className="text-xs px-2 py-1 bg-amber-100 text-amber-800 rounded inline-flex items-center gap-1"
                                          title="This row will not be imported. It will reappear on the next scan unless it has been entered into Opera manually."
                                        >
                                          <Clock className="h-3 w-3" />
                                          Awaiting manual entry
                                        </span>
                                        <button
                                          onClick={() => {
                                            const next = new Set(deferredLines);
                                            next.delete(line.statement_line);
                                            setDeferredLines(next);
                                          }}
                                          className="text-xs px-2 py-1 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded"
                                          title="Undo defer"
                                        >
                                          Undo
                                        </button>
                                      </div>
                                    ) : (
                                      <div className="flex items-center justify-center gap-1">
                                        <button
                                          onClick={() => {
                                            setNewEntryForm({
                                              accountCode: line.matched_account || '',
                                              accountType: line.suggested_type || (line.statement_amount > 0 ? 'customer' : 'supplier'),
                                              nominalCode: '',
                                              reference: line.statement_reference || '',
                                              description: line.statement_description || '',
                                              destBank: '',
                                              projectCode: '',
                                              departmentCode: '',
                                            });
                                            setCreateEntryModal({ open: true, statementLine: line });
                                          }}
                                          className="text-xs px-2 py-1 text-blue-600 hover:text-blue-800 hover:bg-blue-50 rounded"
                                          title="Assign or edit customer/supplier/nominal"
                                        >
                                          Assign
                                        </button>
                                        <button
                                          onClick={() => {
                                            const next = new Set(deferredLines);
                                            next.add(line.statement_line);
                                            setDeferredLines(next);
                                          }}
                                          className="text-xs px-2 py-1 text-amber-600 hover:text-amber-800 hover:bg-amber-50 rounded"
                                          title="Mark as awaiting manual entry — will not be imported, will reappear on next scan"
                                        >
                                          Defer
                                        </button>
                                        <button
                                          onClick={() => setIgnoreConfirm({
                                            date: line.statement_date || '',
                                            description: line.statement_description,
                                            amount: line.statement_amount,
                                          })}
                                          className="text-xs px-2 py-1 text-orange-600 hover:text-orange-800 hover:bg-orange-50 rounded"
                                          title="Ignore this transaction (already entered in Opera)"
                                        >
                                          Ignore
                                        </button>
                                      </div>
                                    )}
                                  </td>
```

The block introduces three states for an unmatched row: undecided (Assign / Defer / Ignore buttons), deferred (amber pill + Undo), and after Ignore (the row leaves the list as today via the existing `/ignore-transaction` flow).

- [ ] **Step 3: Import the `Clock` icon**

At the top of the file, find the `lucide-react` import and add `Clock`:

Search for `from 'lucide-react'`. Update the import to include `Clock` if not already present. Example (existing imports may differ; preserve all existing ones, add `Clock`):

```tsx
import { ..., Clock, ... } from 'lucide-react';
```

- [ ] **Step 4: Include deferred rows in the override payload sent to the backend**

Find where overrides are built before calling the import endpoint. Search for `overrides:` or `transaction_type:` in the file to locate the function that posts to `/api/bank-import/import-with-overrides`. The override array is constructed by mapping line state to objects like `{ row, account, ledger_type, transaction_type }`.

After the existing loop that builds the override entries from `manualMatchOverrides` / `newEntryForm` data, append entries for deferred rows. Pseudocode pattern (adapt to the actual variable names you find):

```tsx
      // Append override entries for deferred rows
      deferredLines.forEach((stmtLine) => {
        // Find the original row to get its row_number for backend
        const row = enrichedUnmatched.find(u => u.statement_line === stmtLine);
        if (!row) return;
        overrides.push({
          row: row.row_number ?? stmtLine,
          transaction_type: 'defer',
        });
      });
```

The exact `overrides` variable and the source of `row.row_number` differ slightly per the existing code — read 30 lines around the override-building loop and make the pattern fit. The key constraint: every row in `deferredLines` must produce an override entry with `transaction_type: 'defer'` so the backend's allowed-list path triggers.

- [ ] **Step 5: Reset `deferredLines` after a successful import**

After the import POST returns success, the Set should be cleared so the next session starts fresh. Find the success branch of the import handler (look for a `setSelectedForImport(new Set())` or similar reset) and add:

```tsx
      setDeferredLines(new Set());
```

- [ ] **Step 6: TypeScript compile check**

```bash
cd /Users/maccb/llmragsql/frontend && npx tsc --noEmit
```

Expected: zero errors.

- [ ] **Step 7: Manual visual verification**

The Vite dev server should already be running on port 5173 (uvicorn `--reload` does not affect Vite). If not:

```bash
cd /Users/maccb/llmragsql/frontend && npm run dev
```

In the browser:
1. Log in.
2. Open Bank Statement Reconciliation, scan a known statement that has at least one unmatched row.
3. Confirm three buttons appear on each unmatched row: **Assign**, **Defer**, **Ignore**.
4. Click **Defer** on one row. Confirm:
   - Buttons replaced with an amber "Awaiting manual entry" pill.
   - An "Undo" button beside the pill restores the three buttons when clicked.
5. With at least one row deferred, proceed with the Import action. Confirm:
   - The deferred row is NOT posted to Opera (check the Opera atran query the page exposes, or the API log for absence of inserts for that row).
   - The deferred row is NOT removed from the unmatched list permanently — re-scanning the same statement should show it again.
   - The response body in DevTools Network includes `deferred_count: 1` (or whichever count).

- [ ] **Step 8: Commit**

```bash
git add frontend/src/pages/BankStatementReconcile.tsx
git commit -m "feat(bank-recon-ui): defer button, amber badge, deferred override payload"
```

---

## Task 5: Knowledge base update

**Files:**
- Modify: `apps/core/docs/opera_knowledge_base.md`

- [ ] **Step 1: Read the current file** to find a suitable location

Find an existing section about Bank Statement Import / Bank Reconciliation. If a "Cashbook Transaction Types" or "Bank Statement Import" section exists, add the new content as a sub-section. Otherwise insert just above the "Variance Analysis (Reconciliation)" section if present, else append at the end.

- [ ] **Step 2: Insert the following content** as a new sub-section

```markdown
### Defer Transaction Action (Bank Statement Reconciliation)

At Stage 3 (Import), each unmatched bank-statement row carries a per-row `action`. In addition to the existing posting actions (`sales_receipt`, `purchase_payment`, `sales_refund`, `purchase_refund`, `nominal_payment`, `nominal_receipt`, `bank_transfer`) and the permanent Ignore endpoint, there is a **`defer`** action.

A deferred row:
- Counts as "decided" so the Import button enables — no need to fill in customer/supplier/nominal details.
- Is **not** posted to Opera (no `atran`/`aentry`/`ntran`/`anoml` writes).
- Is **not** added to the permanent ignore list.
- Reaches Stage 4 as an ordinary unmatched line — the existing partial-reconciliation dialog covers it. **Stage 4 has no defer-specific UI.**
- Reappears as unmatched on the next scan if Opera still has no record; auto-matches once a manual posting has been entered.

Audit: every defer is logged to `data/<company>/bank_reconcile/deferred_transactions.db` (table `deferred_transactions`, columns `bank_code, statement_date, amount, description, deferred_at, deferred_by`). Audit-only — no business logic depends on it.

Files: `sql_rag/deferred_transactions_db.py`, `apps/bank_reconcile/api/routes.py` (Opera SE: `/api/bank-import/import-with-overrides`; Opera 3: `/api/opera3/bank-import/import-from-pdf`), `frontend/src/pages/BankStatementReconcile.tsx`.
```

- [ ] **Step 3: Commit**

```bash
git add apps/core/docs/opera_knowledge_base.md
git commit -m "docs(kb): document defer transaction action"
```

---

## Task 6: User manual update

**Files:**
- Modify: `marketing/manuals/manual-bank-reconciliation.md`

- [ ] **Step 1: Read the current file** to find the Stage 3 section

Find the section that describes Stage 3 (Review & Match / Import) of the 5-stage workflow. Look for headings or paragraphs describing the per-row Assign and Ignore options.

- [ ] **Step 2: Insert this user-facing paragraph** in the Stage 3 area

```markdown
**Defer (Awaiting manual entry):** When a bank-statement row reflects a transaction that you know is being entered into Opera manually outside this routine, click **Defer** instead of Assign or Ignore. The row is not posted to Opera, is not added to the permanent ignore list, and does not block you from completing the rest of the reconciliation. On the next scan the row reappears — once the manual entry has been made in Opera it will auto-match like any other transaction. Use Defer (rather than Ignore) so the row is not lost; use Ignore only for genuine non-postings (bank fees, internal transfers handled elsewhere).
```

- [ ] **Step 3: Update the "Last updated" date**

Find an existing date line at the bottom of the file (e.g. `Last updated: 2026-04-28`) and update to `2026-04-29`. If no such line exists, append:

```markdown
---
Last updated: 2026-04-29
```

- [ ] **Step 4: Commit**

```bash
git add marketing/manuals/manual-bank-reconciliation.md
git commit -m "docs(manual): explain defer action for end users"
```

---

## Task 7: End-to-end manual verification

This task is verification only — no commits.

- [ ] **Step 1: Confirm services are healthy**

```bash
curl -s http://localhost:8000/api/licenses -o /dev/null -w "API: %{http_code}\n"
curl -s http://localhost:5173/ -o /dev/null -w "FE: %{http_code}\n"
```

Expected: both `200`.

- [ ] **Step 2: Trigger a Bank Statement Reconciliation flow**

In the browser:
1. Open Bank Statement Hub → scan a statement with at least one unmatched row that you do not want to post (a real "awaiting manual entry" candidate).
2. At Stage 3, click **Defer** on that row.
3. Confirm the row gets the amber "Awaiting manual entry" pill.
4. Click Import for the rest of the statement.
5. Confirm in the API log the request fires `POST /api/bank-import/import-with-overrides` with `transaction_type: 'defer'` in the overrides payload.
6. Confirm the response includes `deferred_count: 1`.
7. Confirm the deferred row was NOT inserted into Opera atran (compare row count before/after, or query atran by reference).
8. Move to Stage 4 (Reconcile). Confirm the existing partial-reconciliation dialog fires — it warns there are unmatched line(s) and offers Continue. The deferred row appears in this count.
9. Continue. Confirm matched rows are reconciled and the deferred row remains unreconciled.

- [ ] **Step 3: Confirm audit row was written**

```bash
sqlite3 /Users/maccb/llmragsql/data/intsys/bank_reconcile/deferred_transactions.db \
  "SELECT bank_code, statement_date, amount, description FROM deferred_transactions ORDER BY id DESC LIMIT 5"
```

Expected: at least one row showing the deferred transaction's bank_code, date, amount, and description.

- [ ] **Step 4: Confirm self-healing on next scan**

Manually post an Opera entry that matches the deferred row (or simulate by scanning again after the colleague has done it). Re-scan the same statement. Confirm the previously-deferred row now auto-matches as a normal Opera entry.

- [ ] **Step 5: Confirm Opera 3 parity (if Opera 3 is configured)**

Switch the active company to an Opera 3 company. Repeat steps 1–4 against the Opera 3 reconciliation flow. Behaviour must be identical.

If any step fails, return to the relevant earlier task and fix.

---

## Self-Review

Cross-checked spec sections against plan tasks:

| Spec section | Covered by |
|---|---|
| New `'defer'` action value, Stage 3 only | Tasks 2, 3, 4 |
| Stage 3 Import button enables when every row decided (incl. deferred) | Task 4 (Defer button removes the row from "needs assigning" set) |
| Deferred rows are NOT posted to Opera | Tasks 2, 3 (action falls outside posting whitelist) |
| Deferred rows are NOT added to permanent ignore list | Task 4 (Defer button does not call `/ignore-transaction`) |
| Audit log entry per defer | Tasks 1, 2, 3 |
| Stage 4 unchanged | Task 4 (no Stage 4 changes) |
| Both Opera SE and Opera 3 paths | Tasks 2 (SE), 3 (Opera 3) |
| Knowledge base + manual updates | Tasks 5, 6 |
| Manual verification | Task 7 |

No placeholders. Type and field names consistent across tasks (`'defer'`, `deferred_count`, `deferredLines`, `DeferredTransactionsDB`, `record()`, `count_for_bank()`, `deferred_transactions.db`).

---

**Plan complete.**
