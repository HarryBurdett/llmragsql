# Sequential Statement Gating Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow the operator to open and process the *next* bank statement (Match / Assign / Import) as soon as every row in the *current* statement has been decided (imported / ignored / deferred), without waiting for the current statement to be reconciled. Reconciliation of the next statement remains gated by the existing opening-balance check.

**Architecture:** Pure derivation + gate-relaxation. Compute a new `state` per statement from existing data (`bank_statement_imports.is_reconciled`, presence in `deferred_transactions.db`, drafts in email storage). Surface `state` and `deferred_count` on every statement entry returned by `/api/bank-import/scan-all-banks` (and the Opera 3 mirror). Relax the "next statement openable" gate from `prior.is_reconciled` to `prior.state in ('imported','reconciled')`. Frontend renders an amber "Imported · N deferred" pill and a per-bank summary line. No new tables, no new endpoints, no Opera schema changes.

**Tech Stack:** Python 3 (FastAPI, SQLite), pytest, React + TypeScript, Tailwind.

**Spec:** `docs/superpowers/specs/2026-04-30-sequential-statement-gating-design.md`

---

## File Structure

| File | Role | Status |
|---|---|---|
| `sql_rag/deferred_transactions_db.py` | Add `count_for_statement(bank_code, period_start, period_end)` helper for the new state derivation. | MODIFY |
| `tests/test_deferred_transactions_db.py` | Tests for the new helper. | MODIFY |
| `apps/bank_reconcile/api/routes.py` | Compute `state` + `deferred_count` per statement entry in `scan_all_banks_for_statements` (Opera SE) and `opera3_scan_emails_for_bank_statements` (Opera 3). Relax the next-statement gate. | MODIFY |
| `frontend/src/pages/BankStatementHub.tsx` | New types fields, amber "Imported · N deferred" pill, per-bank summary line, Process-button enable rule reads `state`. | MODIFY |
| `apps/core/docs/opera_knowledge_base.md` | Document the `imported` state and the relaxed gate. | MODIFY |
| `marketing/manuals/manual-bank-reconciliation.md` | One paragraph on the new "process next while waiting on deferred row" behaviour. Bump Last-updated. | MODIFY |

---

## Task 1: Add `count_for_statement` helper to `DeferredTransactionsDB`

**Files:**
- Modify: `sql_rag/deferred_transactions_db.py`
- Modify: `tests/test_deferred_transactions_db.py`

The existing `count_for_bank(bank_code)` helper counts every deferred row for a bank, regardless of which statement period the row belongs to. The new state-derivation logic needs to know whether *this specific statement* has any deferred rows — so we add a period-filtered helper.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_deferred_transactions_db.py`:

```python
def test_count_for_statement_returns_zero_with_no_rows(tmpdb):
    assert tmpdb.count_for_statement(
        bank_code="BC010",
        period_start="2026-04-01",
        period_end="2026-04-30",
    ) == 0


def test_count_for_statement_includes_only_in_period(tmpdb):
    tmpdb.record(bank_code="BC010", statement_date="2026-04-15",
                 amount=10.0, description="A", deferred_by="admin")
    tmpdb.record(bank_code="BC010", statement_date="2026-04-25",
                 amount=20.0, description="B", deferred_by="admin")
    tmpdb.record(bank_code="BC010", statement_date="2026-05-10",
                 amount=30.0, description="C — outside period", deferred_by="admin")
    tmpdb.record(bank_code="BC020", statement_date="2026-04-15",
                 amount=40.0, description="D — wrong bank", deferred_by="admin")

    count = tmpdb.count_for_statement(
        bank_code="BC010",
        period_start="2026-04-01",
        period_end="2026-04-30",
    )
    assert count == 2  # only A and B


def test_count_for_statement_period_inclusive_at_boundaries(tmpdb):
    tmpdb.record(bank_code="BC010", statement_date="2026-04-01",
                 amount=10.0, description="start of period", deferred_by="admin")
    tmpdb.record(bank_code="BC010", statement_date="2026-04-30",
                 amount=20.0, description="end of period", deferred_by="admin")

    count = tmpdb.count_for_statement(
        bank_code="BC010",
        period_start="2026-04-01",
        period_end="2026-04-30",
    )
    assert count == 2  # both boundary dates included


def test_count_for_statement_handles_missing_period_args(tmpdb):
    tmpdb.record(bank_code="BC010", statement_date="2026-04-15",
                 amount=10.0, description="A", deferred_by="admin")

    # If either period bound is None or empty, fall back to count_for_bank semantics.
    assert tmpdb.count_for_statement(bank_code="BC010", period_start=None, period_end=None) == 1
    assert tmpdb.count_for_statement(bank_code="BC010", period_start="", period_end="") == 1
    assert tmpdb.count_for_statement(bank_code="BC010", period_start="2026-04-01", period_end=None) == 1
```

- [ ] **Step 2: Run tests to confirm they fail**

Run from the repo root:

```bash
source venv/bin/activate && pytest tests/test_deferred_transactions_db.py -v
```

Expected: `count_for_statement` does not exist → `AttributeError` for every new test.

- [ ] **Step 3: Add the helper**

Open `sql_rag/deferred_transactions_db.py`. After the existing `count_for_bank` method, append:

```python
    def count_for_statement(
        self,
        bank_code: str,
        period_start: Optional[str],
        period_end: Optional[str],
    ) -> int:
        """Count deferred rows for a bank, optionally filtered to a statement period.

        If `period_start` or `period_end` is None or empty, the period filter is
        skipped and behaviour is equivalent to `count_for_bank(bank_code)`. The
        period bounds are inclusive (`statement_date BETWEEN start AND end`).
        """
        with self._connect() as conn:
            if period_start and period_end:
                cur = conn.execute(
                    """
                    SELECT COUNT(*) FROM deferred_transactions
                    WHERE bank_code = ?
                      AND statement_date IS NOT NULL
                      AND statement_date >= ?
                      AND statement_date <= ?
                    """,
                    (bank_code, period_start, period_end),
                )
            else:
                cur = conn.execute(
                    "SELECT COUNT(*) FROM deferred_transactions WHERE bank_code = ?",
                    (bank_code,),
                )
            return int(cur.fetchone()[0])
```

- [ ] **Step 4: Run tests**

```bash
source venv/bin/activate && pytest tests/test_deferred_transactions_db.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Run the full suite to confirm no regressions**

```bash
source venv/bin/activate && pytest tests/ -q
```

Expected: every test still passes.

- [ ] **Step 6: Commit**

```bash
git add sql_rag/deferred_transactions_db.py tests/test_deferred_transactions_db.py
git commit -m "feat(deferred-db): add count_for_statement period-filtered helper"
```

---

## Task 2: Add `derive_statement_state()` helper

**Files:**
- Modify: `sql_rag/deferred_transactions_db.py`
- Modify: `tests/test_deferred_transactions_db.py`

The existing scan-all-banks code already sets various flags on each statement row (`is_reconciled`, `is_imported`, `extraction_status`, etc.). We extract a small pure function that takes those signals plus the deferred count and returns the canonical state string. This keeps the state machine documented and tested in one place.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_deferred_transactions_db.py`:

```python
from sql_rag.deferred_transactions_db import derive_statement_state


def test_derive_state_reconciled():
    assert derive_statement_state(
        is_reconciled=True,
        has_import_record=True,
        has_draft=False,
        deferred_count=0,
        extraction_status='cached',
    ) == 'reconciled'


def test_derive_state_reconciled_overrides_other_flags():
    """If is_reconciled is True, that beats everything else."""
    assert derive_statement_state(
        is_reconciled=True,
        has_import_record=True,
        has_draft=True,
        deferred_count=5,
        extraction_status='extracted',
    ) == 'reconciled'


def test_derive_state_imported_when_deferred_present():
    assert derive_statement_state(
        is_reconciled=False,
        has_import_record=True,
        has_draft=False,
        deferred_count=2,
        extraction_status='extracted',
    ) == 'imported'


def test_derive_state_in_progress_when_draft_only():
    assert derive_statement_state(
        is_reconciled=False,
        has_import_record=False,
        has_draft=True,
        deferred_count=0,
        extraction_status='extracted',
    ) == 'in_progress'


def test_derive_state_pending_extraction():
    assert derive_statement_state(
        is_reconciled=False,
        has_import_record=False,
        has_draft=False,
        deferred_count=0,
        extraction_status='pending_extraction',
    ) == 'pending_extraction'


def test_derive_state_failed_extraction_falls_back_to_pending():
    assert derive_statement_state(
        is_reconciled=False,
        has_import_record=False,
        has_draft=False,
        deferred_count=0,
        extraction_status='failed',
    ) == 'pending_extraction'


def test_derive_state_ready_default():
    """No draft, no import, no deferred, extraction is good — ready to be processed."""
    assert derive_statement_state(
        is_reconciled=False,
        has_import_record=False,
        has_draft=False,
        deferred_count=0,
        extraction_status='extracted',
    ) == 'ready'


def test_derive_state_imported_with_no_deferred_falls_to_reconciled_intent():
    """An import record without deferred rows — Stage 4 should already have run.
    Treat as 'reconciled' for the purpose of state derivation; the live data
    will only ever be in this combo briefly during the import → reconcile
    transition. The downstream gate for 'next statement openable' accepts
    both 'imported' and 'reconciled', so the practical effect is identical."""
    assert derive_statement_state(
        is_reconciled=False,
        has_import_record=True,
        has_draft=False,
        deferred_count=0,
        extraction_status='extracted',
    ) == 'reconciled'
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
source venv/bin/activate && pytest tests/test_deferred_transactions_db.py -v
```

Expected: `ImportError` on `derive_statement_state`.

- [ ] **Step 3: Add the function**

Append to `sql_rag/deferred_transactions_db.py` (at module level, below the `DeferredTransactionsDB` class):

```python
def derive_statement_state(
    *,
    is_reconciled: bool,
    has_import_record: bool,
    has_draft: bool,
    deferred_count: int,
    extraction_status: Optional[str] = None,
) -> str:
    """Compute the canonical statement state from the underlying flags.

    Returns one of:
        'reconciled'           — Stage 4 complete, OR import done with no
                                 deferred rows still pending.
        'imported'             — import done, deferred rows still pending,
                                 not yet reconciled. The next statement is
                                 openable when prior is in this state.
        'in_progress'          — draft started but import not yet completed.
        'ready'                — extraction complete, no draft, no import,
                                 awaiting operator action.
        'pending_extraction'   — extraction failed or in progress; user can
                                 do nothing until it succeeds.

    See the spec for the full state machine.
    """
    if extraction_status in ('pending_extraction', 'failed'):
        return 'pending_extraction'
    if is_reconciled:
        return 'reconciled'
    if has_import_record and deferred_count > 0:
        return 'imported'
    if has_import_record:
        # Imported with zero deferred — Stage 4 should have run cleanly.
        return 'reconciled'
    if has_draft:
        return 'in_progress'
    return 'ready'
```

- [ ] **Step 4: Run tests**

```bash
source venv/bin/activate && pytest tests/test_deferred_transactions_db.py -v
```

Expected: all 8 new tests pass.

- [ ] **Step 5: Run the full suite**

```bash
source venv/bin/activate && pytest tests/ -q
```

Expected: every test passes.

- [ ] **Step 6: Commit**

```bash
git add sql_rag/deferred_transactions_db.py tests/test_deferred_transactions_db.py
git commit -m "feat(deferred-db): add derive_statement_state() function for sequential gating"
```

---

## Task 3: Surface `state` and `deferred_count` in scan-all-banks (Opera SE)

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py`

In `scan_all_banks_for_statements` (the function decorated with `@router.get("/api/bank-import/scan-all-banks")`, starts around line 5880), each statement's response dict already gets a `status` field set to one of `'ready' | 'sequence_gap' | 'pending_extraction' | 'imported' | 'already_processed'` etc. Today this string mixes "where is this in the chain" with "what's the import state". We'll add two **new** sibling fields — `state` (canonical state from `derive_statement_state`) and `deferred_count` — without removing the existing `status` (some downstream code still reads it). The `state` field is the one the new UX uses.

- [ ] **Step 1: Locate the per-statement assembly section**

Read `apps/bank_reconcile/api/routes.py` lines 5880–7100 to understand the response shape. The function builds `all_banks` keyed by `bank_code`, each containing a list of statement dicts. Each dict has fields like `filename`, `email_id`, `attachment_id`, `period_start`, `period_end`, `is_reconciled` (sometimes), `extraction_status`, etc.

The function ends with a per-bank loop at approximately line 7100 that computes the `extraction_status: 'complete' | 'incomplete'` gate — the new `state` derivation should run **inside that same loop**, applied to every statement after `extraction_status` and `deferred_count` are known.

- [ ] **Step 2: Add the import**

Find the imports at the top of the file. Add:

```python
from sql_rag.deferred_transactions_db import (
    DeferredTransactionsDB,
    derive_statement_state,
)
from sql_rag.company_data import get_current_db_path
```

(Both are likely already imported elsewhere in the file. If `derive_statement_state` is not yet imported in this file, add it. If `get_current_db_path` is already imported once, don't re-import.)

- [ ] **Step 3: Build a deferred-count lookup once per scan**

Just before the per-bank loop where `state` gets assigned, build a single `DeferredTransactionsDB` instance and cache it for the duration of the scan:

```python
        # Build deferred-count lookup once for the whole scan. Every statement's
        # state derivation needs to know whether deferred rows are pending for
        # that bank+period combination.
        deferred_db = None
        try:
            deferred_path = get_current_db_path("bank_reconcile/deferred_transactions.db")
            deferred_db = DeferredTransactionsDB(deferred_path)
        except Exception as defer_err:
            logger.warning("Could not open deferred_transactions DB for scan-all-banks: %s", defer_err)
```

This goes immediately above the per-bank loop that already exists in the function (search for the loop that walks `all_banks.items()` and computes `extraction_status` per bank — the assembly that landed in commit `b4d2c64` earlier).

- [ ] **Step 4: Compute `state` + `deferred_count` per statement**

Inside the per-bank loop, after each statement dict has `extraction_status`, `is_reconciled`, etc. set, add:

```python
                # --- Sequential statement gating: derive state + deferred_count ---
                period_start = stmt.get('period_start')
                period_end = stmt.get('period_end')
                is_reconciled = bool(stmt.get('is_reconciled', False))
                # has_import_record: filename appears in imported_nr_filenames OR reconciled_filenames
                has_import_record = bool(
                    is_reconciled
                    or (filename and filename in imported_nr_filenames)
                )
                has_draft = bool(stmt.get('has_draft', False))

                deferred_count_for_stmt = 0
                if deferred_db and bank_code:
                    try:
                        deferred_count_for_stmt = deferred_db.count_for_statement(
                            bank_code=bank_code,
                            period_start=period_start,
                            period_end=period_end,
                        )
                    except Exception as count_err:
                        logger.warning(
                            "Could not count deferred rows for %s/%s/%s: %s",
                            bank_code, period_start, period_end, count_err,
                        )

                stmt['deferred_count'] = deferred_count_for_stmt
                stmt['state'] = derive_statement_state(
                    is_reconciled=is_reconciled,
                    has_import_record=has_import_record,
                    has_draft=has_draft,
                    deferred_count=deferred_count_for_stmt,
                    extraction_status=stmt.get('extraction_status'),
                )
```

The loop variable names — `stmt`, `filename`, `bank_code` — must match what the surrounding code already uses. Read 30 lines of context to confirm before pasting; substitute names if they differ.

(If `imported_nr_filenames` is not yet visible in this scope, look up where the existing `_tracking = email_storage.get_all_statement_tracking_data()` call happens earlier in the function — that returns a dict that includes `imported_nr_filenames` already used elsewhere. Hoist or thread it into this loop so the new computation can see it.)

- [ ] **Step 5: Smoke-test the import**

```bash
cd /Users/maccb/llmragsql && source venv/bin/activate && python -c "from apps.bank_reconcile.api.routes import router; print('ok')"
```

Expected: prints `ok`.

- [ ] **Step 6: Run the full test suite**

```bash
source venv/bin/activate && pytest tests/ -q
```

Expected: every test passes.

- [ ] **Step 7: Commit**

```bash
git add apps/bank_reconcile/api/routes.py
git commit -m "feat(scan): surface state + deferred_count per statement (Opera SE)"
```

---

## Task 4: Relax the next-statement gate (Opera SE)

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py`

Today the scan logic gates a statement to `ready` only when the previous statement is fully reconciled. After this work, a previous statement in `imported` state also qualifies.

- [ ] **Step 1: Locate the chain check**

In `scan_all_banks_for_statements`, find the section that computes whether each statement's opening balance matches the prior statement's closing (or the bank's `nk_recbal`). Common patterns to search for: `reconciled_closing_balances`, `chain_complete`, `closing balance match`. There are several places this happens; the one we want is where statements are marked `ready` vs `pending`.

A typical block looks like:

```python
                if matched_bank_code:
                    bank_rec_opens = reconciled_opening_balances.get(matched_bank_code, set())
                    chain_complete = closing is not None and round(closing, 2) in bank_rec_opens
                    if chain_complete:
                        stmt_entry['category'] = 'already_processed'
                        stmt_entry['status'] = 'already_processed'
                    else:
                        stmt_entry['status'] = 'ready'
```

There may be multiple such sites (cache HIT path, cache MISS path, folder path) — the change applies to each.

- [ ] **Step 2: Build the "imported-also-counts-as-reconciled" balance set**

Just below where `reconciled_opening_balances` and `reconciled_closing_balances` are loaded from the tracking dict (early in the function), add a virtual extension for imported-but-not-reconciled statements:

```python
        # Sequential gating: a statement that is imported but has deferred rows
        # advances the chain virtually — its closing balance is treated as a
        # reconciled opener for the NEXT statement, even though Opera's nk_recbal
        # hasn't moved yet. This unblocks operators who are waiting on a third
        # party to resolve a deferred row in the prior statement.
        imported_pending_closings: dict[str, set[float]] = {}
        try:
            for fn, info in cached_stmt_info.items():
                if info.get('bank_code') == 'DEDUP':
                    continue
                bcode = info.get('bank_code')
                if not bcode:
                    continue
                # imported_nr_filenames contains files imported but not yet reconciled
                if fn in imported_nr_filenames:
                    closing = info.get('closing_balance')
                    if closing is not None:
                        imported_pending_closings.setdefault(bcode, set()).add(
                            round(float(closing), 2)
                        )
        except Exception as e:
            logger.warning("Could not build imported_pending_closings: %s", e)
```

(`cached_stmt_info` and `imported_nr_filenames` already come out of `get_all_statement_tracking_data()`. Verify the variable names against the existing code.)

- [ ] **Step 3: Treat imported-pending closings as eligible openers**

For every chain check that compares a statement's opening balance against a known set of reconciled openers (or against `nk_recbal`), extend the comparison to also accept openers that match an imported-pending closing.

A clean way to do this is one helper near the top of the function:

```python
        def _opening_unblocks_chain(bank_code_local: str, opening: Optional[float]) -> bool:
            """True if this opening balance matches a reconciled OR imported-pending closing,
            i.e. the prior statement is in 'reconciled' or 'imported' state."""
            if opening is None or not bank_code_local:
                return False
            target = round(float(opening), 2)
            # Reconciled prior — the existing rule.
            if target == round(float(all_banks.get(bank_code_local, {}).get('reconciled_balance') or 0), 2):
                return True
            if target in reconciled_opening_balances.get(bank_code_local, set()):
                return True
            # Imported-pending prior — new rule.
            if target in imported_pending_closings.get(bank_code_local, set()):
                return True
            return False
```

Then every site that previously did `if opening matches reconciled balance` becomes `if _opening_unblocks_chain(matched_bank_code, opening)`. Search for the existing comparisons (typically `round(opening_balance, 2) == round(reconciled_balance, 2)` or similar) and replace each.

If there's a `validate_statement` call elsewhere that does the same check, leave it for now — its scope is the per-bank statement reconciliation flow, not the scan-all-banks classification. We're only relaxing the *display* gate here.

- [ ] **Step 4: Smoke-test the import**

```bash
cd /Users/maccb/llmragsql && source venv/bin/activate && python -c "from apps.bank_reconcile.api.routes import router; print('ok')"
```

Expected: prints `ok`.

- [ ] **Step 5: Run the full suite**

```bash
source venv/bin/activate && pytest tests/ -q
```

Expected: every test passes.

- [ ] **Step 6: Commit**

```bash
git add apps/bank_reconcile/api/routes.py
git commit -m "feat(scan): next statement openable when prior is imported-with-deferred (Opera SE)"
```

---

## Task 5: Mirror in Opera 3 scan endpoint

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py` — `opera3_scan_emails_for_bank_statements` around line 11061.

The Opera 3 scan mirror has the same structure. Apply Tasks 3 + 4 patterns inside it.

- [ ] **Step 1: Locate the Opera 3 scan endpoint**

Find `@router.get("/api/opera3/bank-import/scan-emails")` and the function below it. Read the structure — it builds `all_banks` and per-statement entries similarly.

- [ ] **Step 2: Add `state` + `deferred_count` per statement**

Inside the per-statement assembly, mirror Task 3 Step 4. The same `derive_statement_state` and `DeferredTransactionsDB` helpers apply — they're not data-source-specific.

- [ ] **Step 3: Add the imported-pending closings + chain unblock**

Mirror Task 4 — build the `imported_pending_closings` dict and use the `_opening_unblocks_chain` helper at every chain-check site within this Opera 3 function.

- [ ] **Step 4: Smoke-test the import**

```bash
cd /Users/maccb/llmragsql && source venv/bin/activate && python -c "from apps.bank_reconcile.api.routes import router; print('ok')"
```

Expected: prints `ok`.

- [ ] **Step 5: Run the full suite**

```bash
source venv/bin/activate && pytest tests/ -q
```

Expected: every test passes.

- [ ] **Step 6: Commit**

```bash
git add apps/bank_reconcile/api/routes.py
git commit -m "feat(scan): mirror sequential gating + state+deferred_count (Opera 3)"
```

---

## Task 6: Frontend — render new state, pill, summary, gate Process button

**Files:**
- Modify: `frontend/src/pages/BankStatementHub.tsx`

- [ ] **Step 1: Extend `StatementEntry` type**

Find the `StatementEntry` interface near the top of the file (the one with `email_id`, `filename`, `status`, `extraction_status`, etc.). Add two new optional fields:

```tsx
  state?: 'ready' | 'in_progress' | 'imported' | 'reconciled' | 'pending_extraction' | 'sequence_gap' | 'already_processed';
  deferred_count?: number;
```

- [ ] **Step 2: Render the amber "Imported · N deferred" pill on the statement row**

Find the per-statement rendering block (where badges like "Ready", "Pending", "Reconciled" are rendered). The exact location is around the `StatementRow` component (search for `'ready'` or `'imported'` in JSX). Add a new conditional:

```tsx
{stmt.state === 'imported' && (stmt.deferred_count ?? 0) > 0 && (
  <span className="px-2 py-0.5 text-xs font-medium bg-amber-100 text-amber-800 rounded-full inline-flex items-center gap-1">
    <Clock className="h-3 w-3" />
    Imported · {stmt.deferred_count} deferred
  </span>
)}
```

`Clock` is already imported in this file (used by the deferred Pending pill from earlier work). If it isn't, add it to the lucide-react imports.

Place this badge near the existing status badges so it sits with them.

- [ ] **Step 3: Per-bank summary line**

Find the `BankCard` component (search for `function BankCard`). Inside its rendering, near where the bank header / counts already appear, add a derived value and the line below:

```tsx
  // Per-bank summary of outstanding work
  const importedCount = bank.statements.filter(s => s.state === 'imported').length;
  const totalDeferred = bank.statements.reduce(
    (acc, s) => acc + ((s.state === 'imported') ? (s.deferred_count ?? 0) : 0),
    0,
  );
```

Render below the bank header:

```tsx
{importedCount > 0 && (
  <div className="px-4 py-2 text-sm text-amber-700 bg-amber-50 border-t border-amber-100">
    {importedCount} statement{importedCount !== 1 ? 's' : ''} imported with deferred items, {totalDeferred} transaction{totalDeferred !== 1 ? 's' : ''} awaiting decision
  </div>
)}
```

Put it just above the statement table inside the `BankCard`. If a similar amber banner already exists from earlier work (the per-bank `extraction_status === 'incomplete'` banner), this one sits below it and uses different wording.

- [ ] **Step 4: Process-button gate uses `state`**

Find the existing Process button (search for `onClick={() => onProcess(stmt)}` or similar). The button is currently disabled based on `stmt.status` or other flags. Update the disabled rule so it considers the new `state` field — Process is allowed when the **previous** statement in the bank's list is in `state === 'imported'` or `state === 'reconciled'`. Today the equivalent rule probably uses `is_reconciled`.

The existing code likely uses `firstReadyIdx` to find the next statement to process and shows a "Next" badge on that statement. Update the search:

```tsx
const firstReadyIdx = bank.statements.findIndex(s => s.state === 'ready');
```

(If the existing search uses `s.status === 'ready'`, change it to read `s.state === 'ready'` — `state` is now the canonical field.) The Process button on the row at `firstReadyIdx` is enabled; on others it's disabled.

- [ ] **Step 5: TypeScript compile check**

```bash
cd /Users/maccb/llmragsql/frontend && npx tsc --noEmit
```

Expected: zero errors.

- [ ] **Step 6: Manual visual check**

The Vite dev server is running at http://localhost:5173/ with HMR. After saving:

1. Hard-refresh the browser.
2. Open the Bank Statement Hub.
3. Verify nothing visually changed for banks/statements that are not in the new `imported` state — existing badges and counts remain unchanged.
4. If you have a statement in `imported` state (deferred row pending), the new amber "Imported · N deferred" pill should appear on its row, and the per-bank summary line should appear above the bank's table.

(Full end-to-end verification on live data is part of Task 8.)

- [ ] **Step 7: Commit**

```bash
git add frontend/src/pages/BankStatementHub.tsx
git commit -m "feat(bank-hub-ui): render state pill + per-bank summary + state-based Process gate"
```

---

## Task 7: Knowledge base + manual updates

**Files:**
- Modify: `apps/core/docs/opera_knowledge_base.md`
- Modify: `marketing/manuals/manual-bank-reconciliation.md`

- [ ] **Step 1: Add KB entry**

Find an appropriate sub-section in `apps/core/docs/opera_knowledge_base.md` — most likely near the existing "Defer Transaction Action" sub-section. Insert:

```markdown
### Sequential Statement Gating (Bank Statement Reconciliation)

Per bank, every statement carries a derived state — one of `pending_extraction`, `ready`, `in_progress`, `imported`, `reconciled` — computed by `derive_statement_state()` in `sql_rag/deferred_transactions_db.py` from existing flags (`bank_statement_imports.is_reconciled`, presence in `deferred_transactions.db`, drafts in email storage).

**Key rule (changed):** the *next* statement becomes openable for processing when the prior statement is in state `imported` **or** `reconciled` (previously: `reconciled` only). The new `imported` state means "every row decided, at least one row deferred — Stage 4 still waiting on the deferred-row resolution".

The chain integrity for **reconciliation** is preserved by the existing opening-balance check (statement's opening must equal Opera's `nk_recbal`). Since `nk_recbal` only advances on actual Stage 4 completion, the next statement still cannot reconcile until the prior one does. Imports keep flowing through, customer/supplier ledgers stay current, but reconciliations strictly serialise.

UI: rows in `imported` state show an amber "Imported · N deferred" pill; per-bank summary line above the table aggregates the outstanding count.

Files: `sql_rag/deferred_transactions_db.py` (state derivation), `apps/bank_reconcile/api/routes.py` (`scan-all-banks` Opera SE + Opera 3 mirror), `frontend/src/pages/BankStatementHub.tsx`.
```

- [ ] **Step 2: Add manual entry**

Find the section in `marketing/manuals/manual-bank-reconciliation.md` that explains the Defer flow (added earlier today). Add:

```markdown
**Working through statements while waiting on a deferred row:** Once you have decided every row in the current statement (imported, ignored, or deferred), you can move on to the next statement straight away — no need to wait for the deferred row to be resolved. Stages 1–3 (extract, match, import) run on the next statement and Opera's customer/supplier ledgers stay up to date. The only thing that has to wait is the *reconciliation* of the next statement; that's blocked until the prior one fully reconciles, which happens automatically once you resolve the deferred row.
```

Update the "Last updated" date at the bottom to today (2026-04-30).

- [ ] **Step 3: Commit**

```bash
git add apps/core/docs/opera_knowledge_base.md marketing/manuals/manual-bank-reconciliation.md
git commit -m "docs: document sequential statement gating + imported state"
```

---

## Task 8: End-to-end manual verification

This task is verification only — no commits.

- [ ] **Step 1: Identify the live blocking statement on Cloudsis**

Log into the Cloudsis installation. Find the statement that's currently held up because of the colleague's pending answer.

- [ ] **Step 2: Defer the stuck row**

Use the Defer button shipped earlier today. Confirm the statement now shows the amber "Imported · N deferred" pill on the Bank Statement Hub.

- [ ] **Step 3: Verify the next statement is openable**

The next statement (chronologically next in the bank's list) should now show as `state === 'ready'` with its Process button enabled. If it is in `pending_extraction` for a different reason (e.g. AI quota), that's not this feature's concern.

- [ ] **Step 4: Process the next statement**

Open the next statement. Run through Stages 1–3 — match, assign, import. Confirm the import succeeds and Opera's customer/supplier balances are updated (check via Opera Customer / Supplier Lookup or a SQL query of `sname.sn_currbal`, `pname.pn_currbal`).

- [ ] **Step 5: Confirm reconciliation is gated**

Try to reconcile the next statement. The opening-balance check should refuse — the prior statement is still in `imported` state and Opera's `nk_recbal` hasn't moved. Confirm the error message is clear (the existing partial-reconciliation dialog or balance-mismatch error).

- [ ] **Step 6: Resolve the deferred row**

When the colleague answers, return to the original statement, edit the deferred row's Type / Account, and import it. Confirm the original statement now reaches `reconciled` state.

- [ ] **Step 7: Confirm chain unblock**

After Step 6, the next statement should now reconcile cleanly (its opening balance matches the new `nk_recbal`). Run the Reconcile flow, confirm it succeeds.

- [ ] **Step 8: Confirm Opera 3 parity**

If a Cloudsis Opera 3 environment is reachable, repeat the entire flow there. Behaviour must be identical.

If any step fails, return to the relevant earlier task and fix.

---

## Self-Review

| Spec section | Covered by |
|---|---|
| Goal: process next statement while current is partial | Tasks 3, 4, 5 (backend), Task 6 (frontend) |
| Statement state machine | Tasks 1, 2 (helpers) + Task 3 (consumption) |
| Data model: derive state from existing data | Task 2 |
| API: surface `state` + `deferred_count` | Tasks 3, 5 |
| API: relax next-statement gate | Tasks 4, 5 |
| UX: amber pill for imported state | Task 6 |
| UX: per-bank summary line | Task 6 |
| UX: Process button gate uses `state` | Task 6 |
| Both Opera SE and Opera 3 paths | Tasks 4 (SE), 5 (Opera 3) |
| Knowledge base + manual updates | Task 7 |
| End-to-end verification on live Cloudsis data | Task 8 |

No placeholders. Type and field names are consistent across tasks (`derive_statement_state`, `DeferredTransactionsDB.count_for_statement`, `state`, `deferred_count`, `imported_pending_closings`, `_opening_unblocks_chain`).

---

**Plan complete.**
