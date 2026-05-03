# Matcher Period-Bound Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the bank-statement matcher from pulling aentries from outside the statement period; stop the complete-reconciliation handler from applying `ae_tmpstat` reservations to entries outside the period; provide a safe utility to clear orphan tmpstat markers from prior accidents.

**Architecture:** Three changes in series. (1) `match_statement_to_cashbook` gains required `period_start`/`period_end` parameters and restricts its candidate aentry pool to those bounds (with a small grace window for end-of-period postings). (2) The complete-reconciliation handler at `/api/bank-reconciliation/complete` validates every entry being reconciled is in-period before any tmpstat write; refuses with a structured error otherwise. (3) A new endpoint `/api/reconcile/bank/{bank_code}/clear-orphan-tmpstat` lets the operator clear dangling reservations safely.

**Tech Stack:** Python 3.9, FastAPI, pyodbc, pytest. No new external deps.

**Source spec:** `docs/superpowers/specs/2026-05-03-matcher-period-bound-design.md`

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `sql_rag/opera_sql_import.py` | **modify** | `match_statement_to_cashbook` accepts period bounds and applies them to the candidate query |
| `sql_rag/opera3_foxpro_import.py` | **modify** | Opera 3 mirror of the same matcher behaviour |
| `apps/bank_reconcile/api/routes.py` | **modify** | (a) `match-statement` endpoint passes period bounds; (b) `complete-reconciliation` validates entries; (c) new `clear-orphan-tmpstat` endpoint |
| `tests/test_matcher_period_bound.py` | **create** | Matcher candidate-restriction tests with fixture connector |
| `tests/test_complete_reconciliation_period_validation.py` | **create** | Validation tests with fixture |
| `tests/test_clear_orphan_tmpstat.py` | **create** | Clear-utility tests |
| `apps/core/docs/opera_knowledge_base.md` | **modify** | Document the period-bound rule + tmpstat semantics |
| `~/opera-knowledge-ref/.../business-rules/matcher-period-bound.md` | **create** | Central KB |

---

## Task 1: Matcher accepts period bounds (signature change + filtered query)

**Files:**
- Modify: `sql_rag/opera_sql_import.py::match_statement_to_cashbook`
- Test: `tests/test_matcher_period_bound.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_matcher_period_bound.py
"""Tests for the matcher's period-bound candidate restriction.

Critical regression coverage: today's bug paired a 2026-02-28 aentry
with a 2026-03-01..2026-03-31 statement using a 45-day date tolerance.
The fix is to bound the candidate pool by the statement period itself
(plus a small grace window), rejecting out-of-period candidates
deterministically.
"""
from __future__ import annotations

import inspect
from datetime import date

import pytest


def test_match_statement_to_cashbook_signature_has_period_bounds():
    """Signature pins the new keyword arguments — explicit, not magic."""
    from sql_rag.opera_sql_import import OperaSQLImport
    sig = inspect.signature(OperaSQLImport.match_statement_to_cashbook)
    assert 'period_start' in sig.parameters
    assert 'period_end' in sig.parameters
    assert 'period_grace_days' in sig.parameters


def test_match_query_restricts_aentry_pool_by_period_bounds():
    """The SQL emitted for the unreconciled-aentry candidate pool must
    include `ae_lstdate BETWEEN '<period_start - grace>' AND '<period_end + grace>'`.
    """
    captured: list[str] = []

    class _Spy:
        def execute_query(self, q):
            captured.append(q)
            class _DF:
                empty = True
                def to_dict(self, *a, **k): return []
                def iterrows(self): return iter([])
            return _DF()

    from sql_rag.opera_sql_import import OperaSQLImport
    op = OperaSQLImport(_Spy())
    op.match_statement_to_cashbook(
        bank_account="BB005",
        statement_transactions=[],
        period_start=date(2026, 3, 1),
        period_end=date(2026, 3, 31),
        period_grace_days=7,
    )
    # At least one query targets aentry on this bank with the
    # period-bounded ae_lstdate filter.
    aentry_queries = [q for q in captured
                      if "FROM aentry" in q and "ae_acnt = 'BB005'" in q]
    assert aentry_queries, f"no aentry candidate query emitted: {captured}"
    bounded = [q for q in aentry_queries if "ae_lstdate BETWEEN" in q]
    assert bounded, (
        "aentry candidate query must include "
        "`ae_lstdate BETWEEN <period_start-grace> AND <period_end+grace>`"
    )
    # Verify exact bounds
    q = bounded[0]
    assert "'2026-02-22'" in q  # period_start - 7 days
    assert "'2026-04-07'" in q  # period_end + 7 days


def test_match_falls_back_with_warning_when_period_bounds_missing():
    """Backwards-compat: if period bounds aren't passed, fall back to
    the old date-tolerance behaviour but log a warning.
    """
    captured_logs: list[str] = []

    import logging
    handler = logging.StreamHandler()
    class _Capture(logging.Handler):
        def emit(self, record):
            captured_logs.append(record.getMessage())
    test_handler = _Capture()
    logger = logging.getLogger("sql_rag.opera_sql_import")
    logger.addHandler(test_handler)

    class _Stub:
        def execute_query(self, q):
            class _DF:
                empty = True
                def to_dict(self, *a, **k): return []
                def iterrows(self): return iter([])
            return _DF()

    from sql_rag.opera_sql_import import OperaSQLImport
    try:
        op = OperaSQLImport(_Stub())
        op.match_statement_to_cashbook(
            bank_account="BB005",
            statement_transactions=[],
            period_start=None,
            period_end=None,
        )
    finally:
        logger.removeHandler(test_handler)

    assert any("period bounds not provided" in m.lower() for m in captured_logs), \
        f"expected fallback warning; got logs: {captured_logs}"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
source venv/bin/activate && python -m pytest tests/test_matcher_period_bound.py -v
```

Expected: 3 tests FAIL — `match_statement_to_cashbook` doesn't accept the new parameters yet.

- [ ] **Step 3: Modify `match_statement_to_cashbook`**

In `sql_rag/opera_sql_import.py`, find `def match_statement_to_cashbook` (around line 8398).

Update the signature:

```python
    def match_statement_to_cashbook(
        self,
        bank_account: str,
        statement_transactions: List[Dict[str, Any]],
        date_tolerance_days: int = 14,
        period_start: Optional[date] = None,
        period_end: Optional[date] = None,
        period_grace_days: int = 7,
    ) -> Dict[str, Any]:
```

Replace the unreconciled-aentry candidate query (currently around lines 8441-8448) with a period-bounded version. Find this block:

```python
            query = f"""
                SELECT ae_entry, ae_value/100.0 as amount_pounds, ae_lstdate,
                       ae_entref, ae_comment, ae_cbtype, ae_complet
                FROM aentry WITH (NOLOCK)
                WHERE ae_acnt = '{bank_account}'
                  AND ae_reclnum = 0
                ORDER BY ae_lstdate, ae_entry
            """
```

Replace with period-bound logic:

```python
            # Period-bound the candidate pool (matcher-period-bound spec).
            # Statements never legitimately match aentries outside their
            # own period — the 7-day grace window covers month-end
            # postings dated a few days late.
            if period_start is not None and period_end is not None:
                from datetime import timedelta
                window_start = period_start - timedelta(days=period_grace_days)
                window_end = period_end + timedelta(days=period_grace_days)
                period_filter = (
                    f"AND ae_lstdate BETWEEN "
                    f"'{window_start.isoformat()}' AND '{window_end.isoformat()}'"
                )
            else:
                logger.warning(
                    "match_statement_to_cashbook: period bounds not provided "
                    "for bank %s — falling back to unbounded candidate pool. "
                    "Pass period_start/period_end to enforce in-period matching.",
                    bank_account,
                )
                period_filter = ""

            query = f"""
                SELECT ae_entry, ae_value/100.0 as amount_pounds, ae_lstdate,
                       ae_entref, ae_comment, ae_cbtype, ae_complet
                FROM aentry WITH (NOLOCK)
                WHERE ae_acnt = '{bank_account}'
                  AND ae_reclnum = 0
                  {period_filter}
                ORDER BY ae_lstdate, ae_entry
            """
```

Apply the same `period_filter` to the second-pass already-reconciled query lower in the function (the `SELECT ... FROM aentry WHERE ... AND ae_reclnum > 0` query around line 8665) — same bounds, same fallback behaviour.

- [ ] **Step 4: Run tests to verify they pass**

```bash
source venv/bin/activate && python -m pytest tests/test_matcher_period_bound.py -v
```

Expected: 3 tests PASS.

```bash
source venv/bin/activate && python -m pytest tests/ -x -q
```

Expected: full suite still passes.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/opera_sql_import.py tests/test_matcher_period_bound.py
git commit -m "feat(matcher): period-bound candidate pool for match_statement_to_cashbook

Matcher's candidate-aentry pool is now restricted to ae_lstdate within
[period_start - grace, period_end + grace]. Default grace = 7 days.
Caller passes period_start/period_end (both required to enforce);
without them, falls back to the old unbounded behaviour with a
warning, so existing callers don't break.

The 7-day grace window covers month-end postings dated a few days
late. The 45-day tolerance that previously applied to candidate
selection is unrelated — it remains for fuzzy-suggested matches but
the candidate pool is now period-strict.

Regression: today's Cloudsis case paired a 2026-02-28 aentry against
a March 5202 statement (45 days). With period_start=2026-03-01,
period_end=2026-03-31, grace=7, the aentry from 02-28 still matches
(02-22 boundary), but a 02-15 aentry would NOT — and a 04-15 aentry
would not match a March statement either.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: API endpoint passes period bounds to the matcher

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py::match_statement_to_cashbook` endpoint

- [ ] **Step 1: Write the test**

Append to `tests/test_matcher_period_bound.py`:

```python
def test_match_statement_endpoint_passes_period_bounds():
    """The /api/bank-reconciliation/match-statement endpoint must pass
    period_start/period_end through to the matcher when the request
    body includes them. Source-level test (we don't spin up FastAPI).
    """
    from pathlib import Path
    routes = Path(__file__).resolve().parent.parent / "apps" / "bank_reconcile" / "api" / "routes.py"
    src = routes.read_text(encoding='utf-8')

    # Find the match_statement_to_cashbook endpoint body
    start = src.find("@router.post(\"/api/bank-reconciliation/match-statement\"")
    assert start != -1, "match-statement endpoint not found"
    end = src.find("@router.", start + 10)
    body = src[start:end] if end != -1 else src[start:start + 6000]

    # The endpoint must extract period bounds from the request body
    # and pass them to the matcher
    assert "period_start" in body, \
        "match-statement endpoint must read period_start from request body"
    assert "period_end" in body, \
        "match-statement endpoint must read period_end from request body"
    assert ("period_start=period_start" in body
            or "period_start=ps" in body
            or "period_start=parsed_period_start" in body
            or "period_start=" in body and "period_end=" in body), \
        "endpoint must pass the period bounds to opera_import.match_statement_to_cashbook(...)"
```

- [ ] **Step 2: Run test, verify failure**

```bash
source venv/bin/activate && python -m pytest tests/test_matcher_period_bound.py::test_match_statement_endpoint_passes_period_bounds -v
```

Expected: FAIL — the endpoint doesn't yet read or pass period bounds.

- [ ] **Step 3: Update the endpoint**

In `apps/bank_reconcile/api/routes.py`, find `async def match_statement_to_cashbook` (around line 10287). Modify it to accept and forward period bounds.

Add to the request body parsing — after `statement_transactions = request_body['statement_transactions']`:

```python
        # Extract period bounds from request (or from import_id if available).
        # Required for the period-bound matcher restriction. If absent,
        # the matcher logs a warning and falls back to unbounded behaviour
        # — preserving backwards compat.
        period_start = request_body.get('period_start') if request_body else None
        period_end = request_body.get('period_end') if request_body else None
        if (period_start is None or period_end is None) and import_id and email_storage:
            with email_storage._get_connection() as _c:
                _cur = _c.cursor()
                _cur.execute(
                    "SELECT period_start, period_end FROM bank_statement_imports WHERE id = ?",
                    (import_id,),
                )
                row = _cur.fetchone()
                if row is not None:
                    period_start = period_start or row['period_start']
                    period_end = period_end or row['period_end']

        from datetime import date as _date_type, datetime as _dt
        def _to_date(v):
            if v is None:
                return None
            if isinstance(v, _date_type) and not isinstance(v, _dt):
                return v
            if isinstance(v, _dt):
                return v.date()
            if isinstance(v, str):
                try:
                    return _dt.fromisoformat(v.replace('Z', '+00:00')).date()
                except ValueError:
                    try:
                        return _date_type.fromisoformat(v[:10])
                    except ValueError:
                        return None
            return None

        parsed_period_start = _to_date(period_start)
        parsed_period_end = _to_date(period_end)
```

Then update the matcher call (currently `result = opera_import.match_statement_to_cashbook(bank_account=bank_code, statement_transactions=statement_transactions, date_tolerance_days=date_tolerance_days)`):

```python
        result = opera_import.match_statement_to_cashbook(
            bank_account=bank_code,
            statement_transactions=statement_transactions,
            date_tolerance_days=date_tolerance_days,
            period_start=parsed_period_start,
            period_end=parsed_period_end,
        )
```

- [ ] **Step 4: Run tests**

```bash
source venv/bin/activate && python -m pytest tests/ -x -q
```

Expected: full suite passes.

- [ ] **Step 5: Commit**

```bash
git add apps/bank_reconcile/api/routes.py tests/test_matcher_period_bound.py
git commit -m "feat(api): match-statement endpoint passes period bounds to matcher

Endpoint extracts period_start/period_end from the request body, or
falls back to bank_statement_imports.period_start/period_end via the
import_id when one is provided. Date strings/datetimes are normalised
to date.

Source-level test pins the change so a future refactor can't drop the
forwarding silently.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Frontend passes period bounds when calling the matcher

**Files:**
- Modify: `frontend/src/pages/BankStatementReconcile.tsx::runMatchingFromUnreconciled`

- [ ] **Step 1: Find and update the fetch call**

Locate the fetch call that currently posts to `/api/bank-reconciliation/match-statement` (around line 1692 in `frontend/src/pages/BankStatementReconcile.tsx`):

```typescript
      const response = await authFetch(
        matchUrl,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ statement_transactions: statementTransactions })
        }
      );
```

Change the body to include the statement period:

```typescript
      const response = await authFetch(
        matchUrl,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            statement_transactions: statementTransactions,
            // Period bounds enforce in-period matching — out-of-period
            // aentries cannot pair with this statement, preventing the
            // tmpstat-on-wrong-row class of bug.
            period_start: importedStatementData?.period_start ?? null,
            period_end: importedStatementData?.period_end ?? null,
          }),
        }
      );
```

If `importedStatementData?.period_start`/`period_end` aren't already typed on the data structure, add them. They live on the response from `preview-from-pdf`/`load-import` which already returns period info.

- [ ] **Step 2: Type-check the frontend**

```bash
cd /Users/maccb/llmragsql/frontend && npx tsc --noEmit 2>&1 | grep -E "BankStatementReconcile.tsx" | head -5
```

Expected: no errors specific to this file.

- [ ] **Step 3: Commit**

```bash
git -C /Users/maccb/llmragsql add frontend/src/pages/BankStatementReconcile.tsx
git -C /Users/maccb/llmragsql commit -m "feat(reconcile-ui): pass period bounds to match-statement endpoint

The reconcile view now forwards the statement's period_start/period_end
when running matching, so the backend matcher can restrict its
candidate aentry pool to in-period rows. Eliminates the class of bug
where running matching against a March statement could pair entries
from February or April.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Complete-reconciliation handler validates entries are in-period

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py::complete_reconciliation` endpoint
- Test: `tests/test_complete_reconciliation_period_validation.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_complete_reconciliation_period_validation.py
"""Tests for the complete-reconciliation handler's period validation.

Today's Cloudsis incident: a March statement with a 45-day-old February
aentry pulled in by the matcher reached the complete-reconciliation
handler, which silently accepted it and applied ae_tmpstat on the
out-of-period row. This validation step refuses such input.
"""
from __future__ import annotations

from pathlib import Path


def test_complete_reconciliation_validates_period_in_source():
    """Source-level: the endpoint contains a validation block that
    refuses entries outside the statement period before any tmpstat
    write. Locks the rule in via grep so a refactor can't lose it.
    """
    routes = Path(__file__).resolve().parent.parent / "apps" / "bank_reconcile" / "api" / "routes.py"
    src = routes.read_text(encoding='utf-8')

    # Find the complete_reconciliation function
    start = src.find("def complete_reconciliation(")
    assert start != -1, "complete_reconciliation function not found"
    # Approximate end: next "def " at outer indentation
    end = src.find("\nasync def ", start + 10)
    if end == -1:
        end = src.find("\ndef ", start + 10)
    body = src[start:end] if end != -1 else src[start:start + 12000]

    # The validation block must:
    # 1. Compare entry dates to period bounds.
    # 2. Refuse with structured error before applying tmpstat.
    assert "out_of_period" in body or "out-of-period" in body, \
        "complete_reconciliation must collect out-of-period entries"
    assert "period_start" in body and "period_end" in body, \
        "complete_reconciliation must reference period bounds in validation"
    assert "Entries fall outside the statement period" in body \
        or "outside the statement period" in body, \
        "validation must surface a clear error message"
```

- [ ] **Step 2: Run test, verify failure**

```bash
source venv/bin/activate && python -m pytest tests/test_complete_reconciliation_period_validation.py -v
```

Expected: FAIL — validation logic doesn't exist yet.

- [ ] **Step 3: Add validation to the endpoint**

Locate `complete_reconciliation` in `apps/bank_reconcile/api/routes.py` (search for `def complete_reconciliation`). Find the early section that loads `matched_entries` from the request body. Just before any reconciliation/tmpstat write, add this validation block:

```python
        # Period-bound validation (matcher-period-bound spec).
        # Entries paired by the matcher must fall within the statement's
        # own period (with the same grace window the matcher uses) before
        # we apply ae_tmpstat or commit reconcile state. Today's bug
        # silently set tmpstat on a Feb entry because the matcher's old
        # 45-day tolerance pulled it in against a March statement.
        from datetime import date as _date_type, datetime as _dt, timedelta as _td

        def _to_date(v):
            if v is None:
                return None
            if isinstance(v, _date_type) and not isinstance(v, _dt):
                return v
            if isinstance(v, _dt):
                return v.date()
            if isinstance(v, str):
                try:
                    return _dt.fromisoformat(v.replace('Z', '+00:00')).date()
                except ValueError:
                    try:
                        return _date_type.fromisoformat(v[:10])
                    except ValueError:
                        return None
            return None

        # Period from request body (preferred) or from import_id lookup
        period_start = _to_date(request_body.get('period_start') if request_body else None)
        period_end = _to_date(request_body.get('period_end') if request_body else None)
        if (period_start is None or period_end is None) and import_id and email_storage:
            with email_storage._get_connection() as _c:
                _cur = _c.cursor()
                _cur.execute(
                    "SELECT period_start, period_end FROM bank_statement_imports WHERE id = ?",
                    (import_id,),
                )
                _row = _cur.fetchone()
                if _row is not None:
                    period_start = period_start or _to_date(_row['period_start'])
                    period_end = period_end or _to_date(_row['period_end'])

        period_grace_days = 7
        if period_start is not None and period_end is not None and matched_entries:
            grace_start = period_start - _td(days=period_grace_days)
            grace_end = period_end + _td(days=period_grace_days)
            entry_numbers = [
                str(m.get('entry_number', '')).strip()
                for m in matched_entries
                if m.get('entry_number')
            ]
            if entry_numbers:
                quoted = ','.join(f"'{e.replace(chr(39), chr(39)+chr(39))}'" for e in entry_numbers)
                df = sql_connector.execute_query(f"""
                    SELECT ae_entry, ae_lstdate
                    FROM aentry WITH (NOLOCK)
                    WHERE ae_acnt = '{bank_code}'
                    AND RTRIM(ae_entry) IN ({quoted})
                """)
                date_by_entry = {}
                if df is not None and not df.empty:
                    for _, r in df.iterrows():
                        ent = str(r.get('ae_entry', '')).strip()
                        d = _to_date(r.get('ae_lstdate'))
                        date_by_entry[ent] = d

                out_of_period = []
                for m in matched_entries:
                    ent = str(m.get('entry_number', '')).strip()
                    d = date_by_entry.get(ent)
                    if d is None:
                        continue  # entry not found — let the existing flow handle that
                    if d < grace_start or d > grace_end:
                        out_of_period.append({
                            'entry': ent,
                            'date': d.isoformat(),
                            'period_start': period_start.isoformat(),
                            'period_end': period_end.isoformat(),
                        })

                if out_of_period:
                    return {
                        "success": False,
                        "error": "Entries fall outside the statement period",
                        "out_of_period": out_of_period,
                    }
```

Place this immediately after `matched_entries = request_body.get('matched_entries', [])` and before any `acquire_import_lock(...)` / Opera write.

- [ ] **Step 4: Run tests**

```bash
source venv/bin/activate && python -m pytest tests/ -x -q
```

Expected: full suite passes.

- [ ] **Step 5: Commit**

```bash
git add apps/bank_reconcile/api/routes.py tests/test_complete_reconciliation_period_validation.py
git commit -m "feat(reconcile): refuse out-of-period entries before tmpstat write

complete_reconciliation now validates that every matched entry's
ae_lstdate falls within the statement period (same 7-day grace as
the matcher) BEFORE applying ae_tmpstat or any other Opera write.
On violation: returns 200 with success=false and a structured
out_of_period list. The frontend surfaces it as a modal.

Closes the second half of today's Cloudsis bug: matcher correctly
restricted but the handler still accepted whatever was sent. Now
both sides enforce the rule.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Frontend surfaces out-of-period error in a modal

**Files:**
- Modify: `frontend/src/pages/BankStatementReconcile.tsx`

- [ ] **Step 1: Locate the reconcile mutation**

Find `markReconciledMutation` (around line 1312) and the related `confirmReconcile` function that POSTs to `/api/bank-reconciliation/complete`. The current `onError`/`onSuccess` doesn't yet handle the structured `out_of_period` response.

- [ ] **Step 2: Update the response handling**

Modify the mutation to surface the new error. In the `mutationFn` after the response parse, before treating it as success:

```typescript
      const response = await apiClient.markEntriesReconciled(selectedBank, {
        entries,
        statement_number: parseInt(statementNumber) || (statusQuery.data?.last_stmt_no || 0) + 1,
        statement_date: statementDate,
        reconciliation_date: statementDate,
      });
      const data = response.data;

      // Period validation surface (matcher-period-bound spec): the API
      // returns success=false with an out_of_period list when the
      // matched entries straddle the statement period.
      if (data && (data as any).out_of_period && Array.isArray((data as any).out_of_period)) {
        const oop = (data as any).out_of_period as Array<{entry: string; date: string; period_start: string; period_end: string}>;
        const lines = oop.map(e => `  • ${e.entry} dated ${e.date}`).join('\n');
        const period = oop.length > 0 ? `${oop[0].period_start} to ${oop[0].period_end}` : '';
        const msg = `Cannot reconcile — these entries fall outside the statement period (${period}):\n\n${lines}\n\nEdit your selection to include only in-period entries, or extend the period if these legitimately belong.`;
        throw new Error(msg);
      }

      return data;
```

The existing error-handling UI will show this message. If there's a custom error pathway, route it there.

- [ ] **Step 3: Type-check**

```bash
cd /Users/maccb/llmragsql/frontend && npx tsc --noEmit 2>&1 | grep -E "BankStatementReconcile.tsx" | head -5
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git -C /Users/maccb/llmragsql add frontend/src/pages/BankStatementReconcile.tsx
git -C /Users/maccb/llmragsql commit -m "feat(reconcile-ui): surface out-of-period error from complete endpoint

When the API refuses a reconcile attempt because matched entries
straddle the statement period, the modal now lists each offending
entry's date and the period bounds. Operator gets a clear next step:
edit selection or fix period.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Clear-orphan-tmpstat endpoint and tests

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py` (new endpoint)
- Test: `tests/test_clear_orphan_tmpstat.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_clear_orphan_tmpstat.py
"""Tests for the orphan-tmpstat clear endpoint."""
from __future__ import annotations

from pathlib import Path


def test_clear_orphan_tmpstat_endpoint_exists_and_filters_correctly_in_source():
    """The new endpoint must:
      1. Be registered at the expected path.
      2. Filter aentries by ae_tmpstat > 0 AND ae_reclnum = 0.
      3. Use ROWLOCK on the UPDATE.
      4. Run a SELECT first to return the list to the user.
    """
    routes = Path(__file__).resolve().parent.parent / "apps" / "bank_reconcile" / "api" / "routes.py"
    src = routes.read_text(encoding='utf-8')

    # Endpoint registered
    assert "/api/reconcile/bank/{bank_code}/clear-orphan-tmpstat" in src, \
        "clear-orphan-tmpstat endpoint not registered"

    # Find the function
    start = src.find('@router.post("/api/reconcile/bank/{bank_code}/clear-orphan-tmpstat")')
    if start == -1:
        start = src.find('"/api/reconcile/bank/{bank_code}/clear-orphan-tmpstat"')
    assert start != -1
    end = src.find("\n@router.", start + 10)
    if end == -1:
        end = start + 6000
    body = src[start:end]

    assert "ae_tmpstat > 0" in body or "ae_tmpstat &gt; 0" in body, \
        "must filter ae_tmpstat > 0"
    assert "ae_reclnum = 0" in body or "ae_reclnum IS NULL OR ae_reclnum = 0" in body, \
        "must require ae_reclnum = 0 (orphan, not real reconcile)"
    assert "WITH (ROWLOCK)" in body or "ROWLOCK" in body, \
        "UPDATE must use ROWLOCK per project rules"
    assert "SELECT" in body and "UPDATE" in body, \
        "endpoint must SELECT (list) before UPDATE (clear)"


def test_clear_endpoint_writes_log_entry_per_clear():
    """The endpoint must log when it modifies Opera state."""
    routes = Path(__file__).resolve().parent.parent / "apps" / "bank_reconcile" / "api" / "routes.py"
    src = routes.read_text(encoding='utf-8')
    start = src.find('"/api/reconcile/bank/{bank_code}/clear-orphan-tmpstat"')
    body = src[start:start + 6000] if start != -1 else ""
    assert "logger.info" in body or "logger.warning" in body, \
        "clear endpoint must log when modifying Opera"
```

- [ ] **Step 2: Run, verify failure**

```bash
source venv/bin/activate && python -m pytest tests/test_clear_orphan_tmpstat.py -v
```

Expected: 2 tests FAIL.

- [ ] **Step 3: Add the endpoint**

Append to `apps/bank_reconcile/api/routes.py` (near the other reconcile endpoints):

```python
@router.get("/api/reconcile/bank/{bank_code}/orphan-tmpstat")
async def list_orphan_tmpstat(bank_code: str):
    """List aentries on this bank with dangling ae_tmpstat reservations
    that aren't part of a real reconciliation (ae_reclnum = 0). These
    are the residue of partial-reconcile attempts that didn't finalise
    — they block the entries from future reconciliations until cleared.

    Read-only: returns the list. Use the matching POST to clear.
    """
    if not sql_connector:
        return {"success": False, "error": "No database connection"}
    try:
        df = sql_connector.execute_query(f"""
            SELECT ae_entry, ae_lstdate, ae_value/100.0 AS value_pds,
                   ae_entref, ae_tmpstat, ae_statln
            FROM aentry WITH (NOLOCK)
            WHERE ae_acnt = '{bank_code}'
              AND ae_tmpstat > 0
              AND (ae_reclnum IS NULL OR ae_reclnum = 0)
            ORDER BY ae_lstdate, ae_entry
        """)
        rows = []
        if df is not None and not df.empty:
            for _, r in df.iterrows():
                rows.append({
                    'entry': str(r.get('ae_entry', '')).strip(),
                    'date': str(r.get('ae_lstdate', ''))[:10],
                    'value': float(r.get('value_pds', 0) or 0),
                    'reference': (r.get('ae_entref') or '').strip(),
                    'tmpstat': int(r.get('ae_tmpstat') or 0),
                    'statement_line': int(r.get('ae_statln') or 0),
                })
        return {"success": True, "count": len(rows), "entries": rows}
    except Exception as e:
        logger.error(f"list_orphan_tmpstat failed for {bank_code}: {e}")
        return {"success": False, "error": friendly_db_error(e) if 'friendly_db_error' in globals() else str(e)}


@router.post("/api/reconcile/bank/{bank_code}/clear-orphan-tmpstat")
async def clear_orphan_tmpstat(bank_code: str, request: Request):
    """Clear ae_tmpstat reservations on aentries that are not part of
    a real reconciliation (ae_reclnum = 0). Use the GET endpoint above
    to preview what will be cleared.

    Optional request body: {"entry_numbers": ["P10000...", ...]} to
    restrict to specific entries. Without it, clears ALL orphan tmpstats
    on the bank.

    SAFE: only touches ae_tmpstat (the temporary-status field), and
    only on entries with ae_reclnum = 0 (no committed reconcile data).
    """
    if not sql_connector:
        return {"success": False, "error": "No database connection"}

    body = {}
    try:
        body = await request.json()
    except Exception:
        body = {}
    entry_numbers = body.get('entry_numbers') if isinstance(body, dict) else None

    extra_filter = ""
    if entry_numbers:
        # Validate
        if not isinstance(entry_numbers, list) or any(not isinstance(e, str) for e in entry_numbers):
            return {"success": False, "error": "entry_numbers must be a list of strings"}
        quoted = ','.join(f"'{e.replace(chr(39), chr(39)+chr(39))}'" for e in entry_numbers)
        extra_filter = f" AND RTRIM(ae_entry) IN ({quoted})"

    try:
        # Preview the affected rows for the response
        df = sql_connector.execute_query(f"""
            SELECT ae_entry, ae_lstdate, ae_value/100.0 AS value_pds, ae_tmpstat
            FROM aentry WITH (NOLOCK)
            WHERE ae_acnt = '{bank_code}'
              AND ae_tmpstat > 0
              AND (ae_reclnum IS NULL OR ae_reclnum = 0)
              {extra_filter}
        """)
        affected = []
        if df is not None and not df.empty:
            for _, r in df.iterrows():
                affected.append({
                    'entry': str(r.get('ae_entry', '')).strip(),
                    'date': str(r.get('ae_lstdate', ''))[:10],
                    'value': float(r.get('value_pds', 0) or 0),
                    'previous_tmpstat': int(r.get('ae_tmpstat') or 0),
                })

        if not affected:
            return {"success": True, "cleared": 0, "entries": []}

        # Apply the clear in a short, locked update
        sql_connector.execute_non_query(f"""
            UPDATE aentry WITH (ROWLOCK)
            SET ae_tmpstat = 0
            WHERE ae_acnt = '{bank_code}'
              AND ae_tmpstat > 0
              AND (ae_reclnum IS NULL OR ae_reclnum = 0)
              {extra_filter}
        """)

        for a in affected:
            logger.info(
                f"clear_orphan_tmpstat: {bank_code} cleared {a['entry']} "
                f"({a['date']}, £{a['value']:.2f}, was tmpstat={a['previous_tmpstat']})"
            )

        return {"success": True, "cleared": len(affected), "entries": affected}
    except Exception as e:
        logger.error(f"clear_orphan_tmpstat failed for {bank_code}: {e}")
        return {"success": False, "error": friendly_db_error(e) if 'friendly_db_error' in globals() else str(e)}
```

- [ ] **Step 4: Run tests**

```bash
source venv/bin/activate && python -m pytest tests/ -x -q
```

Expected: full suite passes.

- [ ] **Step 5: Commit**

```bash
git add apps/bank_reconcile/api/routes.py tests/test_clear_orphan_tmpstat.py
git commit -m "feat(reconcile): clear-orphan-tmpstat endpoint for stranded reservations

Two new endpoints:
  GET  /api/reconcile/bank/{bank_code}/orphan-tmpstat      — list
  POST /api/reconcile/bank/{bank_code}/clear-orphan-tmpstat — clear

Filter: ae_tmpstat > 0 AND ae_reclnum = 0 (committed reconciliations
left alone). Optional entry_numbers list restricts the clear to
specific entries.

ROWLOCK on the UPDATE per project rules. Every clear logs an info
line so audit trails see the modification.

Use case: today's Cloudsis incident left tmpstat reservations on
4 aentries (P100000731, P100000742, R500000366, P100000749) that
were paired by the matcher's old 45-day tolerance but never
finalised. The operator can list and clear them via this endpoint
without going through Opera's UI.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Frontend utility — Clear Orphan Tmpstat button

**Files:**
- Modify: `frontend/src/pages/BankStatementReconcile.tsx`

- [ ] **Step 1: Add a small panel near the reconcile entry list**

Find a sensible place in the reconcile view's render — near the existing entries list. Add:

```typescript
  // Orphan-tmpstat utility (cleanup for partial-reconcile residue)
  const orphanTmpstatQuery = useQuery({
    queryKey: ['orphanTmpstat', selectedBank],
    queryFn: async () => {
      const res = await authFetch(
        `/api/reconcile/bank/${selectedBank}/orphan-tmpstat`,
      );
      const data = await res.json();
      return data;
    },
    enabled: !!selectedBank,
    staleTime: 60_000,
  });

  const clearOrphanTmpstatMutation = useMutation<any, Error, void>({
    mutationFn: async () => {
      const res = await authFetch(
        `/api/reconcile/bank/${selectedBank}/clear-orphan-tmpstat`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({}),  // no entry_numbers = clear all on bank
        },
      );
      return res.json();
    },
    onSuccess: () => {
      orphanTmpstatQuery.refetch();
      entriesQuery.refetch();
      statusQuery.refetch();
    },
  });
```

In the JSX, near where reconcile entries are listed, add a small panel that only appears when there are orphans:

```tsx
{orphanTmpstatQuery.data?.success && orphanTmpstatQuery.data.count > 0 && (
  <div className="my-3 p-3 bg-amber-50 border border-amber-300 rounded-md">
    <div className="flex items-start justify-between">
      <div>
        <p className="text-sm font-semibold text-amber-900">
          {orphanTmpstatQuery.data.count} orphan partial-reconcile reservation{orphanTmpstatQuery.data.count === 1 ? '' : 's'}
        </p>
        <p className="text-xs text-amber-800 mt-1">
          These entries have a ae_tmpstat marker from an earlier reconcile
          attempt that did not finalise. Until cleared, they block the
          entries from being reconciled normally.
        </p>
        <details className="mt-2 text-xs">
          <summary className="cursor-pointer text-amber-900">Show entries</summary>
          <ul className="mt-1 ml-4 list-disc text-amber-800">
            {(orphanTmpstatQuery.data.entries || []).map((e: any) => (
              <li key={e.entry}>
                <span className="font-mono">{e.entry}</span> · {e.date} · £{Number(e.value).toFixed(2)} · tmpstat={e.tmpstat}
              </li>
            ))}
          </ul>
        </details>
      </div>
      <button
        onClick={() => clearOrphanTmpstatMutation.mutate()}
        disabled={clearOrphanTmpstatMutation.isPending}
        className="px-3 py-1.5 text-sm font-medium bg-amber-200 hover:bg-amber-300 border border-amber-400 rounded disabled:opacity-50"
      >
        {clearOrphanTmpstatMutation.isPending ? 'Clearing…' : 'Clear all'}
      </button>
    </div>
    {clearOrphanTmpstatMutation.isSuccess && (
      <p className="text-xs text-green-700 mt-2">
        Cleared {clearOrphanTmpstatMutation.data?.cleared || 0} reservation(s).
      </p>
    )}
    {clearOrphanTmpstatMutation.isError && (
      <p className="text-xs text-red-700 mt-2">
        {clearOrphanTmpstatMutation.error?.message}
      </p>
    )}
  </div>
)}
```

- [ ] **Step 2: Type-check**

```bash
cd /Users/maccb/llmragsql/frontend && npx tsc --noEmit 2>&1 | grep -E "BankStatementReconcile.tsx" | head -5
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git -C /Users/maccb/llmragsql add frontend/src/pages/BankStatementReconcile.tsx
git -C /Users/maccb/llmragsql commit -m "feat(reconcile-ui): orphan-tmpstat utility panel

When the bank has orphan ae_tmpstat reservations (from partial
reconciles that didn't finalise), the reconcile view shows an
amber notice with the count, an expandable list of entries, and a
'Clear all' button that calls the new clear-orphan-tmpstat endpoint.
Refreshes the entries list after a successful clear so the operator
can immediately retry.

Closes the operator-side recovery path for today's Cloudsis incident.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Live verification on Cloudsis

**Files:**
- Manual test only — no commits.

- [ ] **Step 1: Restart the API**

```bash
pkill -f "uvicorn api.main" 2>/dev/null; sleep 2
source venv/bin/activate && nohup uvicorn api.main:app --reload --host 0.0.0.0 --port 8000 > /tmp/api.log 2>&1 & disown
sleep 4
curl -s http://localhost:8000/api/health
```

Expected: `{"status":"healthy","service":"sql-rag-api"}`.

- [ ] **Step 2: Login**

```bash
curl -s -X POST -H 'Content-Type: application/json' -d '{"username":"admin","password":"Harry"}' http://localhost:8000/api/auth/force-clear-session > /dev/null
TOKEN=$(curl -s -X POST -c /tmp/c.txt -H 'Content-Type: application/json' -d '{"username":"admin","password":"Harry"}' http://localhost:8000/api/auth/login | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))")
```

- [ ] **Step 3: List orphan tmpstat reservations on BB005**

```bash
curl -s -b /tmp/c.txt -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/reconcile/bank/BB005/orphan-tmpstat | python3 -m json.tool
```

Expected: lists the orphan reservations from today's incident (P100000731, P100000742, R500000366, P100000749). The user said earlier they'd clear via Opera's UI; the list might be empty if they did.

- [ ] **Step 4: Run match-statement against the April Monzo statement and confirm period-bound restriction**

The statement file is at `/Users/maccb/Downloads/bank-statements/BB005-monzo/Monzo_bank_statement_2026-04-01-2026-04-28_2944.pdf`. Hit:

```bash
# Use preview-from-pdf which internally calls the matcher
curl -s -b /tmp/c.txt -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/bank-import/preview-from-pdf?file_path=/Users/maccb/Downloads/bank-statements/BB005-monzo/Monzo_bank_statement_2026-04-01-2026-04-28_2944.pdf&bank_code=BB005" \
  > /tmp/april_match.json
python3 -c "
import json
d = json.load(open('/tmp/april_match.json'))
print('matched_receipts:', len(d.get('matched_receipts', [])))
print('matched_payments:', len(d.get('matched_payments', [])))
print('matched_refunds:', len(d.get('matched_refunds', [])))
print('already_posted:', len(d.get('already_posted', [])))
print('unmatched:', len(d.get('unmatched', [])))
"
```

Expected: a sensible breakdown for April. The matcher's candidate pool should now exclude any aentry outside `[2026-03-25, 2026-05-05]` (period_start - 7 to period_end + 7).

- [ ] **Step 5: Check log for the period filter**

```bash
grep -E "ae_lstdate BETWEEN '2026-03-25'|ae_lstdate BETWEEN '2026-03-2[0-9]'" /tmp/api.log | tail -5
```

Expected: at least one log entry showing the period-bounded query was emitted (the matcher's candidate query).

If the log shows the unbounded fallback warning ("period bounds not provided"), that means the calling endpoint didn't pass them — diagnose accordingly.

- [ ] **Step 6: Mark task done — no commit**

---

## Task 9: KB updates

**Files:**
- Modify: `apps/core/docs/opera_knowledge_base.md`
- Create: `~/opera-knowledge-ref/.../business-rules/matcher-period-bound.md`

- [ ] **Step 1: Append local KB section**

Append to `apps/core/docs/opera_knowledge_base.md`:

```markdown

## Matcher Period-Bound Rule

The bank-statement-to-cashbook matcher (`match_statement_to_cashbook`) restricts its candidate aentry pool to `ae_lstdate BETWEEN [period_start - grace, period_end + grace]`. Default grace = 7 days (covers month-end postings dated a few days late).

The `/api/bank-reconciliation/match-statement` endpoint passes period bounds; the frontend reconcile view forwards `importedStatementData.period_start` / `period_end`. If a caller doesn't pass period bounds, the matcher logs a warning and falls back to unbounded candidates — but this is a deprecated path; new callers MUST pass them.

The complete-reconciliation handler (`/api/bank-reconciliation/complete`) re-validates each entry's `ae_lstdate` against the period before applying any `ae_tmpstat` write. Out-of-period entries cause a structured 200 response with `success=false` and an `out_of_period` array — the frontend surfaces this as a modal listing each offender's date and the period bounds.

Orphan tmpstat reservations (entries with `ae_tmpstat > 0 AND ae_reclnum = 0` from prior partial-reconcile attempts that didn't finalise) can be listed via `GET /api/reconcile/bank/{bank_code}/orphan-tmpstat` and cleared via `POST /api/reconcile/bank/{bank_code}/clear-orphan-tmpstat`. Clears use ROWLOCK and only touch `ae_tmpstat` — committed reconciliations (`ae_reclnum > 0`) are never affected.
```

- [ ] **Step 2: Pull and write central KB file**

```bash
cd ~/opera-knowledge-ref && git stash push -u -m "preserve unrelated work" 2>/dev/null
cd ~/opera-knowledge-ref && git pull --rebase origin main
cat > ~/opera-knowledge-ref/packages/opera-knowledge/business-rules/matcher-period-bound.md <<'EOF'
# Matcher Period-Bound Rule

The bank-statement-to-cashbook matcher restricts its aentry candidate pool to the statement's own period (with a small grace window).

## Rule

```
candidate_pool = { aentry where
  ae_acnt = bank_code
  AND ae_reclnum = 0
  AND ae_lstdate BETWEEN [period_start - grace_days, period_end + grace_days]
}
```

Default grace = 7 days. The window covers end-of-period postings dated a few days late but excludes random earlier or later transactions that have nothing to do with the statement.

## Why

A 45-day-tolerance candidate selector lets a Feb aentry pair with a March statement, then the complete-reconciliation handler silently sets `ae_tmpstat` on the wrong row. Both layers (matcher AND validator) now enforce the rule, so a bypass at one layer is caught at the next.

## Caller contract

- `match_statement_to_cashbook(period_start=..., period_end=..., period_grace_days=7)` is the supported call. If period bounds are omitted, the function logs a warning and falls back to unbounded — but this path is deprecated.
- `complete_reconciliation` re-validates the matched entries against the same period+grace before any Opera write. Returns a structured `out_of_period` list on violation.

## Tmpstat reservation cleanup

`ae_tmpstat` reservations from prior partial reconciles that didn't finalise (i.e. `ae_tmpstat > 0 AND ae_reclnum = 0`) can be cleared via the dedicated endpoints:

- `GET /api/reconcile/bank/{code}/orphan-tmpstat` — list
- `POST /api/reconcile/bank/{code}/clear-orphan-tmpstat` — clear (optional `entry_numbers` list)

Both endpoints touch only `ae_tmpstat`; committed reconciliations are never affected. ROWLOCK on the UPDATE.
EOF
cd ~/opera-knowledge-ref && git add packages/opera-knowledge/business-rules/matcher-period-bound.md
git commit -m "Document matcher period-bound rule + tmpstat semantics

The bank-statement matcher restricts its aentry candidate pool to the
statement's own period plus a small grace window. The complete-
reconciliation handler re-validates the rule before any tmpstat write,
returning structured out_of_period responses on violation.

Includes the orphan-tmpstat list/clear endpoints used to recover from
partial reconcile attempts that didn't finalise.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
git push origin main
cd ~/opera-knowledge-ref && git stash pop 2>/dev/null
```

- [ ] **Step 3: Commit local KB**

```bash
git -C /Users/maccb/llmragsql add apps/core/docs/opera_knowledge_base.md
git -C /Users/maccb/llmragsql commit -m "$(cat <<'EOF'
docs(kb): document matcher period-bound rule + tmpstat utilities

Mirrors the central KB entry at
opera-knowledge-ref/packages/opera-knowledge/business-rules/
matcher-period-bound.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git -C /Users/maccb/llmragsql push
```

- [ ] **Step 4: Verify**

```bash
echo "=== Local KB ==="
git -C /Users/maccb/llmragsql log --oneline -1 -- apps/core/docs/opera_knowledge_base.md
echo "=== Central KB ==="
git -C ~/opera-knowledge-ref log --oneline -1 -- packages/opera-knowledge/business-rules/matcher-period-bound.md
echo "=== Central remote check ==="
git -C ~/opera-knowledge-ref status -sb | head -3
```

---

## Done Criteria

- [ ] `match_statement_to_cashbook` accepts `period_start`/`period_end`/`period_grace_days`; emitted SQL includes `ae_lstdate BETWEEN ...` filter when bounds are provided.
- [ ] `/api/bank-reconciliation/match-statement` endpoint passes period bounds (from request body or via import_id lookup).
- [ ] Frontend reconcile view forwards `period_start`/`period_end` from `importedStatementData`.
- [ ] `complete_reconciliation` validates entries are in-period before any Opera write; returns structured `out_of_period` on violation.
- [ ] Frontend surfaces the `out_of_period` error in a clear modal.
- [ ] `GET` and `POST` orphan-tmpstat endpoints exist with documented semantics.
- [ ] Frontend utility panel surfaces orphan reservations when present.
- [ ] All tests pass; the new tests cover signature, query bounds, fallback warning, validation refusal, endpoint registration.
- [ ] Live test on Cloudsis BB005 confirms period-bounded matching.
- [ ] Local KB and central KB updated and pushed.
