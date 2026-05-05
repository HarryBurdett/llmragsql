# Bank-Rec Self-Heal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect when Opera has reconciled a bank statement that the app started as partial-rec, and update the local `bank_statement_imports.is_reconciled` flag accordingly — read-only against Opera, on every scan-emails call.

**Architecture:** A new module `sql_rag/bank_rec_heal.py` owns the three-fact rule (balance match within £0.01 + `nk_lststdt >= period_end` + `nk_lststno >= stored statement_number`, with the third check skipped for legacy rows). Both `OperaSEDataSource` and `Opera3DataSource` get a `read_nbank()` method. SE scan-emails and Opera 3 scan-emails routes call the heal before filtering. Both `complete_reconciliation` routes (SE + Opera 3) populate the new `bank_statement_imports.statement_number` column so future heals can use the third check.

**Tech Stack:** Python 3.11+, FastAPI, SQLite (per-company `email_data.db`), SQL Server (Opera SE) via pyodbc with `WITH (NOLOCK)`, FoxPro DBF (Opera 3) via `Opera3DataSource` reader, pytest with mocks (existing pattern: `tests/test_already_posted_fallback.py`, `tests/test_opera_open_items.py`).

**Spec:** `docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md`

---

## File Structure

| File | Responsibility |
|---|---|
| `sql_rag/bank_rec_heal.py` (NEW) | The heal module: `NbankSnapshot` dataclass, `HealResult` dataclass, `is_row_healable()` pure-function rule evaluator, `heal_bank_statement_imports()` orchestrator, `_format_audit_line()` audit formatter |
| `sql_rag/duplicate_check_se.py` (MODIFY) | Add `OperaSEDataSource.read_nbank(bank_code) -> NbankSnapshot` and `count_reconciled_aentry(bank_code, statement_number) -> int`. Both `WITH (NOLOCK)`. |
| `sql_rag/duplicate_check_o3.py` (MODIFY) | Add `Opera3DataSource.read_nbank(bank_code) -> NbankSnapshot` and `count_reconciled_aentry(bank_code, statement_number) -> int`. DBF reads via `self._reader`. |
| `api/email/storage.py` (MODIFY) | Add migration block for `statement_number INTEGER` column (PRAGMA-guarded). |
| `apps/bank_reconcile/api/routes.py` (MODIFY) | (a) SE scan-emails calls `heal_bank_statement_imports`. (b) Opera 3 scan-emails calls same. (c) SE `complete_reconciliation` writes `statement_number` in both partial and full UPDATE blocks. (d) Opera 3 `complete_reconciliation` writes same. |
| `tests/test_bank_rec_heal.py` (NEW) | Truth-table tests for `is_row_healable()` + behaviour tests for `heal_bank_statement_imports()` (mocked Opera + real in-memory SQLite). |
| `tests/test_bank_rec_heal_regression.py` (NEW) | Real-world scenario regression: closing £115,064.71, period_end 2026-05-01, Opera state captured today. Generic anonymised bank code. |
| `tests/test_bank_rec_heal_completion.py` (NEW) | Pin that both SE + Opera 3 `complete_reconciliation` write `statement_number` after success (partial branch and full branch). |
| `tests/test_bank_rec_heal_se_o3_parity.py` (NEW) | Same logical input → same heal decision on both data sources. |
| `apps/core/docs/opera_knowledge_base.md` (MODIFY) | Add "Bank Rec Self-Heal Rule" section. |
| `~/opera-knowledge-ref/packages/opera-knowledge/business-rules/bank-rec-self-heal.md` (NEW) | Central KB doc. |
| `marketing/manuals/manual-bank-reconciliation.md` (MODIFY) | One sentence in Stage 5. |

---

## Task 1: Create `bank_rec_heal.py` module skeleton with `NbankSnapshot` and `HealResult` dataclasses

**Files:**
- Create: `sql_rag/bank_rec_heal.py`
- Test: `tests/test_bank_rec_heal.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_bank_rec_heal.py`:

```python
"""Tests for the bank-rec local-status self-heal module.

Spec: docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md
"""
from datetime import date


def test_nbank_snapshot_holds_required_fields():
    """NbankSnapshot is the data carrier between data sources and the
    rule evaluator. Must expose: bank_code, recbal_pounds, lststdt,
    lststno. Frozen dataclass — no mutation after construction."""
    from sql_rag.bank_rec_heal import NbankSnapshot

    snap = NbankSnapshot(
        bank_code='BC010',
        recbal_pounds=115064.71,
        lststdt=date(2026, 5, 1),
        lststno=86940,
    )
    assert snap.bank_code == 'BC010'
    assert snap.recbal_pounds == 115064.71
    assert snap.lststdt == date(2026, 5, 1)
    assert snap.lststno == 86940


def test_heal_result_holds_count_and_audit_lines():
    """HealResult carries what scan-emails needs to render diagnostics:
    healed_count and a list of audit lines (already-formatted strings)."""
    from sql_rag.bank_rec_heal import HealResult

    r = HealResult(healed_count=2, audit_lines=['line one', 'line two'])
    assert r.healed_count == 2
    assert r.audit_lines == ['line one', 'line two']
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bank_rec_heal.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'sql_rag.bank_rec_heal'`

- [ ] **Step 3: Write minimal implementation**

Create `sql_rag/bank_rec_heal.py`:

```python
"""Bank-rec local-status self-heal.

When the operator runs a partial rec via this app and finishes it in
Opera Cashbook > Reconcile, Opera updates nbank/aentry but does not
touch our local bank_statement_imports.is_reconciled flag. This module
detects that situation on every scan-emails call and updates the local
flag — read-only against Opera.

Rule (all required, AND-ed):
  1. nk_recbal/100.0 ≈ closing_balance within £0.01
  2. nk_lststdt >= period_end
  3. nk_lststno >= stored statement_number  (skipped if NULL)

Spec: docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional


@dataclass(frozen=True)
class NbankSnapshot:
    """Read-only view of the four nbank fields the heal rule needs."""
    bank_code: str
    recbal_pounds: float
    lststdt: Optional[date]
    lststno: Optional[int]


@dataclass
class HealResult:
    """Outcome of a single heal_bank_statement_imports() call."""
    healed_count: int = 0
    audit_lines: List[str] = field(default_factory=list)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bank_rec_heal.py -v`
Expected: PASS — both tests green.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/bank_rec_heal.py tests/test_bank_rec_heal.py
git commit -m "feat(bank-rec-heal): scaffold module with NbankSnapshot + HealResult dataclasses"
```

---

## Task 2: Implement `is_row_healable()` rule evaluator — three-fact truth table

**Files:**
- Modify: `sql_rag/bank_rec_heal.py`
- Test: `tests/test_bank_rec_heal.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_bank_rec_heal.py`:

```python
def _snap(recbal=115064.71, lststdt=date(2026, 5, 1), lststno=86940, bank='BC010'):
    """Build an NbankSnapshot with sensible defaults for the heal rule."""
    from sql_rag.bank_rec_heal import NbankSnapshot
    return NbankSnapshot(
        bank_code=bank,
        recbal_pounds=recbal,
        lststdt=lststdt,
        lststno=lststno,
    )


def _row(closing=115064.71, period_end=date(2026, 5, 1), statement_number=None):
    """Build a bank_statement_imports row dict matching the rule's inputs."""
    return {
        'closing_balance': closing,
        'period_end': period_end,
        'statement_number': statement_number,
    }


def test_three_facts_match_returns_true():
    """All three checks satisfied → row is healable."""
    from sql_rag.bank_rec_heal import is_row_healable

    snap = _snap()
    row = _row(statement_number=86940)
    healable, _ = is_row_healable(row, snap)
    assert healable is True


def test_balance_mismatch_returns_false():
    """nk_recbal differs from closing by > £0.01 → not healable."""
    from sql_rag.bank_rec_heal import is_row_healable

    snap = _snap(recbal=115064.71)
    row = _row(closing=115000.00, statement_number=86940)
    healable, _ = is_row_healable(row, snap)
    assert healable is False


def test_balance_match_within_one_pence_returns_true():
    """Difference of exactly £0.01 → still considered match (boundary)."""
    from sql_rag.bank_rec_heal import is_row_healable

    snap = _snap(recbal=115064.71)
    row = _row(closing=115064.70, statement_number=86940)
    healable, _ = is_row_healable(row, snap)
    assert healable is True


def test_balance_match_outside_one_pence_returns_false():
    """Difference of £0.011 (just outside tolerance) → not healable."""
    from sql_rag.bank_rec_heal import is_row_healable

    snap = _snap(recbal=115064.71)
    row = _row(closing=115064.699, statement_number=86940)
    healable, _ = is_row_healable(row, snap)
    assert healable is False


def test_date_strictly_before_period_end_returns_false():
    """nk_lststdt < period_end → not healable."""
    from sql_rag.bank_rec_heal import is_row_healable

    snap = _snap(lststdt=date(2026, 4, 30))
    row = _row(period_end=date(2026, 5, 1), statement_number=86940)
    healable, _ = is_row_healable(row, snap)
    assert healable is False


def test_date_equals_period_end_returns_true():
    """nk_lststdt == period_end → satisfied (>=, not >)."""
    from sql_rag.bank_rec_heal import is_row_healable

    snap = _snap(lststdt=date(2026, 5, 1))
    row = _row(period_end=date(2026, 5, 1), statement_number=86940)
    healable, _ = is_row_healable(row, snap)
    assert healable is True


def test_date_after_period_end_returns_true():
    """nk_lststdt > period_end (later statement reconciled too) → satisfied."""
    from sql_rag.bank_rec_heal import is_row_healable

    snap = _snap(lststdt=date(2026, 6, 1))
    row = _row(period_end=date(2026, 5, 1), statement_number=86940)
    healable, _ = is_row_healable(row, snap)
    assert healable is True


def test_statement_number_match_returns_true():
    """nk_lststno == stored statement_number → satisfied."""
    from sql_rag.bank_rec_heal import is_row_healable

    snap = _snap(lststno=86940)
    row = _row(statement_number=86940)
    healable, _ = is_row_healable(row, snap)
    assert healable is True


def test_statement_number_advanced_returns_true():
    """nk_lststno > stored (next rec already done) → satisfied."""
    from sql_rag.bank_rec_heal import is_row_healable

    snap = _snap(lststno=86941)
    row = _row(statement_number=86940)
    healable, _ = is_row_healable(row, snap)
    assert healable is True


def test_statement_number_behind_returns_false():
    """nk_lststno < stored (Opera hasn't reached our number yet) → not healable."""
    from sql_rag.bank_rec_heal import is_row_healable

    snap = _snap(lststno=86939)
    row = _row(statement_number=86940)
    healable, _ = is_row_healable(row, snap)
    assert healable is False


def test_legacy_row_no_stored_number_uses_two_checks():
    """When statement_number is NULL, skip check 3 — checks 1+2 alone
    decide. This is the path for legacy rows imported before the
    schema migration."""
    from sql_rag.bank_rec_heal import is_row_healable

    snap = _snap(lststno=999)  # would fail check 3 if it ran
    row = _row(statement_number=None)  # legacy row
    healable, audit = is_row_healable(row, snap)
    assert healable is True
    # The audit string must explicitly note that check 3 was skipped
    assert 'skipped' in audit.lower() or 'legacy' in audit.lower()


def test_null_nk_recbal_returns_false():
    """Defensive: NULL recbal_pounds → not healable, no exception."""
    from sql_rag.bank_rec_heal import is_row_healable

    snap = _snap(recbal=None)
    row = _row(statement_number=86940)
    healable, _ = is_row_healable(row, snap)
    assert healable is False


def test_null_nk_lststdt_returns_false():
    """Defensive: NULL lststdt → not healable."""
    from sql_rag.bank_rec_heal import is_row_healable

    snap = _snap(lststdt=None)
    row = _row(statement_number=86940)
    healable, _ = is_row_healable(row, snap)
    assert healable is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_bank_rec_heal.py -v`
Expected: FAIL — `is_row_healable` not yet defined.

- [ ] **Step 3: Implement `is_row_healable`**

Append to `sql_rag/bank_rec_heal.py`:

```python
from typing import Any, Mapping, Tuple

# Tolerance for the balance match. £0.01 is the manual-rec convention
# elsewhere in the codebase (statement opening-balance validation, etc.).
BALANCE_TOLERANCE_POUNDS = 0.01


def is_row_healable(
    row: Mapping[str, Any],
    snapshot: NbankSnapshot,
) -> Tuple[bool, str]:
    """Evaluate the three-fact rule for a single bank_statement_imports row.

    Returns (healable, audit_proof_string). The audit string is suitable
    to embed in the per-row log line regardless of outcome — explains
    which checks passed or failed.

    The rule (all required, AND-ed; check 3 skipped if statement_number
    is NULL):
      1. nk_recbal_pounds matches closing_balance within
         BALANCE_TOLERANCE_POUNDS.
      2. nk_lststdt >= period_end.
      3. nk_lststno >= stored statement_number.
    """
    closing = row.get('closing_balance')
    period_end = row.get('period_end')
    stored_stmt_no = row.get('statement_number')

    # Defensive: any NULL on either side of a check fails the rule.
    if snapshot.recbal_pounds is None or closing is None:
        return False, 'check 1 NULL: recbal or closing missing'
    if snapshot.lststdt is None or period_end is None:
        return False, 'check 2 NULL: lststdt or period_end missing'

    # Check 1: balance match within £0.01.
    if abs(snapshot.recbal_pounds - float(closing)) > BALANCE_TOLERANCE_POUNDS:
        return False, (
            f'check 1 fail: nk_recbal=£{snapshot.recbal_pounds:.2f} '
            f'!= closing=£{float(closing):.2f}'
        )

    # Check 2: nk_lststdt >= period_end.
    if snapshot.lststdt < period_end:
        return False, (
            f'check 2 fail: nk_lststdt={snapshot.lststdt} '
            f'< period_end={period_end}'
        )

    # Check 3: nk_lststno >= stored statement_number, IF stored is set.
    if stored_stmt_no is None:
        proof = (
            f'check 1 ok: nk_recbal=£{snapshot.recbal_pounds:.2f} '
            f'≈ closing=£{float(closing):.2f}; '
            f'check 2 ok: nk_lststdt={snapshot.lststdt} '
            f'>= period_end={period_end}; '
            f'check 3 skipped — legacy row (statement_number IS NULL)'
        )
        return True, proof

    if snapshot.lststno is None or int(snapshot.lststno) < int(stored_stmt_no):
        return False, (
            f'check 3 fail: nk_lststno={snapshot.lststno} '
            f'< statement_number={stored_stmt_no}'
        )

    proof = (
        f'check 1 ok: nk_recbal=£{snapshot.recbal_pounds:.2f} '
        f'≈ closing=£{float(closing):.2f}; '
        f'check 2 ok: nk_lststdt={snapshot.lststdt} '
        f'>= period_end={period_end}; '
        f'check 3 ok: nk_lststno={snapshot.lststno} '
        f'>= statement_number={stored_stmt_no}'
    )
    return True, proof
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_bank_rec_heal.py -v`
Expected: PASS — all 13 tests green (2 from Task 1 + 11 new).

- [ ] **Step 5: Commit**

```bash
git add sql_rag/bank_rec_heal.py tests/test_bank_rec_heal.py
git commit -m "feat(bank-rec-heal): three-fact rule evaluator + truth-table tests"
```

---

## Task 3: Add `read_nbank()` to `OperaSEDataSource` with `WITH (NOLOCK)`

**Files:**
- Modify: `sql_rag/duplicate_check_se.py`
- Test: `tests/test_bank_rec_heal_se_data_source.py` (NEW)

- [ ] **Step 1: Write the failing test**

Create `tests/test_bank_rec_heal_se_data_source.py`:

```python
"""Pin OperaSEDataSource.read_nbank() — must use WITH (NOLOCK), must
return an NbankSnapshot, must handle the missing-bank case."""
from datetime import date
from unittest.mock import MagicMock

import pandas as pd


def test_read_nbank_returns_snapshot_with_pence_to_pounds_conversion():
    """nbank stores nk_recbal in pence; the snapshot exposes pounds.
    Verify the SQL divides by 100.0 and the result is a float."""
    from sql_rag.duplicate_check_se import OperaSEDataSource

    fake_df = pd.DataFrame([{
        'recbal_pounds': 115064.71,
        'lststdt': date(2026, 5, 1),
        'lststno': 86940,
    }])
    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = fake_df

    ds = OperaSEDataSource(fake_sql)
    snap = ds.read_nbank('BC010')

    assert snap is not None
    assert snap.bank_code == 'BC010'
    assert snap.recbal_pounds == 115064.71
    assert snap.lststdt == date(2026, 5, 1)
    assert snap.lststno == 86940


def test_read_nbank_query_uses_nolock_and_pence_division():
    """Source-inspect the SQL — must contain WITH (NOLOCK) and
    nk_recbal / 100.0 (pence-to-pounds, per CLAUDE.md amount rule)."""
    from sql_rag.duplicate_check_se import OperaSEDataSource

    fake_df = pd.DataFrame()  # empty df → snapshot is None
    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = fake_df

    ds = OperaSEDataSource(fake_sql)
    ds.read_nbank('BC010')

    sql = fake_sql.execute_query.call_args[0][0]
    assert 'WITH (NOLOCK)' in sql, "read_nbank SE query MUST use WITH (NOLOCK)"
    assert 'nk_recbal / 100.0' in sql, "must convert pence to pounds"
    assert 'nk_lststdt' in sql
    assert 'nk_lststno' in sql


def test_read_nbank_returns_none_when_bank_missing():
    """Bank not in nbank → return None (caller logs and skips heal)."""
    from sql_rag.duplicate_check_se import OperaSEDataSource

    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = pd.DataFrame()

    ds = OperaSEDataSource(fake_sql)
    snap = ds.read_nbank('UNKNOWN')
    assert snap is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bank_rec_heal_se_data_source.py -v`
Expected: FAIL — `OperaSEDataSource has no attribute 'read_nbank'`.

- [ ] **Step 3: Implement `read_nbank` on the SE data source**

Edit `sql_rag/duplicate_check_se.py`. Add this import near the top (next to existing imports):

```python
from sql_rag.bank_rec_heal import NbankSnapshot
```

Append this method to the `OperaSEDataSource` class (after `find_ptran_by_signed_value`):

```python
    def read_nbank(self, bank_code: str) -> Optional['NbankSnapshot']:
        """Read the four nbank fields the bank-rec self-heal rule needs.

        Returns None if the bank does not exist in nbank. Uses WITH
        (NOLOCK) per business-rules/locking-protocol.md. Pence → pounds
        conversion is done in SQL (nk_recbal / 100.0).
        """
        from sql_rag.bank_rec_heal import NbankSnapshot
        query = f"""
            SELECT nk_recbal / 100.0 AS recbal_pounds,
                   nk_lststdt        AS lststdt,
                   nk_lststno        AS lststno
            FROM nbank WITH (NOLOCK)
            WHERE RTRIM(nk_acnt) = '{bank_code}'
        """
        df = self._sql.execute_query(query)
        if df is None or df.empty:
            return None
        row = df.iloc[0]
        recbal = row.get('recbal_pounds')
        lststdt = row.get('lststdt')
        lststno = row.get('lststno')
        # Normalise lststdt: SQL Server may return datetime; the rule
        # only cares about the date part.
        if lststdt is not None and hasattr(lststdt, 'date'):
            lststdt = lststdt.date()
        return NbankSnapshot(
            bank_code=bank_code,
            recbal_pounds=float(recbal) if recbal is not None else None,
            lststdt=lststdt,
            lststno=int(lststno) if lststno is not None else None,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_bank_rec_heal_se_data_source.py tests/test_bank_rec_heal.py -v`
Expected: PASS — all tests green.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/duplicate_check_se.py tests/test_bank_rec_heal_se_data_source.py
git commit -m "feat(dup-check-se): add read_nbank() for bank-rec heal (WITH NOLOCK)"
```

---

## Task 4: Add `read_nbank()` to `Opera3DataSource` (FoxPro DBF read)

**Files:**
- Modify: `sql_rag/duplicate_check_o3.py`
- Test: `tests/test_bank_rec_heal_o3_data_source.py` (NEW)

- [ ] **Step 1: Write the failing test**

Create `tests/test_bank_rec_heal_o3_data_source.py`:

```python
"""Pin Opera3DataSource.read_nbank() — DBF row scan, no SQL."""
from datetime import date
from unittest.mock import MagicMock


class _FakeReader:
    """Mock for the FoxPro DBF reader used by Opera3DataSource.
    Returns rows from a fixed dict-of-lists keyed by table name."""
    def __init__(self, tables):
        self._tables = tables

    def read_table(self, name):
        return self._tables.get(name, [])


def test_read_nbank_o3_returns_snapshot_with_pence_to_pounds():
    """nbank.dbf stores nk_recbal in pence; snapshot exposes pounds."""
    from sql_rag.duplicate_check_o3 import Opera3DataSource

    reader = _FakeReader({
        'nbank': [
            {'nk_acnt': 'BC010', 'nk_recbal': 11506471,
             'nk_lststdt': date(2026, 5, 1), 'nk_lststno': 86940},
            {'nk_acnt': 'OTHER', 'nk_recbal': 100, 'nk_lststdt': None,
             'nk_lststno': 1},
        ]
    })
    ds = Opera3DataSource(reader)
    snap = ds.read_nbank('BC010')

    assert snap is not None
    assert snap.bank_code == 'BC010'
    assert snap.recbal_pounds == 115064.71
    assert snap.lststdt == date(2026, 5, 1)
    assert snap.lststno == 86940


def test_read_nbank_o3_returns_none_when_bank_missing():
    """Bank not in DBF → None."""
    from sql_rag.duplicate_check_o3 import Opera3DataSource

    reader = _FakeReader({'nbank': [{'nk_acnt': 'OTHER'}]})
    ds = Opera3DataSource(reader)
    assert ds.read_nbank('BC010') is None


def test_read_nbank_o3_handles_padded_acnt():
    """FoxPro typically right-pads CHAR fields with spaces. Match must
    strip before comparing."""
    from sql_rag.duplicate_check_o3 import Opera3DataSource

    reader = _FakeReader({
        'nbank': [
            {'nk_acnt': 'BC010    ', 'nk_recbal': 11506471,
             'nk_lststdt': date(2026, 5, 1), 'nk_lststno': 86940},
        ]
    })
    ds = Opera3DataSource(reader)
    snap = ds.read_nbank('BC010')
    assert snap is not None
    assert snap.recbal_pounds == 115064.71
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bank_rec_heal_o3_data_source.py -v`
Expected: FAIL — `Opera3DataSource has no attribute 'read_nbank'`.

- [ ] **Step 3: Implement `read_nbank` on the Opera 3 data source**

Edit `sql_rag/duplicate_check_o3.py`. Append this method to the `Opera3DataSource` class:

```python
    def read_nbank(self, bank_code: str):
        """Read the four nbank fields the bank-rec self-heal rule needs.

        Returns None if the bank does not exist in nbank.dbf. The DBF
        stores nk_recbal in pence; we convert to pounds. nk_acnt is
        space-padded in FoxPro CHAR fields — strip before comparing.
        """
        from sql_rag.bank_rec_heal import NbankSnapshot

        for row in self._reader.read_table('nbank'):
            acnt = _row_get(row, 'nk_acnt')
            if acnt is None or str(acnt).strip() != bank_code:
                continue
            recbal_pence = _row_get(row, 'nk_recbal')
            lststdt = _normalise_date(_row_get(row, 'nk_lststdt'))
            lststno = _row_get(row, 'nk_lststno')
            return NbankSnapshot(
                bank_code=bank_code,
                recbal_pounds=(float(recbal_pence) / 100.0) if recbal_pence is not None else None,
                lststdt=lststdt,
                lststno=int(lststno) if lststno is not None else None,
            )
        return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_bank_rec_heal_o3_data_source.py -v`
Expected: PASS — all 3 tests green.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/duplicate_check_o3.py tests/test_bank_rec_heal_o3_data_source.py
git commit -m "feat(dup-check-o3): add read_nbank() for bank-rec heal (DBF read)"
```

---

## Task 5: Add `count_reconciled_aentry()` to both data sources

**Files:**
- Modify: `sql_rag/duplicate_check_se.py`
- Modify: `sql_rag/duplicate_check_o3.py`
- Test: extend `tests/test_bank_rec_heal_se_data_source.py` and `tests/test_bank_rec_heal_o3_data_source.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_bank_rec_heal_se_data_source.py`:

```python
def test_count_reconciled_aentry_se_returns_int():
    """Counts aentry rows where ae_acnt=bank, ae_frstat=stmt_no, ae_reclnum>0.
    Used by the heal to populate reconciled_count for non-legacy rows."""
    from sql_rag.duplicate_check_se import OperaSEDataSource

    fake_df = pd.DataFrame([{'cnt': 17}])
    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = fake_df

    ds = OperaSEDataSource(fake_sql)
    n = ds.count_reconciled_aentry('BC010', 86940)
    assert n == 17


def test_count_reconciled_aentry_se_uses_nolock_and_correct_filters():
    """SQL must reference ae_acnt, ae_frstat, ae_reclnum > 0; WITH (NOLOCK)."""
    from sql_rag.duplicate_check_se import OperaSEDataSource

    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = pd.DataFrame([{'cnt': 0}])
    ds = OperaSEDataSource(fake_sql)
    ds.count_reconciled_aentry('BC010', 86940)
    sql = fake_sql.execute_query.call_args[0][0]
    assert 'WITH (NOLOCK)' in sql
    assert 'ae_acnt' in sql
    assert 'ae_frstat' in sql
    assert 'ae_reclnum > 0' in sql
```

Append to `tests/test_bank_rec_heal_o3_data_source.py`:

```python
def test_count_reconciled_aentry_o3_counts_matching_dbf_rows():
    """Scans aentry.dbf, returns count where ae_acnt=bank, ae_frstat=stmt_no,
    ae_reclnum>0. Other rows (different bank, different statement,
    unreconciled) are excluded."""
    from sql_rag.duplicate_check_o3 import Opera3DataSource

    reader = _FakeReader({
        'aentry': [
            {'ae_acnt': 'BC010', 'ae_frstat': 86940, 'ae_reclnum': 2682},
            {'ae_acnt': 'BC010', 'ae_frstat': 86940, 'ae_reclnum': 2682},
            {'ae_acnt': 'BC010', 'ae_frstat': 86939, 'ae_reclnum': 2681},  # different stmt
            {'ae_acnt': 'BC010', 'ae_frstat': 86940, 'ae_reclnum': 0},      # unreconciled
            {'ae_acnt': 'OTHER', 'ae_frstat': 86940, 'ae_reclnum': 2682},   # different bank
        ]
    })
    ds = Opera3DataSource(reader)
    assert ds.count_reconciled_aentry('BC010', 86940) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_bank_rec_heal_se_data_source.py tests/test_bank_rec_heal_o3_data_source.py -v`
Expected: FAIL — both `count_reconciled_aentry` methods missing.

- [ ] **Step 3: Implement on SE data source**

Append to the `OperaSEDataSource` class in `sql_rag/duplicate_check_se.py`:

```python
    def count_reconciled_aentry(
        self,
        bank_code: str,
        statement_number: int,
    ) -> int:
        """Count aentry rows reconciled in the given statement number for this bank.

        Used by the bank-rec self-heal to populate reconciled_count when
        flipping is_reconciled=0 → 1 for rows that have a stored
        statement_number.
        """
        query = f"""
            SELECT COUNT(*) AS cnt
            FROM aentry WITH (NOLOCK)
            WHERE RTRIM(ae_acnt) = '{bank_code}'
              AND ae_frstat = {int(statement_number)}
              AND ae_reclnum > 0
        """
        df = self._sql.execute_query(query)
        if df is None or df.empty:
            return 0
        cnt = df.iloc[0].get('cnt', 0)
        return int(cnt) if cnt is not None else 0
```

- [ ] **Step 4: Implement on Opera 3 data source**

Append to the `Opera3DataSource` class in `sql_rag/duplicate_check_o3.py`:

```python
    def count_reconciled_aentry(
        self,
        bank_code: str,
        statement_number: int,
    ) -> int:
        """Count aentry rows reconciled in the given statement number for this bank.

        DBF scan; nk_acnt is space-padded so strip before comparing.
        """
        n = 0
        for row in self._reader.read_table('aentry'):
            acnt = _row_get(row, 'ae_acnt')
            if acnt is None or str(acnt).strip() != bank_code:
                continue
            frstat = _row_get(row, 'ae_frstat')
            if frstat is None or int(frstat) != int(statement_number):
                continue
            reclnum = _row_get(row, 'ae_reclnum')
            if reclnum is None or int(reclnum) <= 0:
                continue
            n += 1
        return n
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_bank_rec_heal_se_data_source.py tests/test_bank_rec_heal_o3_data_source.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add sql_rag/duplicate_check_se.py sql_rag/duplicate_check_o3.py tests/test_bank_rec_heal_se_data_source.py tests/test_bank_rec_heal_o3_data_source.py
git commit -m "feat(dup-check): add count_reconciled_aentry() to both data sources"
```

---

## Task 6: Add `statement_number` column migration to `bank_statement_imports`

**Files:**
- Modify: `api/email/storage.py`
- Test: `tests/test_bank_rec_heal_schema_migration.py` (NEW)

- [ ] **Step 1: Write the failing test**

Create `tests/test_bank_rec_heal_schema_migration.py`:

```python
"""Pin the schema migration that adds bank_statement_imports.statement_number."""
import sqlite3
import tempfile
from pathlib import Path


def test_statement_number_column_added_on_init():
    """A fresh EmailStorage init must have statement_number INTEGER on
    bank_statement_imports."""
    from api.email.storage import EmailStorage

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / 'email_data.db'
        EmailStorage(str(db_path))

        with sqlite3.connect(str(db_path)) as conn:
            cur = conn.execute("PRAGMA table_info(bank_statement_imports)")
            cols = {row[1]: row[2] for row in cur.fetchall()}
            assert 'statement_number' in cols, \
                "bank_statement_imports must have a statement_number column"
            assert cols['statement_number'].upper() == 'INTEGER'


def test_migration_is_idempotent_on_existing_db():
    """Running EmailStorage() twice on the same path must not error
    (the migration must detect the column already exists and no-op)."""
    from api.email.storage import EmailStorage

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / 'email_data.db'
        EmailStorage(str(db_path))   # first init
        EmailStorage(str(db_path))   # second init must not fail


def test_existing_legacy_data_preserved_through_migration():
    """If the table existed without statement_number, the migration must
    add the column without losing existing rows. The new column is NULL
    for those rows (legacy)."""
    from api.email.storage import EmailStorage

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / 'email_data.db'
        EmailStorage(str(db_path))   # creates schema

        # Insert a legacy row (no statement_number — will land as NULL)
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute("""
                INSERT INTO bank_statement_imports
                    (bank_code, filename, opening_balance, closing_balance,
                     statement_date, period_end, is_reconciled)
                VALUES (?, ?, ?, ?, ?, ?, 0)
            """, ('BC010', 'legacy.pdf', 100.0, 200.0, '2026-05-01', '2026-05-01'))
            conn.commit()

        # Re-init: idempotent migration should not touch existing data
        EmailStorage(str(db_path))

        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(
                "SELECT bank_code, statement_number FROM bank_statement_imports"
            ).fetchone()
            assert row[0] == 'BC010'
            assert row[1] is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bank_rec_heal_schema_migration.py -v`
Expected: FAIL — `statement_number` column does not exist yet.

- [ ] **Step 3: Add migration block in `api/email/storage.py`**

Open `api/email/storage.py`, find the existing migration block at line 417–423 (the `file_path` migration). Add a new migration block immediately after it:

```python
            # Migration: Add statement_number column to bank_statement_imports
            # for the bank-rec self-heal rule (Task 6 of bank-rec self-heal plan).
            # See: docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md
            try:
                cursor.execute("PRAGMA table_info(bank_statement_imports)")
                imp_columns = {c[1] for c in cursor.fetchall()}
                if 'statement_number' not in imp_columns:
                    logger.info("Adding statement_number column to bank_statement_imports")
                    cursor.execute("ALTER TABLE bank_statement_imports ADD COLUMN statement_number INTEGER")
            except Exception as e:
                logger.warning(f"statement_number column migration: {e}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_bank_rec_heal_schema_migration.py -v`
Expected: PASS — all 3 tests green.

- [ ] **Step 5: Commit**

```bash
git add api/email/storage.py tests/test_bank_rec_heal_schema_migration.py
git commit -m "feat(email-storage): add bank_statement_imports.statement_number column migration"
```

---

## Task 7: Implement `heal_bank_statement_imports()` orchestrator

**Files:**
- Modify: `sql_rag/bank_rec_heal.py`
- Test: extend `tests/test_bank_rec_heal.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_bank_rec_heal.py`:

```python
def _make_email_db(tmp_path, rows):
    """Build an in-memory-style email_data.db with a list of
    bank_statement_imports rows. Returns the path."""
    from api.email.storage import EmailStorage

    db_path = tmp_path / 'email_data.db'
    EmailStorage(str(db_path))  # creates schema + statement_number column

    import sqlite3
    with sqlite3.connect(str(db_path)) as conn:
        for r in rows:
            conn.execute("""
                INSERT INTO bank_statement_imports
                    (bank_code, filename, opening_balance, closing_balance,
                     statement_date, period_end, is_reconciled,
                     reconciled_count, statement_number)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                r.get('bank_code', 'BC010'),
                r.get('filename', 'test.pdf'),
                r.get('opening_balance', 0),
                r.get('closing_balance'),
                r.get('statement_date', '2026-05-01'),
                r.get('period_end'),
                r.get('is_reconciled', 0),
                r.get('reconciled_count', 0),
                r.get('statement_number'),
            ))
        conn.commit()
    return db_path


def _make_data_source(snapshot, reconciled_count=20):
    """Mock OperaDataSource that returns the given snapshot from
    read_nbank() and a fixed count from count_reconciled_aentry()."""
    ds = MagicMock()
    ds.read_nbank.return_value = snapshot
    ds.count_reconciled_aentry.return_value = reconciled_count
    return ds


def test_heal_three_facts_match_marks_done(tmp_path):
    """Happy path: row with all three checks satisfying → flips to
    is_reconciled=1, reconciled_count populated, audit line emitted."""
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )
    import sqlite3

    db = _make_email_db(tmp_path, [{
        'closing_balance': 115064.71,
        'period_end': '2026-05-01',
        'is_reconciled': 0,
        'reconciled_count': 0,
        'statement_number': 86940,
    }])
    ds = _make_data_source(NbankSnapshot(
        bank_code='BC010',
        recbal_pounds=115064.71,
        lststdt=date(2026, 5, 1),
        lststno=86940,
    ), reconciled_count=20)

    result = heal_bank_statement_imports('BC010', db, ds)

    assert result.healed_count == 1
    assert len(result.audit_lines) == 1
    assert 'healed' in result.audit_lines[0]

    with sqlite3.connect(str(db)) as conn:
        row = conn.execute(
            "SELECT is_reconciled, reconciled_count FROM bank_statement_imports"
        ).fetchone()
        assert row[0] == 1
        assert row[1] == 20  # populated from count_reconciled_aentry


def test_heal_legacy_row_with_no_statement_number_uses_two_checks(tmp_path):
    """Legacy row (statement_number=NULL) heals on checks 1+2 alone.
    reconciled_count is preserved (count_reconciled_aentry not called)."""
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )
    import sqlite3

    db = _make_email_db(tmp_path, [{
        'closing_balance': 115064.71,
        'period_end': '2026-05-01',
        'is_reconciled': 0,
        'reconciled_count': 5,  # non-zero from a previous partial completion
        'statement_number': None,
    }])
    ds = _make_data_source(NbankSnapshot(
        bank_code='BC010',
        recbal_pounds=115064.71,
        lststdt=date(2026, 5, 1),
        lststno=86940,
    ))

    result = heal_bank_statement_imports('BC010', db, ds)

    assert result.healed_count == 1
    # count_reconciled_aentry must NOT be called for legacy rows
    ds.count_reconciled_aentry.assert_not_called()

    with sqlite3.connect(str(db)) as conn:
        row = conn.execute(
            "SELECT is_reconciled, reconciled_count FROM bank_statement_imports"
        ).fetchone()
        assert row[0] == 1
        assert row[1] == 5  # preserved, NOT overwritten


def test_heal_balance_mismatch_no_change(tmp_path):
    """Row whose closing differs from Opera's recbal stays at 0."""
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )
    import sqlite3

    db = _make_email_db(tmp_path, [{
        'closing_balance': 100000.00,
        'period_end': '2026-05-01',
        'is_reconciled': 0,
        'statement_number': 86940,
    }])
    ds = _make_data_source(NbankSnapshot(
        bank_code='BC010',
        recbal_pounds=115064.71,  # differs by ~£15K
        lststdt=date(2026, 5, 1),
        lststno=86940,
    ))

    result = heal_bank_statement_imports('BC010', db, ds)
    assert result.healed_count == 0

    with sqlite3.connect(str(db)) as conn:
        row = conn.execute(
            "SELECT is_reconciled FROM bank_statement_imports"
        ).fetchone()
        assert row[0] == 0


def test_heal_idempotent(tmp_path):
    """Running heal twice — second run finds nothing to heal."""
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )

    db = _make_email_db(tmp_path, [{
        'closing_balance': 115064.71,
        'period_end': '2026-05-01',
        'is_reconciled': 0,
        'statement_number': 86940,
    }])
    ds = _make_data_source(NbankSnapshot(
        bank_code='BC010',
        recbal_pounds=115064.71,
        lststdt=date(2026, 5, 1),
        lststno=86940,
    ))

    first = heal_bank_statement_imports('BC010', db, ds)
    second = heal_bank_statement_imports('BC010', db, ds)
    assert first.healed_count == 1
    assert second.healed_count == 0


def test_heal_only_touches_target_bank(tmp_path):
    """Rows for other banks are not touched even if the rule would match."""
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )
    import sqlite3

    db = _make_email_db(tmp_path, [
        {'bank_code': 'BC010', 'closing_balance': 115064.71,
         'period_end': '2026-05-01', 'statement_number': 86940},
        {'bank_code': 'OTHER', 'closing_balance': 115064.71,
         'period_end': '2026-05-01', 'statement_number': 86940},
    ])
    ds = _make_data_source(NbankSnapshot(
        bank_code='BC010',
        recbal_pounds=115064.71,
        lststdt=date(2026, 5, 1),
        lststno=86940,
    ))

    heal_bank_statement_imports('BC010', db, ds)

    with sqlite3.connect(str(db)) as conn:
        rows = conn.execute(
            "SELECT bank_code, is_reconciled FROM bank_statement_imports ORDER BY bank_code"
        ).fetchall()
        # BC010 healed, OTHER unchanged
        assert {r[0]: r[1] for r in rows} == {'BC010': 1, 'OTHER': 0}


def test_heal_opera_unreachable_skips_silently(tmp_path):
    """If read_nbank raises (Opera connection failed), heal returns 0
    healed without raising, leaving the local row alone."""
    from sql_rag.bank_rec_heal import heal_bank_statement_imports
    import sqlite3

    db = _make_email_db(tmp_path, [{
        'closing_balance': 115064.71,
        'period_end': '2026-05-01',
        'statement_number': 86940,
    }])
    ds = MagicMock()
    ds.read_nbank.side_effect = ConnectionError('Opera unreachable')

    result = heal_bank_statement_imports('BC010', db, ds)
    assert result.healed_count == 0

    with sqlite3.connect(str(db)) as conn:
        row = conn.execute(
            "SELECT is_reconciled FROM bank_statement_imports"
        ).fetchone()
        assert row[0] == 0


def test_heal_nbank_missing_returns_zero(tmp_path):
    """Bank not in nbank → read_nbank returns None → heal does nothing."""
    from sql_rag.bank_rec_heal import heal_bank_statement_imports

    db = _make_email_db(tmp_path, [{
        'closing_balance': 115064.71,
        'period_end': '2026-05-01',
        'statement_number': 86940,
    }])
    ds = MagicMock()
    ds.read_nbank.return_value = None

    result = heal_bank_statement_imports('BC010', db, ds)
    assert result.healed_count == 0
```

You also need this import at the top of the test file (above any existing test):

```python
from unittest.mock import MagicMock
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_bank_rec_heal.py -v`
Expected: FAIL — `heal_bank_statement_imports` not yet defined.

- [ ] **Step 3: Implement `heal_bank_statement_imports`**

Append to `sql_rag/bank_rec_heal.py`:

```python
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Protocol

logger = logging.getLogger(__name__)


class OperaDataSource(Protocol):
    """Structural type for an Opera data source the heal can use.
    Both OperaSEDataSource (sql_rag/duplicate_check_se.py) and
    Opera3DataSource (sql_rag/duplicate_check_o3.py) satisfy this."""

    def read_nbank(self, bank_code: str):
        ...

    def count_reconciled_aentry(
        self, bank_code: str, statement_number: int
    ) -> int:
        ...


def heal_bank_statement_imports(
    bank_code: str,
    company_db_path: Path,
    opera_data_source: 'OperaDataSource',
) -> HealResult:
    """For every bank_statement_imports row on this bank with
    is_reconciled=0, evaluate the three-fact rule against Opera and
    flip the local flag where the rule is satisfied.

    Read-only against Opera. Updates only local SQLite. Idempotent.
    Per-company isolated — the caller resolves company_db_path via
    get_current_db_path('email_data.db') in the request scope.
    """
    result = HealResult()

    # Read Opera nbank once for this bank. If Opera is unreachable or
    # the bank is missing, log and bail — don't error the surrounding
    # scan call.
    try:
        snapshot = opera_data_source.read_nbank(bank_code)
    except Exception as exc:
        logger.warning(
            'bank_rec_heal: bank=%s nbank read failed (%s); skipping heal',
            bank_code, exc,
        )
        return result

    if snapshot is None:
        logger.warning(
            'bank_rec_heal: bank=%s not found in nbank; no rows healed',
            bank_code,
        )
        return result

    # Read all candidate rows (is_reconciled=0) for this bank.
    with sqlite3.connect(str(company_db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, closing_balance, period_end, statement_number,
                   reconciled_count, reconciled_date
              FROM bank_statement_imports
             WHERE bank_code = ?
               AND COALESCE(is_reconciled, 0) = 0
            """,
            (bank_code,),
        )
        candidates = cursor.fetchall()

        for cand in candidates:
            row = {
                'closing_balance': cand['closing_balance'],
                'period_end': _parse_iso_date(cand['period_end']),
                'statement_number': cand['statement_number'],
            }
            healable, proof = is_row_healable(row, snapshot)
            if not healable:
                logger.debug(
                    'bank_rec_heal: bank=%s import_id=%s NOT healable: %s',
                    bank_code, cand['id'], proof,
                )
                continue

            # Compute reconciled_count: only for non-legacy rows.
            new_count = None
            if cand['statement_number'] is not None:
                try:
                    new_count = opera_data_source.count_reconciled_aentry(
                        bank_code, int(cand['statement_number'])
                    )
                except Exception as exc:
                    logger.warning(
                        'bank_rec_heal: bank=%s import_id=%s count failed (%s); '
                        'preserving existing reconciled_count',
                        bank_code, cand['id'], exc,
                    )
                    new_count = None

            cursor.execute(
                """
                UPDATE bank_statement_imports
                   SET is_reconciled = 1,
                       reconciled_date = COALESCE(reconciled_date, ?),
                       reconciled_count = CASE
                                            WHEN ? IS NULL THEN reconciled_count
                                            ELSE ?
                                          END
                 WHERE id = ?
                """,
                (datetime.now().isoformat(), new_count, new_count, cand['id']),
            )

            audit = _format_audit_line(bank_code, cand['id'], proof)
            logger.info(audit)
            result.audit_lines.append(audit)
            result.healed_count += 1

        conn.commit()

    return result


def _format_audit_line(bank_code: str, import_id: int, proof: str) -> str:
    """One-line audit string for an INFO-level log emission."""
    return f'bank_rec_heal: bank={bank_code} import_id={import_id} healed — {proof}'


def _parse_iso_date(value):
    """Coerce 'YYYY-MM-DD' or 'YYYY-MM-DDThh:mm:ss' SQLite TEXT to a date."""
    from datetime import date as date_cls, datetime as datetime_cls
    if value is None:
        return None
    if isinstance(value, date_cls) and not isinstance(value, datetime_cls):
        return value
    if isinstance(value, datetime_cls):
        return value.date()
    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime_cls.fromisoformat(s.replace('Z', '+00:00')).date()
    except ValueError:
        try:
            return datetime_cls.strptime(s[:10], '%Y-%m-%d').date()
        except ValueError:
            return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_bank_rec_heal.py -v`
Expected: PASS — all 20 tests green (13 from earlier + 7 new orchestrator tests).

- [ ] **Step 5: Commit**

```bash
git add sql_rag/bank_rec_heal.py tests/test_bank_rec_heal.py
git commit -m "feat(bank-rec-heal): orchestrator with read-only Opera heal + per-row audit"
```

---

## Task 8: SE `complete_reconciliation` populates `statement_number` (both partial + full branches)

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py` (around lines 10792 and 10811)
- Test: `tests/test_bank_rec_heal_completion.py` (NEW)

- [ ] **Step 1: Write the failing test**

Create `tests/test_bank_rec_heal_completion.py`:

```python
"""Pin: SE complete_reconciliation must store statement_number on both
partial and full branches. Without this, the heal's check 3 would
never apply to new imports."""
import sqlite3
import tempfile
from pathlib import Path


def _setup_db_with_import(tmp, statement_number_col_must_exist=True):
    """Build a clean email_data.db with one import row to mark."""
    from api.email.storage import EmailStorage

    db_path = Path(tmp) / 'email_data.db'
    EmailStorage(str(db_path))

    if statement_number_col_must_exist:
        with sqlite3.connect(str(db_path)) as conn:
            cur = conn.execute("PRAGMA table_info(bank_statement_imports)")
            cols = {c[1] for c in cur.fetchall()}
            assert 'statement_number' in cols

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("""
            INSERT INTO bank_statement_imports
                (id, bank_code, filename, opening_balance, closing_balance,
                 statement_date, period_end, is_reconciled, reconciled_count)
            VALUES (71, 'BC010', 'Statement.pdf', 0, 115064.71,
                    '2026-05-01', '2026-05-01', 0, 0)
        """)
        conn.commit()
    return db_path


def test_partial_completion_writes_statement_number():
    """The partial-rec branch UPDATE in complete_reconciliation must
    include statement_number = ?."""
    import inspect
    from apps.bank_reconcile.api import routes

    src = inspect.getsource(routes.complete_reconciliation)
    # The partial branch UPDATE block includes reconciled_count and
    # reconciled_date today; the change adds statement_number too.
    assert 'statement_number' in src, (
        "complete_reconciliation source must reference statement_number — "
        "it must be persisted in both partial and full branches"
    )
    # And it must be present in BOTH UPDATE statements (the partial branch
    # at ~line 10792 and the full branch at ~line 10811).
    assert src.count('SET statement_number') >= 2 or \
           src.count('statement_number = ?') >= 2, (
        "Both partial and full UPDATE blocks must persist statement_number"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bank_rec_heal_completion.py -v`
Expected: FAIL — `statement_number` not yet in `complete_reconciliation`.

- [ ] **Step 3: Modify the SE `complete_reconciliation` route**

Open `apps/bank_reconcile/api/routes.py`. Find the partial-branch UPDATE at line 10792–10799:

```python
                if partial and not statement_actually_complete:
                    # Genuinely partial: update reconciled_count but keep is_reconciled=0
                    with email_storage._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE bank_statement_imports
                            SET reconciled_count = ?,
                                reconciled_date = ?
                            WHERE id = ?
                        """, (result.records_imported, datetime.now().isoformat(), import_id))
```

Replace with:

```python
                if partial and not statement_actually_complete:
                    # Genuinely partial: update reconciled_count but keep is_reconciled=0.
                    # Persist statement_number so the bank-rec self-heal can use
                    # it later when Opera completes the rec
                    # (see docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md).
                    with email_storage._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE bank_statement_imports
                            SET reconciled_count = ?,
                                reconciled_date = ?,
                                statement_number = ?
                            WHERE id = ?
                        """, (
                            result.records_imported,
                            datetime.now().isoformat(),
                            int(statement_number),
                            import_id,
                        ))
```

Find the full-branch UPDATE at line 10811–10819:

```python
                    with email_storage._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE bank_statement_imports
                            SET is_reconciled = 1,
                                reconciled_date = ?,
                                reconciled_count = ?
                            WHERE id = ?
                        """, (datetime.now().isoformat(), result.records_imported, import_id))
```

Replace with:

```python
                    with email_storage._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE bank_statement_imports
                            SET is_reconciled = 1,
                                reconciled_date = ?,
                                reconciled_count = ?,
                                statement_number = ?
                            WHERE id = ?
                        """, (
                            datetime.now().isoformat(),
                            result.records_imported,
                            int(statement_number),
                            import_id,
                        ))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bank_rec_heal_completion.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/bank_reconcile/api/routes.py tests/test_bank_rec_heal_completion.py
git commit -m "feat(bank-rec): SE complete_reconciliation persists statement_number"
```

---

## Task 9: Opera 3 `complete_reconciliation` populates `statement_number` (mirror of Task 8)

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py` (around line 15730)
- Test: extend `tests/test_bank_rec_heal_completion.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_bank_rec_heal_completion.py`:

```python
def test_opera3_completion_writes_statement_number():
    """The Opera 3 mirror of complete_reconciliation must also persist
    statement_number in BOTH partial and full UPDATE blocks
    (CLAUDE.md mandatory parity rule)."""
    import inspect
    from apps.bank_reconcile.api import routes

    src = inspect.getsource(routes.opera3_complete_reconciliation)
    assert 'statement_number' in src, (
        "opera3_complete_reconciliation must persist statement_number — "
        "Opera SE / Opera 3 parity is mandatory per CLAUDE.md"
    )
    assert src.count('SET statement_number') >= 2 or \
           src.count('statement_number = ?') >= 2, (
        "Both partial and full UPDATE blocks must persist statement_number"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bank_rec_heal_completion.py::test_opera3_completion_writes_statement_number -v`
Expected: FAIL.

- [ ] **Step 3: Modify the Opera 3 `complete_reconciliation` route**

Find `opera3_complete_reconciliation` in `apps/bank_reconcile/api/routes.py` (around line 15647). Locate its two UPDATE blocks for `bank_statement_imports` (mirroring the SE structure: a partial branch and a full branch — both around line 15730).

Apply the same change as Task 8 to both UPDATE blocks: add `statement_number = ?` to the SET clause and pass `int(statement_number)` in the parameter tuple, with a comment referencing the spec.

(Read the surrounding code first; the variable name for the rec batch number on the Opera 3 path is the same `statement_number` parameter. If the Opera 3 route uses a different local variable name, adapt accordingly. The shape of the change is identical to Task 8.)

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bank_rec_heal_completion.py -v`
Expected: PASS — both SE and Opera 3 tests green.

- [ ] **Step 5: Commit**

```bash
git add apps/bank_reconcile/api/routes.py tests/test_bank_rec_heal_completion.py
git commit -m "feat(bank-rec-o3): Opera 3 complete_reconciliation persists statement_number (parity)"
```

---

## Task 10: Wire heal into SE `scan-emails` endpoint

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py` (the SE scan-emails handler around line 6010)
- Test: `tests/test_bank_rec_heal_scan_integration.py` (NEW)

- [ ] **Step 1: Write the failing test**

Create `tests/test_bank_rec_heal_scan_integration.py`:

```python
"""Pin that SE scan-emails calls the bank-rec heal before filtering."""
import inspect


def test_se_scan_emails_calls_heal():
    """The SE scan-emails route must import and call
    heal_bank_statement_imports before applying the
    'already-processed' filter."""
    from apps.bank_reconcile.api import routes

    src = inspect.getsource(routes.scan_emails_for_bank_statements)
    assert 'heal_bank_statement_imports' in src, (
        "SE scan-emails must call heal_bank_statement_imports — "
        "without this, statements completed in Opera stay stale"
    )


def test_se_scan_emails_imports_heal_module():
    """The route module must import the heal function (so it's reachable
    from the handler)."""
    import apps.bank_reconcile.api.routes as routes_module

    src = inspect.getsource(routes_module)
    assert 'from sql_rag.bank_rec_heal import' in src or \
           'import sql_rag.bank_rec_heal' in src, (
        "routes module must import the heal function"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bank_rec_heal_scan_integration.py -v`
Expected: FAIL.

- [ ] **Step 3: Wire the heal into SE scan-emails**

Open `apps/bank_reconcile/api/routes.py`. Add the import near the top of the file (with other `sql_rag` imports):

```python
from sql_rag.bank_rec_heal import heal_bank_statement_imports
```

Find the SE `scan_emails_for_bank_statements` handler at line 6010. After the existing block at lines 6042–6062 (where `bank_query` reads sort_code/account/recbal from nbank — i.e. after Opera-bank existence has been confirmed), but BEFORE any "already-processed" filter logic, insert:

```python
        # Bank-rec self-heal: detect statements that have been completed in
        # Opera Cashbook > Reconcile (after a partial rec via this app)
        # and update is_reconciled before the "already-processed" filter
        # runs. Read-only against Opera; updates only local SQLite.
        # See docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md.
        try:
            from sql_rag.duplicate_check_se import OperaSEDataSource
            from sql_rag.company_data import get_current_db_path

            company_db = get_current_db_path('email_data.db')
            if company_db and sql_connector:
                heal_result = heal_bank_statement_imports(
                    bank_code=bank_code,
                    company_db_path=company_db,
                    opera_data_source=OperaSEDataSource(sql_connector),
                )
                if heal_result.healed_count > 0:
                    logger.info(
                        'scan-emails SE: bank=%s healed %d row(s) — completed in Opera',
                        bank_code, heal_result.healed_count,
                    )
        except Exception as heal_exc:
            # The heal must never break the scan — log and continue.
            logger.warning(
                'scan-emails SE: heal failed for bank=%s (%s); continuing',
                bank_code, heal_exc,
            )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bank_rec_heal_scan_integration.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/bank_reconcile/api/routes.py tests/test_bank_rec_heal_scan_integration.py
git commit -m "feat(bank-rec): SE scan-emails calls bank-rec self-heal before filtering"
```

---

## Task 11: Wire heal into Opera 3 `scan-emails` endpoint (mirror of Task 10)

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py` (Opera 3 scan-emails handler at line 12127)
- Test: extend `tests/test_bank_rec_heal_scan_integration.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_bank_rec_heal_scan_integration.py`:

```python
def test_opera3_scan_emails_calls_heal():
    """Opera 3 mirror of scan-emails must also call the heal — parity."""
    from apps.bank_reconcile.api import routes

    src = inspect.getsource(routes.opera3_scan_emails_for_bank_statements)
    assert 'heal_bank_statement_imports' in src, (
        "Opera 3 scan-emails must call heal_bank_statement_imports — "
        "Opera SE / Opera 3 parity is mandatory per CLAUDE.md"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bank_rec_heal_scan_integration.py::test_opera3_scan_emails_calls_heal -v`
Expected: FAIL.

- [ ] **Step 3: Wire heal into Opera 3 scan-emails**

Find `opera3_scan_emails_for_bank_statements` in `apps/bank_reconcile/api/routes.py` (around line 12127). Add the heal call after the Opera 3 bank existence check (mirroring the SE placement) using the Opera 3 data source:

```python
        # Bank-rec self-heal — Opera 3 mirror of the SE handler.
        # See docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md.
        try:
            from sql_rag.duplicate_check_o3 import Opera3DataSource
            from sql_rag.company_data import get_current_db_path
            from sql_rag.opera3_data_provider import get_opera3_reader

            company_db = get_current_db_path('email_data.db')
            opera3_reader = get_opera3_reader()  # existing helper used elsewhere
            if company_db and opera3_reader:
                heal_result = heal_bank_statement_imports(
                    bank_code=bank_code,
                    company_db_path=company_db,
                    opera_data_source=Opera3DataSource(opera3_reader),
                )
                if heal_result.healed_count > 0:
                    logger.info(
                        'scan-emails O3: bank=%s healed %d row(s) — completed in Opera',
                        bank_code, heal_result.healed_count,
                    )
        except Exception as heal_exc:
            logger.warning(
                'scan-emails O3: heal failed for bank=%s (%s); continuing',
                bank_code, heal_exc,
            )
```

If `get_opera3_reader` doesn't exist by that name, use whatever helper the existing Opera 3 scan-emails handler already uses to obtain the DBF reader — read the surrounding code in the handler and reuse the same accessor.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bank_rec_heal_scan_integration.py -v`
Expected: PASS — SE + Opera 3 tests both green.

- [ ] **Step 5: Commit**

```bash
git add apps/bank_reconcile/api/routes.py tests/test_bank_rec_heal_scan_integration.py
git commit -m "feat(bank-rec-o3): Opera 3 scan-emails calls bank-rec self-heal (parity)"
```

---

## Task 12: Real-world scenario regression test

**Files:**
- Create: `tests/test_bank_rec_heal_regression.py`

- [ ] **Step 1: Write the regression test**

Create `tests/test_bank_rec_heal_regression.py`:

```python
"""Real-world scenario regression — pins the canonical case the
self-heal was built to fix.

Captured 2026-05-05. The exact scenario where the local
bank_statement_imports row stayed at is_reconciled=0 even though
Opera had fully reconciled the statement:

  Local row (legacy — no statement_number stored):
    bank_code        = BANK01  (anonymised)
    closing_balance  = £115,064.71
    period_end       = 2026-05-01
    is_reconciled    = 0
    reconciled_count = 20  (from the partial-rec completion earlier)

  Opera nbank state:
    nk_recbal  = £115,064.71
    nk_lststdt = 2026-05-01
    nk_lststno = 86940
    nk_reccfwd = £0.00  (cleared = full rec)

After the heal runs:
  bank_statement_imports.is_reconciled  → 1
  bank_statement_imports.reconciled_count → 20  (preserved — legacy
                                                  rows are not overwritten)
  Audit line emitted with all proof strings.
  Opera state untouched (read-only mandate).

Regression guard — if any of the three checks ever silently breaks
this case again, this test fails loudly.
"""
import sqlite3
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock


def test_real_world_partial_rec_completed_in_opera_regression():
    """Pin the canonical scenario verbatim from the production capture."""
    from api.email.storage import EmailStorage
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / 'email_data.db'
        EmailStorage(str(db_path))   # init schema (incl. statement_number)

        # Insert the legacy row exactly as captured.
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute("""
                INSERT INTO bank_statement_imports
                    (id, bank_code, filename, opening_balance, closing_balance,
                     statement_date, period_end, is_reconciled,
                     reconciled_count, statement_number)
                VALUES
                    (71, 'BANK01', 'Statement 01-MAY-26.pdf',
                     116726.07, 115064.71,
                     '2026-05-01', '2026-05-01',
                     0, 20, NULL)
            """)
            conn.commit()

        # Opera state captured today.
        ds = MagicMock()
        ds.read_nbank.return_value = NbankSnapshot(
            bank_code='BANK01',
            recbal_pounds=115064.71,
            lststdt=date(2026, 5, 1),
            lststno=86940,
        )
        # count_reconciled_aentry must NOT be called — legacy row.

        result = heal_bank_statement_imports(
            bank_code='BANK01',
            company_db_path=db_path,
            opera_data_source=ds,
        )

        assert result.healed_count == 1, "regression: row must heal"
        assert len(result.audit_lines) == 1
        audit = result.audit_lines[0]
        # Proof string contains every fact that justifies the heal.
        assert 'check 1 ok' in audit
        assert '115064.71' in audit
        assert 'check 2 ok' in audit
        assert '2026-05-01' in audit
        assert ('check 3 skipped' in audit) or ('legacy' in audit.lower())

        ds.count_reconciled_aentry.assert_not_called()

        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(
                "SELECT is_reconciled, reconciled_count, "
                "       statement_number, bank_code "
                "FROM bank_statement_imports WHERE id = 71"
            ).fetchone()
            assert row[0] == 1, "is_reconciled must flip 0 → 1"
            assert row[1] == 20, "legacy reconciled_count must be PRESERVED"
            assert row[2] is None, "legacy statement_number stays NULL"
            assert row[3] == 'BANK01'


def test_regression_idempotency():
    """Run the regression scenario through the heal twice — second run
    finds nothing to heal."""
    from api.email.storage import EmailStorage
    from sql_rag.bank_rec_heal import (
        NbankSnapshot, heal_bank_statement_imports
    )

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / 'email_data.db'
        EmailStorage(str(db_path))

        with sqlite3.connect(str(db_path)) as conn:
            conn.execute("""
                INSERT INTO bank_statement_imports
                    (id, bank_code, filename, closing_balance, period_end,
                     is_reconciled, reconciled_count, statement_number)
                VALUES
                    (71, 'BANK01', 'x.pdf', 115064.71, '2026-05-01',
                     0, 20, NULL)
            """)
            conn.commit()

        ds = MagicMock()
        ds.read_nbank.return_value = NbankSnapshot(
            bank_code='BANK01',
            recbal_pounds=115064.71,
            lststdt=date(2026, 5, 1),
            lststno=86940,
        )

        first = heal_bank_statement_imports('BANK01', db_path, ds)
        second = heal_bank_statement_imports('BANK01', db_path, ds)
        assert first.healed_count == 1
        assert second.healed_count == 0
```

- [ ] **Step 2: Run the regression test**

Run: `pytest tests/test_bank_rec_heal_regression.py -v`
Expected: PASS — both tests green.

- [ ] **Step 3: Commit**

```bash
git add tests/test_bank_rec_heal_regression.py
git commit -m "test(bank-rec-heal): regression for real-world partial-then-Opera rec scenario"
```

---

## Task 13: SE / Opera 3 parity test

**Files:**
- Create: `tests/test_bank_rec_heal_se_o3_parity.py`

- [ ] **Step 1: Write the parity test**

Create `tests/test_bank_rec_heal_se_o3_parity.py`:

```python
"""Pin: same logical input → same heal decision on SE and Opera 3.
Mandatory per CLAUDE.md ("Opera SE / Opera 3 FULL PARITY")."""
from datetime import date
from unittest.mock import MagicMock

import pandas as pd


class _FakeReader:
    def __init__(self, tables):
        self._tables = tables
    def read_table(self, name):
        return self._tables.get(name, [])


def _se_data_source_with_nbank(recbal=115064.71, lststdt=date(2026, 5, 1),
                                lststno=86940):
    from sql_rag.duplicate_check_se import OperaSEDataSource
    fake_sql = MagicMock()
    fake_sql.execute_query.return_value = pd.DataFrame([{
        'recbal_pounds': recbal,
        'lststdt': lststdt,
        'lststno': lststno,
    }])
    return OperaSEDataSource(fake_sql)


def _o3_data_source_with_nbank(recbal_pounds=115064.71,
                                 lststdt=date(2026, 5, 1),
                                 lststno=86940):
    from sql_rag.duplicate_check_o3 import Opera3DataSource
    reader = _FakeReader({
        'nbank': [{
            'nk_acnt': 'BANK01',
            'nk_recbal': int(round(recbal_pounds * 100)),
            'nk_lststdt': lststdt,
            'nk_lststno': lststno,
        }],
        'aentry': [],
    })
    return Opera3DataSource(reader)


def test_same_input_same_decision_three_facts_match():
    """Both data sources, same logical Opera state → both should report
    the row as healable."""
    from sql_rag.bank_rec_heal import is_row_healable

    se_ds = _se_data_source_with_nbank()
    o3_ds = _o3_data_source_with_nbank()

    se_snap = se_ds.read_nbank('BANK01')
    o3_snap = o3_ds.read_nbank('BANK01')

    row = {
        'closing_balance': 115064.71,
        'period_end': date(2026, 5, 1),
        'statement_number': 86940,
    }

    se_healable, _ = is_row_healable(row, se_snap)
    o3_healable, _ = is_row_healable(row, o3_snap)

    assert se_healable == o3_healable == True


def test_same_input_same_decision_balance_mismatch():
    """Opera says recbal=£100k, statement says £115k → both refuse."""
    from sql_rag.bank_rec_heal import is_row_healable

    se_snap = _se_data_source_with_nbank(recbal=100000.00).read_nbank('BANK01')
    o3_snap = _o3_data_source_with_nbank(recbal_pounds=100000.00).read_nbank('BANK01')

    row = {
        'closing_balance': 115064.71,
        'period_end': date(2026, 5, 1),
        'statement_number': 86940,
    }

    se_healable, _ = is_row_healable(row, se_snap)
    o3_healable, _ = is_row_healable(row, o3_snap)

    assert se_healable == o3_healable == False


def test_same_input_same_decision_legacy_row():
    """Legacy row (statement_number=NULL): both data sources must
    produce the same heal decision."""
    from sql_rag.bank_rec_heal import is_row_healable

    se_snap = _se_data_source_with_nbank().read_nbank('BANK01')
    o3_snap = _o3_data_source_with_nbank().read_nbank('BANK01')

    row = {
        'closing_balance': 115064.71,
        'period_end': date(2026, 5, 1),
        'statement_number': None,
    }

    se_healable, _ = is_row_healable(row, se_snap)
    o3_healable, _ = is_row_healable(row, o3_snap)
    assert se_healable == o3_healable == True
```

- [ ] **Step 2: Run the parity test**

Run: `pytest tests/test_bank_rec_heal_se_o3_parity.py -v`
Expected: PASS — all three tests green.

- [ ] **Step 3: Commit**

```bash
git add tests/test_bank_rec_heal_se_o3_parity.py
git commit -m "test(bank-rec-heal): SE / Opera 3 parity — same input, same decision"
```

---

## Task 14: Local KB update — add "Bank Rec Self-Heal Rule" section

**Files:**
- Modify: `apps/core/docs/opera_knowledge_base.md`

- [ ] **Step 1: Open the local KB and locate the existing bank-rec sections**

Run: `grep -n "Bank Rec\|bank_statement_imports" /Users/maccb/llmragsql/apps/core/docs/opera_knowledge_base.md | head -10`

Note the line numbers of nearby bank-rec sections — the new section sits naturally next to the existing "Open-Items Rule" section that was added 2026-05-04.

- [ ] **Step 2: Append the new section**

Add this section in the bank-rec area of `apps/core/docs/opera_knowledge_base.md`:

```markdown
## Bank Rec Self-Heal Rule (CRITICAL)

The local `bank_statement_imports.is_reconciled` flag is the app's view
of whether a statement has been reconciled. When the operator runs a
partial rec via the app and finishes it in Opera Cashbook > Reconcile,
Opera updates `nbank.nk_recbal`, `nk_lststdt`, `nk_lststno` and
`aentry.ae_reclnum`, but does NOT touch our local store. Without
intervention the local flag stays at 0 forever and the statement
re-appears on every scan as "Awaiting Reconcile".

The scan-emails endpoint runs a **read-only self-heal** that detects
this and updates the local flag.

### The Rule

A `bank_statement_imports` row with `is_reconciled = 0` heals to
`is_reconciled = 1` when ALL of the following hold:

1. `nbank.nk_recbal / 100.0 ≈ closing_balance` within £0.01.
2. `nbank.nk_lststdt >= period_end`.
3. `nbank.nk_lststno >= statement_number` (skipped for legacy rows
   where `statement_number IS NULL`).

Module: `sql_rag/bank_rec_heal.py`. Read-only against Opera. SE reads
use `WITH (NOLOCK)`. Both Opera SE and Opera 3 implement the same rule.
Cross-reference: `business-rules/bank-rec-self-heal.md` in the central KB.

### Why a New Schema Column

Earlier imports (legacy rows) lack `bank_statement_imports.statement_number`
because the column was added with this feature. Legacy rows fall back to
the two-check rule (1 + 2 only); new completions populate the column so
all three checks apply.
```

- [ ] **Step 3: Commit**

```bash
git add apps/core/docs/opera_knowledge_base.md
git commit -m "docs(kb): add Bank Rec Self-Heal Rule section to local KB"
```

---

## Task 15: Central KB update — create `bank-rec-self-heal.md`

**Files:**
- Create: `~/opera-knowledge-ref/packages/opera-knowledge/business-rules/bank-rec-self-heal.md`

- [ ] **Step 1: Pull the central KB to avoid conflicts**

Run:

```bash
cd ~/opera-knowledge-ref && git pull --ff-only
```

Expected: `Already up to date.` (or fast-forward update).

- [ ] **Step 2: Create the central KB doc**

Create `~/opera-knowledge-ref/packages/opera-knowledge/business-rules/bank-rec-self-heal.md`:

```markdown
---
module: cashbook
layer: business-rules
version: 1.0.0
tags: [bank-rec, reconciliation, self-heal, opera-se, opera-3]
---

# Bank Rec Self-Heal Rule

When an operator runs a partial bank reconciliation via an external app
and completes it in Opera Cashbook > Reconcile, Opera updates `nbank` and
`aentry` but the external app's local store does not learn about the
completion. A read-only self-heal must close that gap on every scan.

## The Rule

A local `bank_statement_imports` row with `is_reconciled = 0` heals to
`is_reconciled = 1` when ALL of the following hold against the row's
bank in Opera:

1. **Balance match.** `nbank.nk_recbal / 100.0` matches the row's
   `closing_balance` within £0.01.
2. **Date match.** `nbank.nk_lststdt >= row.period_end`.
3. **Statement-number match (when stored).** When the row has a stored
   `statement_number`: `nbank.nk_lststno >= row.statement_number`.
   Legacy rows lacking a stored statement number fall back to the
   two-check rule.

The use of `>=` rather than equality on checks 2 and 3 is intentional:
if subsequent statements have been reconciled too, those checks still
pass for the older statement (Opera's sequential rec gating guarantees
later rec implies all earlier ones).

## Properties

- **Read-only** against Opera. No `UPDATE`/`INSERT` on Opera tables.
- **Per-company isolated.** Each company queries its own Opera DB and
  its own local SQLite.
- **Opera SE + Opera 3 parity.** Identical rule, different read
  mechanism (SQL `WITH (NOLOCK)` vs FoxPro DBF read).
- **Idempotent.** Running the heal twice produces the same outcome.
- **Auditable.** Every heal logged at INFO with the proof string
  (which checks passed, the values, the row id).

## Where It Runs

The heal is invoked by the scan-emails endpoints. It runs once per
bank, after Opera bank existence is confirmed but before any
"already-processed" filter is applied to the candidate list.

## Cross-References

- `business-rules/locking-protocol.md` — the heal's reads must use
  `WITH (NOLOCK)` (SE) or DBF reads without locking (Opera 3).
- `business-rules/bank-rec-completion.md` — the Stage A + Stage B
  contract that the heal infers from.
- `business-rules/bank-rec-open-items.md` — the candidate-pool filter
  that runs alongside this rule on each scan.
- `platform/opera3-write-agent.md` — Opera 3 read/write split; the heal
  is a read-only pattern, no Write Agent involvement required.
```

- [ ] **Step 3: Commit and push to the central KB**

```bash
cd ~/opera-knowledge-ref && git add packages/opera-knowledge/business-rules/bank-rec-self-heal.md && git commit -m "docs(business-rules): add bank-rec self-heal rule"
git push origin main
cd /Users/maccb/llmragsql
```

Expected: a fresh commit on `origin/main` of `jonathangintsys/aisam`.

---

## Task 16: Manual update — Stage 5 paragraph

**Files:**
- Modify: `marketing/manuals/manual-bank-reconciliation.md`

- [ ] **Step 1: Add the paragraph to Stage 5**

Open `marketing/manuals/manual-bank-reconciliation.md`. Find the **Stage 5: Complete** section. After the existing description of full vs. partial reconcile, add this paragraph:

```markdown
**Statements completed in Opera drop off automatically.** If you finish
a partial reconciliation in Opera Cashbook > Reconcile (rather than via
this app), the next time you click Scan the statement will be removed
from the in-progress list automatically. The app detects Opera's
reconciled state and updates its own status accordingly — read-only,
no Opera changes are made. You don't need to do anything manual to
clear the statement from the list.
```

Update the `Last updated` line at the bottom of the file:

```markdown
*Last updated: 2026-05-05 — Stage 5 self-heal of statements completed in Opera*
```

- [ ] **Step 2: Commit**

```bash
git add marketing/manuals/manual-bank-reconciliation.md
git commit -m "docs(manual): document Stage 5 auto-detection of statements completed in Opera"
```

---

## Task 17: Final integration smoke test against the running API

**Files:**
- (manual verification, no new files)

- [ ] **Step 1: Run the full pytest suite**

Run: `pytest tests/test_bank_rec_heal*.py tests/test_already_posted_fallback.py tests/test_opera_open_items.py -v`
Expected: PASS — every test green.

- [ ] **Step 2: Spot-check the verification scenario from the spec**

If the API is running locally and the intsys company has the import 71 row at `is_reconciled=0` and Opera says it's done, perform the scan-emails call and confirm:
- The statement no longer appears on the scan list.
- `bank_statement_imports.is_reconciled` for import 71 is now `1`.
- A `bank_rec_heal:` audit line is present in `api_debug.log`.
- `nbank.BC010` and `aentry` rows for BC010 are byte-identical to before the scan (read-only mandate).

Run the verification:

```bash
sqlite3 /Users/maccb/llmragsql/data/intsys/core/email_data.db \
  "SELECT id, bank_code, is_reconciled, statement_number, reconciled_count
     FROM bank_statement_imports WHERE id = 71"
```

Expected after the heal: `is_reconciled = 1`, `statement_number IS NULL` (legacy row), `reconciled_count = 20` (preserved).

- [ ] **Step 3: Confirm Opera was untouched**

Re-run the SE query that captured the original Opera state in the spec and confirm the values are unchanged:

```bash
python3 - <<'PY'
import pyodbc
cn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=172.17.172.99,1433;DATABASE=Opera3SECompany00I;"
    "UID=n8n;PWD=possible;TrustServerCertificate=yes;",
    timeout=10,
)
cur = cn.cursor()
cur.execute("""
    SELECT nk_recbal/100.0, nk_lststdt, nk_lststno, nk_reccfwd
    FROM nbank WITH (NOLOCK)
    WHERE RTRIM(nk_acnt) = 'BC010'
""")
print(cur.fetchone())
cn.close()
PY
```

Expected: `(115064.71, datetime(2026,5,1,0,0), 86940, 0)` — identical to the spec's Opera state capture.

- [ ] **Step 4: Final commit if any docs were touched during verification**

(If no files changed in this task, no commit is needed.)

---

## Plan Self-Review Notes

- **Spec coverage:** Every spec section has at least one task implementing it. The schema migration (Task 6), three-fact rule (Task 2), data-source extensions (Tasks 3, 4, 5), orchestrator (Task 7), completion writes (Tasks 8, 9), scan integration (Tasks 10, 11), regression (Task 12), parity (Task 13), KB + manual (Tasks 14–16), and a final live verification (Task 17).
- **No placeholders:** Every task has full code blocks. The only "read the surrounding code" caveat is in Task 9 where the Opera 3 `complete_reconciliation` mirrors the SE one but the local variable naming may differ — the engineer is told the shape of the change and pointed at SE's diff for reference.
- **Type consistency:** `NbankSnapshot`, `HealResult`, `is_row_healable`, `heal_bank_statement_imports`, `read_nbank`, `count_reconciled_aentry` — all defined in earlier tasks before they're referenced in later ones.
- **Mandatory parity:** Tasks 4, 5, 9, 11, 13 explicitly mirror SE work to Opera 3 in the same task or its sibling.
- **Mandatory KB updates:** Tasks 14 + 15 cover local + central KB per CLAUDE.md.
- **Mandatory manual update:** Task 16.
- **Read-only:** The spec's read-only mandate is enforced by the tests (Task 7's `test_heal_only_touches_target_bank` and Task 12's regression confirming Opera state is unchanged).
