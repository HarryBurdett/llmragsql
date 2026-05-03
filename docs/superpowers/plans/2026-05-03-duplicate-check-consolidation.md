# Duplicate-Check Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace six scattered duplicate-check implementations across analyse-time and post-time with one type-aware, sign-aware function that both flows call. Apply equally to Opera SE and Opera 3.

**Architecture:** A new module `sql_rag/duplicate_check.py` exposes `check_for_duplicate(...)` returning a `DuplicateCheckResult`. A `DataSource` protocol is implemented for both Opera SE and Opera 3. Existing call sites (`bank_import.py::_is_already_posted`, `opera_sql_import.py::check_duplicate_before_posting`, `bank_duplicates.py::find_duplicates` "exact" path, plus their Opera 3 mirrors) become thin wrappers that delegate to the new function while preserving their existing return shapes.

**Tech Stack:** Python 3.9, pyodbc (Opera SE), pytest, no new external deps.

**Source spec:** `docs/superpowers/specs/2026-05-03-duplicate-check-consolidation-design.md`

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `sql_rag/duplicate_check.py` | **create** | The function, types, action-type mapping, DataSource protocol |
| `sql_rag/duplicate_check_se.py` | **create** | Opera SE DataSource (uses SQLConnector) |
| `sql_rag/duplicate_check_o3.py` | **create** | Opera 3 DataSource (uses FoxPro reader) |
| `tests/test_duplicate_check.py` | **create** | Unit tests with fixture DataSource |
| `tests/test_duplicate_check_regression.py` | **create** | Regression tests for historical bugs (sign-blind ABS, type filter drops, pt_ref/st_ref typos, Cloudsis P051 case) |
| `sql_rag/bank_import.py` | **modify** | `_is_already_posted` delegates to new function |
| `sql_rag/opera_sql_import.py` | **modify** | `check_duplicate_before_posting` delegates to new function |
| `sql_rag/bank_duplicates.py` | **modify** | `find_duplicates` "exact" path delegates to new function |
| `sql_rag/bank_import_opera3.py` | **modify** | Opera 3 mirror of `_is_already_posted` |
| `sql_rag/opera3_foxpro_import.py` | **modify** | Opera 3 mirror of `check_duplicate_before_posting` |
| `apps/core/docs/opera_knowledge_base.md` | **modify** | Document the function |
| `~/opera-knowledge-ref/.../business-rules/duplicate-check.md` | **create** | Central KB docs |

---

## Task 1: Define types and action-type map

**Files:**
- Create: `sql_rag/duplicate_check.py` (skeleton — types only, no logic yet)
- Test: `tests/test_duplicate_check.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_duplicate_check.py
"""Unit tests for sql_rag.duplicate_check — single-source-of-truth
duplicate detection for bank reconcile."""
from __future__ import annotations

from sql_rag.duplicate_check import (
    DuplicateCheckResult,
    DuplicateMatchKind,
    ACTION_TYPE_MAP,
    DataSource,
)


def test_match_kind_enum_has_required_values():
    expected = {'NONE', 'CASHBOOK_DUPLICATE', 'LEDGER_ALLOCATION_TARGET'}
    actual = {k.name for k in DuplicateMatchKind}
    assert actual == expected


def test_match_kind_value_strings_are_stable():
    """Lock the .value strings — they appear in logs and audit fields."""
    assert DuplicateMatchKind.NONE.value == "none"
    assert DuplicateMatchKind.CASHBOOK_DUPLICATE.value == "cashbook_duplicate"
    assert DuplicateMatchKind.LEDGER_ALLOCATION_TARGET.value == "ledger_allocation_target"


def test_result_dataclass_fields():
    r = DuplicateCheckResult(
        kind=DuplicateMatchKind.NONE,
        matched_table=None,
        matched_entry=None,
        reason="no match",
    )
    assert r.kind is DuplicateMatchKind.NONE
    assert r.matched_table is None
    assert r.matched_entry is None
    assert r.reason == "no match"


def test_action_type_map_covers_all_actions():
    """Every action the bank-import flow can produce must have a mapping."""
    required_actions = {
        'sales_receipt', 'sales_refund',
        'purchase_payment', 'purchase_refund',
        'nominal_payment', 'nominal_receipt',
        'bank_transfer',
    }
    assert required_actions.issubset(set(ACTION_TYPE_MAP.keys()))


def test_action_type_map_has_correct_at_types():
    """The at_type values must match Opera's cashbook conventions
    (CLAUDE.md / opera_knowledge_base.md):
      1=Nominal Pmt, 2=Nominal Rcpt, 3=Sales Refund,
      4=Sales Receipt, 5=Purchase Pmt, 6=Purchase Refund, 8=Bank Transfer
    """
    assert ACTION_TYPE_MAP['nominal_payment']['at_type'] == 1
    assert ACTION_TYPE_MAP['nominal_receipt']['at_type'] == 2
    assert ACTION_TYPE_MAP['sales_refund']['at_type'] == 3
    assert ACTION_TYPE_MAP['sales_receipt']['at_type'] == 4
    assert ACTION_TYPE_MAP['purchase_payment']['at_type'] == 5
    assert ACTION_TYPE_MAP['purchase_refund']['at_type'] == 6
    assert ACTION_TYPE_MAP['bank_transfer']['at_type'] == 8


def test_action_type_map_has_correct_ledger_types():
    """Ledger types per central KB:
      sales_receipt   → stran 'R'
      sales_refund    → stran 'F'
      purchase_payment → ptran 'P'
      purchase_refund → ptran 'F'
      nominal_*, bank_transfer → no ledger row
    """
    assert ACTION_TYPE_MAP['sales_receipt']['st_trtype'] == 'R'
    assert ACTION_TYPE_MAP['sales_refund']['st_trtype'] == 'F'
    assert ACTION_TYPE_MAP['purchase_payment']['pt_trtype'] == 'P'
    assert ACTION_TYPE_MAP['purchase_refund']['pt_trtype'] == 'F'
    # Nominal and bank-transfer actions have no ledger type
    assert ACTION_TYPE_MAP['nominal_payment'].get('st_trtype') is None
    assert ACTION_TYPE_MAP['nominal_payment'].get('pt_trtype') is None
    assert ACTION_TYPE_MAP['bank_transfer'].get('st_trtype') is None
    assert ACTION_TYPE_MAP['bank_transfer'].get('pt_trtype') is None


def test_datasource_protocol_signatures_pinned():
    import inspect
    sig = inspect.signature(DataSource.find_aentry_by_signed_value)
    params = list(sig.parameters)
    assert params == [
        'self', 'bank_code', 'date_from', 'date_to',
        'signed_pence', 'expected_at_type', 'exclude_entry_numbers',
    ], f"find_aentry_by_signed_value signature drifted: {params}"

    sig = inspect.signature(DataSource.find_stran_by_signed_value)
    params = list(sig.parameters)
    assert params == [
        'self', 'account_code', 'date_from', 'date_to',
        'signed_pounds', 'st_trtype',
    ], f"find_stran_by_signed_value signature drifted: {params}"

    sig = inspect.signature(DataSource.find_ptran_by_signed_value)
    params = list(sig.parameters)
    assert params == [
        'self', 'account_code', 'date_from', 'date_to',
        'signed_pounds', 'pt_trtype',
    ], f"find_ptran_by_signed_value signature drifted: {params}"

    class _Good:
        def find_aentry_by_signed_value(self, bank_code, date_from, date_to,
                                         signed_pence, expected_at_type,
                                         exclude_entry_numbers): return []
        def find_stran_by_signed_value(self, account_code, date_from, date_to,
                                        signed_pounds, st_trtype): return []
        def find_ptran_by_signed_value(self, account_code, date_from, date_to,
                                        signed_pounds, pt_trtype): return []
    class _Bad:
        def find_aentry_by_signed_value(self, *a, **kw): return []
        # missing the other two
    assert isinstance(_Good(), DataSource)
    assert not isinstance(_Bad(), DataSource)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'sql_rag.duplicate_check'`.

- [ ] **Step 3: Write minimal implementation**

```python
# sql_rag/duplicate_check.py
"""Single source of truth for bank-import duplicate detection.

Replaces six scattered implementations across analyse-time and
post-time, on Opera SE and Opera 3. See
docs/superpowers/specs/2026-05-03-duplicate-check-consolidation-design.md.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

__all__ = [
    "DuplicateMatchKind",
    "DuplicateCheckResult",
    "DataSource",
    "ACTION_TYPE_MAP",
    "check_for_duplicate",
]


class DuplicateMatchKind(Enum):
    """What the duplicate check found.

    Caller contract:
      NONE: safe to post — no matching entry in cashbook OR ledger.
      CASHBOOK_DUPLICATE: an aentry of the matching at_type and signed
        amount exists. Do NOT post — the transaction is already in
        Opera's cashbook.
      LEDGER_ALLOCATION_TARGET: no cashbook entry, but a credit-note-
        type stran/ptran row of the matching refund type exists. The
        bank line is NOT a duplicate — it's the missing payment for
        an existing credit note. POST it; optionally auto-allocate.
    """
    NONE = "none"
    CASHBOOK_DUPLICATE = "cashbook_duplicate"
    LEDGER_ALLOCATION_TARGET = "ledger_allocation_target"


@dataclass(frozen=True)
class DuplicateCheckResult:
    """Result of check_for_duplicate.

    Invariants:
      - kind=NONE: matched_table and matched_entry are None.
      - kind=CASHBOOK_DUPLICATE: matched_table='aentry', matched_entry
        is the ae_entry string.
      - kind=LEDGER_ALLOCATION_TARGET: matched_table is 'stran' or
        'ptran', matched_entry is the unique key (st_trref/pt_trref).
      - reason is always a non-empty human-readable string.
    """
    kind: DuplicateMatchKind
    matched_table: Optional[str]
    matched_entry: Optional[str]
    reason: str

    @property
    def is_duplicate(self) -> bool:
        """Convenience for callers that only care about the cashbook
        question (the existing _is_already_posted contract).
        """
        return self.kind is DuplicateMatchKind.CASHBOOK_DUPLICATE


# Mapping: bank-import action → Opera posting types
# ----------------------------------------------------------------
# Source: apps/core/docs/opera_knowledge_base.md "Cashbook Transaction
# Types (at_type)" plus the central KB business-rules. Locking this
# here as a single mapping prevents the per-callsite drift that
# bit us multiple times in production.
ACTION_TYPE_MAP: Dict[str, Dict[str, Any]] = {
    'sales_receipt':    {'at_type': 4, 'st_trtype': 'R'},
    'sales_refund':     {'at_type': 3, 'st_trtype': 'F'},
    'purchase_payment': {'at_type': 5, 'pt_trtype': 'P'},
    'purchase_refund':  {'at_type': 6, 'pt_trtype': 'F'},
    'nominal_payment':  {'at_type': 1},
    'nominal_receipt':  {'at_type': 2},
    'bank_transfer':    {'at_type': 8},
}


@runtime_checkable
class DataSource(Protocol):
    """Protocol for the data lookups the function needs."""

    def find_aentry_by_signed_value(
        self,
        bank_code: str,
        date_from: date,
        date_to: date,
        signed_pence: int,
        expected_at_type: int,
        exclude_entry_numbers: Optional[List[str]],
    ) -> List[Dict[str, Any]]:
        """Return aentry rows for this bank in the date window where
        ae_value matches signed_pence (within ±1p) AND at_type =
        expected_at_type. Excludes any ae_entry in exclude_entry_numbers.
        Each row is a dict with at minimum 'ae_entry'.
        """
        ...

    def find_stran_by_signed_value(
        self,
        account_code: str,
        date_from: date,
        date_to: date,
        signed_pounds: float,
        st_trtype: str,
    ) -> List[Dict[str, Any]]:
        """Return stran rows for this customer in the date window where
        st_trvalue matches signed_pounds (within £0.01) AND st_trtype =
        the given character.
        """
        ...

    def find_ptran_by_signed_value(
        self,
        account_code: str,
        date_from: date,
        date_to: date,
        signed_pounds: float,
        pt_trtype: str,
    ) -> List[Dict[str, Any]]:
        """Return ptran rows for this supplier in the date window where
        pt_trvalue matches signed_pounds (within £0.01) AND pt_trtype =
        the given character.
        """
        ...
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py -v
```

Expected: 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/duplicate_check.py tests/test_duplicate_check.py
git commit -m "feat(dup-check): scaffold types, action-type map, protocol

First commit toward consolidating six scattered duplicate-check
implementations into one tested function. This commit lands the
public types only — no logic yet. Tests pin the enum values, dataclass
shape, action-type mapping (the at_type/st_trtype/pt_trtype values
that drift between callsites in production), and DataSource protocol
signatures.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Implement check_for_duplicate — cashbook path

**Files:**
- Modify: `sql_rag/duplicate_check.py`
- Modify: `tests/test_duplicate_check.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_duplicate_check.py`:

```python
from datetime import date


class _FakeDataSource:
    """In-memory DataSource for unit tests."""
    def __init__(
        self,
        aentry_results: list[dict] | None = None,
        stran_results: list[dict] | None = None,
        ptran_results: list[dict] | None = None,
    ):
        self._aentry = aentry_results or []
        self._stran = stran_results or []
        self._ptran = ptran_results or []

    def find_aentry_by_signed_value(self, bank_code, date_from, date_to,
                                     signed_pence, expected_at_type,
                                     exclude_entry_numbers):
        excluded = set(exclude_entry_numbers or [])
        return [
            r for r in self._aentry
            if r.get('ae_entry') not in excluded
        ]

    def find_stran_by_signed_value(self, account_code, date_from, date_to,
                                    signed_pounds, st_trtype):
        return list(self._stran)

    def find_ptran_by_signed_value(self, account_code, date_from, date_to,
                                    signed_pounds, pt_trtype):
        return list(self._ptran)


def test_no_duplicate_when_nothing_matches():
    """Empty cashbook AND empty ledgers → NONE."""
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource()
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-198.00,
        action="sales_refund",
        account_code="P051",
        description="P Flannery",
        reference="Refund",
    )
    assert result.kind is DuplicateMatchKind.NONE
    assert result.matched_entry is None


def test_cashbook_duplicate_when_aentry_of_correct_at_type_exists():
    """A sales_refund (-£198) finds a matching atran with at_type=3."""
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource(
        aentry_results=[{'ae_entry': 'P100000755', 'at_type': 3,
                         'ae_value': -19800}],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-198.00,
        action="sales_refund",
        account_code="P051",
        description="P Flannery",
        reference="Refund",
    )
    assert result.kind is DuplicateMatchKind.CASHBOOK_DUPLICATE
    assert result.matched_table == 'aentry'
    assert result.matched_entry == 'P100000755'


def test_cashbook_duplicate_excludes_consumed_entries():
    """Multi-occurrence: if the matching aentry is in the exclude set,
    it should NOT be returned as a duplicate (the second identical
    bank line should post).
    """
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource(
        aentry_results=[],  # exclude_entry_numbers makes the lookup empty
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 17),
        signed_amount_pounds=-6.99,
        action="purchase_payment",
        account_code="LIME",
        description="Lime card purchase",
        reference="",
        exclude_entry_numbers=['P100008190'],
    )
    assert result.kind is DuplicateMatchKind.NONE


def test_cashbook_match_requires_correct_at_type():
    """Sign-blind ABS regression: a -£198 sales_refund must NOT match
    a +£198 sales_receipt even though magnitudes are equal. The
    expected_at_type filter ensures this.
    """
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind

    class _StubDS:
        def find_aentry_by_signed_value(self, bank_code, date_from, date_to,
                                         signed_pence, expected_at_type,
                                         exclude_entry_numbers):
            # The DataSource implementation is responsible for filtering
            # by at_type. If a caller passed at_type=3 (sales_refund),
            # we'd return only at_type=3 rows. Simulate the correct
            # filter behaviour: we received at_type=3 for the search,
            # only the at_type=3 rows are returned.
            assert expected_at_type == 3
            return []  # nothing of at_type=3
        def find_stran_by_signed_value(self, *a, **kw): return []
        def find_ptran_by_signed_value(self, *a, **kw): return []

    result = check_for_duplicate(
        data_source=_StubDS(),
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-198.00,
        action="sales_refund",
        account_code="P051",
        description="",
        reference="",
    )
    assert result.kind is DuplicateMatchKind.NONE


def test_unknown_action_raises_value_error():
    """An action not in ACTION_TYPE_MAP must raise — never silently match."""
    from sql_rag.duplicate_check import check_for_duplicate
    import pytest as _pt
    ds = _FakeDataSource()
    with _pt.raises(ValueError, match="not in ACTION_TYPE_MAP"):
        check_for_duplicate(
            data_source=ds,
            bank_code="BB005",
            transaction_date=date(2026, 4, 16),
            signed_amount_pounds=0.0,
            action="totally_made_up_action",
            account_code="X",
            description="",
            reference="",
        )
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py -v
```

Expected: 5 new tests FAIL with `ImportError: cannot import name 'check_for_duplicate'`.

- [ ] **Step 3: Write minimal implementation**

Append to `sql_rag/duplicate_check.py`:

```python
from datetime import timedelta


def _signed_pence(signed_pounds: float) -> int:
    return int(round(signed_pounds * 100))


def check_for_duplicate(
    *,
    data_source: DataSource,
    bank_code: str,
    transaction_date: date,
    signed_amount_pounds: float,
    action: str,
    account_code: Optional[str],
    description: str,
    reference: str,
    date_tolerance_days: int = 14,
    exclude_entry_numbers: Optional[List[str]] = None,
) -> DuplicateCheckResult:
    """Single source of truth for the bank-import duplicate decision.

    Order:
      1. Cashbook (atran/aentry) check, type-aware AND sign-aware. The
         cashbook is authoritative — if the bank entry is already there
         with the correct at_type and signed amount, it's a duplicate.
      2. Ledger advisory check (refunds only): if no cashbook duplicate,
         look for a matching credit-note-type stran/ptran row. If found,
         return LEDGER_ALLOCATION_TARGET — the caller should POST and
         optionally auto-allocate.
      3. Otherwise: NONE.

    Sign-aware throughout: a +£X receipt is never a duplicate of a -£X
    refund. Type-aware: the cashbook check filters by the at_type for
    the action, so matching magnitudes of different type don't collide.

    Raises ValueError if action is not in ACTION_TYPE_MAP.
    """
    if action not in ACTION_TYPE_MAP:
        raise ValueError(
            f"action {action!r} is not in ACTION_TYPE_MAP — "
            f"add it explicitly, do not let unknown actions silently match."
        )

    type_map = ACTION_TYPE_MAP[action]
    expected_at_type = type_map['at_type']

    date_from = transaction_date - timedelta(days=date_tolerance_days)
    date_to = transaction_date + timedelta(days=date_tolerance_days)
    signed_pence = _signed_pence(signed_amount_pounds)

    # 1. Cashbook check — authoritative
    aentry_rows = data_source.find_aentry_by_signed_value(
        bank_code,
        date_from,
        date_to,
        signed_pence,
        expected_at_type,
        list(exclude_entry_numbers or []),
    )
    if aentry_rows:
        row = aentry_rows[0]
        return DuplicateCheckResult(
            kind=DuplicateMatchKind.CASHBOOK_DUPLICATE,
            matched_table='aentry',
            matched_entry=str(row.get('ae_entry', '')).strip() or None,
            reason=(
                f"cashbook entry {row.get('ae_entry')} already posted "
                f"(at_type={expected_at_type}, ae_value~={signed_pence}p) "
                f"on {bank_code} in window {date_from}..{date_to}"
            ),
        )

    # 2. Ledger advisory (refund actions only) — task 3 implements
    # 3. NONE
    return DuplicateCheckResult(
        kind=DuplicateMatchKind.NONE,
        matched_table=None,
        matched_entry=None,
        reason=(
            f"no cashbook match for {action} on {bank_code} "
            f"({signed_amount_pounds:+.2f}, {transaction_date.isoformat()})"
        ),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py -v
```

Expected: 12 tests PASS (7 from Task 1 + 5 new).

- [ ] **Step 5: Commit**

```bash
git add sql_rag/duplicate_check.py tests/test_duplicate_check.py
git commit -m "feat(dup-check): cashbook path — type-aware, sign-aware, exclude-aware

The cashbook (atran/aentry) is authoritative: if a matching aentry
of the action's expected at_type and signed amount exists in the
date window, it's a duplicate. The exclude_entry_numbers parameter
preserves multi-occurrence support (two identical £6.99 lines must
both post if only one Opera entry exists).

Unknown actions raise ValueError — never silently match. Type+sign
combination prevents the +£198 receipt vs -£198 refund false-positive
that bit us in the Cloudsis P051 case.

Ledger advisory path (Stage 2) follows.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Implement ledger advisory path

**Files:**
- Modify: `sql_rag/duplicate_check.py`
- Modify: `tests/test_duplicate_check.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_duplicate_check.py`:

```python
def test_ledger_allocation_target_for_sales_refund():
    """No cashbook entry, but stran has a type='F' or 'C' row matching the
    refund amount → LEDGER_ALLOCATION_TARGET. The caller should post the
    refund payment, not refuse it.
    """
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource(
        aentry_results=[],
        stran_results=[{'st_trref': 'CN0001', 'st_trvalue': -198.00,
                        'st_trtype': 'F'}],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-198.00,
        action="sales_refund",
        account_code="P051",
        description="",
        reference="",
    )
    assert result.kind is DuplicateMatchKind.LEDGER_ALLOCATION_TARGET
    assert result.matched_table == 'stran'
    assert result.matched_entry == 'CN0001'


def test_ledger_allocation_target_for_purchase_refund():
    """ptran credit-note-type row matches purchase_refund amount."""
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource(
        aentry_results=[],
        ptran_results=[{'pt_trref': 'CN9999', 'pt_trvalue': 100.00,
                        'pt_trtype': 'F'}],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 17),
        signed_amount_pounds=100.00,
        action="purchase_refund",
        account_code="SUPP1",
        description="",
        reference="",
    )
    assert result.kind is DuplicateMatchKind.LEDGER_ALLOCATION_TARGET
    assert result.matched_table == 'ptran'
    assert result.matched_entry == 'CN9999'


def test_ledger_advisory_skipped_for_non_refund_actions():
    """sales_receipt, purchase_payment, nominal_*, bank_transfer don't
    consult the ledger — they're authoritatively decided by cashbook.
    """
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource(
        aentry_results=[],
        stran_results=[{'st_trref': 'X', 'st_trvalue': -50.00, 'st_trtype': 'F'}],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=50.00,
        action="sales_receipt",  # NOT a refund
        account_code="P051",
        description="",
        reference="",
    )
    assert result.kind is DuplicateMatchKind.NONE


def test_ledger_advisory_requires_account_code():
    """Without account_code we can't query the ledger; result is NONE."""
    from sql_rag.duplicate_check import check_for_duplicate, DuplicateMatchKind
    ds = _FakeDataSource(
        aentry_results=[],
        stran_results=[{'st_trref': 'X', 'st_trvalue': -50.00, 'st_trtype': 'F'}],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-50.00,
        action="sales_refund",
        account_code=None,
        description="",
        reference="",
    )
    assert result.kind is DuplicateMatchKind.NONE
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py -v
```

Expected: 4 new tests FAIL.

- [ ] **Step 3: Write minimal implementation**

In `sql_rag/duplicate_check.py`, replace the existing comment line `# 2. Ledger advisory (refund actions only) — task 3 implements` and the trailing `# 3. NONE` block with:

```python
    # 2. Ledger advisory — refund actions only
    if action == 'sales_refund' and account_code:
        st_trtype = type_map.get('st_trtype')
        if st_trtype:
            stran_rows = data_source.find_stran_by_signed_value(
                account_code, date_from, date_to, signed_amount_pounds, st_trtype
            )
            if stran_rows:
                row = stran_rows[0]
                return DuplicateCheckResult(
                    kind=DuplicateMatchKind.LEDGER_ALLOCATION_TARGET,
                    matched_table='stran',
                    matched_entry=str(row.get('st_trref', '')).strip() or None,
                    reason=(
                        f"stran row {row.get('st_trref')} (type={st_trtype}, "
                        f"value={row.get('st_trvalue')}) is an allocation target "
                        f"for this refund — POST, then optionally allocate"
                    ),
                )
    elif action == 'purchase_refund' and account_code:
        pt_trtype = type_map.get('pt_trtype')
        if pt_trtype:
            ptran_rows = data_source.find_ptran_by_signed_value(
                account_code, date_from, date_to, signed_amount_pounds, pt_trtype
            )
            if ptran_rows:
                row = ptran_rows[0]
                return DuplicateCheckResult(
                    kind=DuplicateMatchKind.LEDGER_ALLOCATION_TARGET,
                    matched_table='ptran',
                    matched_entry=str(row.get('pt_trref', '')).strip() or None,
                    reason=(
                        f"ptran row {row.get('pt_trref')} (type={pt_trtype}, "
                        f"value={row.get('pt_trvalue')}) is an allocation target "
                        f"for this refund — POST, then optionally allocate"
                    ),
                )

    # 3. NONE
    return DuplicateCheckResult(
        kind=DuplicateMatchKind.NONE,
        matched_table=None,
        matched_entry=None,
        reason=(
            f"no cashbook match for {action} on {bank_code} "
            f"({signed_amount_pounds:+.2f}, {transaction_date.isoformat()})"
        ),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py -v
```

Expected: 16 tests PASS (12 + 4 new).

- [ ] **Step 5: Commit**

```bash
git add sql_rag/duplicate_check.py tests/test_duplicate_check.py
git commit -m "feat(dup-check): ledger advisory path for refund actions

When no cashbook duplicate is found and the action is a refund, look
for a matching credit-note-type stran/ptran row. If found, return
LEDGER_ALLOCATION_TARGET. The caller's correct response is POST the
refund payment and optionally allocate it to the credit note — NOT
refuse the post.

This was the central confusion that broke the Cloudsis P051 -£198
flow today: an unallocated stran credit was treated as a duplicate
when it was actually the allocation target.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Implement Opera SE DataSource

**Files:**
- Create: `sql_rag/duplicate_check_se.py`
- Modify: `tests/test_duplicate_check.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_duplicate_check.py`:

```python
def test_se_datasource_construction_and_protocol():
    """OperaSEDataSource exists, takes a SQLConnector, satisfies protocol."""
    from sql_rag.duplicate_check_se import OperaSEDataSource
    from sql_rag.duplicate_check import DataSource

    class _StubConn:
        def execute_query(self, q):
            raise NotImplementedError
    ds = OperaSEDataSource(_StubConn())
    assert isinstance(ds, DataSource)


def test_se_datasource_uses_signed_comparison_and_at_type():
    """Smoke test — verify the SQL the SE DataSource emits uses signed
    comparison (`a.at_value - signed_pence`) and a type filter
    (`a.at_type = expected_at_type`). Catches regressions back to
    sign-blind ABS-on-ABS.
    """
    captured_queries: list[str] = []

    class _SpyConn:
        def execute_query(self, q):
            captured_queries.append(q)
            class _DF:
                empty = True
                def to_dict(self, *a, **k): return []
                def iterrows(self): return iter([])
            return _DF()

    from sql_rag.duplicate_check_se import OperaSEDataSource
    from datetime import date as _date

    ds = OperaSEDataSource(_SpyConn())
    ds.find_aentry_by_signed_value(
        'BB005', _date(2026, 4, 1), _date(2026, 4, 30),
        signed_pence=-19800, expected_at_type=3,
        exclude_entry_numbers=[],
    )
    assert any("ABS(a.at_value - -19800)" in q for q in captured_queries), \
        f"signed comparison not found in queries: {captured_queries}"
    assert any("a.at_type = 3" in q for q in captured_queries), \
        f"at_type filter not found in queries: {captured_queries}"
    # Critical: NO ABS(ABS(...)) — that's the sign-blind regression
    assert not any("ABS(ABS(" in q for q in captured_queries), \
        "sign-blind ABS-on-ABS regression: " + str(captured_queries)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py -v
```

Expected: 2 new tests FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write minimal implementation**

```python
# sql_rag/duplicate_check_se.py
"""Opera SQL SE DataSource for the duplicate-check function.

All queries use:
  - WITH (NOLOCK) per project locking rules.
  - Signed comparison (ABS(value - signed) < tolerance) — NOT ABS-on-ABS.
  - Explicit type filter — at_type / st_trtype / pt_trtype.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional


class OperaSEDataSource:
    def __init__(self, sql_connector: Any) -> None:
        self._sql = sql_connector

    def find_aentry_by_signed_value(
        self,
        bank_code: str,
        date_from: date,
        date_to: date,
        signed_pence: int,
        expected_at_type: int,
        exclude_entry_numbers: Optional[List[str]],
    ) -> List[Dict[str, Any]]:
        excl_clause = ''
        if exclude_entry_numbers:
            quoted = ','.join(
                f"'{e.replace(chr(39), chr(39)+chr(39))}'"
                for e in exclude_entry_numbers
            )
            excl_clause = f" AND RTRIM(e.ae_entry) NOT IN ({quoted})"
        query = f"""
            SELECT TOP 5 a.at_entry as ae_entry, a.at_value as ae_value, a.at_type
            FROM atran a WITH (NOLOCK)
            JOIN aentry e WITH (NOLOCK)
              ON e.ae_entry = a.at_entry AND e.ae_acnt = a.at_acnt
            WHERE a.at_acnt = '{bank_code}'
            AND a.at_pstdate BETWEEN '{date_from.isoformat()}' AND '{date_to.isoformat()}'
            AND ABS(a.at_value - {signed_pence}) < 1
            AND a.at_type = {expected_at_type}
            {excl_clause}
        """
        df = self._sql.execute_query(query)
        if df is None or df.empty:
            return []
        return [
            {'ae_entry': str(row['ae_entry']).strip(),
             'ae_value': row.get('ae_value'),
             'at_type': row.get('at_type')}
            for _, row in df.iterrows()
        ]

    def find_stran_by_signed_value(
        self,
        account_code: str,
        date_from: date,
        date_to: date,
        signed_pounds: float,
        st_trtype: str,
    ) -> List[Dict[str, Any]]:
        query = f"""
            SELECT TOP 5 st_trref, st_trvalue, st_trtype
            FROM stran WITH (NOLOCK)
            WHERE RTRIM(st_account) = '{account_code}'
            AND st_trdate BETWEEN '{date_from.isoformat()}' AND '{date_to.isoformat()}'
            AND ABS(st_trvalue - {signed_pounds}) < 0.01
            AND st_trtype = '{st_trtype}'
        """
        df = self._sql.execute_query(query)
        if df is None or df.empty:
            return []
        return [
            {'st_trref': (row.get('st_trref') or '').strip(),
             'st_trvalue': row.get('st_trvalue'),
             'st_trtype': row.get('st_trtype')}
            for _, row in df.iterrows()
        ]

    def find_ptran_by_signed_value(
        self,
        account_code: str,
        date_from: date,
        date_to: date,
        signed_pounds: float,
        pt_trtype: str,
    ) -> List[Dict[str, Any]]:
        query = f"""
            SELECT TOP 5 pt_trref, pt_trvalue, pt_trtype
            FROM ptran WITH (NOLOCK)
            WHERE RTRIM(pt_account) = '{account_code}'
            AND pt_trdate BETWEEN '{date_from.isoformat()}' AND '{date_to.isoformat()}'
            AND ABS(pt_trvalue - {signed_pounds}) < 0.01
            AND pt_trtype = '{pt_trtype}'
        """
        df = self._sql.execute_query(query)
        if df is None or df.empty:
            return []
        return [
            {'pt_trref': (row.get('pt_trref') or '').strip(),
             'pt_trvalue': row.get('pt_trvalue'),
             'pt_trtype': row.get('pt_trtype')}
            for _, row in df.iterrows()
        ]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py -v
```

Expected: 18 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/duplicate_check_se.py tests/test_duplicate_check.py
git commit -m "feat(dup-check): Opera SE DataSource — signed + type-aware queries

OperaSEDataSource emits SQL with:
  - WITH (NOLOCK) per project locking rules
  - Signed comparison ABS(value - signed_amount) — NOT ABS(ABS(...))
  - Explicit type filter (at_type / st_trtype / pt_trtype)

The smoke test asserts the emitted SQL contains the signed comparison
AND the type filter AND does NOT contain ABS-on-ABS — locking the
historical bug class out at compile time.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Implement Opera 3 DataSource

**Files:**
- Create: `sql_rag/duplicate_check_o3.py`
- Modify: `tests/test_duplicate_check.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_duplicate_check.py`:

```python
def test_o3_datasource_construction_and_protocol():
    from sql_rag.duplicate_check_o3 import Opera3DataSource
    from sql_rag.duplicate_check import DataSource
    class _Stub:
        def read_table(self, name): return []
    assert isinstance(Opera3DataSource(_Stub()), DataSource)


def test_o3_datasource_filters_aentry_by_bank_at_type_and_signed_value():
    """Verify Opera3DataSource filters aentry rows correctly:
    bank, signed pence (within tolerance), at_type, exclude list, date.
    """
    from sql_rag.duplicate_check_o3 import Opera3DataSource
    from datetime import date as _date

    rows_by_table = {
        'atran': [
            {'at_acnt': 'BB005', 'at_entry': 'P100000755',
             'at_value': -19800, 'at_type': 3,
             'at_pstdate': _date(2026, 4, 16)},
            {'at_acnt': 'BB005', 'at_entry': 'R100000407',
             'at_value': 19800, 'at_type': 4,
             'at_pstdate': _date(2026, 4, 16)},
            {'at_acnt': 'BB005', 'at_entry': 'P100000900',
             'at_value': -19800, 'at_type': 3,
             'at_pstdate': _date(2026, 5, 5)},  # outside window
        ],
        'aentry': [],
    }

    class _Reader:
        def read_table(self, name): return rows_by_table.get(name, [])

    ds = Opera3DataSource(_Reader())
    rows = ds.find_aentry_by_signed_value(
        'BB005', _date(2026, 4, 1), _date(2026, 4, 30),
        signed_pence=-19800, expected_at_type=3,
        exclude_entry_numbers=[],
    )
    assert len(rows) == 1
    assert rows[0]['ae_entry'] == 'P100000755'

    # Now exclude the matching entry — should return empty
    rows = ds.find_aentry_by_signed_value(
        'BB005', _date(2026, 4, 1), _date(2026, 4, 30),
        signed_pence=-19800, expected_at_type=3,
        exclude_entry_numbers=['P100000755'],
    )
    assert rows == []
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py -v
```

Expected: 2 new tests FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write minimal implementation**

```python
# sql_rag/duplicate_check_o3.py
"""Opera 3 (FoxPro DBF) DataSource for the duplicate-check function."""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, Iterable, List, Optional


def _row_get(row: Any, *keys: str) -> Any:
    """Case-insensitive dict-or-attr lookup."""
    for k in keys:
        for variant in (k, k.upper(), k.lower()):
            if isinstance(row, dict) and variant in row:
                v = row[variant]
                if v is not None:
                    return v
            elif hasattr(row, variant):
                v = getattr(row, variant)
                if v is not None:
                    return v
    return None


def _normalise_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date) and not hasattr(v, 'hour'):
        return v
    if hasattr(v, 'date'):
        return v.date()
    return None


class Opera3DataSource:
    def __init__(self, reader: Any) -> None:
        self._reader = reader

    def find_aentry_by_signed_value(
        self,
        bank_code: str,
        date_from: date,
        date_to: date,
        signed_pence: int,
        expected_at_type: int,
        exclude_entry_numbers: Optional[List[str]],
    ) -> List[Dict[str, Any]]:
        excluded = set(exclude_entry_numbers or [])
        out: List[Dict[str, Any]] = []
        for row in self._reader.read_table('atran'):
            acnt = _row_get(row, 'at_acnt')
            if not acnt or str(acnt).strip() != bank_code:
                continue
            entry = _row_get(row, 'at_entry')
            entry_str = str(entry).strip() if entry is not None else ''
            if entry_str in excluded:
                continue
            value = _row_get(row, 'at_value')
            if value is None or abs(float(value) - signed_pence) >= 1:
                continue
            at_type = _row_get(row, 'at_type')
            if at_type is None or int(at_type) != int(expected_at_type):
                continue
            d = _normalise_date(_row_get(row, 'at_pstdate'))
            if d is None or not (date_from <= d <= date_to):
                continue
            out.append({
                'ae_entry': entry_str,
                'ae_value': value,
                'at_type': at_type,
            })
        return out

    def find_stran_by_signed_value(
        self,
        account_code: str,
        date_from: date,
        date_to: date,
        signed_pounds: float,
        st_trtype: str,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in self._reader.read_table('stran'):
            acnt = _row_get(row, 'st_account')
            if not acnt or str(acnt).strip() != account_code:
                continue
            tr_type = _row_get(row, 'st_trtype')
            if tr_type is None or str(tr_type).strip() != st_trtype:
                continue
            value = _row_get(row, 'st_trvalue')
            if value is None or abs(float(value) - signed_pounds) >= 0.01:
                continue
            d = _normalise_date(_row_get(row, 'st_trdate'))
            if d is None or not (date_from <= d <= date_to):
                continue
            out.append({
                'st_trref': str(_row_get(row, 'st_trref') or '').strip(),
                'st_trvalue': value,
                'st_trtype': tr_type,
            })
        return out

    def find_ptran_by_signed_value(
        self,
        account_code: str,
        date_from: date,
        date_to: date,
        signed_pounds: float,
        pt_trtype: str,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in self._reader.read_table('ptran'):
            acnt = _row_get(row, 'pt_account')
            if not acnt or str(acnt).strip() != account_code:
                continue
            tr_type = _row_get(row, 'pt_trtype')
            if tr_type is None or str(tr_type).strip() != pt_trtype:
                continue
            value = _row_get(row, 'pt_trvalue')
            if value is None or abs(float(value) - signed_pounds) >= 0.01:
                continue
            d = _normalise_date(_row_get(row, 'pt_trdate'))
            if d is None or not (date_from <= d <= date_to):
                continue
            out.append({
                'pt_trref': str(_row_get(row, 'pt_trref') or '').strip(),
                'pt_trvalue': value,
                'pt_trtype': tr_type,
            })
        return out
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py -v
```

Expected: 20 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/duplicate_check_o3.py tests/test_duplicate_check.py
git commit -m "feat(dup-check): Opera 3 DataSource — same protocol as SE

Opera3DataSource walks the FoxPro reader's tables (atran/aentry,
stran, ptran) and applies the same filters as SE: bank, signed
amount, type, date window, exclude list. Per project rule (full
parity), this lands alongside SE in the same plan.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Regression test fixtures for the historical bugs

**Files:**
- Create: `tests/test_duplicate_check_regression.py`

- [ ] **Step 1: Write regression tests against the fixture DataSource**

```python
# tests/test_duplicate_check_regression.py
"""Regression tests pinning each historical bug we fixed during the
2026-05-03 audit. These run against a fixture DataSource — no live
Opera dependency.

Each test reproduces a specific class of bug that was bitten in
production. If any of these fail in the future, that regression will
be caught at PR time.
"""
from __future__ import annotations

from datetime import date

from sql_rag.duplicate_check import (
    check_for_duplicate,
    DuplicateMatchKind,
)


class _FixtureDS:
    def __init__(self, aentry=None, stran=None, ptran=None):
        self._aentry = aentry or []
        self._stran = stran or []
        self._ptran = ptran or []

    def find_aentry_by_signed_value(self, bank_code, date_from, date_to,
                                     signed_pence, expected_at_type,
                                     exclude_entry_numbers):
        excluded = set(exclude_entry_numbers or [])
        return [
            r for r in self._aentry
            if r.get('at_type') == expected_at_type
            and abs(r.get('ae_value', 0) - signed_pence) < 1
            and r.get('ae_entry') not in excluded
        ]

    def find_stran_by_signed_value(self, account_code, date_from, date_to,
                                    signed_pounds, st_trtype):
        return [
            r for r in self._stran
            if r.get('st_trtype') == st_trtype
            and abs(r.get('st_trvalue', 0) - signed_pounds) < 0.01
        ]

    def find_ptran_by_signed_value(self, account_code, date_from, date_to,
                                    signed_pounds, pt_trtype):
        return [
            r for r in self._ptran
            if r.get('pt_trtype') == pt_trtype
            and abs(r.get('pt_trvalue', 0) - signed_pounds) < 0.01
        ]


def test_regression_cloudsis_p051_refund_vs_misposted_receipt():
    """Cloudsis BB005, 2026-04-16:
    - existing aentry R100000407 has at_type=4 (sales_receipt) value=+£198 — misposted
    - bank line is -£198 sales_refund
    Sign-blind ABS-on-ABS matched these falsely. The fix: signed +
    type-aware. Refund must NOT match the misposted receipt.
    """
    ds = _FixtureDS(
        aentry=[
            {'ae_entry': 'R100000407', 'ae_value': 19800, 'at_type': 4},
        ],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-198.00,
        action="sales_refund",
        account_code="P051",
        description="P Flannery",
        reference="Refund",
    )
    assert result.kind is DuplicateMatchKind.NONE, (
        f"Refund must NOT match a +£198 receipt — got {result}"
    )


def test_regression_multi_occurrence_two_lime_purchases_both_post():
    """Two identical -£6.99 Lime card purchases on different dates;
    Opera has only ONE matching aentry. The second bank line must
    NOT see the same Opera entry as a duplicate.
    """
    existing_entry = {'ae_entry': 'P100008190', 'ae_value': -699,
                      'at_type': 5}
    ds = _FixtureDS(aentry=[existing_entry])

    # First line: matches the entry → duplicate
    result1 = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 17),
        signed_amount_pounds=-6.99,
        action="purchase_payment",
        account_code="LIME",
        description="Lime", reference="",
    )
    assert result1.kind is DuplicateMatchKind.CASHBOOK_DUPLICATE
    assert result1.matched_entry == 'P100008190'

    # Second line: caller passes the consumed entry in exclude list
    result2 = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 19),
        signed_amount_pounds=-6.99,
        action="purchase_payment",
        account_code="LIME",
        description="Lime", reference="",
        exclude_entry_numbers=['P100008190'],
    )
    assert result2.kind is DuplicateMatchKind.NONE


def test_regression_unallocated_credit_note_is_allocation_target_not_duplicate():
    """The Cloudsis case: P051 has an unallocated -£198 stran credit
    note (type='F'). The bank line is a -£198 sales_refund. The credit
    note is the *target* the new refund should allocate to — it is NOT
    a duplicate.
    """
    ds = _FixtureDS(
        aentry=[],  # no cashbook entry
        stran=[{'st_trref': 'CN_P051_198', 'st_trvalue': -198.00,
                'st_trtype': 'F'}],
    )
    result = check_for_duplicate(
        data_source=ds,
        bank_code="BB005",
        transaction_date=date(2026, 4, 16),
        signed_amount_pounds=-198.00,
        action="sales_refund",
        account_code="P051",
        description="P Flannery", reference="Refund",
    )
    assert result.kind is DuplicateMatchKind.LEDGER_ALLOCATION_TARGET
    assert result.matched_entry == 'CN_P051_198'
    # CRITICAL: NOT a duplicate — caller must POST.
    assert not result.is_duplicate


def test_regression_action_type_map_includes_pt_trtype_for_purchase_refund():
    """Direct lock-in of the historical pt_ref vs pt_trtype confusion.
    If anyone changes ACTION_TYPE_MAP['purchase_refund']['pt_trtype']
    to something other than 'F', this test fails.
    """
    from sql_rag.duplicate_check import ACTION_TYPE_MAP
    assert ACTION_TYPE_MAP['purchase_refund']['pt_trtype'] == 'F'
    assert ACTION_TYPE_MAP['sales_refund']['st_trtype'] == 'F'
```

- [ ] **Step 2: Run tests to verify they pass (no implementation needed — function already supports these)**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check_regression.py -v
```

Expected: 4 tests PASS.

- [ ] **Step 3: Run full suite**

```bash
source venv/bin/activate && python -m pytest tests/ -x -q
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_duplicate_check_regression.py
git commit -m "test(dup-check): regression suite pinning historical bugs

Five tests covering each class of bug we fixed in the 2026-05-03
audit:
  - Cloudsis P051 refund-vs-misposted-receipt sign-blind match
  - Multi-occurrence two-£6.99-Lime-purchases via exclude_entry_numbers
  - Unallocated credit note as allocation target, not duplicate
  - ACTION_TYPE_MAP type-character invariants

Each runs against a fixture DataSource — no live Opera dependency.
If any future change reintroduces these classes, CI will catch it.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Refactor _is_already_posted to delegate

**Files:**
- Modify: `sql_rag/bank_import.py`

- [ ] **Step 1: Write a callsite test**

Append to `tests/test_duplicate_check.py`:

```python
def test_is_already_posted_delegates_to_check_for_duplicate():
    """The _is_already_posted method must call check_for_duplicate —
    not maintain its own per-action SQL.
    """
    from pathlib import Path
    bi = Path(__file__).resolve().parent.parent / "sql_rag" / "bank_import.py"
    src = bi.read_text(encoding='utf-8')

    assert "check_for_duplicate" in src, \
        "_is_already_posted should call check_for_duplicate"
    # Heuristic: the legacy ABS(ABS( pattern is gone from this file
    assert "ABS(ABS(at_value)" not in src, \
        "Sign-blind ABS-on-ABS pattern reappeared in bank_import.py"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py::test_is_already_posted_delegates_to_check_for_duplicate -v
```

Expected: FAIL — `bank_import.py` does not yet call `check_for_duplicate`.

- [ ] **Step 3: Refactor the implementation**

In `sql_rag/bank_import.py`, find `def _is_already_posted` (currently around line 1447).

Replace the entire body with a delegation:

```python
    def _is_already_posted(
        self,
        txn,
        consumed_entries: Optional[set] = None,
    ) -> Tuple[bool, str]:
        """Decide whether this transaction is already in Opera.

        Single source of truth: delegates to
        sql_rag.duplicate_check.check_for_duplicate. The wrapper exists
        only to preserve the existing return contract used by callers
        (the (bool, reason) tuple plus side-effects on `txn` and
        `consumed_entries`).
        """
        from sql_rag.duplicate_check import (
            check_for_duplicate,
            DuplicateMatchKind,
        )
        from sql_rag.duplicate_check_se import OperaSEDataSource

        if not txn.action or txn.action in ('skip',):
            return False, ""

        ds = OperaSEDataSource(self.sql_connector)

        # Translate consumed_entries to ae_entry exclude list
        excluded: list[str] = []
        for k in (consumed_entries or set()):
            if isinstance(k, str) and k.startswith('aentry:'):
                excluded.append(k.split(':', 1)[1].strip())
            elif isinstance(k, str) and ':' not in k:
                excluded.append(k.strip())

        try:
            result = check_for_duplicate(
                data_source=ds,
                bank_code=self.bank_code,
                transaction_date=txn.date,
                signed_amount_pounds=float(txn.amount),
                action=txn.action,
                account_code=getattr(txn, 'matched_account', None) or '',
                description=getattr(txn, 'name', '') or '',
                reference=getattr(txn, 'reference', '') or '',
                exclude_entry_numbers=excluded or None,
            )
        except ValueError:
            # Unknown action — propagate as "not posted" so the caller
            # surfaces a clearer downstream error. Don't silently match.
            return False, ""

        if result.kind is DuplicateMatchKind.CASHBOOK_DUPLICATE:
            txn.is_duplicate = True
            return True, result.reason
        # LEDGER_ALLOCATION_TARGET → caller posts; we surface the info.
        if result.kind is DuplicateMatchKind.LEDGER_ALLOCATION_TARGET:
            return False, f"allocation target: {result.reason}"
        return False, ""
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py tests/test_duplicate_check_regression.py -v
```

Expected: 25+ PASS.

```bash
source venv/bin/activate && python -m pytest tests/ -x -q
```

Expected: all tests pass — no regressions in the wider suite.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/bank_import.py tests/test_duplicate_check.py
git commit -m "refactor(bank_import): _is_already_posted delegates to check_for_duplicate

First call-site migrated. The method now constructs an
OperaSEDataSource, delegates the decision, and translates the result
back to the (bool, reason) tuple the existing callers expect. ~150
lines of inline Check 0/0b/1/2/3 SQL deleted from this file.

Sign-blind ABS-on-ABS patterns no longer present in bank_import.py
(test enforces).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Refactor check_duplicate_before_posting (Opera SE post-time)

**Files:**
- Modify: `sql_rag/opera_sql_import.py`

- [ ] **Step 1: Write the callsite test**

Append to `tests/test_duplicate_check.py`:

```python
def test_check_duplicate_before_posting_delegates():
    from pathlib import Path
    osi = Path(__file__).resolve().parent.parent / "sql_rag" / "opera_sql_import.py"
    src = osi.read_text(encoding='utf-8')

    # Find the check_duplicate_before_posting function body specifically
    start = src.find("def check_duplicate_before_posting")
    assert start != -1
    # Approximate end: next "def " at the same indentation level
    end = src.find("\n    def ", start + 10)
    if end == -1:
        end = start + 10000
    body = src[start:end]

    assert "check_for_duplicate" in body, \
        "check_duplicate_before_posting must delegate to check_for_duplicate"
    assert "ABS(ABS(st_trvalue)" not in body, \
        "sign-blind stran regression in check_duplicate_before_posting"
    assert "ABS(ABS(pt_trvalue)" not in body, \
        "sign-blind ptran regression in check_duplicate_before_posting"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py::test_check_duplicate_before_posting_delegates -v
```

Expected: FAIL.

- [ ] **Step 3: Refactor the implementation**

In `sql_rag/opera_sql_import.py`, find `def check_duplicate_before_posting`. Replace the entire body with:

```python
    def check_duplicate_before_posting(
        self,
        bank_account: str,
        transaction_date,
        amount_pounds: float,
        signed_amount_pounds: Optional[float] = None,
        account_code: str = '',
        account_type: str = 'nominal',
        date_tolerance_days: int = 1,
        description: str = '',
        exclude_entry_numbers: Optional[list] = None,
    ) -> dict:
        """Pre-flight duplicate check before posting to Opera.

        Single source of truth: delegates to
        sql_rag.duplicate_check.check_for_duplicate. The wrapper only
        exists to preserve the existing return shape used by callers
        in apps/bank_reconcile/api/routes.py.
        """
        from datetime import date as _date_type, datetime as _dt
        from sql_rag.duplicate_check import (
            check_for_duplicate,
            DuplicateMatchKind,
        )
        from sql_rag.duplicate_check_se import OperaSEDataSource

        # Normalise date input
        if isinstance(transaction_date, str):
            txn_date = _date_type.fromisoformat(transaction_date[:10])
        elif isinstance(transaction_date, _dt):
            txn_date = transaction_date.date()
        else:
            txn_date = transaction_date

        # Map account_type → action (the legacy contract this wrapper preserves)
        type_to_action = {
            'customer': 'sales_receipt',
            'customer_refund': 'sales_refund',
            'supplier': 'purchase_payment',
            'supplier_refund': 'purchase_refund',
            'transfer_out': 'bank_transfer',
            'transfer_in': 'bank_transfer',
            'transfer': 'bank_transfer',
            'nominal_payment': 'nominal_payment',
            'nominal_receipt': 'nominal_receipt',
            'nominal': 'nominal_payment',  # default — sign decides downstream
        }
        action = type_to_action.get(account_type, 'nominal_payment')

        # Derive signed amount if caller didn't pass one
        if signed_amount_pounds is None:
            if account_type in ('supplier', 'customer_refund', 'nominal_payment',
                                'transfer_out'):
                signed_amount_pounds = -abs(amount_pounds)
            else:
                signed_amount_pounds = abs(amount_pounds)

        ds = OperaSEDataSource(self.sql)
        try:
            result = check_for_duplicate(
                data_source=ds,
                bank_code=bank_account,
                transaction_date=txn_date,
                signed_amount_pounds=float(signed_amount_pounds),
                action=action,
                account_code=account_code or None,
                description=description or '',
                reference='',
                date_tolerance_days=date_tolerance_days,
                exclude_entry_numbers=list(exclude_entry_numbers or []) or None,
            )
        except ValueError as e:
            return {
                'is_duplicate': False,
                'details': f"unknown action_type {account_type!r}: {e}",
            }

        if result.kind is DuplicateMatchKind.CASHBOOK_DUPLICATE:
            return {
                'is_duplicate': True,
                'location': 'cashbook',
                'details': result.reason,
                'entry_number': result.matched_entry,
            }
        # LEDGER_ALLOCATION_TARGET is informational, not a duplicate at
        # post time — the new posting is the missing payment for the
        # credit note.
        return {'is_duplicate': False, 'details': result.reason}
```

If the file already has imports of `Optional` etc., keep them; only re-import what's missing. Preserve the existing wrapper at module scope.

- [ ] **Step 4: Run tests to verify they pass**

```bash
source venv/bin/activate && python -m pytest tests/ -x -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add sql_rag/opera_sql_import.py tests/test_duplicate_check.py
git commit -m "refactor(opera-sql-import): check_duplicate_before_posting delegates

The post-time SE duplicate check now delegates to check_for_duplicate.
Existing return shape preserved (the dict with 'is_duplicate'/'details'/
'location'/'entry_number') so callers in apps/bank_reconcile/api/
routes.py keep working unchanged.

Sign-blind ABS-on-ABS patterns removed from this file (test enforces
both stran and ptran).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Refactor bank_duplicates.py "exact" path

**Files:**
- Modify: `sql_rag/bank_duplicates.py`

- [ ] **Step 1: Write the callsite test**

Append to `tests/test_duplicate_check.py`:

```python
def test_bank_duplicates_exact_path_uses_signed_comparison():
    from pathlib import Path
    bd = Path(__file__).resolve().parent.parent / "sql_rag" / "bank_duplicates.py"
    src = bd.read_text(encoding='utf-8')

    # The "exact" cashbook match in find_duplicates uses signed,
    # NOT ABS-on-ABS
    assert "ABS(ABS(at_value)" not in src, \
        "Sign-blind ABS regression in bank_duplicates.py exact path"
```

- [ ] **Step 2: Run test to verify status**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py::test_bank_duplicates_exact_path_uses_signed_comparison -v
```

May already pass — earlier this session we changed the cashbook exact match to use signed comparison. If it passes, the test still serves as a regression lock.

- [ ] **Step 3: If needed, refactor**

If the test fails, locate the offending `ABS(ABS(at_value) - ...)` in `sql_rag/bank_duplicates.py` and change to `ABS(at_value - {signed_pence})` per the existing pattern at lines around 349.

- [ ] **Step 4: Run full suite**

```bash
source venv/bin/activate && python -m pytest tests/ -x -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit (only if changes made)**

```bash
git add sql_rag/bank_duplicates.py tests/test_duplicate_check.py
git commit -m "test(bank_duplicates): pin signed-comparison invariant

Adds a test that fails if ABS(ABS(at_value) - ...) reappears in
bank_duplicates.py — locking out the historical sign-blind regression
that bit production multiple times.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

If no source change was needed, commit only the test file.

---

## Task 10: Refactor Opera 3 mirrors

**Files:**
- Modify: `sql_rag/bank_import_opera3.py`
- Modify: `sql_rag/opera3_foxpro_import.py`

- [ ] **Step 1: Write the callsite tests**

Append to `tests/test_duplicate_check.py`:

```python
def test_opera3_bank_import_uses_check_for_duplicate():
    from pathlib import Path
    f = Path(__file__).resolve().parent.parent / "sql_rag" / "bank_import_opera3.py"
    src = f.read_text(encoding='utf-8')
    assert "check_for_duplicate" in src, \
        "Opera 3 _is_already_posted should delegate"
    assert "ABS(ABS(" not in src.replace("ABS(ABS()", "")  # noqa: helper for grep


def test_opera3_foxpro_import_uses_check_for_duplicate():
    from pathlib import Path
    f = Path(__file__).resolve().parent.parent / "sql_rag" / "opera3_foxpro_import.py"
    src = f.read_text(encoding='utf-8')
    assert "check_for_duplicate" in src, \
        "Opera 3 check_duplicate_before_posting should delegate"
```

- [ ] **Step 2: Run tests**

```bash
source venv/bin/activate && python -m pytest tests/test_duplicate_check.py -v
```

Expected: 2 new tests FAIL.

- [ ] **Step 3: Refactor `bank_import_opera3.py::_is_already_posted`**

Find the method (currently around line 1058) and replace its body with a delegation pattern using `Opera3DataSource`. Pattern matches Task 7 but uses `self.reader` instead of `self.sql_connector`:

```python
    def _is_already_posted(self, txn, bank_code: str = "") -> Tuple[bool, str]:
        """Opera 3 mirror of bank_import._is_already_posted — delegates
        to the same check_for_duplicate function, just with a FoxPro
        DataSource instead of SQL.
        """
        from sql_rag.duplicate_check import (
            check_for_duplicate,
            DuplicateMatchKind,
        )
        from sql_rag.duplicate_check_o3 import Opera3DataSource

        if not txn.action or txn.action in ('skip',):
            return False, ""

        ds = Opera3DataSource(self.reader)
        try:
            result = check_for_duplicate(
                data_source=ds,
                bank_code=bank_code or self.bank_code,
                transaction_date=txn.date,
                signed_amount_pounds=float(txn.amount),
                action=txn.action,
                account_code=getattr(txn, 'matched_account', None) or '',
                description=getattr(txn, 'name', '') or '',
                reference=getattr(txn, 'reference', '') or '',
            )
        except ValueError:
            return False, ""

        if result.kind is DuplicateMatchKind.CASHBOOK_DUPLICATE:
            txn.is_duplicate = True
            return True, result.reason
        if result.kind is DuplicateMatchKind.LEDGER_ALLOCATION_TARGET:
            return False, f"allocation target: {result.reason}"
        return False, ""
```

- [ ] **Step 4: Refactor `opera3_foxpro_import.py::check_duplicate_before_posting`**

Find the method and replace its body with the same pattern as Task 8 but with `Opera3DataSource`:

```python
    def check_duplicate_before_posting(
        self,
        bank_account: str,
        transaction_date,
        amount_pounds: float,
        account_code: str = '',
        account_type: str = 'nominal',
        date_tolerance_days: int = 1,
        signed_amount_pounds=None,
    ) -> dict:
        """Opera 3 mirror of opera_sql_import.check_duplicate_before_posting."""
        from datetime import date as _date_type
        from sql_rag.duplicate_check import (
            check_for_duplicate,
            DuplicateMatchKind,
        )
        from sql_rag.duplicate_check_o3 import Opera3DataSource

        if isinstance(transaction_date, str):
            txn_date = _date_type.fromisoformat(transaction_date[:10])
        elif hasattr(transaction_date, 'date'):
            txn_date = transaction_date.date()
        else:
            txn_date = transaction_date

        type_to_action = {
            'customer': 'sales_receipt',
            'customer_refund': 'sales_refund',
            'supplier': 'purchase_payment',
            'supplier_refund': 'purchase_refund',
            'transfer_out': 'bank_transfer',
            'transfer_in': 'bank_transfer',
            'transfer': 'bank_transfer',
            'nominal_payment': 'nominal_payment',
            'nominal_receipt': 'nominal_receipt',
            'nominal': 'nominal_payment',
        }
        action = type_to_action.get(account_type, 'nominal_payment')

        if signed_amount_pounds is None:
            if account_type in ('supplier', 'customer_refund',
                                'nominal_payment', 'transfer_out'):
                signed_amount_pounds = -abs(amount_pounds)
            else:
                signed_amount_pounds = abs(amount_pounds)

        ds = Opera3DataSource(self.reader)
        try:
            result = check_for_duplicate(
                data_source=ds,
                bank_code=bank_account,
                transaction_date=txn_date,
                signed_amount_pounds=float(signed_amount_pounds),
                action=action,
                account_code=account_code or None,
                description='',
                reference='',
                date_tolerance_days=date_tolerance_days,
            )
        except ValueError as e:
            return {'is_duplicate': False, 'details': f"unknown action_type {account_type!r}: {e}"}

        if result.kind is DuplicateMatchKind.CASHBOOK_DUPLICATE:
            return {
                'is_duplicate': True,
                'location': 'cashbook',
                'details': result.reason,
                'entry_number': result.matched_entry,
            }
        return {'is_duplicate': False, 'details': result.reason}
```

- [ ] **Step 5: Run tests**

```bash
source venv/bin/activate && python -m pytest tests/ -x -q
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add sql_rag/bank_import_opera3.py sql_rag/opera3_foxpro_import.py tests/test_duplicate_check.py
git commit -m "refactor(opera3): both Opera 3 callsites delegate to check_for_duplicate

Opera 3 parity: bank_import_opera3._is_already_posted and
opera3_foxpro_import.check_duplicate_before_posting now delegate to
the same check_for_duplicate function via Opera3DataSource. Six call
sites across SE and Opera 3 are now one function call repeated six
times. End of bug class.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: Live verification on Cloudsis

**Files:**
- Manual test against the running Cloudsis BB005 Monzo case.

- [ ] **Step 1: Restart API**

```bash
pkill -f "uvicorn api.main" 2>/dev/null; sleep 2
source venv/bin/activate && nohup uvicorn api.main:app --reload --host 0.0.0.0 --port 8000 > /tmp/api.log 2>&1 & disown
sleep 4
curl -s http://localhost:8000/api/health
```

Expected: `{"status":"healthy","service":"sql-rag-api"}`.

- [ ] **Step 2: Trigger a duplicate-check via the matching API**

Hit the analyse endpoint for the Cloudsis April Monzo statement:

```bash
curl -s -X POST -H 'Content-Type: application/json' -d '{"username":"admin","password":"Harry"}' http://localhost:8000/api/auth/force-clear-session > /dev/null
TOKEN=$(curl -s -X POST -c /tmp/c.txt -H 'Content-Type: application/json' -d '{"username":"admin","password":"Harry"}' http://localhost:8000/api/auth/login | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))")

curl -s -b /tmp/c.txt -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/bank-import/preview-from-pdf?file_path=/Users/maccb/Downloads/bank-statements/BB005-monzo/Monzo_bank_statement_2026-04-01-2026-04-28_2944.pdf&bank_code=BB005" \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('totals:',
  'auto_matched=', len(d.get('matched_receipts', [])) + len(d.get('matched_payments', [])) + len(d.get('matched_refunds', [])),
  'already_posted=', len(d.get('already_posted', [])),
  'unmatched=', len(d.get('unmatched', [])),
)
"
```

Expected: a sensible breakdown with `already_posted` ≥ 7 (the entries that were imported earlier in the session).

- [ ] **Step 3: Verify the log shows function-derived reasons**

```bash
grep -E "cashbook entry .* already posted|cashbook_duplicate|allocation target" /tmp/api.log | tail -10
```

Expected: log lines whose reasons clearly come from `check_for_duplicate`.

- [ ] **Step 4: Mark task done — no commit (verification only)**

---

## Task 12: KB updates

**Files:**
- Modify: `apps/core/docs/opera_knowledge_base.md`
- Create: `~/opera-knowledge-ref/packages/opera-knowledge/business-rules/duplicate-check.md`

- [ ] **Step 1: Append to local KB**

Append to `apps/core/docs/opera_knowledge_base.md`:

```markdown

## Duplicate-Check Decision Function

**Single source of truth** for "is this bank line already in Opera?". Lives at `sql_rag/duplicate_check.py::check_for_duplicate`. Six call sites across SE and Opera 3 delegate to it:

- `sql_rag/bank_import.py::_is_already_posted` (analyse-time, SE)
- `sql_rag/bank_import_opera3.py::_is_already_posted` (analyse-time, O3)
- `sql_rag/opera_sql_import.py::check_duplicate_before_posting` (post-time, SE)
- `sql_rag/opera3_foxpro_import.py::check_duplicate_before_posting` (post-time, O3)
- `sql_rag/bank_duplicates.py::find_duplicates` (cashbook "exact" match)

Result kinds:
- `CASHBOOK_DUPLICATE`: an aentry of the action's expected at_type and signed amount exists. Refuse to post.
- `LEDGER_ALLOCATION_TARGET`: no cashbook entry, but a stran/ptran credit-note-type row matches the refund. POST and optionally allocate.
- `NONE`: safe to post.

The `ACTION_TYPE_MAP` in the module is the authoritative reference for which `at_type` / `st_trtype` / `pt_trtype` corresponds to each bank-import action. Cashbook conventions (1=Nominal Pmt, 2=Nominal Rcpt, 3=Sales Refund, 4=Sales Receipt, 5=Purchase Pmt, 6=Purchase Refund, 8=Bank Transfer) are mirrored from CLAUDE.md and the central KB.

Sign-aware AND type-aware throughout. The historical bug class (`ABS(ABS(value) - amount)` matching opposite-direction transactions) is locked out by tests in `tests/test_duplicate_check.py` and `tests/test_duplicate_check_regression.py`.
```

- [ ] **Step 2: Pull and write the central KB file**

```bash
cd ~/opera-knowledge-ref && git stash push -u -m "preserve unrelated work" 2>/dev/null
cd ~/opera-knowledge-ref && git pull --rebase origin main
cat > ~/opera-knowledge-ref/packages/opera-knowledge/business-rules/duplicate-check.md <<'EOF'
# Duplicate-Check Decision

The single canonical answer to **"is this bank line already posted in Opera?"**. All consumers MUST call `sql_rag.duplicate_check.check_for_duplicate`. No inline SQL, no per-callsite type filters, no ABS-on-ABS comparisons.

## Result kinds

- `CASHBOOK_DUPLICATE`: a matching aentry already exists for this action's at_type and signed amount. The bank line is a duplicate; refuse to post.
- `LEDGER_ALLOCATION_TARGET`: no cashbook entry yet, but a credit-note-type row exists in stran (sales_refund) or ptran (purchase_refund). The bank line is the **missing payment**, not a duplicate — POST and optionally allocate.
- `NONE`: no match; safe to post.

## Action-type map (canonical)

| Action | at_type | Ledger filter |
|---|---|---|
| sales_receipt | 4 | stran st_trtype='R' |
| sales_refund | 3 | stran st_trtype='F' |
| purchase_payment | 5 | ptran pt_trtype='P' |
| purchase_refund | 6 | ptran pt_trtype='F' |
| nominal_payment | 1 | (no ledger row) |
| nominal_receipt | 2 | (no ledger row) |
| bank_transfer | 8 | (no ledger row) |

The cashbook check is type-aware AND sign-aware: it queries `ABS(at_value - signed_pence) < 1 AND at_type = expected_at_type`. NEVER `ABS(ABS(at_value) - amount_pence)`. The latter sign-blind form created multiple production bugs (e.g. a +£198 receipt and a -£198 refund matched as duplicates). It's banned.

## Why the ledger advisory matters

A common production confusion: a customer has an unallocated -£198 credit note (stran, type='F') and the bank statement shows a -£198 refund payment to that customer. The credit note is the **target** the new refund will allocate to — it is NOT a duplicate of the new refund. The function returns `LEDGER_ALLOCATION_TARGET` so callers know to POST the refund and optionally trigger an allocation step.

## Caller contract

Wrappers that return the legacy `(is_duplicate, reason)` tuple or the legacy `{'is_duplicate', 'details', ...}` dict translate `CASHBOOK_DUPLICATE` to "yes, duplicate", `LEDGER_ALLOCATION_TARGET` to "no, allocation target — proceed", and `NONE` to "no". They do not introduce new logic.

## Do not duplicate this logic

If you find inline duplicate-check SQL anywhere in the codebase, it is a bug. Replace it with a `check_for_duplicate` call.
EOF
cd ~/opera-knowledge-ref && git add packages/opera-knowledge/business-rules/duplicate-check.md
git commit -m "Document duplicate-check decision function

The single canonical answer to 'is this bank line already posted?'.
All consumers must call sql_rag.duplicate_check.check_for_duplicate.

Six call sites previously had divergent logic — now consolidated.
Includes the action-type map (cashbook at_type and ledger st_trtype/
pt_trtype per action) and the LEDGER_ALLOCATION_TARGET semantics that
prevent treating credit notes as duplicates.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
git push origin main
cd ~/opera-knowledge-ref && git stash pop 2>/dev/null
```

- [ ] **Step 3: Commit local KB**

```bash
git -C /Users/maccb/llmragsql add apps/core/docs/opera_knowledge_base.md
git -C /Users/maccb/llmragsql commit -m "$(cat <<'EOF'
docs(kb): document the duplicate-check function

Mirrors the central KB entry at
opera-knowledge-ref/packages/opera-knowledge/business-rules/
duplicate-check.md. Documents the result kinds, action-type map, and
the contract that LEDGER_ALLOCATION_TARGET means "post, don't refuse".

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git -C /Users/maccb/llmragsql push
```

- [ ] **Step 4: Verify both KBs reach their remotes**

```bash
echo "=== Local KB ==="
git -C /Users/maccb/llmragsql log --oneline -1 -- apps/core/docs/opera_knowledge_base.md
echo "=== Central KB ==="
git -C ~/opera-knowledge-ref log --oneline -1 -- packages/opera-knowledge/business-rules/duplicate-check.md
echo "=== Central remote check ==="
git -C ~/opera-knowledge-ref status -sb | head -3
```

---

## Done Criteria (final)

- [ ] `sql_rag/duplicate_check.py` exists with full type and protocol coverage.
- [ ] `sql_rag/duplicate_check_se.py` and `sql_rag/duplicate_check_o3.py` exist and satisfy the protocol.
- [ ] `tests/test_duplicate_check.py` and `tests/test_duplicate_check_regression.py` pass with full matrix coverage.
- [ ] Six call sites delegate to `check_for_duplicate`; no inline duplicate-check SQL remains in `bank_import.py`, `bank_import_opera3.py`, `opera_sql_import.py::check_duplicate_before_posting`, `opera3_foxpro_import.py::check_duplicate_before_posting`, or `bank_duplicates.py::find_duplicates "exact"`.
- [ ] No `ABS(ABS(...))` patterns remain in any of those files (test enforces).
- [ ] Existing test suite passes.
- [ ] Live test on Cloudsis Monzo statement: analyse returns sensible already_posted set; logs show function-derived reasons.
- [ ] Local KB and central KB both updated and pushed.
