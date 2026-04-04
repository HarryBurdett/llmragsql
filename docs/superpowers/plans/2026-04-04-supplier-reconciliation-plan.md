# Supplier Statement Reconciliation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the supplier statement reconciliation system so it is mathematically precise, fully automated, and production-ready.

**Architecture:** Two-list comparison of outstanding items (their statement vs our purchase ledger). Match by reference only. Unmatched items explain the balance difference — always, to the penny. Background email processing with configurable per-supplier automation flags.

**Tech Stack:** Python FastAPI, React/TypeScript, SQLite, Opera SQL SE (via pyodbc), Gemini Vision API

**Spec:** `docs/superpowers/specs/2026-04-04-supplier-reconciliation-design.md`

---

## File Structure

### New files
| File | Responsibility |
|------|---------------|
| `sql_rag/supplier_reconciler.py` | New reconciliation engine — two-list comparison, math guarantee |
| `sql_rag/supplier_config.py` | Supplier config table management + Opera sync |
| `tests/test_supplier_reconciler.py` | Reconciliation engine tests |
| `tests/test_supplier_config.py` | Supplier config sync tests |

### Modified files
| File | Changes |
|------|---------|
| `sql_rag/supplier_statement_db.py` | Add `supplier_config` table, add `unmatched_opera` table (replaces `statement_opera_only`), schema migrations |
| `sql_rag/supplier_statement_extract.py` | Add reference cleanup (strip `*OVERDUE*` etc.) |
| `apps/suppliers/api/background.py` | Plug in new reconciler, fix duplicate prevention, add sender verification |
| `apps/suppliers/api/routes.py` | Update reconciliation endpoints, add supplier config endpoints, fix response email generation |
| `frontend/src/pages/SupplierStatementDetail.tsx` | New reconciliation panel, two-table layout, correct math |
| `frontend/src/pages/SupplierStatementQueue.tsx` | Show supplier name, correct status display |
| `frontend/src/pages/SupplierDirectory.tsx` | Add flag management (auto-respond, never-communicate, etc.) |
| `frontend/src/pages/SupplierSettings.tsx` | Add `next_payment_run_date` parameter |

### Deleted/replaced
| File | Reason |
|------|--------|
| `sql_rag/supplier_statement_reconcile.py` | Replaced by `supplier_reconciler.py` — clean break, no salvageable logic |

---

### Task 1: New Reconciliation Engine

**Files:**
- Create: `sql_rag/supplier_reconciler.py`
- Create: `tests/test_supplier_reconciler.py`

This is the core. Everything else depends on it being correct.

- [ ] **Step 1: Write test for basic reference matching**

```python
# tests/test_supplier_reconciler.py
"""
Tests for the supplier statement reconciliation engine.
Uses pure data — no database connections needed.
"""

def test_exact_match_all_agreed():
    """Both sides have the same items — balances agree."""
    their_items = [
        {'reference': 'INV-001', 'amount': 100.00, 'type': 'invoice'},
        {'reference': 'INV-002', 'amount': 200.00, 'type': 'invoice'},
    ]
    our_items = [
        {'reference': 'INV-001', 'pt_trbal': 100.00},
        {'reference': 'INV-002', 'pt_trbal': 200.00},
    ]
    result = reconcile(their_items, our_items)
    assert len(result.agreed) == 2
    assert len(result.theirs_only) == 0
    assert len(result.ours_only) == 0
    assert result.difference == 0.0
    assert result.math_checks_out == True
```

- [ ] **Step 2: Write test for unmatched items explaining the difference**

```python
def test_unmatched_items_explain_difference():
    """Items on one side only fully explain the balance difference."""
    their_items = [
        {'reference': 'INV-001', 'amount': 100.00, 'type': 'invoice'},
        {'reference': 'INV-999', 'amount': 50.00, 'type': 'invoice'},  # not on ours
    ]
    our_items = [
        {'reference': 'INV-001', 'pt_trbal': 100.00},
        {'reference': 'PAY-001', 'pt_trbal': -75.00},  # not on theirs
    ]
    # Their balance = 150, Our balance = 25, Difference = 125
    result = reconcile(their_items, our_items)
    assert len(result.theirs_only) == 1  # INV-999
    assert len(result.ours_only) == 1    # PAY-001
    assert result.difference == 125.0
    # theirs_only net (50) - ours_only net (-75) = 50 + 75 = 125 = difference
    assert result.math_checks_out == True
```

- [ ] **Step 3: Write test for case-insensitive matching and reference cleanup**

```python
def test_case_insensitive_and_cleanup():
    """References match case-insensitively with artefacts stripped."""
    their_items = [
        {'reference': 'inv-001 *OVERDUE*', 'amount': 100.00, 'type': 'invoice'},
    ]
    our_items = [
        {'reference': 'INV-001', 'pt_trbal': 100.00},
    ]
    result = reconcile(their_items, our_items)
    assert len(result.agreed) == 1
    assert len(result.theirs_only) == 0
    assert len(result.ours_only) == 0
```

- [ ] **Step 4: Write test for amount difference on matched reference**

```python
def test_amount_difference_on_agreed_item():
    """Same reference, different amount — flagged but still matched."""
    their_items = [
        {'reference': 'INV-001', 'amount': 110.00, 'type': 'invoice'},
    ]
    our_items = [
        {'reference': 'INV-001', 'pt_trbal': 100.00},
    ]
    result = reconcile(their_items, our_items)
    assert len(result.agreed) == 1
    assert result.agreed[0].amount_difference == 10.00
    assert result.difference == 10.0
    assert result.math_checks_out == True
```

- [ ] **Step 5: Write test for empty references always unmatched**

```python
def test_empty_reference_always_unmatched():
    """Opera items with no reference go to ours_only."""
    their_items = [
        {'reference': 'INV-001', 'amount': 100.00, 'type': 'invoice'},
    ]
    our_items = [
        {'reference': 'INV-001', 'pt_trbal': 100.00},
        {'reference': '', 'pt_trbal': 50.00},
        {'reference': None, 'pt_trbal': -25.00},
    ]
    result = reconcile(their_items, our_items)
    assert len(result.agreed) == 1
    assert len(result.ours_only) == 2
    assert result.math_checks_out == True
```

- [ ] **Step 6: Write test for duplicate references in Opera**

```python
def test_duplicate_opera_references_grouped():
    """Multiple Opera items with same ref are grouped."""
    their_items = [
        {'reference': 'INV-001', 'amount': 300.00, 'type': 'invoice'},
    ]
    our_items = [
        {'reference': 'INV-001', 'pt_trbal': 200.00},
        {'reference': 'INV-001', 'pt_trbal': 100.00},
    ]
    result = reconcile(their_items, our_items)
    assert len(result.agreed) == 1
    assert result.agreed[0].our_amount == 300.00  # aggregated
    assert result.agreed[0].amount_difference == 0.0
```

- [ ] **Step 7: Run tests to verify they fail**

Run: `cd /Users/maccb/llmragsql && source venv/bin/activate && python -m pytest tests/test_supplier_reconciler.py -v`
Expected: All FAIL (module not found)

- [ ] **Step 8: Implement the reconciliation engine**

Create `sql_rag/supplier_reconciler.py`:

```python
"""
Supplier Statement Reconciliation Engine.

Compares two lists of outstanding transactions:
- Their side: extracted from supplier statement PDF
- Our side: from Opera ptran (pt_trbal <> 0)

Match by reference only. Unmatched items explain the balance difference.
The math ALWAYS balances — if it doesn't, there's a bug.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import re

def clean_reference(ref: str) -> str:
    """Normalise a reference for matching. Case-insensitive, strip artefacts."""
    if not ref:
        return ''
    ref = ref.strip().upper()
    ref = re.sub(r'\*OVERDUE\*', '', ref)
    ref = re.sub(r'\*+', '', ref)
    return ref.strip()


@dataclass
class AgreedItem:
    reference: str
    their_amount: float
    our_amount: float  # sum of pt_trbal for this ref
    amount_difference: float  # their - ours
    their_detail: Dict[str, Any] = field(default_factory=dict)
    our_details: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class UnmatchedItem:
    reference: str
    amount: float  # signed: positive = debit, negative = credit
    detail: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReconciliationResult:
    their_balance: float = 0.0
    our_balance: float = 0.0
    difference: float = 0.0

    agreed: List[AgreedItem] = field(default_factory=list)
    theirs_only: List[UnmatchedItem] = field(default_factory=list)
    ours_only: List[UnmatchedItem] = field(default_factory=list)

    # Reconciliation check
    theirs_only_net: float = 0.0
    ours_only_net: float = 0.0
    amount_diffs_net: float = 0.0
    math_checks_out: bool = False

    supplier_code: str = ''
    supplier_name: str = ''
    statement_date: str = ''


def reconcile(
    their_items: List[Dict[str, Any]],
    our_items: List[Dict[str, Any]],
) -> ReconciliationResult:
    """
    Reconcile supplier statement items against Opera outstanding items.

    Args:
        their_items: List of dicts with 'reference', 'amount' (positive = debit),
                     and optional 'type', 'date', 'debit', 'credit'.
        our_items: List of dicts with 'reference' and 'pt_trbal' (signed).

    Returns:
        ReconciliationResult with agreed, theirs_only, ours_only lists
        and a math_checks_out flag that MUST be True.
    """
    result = ReconciliationResult()

    # Calculate balances
    for item in their_items:
        amt = item.get('debit', 0) or 0
        crd = item.get('credit', 0) or 0
        if amt == 0 and crd == 0:
            amt = item.get('amount', 0) or 0
        result.their_balance += amt - crd

    result.our_balance = sum(item.get('pt_trbal', 0) or 0 for item in our_items)
    result.difference = result.their_balance - result.our_balance

    # Build Opera lookup: group by cleaned reference
    # Multiple items with same ref are aggregated
    opera_by_ref: Dict[str, List[Dict]] = {}
    opera_no_ref: List[Dict] = []

    for item in our_items:
        ref = clean_reference(item.get('reference', '') or '')
        if not ref:
            opera_no_ref.append(item)
        else:
            opera_by_ref.setdefault(ref, []).append(item)

    # Match each statement line against Opera
    matched_opera_refs = set()

    for item in their_items:
        ref = clean_reference(item.get('reference', '') or '')
        amt = item.get('debit', 0) or 0
        crd = item.get('credit', 0) or 0
        if amt == 0 and crd == 0:
            amt = item.get('amount', 0) or 0
        their_net = amt - crd

        if ref and ref in opera_by_ref:
            # Matched by reference
            opera_group = opera_by_ref[ref]
            our_total = sum(o.get('pt_trbal', 0) or 0 for o in opera_group)
            diff = their_net - our_total

            result.agreed.append(AgreedItem(
                reference=ref,
                their_amount=their_net,
                our_amount=our_total,
                amount_difference=diff,
                their_detail=item,
                our_details=opera_group,
            ))
            matched_opera_refs.add(ref)
        else:
            # Not matched — on their statement only
            result.theirs_only.append(UnmatchedItem(
                reference=ref or '(no ref)',
                amount=their_net,
                detail=item,
            ))

    # Opera items not matched to any statement line
    for ref, opera_group in opera_by_ref.items():
        if ref not in matched_opera_refs:
            total = sum(o.get('pt_trbal', 0) or 0 for o in opera_group)
            result.ours_only.append(UnmatchedItem(
                reference=ref,
                amount=total,
                detail=opera_group[0] if len(opera_group) == 1 else {'items': opera_group},
            ))

    # Empty-ref Opera items always unmatched
    for item in opera_no_ref:
        result.ours_only.append(UnmatchedItem(
            reference='(no ref)',
            amount=item.get('pt_trbal', 0) or 0,
            detail=item,
        ))

    # Calculate nets
    result.theirs_only_net = sum(i.amount for i in result.theirs_only)
    result.ours_only_net = sum(i.amount for i in result.ours_only)
    result.amount_diffs_net = sum(i.amount_difference for i in result.agreed)

    # THE GUARANTEE: difference = theirs_only_net - ours_only_net + amount_diffs_net
    explained = result.theirs_only_net - result.ours_only_net + result.amount_diffs_net
    result.math_checks_out = abs(result.difference - explained) < 0.01

    return result
```

- [ ] **Step 9: Run tests to verify they pass**

Run: `cd /Users/maccb/llmragsql && source venv/bin/activate && python -m pytest tests/test_supplier_reconciler.py -v`
Expected: All PASS

- [ ] **Step 10: Commit**

```bash
git add sql_rag/supplier_reconciler.py tests/test_supplier_reconciler.py
git commit -m "feat: new supplier reconciliation engine — reference-only matching with math guarantee"
```

---

### Task 2: Supplier Config Table and Opera Sync

**Files:**
- Create: `sql_rag/supplier_config.py`
- Create: `tests/test_supplier_config.py`
- Modify: `sql_rag/supplier_statement_db.py`

- [ ] **Step 1: Add `supplier_config` table to DB schema**

In `sql_rag/supplier_statement_db.py`, add after existing table creation:

```python
cursor.execute("""
    CREATE TABLE IF NOT EXISTS supplier_config (
        account_code TEXT PRIMARY KEY,
        name TEXT,
        balance DECIMAL(15,2),
        payment_terms_days INTEGER,
        payment_method TEXT,
        reconciliation_active BOOLEAN DEFAULT 1,
        auto_respond BOOLEAN DEFAULT 0,
        never_communicate BOOLEAN DEFAULT 0,
        statements_contact_position TEXT,
        last_synced DATETIME,
        last_statement_date DATE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
""")
```

- [ ] **Step 2: Write test for sync from Opera**

```python
# tests/test_supplier_config.py
def test_sync_creates_new_suppliers():
    """Sync detects new suppliers from Opera pname."""
    # Uses mock SQL connector returning test data
    ...

def test_sync_preserves_local_flags():
    """Sync updates name/balance but never overwrites our flags."""
    ...

def test_sync_excludes_dormant():
    """Dormant suppliers (pn_dormant=1) are excluded."""
    ...
```

- [ ] **Step 3: Implement `supplier_config.py`**

```python
"""
Supplier config management.
Syncs from Opera pname, stores local automation flags.
"""

class SupplierConfigManager:
    def __init__(self, db_path, sql_connector):
        ...

    def sync_from_opera(self):
        """One-way sync: read pname, update local records, preserve flags."""
        ...

    def get_config(self, account_code) -> dict:
        """Get supplier config with flags."""
        ...

    def update_flags(self, account_code, **flags):
        """Update local flags only (never writes to Opera)."""
        ...

    def get_all(self, active_only=False) -> list:
        """List all suppliers with their config."""
        ...
```

- [ ] **Step 4: Run tests, verify pass**

- [ ] **Step 5: Commit**

```bash
git add sql_rag/supplier_config.py sql_rag/supplier_statement_db.py tests/test_supplier_config.py
git commit -m "feat: supplier config table with Opera sync and local flags"
```

---

### Task 3: Update Background Processor

**Files:**
- Modify: `apps/suppliers/api/background.py`

- [ ] **Step 1: Replace reconciler import with new engine**

Change `from sql_rag.supplier_statement_reconcile import ...` to `from sql_rag.supplier_reconciler import reconcile`.

- [ ] **Step 2: Fix duplicate prevention — database check only**

Replace in-memory `_processed_email_ids` set with database query on `source_email_id`. Remove the set entirely.

- [ ] **Step 3: Add sender verification step**

After identifying the supplier, check sender email/name against Opera contacts. Park as `unverified_sender` if no match.

- [ ] **Step 4: Add supplier config flag checks**

Read `supplier_config` for the matched supplier. Skip if `reconciliation_active` is false.

- [ ] **Step 5: Plug in new reconciler**

Build `their_items` from extracted statement lines. Build `our_items` by querying `ptran WHERE pt_trbal <> 0` with parameterised SQL and NOLOCK. Call `reconcile()`. Assert `math_checks_out`.

- [ ] **Step 6: Save reconciliation results**

Save agreed items, theirs_only, ours_only to the database. Use `pt_trbal` as signed value for ours_only.

- [ ] **Step 7: Implement response decision logic**

Check `never_communicate`, balance agreement, `auto_respond` flags per the spec decision table.

- [ ] **Step 8: Add audit trail logging**

Log every event (received, verified, reconciled, held, sent) to `supplier_communications`.

- [ ] **Step 9: Commit**

```bash
git add apps/suppliers/api/background.py
git commit -m "feat: background processor uses new reconciler with sender verification and audit trail"
```

---

### Task 4: Update API Endpoints

**Files:**
- Modify: `apps/suppliers/api/routes.py`

- [ ] **Step 1: Add supplier config endpoints**

```
GET  /api/supplier-config              — list all with flags
GET  /api/supplier-config/{account}    — single supplier config
PUT  /api/supplier-config/{account}    — update flags
POST /api/supplier-config/sync         — trigger Opera sync
```

- [ ] **Step 2: Update statement lines endpoint**

Return `theirs_only`, `ours_only`, `agreed` lists from the new reconciler format. Include signed values. Calculate `theirs_only_net`, `ours_only_net`, `amount_diffs_net` in the summary. Return `math_checks_out`.

- [ ] **Step 3: Update statement detail endpoint**

Return `opera_balance` (raw `pn_currbal`), `their_balance` (statement closing), `difference`. Use correct sign convention.

- [ ] **Step 4: Update approve endpoint**

Accept all statement statuses (not just `queued`). Use new response email generator with payment estimation.

- [ ] **Step 5: Update response email generation**

Rewrite `_generate_default_response` per spec Section 4:
- Balance comparison
- Items requiring supplier attention (theirs_only invoices/credits we need)
- Payments we've made (ours_only payments with date/ref)
- Payment schedule (using `pt_dueday` and `next_payment_run_date`)
- Attach original PDF

- [ ] **Step 6: Add `next_payment_run_date` to settings**

Add to `supplier_automation_config` defaults. Expose via existing settings endpoint.

- [ ] **Step 7: Add audit trail endpoint**

```
GET /api/supplier-communications/{account} — all communications for a supplier
```

- [ ] **Step 8: Commit**

```bash
git add apps/suppliers/api/routes.py
git commit -m "feat: updated API endpoints for new reconciler, supplier config, payment estimation"
```

---

### Task 5: Update Frontend — Statement Detail

**Files:**
- Modify: `frontend/src/pages/SupplierStatementDetail.tsx`

- [ ] **Step 1: Replace reconciliation panel**

Single panel showing:
```
Balance per their statement:    £X,XXX.XX
Balance per our records:        £X,XXX.XX
Difference:                     £X,XXX.XX

Represented by:
  On their statement, not ours (N items)    £X,XXX.XX
  On our account, not theirs (N items)      £X,XXX.XX
  Amount differences (N items)              £X,XXX.XX
  Total                                     £X,XXX.XX  ← must equal difference
```

- [ ] **Step 2: Update statement lines table**

Columns: Date, Reference, Type, Debit, Credit, Exists, Status. No balance column.

- [ ] **Step 3: Update "our only" table**

Columns: Date, Reference, Type, Debit, Credit. Using signed values to put amounts in correct column.

- [ ] **Step 4: Keep PDF viewer and approve button as-is**

Already working — no changes needed.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/pages/SupplierStatementDetail.tsx
git commit -m "feat: new reconciliation display with guaranteed math"
```

---

### Task 6: Update Frontend — Supplier Directory

**Files:**
- Modify: `frontend/src/pages/SupplierDirectory.tsx`

- [ ] **Step 1: Add flag management**

Show each supplier with toggles for:
- Reconciliation active (on/off)
- Auto-respond (on/off)
- Never communicate (on/off)
- Statements contact (dropdown from Opera contacts)

- [ ] **Step 2: Add sync button**

Manual "Sync from Opera" button that triggers `/api/supplier-config/sync`.

- [ ] **Step 3: Show last statement date and balance**

Per supplier row: last statement received, current balance, number of open queries.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/pages/SupplierDirectory.tsx
git commit -m "feat: supplier directory with automation flag management"
```

---

### Task 7: Update Frontend — Settings and Queue

**Files:**
- Modify: `frontend/src/pages/SupplierSettings.tsx`
- Modify: `frontend/src/pages/SupplierStatementQueue.tsx`

- [ ] **Step 1: Add `next_payment_run_date` to settings page**

Date picker for the next payment run. Saved to `supplier_automation_config`.

- [ ] **Step 2: Verify queue shows supplier names**

Already implemented — verify it works with the updated API.

- [ ] **Step 3: Commit**

```bash
git add frontend/src/pages/SupplierSettings.tsx frontend/src/pages/SupplierStatementQueue.tsx
git commit -m "feat: payment run date setting, queue display updates"
```

---

### Task 8: Cleanup and Remove Old Code

**Files:**
- Delete: `sql_rag/supplier_statement_reconcile.py`
- Modify: any files importing the old reconciler

- [ ] **Step 1: Find all imports of old reconciler**

```bash
grep -r "supplier_statement_reconcile" --include="*.py" .
```

- [ ] **Step 2: Update all imports to use new `supplier_reconciler`**

- [ ] **Step 3: Remove old file**

- [ ] **Step 4: Run full test suite**

```bash
python -m pytest tests/ -v
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: remove old reconciler, all imports point to new engine"
```

---

### Task 9: End-to-End Test

**Files:**
- Modify: `scripts/create_test_statements_v2.py` (if needed)

- [ ] **Step 1: Reset test data**

Clear `supplier_statements`, `statement_lines`, `statement_opera_only` tables.

- [ ] **Step 2: Send test statements to Intsys mailbox**

Run the test statement generator or forward statements manually.

- [ ] **Step 3: Verify background processor picks them up**

Check API logs for detection, extraction, sender verification, reconciliation.

- [ ] **Step 4: Verify reconciliation math on every statement**

For each statement: confirm `math_checks_out == True`. Verify the difference is fully explained by the two lists.

- [ ] **Step 5: Verify response emails**

Check that auto-respond suppliers get emails. Check that held suppliers appear in review queue. Check email content matches spec Section 4.

- [ ] **Step 6: Verify supplier directory flags work**

Set Amazon to `never_communicate`. Send a statement. Verify no email sent but reconciliation saved.

- [ ] **Step 7: Commit any fixes**

```bash
git add -A
git commit -m "test: end-to-end supplier reconciliation verified"
```
