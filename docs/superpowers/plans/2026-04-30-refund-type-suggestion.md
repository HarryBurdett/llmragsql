# Refund-Type Suggestion for Unmatched Bank Lines — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a bank statement row lands in the *Unmatched Transactions* bucket, default the Type dropdown to **Sales Refund** (negative amount + customer name match) or **Purchase Refund** (positive amount + supplier name match) instead of always defaulting to Nominal. Leave the row in the Unmatched bucket — the user reviews and ticks Include before importing.

**Architecture:** Two coordinated changes. (1) `extract_payee_name_full()` in `sql_rag/bank_import.py` strips trailing bank-method suffixes like `(Faster Payments)` so the existing fuzzy matcher can find the customer/supplier behind names like `Diskel (Faster Payments)`. (2) The frontend's `getSmartDefaultTransactionType` in `Imports.tsx` is extended with the two missing branches: negative + customer match → `sales_refund`, positive + supplier match → `purchase_refund`. Posting itself is unchanged — the suggestion just pre-fills the form.

**Tech Stack:** Python 3 (regex + pytest), TypeScript / React (frontend default).

**Spec:** `docs/superpowers/specs/2026-04-30-bank-method-suffix-stripping-design.md`

---

## File Structure

| File | Role | Status |
|---|---|---|
| `sql_rag/bank_import.py` | Add `_BANK_METHOD_SUFFIX_RE` constant and a strip call inside `extract_payee_name_full()`. | MODIFY |
| `sql_rag/bank_import_opera3.py` | Mirror — same regex, same strip. | MODIFY |
| `tests/test_bank_method_suffix.py` | New unit tests for the strip behaviour. | **CREATE** |
| `frontend/src/pages/Imports.tsx` | Extend `getSmartDefaultTransactionType` with the two refund branches (neg+customer → sales_refund; pos+supplier → purchase_refund). | MODIFY |
| `apps/core/docs/opera_knowledge_base.md` | Short paragraph documenting the new suffixes recognised and the refund-default rule. | MODIFY |

---

## Task 1: Bank-method suffix stripping in `extract_payee_name_full()` (Opera SE)

**Files:**
- Modify: `sql_rag/bank_import.py`
- Create: `tests/test_bank_method_suffix.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_bank_method_suffix.py` with this content:

```python
"""Tests for bank-method suffix stripping in extract_payee_name_full().

Strips trailing parenthesised bank-method suffixes like `(Faster Payments)`,
`(Direct Debit)`, `(Standing Order)`, `(BACS)`, `(CHAPS)`, `(Card Payment)`,
`(Cheque)`, `(Cash)`, `(Online Payment)`, `(Transfer)` — including truncated
forms like `(Faster Pay...)` or `(Faster Pay…)`. Embedded parens that are
part of legal company names (e.g. `(Bristol)`, `(oval)`) are preserved.
"""

import pytest

from sql_rag.bank_import import extract_payee_name_full


@pytest.mark.parametrize("input_text,expected", [
    # Bank-method suffix stripping
    ("Diskel (Faster Payments)", "Diskel"),
    ("Diskel (Faster Payment)", "Diskel"),
    ("Diskel (Faster Pay...)", "Diskel"),
    ("Diskel (Faster Pay…)", "Diskel"),
    ("Customer (Direct Debit)", "Customer"),
    ("Customer (Standing Order)", "Customer"),
    ("Customer (BACS)", "Customer"),
    ("Customer (CHAPS)", "Customer"),
    ("Customer (Card Payment)", "Customer"),
    ("Customer (Cheque)", "Customer"),
    ("Customer (Cash)", "Customer"),
    ("Customer (Online Payment)", "Customer"),
    ("Customer (Transfer)", "Customer"),
    # Embedded parens preserved when there's also a trailing bank-method suffix
    ("P Flannery Plant Hire(oval) Limited (Faster Pay...)",
     "P Flannery Plant Hire(oval) Limited"),
    # Embedded parens preserved when there's NO trailing bank-method suffix
    ("Acme (Bristol) Ltd", "Acme (Bristol) Ltd"),
    ("P Flannery Plant Hire(oval) Limited", "P Flannery Plant Hire(oval) Limited"),
    # Non-matching parens preserved
    ("Customer (Old Name) (Faster Payments)", "Customer (Old Name)"),
    # Empty input
    ("", ""),
])
def test_extract_payee_name_full_strips_bank_method_suffix(input_text, expected):
    assert extract_payee_name_full(input_text) == expected
```

- [ ] **Step 2: Run tests to verify they fail**

Run from the repo root:

```bash
source venv/bin/activate && pytest tests/test_bank_method_suffix.py -v
```

Expected: every test that has a non-trivial expected output fails (the function currently returns the input unchanged for `Diskel (Faster Payments)` etc.). Empty-input and the three "preserved" cases may pass already.

- [ ] **Step 3: Add the regex constant**

Open `sql_rag/bank_import.py`. Find the existing module-level imports (around lines 1–60). After the `import re` line and any other module-level constants but before `def extract_payee_name`, add:

```python
# Trailing-anchored regex matching parenthesised bank-method suffixes appended
# to the end of a bank statement description, e.g. `Diskel (Faster Payments)`,
# `Customer (Direct Debit)`, `Customer (Faster Pay...)`. The `$` anchor is
# critical — embedded parens that are part of a legal company name (e.g.
# `Acme (Bristol) Ltd`, `P Flannery Plant Hire(oval) Limited`) must be
# preserved when there is no trailing bank-method suffix.
_BANK_METHOD_SUFFIX_RE = re.compile(
    r'\s*\(\s*(faster\s*payments?|direct\s*debit|standing\s*order|'
    r'bacs|chaps|card\s*payment|cheque|cash|online\s*payment|'
    r'transfer)\s*(?:\.\.\.|…)?\s*\)\s*$',
    re.IGNORECASE,
)
```

- [ ] **Step 4: Apply the strip in `extract_payee_name_full()`**

Find the end of the `extract_payee_name_full()` function — specifically the `return cleaned if cleaned else text` line at line 194. Replace the final `return` line with a loop that runs the new regex strip, then returns the cleaned text. The current code is:

```python
    cleaned = re.sub(r'\s+(?:to|from)\s*$', '', cleaned, flags=re.IGNORECASE).strip()

    return cleaned if cleaned else text
```

Replace with:

```python
    cleaned = re.sub(r'\s+(?:to|from)\s*$', '', cleaned, flags=re.IGNORECASE).strip()

    # Strip trailing bank-method suffix (e.g. `(Faster Payments)`) — loop because
    # the existing trailing-`Ref:` strip above might have exposed a fresh match.
    result = cleaned if cleaned else text
    while True:
        new_result = _BANK_METHOD_SUFFIX_RE.sub('', result).strip()
        if new_result == result:
            break
        result = new_result

    return result
```

- [ ] **Step 5: Run tests to verify they pass**

Run:

```bash
source venv/bin/activate && pytest tests/test_bank_method_suffix.py -v
```

Expected: all tests pass.

- [ ] **Step 6: Run the full test suite to confirm no regressions**

Run:

```bash
source venv/bin/activate && pytest tests/ -q
```

Expected: all tests pass — no existing tests broken.

- [ ] **Step 7: Commit**

```bash
git add sql_rag/bank_import.py tests/test_bank_method_suffix.py
git commit -m "feat(bank-import): strip bank-method suffix from payee names (Opera SE)"
```

---

## Task 2: Mirror in Opera 3 reconciler

**Files:**
- Modify: `sql_rag/bank_import_opera3.py`

The Opera 3 module mirrors the SE one and keeps its own copy of `extract_payee_name_full`. Apply the same change.

- [ ] **Step 1: Inspect the Opera 3 module**

Find the `extract_payee_name_full` function in `sql_rag/bank_import_opera3.py`. It mirrors the SE version (verify by reading 5–10 lines of context — same prefix-stripping logic, same trailing-suffix logic).

- [ ] **Step 2: Add the regex constant**

Add the same constant just before the function definition (mirror Task 1 Step 3):

```python
# Trailing-anchored regex matching parenthesised bank-method suffixes appended
# to the end of a bank statement description, e.g. `Diskel (Faster Payments)`,
# `Customer (Direct Debit)`, `Customer (Faster Pay...)`. The `$` anchor is
# critical — embedded parens that are part of a legal company name (e.g.
# `Acme (Bristol) Ltd`, `P Flannery Plant Hire(oval) Limited`) must be
# preserved when there is no trailing bank-method suffix.
_BANK_METHOD_SUFFIX_RE = re.compile(
    r'\s*\(\s*(faster\s*payments?|direct\s*debit|standing\s*order|'
    r'bacs|chaps|card\s*payment|cheque|cash|online\s*payment|'
    r'transfer)\s*(?:\.\.\.|…)?\s*\)\s*$',
    re.IGNORECASE,
)
```

- [ ] **Step 3: Apply the strip in `extract_payee_name_full()`**

Find the final `return cleaned if cleaned else text` (or equivalent) line in the Opera 3 module's `extract_payee_name_full`. Replace with the same loop as Task 1 Step 4:

```python
    # Strip trailing bank-method suffix (e.g. `(Faster Payments)`) — loop because
    # the existing trailing-`Ref:` strip above might have exposed a fresh match.
    result = cleaned if cleaned else text
    while True:
        new_result = _BANK_METHOD_SUFFIX_RE.sub('', result).strip()
        if new_result == result:
            break
        result = new_result

    return result
```

If the Opera 3 function structure differs slightly (different variable names, different prefix-strip pattern), preserve the existing logic and add the new strip at the end. The principle is the same — the new regex is the LAST stripper to fire.

- [ ] **Step 4: Add a smoke test for the Opera 3 mirror**

Append to `tests/test_bank_method_suffix.py`:

```python
from sql_rag.bank_import_opera3 import extract_payee_name_full as extract_payee_name_full_o3


def test_opera3_mirror_strips_bank_method_suffix():
    """Opera 3 mirror must produce the same result as SE for the same input."""
    assert extract_payee_name_full_o3("Diskel (Faster Payments)") == "Diskel"
    assert extract_payee_name_full_o3(
        "P Flannery Plant Hire(oval) Limited (Faster Pay...)"
    ) == "P Flannery Plant Hire(oval) Limited"
    assert extract_payee_name_full_o3("Acme (Bristol) Ltd") == "Acme (Bristol) Ltd"
```

- [ ] **Step 5: Run tests**

```bash
source venv/bin/activate && pytest tests/test_bank_method_suffix.py -v
```

Expected: every test (SE + O3) passes.

```bash
source venv/bin/activate && pytest tests/ -q
```

Expected: full suite passes.

- [ ] **Step 6: Commit**

```bash
git add sql_rag/bank_import_opera3.py tests/test_bank_method_suffix.py
git commit -m "feat(bank-import-o3): mirror bank-method suffix strip"
```

---

## Task 3: Frontend default — refund branches in `getSmartDefaultTransactionType`

**Files:**
- Modify: `frontend/src/pages/Imports.tsx`

The frontend already does substring-based fuzzy matching against the loaded `customers` and `suppliers` lists in `getSmartDefaultTransactionType` (around line 1014). Today it handles two of the four sign-direction combinations:

- positive amount + customer match → `sales_receipt`
- negative amount + supplier match → `purchase_payment`

Add the missing two:

- negative amount + customer match → `sales_refund`
- positive amount + supplier match → `purchase_refund`

- [ ] **Step 1: Inspect the existing function**

Read `frontend/src/pages/Imports.tsx` lines 1014–1047. Confirm the current pattern: two `if` blocks gating on `isPositive` and the corresponding list (customers / suppliers). The function returns at the first match; the final fallback is the sign-based `nominal_*`.

- [ ] **Step 2: Add the two refund branches**

Replace the body of `getSmartDefaultTransactionType` between the existing supplier-match block and the final return. The current code is:

```tsx
    if (!isPositive && suppliers.length > 0) {
      for (const supp of suppliers) {
        const suppName = (supp.name || '').toLowerCase();
        if (suppName.length >= 3 && combined.includes(suppName)) {
          return 'purchase_payment';
        }
      }
    }

    // Default to nominal - if no customer/supplier name found, it's likely a bank charge,
    // fee, interest, or other nominal entry
    return isPositive ? 'nominal_receipt' : 'nominal_payment';
```

Replace with:

```tsx
    if (!isPositive && suppliers.length > 0) {
      for (const supp of suppliers) {
        const suppName = (supp.name || '').toLowerCase();
        if (suppName.length >= 3 && combined.includes(suppName)) {
          return 'purchase_payment';
        }
      }
    }

    // Refund cases — opposite sign-direction matches.
    // Negative amount + customer match → we are refunding the customer
    // (overpayment, mistaken collection, or credit-note repayment).
    if (!isPositive && customers.length > 0) {
      for (const cust of customers) {
        const custName = (cust.name || '').toLowerCase();
        if (custName.length >= 3 && combined.includes(custName)) {
          return 'sales_refund';
        }
      }
    }

    // Positive amount + supplier match → supplier is refunding us
    // (overpayment recovered, returned goods, etc.).
    if (isPositive && suppliers.length > 0) {
      for (const supp of suppliers) {
        const suppName = (supp.name || '').toLowerCase();
        if (suppName.length >= 3 && combined.includes(suppName)) {
          return 'purchase_refund';
        }
      }
    }

    // Default to nominal - if no customer/supplier name found, it's likely a bank charge,
    // fee, interest, or other nominal entry
    return isPositive ? 'nominal_receipt' : 'nominal_payment';
```

- [ ] **Step 3: TypeScript compile check**

```bash
cd /Users/maccb/llmragsql/frontend && npx tsc --noEmit
```

Expected: zero errors.

- [ ] **Step 4: Manual visual verification**

The Vite dev server is already running at http://localhost:5173/. With HMR your changes should be live after save.

In the browser:
1. Log in to the Cloudsis installation (or whatever installation is currently active).
2. Open the Bank Statement Hub and re-scan the 30 Apr Cloudsis statement (or any statement that includes a customer-refund row like Diskel or P Flannery).
3. Confirm in the *Unmatched Transactions* table:
   - Rows where the description contains a customer name AND the amount is negative now default Type to **Sales Refund** (not Nominal Payment).
   - Rows where the description contains a supplier name AND the amount is positive now default Type to **Purchase Refund**.
   - Rows with no customer/supplier name match (e.g. HISCOX UNDERWRITIN, bank charges) continue to default to Nominal Payment / Nominal Receipt as before.
4. Pick one refund row, tick Incl, and click Import to Opera. Confirm it posts as a sales refund / purchase refund (check the resulting atran/stran or atran/ptran rows in Opera).

- [ ] **Step 5: Commit**

```bash
git add frontend/src/pages/Imports.tsx
git commit -m "feat(bank-import-ui): default Type to Sales/Purchase Refund on opposite-sign customer/supplier match"
```

---

## Task 4: Knowledge base update

**Files:**
- Modify: `apps/core/docs/opera_knowledge_base.md`

- [ ] **Step 1: Find the Bank Statement Import section**

Read the file and find the existing "Bank Statement Import" or "Bank Statement Reconciliation" section. If a sub-section about payee-name extraction or the matching pipeline exists, add the new content under it. Otherwise add at the end of the bank-import section.

- [ ] **Step 2: Insert the following content**

```markdown
### Bank-Method Suffix Stripping (Payee Name Extraction)

`extract_payee_name_full()` in `sql_rag/bank_import.py` (and the Opera 3 mirror) strips a trailing parenthesised bank-method suffix before fuzzy-matching against `sname` / `pname`. Recognised suffixes (case-insensitive, optional `...` or `…` for truncation):

- `(Faster Payments)` / `(Faster Payment)` / `(Faster Pay...)`
- `(Direct Debit)`
- `(Standing Order)`
- `(BACS)`
- `(CHAPS)`
- `(Card Payment)`
- `(Cheque)`
- `(Cash)`
- `(Online Payment)`
- `(Transfer)`

The strip is anchored to end-of-string (`$`), so embedded parens that are part of a legal company name — `Acme (Bristol) Ltd`, `P Flannery Plant Hire(oval) Limited` — are preserved when there is no trailing bank-method suffix appended.

### Refund Suggestion for Unmatched Bank Lines

When a row lands in the *Unmatched Transactions* table, the frontend's `getSmartDefaultTransactionType` (in `Imports.tsx`) checks the four sign-and-list combinations:

| Amount | Match | Default Type |
|---|---|---|
| positive | customer in `sname` | `sales_receipt` |
| negative | supplier in `pname` | `purchase_payment` |
| **negative** | **customer in `sname`** | **`sales_refund`** (new) |
| **positive** | **supplier in `pname`** | **`purchase_refund`** (new) |
| — | no match | `nominal_receipt` / `nominal_payment` |

This is a *suggestion* — the user reviews and ticks Include before importing. The credit-note check (`_check_customer_refund` requiring an unallocated `stran` row) lives in the auto-classification path and is unchanged. Suggestions cover real-world cases like customer overpayments and mistaken GoCardless collections where no `stran` credit-note row exists yet.

Files: `sql_rag/bank_import.py`, `sql_rag/bank_import_opera3.py`, `frontend/src/pages/Imports.tsx`.
```

- [ ] **Step 3: Commit**

```bash
git add apps/core/docs/opera_knowledge_base.md
git commit -m "docs(kb): document bank-method suffix strip and refund-type suggestion"
```

---

## Self-Review

Cross-checked the spec against the plan tasks:

| Spec section | Covered by |
|---|---|
| Goal: smarter default Type for unmatched rows | Tasks 1, 2, 3 |
| Strip bank-method suffix in `extract_payee_name_full()` | Tasks 1 (SE), 2 (O3) |
| End-of-string anchor preserves embedded parens | Tasks 1 + 2 (regex with `$` anchor) |
| Frontend defaults `sales_refund` / `purchase_refund` for opposite-sign matches | Task 3 |
| Existing auto-classification path unchanged | Task 3 (only the dropdown default changes; posting and credit-note check are not in scope) |
| Genuinely unmatched rows still default to nominal | Task 3 (final fallback unchanged) |
| Both Opera SE and Opera 3 paths | Tasks 1, 2 |
| Knowledge base update | Task 4 |

No placeholders. Type names consistent across tasks (`_BANK_METHOD_SUFFIX_RE`, `extract_payee_name_full`, `getSmartDefaultTransactionType`, `sales_refund`, `purchase_refund`).

---

**Plan complete.**
