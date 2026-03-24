# Auto Bank Reconciliation - Planning Document

**Created:** 2026-02-09
**Updated:** 2026-02-11
**Status:** Approved - Ready for Implementation
**Prerequisite:** Bank Statement Import (implemented)

---

## Goal

Automatically reconcile Opera cashbook entries against bank statement after import, completing the full automation cycle:

```
Bank Statement → Import to Opera → Auto-Allocate → Auto-Reconcile → Done
```

The complete workflow handles everything from raw statement to fully reconciled books.

---

## Key Principles (Agreed)

1. **Statements are sequential** - must reconcile statement N before N+1
2. **Balance validation is critical** - opening balance must match Opera's expected
3. **Same underlying data** - our tool and Opera work on same tables
4. **Recommend our tool** - for automation benefits, Opera as fallback
5. **Efficiency focused** - minimise user thinking and potential for mistakes

---

## Statement Validation Rules

### Opening Balance Check
```
Statement Opening Balance MUST = Opera's Expected Opening Balance (nk_recbal)
```
**If mismatch: BLOCK completely** - user must investigate before proceeding.

### Closing Balance Check
```
Opening Balance + Sum of Statement Entries = Closing Balance
```
**Must balance before completion** - ensures integrity.

### Sequential Processing
- Cannot skip statements
- Previous statement must be fully completed before starting next
- Multiple pending statements? Only show the NEXT valid one (by opening balance match)

---

## Matching Logic

### Tier 1: Auto-Reconcile (Confident)
**Entries imported from the statement**
- Exact reference match (ae_entref = statement reference)
- Exact amount match
- **Action:** Auto-reconcile without user intervention

### Tier 2: Suggested Reconcile (Probable)
**Existing entries entered before statement arrived**
- Approximate date match (within 3 days)
- Exact amount match
- Same customer/supplier account
- **Action:** Highlight in different colour, user reviews and accepts/corrects

### Tier 3: Unmatched
**No match found**
- Statement line with no matching cashbook entry
- **Action:** Flag for user, offer to create missing entry

---

## Statement Line Numbering

Opera assigns statement line numbers (ae_statln) to maintain order:
- Standard increment: 10, 20, 30, 40...
- Gaps (1-9, 11-19, etc.) allow inserting unmatched items later

### Gap Logic
When auto-reconciling, must leave room for unmatched lines:
- Count unmatched lines BEFORE each matched line
- If ≤9 unmatched before: use position × 10
- If >9 unmatched before: increase gap (add extra 10 per 10 unmatched)

**Example:**
```
Statement has 15 lines, lines 1-12 unmatched, lines 13-15 matched
Line 13 → ae_statln = 130 (leaves room for 12 items: 10,20,30...120)
Line 14 → ae_statln = 140
Line 15 → ae_statln = 150
```

---

## Statement Number Handling

1. **Try to extract from statement** (if present in file/email)
2. **If not found:** use Opera's next sequential number (nk_lststno + 1)
3. **After completion:** update Opera's next statement number
   - If statement had number: next = that number + 1
   - If no number: next = current + 1

---

## Reconciliation State

### Partial Progress Allowed
- User can start reconciling, investigate items, come back later
- Progress is saved
- **Cannot complete until 100% matched**

### State Storage
- **Opera tables:** Core state (ae_reclnum, ae_statln, nk_recbal, etc.)
- **Our system:** Supplementary info (statement file reference, suggested matches, progress)

Opera remains authoritative source of truth.

---

## Opera Tables - Complete Field Reference

### AENTRY (Cashbook Entries)

| Field | Type | Purpose |
|-------|------|---------|
| `ae_reclnum` | Integer | Reconciliation batch number (0 = unreconciled) |
| `ae_recdate` | Date | Date entry was reconciled |
| `ae_statln` | Integer | Statement line number (10, 20, 30...) |
| `ae_frstat` | Integer | From statement number |
| `ae_tostat` | Integer | To statement number |
| `ae_recbal` | Integer | Running reconciled balance AFTER this entry (PENCE) |
| `ae_tmpstat` | Integer | Temporary/pending flag |
| `ae_complet` | Integer | Entry complete (1 = posted to NL) |

### NBANK (Bank Master)

| Field | Type | Purpose |
|-------|------|---------|
| `nk_recbal` | Integer | Total reconciled balance (PENCE) = expected opening for next statement |
| `nk_curbal` | Integer | Current balance (PENCE) = total of all entries |
| `nk_lstrecl` | Integer | Last reconciliation batch number |
| `nk_lststno` | Integer | Last statement number |
| `nk_lststdt` | Date | Last statement date |
| `nk_reclnum` | Integer | Reconciliation number (mirrors nk_lstrecl) |
| `nk_recldte` | Date | Reconciliation completion date |
| `nk_recstfr` | Integer | From statement number |
| `nk_recstto` | Integer | To statement number |
| `nk_recstdt` | Date | Statement date |

**Note:** All balance fields in PENCE - divide by 100 for pounds.

---

## User Interface Flow

### After Statement Import

```
┌─────────────────────────────────────────────────────────────┐
│ Import Complete                                             │
│                                                             │
│ 15 transactions imported successfully                       │
│ 3 already existed in Opera                                  │
│ 12 allocations made                                         │
│                                                             │
│ Ready to reconcile this statement?                          │
│                                                             │
│              [Reconcile Now]    [Later]                     │
└─────────────────────────────────────────────────────────────┘
```

### Reconciliation Screen

```
┌─────────────────────────────────────────────────────────────┐
│ Bank Reconciliation - BC010 Current Account                 │
├─────────────────────────────────────────────────────────────┤
│ Statement: 260209    Date: 09-Feb-2026                      │
│ Opening Balance: £12,345.67 ✓ (matches Opera)               │
│ Closing Balance: £14,892.34                                 │
├─────────────────────────────────────────────────────────────┤
│ Matching Summary:                                           │
│   ● 15 Auto-matched (green)                                 │
│   ● 3 Suggested matches (amber) - review required           │
│   ● 2 Unmatched statement lines (red)                       │
├─────────────────────────────────────────────────────────────┤
│ Statement Lines:                                            │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ ✓ 10  09-Feb  £500.00   BACS-12345   → R200001234       │ │
│ │ ✓ 20  09-Feb  £250.00   BACS-12346   → R200001235       │ │
│ │ ? 30  08-Feb  £175.00   DD-RATES     → P100008036 ?     │ │
│ │ ✗ 40  07-Feb  £12.50    BANK FEE     → [Create Entry]   │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Difference: £12.50 (unmatched bank fee)                     │
│                                                             │
│ [Save Progress]              [Complete Reconciliation]      │
│                              (disabled until difference = 0)│
└─────────────────────────────────────────────────────────────┘
```

**Colour Coding:**
- **Green:** Auto-matched (imported entries)
- **Amber:** Suggested match (existing entries) - user to verify
- **Red:** Unmatched - user to resolve

---

## API Design

### Validate Statement for Reconciliation

```python
@app.post("/api/bank-reconciliation/validate-statement")
async def validate_statement_for_reconciliation(
    bank_code: str,
    opening_balance: float,  # From statement
    closing_balance: float,  # From statement
    statement_number: Optional[int] = None,  # If on statement
    statement_date: str
) -> Dict:
    """
    Validate statement is next in sequence and opening balance matches.

    Returns:
        - valid: bool
        - expected_opening: float (from Opera)
        - opening_matches: bool
        - next_statement_number: int
        - error_message: str (if invalid)
    """
```

### Match Statement to Cashbook

```python
@app.post("/api/bank-reconciliation/match-statement")
async def match_statement_to_cashbook(
    bank_code: str,
    statement_transactions: List[StatementTransaction],
    statement_date: str
) -> Dict:
    """
    Match statement lines to cashbook entries.

    Returns:
        - auto_matched: List[{statement_line, entry_number, confidence: 100}]
        - suggested_matched: List[{statement_line, entry_number, confidence: 70-95}]
        - unmatched_statement: List[{statement_line, amount, reference}]
        - unmatched_cashbook: List[{entry_number, amount, reference}]
    """
```

### Complete Reconciliation

```python
@app.post("/api/bank-reconciliation/complete")
async def complete_reconciliation(
    bank_code: str,
    statement_number: int,
    statement_date: str,
    closing_balance: float,
    matched_entries: List[Dict]  # {entry_number, statement_line}
) -> Dict:
    """
    Mark all matched entries as reconciled.
    Only succeeds if closing balance validates.

    Updates:
        - aentry: ae_reclnum, ae_recdate, ae_statln, ae_frstat, ae_tostat, ae_recbal
        - nbank: nk_recbal, nk_lstrecl, nk_lststno, etc.
    """
```

### Create Missing Entry

```python
@app.post("/api/bank-reconciliation/create-entry")
async def create_entry_for_unmatched(
    bank_code: str,
    amount: float,
    date: str,
    reference: str,
    description: str,
    transaction_type: str  # 'bank_charge', 'interest', 'other'
) -> Dict:
    """
    Create cashbook entry for unmatched statement line (e.g., bank fees).
    Returns new entry_number for immediate matching.
    """
```

---

## Implementation Order

### Phase 1: Core Matching Engine
1. `match_statement_to_cashbook()` - matching logic
2. Statement line number calculation with gap logic
3. Validation functions (opening balance, closing balance)

### Phase 2: API Endpoints
1. `/api/bank-reconciliation/validate-statement`
2. `/api/bank-reconciliation/match-statement`
3. `/api/bank-reconciliation/complete`
4. `/api/bank-reconciliation/create-entry`

### Phase 3: Frontend
1. Post-import prompt (Reconcile Now / Later)
2. Reconciliation screen with colour-coded matches
3. Suggested match review UI
4. Create entry dialog for unmatched lines

### Phase 4: Integration
1. Connect to statement import flow
2. Track reconciliation state in our system
3. Handle multiple pending statements (show next only)

---

## Files to Modify

| File | Changes |
|------|---------|
| `sql_rag/opera_sql_import.py` | Add matching logic, statement validation |
| `api/main.py` | New reconciliation endpoints |
| `frontend/src/pages/Imports.tsx` | Post-import reconciliation prompt |
| `frontend/src/pages/BankStatementReconcile.tsx` | Enhanced with auto-match, colour coding |
| `api/email/storage.py` | Track reconciliation state/progress |

---

## Testing Scenarios

1. **Happy path:** Import statement, all entries match, reconcile in one click
2. **Suggested matches:** Some existing entries need user verification
3. **Unmatched lines:** Bank fees need entry creation
4. **Balance mismatch:** Opening balance doesn't match - blocked
5. **Partial progress:** Start reconciliation, save, come back later
6. **Multiple statements:** 3 months pending, only next one shown
7. **Opera interaction:** Start in our tool, view in Opera, both see same state

---

## Notes

- Builds on existing `mark_entries_reconciled()` function
- Statement import preserves bank references - key enabler for matching
- Auto-allocate happens BEFORE reconcile (receipts to invoices, then reconcile)
- Recommend our tool for reconciliation, but Opera works as fallback
- All amounts in Opera are PENCE - convert for display
