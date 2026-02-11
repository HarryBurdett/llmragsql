# Auto Bank Reconciliation - Planning Document

**Created:** 2026-02-09
**Status:** Planning
**Prerequisite:** Bank Statement Import (implemented)

---

## Goal

Automatically reconcile Opera cashbook entries against bank statement after import, completing the full automation cycle:

```
Bank Statement → Import to Opera → Auto-Reconcile → Done
```

Because bank statement import preserves exact bank references, the reconciliation should be seamless - entries created from the statement should match the statement lines exactly.

---

## Current Manual Process

1. User downloads bank statement
2. User manually enters transactions in Opera (or uses our import)
3. User opens Opera Bank Reconciliation
4. User enters statement date and closing balance
5. User manually ticks each cashbook entry that appears on statement
6. User posts when difference = 0

**Pain point:** Steps 4-6 are tedious when entries were just imported from the same statement.

---

## Proposed Automation

### Trigger Point

Auto-reconciliation triggers **immediately after successful statement import** as part of the same workflow - not a separate step. The full process is:

```
Import Statement → Create Opera Entries → Auto-Reconcile → Complete
```

This is seamless because the entries we just created have the exact same references as the statement lines we're reconciling against.

### Matching Logic

Match imported Opera entries to statement lines by:

| Match Level | Criteria | Confidence |
|-------------|----------|------------|
| Exact | Reference + Amount + Date | 100% |
| High | Reference + Amount (date within 3 days) | 95% |
| Medium | Amount + Date (no reference match) | 70% |
| Low | Amount only (different date) | 50% |

**Priority:** Start with exact matches only (safe), expand later.

### Opera Tables Updated

When marking entry as reconciled:
- `aentry.ae_stment` - Statement number
- `aentry.ae_stmtln` - Statement line number (10, 20, 30...)
- `nbank.nk_lastrec` - Last reconciled balance (in pence)
- `nbank.nk_stmtno` - Last statement number

### Existing Implementation

We already have `mark_entries_reconciled()` in `opera_sql_import.py` that handles the Opera updates. The auto-reconciliation would:
1. Match statement lines to cashbook entries
2. Call `mark_entries_reconciled()` for matched entries

---

## What We Need to Determine

### Questions

1. **Statement number handling**
   - Auto-generate from date? (e.g., YYMMDD format: 260209)
   - User provides?
   - Increment from last statement?

2. **Partial reconciliation**
   - If some entries don't match, reconcile what we can?
   - Or require 100% match before reconciling?

3. **Bank fees and adjustments**
   - Statement may include fees not in cashbook
   - How to handle? Manual entry first? Auto-create?

4. **Timing differences**
   - Payment sent but not yet on statement
   - Receipt on statement but not yet entered
   - How strict on date matching?

5. **Multi-statement handling**
   - Import covers multiple statement periods?
   - Reconcile to most recent only?

---

## Implementation Phases

### Phase 1: Basic Auto-Match (Post-Import)

After successful import, automatically:
1. Get all unreconciled entries for the bank account
2. Match to statement lines by reference + amount
3. Present summary: "X of Y entries matched"
4. User confirms → mark as reconciled

### Phase 2: Difference Handling

Handle common differences:
- Bank fees → prompt to create bank payment entry
- Interest → prompt to create bank receipt entry
- Unmatched → show for manual review

### Phase 3: Full Automation

Complete hands-off reconciliation:
- Auto-create fee/interest entries
- Auto-assign statement numbers
- Email notification of reconciliation status

---

## API Design

```python
@app.post("/api/bank-reconciliation/auto-match")
async def auto_match_for_reconciliation(
    bank_code: str,
    statement_date: str,
    statement_balance: float,
    transactions: List[StatementTransaction]  # From imported statement
):
    """
    Match statement transactions to unreconciled cashbook entries.
    Returns matched pairs and unmatched on both sides.
    """

@app.post("/api/bank-reconciliation/reconcile-matched")
async def reconcile_matched_entries(
    bank_code: str,
    statement_number: int,
    statement_date: str,
    matched_entries: List[Dict]  # entry_number + statement_line pairs
):
    """
    Mark matched entries as reconciled.
    Uses existing mark_entries_reconciled() function.
    """
```

---

## Integration with Statement Import

The ideal flow after import:

```
Import Complete
     │
     ▼
"Import successful: 15 transactions imported"
"Auto-reconcile available: 15 of 15 entries match statement"
     │
     ▼
[Skip] [Review Matches] [Reconcile Now]
     │
     ▼ (if Reconcile Now)
Enter statement number: [260209]
Statement closing balance: [£12,345.67]
     │
     ▼
"Reconciliation complete. Balance: £0.00 difference"
```

---

## Files to Modify

| File | Changes |
|------|---------|
| `sql_rag/opera_sql_import.py` | Add auto-match logic |
| `api/main.py` | New endpoints for auto-reconciliation |
| `frontend/src/pages/Imports.tsx` | Post-import reconciliation prompt |
| `frontend/src/pages/BankStatementReconcile.tsx` | Auto-match button |

---

## Next Steps

1. Discuss matching logic strictness with user
2. Determine statement number handling preference
3. Design the post-import UI prompt
4. Implement Phase 1 basic auto-match
5. Test with real data

---

## Notes

- Builds on existing `BankStatementReconcile.tsx` component
- Reuses `mark_entries_reconciled()` for Opera updates
- Statement import already preserves bank references - key enabler
- Consider: should auto-allocate (invoices) happen before or after reconcile?
