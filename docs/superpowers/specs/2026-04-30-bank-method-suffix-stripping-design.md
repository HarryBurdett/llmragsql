# Smarter Refund-Type Suggestion for Unmatched Bank Lines — Design Spec

**Date**: 2026-04-30
**Status**: Approved (defaults accepted, ready for implementation plan)

## Problem

When a bank statement description ends with a bank-method suffix in parentheses — for example `Diskel (Faster Payments)` or `P Flannery Plant Hire(oval) Limited (Faster Pay...)` — the existing fuzzy matcher in `sql_rag/bank_import.py` does not find the customer in `sname` because the parenthesised suffix is included in the name being matched. The line falls through to the default `nominal_payment` type and shows up in the *Unmatched Transactions* table for the user to manually correct: change the Type dropdown from "Nominal Payment" → "Sales Refund", then enter the customer in the Assign Account dropdown.

Real-world example (30 Apr 2026, Cloudsis):

| Bank description | Amount | Should suggest | Currently suggests |
|---|---|---|---|
| `Diskel (Faster Payments)` | −£415.20 | Sales Refund (customer in sname) | Nominal Payment |
| `P Flannery Plant Hire(oval) Limited (Faster Pay...)` | −£198.00 | Sales Refund (customer in sname) | Nominal Payment |

Customer refunds are not always backed by a credit note in `stran`. Per accounting reality at this site:

- **Common case** — customer overpaid (paid same invoice twice, or mistaken GoCardless collection). The overpayment may not have an unallocated `R` row in `stran` to anchor the refund against; the user knows it's a refund from email correspondence.
- **Less common case** — customer has a sales credit note in `stran` (`st_trtype='C'`, `st_trbal<0`) and the bank line is repaying that.

In both cases the user knows the line is a refund. The system should default the Type accordingly so the user clicks once (Incl) instead of changing two dropdowns per row.

## Goal

When a row lands in the *Unmatched Transactions* bucket, suggest a smarter default `Type` based on a fuzzy match against `sname` / `pname` after stripping bank-method suffixes. The row stays in the Unmatched bucket — the user reviews and ticks Include to import.

## Out of Scope

- Auto-routing matched rows to a different bucket. The user explicitly wants them to stay in *Unmatched* for review.
- Auto-importing refunds. The user reviews and confirms each row.
- Loosening or strengthening the credit-note rule for the existing auto-classification path. That path is unchanged; it sits ahead of the new suggestion logic.
- UI redesign. Only the default value of the Type dropdown changes; everything else looks the same.
- Pattern-learning / `bank_aliases.db` changes. Alias auto-learning on successful fuzzy match (≥ 0.85) is already in place and continues to work.

## Approach

Two coordinated changes inside `sql_rag/bank_import.py` (and the Opera 3 mirror):

### 1. Strip bank-method suffixes during name extraction

`extract_payee_name_full()` already strips a list of prefixes (`Card Payment to`, `Giro Direct Credit From`, etc.) and a list of trailing suffixes (`Ref: ...`, `On DD Mon ...`, trailing `*`). Add a new trailing-anchored regex that strips parenthesised bank-method suffixes:

```python
_BANK_METHOD_SUFFIX_RE = re.compile(
    r'\s*\(\s*(faster\s*payments?|direct\s*debit|standing\s*order|'
    r'bacs|chaps|card\s*payment|cheque|cash|online\s*payment|'
    r'transfer)\s*(?:\.\.\.|…)?\s*\)\s*$',
    re.IGNORECASE,
)
```

The non-capturing `(?:\.\.\.|…)?` allows for truncated forms like `(Faster Pay...)` from extractors that cap descriptions at fixed widths. The `$` anchor is critical — it preserves embedded parens that are part of the legal name (e.g. `Acme (Bristol) Ltd`, `P Flannery Plant Hire(oval) Limited`).

Run the new regex strip in `extract_payee_name_full()` after the existing prefix and `Ref:`/date/`*` suffix strippers, looping until no further match (cheap — single string operation).

### 2. Use the cleaned name to populate `suggested_type` and `suggested_account` on unmatched rows

Today, an unmatched row gets a default Type based on amount sign — `nominal_payment` for negative, `nominal_receipt` for positive. Add a helper that runs *after* the existing matched-bucket pipeline has decided the row is unmatched:

```python
def suggest_type_for_unmatched(txn: BankTransaction, customer_match, supplier_match) -> tuple[str | None, str | None]:
    """
    Return (suggested_type, suggested_account_code) for an unmatched row,
    or (None, None) if no useful suggestion is available.
    """
    if txn.amount < 0 and customer_match and customer_match.score >= MIN_SUGGEST_SCORE:
        return ('sales_refund', customer_match.account)
    if txn.amount > 0 and supplier_match and supplier_match.score >= MIN_SUGGEST_SCORE:
        return ('purchase_refund', supplier_match.account)
    return (None, None)
```

`MIN_SUGGEST_SCORE` should be slightly lower than the auto-import threshold (currently 0.85) — say **0.70** — because the suggestion is reviewed by the user, so a few false positives are acceptable but missed-true-positives waste user time. Tunable; revisit after first usage.

The match used by the suggester is the same fuzzy match the auto-classification already runs — just with the cleaned name from change (1). If the auto-classification didn't finalise (no credit note found, or some other gating), we still get the match object for use by the suggester.

The suggester output populates new fields on the unmatched row dict returned by the preview / scan endpoint:

```python
{
    "row": 7,
    "date": "2026-04-23",
    "name": "Diskel (Faster Payments)",
    "amount": -415.20,
    "suggested_type": "sales_refund",
    "suggested_account": "DSK001",
    "suggested_account_name": "Diskel Ltd",
    ...existing fields unchanged...
}
```

### Why credit-note check is bypassed for the suggestion

The existing rule "if no credit note → skip" lives in the *auto-classification* layer, where the system is making a binding decision. That rule stays.

The new suggestion layer is *informational only* — the user reviews the suggestion, ticks Include, and the import path then does the actual posting. The user is the source of truth for "is this a refund?". A wrong suggestion is one dropdown change to fix; a missed suggestion is two.

This matches the project's "minimise user entry" goal without compromising posting-time safety.

## Files Touched

| File | Change |
|---|---|
| `sql_rag/bank_import.py` | Add `_BANK_METHOD_SUFFIX_RE` and a strip call inside `extract_payee_name_full()`. Add `suggest_type_for_unmatched()`. Wire the suggester output onto every row that ends up in the unmatched-transactions response. |
| `sql_rag/bank_import_opera3.py` | Mirror — same regex, same suggester, same wiring. |
| `apps/bank_reconcile/api/routes.py` | Each unmatched-row response object gets `suggested_type`, `suggested_account`, `suggested_account_name` fields populated from the suggester. Apply to both `/api/bank-import/preview-from-pdf` (and email/CSV equivalents) and the Opera 3 mirror endpoint. |
| `frontend/src/pages/Imports.tsx` | The Type dropdown's default for an unmatched row reads `suggested_type` if present, falling back to the current sign-based default. Same for the Assign Account field — pre-fill from `suggested_account`/`suggested_account_name` when available. |
| `tests/test_bank_method_suffix.py` | NEW. Unit tests for `extract_payee_name_full()` + `suggest_type_for_unmatched()`. |
| `apps/core/docs/opera_knowledge_base.md` | Short paragraph documenting the new recognised suffixes and the suggestion logic. |

## Testing

Unit tests in `tests/test_bank_method_suffix.py`:

### `extract_payee_name_full()`

| Input | Expected output |
|---|---|
| `Diskel (Faster Payments)` | `Diskel` |
| `Diskel (Faster Pay...)` | `Diskel` |
| `Diskel (Faster Pay…)` | `Diskel` |
| `P Flannery Plant Hire(oval) Limited (Faster Pay...)` | `P Flannery Plant Hire(oval) Limited` |
| `Customer (Direct Debit)` | `Customer` |
| `Customer (Standing Order)` | `Customer` |
| `Customer (BACS)` | `Customer` |
| `Customer (CHAPS)` | `Customer` |
| `Customer (Card Payment)` | `Customer` |
| `Acme (Bristol) Ltd` | `Acme (Bristol) Ltd` (unchanged) |
| `P Flannery Plant Hire(oval) Limited` | `P Flannery Plant Hire(oval) Limited` (unchanged — no trailing bank suffix) |
| `Ref: ABC (Faster Payments)` | `Ref: ABC` (existing Ref-strip then bank-method strip — both applied; final result depends on order, see Integration) |
| `` (empty) | `` |

### `suggest_type_for_unmatched()`

| Setup | Expected |
|---|---|
| Negative amount, customer match score 0.85 | `('sales_refund', customer.account)` |
| Negative amount, customer match score 0.65 (below threshold) | `(None, None)` |
| Negative amount, no customer match | `(None, None)` |
| Positive amount, supplier match score 0.85 | `('purchase_refund', supplier.account)` |
| Positive amount, customer match (no supplier) | `(None, None)` — wrong direction |
| Positive amount, both match | `('purchase_refund', supplier.account)` — supplier wins on positive |

### Integration

End-to-end test that mocks `sname` containing "Diskel Ltd" and "P Flannery Plant Hire (oval) Limited", then runs a preview against a bank line `Diskel (Faster Payments)` for −£415.20. Asserts the response row has `suggested_type='sales_refund'` and `suggested_account` set to Diskel's account code.

### Frontend

Manual visual verification: re-run the Cloudsis 30-Apr-2026 statement scan. Confirm the Type dropdown for the Diskel and P Flannery rows now defaults to "Sales Refund" with the correct customer pre-filled in Assign Account. The HISCOX UNDERWRITIN row continues to default to "Nominal Payment" (no customer match).

## Success Criteria

1. Diskel and P Flannery rows from the 30 Apr Cloudsis statement now default to Type = Sales Refund with the matched customer pre-filled.
2. Genuinely unmatched lines (e.g. HISCOX UNDERWRITIN with no fuzzy hit in `sname` or `pname`) continue to default to `Nominal Payment` / `Nominal Receipt`.
3. Names with embedded parens (`Acme (Bristol) Ltd`, `P Flannery Plant Hire(oval) Limited`) are unchanged when there's no bank-method suffix appended.
4. The existing auto-classification (matched receipts / payments / refunds with credit-note check) is unchanged. No row that previously routed correctly now routes incorrectly.
5. Behaviour identical for Opera SE and Opera 3.
6. The user's posting-time pipeline is unchanged — the existing import-with-overrides endpoint creates the same Opera-side state it does today; the suggestion just pre-fills the form.
7. `bank_aliases.db` continues to learn from successful fuzzy matches at score ≥ 0.85 — no change.
