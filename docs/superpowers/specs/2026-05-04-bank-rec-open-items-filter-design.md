# Bank-Rec Open-Items Filter — Design Spec

**Date**: 2026-05-04
**Status**: Approved (brainstorming complete, ready for implementation plan)

## Problem

When scanning a bank statement against Opera, the matcher decides for each statement line: "is this already in Opera (don't re-post) or new (post it)?" The matcher's candidate pool is built from `atran`/`aentry` rows on the bank, then filtered by amount, date, sign, and (in some paths) action-derived `at_type`.

Three real-world cases on Cloudsis BB005 (2026-05-04 April 1-28 statement) exposed two distinct bugs in the candidate-pool definition:

1. **Reconciled entries leak in.** Some matcher paths don't filter on `ae_reclnum`. Entries reconciled in past batches re-appear as candidates and falsely match unrelated current statement lines.
2. **Correction-pair-matched entries leak in.** When the operator uses Opera's matching facility to link a mistaken posting with its reversing entry (e.g. R100000407 +£198 with P100000755 −£198), Opera marks both `aentry.ae_remove = True`. Those entries are settled — they should NOT appear in the reconciliation candidate pool. The current code ignores the flag, so the statement line gets falsely flagged as "in Opera" when it isn't.

Both bugs are **read-side only** — they're about which Opera rows the matcher considers, not about modifying Opera data. The data is correct; the code's filter is incomplete.

The Opera convention, per direct guidance from the operator:
- **Once an entry is reconciled it is deemed correct and final**: `ae_reclnum > 0` entries belong to past statements and must never re-match.
- **Once a correction pair is matched in Opera, both entries are settled**: `ae_remove = True` entries do not appear in the bank reconciliation process.

## Goal

Define ONE rule for "candidate aentry for bank-rec matching", apply it consistently at every fetch site, and pin the rule with tests. No data changes. No workflow changes. Read-side only.

## The Rule

An `aentry` row is a candidate for matching against a new bank statement iff:

```
ae_reclnum = 0 AND ae_remove = 0
```

(Equivalent SQL fragment lives in one place; `ae_remove` is a `bit` field — `0` means False / not removed.)

`atran` rows are filtered transitively via the JOIN to `aentry`. Per operator guidance, `ae_remove` is the canonical flag (the matching state lives at the entry-header level); `at_remove` exists but the candidate-pool filter is on aentry.

## Out of Scope

- Modifying Opera data (deleting ghost postings, etc.) — operator handles via Opera UI
- Capturing Opera's matching-facility operation in the transaction-snapshot library — worth doing later but not blocking this fix
- Workflow changes (e.g. surfacing matched-out entries as a separate UI category) — current behaviour is "they don't show up", which is correct
- Per-bank or per-company carve-outs — the rule is universal

## Architecture

**Single source of truth**: a new module `sql_rag/opera_open_items.py` (~20 lines) exporting:

- `OPEN_FOR_REC_SQL: str = "ae_reclnum = 0 AND ae_remove = 0"` — SQL fragment to append to WHERE clauses that select reconciliation candidates.
- `is_open_for_rec(aentry_row: dict) -> bool` — Python equivalent for in-memory filters.

Every consumer imports from this module rather than re-stating the rule. Tests pin the contract.

## Files Touched

| File | Change |
|---|---|
| `sql_rag/opera_open_items.py` | **CREATE** — new module with the SQL fragment + Python helper |
| `tests/test_opera_open_items.py` | **CREATE** — unit tests on `is_open_for_rec` (truth table) |
| `tests/test_bank_rec_candidate_filter.py` | **CREATE** — contract test: every candidate-fetcher's source contains `ae_reclnum = 0` and `ae_remove = 0` |
| `sql_rag/bank_import.py` | MODIFY `_is_already_posted_typeblind` — JOIN aentry, apply filter, drop the unused `at_remove` reference my earlier fix added |
| `sql_rag/duplicate_check_se.py` | MODIFY `OperaSEDataSource` — apply the filter to its candidate query |
| `sql_rag/opera_sql_import.py::match_statement_to_cashbook` | MODIFY — extend existing `WHERE ae_reclnum = 0` to also include `AND ae_remove = 0` |
| `sql_rag/bank_import_opera3.py`, `sql_rag/duplicate_check_o3.py`, `sql_rag/opera3_foxpro_import.py` | MODIFY Opera 3 mirrors of the same three changes (DBF row predicate version of the rule) |
| `apps/core/docs/opera_knowledge_base.md` | MODIFY — add "Bank Rec Open-Items Rule" section |
| `~/opera-knowledge-ref/packages/opera-knowledge/business-rules/bank-rec-open-items.md` | **CREATE** — central KB doc |
| `marketing/manuals/manual-bank-reconciliation.md` | MODIFY — note that correction-pair-matched entries are correctly excluded from scans |

## Data Flow

1. Statement scan → AI extracts → preview/process endpoint
2. Per statement line, matcher fetches Opera candidates via one of the three call sites
3. Every fetch query applies `OPEN_FOR_REC_SQL` (or its DBF equivalent on Opera 3)
4. Type-aware (`check_for_duplicate`) and type-blind (`_is_already_posted_typeblind`) paths run on the same filtered pool
5. Match → flag "in Opera". No match → flag "needs posting".

## Error Handling

- **Orphan atran (no aentry row)**: exclude from candidate pool, log WARNING (data-integrity signal worth seeing — shouldn't happen in normal Opera state).
- **`ae_remove` IS NULL**: treat as False (consistent with how Opera reads NULL bit fields). The SQL filter `ae_remove = 0` matches NULL via `ISNULL(ae_remove, 0) = 0` if needed; the in-memory helper coerces None→False.
- **SQL errors**: caught, logged at WARNING, return empty candidate set. Fails safe — statement line shows as "needs posting" rather than wrongly flagged "in Opera".
- **Public API unchanged**: function signatures, return types, response shapes are all the same; only the candidate pool narrows.

## Testing

| Test | Pins |
|---|---|
| `test_is_open_for_rec_truth_table` | All 6 combinations of (`ae_reclnum=0`, `>0`) × (`ae_remove=False`, `True`, `None`). Asserts only (`reclnum=0`, `remove=False`) and (`reclnum=0`, `remove=None`) return True. |
| `test_open_for_rec_sql_fragment` | The SQL string contains exactly `ae_reclnum = 0 AND ae_remove = 0`. |
| `test_typeblind_fallback_uses_open_filter` | Source of `_is_already_posted_typeblind` contains the filter from `OPEN_FOR_REC_SQL`. |
| `test_se_data_source_uses_open_filter` | Source of `OperaSEDataSource` candidate query contains the filter. |
| `test_match_statement_to_cashbook_uses_open_filter` | Source of `match_statement_to_cashbook` contains the filter. |
| `test_opera3_mirrors_use_open_filter` | DBF-side equivalents in opera3_foxpro_import.py and bank_import_opera3.py respect the same rule. |
| `test_flannery_198_regression` | Build statement-line + candidate row resembling the £198 case. With `ae_remove=True` → "no match". With `ae_remove=False` → "match". With `ae_reclnum=5` → "no match". Pins the user-reported scenario. |
| `test_se_o3_parity` | Same input rows produce same matching decision on both data sources. |

## Verification

After implementation:

1. Cloudsis BB005 (Opera3SECompany00C), April 1-28 statement, 8 transactions:
   - 7 with `ae_remove=False, ae_reclnum=0` → flagged "in Opera"
   - 1 (Flannery £198) with `ae_remove=True` → flagged "needs posting"
2. Re-scan must yield: `total=8, in_opera=7, unmatched=1`.

## KB / Manual

**Local KB** (`apps/core/docs/opera_knowledge_base.md`): add a section on "Bank Rec Open-Items Rule":

> An `aentry` row is a candidate for matching against a new bank statement iff `ae_reclnum = 0 AND ae_remove = 0`. Reconciled entries (`ae_reclnum > 0`) belong to past statements and never re-match. Correction-pair-matched entries (`ae_remove = True`) are settled via Opera's matching facility and don't appear in bank reconciliation. Both filters MUST be applied at every candidate-fetch site. The canonical filter lives in `sql_rag/opera_open_items.py`.

**Central KB** (`~/opera-knowledge-ref/packages/opera-knowledge/business-rules/bank-rec-open-items.md`): cross-references `period-reconciliation.md` and `bank-rec-completion.md`.

**Manual** (`marketing/manuals/manual-bank-reconciliation.md`): add a brief note that correction-pair-matched entries (matched in Opera) are correctly excluded from bank-rec scans.

## Success Criteria

1. The £198 Flannery scenario re-scans clean: 7 in-Opera + 1 unmatched.
2. The contract tests fail loudly if any new candidate-fetcher omits the filter.
3. No Opera data is modified by this implementation.
4. Function signatures, response shapes unchanged — fully backwards-compatible.
5. SE and Opera 3 produce identical matching decisions for the same logical input.
