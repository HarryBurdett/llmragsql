# Stages 3-5 Audit Findings

**Date:** 2026-05-05
**Scope:** Stage 3 (Import), Stage 4 (Reconcile), Stage 5 (Complete).

## Summary

The core posting paths and the just-shipped self-heal are well-structured. The most significant gaps are around the Stage 5 reversal contract: the in-app `unreconcile` endpoint clears Stage A on aentry but only resets `nk_recbal` on nbank — every other Stage B field (`nk_lststno`, `nk_lststdt`, `nk_lstrecl`, `nk_lststno`, `nk_recstfr/to`, `nk_reccfwd`, `nk_recldte`, `nk_reclnum`) is left stale, and Opera 3 unreconcile is even thinner than SE. This puts the heal three-fact rule at risk of false positives after a reversal. The Opera 3 `complete_reconciliation` route also lacks the partial→full smart-promotion logic that SE has, breaking parity. Several smaller correctness/UX issues exist in the auto-reconcile path's statement-number derivation, the Opera 3 stran column-name inconsistency (`st_cusref` on writes vs `st_custref` on reads), and the Stage 4 selected-count display ignoring toggle overrides.

## Findings

### CRITICAL

#### F1: Stage 5 reversal endpoint does not reset Stage B fields on nbank
**Where:** `apps/bank_reconcile/api/routes.py:946-984` (SE `unreconcile_entries`); mirror gap at `apps/bank_reconcile/api/routes.py:15059-15069` (Opera 3).
**Symptom:** After running the in-app unreconcile, a reversed bank still has `nk_lststno`, `nk_lststdt`, `nk_lstrecl`, `nk_reclnum`, `nk_recldte`, `nk_recstfr`, `nk_recstto`, `nk_recstdt`, `nk_recstln`, and `nk_reccfwd` pointing at the (now-reverted) statement.
**Cause:** The endpoint only updates `nk_recbal` (recalculated as `SUM(ae_value) WHERE ae_reclnum>0`). On Opera 3 it only sets `nk_recbal`, and crucially it does NOT clear `ae_recdate` or `ae_recbal` on aentry either (SE does clear `ae_recdate` but not `ae_recbal`).
**Impact:** Two concrete consequences:
  1. The bank-rec self-heal three-fact rule (check 1: `nk_recbal ≈ closing`; check 2: `nk_lststdt >= period_end`; check 3: `nk_lststno >= statement_number`) can falsely fire after a reversal because checks 2 and 3 still pass against stale fields. Check 1 normally protects (recbal is recalculated), but if the user re-runs a partial rec on the same bank, bringing recbal back near closing, the heal could mis-flip an unrelated row.
  2. Opera Cashbook > Reconcile UI will show the reversed statement as "in progress" (because `nk_recstfr`/`nk_recstto` still point at it), while the entries are no longer reconciled — confusing operator state.
**Repro:** Static-only — see `unreconcile_entries` route at line 924 onwards. Compare to the proper reversal in `scripts/reverse_bank_rec_batch.py:107-` which correctly walks back to the previous batch's closing state and updates every Stage B field.
**Fix sketch:** Replace the route's nbank UPDATE with the same logic as `scripts/reverse_bank_rec_batch.py`: snapshot the prior batch's closing state and revert `nk_lststno/lststdt/lstrecl/reclnum/recldte/recstfr/recstto/recstdt/recstln/reccfwd` to those values; clear `ae_recdate` AND `ae_recbal` on the aentry rows; mirror in the Opera 3 endpoint. Keep the JSON audit-trail write that the script already does.

#### F2: Opera 3 `complete_reconciliation` endpoint missing partial→full smart promotion (parity gap)
**Where:** `apps/bank_reconcile/api/routes.py:15780-15833` (Opera 3 partial branch).
**Symptom:** A partial reconciliation on Opera 3 that brings Opera's reconciled balance to the statement closing balance is NOT auto-promoted to full, while the SE equivalent IS (SE: `apps/bank_reconcile/api/routes.py:10810-10848`).
**Cause:** The Opera 3 route writes `is_reconciled=1` only when `not partial`. The SE route additionally tests `result.new_reconciled_balance ≈ closing_balance` and promotes partial → full when true.
**Impact:** SE/Opera 3 behavioural divergence (CLAUDE.md mandates parity). Opera 3 customers running partial recs that complete the bank balance will see the statement stay "in progress" until the next scan triggers the heal — works eventually but inconsistent with SE which closes immediately. This violates the documented contract from the audit prompt: "Smart promotion of partial→full when balances match."
**Repro:** Static-only.
**Fix sketch:** Mirror SE's promotion check inside the Opera 3 partial branch, using `result.new_reconciled_balance` from `mark_entries_reconciled` (the Opera 3 version returns this, see `sql_rag/opera3_foxpro_import.py:5910`).

#### F3: In-app `unreconcile` endpoint leaves `ae_recbal` set on reversed aentry rows
**Where:** `apps/bank_reconcile/api/routes.py:946-958` (SE); also see Opera 3 at `15037-15043`.
**Symptom:** After unreconcile, `ae_recbal` (running reconciled balance per entry) still holds the stale value from the original rec.
**Cause:** The Stage A clear list is `ae_reclnum, ae_recdate, ae_statln, ae_frstat, ae_tostat, ae_tmpstat` — `ae_recbal` is missing. Opera 3 endpoint also misses `ae_recdate`, `ae_recbal` and on Opera 3 doesn't even reset `nk_recbal` to zero/proper value when last batch reversed.
**Impact:** Future rec of those entries will write a fresh `ae_recbal` that's correct, so the immediate impact is small, but any audit/report on aentry sees a stale "reconciled balance after this entry" for an entry whose rec status is 0. Audit confusion at minimum; could mislead reconciliation diff reports that read `ae_recbal` directly.
**Repro:** Static-only.
**Fix sketch:** Add `ae_recbal = 0` (and on Opera 3 also `ae_recdate = NULL` plus `ae_recbal = 0`) to the UPDATE column list. Better: replace with a call to the proper Stage-A reset routine used by `scripts/reverse_bank_rec_batch.py`.

### SHOULD-FIX

#### F4: Auto-reconcile path generates statement_number from date — collisions and overflow
**Where:** `apps/bank_reconcile/api/routes.py:4577` (SE auto-reconcile in `import_bank_statement_from_pdf`); same pattern at `:5199, :10089, :13577` (Opera 3).
**Symptom:** Two statements on the same day on the same bank will produce the same statement_number; in normal bank-numbering schemes this clashes with already-stored Opera `nk_lststno` values.
**Cause:** `statement_number = int(latest_date.strftime('%y%m%d'))` derives a 6-digit number from the latest transaction date instead of taking it from the statement metadata or `nk_lststno + 1`.
**Impact:** If `nk_lststno` was 86940 and the user runs auto-reconcile after 1 May 2026, the new "statement number" becomes 260501, jumping the sequence by 173,561. Subsequent partial recs through the proper Stage 5 path use `last_stmt_no + 1` (see `BankStatementReconcile.tsx:2022`) and inherit the inflated number. Opera's bank rec list now shows numbers that don't correspond to the bank's real statement IDs.
**Repro:** Static-only — see `auto_reconcile` block at line 4543.
**Fix sketch:** Replace with `last_stmt_no + 1` from `nbank.nk_lststno`, or read from `statement_info_dict`'s extracted statement number. The same fix needs to land in all four locations (SE x2, Opera 3 x2).

#### F5: Opera 3 stran writes use `st_cusref` while reads use `st_custref` (column-name drift)
**Where:** `sql_rag/opera3_foxpro_import.py:1952` (`st_cusref` write in import_sales_receipt); `:5052` and `:5066` (`st_custref` reads); also writes at `:2282, :3408, :6381, :6424` and `:1977, :2307, :3433, :6406, :6449` (`st_custype` write vs nothing-read).
**Symptom:** A sales receipt posted via Opera 3 fills `st_cusref` (9-char field name) but later code reads `st_custref` (10-char) on the same rows.
**Cause:** Either a typo in writes that has gone unnoticed because Opera 3 DBF column names are space-padded and may map both, OR the FoxPro DBF column is genuinely `st_cusref` (max DBF field name = 10 chars and Opera 3 uses 9) and reads have been accessing a non-existent attribute. SE schema (`scripts/opera_snapshot.json`) confirms the SE column is `st_custref`. The same mismatch appears on `st_custype` (write) — `stran` schema in SE has `st_type`, no `st_custype`.
**Impact:** Either (a) writes are silently dropping these fields if the DBF column is named differently from what we write, leaving payment-method blank on receipts and missing customer-type on stran; or (b) reads are silently returning empty strings. The GoCardless route at `apps/gocardless/api/routes.py:4988` reads `st_cusref` (matching the write side) — suggesting Opera 3 DBF actually uses `st_cusref`, which would mean every read at `:5052/5066` for Opera 3 is broken. Either way, customer-reference bookkeeping is wrong on Opera 3, with downstream effects on bank-rec matching that uses `st_custref`/`st_cusref` as a tie-breaker.
**Repro:** Static-only — `grep -n "st_cusref\|st_custref" sql_rag/opera3_foxpro_import.py`.
**Fix sketch:** Inspect a real Opera 3 stran.dbf header for the actual field name, then standardise on it across writes and reads. Update central KB at `~/opera-knowledge-ref/packages/opera-knowledge/schema/` with the canonical Opera 3 stran column names.

#### F6: Auto-reconcile uses sequential 10/20/30 line numbers without gap-logic for unmatched lines
**Where:** `apps/bank_reconcile/api/routes.py:4554-4563` (SE auto-reconcile after PDF import).
**Symptom:** Auto-reconciled entries get `statement_line = 10, 20, 30, ...` regardless of how many unmatched/skipped lines lie between them.
**Cause:** The block at `:4554` does `statement_line = 10` then `+= 10` per imported entry. The proper path (`OperaSQLImport.calculate_statement_line_numbers`) widens gaps when >9 unmatched items separate matched ones, which is what Opera does natively.
**Impact:** Two side-effects: (1) line numbers don't match what Opera's reconcile screen expects when a future operator re-opens the rec; (2) future inserts of forgotten lines into the same rec batch can collide because the gaps are exhausted.
**Repro:** Static-only.
**Fix sketch:** Reuse `OperaSQLImport.calculate_statement_line_numbers` with the actual `total_lines` and `unmatched_positions` derived from the original statement extraction.

#### F7: Stage 4 `selectedCount` display ignores toggle overrides
**Where:** `frontend/src/pages/BankStatementReconcile.tsx:2545-2547`.
**Symptom:** The button label and disable state shows e.g. "5 Entries" even after the operator toggles two of them ✗ (excluding from the rec batch). The actual posted count after Confirm is 3.
**Cause:** `selectedCount` only counts `selectedAutoMatches`/`selectedSuggestedMatches` set membership; it does not subtract entries with `manualMatchOverrides.get(line.statement_line) === false` and does not add unmatched-statement lines toggled to ✓. The post-payload (line 1966-1982) DOES filter on overrides, so behaviour is correct — only the displayed number lies.
**Impact:** Operator-confidence bug: confirmation dialog says "mark 5 entries" but only 3 actually post. UX, not correctness.
**Repro:** Static-only.
**Fix sketch:** Compute `selectedCount` using the same predicate as `selectedEntriesToReconcile` (the `manualMatchOverrides.get(...) !== false` filter at line 1967/1976).

#### F8: Stage 4 manual-match of unmatched lines is collected but never sent
**Where:** `frontend/src/pages/BankStatementReconcile.tsx:1986-1992`.
**Symptom:** Operator clicks ✗ → ✓ on an unmatched statement line. UI shows the row as matched. On submit, the line is silently discarded — never posted to `/complete`.
**Cause:** Code computes `manuallyMatchedLines` (line 1986) but never adds it to the request payload. Manual at `marketing/manuals/manual-bank-reconciliation.md:86` implies clicking ✓ on an unmatched line should include it; in reality there's no Opera entry to mark, so semantically there's nothing to do — but the UI doesn't communicate this.
**Impact:** Operator believes line is reconciled but it isn't. The line stays open in Opera and reappears on next scan as still-unmatched. The `remainingUnmatched` math at line 1995-1998 also miscounts: lines toggled ✓ from unmatched are subtracted from the unmatched count (correctly making the user think they're handled) but they're not actually posted.
**Repro:** Static-only.
**Fix sketch:** Either remove the toggle on rows where `entry_number` is null (no Opera entry to reconcile, so toggling ✓ is meaningless), or add a UI hint explaining the toggle on unmatched rows requires posting an entry first. Update the manual at line 86 to match.

#### F9: SE `complete_reconciliation` "promoted to full" path leaves entries in `ae_tmpstat` only
**Where:** `apps/bank_reconcile/api/routes.py:10810-10848`; partial branch in `sql_rag/opera_sql_import.py:7919-7931`.
**Symptom:** When `partial=True` is sent and the route auto-detects `new_reconciled_balance ≈ closing_balance` and "promotes" to full, the entries that were just written get `ae_tmpstat = stmt_line` (partial markers). They never get `ae_reclnum` or `ae_recbal` written. The local DB UPDATE at line 10852 sets `is_reconciled=1` regardless.
**Cause:** Promotion logic only flips the local flag and the response; it does not re-call `mark_entries_reconciled` with `partial=False` to upgrade Stage A fields.
**Impact:** Entries are not permanently reconciled in Opera (they stay in tmpstat which Opera treats as "in-progress reconciliation"), but the app says they're done. Next time someone opens the rec in Opera Cashbook > Reconcile, the entries will appear pre-ticked but uncommitted. nbank Stage B fields are also not updated to a "complete" state — `nk_recbal` was not advanced because `mark_entries_reconciled` was called with `partial=True`.
**Repro:** Static-only — exercises only on the rare "Opera was already at closing balance" path.
**Fix sketch:** When promotion fires, call `mark_entries_reconciled` again with `partial=False` to upgrade Stage A, OR reject the promotion entirely (leave it as a partial rec the heal can later confirm). Cleanest: detect the condition BEFORE calling mark_entries_reconciled and override `partial=False` from the start.

#### F10: SE/Opera 3 unreconcile not behind the `complete_reconciliation` rec-contract
**Where:** SE `apps/bank_reconcile/api/routes.py:924-1005`; Opera 3 `:14982-15086`.
**Symptom:** Reversal endpoint exists but is not wired to a UI button in `BankStatementReconcile.tsx`. The proper reversal tool (`scripts/reverse_bank_rec_batch.py`) is CLI-only.
**Cause:** No frontend integration. Audit prompt notes "Reversal tool for undoing wrong recs" as a Stage 5 capability — currently only accessible by support engineer running a Python script.
**Impact:** Operator-facing claim ("if a reconciliation needs to be reversed... contact support" — manual line 107) is the actual workflow, which doesn't scale beyond pilot. Without UI access, mistakes in production require admin escalation.
**Repro:** Frontend grep for `unreconcile|reverse` returns no UI button.
**Fix sketch:** Add a "Reverse Reconciliation" action to the BankStatementReconcile UI that calls the FULL reversal script behaviour (not the weaker `unreconcile_entries` route). This needs F1 fixed first so the route is safe to call.

#### F11: Opera 3 reversal script does not exist
**Where:** `scripts/reverse_bank_rec_batch.py` — SE only.
**Symptom:** No Opera 3 mirror reversal tool. CLAUDE.md mandates SE/Opera 3 parity.
**Cause:** Tool was added for SE on 2026-05-03 (per audit prompt), Opera 3 mirror not yet shipped.
**Impact:** Opera 3 customers cannot recover from a wrong rec without manual DBF surgery.
**Repro:** `find /Users/maccb/llmragsql/scripts -name "reverse*"` returns one file.
**Fix sketch:** Mirror `scripts/reverse_bank_rec_batch.py` for Opera 3 using the Opera3Reader/dbf-write pattern from `sql_rag/opera3_foxpro_import.py:5808+` (Stage A reset) and `5847+` (Stage B reset). Use the Opera3WriteAgent if remote.

#### F12: Period-bound check uses `ae_lstdate`; matcher and posting use `at_pstdate` — different fields
**Where:** `apps/bank_reconcile/api/routes.py:10720-10755` reads `ae_lstdate` to bound entries to the statement period; the duplicate-check in `sql_rag/duplicate_check_se.py:43` and matcher both use `at_pstdate`.
**Symptom:** A back-dated entry where `ae_lstdate` (record-modified date) is recent but `at_pstdate` (post date) is old will pass the period-bound check at line 10749 (because `ae_lstdate >= grace_start`) yet the duplicate-check looking at `at_pstdate` for the same entry rejects it from rec-batch eligibility.
**Cause:** Two different date fields used for "is this entry in the period". `ae_lstdate` is "last date" — generally the post date but updated by some Opera operations to "now". `at_pstdate` is the canonical post date.
**Impact:** Period-grace-7-days check is leaky in either direction. An entry that was edited within 7 days but posted months ago will pass; an entry posted within the window but with an older `ae_lstdate` (rare) might fail.
**Repro:** Static-only.
**Fix sketch:** Use `at_pstdate` (joined from atran) in the period-bound query, matching the matcher's truth. Or update KB to clarify which date is canonical for "in-period" decisions.

#### F13: `import_sales_refund` does not increment `sn_nextpay`; `import_purchase_refund` does not increment `pn_nextpay`
**Where:** `sql_rag/opera_sql_import.py:2989-2995` (sales refund); `:5070-5076` (purchase refund). Compare to `:2479-2486` (sales receipt) and `:3454-3461` (purchase payment) which DO increment.
**Symptom:** After refunds, the next payment counter on the customer/supplier is not advanced.
**Cause:** Asymmetric implementation between receipts/payments and refunds.
**Impact:** Cheque/BACS payment numbering counter on the customer/supplier may diverge from Opera's expectation. Likely benign (Opera regenerates these on-demand for printing) but inconsistent with how Opera natively tracks refunds.
**Repro:** Static-only.
**Fix sketch:** Add `sn_nextpay = sn_nextpay + 1` (and pn_nextpay) to the refund UPDATE statements, mirror in Opera 3.

### COSMETIC

#### F14: Stage 4 statement-line display uses `line.statement_line * 10` regardless of gap logic
**Where:** `frontend/src/pages/BankStatementReconcile.tsx:2648`.
**Symptom:** UI shows "Line: 10, 20, 30..." but the actual posted `ae_statln` may have wider gaps for unmatched lines.
**Cause:** Display computes a placeholder line number rather than using the value the server will assign.
**Impact:** Display inconsistency with what's written to Opera. Operator confusion if comparing the screen to Opera's reconciled view.
**Fix sketch:** Have the server return the calculated line numbers in the matching response and render those.

#### F15: Confusing comment on rec-batch number guard
**Where:** `sql_rag/opera_sql_import.py:7836-7849`.
**Symptom:** Comment says `nk_lstrecl` "normally already points at the next batch (>=1)" — but for a fresh bank or post-reversal it can be 0. The guard refuses with a message pointing at `scripts/reset_bank_rec_sequence.py`.
**Impact:** Operator hits the guard; they need a script not exposed in UI. CLI-only setup script for production-recovery scenarios.
**Fix sketch:** Auto-reset to 1 when the guard fires (after confirming no other rec is in flight) OR expose the reset as a route.

#### F16: `verify_balance_after_import` is advisory only — drift logged but not corrected
**Where:** `sql_rag/opera_sql_import.py:1133-1175`.
**Symptom:** If concurrent posting drifts the bank balance, drift is logged at WARNING but the transaction commits anyway.
**Impact:** Documented behaviour ("never raises or rolls back — purely advisory"). Adequate for now if log monitoring is in place; not adequate if log monitoring is not in place. For a finance-grade system targeting "100% reliability" (CLAUDE.md), advisory-only verification is weak.
**Fix sketch:** Long-term, fold the verify into the same transaction with a re-read and CHECK constraint, or escalate WARNING to a metric/alarm.

#### F17: Promotion-detection check at SE complete reconciliation is essentially never true
**Where:** `apps/bank_reconcile/api/routes.py:10813-10817`.
**Symptom:** `new_rec_bal_check is not None and abs(new_rec_bal_check - closing_balance) < 0.01` — but for a partial rec, `new_reconciled_balance` returns the UNCHANGED `nk_recbal` (because partial does not touch nk_recbal). So the promotion can only fire when Opera was already at the closing balance BEFORE this partial — i.e. the user partial-rec'd 0 entries.
**Impact:** Dead code in practice. Either the design needs revisiting (intent unclear) or the check should be removed/simplified. Related to F9.
**Fix sketch:** Confirm intent with the design author (see comment at line 10810: "This catches partial reconciliations that complete the statement"). If the intent is "detect that Opera reached closing through some other path", the check is correct but rare; if the intent is "if matched entries totalled to closing, promote", the calculation needs to be `current_rec_balance + sum(matched_values)`.

## Confirmed-good areas

- **Bank-rec self-heal logic** (`sql_rag/bank_rec_heal.py`) — three-fact rule cleanly implemented; legacy fallback uses `transactions_imported` (not the date-range count, per spec); idempotent (single SQLite transaction with commit at end); read-only against Opera (only `read_nbank` and `count_reconciled_aentry` calls).
- **Heal call sites** — both SE (`apps/bank_reconcile/api/routes.py:6072-6099`) and Opera 3 (`:12217-12245`) call the heal once per bank before the "already-processed" filter, with the proper `try/except + warning` so heal failures don't break scans. Per-company isolation via `get_current_db_path('email_data.db')`.
- **`statement_number` persistence** on partial AND full branches in both SE (`apps/bank_reconcile/api/routes.py:10826-10837` and `:10849-10863`) and Opera 3 (`:15800-15815` and `:15818-15833`).
- **Stage 5 Stage A on full reconciliation** (`sql_rag/opera_sql_import.py:7937-7951`): rec batch number from `nk_lstrecl` (refused if <1, never derived); `ae_recdate`, `ae_statln`, `ae_frstat`, `ae_tostat`, `ae_recbal` all written; `ae_tmpstat` cleared.
- **Stage 5 Stage B on full reconciliation** (`sql_rag/opera_sql_import.py:7989-8004`): all required fields (`nk_recbal, nk_reccfwd, nk_lstrecl, nk_lststno, nk_lststdt, nk_reclnum, nk_recldte, nk_recstfr, nk_recstto, nk_recstdt, nk_recstln`) written in one UPDATE.
- **Posting completeness for sales receipt** (`sql_rag/opera_sql_import.py:2048-2541`): all required tables (aentry, atran, ntran×2, anoml×2, stran, sname, atype, nbank, nacnt+nhist+nsubt+ntype) updated; journal numbers from `nparm.np_nexjrnl` via `_get_next_journal`; ntran nt_type/nt_subt from `_get_nacnt_type`; entry numbers from `increment_atype_entry`; period from `get_period_for_date`; double-entry balanced; deadlock retry wrapper; ROWLOCK hints on UPDATE statements; pre-flight record-locked check.
- **Posting completeness for purchase payment** (`:3055-3520`) — same pattern, equivalent to sales receipt.
- **Posting completeness for sales refund** (`:2610-3050`), **purchase refund** (`:4705-`), and **bank transfer** (`:9118-`) — all create the right tables; bank transfer uses alphabetical lock order to avoid deadlocks.
- **VAT tracking** (`sql_rag/opera_sql_import.py:4087-` in `import_nominal_entry`) — `zvtran` and `nvat` written when VAT applies; correct `nv_vattype` (S for receipts, P for payments).
- **Period validation** — every posting method calls `get_period_posting_decision` with the appropriate ledger type ('SL', 'PL', 'NL') BEFORE writing anything; a closed-year guard at routes layer (`apps/bank_reconcile/api/routes.py:4144-4158`) refuses postings dated before the open-year start.
- **Bank-level import lock** (`sql_rag/import_lock.py` via `acquire_import_lock(_bank_lock_key(bank_code))`) — applied around `complete_reconciliation`, `unreconcile`, `import_bank_statement_from_pdf`, etc. Releases in finally/exception paths.
- **NOLOCK on reads** in heal data sources (`sql_rag/duplicate_check_se.py:127, 161, 189`) — per locking-protocol mandate.
- **Stage 4 toggle math at completion call site** (`frontend/src/pages/BankStatementReconcile.tsx:1966-1972, 1975-1982`): the filter `selectedAutoMatches.has(...) && manualMatchOverrides.get(stmt_line) !== false` is correctly applied to both auto_matched and suggested_matched when building `selectedEntriesToReconcile`, AND in the balance-summary calculation (`:2743-2746`). `remainingUnmatched` (`:1995-1998`) correctly counts truly-unmatched + toggled-off matches.
- **Partial-rec triggers user prompt** (`:2002-2011`) when `remainingUnmatched > 0`, then proceeds with `partial=true`.
- **Self-heal regression scenario** (intsys/BC010/import 71) — implementation matches the spec at `docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md`, including the legacy-row fallback to `transactions_imported`.
