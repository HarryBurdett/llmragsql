# Stages 1-2 Audit Findings

**Date:** 2026-05-05
**Scope:** Stage 1 (Select Statement), Stage 2 (Review & Match).

## Summary

The 2026-05-04 open-items rule (`ae_reclnum=0 AND ae_remove=0`) was applied to the consolidated duplicate-check path (`duplicate_check_se.py`, `duplicate_check_o3.py`) and to the `_is_already_posted_typeblind` fallback in `bank_import.py`, but **two independent matching paths still fetch reconciled candidates without the `ae_remove` filter** (`StatementReconciler.get_unreconciled_entries` / `get_all_entries` on both SE and Opera 3) and one of them is the active Stage 2 matcher used by `/api/reconcile/process-statement`. Several Opera-3 paths read a non-existent column name (`nb_acnt`/`NB_ACNT`) which silently returns NULL and disables both balance validation and sequential gating in Opera 3 scan-emails. There are also significant SE/Opera-3 parity gaps in scan-folder, the type-blind fallback, and `_check_repeat_entry`. Pervasive f-string SQL interpolation puts every endpoint in this scope at SQL-injection / quote-breaking risk.

## Findings

### CRITICAL

#### F1: `StatementReconciler.get_unreconciled_entries` and `get_all_entries` ignore the open-items rule (SE + Opera 3 parity)
**Where:**
- `sql_rag/statement_reconcile.py:1197-1213` (SE — `get_unreconciled_entries`)
- `sql_rag/statement_reconcile.py:1424-1440` (SE — `get_all_entries`, no `ae_reclnum` filter at all)
- `sql_rag/statement_reconcile_opera3.py:837-893` (Opera 3 — `get_unreconciled_entries`)
- `sql_rag/statement_reconcile_opera3.py:895-952` (Opera 3 — `get_all_entries`)

This is the candidate fetch invoked from `/api/reconcile/process-statement` (`apps/bank_reconcile/api/routes.py:1407`) — the heart of Stage 2 for the legacy reconcile flow.

**Symptom:** A statement line silently re-pairs with an entry the operator previously matched out via Opera's matching facility (`ae_remove=True`). The line is rendered as already-in-Opera and the new posting is suppressed; the bank statement balance loses a transaction.
**Cause:** The SE WHERE clause is `ae_acnt = '...' AND (ae_reclnum = 0 OR ae_reclnum IS NULL) AND ae_complet = 1`; the Opera-3 in-memory filter only checks `ae_reclnum != 0` and `ae_complet != 1`. Neither uses `OPEN_FOR_REC_SQL` / `is_open_for_rec()`. The 2026-05-04 spec mandates *every* candidate-fetch site applies the rule, but this site was missed in the round of changes.
**Impact:** Re-introduces the exact Cloudsis BB005 / Flannery £198 bug for any statement that flows through the legacy `/api/reconcile/process-statement` path or `process_statement_unified`. Production-blocking for any company that uses Opera's matching facility.
**Repro:** Static-only — see code. The contract test `tests/test_bank_rec_candidate_filter.py` evidently doesn't cover this site.
**Fix sketch:** Add `AND ae_remove = 0` to the SE queries (or substitute `OPEN_FOR_REC_SQL`); for Opera 3 add `is_open_for_rec(row)` to the in-memory filter loops. Mirror the change in both `get_all_entries` paths and add a contract-test entry that imports the source of these two functions.

#### F2: Opera 3 scan-emails reads non-existent column `nb_acnt`/`NB_ACNT` — `reconciled_balance` always None
**Where:** `apps/bank_reconcile/api/routes.py:12207-12213` (and same pattern at lines 12835-12836, 13062-13067, 14457-14458)
**Symptom:** Opera 3 users get no opening-balance validation and no sequential gating. Statements show the "Found 1 statement" message but the response field `reconciled_balance` is `None`, so:
- The "expected next opening" guidance message is never produced.
- The chain-based already-processed filter at line 12446 (`statement_opening_balance < eff_bal - 0.01`) never fires (eff_bal=0/None).
- The sequence ordering loop (line 12608) silently no-ops, leaving statements unsorted.
**Cause:** The actual Opera 3 nbank column is `nk_acnt` (verified against `scripts/opera_snapshot.json` and `sql_rag/opera3_data_provider.py:1527`). The scan-emails handler reads `record.get('NB_ACNT', record.get('nb_acnt', ''))` — neither key exists, so `nb_acnt` resolves to the empty string and the `if nb_acnt == bank_code.upper()` test never passes for any record.
**Impact:** Opera 3 users effectively run Stage 1 without any of the safety rails. Statements out of sequence, against wrong accounts, or for periods already reconciled all pass through unfiltered. Direct violation of the documented "balance match", "account match" and "sequential import" rules in CLAUDE.md.
**Repro:** Static-only — see code. The Opera-3 preview-from-pdf at line 13062 has the `nk_acnt` fallback, which is what saved the matching there; scan-emails was missed.
**Fix sketch:** Replace every `record.get('NB_ACNT', record.get('nb_acnt', ''))` with `record.get('nk_acnt', record.get('NK_ACNT', ''))` (or use `_row_get` from `duplicate_check_o3.py`). Also fix occurrences at lines 12835-12836 and 14457-14458.

#### F3: Opera 3 `_is_already_posted` mirror has no type-blind fallback — risks double-posting
**Where:** `sql_rag/bank_import_opera3.py:1058-1092`
**Symptom:** When the matcher cannot classify a transaction (`txn.action` is None or `'skip'`), the SE path runs `_is_already_posted_typeblind` to look up "is there *any* atran row for this bank with this signed amount in the date window?" and flags it as duplicate. The Opera 3 mirror returns `(False, "")` and lets the row fall through.
**Cause:** Spec parity miss in commit 856d5ad. The SE function (`bank_import.py:1466-1528`) was updated to restore the type-blind fallback after the bug Charlie found in Cloudsis BB005 (HISCOX direct debit posted as `at_type=1` not `at_type=5`). The Opera 3 file was not updated.
**Impact:** Double-posting risk on Opera 3 for any direct-debit / standing-order whose statement description doesn't match a customer/supplier name strongly enough to classify it. Same class of bug as the original SE incident — finance-critical.
**Repro:** Static-only — see code.
**Fix sketch:** Add an in-memory equivalent that scans `atran` for rows on this bank with matching signed `at_value` within ±7 days, joined to `aentry` via `ae_acnt`+`ae_cbtype`+`ae_entry`, and applies `is_open_for_rec()`.

#### F4: `/api/opera3/bank-import/scan-folder` delegates to the SE handler and uses the SE SQL connector
**Where:** `apps/bank_reconcile/api/routes.py:5976-5982`
**Symptom:** When an Opera-3 user hits scan-folder, the handler calls the SE function (`scan_folder_for_bank_statements` at line 5497), which runs `sql_connector.execute_query(... FROM nbank WITH (NOLOCK) WHERE RTRIM(nk_acnt) = :bank_code)` against whatever `sql_connector` is currently bound. For an Opera-3-only company there is no SQL connector (it's None or — worse — set to a stale SE connector from a prior request).
**Cause:** Stub delegation that was never actually implemented. The SE function uses `sql_connector` (SQL Server connection); the Opera 3 version should be reading `nbank` from the FoxPro DBF via `Opera3Reader`.
**Impact:** scan-folder either silently returns wrong reconciled balances (if the SE connector belongs to a different company), or the SQL query throws, or `reconciled_balance` is None (no validation, no sequential gating). Opera 3 users have no working folder-scan path. Combined with F2, Opera 3 scan-emails-with-folder mode is also broken.
**Repro:** Static-only — see code.
**Fix sketch:** Create a real Opera-3 implementation reading `nbank` via `Opera3Reader`; do not share state with the SE connector. Mirror F2's `nk_acnt` fix.

#### F5: Pervasive f-string SQL interpolation throughout Stage 1/2 candidate paths
**Where (representative — many more):**
- `apps/bank_reconcile/api/routes.py:165-170` (`_auto_clean_resolved_defers` — `f"WHERE at_acnt = '{bank_code}' AND ABS(...) {abs(amount_pence)}"`)
- `apps/bank_reconcile/api/routes.py:296-299` (`bank_sql = f"... WHERE nk_acnt = '{bank_code}'"`)
- `apps/bank_reconcile/api/routes.py:3540-3573` (`preview-from-pdf` — `bank_code`, `stmt_sort`, `stmt_acct` all f-string interpolated)
- `sql_rag/duplicate_check_se.py:36-46, 67-100` (every WHERE built by f-string)
- `sql_rag/bank_import.py:1133-1141, 1167-1187, 970-977, 1048-1064` (every helper query)
- `sql_rag/statement_reconcile.py:540, 1191-1213, 1418-1440` (every aentry read)
- `sql_rag/opera_sql_import.py:8385-8410, 8617-8630` (period-bound matcher uses isoformat dates as f-string args)

**Symptom:** Any value containing a single quote (or unexpected whitespace, or a malicious payload from a PDF the AI extracted) breaks the SQL or executes attacker-controlled SQL. `bank_code` flows in from URL query params, `stmt_sort`/`stmt_acct`/`stmt_desc` flow in from AI extraction over arbitrary PDF content, deferred-row descriptions are user input, etc.
**Cause:** Every author chose f-strings; `sql_connector.execute_query` accepts a `params=` kwarg (used in two places: `bank_import.py:1201` and `routes.py:6051`) but was bypassed at most call sites.
**Impact:** Even ignoring malicious-payload SQLi, this is brittle: any apostrophe in a customer name breaks the query, any AI hallucination of a comma-formatted account number explodes. With audit DB access through SQLite parameter binding the contrast is stark — same codebase has both styles. Production reliability + security risk.
**Repro:** Static-only — see code. To verify SQLi: run a scan with `bank_code` set to `BC010' OR '1'='1`. Any of the ~20 sites I inspected would execute the injected SQL.
**Fix sketch:** Convert every dynamic value to bind parameters via `execute_query(query, params=[...])` (the SE connector already supports both positional and named binding — see `routes.py:6051` and `bank_import.py:1201` for working examples). Audit all `execute_query(f"..."` and `execute_query(query)` where `query` was built with f-strings.

### SHOULD-FIX

#### F6: Opera 3 scan-emails sequential gating depends on a self-referential `bank_rec_openings`
**Where:** `apps/bank_reconcile/api/routes.py:12273-12315`
**Symptom:** Opera 3's `_opening_unblocks_chain` checks against `bank_rec_openings = _tracking['reconciled_opening_balances'].get(bank_code, set())`. If F2 above means `reconciled_balance` is None, the SE-equivalent code returns False on most paths, but the Opera-3 path still treats `chain_complete = stmt_closing in bank_rec_openings` (line 12435) as gospel — so a statement whose closing happens to equal a previously-stored opening will be filtered out of "ready" without any account/sort-code or balance check having succeeded.
**Cause:** The chain-completion shortcut at line 12441 fires before the account-matches/balance check.
**Impact:** Edge case: a fresh statement for the right account is silently moved to "already processed" because its closing matches some prior reconciled opening (e.g. operator imported an old one for a sibling bank).
**Fix sketch:** Gate `chain_complete` on `account_matches=True` (the SE handler does this at line 6302).

#### F7: Bank-transfer detection matches on sort-code substring without bounds
**Where:** `sql_rag/bank_import.py:1232-1257`
**Symptom:** `_check_bank_transfer` strips dashes/spaces from memo+name+reference and substring-matches each other Opera bank's sort code. A six-digit number anywhere in a statement description (date, reference, customer ref) becomes a false-positive bank transfer. Real-world example: a customer's invoice number `INV202609001234` would match a bank with sort code `262090` (substring), classifying the receipt as a bank transfer to the wrong account.
**Cause:** Plain substring search after normalization, no word boundaries.
**Impact:** Mis-classification at Stage 2 → bank transfer posted instead of sales receipt → debtor not paid down, two banks' balances reconciled wrong.
**Fix sketch:** Match on `\b{sort}\b` against the *unnormalized* text, OR require sort-code adjacent to the literal string "sort code" / similar context, OR drop sort-code-only matching entirely and rely on account-number matches.

#### F8: `_match_transaction` ambiguity-resolution writes `match_type` only when fuzzy match wins; ambiguous fuzzy-review path doesn't set it
**Where:** `sql_rag/bank_import.py:1346-1395`
**Symptom:** When both customer & supplier match strongly (score_diff < 0.15), the code sets `matched_account`, `matched_name`, `match_score`, `action`, but never `match_type`. Downstream renderers that branch on `txn.match_type == 'customer' / 'supplier'` (e.g. for icon / colour) will see the null. Same in the score-diff > 0.15 review-flag path.
**Cause:** Field oversight in the ambiguity branches; the simple "is_match" branches at 1401, 1425 do set it.
**Impact:** Cosmetic UI glitch most of the time; could mis-render counts.
**Fix sketch:** Set `txn.match_type = 'customer' if txn.action.startswith('sales') else 'supplier'` in each ambiguous-resolution branch.

#### F9: `_check_repeat_entry` builds raw SQL from AI-derived txn name/memo/reference
**Where:** `sql_rag/bank_import.py:1024-1068`
**Symptom:** `clean = text.strip().replace("'", "''").upper()` then injected into `LIKE '%{term}%'`. Single quotes are escaped, but `%` and `_` (SQL LIKE wildcards) and `[` (T-SQL wildcard) are not. A description like `100% PURE` produces `LIKE '%100% PURE%'` — still works in this case, but a payee containing `%_%` or `[abc]` would cause unintended LIKE matching, returning candidates the operator didn't expect.
**Cause:** No LIKE-pattern escaping helper.
**Impact:** Edge-case false-positive matches in repeat-entry detection.
**Fix sketch:** Escape `%`, `_`, `[`, `]` in `term` before substituting; or convert to parameterized query (preferred).

#### F10: `_match_transaction` is wrapped in defer-skip but `_is_already_posted` runs after defer-aware match for `'skip'` action only
**Where:** `sql_rag/bank_import.py:1273-1276` (defer skip) + `1466-1482` (action-None/'skip' fallback)
**Symptom:** A row marked `txn.action == 'defer'` skips matching, but `_is_already_posted` is still called by `process_transactions` (line 1881 and 1890). The fallback-typeblind branch only fires when `not txn.action or txn.action in ('skip',)` — defer is a non-trivial action that bypasses the type-aware path AND doesn't get type-blind fallback. So a deferred row coincidentally already in Opera will not be flagged duplicate and the operator will see "deferred" instead of "already posted".
**Cause:** Asymmetric action-set in the fallback condition.
**Impact:** Operator could re-defer a row that's already posted; not data corruption but operationally confusing. May cause `deferred_count` to over-count.
**Fix sketch:** Add `'defer'` to the fallback set, or skip `_is_already_posted` entirely for deferred rows (returning False with a defer-specific reason).

#### F11: Sync cooldown timezone bug in scan-all-banks
**Where:** `apps/bank_reconcile/api/routes.py:6625-6645`
**Symptom:** The "synced within last 5 minutes" check parses `last_sync` from ISO string, then computes `(datetime.now(last_dt.tzinfo) if last_dt.tzinfo else datetime.utcnow()) - last_dt.replace(tzinfo=None)`. If `last_sync` is timezone-aware (UTC offset attached), `datetime.now(tz)` returns a tz-aware now, then subtracts `last_dt.replace(tzinfo=None)` — that's an unsupported subtraction (aware − naive) and will raise TypeError. The outer `try/except Exception: pass` swallows it silently.
**Cause:** The conditional branches on tz-presence but mixes aware/naive in the subtraction.
**Impact:** Cooldown silently fails — every scan re-syncs. Wastes IMAP bandwidth and slows scans. Failure is silent so nobody notices.
**Fix sketch:** Normalise both sides to UTC naive: `now = datetime.utcnow(); last = last_dt.astimezone(timezone.utc).replace(tzinfo=None) if last_dt.tzinfo else last_dt`. Then `now - last < timedelta(minutes=5)`.

#### F12: Sequential-gating chain-advance only ever picks one statement per chain; no cycle-break tolerance
**Where:** `apps/bank_reconcile/api/routes.py:6463-6485` and `12608-12628`
**Symptom:** The order-by-chain loop picks the first statement whose opening matches `current_bal` exactly (±£0.01). If two statements have the same opening (e.g. duplicate emails after the dedup step missed one, or two banks with co-incidentally equal openings, or sub-pence rounding split a chain), the loop picks the first found and ignores the rest.
**Cause:** `for i, s in enumerate(remaining): ... break` — first match wins.
**Impact:** Edge-case sequence display issues where a real second statement is sorted as if out-of-sequence.
**Fix sketch:** When ambiguous (multiple openings match), break by `period_start` ascending or `received_at` ascending; log a WARNING.

#### F13: `derive_statement_state` doesn't surface `'imported'` vs `'reconciled-with-zero-deferred'` distinction the spec describes
**Where:** `sql_rag/deferred_transactions_db.py:168-205`
**Symptom:** Per the design spec (2026-04-30), an imported-but-not-reconciled statement with zero deferred rows should return `'reconciled'` (because Stage 4 ran cleanly). The current implementation returns `'imported'` whenever `has_import_record=True` and `is_reconciled=False`, ignoring `deferred_count`. The doc-string at line 197-201 acknowledges this asymmetry.
**Cause:** Designer left it vague: "Both render the same way (amber 'awaiting reconcile' badge); `deferred_count` flows through separately."
**Impact:** Per the spec the next-statement gate is `prior.state in ('imported', 'reconciled')` — both unblock — so functional impact is nil. But the per-bank "X statements imported with deferred items" summary the spec describes will count statements where deferred_count is genuinely zero, just because Stage 4 didn't run for some other reason. UI counts may mislead.
**Fix sketch:** If the spec wants strict separation, update the function to `if has_import_record and deferred_count == 0: return 'reconciled'` or add a separate flag the UI consumes.

#### F14: `_auto_clean_resolved_defers` matches on absolute amount only — ignores sign
**Where:** `apps/bank_reconcile/api/routes.py:165-170`
**Symptom:** SQL is `ABS(ABS(at_value) - {abs(amount_pence)}) < 1`, matching any atran row whose absolute amount equals the deferred row's, regardless of sign or transaction type. A deferred receipt of £100 would auto-clean if a £100 payment posts.
**Cause:** Sign-blind match. The recent push to make duplicate-check sign-aware everywhere (`duplicate_check.py`) didn't reach this defer-cleanup helper.
**Impact:** Defer audit rows could be silently dropped against unrelated transactions. Operator loses the audit trail of the deferred row when the wrong-direction transaction posts.
**Fix sketch:** Use signed comparison: `at_value = {amount_pence}` (preserving sign); or at minimum filter by direction (`AND at_value > 0` for receipts, `< 0` for payments).

#### F15: scan-emails `email_storage.record_bank_statement_import` swallow all exceptions silently
**Where:** `apps/bank_reconcile/api/routes.py:6314-6329`, `12452-12485`
**Symptom:** When auto-skipping a "below reconciled" statement, the code calls `email_storage.record_bank_statement_import(...)` inside a bare `try/except: pass`. If the insert fails (schema drift, unique constraint, locked DB), the statement is filtered out for THIS scan but never tracked, so the next scan re-evaluates it — the user sees the same skipped statement returning every time.
**Cause:** Bare `except` with no logging.
**Impact:** Doesn't corrupt data, but costs the operator a scan-cycle every time and pollutes logs.
**Fix sketch:** Catch `Exception as e` and log at WARNING level.

#### F16: `bank_aliases.lookup_alias` does not pass `bank_code` — direction-aware but not bank-aware
**Where:** `sql_rag/bank_aliases.py:241-272`
**Symptom:** The alias schema is `UNIQUE(bank_name, ledger_type)` — there is no per-bank scoping. A name "AMAZON" that maps to one supplier when seen on the personal-card bank could map to a different supplier on the business bank, but the alias system flattens them.
**Cause:** Schema design — see `bank_aliases.py:97-110`.
**Impact:** Multi-bank companies get cross-bank alias bleed. Saved alias from BC010 applies to BC050.
**Fix sketch:** Add `bank_code` to the unique constraint and to lookup. Existing repeat_entry_aliases table already does this — same pattern.

#### F17: `RTRIM(nk_acnt)` inconsistency between scan-emails and other endpoints
**Where:** Compare `apps/bank_reconcile/api/routes.py:6049` (`WHERE nk_acnt = :bank_code`) vs `5534` (`WHERE RTRIM(nk_acnt) = :bank_code`)
**Symptom:** SQL Server CHAR fields are space-padded. A query without `RTRIM` will fail to match a passed `'BC010'` against a stored `'BC010    '` unless SQL Server auto-pads the RHS to the column's declared width (which it does for `=` but not for parameter binding under all collations).
**Cause:** Mixed conventions across endpoints.
**Impact:** Edge cases where `nbank.nk_acnt` is varchar (post-migration) or where collation differs — bank not found, scan returns empty.
**Fix sketch:** Pick one convention (`RTRIM(nk_acnt) = :bank_code`) and apply uniformly. Consult the existing `routes.py` patterns — most use RTRIM.

#### F18: Manual auto-extraction during scan blocks the scan loop on Gemini call latency
**Where:** `apps/bank_reconcile/api/routes.py:6332-6369`, `7106-7162`, `7456-7489`, `12486-12525`
**Symptom:** When the cache misses, the scan endpoint runs full Gemini extraction synchronously per statement. A 30-day scan with 10 unseen statements blocks for 10× extraction latency (5-30s each). The Gemini throttle module enforces ≥1s between calls process-wide, so the scan can serialise. Sequential gating is honoured via `extraction_status='pending_extraction'`, but the scan itself hangs.
**Cause:** Inline AI extraction in the scan path.
**Impact:** Operator hits "Scan" → request can take 60-300s → may timeout at the proxy layer / browser. Triggers F11 (cooldown) re-evaluation on retry.
**Fix sketch:** Either (a) skip extraction during scan (status=pending_extraction, defer to user clicking Process), or (b) run extractions in parallel with `asyncio.gather`, or (c) move to a background worker and return optimistically.

### COSMETIC

#### F19: Dead placeholder endpoint `/api/reconcile/bank/{bank_code}/scan-emails`
**Where:** `apps/bank_reconcile/api/routes.py:1902-1920`
**Symptom:** Endpoint returns "Email scanning not yet implemented" — the real path is `/api/bank-import/scan-emails`. The mirror at `/api/opera3/reconcile/bank/{bank_code}/scan-emails` (line 15389-15390) appears to also exist as a placeholder.
**Impact:** None at runtime (no caller). API surface clutter.
**Fix sketch:** Delete both, or document that they're stubs to remove in a future cleanup.

#### F20: Inconsistent error response shape — `success/error` vs `error/statements_found` vs HTTPException
**Where:** Multiple in `routes.py` — `6066-6070`, `6582`, `7697`, `8056-8058`, etc.
**Symptom:** Some error paths return `{"success": False, "error": str(e)}`, others raise HTTPException(500), the bank-not-found path returns `{"error": ..., "statements": []}` with no `success` key.
**Impact:** Frontend has to handle 3 shapes; failure messaging is inconsistent.
**Fix sketch:** Pick one — `{"success": False, "error": friendly_db_error(e)}` — and apply uniformly.

#### F21: Excessive logging at INFO level inside hot scan loop
**Where:** Throughout `scan_all_banks_for_statements` — `logger.info` for every PDF processed, every cache hit/miss, every fallback.
**Impact:** Noisy logs at production volume. 100-bank scan can produce ~1000 INFO lines.
**Fix sketch:** Demote per-statement detail to DEBUG; keep per-bank summary at INFO.

#### F22: `imported_pending_closings` keyed differently in SE and Opera 3 sequential gating
**Where:** `apps/bank_reconcile/api/routes.py:6744-6760` (SE — dict per bank) vs `12286-12298` (Opera 3 — flat set for the requested bank only)
**Impact:** Slight conceptual divergence; both work but the SE version supports multi-bank in one pass and Opera 3 hard-codes the single-bank case. If Opera 3 ever switches to multi-bank scan, the gating won't translate. Cosmetic for now.
**Fix sketch:** Align signatures so the helper takes `bank_code` and returns the per-bank set.

## Confirmed-good areas

- **Open-items rule applied at consolidated duplicate-check sites:** `sql_rag/opera_open_items.py` is correctly imported by `sql_rag/duplicate_check_se.py:14`, `sql_rag/duplicate_check_o3.py:52`, `sql_rag/bank_import.py:1568`, and `sql_rag/opera_sql_import.py:8401`. The SQL fragment alias-prefix rewrite (`replace('AND ', 'AND a.')`) produces correct output on inspection — verified by running the substitution.
- **Type-blind fallback bugfix (typeblind atran columns):** `_is_already_posted_typeblind` at `bank_import.py:1530-1602` correctly uses `t.at_entry`, `t.at_acnt`, `t.at_value`, `t.at_pstdate`, `t.at_type`, `a.ae_acnt`, `a.ae_cbtype`, `a.ae_entry` and joins atran→aentry. The 2026-05-04 column-name regression has been corrected.
- **Period-bound matcher** (`opera_sql_import.py:8385-8410`): correctly applies `period_start - grace`/`period_end + grace` window when bounds supplied; falls back with WARNING when not. Both unreconciled and already-reconciled passes use the same window. Behaviour matches `docs/superpowers/specs/2026-05-03-matcher-period-bound-design.md`.
- **Customer/supplier dormant filter** (`bank_import.py:811, 814, 862, 865`): `sn_dormant = 0` and `pn_dormant = 0` applied per CLAUDE.md mandate.
- **Sequential gating spec adherence** (SE path): `derive_statement_state` correctly returns `'imported'` for has_import_record + not reconciled, the next-statement gate at `routes.py:6772-6776` correctly accepts `imported` priors, and `_build_imported_pending_closings` correctly chains forward without requiring Stage-4 completion. (See F13 for one nit.)
- **PDF extraction cache singleton reset on company switch** (`api/main.py:253-257`): `reset_extraction_cache()` and other singletons are correctly reset when `_ensure_company_context` detects a company change. Preview-from-pdf overrides the cache contents at `routes.py:3653-3673` after the balance-chain validation, so corrupted-balance cache entries from the 2026-05-04 incident do get healed on re-preview.
- **Defer audit DB**: `DeferredTransactionsDB` uses parameterised SQLite queries throughout; the `count_for_statement` filter correctly applies the period bounds when supplied.
- **Bank statement import audit DB writes**: `email_storage.record_bank_statement_import` uses bind parameters (per CLAUDE.md SQLite parameterisation rule), no f-string risk in tracking-DB writes.
- **NOLOCK on SE reads in scope:** `apps/bank_reconcile/api/routes.py:6045-6051, 6664, 5534`, `sql_rag/bank_import.py:810, 865, 1054-1055, 1136, 1170, 1198`, `sql_rag/duplicate_check_se.py:39-46, 69, 95`, `sql_rag/opera_sql_import.py:8405` — all consistent.
- **Period-reconciliation self-heal at scan time** (`routes.py:6077-6099`, `12222-12245`): correctly read-only against Opera; only writes to local SQLite. Errors swallowed but logged at WARNING (good).
- **Sequential statement gating spec compliance** for the documented `state ∈ ('imported','reconciled')` next-statement gate — verified `routes.py:6762-6777` (SE) and `12300-12315` (Opera 3, modulo F2 disabling it).
