# GoCardless Workflow Audit Findings

**Date:** 2026-05-05
**Scope:** GoCardless email scan, preview, import, posting, mandate/subscription sync, fees handling. SE + Opera 3 parity.

## Summary

The Opera SE GoCardless implementation is largely complete, but has several CRITICAL gaps before production: a NameError bug that will crash every Opera 3 GoCardless batch with fees, no idempotency check at the import endpoints (double-clicking can double-post), Opera 3 import endpoint missing parameters and safety checks that SE has (mandate verification, currency validation, import lock, vat_on_fees), unvalidated `data_path` query param on every Opera 3 endpoint (cross-tenant data path bypass), and 81 raw `str(e)` leaks. Live GoCardless API calls default to `sandbox=False` even though MEMORY.md says "DO NOT make live API requests". VAT tracking and basic posting logic look correct on the SE side; auto-allocation logic is conservative. Most issues are concentrated in the Opera 3 path.

## Findings

### CRITICAL

#### F1: Opera 3 GC batch with fees throws NameError on every import
**Where:** `sql_rag/opera3_foxpro_import.py:4005-4006`
**Symptom:** Any Opera 3 GoCardless batch where `gocardless_fees > 0` will throw `NameError: name 'tables_updated' is not defined` mid-transaction (the FoxPro tables/balances ABOVE that point will already have been written, the writes after will not). The import returns `success=False` but Opera is left in a partially-posted state — aentry, atran, stran, ntran for each customer will exist, plus the fees ntran lines and VAT zvtran/nvat, but the fees aentry/atran will be incomplete and `tables_updated` summary in the response will never be built.
**Cause:** Lines 4005-4006 call `tables_updated.add('aentry')` / `tables_updated.add('atran')` inside the fees block, but `tables_updated` is first assigned (as a list, line 4104) only AFTER the fees block returns. Looks like a refactor leftover where the variable was previously a set defined earlier.
**Impact:** 100% failure on any Opera 3 customer that pays fees — i.e. every realistic batch. Partial posting risk on Opera 3.
**Repro:** Call `/api/opera3/gocardless/import` (or `/api/opera3/gocardless/import-from-email`) with `gocardless_fees=2.50`. The exception will fire after the customer payments are written and the fees ntran/aentry are written, leaving the cashbook entry without a complete atran for fees.
**Fix sketch:** Remove the two `tables_updated.add(...)` calls (or initialise `tables_updated` as a set at the top of the `try` and convert to list at the end).

#### F2: No idempotency check at `/api/gocardless/import` — double-click double-posts
**Where:** `apps/gocardless/api/routes.py:581-878` (SE), `apps/gocardless/api/routes.py:3506-3675` (Opera 3), `apps/gocardless/api/routes.py:3195-3429` (SE import-from-email), `apps/gocardless/api/routes.py:5874-6076` (Opera 3 import-from-email)
**Symptom:** Posting the same payout twice will post twice. The user-facing scan-emails / api-payouts endpoints filter out already-imported payouts, but the actual import endpoints do not.
**Cause:** `email_storage.is_gocardless_payout_imported(payout_id)` and `is_gocardless_reference_imported(bank_reference)` exist (`api/email/storage.py:1204, 1229`) and are called on listing pages (line 1958, 1961, 6117) but NOT before posting in any of the four import endpoints. A double-click, retry-after-timeout, frontend bug, or race between two browser tabs all double-post.
**Impact:** Duplicate sales receipts in Opera; double customer balance reduction; control account drift; bank balance overstated; eventual reconciliation failure. Real-money mistake.
**Repro:** Hit `/api/gocardless/import` twice with the same `payout_id` and `reference` — both post.
**Fix sketch:** Before acquiring the import lock, call `email_storage.is_gocardless_payout_imported(payout_id, target_system)` (and `is_gocardless_reference_imported(reference, ...)` as a fallback). If True, return `{"success": False, "error": "Payout already imported"}`.

#### F3: Opera 3 import endpoint skips mandate→customer verification (SE blocks; O3 doesn’t)
**Where:** `apps/gocardless/api/routes.py:3506-3675` (Opera 3 `/api/opera3/gocardless/import`), and `apps/gocardless/api/routes.py:3195-3429` (SE `/api/gocardless/import-from-email`)
**Symptom:** SE `/api/gocardless/import` enforces (lines 644-674): if a payment carries `mandate_id`, the importing account MUST equal the account the mandate is linked to in `gocardless_mandates`, else BLOCK. Opera 3 import has no such check. SE `import-from-email` also doesn’t do it (the validated_payments at line 3257 carry mandate_id but the BLOCKED check is absent).
**Cause:** Safety check copied to one endpoint only; never propagated to the others.
**Impact:** Posting a GoCardless receipt to the wrong customer is the worst possible class of bug — you’ve clobbered customer A’s balance with customer B’s payment, and B is now showing as paid even though they’re not. It corrupts AR aging, statements, and customer trust. The mandate check is the only bulletproof guardrail.
**Repro:** Submit a payment to `/api/opera3/gocardless/import` with `mandate_id=MD000XXX` (linked to A046 in `gocardless_mandates`) but `customer_account=A123`. SE rejects; Opera 3 silently posts to A123.
**Fix sketch:** Lift the mandate verification block (lines 644-674) into a helper and call it from all four import endpoints.

#### F4: Opera 3 import endpoint missing critical parameters (currency, vat_on_fees, fees_vat_code, auto_allocate, import lock)
**Where:** `apps/gocardless/api/routes.py:3506-3675`
**Symptom:** Compared with `/api/gocardless/import` (SE), the Opera 3 endpoint:
- Does NOT accept `vat_on_fees` query param → VAT on fees can never be split correctly.
- Does NOT accept `fees_vat_code` → defaults inside `import_gocardless_batch` to '2' but never plumbed.
- Does NOT accept `currency` → cannot reject foreign-currency batches.
- Does NOT accept `auto_allocate` → frontend cannot toggle.
- Does NOT call `acquire_import_lock(...)` (SE does at line 770) → concurrent Opera 3 imports on same bank race each other.
**Cause:** Endpoint built as a stub that wasn’t kept in sync with SE.
**Impact:** VAT returns wrong on Opera 3 GC fees (no zvtran/nvat split possible because vat_on_fees=0 always). Foreign-currency Opera 3 imports silently post as GBP. Concurrent imports can collide on FoxPro file locks and produce partial states. Mandatory parity rule (CLAUDE.md "Opera 3 Full Parity") is violated.
**Repro:** Compare query parameters in `/api/gocardless/import` (line 581-599) vs `/api/opera3/gocardless/import` (3506-3522). Try a EUR payout against Opera 3 — silently accepted.
**Fix sketch:** Add the missing query params, plumb them through to `import_gocardless_batch`, add company-scoped `acquire_import_lock` (matching SE’s `_bank_lock_key` pattern), and invoke the F3 mandate-verification helper.

#### F5: Opera 3 `import_gocardless_batch` accepts `currency` param but never validates it
**Where:** `sql_rag/opera3_foxpro_import.py:3141-3204` (and the entire body)
**Symptom:** SE rejects foreign-currency batches (`opera_sql_import.py:6079-6090` — compares to `get_home_currency()`). Opera 3 has the parameter `currency: str = None` (line 3141) but no use of it anywhere in the method body.
**Cause:** Param added for signature parity but logic never written.
**Impact:** Posting a EUR payout into Opera 3 would post EUR amounts as GBP — every receipt is incorrect. Customer balances wrong. Reconciliation impossible.
**Repro:** Call `import_gocardless_batch(currency="EUR", ...)` on Opera 3 — proceeds.
**Fix sketch:** Mirror the SE check: `if currency and currency.upper() != home_currency['code'].upper(): return ImportResult(success=False, errors=[...])`. Use `Opera3Config` to read home currency.

#### F6: Live GoCardless API enabled by default (`sandbox=False`)
**Where:** `apps/gocardless/api/routes.py:1907, 4413, 4605, 5147, 5582, 5660, 6272, 6490, 6700, 6895, 7084, 8172, 8343, 8391, 8911, 9027, 9070, 9113, 9140, 9167, 9196` (all 22 client instantiations)
**Symptom:** `sandbox = settings.get("api_sandbox", False)` — default is False, i.e. live.
**Cause:** Default value chosen incorrectly given MEMORY.md’s explicit instruction: "GoCardless: DO NOT make live API requests — test environment. Use sandbox mode or mock responses only."
**Impact:** Any merchant who saves their access token without explicitly toggling Sandbox mode will hit the live API. Calls like `request_gocardless_payment` (line 8057) collect real money. Mandate/subscription cancels are irreversible. This violates the project safety rule.
**Repro:** Save a token without toggling Sandbox; call `/api/gocardless/api-payouts` — hits api.gocardless.com.
**Fix sketch:** Default `sandbox` to True until the merchant explicitly opts in via Settings, OR add a hard guard that requires `settings.get('api_environment_confirmed') == 'live'`. Document the default behaviour in the manual.

#### F7: SE import `/api/gocardless/import-from-email` does NOT pass `payout_id` → import_history can’t use payout_id dedup
**Where:** `apps/gocardless/api/routes.py:3195-3429`
**Symptom:** This endpoint records a history row (line 3359) but never receives or stores `payout_id`. The history-based "is this payout already imported" check (`is_gocardless_payout_imported`) cannot match, so a subsequent import via `/api/gocardless/import` with the same payout_id will dedup only by `bank_reference` — and if the reference differs (e.g. user retyped it), it double-posts.
**Cause:** Endpoint built before payout_id became the canonical idempotency key.
**Impact:** Weakened idempotency on the email path. Combined with F2, the dedup story is fragile.
**Repro:** Import via email (no payout_id stored). Then call `/api/gocardless/import` with payout_id from the same batch — `is_gocardless_payout_imported` returns False, so it proceeds.
**Fix sketch:** Add `payout_id: str = Query(None)` query param, parse it from the GoCardless email body if available (the API matches on reference anyway), and pass to `record_gocardless_import`. Also matches Opera 3 endpoint at line 3518 which already takes payout_id.

#### F8: Unvalidated `data_path` query param on every Opera 3 endpoint
**Where:** `apps/gocardless/api/routes.py:3508, 3885, 3920, 3952, 4033, 4082, 4109, 4276, 4338, 4401, 4587, 4708, 4835, 5090, 5224, 5263, 5422, 5513, 5622, 5687, 5876, 7056, 7066, 7072, 7181, 7204, 7350, …` (all O3 endpoints)
**Symptom:** Every Opera 3 endpoint accepts `data_path: str = Query(...)` directly from the client and uses it to build `Opera3Reader(data_path)` and `get_opera3_writer(data_path)`. There is no allowlist, no comparison against the active company’s configured data path, no path-traversal sanitisation.
**Cause:** Originally a developer convenience; never tightened for multi-tenant deployment.
**Impact:** A logged-in user for company A can read or post GoCardless receipts into company B’s Opera 3 data simply by supplying B’s path. Cross-tenant data leak / tenant takeover. With Opera 3 file shares often UNC-mounted, the path could even point to an arbitrary network share.
**Repro:** Authenticate as company A; POST `/api/opera3/gocardless/import?data_path=\\\\company-b-server\\opera3\\...` — request proceeds.
**Fix sketch:** Resolve data_path from session-bound company config, NOT from query param. Either drop the param entirely and look it up server-side, or validate the supplied path matches the active company’s configured `opera3_data_path`.

#### F9: SE & O3 customer queries lack `sn_dormant = 0` filter (CLAUDE.md mandates dormant exclusion)
**Where:** `apps/gocardless/api/routes.py:311-314` (`_match_gocardless_payments_helper`), `apps/gocardless/api/routes.py:6297-6301` (mandates/sync), `apps/gocardless/api/routes.py:7393-7407` (eligible-customers), `apps/gocardless/api/routes.py:7467-7473` (suggest-match), `apps/gocardless/api/routes.py:4436-4445` (Opera 3 mandate sync)
**Symptom:** None of the customer-side queries filter `sn_dormant = 0`. A dormant customer can be matched and chosen as the import target. The bank-rec module does filter (`CLAUDE.md` says "Dormant accounts excluded from matching"), but GoCardless was not updated.
**Cause:** Missed when the dormant-exclusion rule was added.
**Impact:** Either (a) the import to Opera will fail downstream because Opera rejects posting to dormant accounts, leaving the user confused; or (b) it succeeds in a way that revives a dormant account silently. Both are bugs.
**Repro:** Make a customer dormant in Opera. Run match — they still come back as a candidate. Import — fails or revives.
**Fix sketch:** Add `AND (sn_dormant = 0 OR sn_dormant IS NULL)` to every sname query in this module, and the equivalent attribute filter for Opera 3 (`_o3_get_int(r, 'sn_dormant') != 1`).

### SHOULD-FIX

#### F10: Opera 3 lock key not company-scoped
**Where:** `apps/gocardless/api/routes.py:5982`
**Symptom:** `lock_key = f"opera3_{posting_bank}"` — no company prefix. SE uses `_bank_lock_key()` which prepends `company_id`.
**Cause:** Inconsistent helper usage.
**Impact:** Two Opera 3 companies that happen to share the same `posting_bank` code (e.g. `BC010`) cross-block each other’s GoCardless imports.
**Repro:** Configure two Opera 3 companies, both with bank code `BC010`, fire imports concurrently — one blocks the other unnecessarily.
**Fix sketch:** Use `_bank_lock_key(posting_bank)` consistently or build an explicit `f"{company_id}:opera3:{posting_bank}"` key.

#### F11: 81 raw `str(e)` leaks across the GoCardless routes (CLAUDE.md "Never expose raw database errors")
**Where:** `apps/gocardless/api/routes.py` — 81 occurrences of `return {"success": False, "error": str(e)}` vs only 4 uses of `friendly_db_error`. Examples: lines 233, 535, 1268, 1392, 1670, 2349, 3059, 3088, 3429, 3503, 3675, 5871, 6076, 6462, 7449, 8345, 9201.
**Symptom:** Raw pyodbc / SQLAlchemy / FoxPro error text leaks to the user — including server names, paths, query fragments.
**Cause:** Boilerplate exception handling never updated to use `friendly_db_error`.
**Impact:** Information disclosure (server topology, schema details), poor UX, debug-style strings shown in production.
**Repro:** Cause any Opera DB error during a GoCardless flow — raw pyodbc trace bubbles to JSON response.
**Fix sketch:** Replace the `str(e)` returns with `friendly_db_error(e)` (already imported as a per-request global in this module).

#### F12: SE/O3 `nt_posttyp` divergence on cashbook receipts ('S' vs 'R')
**Where:** `sql_rag/opera_sql_import.py:6417, 6443` (both 'S'); `sql_rag/opera3_foxpro_import.py:3475, 3517` (both 'R'). Single-receipt path has the same divergence (`opera_sql_import.py:2359` 'S' vs `opera3_foxpro_import.py:1831` 'R').
**Symptom:** Same business event posts a different `nt_posttyp` value depending on which Opera variant the customer is on.
**Cause:** Long-standing inconsistency. Snapshot at `scripts/opera_snapshot.json` shows real Opera receipts often use 'A' (line 638115+), so neither value matches Opera’s native posting.
**Impact:** Difficult to merge / compare nominal data across customer estates. May affect downstream Opera reports that rely on `nt_posttyp` for filtering. Not financially incorrect but conceptually wrong.
**Repro:** Compare `ntran` rows produced by SE vs Opera 3 for an identical GoCardless batch.
**Fix sketch:** Decide the canonical value (likely 'A' per snapshot — verify against Opera’s SDK docs) and align both paths.

#### F13: Reference / customer_name strings interpolated unescaped in many SQL inserts
**Where:** `sql_rag/opera_sql_import.py:6289, 6342, 6366, 6394, 6442, 6474, 6489, 6655, 6691, 6720, 6750, 6776, 6793, 6810, 6833, 6854` and similar; `apps/gocardless/api/routes.py:8131-8136` (opera_account interpolated with only `replace("'", "''")`)
**Symptom:** `'{reference[:20]}'`, `'{customer_account}'`, `'{customer_name…}'` etc. inserted into f-string SQL. Customer name uses `replace("'", "''")` (partial defence). `reference` is not escaped at all (the import endpoint accepts it via `reference: str = Query("GoCardless")` — user-controllable).
**Cause:** Style choice: dynamic SQL with f-strings rather than parameterised queries.
**Impact:** A reference containing an apostrophe will break the INSERT. Worse, on writers that don’t cleanse, anything passed via the Body/query (reference, fees_nominal_account, vat_nominal_account, customer_account when not yet validated) becomes a SQL-injection vector. Most callers happen to be other validated paths, so the practical risk is mostly breakage rather than exploitation, but it’s fragile.
**Repro:** Call `/api/gocardless/import?reference=GC's-test` — likely 500 / SQL syntax error.
**Fix sketch:** Switch to bound parameters (`text(SQL).bindparams(...)`) or at minimum pass every interpolated value through a single `_sql_quote()` helper.

#### F14: `is_gocardless_reference_imported` keyed on user-supplied `reference` (not GC bank_reference)
**Where:** `apps/gocardless/api/routes.py:812, 3362, 3644, 6025` (all four import endpoints record `bank_reference=reference` where `reference` is the user-supplied query param, defaulting to `"GoCardless"`)
**Symptom:** Every import where the user accepts the default reference value ends up storing `bank_reference="GoCardless"` in the history table. Because the dedup helper does `SELECT … WHERE bank_reference = ?`, only the FIRST batch imported with the default ref actually gets remembered for dedup; every subsequent batch with the same default ref gets correctly dedup-matched but for the wrong reason (different payout, same bogus key).
**Cause:** The user-supplied "reference" query arg got conflated with GoCardless’s real bank-statement reference (e.g. `INTSYSUKLTD-ABC123`). They should be separate columns.
**Impact:** Either (a) false-negative — users see the same payout offered repeatedly because the imports library can’t recognise it, or (b) false-positive — the second distinct payout-with-default-ref gets blocked. The dedup story relies on `bank_reference` being a unique payout ID, which it isn’t when defaulted.
**Repro:** Two distinct payouts both imported with `reference="GoCardless"`. The second one’s `is_gocardless_reference_imported("GoCardless")` returns True, blocking it (or worse, allowing it depending on path).
**Fix sketch:** Always store the GC bank_reference (e.g. `INTSYSUKLTD-XYZ`) parsed from the email/API payout, NOT the user-supplied display reference. Query string param can be renamed `display_reference` or similar.

#### F15: Email scan filters by subject keywords — narrow and brittle
**Where:** `apps/gocardless/api/routes.py:2762, 5756`
**Symptom:** `if not any(keyword in subject for keyword in ['payout', 'payment', 'collected', 'paid']): continue` — silently drops any email whose subject doesn’t match. New GoCardless email templates (e.g. "Your weekly summary", "Your funds are on the way") would be skipped.
**Cause:** Hardcoded subject-keyword whitelist.
**Impact:** Missed payouts; system reports "no GoCardless emails found" when there are unprocessed ones.
**Repro:** Receive a payout notification with a non-matching subject template (which has happened historically).
**Fix sketch:** Either remove the subject filter (fall back to body content sniffing — `parse_gocardless_email` already returns no payments if it can’t parse) or move the keyword list to settings.

#### F16: Hardcoded `'NP'` / `'GoCardless'` fallback fees cbtype
**Where:** `sql_rag/opera_sql_import.py:6639` (SE), `sql_rag/opera3_foxpro_import.py:3814, 3819` (Opera 3)
**Symptom:** If `fees_payment_type` is unset and the lookup query returns nothing, the code falls back to literal `'NP'` (or sometimes uses the at iter to find any non-batched 'P' — but still defaults `'NP'`).
**Cause:** Hardcoded company-specific value (CLAUDE.md "No hardcoded values").
**Impact:** Companies that don’t have an `NP` cbtype in their `atype` table will get cryptic INSERT failures. Per CLAUDE.md, this kind of hardcoding is forbidden.
**Repro:** Set up a company without an NP atype and try to import GC fees with `fees_payment_type` unset.
**Fix sketch:** If lookup fails, return an explicit error asking the user to configure a fees payment type, rather than guessing.

#### F17: Email scan duplicate-detection check is amount-only (not invoice-specific)
**Where:** `apps/gocardless/api/routes.py:2839-2852` (`Check 2: NET amount in cashbook`), `2863-2887` (`Check 3: GROSS amount`), `2894-2929` (`Check 3b: batched entries`), `2935-2952` (`Check 3c: individual payment amounts`), `2956-2976` (`Check 4: FEES amount`)
**Symptom:** Five increasingly fuzzy heuristics try to find duplicates by amount and reference. If two unrelated payouts have the same gross amount within 14 days, they’ll be flagged as duplicates.
**Cause:** Bolted-on layers of fallback dedup logic.
**Impact:** False-positive duplicate warnings shown to the user, eroding trust in the workflow. If they ignore the warning and import, no harm done; if they trust the warning, they skip a legitimate payout.
**Repro:** Two genuine payouts of identical gross amount within 14 days.
**Fix sketch:** Promote payout_id and bank_reference to the canonical idempotency key (see F2 / F14) and downgrade these heuristics to mere advisory warnings.

#### F18: `nt_posttyp = 'T'` for bank transfer not used in GC payout transfer
**Where:** `sql_rag/opera_sql_import.py:6952-6975` invokes `import_bank_transfer` for the GC→destination transfer.
**Observation:** Could not verify in this audit whether `import_bank_transfer` itself uses `'T'` correctly, but the GoCardless caller’s use looks correct (it delegates to the bank-transfer importer). Worth a follow-up audit of `import_bank_transfer` to ensure it independently meets the posting checklist.
**Impact:** Out of scope for this audit but flagged for follow-up.
**Fix sketch:** Audit `import_bank_transfer` separately.

#### F19: `record_gocardless_import` failures swallowed — silent state drift
**Where:** `apps/gocardless/api/routes.py:824, 3372, 3656, 6035` (every import path)
**Symptom:** Each import path wraps `email_storage.record_gocardless_import(...)` in `try/except` and only logs a warning. If recording fails (disk full, locked SQLite, schema mismatch), Opera ledger is updated but local idempotency store is not, so the user can re-import the same batch.
**Cause:** Defensive coding that prefers continuing the response over surfacing the failure.
**Impact:** Silent failure of the dedup mechanism. Combined with F2, this is the most likely path to a duplicate posting in production.
**Repro:** Make `gocardless_imports.db` read-only; import — Opera updated, history not, double-posting now possible.
**Fix sketch:** If `record_gocardless_import` raises, return success=True with a `warning` key but ALSO surface a hard error / require user acknowledgement. Or, ideally, do the history insert in the same DB transaction window (move it into the deadlock-retry block before commit).

#### F20: Match helper uses fuzzy `name_contains` matching that can grab the wrong customer
**Where:** `apps/gocardless/api/routes.py:374-398`
**Symptom:** Priority-3 / Priority-4 fall back to `if norm_name in stored_name or stored_name in norm_name`. Two short or generic customer names ("Smith Ltd" and "John Smith Ltd") will collide; the first iteration wins.
**Cause:** Fuzzy contains-match without a tie-breaker.
**Impact:** Wrong customer posted in cases where mandate_id is missing AND multiple Opera customers have overlapping names. With mandate verification (F3), this is mitigated for SE main-import only — but partial.
**Fix sketch:** Require a minimum length, prefer exact match, and warn / prompt the user when a contains-match is the only result. The existing duplicate-customer-amount warning catches some of these but not all.

#### F21: Subscription/repeat-document linkages don’t cancel cleanly when mandate is cancelled
**Where:** `apps/gocardless/api/routes.py:6603-6660` (`cancel_gocardless_mandate`), `apps/gocardless/api/routes.py:8596-8643` (`link_subscription_to_document`/unlink)
**Observation:** Cancelling a mandate hits the live API but does not cascade to gocardless_subscriptions (link table). A subsequent `sync_gocardless_subscriptions` would catch it, but in the meantime the local view is inconsistent.
**Impact:** UI may show a "live" subscription pointing at a cancelled mandate.
**Fix sketch:** On mandate cancel, mark associated subscriptions inactive in `gocardless_subscriptions` immediately.

### COSMETIC

#### F22: Some sname/atype queries miss `WITH (NOLOCK)` (CLAUDE.md mandates NOLOCK on reads)
**Where:** `apps/gocardless/api/routes.py:890-895` (`get_gocardless_batch_types` — `FROM atype` no NOLOCK), `apps/gocardless/api/routes.py:7467-7473` (`suggest_mandate_match` — `FROM sname` no NOLOCK), `apps/gocardless/api/routes.py:6297-6301` (`mandates/sync` — `FROM sname` no NOLOCK), `apps/gocardless/api/routes.py:7385-7408` (`eligible-customers` — `FROM sname` no NOLOCK)
**Symptom:** Read-side queries can briefly block writers in Opera SE.
**Impact:** Minor; could cause occasional 30-second waits during a backup/lock.
**Fix sketch:** Add `WITH (NOLOCK)` to all read queries.

#### F23: `parse_gocardless_email` year-boundary heuristic for payment date is fragile
**Where:** `sql_rag/gocardless_parser.py:233-236, 250-251`
**Symptom:** When the email contains a month name without a year, the parser guesses: "if month is in future, use previous year". Right at year boundaries (Dec emails received in Jan) this can pick the wrong year by one.
**Impact:** Posting date off by one year; period-validation will block, surfaced as confusing UX.
**Fix sketch:** Prefer the email’s `received_at` date to disambiguate, fall back to the heuristic only if needed.

#### F24: `parse_gocardless_email` does not handle multi-currency line items
**Where:** `sql_rag/gocardless_parser.py:348` regex captures GBP/EUR/USD/CAD/AUD but stores all amounts in a single `amount` field. If a payout mixes EUR and GBP transactions (rare but possible with FX), the parser silently aggregates them.
**Impact:** Wrong gross_amount for genuinely mixed-currency payouts.
**Fix sketch:** Track per-payment currency in `GoCardlessPayment` dataclass; reject mixed batches at the parser stage.

#### F25: Hardcoded archive folder default `"Archive/GoCardless"` 
**Where:** `apps/gocardless/api/routes.py:3208, 5889`
**Symptom:** Folder name baked into endpoint signature.
**Impact:** Can be overridden by client, but the default is a hardcoded literal — minor CLAUDE.md "no hardcoded values" smell.
**Fix sketch:** Pull default from settings (`gocardless_archive_folder`).

#### F26: Inconsistent input_by string `"GOCARDLS"` (8 chars exactly — magic constant)
**Where:** Across the module — every import passes `input_by="GOCARDLS"`. Length matches the at_inputby field width but is not configurable.
**Impact:** Cosmetic; user audit trail always shows GOCARDLS.
**Fix sketch:** Pull from settings or include the actual logged-in user (perhaps as `<user>:GC` truncated to 8).

#### F27: `parse_gocardless_email` uses `datetime.now()` for year disambiguation — non-deterministic for tests/replay
**Where:** `sql_rag/gocardless_parser.py:234-236, 249-251`
**Impact:** Reparsing an old email at a later date may yield a different inferred year.
**Fix sketch:** Inject a "now" / "received_at" parameter into the parser.

## Confirmed-good areas

- **Mandate→customer verification on `/api/gocardless/import`** (SE only) — lines 644-674 are robust; correctly BLOCKS on mismatch.
- **VAT tracking on fees in SE** — both `zvtran` (line 6823) and `nvat` (line 6847) created with `nv_vattype='P'` (correct purchase/input VAT), VAT rate looked up from `ztax` not hardcoded.
- **Period validation** — both endpoints call `validate_posting_period(... 'SL')` before posting.
- **Sequence allocation** — uses `_get_next_id`, `_get_next_journal(count=N)`, `increment_atype_entry` (no MAX+1 patterns in the GC batch posting).
- **Auto-allocation logic** — conservative: never allocates on amount-only matches to individual invoices; respects payment-request invoice list and "clears whole account" rules; salloc created at allocation time, not at posting (per CLAUDE.md "Opera Allocation Rules"). Backfills `gocardless_customer_id` after name-match for future precision.
- **Duplicate-customer-in-batch warning** — line 6195 surfaces matched warnings.
- **Currency check on SE side** — `import_gocardless_batch` (SE) rejects foreign currency at lines 6079-6090.
- **Period decision routing** — `posting_decision.post_to_nominal` / `post_to_transfer_file` correctly gates ntran vs anoml.
- **Bank balance updates** — `update_nbank_balance` and `update_nacnt_balance` invoked after each ntran insert, including fees.
- **Per-company GoCardless settings** — `_load_gocardless_settings` resolves via `get_company_db_path` (line 949). `reset_payments_db` is correctly called on company switch (`api/main.py:259`).
- **Import history schema** — `gocardless_imports` table has `payout_id`, `bank_reference`, `target_system` columns and the helpers (`is_gocardless_payout_imported`, etc.) work — they’re just not invoked from the import endpoints (F2).
- **Bank-level import lock on SE** — `acquire_import_lock(_bank_lock_key(posting_bank))` correctly used at three SE endpoints.
- **Self-heal post-commit verification** — `verify_balance_after_import`, `verify_nominal_balances`, `verify_ledger_after_import` invoked after the GC batch posts (lines 6880-6893).
- **Auto-transfer to destination bank** — net amount transfer post-batch (line 6952) handles GC Control bank → real bank flow.
- **No salloc at posting time** — explicit comment in code (line 6382), correctly defers to `auto_allocate_receipt`.
- **PDF/OCR/parser** — small, regex-based, no external network calls; resilient to missing fields (calculated_gross fallback, line 396).
