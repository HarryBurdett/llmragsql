# Suppliers Workflow Audit Findings

**Date:** 2026-05-05
**Scope:** Supplier statement reconciliation, aged-creditor, contacts, onboarding, remittance, automation settings. SE + Opera 3 parity.

## Summary

The Suppliers app is functional in happy-path SE flows, but it is NOT production-ready. Two systemic gaps dominate: (1) Opera 3 has effectively no supplier-statement reconciliation pipeline at all — `get_supplier_data_provider()` is hard-wired to return an SE provider with a TODO comment, and `routes.py` (the main file, 5,015 lines, 70+ endpoints) has zero Opera 3 mirrors; (2) per-supplier and global automation settings (`never_communicate`, `auto_respond`, `require_approval_above`, `statements_contact_position`, `verified` flag on approved senders, `onboarding_require_bank_verify`) are stored and exposed in the UI but never enforced at email-send time, so any operator click will dispatch mail regardless of policy. Additionally there are correctness bugs (NameError in three Opera 3 aged-creditor endpoints; AttributeError on Opera 3 contact-delete; dedup typo `conn_d3.close()`), pervasive `intsys@wimbledoncloud.net` hardcoding, ~80 `str(e)` leaks of raw DB errors to clients, the bank-detail-change scan having no scheduler, and ~50+ f-string SQL sites that are both an injection vector and (in routes.py creditors endpoints) a NOLOCK violation.

## Findings

### CRITICAL

#### F1: Opera 3 supplier-data provider does not exist — entire reconciliation pipeline is SE-only
**Where:** `sql_rag/supplier_data_provider.py:141-146`; `apps/suppliers/api/background.py:121,298,587`; `apps/suppliers/api/routes.py:2832-2837`
**Symptom:** Any customer on Opera 3 cannot reconcile supplier statements. The auto-processor returns "No SQL connector available" at `background.py:292-295` for Opera 3 installs, and every operator-driven reconciliation endpoint will fail similarly because the provider tries to run T-SQL through `sql_connector` (None for Opera 3).
**Cause:** `get_supplier_data_provider()` ignores `current_company`/`active_system_id` and unconditionally returns `OperaSESupplierDataProvider(sql_connector)` with the comment `# For now, always return Opera SE. Opera 3 implementation is TODO.` There is no `supplier_data_opera3.py`. `apps/suppliers/api/routes.py` (5,015 lines, 70+ endpoints) has zero `/api/opera3/...` supplier-statement / supplier-config / supplier-security / supplier-directory / supplier-queries / supplier-communications endpoints.
**Impact:** Direct violation of CLAUDE.md "Opera 3 FULL PARITY (MANDATORY)". Opera 3 customers receive a degraded product they were sold; the auto-process pipeline silently no-ops on every received PDF. README.md and module docs claim "Works with both Opera SQL SE and Opera 3" — that is false.
**Repro:** Static. Switch a company to Opera 3, send a supplier statement PDF — auto-processor logs "No SQL connector available" and parks at `received`.
**Fix sketch:** Implement `Opera3SupplierDataProvider` against `Opera3Reader` (pname/ptran/zcontacts/pterms read methods). Make `get_supplier_data_provider()` choose by `current_company['system_type']`. Add `/api/opera3/...` mirrors for the supplier-statement core endpoints. Background processor must accept a per-company provider and call the right one (it currently runs once per sync cycle on a thread with no company context — see F2).

---

#### F2: Auto-processor singleton DB + post-sync callback runs without per-company context — cross-company writes possible
**Where:** `api/main.py:715-718` (registration); `apps/suppliers/api/background.py:42,123,210` (`get_supplier_statement_db()`/`get_current_db_path('supplier_statements.db')`); `sql_rag/supplier_statement_db.py:980-988` (singleton)
**Symptom:** `auto_process_supplier_statements` is registered as a single post-sync callback and the periodic sync loop runs in a thread with no contextvar. `get_supplier_statement_db()` returns a singleton bound to whichever company called `_resolve_db_path()` first; statements arriving for company B can land in company A's `supplier_statements.db` if the company switched between sync cycles and the singleton has not been reset. PDFs are also written to `<supplier_statements.db.parent>/pdfs` resolved at call time — same race.
**Cause:** Same root cause as cross-cutting F4 (single shared `EmailSyncManager`). The supplier auto-processor compounds it because (a) it has its own module-level singleton (`_db_instance`) and (b) it does its own `get_current_db_path` calls inside the callback rather than capturing a per-company context at registration time.
**Impact:** Multi-tenant data leak risk for the supplier statement store and the PDF cache. CLAUDE.md "Company independence (ABSOLUTE RULE)" violation.
**Repro:** Static; matches cross-cutting F4 mechanism.
**Fix sketch:** Either (a) one supplier auto-processor per company (matching the per-company `EmailSyncManager` proposal), or (b) capture `(company_id, db_path)` at the start of every callback invocation and refuse to reuse the singleton. Singleton reset in `_ensure_company_context` (line 250) only protects HTTP request paths, not the periodic-sync thread.

---

#### F3: `auto_respond` and `never_communicate` per-supplier flags are NEVER enforced on the operator-driven send paths
**Where:** Stored in `supplier_config` (sql_rag/supplier_statement_db.py:299-301) and updatable via `PUT /api/supplier-config/{account}` (routes.py:129-164). Read only in `apps/suppliers/api/background.py:519-543`. NOT checked in:
- `POST /api/supplier-statements/{statement_id}/approve` (routes.py:2306-2455)
- `POST /api/supplier-statements/{statement_id}/acknowledge` (routes.py:2458-2620)
- `POST /api/supplier-statements/queue/bulk-approve` (routes.py:2673-2801)
- `POST /api/supplier-statements/{statement_id}/send-updated-status` (routes.py:1227-1363)
- `POST /api/supplier-remittance/{account}/send` (routes_remittance.py:437-694)
- `POST /api/supplier-queries/{query_id}/send-reminder` (routes.py:878+)
- `POST /api/supplier-statements/process-email/{email_id}` (routes.py:3284-3500) which sends an acknowledgement straight away with hardcoded `intsys@wimbledoncloud.net` From and no flag check.
**Symptom:** A supplier flagged `never_communicate=1` in the UI will still receive emails when an operator clicks Approve, Acknowledge, Send Reminder, Send Remittance, or runs Bulk Approve.
**Cause:** Gating logic was added to the auto-pipeline only. The operator path was never harmonised.
**Impact:** Direct violation of CLAUDE.md "every outbound supplier email checks `auto_respond` AND `never_communicate`". Real risk of regulatory/reputation damage (e.g. legal hold, factoring, sensitive supplier).
**Repro:** Set `never_communicate=1` for a supplier via `PUT /api/supplier-config/{account}`, then click Approve on any pending statement for that supplier. Email is sent.
**Fix sketch:** Centralise outbound checks into one helper `_can_send_to(supplier_code) -> (bool, reason)` that loads supplier_config + global toggles + verified-sender check, and call it at the head of every outbound endpoint. Reject with a clear 409 when blocked.

---

#### F4: `require_approval_above` threshold is configured but never enforced — auto-pipeline can send queries without approval regardless of variance
**Where:** Default registered at `sql_rag/supplier_statement_db.py:382`. **Zero callers.** No grep hit anywhere outside the default-config registration.
**Symptom:** A supplier statement with a £50,000 variance goes out automatically if `auto_respond_with_queries='true'`, even though the user-visible setting promises "Require manual approval for responses with variance above this amount".
**Cause:** Setting was added to the schema/UI but the gate logic was never written.
**Impact:** Money-system safety control is dead. Customers see the toggle, believe protection exists, click Send/Auto-process, and dispatch unapproved high-variance responses.
**Repro:** Set `require_approval_above=100`, send a synthetic statement with variance £500. Auto-pipeline sends despite the threshold.
**Fix sketch:** In `_send_acknowledgement`/the response-decision block at `apps/suppliers/api/background.py:517-543`, check `abs(recon_result.difference) > require_approval_above` and route to status `awaiting_approval` instead of `acknowledged`/auto-send. Mirror the gate in the operator-driven approve path (or just hard-block the Approve button for over-threshold variances unless explicitly overridden).

---

#### F5: Bank-detail change scan has no scheduler — security alerts only fire if a user manually clicks the button
**Where:** `apps/suppliers/api/routes.py:1605-1790` is `POST /api/supplier-security/scan-changes`. No call site exists in the codebase outside the route definition (grep `scan_supplier_changes\|scan-changes` returns only the route and the spec doc).
**Symptom:** A fraudster who silently changes `pname.pn_bankac`/`pn_banksor` (e.g., via direct DB edit, social engineering, or a compromised Opera client) will not trigger an alert. The detection logic exists but is never invoked. Suppliers with changed bank details will be paid via the new (potentially fraudulent) account on the next BACS run.
**Cause:** Implementation-plan step 1 was completed (the endpoint), step 2 (background scheduler / call into `_ensure_company_context` / call after each sync cycle) was not.
**Impact:** Direct violation of CLAUDE.md "Bank-detail change alerts: when a supplier's bank details change, the security_alert_recipients setting must fire — fraud prevention". This is the showcase fraud-prevention feature in the marketing material.
**Repro:** Change `pn_bankac` for any supplier in Opera. Wait. Nothing happens. The dashboard's `security_alerts` list (routes.py:316-330) only shows alerts that the manual scan has already inserted into `supplier_change_audit`.
**Fix sketch:** Register `scan_supplier_changes` as a periodic job (e.g. once per hour) per company, and additionally call it from the post-sync callback. Add an Opera 3 mirror because the current SQL is SE-only.

---

#### F6: `scan_supplier_changes` (bank-detail audit) is SE-only — Opera 3 customers have NO bank-change detection
**Where:** `apps/suppliers/api/routes.py:1628-1636` issues raw T-SQL against `pname WITH (NOLOCK)`. No `/api/opera3/supplier-security/scan-changes` exists.
**Symptom:** Opera 3 customers cannot detect bank-detail changes at all. The fraud-prevention feature is unavailable on FoxPro.
**Cause:** Same parity gap as F1 — Opera 3 mirror was never written.
**Impact:** Direct CLAUDE.md "Opera 3 FULL PARITY" violation on a security-critical feature.
**Repro:** Static. Search `/api/opera3/supplier-security` — no matches.
**Fix sketch:** Build an Opera 3 mirror that reads `pname` via `Opera3Reader` and shares the SQLite-side audit comparison logic.

---

#### F7: Opera 3 aged-creditor endpoints have a NameError — `balance` is referenced before assignment in three loops
**Where:** `apps/suppliers/api/routes_aged.py:530-538` (summary), `:625-632` (trend), `:730-738` (detail). All three loops do:
```
trtype = _o3_get_str(rec, "pt_trtype")
if balance == 0:                         # <-- NameError, balance not yet defined
    continue
balance = _o3_get_num(rec, "pt_trbal")
if balance == 0:
    continue
```
**Symptom:** Every Opera 3 aged-creditors call raises `NameError: name 'balance' is not defined` on the very first ptran record. All three Opera 3 endpoints are broken.
**Cause:** Code copy-paste error during Opera 3 mirror write — the `if balance == 0` line is mis-positioned (was probably meant to be `if not trtype: continue` or simply removed when `balance` was defined later). All three endpoints share the bug.
**Impact:** Opera 3 users see a 500 error on Aged Creditors. Direct violation of CLAUDE.md "Opera 3 FULL PARITY". The fact this has never been reported suggests no Opera 3 customer has used the page yet.
**Repro:** Static. Trace the variable flow at any of the three line ranges.
**Fix sketch:** Delete the leading `if balance == 0` block at lines 532, 627, 732, OR move the `balance = _o3_get_num(...)` assignment above it. Add a regression test that exercises the Opera 3 branch with a non-empty fixture.

---

#### F8: Opera 3 contact-delete calls a method that does not exist — `delete_contact_by_zcontact_id` is not on `SupplierStatementDB`
**Where:** `apps/suppliers/api/routes_contacts.py:1072` calls `db.delete_contact_by_zcontact_id(str(contact_id))`. `sql_rag/supplier_statement_db.py` only defines `delete_contact(self, contact_id: int)` (line 891).
**Symptom:** Every Opera 3 supplier-contact-delete invocation raises `AttributeError: 'SupplierStatementDB' object has no attribute 'delete_contact_by_zcontact_id'`. Caught by the generic `except Exception` and returned to the user as `{"success": False, "error": "<the AttributeError>"}` (raw `str(e)` leak — see F12).
**Cause:** Method was renamed/never written but the caller wasn't updated.
**Impact:** Opera 3 contact deletion silently fails with a confusing error to the user.
**Repro:** Static. `grep -n "delete_contact_by_zcontact_id" sql_rag/supplier_statement_db.py` returns nothing.
**Fix sketch:** Either implement `delete_contact_by_zcontact_id` (delete from `supplier_contacts_ext` where `zcontact_id = ?`) or change the caller to `db.delete_contact(int(contact_id))` if the IDs match.

---

#### F9: `_check_remittance_already_sent` is defined but never called — same remittance can be sent twice
**Where:** `apps/suppliers/api/routes_remittance.py:173-182` defines `_check_remittance_already_sent(supplier_code, payment_ref)`. Nothing calls it.
**Symptom:** Calling `POST /api/supplier-remittance/{account}/send` twice with the same `payment_ref` will send the email twice and create two `supplier_remittance_log` rows. There is no de-dup at the SQLite layer either (no `UNIQUE(supplier_code, payment_ref)` constraint).
**Cause:** Helper was written and forgotten.
**Impact:** Duplicate remittances → supplier confusion, audit-trail noise. Idempotency is a CLAUDE.md hard rule.
**Repro:** `POST /api/supplier-remittance/ABC/send {payment_ref: 'P12345'}` twice. Second call succeeds and sends another email.
**Fix sketch:** Call `_check_remittance_already_sent` at the head of `send_remittance` and return 409. Also add `UNIQUE(supplier_code, payment_ref)` to `supplier_remittance_log` as a belt-and-braces guard.

---

#### F10: `verified` flag on `supplier_approved_emails` is set but never checked — any added sender bypasses verification
**Where:** Schema column `supplier_statement_db.py:170`. Add endpoint `apps/suppliers/api/routes.py:1973-1977` always inserts with `verified=0`. The verification gate at `apps/suppliers/api/background.py:354-363` queries the table without filtering on `verified=1`:
```
"SELECT email_address FROM supplier_approved_emails WHERE supplier_code = ?"
```
**Symptom:** An admin who adds an unverified sender immediately allows that sender to bypass the unverified-sender quarantine. No phone-verification step is required.
**Cause:** The flag was added to the schema as a security feature but the matching `WHERE verified = 1` filter never made it into the verification path.
**Impact:** Direct violation of `onboarding_require_bank_verify` spirit — and of the broader fraud-prevention story (anyone with `add_approved_sender` access can effectively fast-track an attacker's email).
**Repro:** Add a sender via `POST /api/supplier-security/approved-senders` (verified is hard-coded to 0). Send a statement from that address. Background.py treats it as verified.
**Fix sketch:** Either change the SQL to `AND verified = 1` (and add a UI for the verification step) or remove the flag entirely and document that all add-events imply verification (less safe).

---

#### F11: `statements_contact_position` per-supplier setting is dead — never read at email-send time
**Where:** Stored at `sql_rag/supplier_config.py:78`, exposed via `PUT /api/supplier-config/{account}` (`routes.py:148`). Never read by `_get_supplier_contact_email` (routes.py:2808-2855), `_get_contact_email` (routes_remittance.py:137-170), or anywhere else.
**Symptom:** A supplier configured to "send statements to the AP Manager" still receives emails to the first contact in `zcontacts` (the Opera default) or `pname.pn_email`.
**Cause:** Setting was added to the schema/UI as part of the supplier-config UI; the consumer code was never wired up.
**Impact:** Operator UX promise broken — one of the four supported per-supplier flags has no effect. Statements can land in the wrong inbox at the supplier end.
**Repro:** Set `statements_contact_position='Accounts Manager'`. Send a statement response. Recipient is the first zcontacts row regardless.
**Fix sketch:** Inside `_get_supplier_contact_email`, if `supplier_config.statements_contact_position` is set, query `zcontacts WHERE zc_account = ? AND zc_module = 'P' AND UPPER(RTRIM(zc_pos)) = UPPER(?)` first and prefer that contact's email. Same hook in `_get_contact_email` for remittances.

---

#### F12: Pervasive `str(e)` raw error leakage to clients — bypasses `friendly_db_error` rule
**Where:** ~80 sites across `apps/suppliers/api/`. Counts: `routes.py` 55, `routes_contacts.py` 17, `routes_onboarding.py` 9, `routes_aged.py` 6, `routes_remittance.py` 7. Plus `routes.py:1093` `email_error = f"Email send error: {str(e)}"` and several similar.
**Symptom:** Raw pyodbc error strings (full SQL with table names, file paths, server names, stack-trace fragments) are returned to the browser via `{"success": False, "error": str(e)}`. The CLAUDE.md "Never expose raw database errors" rule mandates `friendly_db_error()` instead.
**Cause:** Pattern was copied across the file before the rule was introduced; never refactored.
**Impact:** Information disclosure (server name, schema, paths), poor UX, and inconsistent with the rest of the app.
**Repro:** Trigger any DB error (e.g. take Opera DB offline mid-call). UI shows raw "ODBC Driver 17 ..." text.
**Fix sketch:** Wrap every `return {"success": False, "error": str(e)}` site with `friendly_db_error(e)` for DB errors and a sanitised generic message for everything else. Remove `email_error` raw exposure.

---

#### F13: `intsys@wimbledoncloud.net` hardcoded in 7 send paths — single-tenant lock-in, breaks white-label
**Where:**
- `apps/suppliers/api/background.py:618` (fallback when config missing)
- `apps/suppliers/api/routes.py:1084` (send-reminder)
- `apps/suppliers/api/routes.py:1769` (security alert email)
- `apps/suppliers/api/routes.py:2390` (approve)
- `apps/suppliers/api/routes.py:2564` (acknowledge)
- `apps/suppliers/api/routes.py:2755` (bulk-approve)
- `apps/suppliers/api/routes.py:3455` (process-email pipeline)
**Symptom:** Every customer sends supplier emails as `intsys@wimbledoncloud.net` regardless of their company config. Other companies cannot reply (the address is Intsys's own SMTP relay) and DMARC will fail at the supplier end for any well-configured supplier.
**Cause:** Quick-fix during demo period; never moved to per-company config.
**Impact:** Direct violation of "NEVER hardcode... or any company-specific values". Hard blocker for shipping the product to a second customer.
**Repro:** `grep -n "intsys@wimbledoncloud.net" apps/suppliers/api/`.
**Fix sketch:** Read from `current_company['email_settings']['from_email']` (already exists for other modules) or from `supplier_automation_config['from_email']`, with a clear fail-closed error if not set. The remittance path (routes_remittance.py:600-609) already does this correctly — generalise that helper.

---

#### F14: f-string SQL on `pname`/`ptran` with URL-path interpolation and missing NOLOCK
**Where:** ~50 sites in `apps/suppliers/api/routes.py` and friends. Notable injection points (URL path → SQL):
- `:505,980,1277,1451,2272,2363,2536,2725` — `WHERE pn_account = '{supplier_code}'`
- `:2164` — `WHERE pn_name LIKE '%{search}%' OR pn_account LIKE '%{search}%'` from `?search=` query parameter
- `:3610,3832` — `WHERE RTRIM(pt_account) = '{stmt['supplier_code']}'`
- Missing NOLOCK at `routes.py:4358,4454,4510,4564,4594` (creditors detail/transactions/statement endpoints).
**Symptom:** SQL injection vector via authenticated user. NOLOCK violation in five creditors endpoints that interactively scan `pname`/`ptran`.
**Cause:** Pattern pre-dates parameter-binding and locking-discipline rollouts.
**Impact:** Same as cross-cutting F5/F6 but specific to the supplier app surface.
**Repro:** Static. `grep -nE "WHERE.*=\s*'\{" apps/suppliers/api/routes.py`.
**Fix sketch:** Adopt parameterised queries using the connector's `params=` form (the onboarding file `routes_onboarding.py:53,73` already does this — there's the template). For the creditors endpoints, add `WITH (NOLOCK)`.

---

#### F15: `pn_dormant` filter is missing on every Opera read in the suppliers app
**Where:** Across `apps/suppliers/api/routes.py`, `routes_aged.py`, `routes_onboarding.py`, `routes_contacts.py`, `routes_remittance.py`. Only `supplier_data_opera_se.py:71-95` and `supplier_config.py:181` apply the filter (and only for the bulk `get_all_suppliers` query). Specific gaps:
- `routes.py:1628-1636` (scan-changes) reads ALL of pname including dormants
- `routes.py:2143-2228` (supplier-directory) reads ALL of pname
- `routes.py:4356-4421` (creditors-report) reads pname without the filter
- `routes_aged.py:130-135` (aged-creditors) joins ptran→pname without filter
- `routes_onboarding.py:351` (detect-new) reads ALL of pname including dormants → creates onboarding records for dormants
**Symptom:** Dormant suppliers appear in supplier directory, aged-creditors lists, onboarding queue, and trigger bank-detail-change alerts even though they are no longer transactable. Statements reaching us for a dormant supplier are processed and an email is sent.
**Cause:** The filter was added to one helper and never propagated.
**Impact:** Direct violation of CLAUDE.md "Dormant accounts excluded — `pn_dormant = 0` filter on every... query. Cannot post to dormant accounts." Operational noise and possible accidental communication with archived suppliers.
**Repro:** Mark a supplier dormant in Opera; observe it still appearing on `/api/supplier-directory` and the aged-creditors page.
**Fix sketch:** Add `AND (pn_dormant = 0 OR pn_dormant IS NULL)` to every `pname` read in the supplier app. Same on the Opera 3 side once F1 lands.

---

### SHOULD-FIX

#### F16: Concurrent same-supplier-same-date statement race — `create_statement` doesn't catch `IntegrityError`
**Where:** `sql_rag/supplier_statement_db.py:454-482`. Unique index defined at `:312-315` covers `(supplier_code, statement_date) WHERE supplier_code != 'UNKNOWN'`.
**Symptom:** Two concurrent emails for the same supplier+date both pass the read-then-write dedup check (`background.py:244-256, 314-329`) and one fails the unique index. The exception bubbles up unhandled, the email is still marked as processed (the `processed_emails` row), but the user sees an error in the UI on the next list call.
**Cause:** TOCTOU + no `INSERT OR IGNORE` / try/except inside `create_statement`. The unique index works as a safety net but the error path is messy.
**Impact:** Operator-visible noise; not a data-corruption issue thanks to the unique index.
**Repro:** Send the same PDF twice in quick succession (same email forwarded by two different recipients into the shared inbox).
**Fix sketch:** Wrap `create_statement` insert in try/except `sqlite3.IntegrityError`, fetch the existing row, return its id. Add an integration test covering two parallel emails.

---

#### F17: Dedup typo `conn_d3.close()` in unreachable except path
**Where:** `apps/suppliers/api/background.py:331`. `conn_d3` is undefined; the only nearby connection is `conn_dd`.
**Symptom:** If the `c_dd.fetchone()` block at line 324 returns truthy AND the subsequent DELETE statements raise (very rare), the cleanup path itself raises `NameError` and gets swallowed by the outer `except Exception: pass` at line 332. Connection leak under that race.
**Cause:** Typo during refactor.
**Impact:** SQLite connection-leak risk under unlikely conditions; not a correctness issue under normal use.
**Repro:** Static.
**Fix sketch:** `conn_d3` → `conn_dd` (or restructure into a `with` block that auto-closes).

---

#### F18: Ref-only matching does not match the documented "date+ref / date+amount / amount+date" matrix
**Where:** `sql_rag/supplier_reconciler.py` (the active reconciler) is reference-only. The CLAUDE.md context for this audit (and `apps/suppliers/docs/spec.md`) describes "Items match by date+ref, date+amount, ref-only, amount+date".
**Symptom:** Statements where the supplier truncates / mangles the reference (common with HSBC/Adobe-style invoice numbers) but the date+amount uniquely identify the transaction get classified as `theirs_only` and converted to a query.
**Cause:** Spec/implementation drift. The richer matching matrix was documented but never implemented.
**Impact:** Higher false-positive query rate; manual operator effort.
**Repro:** Construct a statement with reference `INV/0010` while Opera has `INV-0010`. Reconciler will not match.
**Fix sketch:** Either update the spec to "ref-only" (honest) or extend `reconcile()` to take optional `(date, abs_amount)` per item and try a date+amount fallback when ref-only fails.

---

#### F19: `_get_supplier_contact_email` priority order favours Opera-default over local override
**Where:** `apps/suppliers/api/routes.py:2808-2855`. Priority 1 = `provider.get_supplier_contact()` (Opera zcontacts top row). Priority 2 = `supplier_contacts_ext WHERE is_statement_contact = 1`.
**Symptom:** Operator sets a local override saying "send statements to alice@" but emails still go to the Opera-default `bob@` because Priority 1 (Opera) fires first.
**Cause:** Order is wrong — local overrides should win.
**Impact:** UI promise broken; user sets the override and observes nothing changes.
**Repro:** Set `is_statement_contact=1` for a different contact than the first zcontacts row. Statement responses still go to the first zcontacts row.
**Fix sketch:** Swap Priority 1 and Priority 2. Also use `statements_contact_position` (F11) as the *zcontacts* selector instead of "first row".

---

#### F20: `_run_migrations` (routes.py:39) and `add_statement_lines` use unbounded ALTER TABLE / INSERT loops without WAL or busy_timeout
**Where:** `sql_rag/supplier_statement_db.py:64-451` runs ALTER TABLE migrations on every `__init__`. SQLite default `busy_timeout=0` will raise `OperationalError: database is locked` if two callers (HTTP request + auto-processor) hit it simultaneously during a fresh-install company switch.
**Symptom:** Intermittent "database is locked" on the first request after a company switch with auto-process active. The `__init__` is called from many entry points (every `SupplierStatementDB()` direct instantiation in routes_contacts.py — 12 sites — runs `_init_db` again).
**Cause:** No `PRAGMA busy_timeout` + no `journal_mode=WAL` + idempotent schema work runs every time.
**Impact:** Flaky cold-start.
**Fix sketch:** Set `PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000` in `_get_connection()`. Move the schema migration to a startup hook so it runs once, not per-instantiation.

---

#### F21: Process-email pipeline (routes.py:3284) uses deprecated `SupplierStatementReconciler` and bypasses gating
**Where:** `apps/suppliers/api/routes.py:3293-3500`. Imports `from sql_rag.supplier_statement_reconcile import SupplierStatementReconciler` which the file itself flags `DEPRECATED` at line 1. Sends acknowledgement directly with hardcoded From, no `never_communicate`/`auto_respond` gate, no idempotency check.
**Symptom:** A separate code path that does the same job as `auto_process_supplier_statements` but without the gating, dedup, or supplier-config respect that the auto-pipeline has.
**Cause:** Older endpoint kept around for manual UI invocation; not migrated.
**Impact:** Same statement can be re-processed (no `source_email_id` check), bypassing `never_communicate`.
**Fix sketch:** Either remove the endpoint and call `_process_single_email` from background.py, or refactor the latter into a shared helper that both call.

---

#### F22: `process_supplier_statement` (routes.py:3773) uses LIKE substring match on `pt_trref` — false positives
**Where:** `apps/suppliers/api/routes.py:3849-3852`:
```
ref_matches = ptran_df[
    (ptran_df['pt_trref'].str.contains(line['reference'], case=False, na=False)) |
    (ptran_df['pt_supref'].str.contains(line['reference'], case=False, na=False))
]
```
**Symptom:** `INV-001` will match `INV-0010`, `INV-0011`, `INV-0012`. First match wins, marks the line `matched`, and the statement is silently mis-reconciled.
**Cause:** Quick implementation; no boundary check.
**Impact:** Wrong reconciliation results — one of the more dangerous quiet failure modes for a finance system.
**Fix sketch:** Use exact match (`==`) on `clean_reference()` (the helper from `supplier_reconciler.py`) — the active auto-pipeline already gets this right.

---

#### F23: `onboarding_require_bank_verify` setting + `verify_bank` endpoint do not actually verify anything
**Where:** Setting registered at `sql_rag/supplier_statement_db.py:406`. Endpoint `apps/suppliers/api/routes_onboarding.py:219-254` only flips a flag — no phone verification, no two-factor, nothing tied to actual bank-detail comparison.
**Symptom:** "Bank verified" is a click-to-tick. No remittance or payment endpoint refuses to act if the supplier is unverified.
**Cause:** The verification *workflow* was never built; just the schema.
**Impact:** Marketing claim "Onboarding bank verification" is fictional. Real fraud risk: an attacker who hijacks an unverified supplier still gets paid.
**Fix sketch:** Either remove the feature from the UI (honest) or build a phone-confirmation step + a server-side gate at remittance/BACS-export time that fails closed if `bank_verified=0`.

---

#### F24: Naive `datetime.now()` everywhere — timezone-blind processing-SLA and reminder calculations
**Where:** `apps/suppliers/api/background.py:32`, every `datetime.now()` site. `query_response_days`, `follow_up_reminder_days`, `processing_sla_hours` are all measured against naive system clock.
**Symptom:** A server in UTC and an operator in BST will disagree about "is this overdue?" by an hour. SQLite `julianday('now')` (used in dashboard) is UTC; Python `datetime.now()` is local. Mixed comparisons in `routes.py:282-290` (overdue queries).
**Cause:** Convention drift; nobody settled on UTC across the app.
**Impact:** Edge-case incorrect "overdue" flags around midnight.
**Fix sketch:** Standardise on UTC: `datetime.utcnow()` everywhere, and use `datetime('now')` (UTC) in SQLite where comparing.

---

#### F25: `friendly_db_error` is never imported into the suppliers app
**Where:** `apps/suppliers/api/routes.py` — no `from api.main import friendly_db_error`.
**Symptom:** Even if the team agreed to wrap errors, the helper is not in scope.
**Fix sketch:** Import it once at the top of each routes file (or move to `api/errors.py` to avoid the import cycle).

---

### COSMETIC

#### F26: 12 endpoints in `routes_contacts.py` use `SupplierStatementDB()` direct constructor instead of `get_supplier_statement_db()`
**Where:** `apps/suppliers/api/routes_contacts.py` — every endpoint instantiates `SupplierStatementDB()` (no args) on each call. Re-runs the full `_init_db` (incl. ALTER TABLE migrations) per request.
**Impact:** Slower per-request overhead; relates to F20 above.
**Fix sketch:** Use the singleton `get_supplier_statement_db()` (already used elsewhere). Path resolution is identical so behaviour is preserved.

---

#### F27: `apps/suppliers/api/background.py:231-235` hardcodes a UK bank-name skip list
**Where:**
```
bank_names = ['barclays', 'natwest', 'hsbc', 'lloyds', 'santander', 'nationwide',
              'metro bank', 'starling', 'monzo', 'revolut', 'tide', 'virgin money']
```
**Symptom:** Bank statements from any UK bank not in this list (e.g. Cynergy, Allica, ClearBank) get processed as supplier statements. The list is also region-specific (UK only).
**Fix sketch:** Replace with a positive heuristic ("does the document have an account number in the IBAN/sort-code+account format?") or a config-driven list.

---

#### F28: `processed_emails` table created at runtime in callback (background.py:51-55)
**Where:** `apps/suppliers/api/background.py:50-55` `CREATE TABLE IF NOT EXISTS processed_emails ...` is run on every sync cycle. Should be in `_init_db` of `SupplierStatementDB`.
**Impact:** Mild — once per sync interval. Cleaner schema if hoisted.
**Fix sketch:** Move into `_init_db`.

---

#### F29: Subject template hardcoded English in `acknowledge` and `process-email` paths despite configurable templates
**Where:** `apps/suppliers/api/routes.py:2550` `f"Statement Received - {supplier_name} - {statement_date}"`; `:3431`. The `email_template_subject_*` config keys exist but are not used for these paths.
**Fix sketch:** Use `_generate_subject` from elsewhere in the file.

---

#### F30: Deprecated `supplier_statement_reconcile.py` module is still imported by routes.py:3293
**Where:** Module's own docstring says "DEPRECATED... Do NOT add new callers". Still has callers.
**Fix sketch:** Extract `find_supplier()` into `supplier_lookup.py` (as the docstring proposes), update callers, delete the deprecated module.

---

## Confirmed-good areas

- **Per-company DB path resolution for `SupplierStatementDB`**: `_resolve_db_path()` (sql_rag/supplier_statement_db.py:46-56) correctly delegates to `get_current_db_path('supplier_statements.db')`. The singleton reset on company switch via `reset_supplier_statement_db()` is wired correctly in `_ensure_company_context` (api/main.py:250).
- **NOLOCK on every read in `supplier_data_opera_se.py`**: every f-string includes `WITH (NOLOCK)`. The escape via `replace("'", "''")` is also applied to every account-code parameter — partial mitigation for the injection point (still f-string SQL, but at least quoted).
- **Math correctness of the active reconciler**: `sql_rag/supplier_reconciler.py` has an explicit `math_checks_out` invariant `theirs_only_net - ours_only_net + amount_diffs_net == difference` and is well-structured. The auto-pipeline aborts (`reconciliation_error` status) if the invariant fails — a strong correctness backstop.
- **Open-items / `pt_remove` non-issue**: confirmed via `scripts/opera_snapshot.json` that `pt_remove` does not exist in `ptran`, so the SE bank-rec open-items rule does not apply to supplier reconciliation. The reference-only join over `pt_trbal <> 0` is appropriate.
- **Unique index `idx_statements_supplier_date`**: correctly uses partial-index syntax (`WHERE supplier_code != 'UNKNOWN'`) to allow many UNKNOWN-supplier rows while preventing accidental duplicates for matched ones.
- **Sender quarantine path**: when a sender isn't on Opera contacts and isn't in the approved-emails table, `background.py:367-373` parks the statement at `unverified_sender` and does NOT process — good. (The `verified` flag bypass is the issue — F10 — not the quarantine logic itself.)
- **`pterms` payment-terms resolution**: account-override → terms-profile → 30 fallback at `supplier_data_opera_se.py:459-506` is the correct three-tier lookup.
- **Test-mode redirect**: `_get_supplier_contact_email` (routes.py:2818-2829) honours `test_mode_email` and logs the redirect — useful for safe development.
- **Aged-creditors SE summary uses `phist`/`pparm` correctly**: `routes_aged.py:101-114` reads `pparm.pp_period`/`pp_percday` for the aging configuration rather than hardcoding 30/60/90, matching Opera's own report semantics.
