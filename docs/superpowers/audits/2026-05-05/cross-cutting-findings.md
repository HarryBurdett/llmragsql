# Cross-Cutting Audit Findings

**Date:** 2026-05-05
**Scope:** Multi-company isolation, SE/Opera 3 parity, locking discipline, hardcoding, KB coverage, error handling, code rot, test gaps.

## Summary

Recent rec-correctness work (open-items, self-heal, completion contract, period-bound matcher, type-blind fallback) is well-tested and well-documented on the SE side. The biggest cross-cutting risks are (1) Opera 3 missing several SE-only correctness fixes (type-blind fallback, period-bound matcher, open-items rule applied consistently), creating divergent behaviour the CLAUDE.md "full parity" rule explicitly forbids; (2) per-request multi-company state held in module globals (sql_connector, email_storage, current_company) instead of a contextvar/dependency, with a continuously-running periodic email sync that mutates these globals across companies; and (3) pervasive f-string SQL with raw URL-path interpolation — both a SQL-injection vector (auth-protected, but still wrong) and a NOLOCK-discipline violation in `apps/bank_reconcile/api/routes.py` for nine read sites.

## Findings

### CRITICAL

#### F1: Opera 3 missing the type-blind already-posted fallback (real-money double-post risk)
**Where:** `sql_rag/bank_import_opera3.py:1058-1092` vs `sql_rag/bank_import.py:1455-1605`
**Symptom:** Opera 3 users may double-post a transaction that Opera already holds under a different `at_type` than the matcher infers. Mirror of the "Cloudsis BB005 HISCOX" example documented in `bank_import.py:1526-1527`.
**Cause:** SE's `_is_already_posted` falls through to `_is_already_posted_typeblind` when (a) `txn.action` is missing/`'skip'` and (b) the type-aware `check_for_duplicate` returns no match. Opera 3's `_is_already_posted` returns `(False, "")` in both cases — no fallback exists.
**Impact:** Direct violation of CLAUDE.md mandatory parity rule. Opera 3 customers can silently double-post. Note also commit `c123efc` ("typeblind fallback applies open-items rule") only patched the SE file.
**Repro:** Static. Compare the two files.
**Fix sketch:** Port `_is_already_posted_typeblind` to `bank_import_opera3.py`, using `Opera3Reader` to scan `aentry`/`atran` with the same date-tolerance + signed-amount + open-items-rule filter. Add a parallel `tests/test_already_posted_fallback_o3.py`.

---

#### F2: Opera 3 `match-statement` endpoint violates open-items rule and period-bound contract
**Where:** `apps/bank_reconcile/api/routes.py:15575-15582` (Opera 3) vs `sql_rag/opera_sql_import.py:8333-8410` (SE)
**Symptom:** Opera 3 candidate pool for matching a new statement includes (a) entries flagged `ae_remove = True` (correction-pair-matched) and (b) entries from any historical period — both of which can pair against an unrelated statement line.
**Cause:** SE imports `OPEN_FOR_REC_SQL` (line 8401) and applies a period-bound window (lines 8385-8398). Opera 3 inline-filters only `ae_reclnum > 0` and `ae_complet == 0` (lines 15579-15582) — the `ae_remove` field is never checked, no period bounds.
**Impact:** Violates the rules from `business-rules/bank-rec-open-items.md` and `business-rules/matcher-period-bound.md`. The exact failure mode the £198 P Flannery regression was meant to prevent — except for Opera 3.
**Repro:** Static. Diff the two endpoints.
**Fix sketch:** Use `from sql_rag.opera_open_items import is_open_for_rec` for the in-memory filter (no SQL fragment available for FoxPro). Add a `period_start`/`period_end`/`period_grace_days` parameter to the endpoint and apply the same window. Add a parity regression alongside `tests/test_match_statement_to_cashbook.py`.

---

#### F3: Opera 3 `unreconciled-difference` queries omit the `ae_remove` filter
**Where:** `sql_rag/opera3_data_provider.py:1561-1568`, `sql_rag/statement_reconcile_opera3.py:858-863` (and SE counterpart `sql_rag/statement_reconcile.py:540` which has the same bug)
**Symptom:** `ae_reclnum = 0` alone selects entries the operator has linked into a correction pair — those should NOT contribute to the unreconciled total. Both ledgers can show inflated unreconciled difference for legitimate correction-paired entries.
**Cause:** All three sites pre-date the `opera_open_items.py` SSOT. None of them route through it. The bank-rec heal regression test (`test_flannery_regression.py`) only covers candidate-pool, not balance-derivation.
**Impact:** Violates the open-items rule for balance computation. SE/O3 same-bug parity, so users see consistent (wrong) numbers — but a future correction-pair scenario will surface a phantom non-zero difference.
**Repro:** Static. `grep ae_reclnum sql_rag/statement_reconcile.py sql_rag/statement_reconcile_opera3.py sql_rag/opera3_data_provider.py | grep -v ae_remove`.
**Fix sketch:** Replace the inline filters with a call to `is_open_for_rec()` (Python, both Opera 3 sites) or append `AND e.ae_remove = 0` (SE site). Update `tests/test_flannery_regression.py` to also cover the unreconciled-difference path.

---

#### F4: Multi-company periodic email sync mutates a single shared `EmailSyncManager`; race during company switch can write company-A emails to company-B storage
**Where:** `api/main.py:213-231` (mutates `email_sync_manager.storage`/`.providers`); `api/email/sync.py:115-275` (sync_provider reads `self.storage`/`self.providers` at lines 123, 153, 156, 165, 217, etc.); `api/main.py:717` starts `start_periodic_sync(interval_minutes=5)`
**Symptom:** Cross-company data leak. Company A's IMAP messages land in Company B's `email_data.db` if `_ensure_company_context(B)` swaps the manager's `.storage` while a sync started under A is mid-flight. Same for `auto_process_supplier_statements` callback (`apps/suppliers/api/background.py`).
**Cause:** A single global `email_sync_manager` is started once with the default-company storage. Per-request middleware swaps `email_sync_manager.storage` and `.providers` in place (lines 215, 218). The 5-minute periodic loop runs in a thread-pool executor (`api/email/sync.py:93`), independent of any HTTP request's contextvar.
**Impact:** CRITICAL — direct violation of the company-isolation rule. The "single uvicorn worker" comment in `_ensure_company_context` at line 168 ignores this thread-pool path. Even with one worker, an HTTP request and a background sync run concurrently.
**Repro:** Static. Inspect lines listed above.
**Fix sketch:** Either (1) keep one `EmailSyncManager` per company (`_company_email_sync_managers` dict already declared at line 148 but not populated) and iterate them in the periodic loop, or (2) capture a local snapshot of `(storage, providers, company_id)` at the start of `_sync_loop`/`sync_provider` and never reach back through `self`. Either way, the sync must NOT read mutating globals.

---

#### F5: f-string SQL with URL-path interpolation across the bank-rec routes — SQL injection vector + NOLOCK gaps
**Where:** `apps/bank_reconcile/api/routes.py` (multiple sites: lines 167, 297-298, 332-334, 357-358, 371, 391, 411-412, 472-473, 489-490, 955, 971-983, 1131, 1189, 1211, 1331, 3548, 3571, 5307, 5333, 5409, 10733, 14425, ...); also `sql_rag/opera_sql_import.py` 50+ sites under `WHERE ... = '{...}'`.
**Symptom:** A logged-in user can send `bank_code='; DROP TABLE atran--` via path parameter and the SQL is built without parameterisation. Auth gate exists but does not prevent injection by an authorised user, contractor, or attacker who steals a session token.
**Cause:** Pervasive use of `f"... WHERE col = '{var}'"` instead of bound parameters. `sql_connector.execute_query` does support parameter binding but route code never uses it.
**Impact:** Injection risk in production deployment; also drives the NOLOCK gap (see F6) since the same f-strings are missing `WITH (NOLOCK)`.
**Repro:** Static. `grep -nE "WHERE.*'\{" apps/bank_reconcile/api/routes.py | wc -l` → 100+ matches.
**Fix sketch:** Adopt parameterised queries everywhere user input flows into SQL. As a quick mitigation, add a strict whitelist validator in the route signature (e.g., `bank_code: str = Path(..., regex=r'^[A-Z0-9]{3,8}$')`).

---

#### F6: Nine route-level Opera reads missing `WITH (NOLOCK)` (locking-discipline violation)
**Where:** `apps/bank_reconcile/api/routes.py:295-411` and 472-490. Specific lines:
- `:297` `FROM nbank` (no NOLOCK)
- `:316` `FROM ntran`
- `:332` `FROM atran`
- `:357` `FROM atran`
- `:381` `FROM ntran`
- `:390` `FROM nacnt`
- `:411` `FROM ntran`
- `:472` `FROM anoml`
- `:489` `FROM anoml`
**Symptom:** Reader can be blocked by a concurrent Opera-desktop user holding row/page locks; or worse, take exclusive locks of its own that interfere with Opera writers.
**Cause:** The `/api/reconcile/bank/{bank_code}` endpoint pre-dates the locking-discipline rollout. New code (`sql_rag/bank_rec_heal.py`, `sql_rag/duplicate_check_se.py`, `sql_rag/opera_sql_import.py`) does this correctly.
**Impact:** Direct violation of CLAUDE.md "Every SELECT against an Opera table has WITH (NOLOCK)" rule. Same path is used by the Balance Check page so it's hot.
**Repro:** Static. Run `grep -nE "FROM\s+(nbank|atran|aentry|stran|ptran|ntran|sname|pname|nacnt)\b" apps/bank_reconcile/api/routes.py | grep -v NOLOCK`.
**Fix sketch:** Add `WITH (NOLOCK)` to the nine SELECT sites. (Routine, mechanical change. Also a great candidate for a CI lint rule.)

---

#### F7: NOLOCK reads inside write transactions on aentry — race on rec-batch validation
**Where:** `sql_rag/opera_sql_import.py:7823-7827`, `:7856-7861`, plus 90 other NOLOCK reads inside `with self.sql.engine.connect() as conn: trans = conn.begin()` blocks.
**Symptom:** Two concurrent reconciliations on the same bank can both pass the "is this entry already reconciled?" validation (line 7858 `aentry WITH (NOLOCK) ... ae_reclnum`) and then both update the same entry, double-counting in `nk_recbal`.
**Cause:** NOLOCK = uncommitted/dirty read. When a SELECT inside a transaction will be followed by an UPDATE on the same rows, the correct hint is `UPDLOCK, ROWLOCK` (which the file already uses correctly at lines 247, 812, 1028, 1055, 1414 for sequence allocation) — not NOLOCK.
**Impact:** Real risk for multi-user scenarios. The `import_lock` SQLite gate prevents two SQL RAG imports running concurrently for the same bank, but it does NOT prevent an Opera-desktop user reconciling at the same time.
**Repro:** Static; `grep -B 2 "WITH (NOLOCK)" sql_rag/opera_sql_import.py | grep -A 2 "trans.begin"` finds many candidates.
**Fix sketch:** For SELECTs that drive an UPDATE in the same transaction, use `WITH (UPDLOCK, ROWLOCK)`. For SELECTs that are purely informational (e.g. logging), keep NOLOCK. Audit the 92 sites and reclassify.

---

#### F8: Hardcoded `/Users/maccb/Downloads` path in production code
**Where:** `sql_rag/file_archive.py:19` `DOWNLOADS_BASE = Path("/Users/maccb/Downloads")`; `api/main.py:7669`; `api/main.py:105` (log file).
**Symptom:** On any non-Mac, any non-`maccb` user, the bank statement scan/archive flow silently writes to a non-existent path. The fallback in `get_configurable_base_folder()` returns this path when the per-company `bank_statements_base_folder` setting is missing.
**Cause:** Developer-machine assumption baked in.
**Impact:** Direct violation of "NEVER hardcode account codes, company IDs, bank names, skip patterns, or any company-specific values" — extends to user paths. Production deploys will scan an empty/non-existent folder; statement archive will fail.
**Repro:** Static.
**Fix sketch:** Replace `DOWNLOADS_BASE` with a value from `company_settings.json` (already supported in `get_configurable_base_folder`) and surface a clear error if not set, instead of silently falling back. Apply same change to `api/main.py:7669`. Move logger file path to a config option.

---

### SHOULD-FIX

#### F9: 42 SE bank-rec endpoints have no Opera 3 mirror (selected critical paths listed)
**Where:** `apps/bank_reconcile/api/routes.py` — see full list in audit transcript. Notable ones:
- `/api/bank-import/import-with-overrides` (line 4763) — manual override import
- `/api/bank-import/import-from-email` (line 9358)
- `/api/bank-import/persist-decisions` (line 3264)
- `/api/bank-import/duplicate-override` (line 2819)
- `/api/reconcile/bank/{bank_code}/orphan-tmpstat` (line 1115) — and clear-orphan-tmpstat
- `/api/reconcile/bank/{bank_code}/audit-defer` (line 16115) — and `/deferred-items`
- `/api/reconcile/refresh-matches` (line 1509)
- `/api/bank-import/raw-preview-email`, `/preview-from-email` (preview path)
**Symptom:** Opera 3 users cannot use these features at all (frontend will route them to a non-existent endpoint and 404), or will have to fall back to the SE pipeline which won't work for FoxPro data.
**Cause:** Features added on SE without the parallel Opera 3 endpoint. Some are minor (settings only); some are not (audit-defer, deferred-items).
**Impact:** Violates "full parity" mandate; Opera 3 customer experience is degraded.
**Repro:** `python3 -c "import re; …"` in the audit transcript shows 78 SE vs 39 O3 endpoints, 42 missing.
**Fix sketch:** Categorise each missing one: cross-platform (e.g. archive endpoints — keep SE-only is fine), or Opera-touching (must mirror). Triage and add the missing Opera-touching mirrors.

---

#### F10: Bank-rec startup integrity check is SE-only
**Where:** `sql_rag/bank_rec_integrity.py` is hard-wired to a SQL connector. Called from `api/main.py:280` inside `_ensure_company_context`. No Opera 3 equivalent.
**Symptom:** When the active company is on Opera 3, the startup integrity check is silently skipped — no warning is logged that we couldn't run it. So the `nk_lstrecl == nk_reclnum` invariant goes unverified.
**Cause:** Module written for SE only.
**Impact:** Loss of a safety net introduced specifically for the Cloudsis batch 209 incident, when applied to Opera 3 customers.
**Fix sketch:** Add a thin `bank_rec_integrity_o3.py` that reads `nbank.dbf` via `Opera3Reader` and applies the same invariants. Wire it into `_ensure_company_context` alongside the SE call.

---

#### F11: Stub endpoint left behind: `/api/reconcile/bank/{bank_code}/scan-emails` returns hardcoded placeholder
**Where:** `apps/bank_reconcile/api/routes.py:1902-1920`
**Symptom:** Caller sees `{"success": True, "statements_found": []}` regardless of state.
**Cause:** Real implementation lives at `/api/bank-import/scan-emails` (line 6009). The stub at line 1902 was left behind during the apps refactor.
**Impact:** Frontend may bind to the wrong endpoint and silently get empty results. Code rot.
**Fix sketch:** Delete the stub and verify no frontend code calls it.

---

#### F12: `_ensure_company_context` mutates `config.ini` on disk every time a new company is touched
**Where:** `api/main.py:182-185`
**Symptom:** First request for a not-yet-seen company writes `config.ini` to set the active company's database name, then constructs a `SQLConnector(CONFIG_PATH)`. Concurrent or partial failures leave the on-disk config pointing at an arbitrary company.
**Cause:** `SQLConnector` reads from a config file path rather than accepting a config dict. The auth middleware at `api/main.py:914-924` works around this by writing a temp config — that path is correct. The lazy creation in `_ensure_company_context` is the legacy path.
**Impact:** A crashed process can leave config.ini in an unexpected state for the next start; harder to reason about which company is active.
**Fix sketch:** Consolidate on the temp-config approach (line 914-924) and remove the in-place `config["database"]["database"] = …; save_config(...)` at lines 183-184.

---

#### F13: Pervasive raw `str(e)` returned to UI — bypasses `friendly_db_error`
**Where:** `apps/bank_reconcile/api/routes.py` — 123 occurrences of `"error": str(e)`. Sample lines: 266, 656, 764, 795, 843, 918, 1005, 1043, 1069, 1088 (and ~113 more).
**Symptom:** Operators see raw pyodbc error strings ("[Microsoft][ODBC Driver 17][SQL Server]Cannot open database 'Opera3SECompany00I' …") instead of "Opera database is currently unavailable — try again in a few minutes".
**Cause:** Routes catch `Exception as e` and surface `str(e)` directly. `friendly_db_error()` exists in `api/main.py:366-421` and IS imported via `_sync_from_main` (line 65) but only used at line 2612 etc.
**Impact:** Violates CLAUDE.md "Never expose raw database errors". UX regression.
**Fix sketch:** Wrap every `str(e)` in routes.py with `friendly_db_error(e)`. Routine mechanical change.

---

#### F14: Hardcoded bank vendor list in `BANK_PATTERNS`
**Where:** `sql_rag/file_archive.py:272-293`, also routes.py:7670 hardcodes `["barclays", "hsbc", "lloyds", "natwest"]`.
**Symptom:** Customers using Monzo, Starling, NatWest International, Metro Bank, Tide, Revolut, etc. are not recognised.
**Cause:** Hardcoded UK retail-bank patterns.
**Impact:** Detection/folder-routing fails for any non-Big-4 bank. Feature is unusable for those customers without a code change.
**Fix sketch:** Move the bank patterns into `data/{company}/core/bank_patterns.json` (or store in `email_data.db`); seed defaults; let users add their own.

---

#### F15: Recent fixes not reflected in central KB or local KB — type-blind fallback
**Where:** Local KB: `apps/core/docs/opera_knowledge_base.md` and central KB: `~/opera-knowledge-ref/packages/opera-knowledge/business-rules/`.
**Symptom:** The type-blind already-posted fallback (commits `a682c81`, `3e09398`, `7cb3649`, `c123efc` over the past week) has no KB entry.
**Cause:** KB-update policy requires same-commit updates; for these the central commit is missing.
**Impact:** Future contributors won't know the fallback exists; could remove it as "dead code" since it's only triggered via a fallback path. Violates CLAUDE.md "Knowledge Base Must Be Updated".
**Fix sketch:** Add `business-rules/already-posted-typeblind.md` to central KB, and a paragraph to `opera_knowledge_base.md` cross-linking it.

---

#### F16: `matcher-period-bound.md` rule statement omits `ae_remove`
**Where:** `~/opera-knowledge-ref/packages/opera-knowledge/business-rules/matcher-period-bound.md` lines 5-15
**Symptom:** The candidate-pool snippet shows only `ae_reclnum = 0` — the open-items rule mandates `AND ae_remove = 0`.
**Impact:** Anyone implementing this rule from the doc misses the `ae_remove` filter.
**Fix sketch:** Cross-reference `bank-rec-open-items.md` and update the candidate snippet to show both predicates (or just reference `OPEN_FOR_REC_SQL`).

---

#### F17: Module globals (`sql_connector`, `email_storage`, `current_company`) used instead of contextvars
**Where:** `api/main.py:304-312` declares plain globals; `_ensure_company_context` mutates them per-request.
**Symptom:** Future maintainers will reach for awaitable APIs (`asyncio.gather`, `asyncio.to_thread`, scheduled tasks) and silently break isolation. Today the only `await` in routes is `apps/bank_reconcile/api/routes.py:7239` (provider auth) — small attack surface, but the design is brittle.
**Cause:** `_request_company_id` is a `ContextVar`; nothing else is. The model is "single uvicorn worker, no concurrency" (per the comment at line 167) — but the periodic email sync IS concurrent (F4) and any future I/O parallelism would be too.
**Fix sketch:** Wrap module-global access in a per-request resolver (e.g. `def get_sql_connector(): return _company_sql_connectors[get_current_company_id()]`) or thread `sql_connector` through call sites explicitly.

---

#### F18: Tests miss SE/Opera 3 parity coverage in critical areas
**Where:** `tests/` directory has 31 test files / 277 collected tests.
**Symptom:** No `test_already_posted_fallback_o3.py` (so F1 wouldn't surface), no `test_match_statement_to_cashbook_o3.py` (F2 wouldn't surface), no integration test for `/api/opera3/bank-reconciliation/match-statement`. No multi-company isolation test.
**Cause:** Test discipline is good for SE but Opera 3 is under-covered.
**Impact:** Parity gaps land silently — exactly the situation CLAUDE.md mandates against.
**Fix sketch:** Add a parity test pattern: parameterise existing SE tests over both data sources where the abstraction allows.

---

### COSMETIC

#### F19: Three giant route handlers (>500 lines)
**Where:** `apps/bank_reconcile/api/routes.py`:
- `scan_all_banks_for_statements` line 6589 — 1476 lines
- `import_bank_statement_from_email` line 9359 — 810 lines
- `import_bank_statement_from_pdf` line 3900 — 736 lines
- `opera3_scan_emails_for_bank_statements` line 12171 — 588 lines
- `preview_bank_import_from_email` line 8777 — 582 lines
- `scan_emails_for_bank_statements` line 6010 — 579 lines
**Symptom:** Hard to read, hard to test, hard to spot regressions.
**Fix sketch:** Extract per-stage helpers; especially the format-detection, AI-extract, archive, and validate sub-paths.

---

#### F20: Same-bug parity in `unreconciled-difference` queries (cosmetic to F3)
The SE site at `sql_rag/statement_reconcile.py:540` has the same `ae_reclnum = 0 OR ae_reclnum IS NULL` filter without `ae_remove`. Promote to SHOULD-FIX once F3 is opened.

---

#### F21: TODOs left in code
- `apps/bank_reconcile/api/routes.py:1914`: stub endpoint mentioned in F11.
- `sql_rag/opera_sql_import.py:12594`: "Resolve product sales nominal from cn_anal" — non-bank-rec but live in the codebase.

---

#### F22: Save-config + connector reload pattern races on first-touch
Tied to F12. When two requests for never-before-seen companies arrive in quick succession, both can race through `if company_id not in _company_sql_connectors:` and both write `config.ini`. The single-worker assumption masks this on prod, but stress/integration tests would expose it.

---

## Confirmed-good areas

- **`sql_rag/bank_rec_heal.py`** — clean module: typed dataclasses, read-only against Opera, uses `is_open_for_rec` semantics, parameterised SQLite, both SE and O3 callers wired in `routes.py:6078, 12223`.
- **Open-items rule SSOT** (`sql_rag/opera_open_items.py`) is correctly imported in SE write/read paths: `bank_import.py:1568`, `duplicate_check_se.py:14`, `duplicate_check_o3.py:52`, `opera_sql_import.py:8401`.
- **`sql_rag/import_lock.py`** — short transactions, immediate commit, 5-min stale-lock cleanup, per-company db path resolution.
- **Per-company DB layout** in `sql_rag/company_data.py` is well-organised; `bank_aliases.db`, `bank_patterns.db`, `pdf_extraction_cache.db`, `email_data.db`, `import_locks.db` all routed via `get_current_db_path` and reset on company switch (`api/main.py:248-272`).
- **Reset-singletons block in `_ensure_company_context`** (`api/main.py:248-272`) covers: supplier_statement_db, pdf_extraction_cache, gocardless_payments, opera_config control-accounts cache, customer_linker cache. `pdf_extraction_cache._cache_instance` is invalidated on path mismatch (`pdf_extraction_cache.py:192-194`), so the singleton is robust.
- **`SKIP_PATTERNS`** is empty in both `bank_import.py:558` and `bank_import_opera3.py:392` — confirmed no hardcoded skip list, in line with the rule.
- **Sequence allocation** uses `WITH (UPDLOCK, ROWLOCK)` correctly (`opera_sql_import.py:1028, 1055, 1414, 11721, 11891, 12105, 12318, 12542, 13102`).
- **NOLOCK in `sql_rag/bank_import.py`** — every Opera read in this file has the hint (14 sites, no misses).
- **Rate-limit error handling** — Gemini 429s caught and surfaced via `RateLimitExhaustedError` at `routes.py:6355, 7141, 7475` with explicit `extraction_failure_reason='rate_limit'` for the UI.
- **`friendly_db_error()`** covers all the major SQL Server error categories (4060, 18456, 1205, lock timeout, invalid object name).
- **Bank-rec self-heal** (commit `0c8021a`) has full SE + O3 parity: same module called from both scan handlers (`routes.py:6078` and `routes.py:12223`), shared `OperaDataSource` Protocol with separate SE/O3 implementations, dedicated tests including `test_bank_rec_heal_parity_and_routes.py`.
- **`P Flannery £198` regression test** exists at `tests/test_flannery_regression.py:21` and locks in the `ae_remove=True` candidate-exclusion behaviour. (But limited to candidate-pool path; see F3.)
- **Bank-rec completion contract + reversal tool** documented in `business-rules/bank-rec-completion.md` and implemented end-to-end (per commits `aa9dfc4`, `cf97c52`).
- **Manual** (`marketing/manuals/manual-bank-reconciliation.md`) is current — last-updated 2026-05-05 and reflects Stage 5 self-heal.
