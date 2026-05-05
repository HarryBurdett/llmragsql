# Opera 3 Column-Reference Audit

**Date:** 2026-05-05
**Scope:** Bank reconciliation + GoCardless workflows on Opera 3.
**Schema source:** `scripts/opera_snapshot.json` (Opera SE — same column names on Opera 3 per `~/opera-knowledge-ref/.../platform/foxpro-vs-sql.md`).
**Method:** Static scan of all in-scope files for dict-style accesses (`.get('X')`, `['X']`) where `X` matches Opera column-name regex `[a-z]{2,3}_[a-z][a-z_]*`. Each candidate column was cross-checked against the canonical column list of every Opera table in the snapshot. Kept only accesses whose key prefix matches a real Opera table prefix (filtering out local-SQLite, JSON-payload, and Python-data keys).

## Summary

Scanned 14 files, ~43k lines, ~866 dict-access sites matching the Opera-style key regex. After verifying surrounding context (which Opera table the row was read from, whether the key is a synthesized rename, whether the source is a SQL alias, etc.), three real classes of bug were identified, all in the **bank reconciliation** workflow (none in GoCardless). All three are columns that do not exist on the target Opera table — exactly the silent-failure pattern that motivated the audit.

- **CRITICAL findings: 3 distinct typos / wrong column names**, totalling **11 affected sites** across 3 files.
- **Borderline cases reviewed: ~120**, all confirmed safe (synthesized dict keys, SQL aliases, valid columns flagged by anchor-detection heuristic).

## Findings

### CRITICAL (column doesn't exist on the target table → silently broken)

#### F1: `nb_acnt` / `nb_sort` / `nb_number` / `nb_name` on `nbank` — wrong table prefix (should be `nk_…`)

The `nb_` prefix belongs to the budget table `nbudg`, not `nbank`. The actual `nbank` columns are `nk_acnt`, `nk_sort`, `nk_number`, `nk_bkname` (no `nk_name` — see F1b). The chained `record.get('NB_ACNT', record.get('nb_acnt', ''))` falls through to `''` because **neither** the uppercase nor lowercase form of `nb_*` exists on `nbank`. Where a third fallback `record.get('nk_acnt', '')` is present, the bug is masked at runtime. Where it isn't, the value is silently empty.

This is the seed example.

**Sites where there is NO `nk_*` fallback (silently broken — high severity):**

- `apps/bank_reconcile/api/routes.py:12207` — `nb_acnt = str(record.get('NB_ACNT', record.get('nb_acnt', ''))).strip().upper()` — bank-existence check trivially fails; reconciled-balance lookup silently disabled.
- `apps/bank_reconcile/api/routes.py:12835` — same pattern, in `download-bank-statement` flow; reconciled balance for the email-attached statement always None.
- `sql_rag/bank_import_opera3.py:1475` — `code = record.get('nb_acnt', '').strip()` — `validate_bank_account()` always returns False (would return False except the function swallows exceptions and returns True on error).
- `sql_rag/bank_import_opera3.py:754` — first `.get('nb_acnt')` always misses, but second `.get('nk_acnt', '')` rescues it. Wasteful and misleading; not silently wrong but should be cleaned up.
- `sql_rag/bank_import_opera3.py:758` — `b.get('nb_sort') or b.get('nk_sort', '')` — first miss, fallback rescues. Same shape.
- `sql_rag/bank_import_opera3.py:759` — `b.get('nb_number') or b.get('nk_number', '')` — same.
- `sql_rag/bank_import_opera3.py:760` — `b.get('nb_name') or b.get('nk_name', '')` — see F1b; even the fallback `nk_name` does not exist (real column is `nk_bkname`).

**Sites with a `nk_acnt` final fallback (works, but masking the bug):**

- `apps/bank_reconcile/api/routes.py:13062` — `nb_acnt = str(record.get('NB_ACNT', record.get('nb_acnt', record.get('nk_acnt', '')))).strip().upper()`
- `apps/bank_reconcile/api/routes.py:13090` — `rec_code = str(rec.get('NB_ACNT', rec.get('nb_acnt', rec.get('nk_acnt', '')))).strip()`
- `apps/bank_reconcile/api/routes.py:14457` — `nb_acnt = str(record.get('NB_ACNT', record.get('nb_acnt', record.get('nk_acnt', '')))).strip().upper()`

**Effect:** depends on the site. Most damaging are `routes.py:12207` and `:12835` (no `nk_acnt` fallback), where the safety rails — bank existence verification and reconciled-balance match — are silently disabled on Opera 3. This is exactly the seed bug that started the audit.

**Fix:** drop every `nb_*` key on `nbank` reads. Use `nk_acnt`, `nk_sort`, `nk_number`, `nk_bkname` directly (with the standard `UPPER`/`lower` casing fallback if `dbfread` returns mixed cases on a given system).

#### F1b: `nk_name` on `nbank` — column does not exist (should be `nk_bkname`)

- `sql_rag/bank_import_opera3.py:760` — `(b.get('nb_name') or b.get('nk_name', '')).strip()`

`nbank` has no `nk_name` column; the human-readable name is in `nk_bkname` (also referenced as `nk_desc`, which does exist and stores the description). Both the primary key (`nb_name`) and the fallback (`nk_name`) miss, so `name` ends up as `''` for every bank when assembling `_other_banks` — degrading bank-transfer detection on Opera 3 (the txn falls back to `bank['code']` when displaying matched names — see L781, L791).

**Fix:** replace `nk_name` with `nk_bkname` (or `nk_desc`).

#### F2: `nk_forgn` on `nbank` — column does not exist (should be `nk_fcurr`)

- `apps/bank_reconcile/api/routes.py:15982` — `is_foreign = (row.get('nk_forgn', 0) or 0) == 1` — in the `/api/opera3/cashbook/bank-accounts` endpoint that lists banks for transfers.

The `nbank` schema has no `nk_forgn`. Foreign-currency banks are identified in SE by `nk_fcurr` being non-empty (see e.g. `sql_rag/bank_import.py:1200`, `sql_rag/opera_sql_import.py:1238`, `:9179`, `:9649` — every SE site uses `nk_fcurr IS NULL OR RTRIM(nk_fcurr) = ''` to mean "GBP / not foreign").

**Effect:** `is_foreign` is always `False` on Opera 3, so the foreign-currency exclusion in `/api/opera3/cashbook/bank-accounts` is silently disabled. Foreign-currency bank accounts are listed as transfer destinations on Opera 3, breaking parity with SE.

**Fix:** replace `(row.get('nk_forgn', 0) or 0) == 1` with `bool(str(row.get('nk_fcurr') or '').strip())` (mirror the SE condition: a non-empty `nk_fcurr` means foreign).

#### F3: `nk_lstrcln` on `nbank` — typo (should be `nk_lstrecl`)

- `sql_rag/opera3_data_provider.py:1541` — `last_rec_line = int(bank_info.get('nk_lstrcln', 0) or 0)`

The real column is `nk_lstrecl` ("last reconciled line number"). Note the column ordering differs by one character — the typo swaps the last two consonants.

**Effect:** `last_rec_line` is always `0` in `get_bank_reconciliation_status()` for Opera 3, surfaced in the API response as `"last_rec_line": 0`. Any frontend / downstream logic that uses this field to know where the previous reconciliation finished gets wrong data.

**Fix:** rename to `nk_lstrecl`.

### LIKELY-FINE / borderline (reviewed, not findings)

The audit script flagged a number of additional accesses; on review every one of them is safe. I list the categories so the controller can sanity-check.

#### N1: Synthesised dict keys (renames in `Opera3DataProvider`)

- `ae_date`, `ae_ref`, `ae_detail` accesses across `apps/bank_reconcile/api/routes.py`, `apps/gocardless/api/routes.py`, and `sql_rag/statement_reconcile_opera3.py` are not Opera column reads. They are synthesised dict keys produced inside `sql_rag/opera3_data_provider.py` (lines 866–891 and 922–950) which renames `ae_lstdate → ae_date`, `ae_entref → ae_ref`, `ae_comment → ae_detail` before returning the dict to callers. Verified the renamer reads `row.get('ae_lstdate')` etc. correctly. Not findings.

#### N2: SQL Server alias reads inside non-Opera-3 routes

- `at_date`, `ae_ref`, `as_of_date` accesses in `apps/gocardless/api/routes.py` (e.g. L512, L514, L1828, L2046, L2070, L2104, L2561, L2581, L2603, L2833, L2852, L2876, L4050, L4771, L4772, L4981, L4982) all read from rows returned by `sql_connector.execute_query(...)` against SQL Server, where the SELECT lists explicitly alias `at_pstdate as at_date` and `ae_entref as ae_ref` (e.g. L491, L2032, L2057, L2090, L2549, L2569, L2590, L2822, L2840, L2864, L4760-ish, L4970-ish). These rows are SE objects, not Opera 3 FoxPro records. Not findings, and not even Opera 3 paths.
- `as_of_date` is a Python parameter on the VAT-codes helper, not an Opera column.
- `ih_econtr`, `ih_scontr`, `st_dueday`, `st_trdate` accesses in `apps/gocardless/api/routes.py` are also SQL alias reads against SE. Confirmed by reading the surrounding `sql_connector.execute_query` SELECT statements.

#### N3: GoCardless API JSON payload keys

- `fx_amount`, `fx_currency`, `fx_rate` (e.g. `sql_rag/gocardless_api.py:768-770`, `apps/bank_reconcile/api/routes.py:2656,2659,3828,9267,9270`) are keys on a JSON dict returned by the GoCardless REST API (`fx_data = data.get("fx", {})`). They happen to match the prefix of `fnoml.fx_*` (the foreign-currency nominal transfer file) but are unrelated. Not findings.
- `use_count` in `sql_rag/bank_import_opera3.py:553` is on the local `bank_import_aliases` SQLite row, not Opera. Not a finding.

#### N4: Anchor mismatch on valid columns (heuristic limitation)

The audit script picked the most-recent `read_table('X')` call in scope to guess the source table. On large functions (e.g. routes.py L11865+, L11953+, opera3_foxpro_import.py L6087+), the anchor was sometimes a different table than the actual loop variable. After manual review, all `arhead`/`arline`/`aentry` accesses in those scopes (`ae_acnt`, `ae_desc`, `ae_entry`, `ae_every`, `ae_freq`, `ae_nxtpost`, `ae_posted`, `ae_topost`, `ae_type`, `at_account`, `at_acnt`, `at_cbtype`, `at_comment`, `at_entref`, `at_entry`, `at_job`, `at_project`, `at_value`, `at_vatcde`, `at_vatval`, `na_acnt`, `na_desc`, `na_allwprj`, `na_allwjob`, `na_project`, `na_job`, `pn_account`, `pn_name`, `pn_suptype`, `sn_account`, `sn_name`, `sn_custype`) read columns that DO exist on those tables. Not findings.

- `pt_account`, `pt_trdate`, `st_account`, `st_trdate` in `sql_rag/opera3_foxpro.py:431/439/467/475` — verified against snapshot: all four are real columns on `ptran` / `stran`. Not findings.
- `pt_trbal`, `pt_trref`, `pt_trtype`, `st_trbal`, `st_trref`, `st_trtype` in `sql_rag/bank_import_opera3.py:804-842` — all real `ptran` / `stran` columns. Not findings.

#### N5: Reads from already-synthesised `bank_info` dicts

- `bank_info.get('nk_desc')`, `bank_info.get('nk_sort')`, `bank_info.get('nk_number')` in `apps/bank_reconcile/api/routes.py:14796-14798` — `bank_info` here is a row freshly read from `nbank` via `Opera3Reader.read_table('nbank')`, so the keys are the real `nbank` columns. The audit script anchored these to `nacnt` because `nacnt` was the most-recent `read_table` call in surrounding scope. Not findings.
- `row.get('nk_acnt')`, `row.get('nk_desc')`, `row.get('nk_sort')`, `row.get('nk_number')` in `sql_rag/statement_reconcile_opera3.py:291-294` — `row` is `nbank_records[0]` from `self.reader.query('nbank', ...)`. Not findings.

## Confirmed-good areas

Files scanned and confirmed clean of dict-style Opera column-name typos:

- `sql_rag/bank_rec_heal.py` — only reads from local SQLite `bank_statement_imports` (`closing_balance`, `period_end`, `statement_number`); no Opera column reads at all.
- `sql_rag/duplicate_check_o3.py` — no problematic patterns found.
- `sql_rag/opera3_agent_client.py` — pure HTTP client to the agent; no FoxPro reads.
- `sql_rag/opera3_write_provider.py` — pure facade; no column reads.
- `sql_rag/gocardless_parser.py`, `sql_rag/gocardless_payments.py` — JSON payloads only; no Opera column reads.
- `opera3_agent/service.py`, `opera3_agent/transaction_safety.py`, `opera3_agent/harbour_dbf.py` — handle plain Python dicts (operation params, WAL, results); no Opera column-name dict access.
- `sql_rag/opera3_foxpro_import.py` — all `aentry/atran/arhead/arline/sname/pname/nacnt/ptran/stran/ntran/nbank` writes use real columns (32+ uppercase/lowercase chained-fallback patterns, all verified against snapshot).
- `sql_rag/statement_reconcile_opera3.py` — bank reads use real `nk_*` columns; the synthesised `ae_date`/`ae_ref`/`ae_detail` keys are all defined within `Opera3DataProvider` and consumed self-consistently.

## Process notes

- **Tables checked against the snapshot:** every Opera table whose 2-3 letter prefix appeared as a key (covers `nbank`, `nbudg`, `aentry`, `arhead`, `arline`, `atran`, `ntran`, `stran`, `ptran`, `sname`, `pname`, `nacnt`, `nhist`, `astat`, `atype`, `nname`, `fnoml`, `ihead`, `itran`, plus ~30 others touched by the prefix→table mapping).
- **Files scanned:** 14 (all listed in scope plus `opera3_agent/service.py`, `opera3_agent/transaction_safety.py`, `opera3_agent/harbour_dbf.py`).
- **Total dict-access patterns examined:** 866 matched the Opera-key regex; ~205 were prefix-Opera-like and worth investigating; ~120 were borderline anchor-mismatch reviews; ~85 had columns that didn't exist on any table at all (most were synthesised keys / API JSON keys, narrowed to 11 sites across 3 files which are real bugs).
- **Total CRITICAL findings:** 3 distinct issues (F1, F2, F3), with F1 having two sub-flavours (silently broken vs masked by `nk_acnt` fallback) and a related sub-finding F1b (`nk_name` vs `nk_bkname`).
