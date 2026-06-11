# Opera 3 Write Agent — Hardening Implementation Brief

**Status:** Approved for implementation
**Owner:** Jonathan
**Author:** Harry (2026-06-11)
**Context:** Follow-up to `docs/2026-06-11-opera3-cdx-write-path-memo.md`.
This brief is self-contained — it can be pasted directly into Claude Code
(or followed by hand) inside the `llmragsql` repo.

## Objective

Make the Opera 3 Write Agent production-trustworthy: every posting either
completes fully verified (tables, fields, balances, indexes all correct) or
is detected, compensated/blocked, and reported. No silent failure mode of
any kind. Foolproof locking against both concurrent agent requests and live
Opera users.

**Honest engineering standard:** shared DBF/CDX storage has no atomic
multi-table commit, so "physically impossible to fail" does not exist —
the standard is **"never silently wrong"**: detected ➜ compensated ➜ or
blocked with `manual_review_required`. Everything below serves that.

## Ground truth (verified 2026-06-11 — do not re-litigate)

- `opera3_agent/service.py` `_get_importer()` (≈line 383) instantiates
  `Opera3FoxProImport` from `sql_rag/opera3_foxpro_import.py` — the python
  `dbf`-package engine. This engine NEVER touches CDX files, uses
  `fcntl.flock` on `.lck` sidecar files (invisible to Opera/VFP byte-range
  locks), and has an unguarded `import fcntl` (line 32) so it cannot even
  import on Windows.
- `opera3_agent/harbour_dbf.py` (ctypes wrapper) and
  `opera3_agent/harbour/dbfbridge.prg` (Harbour source) are built but NOT
  wired into any write path. `HarbourDBF` is only imported for the
  `/health` `harbour_available` flag (service.py ≈line 657).
- `dbfbridge.prg` is already correctly configured: `DBFCDX` RDD,
  `DBFFPT` memo support, `SET AUTOPEN ON`, `SET DELETED ON`,
  `rddInfo(RDDI_LOCKSCHEME, DB_DBFLOCK_VFP)` (VFP-compatible byte-range
  locks — essential for coexistence with Opera clients), and
  `DBF_UNLOCK` performs `dbCommit()` before `dbUnlock()`.
- `harbour/libdbfbridge.dll` has never been compiled. `harbour/build.sh`
  exists.
- The posting engine mutates DBFs via exactly three idioms (grep-verified):
  1. `table.append({...})` — 86 call sites
  2. `with record:` blocks assigning fields on a located record — ~15 sites
     (lines 486, 567, 778, 941, 956, 992, 1057, 5228, 5288, 5533, 5588,
     6939, 6991)
  3. `table.write(table.current_record, {...})` — reconcile path
     (lines 5796, 5829, 5835, 5856, 5873)
- `service.py` endpoints are concurrent async handlers with NO global
  write serialisation. `harbour_dbf.py` serialises individual ctypes calls
  (Harbour VM is single-threaded) but NOT whole transactions.
- Canonical lock order (KB doc `opera-knowledge/platform/opera3-write-agent.md`):
  `aentry → atran → ntran → ptran/stran → nacnt → nbank → anoml`.

## Non-negotiable rules

1. Do NOT rewrite the posting business logic in
   `sql_rag/opera3_foxpro_import.py` (~7k lines of validated Opera posting
   rules). Abstract the table access UNDER it.
2. No code path may write to a DBF without going through the Harbour
   bridge in production. No silent fallback to the python `dbf` package —
   ever.
3. Reads may continue via `dbfread`/`dbf` package (read-only is safe).
4. Refuse, never truncate: any value that does not fit its field is a
   structured error before any lock is taken.
5. All work below lands with tests. Each WP has explicit acceptance
   criteria; do not mark a WP done without demonstrating them.

---

## WP0 — Hard write-path gate (do first, ship immediately)

**File:** `opera3_agent/service.py`

1. At lifespan startup, attempt to initialise `HarbourDBF`. Store the
   result (instance or `None` + the load error string) in app state.
2. Add a dependency `require_safe_write_path()` applied to EVERY mutating
   endpoint — all `/import/*`, `/allocate/*`, `/reconcile/mark`. It raises
   HTTP 503 with body
   `{"error": "unsafe_write_path_disabled", "detail": "Harbour DBFCDX bridge not loaded: <reason>. Direct python-dbf writes are forbidden in production. Set OPERA3_ALLOW_UNSAFE_WRITES=1 only on a single-user dev copy."}`
   unless EITHER the bridge loaded successfully OR env var
   `OPERA3_ALLOW_UNSAFE_WRITES=1` is set.
3. When the override env var is set, log a WARNING banner on startup and
   include `"unsafe_write_path": true` in `/health` and `/status`.
4. Read-only endpoints (`/health`, `/status`, `/wal/*`, `/check/duplicate`,
   generic read) are NOT gated.

**Acceptance:** unit tests proving (a) all mutating endpoints 503 when the
bridge is absent and the override unset; (b) override permits writes and is
visible on `/health`; (c) read endpoints unaffected.

## WP1 — Compile and self-test the bridge

**Files:** `opera3_agent/harbour/build.sh`, new
`opera3_agent/harbour/build_windows.bat`, new
`opera3_agent/harbour_selftest.py`

1. On the Windows Opera host (or any Windows box): install Harbour 3.2+
   (or use the official binary distribution), then build
   `libdbfbridge.dll` from `dbfbridge.prg` with `hbmk2 -shared` per
   `build.sh` flags. Commit a `build_windows.bat` that reproduces the build.
2. Write `harbour_selftest.py`: creates a scratch DBF with a structural
   CDX (two tags) via the bridge, appends 100 records, replaces fields of
   each type (C/N/D/L/M), seeks every record via both tags, checks
   `reccount`/`recno`, exercises `rlock`/`unlock` from two processes
   (second process must observe the first's lock), and confirms
   `dbCommit` flushed (reopen and re-verify). Exits non-zero on any
   failure.
3. The deployment package builder already auto-includes the DLL if present
   (commit `b3ac6c0`) — verify it does.

**Acceptance:** self-test passes on the Windows host; `/health` reports
`harbour_available: true`; the two-process lock-visibility check passes.

## WP2 — Table-access backend under the posting engine

**New file:** `sql_rag/opera3_table_backend.py`
**Modified:** `sql_rag/opera3_foxpro_import.py`, `opera3_agent/service.py`

1. Define the backend interface (plain class or Protocol):

   ```
   class TableBackend:
       def open(self, table_name: str, dbf_path: Path) -> TableHandle
       def close_all(self) -> None

   class TableHandle:
       def append(self, record: dict) -> int            # returns recno
       def update_current(self, updates: dict) -> None   # replace fields on current record
       def goto(self, recno: int) -> None
       def goto_top(self) / goto_bottom(self)
       def seek(self, tag: str, key) -> bool
       def recno(self) -> int
       def reccount(self) -> int
       def rlock(self) -> bool                           # current record, with retry/timeout
       def unlock(self) -> None                          # commits + unlocks (Harbour semantics)
       def scan(self) -> Iterator[dict]                  # read-only iteration
   ```

2. Two implementations:
   - `PythonDbfBackend` — wraps the current `dbf` package behaviour
     exactly (keeps today's dev workflow on macOS). Move `import fcntl`
     and the `.lck` flock logic INSIDE this class so the module imports
     cleanly on Windows.
   - `HarbourBackend` — wraps `opera3_agent.harbour_dbf.HarbourDBF`:
     `append` = `hb_dbf_append` + per-field `hb_dbf_replace_{c,n,d,l,m}`;
     `update_current` = rlock → replace fields → unlock;
     navigation/seek/recno/reccount map 1:1 to existing bridge functions.
     All values pass through the typed conversion layer (WP4 item 2).
3. Refactor `Opera3FoxProImport` to take a `backend` constructor argument
   and route ALL mutations through it. The three idioms map mechanically:
   - `table.append({...})` → `handle.append({...})`
   - `with record:` + field assignment → `handle.update_current({...})`
     (the record is already located by the surrounding code)
   - `table.write(table.current_record, {...})` → `handle.update_current({...})`
   Read-only scans/lookups may stay on `dbfread` or go through
   `handle.scan()` — either is acceptable, but be consistent per function.
4. Backend selection in `service.py::_get_importer()`:
   `HarbourBackend` when the bridge is loaded; `PythonDbfBackend` ONLY
   when `OPERA3_ALLOW_UNSAFE_WRITES=1`. Selection is logged at startup.
   There is no runtime fallback between backends.

**Acceptance:** (a) full existing test suite passes against
`PythonDbfBackend` (proves the refactor is behaviour-preserving);
(b) the same suite passes against `HarbourBackend` on Windows against a
copy of Opera demo data; (c) `import sql_rag.opera3_foxpro_import`
succeeds on Windows; (d) grep proves zero remaining direct
`dbf.Table`/`.append(`/`with record`/`.write(` mutation sites outside
`PythonDbfBackend`.

## WP3 — Single-writer queue + enforced lock order

**Files:** `opera3_agent/service.py`, `sql_rag/opera3_table_backend.py`

1. Create one module-level
   `WRITE_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="opera3-writer")`.
   `run_import_with_safety` submits the entire import callable (prepare →
   write → verify) to this executor via `loop.run_in_executor`. Result:
   whole transactions are strictly serialised; agent-vs-agent interleaving
   is impossible by construction. (At <500 ms per posting this costs
   nothing; the only remaining contention is agent-vs-Opera-user, handled
   by RLOCK.)
2. Central lock-order constant in `opera3_table_backend.py`:

   ```
   LOCK_ORDER = ["aentry", "atran", "ntran", "ptran", "stran",
                 "nacnt", "nbank", "anoml", "atype", "nhist",
                 "sname", "pname", "salloc", "palloc", "zvtran",
                 "nvat", "nextid", "arhead", "arline"]
   ```

   `HarbourBackend` tracks tables opened-for-write per operation and
   raises `LockOrderViolation` if a write-open arrives out of order
   relative to tables already opened in this operation. New tables must be
   added to this ONE list (header before detail, parent before child) —
   never per-endpoint orderings.
3. RLOCK discipline in `HarbourBackend.rlock()`: retry loop, 5-second
   timeout, never force, structured `lock_timeout` error to the caller.
   Locks released (with `dbCommit`) immediately after each table's write
   phase. On any exception, `unlock_all` + `close_all` in a finally block.
4. Prepare-before-lock stays as designed: no lookup, ID generation, or
   validation while any lock is held. Add an assertion hook (debug mode)
   that fails if `seek`/`scan` is called while a lock is held.

**Acceptance:** (a) test posting two slow transactions concurrently —
WAL timestamps prove sequential execution; (b) unit test for
`LockOrderViolation` on a deliberately out-of-order write; (c) lock
timeout test: hold a record lock from a second process, attempt a posting,
receive structured `lock_timeout` within ~5 s, no partial write
(verify + compensation clean).

## WP4 — Field/value correctness hardening

**Files:** new `opera3_agent/schema_contract.py`, new
`sql_rag/opera3_field_convert.py`, `opera3_agent/transaction_safety.py`,
`opera3_agent/write_ahead_log.py`, `opera3_agent/service.py`

1. **Schema contract.** Script generates
   `opera3_agent/expected_schema.json` from a live data set: for every
   table the agent writes (see LOCK_ORDER) — field name, type, width,
   decimals, plus CDX tag names. At agent startup, read actual DBF headers
   and diff against the contract. Any mismatch → agent starts in
   read-only mode, `/health` reports `schema_mismatch` with the diff, all
   mutating endpoints 503. (Catches Opera version upgrades changing
   structures.)
2. **Typed conversion layer** (`opera3_field_convert.py`): single function
   per VFP type used by `HarbourBackend` for every outgoing value.
   C: encode with the configured codepage, error on over-width (no
   truncation). N: Decimal-based range check against width/decimals, error
   on overflow; never floats for money. D: `YYYYMMDD` string, reject
   out-of-range. L: strict bool. M: memo via `hb_dbf_replace_m`. Unit
   tests per type including boundary and overflow cases.
3. **Accounting invariants in post-write verification**
   (`transaction_safety.py`, runs inside the same writer-thread operation):
   - sum of `ntran` debits == sum of credits for the posting (to the penny)
   - `na_*` balance deltas on `nacnt` == posted amounts
   - control-account movement == ledger movement (debtors/creditors)
   - `zvtran`/`nvat` rows consistent with the VAT amounts posted
   - row counts and key fields as already implemented
   Any invariant failure → existing compensation path → on compensation
   failure, writes_blocked (already implemented — keep).
4. **Idempotency.** Every mutating request accepts a client-supplied
   `operation_id` (UUID, required). WAL gains a unique index on it. A
   duplicate `operation_id` returns the stored original result with
   `"duplicate": true` instead of re-posting. The SAM/bank-rec/gocardless
   clients must send it (one-line change in `opera3_agent_client.py`).

**Acceptance:** (a) startup against tampered schema (one widened field)
→ read-only mode + diff visible on `/health`; (b) conversion-layer unit
tests pass incl. overflow refusal; (c) deliberately corrupt a posting in a
test (monkeypatch one write) → invariant check catches it, compensation
runs, operation reported failed; (d) replay the same `operation_id` twice
→ exactly one set of records in the DBFs.

## WP5 — Verification suite (the go-live gate)

**New:** `opera3_agent/tests/integration/` + a written runbook
`docs/opera3-write-agent-verification-runbook.md`

Run on a **copy** of live Opera 3 data on the Windows host:

1. **Golden-master parallel run.** For EVERY transaction type the agent
   posts (purchase payment, sales receipt, both refunds, bank transfer,
   nominal entry, GoCardless batch, recurring entry, allocation, reconcile
   mark): post natively through the Opera 3 UI on copy A; post identical
   data via the agent on copy B; capture both with the snapshot tool
   (`ai-sam` transaction snapshot library) and field-level diff. Allowed
   diffs: audit timestamps/user-stamps only — document each one. Any other
   diff is a defect.
2. **CDX integrity.** After each agent posting: from a SEPARATE Harbour
   session, `SEEK` the new records via every tag of every touched table;
   record `ordKeyCount()` per tag, run `hb_dbf_reindex`, confirm key
   counts unchanged and seeks still succeed (reindex-invariance proves the
   incremental index updates were complete).
3. **Fault injection.** Scripted kill of the agent process at randomised
   points during postings (≥50 iterations), plus one hard power-off of the
   test VM mid-batch. After each: restart agent → WAL replay → assert data
   copy passes the full invariant + CDX suite, and any incomplete
   operation is either fully compensated or `writes_blocked` is raised.
   Silence is failure: every iteration must end in a recorded verdict.
4. **Coexistence.** With an Opera client session holding a record lock on
   a target row, drive postings: agent must wait ≤5 s, error cleanly, and
   leave no partial state. Conversely, sustained agent posting load must
   not produce visible errors in the Opera client (brief "record in use"
   flicker acceptable).

**Acceptance:** all four sections pass and the runbook records the
evidence (snapshot diffs, key counts, kill-test log). This WP gates
production enablement — no exceptions.

## WP6 — Production enablement checklist

1. WP0 gate merged and deployed; `OPERA3_ALLOW_UNSAFE_WRITES` unset on the
   production host.
2. DLL deployed; `/health` shows `harbour_available: true`,
   `schema_mismatch: false`, `unsafe_write_path: false`.
3. WP5 runbook signed off (Harry + Jonathan).
4. Installations page configured with Agent URL/Key; health banner
   integration live; clients sending `operation_id`.
5. Manual-repair runbook for `manual_review_required` written (which
   tables to inspect via WAL operation detail, when to run Opera Data
   Repair).
6. Agent bound to LAN interface only; agreed whether TLS / localhost +
   reverse proxy is required for this site before go-live.
7. First production week: review `/wal/recent` daily; run the CDX
   reindex-invariance check on touched tables after day 1 and day 5.

---

## Out of scope (explicitly)

- Rewriting posting business rules, allocation logic, or duplicate
  detection — unchanged.
- Automated balance reversal in compensation (Phase 5 of the memo) — the
  soft-delete + logged-adjustments design stays for now; WP4's invariants
  make failures loud, which is the requirement.
- Opera SE / SQL Server paths — untouched.

## Order of work

WP0 → WP1 → WP2 → WP3 → WP4 → WP5 → WP6. WP0 is independent and ships
immediately. WP2 is the largest item; do the `PythonDbfBackend`
extraction first (behaviour-preserving, fully testable on macOS), then
`HarbourBackend` on Windows.
