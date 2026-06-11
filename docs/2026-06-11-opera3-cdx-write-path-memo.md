# Opera 3 Write Path — CDX Index Maintenance: Findings & Completion Plan

**From:** Harry
**For:** Jonathan
**Date:** 2026-06-11
**Re:** "CDX index maintenance is unverified: the posting engine writes via the
python dbf package, not the Harbour DBFCDX bridge" — design-intent question

## TL;DR

Your reading is correct, and your caution is the designed behaviour. The
original design **always intended every production write to go through the
Harbour DBFCDX bridge**. The python `dbf` posting engine is a dev-era
artefact that predates the Write Agent by a month and was never migrated —
the work stalled at the "compile `libdbfbridge.dll` on Windows" step.
**Keep production postings disabled on FoxPro** until the bridge is wired in
per the plan below.

## How we got here (git history)

1. **2026-02-04** — `sql_rag/opera3_foxpro_import.py` built as "Opera 3 full
   import capability with equivalent locking to SQL SE". This is the
   7,000-line posting engine using the python `dbf` package. It was written
   for **local dev on the Mac**: `fcntl.flock` locks on `.lck` sidecar files
   plus an SMB download-modify-upload mode.
2. **2026-03-10/11** — the Write Agent landed (`service.py`, WAL,
   `transaction_safety.py`, **and** `harbour_dbf.py` + `dbfbridge.prg`, all
   together). The design spec
   (`docs/superpowers/specs/2026-03-31-opera3-write-agent-design.md`) states:
   *"Index maintenance | Harbour DBFCDX maintains CDX indexes automatically"*
   and lists `harbour/libdbfbridge.dll` as **NOT BUILT**.
3. The canonical KB doc
   (`ai-sam/packages/opera-knowledge/platform/opera3-write-agent.md`) repeats
   the rule: *"All writes go through Harbour DBFCDX… A direct write that
   bypasses Harbour can desync the index — DO NOT add code paths that write
   to DBFs directly."*
4. The migration stopped there. `service.py` `_get_importer()` still
   instantiates `Opera3FoxProImport` (python `dbf`); `HarbourDBF` is imported
   only to set the `harbour_available` flag on `/health`. `harbour/` contains
   only `dbfbridge.prg` + `build.sh` — the DLL was never compiled.

The interim state was conscious: `transaction_safety.py` says three times
that compensation uses CDX-safe soft-deletes and that balance reversal is
logged-not-applied *"until Harbour bridge enables safe automated writes."*

## Three independent reasons the python `dbf` path can't go to production

1. **CDX desync (your finding, confirmed):** zero CDX/index references in
   the 7,054-line engine. The `dbf` package appends to the DBF and memo
   files but never touches the structural `.cdx` — Opera's DBFCDX driver
   would read stale indexes: new postings invisible to seeks, table flagged
   as index-corrupt.
2. **Locking is not interoperable with Opera:** the engine `flock`s `.lck`
   sidecar files. VFP/Opera clients use byte-range locks on the DBF itself,
   so live Opera users and this engine never see each other's locks. The
   bridge exists precisely to get genuine `RLOCK` semantics.
3. **It cannot run on the Windows host at all:** unguarded `import fcntl`
   at the top of `opera3_foxpro_import.py` — `fcntl` does not exist on
   Windows, so the module fails at import time on the Opera server.

## Recommended completion plan

### Phase 0 — guardrail now (small, do first)

Add a hard gate in `service.py`: every `/import/*`, `/allocate/*` and
`/reconcile/mark` endpoint refuses with a structured error unless the
Harbour bridge loaded successfully (`harbour_available == true`) or an
explicit `OPERA3_ALLOW_UNSAFE_WRITES=1` dev-only env var is set. That makes
"no production postings" enforced by code rather than by convention.

### Phase 1 — compile the bridge

Build `libdbfbridge.dll` on a Windows host with Harbour installed
(`harbour/build.sh` documents the flags; the package builder from commit
`b3ac6c0` already auto-includes the DLL if present). Smoke-test the
`hb_dbf_*` exports through `harbour_dbf.py` against a scratch DBF/CDX pair.

### Phase 2 — table-access adapter (the real work)

The posting logic is coded directly against the `dbf` package API
(`dbf.Table`, `.append({...})`, field assignment). Don't rewrite the 7k
lines of posting rules — abstract the table access:

- Define a small backend interface: `open / append / replace / seek /
  rlock / unlock / close`.
- Two implementations: `PythonDbfBackend` (Mac dev, current behaviour,
  keeps `fcntl` import inside it) and `HarbourBackend` (production,
  wraps `HarbourDBF`). Note the Harbour VM is single-threaded —
  `harbour_dbf.py` already serialises calls; keep all writes on one worker.
- Route `Opera3FoxProImport._open_table` / append / replace through it.
  Selection by platform/config, never silent fallback.

### Phase 3 — locking discipline

Replace the `.lck` flock scheme with the bridge's `RLOCK` per the canonical
global lock order in the KB doc (`aentry → atran → ntran → ptran/stran →
nacnt → nbank → anoml`). Prepare-everything-before-locking and the 5-second
timeout rules carry over unchanged.

### Phase 4 — verification before enabling

On a **copy** of live Opera 3 data on Windows:

- Post each transaction type through the agent, then `SEEK` the new records
  via every CDX tag from a separate Harbour/VFP session — proves index
  maintenance.
- Diff a freshly `REINDEX`ed copy of each CDX against the as-written one.
- Run the snapshot tool before/after each posting type and diff against the
  Opera-native equivalents in the transaction library.
- Run Opera itself against the data copy and eyeball the posted entries.

### Phase 5 — production enablement

Wire the Installations-page Agent URL/Key/health integration, and upgrade
`transaction_safety.py` compensation to do real balance reversal (the docs
already promise this "when Harbour bridge is compiled").

## Pointers

- Posting engine: `sql_rag/opera3_foxpro_import.py`
- Agent service + importer wiring: `opera3_agent/service.py` (`_get_importer`)
- Bridge wrapper: `opera3_agent/harbour_dbf.py`; source `opera3_agent/harbour/dbfbridge.prg`
- Design spec: `docs/superpowers/specs/2026-03-31-opera3-write-agent-design.md`
- Canonical KB doc: `ai-sam/packages/opera-knowledge/platform/opera3-write-agent.md`
