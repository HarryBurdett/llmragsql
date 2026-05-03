# Snapshot Rollup + Local Mirror + CI Gate — Design Spec

**Status:** Draft for review
**Date:** 2026-05-03
**Author:** Claude (per Charlie Burdett's mandate to fix accumulated issues)

## Goal

The transaction-snapshot feature captures raw JSON snapshots of Opera before/after database state — these tell us **exactly how Opera should be updated when posting transactions**. Today the raw JSON reaches the central repo, but the human-readable distillation (`COMPLETE_FIELD_REFERENCE.md`) goes stale silently and the local mirror drifts. This spec closes that loop deterministically.

## Why

Today's audit found:
- 69 raw JSON snapshots in `~/opera-knowledge-ref/.../transaction-library/` — current.
- `COMPLETE_FIELD_REFERENCE.md` last regenerated 2026-04-06.
- ~22 snapshots from 2026-04-07 onwards (payroll, stock, sop, additional cashbook) are NOT reflected in the rollup.
- A local copy at `apps/core/docs/opera_transaction_field_reference.md` was byte-identical to central but maintained by manual copy.
- No automated regenerator script exists.

Result: developers reading the field reference today miss real Opera posting patterns from the last month. That's the silent failure mode the project's CLAUDE.md explicitly forbids.

## Constraints (must hold)

- **Deterministic:** same snapshots in → same markdown out. Diffable.
- **Idempotent:** running the regenerator twice produces no change.
- **Atomic:** never leave a half-written rollup file. Either the new file lands or the old one stays.
- **Local == central by construction.** No "remember to copy". Drift must be impossible.
- **Handles deletions:** if a snapshot JSON is removed from central, the rollup reflects that.
- **CI-enforced:** PR blocked if rollup is stale relative to JSON inputs.

## Architecture

```
┌─────────────────────────────────────────┐
│ ~/opera-knowledge-ref/.../              │
│   transaction-library/*.json (69 files) │  ← canonical input
└────────────────────┬────────────────────┘
                     │
                     │ regenerate (deterministic)
                     ▼
┌─────────────────────────────────────────┐
│ scripts/regenerate_field_reference.py   │
│  • read all *.json sorted by name       │
│  • render markdown deterministically    │
│  • atomic write                         │
└────────────────────┬────────────────────┘
                     │
                     │ writes
                     ▼
┌─────────────────────────────────────────┐
│ ~/opera-knowledge-ref/.../              │
│   transaction-library/                  │
│   COMPLETE_FIELD_REFERENCE.md           │  ← single source of truth
└────────────────────┬────────────────────┘
                     │
                     │ symlinked from
                     ▼
┌─────────────────────────────────────────┐
│ apps/core/docs/                         │
│   opera_transaction_field_reference.md  │  ← symlink (NOT copy)
└─────────────────────────────────────────┘

CI gate:
  on every push, run regenerate, compare to committed file,
  fail if they differ.
```

## Components

### 1. `scripts/regenerate_field_reference.py` (new)

Pure function: reads JSON snapshots, writes markdown.

**Input:** every `.json` file in the transaction-library directory.

**Algorithm:**
1. Discover JSON files, sorted lexicographically (deterministic order).
2. For each, parse and validate against an expected schema (raise on malformed).
3. Group by module (cashbook, sales_ledger, purchase_ledger, nominal, gocardless, payroll, stock, sop, pop, customer_master, supplier_master).
4. Within each module, sort by name then timestamp (deterministic).
5. For each transaction:
   - Header: name, recorded_at, source.
   - Description (if any).
   - Per-table change summary: rows added, rows modified, fields changed.
   - For added rows: full JSON (max 3 examples shown, "+N more" marker).
   - For modified rows: field-by-field before/after table.
6. Render to markdown using a fixed template.
7. Atomic write (write to `.tmp`, fsync, rename).

**CLI:**
```
python scripts/regenerate_field_reference.py             # writes to default location
python scripts/regenerate_field_reference.py --check     # exit 1 if rollup stale, 0 if current
python scripts/regenerate_field_reference.py --output X  # explicit output path
```

**Determinism guarantees:**
- File ordering: lexicographic.
- Module ordering: fixed list in code.
- Within-module ordering: name (lexicographic) then timestamp.
- JSON pretty-printing in code blocks: `sort_keys=True`, `indent=2`.
- Field ordering in tables: lexicographic.
- No timestamps in output (would break determinism — only snapshot's `recorded_at` from input).

### 2. Local mirror via symlink

Replace the byte-copy at `apps/core/docs/opera_transaction_field_reference.md` with a **symlink** to the central file.

**Rationale:** drift is impossible by construction. Reading the local path always reads the central file. No manual copy step. No CI sync required.

**Caveat:** symlinks must work on dev machines (they do on macOS/Linux; Windows users need symlinks enabled in git config — already standard for this project).

**Setup script** at `scripts/setup_local_kb_mirror.py`:
- Detects if `~/opera-knowledge-ref/` is cloned.
- Replaces the local file with a symlink.
- Idempotent — safe to re-run.
- Failure mode: if central repo not cloned, prints clear instructions and exits.

### 3. CI gate

**GitHub Actions workflow** at `.github/workflows/snapshot-rollup-check.yml`:
- Triggers on PR to main and pushes to main.
- Steps:
  1. Clone the central knowledge repo (read-only).
  2. Run `python scripts/regenerate_field_reference.py --check`.
  3. If exit 1 → fail with message "Rollup stale. Run `scripts/regenerate_field_reference.py` and commit the result."
- Block merge on failure.

**This catches:** any new JSON snapshot committed to central without the rollup being regenerated.

### 4. Test suite

`tests/test_regenerate_field_reference.py`:
- **Fixture-based:** small fixed set of fake JSON snapshots → assert exact output markdown.
- **Determinism test:** run twice, assert byte-identical output.
- **Idempotency test:** run, run again, assert no change to file.
- **Atomicity test:** simulate a crash mid-write, assert old file intact.
- **Deletion test:** remove a fixture JSON, run, assert it's gone from rollup.
- **Schema validation test:** malformed JSON raises, no partial output.
- **Real-data smoke test:** run against actual central repo, assert expected modules present.

## Data flow

```
1. Snapshot captured: transaction_snapshot/after writes JSON to central
2. Developer commits + pushes JSON to opera-knowledge-ref
3. CI in opera-knowledge-ref runs regenerator on every push
4. Regenerator updates COMPLETE_FIELD_REFERENCE.md, commits the diff
5. Developer in main repo pulls central → both JSON and rollup are current
6. Local symlink reads the rollup directly — no separate sync needed
```

OR, alternative if we don't want CI in the central repo:

```
1. Developer captures snapshot via the app
2. App writes JSON to central + immediately runs regenerator
3. App commits both JSON and rollup in same commit (via git)
4. Developer pushes
```

**Decision:** start with the simpler "regenerate at capture time" — modify the snapshot-capture endpoint to invoke the regenerator after writing the JSON. CI gate in the main repo (this repo) verifies the rollup is current; if it's not, the developer ran out-of-sync and needs to regenerate.

## Error handling

- Central repo not cloned: regenerator prints clear instructions; doesn't crash silently.
- JSON parse error: name the bad file and exit 4. Don't write a partial rollup.
- File system errors during atomic write: roll back; old file intact.
- Symlink creation fails (Windows without symlinks): falls back to a one-shot copy with a clear warning. Documented as a setup limitation.

## Testing strategy

- Unit tests: fixtures + exact output assertions (above).
- Integration test: real central repo, run regenerator, assert known-good module list.
- CI dry-run: open a PR adding a fake snapshot JSON without regenerating; confirm CI blocks.

## Migration plan

1. Write tests for the regenerator (TDD).
2. Implement regenerator script.
3. Run regenerator manually; verify output is sensible (diff against current `COMPLETE_FIELD_REFERENCE.md` to spot any structural surprises).
4. Replace local file with symlink (via setup script).
5. Wire snapshot-capture endpoint to call regenerator after JSON write.
6. Add CI gate.
7. Catch-up: run the regenerator over the current 69 snapshots, commit the (likely large) updated rollup, push to central.
8. Verify CI blocks a deliberate test PR.

## Out of scope (separate spec if needed)

- **Automating updates to `schema/cashbook.md`, `business-rules/*.md`, etc.** — those add human interpretation that JSON cannot mechanically generate. They remain manually maintained, but updated as part of code-review per the KB update policy spec (#6).
- **A different rollup format** (e.g. per-module separate files instead of one giant file). Out of scope; if needed, separate spec.

## Done criteria

- [ ] All tests in `tests/test_regenerate_field_reference.py` pass.
- [ ] Running the regenerator on the current central repo produces a rollup that includes all 22+ post-Apr-6 snapshots.
- [ ] Local file at `apps/core/docs/opera_transaction_field_reference.md` is a symlink to the central file.
- [ ] Snapshot-capture endpoint runs the regenerator after JSON write.
- [ ] CI gate blocks a PR with stale rollup.
- [ ] No manual copy-paste step anywhere in the workflow.
- [ ] Documentation in central repo README points devs at the regenerator and CI behaviour.

## Risks / failure modes

- **Regenerator output is huge** (8,400+ lines today, will grow). **Mitigation:** keep deterministic + diffable; consider per-module split as a future spec if it becomes unwieldy.
- **Central repo CI conflict:** if both this repo and central repo have CI gates, they may race. **Mitigation:** central is the source of truth — CI in central wins, this repo's CI only verifies what it pulled is current.
- **Symlink unsupported on Windows:** documented; falls back to copy with warning.
