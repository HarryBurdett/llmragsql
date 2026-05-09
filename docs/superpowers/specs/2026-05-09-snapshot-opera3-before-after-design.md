# Design: `snapshot_opera3.py` — before/after with `--data-path`

## Context

`scripts/snapshot_opera.py` already exists for Opera SE. It uses
hard-coded MSSQL credentials and supports a `before`/`after` workflow:

1. Run `before` — snapshot every table to `scripts/opera_snapshot.json`.
2. User performs a transaction in Opera (post a sales receipt, allocate
   a payment, etc.).
3. Run `after` — snapshot again, compare against `before`, print a
   per-table diff of new and modified records.

The diff is the actual product. It tells the developer exactly which
tables and which fields a transaction touches, which lets them
replicate the transaction in code (e.g., the SAM TS rewrite).

`scripts/snapshot_opera3.py` (current) only does the snapshot half —
no `before`/`after`, no diff. It also takes a positional `data_path`
which has to be retyped on every run.

## Goal

Bring the Opera 3 snapshot tool to feature parity with the SE one,
preserving the same workflow and diff output, while supporting
exactly one Opera 3 installation pointed at via a `--data-path` flag
that the user sets once.

## Non-goals

- Multi-company / seqco-based discovery (single install assumption).
- Reusing `companies/*.json` config files.
- Multi-host or remote installations.
- Live SMB mounting (assumes the path is already accessible).
- Writing back to Opera 3 (read-only).

## CLI

```
python scripts/snapshot_opera3.py before --data-path <path> [--limit N]
python scripts/snapshot_opera3.py after  [--data-path <path>] [--limit N]
```

- `before` requires `--data-path`; `--limit` defaults to 500.
- `after` reads `--data-path` from the before snapshot file. If the
  user passes `--data-path` to `after` and it differs from the saved
  one, the script errors out and tells them to re-run `before`. This
  catches the "snapshotted company A, then ran a transaction in
  company B and tried to diff" mistake.
- No positional `data_path` argument anymore (was a single positional
  in the current tool; replaced by the explicit flag tied to `before`).

## State files

- `scripts/opera3_snapshot.json` — the before snapshot (overwritten on
  each `before`). Same shape as today's tool's output, plus the
  `data_path` and `limit` fields used to validate `after`.
- `scripts/opera3_comparison_result.json` — full `before + after + changes`
  written on each `after` run, for offline review. Mirrors
  `scripts/comparison_result.json` from the SE tool.

Both files live in `scripts/` (git-ignored), so customer-sample data
never enters source control.

## Diff logic

Identical to SE's `compare_snapshots`:

For each table present in the after snapshot:

1. Detect the key field via the existing `guess_key_field` heuristic
   (`_unique`, `_pstid`, `_entry`, `_jrnl`, `_id`, `_account`,
   `_acnt`, `_code` suffix). Falls back to first column.
2. **New records**: rows whose key value isn't in the before set.
3. **Modified records**: rows whose key matches a before row but
   whose values differ in any field.

Output per table:

```
================================================================================
TABLE: stran
================================================================================
  NEW RECORDS: 1
  --- New Record #1 ---
    st_account: CUST01
    st_trref: INV12345
    ...
  MODIFIED RECORDS: 1
  --- Modified Record (key=BANK01) ---
    nk_curbal: 100000 -> 105000
SUMMARY
  stran: 1 new, 0 modified
  nbank: 0 new, 1 modified
  ...
```

Tables with no changes are omitted from the per-table sections but
appear in summary only if they had any change.

## Reuse

Six functions are shared with `scripts/snapshot_opera.py`:
`serialize_value`, `guess_key_field`, `find_new_records`,
`find_modified_records`, `compare_snapshots`, plus the per-table
output formatting.

The cleanest approach is to copy the diff functions into
`snapshot_opera3.py` rather than introduce an import boundary
between the two scripts. They're standalone admin scripts and
duplication of ~80 LOC is fine; both files stay self-contained and
the SE one doesn't need touching. (If divergence appears later,
extract a shared `scripts/_snapshot_common.py`.)

## Validation

- `--data-path` must point to an existing directory. Error and exit
  if not.
- `dbfread` must be importable. Error and exit if not.
- `before` snapshot file must exist before `after`. Error if missing.
- After read, the saved snapshot's `data_path` is checked against the
  `--data-path` override (if any). Mismatch errors out.

## Output framing

Every snapshot JSON gains:

- `mode`: `"before"` or `"after"`
- `limit`: the sample limit used
- `data_path`: already present

The comparison result JSON additionally gains:

- `changes`: the per-table diff dict produced by `compare_snapshots`

## Tests

This is a small admin script — formal automated tests aren't worth
the bookkeeping. Validation is by manual run:

1. Take a `before` against a known Opera 3 install.
2. Post a known transaction (e.g., a sales receipt for £100 against
   a test customer) via the Opera 3 UI.
3. Run `after` and verify the diff shows the expected new rows in
   `stran` (sales transaction), `aentry`/`atran` (cashbook entry),
   `ntran` (nominal posting), and `sname.sn_currbal` updated.
4. Repeat for two more transaction types (purchase payment, journal)
   to confirm the diff catches each pattern correctly.

If anything misbehaves with FoxPro-specific data types or DBF read
edge cases, fix it in `Opera3FieldParser` (in
`sql_rag/opera3_foxpro.py`) which is shared with the runtime code.

## Out of scope

- Subset-by-table snapshotting (e.g., only sales-side tables).
- Multi-step diff (before → mid → after).
- HTML or markdown diff output. Plain stdout only, mirroring SE.
- Integration with the SAM rewrite TS code at runtime. This is a
  developer tool, not part of any plugin code path.
