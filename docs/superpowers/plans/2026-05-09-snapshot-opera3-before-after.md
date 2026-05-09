# `snapshot_opera3.py` before/after Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `scripts/snapshot_opera3.py` to mirror `scripts/snapshot_opera.py`'s before/after diff workflow against an Opera 3 (FoxPro DBF) installation, with the install path supplied once via `--data-path` and persisted in the snapshot file.

**Architecture:** Single-file script, `argparse` with two subcommands (`before` / `after`). `before` requires `--data-path`, captures a full snapshot to `scripts/opera3_snapshot.json` (the saved data path is embedded). `after` re-reads that path from the saved snapshot, takes a fresh snapshot, runs the same Ratcliff/Obershelp-style diff used by `snapshot_opera.py`, and writes both snapshots plus the changes dict to `scripts/opera3_comparison_result.json`. The diff functions are copied verbatim from `snapshot_opera.py` rather than extracted to a shared module — both scripts stay self-contained admin tools.

**Tech Stack:** Python 3.9+, `argparse`, `json`, `pathlib`, `dbfread` (via existing `sql_rag.opera3_foxpro.Opera3Reader`). Tests via `pytest` (already in the project), no new dependencies.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `scripts/snapshot_opera3.py` | **Modify** | Add subcommand CLI, diff helpers, `before`/`after` modes |
| `tests/scripts/test_snapshot_opera3.py` | **Create** | Unit tests for diff helpers and CLI argument parsing |
| `docs/sam-rewrite/snapshot_opera3.md` | already updated in prior commit (84c8092) | Operating notes — keep aligned |

The script remains one self-contained file. Tests live in `tests/scripts/` (existing convention; the project already has `tests/` at the root).

---

### Task 1: Test scaffold + smoke import

**Files:**
- Create: `tests/scripts/__init__.py`
- Create: `tests/scripts/test_snapshot_opera3.py`

- [ ] **Step 1: Create empty package marker so pytest finds the test module**

```bash
mkdir -p tests/scripts
touch tests/scripts/__init__.py
```

- [ ] **Step 2: Write the smoke import test**

```python
# tests/scripts/test_snapshot_opera3.py
"""Tests for scripts/snapshot_opera3.py.

The script is a developer admin tool — full-fidelity coverage isn't
worth the bookkeeping. We test:
  - the diff helpers (pure functions, easy to fixture)
  - CLI argument parsing (catches breakage from refactors)

Actual DBF reading is verified by manual run against a real Opera 3
install per docs/sam-rewrite/snapshot_opera3.md.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[2] / 'scripts' / 'snapshot_opera3.py'


def load_module():
    """Load the script as a module so tests can call its functions."""
    spec = importlib.util.spec_from_file_location('snapshot_opera3', SCRIPT)
    assert spec and spec.loader, f'Could not load {SCRIPT}'
    mod = importlib.util.module_from_spec(spec)
    sys.modules['snapshot_opera3'] = mod
    spec.loader.exec_module(mod)
    return mod


def test_module_loads():
    mod = load_module()
    assert hasattr(mod, 'main'), 'snapshot_opera3.py must export main()'
```

- [ ] **Step 3: Run the test — expect it to pass against the current script**

Run: `pytest tests/scripts/test_snapshot_opera3.py::test_module_loads -v`
Expected: PASS (the script is already importable and exports `main`).

- [ ] **Step 4: Commit**

```bash
git add tests/scripts/__init__.py tests/scripts/test_snapshot_opera3.py
git commit -m "test: scaffold unit tests for snapshot_opera3.py"
```

---

### Task 2: Add the diff helpers (copied from snapshot_opera.py)

**Files:**
- Modify: `scripts/snapshot_opera3.py` (add module-level functions)
- Modify: `tests/scripts/test_snapshot_opera3.py` (add tests for the helpers)

- [ ] **Step 1: Write tests for `find_new_records`**

Add to `tests/scripts/test_snapshot_opera3.py`:

```python
def test_find_new_records_with_key_field():
    mod = load_module()
    before = [{'id': 1, 'name': 'A'}, {'id': 2, 'name': 'B'}]
    after = [{'id': 1, 'name': 'A'}, {'id': 2, 'name': 'B'}, {'id': 3, 'name': 'C'}]
    new = mod.find_new_records(before, after, 'id')
    assert new == [{'id': 3, 'name': 'C'}]


def test_find_new_records_no_key_field_falls_back_to_full_record_compare():
    mod = load_module()
    before = [{'name': 'A'}]
    after = [{'name': 'A'}, {'name': 'B'}]
    new = mod.find_new_records(before, after, None)
    assert new == [{'name': 'B'}]


def test_find_new_records_empty_before():
    mod = load_module()
    new = mod.find_new_records([], [{'id': 1}], 'id')
    assert new == [{'id': 1}]
```

- [ ] **Step 2: Run the tests — expect them to FAIL with `AttributeError`**

Run: `pytest tests/scripts/test_snapshot_opera3.py::test_find_new_records_with_key_field -v`
Expected: FAIL — `find_new_records` doesn't exist yet.

- [ ] **Step 3: Copy `find_new_records`, `find_modified_records`, `compare_snapshots` from `scripts/snapshot_opera.py`**

Read `scripts/snapshot_opera.py:119-216` for the canonical implementations and paste them into `scripts/snapshot_opera3.py` immediately after `guess_key_field`. They reference no other helpers in `snapshot_opera.py`, so the copy is clean.

For reference, here are the three functions verbatim:

```python
def find_new_records(before_records, after_records, key_field):
    if not key_field:
        before_set = {json.dumps(r, sort_keys=True) for r in before_records}
        return [r for r in after_records if json.dumps(r, sort_keys=True) not in before_set]
    before_keys = {r.get(key_field) for r in before_records}
    return [r for r in after_records if r.get(key_field) not in before_keys]


def find_modified_records(before_records, after_records, key_field):
    if not key_field:
        return []
    before_map = {r.get(key_field): r for r in before_records}
    after_map = {r.get(key_field): r for r in after_records}

    modified = []
    for key, after_record in after_map.items():
        if key in before_map:
            before_record = before_map[key]
            changes = {}
            for field in after_record:
                before_val = before_record.get(field)
                after_val = after_record.get(field)
                if before_val != after_val:
                    changes[field] = {'before': before_val, 'after': after_val}
            if changes:
                modified.append({'key': key, 'changes': changes})
    return modified


def compare_snapshots(before, after):
    print("\n" + "="*80)
    print("COMPARISON RESULTS")
    print("="*80)
    print(f"Before: {before['timestamp']}")
    print(f"After:  {after['timestamp']}")

    all_changes = {}

    for table_name in after['tables']:
        before_table = before['tables'].get(table_name, {'records': [], 'exists': False})
        after_table = after['tables'][table_name]

        if not after_table['exists']:
            continue

        key_field = after_table.get('key_field')

        new_records = find_new_records(
            before_table.get('records', []),
            after_table.get('records', []),
            key_field
        )

        modified_records = find_modified_records(
            before_table.get('records', []),
            after_table.get('records', []),
            key_field
        )

        if new_records or modified_records:
            all_changes[table_name] = {
                'new_records': new_records,
                'modified_records': modified_records
            }

    if not all_changes:
        print("\nNo changes detected!")
        return {}

    for table_name, changes in all_changes.items():
        print(f"\n{'='*80}")
        print(f"TABLE: {table_name}")
        print(f"{'='*80}")

        if changes['new_records']:
            print(f"\n  NEW RECORDS: {len(changes['new_records'])}")
            for i, record in enumerate(changes['new_records'], 1):
                print(f"\n  --- New Record #{i} ---")
                for field, value in sorted(record.items()):
                    if value is not None and value != '' and value != 0:
                        print(f"    {field}: {value}")

        if changes['modified_records']:
            print(f"\n  MODIFIED RECORDS: {len(changes['modified_records'])}")
            for mod in changes['modified_records']:
                print(f"\n  --- Modified Record (key={mod['key']}) ---")
                for field, change in sorted(mod['changes'].items()):
                    print(f"    {field}: {change['before']} -> {change['after']}")

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    for table_name, changes in all_changes.items():
        new_count = len(changes['new_records'])
        mod_count = len(changes['modified_records'])
        print(f"  {table_name}: {new_count} new, {mod_count} modified")

    return all_changes
```

- [ ] **Step 4: Run tests — expect PASS**

Run: `pytest tests/scripts/test_snapshot_opera3.py -v`
Expected: 4 tests pass (the smoke test from Task 1 + 3 new ones).

- [ ] **Step 5: Add tests for `find_modified_records`**

```python
def test_find_modified_records_detects_field_changes():
    mod = load_module()
    before = [{'id': 1, 'balance': 100, 'name': 'A'}]
    after = [{'id': 1, 'balance': 150, 'name': 'A'}]
    modified = mod.find_modified_records(before, after, 'id')
    assert modified == [
        {'key': 1, 'changes': {'balance': {'before': 100, 'after': 150}}}
    ]


def test_find_modified_records_no_changes_when_identical():
    mod = load_module()
    before = [{'id': 1, 'name': 'A'}]
    after = [{'id': 1, 'name': 'A'}]
    assert mod.find_modified_records(before, after, 'id') == []


def test_find_modified_records_returns_empty_without_key_field():
    mod = load_module()
    assert mod.find_modified_records([{'a': 1}], [{'a': 2}], None) == []
```

- [ ] **Step 6: Run tests — expect PASS**

Run: `pytest tests/scripts/test_snapshot_opera3.py -v`
Expected: 7 tests pass.

- [ ] **Step 7: Add a test for the orchestrator `compare_snapshots`**

```python
def test_compare_snapshots_returns_only_changed_tables(capsys):
    mod = load_module()
    before = {
        'timestamp': '2026-01-01T00:00:00',
        'tables': {
            'pname': {
                'exists': True, 'key_field': 'id',
                'records': [{'id': 1, 'name': 'A'}],
            },
            'untouched': {
                'exists': True, 'key_field': 'id',
                'records': [{'id': 1}],
            },
        },
    }
    after = {
        'timestamp': '2026-01-01T01:00:00',
        'tables': {
            'pname': {
                'exists': True, 'key_field': 'id',
                'records': [{'id': 1, 'name': 'A'}, {'id': 2, 'name': 'B'}],
            },
            'untouched': {
                'exists': True, 'key_field': 'id',
                'records': [{'id': 1}],
            },
        },
    }
    changes = mod.compare_snapshots(before, after)
    assert set(changes.keys()) == {'pname'}
    assert changes['pname']['new_records'] == [{'id': 2, 'name': 'B'}]
    assert changes['pname']['modified_records'] == []
```

- [ ] **Step 8: Run tests — expect PASS**

Run: `pytest tests/scripts/test_snapshot_opera3.py -v`
Expected: 8 tests pass.

- [ ] **Step 9: Commit**

```bash
git add scripts/snapshot_opera3.py tests/scripts/test_snapshot_opera3.py
git commit -m "feat: copy diff helpers from snapshot_opera.py into snapshot_opera3.py"
```

---

### Task 3: Switch CLI to subcommand-based with `before` / `after`

**Files:**
- Modify: `scripts/snapshot_opera3.py` — replace positional `data_path` with subcommand CLI
- Modify: `tests/scripts/test_snapshot_opera3.py` — argparse tests

- [ ] **Step 1: Write the test for the new argparse shape**

Add to `tests/scripts/test_snapshot_opera3.py`:

```python
def test_argparse_before_requires_data_path():
    mod = load_module()
    parser = mod.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(['before'])  # --data-path missing


def test_argparse_before_accepts_data_path_and_limit():
    mod = load_module()
    parser = mod.build_parser()
    args = parser.parse_args(['before', '--data-path', '/tmp/x', '--limit', '50'])
    assert args.mode == 'before'
    assert args.data_path == '/tmp/x'
    assert args.limit == 50


def test_argparse_after_data_path_optional():
    mod = load_module()
    parser = mod.build_parser()
    args = parser.parse_args(['after'])
    assert args.mode == 'after'
    assert args.data_path is None


def test_argparse_after_accepts_data_path_override():
    mod = load_module()
    parser = mod.build_parser()
    args = parser.parse_args(['after', '--data-path', '/tmp/x'])
    assert args.data_path == '/tmp/x'
```

- [ ] **Step 2: Run tests — expect them to FAIL with `AttributeError: build_parser`**

Run: `pytest tests/scripts/test_snapshot_opera3.py -v`
Expected: 4 new tests fail; rest still pass.

- [ ] **Step 3: Replace the old `main()` with a subcommand parser**

In `scripts/snapshot_opera3.py`, replace the existing `main()` (currently a single positional `data_path` parser) with the following. Add `build_parser()` as a separate function so tests can inspect it without invoking `main()`.

```python
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Snapshot an Opera 3 (FoxPro) installation. '
                    'Companion to snapshot_opera.py for SQL Server SE.'
    )
    sub = parser.add_subparsers(dest='mode', required=True)

    before = sub.add_parser('before', help='Take the before snapshot')
    before.add_argument(
        '--data-path',
        required=True,
        help='Path to the Opera 3 data folder (e.g. /mnt/opera3/DATA)',
    )
    before.add_argument(
        '--output',
        default=str(DEFAULT_OUTPUT),
        help=f'Output JSON path (default: {DEFAULT_OUTPUT})',
    )
    before.add_argument(
        '--limit',
        type=int,
        default=500,
        help='Max records to sample per table (default: 500)',
    )

    after = sub.add_parser('after', help='Take the after snapshot and diff')
    after.add_argument(
        '--data-path',
        default=None,
        help='Override saved data path (errors if it differs from before).',
    )
    after.add_argument(
        '--output',
        default=str(DEFAULT_OUTPUT),
        help=f'Before-snapshot JSON path (default: {DEFAULT_OUTPUT})',
    )
    after.add_argument(
        '--comparison-output',
        default=str(DEFAULT_COMPARISON),
        help=f'Comparison JSON path (default: {DEFAULT_COMPARISON})',
    )
    after.add_argument(
        '--limit',
        type=int,
        default=500,
        help='Max records to sample per table (default: 500)',
    )

    return parser
```

Add the `DEFAULT_COMPARISON` constant near `DEFAULT_OUTPUT` (top of the file):

```python
DEFAULT_COMPARISON = Path(__file__).parent / 'opera3_comparison_result.json'
```

Then rewrite `main()`:

```python
def main():
    args = build_parser().parse_args()

    if not DBF_AVAILABLE:
        print('dbfread is not installed. Run: pip install dbfread', file=sys.stderr)
        sys.exit(2)

    if args.mode == 'before':
        run_before(args)
    elif args.mode == 'after':
        run_after(args)
    else:
        # argparse with required=True will already have errored
        sys.exit(2)
```

Leave `run_before` and `run_after` undefined for now — Task 4 implements them.

- [ ] **Step 4: Run argparse tests — expect PASS, but the script can't actually run**

Run: `pytest tests/scripts/test_snapshot_opera3.py -v`
Expected: 12 tests pass (4 smoke + 7 helpers + ... wait, count again: 1 smoke + 6 helpers + 4 argparse + 1 compare = 12).

- [ ] **Step 5: Commit**

```bash
git add scripts/snapshot_opera3.py tests/scripts/test_snapshot_opera3.py
git commit -m "refactor: switch snapshot_opera3.py CLI to before/after subcommands"
```

---

### Task 4: Implement `before` and `after` modes

**Files:**
- Modify: `scripts/snapshot_opera3.py` — add `run_before` and `run_after`
- Modify: `tests/scripts/test_snapshot_opera3.py` — integration tests using fixtures

- [ ] **Step 1: Write integration tests for the saved-snapshot validation logic**

Add to `tests/scripts/test_snapshot_opera3.py`:

```python
import json as _json
import tempfile


def _make_snapshot(data_path: str, limit: int = 500) -> dict:
    return {
        'timestamp': '2026-01-01T00:00:00',
        'data_path': data_path,
        'engine': 'foxpro',
        'mode': 'before',
        'limit': limit,
        'tables': {
            'pname': {
                'exists': True,
                'columns': ['id', 'name'],
                'field_types': {'id': 'N', 'name': 'C'},
                'records': [{'id': 1, 'name': 'A'}],
                'count': 1,
                'sampled': 1,
                'key_field': 'id',
                'last_modified': '2026-01-01T00:00:00',
            }
        },
    }


def test_validate_data_path_match_succeeds(tmp_path):
    mod = load_module()
    saved = tmp_path / 'before.json'
    saved.write_text(_json.dumps(_make_snapshot('/mnt/o3')))
    # No override — accepted
    assert mod.validate_after_path(str(saved), None) == '/mnt/o3'
    # Override matches — accepted
    assert mod.validate_after_path(str(saved), '/mnt/o3') == '/mnt/o3'


def test_validate_data_path_mismatch_errors(tmp_path):
    mod = load_module()
    saved = tmp_path / 'before.json'
    saved.write_text(_json.dumps(_make_snapshot('/mnt/o3')))
    with pytest.raises(SystemExit) as exc:
        mod.validate_after_path(str(saved), '/mnt/different')
    assert exc.value.code == 2


def test_validate_after_errors_when_before_missing(tmp_path):
    mod = load_module()
    saved = tmp_path / 'missing.json'
    with pytest.raises(SystemExit) as exc:
        mod.validate_after_path(str(saved), None)
    assert exc.value.code == 2
```

- [ ] **Step 2: Run tests — expect FAIL with `AttributeError: validate_after_path`**

Run: `pytest tests/scripts/test_snapshot_opera3.py -v`
Expected: 3 new tests fail.

- [ ] **Step 3: Implement `validate_after_path`**

Add to `scripts/snapshot_opera3.py` between `compare_snapshots` and `main`:

```python
def validate_after_path(saved_snapshot_path: str, override: str | None) -> str:
    """Resolve the data path for `after` mode.

    Reads the saved-snapshot JSON to find the path used by `before`.
    If the caller supplies --data-path, it must match; mismatches
    error out so the user can't accidentally diff different
    installations.

    Returns the data path that should be used.
    """
    saved = Path(saved_snapshot_path)
    if not saved.exists():
        print(
            f'No before snapshot found at {saved}. Run `before` first.',
            file=sys.stderr,
        )
        sys.exit(2)
    try:
        snapshot = json.loads(saved.read_text())
    except json.JSONDecodeError as e:
        print(f'Could not parse {saved}: {e}', file=sys.stderr)
        sys.exit(2)
    saved_path = snapshot.get('data_path')
    if not saved_path:
        print(
            f'Saved snapshot is missing data_path. Re-run `before`.',
            file=sys.stderr,
        )
        sys.exit(2)
    if override is not None and override != saved_path:
        print(
            f'--data-path mismatch: saved={saved_path!r}, '
            f'override={override!r}. Re-run `before` if you want to '
            f'snapshot a different installation.',
            file=sys.stderr,
        )
        sys.exit(2)
    return saved_path
```

- [ ] **Step 4: Run tests — expect PASS**

Run: `pytest tests/scripts/test_snapshot_opera3.py -v`
Expected: all 15 tests pass.

- [ ] **Step 5: Implement `run_before`**

Add to `scripts/snapshot_opera3.py`:

```python
def run_before(args) -> None:
    data_path = Path(args.data_path)
    if not data_path.exists():
        print(f'Data path does not exist: {data_path}', file=sys.stderr)
        sys.exit(2)

    print(f'Taking BEFORE snapshot of {data_path}')
    reader = Opera3Reader(str(data_path))
    snapshot = snapshot_all_tables(reader, limit=args.limit)
    snapshot['mode'] = 'before'
    snapshot['limit'] = args.limit

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(snapshot, indent=2, default=serialize_value))
    print(f'\nBefore snapshot saved to: {output}')
    print('\nNow perform the transaction in Opera 3, then run:')
    print(f'  python {Path(__file__).name} after')
```

- [ ] **Step 6: Implement `run_after`**

Add to `scripts/snapshot_opera3.py`:

```python
def run_after(args) -> None:
    data_path = validate_after_path(args.output, args.data_path)
    print(f'Loading before snapshot from {args.output}')
    before = json.loads(Path(args.output).read_text())

    target = Path(data_path)
    if not target.exists():
        print(f'Data path no longer exists: {target}', file=sys.stderr)
        sys.exit(2)

    print(f'Taking AFTER snapshot of {target}')
    reader = Opera3Reader(str(target))
    after = snapshot_all_tables(reader, limit=args.limit)
    after['mode'] = 'after'
    after['limit'] = args.limit

    changes = compare_snapshots(before, after)

    comparison_output = Path(args.comparison_output)
    comparison_output.parent.mkdir(parents=True, exist_ok=True)
    comparison_output.write_text(
        json.dumps(
            {'before': before, 'after': after, 'changes': changes},
            indent=2,
            default=serialize_value,
        )
    )
    print(f'\nFull comparison saved to: {comparison_output}')
```

- [ ] **Step 7: Replace the old `snapshot_all_tables` print prefix**

The current `snapshot_all_tables` prints "Snapshotting Opera 3 data from ...". Now that `run_before` / `run_after` print their own per-mode banner, that line is redundant. Remove the early "Snapshotting Opera 3 data from ..." print at the top of `snapshot_all_tables` (currently at the start of the function); leave the per-table progress prints unchanged.

For clarity, the helper now reads:

```python
def snapshot_all_tables(reader: Opera3Reader, limit: int = 500):
    table_listings = reader.list_tables(include_unknown=True)
    print(f"  Found {len(table_listings)} DBF tables in {reader.data_path}")
    # ... existing per-table loop unchanged ...
```

- [ ] **Step 8: Remove the obsolete top-level orchestration code**

The old `main()` body (after replacement in Task 3) referenced no helpers besides `snapshot_all_tables`. Confirm:

Run: `pytest tests/scripts/test_snapshot_opera3.py -v`
Expected: all 15 tests still pass.

Run: `python scripts/snapshot_opera3.py --help`
Expected: argparse prints `before` and `after` subcommands with the documented options.

Run: `python scripts/snapshot_opera3.py before --help`
Expected: shows `--data-path` (required), `--output`, `--limit`.

Run: `python scripts/snapshot_opera3.py after --help`
Expected: shows `--data-path` (optional), `--output`, `--comparison-output`, `--limit`.

- [ ] **Step 9: Commit**

```bash
git add scripts/snapshot_opera3.py tests/scripts/test_snapshot_opera3.py
git commit -m "feat: snapshot_opera3.py before/after with --data-path"
```

---

### Task 5: Final smoke check + doc cross-link

**Files:**
- Modify: `scripts/snapshot_opera3.py` (only if smoke check finds anything)
- Modify: `docs/sam-rewrite/snapshot_opera3.md` (add link to the central INDEX if missing)

- [ ] **Step 1: Static lint pass**

Run: `python -m py_compile scripts/snapshot_opera3.py`
Expected: no output (script parses cleanly).

Run: `python -c "import ast; ast.parse(open('scripts/snapshot_opera3.py').read())"`
Expected: no output (defensive double-check).

- [ ] **Step 2: Negative-path smoke check via the CLI**

Run: `python scripts/snapshot_opera3.py before --data-path /does/not/exist 2>&1 | head -3`
Expected: `Data path does not exist: /does/not/exist`, exit code non-zero.

```bash
python scripts/snapshot_opera3.py before --data-path /does/not/exist
echo "exit=$?"
```
Expected: `exit=2`.

Run `after` with no before snapshot:

```bash
mv scripts/opera3_snapshot.json scripts/opera3_snapshot.json.bak 2>/dev/null || true
python scripts/snapshot_opera3.py after 2>&1 | head -3
echo "exit=$?"
mv scripts/opera3_snapshot.json.bak scripts/opera3_snapshot.json 2>/dev/null || true
```
Expected: `No before snapshot found at scripts/opera3_snapshot.json. Run \`before\` first.`, exit code 2.

- [ ] **Step 3: Verify docs/sam-rewrite/snapshot_opera3.md still references the right paths**

Run: `grep -E '(scripts/snapshot_opera3|--data-path|opera3_comparison_result)' docs/sam-rewrite/snapshot_opera3.md`
Expected: matches show the correct paths and CLI shape consistent with the implementation.

If anything's out of date (e.g. references the removed positional `data_path` argument), patch the doc inline to match. The doc was already updated in commit `84c8092` to describe the subcommand CLI, so it should be accurate.

- [ ] **Step 4: Final test run**

Run: `pytest tests/scripts/test_snapshot_opera3.py -v`
Expected: 15 tests pass.

- [ ] **Step 5: Commit if any doc fixes were needed**

```bash
# If grep showed stale references and you patched them:
git add docs/sam-rewrite/snapshot_opera3.md
git commit -m "docs: align snapshot_opera3.md with final CLI shape"

# If no changes were needed, skip this commit.
```

---

## Self-review

**Spec coverage:** Walked through the spec deleted in commit `84c8092`. The CLI shape (subcommands, `--data-path` flag, persistence, mismatch error), state files (`opera3_snapshot.json`, `opera3_comparison_result.json`), diff functions copied from `snapshot_opera.py`, and validation behaviour are all present in the tasks above. The "Tests" section of the spec ("manual run against a real Opera 3 install") is documented in `docs/sam-rewrite/snapshot_opera3.md` and isn't part of the automated plan because no Opera 3 install is reachable in CI.

**Placeholder scan:** No "TBD", "TODO", or "appropriate error handling" left over. Every step shows the exact code or command. Function names and signatures are consistent across tasks (`build_parser`, `run_before`, `run_after`, `validate_after_path`, `serialize_value`, `guess_key_field`, `find_new_records`, `find_modified_records`, `compare_snapshots`, `snapshot_all_tables`, `snapshot_table`).

**Type consistency:** `args.data_path`, `args.limit`, `args.output`, `args.comparison_output`, `args.mode` are referenced consistently. `DEFAULT_OUTPUT` and `DEFAULT_COMPARISON` are both `Path` objects. The `validate_after_path` signature accepts `str | None` for the override and returns the resolved data path as `str`.
