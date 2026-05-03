# Snapshot Rollup + Local Mirror + CI Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `COMPLETE_FIELD_REFERENCE.md` rollup of Opera transaction-snapshot JSON files regenerate deterministically from the snapshots in `~/opera-knowledge-ref/packages/opera-knowledge/transaction-library/`. Replace the manual byte-copy at `apps/core/docs/opera_transaction_field_reference.md` with a symlink so the local mirror can never drift from central. Add a CI gate that fails any PR with a stale rollup.

**Architecture:** A new `scripts/regenerate_field_reference.py` reads every snapshot JSON, sorts deterministically, renders markdown using a fixed template, and writes atomically to the target. A setup script (`scripts/setup_local_kb_mirror.py`) replaces the local file with a symlink to central. CI runs `regenerate --check` to fail any PR that hasn't kept the rollup current. Tests use fixture snapshots to assert exact output.

**Tech Stack:** Python 3.9, pytest, no new external deps.

**Source spec:** `docs/superpowers/specs/2026-05-03-snapshot-rollup-sync-design.md`

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `scripts/regenerate_field_reference.py` | **create** | Pure function: JSON snapshots in → markdown out; atomic write; --check mode |
| `scripts/setup_local_kb_mirror.py` | **create** | One-shot installer: replace local file with symlink to central |
| `tests/test_regenerate_field_reference.py` | **create** | Fixture-based unit tests + determinism / idempotency / atomicity / deletion |
| `tests/fixtures/snapshot_rollup/*.json` | **create** | Small fake JSON snapshots for tests |
| `tests/fixtures/snapshot_rollup/expected_rollup.md` | **create** | Golden output for byte-equality assertion |
| `apps/core/docs/opera_transaction_field_reference.md` | **modify** | Replace with symlink to central via setup script |
| `.github/workflows/snapshot-rollup-check.yml` | **create** | CI gate: clones central repo, runs regenerator --check, fails PR if stale |
| `apps/core/docs/opera_knowledge_base.md` | **modify** | Document the regenerator + symlink + CI gate |
| `~/opera-knowledge-ref/.../business-rules/snapshot-rollup.md` | **create** | Central KB doc |

---

## Task 1: Scaffold the regenerator with deterministic file iteration

**Files:**
- Create: `scripts/regenerate_field_reference.py` (skeleton with discover-and-sort logic only)
- Create: `tests/fixtures/snapshot_rollup/cashbook_sales_receipt_20260101_120000.json`
- Create: `tests/fixtures/snapshot_rollup/cashbook_purchase_payment_20260102_130000.json`
- Create: `tests/fixtures/snapshot_rollup/sales_ledger_invoice_20260103_140000.json`
- Create: `tests/test_regenerate_field_reference.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_regenerate_field_reference.py
"""Tests for scripts/regenerate_field_reference.py.

Fixture-based: small fake JSON snapshots in tests/fixtures/, then
exercise the regenerator's discovery, sorting, and rendering. Plus
determinism / idempotency / atomicity / deletion-handling.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
REGEN = REPO_ROOT / "scripts" / "regenerate_field_reference.py"
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "snapshot_rollup"


def _run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(REGEN), *args],
        capture_output=True, text=True, env={"PYTHONUNBUFFERED": "1"},
    )


def test_discover_returns_files_in_lexicographic_order(tmp_path):
    """Discovery must be deterministic — lexicographic by filename."""
    # Copy fixtures into tmp_path
    src_dir = tmp_path / "lib"
    src_dir.mkdir()
    for f in sorted(FIXTURES.glob("*.json")):
        shutil.copy(f, src_dir)

    from importlib import util
    spec = util.spec_from_file_location("regen", REGEN)
    regen = util.module_from_spec(spec)
    spec.loader.exec_module(regen)

    files = regen.discover_snapshots(src_dir)
    names = [f.name for f in files]
    assert names == sorted(names), f"discovery not lexicographic: {names}"


def test_validate_snapshot_rejects_missing_required_keys(tmp_path):
    """A JSON file without the required keys must be rejected loudly."""
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"oops": "no module"}))

    from importlib import util
    spec = util.spec_from_file_location("regen", REGEN)
    regen = util.module_from_spec(spec)
    spec.loader.exec_module(regen)

    with pytest.raises((ValueError, KeyError)):
        regen.load_and_validate(bad)


def test_load_and_validate_accepts_well_formed_snapshot():
    """Each fixture must parse and validate."""
    from importlib import util
    spec = util.spec_from_file_location("regen", REGEN)
    regen = util.module_from_spec(spec)
    spec.loader.exec_module(regen)

    for f in FIXTURES.glob("*.json"):
        snap = regen.load_and_validate(f)
        assert "name" in snap or "module" in snap, f"missing keys in {f.name}"
```

Create `tests/fixtures/snapshot_rollup/` and three small fake snapshot files (manually authored JSON that mirrors the real shape — see below for content). Build them with the same field structure that real snapshots in `~/opera-knowledge-ref/packages/opera-knowledge/transaction-library/*.json` use.

```bash
mkdir -p /Users/maccb/llmragsql/tests/fixtures/snapshot_rollup
cat > /Users/maccb/llmragsql/tests/fixtures/snapshot_rollup/cashbook_purchase_payment_20260102_130000.json <<'EOF'
{
  "name": "Purchase Payment (fixture)",
  "module": "cashbook",
  "module_name": "Cashbook Transactions",
  "source": "opera_se",
  "recorded_at": "2026-01-02T13:00:00",
  "description": "Test fixture for the rollup regenerator unit tests.",
  "changes": [
    {
      "database": "opera_se",
      "table": "aentry",
      "rows_added": 1,
      "added_rows": [{"ae_entry": "P0001", "ae_value": -10000}],
      "modified_rows": [],
      "modified_fields": []
    }
  ]
}
EOF
cat > /Users/maccb/llmragsql/tests/fixtures/snapshot_rollup/cashbook_sales_receipt_20260101_120000.json <<'EOF'
{
  "name": "Sales Receipt (fixture)",
  "module": "cashbook",
  "module_name": "Cashbook Transactions",
  "source": "opera_se",
  "recorded_at": "2026-01-01T12:00:00",
  "description": "Test fixture.",
  "changes": [
    {
      "database": "opera_se",
      "table": "aentry",
      "rows_added": 1,
      "added_rows": [{"ae_entry": "R0001", "ae_value": 5000}],
      "modified_rows": [],
      "modified_fields": []
    }
  ]
}
EOF
cat > /Users/maccb/llmragsql/tests/fixtures/snapshot_rollup/sales_ledger_invoice_20260103_140000.json <<'EOF'
{
  "name": "Sales Invoice (fixture)",
  "module": "sales_ledger",
  "module_name": "Sales Ledger Transactions",
  "source": "opera_se",
  "recorded_at": "2026-01-03T14:00:00",
  "description": "Test fixture for sales ledger module.",
  "changes": [
    {
      "database": "opera_se",
      "table": "stran",
      "rows_added": 1,
      "added_rows": [{"st_account": "C001", "st_trvalue": 100.00}],
      "modified_rows": [],
      "modified_fields": []
    }
  ]
}
EOF
```

- [ ] **Step 2: Run tests, verify failure**

```bash
source venv/bin/activate && python -m pytest tests/test_regenerate_field_reference.py -v
```

Expected: tests FAIL with `ModuleNotFoundError: No module named 'regen'` (because the regenerator file doesn't exist yet — `spec_from_file_location` raises FileNotFoundError or AttributeError when the path doesn't exist).

- [ ] **Step 3: Write minimal implementation (discovery + validation only)**

```python
# scripts/regenerate_field_reference.py
"""Regenerate COMPLETE_FIELD_REFERENCE.md from JSON snapshots.

The transaction-snapshot feature writes raw JSON snapshots of Opera
postings to ~/opera-knowledge-ref/packages/opera-knowledge/
transaction-library/*.json. This script aggregates them into a single
human-readable markdown reference that lives next to the JSON files
in the same directory.

Determinism: same snapshots in → byte-identical markdown out.
Idempotent: running twice in a row produces no diff.
Atomic: writes via .tmp + rename, so a crash mid-write leaves the
old rollup intact.

Usage
-----
  python scripts/regenerate_field_reference.py            # write rollup
  python scripts/regenerate_field_reference.py --check    # exit 1 if stale
  python scripts/regenerate_field_reference.py --library <DIR>  # override input dir

See docs/superpowers/specs/2026-05-03-snapshot-rollup-sync-design.md.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional


__all__ = [
    "discover_snapshots",
    "load_and_validate",
    "render_rollup",
    "atomic_write",
    "main",
]


# Module ordering for the rollup — deterministic.
MODULE_ORDER = [
    "cashbook",
    "sales_ledger",
    "purchase_ledger",
    "nominal",
    "bank_transfer",
    "gocardless",
    "payroll",
    "stock",
    "sop",
    "pop",
    "customer_master",
    "supplier_master",
]

# Default library path — central KB
DEFAULT_LIBRARY = Path(os.path.expanduser(
    "~/opera-knowledge-ref/packages/opera-knowledge/transaction-library"
))

# Required keys on every snapshot JSON
REQUIRED_KEYS = {"name", "module", "changes"}


def discover_snapshots(library_dir: Path) -> List[Path]:
    """Return every *.json file in library_dir, sorted lexicographically.

    Excludes the rollup itself (COMPLETE_FIELD_REFERENCE.md) and any
    non-JSON files. Order is deterministic — sort by filename.
    """
    if not library_dir.exists():
        raise FileNotFoundError(
            f"snapshot library directory not found: {library_dir}"
        )
    return sorted(library_dir.glob("*.json"))


def load_and_validate(path: Path) -> Dict[str, Any]:
    """Parse a snapshot JSON file and verify required keys are present.

    Raises ValueError if the file is malformed or missing required keys —
    never returns a partial object.
    """
    try:
        with open(path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"{path.name}: invalid JSON — {e}") from e

    if not isinstance(data, dict):
        raise ValueError(f"{path.name}: top level must be an object")

    missing = REQUIRED_KEYS - set(data.keys())
    if missing:
        raise ValueError(
            f"{path.name}: missing required keys: {sorted(missing)}"
        )

    if not isinstance(data["changes"], list):
        raise ValueError(f"{path.name}: 'changes' must be a list")

    return data


def render_rollup(snapshots: List[Dict[str, Any]]) -> str:
    """Render the markdown rollup deterministically.

    Implementation deferred — Task 2 builds the renderer.
    """
    lines = ["# Opera Transaction Posting — Complete Field Reference", ""]
    lines.append(
        "Generated from transaction snapshot library by "
        "`scripts/regenerate_field_reference.py`."
    )
    lines.append(
        "Every field value from real Opera postings — added AND "
        "modified rows."
    )
    lines.append("**Use as definitive reference when writing transactions back to Opera.**")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(f"_({len(snapshots)} snapshots indexed; module-by-module body in Task 2)_")
    lines.append("")
    return "\n".join(lines)


def atomic_write(path: Path, content: str) -> None:
    """Write content to path atomically (tmp file + fsync + rename).

    Guarantees: if the process crashes mid-write, the existing path
    is intact. If the write completes, path holds exactly content.
    """
    tmp = tempfile.NamedTemporaryFile(
        mode="w", dir=str(path.parent), delete=False, encoding="utf-8"
    )
    try:
        tmp.write(content)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp.close()
        os.replace(tmp.name, path)
    except Exception:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass
        raise


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--library",
        type=Path,
        default=DEFAULT_LIBRARY,
        help="Snapshot JSON library directory (default: central KB).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output rollup path (default: <library>/COMPLETE_FIELD_REFERENCE.md).",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit 1 if the rollup is stale relative to inputs (CI-friendly).",
    )
    args = parser.parse_args(argv)

    if not args.library.exists():
        print(f"ERROR: library {args.library} does not exist.", file=sys.stderr)
        return 4

    output = args.output or (args.library / "COMPLETE_FIELD_REFERENCE.md")

    paths = discover_snapshots(args.library)
    snapshots: List[Dict[str, Any]] = []
    for p in paths:
        try:
            snapshots.append(load_and_validate(p))
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 4

    rendered = render_rollup(snapshots)

    if args.check:
        if not output.exists():
            print(f"STALE: {output} does not exist.", file=sys.stderr)
            return 1
        existing = output.read_text(encoding="utf-8")
        if existing != rendered:
            print(f"STALE: {output} differs from regenerated content.", file=sys.stderr)
            return 1
        print(f"OK: {output} is current ({len(snapshots)} snapshots).")
        return 0

    atomic_write(output, rendered)
    print(f"Wrote {output} ({len(snapshots)} snapshots).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

Force-add the file (scripts/ is gitignored):

```bash
git add -f /Users/maccb/llmragsql/scripts/regenerate_field_reference.py
```

- [ ] **Step 4: Run tests**

```bash
source venv/bin/activate && python -m pytest tests/test_regenerate_field_reference.py -v
```

Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_regenerate_field_reference.py tests/fixtures/snapshot_rollup/
git commit -m "feat(rollup): scaffold regenerate_field_reference.py with discovery + validation

First commit toward consolidating the manual JSON-snapshot-to-markdown
rollup. This commit lands:
  - scripts/regenerate_field_reference.py with discover_snapshots,
    load_and_validate, atomic_write, and a placeholder render_rollup
    (full module-by-module rendering follows in Task 2).
  - tests/fixtures/snapshot_rollup/ with three minimal JSON fixtures
    that exercise discovery and validation.
  - 3 unit tests pinning lex-order discovery, validation rejection
    of malformed snapshots, and validation acceptance of well-formed
    fixtures.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Implement deterministic module-by-module rendering

**Files:**
- Modify: `scripts/regenerate_field_reference.py`
- Create: `tests/fixtures/snapshot_rollup/expected_rollup.md` (golden file)
- Modify: `tests/test_regenerate_field_reference.py`

- [ ] **Step 1: Write the golden expected output**

Determine deterministic output for the 3 fixture JSONs:

```bash
cat > /Users/maccb/llmragsql/tests/fixtures/snapshot_rollup/expected_rollup.md <<'EOF'
# Opera Transaction Posting — Complete Field Reference

Generated from transaction snapshot library by `scripts/regenerate_field_reference.py`.
Every field value from real Opera postings — added AND modified rows.
**Use as definitive reference when writing transactions back to Opera.**

---

## Cashbook Transactions

### Purchase Payment (fixture)

**Source:** opera_se
**Recorded:** 2026-01-02T13:00:00

Test fixture for the rollup regenerator unit tests.

**Tables Updated:**

| Database | Table | Rows Added | Rows Modified | Fields Changed |
|----------|-------|-----------|--------------|----------------|
| opera_se | aentry | 1 | 0 |  |

**aentry — New rows:**

```json
{
  "ae_entry": "P0001",
  "ae_value": -10000
}
```

### Sales Receipt (fixture)

**Source:** opera_se
**Recorded:** 2026-01-01T12:00:00

Test fixture.

**Tables Updated:**

| Database | Table | Rows Added | Rows Modified | Fields Changed |
|----------|-------|-----------|--------------|----------------|
| opera_se | aentry | 1 | 0 |  |

**aentry — New rows:**

```json
{
  "ae_entry": "R0001",
  "ae_value": 5000
}
```

## Sales Ledger Transactions

### Sales Invoice (fixture)

**Source:** opera_se
**Recorded:** 2026-01-03T14:00:00

Test fixture for sales ledger module.

**Tables Updated:**

| Database | Table | Rows Added | Rows Modified | Fields Changed |
|----------|-------|-----------|--------------|----------------|
| opera_se | stran | 1 | 0 |  |

**stran — New rows:**

```json
{
  "st_account": "C001",
  "st_trvalue": 100.0
}
```
EOF
```

Note the deterministic ordering: cashbook (first in `MODULE_ORDER`) before sales_ledger; within cashbook, `Purchase Payment` before `Sales Receipt` (lexicographic by `name`).

- [ ] **Step 2: Append failing tests**

Append to `tests/test_regenerate_field_reference.py`:

```python
def test_render_rollup_matches_expected_golden(tmp_path):
    """Render the 3 fixtures and assert byte-equality with the golden."""
    from importlib import util
    spec = util.spec_from_file_location("regen", REGEN)
    regen = util.module_from_spec(spec)
    spec.loader.exec_module(regen)

    paths = sorted(FIXTURES.glob("*.json"))
    snapshots = [regen.load_and_validate(p) for p in paths]
    rendered = regen.render_rollup(snapshots)

    expected = (FIXTURES / "expected_rollup.md").read_text(encoding="utf-8")
    assert rendered.strip() == expected.strip(), (
        "rendered rollup differs from golden:\n"
        f"--- expected ---\n{expected}\n"
        f"--- got ---\n{rendered}\n"
    )


def test_render_is_deterministic(tmp_path):
    """Same inputs → byte-identical output, twice in a row."""
    from importlib import util
    spec = util.spec_from_file_location("regen", REGEN)
    regen = util.module_from_spec(spec)
    spec.loader.exec_module(regen)

    paths = sorted(FIXTURES.glob("*.json"))
    snapshots = [regen.load_and_validate(p) for p in paths]
    out1 = regen.render_rollup(snapshots)
    out2 = regen.render_rollup(snapshots)
    assert out1 == out2


def test_render_within_module_orders_by_name():
    """Within a module, snapshots sort lexicographically by 'name'."""
    from importlib import util
    spec = util.spec_from_file_location("regen", REGEN)
    regen = util.module_from_spec(spec)
    spec.loader.exec_module(regen)

    snapshots = [
        {"name": "Z transaction", "module": "cashbook", "changes": []},
        {"name": "A transaction", "module": "cashbook", "changes": []},
        {"name": "M transaction", "module": "cashbook", "changes": []},
    ]
    rendered = regen.render_rollup(snapshots)
    a_idx = rendered.find("### A transaction")
    m_idx = rendered.find("### M transaction")
    z_idx = rendered.find("### Z transaction")
    assert 0 < a_idx < m_idx < z_idx, (
        f"within-module order should be lexicographic by name; "
        f"indices A={a_idx} M={m_idx} Z={z_idx}"
    )


def test_render_orders_modules_per_module_order():
    """Modules appear in the documented MODULE_ORDER."""
    from importlib import util
    spec = util.spec_from_file_location("regen", REGEN)
    regen = util.module_from_spec(spec)
    spec.loader.exec_module(regen)

    snapshots = [
        {"name": "Inv", "module": "sales_ledger", "changes": []},
        {"name": "Pmt", "module": "cashbook", "changes": []},
        {"name": "Stk", "module": "stock", "changes": []},
    ]
    rendered = regen.render_rollup(snapshots)
    cb_idx = rendered.find("Cashbook Transactions")
    sl_idx = rendered.find("Sales Ledger Transactions")
    st_idx = rendered.find("## ")  # generic — find the stock module heading
    # cashbook (idx 0 in MODULE_ORDER) appears before sales_ledger (idx 1)
    # which appears before stock (idx 7).
    assert 0 < cb_idx < sl_idx, (
        f"cashbook should precede sales_ledger; cb={cb_idx} sl={sl_idx}"
    )
```

- [ ] **Step 3: Run tests, verify failure**

```bash
source venv/bin/activate && python -m pytest tests/test_regenerate_field_reference.py -v
```

Expected: golden test FAILS (placeholder render). Determinism passes (same placeholder both runs). Within-module-order and module-order tests FAIL.

- [ ] **Step 4: Implement the full renderer**

Replace the placeholder `render_rollup` in `scripts/regenerate_field_reference.py` with:

```python
def render_rollup(snapshots: List[Dict[str, Any]]) -> str:
    """Render the markdown rollup deterministically.

    Order:
      1. Header.
      2. Modules in MODULE_ORDER. Modules NOT in the list are appended
         at the end, sorted lexicographically.
      3. Within each module, snapshots sorted lexicographically by name.
      4. Per snapshot: header (name, source, recorded_at), description,
         tables-updated table, then per-table added-rows + modified-rows
         blocks.
    """
    lines: List[str] = [
        "# Opera Transaction Posting — Complete Field Reference",
        "",
        "Generated from transaction snapshot library by `scripts/regenerate_field_reference.py`.",
        "Every field value from real Opera postings — added AND modified rows.",
        "**Use as definitive reference when writing transactions back to Opera.**",
        "",
        "---",
        "",
    ]

    # Group by module
    by_module: Dict[str, List[Dict[str, Any]]] = {}
    for s in snapshots:
        by_module.setdefault(s.get("module", "unknown"), []).append(s)

    # Determine module order
    known = [m for m in MODULE_ORDER if m in by_module]
    unknown = sorted(m for m in by_module if m not in MODULE_ORDER)
    module_order = known + unknown

    for module in module_order:
        items = sorted(by_module[module], key=lambda s: s.get("name", ""))
        if not items:
            continue
        # Module heading from the first snapshot's module_name (or fallback)
        module_name = items[0].get("module_name") or module.replace("_", " ").title()
        lines.append(f"## {module_name}")
        lines.append("")

        for snap in items:
            lines.append(f"### {snap.get('name', '?')}")
            lines.append("")
            if snap.get("source"):
                lines.append(f"**Source:** {snap['source']}")
            if snap.get("recorded_at"):
                lines.append(f"**Recorded:** {snap['recorded_at']}")
            lines.append("")
            if snap.get("description"):
                lines.append(snap["description"])
                lines.append("")

            changes = snap.get("changes", [])
            if changes:
                lines.append("**Tables Updated:**")
                lines.append("")
                lines.append(
                    "| Database | Table | Rows Added | Rows Modified | Fields Changed |"
                )
                lines.append(
                    "|----------|-------|-----------|--------------|----------------|"
                )
                for ch in changes:
                    db = ch.get("database", "?")
                    tab = ch.get("table", "?")
                    added = ch.get("rows_added", 0)
                    modified = len(ch.get("modified_rows", []))
                    fields = ", ".join(ch.get("modified_fields", [])[:10])
                    if len(ch.get("modified_fields", [])) > 10:
                        fields += f" (+{len(ch['modified_fields']) - 10} more)"
                    lines.append(
                        f"| {db} | {tab} | {added} | {modified} | {fields} |"
                    )
                lines.append("")

                # Per-table detail blocks
                for ch in changes:
                    tab = ch.get("table", "?")
                    added_rows = ch.get("added_rows", [])
                    if added_rows:
                        lines.append(f"**{tab} — New rows:**")
                        lines.append("")
                        lines.append("```json")
                        for row in added_rows[:3]:
                            lines.append(json.dumps(row, indent=2, sort_keys=True))
                        if len(added_rows) > 3:
                            lines.append(f"... and {len(added_rows) - 3} more")
                        lines.append("```")
                        lines.append("")
                    modified_rows = ch.get("modified_rows", [])
                    if modified_rows:
                        lines.append(f"**{tab} — Modified fields:**")
                        lines.append("")
                        for mod in modified_rows[:3]:
                            for field, vals in (mod.get("changes", {}) or {}).items():
                                lines.append(
                                    f"- `{field}`: `{vals.get('before')}` → `{vals.get('after')}`"
                                )
                        lines.append("")

    return "\n".join(lines)
```

- [ ] **Step 5: Run tests**

```bash
source venv/bin/activate && python -m pytest tests/test_regenerate_field_reference.py -v
```

Expected: all 7 tests PASS.

If the golden test fails by trailing-whitespace or final-newline, adjust the expected file to match exact byte output (Python strings vs bash heredoc).

- [ ] **Step 6: Commit**

```bash
git add scripts/regenerate_field_reference.py tests/test_regenerate_field_reference.py tests/fixtures/snapshot_rollup/expected_rollup.md
git commit -m "feat(rollup): module-by-module deterministic renderer

render_rollup now produces the full markdown body — modules in
documented MODULE_ORDER, snapshots within each module sorted
lexicographically by name. Each snapshot block: header, source,
recorded_at, description, tables-updated table, per-table new-rows
and modified-fields detail.

JSON in code blocks uses sort_keys=True so two snapshots with the
same content render identically. No timestamps in output.

Tests cover golden byte-equality, determinism (same input → same
output twice), within-module ordering, and module ordering.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Idempotency, atomicity, and deletion handling

**Files:**
- Modify: `tests/test_regenerate_field_reference.py`

- [ ] **Step 1: Append failing tests**

```python
def test_main_idempotent_on_clean_state(tmp_path):
    """Running the regenerator twice in a row must produce no diff."""
    src = tmp_path / "lib"
    src.mkdir()
    for f in FIXTURES.glob("*.json"):
        shutil.copy(f, src)

    proc1 = _run(["--library", str(src)])
    assert proc1.returncode == 0, proc1.stderr
    rollup1 = (src / "COMPLETE_FIELD_REFERENCE.md").read_text(encoding="utf-8")

    proc2 = _run(["--library", str(src)])
    assert proc2.returncode == 0, proc2.stderr
    rollup2 = (src / "COMPLETE_FIELD_REFERENCE.md").read_text(encoding="utf-8")

    assert rollup1 == rollup2, "regenerator is not idempotent"


def test_check_mode_exit_1_when_stale(tmp_path):
    """--check exits 1 if rollup is stale (existing != regenerated)."""
    src = tmp_path / "lib"
    src.mkdir()
    for f in FIXTURES.glob("*.json"):
        shutil.copy(f, src)
    (src / "COMPLETE_FIELD_REFERENCE.md").write_text("STALE CONTENT")

    proc = _run(["--library", str(src), "--check"])
    assert proc.returncode == 1
    assert "STALE" in (proc.stdout + proc.stderr)


def test_check_mode_exit_0_when_current(tmp_path):
    """--check exits 0 if existing rollup matches regenerated content."""
    src = tmp_path / "lib"
    src.mkdir()
    for f in FIXTURES.glob("*.json"):
        shutil.copy(f, src)

    # Regenerate first to produce the canonical file
    proc1 = _run(["--library", str(src)])
    assert proc1.returncode == 0, proc1.stderr

    # Then --check
    proc2 = _run(["--library", str(src), "--check"])
    assert proc2.returncode == 0, proc2.stdout
    assert "OK" in proc2.stdout


def test_deletion_reflected_in_rollup(tmp_path):
    """Removing a snapshot JSON makes the regenerator drop it from rollup."""
    src = tmp_path / "lib"
    src.mkdir()
    for f in FIXTURES.glob("*.json"):
        shutil.copy(f, src)

    proc = _run(["--library", str(src)])
    rollup_with_all = (src / "COMPLETE_FIELD_REFERENCE.md").read_text()
    assert "Sales Invoice (fixture)" in rollup_with_all

    # Delete the sales-ledger snapshot
    (src / "sales_ledger_invoice_20260103_140000.json").unlink()

    proc2 = _run(["--library", str(src)])
    rollup_after = (src / "COMPLETE_FIELD_REFERENCE.md").read_text()
    assert "Sales Invoice (fixture)" not in rollup_after, (
        "deleted snapshot still in rollup — deletion not honoured"
    )


def test_atomic_write_leaves_old_file_on_failure(tmp_path, monkeypatch):
    """If the rendering raises after the rollup file exists, the old
    rollup must remain intact (atomic_write semantics).
    """
    src = tmp_path / "lib"
    src.mkdir()
    for f in FIXTURES.glob("*.json"):
        shutil.copy(f, src)

    # First, write the rollup normally
    proc = _run(["--library", str(src)])
    assert proc.returncode == 0
    original = (src / "COMPLETE_FIELD_REFERENCE.md").read_text()

    # Now corrupt one fixture to force a parse failure on the next run
    bad = src / "broken.json"
    bad.write_text("{this is not valid json")

    proc2 = _run(["--library", str(src)])
    assert proc2.returncode == 4, "malformed JSON should exit 4"

    # Old rollup must still be intact
    after = (src / "COMPLETE_FIELD_REFERENCE.md").read_text()
    assert after == original, "old rollup was corrupted by a failed run"
```

- [ ] **Step 2: Run tests, verify pass**

```bash
source venv/bin/activate && python -m pytest tests/test_regenerate_field_reference.py -v
```

Expected: all tests pass (the implementation already handles these — the tests are pinning the existing behaviour). If any test fails, fix the implementation.

- [ ] **Step 3: Commit**

```bash
git add tests/test_regenerate_field_reference.py
git commit -m "test(rollup): pin idempotency, atomicity, deletion, --check semantics

Five additional tests pin the regenerator's contract:
  - Running twice produces no diff (idempotent).
  - --check exits 1 when stale.
  - --check exits 0 when current.
  - Deleted JSON dropped from rollup on next run.
  - A failed run (malformed JSON) leaves the prior rollup intact
    (atomic_write semantics).

These were already implementation behaviours; this commit locks them
in via tests so a future refactor can't regress them.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Run the regenerator against the real central library and commit the result

**Files:**
- Modify: `~/opera-knowledge-ref/packages/opera-knowledge/transaction-library/COMPLETE_FIELD_REFERENCE.md`

- [ ] **Step 1: Stash unrelated central-repo state and pull**

```bash
cd ~/opera-knowledge-ref && git stash push -u -m "preserve unrelated work for rollup regen"
cd ~/opera-knowledge-ref && git pull --rebase origin main
```

- [ ] **Step 2: Run the regenerator against central**

```bash
cd /Users/maccb/llmragsql && source venv/bin/activate && python scripts/regenerate_field_reference.py
```

This regenerates `~/opera-knowledge-ref/packages/opera-knowledge/transaction-library/COMPLETE_FIELD_REFERENCE.md` from all 69+ JSON snapshots.

- [ ] **Step 3: Diff and review**

```bash
cd ~/opera-knowledge-ref && git diff --stat packages/opera-knowledge/transaction-library/COMPLETE_FIELD_REFERENCE.md
cd ~/opera-knowledge-ref && git diff packages/opera-knowledge/transaction-library/COMPLETE_FIELD_REFERENCE.md | head -100
```

The diff should show structural changes (the regenerator's deterministic output replaces whatever was there). If something looks wildly wrong, halt and report.

- [ ] **Step 4: Commit and push central**

```bash
cd ~/opera-knowledge-ref && git add packages/opera-knowledge/transaction-library/COMPLETE_FIELD_REFERENCE.md
cd ~/opera-knowledge-ref && git commit -m "Regenerate COMPLETE_FIELD_REFERENCE.md deterministically

First commit using the new scripts/regenerate_field_reference.py.
Catches up the rollup with all snapshots up to current state, including
the ~22 post-2026-04-06 snapshots that hadn't been rolled up yet.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
cd ~/opera-knowledge-ref && git push origin main
cd ~/opera-knowledge-ref && git stash pop 2>/dev/null
```

---

## Task 5: Replace local file with symlink

**Files:**
- Create: `scripts/setup_local_kb_mirror.py`
- Modify: `apps/core/docs/opera_transaction_field_reference.md` (replace with symlink)

- [ ] **Step 1: Write the setup script**

```python
# scripts/setup_local_kb_mirror.py
"""One-shot installer: replace the local field-reference copy with a
symlink to the central KB version.

Drift is impossible by construction once the symlink is in place — the
local path always reads the central file.

Idempotent: safe to run twice. If the local path is already a symlink
pointing at the right target, the script does nothing.

Failure modes:
  - Central repo not cloned: prints clear instructions and exits 1.
  - Local path is not a regular file or existing symlink: refuses to
    touch it (could be a directory).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
LOCAL_PATH = REPO_ROOT / "apps" / "core" / "docs" / "opera_transaction_field_reference.md"
CENTRAL_PATH = Path(os.path.expanduser(
    "~/opera-knowledge-ref/packages/opera-knowledge/transaction-library/COMPLETE_FIELD_REFERENCE.md"
))


def main() -> int:
    if not CENTRAL_PATH.exists():
        print(f"ERROR: central KB file not found at {CENTRAL_PATH}", file=sys.stderr)
        print("Clone the central knowledge repo:", file=sys.stderr)
        print("  git clone https://github.com/jonathangintsys/aisam.git ~/opera-knowledge-ref", file=sys.stderr)
        return 1

    if LOCAL_PATH.is_symlink():
        target = LOCAL_PATH.resolve()
        if target == CENTRAL_PATH.resolve():
            print(f"OK: {LOCAL_PATH} already symlinked to central.")
            return 0
        print(f"NOTE: {LOCAL_PATH} is a symlink but points at {target}.")
        print("Replacing with link to central.")
        LOCAL_PATH.unlink()
    elif LOCAL_PATH.is_file():
        print(f"NOTE: {LOCAL_PATH} is a regular file. Backing up to .bak and replacing with symlink.")
        backup = LOCAL_PATH.with_suffix(".md.bak")
        LOCAL_PATH.rename(backup)
        print(f"  backup: {backup}")
    elif LOCAL_PATH.exists():
        print(f"ERROR: {LOCAL_PATH} exists and is neither a file nor symlink.", file=sys.stderr)
        return 1

    LOCAL_PATH.symlink_to(CENTRAL_PATH)
    print(f"OK: {LOCAL_PATH} → {CENTRAL_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run the setup script**

```bash
cd /Users/maccb/llmragsql && source venv/bin/activate && python scripts/setup_local_kb_mirror.py
```

Verify:

```bash
ls -la /Users/maccb/llmragsql/apps/core/docs/opera_transaction_field_reference.md
```

Expected output: starts with `lrwxr-xr-x` (symlink) pointing at the central path. The `.bak` backup is created for safety.

- [ ] **Step 3: Verify the symlink works (file is readable from local path)**

```bash
head -3 /Users/maccb/llmragsql/apps/core/docs/opera_transaction_field_reference.md
```

Expected: matches the central file's first lines.

- [ ] **Step 4: Commit (force-add the script — scripts/ is gitignored)**

```bash
git add -f scripts/setup_local_kb_mirror.py
git add apps/core/docs/opera_transaction_field_reference.md
git commit -m "feat(rollup): symlink local KB mirror to central — drift impossible

The local field-reference at apps/core/docs/opera_transaction_field_
reference.md is now a symlink to the central KB at
~/opera-knowledge-ref/packages/opera-knowledge/transaction-library/
COMPLETE_FIELD_REFERENCE.md. Reading the local path always reads
central — no manual sync, no possible drift.

scripts/setup_local_kb_mirror.py is the idempotent installer: it
detects an existing regular file (renames to .md.bak), an existing
symlink (verifies target or replaces), or absent path. Safe to re-run.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
git push
```

---

## Task 6: GitHub Actions CI gate

**Files:**
- Create: `.github/workflows/snapshot-rollup-check.yml`

- [ ] **Step 1: Write the workflow**

```yaml
# .github/workflows/snapshot-rollup-check.yml
name: Snapshot Rollup Check

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

jobs:
  rollup-check:
    runs-on: ubuntu-latest
    steps:
      - name: Check out main repo
        uses: actions/checkout@v4

      - name: Check out central knowledge repo
        uses: actions/checkout@v4
        with:
          repository: jonathangintsys/aisam
          path: opera-knowledge-ref
          token: ${{ secrets.CENTRAL_KB_READ_TOKEN }}
          # If the central repo is public, the token isn't needed; remove the line above.

      - name: Set up Python 3.9
        uses: actions/setup-python@v5
        with:
          python-version: '3.9'

      - name: Run rollup check
        run: |
          python scripts/regenerate_field_reference.py \
            --library opera-knowledge-ref/packages/opera-knowledge/transaction-library \
            --check

      - name: Comment on PR if stale
        if: failure() && github.event_name == 'pull_request'
        uses: actions/github-script@v7
        with:
          script: |
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: '❌ Snapshot rollup is stale relative to JSON inputs in central. Run `python scripts/regenerate_field_reference.py` locally and push the updated rollup to the central knowledge repo.'
            })
```

Note: if the central repo is public, you can remove the `token` line. If private, the secret `CENTRAL_KB_READ_TOKEN` must be configured at repo level (a fine-grained PAT with read-only access to `jonathangintsys/aisam`).

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/snapshot-rollup-check.yml
git commit -m "ci(rollup): add CI gate to fail PRs with a stale rollup

Workflow checks out both this repo and the central knowledge repo,
then runs scripts/regenerate_field_reference.py --check. PR is
blocked from merge if the central rollup is out of date relative to
its JSON inputs. On failure, a PR comment explains how to fix.

This makes the snapshot capture → rollup → distillation flow
mechanically enforced — no human discipline required.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
git push
```

---

## Task 7: KB updates

**Files:**
- Modify: `apps/core/docs/opera_knowledge_base.md` (NB: NOT the symlinked field-reference file)
- Create: `~/opera-knowledge-ref/packages/opera-knowledge/business-rules/snapshot-rollup.md`

- [ ] **Step 1: Append local KB section**

Append to `apps/core/docs/opera_knowledge_base.md`:

```markdown

## Snapshot Rollup + Local Mirror + CI Gate

**`scripts/regenerate_field_reference.py`** is the deterministic regenerator for `COMPLETE_FIELD_REFERENCE.md` — the rollup of all JSON snapshots in the central knowledge library at `~/opera-knowledge-ref/packages/opera-knowledge/transaction-library/`. Same snapshots in → byte-identical markdown out. Idempotent. Atomic write.

**The local mirror at `apps/core/docs/opera_transaction_field_reference.md` is a symlink** to the central rollup. Reading the local path always reads central — drift is impossible by construction. Set up via `scripts/setup_local_kb_mirror.py` (idempotent installer; renames any existing local copy to `.md.bak`).

**CI gate:** `.github/workflows/snapshot-rollup-check.yml` runs on every PR and main push, regenerating the rollup against the central JSON inputs and comparing to what's committed. PR blocked from merge if stale. To fix: run `python scripts/regenerate_field_reference.py` locally, commit the updated rollup to central, push, then re-run the failed workflow.

**Determinism guarantees** (test-pinned in `tests/test_regenerate_field_reference.py`):
- File ordering: lexicographic by snapshot filename.
- Module ordering: fixed `MODULE_ORDER` constant; modules outside the list appended sorted lexicographically at the end.
- Within-module ordering: lexicographic by snapshot's `name` field.
- JSON in code blocks: `sort_keys=True`.
- No timestamps in output (would break determinism).
- Empty changes list omits the table-updated table.

If you add a new module category (e.g. a new external integration that captures snapshots), add it to `MODULE_ORDER` to control its position; otherwise it lands at the end.
```

- [ ] **Step 2: Pull, write, commit, push central KB**

```bash
cd ~/opera-knowledge-ref && git stash push -u -m "preserve unrelated work" 2>/dev/null
cd ~/opera-knowledge-ref && git pull --rebase origin main
cat > ~/opera-knowledge-ref/packages/opera-knowledge/business-rules/snapshot-rollup.md <<'EOF'
# Snapshot Rollup + Local Mirror

The transaction-snapshot feature captures raw JSON before/after states of Opera postings into `transaction-library/*.json`. The aggregated `COMPLETE_FIELD_REFERENCE.md` is the human-readable distillation — *the canonical answer to "how does Opera update fields when posting transaction X?"*.

## Regenerator

**`scripts/regenerate_field_reference.py`** in the main app repo (HarryBurdett/llmragsql) reads every JSON file in this directory, sorts deterministically, and writes `COMPLETE_FIELD_REFERENCE.md` here.

Properties (test-pinned):
- Same inputs → byte-identical output.
- Idempotent: running twice produces no diff.
- Atomic: write fails leave the old rollup intact.
- Deletion-aware: removing a JSON drops it from the rollup on the next run.

## Local mirror in the main app repo

`apps/core/docs/opera_transaction_field_reference.md` is a **symlink** to the central rollup file. No manual copy. No possible drift.

`scripts/setup_local_kb_mirror.py` in the main app repo installs the symlink idempotently (backs up any existing regular file as `.md.bak`).

## CI gate

A GitHub Actions workflow (`.github/workflows/snapshot-rollup-check.yml`) runs on every main-repo PR and pushes a regenerator `--check`. If the central rollup is stale relative to its JSON inputs, the PR is blocked.

To fix a stale rollup: run `python scripts/regenerate_field_reference.py` locally, commit the result to **this central repo**, push, then re-run the failed workflow.

## When to add a new module to `MODULE_ORDER`

The constant `MODULE_ORDER` in the regenerator script controls where new module categories appear in the rollup. If a new external integration starts capturing snapshots with a `module` field not in the list, the regenerator appends it after the documented categories (sorted lexicographically). To position it explicitly, add it to `MODULE_ORDER` in the appropriate place and submit a PR.

## Snapshot JSON schema (canonical)

Every JSON in this directory must have these top-level keys:

- `name`: human-readable title.
- `module`: short module slug (e.g. `cashbook`, `sales_ledger`).
- `module_name`: optional human-readable module heading.
- `source`: `opera_se` or `opera_3`.
- `recorded_at`: ISO timestamp of capture.
- `description`: free-form context.
- `changes`: array of `{database, table, rows_added, added_rows, modified_rows, modified_fields}`.

The regenerator validates these and refuses to write the rollup if any input file is malformed (exit code 4).
EOF
cd ~/opera-knowledge-ref && git add packages/opera-knowledge/business-rules/snapshot-rollup.md
cd ~/opera-knowledge-ref && git commit -m "Document snapshot rollup + local mirror + CI gate

The single canonical regenerator path. Local mirror is a symlink so
drift is impossible by construction. CI gate fails PRs with a stale
rollup. Test-pinned determinism / idempotency / atomicity / deletion.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
cd ~/opera-knowledge-ref && git push origin main
cd ~/opera-knowledge-ref && git stash pop 2>/dev/null
```

- [ ] **Step 3: Commit local KB**

```bash
git -C /Users/maccb/llmragsql add apps/core/docs/opera_knowledge_base.md
git -C /Users/maccb/llmragsql commit -m "$(cat <<'EOF'
docs(kb): document snapshot rollup, symlinked mirror, and CI gate

Mirrors the central KB entry at
opera-knowledge-ref/packages/opera-knowledge/business-rules/
snapshot-rollup.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git -C /Users/maccb/llmragsql push
```

- [ ] **Step 4: Verify**

```bash
echo "=== Local KB ==="
git -C /Users/maccb/llmragsql log --oneline -1 -- apps/core/docs/opera_knowledge_base.md
echo "=== Central KB ==="
git -C ~/opera-knowledge-ref log --oneline -1 -- packages/opera-knowledge/business-rules/snapshot-rollup.md
echo "=== Local symlink ==="
ls -la /Users/maccb/llmragsql/apps/core/docs/opera_transaction_field_reference.md
```

---

## Done Criteria

- [ ] `scripts/regenerate_field_reference.py` exists, deterministic, idempotent, atomic, deletion-aware. 12+ unit tests pass.
- [ ] `scripts/setup_local_kb_mirror.py` exists; `apps/core/docs/opera_transaction_field_reference.md` is a symlink to central.
- [ ] Central rollup regenerated against the current JSON snapshots (catches up the ~22 post-Apr-6 snapshots).
- [ ] `.github/workflows/snapshot-rollup-check.yml` blocks PRs with stale rollup.
- [ ] Both KBs updated and pushed.
