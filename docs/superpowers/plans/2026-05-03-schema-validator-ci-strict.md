# Schema Validator CI Strict Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `scripts/validate_sql_columns.py` a hard CI gate — no SQL column reference may ship to `main` that does not exist in the canonical Opera schema snapshot. Eliminate the "silent typo" bug class (`pt_ref`, `st_ref`, `at_date`, `nk_lstdate`, etc.) that pyodbc swallows as warnings.

**Architecture:** The validator already exists. Tasks: (1) add YAML-based suppression mechanism with explicit per-suppression reasons, (2) add fixture-based unit tests pinning behaviour, (3) triage the existing 172 candidates into real-bug fixes vs suppressions, (4) wire CI workflow to run the validator on every PR/push, (5) wire pre-commit hook so locals catch problems before push.

**Tech Stack:** Python 3.9, PyYAML (already a transitive dep), pytest, pre-commit, GitHub Actions.

**Source spec:** `docs/superpowers/specs/2026-05-03-schema-validator-ci-strict-design.md`

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `scripts/validate_sql_columns.py` | **modify** | Add YAML suppression loading; tighten `--strict` exit codes; cache snapshot |
| `scripts/sql_validator_suppressions.yaml` | **create** | Authoritative list of false-positive suppressions, each with a reason |
| `tests/test_validate_sql_columns.py` | **create** | Fixture-based unit tests + regression cases |
| `.github/workflows/sql-validator.yml` | **create** | CI workflow runs the validator strict on every PR + main push |
| `.pre-commit-config.yaml` | **create or modify** | Pre-commit hook runs validator on staged Python files |
| `apps/core/docs/opera_knowledge_base.md` | **modify** | Document the validator + suppression policy |
| `~/opera-knowledge-ref/.../business-rules/sql-validator.md` | **create** | Central KB |
| Real-bug fix commits across the codebase | **modify** | One commit per typo fixed (e.g. `at_date` → `at_pstdate`) |

---

## Task 1: YAML suppression mechanism

**Files:**
- Modify: `scripts/validate_sql_columns.py`
- Create: `scripts/sql_validator_suppressions.yaml` (initially empty list)
- Create: `tests/test_validate_sql_columns.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_validate_sql_columns.py
"""Tests for scripts/validate_sql_columns.py.

Fixture-based: small fake snapshot + small fake source tree, then
exercise the validator's classification logic. Plus regression cases
for the typos we caught this session.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
VALIDATOR = REPO_ROOT / "scripts" / "validate_sql_columns.py"


def _run(args: list[str], cwd=REPO_ROOT) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(VALIDATOR), *args],
        cwd=cwd, capture_output=True, text=True, env={"PYTHONUNBUFFERED": "1"},
    )


def test_validator_loads_suppressions_yaml_without_error(tmp_path):
    """The validator must accept a suppressions YAML file without
    crashing — even if it's empty (just a top-level `suppressions: []`).
    """
    yaml_path = REPO_ROOT / "scripts" / "sql_validator_suppressions.yaml"
    assert yaml_path.exists(), (
        f"{yaml_path} must exist (initially empty list)"
    )
    text = yaml_path.read_text()
    assert "suppressions:" in text, "yaml must have top-level 'suppressions' key"


def test_strict_mode_returns_nonzero_when_unknown_columns_present(tmp_path, monkeypatch):
    """--strict exit code is 1 when unsuppressed unknown columns exist.

    We construct a tiny fake repo with a deliberate typo and a tiny
    snapshot, then run the validator with --strict pointed at the
    fake repo. The snapshot path is hard-coded in the validator, so
    we override via env vars / args.
    """
    # Build a fake snapshot with one table
    snap = tmp_path / "opera_snapshot.json"
    snap.write_text(json.dumps({
        "tables": {
            "ptran": {"columns": ["pt_acnt", "pt_trref", "pt_trvalue"]},
        }
    }))
    # Build a fake source file with a typo
    src_dir = tmp_path / "sql_rag"
    src_dir.mkdir()
    src_file = src_dir / "fake_module.py"
    src_file.write_text(
        '"""Fake module for the validator test."""\n'
        'def f(sql): pass\n'
        'q = """SELECT pt_ref FROM ptran WHERE pt_trvalue > 0"""\n'
        'f(q)\n'
    )
    # Empty suppressions file in tmp
    sup = tmp_path / "sql_validator_suppressions.yaml"
    sup.write_text("suppressions: []\n")

    proc = _run(
        [
            "--strict",
            "--snapshot", str(snap),
            "--scan-root", str(src_dir),
            "--suppressions", str(sup),
        ]
    )
    assert proc.returncode == 1, (
        f"--strict should exit 1 with an unsuppressed typo. stdout:\n"
        f"{proc.stdout}\n\nstderr:\n{proc.stderr}"
    )
    assert "pt_ref" in proc.stdout, (
        f"unknown column 'pt_ref' should appear in output: {proc.stdout}"
    )


def test_strict_mode_zero_when_typo_is_suppressed(tmp_path):
    """A suppression with a reason silences the unknown-column finding."""
    snap = tmp_path / "opera_snapshot.json"
    snap.write_text(json.dumps({
        "tables": {
            "ptran": {"columns": ["pt_acnt", "pt_trref", "pt_trvalue"]},
        }
    }))
    src_dir = tmp_path / "sql_rag"
    src_dir.mkdir()
    src_file = src_dir / "fake_module.py"
    src_file.write_text(
        '"""Fake module."""\n'
        'q = """SELECT pt_ref FROM ptran"""\n'
    )
    sup = tmp_path / "sql_validator_suppressions.yaml"
    sup.write_text(
        "suppressions:\n"
        f"  - file: {src_file}\n"
        "    line: 2\n"
        "    column: pt_ref\n"
        "    reason: |\n"
        "      Test suppression — fake column for the validator's own\n"
        "      regression suite. Real production code uses pt_trref.\n"
        "    added: 2026-05-03\n"
        "    added_by: claude\n"
    )

    proc = _run(
        [
            "--strict",
            "--snapshot", str(snap),
            "--scan-root", str(src_dir),
            "--suppressions", str(sup),
        ]
    )
    assert proc.returncode == 0, (
        f"--strict with valid suppression should exit 0. stdout:\n"
        f"{proc.stdout}\n\nstderr:\n{proc.stderr}"
    )


def test_alias_filter_does_not_flag_aliased_columns(tmp_path):
    """`SELECT at_pstdate AS at_date FROM atran` should NOT flag at_date —
    it's an alias defined by the query, not a missing column reference.
    """
    snap = tmp_path / "opera_snapshot.json"
    snap.write_text(json.dumps({
        "tables": {
            "atran": {"columns": ["at_pstdate"]},
        }
    }))
    src_dir = tmp_path / "sql_rag"
    src_dir.mkdir()
    src_file = src_dir / "alias_test.py"
    src_file.write_text(
        '"""Alias test."""\n'
        'q = """SELECT at_pstdate AS at_date FROM atran WHERE at_pstdate > 0"""\n'
    )
    sup = tmp_path / "sql_validator_suppressions.yaml"
    sup.write_text("suppressions: []\n")

    proc = _run(
        [
            "--strict",
            "--snapshot", str(snap),
            "--scan-root", str(src_dir),
            "--suppressions", str(sup),
        ]
    )
    assert proc.returncode == 0, (
        f"alias `AS at_date` should not be flagged. stdout:\n{proc.stdout}"
    )


def test_validator_rejects_malformed_suppression_yaml(tmp_path):
    """Malformed YAML should exit 3 with a clear message, not silently
    proceed.
    """
    snap = tmp_path / "opera_snapshot.json"
    snap.write_text(json.dumps({"tables": {}}))
    src_dir = tmp_path / "sql_rag"
    src_dir.mkdir()
    sup = tmp_path / "broken.yaml"
    sup.write_text("suppressions:\n  - this is not a valid mapping\n  garbage:\n    key: value: bad")

    proc = _run(
        [
            "--strict",
            "--snapshot", str(snap),
            "--scan-root", str(src_dir),
            "--suppressions", str(sup),
        ]
    )
    assert proc.returncode == 3, (
        f"malformed suppressions YAML should exit 3, got {proc.returncode}. "
        f"stdout: {proc.stdout}\nstderr: {proc.stderr}"
    )
    assert "suppression" in (proc.stdout + proc.stderr).lower()


def test_validator_rejects_missing_snapshot(tmp_path):
    """Missing snapshot should exit 2 — never silently pass."""
    proc = _run(
        [
            "--strict",
            "--snapshot", str(tmp_path / "nonexistent.json"),
            "--scan-root", str(tmp_path),
        ]
    )
    assert proc.returncode == 2
```

- [ ] **Step 2: Run, verify failure**

```bash
source venv/bin/activate && python -m pytest tests/test_validate_sql_columns.py -v
```

Expected: most/all FAIL — the validator doesn't yet support `--snapshot`/`--scan-root`/`--suppressions` overrides or the suppression-YAML mechanism.

- [ ] **Step 3: Modify the validator**

In `scripts/validate_sql_columns.py`:

(a) Add YAML import — at the top with the other imports, add:

```python
try:
    import yaml  # type: ignore
except ImportError:
    yaml = None  # validated at runtime
```

(b) Add CLI flags. Replace the existing `argparse.ArgumentParser` block with:

```python
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--strict', action='store_true',
                        help='Exit 1 if any unsuppressed unknown columns are found.')
    parser.add_argument('--suggest', action='store_true',
                        help='Show Levenshtein near-misses for each unknown column.')
    parser.add_argument('--include-wrong-table', action='store_true',
                        help='Also report columns that exist but in a different table than the FROM/UPDATE.')
    parser.add_argument('--snapshot', type=str, default=None,
                        help='Path to opera_snapshot.json (default: scripts/opera_snapshot.json under repo root).')
    parser.add_argument('--scan-root', action='append', default=None,
                        help='Override default scan roots; can be repeated.')
    parser.add_argument('--suppressions', type=str, default=None,
                        help='Path to sql_validator_suppressions.yaml (default: scripts/sql_validator_suppressions.yaml).')
    args = parser.parse_args()
```

(c) Resolve `snapshot_path`, `scan_roots`, `suppressions_path` from args (with sensible defaults) and pass them through to `load_snapshot`, the file iterator, and a new `load_suppressions` function.

```python
    # Resolve paths
    snapshot_path = Path(args.snapshot).resolve() if args.snapshot else SNAPSHOT_PATH
    if not snapshot_path.exists():
        print(f"ERROR: snapshot not found: {snapshot_path}", file=sys.stderr)
        print("Run scripts/snapshot_opera_schema.py to generate it.", file=sys.stderr)
        return 2

    suppressions_path = (
        Path(args.suppressions).resolve() if args.suppressions
        else ROOT / "scripts" / "sql_validator_suppressions.yaml"
    )
    suppressions: list[dict] = []
    if suppressions_path.exists():
        if yaml is None:
            print(f"ERROR: PyYAML required to read {suppressions_path}", file=sys.stderr)
            return 2
        try:
            with open(suppressions_path) as f:
                data = yaml.safe_load(f) or {}
            if not isinstance(data, dict) or 'suppressions' not in data:
                print(f"ERROR: {suppressions_path} must have top-level 'suppressions' list", file=sys.stderr)
                return 3
            suppressions = data.get('suppressions') or []
            if not isinstance(suppressions, list):
                print(f"ERROR: 'suppressions' must be a list in {suppressions_path}", file=sys.stderr)
                return 3
            # Each entry must be a dict with at least file/line/column/reason
            for i, s in enumerate(suppressions):
                if not isinstance(s, dict):
                    print(f"ERROR: suppression #{i} is not a mapping in {suppressions_path}", file=sys.stderr)
                    return 3
                for k in ('file', 'line', 'column', 'reason'):
                    if k not in s:
                        print(f"ERROR: suppression #{i} missing required key '{k}' in {suppressions_path}", file=sys.stderr)
                        return 3
        except yaml.YAMLError as e:
            print(f"ERROR: cannot parse {suppressions_path}: {e}", file=sys.stderr)
            return 3
```

(d) Replace `iter_python_files()` to honour `--scan-root`:

```python
def iter_python_files(scan_roots: Optional[list[Path]] = None) -> List[Path]:
    files: List[Path] = []
    roots: list[Path]
    if scan_roots:
        roots = [Path(r).resolve() for r in scan_roots]
    else:
        roots = [ROOT / top for top in SCAN_ROOTS]
    for root in roots:
        if not root.exists():
            continue
        for p in root.rglob('*.py'):
            if any(part in SKIP_PATH_PARTS for part in p.parts):
                continue
            if p.name in SKIP_FILE_NAMES:
                continue
            files.append(p)
    return sorted(files)
```

Update the call: `files = iter_python_files(args.scan_root)`.

(e) After collecting findings, apply suppressions before strict-exit decision:

```python
def _is_suppressed(finding: Dict, file_path: Path, suppressions: list[dict]) -> bool:
    """Match a finding against the suppression list. A suppression
    matches if file path resolves to the same path AND line number
    matches (exact or within range) AND column matches.
    """
    finding_file = file_path.resolve()
    for s in suppressions:
        try:
            sup_file = Path(s['file']).resolve()
        except Exception:
            continue
        if sup_file != finding_file:
            continue
        if s['column'] != finding['column']:
            continue
        # Line is exact for now; allow integer or string
        if int(s['line']) != int(finding['line']):
            continue
        return True
    return False


# After the per-file findings loop, filter:
# (in main(), inside the file iteration)
        unknowns = [f for f in findings if f['category'] == 'unknown'
                    and not _is_suppressed(f, path, suppressions)]
```

The existing strict-exit at the end of `main()`:

```python
    if args.strict and total_unknown > 0:
        return 1
    return 0
```

stays correct — `total_unknown` only counts unsuppressed findings.

- [ ] **Step 4: Create the (initially empty) suppressions file**

```bash
cat > /Users/maccb/llmragsql/scripts/sql_validator_suppressions.yaml <<'YAML'
# Schema-validator suppression list.
#
# Adding a suppression is a code-review step — explain WHY the false
# positive exists. The validator refuses to start if this file is
# malformed, so be careful with the YAML.
#
# Schema:
#   suppressions:
#     - file: <absolute or repo-relative path>
#       line: <integer line number>
#       column: <the identifier the validator flagged, e.g. db_name>
#       reason: |
#         Free-form text. Multi-line OK. Keep it specific — "false
#         positive" alone is not enough.
#       added: 2026-05-03
#       added_by: <username or 'claude'>
suppressions: []
YAML
```

- [ ] **Step 5: Run tests**

```bash
source venv/bin/activate && python -m pytest tests/test_validate_sql_columns.py -v
```

Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
git add scripts/validate_sql_columns.py scripts/sql_validator_suppressions.yaml tests/test_validate_sql_columns.py
git commit -m "feat(validator): YAML suppressions + CLI overrides + tests

The schema validator already detects 172 candidate unknown-column
references. Before strict mode can ship in CI we need a way to mark
specific findings as known false positives — but with auditable
reasons attached, not bulk silence.

This commit:
  - Adds --snapshot, --scan-root, --suppressions CLI flags so tests
    can target tmp directories.
  - Loads sql_validator_suppressions.yaml and rejects malformed
    files with exit code 3 (never silently ignores).
  - Each suppression requires file/line/column/reason — adding one
    is therefore visible in code review.
  - Tests cover: YAML loads cleanly when empty, --strict exits 1 on
    typos, suppression silences a typo, alias filter still works,
    malformed YAML exits 3, missing snapshot exits 2.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Triage current 172 candidates — false positives → SKIP_PREFIXES

**Files:**
- Modify: `scripts/validate_sql_columns.py` (extend SKIP_PREFIXES / COL_FALSE_POSITIVES)

- [ ] **Step 1: Run the validator and inventory findings**

```bash
source venv/bin/activate && python scripts/validate_sql_columns.py --suggest > /tmp/validator_initial.txt
echo "Total unknown count:" >> /tmp/validator_initial.txt
grep -c "UNKNOWN" /tmp/validator_initial.txt
```

Categorise each finding into:
- **A — Real Opera typo:** must be fixed in code (Task 3).
- **B — Non-Opera prefix:** local SQLite, PostgreSQL system catalog, SQL Server DMV, Python local. Add to `SKIP_PREFIXES`.
- **C — Local app SQLite column:** add to `SKIP_PREFIXES`.
- **D — Suppressible per-line:** Python variables that match the regex by accident; suppress with reason in YAML.

Build the categorisation in `/tmp/validator_triage.md` for human review (this is your working notebook — paste classifications):

```
=== Category B (non-Opera prefixes — extend SKIP_PREFIXES) ===
db_*, pg_*, dm_*, fx_*, as_* (already in SKIP_PREFIXES from current code)
add: zc_* (zcontacts is local SQLite)
add: cb_* (Python local for cashbook control flow)
... etc

=== Category C (local SQLite tables — extend SKIP_PREFIXES or COL_FALSE_POSITIVES) ===
... etc

=== Category A (real Opera typos — Task 3 fixes) ===
api/main.py:7524  ih_invno     → ih_doc
api/main.py:7529  ih_invdat    → ih_invdate
... etc

=== Category D (Python variables to suppress) ===
api/main.py:11633  cb_user_clause   — local var name, not a column
... etc
```

- [ ] **Step 2: Add Category B/C prefixes to the validator**

Edit `scripts/validate_sql_columns.py` `SKIP_PREFIXES` set. Add prefixes that are NOT Opera columns (one per real category found in triage). Keep the existing entries.

For each addition, write a comment explaining what that prefix represents (so future readers don't undo the change without context):

```python
SKIP_PREFIXES = {
    'dm',   # SQL Server dynamic management views (sys.dm_exec_*)
    'pg',   # PostgreSQL system tables
    'db',   # database_name / db_path / db_err — Python locals
    'fx',   # local foreign-exchange dataclasses, NOT Opera
    'as',   # as_of_date param
    'zc',   # zcontacts (our local supplier-contacts table)
    # Add any new prefixes from the triage here, with comments.
}
```

- [ ] **Step 3: Re-run validator after prefix additions**

```bash
source venv/bin/activate && python scripts/validate_sql_columns.py --suggest > /tmp/validator_after_prefixes.txt
grep -c "UNKNOWN" /tmp/validator_after_prefixes.txt
```

Note the reduction. The remaining findings should be (a) real typos to fix in Task 3, plus (b) per-line suppressions to add in Task 4.

- [ ] **Step 4: Commit**

```bash
git add scripts/validate_sql_columns.py
git commit -m "fix(validator): extend SKIP_PREFIXES with non-Opera prefixes from triage

Triage of the validator's 172 unknown-column candidates classified
each one as:
  A) real Opera typo — fix in code (separate commit per fix)
  B) non-Opera prefix (SQL Server DMV, PostgreSQL, Python local,
     local SQLite) — extend SKIP_PREFIXES here
  C) per-line false positive — suppression YAML
This commit lands B. Each prefix has a comment explaining what
it represents so the change isn't reverted without context.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Fix real Opera typos — one commit per cluster

**Files:**
- Modify: each file containing a real Opera typo (one commit per logical cluster).

**Method:** for each typo cluster identified in Task 1's triage:
1. Verify the column exists or doesn't via `scripts/opera_snapshot.json` (or the `--suggest` output).
2. Replace the typo with the correct column name across affected files.
3. Run the wider test suite to confirm no regression.
4. Commit with a clear message naming the typo and the fix.

Common clusters to address:

- `at_date` (12 occurrences) → `at_pstdate`
- `pl_*` columns on palloc (40+ occurrences) → `al_*` (palloc actually uses al_ prefix)
- `ih_invno` → `ih_doc`; `ih_invdat` → `ih_invdate`
- `it_value` (24 occurrences) → look up correct column (likely `it_lineval` or `it_vatval`)
- `na_year` → does not exist; remove or rewrite query
- `pn_acnt` → `pn_account`
- `pt_suppref` → `pt_supref`
- `sn_postcode` → `sn_pstcode`
- `vc_rate` / `vc_code` → look up correct column on the `vat` table

**For EACH cluster, do the following 5-step pattern:**

- [ ] **Step 1: Verify the typo and the correct column**

```bash
source venv/bin/activate && python -c "
import json
s = json.load(open('/Users/maccb/llmragsql/scripts/opera_snapshot.json'))
# Find which table this column belongs to (or doesn't)
target = '<correct_column_name>'
typo = '<typo_column_name>'
for t, info in s['tables'].items():
    cols = info.get('columns', [])
    if target in cols:
        print(f'{target} EXISTS in {t}')
    if typo in cols:
        print(f'{typo} EXISTS in {t}')  # should be empty for real typos
"
```

- [ ] **Step 2: Find affected files**

```bash
grep -rn "<typo_column>" /Users/maccb/llmragsql/sql_rag/ /Users/maccb/llmragsql/api/ /Users/maccb/llmragsql/apps/ --include="*.py" | grep -v venv | head
```

- [ ] **Step 3: Apply the fix**

Use `sed -i` carefully or Edit tool with bounded ranges. Always confirm the surrounding SQL context before changing — the typo might appear inside a comment or string literal where it should NOT be replaced.

```bash
# Example for at_date → at_pstdate (only inside SQL strings, not comments/aliases)
# Open each file in turn and apply targeted Edit calls.
```

- [ ] **Step 4: Run tests**

```bash
source venv/bin/activate && python -m pytest tests/ -x -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit one cluster at a time**

```bash
git add <files>
git commit -m "fix(<area>): correct '<typo>' → '<correct>' in <table>

The '<typo>' column does not exist in Opera's <table> table. Real
column is '<correct>'. pyodbc raised this at query time but the
calling code logged it as a generic warning, so behaviour silently
degraded (e.g. zero results returned where rows should appear).

Files:
  - <file1>:<line> ...

Found by scripts/validate_sql_columns.py during the strict-mode
hardening triage.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

Repeat the 5 steps above for **each cluster**. Many small commits is preferred to one giant commit — keep blast radius and review effort small.

After all clusters are fixed, the validator's `--suggest` output should show only Category D (per-line suppressions) findings remaining.

---

## Task 4: Add per-line suppressions for remaining findings

**Files:**
- Modify: `scripts/sql_validator_suppressions.yaml`

- [ ] **Step 1: Re-run validator and identify remaining unknowns**

```bash
source venv/bin/activate && python scripts/validate_sql_columns.py --suggest > /tmp/remaining.txt
grep "UNKNOWN" /tmp/remaining.txt
```

For EACH remaining unknown:
- Open the file at the line number.
- Confirm visually that the identifier is a Python local variable, not an SQL column reference.
- Add a suppression entry to `scripts/sql_validator_suppressions.yaml` with:
  - `file`: absolute path under the repo root.
  - `line`: integer line number.
  - `column`: the identifier as the validator emitted.
  - `reason`: a short explanation specific to this site (not "false positive" alone).
  - `added`: 2026-05-03.
  - `added_by`: claude.

Example:

```yaml
suppressions:
  - file: /Users/maccb/llmragsql/api/main.py
    line: 11633
    column: cb_user_clause
    reason: |
      Python local variable used as an f-string placeholder name;
      not an SQL column reference. The actual SQL is constructed
      around it, no Opera column with this name.
    added: 2026-05-03
    added_by: claude

  - file: /Users/maccb/llmragsql/sql_rag/opera_sql_import.py
    line: 11944
    column: sn_postcode
    reason: |
      Already verified via snapshot: snput uses sn_pstcode (no 'o').
      This site is in a comment/docstring, not a real query.
    added: 2026-05-03
    added_by: claude
```

- [ ] **Step 2: Re-run validator strict**

```bash
source venv/bin/activate && python scripts/validate_sql_columns.py --strict
```

Expected: exit code 0, "Summary: 0 unknown columns".

- [ ] **Step 3: Commit**

```bash
git add scripts/sql_validator_suppressions.yaml
git commit -m "chore(validator): per-line suppressions for verified false positives

After Task 3 fixed the real typos, the remaining unknown-column
findings are Python local variable names that happen to match the
Opera-column regex (e.g. cb_user_clause, db_name in non-Opera
contexts). Each suppression has a specific reason — bulk-silencing
is not allowed.

Validator now exits clean under --strict.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: GitHub Actions workflow

**Files:**
- Create: `.github/workflows/sql-validator.yml`

- [ ] **Step 1: Write the workflow**

```yaml
# .github/workflows/sql-validator.yml
name: SQL Schema Validator

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python 3.9
        uses: actions/setup-python@v5
        with:
          python-version: '3.9'

      - name: Install PyYAML
        run: pip install pyyaml

      - name: Run schema validator (strict)
        id: validator
        run: |
          python scripts/validate_sql_columns.py --strict
        continue-on-error: false

      - name: Comment on PR if failure
        if: failure() && github.event_name == 'pull_request'
        uses: actions/github-script@v7
        with:
          script: |
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: '❌ Schema validator found unknown columns. Run `python scripts/validate_sql_columns.py --suggest` locally to see findings, then either fix the typo in code or add a suppression to `scripts/sql_validator_suppressions.yaml` with a clear reason.'
            })
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/sql-validator.yml
git commit -m "ci(validator): add SQL schema validator gate to PR + main pushes

Runs scripts/validate_sql_columns.py --strict on every PR and every
push to main. PR is blocked from merge if validator finds any
unsuppressed unknown column. On failure, a PR comment explains the
remediation: fix the typo or add a documented suppression.

Closes the silent-typo bug class (pt_ref, st_ref, at_date, etc.)
that pyodbc swallowed as warnings before this gate.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 3: Verify CI gate by deliberate test**

Open a throwaway branch with a deliberate typo, push, observe CI failure.

```bash
git checkout -b ci-test/validator-deliberate-typo
# Edit any file under sql_rag/ to introduce e.g. SELECT pt_ref FROM ptran
# Push and confirm CI fails
git push origin ci-test/validator-deliberate-typo
# Open a PR via gh CLI to observe the comment posting
gh pr create --title "[TEST] do not merge: deliberate validator typo" --body "Testing CI gate" --base main --head ci-test/validator-deliberate-typo
```

After observing the failed CI run, close the PR (do NOT merge):

```bash
gh pr close --comment "Verified CI gate works."
git checkout main
git branch -D ci-test/validator-deliberate-typo
git push origin --delete ci-test/validator-deliberate-typo
```

---

## Task 6: Pre-commit hook

**Files:**
- Modify or create: `.pre-commit-config.yaml`

- [ ] **Step 1: Inspect existing pre-commit config**

```bash
cat /Users/maccb/llmragsql/.pre-commit-config.yaml 2>/dev/null
```

If file doesn't exist, create it. If it exists, append the new hook to the existing `repos` list.

- [ ] **Step 2: Write the hook entry**

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: sql-validator
        name: SQL schema validator
        entry: python scripts/validate_sql_columns.py --strict
        language: system
        types_or: [python]
        pass_filenames: false
        # Run on every commit that touches Python files; the validator
        # itself decides which subset to scan based on file path globs.
```

- [ ] **Step 3: Test the hook locally**

```bash
pre-commit install 2>/dev/null  # OK if already installed
echo '"""marker"""' > /tmp/touch.py
git add .pre-commit-config.yaml
pre-commit run --all-files
```

- [ ] **Step 4: Commit**

```bash
git add .pre-commit-config.yaml
git commit -m "ci(precommit): SQL validator hook — local catch before push

Pre-commit hook runs scripts/validate_sql_columns.py --strict on
every commit that touches Python. Catches typos before they reach
CI, saving a round-trip.

Bypassable with --no-verify in genuine emergencies (logged in commit
message); CI still blocks on the same gate so bypass alone doesn't
ship a regression.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: KB updates

**Files:**
- Modify: `apps/core/docs/opera_knowledge_base.md`
- Create: `~/opera-knowledge-ref/.../business-rules/sql-validator.md`

- [ ] **Step 1: Append local KB section**

Append to `apps/core/docs/opera_knowledge_base.md`:

```markdown

## SQL Schema Validator (CI gate)

`scripts/validate_sql_columns.py` runs in CI (and as a pre-commit hook) and refuses any commit that introduces a SQL column reference not present in `scripts/opera_snapshot.json`. Closes the silent-typo bug class (`pt_ref`, `st_ref`, `at_date`, `nk_lstdate`, etc.) that pyodbc swallowed as warnings.

**False-positive suppressions:** `scripts/sql_validator_suppressions.yaml` is the authoritative list. Each suppression carries `file`, `line`, `column`, `reason`, `added`, `added_by`. Adding a suppression IS a code-review step; don't try to bulk-silence findings — the YAML refuses to parse if any suppression is missing required keys.

**When the validator flags something new in CI:**
1. Run `python scripts/validate_sql_columns.py --suggest` locally to see "did you mean…" suggestions.
2. If the column genuinely doesn't exist in Opera (typo), fix it in the code.
3. If the identifier is a Python variable that just matches the column-shape regex, add a suppression with a specific reason.

**Refreshing the snapshot:** the validator reads `scripts/opera_snapshot.json` — re-run `scripts/snapshot_opera_schema.py` if Opera schema changes (rare). The snapshot is committed to git so CI uses a deterministic version.
```

- [ ] **Step 2: Pull, write, push central KB**

```bash
cd ~/opera-knowledge-ref && git stash push -u -m "preserve unrelated work" 2>/dev/null
cd ~/opera-knowledge-ref && git pull --rebase origin main
cat > ~/opera-knowledge-ref/packages/opera-knowledge/business-rules/sql-validator.md <<'EOF'
# SQL Schema Validator

A static check that ALL SQL column references in Python source resolve to columns in the Opera schema snapshot. Runs in CI (PR + main push) and as a pre-commit hook.

## Why

pyodbc raises on a missing column at query time, but calling code typically catches it as a generic warning and returns empty results. That's a silent failure: the bug doesn't manifest until someone notices "why are no rows returning?" — sometimes weeks or months after the typo shipped. Examples we hit:

- `pt_ref` instead of `pt_trref` — purchase ledger reference matching silently never returned rows.
- `st_ref` instead of `st_trref` — same for sales ledger.
- `at_date` instead of `at_pstdate` — atran posting-date filters silently empty.
- `nk_lstdate` doesn't exist at all on nbank — silently returned wrong data.

A static gate eliminates the entire class.

## Suppression policy

`scripts/sql_validator_suppressions.yaml` lists known false positives, each with:

- `file`, `line`, `column` (identifies the finding).
- `reason` (free-form explanation — must be specific, "false positive" is insufficient).
- `added` (date), `added_by` (username).

The YAML refuses to load if any required key is missing — adding a suppression is a code-review step, never an undocumented silencer.

## Snapshot path

The validator reads `scripts/opera_snapshot.json`. Refresh with `scripts/snapshot_opera_schema.py` if Opera's schema changes. Commit the refreshed snapshot.
EOF
cd ~/opera-knowledge-ref && git add packages/opera-knowledge/business-rules/sql-validator.md
git commit -m "Document SQL schema validator + suppression policy

Static check on every commit + PR + main push. Refuses any unknown
Opera column reference without a documented suppression. Closes the
silent-typo bug class (pt_ref, st_ref, at_date, nk_lstdate).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
git push origin main
cd ~/opera-knowledge-ref && git stash pop 2>/dev/null
```

- [ ] **Step 3: Commit local KB**

```bash
git -C /Users/maccb/llmragsql add apps/core/docs/opera_knowledge_base.md
git -C /Users/maccb/llmragsql commit -m "$(cat <<'EOF'
docs(kb): document SQL schema validator + suppression policy

Mirrors central KB at
opera-knowledge-ref/packages/opera-knowledge/business-rules/
sql-validator.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git -C /Users/maccb/llmragsql push
```

- [ ] **Step 4: Verify**

```bash
git -C /Users/maccb/llmragsql log --oneline -1 -- apps/core/docs/opera_knowledge_base.md
git -C ~/opera-knowledge-ref log --oneline -1 -- packages/opera-knowledge/business-rules/sql-validator.md
git -C ~/opera-knowledge-ref status -sb | head -3
```

---

## Done Criteria

- [ ] `scripts/validate_sql_columns.py` accepts `--snapshot`, `--scan-root`, `--suppressions`, `--strict`, `--suggest`, `--include-wrong-table`.
- [ ] `scripts/sql_validator_suppressions.yaml` exists; malformed YAML rejected with exit code 3.
- [ ] `tests/test_validate_sql_columns.py` 6+ tests pass.
- [ ] All real typos identified during triage are fixed in code (one commit per cluster).
- [ ] Validator under `--strict` exits 0 on clean main.
- [ ] `.github/workflows/sql-validator.yml` blocks PRs with new typos.
- [ ] `.pre-commit-config.yaml` runs the validator on local commits.
- [ ] Both KBs updated and pushed.
