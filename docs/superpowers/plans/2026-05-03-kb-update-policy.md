# KB Update Policy + Pre-Commit Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the project's mandatory "Opera-related code change requires KB update" rule mechanically enforceable. A pre-commit hook + CI workflow refuse any commit/PR that touches Opera-related files without staging a corresponding KB update OR an explicit `kb-not-required:` justification in the commit message body.

**Architecture:** A new `scripts/kb_update_check.py` reads the staged file list (or, in CI, the PR's diff), matches each path against an Opera-files allowlist (`scripts/kb_update_allowlist.yaml`), and exits non-zero if Opera files are touched without a corresponding KB file change AND no `kb-not-required:` annotation in the commit message. A `docs/kb-update-guide.md` explains the rule plus the directory taxonomy in central KB.

**Tech Stack:** Python 3.9, PyYAML, pytest, pre-commit, GitHub Actions.

**Source spec:** `docs/superpowers/specs/2026-05-03-kb-update-policy-design.md`

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `scripts/kb_update_check.py` | **create** | Hook script: reads staged files (or PR diff in CI), matches allowlist, requires KB update OR annotation |
| `scripts/kb_update_allowlist.yaml` | **create** | Authoritative list of Opera-related file globs that require KB updates when changed |
| `tests/test_kb_update_check.py` | **create** | Fixture-based unit tests for hook logic |
| `.pre-commit-config.yaml` | **modify** | Add the new hook entry alongside the existing SQL validator hook |
| `.github/workflows/kb-update-check.yml` | **create** | CI gate that catches `--no-verify` bypass at PR time |
| `docs/kb-update-guide.md` | **create** | Developer-facing guide: what counts as Opera knowledge, directory taxonomy, escape hatch |
| `apps/core/docs/opera_knowledge_base.md` | **modify** | Document the policy + link to guide |
| `~/opera-knowledge-ref/.../business-rules/kb-update-policy.md` | **create** | Central KB doc |

---

## Task 1: Hook script with file-list + allowlist + annotation parsing

**Files:**
- Create: `scripts/kb_update_check.py`
- Create: `scripts/kb_update_allowlist.yaml`
- Create: `tests/test_kb_update_check.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_kb_update_check.py
"""Tests for scripts/kb_update_check.py.

Fixture-based: stage synthetic file lists and commit messages, then
exercise the hook's classification logic. The hook itself takes file
paths via argv (or stdin), so we don't need a real git repo.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
HOOK = REPO_ROOT / "scripts" / "kb_update_check.py"


def _run(args: list[str], commit_message: str = "", stdin: str = "") -> subprocess.CompletedProcess:
    """Run the hook script with explicit file list (via --files) and
    optional commit message (via --commit-msg-file or env var).
    """
    return subprocess.run(
        [sys.executable, str(HOOK), *args],
        capture_output=True,
        text=True,
        env={
            "PYTHONUNBUFFERED": "1",
            "GIT_COMMIT_MESSAGE": commit_message,
        },
        input=stdin,
    )


def test_pass_when_only_non_opera_files_staged():
    """A change to docs/, frontend/, demos/ should not require a KB update."""
    proc = _run(["--files", "docs/readme.md", "frontend/src/App.tsx"])
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_fail_when_opera_file_staged_without_kb_update():
    """An edit to sql_rag/opera_sql_import.py without a KB file
    staged AND without `kb-not-required:` should exit 1.
    """
    proc = _run(["--files", "sql_rag/opera_sql_import.py"])
    assert proc.returncode == 1
    assert "Opera-related" in proc.stdout or "Opera-related" in proc.stderr


def test_pass_when_opera_file_AND_local_kb_staged():
    """Local KB update alongside an Opera change satisfies the rule."""
    proc = _run([
        "--files",
        "sql_rag/opera_sql_import.py",
        "apps/core/docs/opera_knowledge_base.md",
    ])
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_pass_when_kb_not_required_annotation_present():
    """A commit message body containing `kb-not-required: <reason>`
    bypasses the hook (with the reason logged).
    """
    proc = _run(
        ["--files", "sql_rag/opera_sql_import.py"],
        commit_message="refactor: rename a local var\n\nkb-not-required: pure rename, no new Opera knowledge.",
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_fail_when_annotation_missing_reason():
    """`kb-not-required:` without a reason after the colon must NOT
    bypass — the rule is the reason.
    """
    proc = _run(
        ["--files", "sql_rag/opera_sql_import.py"],
        commit_message="refactor\n\nkb-not-required:",
    )
    assert proc.returncode == 1
    assert "reason" in (proc.stdout + proc.stderr).lower()


def test_glob_match_catches_new_opera_file():
    """The allowlist uses globs like sql_rag/opera_*.py, so adding
    sql_rag/opera_new_module.py is caught.
    """
    proc = _run(["--files", "sql_rag/opera_new_module.py"])
    assert proc.returncode == 1


def test_malformed_allowlist_exits_2(tmp_path):
    """Malformed YAML allowlist must exit 2 with a clear message.
    Provide a custom path via --allowlist.
    """
    bad_allowlist = tmp_path / "bad.yaml"
    bad_allowlist.write_text("opera_files:\n  - 'sql_rag/'\n  garbage:\n    key: value: bad")

    proc = _run([
        "--files", "sql_rag/opera_sql_import.py",
        "--allowlist", str(bad_allowlist),
    ])
    assert proc.returncode == 2
```

- [ ] **Step 2: Run tests, verify failure**

```bash
source venv/bin/activate && python -m pytest tests/test_kb_update_check.py -v
```

Expected: all 7 tests FAIL — `kb_update_check.py` doesn't exist yet.

- [ ] **Step 3: Write the allowlist YAML**

```yaml
# scripts/kb_update_allowlist.yaml
# Authoritative list of Opera-related file globs. A commit that
# touches any matching file requires a corresponding KB update OR an
# explicit `kb-not-required: <reason>` annotation in the commit body.
#
# Adding a glob here is a code-review step — it widens the hook's
# coverage. Removing one is also reviewable for the same reason.

opera_files:
  - "apps/bank_reconcile/api/routes.py"
  - "sql_rag/opera_sql_import.py"
  - "sql_rag/opera3_foxpro_import.py"
  - "sql_rag/opera3_foxpro.py"
  - "sql_rag/bank_import*.py"
  - "sql_rag/statement_reconcile*.py"
  - "sql_rag/duplicate_check*.py"
  - "sql_rag/period_reconciliation*.py"
  - "sql_rag/opera_config.py"
  - "sql_rag/opera_data_provider.py"
  - "sql_rag/opera_sql_provider.py"
  - "sql_rag/opera3_data_provider.py"
  - "sql_rag/opera3_agent_client.py"

# Files that satisfy the KB-update requirement when staged in the same
# commit as an Opera-files change. Any one of these counts.
kb_files:
  - "apps/core/docs/opera_knowledge_base.md"
  - "apps/core/docs/opera_transaction_field_reference.md"

# Note: the central knowledge repo lives outside this repo. The hook
# can't see staged files there. The CI gate is best-effort (checks the
# main repo only); the *push* policy for central is on the developer.
```

- [ ] **Step 4: Write the hook**

```python
# scripts/kb_update_check.py
"""Pre-commit + CI hook: refuse Opera-related code changes that don't
update the knowledge base (or carry an explicit kb-not-required:
annotation in the commit message).

Exit codes:
  0 — staged files satisfy the rule (or no Opera files touched).
  1 — Opera files touched, no KB update staged, no annotation.
  2 — configuration error (allowlist malformed / missing).

Usage as pre-commit hook:
  python scripts/kb_update_check.py

Usage with explicit file list (tests + CI):
  python scripts/kb_update_check.py --files sql_rag/opera_sql_import.py ...

Usage with custom allowlist:
  python scripts/kb_update_check.py --allowlist <path>

Commit message source:
  - Default: read from .git/COMMIT_EDITMSG (pre-commit hook context).
  - Override: GIT_COMMIT_MESSAGE env var (test convenience).
  - Override: --commit-msg-file <path> (CI convenience).
"""
from __future__ import annotations

import argparse
import fnmatch
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional


try:
    import yaml  # type: ignore
except ImportError:
    yaml = None


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ALLOWLIST = REPO_ROOT / "scripts" / "kb_update_allowlist.yaml"


def _load_allowlist(path: Path) -> dict:
    if yaml is None:
        print("ERROR: PyYAML required to read the allowlist.", file=sys.stderr)
        return {}
    if not path.exists():
        print(f"ERROR: allowlist not found at {path}", file=sys.stderr)
        return {}
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        print(f"ERROR: malformed YAML in {path}: {e}", file=sys.stderr)
        return {}
    if not isinstance(data, dict):
        print(f"ERROR: {path} must be a mapping at top level", file=sys.stderr)
        return {}
    if "opera_files" not in data or "kb_files" not in data:
        print(f"ERROR: {path} must have 'opera_files' and 'kb_files' lists", file=sys.stderr)
        return {}
    return data


def _staged_files() -> List[str]:
    """Return git index-staged file paths (relative to repo root)."""
    try:
        result = subprocess.run(
            ["git", "diff", "--cached", "--name-only"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _matches_any(path: str, globs: List[str]) -> bool:
    return any(fnmatch.fnmatch(path, g) for g in globs)


def _read_commit_message(commit_msg_file: Optional[str]) -> str:
    """Read the commit message body for the kb-not-required annotation.

    Sources, in order: --commit-msg-file, GIT_COMMIT_MESSAGE env,
    .git/COMMIT_EDITMSG, empty.
    """
    if commit_msg_file:
        try:
            return Path(commit_msg_file).read_text(encoding="utf-8")
        except OSError:
            return ""
    env_msg = os.environ.get("GIT_COMMIT_MESSAGE")
    if env_msg is not None:
        return env_msg
    editmsg = REPO_ROOT / ".git" / "COMMIT_EDITMSG"
    if editmsg.exists():
        try:
            return editmsg.read_text(encoding="utf-8")
        except OSError:
            return ""
    return ""


def _has_valid_annotation(commit_message: str) -> bool:
    """True if the commit message has a `kb-not-required:` line followed
    by at least one non-whitespace character (the reason).
    """
    for line in commit_message.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("kb-not-required:"):
            after = stripped.split(":", 1)[1].strip()
            return bool(after)
    return False


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--files", nargs="*", default=None,
        help="Explicit file list (test/CI convenience). If absent, reads `git diff --cached`.",
    )
    parser.add_argument(
        "--allowlist", type=Path, default=DEFAULT_ALLOWLIST,
        help="Path to allowlist YAML.",
    )
    parser.add_argument(
        "--commit-msg-file", type=str, default=None,
        help="Path to a commit message file (overrides .git/COMMIT_EDITMSG).",
    )
    args = parser.parse_args(argv)

    allowlist = _load_allowlist(args.allowlist)
    if not allowlist:
        return 2

    opera_globs = allowlist.get("opera_files", [])
    kb_globs = allowlist.get("kb_files", [])

    files = args.files if args.files is not None else _staged_files()
    if not files:
        return 0  # nothing staged

    opera_hits = [f for f in files if _matches_any(f, opera_globs)]
    if not opera_hits:
        return 0  # no Opera-related changes

    kb_hits = [f for f in files if _matches_any(f, kb_globs)]
    if kb_hits:
        return 0  # KB updated alongside the code change

    commit_message = _read_commit_message(args.commit_msg_file)
    if _has_valid_annotation(commit_message):
        # Log the bypass for audit
        print(
            "OK: kb-not-required annotation present; bypass logged.",
            file=sys.stderr,
        )
        return 0

    # Failure path: explain remediation
    print("✗ KB update required for Opera-related changes:", file=sys.stderr)
    for f in opera_hits:
        print(f"    {f}", file=sys.stderr)
    print(file=sys.stderr)
    print("Required action — choose one:", file=sys.stderr)
    print(
        "  1. Update apps/core/docs/opera_knowledge_base.md (and central KB) "
        "with a new section describing what changed, then re-stage.",
        file=sys.stderr,
    )
    print(
        "  2. Add 'kb-not-required: <specific reason>' to the commit message "
        "body explaining why this change does not represent new Opera knowledge.",
        file=sys.stderr,
    )
    print(file=sys.stderr)
    print(
        "Directory taxonomy for central KB:",
        file=sys.stderr,
    )
    print("  - schema/        → table/column documentation", file=sys.stderr)
    print("  - business-rules/→ posting rules, conventions, locking, sequence numbers", file=sys.stderr)
    print("  - query-patterns/→ reusable query examples", file=sys.stderr)
    print("  - transaction-library/ → snapshot JSON (auto-managed)", file=sys.stderr)
    print(file=sys.stderr)
    print("See docs/kb-update-guide.md for full rules.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
```

Force-add (scripts/ is gitignored):

```bash
git add -f scripts/kb_update_check.py scripts/kb_update_allowlist.yaml
```

- [ ] **Step 5: Run tests**

```bash
source venv/bin/activate && python -m pytest tests/test_kb_update_check.py -v
```

Expected: all 7 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/test_kb_update_check.py
git commit -m "feat(kb-policy): hook script + allowlist + tests

scripts/kb_update_check.py reads the git index (or an explicit --files
list for testing/CI) and refuses Opera-related code changes that don't
also stage a KB update — unless the commit message body carries a
kb-not-required: <reason> annotation.

Allowlist at scripts/kb_update_allowlist.yaml lists Opera-touching
file globs (apps/bank_reconcile/api/routes.py, sql_rag/opera_*.py,
bank_import*.py, statement_reconcile*.py, duplicate_check*.py,
period_reconciliation*.py, etc.) and the KB files that satisfy the
requirement when staged. Adding a glob is a code-review step.

Exit codes: 0 (pass), 1 (Opera files staged without KB / annotation),
2 (allowlist malformed). Detailed remediation message on failure.

7 unit tests cover: pass on non-Opera files, fail without KB, pass
with KB, pass with kb-not-required, fail without reason, glob match
on new files, exit 2 on bad YAML.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Pre-commit hook integration

**Files:**
- Modify: `.pre-commit-config.yaml`

- [ ] **Step 1: Inspect existing config**

```bash
cat /Users/maccb/llmragsql/.pre-commit-config.yaml
```

The previous plan added the SQL validator hook. We add the KB hook alongside it.

- [ ] **Step 2: Add the new hook**

Append to the existing `.pre-commit-config.yaml`:

```yaml
      - id: kb-update-check
        name: KB update required for Opera changes
        entry: python scripts/kb_update_check.py
        language: system
        pass_filenames: false
        stages: [pre-commit]
```

- [ ] **Step 3: Verify locally**

```bash
cd /Users/maccb/llmragsql && pre-commit run kb-update-check --all-files 2>&1 | tail -10
```

Expected: hook runs (it'll either pass or skip — there are no staged files at this point).

Test with a synthetic stage:

```bash
git add scripts/kb_update_check.py  # already staged from Task 1
git status --short
# Run the hook explicitly against the index
pre-commit run kb-update-check
```

Expected behaviour: passes because `scripts/kb_update_check.py` isn't in the Opera allowlist (it's a meta-script, not an Opera-touching file).

- [ ] **Step 4: Commit**

```bash
git add .pre-commit-config.yaml
git commit -m "ci(precommit): KB-update hook alongside SQL validator

Pre-commit hook runs scripts/kb_update_check.py on every commit. If
the commit touches an Opera-related file in the allowlist, the
operator must either stage a KB update OR include
'kb-not-required: <reason>' in the commit message body.

Bypassable with --no-verify in genuine emergencies; CI gate (Task 3)
catches that bypass at PR time.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: GitHub Actions CI gate

**Files:**
- Create: `.github/workflows/kb-update-check.yml`

- [ ] **Step 1: Write the workflow**

```yaml
# .github/workflows/kb-update-check.yml
name: KB Update Check

on:
  pull_request:
    branches: [main]

jobs:
  kb-update:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0  # need full history for diff

      - name: Set up Python 3.9
        uses: actions/setup-python@v5
        with:
          python-version: '3.9'

      - name: Install PyYAML
        run: pip install pyyaml

      - name: Compute changed files
        id: diff
        run: |
          base="${{ github.event.pull_request.base.sha }}"
          head="${{ github.event.pull_request.head.sha }}"
          changed=$(git diff --name-only "$base" "$head" | tr '\n' ' ')
          echo "files=$changed" >> "$GITHUB_OUTPUT"

      - name: Compute concatenated commit messages
        id: messages
        run: |
          base="${{ github.event.pull_request.base.sha }}"
          head="${{ github.event.pull_request.head.sha }}"
          messages=$(git log "$base..$head" --pretty=format:"%B%n---%n")
          # Write to a file because multi-line outputs are awkward in $GITHUB_OUTPUT
          echo "$messages" > /tmp/all_messages.txt

      - name: Run KB update check
        env:
          GIT_COMMIT_MESSAGE_FILE: /tmp/all_messages.txt
        run: |
          python scripts/kb_update_check.py \
            --files ${{ steps.diff.outputs.files }} \
            --commit-msg-file /tmp/all_messages.txt
        continue-on-error: false

      - name: Comment on PR if failure
        if: failure()
        uses: actions/github-script@v7
        with:
          script: |
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: 'KB update required for Opera-related changes. Either update apps/core/docs/opera_knowledge_base.md (and the central KB) with the new knowledge, or add `kb-not-required: <reason>` to a commit message in this PR. See docs/kb-update-guide.md for the directory taxonomy and examples.'
            })
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/kb-update-check.yml
git commit -m "ci(kb-policy): PR gate catches --no-verify bypass at PR time

Workflow runs scripts/kb_update_check.py against every PR's diff.
Catches the case where a developer used 'git commit --no-verify' to
skip the local pre-commit hook — CI re-applies the rule, blocking
the merge.

Concatenates commit messages across the PR so a kb-not-required:
annotation in any commit in the chain satisfies the gate.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Developer guide + KB updates

**Files:**
- Create: `docs/kb-update-guide.md`
- Modify: `apps/core/docs/opera_knowledge_base.md`
- Create: `~/opera-knowledge-ref/.../business-rules/kb-update-policy.md`

- [ ] **Step 1: Write the developer guide**

```markdown
# KB Update Guide

Every Opera-related code change must be accompanied by an update to the knowledge base. This is enforced by `scripts/kb_update_check.py` running as a pre-commit hook AND a GitHub Actions PR gate.

## What counts as Opera knowledge?

Opera knowledge is anything that explains:

- **How Opera structures data:** table names, column meanings, types, conventions (e.g. `aentry.ae_value` is in pence; `stran.st_trtype='R'` is a receipt).
- **Posting rules:** which tables get touched in what order for a given transaction type, sign conventions, sequence-number rules, period status, locking protocol.
- **Workflow:** what fields user-facing flows write to, how partial reconciles work, what the matcher uses for candidate selection.
- **Recoveries:** how to clear orphan tmpstats, how to undo a misposting, etc.

NOT Opera knowledge:

- Pure refactors (a function rename without behaviour change).
- Typo fixes in comments.
- Dependency bumps in unrelated code.
- Style-only changes.

When you're not sure — err on the side of updating the KB. Over-documentation is cheap; under-documentation is the bug class we're trying to eliminate.

## Local vs central knowledge bases

| | |
|---|---|
| **Local** (`apps/core/docs/opera_knowledge_base.md`) | Project-specific application of Opera knowledge — how this codebase uses it, references to specific functions/files, examples particular to this app. |
| **Central** (`~/opera-knowledge-ref/packages/opera-knowledge/`) | Canonical shared knowledge across all Opera consumers. Used by other developers and tools. |

Both must be updated. The pre-commit hook can only see the local file (the central repo is outside this checkout); for central, **commit AND push to the shared repo as part of the same change cycle**. Reviewers spot-check that you did.

## Central KB directory taxonomy

The central knowledge repo organises content by category:

| Directory | What goes here |
|---|---|
| `schema/` | Table-level documentation: column names, types, descriptions. One file per table family. |
| `business-rules/` | Posting rules, calculation conventions, sign conventions, locking protocol, sequence numbers, type maps. |
| `query-patterns/` | Reusable patterns for common operations (e.g. how to find an unallocated credit note, how to match a refund). |
| `transaction-library/` | **Auto-managed by the snapshot feature — don't edit by hand.** JSON snapshot files plus the deterministically-rendered `COMPLETE_FIELD_REFERENCE.md`. |

If unsure which directory, default to `business-rules/` for behaviour and `schema/` for structure.

## The `kb-not-required:` annotation

For genuine non-knowledge changes that touch an Opera-allowlisted file, add a line to your commit message body:

```
fix: rename a helper function

The Opera write path in opera_sql_import.py was using a private helper
named _do_thing(); rename to _post_to_aentry() for clarity. No new
Opera knowledge — purely a name change.

kb-not-required: pure rename, no behavioural change to Opera writes.
```

The hook checks that the line starts with `kb-not-required:` (case-insensitive) AND has a non-empty reason after the colon. Empty annotations fail the gate.

**Reviewers: scrutinise PRs that use the annotation.** If the change does represent new knowledge in disguise, request the KB update.

## Pushing the central KB

The hook can't push for you (network calls outside commit-hook scope). After committing locally:

```bash
cd ~/opera-knowledge-ref
git pull --rebase origin main   # always rebase, never lose someone else's work
# Make / verify your KB changes here
git add packages/opera-knowledge/<dir>/<file>.md
git commit -m "Document <topic>"
git push origin main
```

The CI gate runs against the main repo and won't catch a missing central push, so this step is on you.

## Examples

**Good — schema field added:**
- Local: append a "## Sequence Number Behaviour" section.
- Central: edit `business-rules/sequence-numbers.md`.
- Commit message: standard.

**Good — non-knowledge change with annotation:**
- Local: untouched.
- Central: untouched.
- Commit message ends with `kb-not-required: rename a helper function with no behavioural change.`

**Bad — Opera change without KB:**
- Local: untouched.
- Central: untouched.
- Commit message: standard, no annotation.
- → Hook blocks the commit.

## Failure modes and what they mean

```
✗ KB update required for Opera-related changes:
    sql_rag/opera_sql_import.py
```

You touched an Opera-allowlisted file. Choose one:

1. Update `apps/core/docs/opera_knowledge_base.md` with a new section, mirror to central.
2. If genuinely no new knowledge: add `kb-not-required: <specific reason>` to the commit body.
3. (If your change shouldn't actually be in the allowlist) edit `scripts/kb_update_allowlist.yaml` and explain in the commit message — that's also a meta-knowledge change worth a KB note.
```

- [ ] **Step 2: Append local KB section**

Append to `apps/core/docs/opera_knowledge_base.md`:

```markdown

## KB Update Policy (mechanically enforced)

Every Opera-related code change must update the knowledge base. Enforced by:

1. **Pre-commit hook** (`.pre-commit-config.yaml` `kb-update-check`) — runs `scripts/kb_update_check.py` before each commit. Refuses if any file in `scripts/kb_update_allowlist.yaml`'s `opera_files` glob list is staged without (a) a corresponding KB file change OR (b) `kb-not-required: <reason>` in the commit message body.
2. **GitHub Actions PR gate** (`.github/workflows/kb-update-check.yml`) — runs the same check against the PR's full diff and concatenated commit messages, catching `--no-verify` local bypass.

Both gates must pass before a PR can merge.

**Reasons matter.** The `kb-not-required:` annotation requires a specific justification, not just "n/a". Reviewers should challenge weak reasons.

**Both KBs.** The local KB lives at `apps/core/docs/opera_knowledge_base.md`. The central KB is the shared repo at `~/opera-knowledge-ref/packages/opera-knowledge/` (cloned from `https://github.com/jonathangintsys/aisam.git`). Local updates only satisfy the hook; **central updates are still mandatory but on the developer to commit + push** (the hook can't reach outside the repo).

**Allowlist:** see `scripts/kb_update_allowlist.yaml`. Adding/removing a glob is a code-review step — explain the rationale.

**Developer guide:** see `docs/kb-update-guide.md` for the directory taxonomy and examples.
```

- [ ] **Step 3: Pull, write, commit, push central KB**

```bash
cd ~/opera-knowledge-ref && git stash push -u -m "preserve unrelated work" 2>/dev/null
cd ~/opera-knowledge-ref && git pull --rebase origin main
cat > ~/opera-knowledge-ref/packages/opera-knowledge/business-rules/kb-update-policy.md <<'EOF'
# Knowledge Base Update Policy

Every Opera-related code change in any consumer repo must be accompanied by a KB update — both the local KB in that repo AND this central KB. This rule is mechanically enforced in `HarryBurdett/llmragsql` by a pre-commit hook + GitHub Actions PR gate; other consumers should mirror the pattern.

## Why central too

Code that touches Opera tables is using shared knowledge. If the consumer learns something (a posting pattern, a field convention, a recovery procedure), every other consumer benefits from knowing it too. Central is the only place that knowledge is durable across repos.

## Directory taxonomy

| Directory | What goes here |
|---|---|
| `schema/` | Table-level documentation: column names, types, descriptions. |
| `business-rules/` | Posting rules, conventions, sign conventions, locking, sequence numbers, type maps. |
| `query-patterns/` | Reusable query patterns. |
| `transaction-library/` | Auto-managed JSON snapshots and the rendered `COMPLETE_FIELD_REFERENCE.md`. Do not edit by hand. |

## When to use the `kb-not-required:` annotation

Only when the change genuinely contains no new Opera knowledge — a pure rename, formatter pass, or unrelated dependency bump that happens to live in an allowlisted file. The reason after the colon must be specific. Reviewers should challenge vague reasons.

## Push policy

Local consumer-repo CI can verify the local KB is updated, but it cannot reach into this central repo. **It is the developer's responsibility to `git push` the central change** as part of the same work cycle. Reviewers spot-check.
EOF
cd ~/opera-knowledge-ref && git add packages/opera-knowledge/business-rules/kb-update-policy.md
cd ~/opera-knowledge-ref && git commit -m "Document KB update policy (mechanically enforced in consumers)

Every Opera-related code change must update both the consumer's local
KB AND this central KB. The HarryBurdett/llmragsql repo enforces with
a pre-commit hook + PR gate; other consumers should mirror.

Includes the directory taxonomy (schema/business-rules/query-patterns/
transaction-library) and the kb-not-required: annotation rules.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
cd ~/opera-knowledge-ref && git push origin main
cd ~/opera-knowledge-ref && git stash pop 2>/dev/null
```

- [ ] **Step 4: Commit local files**

```bash
git -C /Users/maccb/llmragsql add docs/kb-update-guide.md apps/core/docs/opera_knowledge_base.md
git -C /Users/maccb/llmragsql commit -m "$(cat <<'EOF'
docs(kb): KB update guide + policy section

  - docs/kb-update-guide.md: developer-facing manual for the policy.
    What counts as Opera knowledge, local vs central, directory
    taxonomy, escape hatch (kb-not-required:), push policy, examples,
    failure-mode explanations.
  - apps/core/docs/opera_knowledge_base.md: short policy section
    pointing at the guide, the allowlist, and the two enforcement
    layers.

Mirrors the central KB doc at
opera-knowledge-ref/packages/opera-knowledge/business-rules/
kb-update-policy.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git -C /Users/maccb/llmragsql push
```

- [ ] **Step 5: Verify**

```bash
echo "=== Local KB ==="
git -C /Users/maccb/llmragsql log --oneline -1 -- apps/core/docs/opera_knowledge_base.md
echo "=== Local guide ==="
git -C /Users/maccb/llmragsql log --oneline -1 -- docs/kb-update-guide.md
echo "=== Central KB ==="
git -C ~/opera-knowledge-ref log --oneline -1 -- packages/opera-knowledge/business-rules/kb-update-policy.md
echo "=== Central remote sync ==="
git -C ~/opera-knowledge-ref status -sb | head -3
```

---

## Done Criteria

- [ ] `scripts/kb_update_check.py` exists; 7+ unit tests pass.
- [ ] `scripts/kb_update_allowlist.yaml` lists Opera-related globs and KB satisfaction files.
- [ ] Pre-commit hook entry `kb-update-check` is active and the hook runs locally.
- [ ] GitHub Actions workflow blocks PRs that violate the rule.
- [ ] `docs/kb-update-guide.md` exists and is linked from the local KB section.
- [ ] Both KBs (local + central) document the policy and reach their remotes.
- [ ] Allowlist YAML is malformed-proof: hook exits 2 with a clear message rather than silently passing.
