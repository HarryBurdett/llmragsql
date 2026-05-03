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
