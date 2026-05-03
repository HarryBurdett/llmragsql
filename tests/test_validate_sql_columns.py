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
