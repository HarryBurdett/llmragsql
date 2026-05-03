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
