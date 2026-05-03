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
    cb_idx = rendered.find("## Cashbook")
    sl_idx = rendered.find("## Sales Ledger")
    st_idx = rendered.find("## Stock")
    # cashbook (idx 0 in MODULE_ORDER) appears before sales_ledger (idx 1)
    # which appears before stock (idx 7).
    assert 0 < cb_idx < sl_idx, (
        f"cashbook should precede sales_ledger; cb={cb_idx} sl={sl_idx}"
    )


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
