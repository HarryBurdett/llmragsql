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
