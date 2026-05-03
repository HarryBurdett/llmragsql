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
