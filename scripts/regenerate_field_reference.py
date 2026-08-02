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

    Scans both the flat root (for pre-2026-05-12 entries) AND the
    engine-specific subfolders (opera_se/, opera_3/) introduced on
    2026-05-12 in commit 16bf957. Without the subfolder scan the
    rollup silently goes stale — none of the post-reorg entries land
    in COMPLETE_FIELD_REFERENCE.md.

    Excludes the rollup itself (COMPLETE_FIELD_REFERENCE.md) and any
    non-JSON files. Order is deterministic — sort by filename.
    """
    if not library_dir.exists():
        raise FileNotFoundError(
            f"snapshot library directory not found: {library_dir}"
        )
    # Flat root + engine subfolders. Deduplicate by filename in case the
    # same entry exists at both layers (shouldn't, but be defensive).
    candidates: dict[str, Path] = {}
    for p in library_dir.glob("*.json"):
        candidates[p.name] = p
    for sub in ("opera_se", "opera_3"):
        sub_path = library_dir / sub
        if sub_path.is_dir():
            for p in sub_path.glob("*.json"):
                candidates[p.name] = p
    return sorted(candidates.values(), key=lambda p: p.name)


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
        # Read with newline='' so Python's universal-newlines mode does
        # NOT silently translate any '\r' bytes in the file to '\n'. Some
        # snapshot source data carries embedded CR characters (e.g. old
        # multi-line Opera memo fields) that the renderer preserves; the
        # write path stores those as-is, so the read path must also
        # leave them alone or the comparison reports false STALE.
        # (Path.read_text gained `newline=` only in 3.13 — use open().)
        with open(output, "r", encoding="utf-8", newline="") as f:
            existing = f.read()
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
