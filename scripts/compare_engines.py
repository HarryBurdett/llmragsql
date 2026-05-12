#!/usr/bin/env python3
"""
Compare a transaction's Opera SE capture with its Opera 3 capture.

Both engines should write to the same Opera tables when posting the
same transaction type — but in practice they differ in:
  - Table naming (Opera 3 may prefix with company code: `z_pname` vs `pname`)
  - Column case (Opera 3 FoxPro typically UPPER, SE SQL Server may differ)
  - Value scaling (some columns in pence on SE, pounds on Opera 3, or vice versa)
  - Tables present in one engine but not the other (e.g. SE-only audit tables)

This script walks the transaction-library entries for a given
transaction (e.g. `sales_ledger_invoice`), pairs them by name, and
produces a structured diff highlighting:
  - Tables touched in both engines (with field-level alignment)
  - Tables touched in one engine only
  - Fields modified in both — value-for-value
  - Fields modified in one engine only

Usage:

    python scripts/compare_engines.py <transaction-stem>

Example:

    python scripts/compare_engines.py sales_ledger_invoice

Output: markdown report on stdout. Pipe to a file or paste into the
central knowledge if you want to archive the comparison.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

LIBRARY_ROOT = Path.home() / 'opera-knowledge-ref' / 'packages' / 'opera-knowledge' / 'transaction-library'


def find_latest_entry(stem: str, engine_subdir: str) -> Optional[Path]:
    """Most recent library entry whose filename starts with `stem`
    in the given engine subdirectory (opera_se/ or opera_3/)."""
    folder = LIBRARY_ROOT / engine_subdir
    if not folder.is_dir():
        return None
    candidates = sorted(
        [p for p in folder.glob('*.json') if p.stem.startswith(stem)],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def list_matching(stem: str, engine_subdir: str) -> list[Path]:
    folder = LIBRARY_ROOT / engine_subdir
    if not folder.is_dir():
        return []
    return sorted([p for p in folder.glob('*.json') if p.stem.startswith(stem)])


# Opera 3 prefixes (company code) we strip when aligning table names
# against SE. Real installs use a single letter (A, B, Z, ...) followed
# by underscore. Strip the first `<letter>_` segment if present.
_PREFIX_RX = re.compile(r'^[a-z]_')


def canonical_table(name: str) -> str:
    """Normalise a table name across engines for alignment.
    Lowercase, then strip an Opera-3-style single-letter company prefix
    so `z_pname` and `Z_PNAME` and `pname` all canonicalise to `pname`."""
    n = name.lower()
    return _PREFIX_RX.sub('', n)


def canonical_field(name: str) -> str:
    return name.lower()


def build_change_index(entry: dict) -> dict[str, dict]:
    """Group an entry's changes by canonical table name. Returns:
        {
            canonical_table: {
                'orig_names': set of original table names,
                'rows_added': int,
                'rows_deleted': int,
                'rows_modified': int,
                'fields_modified': set of canonical field names,
                'fields_seen_by_orig': map of orig field name → canonical (for case-mismatch detection)
            }
        }
    """
    out: dict[str, dict] = {}
    for change in entry.get('changes', []):
        ct = canonical_table(change.get('table', ''))
        bucket = out.setdefault(ct, {
            'orig_names': set(),
            'rows_added': 0, 'rows_deleted': 0, 'rows_modified': 0,
            'fields_modified': set(),
            'fields_seen_by_orig': {},
        })
        bucket['orig_names'].add(change.get('table', ''))
        bucket['rows_added'] += change.get('rows_added', 0) or 0
        bucket['rows_deleted'] += change.get('rows_deleted', 0) or 0
        bucket['rows_modified'] += change.get('rows_modified', 0) or 0
        for f in change.get('fields_modified') or []:
            cf = canonical_field(f)
            bucket['fields_modified'].add(cf)
            bucket['fields_seen_by_orig'][f] = cf
    return out


def render_report(stem: str, se_entry: Optional[dict], se_path: Optional[Path],
                  o3_entry: Optional[dict], o3_path: Optional[Path]) -> str:
    lines = []
    lines.append(f"# Engine comparison — `{stem}`")
    lines.append('')
    lines.append(f"- **Opera SE source**: `{se_path.relative_to(LIBRARY_ROOT) if se_path else '— not captured'}`")
    lines.append(f"- **Opera 3 source**: `{o3_path.relative_to(LIBRARY_ROOT) if o3_path else '— not captured'}`")
    lines.append('')

    if not (se_entry and o3_entry):
        lines.append('## Cannot compare')
        if not se_entry:
            lines.append('- No Opera SE entry found for this transaction. Capture one first.')
        if not o3_entry:
            lines.append('- No Opera 3 entry found for this transaction. Capture one first.')
        return '\n'.join(lines) + '\n'

    se_idx = build_change_index(se_entry)
    o3_idx = build_change_index(o3_entry)

    se_tables = set(se_idx.keys())
    o3_tables = set(o3_idx.keys())
    both = sorted(se_tables & o3_tables)
    only_se = sorted(se_tables - o3_tables)
    only_o3 = sorted(o3_tables - se_tables)

    lines.append('## Summary')
    lines.append('')
    lines.append(f"- SE touched **{len(se_tables)}** tables; Opera 3 touched **{len(o3_tables)}** tables")
    lines.append(f"- Touched by **both engines**: {len(both)}")
    lines.append(f"- Touched by **SE only**: {len(only_se)}")
    lines.append(f"- Touched by **Opera 3 only**: {len(only_o3)}")
    lines.append('')

    if only_se:
        lines.append('## Tables touched by SE only')
        lines.append('')
        for t in only_se:
            origs = ', '.join(sorted(se_idx[t]['orig_names']))
            lines.append(f"- `{t}` (as `{origs}`)")
        lines.append('')

    if only_o3:
        lines.append('## Tables touched by Opera 3 only')
        lines.append('')
        for t in only_o3:
            origs = ', '.join(sorted(o3_idx[t]['orig_names']))
            lines.append(f"- `{t}` (as `{origs}`)")
        lines.append('')

    if both:
        lines.append('## Tables touched by both — field-level comparison')
        lines.append('')
        for t in both:
            se = se_idx[t]; o3 = o3_idx[t]
            lines.append(f"### `{t}`")
            lines.append(f"- SE original name(s): `{', '.join(sorted(se['orig_names']))}`")
            lines.append(f"- O3 original name(s): `{', '.join(sorted(o3['orig_names']))}`")
            lines.append(f"- Row counts — SE: added {se['rows_added']}, modified {se['rows_modified']}, deleted {se['rows_deleted']}")
            lines.append(f"- Row counts — O3: added {o3['rows_added']}, modified {o3['rows_modified']}, deleted {o3['rows_deleted']}")
            se_f = se['fields_modified']
            o3_f = o3['fields_modified']
            in_both = sorted(se_f & o3_f)
            se_only = sorted(se_f - o3_f)
            o3_only = sorted(o3_f - se_f)
            if in_both:
                lines.append(f"- Fields modified in **both engines**: {', '.join(f'`{x}`' for x in in_both)}")
            if se_only:
                lines.append(f"- Fields modified by **SE only**: {', '.join(f'`{x}`' for x in se_only)}")
            if o3_only:
                lines.append(f"- Fields modified by **O3 only**: {', '.join(f'`{x}`' for x in o3_only)}")
            lines.append('')

    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser(description="Compare Opera SE vs Opera 3 captures of the same transaction.")
    parser.add_argument('stem', help='Transaction-stem to compare (e.g. sales_ledger_invoice).')
    parser.add_argument('--list', action='store_true', help='List matching entries in both engines and exit.')
    args = parser.parse_args()

    if args.list:
        for engine in ('opera_se', 'opera_3'):
            entries = list_matching(args.stem, engine)
            print(f"== {engine} ({len(entries)}) ==")
            for p in entries:
                print(f"  {p.name}")
        return

    se_path = find_latest_entry(args.stem, 'opera_se')
    o3_path = find_latest_entry(args.stem, 'opera_3')
    se_entry = json.load(open(se_path)) if se_path else None
    o3_entry = json.load(open(o3_path)) if o3_path else None

    print(render_report(args.stem, se_entry, se_path, o3_entry, o3_path))


if __name__ == '__main__':
    main()
