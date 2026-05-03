"""
Validate Opera SQL column references in the codebase against the canonical
schema snapshot at scripts/opera_snapshot.json.

What it does
------------
Walks every .py file under sql_rag/, apps/, api/, scripts/, scans for SQL
string literals, extracts identifiers that look like Opera column names
(e.g. ``pt_trref``), and checks each one exists in the schema. Where the
SQL has a FROM/UPDATE/JOIN clause we narrow to the columns of those
specific tables — so ``pt_ref`` queried against ``ptran`` is flagged as
"unknown column for table ptran (did you mean pt_trref?)".

Why
---
Opera column typos like ``pt_ref`` (should be ``pt_trref``), ``at_date``
(should be ``at_pstdate``), ``st_tref`` (should be ``st_trref``) keep
slipping through because pyodbc raises at query time and the calling
code often catches the exception as a generic warning. This validator
is a static pre-flight: it surfaces every typo in one report so we don't
keep finding them one bug at a time in production.

Usage
-----

  python scripts/validate_sql_columns.py            # report only
  python scripts/validate_sql_columns.py --strict   # exit 1 if any unknown columns found
  python scripts/validate_sql_columns.py --suggest  # also propose fixes via Levenshtein

Output groups problems by file with line numbers and the suggested
correct column (if any) from the schema.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from difflib import get_close_matches
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None  # validated at runtime

ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT_PATH = ROOT / 'scripts' / 'opera_snapshot.json'

# Roots to scan
SCAN_ROOTS = ['sql_rag', 'apps', 'api', 'scripts']

# Files / dirs to skip (snapshot data, archived analyses, generated demos)
SKIP_PATH_PARTS = {
    'venv', '__pycache__', '.pytest_cache', 'node_modules',
    'archive', '_transaction_snapshots', 'demos', 'marketing',
}

# Skip any file matching these names — they tend to be data dumps or
# analyses with synthetic SQL, not production queries.
SKIP_FILE_NAMES = {
    'opera_snapshot.json', 'validate_sql_columns.py',
}

# SQL strings we recognise (must contain at least one of these keywords)
SQL_KEYWORDS_RE = re.compile(r'\b(SELECT|INSERT|UPDATE|DELETE|FROM|WHERE|JOIN|VALUES)\b', re.IGNORECASE)

# Match Opera-style column identifiers: 2-3 letter prefix, underscore, then
# alphanumerics. Excludes one-letter prefixes (filter false positives) and
# single underscore names. Captures things like at_pstdate, ae_value,
# nt_subt, sn_currbal, np_year, zv_vatcode, etc.
OPERA_COL_RE = re.compile(r'\b([a-z]{2,4})_([a-z][a-z0-9_]{0,30})\b')

# Known false-positive identifiers that match the regex but are NOT
# Opera column names — Python attrs, common variable names, dict keys
# that happen to look column-like, etc.
COL_FALSE_POSITIVES = {
    # Python / general
    'os_path', 're_match', 're_search', 're_sub', 'self_ref',
    # FastAPI / Pydantic / SQLAlchemy
    'json_dumps', 'json_loads',
    # Common multi-word identifiers in our codebase that aren't columns
    'co_data', 'co_id',
}

# Prefixes that are NOT Opera but match the column-shaped regex. These
# typically belong to SQL Server DMVs (sys.dm_*), PostgreSQL system
# catalogs (pg_*), application-local SQLite tables, or Python locals.
SKIP_PREFIXES = {
    'dm',   # SQL Server dynamic management views (sys.dm_exec_*)
    'pg',   # PostgreSQL system tables
    'db',   # database_name / db_path / db_err — Python locals
    'fx',   # local foreign-exchange dataclasses, NOT Opera
    'as',   # as_of_date param
    # Application-local SQLite columns (not Opera schema)
    'zc',   # zcontacts (our local supplier-contacts table)
}

# Map common Opera column prefixes back to the table they belong to. Built
# from the snapshot at runtime, but seeded here for tables whose prefix
# is non-obvious.
PREFIX_OVERRIDES = {
    # Table prefix : list of tables where this prefix's columns live
    # (intentionally empty — the snapshot derives this automatically)
}


def load_snapshot(path: Optional[Path] = None) -> Dict[str, List[str]]:
    """Return mapping of table_name -> list of column names."""
    snap_path = path if path is not None else SNAPSHOT_PATH
    with open(snap_path) as f:
        snap = json.load(f)
    tables = snap.get('tables', {})
    return {tname: list(tdef.get('columns', [])) for tname, tdef in tables.items()}


def build_indices(schema: Dict[str, List[str]]) -> Tuple[Set[str], Dict[str, Set[str]], Dict[str, Set[str]]]:
    """
    Returns:
        all_columns: every column name that exists anywhere in Opera
        col_to_tables: column_name -> set of tables containing it
        prefix_to_tables: 2-3 letter prefix -> set of tables whose columns
                          predominantly use that prefix
    """
    all_columns: Set[str] = set()
    col_to_tables: Dict[str, Set[str]] = defaultdict(set)
    prefix_table_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for table, cols in schema.items():
        for col in cols:
            all_columns.add(col)
            col_to_tables[col].add(table)
            m = OPERA_COL_RE.match(col)
            if m:
                prefix = m.group(1)
                prefix_table_counts[prefix][table] += 1

    # For each prefix, the table(s) where it's the dominant convention.
    prefix_to_tables: Dict[str, Set[str]] = {}
    for prefix, table_counts in prefix_table_counts.items():
        max_count = max(table_counts.values())
        if max_count >= 3:  # prefix used by ≥3 columns in the table
            prefix_to_tables[prefix] = {
                t for t, c in table_counts.items() if c >= max(3, max_count // 2)
            }
    return all_columns, dict(col_to_tables), prefix_to_tables


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


def _is_suppressed(finding: Dict, file_path: Path, suppressions: list) -> bool:
    """Match a finding against the suppression list. A suppression
    matches if file path resolves to the same path AND line number
    matches AND column matches.
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


def extract_sql_blocks(source: str) -> List[Tuple[int, str]]:
    """
    Return list of (line_number, block_text) for each multiline string
    literal that contains SQL keywords. Uses a simple parser-ish scan
    over Python triple-quoted and f-string literals — good enough for
    the project's existing style.
    """
    blocks: List[Tuple[int, str]] = []

    # Triple-quoted strings (handles f-strings via the leading f"" / f''')
    for m in re.finditer(r'(?:[fr]{0,2})("""|\'\'\')(.*?)\1', source, re.DOTALL):
        block = m.group(2)
        if SQL_KEYWORDS_RE.search(block):
            line_no = source.count('\n', 0, m.start()) + 1
            blocks.append((line_no, block))

    # Single-line ordinary or f-strings that look SQL-shaped
    for m in re.finditer(r'(?:[fr]{0,2})(["\'])((?:\\.|(?!\1).){8,})\1', source):
        block = m.group(2)
        if SQL_KEYWORDS_RE.search(block) and len(block) >= 20:
            line_no = source.count('\n', 0, m.start()) + 1
            blocks.append((line_no, block))

    return blocks


def tables_referenced_in_sql(sql: str) -> Set[str]:
    """
    Pick out table names following FROM / JOIN / UPDATE / INTO. Strips
    ``WITH (NOLOCK)``, aliases, and the ``schema.`` prefix. Returns lower-cased.
    """
    tables: Set[str] = set()
    for m in re.finditer(
        r'\b(?:FROM|JOIN|UPDATE|INTO)\s+(?:\[?[a-zA-Z0-9_]+\]?\.)?\[?([a-zA-Z][a-zA-Z0-9_]*)\]?',
        sql, re.IGNORECASE,
    ):
        name = m.group(1).lower()
        # Strip alias / subquery noise
        if name in {'with', 'nolock', 'select', 'as', 'on', 'inner', 'left', 'outer'}:
            continue
        tables.add(name)
    return tables


def validate_file(
    path: Path,
    all_columns: Set[str],
    col_to_tables: Dict[str, Set[str]],
    prefix_to_tables: Dict[str, Set[str]],
    schema: Dict[str, List[str]],
) -> List[Dict]:
    """Return list of finding dicts: {line, column, table_context, suggestion}."""
    try:
        text = path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return []

    findings: List[Dict] = []
    for line_no, block in extract_sql_blocks(text):
        ref_tables = tables_referenced_in_sql(block)
        # Set of expected columns (for the tables this SQL touches)
        expected_cols: Set[str] = set()
        for t in ref_tables:
            cols = schema.get(t, [])
            expected_cols.update(cols)

        # Build the set of identifiers introduced by the query as ALIASES
        # (i.e. ``... AS at_date`` or ``... AS my_alias``). These are not
        # column references — they're new names defined by the query.
        # Without this filter every alias of an Opera column would also
        # be flagged downstream.
        alias_names: Set[str] = set()
        for am in re.finditer(r'\bAS\s+([a-z][a-z0-9_]*)\b', block, re.IGNORECASE):
            alias_names.add(am.group(1).lower())

        # Each Opera-style identifier in the SQL block
        for m in OPERA_COL_RE.finditer(block):
            col = f"{m.group(1)}_{m.group(2)}"
            if col in COL_FALSE_POSITIVES:
                continue
            if col in alias_names:
                # The name is defined by the query itself, not a column ref.
                continue
            # If the prefix doesn't belong to any Opera table, skip — likely
            # a Python identifier (e.g. an unrelated variable).
            prefix = m.group(1)
            if prefix in SKIP_PREFIXES:
                continue
            if prefix not in prefix_to_tables:
                continue
            # Skip identifiers that immediately precede ``AS`` — they're the
            # source of an alias and we already track aliases separately.
            # (e.g. ``at_pstdate as at_date`` — the at_date alias is
            # captured above; at_pstdate is the real column.)
            tail = block[m.end():m.end() + 6].lstrip()
            # Skip identifiers in well-known "looks-column-but-isn't" suffixes
            # like ``_clause`` / ``_flag`` / ``_code`` (Python f-string
            # placeholder names that look like Opera columns)
            suffix_part = m.group(2)
            if suffix_part.endswith(('_clause', '_flag', '_code_str', '_ledger')):
                continue
            # Known column? Fine.
            if col in all_columns:
                # Optional stricter check: this column exists somewhere, but
                # is it in one of the referenced tables? Only flag a
                # narrowed mismatch if we DO know the table context.
                if ref_tables and expected_cols and col not in expected_cols:
                    # Column exists, but not in any of the FROM/UPDATE tables
                    # for this query. This catches e.g. selecting `pt_trref`
                    # from a query that only joins stran.
                    findings.append({
                        'line': line_no + block[:m.start()].count('\n'),
                        'column': col,
                        'table_context': sorted(ref_tables),
                        'category': 'wrong_table',
                        'suggestion': f"column exists in {sorted(col_to_tables[col])} but query is on {sorted(ref_tables)}",
                    })
                continue
            # Unknown column. Try to suggest a correction.
            # Prefer suggestions from the SQL's referenced tables; fall
            # back to anywhere in the schema.
            candidate_pool = list(expected_cols) if expected_cols else list(all_columns)
            suggestions = get_close_matches(col, candidate_pool, n=3, cutoff=0.7)
            findings.append({
                'line': line_no + block[:m.start()].count('\n'),
                'column': col,
                'table_context': sorted(ref_tables),
                'category': 'unknown',
                'suggestion': suggestions or [],
            })

    # De-duplicate (same column on same line reported multiple times)
    seen = set()
    unique: List[Dict] = []
    for f in findings:
        key = (f['line'], f['column'], f['category'])
        if key in seen:
            continue
        seen.add(key)
        unique.append(f)
    return unique


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
    suppressions: list = []
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
            print(f"ERROR: cannot parse suppressions file {suppressions_path}: {e}", file=sys.stderr)
            return 3

    schema = load_snapshot(snapshot_path)
    all_columns, col_to_tables, prefix_to_tables = build_indices(schema)

    files = iter_python_files(args.scan_root)
    print(f"Scanning {len(files)} Python files against {len(schema)} Opera tables "
          f"({len(all_columns)} unique columns)…\n")

    total_unknown = 0
    total_wrong_table = 0
    findings_by_file: Dict[Path, List[Dict]] = {}

    for path in files:
        findings = validate_file(path, all_columns, col_to_tables, prefix_to_tables, schema)
        if not findings:
            continue
        findings_by_file[path] = findings

    for path, findings in sorted(findings_by_file.items()):
        rel = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
        unknowns = [f for f in findings if f['category'] == 'unknown'
                    and not _is_suppressed(f, path, suppressions)]
        wrong = [f for f in findings if f['category'] == 'wrong_table']
        if not unknowns and not wrong:
            continue
        if not args.include_wrong_table and not unknowns:
            continue
        print(f"=== {rel} ===")
        for f in unknowns:
            ctx = ', '.join(f['table_context']) or '?'
            sugg = ''
            if args.suggest and f['suggestion']:
                sugg = f"  — did you mean: {', '.join(f['suggestion'])}"
            print(f"  L{f['line']:>5}  UNKNOWN  {f['column']:24s}  (tables: {ctx}){sugg}")
            total_unknown += 1
        if args.include_wrong_table:
            for f in wrong:
                print(f"  L{f['line']:>5}  WRONG_TABLE  {f['column']:24s}  {f['suggestion']}")
                total_wrong_table += 1
        print()

    print(f"\nSummary: {total_unknown} unknown columns" +
          (f", {total_wrong_table} wrong-table references" if args.include_wrong_table else ""))

    if args.strict and total_unknown > 0:
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
