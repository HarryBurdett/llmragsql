#!/usr/bin/env python3
"""
Opera 3 (FoxPro DBF) snapshot script.

Companion to snapshot_opera.py (which targets Opera SE / SQL Server).
Output shape matches snapshot_opera.py so the two snapshots can be
diffed directly.

Reads every .dbf file in the supplied Opera 3 data folder via
sql_rag.opera3_foxpro.Opera3Reader (dbfread under the hood) and
captures field names, types, record count, and up to N sample
records per table.

Usage (from project root):
    python scripts/snapshot_opera3.py /path/to/opera3/data
    python scripts/snapshot_opera3.py /path/to/opera3/data \\
        --output scripts/opera3_snapshot.json
    python scripts/snapshot_opera3.py /path/to/opera3/data --limit 100

Default output: scripts/opera3_snapshot.json (sibling of opera_snapshot.json)
Default sample limit per table: 500 (matches snapshot_opera.py)

Output structure:
    {
      "timestamp": "...",
      "data_path": "...",
      "engine": "foxpro",
      "tables": {
        "pname": {
          "exists": true,
          "columns": [...],
          "field_types": {col: type_char},
          "records": [...],
          "count": N,
          "sampled": M,
          "key_field": "...",
          "last_modified": "..."
        },
        ...
      }
    }

See docs/sam-rewrite/snapshot_opera3.md for full operating notes.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path

# Allow running from project root
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_OUTPUT = Path(__file__).parent / 'opera3_snapshot.json'

try:
    from sql_rag.opera3_foxpro import Opera3Reader, DBF_AVAILABLE
except ImportError as e:
    print(f"Could not import Opera3Reader: {e}", file=sys.stderr)
    print("Make sure you're running from the project root.", file=sys.stderr)
    sys.exit(2)


def serialize_value(val):
    """Match snapshot_opera.py's serialisation."""
    if val is None:
        return None
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, bytes):
        return val.hex()
    if isinstance(val, str):
        return val.rstrip()
    return val


def guess_key_field(columns):
    """Same heuristic as snapshot_opera.py."""
    columns_lower = [c.lower() for c in columns]
    key_patterns = ['_unique', '_pstid', '_entry', '_jrnl', '_id', '_account', '_acnt', '_code']
    for pattern in key_patterns:
        for i, col in enumerate(columns_lower):
            if col.endswith(pattern):
                return columns[i]
    return columns[0] if columns else None


def snapshot_table(reader: Opera3Reader, table_name: str, limit: int = 500):
    try:
        structure = reader.get_table_structure(table_name)
    except Exception as e:
        return {'exists': False, 'error': str(e), 'columns': [], 'records': []}

    fields = structure.get('fields') or []
    columns = [f['name'] for f in fields]
    field_types = {f['name']: f.get('type') for f in fields}
    record_count = structure.get('record_count', 0)
    last_modified = structure.get('last_modified')

    if not columns:
        return {
            'exists': True,
            'columns': [],
            'field_types': {},
            'records': [],
            'count': record_count,
            'key_field': None,
            'last_modified': last_modified,
        }

    try:
        records_raw = reader.read_table(table_name, limit=limit)
    except Exception as e:
        return {
            'exists': False,
            'error': str(e),
            'columns': columns,
            'field_types': field_types,
            'records': [],
        }

    records = []
    for r in records_raw or []:
        rec = {col: serialize_value(r.get(col)) for col in columns}
        records.append(rec)

    return {
        'exists': True,
        'columns': columns,
        'field_types': field_types,
        'records': records,
        'count': record_count,
        'sampled': len(records),
        'key_field': guess_key_field(columns),
        'last_modified': last_modified,
    }


def snapshot_all_tables(reader: Opera3Reader, limit: int = 500):
    table_listings = reader.list_tables(include_unknown=True)
    print(f"  Found {len(table_listings)} DBF tables in {reader.data_path}")

    snapshot = {
        'timestamp': datetime.now().isoformat(),
        'data_path': str(reader.data_path),
        'engine': 'foxpro',
        'tables': {},
    }

    for entry in table_listings:
        table_name = entry['name']
        print(f"  {table_name}...", end=' ', flush=True)
        table_data = snapshot_table(reader, table_name, limit=limit)
        snapshot['tables'][table_name] = table_data
        if table_data.get('exists'):
            count = table_data.get('count', 0)
            sampled = table_data.get('sampled', 0)
            print(f"{count} records ({sampled} sampled)")
        else:
            print(f"error: {table_data.get('error', 'unknown')}")

    return snapshot


def main():
    parser = argparse.ArgumentParser(
        description='Snapshot an Opera 3 (FoxPro) installation. '
                    'Companion to snapshot_opera.py for SQL Server SE.'
    )
    parser.add_argument(
        'data_path',
        help='Path to Opera 3 data folder (e.g. /mnt/opera3 or C:\\Apps\\O3 Server VFP)',
    )
    parser.add_argument(
        '--output',
        default=str(DEFAULT_OUTPUT),
        help=f'Output JSON path (default: {DEFAULT_OUTPUT})',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=500,
        help='Max records to sample per table (default: 500)',
    )
    args = parser.parse_args()

    if not DBF_AVAILABLE:
        print(
            'dbfread is not installed. Run: pip install dbfread',
            file=sys.stderr,
        )
        sys.exit(2)

    data_path = Path(args.data_path)
    if not data_path.exists():
        print(f'Data path does not exist: {data_path}', file=sys.stderr)
        sys.exit(2)

    print(f'Snapshotting Opera 3 data from {data_path}')
    reader = Opera3Reader(str(data_path))
    snapshot = snapshot_all_tables(reader, limit=args.limit)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(snapshot, indent=2, default=serialize_value))
    print(f'\nSnapshot written to {output}')
    print(f'Tables: {len(snapshot["tables"])}')
    print(f'Engine: {snapshot["engine"]}')


if __name__ == '__main__':
    main()
