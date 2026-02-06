"""
Opera 3 Database Snapshot Utility

Takes snapshots of Opera 3 FoxPro tables before and after an operation
to understand exactly how Opera 3 updates the database.

Usage:
    1. Run take_snapshot('before')
    2. Perform operation in Opera 3
    3. Run take_snapshot('after')
    4. Run compare_snapshots() to see differences
"""

import json
import os
from datetime import datetime
from typing import Optional, Dict, List
from sql_rag.opera3_foxpro import get_opera3_reader

SNAPSHOT_DIR = os.path.join(os.path.dirname(__file__), '..', 'snapshots', 'opera3')

# Tables to snapshot for cashbook/batch transactions
CASHBOOK_TABLES = [
    ('aentry', 'ae_entry', 'ae_date'),     # Cashbook entries (header)
    ('abatch', 'ab_entry', 'ab_entry'),    # Batch header
    ('atran', 'at_entry', 'at_date'),      # Cashbook transactions
    ('stran', 'st_unique', 'st_date'),     # Sales ledger transactions
    ('ptran', 'pt_unique', 'pt_date'),     # Purchase ledger transactions
    ('ntran', 'nt_unique', 'nt_date'),     # Nominal ledger transactions
    ('salloc', 'al_truniq', 'al_truniq'),  # Sales allocations
    ('palloc', 'al_truniq', 'al_truniq'),  # Purchase allocations
    ('sname', 'sn_account', 'sn_account'), # Customer balances
    ('pname', 'pn_account', 'pn_account'), # Supplier balances
    ('anoml', 'ax_unique', 'ax_unique'),   # Cashbook transfer file
]


def ensure_snapshot_dir():
    """Create snapshot directory if it doesn't exist."""
    if not os.path.exists(SNAPSHOT_DIR):
        os.makedirs(SNAPSHOT_DIR)


def take_snapshot(name: str, limit: int = 500, tables: Optional[List] = None) -> str:
    """
    Take a snapshot of Opera 3 FoxPro tables.

    Args:
        name: Snapshot name (e.g., 'before', 'after')
        limit: Number of recent records to capture per table
        tables: Optional list of tables to snapshot (defaults to CASHBOOK_TABLES)

    Returns:
        Path to the snapshot file
    """
    ensure_snapshot_dir()

    fox = get_opera3_reader()
    tables_to_snap = tables or CASHBOOK_TABLES

    snapshot = {
        'name': name,
        'timestamp': datetime.now().isoformat(),
        'tables': {}
    }

    for table_info in tables_to_snap:
        table_name = table_info[0]
        pk_field = table_info[1]
        order_by = table_info[2] if len(table_info) > 2 else table_info[1]

        try:
            # Read all records and get most recent
            df = fox.read_table(table_name)

            if df is not None and len(df) > 0:
                # Get column names
                columns = df.columns.tolist()

                # Sort by order field if it exists
                if order_by in columns:
                    df = df.sort_values(by=order_by, ascending=False)

                # Take most recent records
                df = df.head(limit)

                # Convert to list of dicts
                records = []
                for _, row in df.iterrows():
                    record = {}
                    for col in columns:
                        val = row.get(col)
                        if val is None or (hasattr(val, '__len__') and len(str(val)) == 0):
                            record[col] = None
                        elif hasattr(val, 'isoformat'):
                            record[col] = val.isoformat()
                        elif isinstance(val, bytes):
                            record[col] = val.hex()
                        else:
                            try:
                                record[col] = float(val) if isinstance(val, (int, float)) else str(val).strip()
                            except:
                                record[col] = str(val)
                    records.append(record)

                snapshot['tables'][table_name] = {
                    'columns': columns,
                    'records': records,
                    'count': len(records)
                }
                print(f"  {table_name}: {len(records)} records")
            else:
                snapshot['tables'][table_name] = {
                    'columns': [],
                    'records': [],
                    'count': 0
                }
                print(f"  {table_name}: 0 records (or table not found)")

        except Exception as e:
            print(f"  Error snapshotting {table_name}: {e}")
            snapshot['tables'][table_name] = {'error': str(e)}

    # Save snapshot
    filename = f"snapshot_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(SNAPSHOT_DIR, filename)

    with open(filepath, 'w') as f:
        json.dump(snapshot, f, indent=2, default=str)

    print(f"\nSnapshot saved to: {filepath}")
    return filepath


def load_snapshot(filepath: str) -> dict:
    """Load a snapshot from file."""
    with open(filepath, 'r') as f:
        return json.load(f)


def find_latest_snapshots() -> tuple:
    """Find the latest 'before' and 'after' snapshots."""
    ensure_snapshot_dir()

    files = os.listdir(SNAPSHOT_DIR)
    before_files = sorted([f for f in files if 'before' in f.lower()], reverse=True)
    after_files = sorted([f for f in files if 'after' in f.lower()], reverse=True)

    before_path = os.path.join(SNAPSHOT_DIR, before_files[0]) if before_files else None
    after_path = os.path.join(SNAPSHOT_DIR, after_files[0]) if after_files else None

    return before_path, after_path


def compare_snapshots(before_path: Optional[str] = None, after_path: Optional[str] = None) -> dict:
    """
    Compare two snapshots and show differences.

    Args:
        before_path: Path to before snapshot (auto-detects if None)
        after_path: Path to after snapshot (auto-detects if None)

    Returns:
        Dictionary of differences
    """
    if before_path is None or after_path is None:
        auto_before, auto_after = find_latest_snapshots()
        before_path = before_path or auto_before
        after_path = after_path or auto_after

    if not before_path or not after_path:
        print("Error: Could not find before and after snapshots")
        return {}

    print(f"Comparing:")
    print(f"  Before: {os.path.basename(before_path)}")
    print(f"  After:  {os.path.basename(after_path)}")
    print()

    before = load_snapshot(before_path)
    after = load_snapshot(after_path)

    differences = {
        'new_records': {},
        'modified_records': {},
        'summary': []
    }

    for table_name in after['tables']:
        if 'error' in after['tables'][table_name]:
            continue

        after_records = after['tables'][table_name].get('records', [])
        before_records = before['tables'].get(table_name, {}).get('records', [])

        # Get primary key field
        pk_field = None
        for t in CASHBOOK_TABLES:
            if t[0] == table_name:
                pk_field = t[1]
                break

        if not pk_field:
            continue

        # Index before records by primary key
        before_by_pk = {}
        for rec in before_records:
            pk_val = rec.get(pk_field, '')
            if pk_val:
                before_by_pk[str(pk_val).strip()] = rec

        # Find new and modified records
        new_records = []
        modified_records = []

        for rec in after_records:
            pk_val = str(rec.get(pk_field, '')).strip()
            if not pk_val:
                continue

            if pk_val not in before_by_pk:
                new_records.append(rec)
            else:
                # Check if modified
                before_rec = before_by_pk[pk_val]
                changes = {}
                for key in rec:
                    before_val = str(before_rec.get(key, '')).strip()
                    after_val = str(rec.get(key, '')).strip()
                    if before_val != after_val:
                        changes[key] = {'before': before_val, 'after': after_val}
                if changes:
                    modified_records.append({'pk': pk_val, 'changes': changes})

        if new_records:
            differences['new_records'][table_name] = new_records
            differences['summary'].append(f"{table_name}: {len(new_records)} new record(s)")
            print(f"=== {table_name}: {len(new_records)} NEW RECORD(S) ===")
            for rec in new_records:
                print(f"\n  {pk_field}: {rec.get(pk_field)}")
                for key, val in rec.items():
                    if val and str(val).strip():
                        print(f"    {key}: {val}")

        if modified_records:
            differences['modified_records'][table_name] = modified_records
            differences['summary'].append(f"{table_name}: {len(modified_records)} modified record(s)")
            print(f"\n=== {table_name}: {len(modified_records)} MODIFIED RECORD(S) ===")
            for mod in modified_records:
                print(f"\n  {pk_field}: {mod['pk']}")
                for key, change in mod['changes'].items():
                    print(f"    {key}: {change['before']} -> {change['after']}")

    if not differences['summary']:
        print("No differences found between snapshots.")

    # Save comparison results
    result_file = os.path.join(SNAPSHOT_DIR, f"comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(result_file, 'w') as f:
        json.dump(differences, f, indent=2, default=str)
    print(f"\nComparison saved to: {result_file}")

    return differences


def list_snapshots():
    """List all available snapshots."""
    ensure_snapshot_dir()
    files = sorted(os.listdir(SNAPSHOT_DIR), reverse=True)

    print("Available Opera 3 snapshots:")
    for f in files:
        if f.endswith('.json') and 'snapshot_' in f:
            filepath = os.path.join(SNAPSHOT_DIR, f)
            size = os.path.getsize(filepath)
            print(f"  {f} ({size:,} bytes)")


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python opera3_snapshot.py before   - Take 'before' snapshot")
        print("  python opera3_snapshot.py after    - Take 'after' snapshot")
        print("  python opera3_snapshot.py compare  - Compare latest snapshots")
        print("  python opera3_snapshot.py list     - List all snapshots")
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == 'before':
        print("Taking 'before' snapshot of Opera 3...")
        take_snapshot('before')
    elif command == 'after':
        print("Taking 'after' snapshot of Opera 3...")
        take_snapshot('after')
    elif command == 'compare':
        compare_snapshots()
    elif command == 'list':
        list_snapshots()
    else:
        print(f"Unknown command: {command}")
