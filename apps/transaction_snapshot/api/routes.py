"""
Transaction Snapshot Tool

Captures complete before/after snapshots of ALL Opera database tables to
identify exactly which tables and fields are updated for each transaction type.

This builds a permanent library of posting patterns categorised by module
(Cashbook, Sales Ledger, Purchase Ledger, Nominal, etc.) that serves as
the definitive reference for Opera transaction posting.

Works with both Opera SE (SQL Server) and Opera 3 (FoxPro DBF).
"""

import os
import json
import hashlib
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Body

logger = logging.getLogger(__name__)

router = APIRouter()

# ============================================================================
# Transaction Library Storage
# ============================================================================

LIBRARY_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
                           'docs', 'opera-transaction-library')

SNAPSHOT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
                            'data', '_transaction_snapshots')

# Module categories for organising transaction types
MODULES = {
    # Transactions
    'cashbook': 'Cashbook Transactions',
    'sales_ledger': 'Sales Ledger Transactions',
    'purchase_ledger': 'Purchase Ledger Transactions',
    'nominal': 'Nominal Ledger Journals',
    'bank_transfer': 'Bank Transfers',
    'gocardless': 'GoCardless',
    'payroll': 'Payroll',
    'stock': 'Stock Transactions',
    'sop': 'Sales Order Processing',
    'pop': 'Purchase Order Processing',
    'fixed_assets': 'Fixed Assets',
    # Master records
    'customer_master': 'Customer Master (sname)',
    'supplier_master': 'Supplier Master (pname)',
    'nominal_master': 'Nominal Account Master (nname/nacnt)',
    'stock_master': 'Stock/Product Master',
    'employee_master': 'Employee Master (Payroll)',
    'bank_master': 'Bank Account Master (nbank)',
    # System
    'system_config': 'System Configuration',
    'vat': 'VAT / Tax',
    'allocations': 'Allocations (Sales/Purchase)',
    'other': 'Other',
}


def _get_sql_connector():
    """Get the active SQL connector from the main app."""
    try:
        from api.main import sql_connector
        return sql_connector
    except Exception:
        return None


def _get_library_path():
    """Get the library directory, creating if needed."""
    os.makedirs(LIBRARY_DIR, exist_ok=True)
    return LIBRARY_DIR


def _get_snapshot_path():
    """Get the snapshot storage directory, creating if needed."""
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    return SNAPSHOT_DIR


# ============================================================================
# Snapshot Engine — Scans ALL tables
# ============================================================================

def take_snapshot_se(sql_connector) -> Dict[str, Any]:
    """
    Take a complete snapshot of ALL tables in both the company and system databases.
    Returns row counts and checksums for every table, plus full row data for
    tables with < 50,000 rows (for detailed diffing).
    """
    snapshot = {
        'timestamp': datetime.now().isoformat(),
        'source': 'opera_se',
        'databases': {},
    }

    # Get current database name
    db_result = sql_connector.execute_query("SELECT DB_NAME() as db_name")
    company_db = db_result.iloc[0]['db_name'] if db_result is not None else 'unknown'
    system_db = 'Opera3SESystem'

    for db_name in [company_db, system_db]:
        db_snapshot = {}
        try:
            # Get ALL user tables
            tables_df = sql_connector.execute_query(f"""
                SELECT TABLE_NAME
                FROM [{db_name}].INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            if tables_df is None or tables_df.empty:
                continue

            for _, row in tables_df.iterrows():
                table_name = row['TABLE_NAME']
                try:
                    # Get row count
                    count_df = sql_connector.execute_query(f"""
                        SELECT COUNT(*) as cnt FROM [{db_name}].dbo.[{table_name}] WITH (NOLOCK)
                    """)
                    row_count = int(count_df.iloc[0]['cnt']) if count_df is not None else 0

                    # Get checksum for change detection
                    try:
                        checksum_df = sql_connector.execute_query(f"""
                            SELECT CHECKSUM_AGG(CHECKSUM(*)) as chk
                            FROM [{db_name}].dbo.[{table_name}] WITH (NOLOCK)
                        """)
                        checksum = int(checksum_df.iloc[0]['chk']) if checksum_df is not None and checksum_df.iloc[0]['chk'] is not None else 0
                    except Exception:
                        checksum = 0

                    # For small/medium tables, capture full data for row-level diff
                    rows_data = None
                    if row_count > 0 and row_count <= 50000:
                        try:
                            data_df = sql_connector.execute_query(f"""
                                SELECT * FROM [{db_name}].dbo.[{table_name}] WITH (NOLOCK)
                            """)
                            if data_df is not None and not data_df.empty:
                                # Convert to list of dicts, handling special types
                                rows_data = []
                                for _, data_row in data_df.iterrows():
                                    row_dict = {}
                                    for col in data_df.columns:
                                        val = data_row[col]
                                        if val is None:
                                            row_dict[col] = None
                                        elif hasattr(val, 'isoformat'):
                                            row_dict[col] = val.isoformat()
                                        elif isinstance(val, (bytes, bytearray)):
                                            row_dict[col] = val.hex()[:100]  # Truncate binary
                                        else:
                                            try:
                                                row_dict[col] = float(val) if isinstance(val, (int, float)) else str(val).strip()
                                            except (ValueError, TypeError):
                                                row_dict[col] = str(val)[:200]
                                    rows_data.append(row_dict)
                        except Exception as e:
                            logger.debug(f"Could not read full data from {db_name}.{table_name}: {e}")

                    db_snapshot[table_name] = {
                        'row_count': row_count,
                        'checksum': checksum,
                        'rows': rows_data,
                    }
                except Exception as e:
                    logger.debug(f"Could not snapshot {db_name}.{table_name}: {e}")
                    db_snapshot[table_name] = {'row_count': -1, 'checksum': 0, 'error': str(e)}

        except Exception as e:
            logger.warning(f"Could not access database {db_name}: {e}")

        snapshot['databases'][db_name] = db_snapshot
        logger.info(f"Snapshot: {db_name} — {len(db_snapshot)} tables captured")

    return snapshot


# ============================================================================
# Diff Engine — Compares two snapshots
# ============================================================================

def diff_snapshots(before: Dict, after: Dict) -> Dict[str, Any]:
    """
    Compare before and after snapshots. Returns detailed diff showing:
    - Tables with row count changes (added/deleted rows)
    - Tables with checksum changes (modified rows)
    - For each changed table: exact field-level changes
    """
    changes = {
        'timestamp': datetime.now().isoformat(),
        'tables_checked': 0,
        'tables_changed': 0,
        'changes': [],
    }

    # Compare each database
    for db_name in set(list(before.get('databases', {}).keys()) + list(after.get('databases', {}).keys())):
        before_db = before.get('databases', {}).get(db_name, {})
        after_db = after.get('databases', {}).get(db_name, {})

        all_tables = set(list(before_db.keys()) + list(after_db.keys()))
        changes['tables_checked'] += len(all_tables)

        for table_name in sorted(all_tables):
            before_table = before_db.get(table_name, {'row_count': 0, 'checksum': 0, 'rows': None})
            after_table = after_db.get(table_name, {'row_count': 0, 'checksum': 0, 'rows': None})

            before_count = before_table.get('row_count', 0)
            after_count = after_table.get('row_count', 0)
            before_check = before_table.get('checksum', 0)
            after_check = after_table.get('checksum', 0)

            # Skip unchanged tables
            if before_count == after_count and before_check == after_check:
                continue

            changes['tables_changed'] += 1

            table_change = {
                'database': db_name,
                'table': table_name,
                'before_rows': before_count,
                'after_rows': after_count,
                'rows_added': max(0, after_count - before_count),
                'rows_deleted': max(0, before_count - after_count),
                'checksum_changed': before_check != after_check,
                'added_rows': [],
                'deleted_rows': [],
                'modified_rows': [],
                'modified_fields': set(),
            }

            # Detailed row-level diff if we have full row data
            before_rows = before_table.get('rows')
            after_rows = after_table.get('rows')

            if before_rows is not None and after_rows is not None:
                # Find primary key column (id, or first column)
                pk_col = 'id'
                if before_rows and pk_col not in before_rows[0]:
                    # Try common PK patterns
                    for candidate in before_rows[0].keys():
                        if candidate.lower() == 'id' or candidate.lower().endswith('_id'):
                            pk_col = candidate
                            break
                    else:
                        pk_col = list(before_rows[0].keys())[0] if before_rows[0] else None

                if pk_col:
                    before_by_pk = {str(r.get(pk_col, '')): r for r in before_rows}
                    after_by_pk = {str(r.get(pk_col, '')): r for r in after_rows}

                    # Added rows
                    for pk, row in after_by_pk.items():
                        if pk not in before_by_pk:
                            table_change['added_rows'].append(row)

                    # Deleted rows
                    for pk, row in before_by_pk.items():
                        if pk not in after_by_pk:
                            table_change['deleted_rows'].append(row)

                    # Modified rows
                    for pk in before_by_pk:
                        if pk in after_by_pk:
                            before_row = before_by_pk[pk]
                            after_row = after_by_pk[pk]
                            field_changes = {}
                            for field in set(list(before_row.keys()) + list(after_row.keys())):
                                bval = before_row.get(field)
                                aval = after_row.get(field)
                                if str(bval) != str(aval):
                                    field_changes[field] = {'before': bval, 'after': aval}
                                    table_change['modified_fields'].add(field)
                            if field_changes:
                                table_change['modified_rows'].append({
                                    'pk': pk,
                                    'pk_column': pk_col,
                                    'changes': field_changes,
                                })

            # Convert set to list for JSON serialisation
            table_change['modified_fields'] = sorted(list(table_change['modified_fields']))
            changes['changes'].append(table_change)

    return changes


# ============================================================================
# API Endpoints
# ============================================================================

# Preset transaction types — matches what our app posts
PRESETS = [
    {'module': 'cashbook', 'name': 'Sales Receipt — BACS', 'description': 'Receipt from customer via BACS. Creates: aentry, atran, stran, ntran, anoml, nacnt, nbank, sname balance update.'},
    {'module': 'cashbook', 'name': 'Sales Receipt — Cheque', 'description': 'Receipt from customer via cheque.'},
    {'module': 'cashbook', 'name': 'Sales Refund', 'description': 'Refund to customer. Creates: aentry, atran, stran, ntran, anoml, nacnt, nbank, sname balance update. Opposite signs to receipt.'},
    {'module': 'cashbook', 'name': 'Purchase Payment — BACS', 'description': 'Payment to supplier via BACS. Creates: aentry, atran, ptran, ntran, anoml, nacnt, nbank, pname balance update.'},
    {'module': 'cashbook', 'name': 'Purchase Payment — Cheque', 'description': 'Payment to supplier via cheque.'},
    {'module': 'cashbook', 'name': 'Purchase Refund', 'description': 'Refund from supplier. Creates: aentry, atran, ptran, ntran, anoml, nacnt, nbank, pname balance update. Opposite signs to payment.'},
    {'module': 'cashbook', 'name': 'Nominal Payment', 'description': 'Payment to nominal account (no ledger). Creates: aentry, atran, ntran, anoml, nacnt, nbank.'},
    {'module': 'cashbook', 'name': 'Nominal Receipt', 'description': 'Receipt from nominal account (no ledger). Creates: aentry, atran, ntran, anoml, nacnt, nbank.'},
    {'module': 'bank_transfer', 'name': 'Bank Transfer', 'description': 'Internal transfer between two bank accounts. Creates 2x aentry, 2x atran, 2x ntran, 2x anoml, 2x nacnt, 2x nbank.'},
    {'module': 'sales_ledger', 'name': 'Sales Invoice', 'description': 'Sales invoice posting. Creates: stran, snoml, ntran, nacnt, sname balance.'},
    {'module': 'sales_ledger', 'name': 'Sales Credit Note', 'description': 'Sales credit note posting.'},
    {'module': 'sales_ledger', 'name': 'Sales Allocation', 'description': 'Allocate receipt against invoice. Creates: salloc records.'},
    {'module': 'purchase_ledger', 'name': 'Purchase Invoice', 'description': 'Purchase invoice posting. Creates: ptran, pnoml, ntran, nacnt, pname balance.'},
    {'module': 'purchase_ledger', 'name': 'Purchase Credit Note', 'description': 'Purchase credit note posting.'},
    {'module': 'purchase_ledger', 'name': 'Purchase Allocation', 'description': 'Allocate payment against invoice. Creates: palloc records.'},
    {'module': 'nominal', 'name': 'Nominal Journal', 'description': 'Manual nominal journal entry. Creates: ntran (debit + credit), nacnt updates.'},
    {'module': 'gocardless', 'name': 'GoCardless Batch Import', 'description': 'Batch of customer receipts from GoCardless payout. Includes fees split and VAT tracking.'},
    {'module': 'vat', 'name': 'VAT Transaction', 'description': 'Transaction with VAT. Creates: zvtran, nvat records in addition to normal posting.'},
    {'module': 'allocations', 'name': 'Sales Receipt with Auto-Allocate', 'description': 'Sales receipt that auto-allocates to a matching invoice.'},
    {'module': 'allocations', 'name': 'Purchase Payment with Auto-Allocate', 'description': 'Purchase payment that auto-allocates to a matching invoice.'},
    {'module': 'customer_master', 'name': 'New Customer', 'description': 'Create a new customer account in sname.'},
    {'module': 'customer_master', 'name': 'Edit Customer', 'description': 'Modify an existing customer account.'},
    {'module': 'supplier_master', 'name': 'New Supplier', 'description': 'Create a new supplier account in pname.'},
    {'module': 'supplier_master', 'name': 'Edit Supplier', 'description': 'Modify an existing supplier account.'},
    {'module': 'nominal_master', 'name': 'New Nominal Account', 'description': 'Create a new nominal account in nacnt/nname.'},
    {'module': 'bank_master', 'name': 'New Bank Account', 'description': 'Create a new bank account in nbank.'},
    {'module': 'stock_master', 'name': 'New Stock Item', 'description': 'Create a new stock/product record.'},
    {'module': 'employee_master', 'name': 'New Employee', 'description': 'Create a new employee record in payroll.'},
    {'module': 'payroll', 'name': 'Payroll Run', 'description': 'Complete payroll run including NI, tax, pension, nominal postings.'},
]


@router.get("/api/transaction-snapshot/modules")
async def get_modules():
    """Get available module categories for transaction types."""
    return {"success": True, "modules": MODULES}


@router.get("/api/transaction-snapshot/presets")
async def get_presets():
    """Get preset transaction types that match what our app posts."""
    return {"success": True, "presets": PRESETS}


@router.get("/api/transaction-snapshot/library")
async def get_library():
    """Get the transaction type library — all recorded posting patterns."""
    lib_path = _get_library_path()
    library = []

    for filename in sorted(os.listdir(lib_path)):
        if filename.endswith('.json'):
            try:
                with open(os.path.join(lib_path, filename)) as f:
                    entry = json.load(f)
                    library.append({
                        'id': filename.replace('.json', ''),
                        'module': entry.get('module', 'other'),
                        'module_name': MODULES.get(entry.get('module', 'other'), 'Other'),
                        'name': entry.get('name', ''),
                        'description': entry.get('description', ''),
                        'recorded_at': entry.get('recorded_at', ''),
                        'tables_changed': entry.get('tables_changed', 0),
                        'source': entry.get('source', 'opera_se'),
                    })
            except Exception as e:
                logger.warning(f"Could not load library entry {filename}: {e}")

    return {"success": True, "library": library}


@router.get("/api/transaction-snapshot/library/{entry_id}")
async def get_library_entry(entry_id: str):
    """Get a specific transaction type entry with full diff details."""
    lib_path = _get_library_path()
    filepath = os.path.join(lib_path, f"{entry_id}.json")

    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Library entry not found")

    with open(filepath) as f:
        entry = json.load(f)

    return {"success": True, "entry": entry}


@router.post("/api/transaction-snapshot/before")
async def take_before_snapshot(
    module: str = Query(..., description="Module category (cashbook, sales_ledger, etc.)"),
    name: str = Query(..., description="Transaction type name (e.g., 'Sales Receipt — BACS')"),
    description: str = Query("", description="Detailed description of the transaction being entered"),
):
    """
    Take a BEFORE snapshot of all Opera tables.
    Call this, then enter the transaction in Opera, then call /after.
    """
    sql = _get_sql_connector()
    if not sql:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        logger.info(f"Taking BEFORE snapshot for: {module}/{name}")
        snapshot = take_snapshot_se(sql)

        # Save snapshot to temp file
        snap_path = _get_snapshot_path()
        snap_file = os.path.join(snap_path, 'current_before.json')
        meta_file = os.path.join(snap_path, 'current_meta.json')

        with open(snap_file, 'w') as f:
            json.dump(snapshot, f)

        meta = {
            'module': module,
            'name': name,
            'description': description,
            'before_timestamp': snapshot['timestamp'],
            'source': snapshot.get('source', 'opera_se'),
        }
        with open(meta_file, 'w') as f:
            json.dump(meta, f)

        total_tables = sum(len(db) for db in snapshot.get('databases', {}).values())

        return {
            "success": True,
            "message": f"Before snapshot captured — {total_tables} tables across {len(snapshot.get('databases', {}))} database(s)",
            "tables_scanned": total_tables,
            "databases": list(snapshot.get('databases', {}).keys()),
            "timestamp": snapshot['timestamp'],
        }
    except Exception as e:
        logger.error(f"Snapshot failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/transaction-snapshot/after")
async def take_after_snapshot():
    """
    Take an AFTER snapshot and generate the diff.
    Must be called after /before and after the transaction is entered in Opera.
    Saves the result to the transaction library.
    """
    sql = _get_sql_connector()
    if not sql:
        raise HTTPException(status_code=503, detail="No database connection")

    snap_path = _get_snapshot_path()
    snap_file = os.path.join(snap_path, 'current_before.json')
    meta_file = os.path.join(snap_path, 'current_meta.json')

    if not os.path.exists(snap_file) or not os.path.exists(meta_file):
        raise HTTPException(status_code=400, detail="No before snapshot found. Take a before snapshot first.")

    try:
        # Load before snapshot and metadata
        with open(snap_file) as f:
            before = json.load(f)
        with open(meta_file) as f:
            meta = json.load(f)

        # Take after snapshot
        logger.info(f"Taking AFTER snapshot for: {meta['module']}/{meta['name']}")
        after = take_snapshot_se(sql)

        # Generate diff
        diff = diff_snapshots(before, after)

        # Build library entry
        entry = {
            'module': meta['module'],
            'module_name': MODULES.get(meta['module'], 'Other'),
            'name': meta['name'],
            'description': meta['description'],
            'source': meta.get('source', 'opera_se'),
            'recorded_at': datetime.now().isoformat(),
            'before_timestamp': meta['before_timestamp'],
            'after_timestamp': after['timestamp'],
            'tables_checked': diff['tables_checked'],
            'tables_changed': diff['tables_changed'],
            'changes': diff['changes'],
        }

        # Save to library
        lib_path = _get_library_path()
        safe_name = meta['name'].lower().replace(' ', '_').replace('—', '-')
        safe_name = ''.join(c for c in safe_name if c.isalnum() or c in '-_')[:50]
        entry_id = f"{meta['module']}_{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        entry_file = os.path.join(lib_path, f"{entry_id}.json")

        with open(entry_file, 'w') as f:
            json.dump(entry, f, indent=2, default=str)

        # Clean up temp files
        os.remove(snap_file)
        os.remove(meta_file)

        logger.info(f"Transaction library entry saved: {entry_id} — {diff['tables_changed']} tables changed")

        # Generate summary for response
        summary = []
        for change in diff['changes']:
            summary.append({
                'database': change['database'],
                'table': change['table'],
                'rows_added': change['rows_added'],
                'rows_deleted': change['rows_deleted'],
                'rows_modified': len(change.get('modified_rows', [])),
                'fields_modified': change.get('modified_fields', []),
            })

        return {
            "success": True,
            "entry_id": entry_id,
            "message": f"Diff captured — {diff['tables_changed']} table(s) changed across {diff['tables_checked']} scanned",
            "tables_changed": diff['tables_changed'],
            "summary": summary,
        }
    except Exception as e:
        logger.error(f"After snapshot failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/transaction-snapshot/library/{entry_id}")
async def delete_library_entry(entry_id: str):
    """Delete a transaction library entry."""
    lib_path = _get_library_path()
    filepath = os.path.join(lib_path, f"{entry_id}.json")

    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Library entry not found")

    os.remove(filepath)
    return {"success": True, "message": f"Deleted {entry_id}"}


@router.post("/api/transaction-snapshot/cancel")
async def cancel_snapshot():
    """Cancel a pending snapshot (clean up before snapshot without taking after)."""
    snap_path = _get_snapshot_path()
    for f in ['current_before.json', 'current_meta.json']:
        path = os.path.join(snap_path, f)
        if os.path.exists(path):
            os.remove(path)

    return {"success": True, "message": "Snapshot cancelled"}


@router.post("/api/transaction-snapshot/export-to-knowledge")
async def export_to_knowledge(entry_id: str = Query(...)):
    """
    Export a transaction library entry to the Opera knowledge base
    in a format suitable for the knowledge base markdown.
    """
    lib_path = _get_library_path()
    filepath = os.path.join(lib_path, f"{entry_id}.json")

    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Library entry not found")

    with open(filepath) as f:
        entry = json.load(f)

    # Generate markdown
    md_lines = [
        f"### {entry['name']}",
        f"",
        f"**Module:** {entry.get('module_name', entry.get('module', '?'))}",
        f"**Source:** {entry.get('source', 'opera_se')}",
        f"**Recorded:** {entry.get('recorded_at', '?')}",
        f"",
    ]

    if entry.get('description'):
        md_lines.append(f"{entry['description']}")
        md_lines.append("")

    md_lines.append("**Tables Updated:**")
    md_lines.append("")
    md_lines.append("| Database | Table | Rows Added | Rows Modified | Fields Changed |")
    md_lines.append("|----------|-------|-----------|--------------|----------------|")

    for change in entry.get('changes', []):
        fields = ', '.join(change.get('modified_fields', [])[:10])
        if len(change.get('modified_fields', [])) > 10:
            fields += f" (+{len(change['modified_fields']) - 10} more)"
        md_lines.append(
            f"| {change['database']} | {change['table']} | "
            f"{change.get('rows_added', 0)} | {len(change.get('modified_rows', []))} | "
            f"{fields} |"
        )

    md_lines.append("")

    # Detail for added rows
    for change in entry.get('changes', []):
        if change.get('added_rows'):
            md_lines.append(f"**{change['table']} — New rows:**")
            md_lines.append("```json")
            for row in change['added_rows'][:3]:  # Limit to 3 for readability
                md_lines.append(json.dumps(row, indent=2, default=str))
            if len(change['added_rows']) > 3:
                md_lines.append(f"... and {len(change['added_rows']) - 3} more")
            md_lines.append("```")
            md_lines.append("")

        if change.get('modified_rows'):
            md_lines.append(f"**{change['table']} — Modified fields:**")
            for mod in change['modified_rows'][:3]:
                for field, vals in mod.get('changes', {}).items():
                    md_lines.append(f"- `{field}`: `{vals.get('before')}` → `{vals.get('after')}`")
            md_lines.append("")

    markdown = '\n'.join(md_lines)

    return {
        "success": True,
        "markdown": markdown,
        "entry_id": entry_id,
    }
