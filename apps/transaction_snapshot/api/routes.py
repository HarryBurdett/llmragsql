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

# Transaction library — stored in central knowledge repo (shared across all installations)
# Falls back to local docs/ if central repo not available
_CENTRAL_LIBRARY = os.path.expanduser('~/opera-knowledge-ref/packages/opera-knowledge/transaction-library')
_LOCAL_LIBRARY = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
                              'docs', 'opera-transaction-library')
LIBRARY_DIR = _CENTRAL_LIBRARY if os.path.exists(os.path.dirname(_CENTRAL_LIBRARY)) else _LOCAL_LIBRARY

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


def take_snapshot_opera3(data_path: str) -> Dict[str, Any]:
    """
    Take a complete snapshot of ALL Opera 3 FoxPro DBF tables.
    Scans both the company data folder and the System folder.
    Returns row counts and checksums for every table, plus full row data
    for tables with < 50,000 rows (for detailed diffing).
    """
    from pathlib import Path

    snapshot = {
        'timestamp': datetime.now().isoformat(),
        'source': 'opera3',
        'databases': {},
    }

    try:
        from dbfread import DBF
    except ImportError:
        logger.warning("dbfread not installed — cannot snapshot Opera 3")
        return snapshot

    # Scan company data folder and System folder
    base = Path(data_path)
    folders_to_scan = {'company': base}

    # Find System folder — may be at parent level
    system_path = base.parent / 'System'
    if not system_path.exists():
        system_path = base / 'System'
    if system_path.exists():
        folders_to_scan['system'] = system_path

    for folder_label, folder_path in folders_to_scan.items():
        db_snapshot = {}

        if not folder_path.exists():
            continue

        # Find all DBF files
        dbf_files = list(folder_path.glob('*.dbf')) + list(folder_path.glob('*.DBF'))
        # Deduplicate (case-insensitive)
        seen = set()
        unique_dbfs = []
        for f in dbf_files:
            key = f.stem.lower()
            if key not in seen:
                seen.add(key)
                unique_dbfs.append(f)

        for dbf_path in sorted(unique_dbfs, key=lambda f: f.stem.lower()):
            table_name = dbf_path.stem.lower()
            try:
                # Open with shared read access (use smbclient if available)
                dbf = DBF(str(dbf_path), encoding='cp1252', load=False)

                # Count rows
                row_count = 0
                rows_data = []
                checksum_val = 0

                for record in dbf:
                    row_count += 1
                    record_dict = {}
                    for key, value in dict(record).items():
                        if value is None:
                            record_dict[key] = None
                        elif hasattr(value, 'isoformat'):
                            record_dict[key] = value.isoformat()
                        elif isinstance(value, (bytes, bytearray)):
                            record_dict[key] = value.hex()[:100]
                        elif isinstance(value, bool):
                            record_dict[key] = value
                        elif isinstance(value, (int, float)):
                            record_dict[key] = float(value)
                        else:
                            record_dict[key] = str(value).strip()

                    # Simple checksum from string representation
                    checksum_val = (checksum_val + hash(str(record_dict))) & 0xFFFFFFFF

                    if row_count <= 50000:
                        rows_data.append(record_dict)

                db_snapshot[table_name] = {
                    'row_count': row_count,
                    'checksum': checksum_val,
                    'rows': rows_data if row_count <= 50000 else None,
                }
            except Exception as e:
                logger.debug(f"Could not snapshot {folder_label}/{table_name}: {e}")
                db_snapshot[table_name] = {'row_count': -1, 'checksum': 0, 'error': str(e)}

        snapshot['databases'][folder_label] = db_snapshot
        logger.info(f"Snapshot Opera 3: {folder_label} ({folder_path}) — {len(db_snapshot)} tables captured")

    return snapshot


# ============================================================================
# Diff Engine — Compares two snapshots
# ============================================================================

def get_table_field_metadata(sql_connector, db_name: str, table_name: str) -> Dict[str, Dict]:
    """
    Get field metadata for a table — nullable, data type, default value.
    Returns dict of field_name -> {nullable, data_type, default, max_length}
    """
    try:
        df = sql_connector.execute_query(f"""
            SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                   COLUMN_DEFAULT, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM [{db_name}].INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        if df is None or df.empty:
            return {}
        result = {}
        for _, row in df.iterrows():
            col = row['COLUMN_NAME']
            result[col] = {
                'mandatory': row['IS_NULLABLE'] == 'NO',
                'data_type': row['DATA_TYPE'],
                'max_length': int(row['CHARACTER_MAXIMUM_LENGTH']) if row['CHARACTER_MAXIMUM_LENGTH'] is not None else None,
                'default': str(row['COLUMN_DEFAULT']).strip() if row['COLUMN_DEFAULT'] else None,
                'precision': int(row['NUMERIC_PRECISION']) if row['NUMERIC_PRECISION'] is not None else None,
                'scale': int(row['NUMERIC_SCALE']) if row['NUMERIC_SCALE'] is not None else None,
            }
        return result
    except Exception:
        return {}


def diff_snapshots(before: Dict, after: Dict, sql_connector=None) -> Dict[str, Any]:
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
                'field_metadata': {},
            }

            # Get field metadata (mandatory/type/default) for changed tables
            if sql_connector:
                table_change['field_metadata'] = get_table_field_metadata(sql_connector, db_name, table_name)

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


# ============================================================================
# Auto-Classification — Analyses diff to precisely define the transaction
# ============================================================================

def classify_transaction(diff: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyse a before/after diff to precisely classify the transaction type,
    tables updated, amount conventions, sequence sources, and posting pattern.
    Returns a structured classification for the library entry.
    """
    changes = diff.get('changes', [])
    tables_changed = {c['table'].lower(): c for c in changes}

    classification = {
        'auto_detected_type': 'Unknown',
        'transaction_category': 'unknown',
        'tables_updated': [],
        'tables_with_new_rows': [],
        'tables_with_modified_rows': [],
        'amount_conventions': {},
        'sequence_sources': {},
        'balance_updates': [],
        'transfer_files': [],
        'vat_tracking': False,
        'allocation_created': False,
        'posting_characteristics': [],
        'precise_definition': '',
    }

    # Identify tables with new rows vs modified rows
    for change in changes:
        table = change['table'].lower()
        classification['tables_updated'].append(table)
        if change.get('rows_added', 0) > 0:
            classification['tables_with_new_rows'].append(table)
        if len(change.get('modified_rows', [])) > 0:
            classification['tables_with_modified_rows'].append(table)

    has_new = set(classification['tables_with_new_rows'])
    has_mod = set(classification['tables_with_modified_rows'])

    # ---- Determine transaction type from table patterns ----

    # Cashbook transaction (aentry + atran)
    if 'aentry' in has_new and 'atran' in has_new:
        classification['transaction_category'] = 'cashbook'

        # Determine specific type from atran at_type
        atran_change = tables_changed.get('atran', {})
        at_type = None
        for row in atran_change.get('added_rows', []):
            at_type = row.get('at_type')
            if at_type is not None:
                break

        type_map = {
            1: ('Nominal Payment', 'nominal_payment'),
            2: ('Nominal Receipt', 'nominal_receipt'),
            3: ('Sales Refund', 'sales_refund'),
            4: ('Sales Receipt', 'sales_receipt'),
            5: ('Purchase Payment', 'purchase_payment'),
            6: ('Purchase Refund', 'purchase_refund'),
            8: ('Bank Transfer', 'bank_transfer'),
        }
        if at_type is not None:
            type_name, type_code = type_map.get(int(at_type), (f'Unknown (at_type={at_type})', 'unknown'))
            classification['auto_detected_type'] = type_name
        else:
            classification['auto_detected_type'] = 'Cashbook Transaction (type unknown)'

        # Check for batch posting
        aentry_change = tables_changed.get('aentry', {})
        for row in aentry_change.get('added_rows', []):
            complet = row.get('ae_complet')
            if complet is not None:
                if not complet or complet == 0:
                    classification['posting_characteristics'].append('Batch posting (ae_complet=0, awaiting completion)')
                else:
                    classification['posting_characteristics'].append('Immediate posting (ae_complet=1)')

        # Check amounts — pence in aentry/atran
        for row in aentry_change.get('added_rows', []):
            val = row.get('ae_value')
            if val is not None:
                classification['amount_conventions']['aentry.ae_value'] = f'{val} (pence, {"negative=payment" if float(val) < 0 else "positive=receipt"})'

        for row in atran_change.get('added_rows', []):
            val = row.get('at_value')
            if val is not None:
                classification['amount_conventions']['atran.at_value'] = f'{val} (pence, {"negative=payment" if float(val) < 0 else "positive=receipt"})'

    # Sales ledger (stran)
    if 'stran' in has_new:
        if classification['transaction_category'] == 'unknown':
            classification['transaction_category'] = 'sales_ledger'
        stran_change = tables_changed.get('stran', {})
        for row in stran_change.get('added_rows', []):
            trtype = row.get('st_trtype')
            trval = row.get('st_trvalue')
            if trtype:
                type_labels = {'R': 'Receipt', 'I': 'Invoice', 'C': 'Credit Note', 'F': 'Refund'}
                classification['posting_characteristics'].append(f'Sales ledger: st_trtype={trtype} ({type_labels.get(trtype, "?")})')
            if trval is not None:
                classification['amount_conventions']['stran.st_trvalue'] = f'{trval} (pounds)'

    # Purchase ledger (ptran)
    if 'ptran' in has_new:
        if classification['transaction_category'] == 'unknown':
            classification['transaction_category'] = 'purchase_ledger'
        ptran_change = tables_changed.get('ptran', {})
        for row in ptran_change.get('added_rows', []):
            trtype = row.get('pt_trtype')
            trval = row.get('pt_trvalue')
            if trtype:
                type_labels = {'P': 'Payment', 'I': 'Invoice', 'C': 'Credit Note'}
                classification['posting_characteristics'].append(f'Purchase ledger: pt_trtype={trtype} ({type_labels.get(trtype, "?")})')
            if trval is not None:
                classification['amount_conventions']['ptran.pt_trvalue'] = f'{trval} (pounds, negative=payment)')

    # Nominal ledger (ntran)
    if 'ntran' in has_new:
        ntran_change = tables_changed.get('ntran', {})
        ntran_rows = ntran_change.get('added_rows', [])
        classification['posting_characteristics'].append(f'Nominal entries: {len(ntran_rows)} ntran rows (double-entry)')
        for row in ntran_rows[:2]:
            acnt = row.get('nt_acnt', '?')
            val = row.get('nt_value')
            if val is not None:
                side = 'DEBIT' if float(val) > 0 else 'CREDIT'
                classification['posting_characteristics'].append(f'  ntran: {acnt} = {val} ({side}, pounds)')
            jrnl = row.get('nt_jrnl')
            if jrnl:
                classification['sequence_sources']['nt_jrnl'] = f'{jrnl} (from nparm.np_nexjrnl)'

    # Transfer files
    if 'anoml' in has_new:
        classification['transfer_files'].append('anoml (Cashbook → NL transfer)')
    if 'snoml' in has_new:
        classification['transfer_files'].append('snoml (Sales → NL transfer)')
    if 'pnoml' in has_new:
        classification['transfer_files'].append('pnoml (Purchase → NL transfer)')

    # VAT tracking
    if 'zvtran' in has_new or 'nvat' in has_new:
        classification['vat_tracking'] = True
        classification['posting_characteristics'].append('VAT tracking: zvtran and/or nvat records created')

    # Allocation
    if 'salloc' in has_new:
        classification['allocation_created'] = True
        classification['posting_characteristics'].append('Sales allocation created (salloc)')
    if 'palloc' in has_new:
        classification['allocation_created'] = True
        classification['posting_characteristics'].append('Purchase allocation created (palloc)')

    # Balance updates
    if 'nacnt' in has_mod:
        nacnt_change = tables_changed.get('nacnt', {})
        for mod in nacnt_change.get('modified_rows', [])[:3]:
            fields = list(mod.get('changes', {}).keys())
            classification['balance_updates'].append(f'nacnt: {", ".join(fields)}')

    if 'nbank' in has_mod:
        nbank_change = tables_changed.get('nbank', {})
        for mod in nbank_change.get('modified_rows', []):
            fields = list(mod.get('changes', {}).keys())
            classification['balance_updates'].append(f'nbank: {", ".join(fields)}')

    if 'sname' in has_mod:
        classification['balance_updates'].append('sname: customer balance updated (sn_currbal)')
    if 'pname' in has_mod:
        classification['balance_updates'].append('pname: supplier balance updated (pn_currbal)')

    if 'nhist' in has_mod or 'nhist' in has_new:
        classification['balance_updates'].append('nhist: nominal history updated')

    # Sequence sources
    if 'atype' in has_mod:
        classification['sequence_sources']['ae_entry'] = 'atype.ay_entry (entry number counter)'
    if 'nparm' in has_mod:
        classification['sequence_sources']['nt_jrnl'] = 'nparm.np_nexjrnl (journal number counter)'

    # Master record changes (no transaction)
    if classification['transaction_category'] == 'unknown':
        if 'sname' in has_new:
            classification['auto_detected_type'] = 'New Customer'
            classification['transaction_category'] = 'customer_master'
        elif 'pname' in has_new:
            classification['auto_detected_type'] = 'New Supplier'
            classification['transaction_category'] = 'supplier_master'
        elif 'nacnt' in has_new:
            classification['auto_detected_type'] = 'New Nominal Account'
            classification['transaction_category'] = 'nominal_master'
        elif 'nbank' in has_new:
            classification['auto_detected_type'] = 'New Bank Account'
            classification['transaction_category'] = 'bank_master'
        elif 'sname' in has_mod and 'aentry' not in has_new:
            classification['auto_detected_type'] = 'Customer Edit'
            classification['transaction_category'] = 'customer_master'
        elif 'pname' in has_mod and 'aentry' not in has_new:
            classification['auto_detected_type'] = 'Supplier Edit'
            classification['transaction_category'] = 'supplier_master'

    # Bank transfer detection
    if 'aentry' in has_new:
        aentry_rows = tables_changed.get('aentry', {}).get('added_rows', [])
        if len(aentry_rows) == 2:
            classification['posting_characteristics'].append('Bank transfer: 2 aentry records (one per bank)')

    # Foreign currency detection
    for table in ['atran', 'stran', 'ptran']:
        if table in has_new:
            for row in tables_changed.get(table, {}).get('added_rows', []):
                fcurr = row.get('at_fcurr') or row.get('st_fcurr') or row.get('pt_fcurr')
                if fcurr and str(fcurr).strip() and str(fcurr).strip() != 'Sterling':
                    classification['posting_characteristics'].append(f'Foreign currency: {fcurr}')
                    break

    # Build precise definition
    parts = [classification['auto_detected_type']]
    if classification['vat_tracking']:
        parts.append('with VAT')
    if classification['allocation_created']:
        parts.append('with auto-allocation')
    for char in classification['posting_characteristics']:
        if 'Foreign currency' in char:
            parts.append(char.split(': ')[1] if ': ' in char else char)
        if 'Batch posting' in char:
            parts.append('(batch)')
    classification['precise_definition'] = ' — '.join(parts[:4])

    return classification


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
    Automatically detects Opera SE (SQL) vs Opera 3 (FoxPro/SMB).
    """
    # Detect Opera version and take appropriate snapshot
    opera_version = 'opera_se'
    try:
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'config.ini'))
        opera_version = cfg.get('opera', 'version', fallback='sql_se')
    except Exception:
        pass

    try:
        if opera_version == 'opera3':
            # Opera 3 — snapshot DBF files via SMB
            try:
                from sql_rag.smb_access import get_smb_manager
                smb = get_smb_manager()
                if smb and smb.is_connected():
                    data_path = str(smb.get_local_base())
                    # Use opera3_base_path from config if set
                    try:
                        base_path = cfg.get('opera', 'opera3_base_path', fallback='')
                        if base_path:
                            data_path = base_path
                    except Exception:
                        pass
                    logger.info(f"Taking BEFORE snapshot (Opera 3) for: {module}/{name} at {data_path}")
                    snapshot = take_snapshot_opera3(data_path)
                else:
                    raise HTTPException(status_code=503, detail="Opera 3 SMB connection not available")
            except ImportError:
                raise HTTPException(status_code=503, detail="SMB access module not available")
        else:
            # Opera SE — snapshot via SQL
            sql = _get_sql_connector()
            if not sql:
                raise HTTPException(status_code=503, detail="No database connection")
            logger.info(f"Taking BEFORE snapshot (Opera SE) for: {module}/{name}")
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

        # Take after snapshot using same source as before
        source = meta.get('source', 'opera_se')
        logger.info(f"Taking AFTER snapshot ({source}) for: {meta['module']}/{meta['name']}")

        if source == 'opera3':
            # Opera 3 — snapshot DBF files
            try:
                import configparser
                cfg = configparser.ConfigParser()
                cfg.read(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'config.ini'))
                from sql_rag.smb_access import get_smb_manager
                smb = get_smb_manager()
                if smb and smb.is_connected():
                    data_path = cfg.get('opera', 'opera3_base_path', fallback=str(smb.get_local_base()))
                    after = take_snapshot_opera3(data_path)
                else:
                    raise HTTPException(status_code=503, detail="Opera 3 SMB connection not available")
            except ImportError:
                raise HTTPException(status_code=503, detail="SMB access module not available")
        else:
            # Opera SE
            sql = _get_sql_connector()
            if not sql:
                raise HTTPException(status_code=503, detail="No database connection")
            after = take_snapshot_se(sql)

        # Generate diff
        diff = diff_snapshots(before, after, sql_connector=_get_sql_connector() if source != 'opera3' else None)

        # Auto-classify the transaction from the diff data
        classification = classify_transaction(diff)

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
            'classification': classification,
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

        # Auto-commit to central knowledge repo if available
        if LIBRARY_DIR == _CENTRAL_LIBRARY:
            try:
                import subprocess
                repo_dir = os.path.expanduser('~/opera-knowledge-ref')
                subprocess.run(['git', 'add', entry_file], cwd=repo_dir, capture_output=True)
                subprocess.run(
                    ['git', 'commit', '-m', f'Transaction library: {meta["name"]} ({entry_id})'],
                    cwd=repo_dir, capture_output=True
                )
                logger.info(f"Committed to central knowledge repo: {entry_id}")
            except Exception as git_err:
                logger.debug(f"Could not commit to central repo: {git_err}")

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
            "classification": classification,
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
