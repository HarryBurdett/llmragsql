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

from fastapi import APIRouter, HTTPException, Query, Body, Request

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
    'recurring': 'Recurring Entries',
    'gocardless': 'GoCardless',
    'payroll': 'Payroll (separate project — when a payroll-updating app is built)',
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
    'reconciliation': 'Bank Reconciliation',
    'fc': 'Foreign Currency (separate FX project — later release)',
    'other': 'Other',
}


def _get_sql_connector():
    """Get the active SQL connector for the current company/system."""
    try:
        from apps.core.adapters.factory import get_opera_sql
        sql_connector = get_opera_sql()
        from api.main import _get_active_company_id, _company_sql_connectors, active_system_id
        company_id = _get_active_company_id()
        # Try system-scoped connector first (per-session isolation)
        if active_system_id and company_id:
            key = f"{active_system_id}_{company_id}"
            if key in _company_sql_connectors:
                return _company_sql_connectors[key]
        # Fall back to company connector
        if company_id and company_id in _company_sql_connectors:
            return _company_sql_connectors[company_id]
        return sql_connector
    except Exception:
        try:
            from apps.core.adapters.factory import get_opera_sql
            sql_connector = get_opera_sql()
            return sql_connector
        except Exception:
            return None


def _get_library_path():
    """Get the library directory, creating if needed."""
    os.makedirs(LIBRARY_DIR, exist_ok=True)
    return LIBRARY_DIR


# Engine-specific subfolders added 2026-05-12 so SE and Opera 3
# traces stay separated and can be diffed directly.
_LIBRARY_SUBDIRS = ('opera_se', 'opera_3')


def _iter_library_files():
    """Yield (full_path, filename, subdir_engine) for every entry in the
    library, scanning both the engine-specific subfolders (opera_se/,
    opera_3/) AND the flat root for entries written before the
    2026-05-12 reorg. `subdir_engine` is the authoritative engine
    derived from the containing subfolder ('opera_se' | 'opera_3'), or
    None for flat-root/legacy entries. Filenames are unique across
    folders by timestamp so collisions don't happen in practice.
    """
    lib_path = _get_library_path()
    # Flat root — pre-reorg entries (engine unknown from location)
    if os.path.isdir(lib_path):
        for filename in os.listdir(lib_path):
            full = os.path.join(lib_path, filename)
            if os.path.isfile(full) and filename.endswith('.json'):
                yield full, filename, None
    # Engine-specific subfolders — post-reorg entries
    for sub in _LIBRARY_SUBDIRS:
        sub_path = os.path.join(lib_path, sub)
        if os.path.isdir(sub_path):
            for filename in os.listdir(sub_path):
                full = os.path.join(sub_path, filename)
                if os.path.isfile(full) and filename.endswith('.json'):
                    yield full, filename, sub


def _find_library_entry(entry_id: str):
    """Return the full path of an entry by id, checking both the flat
    root and the engine subfolders. Returns None if not found."""
    target = f"{entry_id}.json"
    for full_path, filename, _engine in _iter_library_files():
        if filename == target:
            return full_path
    return None


def _get_snapshot_path():
    """Get the snapshot storage directory, creating if needed."""
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    return SNAPSHOT_DIR


# ============================================================================
# Snapshot Engine — Scans ALL tables
# ============================================================================

def take_snapshot_se(
    sql_connector,
    company_db: str,
    max_rows_for_full_data: int = 500000,
) -> Dict[str, Any]:
    """
    Take a snapshot of ALL tables in the specified company database and the
    Opera SE system database.

    The caller is REQUIRED to supply `company_db` — the actual SQL Server
    database name to scan (e.g. 'Opera3SECompany00Z'). This is resolved
    from the active company's JSON config (companies/<id>.json's
    `database` field) at the endpoint level, BEFORE this function is
    called. We deliberately do NOT ask the connection what database it
    is bound to via `SELECT DB_NAME()` — that has been the source of
    several silent "scanned the wrong company" bugs because pooled
    connections can drift from the company the user actually selected.

    Strategy: Get row counts for all tables via sys.partitions (instant).
    Get checksums for all tables (fast). Read full row data for tables
    up to 500k rows — this is a manual tool, not continuous polling,
    so reading large tables is acceptable for accuracy. Only truly
    massive tables (audit logs, history) get checksum-only.
    """
    snapshot = {
        'timestamp': datetime.now().isoformat(),
        'source': 'opera_se',
        'company_db': company_db,
        'databases': {},
    }

    system_db = 'Opera3SESystem'
    logger.info(
        f"take_snapshot_se: scanning company_db='{company_db}', "
        f"system_db='{system_db}' (explicit — no DB_NAME() indirection)"
    )

    for db_name in [company_db, system_db]:
        db_snapshot = {}
        # Why we use explicit 3-part names and avoid OBJECT_ID():
        #
        # 1. The previous code used `OBJECT_ID('{db}.dbo.' + TABLE_NAME)`
        #    in the JOIN. OBJECT_ID() resolves in the caller's DB
        #    context, so it silently returned NULL when the connection
        #    was bound elsewhere → row_count came back NULL → snapshot
        #    treated tables as 0 rows → row data never captured → the
        #    diff reported "every table changed" (false positives).
        #
        # 2. An earlier "fix" attempted `USE [{db}]; SELECT ...` as a
        #    single multi-statement batch. pd.read_sql sits on the
        #    USE's empty result and doesn't advance to the SELECT's
        #    rows → tables_df is empty → snapshot captures 0 tables.
        #
        # Correct approach: explicit `[{db}].sys.tables`,
        # `[{db}].INFORMATION_SCHEMA.TABLES`, `[{db}].sys.partitions`,
        # and `[{db}].dbo.[{table}]` 3-part names. The login used for
        # the snapshot connector has SELECT on every Opera DB on this
        # server, so cross-DB SELECTs work even if the connection's
        # current_database differs from the target.
        try:
            tables_df = sql_connector.execute_query(f"""
                SELECT t.TABLE_NAME,
                       ISNULL(p_agg.row_count, 0) AS row_count
                FROM [{db_name}].INFORMATION_SCHEMA.TABLES t WITH (NOLOCK)
                LEFT JOIN [{db_name}].sys.tables st WITH (NOLOCK)
                    ON st.name COLLATE DATABASE_DEFAULT
                       = t.TABLE_NAME COLLATE DATABASE_DEFAULT
                LEFT JOIN (
                    SELECT object_id, SUM(rows) AS row_count
                    FROM [{db_name}].sys.partitions WITH (NOLOCK)
                    WHERE index_id IN (0, 1)
                    GROUP BY object_id
                ) AS p_agg ON p_agg.object_id = st.object_id
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                ORDER BY t.TABLE_NAME
            """)
            if tables_df is None or tables_df.empty:
                logger.warning(
                    f"Snapshot: 0 tables returned for [{db_name}]. "
                    "Connector login may lack SELECT permission on this DB."
                )
                continue
            logger.info(f"Snapshot: listed {len(tables_df)} tables in [{db_name}]")

            for _, row in tables_df.iterrows():
                table_name = row['TABLE_NAME']
                row_count = int(row['row_count']) if row['row_count'] is not None else 0

                try:
                    checksum = 0
                    try:
                        checksum_df = sql_connector.execute_query(f"""
                            SELECT CHECKSUM_AGG(CHECKSUM(*)) as chk
                            FROM [{db_name}].dbo.[{table_name}] WITH (NOLOCK)
                        """)
                        checksum = int(checksum_df.iloc[0]['chk']) if checksum_df is not None and checksum_df.iloc[0]['chk'] is not None else 0
                    except Exception:
                        checksum = 0

                    # Read full data for small tables (Opera transaction tables are typically < 5000 rows)
                    # Large tables (reports, audit, system config) get checksum-only
                    rows_data = None
                    if row_count > 0 and row_count <= max_rows_for_full_data:
                            try:
                                data_df = sql_connector.execute_query(f"""
                                    SELECT * FROM [{db_name}].dbo.[{table_name}] WITH (NOLOCK)
                                """)
                                if data_df is not None and not data_df.empty:
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
                                                row_dict[col] = val.hex()[:100]
                                            else:
                                                try:
                                                    row_dict[col] = float(val) if isinstance(val, (int, float)) else str(val).strip()
                                                except (ValueError, TypeError):
                                                    row_dict[col] = str(val)[:200]
                                        rows_data.append(row_dict)
                            except Exception as e:
                                logger.debug(f"Could not read {db_name}.{table_name}: {e}")

                    db_snapshot[table_name] = {
                        'row_count': row_count,
                        'checksum': checksum,
                        'rows': rows_data,
                    }
                except Exception as e:
                    logger.debug(f"Could not snapshot {db_name}.{table_name}: {e}")

        except Exception as e:
            logger.warning(f"Could not access database {db_name}: {e}")

        snapshot['databases'][db_name] = db_snapshot
        logger.info(f"Snapshot: {db_name} — {len(db_snapshot)} tables captured")

    return snapshot


def take_snapshot_opera3(data_path: str, file_filter: str = '') -> Dict[str, Any]:
    """
    Take a complete snapshot of Opera 3 FoxPro DBF tables.
    Scans both the company data folder and the System folder.
    Returns row counts and checksums for every table, plus full row data
    for tables with < 50,000 rows (for detailed diffing).

    `file_filter` (optional): accepts either
      - a short company identifier like `Z` or `INT`  → automatically
        expanded to the glob `Z_*` / `INT_*`, OR
      - a full glob (`Z_*`, `Z_PNAME.*`, etc.) — used as-is when the
        string contains glob characters (`*`, `?`, `[`).
    Applied to the company data folder only. System folder always fully
    scanned (its tables — `seqco`, `NextID`, `LastVer`, … — are
    unprefixed and needed in every trace regardless of company).
    """
    from pathlib import Path
    import fnmatch

    snapshot = {
        'timestamp': datetime.now().isoformat(),
        'source': 'opera3',
        'databases': {},
        'tables_per_folder': {},
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

    raw_pattern = (file_filter or '').strip()
    # If the user typed a short identifier like "Z" with no glob
    # characters, interpret it as a company-code prefix and expand to
    # `Z_*`. Anything containing *, ?, or [ is used verbatim.
    if raw_pattern and not any(ch in raw_pattern for ch in '*?['):
        pattern = f"{raw_pattern}_*"
    else:
        pattern = raw_pattern
    if pattern:
        snapshot['file_filter'] = pattern
        snapshot['file_filter_raw'] = raw_pattern

    for folder_label, folder_path in folders_to_scan.items():
        db_snapshot = {}

        if not folder_path.exists():
            continue

        # Find all DBF files
        dbf_files = list(folder_path.glob('*.dbf')) + list(folder_path.glob('*.DBF'))
        scanned_before_filter = len({f.stem.lower() for f in dbf_files})

        # Apply the optional filter — company folder only. System DBFs
        # always come through so the trace has full sequence/parameter
        # context.
        if pattern and folder_label == 'company':
            up = pattern.upper()
            dbf_files = [f for f in dbf_files if fnmatch.fnmatch(f.name.upper(), up)]

        scanned_after_filter = len({f.stem.lower() for f in dbf_files})
        snapshot['tables_per_folder'][folder_label] = {
            'matched': scanned_after_filter,
            'available_in_folder': scanned_before_filter,
        }
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


def _opera3_smb_config():
    """Read the Opera 3 SMB share name + credentials from config.ini.
    Returns (share, default_server_ip, user, password). The share name and
    a fallback host are parsed from `opera3_server_path` (\\host\Share)."""
    import configparser
    cfg = configparser.ConfigParser()
    cfg.read(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'config.ini'))
    server_path = cfg.get('opera', 'opera3_server_path', fallback='').strip()
    user = cfg.get('opera', 'opera3_share_user', fallback='').strip()
    password = cfg.get('opera', 'opera3_share_password', fallback='')
    share, default_host = '', ''
    unc = server_path.lstrip('\\')
    if unc:
        parts = unc.split('\\', 1)
        default_host = parts[0]
        share = parts[1] if len(parts) > 1 else ''
    return share, default_host, user, password


def take_snapshot_opera3_smb(
    server_ip: str,
    subpath: str,
    file_filter: str = '',
    max_download_mb: int = 500,
) -> Dict[str, Any]:
    """Snapshot an Opera 3 (FoxPro) company that lives on an SMB server,
    identified by IP — WITHOUT an OS-level mount.

    Connects over SMB (smbclient), downloads only the selected company's
    DBFs (+ .fpt/.cdx companions) and the System tables into a temp dir,
    runs the local FoxPro snapshot on them, then deletes the temp dir.

    - Share name + credentials come from config.ini [opera].
    - `subpath`     : folder within the share (e.g. 'Data' or 'Data/P').
    - `file_filter` : company-code prefix (e.g. 'Z') applied to the
      company folder; the System folder is always captured in full.
    - Refuses a company whose DBF+memo footprint exceeds `max_download_mb`
      (some live companies are multi-GB) — mount those locally instead.
    """
    import tempfile, shutil, fnmatch
    try:
        import smbclient
    except ImportError:
        raise HTTPException(status_code=503, detail="smbclient not installed — cannot read Opera 3 over SMB.")

    share, default_host, user, password = _opera3_smb_config()
    server_ip = (server_ip or default_host or '').strip()
    if not share:
        raise HTTPException(status_code=500, detail="opera3_server_path not configured — cannot determine the SMB share name.")
    if not server_ip:
        raise HTTPException(status_code=400, detail="No server IP supplied and none configured.")
    if not user or not password:
        raise HTTPException(status_code=500, detail="Opera 3 SMB credentials (opera3_share_user/password) not configured.")

    smbclient.register_session(server_ip, username=user, password=password)

    def remote(*segs):
        return '\\\\' + '\\'.join([server_ip, share] + [s for s in segs if s])

    subpath_win = (subpath or '').replace('/', '\\').strip('\\')
    company_remote = remote(subpath_win) if subpath_win else remote()

    # Expand a bare company code ('Z') to the prefix glob ('Z_*').
    raw = (file_filter or '').strip()
    pattern = f"{raw}_*" if raw and not any(c in raw for c in '*?[') else raw

    def is_dbf_family(n):
        # Only the files dbfread actually reads: the table (.dbf) and its
        # memo companion (.fpt). We deliberately do NOT pull .cdx indexes —
        # dbfread ignores them, and Opera keeps them open with a deny-read
        # lock (a live batch locks e.g. z_abatch.CDX), which would otherwise
        # abort the whole capture with a sharing violation.
        return n.lower().endswith(('.dbf', '.fpt'))

    def matched(n):
        if not is_dbf_family(n):
            return False
        return fnmatch.fnmatch(n.upper(), pattern.upper()) if pattern else True

    try:
        names = smbclient.listdir(company_remote)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Cannot list \\\\{server_ip}\\{share}\\{subpath_win}: {e}")

    company_files = [n for n in names if matched(n)]
    dbf_files = [n for n in company_files if n.lower().endswith('.dbf')]
    if not dbf_files:
        raise HTTPException(status_code=404, detail=(
            f"No DBF files matched '{pattern or '*'}' in \\\\{server_ip}\\{share}\\{subpath_win}. "
            "Check the path and company identifier."))

    def stat_size(path):
        try:
            return smbclient.stat(path).st_size
        except Exception:
            return 0
    total = sum(stat_size(f"{company_remote}\\{n}") for n in company_files)
    if total > max_download_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=(
            f"Company '{raw or subpath_win}' is ~{total/1e6:.0f} MB over SMB — too large to pull for a snapshot "
            f"(limit {max_download_mb} MB). Mount the share locally and snapshot a local path instead."))

    def download(remote_path, local_path):
        with smbclient.open_file(remote_path, mode='rb', share_access='r') as rf:
            with open(local_path, 'wb') as lf:
                while True:
                    chunk = rf.read(1 << 16)
                    if not chunk:
                        break
                    lf.write(chunk)

    # Opera's Pegasus background service permanently holds a handful of
    # System-service tables open (locking, notifications, messaging,
    # full-text search) — these are NOT posting data and are expected to
    # be unreadable over SMB even when no user is in the company. Skipping
    # them is benign. A locked *company* table, by contrast, means Opera
    # has the company open (a live batch) — that we must not snapshot past.
    SYSTEM_SERVICE_TABLES = {
        'search', 'seqlock', 'seqnotif', 'seqmsgdist', 'seqmsg', 'seqgrp',
        'sequsrgrp', 'seqproc', 'seqco', 'seqaudit', 'seqses',
    }

    def download_resilient(remote_path, local_path, name, bucket):
        try:
            download(remote_path, local_path)
        except Exception as e:
            bucket.append(name)
            # Drop a partial file so dbfread never sees a truncated table.
            try:
                if os.path.exists(local_path):
                    os.remove(local_path)
            except OSError:
                pass
            logger.warning(f"Opera 3 SMB: skipped locked/unreadable '{name}': {e}")

    skipped_company, skipped_system = [], []

    tmp = tempfile.mkdtemp(prefix='o3snap_')
    try:
        data_dir = os.path.join(tmp, 'DATA')
        sys_dir = os.path.join(tmp, 'System')
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(sys_dir, exist_ok=True)

        for n in company_files:
            download_resilient(f"{company_remote}\\{n}", os.path.join(data_dir, n), n, skipped_company)

        # System folder — always at the share root. Per-file resilience so
        # one locked system file doesn't drop them all.
        system_remote = remote('System')
        try:
            sys_names = smbclient.listdir(system_remote)
        except Exception as e:
            sys_names = []
            logger.warning(f"Opera 3 SMB: could not list System folder at {system_remote}: {e}")
        for n in sys_names:
            if is_dbf_family(n):
                download_resilient(f"{system_remote}\\{n}", os.path.join(sys_dir, n), n, skipped_system)

        # A locked COMPANY table = Opera still has the company open, so the
        # capture would be incomplete and the diff would invent phantom
        # rows. Fail honestly. Locked System-SERVICE tables are expected
        # (Pegasus holds them) and tolerated; any OTHER locked system table
        # is treated like a company lock (it should be readable when idle).
        blocking = list(skipped_company) + [
            s for s in skipped_system if s.rsplit('.', 1)[0].lower() not in SYSTEM_SERVICE_TABLES
        ]
        if blocking:
            locked_tables = sorted({s.rsplit('.', 1)[0].lower() for s in blocking})
            shown = ', '.join(locked_tables[:12]) + (f", +{len(locked_tables) - 12} more" if len(locked_tables) > 12 else '')
            raise HTTPException(status_code=409, detail=(
                f"Opera is holding {len(locked_tables)} table(s) open on this company, so they "
                f"can't be read over SMB: {shown}. Finish and close the transaction (and ideally "
                f"exit the company) in Opera so it releases the file locks, then take the snapshot "
                f"again. Tip: take the AFTER snapshot once the posting is fully committed and the "
                f"Opera screen is closed."
            ))

        tolerated = sorted({s.rsplit('.', 1)[0].lower() for s in skipped_system})
        if tolerated:
            logger.info(f"Opera 3 SMB: tolerated {len(tolerated)} always-locked system-service table(s): {tolerated}")

        # Reuse the local FoxPro scanner on the downloaded files. It finds
        # the System folder as data_dir's sibling (tmp/System).
        snapshot = take_snapshot_opera3(data_dir, file_filter='')
        snapshot['source'] = 'opera3'
        snapshot['_smb_server'] = server_ip
        snapshot['_smb_subpath'] = subpath
        snapshot['_smb_filter'] = raw
        snapshot['_dbf_downloaded'] = len(dbf_files)
        snapshot['_mb_downloaded'] = round(total / 1e6, 1)
        if tolerated:
            snapshot['_skipped_system_service'] = tolerated
        logger.info(
            f"Opera 3 SMB snapshot: {server_ip}\\{share}\\{subpath_win} "
            f"filter='{pattern or '*'}' — {len(dbf_files)} DBFs / {total/1e6:.1f} MB"
        )
        return snapshot
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


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
                import json as _json
                from collections import Counter as _Counter

                def _sig(r):
                    return _json.dumps(r, sort_keys=True, default=str)

                # Candidate key column (id / *_id), case-insensitive so it
                # also matches FoxPro's upper-case 'ID'.
                pk_col = None
                if before_rows:
                    if 'id' in before_rows[0]:
                        pk_col = 'id'
                    else:
                        for candidate in before_rows[0].keys():
                            if candidate.lower() == 'id' or candidate.lower().endswith('_id'):
                                pk_col = candidate
                                break

                def _usable(rows, col):
                    # Only trust a key that is present, fully populated AND
                    # unique in these rows. Many FoxPro tables (e.g. salloc)
                    # have no such key — key-matching then silently collapses
                    # rows and misses the added ones. Fall back to full-row
                    # signature diff for those.
                    if not rows or not col or col not in rows[0]:
                        return False
                    vals = [str(r.get(col, '')) for r in rows]
                    return all(v != '' for v in vals) and len(set(vals)) == len(vals)

                if _usable(before_rows, pk_col) and _usable(after_rows, pk_col):
                    before_by_pk = {str(r.get(pk_col, '')): r for r in before_rows}
                    after_by_pk = {str(r.get(pk_col, '')): r for r in after_rows}

                    for pk, row in after_by_pk.items():
                        if pk not in before_by_pk:
                            table_change['added_rows'].append(row)
                    for pk, row in before_by_pk.items():
                        if pk not in after_by_pk:
                            table_change['deleted_rows'].append(row)
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
                    table_change['diff_method'] = f'key:{pk_col}'
                elif len(before_rows) == len(after_rows):
                    # No unique key, but the row COUNT is unchanged → these are
                    # in-place modifications (e.g. balance tables nacnt/nbank/
                    # sname). Pair rows positionally (FoxPro physical order is
                    # stable when rows are updated in place) to recover
                    # field-level before/after.
                    for i, (br, ar) in enumerate(zip(before_rows, after_rows)):
                        field_changes = {}
                        for field in set(list(br.keys()) + list(ar.keys())):
                            if str(br.get(field)) != str(ar.get(field)):
                                field_changes[field] = {'before': br.get(field), 'after': ar.get(field)}
                                table_change['modified_fields'].add(field)
                        if field_changes:
                            table_change['modified_rows'].append({
                                'pk': f'row#{i}', 'pk_column': '(positional)', 'changes': field_changes,
                            })
                    table_change['diff_method'] = 'positional (no unique key, equal row count)'
                else:
                    # No unique key AND the row count changed → additions/
                    # deletions. Diff by full-row signature so the added/
                    # deleted row CONTENTS are captured (e.g. salloc).
                    b = _Counter(_sig(r) for r in before_rows)
                    a = _Counter(_sig(r) for r in after_rows)
                    b_repr, a_repr = {}, {}
                    for r in before_rows:
                        b_repr.setdefault(_sig(r), r)
                    for r in after_rows:
                        a_repr.setdefault(_sig(r), r)
                    for sig, cnt in (a - b).items():
                        table_change['added_rows'].extend([a_repr[sig]] * cnt)
                    for sig, cnt in (b - a).items():
                        table_change['deleted_rows'].extend([b_repr[sig]] * cnt)
                    table_change['diff_method'] = 'row-signature (no unique key)'

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
    {'module': 'vat', 'name': 'VAT Transaction (any VAT-bearing posting — usually already covered)', 'description': 'Any transaction carrying VAT qualifies (sales/purchase invoice with VAT, nominal payment/receipt with VAT). VAT is recorded in zvtran (NOT nvat — captures proved nvat stays 0; that mistaken assumption caused the apps\' missing-from-return bug). If you have already captured the invoice + nominal-with-VAT presets, this one is covered — skip it.'},
    {'module': 'vat', 'name': 'VAT Return / Update', 'description': 'Run the VAT return calculate/commit (MTD/online submission NOT required — disable it in VAT processing options on a demo company). THE decisive VAT-pipeline capture: shows the return building zvtran from the pending sources and marking them consumed (Opera 3 evidence 2026-07-08: +57 zvtran from sanal/panal, va_commvat stamped, zrcsl/zvonline audit rows). On SE company Z there is ONE pending nvat row (2026-07-30 cashbook VAT posting) — this capture proves how nvat is consumed, the last unobserved corner.'},
    {'module': 'reconciliation', 'name': 'Bank Reconciliation — Clear Incomplete', 'description': 'OPTIONAL on SE (the apps\' reverse-rec is already round-trip-verified live). Start a reconciliation, mark some lines provisionally, snapshot BEFORE, click Clear, snapshot AFTER. Expected: in-progress marks cleared on aentry, completed-rec fields on nbank untouched.'},
    {'module': 'allocations', 'name': 'Sales Receipt with Allocation (invoices selected at posting)', 'description': 'Sales receipt where the operator selects the invoice(s) to pay in the posting screen — Opera\'s native at-posting allocation. ("Auto-allocate" is the APPS\' matching feature; this native shape is what it must reproduce.)'},
    {'module': 'allocations', 'name': 'Purchase Payment with Allocation (invoices selected at posting)', 'description': 'Purchase payment where the operator selects the invoice(s) to pay at posting — Opera\'s native at-posting allocation. (The apps\' "auto-allocate" reproduces this shape.)'},
    {'module': 'customer_master', 'name': 'New Customer', 'description': 'Create a new customer account in sname.'},
    {'module': 'customer_master', 'name': 'Edit Customer', 'description': 'Modify an existing customer account.'},
    {'module': 'supplier_master', 'name': 'New Supplier', 'description': 'Create a new supplier account in pname.'},
    {'module': 'supplier_master', 'name': 'Edit Supplier', 'description': 'Modify an existing supplier account.'},
    {'module': 'nominal_master', 'name': 'New Nominal Account', 'description': 'Create a new nominal account in nacnt/nname.'},
    {'module': 'bank_master', 'name': 'New Bank Account', 'description': 'TWO-STEP (capture separately): a bank account REQUIRES an existing nominal account — capture \'New Nominal Account\' first (nname/nacnt), then this one: the Cashbook setup that designates it as a bank (nbank row, zero balances, cashbook-type wiring, link to the nominal).'},
    {'module': 'stock_master', 'name': 'New Stock Item', 'description': 'Create a new stock/product record.'},
    {'module': 'employee_master', 'name': 'New Employee', 'description': 'Create a new employee record in payroll.'},
    {'module': 'payroll', 'name': 'Payroll Run', 'description': 'DEFERRED — separate project (payroll-updating app, not yet scheduled). Multi-step process: capture per stage when the time comes. April 2026 already captured calculation / employee calc / NL analysis / cashbook transfer; the only outstanding nominal-relevant stage is the payroll UPDATE (period-end commit to NL). Not write-back relevant today.'},
    # ---- Added 2026-07-30: from the SE pipeline findings (transfers stamp memos
    # *_done='Y' + per-run journal and NEVER touch VAT; VAT Processing sweeps
    # pending sources into zvtran). Verify the same mechanics on Opera 3.
    {'module': 'nominal', 'name': 'NL Transfer — Cashbook',
     'description': 'ONLY if Opera 3 shows pending cashbook items to transfer (transfer mode). Expected (SE parity): anoml memos stamped ax_done=Y + ax_jrnl=run journal; ntran built (one journal per run); nacnt/nhist updated; NO nvat/zvtran changes. If nothing is pending (RTU on), skip. NOTE: an empty run records nothing — only capture this when the module actually shows pending items.'},
    {'module': 'nominal', 'name': 'NL Transfer — Purchase',
     'description': 'ONLY if pending. Expected: pnoml stamped px_done=Y + px_jrnl (SE split journals per posting type — invoices vs credit notes); ntran built; no VAT changes. NOTE: an empty run records nothing — only capture this when the module actually shows pending items.'},
    {'module': 'nominal', 'name': 'NL Transfer — Stock / SOP',
     'description': 'Nominal transfer of pending STOCK / SOP items (cnoml memos — despatch documents, cx_tref like "…MAIN DEL01328"). Run when Stock shows pending items to transfer. Expect: cnoml memos stamped cx_done=Y with the run journal; ntran built (one journal per run); nacnt/nhist/nsubt/ntype updated; idtab journal counter advanced. Capture this BEFORE switching Real Time Update NL on, since RTU-on removes the staging step. NOTE: an empty run records nothing — only capture this when the module actually shows pending items.'},
    {'module': 'nominal', 'name': 'NL Transfer — Payroll / Wages',
     'description': 'Nominal transfer of pending PAYROLL items (wnoml memos — wx_nacnt W-codes such as W120 Employers NI, W140 SSP, wx_type "W"). Run when Payroll shows pending items to transfer. Expect: wnoml memos stamped wx_done=Y with the run journal; ntran built; nacnt/nhist updated. This is the only capture we have of the payroll-to-nominal path, so it is worth taking even if the run is small. NOTE: an empty run records nothing — only capture this when the module actually shows pending items.'},
    {'module': 'nominal', 'name': 'NL Transfer — Fixed Assets',
     'description': 'Nominal transfer of pending FIXED ASSET items (fnoml memos — depreciation and disposal postings). Only if Fixed Assets shows pending items; on demo Z there are currently none. Expect the same shape as the other transfers: fnoml stamped fx_done=Y, ntran built, nacnt/nhist updated. NOTE: an empty run records nothing — only capture this when the module actually shows pending items.'},
    {'module': 'nominal', 'name': 'NL Transfer — Sales',
     'description': 'ONLY if pending. Expected: snoml stamped sx_done=Y + sx_jrnl; ntran built; no VAT changes. NOTE: an empty run records nothing — only capture this when the module actually shows pending items.'},
    {'module': 'vat', 'name': 'VAT Processing — re-run with pending nvat (Opera 3)',
     'description': 'REQUIRED for the agent VAT verdict on Opera 3. Post (or reuse) a nominal payment WITH VAT first so an nvat row is pending, then bracket a full VAT Processing calculate+commit (MTD not needed). Key questions: does O3 convert pending nvat into zvtran (SE did: nvat.state 1→2, zvtran N-source row) and does it stamp va_commvat by taxdate-in-period? The 2026-07-08 O3 run built zvtran from sanal/panal but left nvat untouched — unresolved whether that was mechanism or period-scope.'},
    {'module': 'nominal_master', 'name': 'New Nominal Account (Opera 3)',
     'description': 'Step 1 of the bank-account pair: create the nominal account in the NL (nname/nacnt). A cashbook bank REQUIRES this to exist first (Harry, 2026-07-30).'},
    {'module': 'bank_master', 'name': 'New Bank Account (Opera 3)',
     'description': 'Step 2, captured SEPARATELY: the Cashbook setup that designates the nominal as a bank — nbank row, zero balances, cashbook-type wiring, link to the nominal.'},
    # ---- Foreign-currency (FC) captures for Phase-2 FX support (added 2026-07-17).
    # Enter each with the company in MULTI-CURRENCY mode and a FOREIGN-currency
    # customer / supplier / bank (FC detection: sprfls.sc_currncy /
    # pprfls.pc_currncy / nbank.nk_fcurr NON-BLANK; home = zxchg.xc_home where
    # xc_home=1). What the FX write-back needs from each diff: the extra FC
    # columns *_fcurr (currency), *_fcrate (exchange rate), *_fcval (foreign
    # value), *_fcbal (foreign balance), *_fcvat (foreign VAT), *_fcmult/*_fcdec
    # (rate multiplier / FC decimals), the rate row consulted in zxchg, AND any
    # exchange gain/loss nominal posting. Company Z (Demo) has FC customers
    # (e.g. ALI0005 = EUR) to use.
    {'module': 'fc', 'name': 'FC Sales Receipt (foreign customer)', 'description': 'Receipt from a FOREIGN-currency customer. All the home Sales-Receipt tables PLUS the FC columns on atran/stran/ntran (*_fcurr/*_fcrate/*_fcval/*_fcbal). Capture BOTH allocated and on-account. Note any exchange gain/loss nominal when the rate moved since the invoice.'},
    {'module': 'fc', 'name': 'FC Purchase Payment (foreign supplier)', 'description': 'Payment to a FOREIGN-currency supplier. Home Purchase-Payment tables PLUS *_fcurr/*_fcrate/*_fcval/*_fcbal on atran/ptran/ntran; capture the exchange gain/loss if the rate differs from the invoice.'},
    {'module': 'fc', 'name': 'FC Nominal Receipt/Payment (foreign bank)', 'description': 'Direct nominal receipt or payment on a FOREIGN-currency BANK (nbank.nk_fcurr set). Capture how nk_curbal (held in the bank currency) relates to the home-currency nominal legs via *_fcrate.'},
    {'module': 'fc', 'name': 'FC Bank Transfer (cross-currency)', 'description': 'Transfer where one leg is a FOREIGN-currency bank (or the two banks differ in currency). Capture both legs\' *_fcurr/*_fcrate/*_fcval and the exchange-difference posting — the hardest FX case.'},
    {'module': 'fc', 'name': 'FC Recurring Entry', 'description': 'Post a due recurring entry whose account is foreign-currency. Capture the FC columns on the generated transaction and how the rate is sourced at post time.'},
    {'module': 'fc', 'name': 'FC Sales Allocation (rate difference)', 'description': 'Allocate an FC receipt against an FC invoice where the RATE DIFFERS between them — capture salloc + the exchange gain/loss the allocation posts.'},
    {'module': 'fc', 'name': 'FC Purchase Allocation (rate difference)', 'description': 'Allocate an FC payment against an FC invoice with a rate difference — palloc + exchange gain/loss.'},
    {'module': 'fc', 'name': 'FC Bank Reconciliation (foreign bank)', 'description': 'Reconcile entries on a FOREIGN-currency bank — capture whether ae_recbal / nk_recbal are held in the bank currency or home, plus any FC-specific reconciliation fields.'},
    {'module': 'fc', 'name': 'FC GoCardless Batch (foreign customer)', 'description': 'GoCardless batch containing a FOREIGN-currency customer receipt — the FC receipt columns within the batch, plus fee/VAT handling in the foreign currency.'},
    {'module': 'fc', 'name': 'FC Sales Invoice (foreign customer)', 'description': 'Sales invoice to a FOREIGN-currency customer — the origination point of FC values (st_fcurr/st_fcrate/st_fcval/st_fcbal + sname FC balances, rate from zxchg). Capture before FC receipts/allocations — they reference this invoice\'s stored rate.'},
    {'module': 'fc', 'name': 'FC Purchase Invoice (foreign supplier)', 'description': 'Purchase invoice from a FOREIGN-currency supplier — FC origination on the PL side (pt_fcurr/pt_fcrate/pt_fcval/pt_fcbal + pname FC balances).'},
    {'module': 'fc', 'name': 'Exchange Rate Update (zxchg)', 'description': 'Update a currency\'s rate in Opera\'s exchange-rate table — zxchg delta only; shows which fields apps read for rate sourcing and whether history is kept.'},
    {'module': 'fc', 'name': 'FC Revaluation (period-end)', 'description': 'Foreign Currency Revaluation routine after a rate move — unrealised gain/loss postings + *_fcbal restatements on open FC balances.'},

    # --- Parity-harness gaps (added 2026-08-02) -------------------------------
    # bank-rec's automated Opera-parity test (tests/golden-master/
    # opera-parity.test.ts) compares the tables OUR postings write against the
    # tables OPERA writes. It can only compare like with like: RTU mode and VAT
    # presence both change a posting's table footprint legitimately, so a
    # capture taken in a different mode is skipped as incomparable.
    #
    # Three of its seven scenarios are skipped today for exactly that reason.
    # These three presets close them, taking coverage from 4/7 to 7/7. The
    # CONFIGURATION IS THE POINT — capture each in the stated mode or it will
    # not match and the gap stays open.
    #
    # RTU = Nominal > Utilities > Set Options > Real Time Update NL
    #       (seqco.co_rtupdnl). ON writes ntran immediately; OFF queues anoml
    #       memos for the NL transfer to build ntran later.
    {'module': 'cashbook', 'name': 'Bank Transfer RTU OFF (parity gap 1 of 3)',
     'description': 'A normal internal bank-to-bank transfer, but captured with Real Time Update NL switched OFF. Expect the two mirrored cashbook sides (2x aentry + 2x atran) and 2x anoml memos, and NO ntran — the NL transfer builds those later. Our bank-transfer test scenario runs RTU-off, so the existing RTU-on capture cannot be compared against it. Turn RTU off, post the transfer, capture, then put RTU back as it was.'},
    {'module': 'cashbook', 'name': 'Nominal Payment with VAT RTU ON (parity gap 2 of 3)',
     'description': 'A nominal payment carrying a VAT code, captured with Real Time Update NL switched ON. Expect aentry + atran, the net/VAT split across anoml, AND ntran written immediately, plus nacnt/nhist. The existing "Nominal Payment with VAT" capture was taken RTU-off so it cannot be compared with our RTU-on scenario. This capture should also settle whether Opera writes nvat or zvtran at posting on an RTU-on nominal payment — two existing captures disagree and it has never been resolved from live evidence.'},
    {'module': 'cashbook', 'name': 'Nominal Payment NO VAT (parity gap 3 of 3)',
     'description': 'A nominal payment with NO VAT code on any line — e.g. a bank charge, or wages to a non-VAT nominal. Expect no VAT table touched at all. Our multi-line-nominal scenario posts no VAT, so today it is compared against a VAT-bearing capture and skipped. Use two or more analysis lines if convenient, so the per-line writes separate.'},
]

# Opera 3 write-feature checklist — the golden masters we need to capture
# to evaluate the Opera 3 write agent against Opera-native behaviour. Scoped
# to the transaction types the write path actually produces (see the Opera 3
# parity table in CLAUDE.md); deliberately excludes generic master-data /
# payroll presets.
OPERA3_PRESETS = [
    {'module': 'cashbook', 'name': 'Sales Receipt — BACS',
     'description': 'Customer receipt via BACS, ALLOCATED to invoice(s). Cashbook at_type 4 (positive value): aentry + atran in PENCE; anoml transfer + ntran in POUNDS (debit bank nominal / credit debtors control); nacnt + nhist balances; nbank.nk_curbal increased. Sales ledger: stran receipt (POUNDS) + salloc rows against the invoice, invoice st_paid/st_trbal reduced, sname.sn_currbal reduced. Entry no. from atype.ay_entry, ids from nextid, journal from nparm. No VAT (VAT was on the original invoice).'},
    {'module': 'cashbook', 'name': 'Sales Receipt — Cheque',
     'description': 'Customer receipt via cheque, ON ACCOUNT (UNALLOCATED). Cashbook at_type 4 (positive value): aentry + atran in PENCE (cheque no. recorded on the entry); anoml transfer + ntran in POUNDS (debit bank / credit debtors control); nacnt + nhist; nbank.nk_curbal increased. Sales ledger: stran unallocated receipt (POUNDS), sname.sn_currbal reduced — NO salloc, the invoice is NOT touched (sits as unallocated credit). Entry no. from atype.ay_entry, ids from nextid. No VAT.'},
    {'module': 'cashbook', 'name': 'Purchase Payment — BACS',
     'description': 'Supplier payment via BACS, ALLOCATED to invoice(s). Cashbook at_type 5 (negative value): aentry + atran in PENCE; anoml transfer + ntran in POUNDS (credit bank / debit creditors control); nacnt + nhist; nbank.nk_curbal decreased. Purchase ledger: ptran payment (POUNDS) + palloc rows against the invoice, invoice pt_paid/pt_trbal reduced, pname.pn_currbal reduced. Entry no. from atype.ay_entry, ids from nextid. No VAT (VAT was on the original invoice).'},
    {'module': 'cashbook', 'name': 'Purchase Payment — Cheque',
     'description': 'Supplier payment via cheque, ON ACCOUNT (UNALLOCATED). Cashbook at_type 5 (negative value): aentry + atran in PENCE (cheque no. recorded); anoml transfer + ntran in POUNDS (credit bank / debit creditors control); nacnt + nhist; nbank.nk_curbal decreased. Purchase ledger: ptran unallocated payment (POUNDS), pname.pn_currbal reduced — NO palloc, invoice NOT touched. No VAT.'},
    {'module': 'cashbook', 'name': 'Sales Refund',
     'description': 'Refund PAID to a customer, against an unallocated credit note. Cashbook at_type 3 (negative value — opposite to a receipt): aentry + atran in PENCE; anoml + ntran in POUNDS (credit bank / debit debtors control); nacnt + nhist; nbank.nk_curbal decreased. Sales ledger: stran refund (POUNDS) + salloc against the credit note, sname.sn_currbal updated. Entry no. from atype.ay_entry. No VAT.'},
    {'module': 'cashbook', 'name': 'Purchase Refund',
     'description': 'Refund RECEIVED from a supplier, against an unallocated credit note. Cashbook at_type 6 (positive value — opposite to a payment): aentry + atran in PENCE; anoml + ntran in POUNDS (debit bank / credit creditors control); nacnt + nhist; nbank.nk_curbal increased. Purchase ledger: ptran refund (POUNDS) + palloc against the credit note, pname.pn_currbal updated. No VAT.'},
    {'module': 'cashbook', 'name': 'Nominal Payment with VAT',
     'description': 'Direct payment to a nominal account carrying a VAT code (no sales/purchase ledger). Cashbook at_type 1 (negative value): aentry + atran in PENCE; anoml + ntran in POUNDS, split net-to-expense-nominal and VAT-to-VAT-control; nacnt + nhist; nbank.nk_curbal decreased. VAT recorded at posting in nvat (NATIVE behaviour, verified 2026-07-08 capture — NOT zvtran; zvtran is built later by VAT Processing). The write agent deliberately writes zvtran-final instead (validated on SE live returns; O3 validation = the VAT Processing re-run preset). Entry no. from atype.ay_entry, journal from nparm.'},
    {'module': 'cashbook', 'name': 'Nominal Receipt with VAT',
     'description': 'Direct receipt to a nominal account carrying a VAT code (no ledger). Cashbook at_type 2 (positive value): aentry + atran in PENCE; anoml + ntran in POUNDS, split net-to-nominal and VAT-to-VAT-control; nacnt + nhist; nbank.nk_curbal increased. VAT recorded in zvtran (Opera VAT analysis). Entry no. from atype.ay_entry, journal from nparm.'},
    {'module': 'bank_transfer', 'name': 'Bank Transfer',
     'description': 'Internal transfer between two bank accounts, at_type 8, no VAT. Two mirrored sides sharing one unique id: 2x aentry + 2x atran (PENCE, opposite signs); 2x anoml + 2x ntran (POUNDS, nt_posttyp = T); both nbank.nk_curbal updated (source down / destination up); both bank nacnt balances updated.'},
    {'module': 'sales_ledger', 'name': 'Sales Invoice',
     'description': 'Sales invoice keyed to the sales ledger. stran invoice (POUNDS) + snoml transfer; ntran debit debtors control / credit sales + credit VAT control, with nacnt + nhist; sname.sn_currbal increased; output VAT recorded in zvtran (Opera VAT analysis). Journal from nparm, ids from nextid.'},
    {'module': 'sales_ledger', 'name': 'Sales Credit Note',
     'description': 'Sales credit note — mirror of a sales invoice, opposite signs. stran credit (POUNDS) + snoml; ntran credit debtors control / debit sales + debit VAT control, with nacnt + nhist; sname.sn_currbal reduced; VAT reversed in zvtran.'},
    {'module': 'purchase_ledger', 'name': 'Purchase Invoice',
     'description': 'Purchase invoice keyed to the purchase ledger. ptran invoice (POUNDS) + pnoml transfer; ntran credit creditors control / debit expense + debit VAT control, with nacnt + nhist; pname.pn_currbal increased; input VAT recorded in zvtran (Opera VAT analysis). Journal from nparm.'},
    {'module': 'purchase_ledger', 'name': 'Purchase Credit Note',
     'description': 'Purchase credit note — mirror of a purchase invoice, opposite signs. ptran credit (POUNDS) + pnoml; ntran debit creditors control / credit expense + credit VAT control, with nacnt + nhist; pname.pn_currbal reduced; VAT reversed in zvtran.'},
    {'module': 'recurring', 'name': 'Recurring Entries',
     'description': 'Post due recurring entries (arhead header + arline lines). Each due line generates its underlying ledger transaction (stran/ptran or nominal) with the same posting, balance and zvtran VAT rules as a keyed invoice, and the recurring schedule dates on arhead/arline are advanced to the next due date.'},
    {'module': 'cashbook', 'name': 'Batch Receipt (allocated + unallocated)',
     'description': 'A cashbook batch of sales receipts posted together and AUTO-COMPLETED (batch created, then finalised in one step). Capture a MIX: some receipts allocated to invoices (stran + salloc, invoice st_paid/st_trbal reduced) and some on account / unallocated (stran only, sname credit). Batch grouping in abatch; entry numbers increment across the batch via atype.ay_entry; auto-complete sets ae_complet=1 and posts the nominal (ntran/nacnt/nhist) in real time. This is the core of the GoCardless payout flow — the full GoCardless posting = this batch receipt + the GoCardless fee as a Nominal Payment with VAT + the net payout as a Bank Transfer (both already captured separately).'},
    {'module': 'allocations', 'name': 'Sales Receipt with Allocation (invoices selected at posting)',
     'description': 'Sales receipt with the invoice(s) selected in the posting screen — Opera 3\'s native at-posting allocation ("auto-allocate" is the APPS\' matching feature; this is the native shape it reproduces). The full allocated Sales Receipt posting (aentry/atran/stran/ntran/anoml/nacnt/nhist/nbank/sname) PLUS salloc rows and the invoice st_paid/st_trbal reduced. Isolates the allocation mechanics (salloc keys, part-payment vs full).'},
    {'module': 'allocations', 'name': 'Purchase Payment with Allocation (invoices selected at posting)',
     'description': 'Purchase payment with the invoice(s) selected at posting — Opera 3\'s native at-posting allocation (the apps\' "auto-allocate" reproduces this). The full allocated Purchase Payment posting (aentry/atran/ptran/ntran/anoml/nacnt/nhist/nbank/pname) PLUS palloc rows and the invoice pt_paid/pt_trbal reduced. Isolates the allocation mechanics (palloc keys, part-payment vs full).'},
    # ---- Added 2026-07-20: STANDALONE allocations (SL/PL Allocation routine
    # applied to EXISTING transactions — no new receipt/payment in the same
    # act). This is the exact shape the agent's post-hoc allocate verb must
    # match; the auto-allocate captures above bundle allocation inside a new
    # receipt/payment posting.
    {'module': 'allocations', 'name': 'Sales Allocation — standalone (existing receipt to invoice)',
     'description': 'Opera SL Allocation routine: allocate an EXISTING on-account receipt against an outstanding invoice, with NO new cashbook posting. Expected delta: salloc rows only (receipt + invoice legs sharing one allocation set), invoice st_paid/st_trbal reduced and the receipt\'s unallocated balance consumed; sname total balance unchanged (allocation moves nothing in or out). NO aentry/atran/ntran/anoml/nbank/zvtran changes. Golden master for the agent\'s sales allocate verb.'},
    {'module': 'allocations', 'name': 'Purchase Allocation — standalone (existing payment to invoice)',
     'description': 'Opera PL Allocation routine: allocate an EXISTING on-account payment against an outstanding purchase invoice, with NO new cashbook posting. Expected delta: palloc rows only (payment + invoice legs sharing one allocation set), invoice pt_paid/pt_trbal reduced and the payment\'s unallocated balance consumed; pname total balance unchanged. NO aentry/atran/ntran/anoml/nbank/zvtran changes. Golden master for the agent\'s purchase allocate verb.'},
    {'module': 'vat', 'name': 'VAT Return / Update',
     'description': 'Run Opera\'s VAT return / VAT update AFTER some VAT-bearing transactions have been posted (invoices, nominal-with-VAT). Snapshot BEFORE, run the VAT return in Opera, then AFTER. This is the step that populates the VAT-analysis file zvtran — which is NOT written at posting time on either engine. Purpose: confirm whether Opera builds zvtran itself from the posted transactions (st_vatval/pt_vatval + nominal VAT-control) or whether it must be written at posting. Determines whether the write-backs need to populate zvtran or leave it to Opera\'s return.'},
    # ---- Added 2026-07-12: the write-agent verbs still lacking golden masters
    # (gocardless-batch chain, cashbook recurring, reconcile mark/unmark,
    # ensure-supplier). Each maps 1:1 to a live agent endpoint.
    {'module': 'reconciliation', 'name': 'Bank Reconciliation — Mark Reconciled',
     'description': 'Reconcile one or more cashbook entries against a bank statement in Opera\'s bank reconciliation. Per entry on aentry: ae_reclnum = the reconciliation batch number, ae_statln = statement line (Opera convention 10, 20, 30…), ae_recbal = running reconciled balance in PENCE, ae_frstat/ae_tostat stamped, ae_tmpstat cleared. On nbank: nk_recbal advanced by the reconciled movement, nk_lststno = the statement number, last-rec fields stamped. No ledger/nominal/VAT changes — reconciliation only marks. Agent endpoint: /reconcile/mark. Capture BOTH a full and a partial (ae_tmpstat-only) reconciliation if possible.'},
    {'module': 'reconciliation', 'name': 'Bank Reconciliation — Clear Incomplete',
     'description': 'Clear an IN-PROGRESS (incomplete) reconciliation in Opera — the only reversal Opera supports: once a reconcile is COMPLETED there is no going back (by design — you only complete once correct). Expected: the in-progress marks (ae_tmpstat / any provisional statement-line stamps) cleared on the affected aentry rows; nbank\'s completed-rec fields (nk_recbal, nk_lststno, last-rec stamps) UNTOUCHED because nothing was completed. NOTE for the write-agent: /reconcile/unmark (reversing completed marks) is an APP-ONLY recovery tool with no Opera-native counterpart — its correctness bar is round-trip identity (after unmark, every field equals the before-mark snapshot; proven live on company Z), not an Opera golden master.'},
    {'module': 'gocardless', 'name': 'GoCardless Payout — Full Chain (app/agent-generated — NOT a native Opera routine)',
     'description': 'The COMPLETE GoCardless payout posting as ONE operation (the chained posting the apps send to /import/gocardless-batch): a batch of customer receipts (one aentry header at the batch gross in PENCE, ae_complet=1; atran per payment in PENCE; stran per customer in POUNDS; sname balances) + the GoCardless FEE legs in the same operation (ntran: debit fee expense net, debit VAT control from the fee\'s ztax code, credit bank; VAT to zvtran per the engine\'s VAT model) + the NET payout transferred to the destination bank (the Bank Transfer pattern, source = collection bank, dest = main bank). nbank/nacnt/nhist all updated. Differs from \'Batch Receipt\': this captures the WHOLE chain in one before/after so the union of writes is the chain\'s golden master (per the chained-postings rule — one agent routine per chain).'},
    {'module': 'cashbook', 'name': 'Recurring Cashbook Entry — Post Due',
     'description': 'Post a DUE recurring CASHBOOK entry (standing order/DD template in arhead + arline — distinct from ledger recurring invoices). Per line: aentry + atran in PENCE (entry no. from atype.ay_entry), anoml/ntran in POUNDS per the RTU setting, nacnt + nhist; nbank.nk_curbal moved by the schedule total. On the schedule: arhead\'s last-posted (ae_lstpost) and next-due (ae_nxtpost) dates advanced by the frequency, posted-count incremented. Agent endpoint: /import/recurring-entry. Capture a schedule with 2+ lines so the per-line vs per-schedule writes separate.'},
    {'module': 'supplier_master', 'name': 'New Supplier (ensure)',
     'description': 'Create a supplier account the way Opera does (the minimal record the agent\'s /supplier/ensure must reproduce): pname row with account/name/defaults (profile link, terms, currency), any companion profile/analysis rows Opera writes alongside, and the id/sequence sources used. ALSO capture editing nothing (re-saving the same supplier) to prove which fields Opera touches on a no-op save — the agent\'s ensure is idempotent-when-exists. Used by AP automation before posting a purchase invoice for an unknown supplier.'},
    # ---- Added 2026-07-30: from the SE pipeline findings (transfers stamp memos
    # *_done='Y' + per-run journal and NEVER touch VAT; VAT Processing sweeps
    # pending sources into zvtran). Verify the same mechanics on Opera 3.
    {'module': 'nominal', 'name': 'NL Transfer — Cashbook (Opera 3)',
     'description': 'ONLY if Opera 3 shows pending cashbook items to transfer (transfer mode). Expected (SE parity): anoml memos stamped ax_done=Y + ax_jrnl=run journal; ntran built (one journal per run); nacnt/nhist updated; NO nvat/zvtran changes. If nothing is pending (RTU on), skip.'},
    {'module': 'nominal', 'name': 'NL Transfer — Purchase (Opera 3)',
     'description': 'ONLY if pending. Expected: pnoml stamped px_done=Y + px_jrnl (SE split journals per posting type — invoices vs credit notes); ntran built; no VAT changes.'},
    {'module': 'nominal', 'name': 'NL Transfer — Sales (Opera 3)',
     'description': 'ONLY if pending. Expected: snoml stamped sx_done=Y + sx_jrnl; ntran built; no VAT changes.'},
    {'module': 'vat', 'name': 'VAT Processing — re-run with pending nvat (Opera 3)',
     'description': 'REQUIRED for the agent VAT verdict on Opera 3. Post (or reuse) a nominal payment WITH VAT first so an nvat row is pending, then bracket a full VAT Processing calculate+commit (MTD not needed). Key questions: does O3 convert pending nvat into zvtran (SE did: nvat.state 1→2, zvtran N-source row) and does it stamp va_commvat by taxdate-in-period? The 2026-07-08 O3 run built zvtran from sanal/panal but left nvat untouched — unresolved whether that was mechanism or period-scope.'},
    {'module': 'nominal_master', 'name': 'New Nominal Account (Opera 3)',
     'description': 'Step 1 of the bank-account pair: create the nominal account in the NL (nname/nacnt). A cashbook bank REQUIRES this to exist first (Harry, 2026-07-30).'},
    {'module': 'bank_master', 'name': 'New Bank Account (Opera 3)',
     'description': 'Step 2, captured SEPARATELY: the Cashbook setup that designates the nominal as a bank — nbank row, zero balances, cashbook-type wiring, link to the nominal.'},
    # ---- Foreign-currency (FC) captures for Phase-2 FX support (added 2026-07-17).
    # Opera 3 (FoxPro) counterparts of the SE FC list. Enter with the company in
    # MULTI-CURRENCY mode and a FOREIGN-currency customer/supplier/bank
    # (sc_currncy / pc_currncy / nk_fcurr non-blank; home = zxchg.xc_home). The
    # write-agent must reproduce the extra FC columns *_fcurr/*_fcrate/*_fcval/
    # *_fcbal/*_fcvat (+ *_fcmult/*_fcdec) and any exchange gain/loss nominal;
    # capture these to compare Opera 3 vs SE FC behaviour side-by-side.
    {'module': 'fc', 'name': 'FC Sales Receipt (foreign customer)',
     'description': 'Opera 3: customer receipt from a FOREIGN-currency customer, allocated and on-account. Same cashbook/SL/nominal posting as the home Sales Receipt PLUS the FC columns on atran/stran/ntran (*_fcurr/*_fcrate/*_fcval/*_fcbal); zxchg rate; exchange gain/loss nominal if the rate moved. Confirm PENCE-vs-POUNDS conventions still hold in the FC value fields.'},
    {'module': 'fc', 'name': 'FC Purchase Payment (foreign supplier)',
     'description': 'Opera 3: payment to a FOREIGN-currency supplier. Home Purchase-Payment posting PLUS *_fcurr/*_fcrate/*_fcval/*_fcbal on atran/ptran/ntran; exchange gain/loss on rate difference.'},
    {'module': 'fc', 'name': 'FC Nominal Receipt/Payment (foreign bank)',
     'description': 'Opera 3: direct nominal receipt/payment on a FOREIGN-currency bank (nbank.nk_fcurr set). Capture nk_curbal (bank currency) vs the home nominal legs via *_fcrate.'},
    {'module': 'fc', 'name': 'FC Bank Transfer (cross-currency)',
     'description': 'Opera 3: transfer with a FOREIGN-currency leg (or two differing currencies). Both legs\' *_fcurr/*_fcrate/*_fcval + the exchange-difference posting.'},
    {'module': 'fc', 'name': 'FC Recurring Entry',
     'description': 'Opera 3: post a due recurring entry on a foreign-currency account — FC columns on the generated transaction + rate sourcing at post time.'},
    {'module': 'fc', 'name': 'FC Sales Allocation (rate difference)',
     'description': 'Opera 3: allocate an FC receipt against an FC invoice at a DIFFERENT rate — salloc + the exchange gain/loss posting.'},
    {'module': 'fc', 'name': 'FC Purchase Allocation (rate difference)',
     'description': 'Opera 3: allocate an FC payment against an FC invoice at a DIFFERENT rate — palloc + the exchange gain/loss posting.'},
    {'module': 'fc', 'name': 'FC Bank Reconciliation (foreign bank)',
     'description': 'Opera 3: reconcile entries on a FOREIGN-currency bank — whether ae_recbal / nk_recbal are in bank currency or home, plus any FC-specific rec fields.'},
    {'module': 'fc', 'name': 'FC GoCardless Batch (foreign customer)',
     'description': 'Opera 3: GoCardless batch containing a FOREIGN-currency customer receipt — FC receipt columns within the batch + fee/VAT in the foreign currency.'},
    # ---- Added 2026-07-20: FC ORIGINATION + master/period-end captures. The
    # invoice is where the FC value/rate ORIGINATE — receipts and rate-difference
    # allocations reference the invoice's stored rate, so capture these FIRST.
    {'module': 'fc', 'name': 'FC Sales Invoice (foreign customer)',
     'description': 'Opera 3: sales invoice to a FOREIGN-currency customer — the origination point of FC values. Home Sales-Invoice posting PLUS st_fcurr/st_fcrate/st_fcval/st_fcbal on stran (and FC columns on ntran if present); sname FC balance fields; rate sourced from zxchg at posting. Do a CREDIT NOTE too (mirror, opposite signs) if time allows. Capture BEFORE the FC receipt/allocation presets — they reference this invoice\'s stored rate.'},
    {'module': 'fc', 'name': 'FC Purchase Invoice (foreign supplier)',
     'description': 'Opera 3: purchase invoice from a FOREIGN-currency supplier — FC origination on the PL side. Home Purchase-Invoice posting PLUS pt_fcurr/pt_fcrate/pt_fcval/pt_fcbal on ptran; pname FC balance fields; zxchg rate at posting. Credit-note mirror optional.'},
    {'module': 'fc', 'name': 'Exchange Rate Update (zxchg)',
     'description': 'Opera 3: update a currency\'s exchange rate in Opera\'s rate table. Expected delta: zxchg only (rate, date, any multiplier/decimals fields) — no transaction tables. Establishes which zxchg fields the apps must read for rate sourcing, and whether Opera keeps rate history or overwrites in place. (SE equivalent captured 2026-04-07.)'},
    {'module': 'fc', 'name': 'FC Revaluation (period-end)',
     'description': 'Opera 3: run the Foreign Currency Revaluation routine after rates have moved — Opera restates open FC balances (debtors/creditors/FC banks) and posts unrealised exchange gains/losses. Capture which tables carry the revaluation (ntran postings, *_fcbal restatements on stran/ptran/nbank, any revaluation audit rows). Apps must at minimum not corrupt these fields; Phase 2 needs to know the shape even if the apps never run the routine themselves.'},
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
                classification['amount_conventions']['ptran.pt_trvalue'] = f'{trval} (pounds, negative=payment)'

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


@router.get("/api/transaction-snapshot/opera3-smb-defaults")
async def get_opera3_smb_defaults():
    """Defaults for the Opera 3 (FoxPro over SMB) capture fields, so the
    UI can prefill the server IP + share without hard-coding them. Also
    returns the known Opera 3 companies (code + data subpath) for quick
    selection. Never returns credentials."""
    share, default_host, _user, _pw = _opera3_smb_config()
    companies = []
    try:
        from api.main import load_companies
        # Authoritative data path = Opera's own company parameters
        # (System/seqco.dbf co_subdir), resolved live against the SMB mount.
        # The per-company JSON is only a fallback when seqco is unreachable —
        # hardcoded paths go stale when installations move company folders
        # (live incident 2026-07-30: six empty captures after the Z refresh).
        mount_root = None
        try:
            from api.main import get_smb_manager
            smb = get_smb_manager()
            if smb and smb.is_connected():
                mount_root = str(smb.get_local_base())
        except Exception:
            mount_root = None
        from sql_rag.opera3_paths import resolve_company_subdir
        for co in (load_companies() or []):
            if str(co.get('opera_version', '')).strip() == '3':
                code = (co.get('opera3_company_code') or '').strip()
                subpath = None
                if mount_root and code:
                    subpath = resolve_company_subdir(mount_root, code)
                companies.append({
                    'id': co.get('id'),
                    'name': co.get('name'),
                    'code': code,
                    'subpath': subpath or (co.get('opera3_data_path') or 'Data').strip(),
                    'subpath_source': 'seqco' if subpath else 'config-fallback',
                })
    except Exception as e:
        logger.warning(f"opera3-smb-defaults: could not load companies: {e}")
    return {
        "success": True,
        "server_ip": default_host,
        "share": share,
        "default_subpath": "Data",
        "companies": companies,
    }


@router.get("/api/transaction-snapshot/presets")
async def get_presets(
    engine: str = Query("", description="Engine whose checklist to return: 'opera_3' → the Opera 3 write-feature checklist; anything else → the generic Opera SE preset list. 'Already captured' is scoped to this engine, so a preset captured on the other engine still shows here."),
):
    """Get preset transaction types to capture, minus the ones already
    captured FOR THIS ENGINE. Opera 3 gets its own focused write-feature
    checklist; Opera SE gets the full generic list."""
    _e = (engine or '').strip().lower()
    is_opera3 = _e in ('opera_3', 'opera3', '3')
    preset_list = OPERA3_PRESETS if is_opera3 else PRESETS
    target_engine = 'opera_3' if is_opera3 else 'opera_se'

    # Already-captured names — scoped to the requested engine so SE
    # captures don't deplete the Opera 3 checklist (and vice versa).
    captured_names = set()
    for full_path, _filename, subdir_engine in _iter_library_files():
        try:
            with open(full_path) as f:
                entry = json.load(f)
        except Exception:
            continue
        entry_engine = (
            subdir_engine
            or entry.get('engine')
            or ('opera_3' if entry.get('source') == 'opera3' else 'opera_se')
        )
        if entry_engine == target_engine:
            captured_names.add(entry.get('name', '').lower())

    remaining = [p for p in preset_list if p['name'].lower() not in captured_names]

    return {
        "success": True,
        "presets": remaining,
        "total_presets": len(preset_list),
        "captured": len(preset_list) - len(remaining),
    }


@router.get("/api/transaction-snapshot/library")
async def get_library():
    """Get the transaction type library — all recorded posting patterns.
    Scans both engine-specific subfolders (opera_se/, opera_3/) and the
    flat root (for entries written before the 2026-05-12 reorg).
    """
    library = []
    for full_path, filename, subdir_engine in sorted(_iter_library_files(), key=lambda x: x[1]):
        try:
            with open(full_path) as f:
                entry = json.load(f)
                # Authoritative engine: the subfolder the file lives in
                # wins; else the stored engine tag; else map legacy
                # `source` ('opera3' → opera_3, anything else → opera_se).
                engine = (
                    subdir_engine
                    or entry.get('engine')
                    or ('opera_3' if entry.get('source') == 'opera3' else 'opera_se')
                )
                library.append({
                    'id': filename.replace('.json', ''),
                    'module': entry.get('module', 'other'),
                    'module_name': MODULES.get(entry.get('module', 'other'), 'Other'),
                    'name': entry.get('name', ''),
                    'description': entry.get('description', ''),
                    'recorded_at': entry.get('recorded_at', ''),
                    'tables_changed': entry.get('tables_changed', 0),
                    'engine': engine,
                    'source': entry.get('source', 'opera_se'),
                })
        except Exception as e:
            logger.warning(f"Could not load library entry {filename}: {e}")

    return {"success": True, "library": library}


@router.get("/api/transaction-snapshot/compare")
async def compare_engines(stem: str):
    """Compare the most recent SE and Opera 3 captures of a transaction.

    `stem` is the filename prefix shared across both engines'
    captures, e.g. `sales_ledger_invoice`. Returns a structured diff
    that pairs tables by canonical name (stripping Opera 3 single-
    letter company prefixes) and aligns fields by canonical name
    (lowercased).
    """
    import re
    _PREFIX_RX = re.compile(r'^[a-z]_')

    def canonical_table(name: str) -> str:
        return _PREFIX_RX.sub('', name.lower())

    def latest_entry(engine_subdir: str):
        from glob import glob
        folder = os.path.join(_get_library_path(), engine_subdir)
        if not os.path.isdir(folder):
            return None, None
        matches = sorted(
            [p for p in glob(os.path.join(folder, '*.json'))
             if os.path.basename(p).startswith(stem)],
            key=os.path.getmtime, reverse=True,
        )
        if not matches:
            return None, None
        with open(matches[0]) as f:
            return os.path.basename(matches[0]).replace('.json', ''), json.load(f)

    def index_changes(entry):
        out = {}
        for c in (entry.get('changes') or []):
            ct = canonical_table(c.get('table', ''))
            bucket = out.setdefault(ct, {
                'orig_names': set(),
                'rows_added': 0, 'rows_modified': 0, 'rows_deleted': 0,
                'fields_modified': set(),
            })
            bucket['orig_names'].add(c.get('table', ''))
            bucket['rows_added'] += c.get('rows_added', 0) or 0
            bucket['rows_modified'] += c.get('rows_modified', 0) or 0
            bucket['rows_deleted'] += c.get('rows_deleted', 0) or 0
            for f in (c.get('fields_modified') or []):
                bucket['fields_modified'].add(f.lower())
        return out

    se_id, se = latest_entry('opera_se')
    o3_id, o3 = latest_entry('opera_3')

    if not (se and o3):
        return {
            "success": False,
            "stem": stem,
            "se_entry_id": se_id,
            "o3_entry_id": o3_id,
            "error": (
                f"Need captures on both engines to compare. "
                f"Have SE: {'yes' if se else 'no'}, Opera 3: {'yes' if o3 else 'no'}."
            ),
        }

    se_idx = index_changes(se)
    o3_idx = index_changes(o3)
    se_tables = set(se_idx); o3_tables = set(o3_idx)

    tables = []
    for ct in sorted(se_tables | o3_tables):
        s = se_idx.get(ct); o = o3_idx.get(ct)
        row = {'canonical': ct, 'in_se': bool(s), 'in_o3': bool(o)}
        if s:
            row.update({
                'se_orig_names': sorted(s['orig_names']),
                'se_rows': {'added': s['rows_added'], 'modified': s['rows_modified'], 'deleted': s['rows_deleted']},
                'se_fields_modified': sorted(s['fields_modified']),
            })
        if o:
            row.update({
                'o3_orig_names': sorted(o['orig_names']),
                'o3_rows': {'added': o['rows_added'], 'modified': o['rows_modified'], 'deleted': o['rows_deleted']},
                'o3_fields_modified': sorted(o['fields_modified']),
            })
        if s and o:
            s_f = s['fields_modified']; o_f = o['fields_modified']
            row['fields_both'] = sorted(s_f & o_f)
            row['fields_se_only'] = sorted(s_f - o_f)
            row['fields_o3_only'] = sorted(o_f - s_f)
        tables.append(row)

    return {
        "success": True,
        "stem": stem,
        "se_entry_id": se_id,
        "o3_entry_id": o3_id,
        "summary": {
            "se_tables_count": len(se_tables),
            "o3_tables_count": len(o3_tables),
            "tables_in_both": len(se_tables & o3_tables),
            "tables_se_only": len(se_tables - o3_tables),
            "tables_o3_only": len(o3_tables - se_tables),
        },
        "tables": tables,
    }


@router.get("/api/transaction-snapshot/library/{entry_id}")
async def get_library_entry(entry_id: str):
    """Get a specific transaction type entry with full diff details."""
    filepath = _find_library_entry(entry_id)
    if not filepath:
        raise HTTPException(status_code=404, detail="Library entry not found")

    with open(filepath) as f:
        entry = json.load(f)

    return {"success": True, "entry": entry}


def _get_request_sql_connector(request=None):
    """Get SQL connector for the current request's session context."""
    try:
        from apps.core.adapters.factory import get_opera_sql
        sql_connector = get_opera_sql()
        from api.main import _company_sql_connectors, active_system_id
        # Try to get session context from request
        if request:
            system_id = getattr(request.state, 'session_system_id', None) or active_system_id
            company_id = None
            try:
                from api.main import _request_company_id
                company_id = _request_company_id.get(None)
            except Exception:
                pass
            if not company_id:
                from api.main import _get_active_company_id
                company_id = _get_active_company_id()

            # Try system-scoped connector
            if system_id and company_id:
                key = f"{system_id}_{company_id}"
                if key in _company_sql_connectors:
                    return _company_sql_connectors[key]
            # Try company connector
            if company_id and company_id in _company_sql_connectors:
                return _company_sql_connectors[company_id]
        return sql_connector
    except Exception:
        return _get_sql_connector()


@router.post("/api/transaction-snapshot/before")
async def take_before_snapshot(
    request: Request,
    module: str = Query(..., description="Module category (cashbook, sales_ledger, etc.)"),
    name: str = Query(..., description="Transaction type name (e.g., 'Sales Receipt — BACS')"),
    description: str = Query("", description="Detailed description of the transaction being entered"),
    data_path: str = Query("", description="For Opera 3: with server_ip set, this is the folder WITHIN the SMB share (e.g. 'Data' or 'Data/P'). Without server_ip, a non-empty value is a local FoxPro DBF folder path. Empty (and no server_ip) → read via SQL against the active company."),
    file_filter: str = Query("", description="Company-code prefix / glob applied to the company data folder only (System folder always fully scanned). e.g. `Z` (expands to `Z_*`) captures only Company Z's tables."),
    server_ip: str = Query("", description="Opera 3 FoxPro server IP. When set, the snapshot connects to that server over SMB (no OS mount), pulls the selected company's DBFs, and reads them. Share name + credentials come from config.ini [opera]."),
    engine: str = Query("", description="Logical engine this capture is filed under: 'opera_se' or 'opera_3'. This is the deliberate declaration made by the menu the user entered — it is INDEPENDENT of how the data is physically read (SQL vs FoxPro). Determines the library subfolder and the entry tag. Empty → inferred (back-compat)."),
):
    """
    Take a BEFORE snapshot of all Opera tables.
    Call this, then enter the transaction in Opera, then call /after.

    Two independent concepts:
    - `engine` (opera_se | opera_3) — the LOGICAL engine the capture is
      filed under. Chosen by the user (which menu they're on).
    - read mechanism — HOW the data is read: FoxPro when `data_path` is
      supplied, otherwise SQL against the active company. Opera 3 can be
      read either way (SQL-SE company or FoxPro DBFs); Opera SE is always
      SQL.
    """
    explicit_opera3_path = (data_path or '').strip()
    smb_server = (server_ip or '').strip()

    # Logical engine — the label this capture is filed under. Driven by
    # the explicit `engine` param (the menu the user chose). Empty →
    # inferred from the read mechanism for older callers.
    _e = (engine or '').strip().lower()
    if _e in ('opera_3', 'opera3', '3'):
        logical_engine = 'opera_3'
    elif _e in ('opera_se', 'sql_se', 'se'):
        logical_engine = 'opera_se'
    else:
        logical_engine = ''  # infer after read-mechanism resolution

    # Opera SE is always SQL — a data_path or server_ip is contradictory.
    if logical_engine == 'opera_se' and (explicit_opera3_path or smb_server):
        raise HTTPException(
            status_code=400,
            detail=("engine='opera_se' cannot be combined with a data_path/server_ip — "
                    "Opera SE is always read via SQL. Use engine='opera_3' to "
                    "snapshot FoxPro DBF files."),
        )

    # Read mechanism (three ways to read Opera 3):
    #   server_ip set        → 'opera3_smb'  (connect over SMB by IP)
    #   local data_path set  → 'opera3'      (local/mounted DBF folder)
    #   otherwise            → config version ('sql_se' → SQL)
    opera_version = 'opera_se'
    try:
        import configparser
        cfg = configparser.ConfigParser()
        cfg.read(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'config.ini'))
        if not explicit_opera3_path and not smb_server:
            opera_version = cfg.get('opera', 'version', fallback='sql_se')
    except Exception:
        pass
    if smb_server:
        opera_version = 'opera3_smb'
    elif explicit_opera3_path:
        opera_version = 'opera3'

    # Infer the logical engine when the caller didn't declare one.
    if not logical_engine:
        logical_engine = 'opera_3' if opera_version in ('opera3', 'opera3_smb') else 'opera_se'

    try:
        if opera_version == 'opera3_smb':
            # Opera 3 over SMB — connect to the server by IP, pull the
            # company's DBFs (+ System) and snapshot them. data_path is the
            # folder within the share; file_filter is the company code.
            snapshot = take_snapshot_opera3_smb(smb_server, subpath=explicit_opera3_path, file_filter=file_filter)
            snapshot['_resolved_data_path'] = f"smb://{smb_server}/{explicit_opera3_path}".rstrip('/')
            snapshot['_file_filter'] = (file_filter or '').strip()
            logger.info(
                f"Taking BEFORE snapshot (Opera 3 SMB) for: {module}/{name} "
                f"— server={smb_server} path='{explicit_opera3_path}' filter='{file_filter}'"
            )
        elif opera_version == 'opera3':
            # Opera 3 — snapshot DBF files
            try:
                resolved_path = explicit_opera3_path
                if not resolved_path:
                    # No explicit path — fall back to SMB-managed mount
                    # behaviour (existing path before this endpoint was
                    # parameterised).
                    from sql_rag.smb_access import get_smb_manager
                    smb = get_smb_manager()
                    if smb and smb.is_connected():
                        resolved_path = str(smb.get_local_base())
                        try:
                            base_path = cfg.get('opera', 'opera3_base_path', fallback='')
                            if base_path:
                                resolved_path = base_path
                        except Exception:
                            pass
                    else:
                        raise HTTPException(status_code=503, detail="Opera 3 SMB connection not available (and no explicit data_path supplied)")
                filter_pattern = (file_filter or '').strip()
                logger.info(f"Taking BEFORE snapshot (Opera 3) for: {module}/{name} at {resolved_path} (source={'explicit' if explicit_opera3_path else 'smb-managed'}, file_filter={filter_pattern or '<none>'})")
                snapshot = take_snapshot_opera3(resolved_path, file_filter=filter_pattern)
                # Stash the resolved path + filter so /after can reuse
                # them without needing the caller to re-supply them.
                snapshot['_resolved_data_path'] = resolved_path
                snapshot['_file_filter'] = filter_pattern
            except ImportError:
                raise HTTPException(status_code=503, detail="SMB access module not available")
        else:
            # Opera SE — snapshot via SQL.
            # The contract is simple: scan the company the user is
            # currently logged into. We resolve the SQL Server database
            # name from companies/<id>.json at the route boundary, then
            # pass it EXPLICITLY into take_snapshot_se. No `DB_NAME()`
            # indirection, no reliance on connection-pool state — the
            # backend asserts which DB to scan rather than asking the
            # connection what it happens to be bound to.
            from api.main import _get_active_company_id, load_company
            company_id = _get_active_company_id()
            if not company_id:
                raise HTTPException(
                    status_code=400,
                    detail="No active Opera company in session. Select a company before taking a snapshot.",
                )
            co_data = load_company(company_id)
            if not co_data or not co_data.get('database'):
                raise HTTPException(
                    status_code=500,
                    detail=(
                        f"Active company '{company_id}' has no `database` field in "
                        f"companies/{company_id}.json — cannot determine which SQL "
                        f"Server database to scan."
                    ),
                )
            company_db = co_data['database']
            sql = _get_request_sql_connector(request)
            if not sql:
                raise HTTPException(status_code=503, detail="No database connection")
            logger.info(
                f"Taking BEFORE snapshot (Opera SE) for: {module}/{name} "
                f"— active company='{company_id}' → database='{company_db}'"
            )
            snapshot = take_snapshot_se(sql, company_db=company_db)
            snapshot['_active_company_id'] = company_id
            # Surface table counts per database so a "zero scanned"
            # outcome is visible in the log, not only in the response.
            for _db, _tables in snapshot.get('databases', {}).items():
                logger.info(
                    f"Snapshot result: db='{_db}' tables_scanned={len(_tables)}"
                )

        # Save snapshot to temp file
        snap_path = _get_snapshot_path()
        snap_file = os.path.join(snap_path, 'current_before.json')
        meta_file = os.path.join(snap_path, 'current_meta.json')

        # Stamp the logical engine (the menu the user chose) so it flows
        # to /after and the library subfolder — decoupled from `source`
        # (the physical read mechanism).
        snapshot['engine'] = logical_engine

        with open(snap_file, 'w') as f:
            json.dump(snapshot, f)

        meta = {
            'module': module,
            'name': name,
            'description': description,
            'before_timestamp': snapshot['timestamp'],
            'engine': logical_engine,
            'source': snapshot.get('source', 'opera_se'),
            # Persist the resolved Opera 3 path + file filter so /after
            # uses the same ones without needing the caller to re-supply.
            'opera3_data_path': snapshot.get('_resolved_data_path', '') if snapshot.get('source') == 'opera3' else '',
            'opera3_file_filter': snapshot.get('_file_filter', '') if snapshot.get('source') == 'opera3' else '',
            # SMB-native read details (present only for Opera 3 over SMB),
            # so /after connects to the same server/path/company.
            'opera3_smb_server': snapshot.get('_smb_server', ''),
            'opera3_smb_subpath': snapshot.get('_smb_subpath', ''),
        }
        with open(meta_file, 'w') as f:
            json.dump(meta, f)

        total_tables = sum(len(db) for db in snapshot.get('databases', {}).values())

        # Opera 3 only — expose the per-folder filter result so the UI
        # can warn if the company filter matched zero tables (the most
        # common cause of "no changes detected" later).
        tables_per_folder = snapshot.get('tables_per_folder') if snapshot.get('source') == 'opera3' else None
        effective_filter = snapshot.get('file_filter') if snapshot.get('source') == 'opera3' else None

        warning = None
        if tables_per_folder:
            company_match = tables_per_folder.get('company', {}).get('matched', 0)
            company_total = tables_per_folder.get('company', {}).get('available_in_folder', 0)
            if effective_filter and company_match == 0 and company_total > 0:
                warning = (
                    f"Filter '{effective_filter}' matched 0 of {company_total} DBF files in the company folder. "
                    "The AFTER snapshot will also be empty — no changes can be detected. "
                    "Check the prefix you entered against what's actually in the folder."
                )

        return {
            "success": True,
            "message": f"Before snapshot captured — {total_tables} tables across {len(snapshot.get('databases', {}))} database(s)",
            "tables_scanned": total_tables,
            "databases": list(snapshot.get('databases', {}).keys()),
            "tables_per_folder": tables_per_folder,
            "effective_filter": effective_filter,
            "warning": warning,
            "timestamp": snapshot['timestamp'],
        }
    except HTTPException:
        raise  # preserve intended status/message (e.g. 409 locked, 413 too large)
    except Exception as e:
        logger.error(f"Snapshot failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/transaction-snapshot/after")
async def take_after_snapshot(request: Request):
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

        smb_server = (meta.get('opera3_smb_server') or '').strip()
        if source == 'opera3' and smb_server:
            # Opera 3 over SMB — reconnect to the same server/path/company
            # that /before used and pull the DBFs again.
            smb_subpath = (meta.get('opera3_smb_subpath') or '').strip()
            filter_pattern = (meta.get('opera3_file_filter') or '').strip()
            logger.info(
                f"Taking AFTER snapshot (Opera 3 SMB) — server={smb_server} "
                f"path='{smb_subpath}' filter='{filter_pattern or '<none>'}'"
            )
            after = take_snapshot_opera3_smb(smb_server, subpath=smb_subpath, file_filter=filter_pattern)
        elif source == 'opera3':
            # Opera 3 — snapshot DBF files. Prefer the resolved path
            # that /before recorded (so the AFTER snapshot definitely
            # reads from the same place); fall back to SMB-managed
            # mount or config.ini if meta wasn't tagged (older snaps).
            try:
                resolved_path = (meta.get('opera3_data_path') or '').strip()
                if not resolved_path:
                    import configparser
                    cfg = configparser.ConfigParser()
                    cfg.read(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'config.ini'))
                    from sql_rag.smb_access import get_smb_manager
                    smb = get_smb_manager()
                    if smb and smb.is_connected():
                        resolved_path = cfg.get('opera', 'opera3_base_path', fallback=str(smb.get_local_base()))
                    else:
                        raise HTTPException(status_code=503, detail="Opera 3 SMB connection not available")
                filter_pattern = (meta.get('opera3_file_filter') or '').strip()
                logger.info(f"Taking AFTER snapshot (Opera 3) at {resolved_path} (file_filter={filter_pattern or '<none>'})")
                after = take_snapshot_opera3(resolved_path, file_filter=filter_pattern)
            except ImportError:
                raise HTTPException(status_code=503, detail="SMB access module not available")
        else:
            # Opera SE — same contract as /before: resolve the active
            # company's database from companies/<id>.json and scan it
            # explicitly. Also enforce that AFTER scans the same DB as
            # BEFORE (the meta file persists the BEFORE company_db),
            # otherwise the diff would be comparing apples to oranges.
            from api.main import _get_active_company_id, load_company
            company_id = _get_active_company_id()
            if not company_id:
                raise HTTPException(
                    status_code=400,
                    detail="No active Opera company in session.",
                )
            co_data = load_company(company_id)
            if not co_data or not co_data.get('database'):
                raise HTTPException(
                    status_code=500,
                    detail=(
                        f"Active company '{company_id}' has no `database` field — "
                        f"cannot determine which SQL Server database to scan."
                    ),
                )
            company_db = co_data['database']
            before_company_db = before.get('company_db') if isinstance(before, dict) else None
            if before_company_db and before_company_db != company_db:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"AFTER company mismatch: BEFORE was captured against "
                        f"'{before_company_db}' but the active company is now "
                        f"'{company_db}'. Switch back to the same company before "
                        f"taking AFTER, or restart the snapshot with BEFORE."
                    ),
                )
            sql = _get_request_sql_connector(request)
            if not sql:
                raise HTTPException(status_code=503, detail="No database connection")
            logger.info(
                f"Taking AFTER snapshot (Opera SE) "
                f"— active company='{company_id}' → database='{company_db}'"
            )
            after = take_snapshot_se(sql, company_db=company_db)
            after['_active_company_id'] = company_id
            for _db, _tables in after.get('databases', {}).items():
                logger.info(
                    f"AFTER snapshot result: db='{_db}' tables_scanned={len(_tables)}"
                )

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
            'engine': meta.get('engine') or ('opera_3' if meta.get('source') == 'opera3' else 'opera_se'),
            'source': meta.get('source', 'opera_se'),
            'recorded_at': datetime.now().isoformat(),
            'before_timestamp': meta['before_timestamp'],
            'after_timestamp': after['timestamp'],
            'tables_checked': diff['tables_checked'],
            'tables_changed': diff['tables_changed'],
            'classification': classification,
            'changes': diff['changes'],
        }

        # Save to library — into the engine-specific subfolder
        # (opera_se/ or opera_3/) so SE and Opera 3 traces stay
        # separated and can be diffed against each other directly. The
        # subfolder follows the LOGICAL engine the user declared, NOT the
        # physical read mechanism — so an Opera-3-over-SQL capture lands
        # in opera_3/, not opera_se/. Falls back to the legacy flat
        # layout if the subfolders don't exist yet on a fresh install.
        lib_path = _get_library_path()
        safe_name = meta['name'].lower().replace(' ', '_').replace('—', '-')
        safe_name = ''.join(c for c in safe_name if c.isalnum() or c in '-_')[:50]
        entry_id = f"{meta['module']}_{safe_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        source_dir_name = entry['engine'] if entry.get('engine') in ('opera_se', 'opera_3') else 'opera_se'
        source_dir = os.path.join(lib_path, source_dir_name)
        if not os.path.isdir(source_dir):
            # Fall back to flat layout if the engine-specific subfolder
            # hasn't been created yet (e.g. fresh install before the
            # 2026-05-12 reorg). Once present, future writes use it.
            source_dir = lib_path
        entry_file = os.path.join(source_dir, f"{entry_id}.json")

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
    except HTTPException:
        raise  # preserve intended status/message (e.g. 409 locked file)
    except Exception as e:
        logger.error(f"After snapshot failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/transaction-snapshot/library/{entry_id}")
async def delete_library_entry(entry_id: str):
    """Delete a transaction library entry."""
    filepath = _find_library_entry(entry_id)
    if not filepath:
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


# ============================================================================
# Field Analysis — Identifies mandatory/optional/unused fields per table
# ============================================================================

def _is_field_populated(value) -> bool:
    """Return True if the value counts as populated (not null, not empty, not zero)."""
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == '':
        return False
    if isinstance(value, (int, float)) and value == 0:
        return False
    return True


def _load_library_entries(module: str = None, transaction_type: str = None) -> List[Dict]:
    """Load all library entries, optionally filtered by module and/or transaction_type."""
    lib_path = _get_library_path()
    entries = []
    if not os.path.exists(lib_path):
        return entries
    for filename in sorted(os.listdir(lib_path)):
        if not filename.endswith('.json'):
            continue
        try:
            with open(os.path.join(lib_path, filename)) as f:
                entry = json.load(f)
            if module and entry.get('module', '') != module:
                continue
            if transaction_type and entry.get('name', '').lower() != transaction_type.lower():
                continue
            entries.append(entry)
        except Exception as e:
            logger.warning(f"Could not load library entry {filename}: {e}")
    return entries


def _analyse_fields_for_table(table_name: str, entries: List[Dict]) -> Dict[str, Any]:
    """
    For a given table, collect all rows that were added across library entries
    and analyse field population rates.

    Returns a dict with 'entries_analysed', 'total_rows', and 'fields' list.
    """
    table_lower = table_name.lower()

    # Gather all added rows for this table across all entries
    all_rows: List[Dict] = []
    entries_with_table = 0

    for entry in entries:
        found_in_entry = False
        for change in entry.get('changes', []):
            if change.get('table', '').lower() == table_lower:
                added = change.get('added_rows', [])
                if added:
                    all_rows.extend(added)
                    found_in_entry = True
        if found_in_entry:
            entries_with_table += 1

    if not all_rows:
        return {
            'table': table_name,
            'entries_analysed': entries_with_table,
            'total_rows': 0,
            'fields': [],
        }

    # Collect all field names across all rows
    all_field_names: List[str] = []
    seen_fields = set()
    for row in all_rows:
        for field in row.keys():
            if field not in seen_fields:
                seen_fields.add(field)
                all_field_names.append(field)

    total_rows = len(all_rows)

    fields_result = []
    for field in all_field_names:
        populated_count = 0
        sample_values: List = []
        seen_sample_values = set()
        unique_values = set()

        for row in all_rows:
            if field not in row:
                continue
            val = row[field]
            if _is_field_populated(val):
                populated_count += 1
            # Collect sample values (up to 5 distinct)
            val_key = str(val) if val is not None else '__none__'
            unique_values.add(val_key)
            if len(sample_values) < 5 and val_key not in seen_sample_values:
                seen_sample_values.add(val_key)
                sample_values.append(val)

        # Classify
        if populated_count == total_rows:
            if len(unique_values) == 1:
                classification = 'constant'
            else:
                classification = 'always'
        elif populated_count == 0:
            classification = 'never'
        else:
            classification = 'sometimes'

        fields_result.append({
            'name': field,
            'populated_count': populated_count,
            'total_count': total_rows,
            'classification': classification,
            'sample_values': sample_values,
        })

    # Sort: always/constant first, then sometimes, then never; within group alphabetical
    order = {'always': 0, 'constant': 1, 'sometimes': 2, 'never': 3}
    fields_result.sort(key=lambda f: (order.get(f['classification'], 9), f['name']))

    return {
        'table': table_name,
        'entries_analysed': entries_with_table,
        'total_rows': total_rows,
        'fields': fields_result,
    }


@router.get("/api/transaction-snapshot/field-analysis")
async def analyse_fields(
    module: str = Query(None, description="Filter by module (e.g. cashbook, sales_ledger)"),
    transaction_type: str = Query(None, description="Filter by transaction type name"),
):
    """
    Analyse field population rates across all library entries (or a filtered subset).

    For each table that appears in the library, identifies which fields are:
    - always: non-null/non-empty/non-zero in every row — effectively mandatory
    - constant: always populated AND always the same value — likely a default
    - sometimes: populated in some rows but not all — optional
    - never: always null/empty/zero — likely unused
    """
    entries = _load_library_entries(module=module, transaction_type=transaction_type)

    if not entries:
        return {
            'success': True,
            'entries_analysed': 0,
            'filters': {'module': module, 'transaction_type': transaction_type},
            'tables': [],
            'message': 'No library entries found matching the filters.',
        }

    # Collect all table names that appear across entries
    table_names: List[str] = []
    seen_tables = set()
    for entry in entries:
        for change in entry.get('changes', []):
            tname = change.get('table', '')
            if tname and tname.lower() not in seen_tables:
                seen_tables.add(tname.lower())
                table_names.append(tname)

    # Analyse each table
    tables_analysis = []
    for table_name in sorted(table_names):
        analysis = _analyse_fields_for_table(table_name, entries)
        if analysis['total_rows'] > 0:
            tables_analysis.append(analysis)

    return {
        'success': True,
        'entries_analysed': len(entries),
        'filters': {'module': module, 'transaction_type': transaction_type},
        'tables': tables_analysis,
    }


@router.get("/api/transaction-snapshot/field-analysis/{table_name}")
async def analyse_table_fields(
    table_name: str,
    module: str = Query(None, description="Filter by module"),
    transaction_type: str = Query(None, description="Filter by transaction type name"),
):
    """
    Detailed field analysis for a single table across all library entries where
    that table was modified (rows added).

    Returns field-by-field population rates, sample values, and cross-table
    relationship hints (which other table.field each field may reference).
    """
    # Load filtered entries for field analysis, but all entries for relationship detection
    entries = _load_library_entries(module=module, transaction_type=transaction_type)
    all_entries = _load_library_entries()  # Full set for relationship detection

    analysis = _analyse_fields_for_table(table_name, entries)

    # Build relationship map for this specific table using all entries
    all_field_values = _collect_field_values(all_entries)
    all_relationships = _detect_relationships(all_field_values)

    # Index relationships by source_table.source_field
    table_lower = table_name.lower()
    rel_by_field: Dict[str, List[Dict]] = {}
    for rel in all_relationships:
        if rel['source_table'] == table_lower:
            key = rel['source_field']
            if key not in rel_by_field:
                rel_by_field[key] = []
            rel_by_field[key].append({
                'target_table': rel['target_table'],
                'target_field': rel['target_field'],
                'confidence': rel['confidence'],
                'reason': rel['reason'],
            })

    # Attach relationship hints to each field
    fields_with_rels = []
    for field_info in analysis['fields']:
        enriched = dict(field_info)
        enriched['relationships'] = rel_by_field.get(field_info['name'], [])
        fields_with_rels.append(enriched)

    return {
        'success': True,
        'table': analysis['table'],
        'entries_analysed': analysis['entries_analysed'],
        'total_rows': analysis['total_rows'],
        'filters': {'module': module, 'transaction_type': transaction_type},
        'fields': fields_with_rels,
    }


# ============================================================================
# Cross-Table Relationship Analysis
# ============================================================================

# Fields that are metadata/system and should never be used for relationship detection
_RELATIONSHIP_IGNORE_FIELDS = frozenset({
    'id', 'datecreated', 'datemodified', 'state', 'sq_user', 'sq_date', 'sq_time',
})


def _is_boolean_field(values: set) -> bool:
    """Return True if all values in the set are 0/0.0 or 1/1.0 (boolean flags)."""
    for v in values:
        try:
            f = float(v)
            if f not in (0.0, 1.0):
                return False
        except (TypeError, ValueError):
            return False
    return True


def _looks_like_reference_field(values: set) -> bool:
    """
    Return True if the values look like account codes, type codes, or references
    (short strings, not long free-text or pure large numbers).
    """
    for v in values:
        s = str(v).strip()
        if not s or s == 'None':
            continue
        # Pure numeric — only keep if it's a short integer (could be a code)
        try:
            f = float(s)
            # Long floating-point numbers (large amounts) are not references
            if abs(f) > 9999999 or (s.count('.') and len(s.split('.')[1]) > 2):
                return False
        except ValueError:
            pass
        # Strings longer than 50 chars are free-text, not codes
        if len(s) > 50:
            return False
    return True


def _collect_field_values(entries: List[Dict]) -> Dict[str, Dict[str, set]]:
    """
    Walk every library entry and collect, per table per field, the complete set
    of non-null/non-empty values seen in added_rows and modified_rows.after.

    Returns: { "table_name": { "field_name": {value1, value2, ...} } }
    """
    field_values: Dict[str, Dict[str, set]] = {}

    for entry in entries:
        for change in entry.get('changes', []):
            table = change.get('table', '').lower()
            if not table:
                continue
            if table not in field_values:
                field_values[table] = {}

            def _record_value(field: str, val):
                if val is None:
                    return
                s = str(val).strip()
                if s == '' or s == 'None':
                    return
                if field not in field_values[table]:
                    field_values[table][field] = set()
                field_values[table][field].add(s)

            # Collect from added_rows
            for row in change.get('added_rows', []):
                for field, val in row.items():
                    _record_value(field, val)

            # Collect from modified_rows — use 'after' values
            for mod_row in change.get('modified_rows', []):
                for field, change_vals in mod_row.get('changes', {}).items():
                    after_val = change_vals.get('after') if isinstance(change_vals, dict) else None
                    _record_value(field, after_val)

    return field_values


def _detect_relationships(field_values: Dict[str, Dict[str, set]]) -> List[Dict[str, Any]]:
    """
    For each (source_table, source_field), check if its values exist in any
    (target_table, target_field). Returns a list of relationship dicts.

    Rules:
    - Only consider fields with 3+ distinct values (avoid single-value false positives)
    - Ignore metadata fields in _RELATIONSHIP_IGNORE_FIELDS
    - Ignore boolean-flag fields (only 0/1 values)
    - Only consider fields that look like reference/code fields (not long free-text)
    - Skip self-matches (same table.field)
    - High confidence: 100% of source values in target
    - Medium confidence: 80%+ of source values in target
    """
    relationships = []

    tables = sorted(field_values.keys())

    for src_table in tables:
        src_fields = field_values[src_table]

        for src_field, src_vals in src_fields.items():
            # Apply ignore rules
            if src_field.lower() in _RELATIONSHIP_IGNORE_FIELDS:
                continue
            if len(src_vals) < 3:
                continue
            if _is_boolean_field(src_vals):
                continue
            if not _looks_like_reference_field(src_vals):
                continue

            for tgt_table in tables:
                tgt_fields = field_values[tgt_table]

                for tgt_field, tgt_vals in tgt_fields.items():
                    # Skip same field
                    if src_table == tgt_table and src_field == tgt_field:
                        continue
                    if tgt_field.lower() in _RELATIONSHIP_IGNORE_FIELDS:
                        continue
                    if not tgt_vals:
                        continue

                    # Count how many source values exist in the target set
                    matched = sum(1 for v in src_vals if v in tgt_vals)
                    ratio = matched / len(src_vals)

                    if ratio < 0.8:
                        continue

                    confidence = 'high' if ratio == 1.0 else 'medium'
                    pct = int(ratio * 100)
                    reason = (
                        f"All {len(src_vals)} values in {src_table}.{src_field} exist in {tgt_table}.{tgt_field}"
                        if ratio == 1.0
                        else f"{matched}/{len(src_vals)} ({pct}%) values in {src_table}.{src_field} exist in {tgt_table}.{tgt_field}"
                    )

                    sample = sorted(src_vals)[:5]

                    relationships.append({
                        'source_table': src_table,
                        'source_field': src_field,
                        'target_table': tgt_table,
                        'target_field': tgt_field,
                        'confidence': confidence,
                        'match_ratio': ratio,
                        'source_distinct_values': len(src_vals),
                        'reason': reason,
                        'sample_values': sample,
                    })

    # Sort: high confidence first, then by source table/field
    relationships.sort(key=lambda r: (0 if r['confidence'] == 'high' else 1, r['source_table'], r['source_field']))

    return relationships


@router.get("/api/transaction-snapshot/relationship-analysis")
async def analyse_relationships():
    """
    Cross-table relationship detection across all library entries.

    Reads every library entry and collects all values seen for each table.field
    (from added_rows and modified_rows.after). Then checks if all (or 80%+) of
    one field's values also appear in another table's field — which suggests a
    foreign-key or validation relationship in Opera.

    Only considers fields with 3+ distinct values. Ignores metadata fields,
    boolean flags, and long free-text strings.
    """
    entries = _load_library_entries()

    if not entries:
        return {
            'success': True,
            'relationships': [],
            'tables_analysed': 0,
            'entries_analysed': 0,
            'message': 'No library entries found.',
        }

    field_values = _collect_field_values(entries)
    relationships = _detect_relationships(field_values)

    return {
        'success': True,
        'relationships': relationships,
        'tables_analysed': len(field_values),
        'entries_analysed': len(entries),
        'high_confidence_count': sum(1 for r in relationships if r['confidence'] == 'high'),
        'medium_confidence_count': sum(1 for r in relationships if r['confidence'] == 'medium'),
    }


@router.post("/api/transaction-snapshot/export-to-knowledge")
async def export_to_knowledge(entry_id: str = Query(...)):
    """
    Export a transaction library entry to the Opera knowledge base
    in a format suitable for the knowledge base markdown.
    """
    filepath = _find_library_entry(entry_id)
    if not filepath:
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
