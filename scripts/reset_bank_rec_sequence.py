#!/usr/bin/env python3
"""
Reset (or set) the rec sequence counters on an Opera SQL SE nbank row.

Use cases:
  - After running scripts/reverse_bank_rec_batch.py, the bank's
    nk_lstrecl/nk_reclnum are 0 — too low for the next rec
    (ae_reclnum=0 is Opera's "unreconciled" sentinel). Use this script
    to set them to the correct next batch number.
  - To repair historical contamination where Path A/B writes left
    nk_lstrecl out of sync with nk_reclnum.

By default the script computes the correct next batch number as
MAX(ae_reclnum on this bank) + 1. You can override with --next-batch.

Safety:
  - Read-only sniff first (dry-run by default)
  - Backup of pre-state JSON to data/_audit/bank_rec_sequence_resets/
  - Single ROWLOCK UPDATE inside a transaction
  - Refuses to clobber a bank where nk_lstrecl already equals nk_reclnum
    AND that value is plausible (>= computed next batch) unless --force

Usage:
    python3 scripts/reset_bank_rec_sequence.py --db Opera3SECompany00C \
        --bank BB005 --next-batch 208 --apply
"""

from __future__ import annotations

import argparse
import configparser
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pyodbc


CONFIG_PATH = "/Users/maccb/llmragsql/config.ini"
AUDIT_DIR = Path("/Users/maccb/llmragsql/data/_audit/bank_rec_sequence_resets")


def open_conn(db_name: str) -> pyodbc.Connection:
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_PATH)
    db = cfg["database"]
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={db['server']},{db['port']};"
        f"DATABASE={db_name};"
        f"UID={db['username']};PWD={db['password']};"
        "TrustServerCertificate=yes;Encrypt=yes;",
        timeout=10,
        autocommit=False,
    )


def _coerce(v: Any) -> Any:
    from decimal import Decimal
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def write_backup(payload: dict, db: str, bank: str) -> Path:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    path = AUDIT_DIR / f"{ts}_{db}_{bank}_pre_reset.json"
    path.write_text(json.dumps(payload, indent=2, default=str))
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--bank", required=True)
    ap.add_argument("--next-batch", type=int, default=None,
                    help="Explicit value for nk_lstrecl and nk_reclnum. "
                         "If omitted, computed as MAX(ae_reclnum) + 1.")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="Write even if counters look already-aligned.")
    args = ap.parse_args()

    print(f"DB    : {args.db}")
    print(f"Bank  : {args.bank}")
    print(f"Apply : {args.apply}")

    conn = open_conn(args.db)
    cur = conn.cursor()

    # Pre-state
    cur.execute("""
        SELECT nk_acnt, nk_desc, nk_curbal, nk_recbal,
               nk_reclnum, nk_recldte, nk_lstrecl, nk_lststno, nk_lststdt
        FROM nbank WITH (NOLOCK) WHERE nk_acnt = ?
    """, args.bank)
    row = cur.fetchone()
    if not row:
        print(f"Bank {args.bank} not found in nbank — exiting.")
        return 1
    cols = [d[0] for d in cur.description]
    nbank_pre = {c: _coerce(v) for c, v in zip(cols, row)}

    print(f"\nPre-state nbank.{args.bank}:")
    print(f"  nk_curbal:   {nbank_pre.get('nk_curbal')} (pence)")
    print(f"  nk_recbal:   {nbank_pre.get('nk_recbal')} (pence)")
    print(f"  nk_reclnum:  {nbank_pre.get('nk_reclnum')}")
    print(f"  nk_recldte:  {nbank_pre.get('nk_recldte')}")
    print(f"  nk_lstrecl:  {nbank_pre.get('nk_lstrecl')}")
    print(f"  nk_lststno:  {nbank_pre.get('nk_lststno')}")

    # Determine target value
    if args.next_batch is not None:
        target = int(args.next_batch)
        target_source = f"--next-batch override"
    else:
        cur.execute("""
            SELECT ISNULL(MAX(ae_reclnum), 0) AS max_existing
            FROM aentry WITH (NOLOCK)
            WHERE ae_acnt = ? AND ae_reclnum IS NOT NULL AND ae_reclnum <> 0
        """, args.bank)
        max_existing = int(cur.fetchone().max_existing or 0)
        target = max_existing + 1
        target_source = f"MAX(ae_reclnum)+1 = {max_existing}+1"
    print(f"\nTarget nk_lstrecl = nk_reclnum = {target}  ({target_source})")

    cur_lstrecl = int(nbank_pre.get("nk_lstrecl") or 0)
    cur_reclnum = int(nbank_pre.get("nk_reclnum") or 0)
    if not args.force and cur_lstrecl == cur_reclnum and cur_lstrecl >= target:
        print(f"\nCounters already aligned at {cur_lstrecl} (>= target {target}). "
              f"Nothing to do. Re-run with --force to override.")
        return 0

    if not args.apply:
        print(f"\nDry-run only. Re-run with --apply to write.")
        return 0

    # Backup
    backup_path = write_backup({
        "timestamp": datetime.now().isoformat(),
        "db": args.db,
        "bank": args.bank,
        "target_value": target,
        "target_source": target_source,
        "nbank_pre": nbank_pre,
    }, args.db, args.bank)
    print(f"\nBackup written: {backup_path}")

    # Apply
    conn.close()
    conn = open_conn(args.db)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE nbank WITH (ROWLOCK)
            SET nk_lstrecl = ?,
                nk_reclnum = ?,
                datemodified = SYSUTCDATETIME()
            WHERE nk_acnt = ?
            """,
            target, target, args.bank,
        )
        conn.commit()
        print(f"  ✓ Set nk_lstrecl = nk_reclnum = {target}")
    except Exception as e:
        conn.rollback()
        print(f"\nERROR — rolled back: {e}")
        return 2

    # Verify
    cur.execute("""
        SELECT nk_lstrecl, nk_reclnum FROM nbank WITH (NOLOCK) WHERE nk_acnt = ?
    """, args.bank)
    r = cur.fetchone()
    print(f"\nPost-state: nk_lstrecl={r.nk_lstrecl}, nk_reclnum={r.nk_reclnum}")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
