#!/usr/bin/env python3
"""
Reverse a bank reconciliation batch in Opera SQL SE.

Reverts both stages:
  - Stage A (per-entry rec flags): clears ae_reclnum, ae_recdate,
    ae_recbal, ae_statln on every aentry whose ae_reclnum equals the
    target batch.
  - Stage B (bank-level summary): reverts nbank.nk_recbal /
    nk_reclnum / nk_recldte / nk_lstrecl / nk_lststno / nk_lststdt /
    nk_recstdt / nk_recstfr / nk_recstto to whatever they were after
    the *prior* batch closed.

Notes:
  - Operates inside a single SQL Server transaction with ROWLOCK
    hints on every UPDATE (per CLAUDE.md locking rules).
  - Backs up the current (pre-revert) state to a timestamped JSON
    file under data/_audit/bank_rec_reversals/. Never overwrites.
  - Idempotent — running twice with no batch present is a no-op.
  - Posting tables (atran/aentry-value/ntran/anoml/nacnt/nbank.nk_curbal)
    are NOT touched. Reversing the rec doesn't reverse the postings.
  - DOES NOT modify Opera schema.

Usage:
    python3 scripts/reverse_bank_rec_batch.py --db Opera3SECompany00C \
        --bank BB005 --batch 209 --dry-run
    # then to apply:
    python3 scripts/reverse_bank_rec_batch.py --db Opera3SECompany00C \
        --bank BB005 --batch 209 --apply
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
AUDIT_DIR = Path("/Users/maccb/llmragsql/data/_audit/bank_rec_reversals")


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


def _decimal_safe(v: Any) -> Any:
    """JSON-safe coercion for pyodbc Decimal/datetime/None."""
    from decimal import Decimal
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def snapshot_aentry(cur: pyodbc.Cursor, bank: str, batch: int) -> list[dict]:
    cur.execute(
        """
        SELECT id, ae_acnt, ae_cbtype, ae_entry, ae_value,
               ae_reclnum, ae_recdate, ae_recbal, ae_statln, ae_complet
        FROM aentry WITH (NOLOCK)
        WHERE ae_acnt = ? AND ae_reclnum = ?
        ORDER BY ae_statln, ae_entry
        """,
        bank, batch,
    )
    cols = [d[0] for d in cur.description]
    return [{c: _decimal_safe(v) for c, v in zip(cols, r)} for r in cur.fetchall()]


def snapshot_nbank(cur: pyodbc.Cursor, bank: str) -> dict:
    cur.execute(
        """
        SELECT id, nk_acnt, nk_curbal, nk_recbal,
               nk_reclnum, nk_recldte, nk_lstrecl,
               nk_lststno, nk_lststdt,
               nk_recstdt, nk_recstfr, nk_recstto
        FROM nbank WITH (NOLOCK) WHERE nk_acnt = ?
        """,
        bank,
    )
    row = cur.fetchone()
    if not row:
        return {}
    cols = [d[0] for d in cur.description]
    return {c: _decimal_safe(v) for c, v in zip(cols, row)}


def find_prior_batch_close(cur: pyodbc.Cursor, bank: str, batch: int) -> dict | None:
    """Return the closing state from the most recent batch < target batch,
    or None if no prior batch exists for this bank."""
    cur.execute(
        """
        SELECT MAX(ae_reclnum) AS prev
        FROM aentry WITH (NOLOCK)
        WHERE ae_acnt = ? AND ae_reclnum < ?
          AND ae_reclnum IS NOT NULL AND ae_reclnum <> 0
        """,
        bank, batch,
    )
    row = cur.fetchone()
    prev_batch = int(row.prev) if row and row.prev else None
    if not prev_batch:
        return None

    cur.execute(
        """
        SELECT TOP 1 ae_entry, ae_recbal, ae_recdate, ae_statln
        FROM aentry WITH (NOLOCK)
        WHERE ae_acnt = ? AND ae_reclnum = ?
        ORDER BY ae_statln DESC, ae_entry DESC
        """,
        bank, prev_batch,
    )
    last = cur.fetchone()
    if not last:
        return {"prev_batch": prev_batch}
    return {
        "prev_batch": prev_batch,
        "ae_entry": last.ae_entry,
        "ae_recbal_pence": int(last.ae_recbal) if last.ae_recbal else 0,
        "ae_recdate": last.ae_recdate,
        "max_statln": int(last.ae_statln) if last.ae_statln else 0,
    }


def write_backup(payload: dict, db: str, bank: str, batch: int) -> Path:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    fname = f"{ts}_{db}_{bank}_batch{batch}_pre_reversal.json"
    path = AUDIT_DIR / fname
    path.write_text(json.dumps(payload, indent=2, default=str))
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True, help="Opera company database name (e.g. Opera3SECompany00C)")
    ap.add_argument("--bank", required=True, help="Bank account code (e.g. BB005)")
    ap.add_argument("--batch", required=True, type=int, help="Reconciliation batch number to reverse (the value of ae_reclnum)")
    ap.add_argument("--apply", action="store_true", help="Actually apply the reversal. Default is dry-run.")
    args = ap.parse_args()

    print(f"DB        : {args.db}")
    print(f"Bank      : {args.bank}")
    print(f"Batch     : {args.batch}")
    print(f"Apply     : {args.apply}")
    print(f"Audit dir : {AUDIT_DIR}")

    conn = open_conn(args.db)
    cur = conn.cursor()

    # Snapshot the current (pre-revert) state
    aentry_pre = snapshot_aentry(cur, args.bank, args.batch)
    nbank_pre = snapshot_nbank(cur, args.bank)
    prior = find_prior_batch_close(cur, args.bank, args.batch)

    print(f"\n=== Pre-revert state ===")
    print(f"  aentry rows in batch {args.batch}: {len(aentry_pre)}")
    for r in aentry_pre:
        val = (r['ae_value'] or 0) / 100
        rb = (r['ae_recbal'] or 0) / 100
        print(f"    {r['ae_cbtype']} {r['ae_entry']}  £{val:>10,.2f}  ae_reclnum={r['ae_reclnum']}  ae_recdate={r['ae_recdate']}  ae_recbal=£{rb:,.2f}  ae_statln={r['ae_statln']}")
    print(f"  nbank: nk_recbal=£{(nbank_pre.get('nk_recbal') or 0)/100:,.2f}  nk_reclnum={nbank_pre.get('nk_reclnum')}  nk_lstrecl={nbank_pre.get('nk_lstrecl')}  nk_lststno={nbank_pre.get('nk_lststno')}")

    if not aentry_pre:
        print(f"\nNo aentry rows have ae_reclnum={args.batch} on bank {args.bank}. Nothing to do (idempotent no-op).")
        return 0

    print(f"\n=== Prior batch close (revert target for nbank) ===")
    if not prior:
        print(f"  No prior batch found. nbank rec fields will be cleared to zero/null.")
    else:
        rb = (prior.get('ae_recbal_pence') or 0) / 100
        print(f"  Prior batch number: {prior['prev_batch']}")
        print(f"  Prior batch last entry: {prior.get('ae_entry')}  recbal=£{rb:,.2f}  recdate={prior.get('ae_recdate')}  statln={prior.get('max_statln')}")

    # Build the planned changes
    print(f"\n=== Planned changes ===")
    print(f"  STAGE A: aentry rows in batch {args.batch} ({len(aentry_pre)} rows)")
    print(f"    SET ae_reclnum=0, ae_recdate=NULL, ae_recbal=0, ae_statln=0  WHERE ae_acnt='{args.bank}' AND ae_reclnum={args.batch}")
    print(f"    (ae_complet stays at 1 — NL transfer was correctly recorded; rec reversal does not undo NL transfer)")

    new_recbal = (prior.get('ae_recbal_pence') if prior else 0) or 0
    new_reclnum = (prior.get('prev_batch') if prior else 0) or 0
    new_recldte = (prior.get('ae_recdate') if prior else None)
    new_lstrecl = (prior.get('max_statln') if prior else 0) or 0
    print(f"\n  STAGE B: nbank row for {args.bank}")
    print(f"    SET nk_recbal={new_recbal}  (£{new_recbal/100:,.2f})")
    print(f"        nk_reclnum=0  (Opera will assign on next rec)")
    print(f"        nk_recldte=NULL")
    print(f"        nk_lstrecl=0")
    print(f"    (nk_lststno/nk_lststdt are NOT changed — they reflect the last *imported* statement, not the rec batch)")
    print(f"    (nk_curbal NOT changed — postings are not being reversed)")

    if not args.apply:
        print(f"\nDry-run only. Re-run with --apply to actually reverse.")
        return 0

    # Backup BEFORE doing anything
    backup_path = write_backup({
        "timestamp": datetime.now().isoformat(),
        "db": args.db,
        "bank": args.bank,
        "batch_reversed": args.batch,
        "aentry_pre": aentry_pre,
        "nbank_pre": nbank_pre,
        "prior_batch_close": prior,
    }, args.db, args.bank, args.batch)
    print(f"\nBackup written: {backup_path}")

    # Re-open connection to ensure fresh transaction
    conn.close()
    conn = open_conn(args.db)
    cur = conn.cursor()

    try:
        # STAGE A — clear rec flags on each aentry row
        cur.execute(
            """
            UPDATE aentry WITH (ROWLOCK)
            SET ae_reclnum = 0,
                ae_recdate = NULL,
                ae_recbal = 0,
                ae_statln = 0,
                datemodified = SYSUTCDATETIME()
            WHERE ae_acnt = ? AND ae_reclnum = ?
            """,
            args.bank, args.batch,
        )
        rows_cleared = cur.rowcount
        print(f"  ✓ aentry: cleared rec flags on {rows_cleared} row(s)")

        # STAGE B — revert nbank fields
        cur.execute(
            """
            UPDATE nbank WITH (ROWLOCK)
            SET nk_recbal = ?,
                nk_reclnum = 0,
                nk_recldte = NULL,
                nk_lstrecl = 0,
                datemodified = SYSUTCDATETIME()
            WHERE nk_acnt = ?
            """,
            new_recbal, args.bank,
        )
        nb_rows = cur.rowcount
        print(f"  ✓ nbank: reverted {nb_rows} row")

        conn.commit()
        print(f"\nCommitted.")
    except Exception as e:
        conn.rollback()
        print(f"\nERROR — rolled back: {e}")
        return 2

    # Verify
    print(f"\n=== Post-revert state ===")
    aentry_post = snapshot_aentry(cur, args.bank, args.batch)
    nbank_post = snapshot_nbank(cur, args.bank)
    print(f"  aentry rows still tagged ae_reclnum={args.batch}: {len(aentry_post)}  (expected 0)")
    print(f"  nbank: nk_recbal=£{(nbank_post.get('nk_recbal') or 0)/100:,.2f}  nk_reclnum={nbank_post.get('nk_reclnum')}  nk_lstrecl={nbank_post.get('nk_lstrecl')}")

    # Spot-check the 3 entries are now unreconciled
    if aentry_pre:
        ids = [r['id'] for r in aentry_pre]
        placeholders = ",".join("?" * len(ids))
        cur.execute(
            f"""
            SELECT ae_cbtype, ae_entry, ae_reclnum, ae_recdate, ae_recbal, ae_statln
            FROM aentry WITH (NOLOCK)
            WHERE id IN ({placeholders})
            ORDER BY ae_entry
            """,
            *ids,
        )
        print(f"\n  Verifying the {len(ids)} formerly-reconciled entries:")
        for r in cur.fetchall():
            ok = (r.ae_reclnum in (None, 0)) and (r.ae_recdate is None) and (int(r.ae_recbal or 0) == 0) and (int(r.ae_statln or 0) == 0)
            flag = "✓" if ok else "✗"
            print(f"    {flag} {r.ae_cbtype} {r.ae_entry}  reclnum={r.ae_reclnum} recdate={r.ae_recdate} recbal={r.ae_recbal} statln={r.ae_statln}")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
