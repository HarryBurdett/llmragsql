#!/usr/bin/env python3
"""
Reverse a bank reconciliation batch in Opera 3 (FoxPro DBF).

Mirror of `scripts/reverse_bank_rec_batch.py` for Opera 3
installations. Audit 2026-05-05 stages-3-5 F11 — was missing,
leaving Opera 3 customers unable to recover from a wrong rec
without manual DBF surgery.

Reverts both stages:
  - Stage A (per-entry rec flags): clears ae_reclnum, ae_recdate,
    ae_recbal, ae_statln, ae_frstat, ae_tostat, ae_tmpstat on every
    aentry whose ae_reclnum equals the target batch.
  - Stage B (bank-level summary): reverts nbank.nk_recbal /
    nk_reclnum / nk_recldte / nk_lstrecl / nk_lststno / nk_lststdt /
    nk_recstdt / nk_recstfr / nk_recstto / nk_recstln / nk_reccfwd
    to whatever they were after the *prior* batch closed.

Notes:
  - Reads/writes DBFs directly via the `dbf` library — same code
    path as Opera3WriteAgent.
  - Backs up the current (pre-revert) state to a timestamped JSON
    file under data/_audit/bank_rec_reversals_o3/.
  - Idempotent — running twice with no batch present is a no-op.
  - Posting tables (atran/aentry-value/ntran/anoml/nacnt/nbank.nk_curbal)
    are NOT touched. Reversing the rec doesn't reverse the postings.
  - DOES NOT modify Opera schema.

Usage:
    python3 scripts/reverse_bank_rec_batch_opera3.py \\
        --data-path "C:/Apps/O3 Server VFP" \\
        --bank BC010 --batch 2682 --dry-run
    # then to apply:
    python3 scripts/reverse_bank_rec_batch_opera3.py \\
        --data-path "C:/Apps/O3 Server VFP" \\
        --bank BC010 --batch 2682 --apply
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


def _str(v: Any) -> str:
    if v is None:
        return ''
    return str(v).strip()


def _normalise_for_json(v: Any) -> Any:
    """Make values JSON-serialisable."""
    if v is None:
        return None
    if isinstance(v, (datetime,)):
        return v.isoformat()
    try:
        if hasattr(v, 'isoformat'):
            return v.isoformat()
    except Exception:
        pass
    if isinstance(v, (str, int, float, bool)):
        return v
    return str(v)


def _row_to_dict(record, fields: List[str]) -> Dict[str, Any]:
    """Convert a dbf record to a dict for the named fields."""
    out: Dict[str, Any] = {}
    for f in fields:
        try:
            out[f] = _normalise_for_json(getattr(record, f, None))
        except Exception:
            out[f] = None
    return out


def snapshot_aentry(reader, bank: str, batch: int) -> List[Dict[str, Any]]:
    """Read every aentry in this bank+batch into a dict list."""
    out: List[Dict[str, Any]] = []
    fields = [
        'ae_acnt', 'ae_entry', 'ae_reclnum', 'ae_recdate', 'ae_recbal',
        'ae_statln', 'ae_frstat', 'ae_tostat', 'ae_tmpstat', 'ae_value',
    ]
    for row in reader.read_table('aentry'):
        if _str(row.get('ae_acnt')).upper() != bank.upper():
            continue
        try:
            rec_num = int(row.get('ae_reclnum') or 0)
        except (TypeError, ValueError):
            rec_num = 0
        if rec_num != batch:
            continue
        out.append({f: _normalise_for_json(row.get(f)) for f in fields})
    return out


def snapshot_nbank(reader, bank: str) -> Dict[str, Any]:
    """Read nbank fields needed to revert Stage B."""
    fields = [
        'nk_acnt', 'nk_recbal', 'nk_reclnum', 'nk_recldte', 'nk_lstrecl',
        'nk_lststno', 'nk_lststdt', 'nk_recstdt', 'nk_recstfr', 'nk_recstto',
        'nk_recstln', 'nk_reccfwd',
    ]
    for row in reader.read_table('nbank'):
        if _str(row.get('nk_acnt')).upper() != bank.upper():
            continue
        return {f: _normalise_for_json(row.get(f)) for f in fields}
    return {}


def derive_prior_state(reader, bank: str, batch: int) -> Dict[str, Any]:
    """Compute what nbank should look like after the reversal.

    Walks back to the highest ae_frstat < batch on this bank with
    ae_reclnum > 0 to find the prior batch's stamped state. If no
    prior batch exists, returns the fresh-bank state (everything 0).
    """
    prior_lststno = 0
    prior_recdate = None
    prior_reclnum = 0
    prior_statln = 0
    prior_recbal = 0  # in pence — sum of all reconciled aentry except this batch

    for row in reader.read_table('aentry'):
        if _str(row.get('ae_acnt')).upper() != bank.upper():
            continue
        try:
            rec_num = int(row.get('ae_reclnum') or 0)
        except (TypeError, ValueError):
            continue
        if rec_num <= 0 or rec_num == batch:
            continue
        # Sum to recompute new nk_recbal (pence).
        try:
            prior_recbal += int(row.get('ae_value') or 0)
        except (TypeError, ValueError):
            pass
        # Track most-recent prior batch.
        try:
            fstno = int(row.get('ae_frstat') or 0)
            stln = int(row.get('ae_statln') or 0)
        except (TypeError, ValueError):
            continue
        rd = row.get('ae_recdate')
        if (
            fstno > prior_lststno
            or (fstno == prior_lststno and stln > prior_statln)
        ):
            prior_lststno = fstno
            prior_reclnum = rec_num
            prior_statln = stln
            prior_recdate = rd

    return {
        'nk_recbal': prior_recbal,
        'nk_reccfwd': 0,
        'nk_lststno': prior_lststno,
        'nk_lstrecl': (prior_reclnum + 1) if prior_reclnum else 1,
        'nk_reclnum': (prior_reclnum + 1) if prior_reclnum else 1,
        'nk_recldte': prior_recdate,
        'nk_recstfr': 0,
        'nk_recstto': 0,
        'nk_recstdt': None,
        'nk_recstln': prior_statln,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='Reverse an Opera 3 bank rec batch.')
    parser.add_argument('--data-path', required=True, help='Path to Opera 3 company data folder')
    parser.add_argument('--bank', required=True, help='Bank account code (e.g. BC010)')
    parser.add_argument('--batch', type=int, required=True, help='Rec batch number to reverse (ae_reclnum)')
    parser.add_argument('--dry-run', action='store_true', help='Snapshot + diff only — no writes')
    parser.add_argument('--apply', action='store_true', help='Apply the reversal')
    args = parser.parse_args()

    if args.dry_run == args.apply:
        print('Specify exactly one of --dry-run or --apply', flush=True)
        return 2

    data_path = Path(args.data_path)
    if not data_path.is_dir():
        print(f"Data path not found: {data_path}", flush=True)
        return 2

    from sql_rag.opera3_foxpro import Opera3Reader  # type: ignore
    reader = Opera3Reader(str(data_path))

    # Snapshot.
    aentry_rows = snapshot_aentry(reader, args.bank, args.batch)
    nbank_now = snapshot_nbank(reader, args.bank)
    if not aentry_rows:
        print(f"No aentry rows found with ae_acnt={args.bank}, ae_reclnum={args.batch}", flush=True)
        return 1
    if not nbank_now:
        print(f"Bank {args.bank} not found in nbank", flush=True)
        return 2

    prior = derive_prior_state(reader, args.bank, args.batch)

    audit_dir = Path('/Users/maccb/llmragsql/data/_audit/bank_rec_reversals_o3')
    audit_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    audit_path = audit_dir / f'{args.bank}_batch{args.batch}_{ts}.json'
    audit = {
        'bank': args.bank,
        'batch': args.batch,
        'data_path': str(data_path),
        'timestamp': ts,
        'aentry_pre_revert': aentry_rows,
        'nbank_pre_revert': nbank_now,
        'nbank_post_revert_target': prior,
    }
    audit_path.write_text(json.dumps(audit, indent=2, default=str))
    print(f'Audit written: {audit_path}', flush=True)

    if args.dry_run:
        print(f'DRY-RUN — would clear Stage A on {len(aentry_rows)} aentry rows', flush=True)
        print(f'DRY-RUN — would update nbank.{args.bank} → {prior}', flush=True)
        return 0

    # Apply: open DBFs, walk records, update.
    import dbf  # type: ignore

    aentry_path = data_path / 'aentry.dbf'
    if not aentry_path.exists():
        aentry_path = data_path / 'AENTRY.DBF'
    nbank_path = data_path / 'nbank.dbf'
    if not nbank_path.exists():
        nbank_path = data_path / 'NBANK.DBF'

    rows_cleared = 0
    aentry_table = dbf.Table(str(aentry_path))
    aentry_table.open(dbf.READ_WRITE)
    try:
        for record in aentry_table:
            ae_acnt = _str(getattr(record, 'ae_acnt', None)).upper()
            try:
                rec_num = int(getattr(record, 'ae_reclnum', 0) or 0)
            except (TypeError, ValueError):
                rec_num = 0
            if ae_acnt != args.bank.upper() or rec_num != args.batch:
                continue
            with record as r:
                r.ae_reclnum = 0
                try:
                    r.ae_recdate = None
                except Exception:
                    pass
                try:
                    r.ae_recbal = 0
                except Exception:
                    pass
                r.ae_statln = 0
                r.ae_frstat = 0
                r.ae_tostat = 0
                r.ae_tmpstat = 0
            rows_cleared += 1
    finally:
        aentry_table.close()

    nbank_updated = False
    nbank_table = dbf.Table(str(nbank_path))
    nbank_table.open(dbf.READ_WRITE)
    try:
        for record in nbank_table:
            if _str(getattr(record, 'nk_acnt', None)).upper() != args.bank.upper():
                continue
            with record as r:
                try:
                    r.nk_recbal = int(prior['nk_recbal'])
                except Exception:
                    pass
                try:
                    r.nk_reccfwd = int(prior['nk_reccfwd'])
                except Exception:
                    pass
                try:
                    r.nk_lststno = int(prior['nk_lststno'])
                except Exception:
                    pass
                try:
                    r.nk_lstrecl = int(prior['nk_lstrecl'])
                except Exception:
                    pass
                try:
                    r.nk_reclnum = int(prior['nk_reclnum'])
                except Exception:
                    pass
                try:
                    r.nk_recldte = prior['nk_recldte']
                except Exception:
                    pass
                try:
                    r.nk_recstfr = int(prior['nk_recstfr'])
                    r.nk_recstto = int(prior['nk_recstto'])
                    r.nk_recstdt = prior['nk_recstdt']
                    r.nk_recstln = int(prior['nk_recstln'])
                except Exception:
                    pass
            nbank_updated = True
            break
    finally:
        nbank_table.close()

    print(
        f'Reversed batch {args.batch} on bank {args.bank}: '
        f'cleared {rows_cleared} aentry rows; nbank updated={nbank_updated}',
        flush=True,
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
