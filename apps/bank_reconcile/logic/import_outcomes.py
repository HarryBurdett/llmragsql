"""
Bank-statement import outcome shaping.

Single helper that converts the import endpoints' internal categorisation
(`imported`, `already_posted`, `errors`, `skipped_*`) into a stable
client-facing response shape with per-row outcomes, counts, and a banner
summary. Every bank-import endpoint (SE + Opera 3) should call
`build_import_outcomes()` so the frontend has one shape to render.

See `docs/superpowers/specs/2026-06-10-bank-statement-partial-posting-design.md`.

The three states:
  - "posted":  wrote successfully to Opera this run.
  - "held":    skipped for a legitimate reason — not a failure.
               Sub-status carries the specific reason
               (`already_posted`, `period_blocked`, `unmatched`, ...).
  - "failed":  genuine error — operator must act.

The result banner uses `summary` (computed from counts) — not raw success
booleans — so the UI never has to decide "is this red or green?" itself.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


# Sub-status taxonomy (held). Add to this list rather than free-form text
# so the UI can map to icons / sort / filter consistently.
HELD_SUB_STATUSES = {
    'already_posted',   # duplicate-check caught it
    'period_blocked',   # target period is closed / blocked
    'unmatched',        # AI extraction couldn't match a customer/supplier
    'user_ignored',     # operator chose to skip
    'incomplete',       # extracted row missing required fields
    'not_selected',     # operator excluded this row before import
    'other',
}

FAILED_SUB_STATUSES = {
    'validation_error',
    'db_error',
    'schema_error',
    'other',
}


def _classify_skip_reason(reason: Optional[str]) -> str:
    """Map a free-form skip_reason string to one of HELD_SUB_STATUSES."""
    if not reason:
        return 'other'
    r = reason.lower()
    if 'already posted' in r or 'duplicate' in r or 'already exists' in r:
        return 'already_posted'
    if 'period' in r and ('block' in r or 'closed' in r):
        return 'period_blocked'
    if 'incomplete' in r or 'missing' in r:
        return 'incomplete'
    if 'unmatch' in r or 'no match' in r or 'matching' in r or 'could not find' in r:
        return 'unmatched'
    if 'ignored' in r or 'user' in r:
        return 'user_ignored'
    return 'other'


def _row_from_txn(txn: Dict[str, Any]) -> Dict[str, Any]:
    """Extract the common subset of fields the UI needs from a txn dict."""
    return {
        'row': txn.get('row') or txn.get('row_number'),
        'amount': txn.get('amount'),
        'date': txn.get('date') or txn.get('post_date'),
        'description': (
            txn.get('description')
            or txn.get('name')
            or txn.get('reference', '')
        ),
        'action': txn.get('action'),
    }


def build_import_outcomes(
    *,
    imported: Optional[List[Dict[str, Any]]] = None,
    already_posted: Optional[List[Dict[str, Any]]] = None,
    period_blocked: Optional[List[Dict[str, Any]]] = None,
    skipped: Optional[List[Dict[str, Any]]] = None,
    errors: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Build the structured outcome payload the result banner uses.

    All inputs default to empty so callers can pass only what they have.
    Each item is a transaction-shaped dict with at least `row`,
    `amount`, `date`, `description`, optionally `action` and (for
    `errors`) `error` / `reason`.

    Returns:
      {
        'success': bool,                # True iff zero failures
        'summary': 'all_posted' | 'all_already_posted' | 'partial' |
                   'nothing_to_import' | 'failed',
        'counts': {
            'posted': int, 'held': int, 'failed': int, 'total': int
        },
        'outcomes': [
            {'row', 'status', 'sub_status', 'reason',
             'amount', 'date', 'description', 'action',
             'opera_entry_ref'?},
            ...
        ],
      }
    """
    imported = imported or []
    already_posted = already_posted or []
    period_blocked = period_blocked or []
    skipped = skipped or []
    errors = errors or []

    outcomes: List[Dict[str, Any]] = []

    for t in imported:
        row = _row_from_txn(t)
        row.update({
            'status': 'posted',
            'sub_status': None,
            'reason': None,
            'opera_entry_ref': t.get('entry_number') or t.get('opera_entry_ref'),
        })
        outcomes.append(row)

    for t in already_posted:
        row = _row_from_txn(t)
        row.update({
            'status': 'held',
            'sub_status': 'already_posted',
            'reason': t.get('reason') or t.get('skip_reason') or 'Already posted in Opera',
            'opera_entry_ref': t.get('opera_entry_ref') or t.get('existing_entry'),
        })
        outcomes.append(row)

    for t in period_blocked:
        row = _row_from_txn(t)
        row.update({
            'status': 'held',
            'sub_status': 'period_blocked',
            'reason': t.get('reason') or 'Period is blocked or closed',
        })
        outcomes.append(row)

    for t in skipped:
        row = _row_from_txn(t)
        sub = _classify_skip_reason(t.get('reason') or t.get('skip_reason'))
        row.update({
            'status': 'held',
            'sub_status': sub,
            'reason': t.get('reason') or t.get('skip_reason') or 'Skipped',
        })
        outcomes.append(row)

    for t in errors:
        row = _row_from_txn(t)
        row.update({
            'status': 'failed',
            'sub_status': t.get('sub_status') or 'other',
            'reason': t.get('error') or t.get('reason') or 'Failed',
        })
        outcomes.append(row)

    # Deterministic order: keep the response stable for snapshot/diff testing
    outcomes.sort(key=lambda r: (r.get('row') or 0, r.get('status', '')))

    posted_n = sum(1 for o in outcomes if o['status'] == 'posted')
    held_n = sum(1 for o in outcomes if o['status'] == 'held')
    failed_n = sum(1 for o in outcomes if o['status'] == 'failed')
    total = posted_n + held_n + failed_n

    if failed_n > 0:
        summary = 'failed'
    elif posted_n == 0 and held_n == 0:
        summary = 'nothing_to_import'
    elif posted_n == 0 and held_n > 0:
        # Distinguish "all already posted" from "all blocked" etc.
        all_already = all(
            o['sub_status'] == 'already_posted'
            for o in outcomes
            if o['status'] == 'held'
        )
        summary = 'all_already_posted' if all_already else 'partial'
    elif held_n == 0:
        summary = 'all_posted'
    else:
        summary = 'partial'

    return {
        'success': failed_n == 0,
        'summary': summary,
        'counts': {
            'posted': posted_n,
            'held': held_n,
            'failed': failed_n,
            'total': total,
        },
        'outcomes': outcomes,
    }
