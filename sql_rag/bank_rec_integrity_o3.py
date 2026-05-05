"""Opera 3 mirror of `bank_rec_integrity.py`.

Read-only diagnostic that runs at API startup for Opera 3 companies
to detect any sign that a non-canonical rec write path has corrupted
nbank's sequence counters.

Post-fix invariant (per business-rules/bank-rec-completion.md):

    nbank.nk_lstrecl == nbank.nk_reclnum

Both must point at the next batch number to use. They diverge when
old buggy paths (`MAX(ae_reclnum)+1` based) wrote one without the
other.

Audit 2026-05-05 cross-cutting F10 — was SE-only, leaving Opera 3
customers without the safety net introduced after the Cloudsis batch
209 incident.

The check is read-only against the FoxPro DBFs. It NEVER mutates
Opera. A failure to read the DBF does not block API startup — it
just logs a warning that the check could not run.
"""
from __future__ import annotations

import logging
from typing import List, Optional

from sql_rag.bank_rec_integrity import BankRecIntegrityIssue

logger = logging.getLogger(__name__)


def _coerce_int(value) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def check_bank_rec_integrity_opera3(reader) -> List[BankRecIntegrityIssue]:
    """Inspect every nbank row in Opera 3 for rec-counter inconsistencies.

    Returns a list of `BankRecIntegrityIssue` (the same dataclass the
    SE check returns — central type, central rendering). Empty list
    means clean. Read-only; never mutates Opera.

    `reader` is an Opera3Reader-compatible object exposing
    `read_table('nbank') -> Iterable[dict-like]`.
    """
    issues: List[BankRecIntegrityIssue] = []
    try:
        rows = list(reader.read_table('nbank'))
    except Exception as exc:
        logger.warning(
            'Opera 3 bank-rec integrity check could not read nbank: %s', exc,
        )
        return issues

    for row in rows:
        bank_acnt = (row.get('nk_acnt') or '').strip() if hasattr(row, 'get') else ''
        # nk_desc on Opera 3 is the bank description; fall back to
        # nk_bkname for older schemas.
        bank_desc = (
            (row.get('nk_desc') or row.get('nk_bkname') or '').strip()
            if hasattr(row, 'get') else ''
        )
        lstrecl = _coerce_int(row.get('nk_lstrecl') if hasattr(row, 'get') else None)
        reclnum = _coerce_int(row.get('nk_reclnum') if hasattr(row, 'get') else None)

        if lstrecl != reclnum:
            issues.append(
                BankRecIntegrityIssue(
                    bank_acnt=bank_acnt,
                    bank_desc=bank_desc,
                    kind='reclnum_lstrecl_mismatch',
                    detail=(
                        f"nk_lstrecl={lstrecl} but nk_reclnum={reclnum} — "
                        "indicates a non-canonical reconciliation write path "
                        "(see business-rules/bank-rec-completion.md). Future "
                        "rec batch numbers will be wrong on this bank until "
                        "the counters are realigned."
                    ),
                )
            )

    return issues


def log_bank_rec_integrity_opera3(reader, *, company_id: Optional[str] = None) -> None:
    """Run the Opera 3 integrity check and log a WARNING per issue.

    Defensive: never raises. A DBF read failure during the check is
    itself logged as a warning so we know we couldn't verify, but we
    don't block startup.
    """
    company_tag = f'[{company_id}] ' if company_id else ''
    try:
        issues = check_bank_rec_integrity_opera3(reader)
    except Exception as exc:
        logger.warning(
            '%sOpera 3 bank-rec integrity check could not run: %s',
            company_tag, exc,
        )
        return

    if not issues:
        return

    for issue in issues:
        logger.warning(
            '%sOpera 3 bank-rec integrity: %s (%s) — %s: %s',
            company_tag,
            issue.bank_acnt,
            issue.bank_desc,
            issue.kind,
            issue.detail,
        )
