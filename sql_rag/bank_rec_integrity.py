"""Bank reconciliation integrity checks.

Read-only diagnostic that runs at API startup (and after every
`_ensure_company_context()` switch) to detect any sign that a
non-canonical reconciliation write path has corrupted the bank's
sequence counters.

Post-fix invariant (per business-rules/bank-rec-completion.md):

    nbank.nk_lstrecl == nbank.nk_reclnum

Both must point at the next batch number to use. They diverge when the
old buggy paths (`MAX(ae_reclnum)+1` based) wrote one without the
other, leaving Opera in an inconsistent state — the exact pattern that
caused the Cloudsis batch 209 incident on 2026-05-04.

The check is read-only with NOLOCK hints. It NEVER mutates Opera. A
failure to query the DB does not block API startup — it just logs a
warning that the check could not run.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class BankRecIntegrityIssue:
    """One issue surfaced by `check_bank_rec_integrity`."""
    bank_acnt: str
    bank_desc: str
    kind: str
    detail: str


def _coerce_int(value) -> int:
    """Treat NULL/None as 0 — same convention as the rec write code."""
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def check_bank_rec_integrity(sql_connector) -> List[BankRecIntegrityIssue]:
    """Inspect every nbank row for rec-counter inconsistencies.

    Returns a list of `BankRecIntegrityIssue`. Empty list = clean.
    Read-only; never mutates Opera.
    """
    query = """
        SELECT nk_acnt, nk_desc, nk_lstrecl, nk_reclnum, nk_recbal
        FROM nbank WITH (NOLOCK)
    """
    df = sql_connector.execute_query(query)
    if df is None or getattr(df, "empty", False):
        return []

    issues: List[BankRecIntegrityIssue] = []
    for _, row in df.iterrows():
        bank_acnt = (row.get("nk_acnt") or "").strip() if hasattr(row, "get") else (row["nk_acnt"] or "").strip()
        bank_desc = ""
        if hasattr(row, "get"):
            desc_raw = row.get("nk_desc") or ""
        else:
            desc_raw = row.get("nk_desc", "") or ""
        bank_desc = str(desc_raw).strip()

        # row may be a dict (test stub) or a pandas Series; both support [] access
        lstrecl = _coerce_int(row["nk_lstrecl"] if "nk_lstrecl" in row else None)
        reclnum = _coerce_int(row["nk_reclnum"] if "nk_reclnum" in row else None)

        if lstrecl != reclnum:
            issues.append(
                BankRecIntegrityIssue(
                    bank_acnt=bank_acnt,
                    bank_desc=bank_desc,
                    kind="reclnum_lstrecl_mismatch",
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


def log_bank_rec_integrity(sql_connector, *, company_id: Optional[str] = None) -> None:
    """Run the integrity check and log a WARNING per issue.

    Defensive: never raises. A DB failure during the check is itself
    logged as a warning so we know we couldn't verify, but we don't
    block startup.
    """
    company_tag = f"[{company_id}] " if company_id else ""
    try:
        issues = check_bank_rec_integrity(sql_connector)
    except Exception as e:
        logger.warning(
            "%sBank-rec integrity check could not run: %s",
            company_tag, e,
        )
        return

    if not issues:
        return

    for issue in issues:
        logger.warning(
            "%sBank-rec integrity: %s (%s) — %s: %s",
            company_tag,
            issue.bank_acnt,
            issue.bank_desc,
            issue.kind,
            issue.detail,
        )
