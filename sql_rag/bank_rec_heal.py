"""Bank-rec local-status self-heal.

When the operator runs a partial rec via this app and finishes it in
Opera Cashbook > Reconcile, Opera updates nbank/aentry but does not
touch our local bank_statement_imports.is_reconciled flag. This module
detects that situation on every scan-emails call and updates the local
flag — read-only against Opera.

Rule (all required, AND-ed):
  1. nk_recbal/100.0 ≈ closing_balance within £0.01
  2. nk_lststdt >= period_end
  3. nk_lststno >= stored statement_number  (skipped if NULL)

Spec: docs/superpowers/specs/2026-05-05-bank-rec-self-heal-design.md
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, List, Mapping, Optional, Protocol, Tuple

logger = logging.getLogger(__name__)

# Tolerance for the balance match. £0.01 is the manual-rec convention
# elsewhere in the codebase (statement opening-balance validation, etc.).
BALANCE_TOLERANCE_POUNDS = 0.01


@dataclass(frozen=True)
class NbankSnapshot:
    """Read-only view of the four nbank fields the heal rule needs."""
    bank_code: str
    recbal_pounds: Optional[float]
    lststdt: Optional[date]
    lststno: Optional[int]


@dataclass
class HealResult:
    """Outcome of a single heal_bank_statement_imports() call."""
    healed_count: int = 0
    audit_lines: List[str] = field(default_factory=list)


class OperaDataSource(Protocol):
    """Structural type for an Opera data source the heal can use.

    Both OperaSEDataSource (sql_rag/duplicate_check_se.py) and
    Opera3DataSource (sql_rag/duplicate_check_o3.py) satisfy this.
    """

    def read_nbank(self, bank_code: str) -> Optional[NbankSnapshot]:
        ...

    def count_reconciled_aentry(
        self, bank_code: str, statement_number: int
    ) -> int:
        ...

    def count_reconciled_aentry_in_period(
        self, bank_code: str, period_start: date, period_end: date
    ) -> int:
        """Date-based fallback used by the heal for legacy rows that
        have no stored statement_number — counts aentry rows where
        ae_recdate falls in [period_start, period_end] AND ae_reclnum>0.
        Same units / same NOLOCK semantics as count_reconciled_aentry.
        """
        ...


def is_row_healable(
    row: Mapping[str, Any],
    snapshot: NbankSnapshot,
) -> Tuple[bool, str]:
    """Evaluate the three-fact rule for a single bank_statement_imports row.

    Returns (healable, audit_proof_string). The audit string is suitable
    to embed in the per-row log line regardless of outcome — explains
    which checks passed or failed.

    The rule (all required, AND-ed; check 3 skipped if statement_number
    is NULL):
      1. nk_recbal_pounds matches closing_balance within
         BALANCE_TOLERANCE_POUNDS.
      2. nk_lststdt >= period_end.
      3. nk_lststno >= stored statement_number.
    """
    closing = row.get('closing_balance')
    period_end = row.get('period_end')
    stored_stmt_no = row.get('statement_number')

    # Defensive: any NULL on either side of a check fails the rule.
    if snapshot.recbal_pounds is None or closing is None:
        return False, 'check 1 NULL: recbal or closing missing'
    if snapshot.lststdt is None or period_end is None:
        return False, 'check 2 NULL: lststdt or period_end missing'

    # Check 1: balance match within £0.01.
    # Use a tiny epsilon (1e-9) so IEEE 754 subtraction of two pence-precise
    # decimals (which can yield 0.010000000000218 etc.) doesn't falsely trip
    # the boundary. 0.01 stays a match; 0.011 still fails.
    if abs(snapshot.recbal_pounds - float(closing)) > BALANCE_TOLERANCE_POUNDS + 1e-9:
        return False, (
            f'check 1 fail: nk_recbal=£{snapshot.recbal_pounds:.2f} '
            f'!= closing=£{float(closing):.2f}'
        )

    # Check 2: nk_lststdt >= period_end.
    if snapshot.lststdt < period_end:
        return False, (
            f'check 2 fail: nk_lststdt={snapshot.lststdt} '
            f'< period_end={period_end}'
        )

    # Check 3: nk_lststno >= stored statement_number, IF stored is set.
    if stored_stmt_no is None:
        proof = (
            f'check 1 ok: nk_recbal=£{snapshot.recbal_pounds:.2f} '
            f'≈ closing=£{float(closing):.2f}; '
            f'check 2 ok: nk_lststdt={snapshot.lststdt} '
            f'>= period_end={period_end}; '
            f'check 3 skipped — legacy row (statement_number IS NULL)'
        )
        return True, proof

    if snapshot.lststno is None or int(snapshot.lststno) < int(stored_stmt_no):
        return False, (
            f'check 3 fail: nk_lststno={snapshot.lststno} '
            f'< statement_number={stored_stmt_no}'
        )

    proof = (
        f'check 1 ok: nk_recbal=£{snapshot.recbal_pounds:.2f} '
        f'≈ closing=£{float(closing):.2f}; '
        f'check 2 ok: nk_lststdt={snapshot.lststdt} '
        f'>= period_end={period_end}; '
        f'check 3 ok: nk_lststno={snapshot.lststno} '
        f'>= statement_number={stored_stmt_no}'
    )
    return True, proof


def heal_bank_statement_imports(
    bank_code: str,
    company_db_path: Path,
    opera_data_source: OperaDataSource,
) -> HealResult:
    """For every bank_statement_imports row on this bank with
    is_reconciled=0, evaluate the three-fact rule against Opera and
    flip the local flag where the rule is satisfied.

    Read-only against Opera. Updates only local SQLite. Idempotent.
    Per-company isolated — the caller resolves company_db_path via
    get_current_db_path('email_data.db') in the request scope.
    """
    result = HealResult()

    # Read Opera nbank once for this bank. If Opera is unreachable or
    # the bank is missing, log and bail — don't error the surrounding
    # scan call.
    try:
        snapshot = opera_data_source.read_nbank(bank_code)
    except Exception as exc:
        logger.warning(
            'bank_rec_heal: bank=%s nbank read failed (%s); skipping heal',
            bank_code, exc,
        )
        return result

    if snapshot is None:
        logger.warning(
            'bank_rec_heal: bank=%s not found in nbank; no rows healed',
            bank_code,
        )
        return result

    # Read all candidate rows (is_reconciled=0) for this bank.
    with sqlite3.connect(str(company_db_path)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, closing_balance, period_end, statement_number,
                   reconciled_count, reconciled_date
              FROM bank_statement_imports
             WHERE bank_code = ?
               AND COALESCE(is_reconciled, 0) = 0
            """,
            (bank_code,),
        )
        candidates = cursor.fetchall()

        for cand in candidates:
            row = {
                'closing_balance': cand['closing_balance'],
                'period_end': _parse_iso_date(cand['period_end']),
                'statement_number': cand['statement_number'],
            }
            healable, proof = is_row_healable(row, snapshot)
            if not healable:
                logger.debug(
                    'bank_rec_heal: bank=%s import_id=%s NOT healable: %s',
                    bank_code, cand['id'], proof,
                )
                continue

            # Compute reconciled_count.
            # New rows with statement_number stored: count by ae_frstat (precise).
            # Legacy rows (statement_number IS NULL): use transactions_imported
            # from the local row as the count — the rec-three-check rule has
            # fired, which means every imported entry has been reconciled in
            # Opera. transactions_imported is the count of entries we posted
            # (with ae_tmpstat) that the user then completed in Opera. Using
            # ae_recdate against the period would mis-count because Opera's
            # ae_recdate is when the user actually clicked Reconcile in Opera,
            # not the statement period. transactions_imported is the right
            # answer.
            new_count: Optional[int] = None
            if cand['statement_number'] is not None:
                try:
                    new_count = opera_data_source.count_reconciled_aentry(
                        bank_code, int(cand['statement_number'])
                    )
                except Exception as exc:
                    logger.warning(
                        'bank_rec_heal: bank=%s import_id=%s count failed (%s); '
                        'preserving existing reconciled_count',
                        bank_code, cand['id'], exc,
                    )
                    new_count = None
            else:
                # Legacy fallback: read transactions_imported from the local
                # row. If 0 / NULL, leave reconciled_count untouched.
                imp = _read_transactions_imported(company_db_path, cand['id'])
                if imp is not None and imp > 0:
                    new_count = imp

            cursor.execute(
                """
                UPDATE bank_statement_imports
                   SET is_reconciled = 1,
                       reconciled_date = COALESCE(reconciled_date, ?),
                       reconciled_count = CASE
                                            WHEN ? IS NULL THEN reconciled_count
                                            ELSE ?
                                          END
                 WHERE id = ?
                """,
                (datetime.now().isoformat(), new_count, new_count, cand['id']),
            )

            audit = _format_audit_line(bank_code, cand['id'], proof)
            logger.info(audit)
            result.audit_lines.append(audit)
            result.healed_count += 1

        conn.commit()

    return result


def _format_audit_line(bank_code: str, import_id: int, proof: str) -> str:
    """One-line audit string for an INFO-level log emission."""
    return (
        f'bank_rec_heal: bank={bank_code} import_id={import_id} healed — {proof}'
    )


def _read_transactions_imported(
    company_db_path: Path, import_id: int
) -> Optional[int]:
    """Read bank_statement_imports.transactions_imported for one row.

    Used by the legacy-row fallback in heal_bank_statement_imports —
    when statement_number IS NULL we cannot count by ae_frstat, so we
    use the locally-tracked count of entries we posted (which were all
    reconciled by definition once the heal three-check rule fires).
    """
    with sqlite3.connect(str(company_db_path)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            'SELECT transactions_imported FROM bank_statement_imports '
            'WHERE id = ?',
            (import_id,),
        ).fetchone()
        if row is None:
            return None
        v = row['transactions_imported']
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None


def _parse_iso_date(value: Any) -> Optional[date]:
    """Coerce 'YYYY-MM-DD' or 'YYYY-MM-DDThh:mm:ss' SQLite TEXT to a date."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00')).date()
    except ValueError:
        try:
            return datetime.strptime(s[:10], '%Y-%m-%d').date()
        except ValueError:
            return None
