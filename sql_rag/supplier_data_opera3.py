"""Opera 3 (FoxPro DBF) implementation of SupplierDataProvider.

Mirrors sql_rag/supplier_data_opera_se.py for installations running
on Opera 3 instead of Opera SE.

Reads from pname / ptran / palloc / zcontacts via Opera3Reader. All
operations are read-only. Writes (e.g. sender verification updates)
are NOT performed here — the supplier reconciliation routines avoid
modifying Opera data outside the controlled posting paths anyway.

CLAUDE.md mandatory parity: every SE supplier query has a matching
Opera 3 implementation. Audit 2026-05-05 Suppliers F1 — was missing.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from sql_rag.supplier_data_provider import (
    SupplierDataProvider,
    SupplierInfo,
    SupplierContact,
    OutstandingTransaction,
    TransactionRef,
)

logger = logging.getLogger(__name__)


def _str(value: Any) -> str:
    if value is None:
        return ''
    try:
        return str(value).strip()
    except Exception:
        return ''


def _num(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _date_str(value: Any) -> str:
    if value is None:
        return ''
    if hasattr(value, 'isoformat'):
        try:
            d = value.date() if isinstance(value, datetime) else value
            return d.isoformat() if hasattr(d, 'isoformat') else ''
        except Exception:
            return ''
    return str(value)[:10]


class Opera3SupplierDataProvider(SupplierDataProvider):
    """Opera 3 (FoxPro DBF) supplier data provider.

    Constructed with an Opera3Reader (or anything with read_table /
    query). All reads are filtered for non-dormant suppliers per
    CLAUDE.md "Dormant accounts excluded" rule.
    """

    def __init__(self, reader: Any) -> None:
        self._reader = reader

    # ------------------------------------------------------------------
    # Supplier master (pname)
    # ------------------------------------------------------------------

    def get_all_suppliers(self) -> List[SupplierInfo]:
        out: List[SupplierInfo] = []
        for row in self._reader.read_table('pname'):
            if int(row.get('pn_dormant', 0) or 0) != 0:
                continue
            acct = _str(row.get('pn_account'))
            if not acct:
                continue
            out.append(SupplierInfo(
                account_code=acct,
                name=_str(row.get('pn_name')),
                balance=_num(row.get('pn_currbal')),
                payment_terms_days=int(row.get('pn_credptd', 30) or 30),
                payment_method=_str(row.get('pn_payment') or row.get('pn_paymeth')),
                is_dormant=False,
            ))
        return out

    def get_supplier(self, account_code: str) -> Optional[SupplierInfo]:
        target = account_code.strip().upper()
        for row in self._reader.read_table('pname'):
            if _str(row.get('pn_account')).upper() != target:
                continue
            return SupplierInfo(
                account_code=_str(row.get('pn_account')),
                name=_str(row.get('pn_name')),
                balance=_num(row.get('pn_currbal')),
                payment_terms_days=int(row.get('pn_credptd', 30) or 30),
                payment_method=_str(row.get('pn_payment') or row.get('pn_paymeth')),
                is_dormant=int(row.get('pn_dormant', 0) or 0) != 0,
            )
        return None

    def get_supplier_name(self, account_code: str) -> Optional[str]:
        info = self.get_supplier(account_code)
        return info.name if info else None

    def get_supplier_balance(self, account_code: str) -> Optional[float]:
        info = self.get_supplier(account_code)
        return info.balance if info else None

    def find_supplier_by_name(
        self,
        name: str,
        account_ref: Optional[str] = None,
    ) -> Optional[SupplierInfo]:
        target = (name or '').strip().lower()
        ref_target = (account_ref or '').strip().upper()
        if not target and not ref_target:
            return None
        # Prefer exact name match; fall back to substring; last fallback
        # to account-code match.
        candidates: List[SupplierInfo] = []
        for s in self.get_all_suppliers():
            sn = (s.name or '').lower()
            if target and sn == target:
                return s
            if ref_target and s.account_code.upper() == ref_target:
                candidates.append(s)
            elif target and target in sn:
                candidates.append(s)
        return candidates[0] if candidates else None

    # ------------------------------------------------------------------
    # Contacts (zcontacts joined to pname.pn_account)
    # ------------------------------------------------------------------

    def _list_contacts(self, account_code: str) -> List[Dict[str, Any]]:
        target = account_code.strip().upper()
        out: List[Dict[str, Any]] = []
        try:
            for row in self._reader.read_table('zcontacts'):
                if _str(row.get('zc_account')).upper() != target:
                    continue
                out.append(row)
        except Exception as exc:
            logger.debug('Opera3 _list_contacts failed: %s', exc)
        return out

    def get_supplier_contact(self, account_code: str) -> Optional[SupplierContact]:
        # Prefer the row flagged as primary; else first with email.
        rows = self._list_contacts(account_code)
        primary = next((r for r in rows if int(r.get('zc_primary', 0) or 0) != 0), None)
        if primary is None:
            primary = next((r for r in rows if _str(r.get('zc_email'))), None)
        if primary is None:
            # Fall back to the pname-level email.
            base = self.get_supplier(account_code)
            if base is None:
                return None
            for row in self._reader.read_table('pname'):
                if _str(row.get('pn_account')).upper() == account_code.strip().upper():
                    em = _str(row.get('pn_email'))
                    if em:
                        return SupplierContact(name=_str(row.get('pn_name')), email=em)
            return None
        return SupplierContact(
            name=_str(primary.get('zc_name')),
            email=_str(primary.get('zc_email')),
            phone=_str(primary.get('zc_telno')),
            mobile=_str(primary.get('zc_mobile')),
            position=_str(primary.get('zc_position')),
        )

    def list_supplier_contacts(self, account_code: str) -> List[SupplierContact]:
        out: List[SupplierContact] = []
        for r in self._list_contacts(account_code):
            email = _str(r.get('zc_email'))
            if not email:
                continue
            out.append(SupplierContact(
                name=_str(r.get('zc_name')),
                email=email,
                phone=_str(r.get('zc_telno')),
                mobile=_str(r.get('zc_mobile')),
                position=_str(r.get('zc_position')),
            ))
        return out

    def verify_sender(self, account_code: str, email: str) -> bool:
        target = (email or '').strip().lower()
        if not target:
            return False
        for c in self.list_supplier_contacts(account_code):
            if c.email.lower() == target:
                return True
        # Also check the pname-level email.
        for row in self._reader.read_table('pname'):
            if _str(row.get('pn_account')).upper() != account_code.strip().upper():
                continue
            if _str(row.get('pn_email')).lower() == target:
                return True
        return False

    # ------------------------------------------------------------------
    # Transactions (ptran / palloc)
    # ------------------------------------------------------------------

    def _ptran_for(self, account_code: str) -> List[Dict[str, Any]]:
        target = account_code.strip().upper()
        out: List[Dict[str, Any]] = []
        for row in self._reader.read_table('ptran'):
            if _str(row.get('pt_account')).upper() != target:
                continue
            # Honour pt_remove if present (correction-pair-matched).
            if int(row.get('pt_remove', 0) or 0) != 0:
                continue
            out.append(row)
        return out

    def get_outstanding_transactions(self, account_code: str) -> List[OutstandingTransaction]:
        out: List[OutstandingTransaction] = []
        for row in self._ptran_for(account_code):
            balance = _num(row.get('pt_trbal'))
            if balance == 0:
                continue
            out.append(OutstandingTransaction(
                reference=_str(row.get('pt_trref')),
                date=_date_str(row.get('pt_trdate')),
                type_code=_str(row.get('pt_trtype')),
                balance=balance,
                value=_num(row.get('pt_trvalue')),
                due_date=_date_str(row.get('pt_duedate')) or None,
                supplier_ref=_str(row.get('pt_yrref')) or None,
                unique_id=_str(row.get('pt_unique')) or None,
            ))
        return out

    def get_all_transaction_refs(self, account_code: str) -> List[TransactionRef]:
        out: List[TransactionRef] = []
        for row in self._ptran_for(account_code):
            value = _num(row.get('pt_trvalue'))
            out.append(TransactionRef(
                reference=_str(row.get('pt_trref')),
                date=_date_str(row.get('pt_trdate')),
                abs_value=abs(value),
            ))
        return out

    def get_outstanding_invoices_due_by(
        self, account_code: str, due_date: str,
    ) -> List[OutstandingTransaction]:
        if isinstance(due_date, str):
            try:
                due = datetime.strptime(due_date[:10], '%Y-%m-%d').date()
            except ValueError:
                return []
        else:
            due = due_date
        out: List[OutstandingTransaction] = []
        for t in self.get_outstanding_transactions(account_code):
            if t.type_code != 'I' or t.balance == 0:
                continue
            t_due = t.due_date or t.date
            if not t_due:
                continue
            try:
                t_due_date = datetime.strptime(t_due[:10], '%Y-%m-%d').date()
            except ValueError:
                continue
            if t_due_date <= due:
                out.append(t)
        return out

    def get_recent_payments(
        self, account_code: str, since_days: int = 90,
    ) -> List[OutstandingTransaction]:
        cutoff = date.today() - timedelta(days=since_days)
        out: List[OutstandingTransaction] = []
        for row in self._ptran_for(account_code):
            tr_type = _str(row.get('pt_trtype'))
            if tr_type != 'P':
                continue
            d_str = _date_str(row.get('pt_trdate'))
            if not d_str:
                continue
            try:
                d = datetime.strptime(d_str[:10], '%Y-%m-%d').date()
            except ValueError:
                continue
            if d < cutoff:
                continue
            out.append(OutstandingTransaction(
                reference=_str(row.get('pt_trref')),
                date=d_str,
                type_code='P',
                balance=_num(row.get('pt_trbal')),
                value=_num(row.get('pt_trvalue')),
                due_date=_date_str(row.get('pt_duedate')) or None,
            ))
        return out

    def get_outstanding_balance(self, account_code: str) -> float:
        return sum(t.balance for t in self.get_outstanding_transactions(account_code))

    def get_outstanding_invoice_total(self, account_code: str) -> float:
        return sum(
            t.balance for t in self.get_outstanding_transactions(account_code)
            if t.balance > 0
        )

    def get_unallocated_payment_total(self, account_code: str) -> float:
        return sum(
            t.balance for t in self.get_outstanding_transactions(account_code)
            if t.balance < 0
        )

    def get_payment_terms_days(self, account_code: str) -> int:
        info = self.get_supplier(account_code)
        return info.payment_terms_days if info else 30

    def check_invoice_exists(
        self, account_code: str, reference: str, amount: float,
    ) -> Optional[str]:
        ref_target = (reference or '').strip().upper()
        amount_target = abs(float(amount))
        for row in self._ptran_for(account_code):
            tr_type = _str(row.get('pt_trtype'))
            if tr_type != 'I':
                continue
            tr_ref = _str(row.get('pt_trref')).upper()
            value = abs(_num(row.get('pt_trvalue')))
            if (
                (ref_target and tr_ref == ref_target)
                or abs(value - amount_target) < 0.01
            ):
                return _str(row.get('pt_unique')) or tr_ref
        return None
