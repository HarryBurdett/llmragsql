"""
Opera SQL SE Supplier Data Provider.

Implements SupplierDataProvider for Opera SQL SE databases using SQLConnector.
All Opera-specific SQL for the supplier reconciliation system lives here.

Opera 3 Parity:
    A parallel implementation (supplier_data_opera3.py) is required for
    Opera 3 FoxPro support. It should query the same DBF tables (pname,
    ptran, pterms, zcontacts) with identical column names.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional

from sql_rag.supplier_data_provider import (
    SupplierDataProvider,
    SupplierInfo,
    SupplierContact,
    OutstandingTransaction,
    TransactionRef,
)

logger = logging.getLogger(__name__)


class OperaSESupplierDataProvider(SupplierDataProvider):
    """
    Supplier data provider for Opera SQL SE.

    Uses SQL Server queries with WITH (NOLOCK) hints for read-only access.
    """

    def __init__(self, sql_connector):
        """
        Initialise with a SQLConnector instance.

        Args:
            sql_connector: SQLConnector configured for the Opera SQL SE database.
        """
        self.sql = sql_connector

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _execute_query(self, query: str):
        """Execute query and return the DataFrame result."""
        return self.sql.execute_query(query)

    def _safe_str(self, value, default: str = '') -> str:
        """Safely convert a value to a stripped string."""
        if value is None:
            return default
        return str(value).strip()

    def _safe_float(self, value, default: float = 0.0) -> float:
        """Safely convert a value to float."""
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    # ------------------------------------------------------------------
    # Supplier master data
    # ------------------------------------------------------------------

    def get_all_suppliers(self) -> List[SupplierInfo]:
        """Get all non-dormant suppliers from pname."""
        df = self._execute_query("""
            SELECT
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS name,
                pn_currbal,
                RTRIM(ISNULL(pn_paymeth, '')) AS paymeth,
                pn_dormant
            FROM pname WITH (NOLOCK)
            WHERE pn_dormant = 0 OR pn_dormant IS NULL
            ORDER BY pn_name
        """)
        if df is None or df.empty:
            return []

        result = []
        for _, row in df.iterrows():
            result.append(SupplierInfo(
                account_code=self._safe_str(row.get('account')),
                name=self._safe_str(row.get('name')),
                balance=self._safe_float(row.get('pn_currbal')),
                payment_method=self._safe_str(row.get('paymeth')),
                is_dormant=False,
            ))
        return result

    def get_supplier(self, account_code: str) -> Optional[SupplierInfo]:
        """Get a single supplier's info from pname."""
        account_code_safe = account_code.replace("'", "''")
        df = self._execute_query(f"""
            SELECT
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS name,
                pn_currbal,
                RTRIM(ISNULL(pn_paymeth, '')) AS paymeth,
                ISNULL(pn_dormant, 0) AS dormant
            FROM pname WITH (NOLOCK)
            WHERE pn_account = '{account_code_safe}'
        """)
        if df is None or df.empty:
            return None

        row = df.iloc[0]
        return SupplierInfo(
            account_code=self._safe_str(row.get('account')),
            name=self._safe_str(row.get('name')),
            balance=self._safe_float(row.get('pn_currbal')),
            payment_method=self._safe_str(row.get('paymeth')),
            is_dormant=bool(row.get('dormant', 0)),
        )

    def get_supplier_name(self, account_code: str) -> Optional[str]:
        """Get just the supplier name from pname."""
        account_code_safe = account_code.replace("'", "''")
        df = self._execute_query(f"""
            SELECT RTRIM(pn_name) AS name
            FROM pname WITH (NOLOCK)
            WHERE pn_account = '{account_code_safe}'
        """)
        if df is None or df.empty:
            return None
        return self._safe_str(df.iloc[0].get('name'))

    def get_supplier_balance(self, account_code: str) -> Optional[float]:
        """Get the current balance from pname.pn_currbal."""
        account_code_safe = account_code.replace("'", "''")
        df = self._execute_query(f"""
            SELECT pn_currbal
            FROM pname WITH (NOLOCK)
            WHERE pn_account = '{account_code_safe}'
        """)
        if df is None or df.empty:
            return None
        return self._safe_float(df.iloc[0].get('pn_currbal'))

    # ------------------------------------------------------------------
    # Contacts
    # ------------------------------------------------------------------

    def get_supplier_contact(self, account_code: str) -> Optional[SupplierContact]:
        """Get the primary contact from zcontacts (module 'P' for purchase)."""
        account_code_safe = account_code.replace("'", "''")
        df = self._execute_query(f"""
            SELECT TOP 1
                RTRIM(zc_contact) AS name,
                RTRIM(ISNULL(zc_email, '')) AS email,
                RTRIM(ISNULL(zc_phone, '')) AS phone,
                RTRIM(ISNULL(zc_mobile, '')) AS mobile,
                RTRIM(ISNULL(zc_pos, '')) AS position
            FROM zcontacts WITH (NOLOCK)
            WHERE zc_account = '{account_code_safe}' AND zc_module = 'P'
            ORDER BY zc_contact
        """)
        if df is None or df.empty:
            return None

        row = df.iloc[0]
        name = self._safe_str(row.get('name'))
        email = self._safe_str(row.get('email'))

        # Must have at least a name to be useful
        if not name and not email:
            return None

        return SupplierContact(
            name=name,
            email=email,
            phone=self._safe_str(row.get('phone')),
            mobile=self._safe_str(row.get('mobile')),
            position=self._safe_str(row.get('position')),
        )

    def verify_sender(self, account_code: str, email: str) -> bool:
        """Check zcontacts for a matching email address (module 'P')."""
        account_code_safe = account_code.replace("'", "''")
        email_lower = email.strip().lower()

        try:
            df = self._execute_query(f"""
                SELECT RTRIM(zc_email) AS email
                FROM zcontacts WITH (NOLOCK)
                WHERE zc_account = '{account_code_safe}' AND zc_module = 'P'
                  AND zc_email IS NOT NULL AND RTRIM(zc_email) != ''
            """)
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    contact_email = self._safe_str(row.get('email')).lower()
                    if contact_email and contact_email == email_lower:
                        return True
        except Exception as exc:
            logger.warning(
                "Could not query zcontacts for supplier %s: %s",
                account_code, exc
            )

        return False

    # ------------------------------------------------------------------
    # Transactions
    # ------------------------------------------------------------------

    def get_outstanding_transactions(self, account_code: str) -> List[OutstandingTransaction]:
        """Get all outstanding (unallocated) transactions from ptran."""
        account_code_safe = account_code.replace("'", "''")
        df = self._execute_query(f"""
            SELECT
                RTRIM(pt_trref) AS reference,
                pt_trdate,
                pt_trtype,
                pt_trbal AS balance,
                pt_trvalue AS value,
                pt_dueday,
                RTRIM(ISNULL(pt_supref, '')) AS supplier_ref,
                pt_unique
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{account_code_safe}' AND pt_trbal <> 0
            ORDER BY pt_trdate
        """)
        if df is None or df.empty:
            return []

        result = []
        for _, row in df.iterrows():
            date_val = row.get('pt_trdate')
            due_val = row.get('pt_dueday')
            result.append(OutstandingTransaction(
                reference=self._safe_str(row.get('reference')),
                date=str(date_val)[:10] if date_val else '',
                type_code=self._safe_str(row.get('pt_trtype')),
                balance=self._safe_float(row.get('balance')),
                value=self._safe_float(row.get('value')),
                due_date=str(due_val)[:10] if due_val else None,
                supplier_ref=self._safe_str(row.get('supplier_ref')),
                unique_id=str(row.get('pt_unique')) if row.get('pt_unique') else None,
            ))
        return result

    def get_all_transaction_refs(self, account_code: str) -> List[TransactionRef]:
        """Get all transaction references (including settled) for existence checking."""
        account_code_safe = account_code.replace("'", "''")
        df = self._execute_query(f"""
            SELECT
                RTRIM(pt_trref) AS reference,
                pt_trdate,
                ABS(pt_trvalue) AS abs_value
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{account_code_safe}'
        """)
        if df is None or df.empty:
            return []

        result = []
        for _, row in df.iterrows():
            date_val = row.get('pt_trdate')
            result.append(TransactionRef(
                reference=self._safe_str(row.get('reference')),
                date=str(date_val)[:10] if date_val else '',
                abs_value=round(self._safe_float(row.get('abs_value')), 2),
            ))
        return result

    def get_outstanding_invoices_due_by(self, account_code: str, due_date: str) -> List[OutstandingTransaction]:
        """Get outstanding invoices due on or before the given date."""
        account_code_safe = account_code.replace("'", "''")
        due_date_safe = due_date.replace("'", "''")
        df = self._execute_query(f"""
            SELECT
                RTRIM(pt_trref) AS reference,
                pt_trdate,
                pt_trtype,
                pt_trbal AS balance,
                pt_trvalue AS value,
                pt_dueday,
                RTRIM(ISNULL(pt_supref, '')) AS supplier_ref,
                pt_unique
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{account_code_safe}'
              AND pt_trtype = 'I'
              AND pt_trbal > 0
              AND pt_dueday IS NOT NULL
              AND pt_dueday <= '{due_date_safe}'
            ORDER BY pt_dueday
        """)
        if df is None or df.empty:
            return []

        result = []
        for _, row in df.iterrows():
            date_val = row.get('pt_trdate')
            due_val = row.get('pt_dueday')
            result.append(OutstandingTransaction(
                reference=self._safe_str(row.get('reference')),
                date=str(date_val)[:10] if date_val else '',
                type_code=self._safe_str(row.get('pt_trtype')),
                balance=self._safe_float(row.get('balance')),
                value=self._safe_float(row.get('value')),
                due_date=str(due_val)[:10] if due_val else None,
                supplier_ref=self._safe_str(row.get('supplier_ref')),
                unique_id=str(row.get('pt_unique')) if row.get('pt_unique') else None,
            ))
        return result

    def get_recent_payments(self, account_code: str, since_days: int = 90) -> List[OutstandingTransaction]:
        """Get recent payments made to a supplier."""
        account_code_safe = account_code.replace("'", "''")
        cutoff = (datetime.now() - timedelta(days=since_days)).strftime('%Y-%m-%d')
        df = self._execute_query(f"""
            SELECT
                RTRIM(pt_trref) AS reference,
                pt_trdate,
                pt_trtype,
                pt_trbal AS balance,
                pt_trvalue AS value,
                ABS(pt_trvalue) AS abs_amount,
                RTRIM(ISNULL(pt_supref, '')) AS supplier_ref,
                pt_unique
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{account_code_safe}' AND pt_trtype = 'P'
              AND pt_trdate >= '{cutoff}'
            ORDER BY pt_trdate DESC
        """)
        if df is None or df.empty:
            return []

        result = []
        for _, row in df.iterrows():
            date_val = row.get('pt_trdate')
            result.append(OutstandingTransaction(
                reference=self._safe_str(row.get('reference')),
                date=str(date_val)[:10] if date_val else '',
                type_code='P',
                balance=self._safe_float(row.get('balance')),
                value=self._safe_float(row.get('value')),
                supplier_ref=self._safe_str(row.get('supplier_ref')),
                unique_id=str(row.get('pt_unique')) if row.get('pt_unique') else None,
            ))
        return result

    # ------------------------------------------------------------------
    # Balance aggregates
    # ------------------------------------------------------------------

    def get_outstanding_balance(self, account_code: str) -> float:
        """Get the sum of all outstanding transaction balances (ptran where pt_trbal <> 0)."""
        account_code_safe = account_code.replace("'", "''")
        df = self._execute_query(f"""
            SELECT SUM(pt_trbal) AS bal
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{account_code_safe}'
        """)
        if df is None or df.empty:
            return 0.0
        val = df.iloc[0].get('bal')
        return self._safe_float(val)

    def get_outstanding_invoice_total(self, account_code: str) -> float:
        """Get total of outstanding invoices (positive balance, type I)."""
        account_code_safe = account_code.replace("'", "''")
        df = self._execute_query(f"""
            SELECT SUM(pt_trbal) AS inv_total
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{account_code_safe}' AND pt_trtype = 'I' AND pt_trbal > 0
        """)
        if df is None or df.empty:
            return 0.0
        val = df.iloc[0].get('inv_total')
        return self._safe_float(val)

    def get_unallocated_payment_total(self, account_code: str) -> float:
        """Get total of unallocated payments (absolute value of negative balance, type P)."""
        account_code_safe = account_code.replace("'", "''")
        df = self._execute_query(f"""
            SELECT SUM(ABS(pt_trbal)) AS pay_total
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{account_code_safe}' AND pt_trtype = 'P' AND pt_trbal < 0
        """)
        if df is None or df.empty:
            return 0.0
        val = df.iloc[0].get('pay_total')
        return self._safe_float(val)

    # ------------------------------------------------------------------
    # Supplier lookup / matching
    # ------------------------------------------------------------------

    def find_supplier_by_name(self, name: str, account_ref: Optional[str] = None) -> Optional[SupplierInfo]:
        """
        Find a supplier by account reference or name.

        Priority:
        1. Exact account code match (most reliable)
        2. Fuzzy name match (LIKE on pn_name)
        """
        # Try by account reference first
        if account_ref:
            account_ref_safe = account_ref.replace("'", "''").strip()
            df = self._execute_query(f"""
                SELECT
                    RTRIM(pn_account) AS account,
                    RTRIM(pn_name) AS name,
                    pn_currbal,
                    RTRIM(ISNULL(pn_paymeth, '')) AS paymeth,
                    ISNULL(pn_dormant, 0) AS dormant
                FROM pname WITH (NOLOCK)
                WHERE pn_account = '{account_ref_safe}'
            """)
            if df is not None and not df.empty:
                row = df.iloc[0]
                return SupplierInfo(
                    account_code=self._safe_str(row.get('account')),
                    name=self._safe_str(row.get('name')),
                    balance=self._safe_float(row.get('pn_currbal')),
                    payment_method=self._safe_str(row.get('paymeth')),
                    is_dormant=bool(row.get('dormant', 0)),
                )

        # Try by name match (case-insensitive)
        if name:
            clean_name = name.replace("'", "''").strip()
            first_word = clean_name.split()[0] if clean_name else ''
            df = self._execute_query(f"""
                SELECT
                    RTRIM(pn_account) AS account,
                    RTRIM(pn_name) AS name,
                    pn_currbal,
                    RTRIM(ISNULL(pn_paymeth, '')) AS paymeth,
                    ISNULL(pn_dormant, 0) AS dormant
                FROM pname WITH (NOLOCK)
                WHERE UPPER(RTRIM(pn_name)) LIKE UPPER('%{clean_name}%')
                   OR UPPER(RTRIM(pn_name)) LIKE UPPER('%{first_word}%')
            """)
            if df is not None and not df.empty:
                row = df.iloc[0]
                return SupplierInfo(
                    account_code=self._safe_str(row.get('account')),
                    name=self._safe_str(row.get('name')),
                    balance=self._safe_float(row.get('pn_currbal')),
                    payment_method=self._safe_str(row.get('paymeth')),
                    is_dormant=bool(row.get('dormant', 0)),
                )

        return None

    # ------------------------------------------------------------------
    # Payment terms
    # ------------------------------------------------------------------

    def get_payment_terms_days(self, account_code: str) -> int:
        """
        Resolve payment terms days for a supplier.

        Checks for an account-specific override in pterms first;
        falls back to the supplier's terms profile, then defaults to 30.
        """
        account_code_safe = account_code.replace("'", "''")
        try:
            # Account-specific override
            df = self._execute_query(f"""
                SELECT TOP 1 pr_termday
                FROM pterms WITH (NOLOCK)
                WHERE pr_account = '{account_code_safe}'
                ORDER BY id
            """)
            if df is not None and not df.empty:
                val = df.iloc[0].get('pr_termday')
                if val is not None:
                    return int(val)

            # Get the supplier's terms profile
            prof_df = self._execute_query(f"""
                SELECT RTRIM(ISNULL(pn_tprfl, '')) AS tprfl
                FROM pname WITH (NOLOCK)
                WHERE pn_account = '{account_code_safe}'
            """)
            if prof_df is not None and not prof_df.empty:
                terms_profile = self._safe_str(prof_df.iloc[0].get('tprfl'))
                if terms_profile:
                    terms_profile_safe = terms_profile.replace("'", "''")
                    df2 = self._execute_query(f"""
                        SELECT TOP 1 pr_termday
                        FROM pterms WITH (NOLOCK)
                        WHERE pr_code = '{terms_profile_safe}'
                        ORDER BY id
                    """)
                    if df2 is not None and not df2.empty:
                        val = df2.iloc[0].get('pr_termday')
                        if val is not None:
                            return int(val)
        except Exception as exc:
            logger.warning(
                "Could not resolve payment terms for %s: %s",
                account_code, exc
            )

        return 30  # sensible default

    # ------------------------------------------------------------------
    # Invoice existence check
    # ------------------------------------------------------------------

    def check_invoice_exists(self, account_code: str, reference: str, amount: float) -> Optional[str]:
        """
        Check if an invoice exists for this supplier.

        Matches by reference (pt_trref or pt_supref LIKE) OR by amount.
        Returns the pt_unique if found, None otherwise.
        """
        account_code_safe = account_code.replace("'", "''")
        reference_safe = reference.replace("'", "''")
        try:
            df = self._execute_query(f"""
                SELECT TOP 1 pt_unique, pt_trref, pt_trvalue
                FROM ptran WITH (NOLOCK)
                WHERE pt_account = '{account_code_safe}'
                  AND pt_trtype = 'I'
                  AND (
                      pt_trref LIKE '%{reference_safe}%'
                      OR pt_supref LIKE '%{reference_safe}%'
                      OR ABS(pt_trvalue - {amount}) < 0.01
                  )
                ORDER BY pt_trdate DESC
            """)
            if df is not None and not df.empty:
                return str(df.iloc[0].get('pt_unique'))
        except Exception as exc:
            logger.warning(
                "Could not check invoice existence for %s/%s: %s",
                account_code, reference, exc
            )

        return None
