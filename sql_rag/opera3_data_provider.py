"""
Opera 3 FoxPro Data Provider

Implements OperaDataProvider for Opera 3 FoxPro DBF files using Opera3Reader.
Performs aggregations in Python since DBF files have no SQL-like aggregation capability.
"""

from typing import List, Dict, Optional, Any
from datetime import date, datetime
from collections import defaultdict
import logging

from sql_rag.opera_data_provider import OperaDataProvider
from sql_rag.opera3_foxpro import Opera3Reader

logger = logging.getLogger(__name__)


class Opera3DataProvider(OperaDataProvider):
    """
    Data provider for Opera 3 FoxPro DBF files.

    Implements Python-based aggregation since DBF files don't support SQL operations.
    Field names from Opera3Reader are UPPERCASE (e.g., SN_ACCOUNT, NT_VALUE).
    """

    TYPE_NAMES = {
        'A': 'Fixed Assets',
        'B': 'Current Assets',
        'C': 'Current Liabilities',
        'D': 'Capital & Reserves',
        'E': 'Sales',
        'F': 'Cost of Sales',
        'G': 'Overheads',
        'H': 'Other'
    }

    MONTH_NAMES = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    def __init__(self, data_path: str):
        """
        Initialize with path to Opera 3 company data folder.

        Args:
            data_path: Path to Opera 3 company data folder containing DBF files
        """
        self.reader = Opera3Reader(data_path)
        self._cache = {}  # Simple cache for frequently accessed tables

    def _get_field(self, record: Dict, field: str, default: Any = None) -> Any:
        """Get field value, trying both upper and lower case."""
        return record.get(field.upper(), record.get(field.lower(), default))

    def _get_str(self, record: Dict, field: str) -> str:
        """Get string field value, stripped."""
        val = self._get_field(record, field, '')
        return str(val).strip() if val else ''

    def _get_num(self, record: Dict, field: str) -> float:
        """Get numeric field value."""
        val = self._get_field(record, field, 0)
        try:
            return float(val) if val else 0.0
        except (TypeError, ValueError):
            return 0.0

    def _get_int(self, record: Dict, field: str) -> int:
        """Get integer field value."""
        return int(self._get_num(record, field))

    def _parse_date(self, val: Any) -> Optional[date]:
        """Parse date from various formats."""
        if val is None:
            return None
        if isinstance(val, date):
            return val
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, str):
            try:
                return date.fromisoformat(val)
            except ValueError:
                return None
        return None

    def _read_table_safe(self, table_name: str) -> List[Dict]:
        """Read table with error handling for missing tables."""
        try:
            return self.reader.read_table(table_name)
        except FileNotFoundError:
            logger.warning(f"Table {table_name} not found in Opera 3 data")
            return []
        except Exception as e:
            logger.error(f"Error reading table {table_name}: {e}")
            return []

    # =========================================================================
    # Customer / Sales Ledger Methods
    # =========================================================================

    def get_customers(self, active_only: bool = True) -> List[Dict]:
        """Get customer master records."""
        records = self._read_table_safe("sname")
        customers = []
        for r in records:
            dormant = self._get_int(r, 'SN_DORMANT')
            if active_only and dormant == 1:
                continue
            customers.append({
                'account': self._get_str(r, 'SN_ACCOUNT'),
                'name': self._get_str(r, 'SN_NAME'),
                'balance': self._get_num(r, 'SN_CURRBAL'),
                'credit_limit': self._get_num(r, 'SN_CRLIM'),
                'on_stop': self._get_int(r, 'SN_STOP'),
                'dormant': dormant,
                'phone': self._get_str(r, 'SN_TELENO'),
                'contact': self._get_str(r, 'SN_CONTACT'),
                'email': self._get_str(r, 'SN_EMAIL')
            })
        return customers

    def get_customer_balances(self) -> List[Dict]:
        """Get customers with non-zero balances."""
        records = self._read_table_safe("sname")
        return [
            {
                'account': self._get_str(r, 'SN_ACCOUNT'),
                'name': self._get_str(r, 'SN_NAME'),
                'balance': self._get_num(r, 'SN_CURRBAL'),
                'credit_limit': self._get_num(r, 'SN_CRLIM'),
                'on_stop': self._get_int(r, 'SN_STOP')
            }
            for r in records
            if self._get_num(r, 'SN_CURRBAL') != 0
        ]

    def get_customer_aging(self, account: Optional[str] = None) -> List[Dict]:
        """Get aged debtors breakdown."""
        sname = self._read_table_safe("sname")
        shist = self._read_table_safe("shist")

        # Build aging lookup from shist (si_age = 1 is most recent)
        aging_lookup = {}
        for r in shist:
            if self._get_int(r, 'SI_AGE') == 1:
                acct = self._get_str(r, 'SI_ACCOUNT')
                aging_lookup[acct] = {
                    'current': self._get_num(r, 'SI_CURRENT'),
                    'period1': self._get_num(r, 'SI_PERIOD1'),
                    'period2': self._get_num(r, 'SI_PERIOD2'),
                    'period3': self._get_num(r, 'SI_PERIOD3'),
                    'period4': self._get_num(r, 'SI_PERIOD4'),
                    'period5': self._get_num(r, 'SI_PERIOD5')
                }

        result = []
        for r in sname:
            balance = self._get_num(r, 'SN_CURRBAL')
            if balance == 0:
                continue

            acct = self._get_str(r, 'SN_ACCOUNT')
            if account and acct.upper() != account.upper():
                continue

            aging = aging_lookup.get(acct, {})
            month3_plus = (
                aging.get('period3', 0) +
                aging.get('period4', 0) +
                aging.get('period5', 0)
            )

            result.append({
                'account': acct,
                'name': self._get_str(r, 'SN_NAME'),
                'balance': balance,
                'current': aging.get('current', 0),
                'month1': aging.get('period1', 0),
                'month2': aging.get('period2', 0),
                'month3_plus': month3_plus,
                'credit_limit': self._get_num(r, 'SN_CRLIM'),
                'phone': self._get_str(r, 'SN_TELENO'),
                'contact': self._get_str(r, 'SN_CONTACT'),
                'on_stop': self._get_int(r, 'SN_STOP')
            })

        return sorted(result, key=lambda x: x['account'])

    def get_sales_transactions(
        self,
        account: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        outstanding_only: bool = False
    ) -> List[Dict]:
        """Get sales ledger transactions."""
        records = self._read_table_safe("stran")
        result = []

        for r in records:
            # Apply filters
            acct = self._get_str(r, 'ST_ACCOUNT')
            if account and acct.upper() != account.upper():
                continue

            balance = self._get_num(r, 'ST_TRBAL')
            if outstanding_only and balance == 0:
                continue

            tr_date = self._parse_date(self._get_field(r, 'ST_TRDATE'))
            if from_date and tr_date and tr_date < from_date:
                continue
            if to_date and tr_date and tr_date > to_date:
                continue

            result.append({
                'account': acct,
                'type': self._get_str(r, 'ST_TRTYPE'),
                'date': tr_date.isoformat() if tr_date else None,
                'reference': self._get_str(r, 'ST_TRREF'),
                'value': self._get_num(r, 'ST_TRVALUE'),
                'balance': balance,
                'due_date': self._parse_date(self._get_field(r, 'ST_DUEDAY'))
            })

        return sorted(result, key=lambda x: x['date'] or '', reverse=True)

    def get_credit_control_metrics(self) -> Dict:
        """Get credit control dashboard metrics."""
        sname = self._read_table_safe("sname")
        stran = self._read_table_safe("stran")
        today = date.today()

        metrics = {}

        # Total outstanding balance
        customers_with_balance = [r for r in sname if self._get_num(r, 'SN_CURRBAL') > 0]
        metrics["total_debt"] = {
            "value": sum(self._get_num(r, 'SN_CURRBAL') for r in customers_with_balance),
            "count": len(customers_with_balance),
            "label": "Total Outstanding"
        }

        # Over credit limit
        over_limit = [
            r for r in sname
            if self._get_num(r, 'SN_CURRBAL') > self._get_num(r, 'SN_CRLIM') > 0
        ]
        metrics["over_credit_limit"] = {
            "value": sum(
                self._get_num(r, 'SN_CURRBAL') - self._get_num(r, 'SN_CRLIM')
                for r in over_limit
            ),
            "count": len(over_limit),
            "label": "Over Credit Limit"
        }

        # Accounts on stop
        on_stop = [r for r in sname if self._get_int(r, 'SN_STOP') == 1]
        metrics["accounts_on_stop"] = {
            "value": sum(self._get_num(r, 'SN_CURRBAL') for r in on_stop),
            "count": len(on_stop),
            "label": "Accounts On Stop"
        }

        # Overdue invoices
        overdue = [
            r for r in stran
            if (self._get_str(r, 'ST_TRTYPE') == 'I' and
                self._get_num(r, 'ST_TRBAL') > 0 and
                self._parse_date(self._get_field(r, 'ST_DUEDAY')) and
                self._parse_date(self._get_field(r, 'ST_DUEDAY')) < today)
        ]
        metrics["overdue_invoices"] = {
            "value": sum(self._get_num(r, 'ST_TRBAL') for r in overdue),
            "count": len(overdue),
            "label": "Overdue Invoices"
        }

        # Recent payments (last 7 days)
        seven_days_ago = date(today.year, today.month, today.day)
        try:
            from datetime import timedelta
            seven_days_ago = today - timedelta(days=7)
        except Exception:
            pass

        recent_payments = [
            r for r in stran
            if (self._get_str(r, 'ST_TRTYPE') == 'R' and
                self._parse_date(self._get_field(r, 'ST_TRDATE')) and
                self._parse_date(self._get_field(r, 'ST_TRDATE')) >= seven_days_ago)
        ]
        metrics["recent_payments"] = {
            "value": sum(abs(self._get_num(r, 'ST_TRVALUE')) for r in recent_payments),
            "count": len(recent_payments),
            "label": "Payments (7 days)"
        }

        # Promises due
        promises = [
            r for r in stran
            if (self._parse_date(self._get_field(r, 'ST_PAYDAY')) and
                self._parse_date(self._get_field(r, 'ST_PAYDAY')) <= today and
                self._get_num(r, 'ST_TRBAL') > 0)
        ]
        metrics["promises_due"] = {
            "value": sum(self._get_num(r, 'ST_TRBAL') for r in promises),
            "count": len(promises),
            "label": "Promises Due"
        }

        # Disputed invoices
        disputed = [
            r for r in stran
            if self._get_int(r, 'ST_DISPUTE') == 1 and self._get_num(r, 'ST_TRBAL') > 0
        ]
        metrics["disputed"] = {
            "value": sum(self._get_num(r, 'ST_TRBAL') for r in disputed),
            "count": len(disputed),
            "label": "In Dispute"
        }

        # Unallocated cash
        unallocated = [
            r for r in stran
            if self._get_str(r, 'ST_TRTYPE') == 'R' and self._get_num(r, 'ST_TRBAL') < 0
        ]
        metrics["unallocated_cash"] = {
            "value": sum(abs(self._get_num(r, 'ST_TRBAL')) for r in unallocated),
            "count": len(unallocated),
            "label": "Unallocated Cash"
        }

        return metrics

    def get_priority_customers(self, limit: int = 10) -> List[Dict]:
        """Get customers needing attention."""
        sname = self._read_table_safe("sname")

        priority = []
        for r in sname:
            balance = self._get_num(r, 'SN_CURRBAL')
            if balance <= 0:
                continue

            on_stop = self._get_int(r, 'SN_STOP') == 1
            credit_limit = self._get_num(r, 'SN_CRLIM')
            over_limit = balance > credit_limit > 0

            if not on_stop and not over_limit:
                continue

            reason = 'ON_STOP' if on_stop else 'OVER_LIMIT' if over_limit else 'HIGH_BALANCE'

            priority.append({
                'account': self._get_str(r, 'SN_ACCOUNT'),
                'customer': self._get_str(r, 'SN_NAME'),
                'balance': balance,
                'credit_limit': credit_limit,
                'phone': self._get_str(r, 'SN_TELENO'),
                'contact': self._get_str(r, 'SN_CONTACT'),
                'priority_reason': reason
            })

        # Sort by balance descending and limit
        priority.sort(key=lambda x: x['balance'], reverse=True)
        return priority[:limit]

    # =========================================================================
    # Supplier / Purchase Ledger Methods
    # =========================================================================

    def get_suppliers(self, active_only: bool = True) -> List[Dict]:
        """Get supplier master records."""
        records = self._read_table_safe("pname")
        return [
            {
                'account': self._get_str(r, 'PN_ACCOUNT'),
                'name': self._get_str(r, 'PN_NAME'),
                'balance': self._get_num(r, 'PN_CURRBAL'),
                'phone': self._get_str(r, 'PN_TELENO'),
                'contact': self._get_str(r, 'PN_CONTACT')
            }
            for r in records
        ]

    def get_supplier_balances(self) -> List[Dict]:
        """Get suppliers with non-zero balances."""
        records = self._read_table_safe("pname")
        return [
            {
                'account': self._get_str(r, 'PN_ACCOUNT'),
                'name': self._get_str(r, 'PN_NAME'),
                'balance': self._get_num(r, 'PN_CURRBAL'),
                'phone': self._get_str(r, 'PN_TELENO'),
                'contact': self._get_str(r, 'PN_CONTACT')
            }
            for r in records
            if self._get_num(r, 'PN_CURRBAL') != 0
        ]

    def get_supplier_aging(self, account: Optional[str] = None) -> List[Dict]:
        """Get aged creditors breakdown."""
        pname = self._read_table_safe("pname")
        phist = self._read_table_safe("phist")

        # Build aging lookup from phist (pi_age = 1 is most recent)
        aging_lookup = {}
        for r in phist:
            if self._get_int(r, 'PI_AGE') == 1:
                acct = self._get_str(r, 'PI_ACCOUNT')
                aging_lookup[acct] = {
                    'current': self._get_num(r, 'PI_CURRENT'),
                    'period1': self._get_num(r, 'PI_PERIOD1'),
                    'period2': self._get_num(r, 'PI_PERIOD2'),
                    'period3': self._get_num(r, 'PI_PERIOD3'),
                    'period4': self._get_num(r, 'PI_PERIOD4'),
                    'period5': self._get_num(r, 'PI_PERIOD5')
                }

        result = []
        for r in pname:
            balance = self._get_num(r, 'PN_CURRBAL')
            if balance == 0:
                continue

            acct = self._get_str(r, 'PN_ACCOUNT')
            if account and acct.upper() != account.upper():
                continue

            aging = aging_lookup.get(acct, {})
            month3_plus = (
                aging.get('period3', 0) +
                aging.get('period4', 0) +
                aging.get('period5', 0)
            )

            result.append({
                'account': acct,
                'name': self._get_str(r, 'PN_NAME'),
                'balance': balance,
                'current': aging.get('current', 0),
                'month1': aging.get('period1', 0),
                'month2': aging.get('period2', 0),
                'month3_plus': month3_plus,
                'phone': self._get_str(r, 'PN_TELENO'),
                'contact': self._get_str(r, 'PN_CONTACT')
            })

        return sorted(result, key=lambda x: x['account'])

    def get_purchase_transactions(
        self,
        account: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        outstanding_only: bool = False
    ) -> List[Dict]:
        """Get purchase ledger transactions."""
        records = self._read_table_safe("ptran")
        result = []

        for r in records:
            acct = self._get_str(r, 'PT_ACCOUNT')
            if account and acct.upper() != account.upper():
                continue

            balance = self._get_num(r, 'PT_TRBAL')
            if outstanding_only and balance == 0:
                continue

            tr_date = self._parse_date(self._get_field(r, 'PT_TRDATE'))
            if from_date and tr_date and tr_date < from_date:
                continue
            if to_date and tr_date and tr_date > to_date:
                continue

            result.append({
                'account': acct,
                'type': self._get_str(r, 'PT_TRTYPE'),
                'date': tr_date.isoformat() if tr_date else None,
                'reference': self._get_str(r, 'PT_TRREF'),
                'value': self._get_num(r, 'PT_TRVALUE'),
                'balance': balance,
                'due_date': self._parse_date(self._get_field(r, 'PT_DUEDAY'))
            })

        return sorted(result, key=lambda x: x['date'] or '', reverse=True)

    def get_creditors_metrics(self) -> Dict:
        """Get creditors control dashboard metrics."""
        pname = self._read_table_safe("pname")
        ptran = self._read_table_safe("ptran")
        today = date.today()

        from datetime import timedelta
        seven_days_from_now = today + timedelta(days=7)
        thirty_days_from_now = today + timedelta(days=30)
        seven_days_ago = today - timedelta(days=7)

        metrics = {}

        # Total creditors balance
        suppliers_with_balance = [r for r in pname if self._get_num(r, 'PN_CURRBAL') != 0]
        metrics["total_creditors"] = {
            "value": sum(self._get_num(r, 'PN_CURRBAL') for r in suppliers_with_balance),
            "count": len(suppliers_with_balance),
            "label": "Total Outstanding"
        }

        # Overdue invoices
        overdue = [
            r for r in ptran
            if (self._get_str(r, 'PT_TRTYPE') == 'I' and
                self._get_num(r, 'PT_TRBAL') > 0 and
                self._parse_date(self._get_field(r, 'PT_DUEDAY')) and
                self._parse_date(self._get_field(r, 'PT_DUEDAY')) < today)
        ]
        metrics["overdue_invoices"] = {
            "value": sum(self._get_num(r, 'PT_TRBAL') for r in overdue),
            "count": len(overdue),
            "label": "Overdue Invoices"
        }

        # Due within 7 days
        due_7 = [
            r for r in ptran
            if (self._get_str(r, 'PT_TRTYPE') == 'I' and
                self._get_num(r, 'PT_TRBAL') > 0 and
                self._parse_date(self._get_field(r, 'PT_DUEDAY')) and
                today <= self._parse_date(self._get_field(r, 'PT_DUEDAY')) < seven_days_from_now)
        ]
        metrics["due_7_days"] = {
            "value": sum(self._get_num(r, 'PT_TRBAL') for r in due_7),
            "count": len(due_7),
            "label": "Due in 7 Days"
        }

        # Due within 30 days
        due_30 = [
            r for r in ptran
            if (self._get_str(r, 'PT_TRTYPE') == 'I' and
                self._get_num(r, 'PT_TRBAL') > 0 and
                self._parse_date(self._get_field(r, 'PT_DUEDAY')) and
                today <= self._parse_date(self._get_field(r, 'PT_DUEDAY')) < thirty_days_from_now)
        ]
        metrics["due_30_days"] = {
            "value": sum(self._get_num(r, 'PT_TRBAL') for r in due_30),
            "count": len(due_30),
            "label": "Due in 30 Days"
        }

        # Recent payments (last 7 days)
        recent_payments = [
            r for r in ptran
            if (self._get_str(r, 'PT_TRTYPE') == 'P' and
                self._parse_date(self._get_field(r, 'PT_TRDATE')) and
                self._parse_date(self._get_field(r, 'PT_TRDATE')) >= seven_days_ago)
        ]
        metrics["recent_payments"] = {
            "value": sum(abs(self._get_num(r, 'PT_TRVALUE')) for r in recent_payments),
            "count": len(recent_payments),
            "label": "Payments (7 days)"
        }

        return metrics

    def get_top_suppliers(self, limit: int = 10) -> List[Dict]:
        """Get top suppliers by balance."""
        pname = self._read_table_safe("pname")

        suppliers = [
            {
                'account': self._get_str(r, 'PN_ACCOUNT'),
                'supplier': self._get_str(r, 'PN_NAME'),
                'balance': self._get_num(r, 'PN_CURRBAL'),
                'phone': self._get_str(r, 'PN_TELENO'),
                'contact': self._get_str(r, 'PN_CONTACT')
            }
            for r in pname
            if self._get_num(r, 'PN_CURRBAL') > 0
        ]

        suppliers.sort(key=lambda x: x['balance'], reverse=True)
        return suppliers[:limit]

    # =========================================================================
    # Nominal Ledger Methods
    # =========================================================================

    def get_nominal_accounts(self) -> List[Dict]:
        """Get nominal account master records."""
        # Try nacnt first, fall back to nname
        records = self._read_table_safe("nacnt")
        if not records:
            records = self._read_table_safe("nname")

        return [
            {
                'account': self._get_str(r, 'NA_ACNT') or self._get_str(r, 'NN_ACNT'),
                'description': self._get_str(r, 'NA_DESC') or self._get_str(r, 'NN_DESC'),
                'type': self._get_str(r, 'NA_TYPE') or self._get_str(r, 'NN_TYPE'),
                'subtype': self._get_str(r, 'NA_SUBT') or self._get_str(r, 'NN_SUBT')
            }
            for r in records
        ]

    def get_nominal_trial_balance(self, year: int) -> List[Dict]:
        """Get trial balance for a financial year."""
        ntran = self._read_table_safe("ntran")
        nacnt = self._read_table_safe("nacnt")

        # Build account description/type lookup
        account_info = {}
        for r in nacnt:
            acnt = self._get_str(r, 'NA_ACNT')
            account_info[acnt] = {
                'description': self._get_str(r, 'NA_DESC'),
                'type': self._get_str(r, 'NA_TYPE'),
                'subtype': self._get_str(r, 'NA_SUBT')
            }

        # Aggregate by account for the specified year
        totals = defaultdict(lambda: {'ytd': 0.0, 'type': '', 'subtype': '', 'desc': ''})

        for r in ntran:
            if self._get_int(r, 'NT_YEAR') != year:
                continue

            acnt = self._get_str(r, 'NT_ACNT')
            value = self._get_num(r, 'NT_VALUE')

            totals[acnt]['ytd'] += value
            # Get type from transaction if available, otherwise from nacnt lookup
            totals[acnt]['type'] = self._get_str(r, 'NT_TYPE') or account_info.get(acnt, {}).get('type', '')
            totals[acnt]['subtype'] = self._get_str(r, 'NT_SUBT') or account_info.get(acnt, {}).get('subtype', '')
            totals[acnt]['desc'] = account_info.get(acnt, {}).get('description', '')

        # Format results
        result = []
        for acnt, data in totals.items():
            ytd = data['ytd']
            if ytd == 0:
                continue

            result.append({
                'account_code': acnt,
                'description': data['desc'],
                'account_type': data['type'],
                'subtype': data['subtype'],
                'opening_balance': 0,
                'ytd_movement': round(ytd, 2),
                'debit': round(ytd, 2) if ytd > 0 else 0,
                'credit': round(abs(ytd), 2) if ytd < 0 else 0
            })

        return sorted(result, key=lambda x: x['account_code'])

    def get_nominal_by_type(self, year: int, types: List[str]) -> Dict[str, float]:
        """Get nominal totals grouped by account type."""
        ntran = self._read_table_safe("ntran")

        type_totals = defaultdict(float)
        types_upper = [t.upper() for t in types]

        for r in ntran:
            if self._get_int(r, 'NT_YEAR') != year:
                continue

            nt_type = self._get_str(r, 'NT_TYPE').upper()
            if nt_type in types_upper:
                type_totals[nt_type] += self._get_num(r, 'NT_VALUE')

        return dict(type_totals)

    def get_nominal_monthly(self, year: int) -> List[Dict]:
        """Get monthly nominal breakdown for P&L accounts."""
        ntran = self._read_table_safe("ntran")

        # Aggregate by period
        monthly = defaultdict(lambda: {'revenue': 0.0, 'cost_of_sales': 0.0, 'overheads': 0.0})

        for r in ntran:
            if self._get_int(r, 'NT_YEAR') != year:
                continue

            period = self._get_int(r, 'NT_PERIOD')
            if period < 1 or period > 12:
                continue

            nt_type = self._get_str(r, 'NT_TYPE').upper()
            value = self._get_num(r, 'NT_VALUE')

            if nt_type in ('E', '30'):
                monthly[period]['revenue'] += -value  # Sales are negative/credits
            elif nt_type in ('F', '35'):
                monthly[period]['cost_of_sales'] += value
            elif nt_type in ('H', '45'):
                monthly[period]['overheads'] += value

        # Format results
        result = []
        for m in sorted(monthly.keys()):
            data = monthly[m]
            revenue = data['revenue']
            cos = data['cost_of_sales']
            overheads = data['overheads']
            gross_profit = revenue - cos
            net_profit = gross_profit - overheads

            result.append({
                'month': m,
                'month_name': self.MONTH_NAMES[m],
                'revenue': round(revenue, 2),
                'cost_of_sales': round(cos, 2),
                'gross_profit': round(gross_profit, 2),
                'overheads': round(overheads, 2),
                'net_profit': round(net_profit, 2),
                'gross_margin_percent': round(gross_profit / revenue * 100, 1) if revenue > 0 else 0
            })

        return result

    def get_finance_summary(self, year: int) -> Dict:
        """Get financial summary with P&L and Balance Sheet overview."""
        ntran = self._read_table_safe("ntran")
        nacnt = self._read_table_safe("nacnt")

        # Aggregate P&L from ntran
        pl_types = defaultdict(float)
        for r in ntran:
            if self._get_int(r, 'NT_YEAR') != year:
                continue
            nt_type = self._get_str(r, 'NT_TYPE').upper()
            if nt_type in ('30', '35', '40', '45', 'E', 'F', 'G', 'H'):
                pl_types[nt_type] += self._get_num(r, 'NT_VALUE')

        sales = pl_types.get('30', 0) + pl_types.get('E', 0)
        cos = pl_types.get('35', 0) + pl_types.get('F', 0)
        other_income = pl_types.get('40', 0) + pl_types.get('G', 0)
        overheads = pl_types.get('45', 0) + pl_types.get('H', 0)

        gross_profit = -sales - cos
        operating_profit = gross_profit + (-other_income) - overheads

        # Aggregate Balance Sheet from nacnt
        bs_types = defaultdict(float)
        type_map = {
            'A': 'Fixed Assets', '05': 'Fixed Assets',
            'B': 'Current Assets', '10': 'Current Assets',
            'C': 'Current Liabilities', '15': 'Current Liabilities',
            'D': 'Capital & Reserves', '25': 'Capital & Reserves',
            '20': 'Long Term Liabilities'
        }

        for r in nacnt:
            na_type = self._get_str(r, 'NA_TYPE').upper()
            if na_type in type_map:
                ytd_dr = self._get_num(r, 'NA_YTDDR')
                ytd_cr = self._get_num(r, 'NA_YTDCR')
                type_name = type_map[na_type]
                bs_types[type_name] += (ytd_dr - ytd_cr)

        fixed_assets = bs_types.get('Fixed Assets', 0)
        current_assets = bs_types.get('Current Assets', 0)
        current_liabilities = abs(bs_types.get('Current Liabilities', 0))
        net_current_assets = current_assets - current_liabilities

        # Ratios
        current_ratio = current_assets / current_liabilities if current_liabilities > 0 else 0
        gross_margin = (gross_profit / -sales * 100) if sales != 0 else 0
        operating_margin = (operating_profit / -sales * 100) if sales != 0 else 0

        return {
            "profit_and_loss": {
                "sales": round(-sales, 2),
                "cost_of_sales": round(cos, 2),
                "gross_profit": round(gross_profit, 2),
                "other_income": round(-other_income, 2),
                "overheads": round(overheads, 2),
                "operating_profit": round(operating_profit, 2)
            },
            "balance_sheet": {
                "fixed_assets": round(fixed_assets, 2),
                "current_assets": round(current_assets, 2),
                "current_liabilities": round(current_liabilities, 2),
                "net_current_assets": round(net_current_assets, 2),
                "total_assets": round(fixed_assets + current_assets, 2)
            },
            "ratios": {
                "gross_margin_percent": round(gross_margin, 1),
                "operating_margin_percent": round(operating_margin, 1),
                "current_ratio": round(current_ratio, 2)
            }
        }

    def get_executive_summary(self, year: int) -> Dict:
        """Get executive KPIs with YoY comparisons."""
        ntran = self._read_table_safe("ntran")

        current_date = datetime.now()
        current_month = current_date.month
        current_quarter = (current_month - 1) // 3 + 1

        # Organize revenue by year and month
        revenue_by_year = defaultdict(lambda: defaultdict(float))

        for r in ntran:
            nt_year = self._get_int(r, 'NT_YEAR')
            if nt_year not in (year, year - 1, year - 2):
                continue

            nt_type = self._get_str(r, 'NT_TYPE').upper()
            if nt_type not in ('E', '30'):
                continue

            period = self._get_int(r, 'NT_PERIOD')
            if period < 1 or period > 12:
                continue

            # Revenue is negative in ledger, negate for display
            revenue_by_year[nt_year][period] += -self._get_num(r, 'NT_VALUE')

        curr_year = revenue_by_year.get(year, {})
        prev_year = revenue_by_year.get(year - 1, {})

        # Current month vs same month last year
        curr_month_rev = curr_year.get(current_month, 0)
        prev_month_rev = prev_year.get(current_month, 0)
        month_yoy_change = ((curr_month_rev - prev_month_rev) / prev_month_rev * 100) if prev_month_rev else 0

        # Quarter comparison
        quarter_months = list(range((current_quarter - 1) * 3 + 1, current_quarter * 3 + 1))
        curr_qtd = sum(curr_year.get(m, 0) for m in quarter_months if m <= current_month)
        prev_qtd = sum(prev_year.get(m, 0) for m in quarter_months if m <= current_month)
        quarter_yoy_change = ((curr_qtd - prev_qtd) / prev_qtd * 100) if prev_qtd else 0

        # YTD comparison
        curr_ytd = sum(curr_year.get(m, 0) for m in range(1, current_month + 1))
        prev_ytd = sum(prev_year.get(m, 0) for m in range(1, current_month + 1))
        ytd_yoy_change = ((curr_ytd - prev_ytd) / prev_ytd * 100) if prev_ytd else 0

        # Rolling 12 months
        rolling_12 = 0
        prev_rolling_12 = 0
        for i in range(12):
            m = current_month - i
            y = year if m > 0 else year - 1
            m = m if m > 0 else m + 12
            rolling_12 += revenue_by_year.get(y, {}).get(m, 0)
            py = y - 1
            prev_rolling_12 += revenue_by_year.get(py, {}).get(m, 0)

        rolling_12_change = ((rolling_12 - prev_rolling_12) / prev_rolling_12 * 100) if prev_rolling_12 else 0

        # Run rate calculations
        recent_months = []
        for i in range(3):
            m = current_month - i
            y = year if m > 0 else year - 1
            m = m if m > 0 else m + 12
            recent_months.append(revenue_by_year.get(y, {}).get(m, 0))
        monthly_run_rate = sum(recent_months) / len(recent_months) if recent_months else 0
        annual_run_rate = monthly_run_rate * 12

        # Projections
        months_elapsed = current_month
        projected_full_year = (curr_ytd / months_elapsed * 12) if months_elapsed > 0 else 0
        prev_full_year = sum(prev_year.get(m, 0) for m in range(1, 13))
        projection_vs_prior = ((projected_full_year - prev_full_year) / prev_full_year * 100) if prev_full_year else 0

        return {
            "period": {
                "current_month": current_month,
                "current_quarter": current_quarter,
                "months_elapsed": months_elapsed
            },
            "kpis": {
                "current_month": {
                    "value": round(curr_month_rev, 2),
                    "prior_year": round(prev_month_rev, 2),
                    "yoy_change_percent": round(month_yoy_change, 1),
                    "trend": "up" if month_yoy_change > 0 else "down" if month_yoy_change < 0 else "flat"
                },
                "quarter_to_date": {
                    "value": round(curr_qtd, 2),
                    "prior_year": round(prev_qtd, 2),
                    "yoy_change_percent": round(quarter_yoy_change, 1),
                    "trend": "up" if quarter_yoy_change > 0 else "down" if quarter_yoy_change < 0 else "flat"
                },
                "year_to_date": {
                    "value": round(curr_ytd, 2),
                    "prior_year": round(prev_ytd, 2),
                    "yoy_change_percent": round(ytd_yoy_change, 1),
                    "trend": "up" if ytd_yoy_change > 0 else "down" if ytd_yoy_change < 0 else "flat"
                },
                "rolling_12_months": {
                    "value": round(rolling_12, 2),
                    "prior_period": round(prev_rolling_12, 2),
                    "change_percent": round(rolling_12_change, 1),
                    "trend": "up" if rolling_12_change > 0 else "down" if rolling_12_change < 0 else "flat"
                },
                "monthly_run_rate": round(monthly_run_rate, 2),
                "annual_run_rate": round(annual_run_rate, 2),
                "projected_full_year": round(projected_full_year, 2),
                "prior_full_year": round(prev_full_year, 2),
                "projection_vs_prior_percent": round(projection_vs_prior, 1)
            }
        }
