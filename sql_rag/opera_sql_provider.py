"""
Opera SQL SE Data Provider

Implements OperaDataProvider for Opera SQL SE databases using SQLConnector.
Wraps existing SQL queries from api/main.py into a unified interface.
"""

from typing import List, Dict, Optional, Any
from datetime import date, datetime
from sql_rag.opera_data_provider import OperaDataProvider


class OperaSQLProvider(OperaDataProvider):
    """
    Data provider for Opera SQL SE using SQLConnector.

    Uses SQL Server queries with aggregations (SUM, COUNT, GROUP BY).
    """

    # Account type names for trial balance grouping
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

    def __init__(self, sql_connector):
        """
        Initialize with a SQLConnector instance.

        Args:
            sql_connector: SQLConnector configured for Opera SQL SE database
        """
        self.sql = sql_connector

    def _execute_query(self, query: str) -> List[Dict]:
        """Execute query and convert result to list of dicts."""
        result = self.sql.execute_query(query)
        if hasattr(result, 'to_dict'):
            return result.to_dict('records')
        return result if result else []

    # =========================================================================
    # Customer / Sales Ledger Methods
    # =========================================================================

    def get_customers(self, active_only: bool = True) -> List[Dict]:
        """Get customer master records."""
        query = """
            SELECT
                RTRIM(sn_account) AS account,
                RTRIM(sn_name) AS name,
                sn_currbal AS balance,
                sn_crlim AS credit_limit,
                sn_stop AS on_stop,
                sn_dormant AS dormant,
                RTRIM(sn_teleno) AS phone,
                RTRIM(sn_contact) AS contact,
                RTRIM(sn_email) AS email
            FROM sname
        """
        if active_only:
            query += " WHERE ISNULL(sn_dormant, 0) = 0"
        return self._execute_query(query)

    def get_customer_balances(self) -> List[Dict]:
        """Get customers with non-zero balances."""
        query = """
            SELECT
                RTRIM(sn_account) AS account,
                RTRIM(sn_name) AS name,
                sn_currbal AS balance,
                sn_crlim AS credit_limit,
                sn_stop AS on_stop
            FROM sname
            WHERE sn_currbal <> 0
        """
        return self._execute_query(query)

    def get_customer_aging(self, account: Optional[str] = None) -> List[Dict]:
        """Get aged debtors breakdown."""
        query = """
            SELECT
                RTRIM(sn.sn_account) AS account,
                RTRIM(sn.sn_name) AS name,
                sn.sn_currbal AS balance,
                ISNULL(sh.si_current, 0) AS current,
                ISNULL(sh.si_period1, 0) AS month1,
                ISNULL(sh.si_period2, 0) AS month2,
                ISNULL(sh.si_period3, 0) + ISNULL(sh.si_period4, 0) + ISNULL(sh.si_period5, 0) AS month3_plus,
                sn.sn_crlim AS credit_limit,
                RTRIM(sn.sn_teleno) AS phone,
                RTRIM(sn.sn_contact) AS contact,
                sn.sn_stop AS on_stop
            FROM sname sn
            LEFT JOIN shist sh ON sn.sn_account = sh.si_account AND sh.si_age = 1
            WHERE sn.sn_currbal <> 0
        """
        if account:
            query += f" AND sn.sn_account = '{account}'"
        query += " ORDER BY sn.sn_account"
        return self._execute_query(query)

    def get_sales_transactions(
        self,
        account: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        outstanding_only: bool = False
    ) -> List[Dict]:
        """Get sales ledger transactions."""
        query = """
            SELECT
                RTRIM(st_account) AS account,
                st_trtype AS type,
                st_trdate AS date,
                RTRIM(st_trref) AS reference,
                st_trvalue AS value,
                st_trbal AS balance,
                st_dueday AS due_date
            FROM stran
            WHERE 1=1
        """
        if account:
            query += f" AND st_account = '{account}'"
        if from_date:
            query += f" AND st_trdate >= '{from_date.isoformat()}'"
        if to_date:
            query += f" AND st_trdate <= '{to_date.isoformat()}'"
        if outstanding_only:
            query += " AND st_trbal <> 0"
        query += " ORDER BY st_trdate DESC"
        return self._execute_query(query)

    def get_credit_control_metrics(self) -> Dict:
        """Get credit control dashboard metrics."""
        metrics = {}

        # Total outstanding balance
        result = self._execute_query(
            "SELECT COUNT(*) AS count, SUM(sn_currbal) AS total FROM sname WHERE sn_currbal > 0"
        )
        if result:
            metrics["total_debt"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Total Outstanding"
            }

        # Over credit limit
        result = self._execute_query(
            """SELECT COUNT(*) AS count, SUM(sn_currbal - sn_crlim) AS total
               FROM sname WHERE sn_currbal > sn_crlim AND sn_crlim > 0"""
        )
        if result:
            metrics["over_credit_limit"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Over Credit Limit"
            }

        # Accounts on stop
        result = self._execute_query(
            "SELECT COUNT(*) AS count, SUM(sn_currbal) AS total FROM sname WHERE sn_stop = 1"
        )
        if result:
            metrics["accounts_on_stop"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Accounts On Stop"
            }

        # Overdue invoices
        result = self._execute_query(
            """SELECT COUNT(*) AS count, SUM(st_trbal) AS total
               FROM stran WHERE st_trtype = 'I' AND st_trbal > 0 AND st_dueday < GETDATE()"""
        )
        if result:
            metrics["overdue_invoices"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Overdue Invoices"
            }

        # Recent payments (last 7 days)
        result = self._execute_query(
            """SELECT COUNT(*) AS count, SUM(ABS(st_trvalue)) AS total
               FROM stran WHERE st_trtype = 'R' AND st_trdate >= DATEADD(day, -7, GETDATE())"""
        )
        if result:
            metrics["recent_payments"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Payments (7 days)"
            }

        # Promises due today or overdue
        result = self._execute_query(
            """SELECT COUNT(*) AS count, SUM(st_trbal) AS total
               FROM stran WHERE st_payday IS NOT NULL
               AND st_payday <= GETDATE() AND st_trbal > 0"""
        )
        if result:
            metrics["promises_due"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Promises Due"
            }

        # Disputed invoices
        result = self._execute_query(
            """SELECT COUNT(*) AS count, SUM(st_trbal) AS total
               FROM stran WHERE st_dispute = 1 AND st_trbal > 0"""
        )
        if result:
            metrics["disputed"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "In Dispute"
            }

        # Unallocated cash
        result = self._execute_query(
            """SELECT COUNT(*) AS count, SUM(ABS(st_trbal)) AS total
               FROM stran WHERE st_trtype = 'R' AND st_trbal < 0"""
        )
        if result:
            metrics["unallocated_cash"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Unallocated Cash"
            }

        return metrics

    def get_priority_customers(self, limit: int = 10) -> List[Dict]:
        """Get customers needing attention."""
        query = f"""
            SELECT TOP {limit}
                RTRIM(sn_account) AS account,
                RTRIM(sn_name) AS customer,
                sn_currbal AS balance,
                sn_crlim AS credit_limit,
                RTRIM(sn_teleno) AS phone,
                RTRIM(sn_contact) AS contact,
                CASE WHEN sn_stop = 1 THEN 'ON_STOP'
                     WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER_LIMIT'
                     ELSE 'HIGH_BALANCE' END AS priority_reason
            FROM sname
            WHERE sn_currbal > 0 AND (sn_stop = 1 OR sn_currbal > sn_crlim)
            ORDER BY sn_currbal DESC
        """
        return self._execute_query(query)

    # =========================================================================
    # Supplier / Purchase Ledger Methods
    # =========================================================================

    def get_suppliers(self, active_only: bool = True) -> List[Dict]:
        """Get supplier master records."""
        query = """
            SELECT
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS name,
                pn_currbal AS balance,
                RTRIM(pn_teleno) AS phone,
                RTRIM(pn_contact) AS contact
            FROM pname
        """
        return self._execute_query(query)

    def get_supplier_balances(self) -> List[Dict]:
        """Get suppliers with non-zero balances."""
        query = """
            SELECT
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS name,
                pn_currbal AS balance,
                RTRIM(pn_teleno) AS phone,
                RTRIM(pn_contact) AS contact
            FROM pname
            WHERE pn_currbal <> 0
        """
        return self._execute_query(query)

    def get_supplier_aging(self, account: Optional[str] = None) -> List[Dict]:
        """Get aged creditors breakdown."""
        query = """
            SELECT
                RTRIM(pn.pn_account) AS account,
                RTRIM(pn.pn_name) AS name,
                pn.pn_currbal AS balance,
                ISNULL(ph.pi_current, 0) AS current,
                ISNULL(ph.pi_period1, 0) AS month1,
                ISNULL(ph.pi_period2, 0) AS month2,
                ISNULL(ph.pi_period3, 0) + ISNULL(ph.pi_period4, 0) + ISNULL(ph.pi_period5, 0) AS month3_plus,
                RTRIM(pn.pn_teleno) AS phone,
                RTRIM(pn.pn_contact) AS contact
            FROM pname pn
            LEFT JOIN phist ph ON pn.pn_account = ph.pi_account AND ph.pi_age = 1
            WHERE pn.pn_currbal <> 0
        """
        if account:
            query += f" AND pn.pn_account = '{account}'"
        query += " ORDER BY pn.pn_account"
        return self._execute_query(query)

    def get_purchase_transactions(
        self,
        account: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        outstanding_only: bool = False
    ) -> List[Dict]:
        """Get purchase ledger transactions."""
        query = """
            SELECT
                RTRIM(pt_account) AS account,
                pt_trtype AS type,
                pt_trdate AS date,
                RTRIM(pt_trref) AS reference,
                pt_trvalue AS value,
                pt_trbal AS balance,
                pt_dueday AS due_date
            FROM ptran
            WHERE 1=1
        """
        if account:
            query += f" AND pt_account = '{account}'"
        if from_date:
            query += f" AND pt_trdate >= '{from_date.isoformat()}'"
        if to_date:
            query += f" AND pt_trdate <= '{to_date.isoformat()}'"
        if outstanding_only:
            query += " AND pt_trbal <> 0"
        query += " ORDER BY pt_trdate DESC"
        return self._execute_query(query)

    def get_creditors_metrics(self) -> Dict:
        """Get creditors control dashboard metrics."""
        metrics = {}

        # Total creditors balance
        result = self._execute_query(
            """SELECT COUNT(*) AS count, SUM(pn_currbal) AS total
               FROM pname WHERE pn_currbal <> 0"""
        )
        if result:
            metrics["total_creditors"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Total Outstanding"
            }

        # Overdue invoices
        result = self._execute_query(
            """SELECT COUNT(*) AS count, SUM(pt_trbal) AS total
               FROM ptran WHERE pt_trtype = 'I' AND pt_trbal > 0 AND pt_dueday < GETDATE()"""
        )
        if result:
            metrics["overdue_invoices"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Overdue Invoices"
            }

        # Due within 7 days
        result = self._execute_query(
            """SELECT COUNT(*) AS count, SUM(pt_trbal) AS total
               FROM ptran WHERE pt_trtype = 'I' AND pt_trbal > 0
               AND pt_dueday >= GETDATE() AND pt_dueday < DATEADD(day, 7, GETDATE())"""
        )
        if result:
            metrics["due_7_days"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Due in 7 Days"
            }

        # Due within 30 days
        result = self._execute_query(
            """SELECT COUNT(*) AS count, SUM(pt_trbal) AS total
               FROM ptran WHERE pt_trtype = 'I' AND pt_trbal > 0
               AND pt_dueday >= GETDATE() AND pt_dueday < DATEADD(day, 30, GETDATE())"""
        )
        if result:
            metrics["due_30_days"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Due in 30 Days"
            }

        # Recent payments (last 7 days)
        result = self._execute_query(
            """SELECT COUNT(*) AS count, SUM(ABS(pt_trvalue)) AS total
               FROM ptran WHERE pt_trtype = 'P' AND pt_trdate >= DATEADD(day, -7, GETDATE())"""
        )
        if result:
            metrics["recent_payments"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Payments (7 days)"
            }

        return metrics

    def get_top_suppliers(self, limit: int = 10) -> List[Dict]:
        """Get top suppliers by balance."""
        query = f"""
            SELECT TOP {limit}
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS supplier,
                pn_currbal AS balance,
                RTRIM(pn_teleno) AS phone,
                RTRIM(pn_contact) AS contact
            FROM pname
            WHERE pn_currbal > 0
            ORDER BY pn_currbal DESC
        """
        return self._execute_query(query)

    # =========================================================================
    # Nominal Ledger Methods
    # =========================================================================

    def get_nominal_accounts(self) -> List[Dict]:
        """Get nominal account master records."""
        query = """
            SELECT
                RTRIM(na_acnt) AS account,
                RTRIM(na_desc) AS description,
                RTRIM(na_type) AS type,
                RTRIM(na_subt) AS subtype
            FROM nacnt
        """
        return self._execute_query(query)

    def get_nominal_trial_balance(self, year: int) -> List[Dict]:
        """Get trial balance for a financial year."""
        query = f"""
            SELECT
                RTRIM(t.nt_acnt) AS account_code,
                RTRIM(n.na_desc) AS description,
                RTRIM(t.nt_type) AS account_type,
                RTRIM(t.nt_subt) AS subtype,
                0 AS opening_balance,
                ISNULL(SUM(t.nt_value), 0) AS ytd_movement,
                CASE
                    WHEN ISNULL(SUM(t.nt_value), 0) > 0 THEN ISNULL(SUM(t.nt_value), 0)
                    ELSE 0
                END AS debit,
                CASE
                    WHEN ISNULL(SUM(t.nt_value), 0) < 0 THEN ABS(ISNULL(SUM(t.nt_value), 0))
                    ELSE 0
                END AS credit
            FROM ntran t
            LEFT JOIN nacnt n ON RTRIM(t.nt_acnt) = RTRIM(n.na_acnt)
            WHERE t.nt_year = {year}
            GROUP BY t.nt_acnt, n.na_desc, t.nt_type, t.nt_subt
            HAVING ISNULL(SUM(t.nt_value), 0) <> 0
            ORDER BY t.nt_acnt
        """
        return self._execute_query(query)

    def get_nominal_by_type(self, year: int, types: List[str]) -> Dict[str, float]:
        """Get nominal totals grouped by account type."""
        type_list = "', '".join(types)
        query = f"""
            SELECT
                RTRIM(nt_type) AS type,
                SUM(nt_value) AS total
            FROM ntran
            WHERE nt_year = {year}
            AND RTRIM(nt_type) IN ('{type_list}')
            GROUP BY RTRIM(nt_type)
        """
        result = self._execute_query(query)
        return {row['type'].strip(): float(row['total'] or 0) for row in result}

    def get_nominal_monthly(self, year: int) -> List[Dict]:
        """Get monthly nominal breakdown for P&L accounts."""
        query = f"""
            SELECT
                nt_period as month,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'E' THEN -nt_value
                    WHEN RTRIM(nt_type) = '30' THEN -nt_value
                    ELSE 0
                END) as revenue,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'F' THEN nt_value
                    WHEN RTRIM(nt_type) = '35' THEN nt_value
                    ELSE 0
                END) as cost_of_sales,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'H' THEN nt_value
                    WHEN RTRIM(nt_type) = '45' THEN nt_value
                    ELSE 0
                END) as overheads
            FROM ntran
            WHERE nt_year = {year}
            AND RTRIM(nt_type) IN ('E', 'F', 'H', '30', '35', '45')
            GROUP BY nt_period
            ORDER BY nt_period
        """
        data = self._execute_query(query)

        months = []
        for row in data:
            m = int(row['month']) if row['month'] else 0
            if m < 1 or m > 12:
                continue

            revenue = float(row['revenue'] or 0)
            cos = float(row['cost_of_sales'] or 0)
            overheads = float(row['overheads'] or 0)
            gross_profit = revenue - cos
            net_profit = gross_profit - overheads

            months.append({
                "month": m,
                "month_name": self.MONTH_NAMES[m],
                "revenue": round(revenue, 2),
                "cost_of_sales": round(cos, 2),
                "gross_profit": round(gross_profit, 2),
                "overheads": round(overheads, 2),
                "net_profit": round(net_profit, 2),
                "gross_margin_percent": round(gross_profit / revenue * 100, 1) if revenue > 0 else 0
            })

        return months

    def get_finance_summary(self, year: int) -> Dict:
        """Get financial summary with P&L and Balance Sheet overview."""
        # Get P&L summary by type
        pl_query = f"""
            SELECT
                RTRIM(nt_type) as type,
                SUM(nt_value) as ytd_movement
            FROM ntran
            WHERE RTRIM(nt_type) IN ('30', '35', '40', '45', 'E', 'F', 'G', 'H')
            AND nt_year = {year}
            GROUP BY RTRIM(nt_type)
        """
        pl_data = self._execute_query(pl_query)

        # Aggregate P&L - handle both letter and number codes
        sales = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('30', 'E'))
        cos = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('35', 'F'))
        other_income = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('40', 'G'))
        overheads = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('45', 'H'))

        gross_profit = -sales - cos  # Sales are negative (credits)
        operating_profit = gross_profit + (-other_income) - overheads

        # Get Balance Sheet summary
        bs_query = f"""
            SELECT
                RTRIM(na_type) as type,
                SUM(na_ytddr - na_ytdcr) as balance
            FROM nacnt
            WHERE RTRIM(na_type) IN ('A', 'B', 'C', 'D', '05', '10', '15', '20', '25')
            GROUP BY RTRIM(na_type)
        """
        bs_data = self._execute_query(bs_query)

        # Map balance sheet types
        type_map = {
            'A': 'Fixed Assets', '05': 'Fixed Assets',
            'B': 'Current Assets', '10': 'Current Assets',
            'C': 'Current Liabilities', '15': 'Current Liabilities',
            'D': 'Capital & Reserves', '25': 'Capital & Reserves',
            '20': 'Long Term Liabilities'
        }
        bs_summary = {}
        for row in bs_data:
            type_name = type_map.get(row['type'].strip(), 'Other')
            bs_summary[type_name] = bs_summary.get(type_name, 0) + float(row['balance'] or 0)

        fixed_assets = bs_summary.get('Fixed Assets', 0)
        current_assets = bs_summary.get('Current Assets', 0)
        current_liabilities = abs(bs_summary.get('Current Liabilities', 0))
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
        current_date = datetime.now()
        current_month = current_date.month
        current_quarter = (current_month - 1) // 3 + 1

        # Get monthly revenue data for both years
        query = f"""
            SELECT
                nt_year,
                nt_period as month,
                SUM(CASE WHEN RTRIM(nt_type) IN ('E', '30') THEN -nt_value ELSE 0 END) as revenue
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year IN ({year}, {year - 1}, {year - 2})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """
        data = self._execute_query(query)

        # Organize by year and month
        revenue_by_year = {}
        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            if y not in revenue_by_year:
                revenue_by_year[y] = {}
            revenue_by_year[y][m] = float(row['revenue'] or 0)

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
