"""
Opera Data Provider Abstraction Layer

Provides a unified interface for accessing Opera accounting data from
both Opera SQL SE (SQL Server) and Opera 3 (FoxPro DBF files).

This allows the same business logic to work with either data source.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
from datetime import date


class OperaDataProvider(ABC):
    """
    Abstract base class for Opera data access.

    Implementations:
    - OperaSQLProvider: Uses SQLConnector for Opera SQL SE
    - Opera3DataProvider: Uses Opera3Reader for Opera 3 FoxPro DBF files
    """

    # =========================================================================
    # Customer / Sales Ledger Methods
    # =========================================================================

    @abstractmethod
    def get_customers(self, active_only: bool = True) -> List[Dict]:
        """
        Get customer master records.

        Returns:
            List of dicts with keys: account, name, balance, credit_limit, on_stop, etc.
        """
        pass

    @abstractmethod
    def get_customer_balances(self) -> List[Dict]:
        """
        Get customers with non-zero balances.

        Returns:
            List of dicts with keys: account, name, balance, credit_limit, on_stop
        """
        pass

    @abstractmethod
    def get_customer_aging(self, account: Optional[str] = None) -> List[Dict]:
        """
        Get aged debtors breakdown from shist table.

        Args:
            account: Optional customer account code to filter by

        Returns:
            List of dicts with keys: account, name, balance, current, month1, month2, month3_plus,
                                     credit_limit, phone, contact, on_stop
        """
        pass

    @abstractmethod
    def get_sales_transactions(
        self,
        account: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        outstanding_only: bool = False
    ) -> List[Dict]:
        """
        Get sales ledger transactions.

        Args:
            account: Filter by customer account code
            from_date: Filter transactions from this date
            to_date: Filter transactions up to this date
            outstanding_only: Only return transactions with non-zero balance

        Returns:
            List of dicts with keys: account, type, date, reference, value, balance, due_date
        """
        pass

    @abstractmethod
    def get_credit_control_metrics(self) -> Dict:
        """
        Get credit control dashboard metrics.

        Returns:
            Dict with keys: total_debt, over_credit_limit, accounts_on_stop,
                          overdue_invoices, recent_payments, promises_due,
                          disputed, unallocated_cash
            Each metric is a dict with: value, count, label
        """
        pass

    @abstractmethod
    def get_priority_customers(self, limit: int = 10) -> List[Dict]:
        """
        Get customers needing attention (on stop or over credit limit).

        Args:
            limit: Maximum number of customers to return

        Returns:
            List of dicts with keys: account, customer, balance, credit_limit,
                                    phone, contact, priority_reason
        """
        pass

    # =========================================================================
    # Supplier / Purchase Ledger Methods
    # =========================================================================

    @abstractmethod
    def get_suppliers(self, active_only: bool = True) -> List[Dict]:
        """
        Get supplier master records.

        Returns:
            List of dicts with keys: account, name, balance, phone, contact, etc.
        """
        pass

    @abstractmethod
    def get_supplier_balances(self) -> List[Dict]:
        """
        Get suppliers with non-zero balances.

        Returns:
            List of dicts with keys: account, name, balance, phone, contact
        """
        pass

    @abstractmethod
    def get_supplier_aging(self, account: Optional[str] = None) -> List[Dict]:
        """
        Get aged creditors breakdown from phist table.

        Args:
            account: Optional supplier account code to filter by

        Returns:
            List of dicts with keys: account, name, balance, current, month1, month2, month3_plus,
                                     phone, contact
        """
        pass

    @abstractmethod
    def get_purchase_transactions(
        self,
        account: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        outstanding_only: bool = False
    ) -> List[Dict]:
        """
        Get purchase ledger transactions.

        Args:
            account: Filter by supplier account code
            from_date: Filter transactions from this date
            to_date: Filter transactions up to this date
            outstanding_only: Only return transactions with non-zero balance

        Returns:
            List of dicts with keys: account, type, date, reference, value, balance, due_date
        """
        pass

    @abstractmethod
    def get_creditors_metrics(self) -> Dict:
        """
        Get creditors control dashboard metrics.

        Returns:
            Dict with keys: total_creditors, overdue_invoices, due_7_days,
                          due_30_days, recent_payments
            Each metric is a dict with: value, count, label
        """
        pass

    @abstractmethod
    def get_top_suppliers(self, limit: int = 10) -> List[Dict]:
        """
        Get top suppliers by balance.

        Args:
            limit: Maximum number of suppliers to return

        Returns:
            List of dicts with keys: account, supplier, balance, phone, contact
        """
        pass

    # =========================================================================
    # Nominal Ledger Methods
    # =========================================================================

    @abstractmethod
    def get_nominal_accounts(self) -> List[Dict]:
        """
        Get nominal account master records.

        Returns:
            List of dicts with keys: account, description, type, subtype
        """
        pass

    @abstractmethod
    def get_nominal_trial_balance(self, year: int) -> List[Dict]:
        """
        Get trial balance for a financial year.

        Args:
            year: Financial year

        Returns:
            List of dicts with keys: account_code, description, account_type, subtype,
                                     ytd_movement, debit, credit
        """
        pass

    @abstractmethod
    def get_nominal_by_type(self, year: int, types: List[str]) -> Dict[str, float]:
        """
        Get nominal totals grouped by account type.

        Args:
            year: Financial year
            types: List of account types to include (e.g., ['E', 'F', 'G', 'H'])

        Returns:
            Dict mapping type code to total value
        """
        pass

    @abstractmethod
    def get_nominal_monthly(self, year: int) -> List[Dict]:
        """
        Get monthly nominal breakdown for P&L accounts.

        Args:
            year: Financial year

        Returns:
            List of dicts with keys: month, month_name, revenue, cost_of_sales,
                                     gross_profit, overheads, net_profit
        """
        pass

    @abstractmethod
    def get_finance_summary(self, year: int) -> Dict:
        """
        Get financial summary with P&L and Balance Sheet overview.

        Args:
            year: Financial year

        Returns:
            Dict with keys: profit_and_loss, balance_sheet, ratios
        """
        pass

    @abstractmethod
    def get_executive_summary(self, year: int) -> Dict:
        """
        Get executive KPIs with YoY comparisons.

        Args:
            year: Financial year

        Returns:
            Dict with keys: period, kpis (current_month, quarter_to_date, year_to_date,
                          rolling_12_months, run_rates, projections)
        """
        pass

    # =========================================================================
    # Reconciliation Methods
    # =========================================================================

    @abstractmethod
    def get_debtors_reconciliation(self, debtors_control: str = 'C110') -> Dict:
        """
        Reconcile Sales Ledger to Debtors Control Account.

        Args:
            debtors_control: Nominal account code for debtors control

        Returns:
            Dict with keys: reconciliation_date, sales_ledger, nominal_ledger,
                          variance, status, message, aged_analysis, top_customers
        """
        pass

    @abstractmethod
    def get_creditors_reconciliation(self, creditors_control: str = 'D110') -> Dict:
        """
        Reconcile Purchase Ledger to Creditors Control Account.

        Args:
            creditors_control: Nominal account code for creditors control

        Returns:
            Dict with keys: reconciliation_date, purchase_ledger, nominal_ledger,
                          variance, status, message, aged_analysis, top_suppliers
        """
        pass


def create_data_provider(source_type: str, **kwargs) -> OperaDataProvider:
    """
    Create appropriate data provider based on source type.

    Args:
        source_type: 'sql_se' or 'opera3'
        **kwargs: For sql_se: sql_connector (SQLConnector instance)
                  For opera3: data_path (path to Opera 3 company data folder)

    Returns:
        OperaDataProvider implementation

    Raises:
        ValueError: If source_type is unknown
        ImportError: If required dependencies are not available

    Example:
        # SQL SE
        provider = create_data_provider('sql_se', sql_connector=my_sql_connector)

        # Opera 3
        provider = create_data_provider('opera3', data_path='/path/to/company/data')
    """
    if source_type == 'sql_se':
        from sql_rag.opera_sql_provider import OperaSQLProvider
        if 'sql_connector' not in kwargs:
            raise ValueError("sql_connector is required for sql_se provider")
        return OperaSQLProvider(kwargs['sql_connector'])

    elif source_type == 'opera3':
        from sql_rag.opera3_data_provider import Opera3DataProvider
        if 'data_path' not in kwargs:
            raise ValueError("data_path is required for opera3 provider")
        return Opera3DataProvider(kwargs['data_path'])

    else:
        raise ValueError(f"Unknown source type: {source_type}. Use 'sql_se' or 'opera3'")
