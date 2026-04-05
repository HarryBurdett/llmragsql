"""
Supplier Data Provider -- abstraction layer for accounting system access.

The supplier reconciliation system calls this interface to get data.
Implement for each accounting system (Opera SE, Opera 3, Sage, Xero, etc.).
The reconciler and UI never know which system is behind the data.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Dict, Any


@dataclass
class SupplierInfo:
    account_code: str
    name: str
    balance: float  # negative = we owe them, positive = we're in credit
    payment_terms_days: int = 30
    payment_method: str = ''
    is_dormant: bool = False


@dataclass
class SupplierContact:
    name: str
    email: str
    phone: str = ''
    mobile: str = ''
    position: str = ''


@dataclass
class OutstandingTransaction:
    reference: str
    date: str  # YYYY-MM-DD
    type_code: str  # I=Invoice, P=Payment, C=Credit, F=Refund
    balance: float  # signed: positive=debit, negative=credit
    value: float  # original transaction value (signed)
    due_date: Optional[str] = None  # YYYY-MM-DD
    supplier_ref: Optional[str] = None  # supplier's own reference
    unique_id: Optional[str] = None  # system-specific unique ID


@dataclass
class TransactionRef:
    """Minimal transaction data for existence checking."""
    reference: str
    date: str
    abs_value: float


class SupplierDataProvider(ABC):
    """Abstract interface for supplier data access."""

    @abstractmethod
    def get_all_suppliers(self) -> List[SupplierInfo]:
        """Get all non-dormant suppliers."""
        pass

    @abstractmethod
    def get_supplier(self, account_code: str) -> Optional[SupplierInfo]:
        """Get a single supplier's info."""
        pass

    @abstractmethod
    def get_supplier_contact(self, account_code: str) -> Optional[SupplierContact]:
        """Get the primary contact for a supplier."""
        pass

    @abstractmethod
    def get_outstanding_transactions(self, account_code: str) -> List[OutstandingTransaction]:
        """Get all outstanding (unallocated) transactions for a supplier."""
        pass

    @abstractmethod
    def get_all_transaction_refs(self, account_code: str) -> List[TransactionRef]:
        """Get all transaction references (including settled) for existence checking."""
        pass

    @abstractmethod
    def get_outstanding_invoices_due_by(self, account_code: str, due_date: str) -> List[OutstandingTransaction]:
        """Get outstanding invoices due on or before the given date."""
        pass

    @abstractmethod
    def get_recent_payments(self, account_code: str, since_days: int = 90) -> List[OutstandingTransaction]:
        """Get recent payments made to a supplier."""
        pass

    @abstractmethod
    def find_supplier_by_name(self, name: str, account_ref: Optional[str] = None) -> Optional[SupplierInfo]:
        """Find a supplier by name or account reference. Used for matching incoming statements."""
        pass

    @abstractmethod
    def verify_sender(self, account_code: str, email: str) -> bool:
        """Check if an email address belongs to a known contact for this supplier."""
        pass

    @abstractmethod
    def get_supplier_balance(self, account_code: str) -> Optional[float]:
        """Get the current balance for a supplier from the master record."""
        pass

    @abstractmethod
    def get_outstanding_balance(self, account_code: str) -> float:
        """Get the sum of all outstanding transaction balances for a supplier."""
        pass

    @abstractmethod
    def get_outstanding_invoice_total(self, account_code: str) -> float:
        """Get the total of outstanding invoices (positive balances) for a supplier."""
        pass

    @abstractmethod
    def get_unallocated_payment_total(self, account_code: str) -> float:
        """Get the total of unallocated payments (negative balances) for a supplier."""
        pass

    @abstractmethod
    def get_payment_terms_days(self, account_code: str) -> int:
        """Get payment terms in days for a supplier."""
        pass

    @abstractmethod
    def check_invoice_exists(self, account_code: str, reference: str, amount: float) -> Optional[str]:
        """
        Check if an invoice exists in the system for a supplier.
        Returns the unique ID if found, None otherwise.
        Matches by reference OR by amount.
        """
        pass

    @abstractmethod
    def get_supplier_name(self, account_code: str) -> Optional[str]:
        """Get just the supplier name. Lightweight alternative to get_supplier()."""
        pass


def get_supplier_data_provider() -> SupplierDataProvider:
    """Get the appropriate data provider based on current config."""
    from api.main import sql_connector
    # For now, always return Opera SE. Opera 3 implementation is TODO.
    from sql_rag.supplier_data_opera_se import OperaSESupplierDataProvider
    return OperaSESupplierDataProvider(sql_connector)
