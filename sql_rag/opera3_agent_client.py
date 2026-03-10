"""
Opera 3 Write Agent Client

HTTP client used by the main application to proxy all Opera 3 write operations
through the remote Write Agent service. The agent runs on the Opera 3 server
alongside the FoxPro data files, ensuring proper CDX index maintenance and
VFP-compatible locking.

Usage:
    client = Opera3AgentClient("http://opera3-server:9000")
    if client.is_available():
        result = client.import_sales_receipt(...)

The client returns the same data structures as Opera3FoxProImport methods,
making it a drop-in replacement.
"""

from __future__ import annotations

import logging
import time
import threading
from datetime import date, datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)


@dataclass
class Opera3ImportResult:
    """Mirror of the dataclass in opera3_foxpro_import.py."""
    success: bool
    records_processed: int = 0
    records_imported: int = 0
    records_failed: int = 0
    entry_number: str = ""
    journal_number: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    new_reconciled_balance: Optional[float] = None


def _dict_to_result(data: dict) -> Opera3ImportResult:
    """Convert a dict response to Opera3ImportResult."""
    return Opera3ImportResult(
        success=data.get("success", False),
        records_processed=data.get("records_processed", 0),
        records_imported=data.get("records_imported", 0),
        records_failed=data.get("records_failed", 0),
        entry_number=data.get("entry_number", ""),
        journal_number=data.get("journal_number", 0),
        errors=data.get("errors", []),
        warnings=data.get("warnings", []),
        new_reconciled_balance=data.get("new_reconciled_balance"),
    )


def _date_to_str(d: Optional[date]) -> Optional[str]:
    """Convert date to ISO string for JSON serialisation."""
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%d")
    return d.isoformat()


class Opera3AgentClient:
    """HTTP client for the Opera 3 Write Agent service.

    Thread-safe. Maintains a persistent health check state that is updated
    by the background health monitor.
    """

    def __init__(
        self,
        base_url: str,
        agent_key: str = "",
        timeout: float = 30.0,
        health_check_interval: float = 30.0,
    ):
        """
        Args:
            base_url: Agent service URL (e.g., "http://opera3-server:9000")
            agent_key: Shared secret for authentication (must match agent config)
            timeout: HTTP request timeout in seconds
            health_check_interval: Seconds between health checks (0 to disable)
        """
        self.base_url = base_url.rstrip("/")
        self.agent_key = agent_key
        self.timeout = timeout
        self.health_check_interval = health_check_interval

        # Health state (updated by background thread)
        self._healthy = False
        self._last_health_check = 0.0
        self._health_info: Dict[str, Any] = {}
        self._health_lock = threading.Lock()

        # Start background health monitor
        if health_check_interval > 0:
            self._health_thread = threading.Thread(
                target=self._health_monitor_loop,
                daemon=True,
                name="opera3-agent-health",
            )
            self._health_thread.start()

    # ================================================================
    # Health monitoring
    # ================================================================

    def _health_monitor_loop(self):
        """Background thread that periodically checks agent health."""
        while True:
            try:
                self._check_health()
            except Exception as e:
                logger.debug(f"Health check error: {e}")
            time.sleep(self.health_check_interval)

    def _check_health(self) -> bool:
        """Perform a health check against the agent."""
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(
                    f"{self.base_url}/health",
                    headers=self._headers(),
                )
                if resp.status_code == 200:
                    data = resp.json()
                    with self._health_lock:
                        self._healthy = data.get("status") == "ok"
                        self._health_info = data
                        self._last_health_check = time.time()
                    return self._healthy
        except Exception:
            pass

        with self._health_lock:
            self._healthy = False
            self._last_health_check = time.time()
        return False

    def is_available(self) -> bool:
        """Check if the agent is available and healthy.

        Uses cached health state from background monitor.
        Falls back to a live check if no recent data.
        """
        with self._health_lock:
            # If we have a recent check, use it
            if time.time() - self._last_health_check < self.health_check_interval * 2:
                return self._healthy

        # No recent check — do a live one
        return self._check_health()

    def get_health_info(self) -> Dict[str, Any]:
        """Get the latest health check info."""
        with self._health_lock:
            return {
                "available": self._healthy,
                "last_check": self._last_health_check,
                "info": self._health_info.copy(),
            }

    # ================================================================
    # HTTP helpers
    # ================================================================

    def _headers(self) -> Dict[str, str]:
        """Build request headers."""
        headers = {"Content-Type": "application/json"}
        if self.agent_key:
            headers["X-Agent-Key"] = self.agent_key
        return headers

    def _post(self, path: str, data: dict) -> dict:
        """Make a POST request to the agent.

        Raises:
            Opera3AgentUnavailable: If the agent is not responding
            Opera3AgentError: If the agent returns an error
        """
        url = f"{self.base_url}{path}"
        try:
            with httpx.Client(timeout=self.timeout) as client:
                resp = client.post(url, json=data, headers=self._headers())

            if resp.status_code == 401:
                raise Opera3AgentError("Authentication failed — check OPERA3_AGENT_KEY")
            if resp.status_code == 503:
                raise Opera3AgentUnavailable(resp.json().get("detail", "Service unavailable"))

            result = resp.json()
            return result

        except httpx.ConnectError:
            raise Opera3AgentUnavailable(
                f"Cannot connect to Opera 3 Write Agent at {self.base_url}. "
                "Ensure the service is running on the Opera 3 server."
            )
        except httpx.TimeoutException:
            raise Opera3AgentUnavailable(
                f"Opera 3 Write Agent at {self.base_url} timed out after {self.timeout}s"
            )

    # ================================================================
    # Transaction import methods (mirror Opera3FoxProImport API)
    # ================================================================

    def import_purchase_payment(
        self,
        bank_account: str,
        supplier_account: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        input_by: str = "IMPORT",
        creditors_control: str = None,
        payment_type: str = "Direct Cr",
        cbtype: str = None,
        validate_only: bool = False,
    ) -> Opera3ImportResult:
        data = self._post("/import/purchase-payment", {
            "bank_account": bank_account,
            "supplier_account": supplier_account,
            "amount_pounds": amount_pounds,
            "reference": reference,
            "post_date": _date_to_str(post_date),
            "input_by": input_by,
            "creditors_control": creditors_control,
            "payment_type": payment_type,
            "cbtype": cbtype,
            "validate_only": validate_only,
        })
        return _dict_to_result(data)

    def import_sales_receipt(
        self,
        bank_account: str,
        customer_account: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        input_by: str = "IMPORT",
        debtors_control: str = None,
        receipt_type: str = "BACS",
        cbtype: str = None,
        validate_only: bool = False,
    ) -> Opera3ImportResult:
        data = self._post("/import/sales-receipt", {
            "bank_account": bank_account,
            "customer_account": customer_account,
            "amount_pounds": amount_pounds,
            "reference": reference,
            "post_date": _date_to_str(post_date),
            "input_by": input_by,
            "debtors_control": debtors_control,
            "receipt_type": receipt_type,
            "cbtype": cbtype,
            "validate_only": validate_only,
        })
        return _dict_to_result(data)

    def import_sales_refund(
        self,
        bank_account: str,
        customer_account: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        input_by: str = "IMPORT",
        debtors_control: str = None,
        payment_method: str = "BACS",
        cbtype: str = None,
        validate_only: bool = False,
        comment: str = "",
    ) -> Opera3ImportResult:
        data = self._post("/import/sales-refund", {
            "bank_account": bank_account,
            "customer_account": customer_account,
            "amount_pounds": amount_pounds,
            "reference": reference,
            "post_date": _date_to_str(post_date),
            "input_by": input_by,
            "debtors_control": debtors_control,
            "payment_method": payment_method,
            "cbtype": cbtype,
            "validate_only": validate_only,
            "comment": comment,
        })
        return _dict_to_result(data)

    def import_purchase_refund(
        self,
        bank_account: str,
        supplier_account: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        input_by: str = "IMPORT",
        creditors_control: str = None,
        payment_type: str = "Direct Cr",
        cbtype: str = None,
        validate_only: bool = False,
        comment: str = "",
    ) -> Opera3ImportResult:
        data = self._post("/import/purchase-refund", {
            "bank_account": bank_account,
            "supplier_account": supplier_account,
            "amount_pounds": amount_pounds,
            "reference": reference,
            "post_date": _date_to_str(post_date),
            "input_by": input_by,
            "creditors_control": creditors_control,
            "payment_type": payment_type,
            "cbtype": cbtype,
            "validate_only": validate_only,
            "comment": comment,
        })
        return _dict_to_result(data)

    def import_bank_transfer(
        self,
        source_bank: str,
        dest_bank: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        comment: str = "",
        input_by: str = "SQLRAG",
        post_to_nominal: bool = True,
        cbtype: str = None,
    ) -> Dict[str, Any]:
        return self._post("/import/bank-transfer", {
            "source_bank": source_bank,
            "dest_bank": dest_bank,
            "amount_pounds": amount_pounds,
            "reference": reference,
            "post_date": _date_to_str(post_date),
            "comment": comment,
            "input_by": input_by,
            "post_to_nominal": post_to_nominal,
            "cbtype": cbtype,
        })

    def import_nominal_entry(
        self,
        bank_account: str,
        nominal_account: str,
        amount_pounds: float,
        reference: str,
        post_date: date,
        description: str = "",
        input_by: str = "IMPORT",
        is_receipt: bool = False,
        cbtype: str = None,
        validate_only: bool = False,
        project_code: str = "",
        department_code: str = "",
        vat_code: str = "",
    ) -> Opera3ImportResult:
        data = self._post("/import/nominal-entry", {
            "bank_account": bank_account,
            "nominal_account": nominal_account,
            "amount_pounds": amount_pounds,
            "reference": reference,
            "post_date": _date_to_str(post_date),
            "description": description,
            "input_by": input_by,
            "is_receipt": is_receipt,
            "cbtype": cbtype,
            "validate_only": validate_only,
            "project_code": project_code,
            "department_code": department_code,
            "vat_code": vat_code,
        })
        return _dict_to_result(data)

    def import_gocardless_batch(
        self,
        bank_account: str,
        payments: List[Dict[str, Any]],
        post_date: date,
        reference: str = "GoCardless",
        gocardless_fees: float = 0.0,
        vat_on_fees: float = 0.0,
        fees_nominal_account: str = None,
        fees_vat_code: str = "2",
        fees_payment_type: str = None,
        complete_batch: bool = False,
        input_by: str = "GOCARDLS",
        cbtype: str = None,
        validate_only: bool = False,
        auto_allocate: bool = False,
        currency: str = None,
        destination_bank: str = None,
        transfer_cbtype: str = None,
    ) -> Opera3ImportResult:
        data = self._post("/import/gocardless-batch", {
            "bank_account": bank_account,
            "payments": payments,
            "post_date": _date_to_str(post_date),
            "reference": reference,
            "gocardless_fees": gocardless_fees,
            "vat_on_fees": vat_on_fees,
            "fees_nominal_account": fees_nominal_account,
            "fees_vat_code": fees_vat_code,
            "fees_payment_type": fees_payment_type,
            "complete_batch": complete_batch,
            "input_by": input_by,
            "cbtype": cbtype,
            "validate_only": validate_only,
            "auto_allocate": auto_allocate,
            "currency": currency,
            "destination_bank": destination_bank,
            "transfer_cbtype": transfer_cbtype,
        })
        return _dict_to_result(data)

    def post_recurring_entry(
        self,
        bank_account: str,
        entry_ref: str,
        override_date: date = None,
        input_by: str = "RECUR",
    ) -> Opera3ImportResult:
        data = self._post("/import/recurring-entry", {
            "bank_account": bank_account,
            "entry_ref": entry_ref,
            "override_date": _date_to_str(override_date),
            "input_by": input_by,
        })
        return _dict_to_result(data)

    # ================================================================
    # Allocation methods
    # ================================================================

    def auto_allocate_receipt(
        self,
        customer_account: str,
        receipt_ref: str,
        receipt_amount: float,
        allocation_date: date,
        bank_account: str = "",
        description: str = None,
    ) -> Dict[str, Any]:
        return self._post("/allocate/receipt", {
            "customer_account": customer_account,
            "receipt_ref": receipt_ref,
            "receipt_amount": receipt_amount,
            "allocation_date": _date_to_str(allocation_date),
            "bank_account": bank_account,
            "description": description,
        })

    def auto_allocate_payment(
        self,
        supplier_account: str,
        payment_ref: str,
        payment_amount: float,
        allocation_date: date,
        bank_account: str = "",
        description: str = None,
    ) -> Dict[str, Any]:
        return self._post("/allocate/payment", {
            "supplier_account": supplier_account,
            "payment_ref": payment_ref,
            "payment_amount": payment_amount,
            "allocation_date": _date_to_str(allocation_date),
            "bank_account": bank_account,
            "description": description,
        })

    # ================================================================
    # Reconciliation methods
    # ================================================================

    def mark_entries_reconciled(
        self,
        bank_account: str,
        entries: List[Dict[str, Any]],
        statement_number: int,
        statement_date: date = None,
        reconciliation_date: date = None,
        partial: bool = False,
    ) -> Opera3ImportResult:
        data = self._post("/reconcile/mark", {
            "bank_account": bank_account,
            "entries": entries,
            "statement_number": statement_number,
            "statement_date": _date_to_str(statement_date),
            "reconciliation_date": _date_to_str(reconciliation_date),
            "partial": partial,
        })
        return _dict_to_result(data)

    # ================================================================
    # Duplicate check
    # ================================================================

    def check_duplicate_before_posting(
        self,
        bank_account: str,
        transaction_date: date,
        amount_pounds: float,
        account_code: str = "",
        account_type: str = "nominal",
        date_tolerance_days: int = 1,
    ) -> Dict[str, Any]:
        return self._post("/check/duplicate", {
            "bank_account": bank_account,
            "transaction_date": _date_to_str(transaction_date),
            "amount_pounds": amount_pounds,
            "account_code": account_code,
            "account_type": account_type,
            "date_tolerance_days": date_tolerance_days,
        })


# ============================================================
# Exceptions
# ============================================================

class Opera3AgentUnavailable(Exception):
    """Raised when the agent service is not reachable."""
    pass


class Opera3AgentError(Exception):
    """Raised when the agent returns an error."""
    pass
