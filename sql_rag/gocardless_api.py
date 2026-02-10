"""
GoCardless API Client

Direct integration with GoCardless API for fetching payouts and payments.
More reliable than email parsing with access to complete payment data.

API Documentation: https://developer.gocardless.com/api-reference/
"""

import requests
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime, date

logger = logging.getLogger(__name__)


@dataclass
class GoCardlessPayment:
    """Individual payment within a payout"""
    id: str
    amount: float  # In pounds (converted from pence)
    currency: str
    status: str
    charge_date: Optional[date]
    customer_name: Optional[str]
    customer_id: Optional[str]
    description: Optional[str]
    reference: Optional[str]
    metadata: Dict[str, Any]


@dataclass
class GoCardlessPayout:
    """A payout from GoCardless to merchant bank account"""
    id: str
    amount: float  # Net amount in pounds
    currency: str
    status: str
    reference: str  # Bank reference (e.g., "INTSYSUKLTD-XM5XEF")
    arrival_date: Optional[date]
    created_at: datetime
    deducted_fees: float  # Fees in pounds (total)
    payout_type: str
    payments: List[GoCardlessPayment]
    fees_vat: float = 0.0  # VAT on fees (from payout items)

    @property
    def gross_amount(self) -> float:
        """Calculate gross amount from payments"""
        return sum(p.amount for p in self.payments)

    @property
    def payment_count(self) -> int:
        return len(self.payments)


class GoCardlessAPIError(Exception):
    """Exception for GoCardless API errors"""
    def __init__(self, message: str, status_code: int = None, error_type: str = None):
        self.message = message
        self.status_code = status_code
        self.error_type = error_type
        super().__init__(message)


class GoCardlessClient:
    """
    Client for GoCardless API

    Usage:
        client = GoCardlessClient(access_token="your_token")
        payouts = client.get_payouts(status="paid")
        for payout in payouts:
            print(f"Payout {payout.reference}: {payout.amount} {payout.currency}")
    """

    SANDBOX_URL = "https://api-sandbox.gocardless.com"
    LIVE_URL = "https://api.gocardless.com"
    API_VERSION = "2015-07-06"

    def __init__(self, access_token: str, sandbox: bool = False):
        """
        Initialize GoCardless client

        Args:
            access_token: GoCardless API access token
            sandbox: Use sandbox environment (default: False for live)
        """
        self.access_token = access_token
        self.base_url = self.SANDBOX_URL if sandbox else self.LIVE_URL
        self.sandbox = sandbox
        self._customers_cache: Dict[str, Dict] = {}

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "GoCardless-Version": self.API_VERSION,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def _request(self, method: str, endpoint: str, params: Dict = None, data: Dict = None) -> Dict:
        """Make API request"""
        url = f"{self.base_url}{endpoint}"

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params,
                json=data,
                timeout=30
            )

            if response.status_code == 401:
                raise GoCardlessAPIError("Invalid access token", 401, "authentication_error")

            if response.status_code == 403:
                raise GoCardlessAPIError("Access forbidden - check token permissions", 403, "forbidden")

            if response.status_code == 429:
                raise GoCardlessAPIError("Rate limit exceeded", 429, "rate_limit")

            if response.status_code >= 400:
                error_data = response.json().get("error", {})
                raise GoCardlessAPIError(
                    error_data.get("message", f"API error: {response.status_code}"),
                    response.status_code,
                    error_data.get("type")
                )

            return response.json()

        except requests.exceptions.Timeout:
            raise GoCardlessAPIError("Request timed out", error_type="timeout")
        except requests.exceptions.ConnectionError:
            raise GoCardlessAPIError("Connection failed", error_type="connection_error")
        except requests.exceptions.RequestException as e:
            raise GoCardlessAPIError(f"Request failed: {e}", error_type="request_error")

    def test_connection(self) -> Dict[str, Any]:
        """
        Test API connection and return account info

        Returns:
            Dict with creditor information
        """
        try:
            result = self._request("GET", "/creditors", params={"limit": 1})
            creditors = result.get("creditors", [])
            if creditors:
                creditor = creditors[0]
                return {
                    "success": True,
                    "environment": "sandbox" if self.sandbox else "live",
                    "creditor_id": creditor.get("id"),
                    "name": creditor.get("name"),
                    "country_code": creditor.get("country_code"),
                    "verified": creditor.get("verification_status") == "successful"
                }
            return {"success": True, "environment": "sandbox" if self.sandbox else "live"}
        except GoCardlessAPIError as e:
            return {"success": False, "error": e.message, "error_type": e.error_type}

    def get_payouts(
        self,
        status: str = "paid",
        limit: int = 50,
        created_at_gte: Optional[date] = None,
        created_at_lte: Optional[date] = None,
        cursor: Optional[str] = None
    ) -> tuple[List[GoCardlessPayout], Optional[str]]:
        """
        Get list of payouts

        Args:
            status: Filter by status (pending, paid, bounced)
            limit: Number of results (max 500)
            created_at_gte: Filter payouts created on or after this date
            created_at_lte: Filter payouts created on or before this date
            cursor: Pagination cursor for next page

        Returns:
            Tuple of (list of payouts, next cursor or None)
        """
        params = {"status": status, "limit": min(limit, 500)}

        if created_at_gte:
            params["created_at[gte]"] = created_at_gte.isoformat() + "T00:00:00Z"
        if created_at_lte:
            params["created_at[lte]"] = created_at_lte.isoformat() + "T23:59:59Z"
        if cursor:
            params["after"] = cursor

        result = self._request("GET", "/payouts", params=params)

        payouts = []
        for p in result.get("payouts", []):
            payout = self._parse_payout(p)
            payouts.append(payout)

        # Get pagination cursor
        meta = result.get("meta", {}).get("cursors", {})
        next_cursor = meta.get("after")

        return payouts, next_cursor

    def get_payout(self, payout_id: str) -> GoCardlessPayout:
        """Get a specific payout by ID"""
        result = self._request("GET", f"/payouts/{payout_id}")
        return self._parse_payout(result.get("payouts", {}))

    def get_payout_items(self, payout_id: str, limit: int = 500) -> List[Dict]:
        """
        Get items (payments) within a payout

        Args:
            payout_id: The payout ID
            limit: Number of results

        Returns:
            List of payout items
        """
        params = {"payout": payout_id, "limit": min(limit, 500)}
        result = self._request("GET", "/payout_items", params=params)
        return result.get("payout_items", [])

    def get_payment(self, payment_id: str) -> Dict:
        """Get a specific payment by ID"""
        result = self._request("GET", f"/payments/{payment_id}")
        return result.get("payments", {})

    def get_customer(self, customer_id: str) -> Dict:
        """Get customer details (cached)"""
        if customer_id in self._customers_cache:
            return self._customers_cache[customer_id]

        try:
            result = self._request("GET", f"/customers/{customer_id}")
            customer = result.get("customers", {})
            self._customers_cache[customer_id] = customer
            return customer
        except GoCardlessAPIError:
            return {}

    def get_mandate(self, mandate_id: str) -> Dict:
        """Get mandate details (cached) - used to link payment to customer"""
        if not hasattr(self, '_mandates_cache'):
            self._mandates_cache = {}

        if mandate_id in self._mandates_cache:
            return self._mandates_cache[mandate_id]

        try:
            result = self._request("GET", f"/mandates/{mandate_id}")
            mandate = result.get("mandates", {})
            self._mandates_cache[mandate_id] = mandate
            return mandate
        except GoCardlessAPIError:
            return {}

    def get_payout_with_payments(self, payout_id: str) -> GoCardlessPayout:
        """
        Get a payout with all its payment details

        This fetches the payout and all associated payments with customer names.
        Also extracts VAT from fee items.
        """
        payout = self.get_payout(payout_id)
        items = self.get_payout_items(payout_id)

        payments = []
        fees_vat = 0.0

        for item in items:
            item_type = item.get("type", "")

            # Extract payments
            if item_type == "payment_paid_out":
                payment_id = item.get("links", {}).get("payment")
                if payment_id:
                    try:
                        payment_data = self.get_payment(payment_id)

                        # Customer is linked via mandate, not directly on payment
                        customer_id = None
                        customer_name = None
                        mandate_id = payment_data.get("links", {}).get("mandate")
                        if mandate_id:
                            mandate = self.get_mandate(mandate_id)
                            customer_id = mandate.get("links", {}).get("customer")
                            if customer_id:
                                customer = self.get_customer(customer_id)
                                customer_name = customer.get("company_name") or \
                                              f"{customer.get('given_name', '')} {customer.get('family_name', '')}".strip()

                        payment = GoCardlessPayment(
                            id=payment_data.get("id"),
                            amount=int(payment_data.get("amount", 0)) / 100,
                            currency=payment_data.get("currency", "GBP"),
                            status=payment_data.get("status"),
                            charge_date=self._parse_date(payment_data.get("charge_date")),
                            customer_name=customer_name,
                            customer_id=customer_id,
                            description=payment_data.get("description"),
                            reference=payment_data.get("reference"),
                            metadata=payment_data.get("metadata", {})
                        )
                        payments.append(payment)
                    except GoCardlessAPIError as e:
                        logger.warning(f"Failed to fetch payment {payment_id}: {e}")

            # Extract VAT from fee items (gocardless_fee, app_fee)
            elif item_type in ("gocardless_fee", "app_fee"):
                taxes = item.get("taxes", [])
                for tax in taxes:
                    # Tax amount is in pence, convert to pounds
                    # Use float() first as API may return string like "80.0"
                    tax_amount = abs(float(tax.get("amount", 0))) / 100
                    fees_vat += tax_amount

        payout.payments = payments
        payout.fees_vat = fees_vat
        return payout

    def _parse_payout(self, data: Dict) -> GoCardlessPayout:
        """Parse payout data from API response"""
        return GoCardlessPayout(
            id=data.get("id"),
            amount=int(data.get("amount", 0)) / 100,
            currency=data.get("currency", "GBP"),
            status=data.get("status"),
            reference=data.get("reference", ""),
            arrival_date=self._parse_date(data.get("arrival_date")),
            created_at=self._parse_datetime(data.get("created_at")),
            deducted_fees=int(data.get("deducted_fees", 0)) / 100,
            payout_type=data.get("payout_type", ""),
            payments=[]
        )

    @staticmethod
    def _parse_date(date_str: Optional[str]) -> Optional[date]:
        """Parse date string to date object"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return None

    @staticmethod
    def _parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string to datetime object"""
        if not dt_str:
            return None
        try:
            # Handle ISO format with timezone
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except ValueError:
            return None


def create_client_from_settings(settings: Dict) -> Optional[GoCardlessClient]:
    """
    Create a GoCardless client from settings dictionary

    Args:
        settings: Dict containing 'api_access_token' and optionally 'api_sandbox'

    Returns:
        GoCardlessClient or None if no token configured
    """
    access_token = settings.get("api_access_token")
    if not access_token:
        return None

    sandbox = settings.get("api_sandbox", False)
    return GoCardlessClient(access_token=access_token, sandbox=sandbox)
