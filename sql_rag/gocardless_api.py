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
    mandate_id: Optional[str]
    description: Optional[str]
    reference: Optional[str]
    metadata: Dict[str, Any]


@dataclass
class GoCardlessPayout:
    """A payout from GoCardless to merchant bank account"""
    id: str
    amount: float  # Net amount in payout currency
    currency: str
    status: str
    reference: str  # Bank reference (e.g., "INTSYSUKLTD-XM5XEF")
    arrival_date: Optional[date]
    created_at: datetime
    deducted_fees: float  # Fees in payout currency (total)
    payout_type: str
    payments: List[GoCardlessPayment]
    fees_vat: float = 0.0  # VAT on fees (from payout items)
    fx_amount: Optional[float] = None  # Amount in home currency (GBP) for foreign currency payouts
    fx_currency: Optional[str] = None  # Home currency code (e.g., "GBP")
    exchange_rate: Optional[str] = None  # FX rate applied by GoCardless
    bank_account_number: Optional[str] = None  # Destination bank account number
    bank_sort_code: Optional[str] = None  # Destination bank sort code

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
        self._mandates_cache: Dict[str, Dict] = {}
        self._bank_accounts_cache: Dict[str, Dict] = {}

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

    def get_creditor_bank_account(self, account_id: str) -> Dict:
        """Get creditor bank account details (cached) — returns account_number, sort_code etc."""

        if account_id in self._bank_accounts_cache:
            return self._bank_accounts_cache[account_id]

        try:
            result = self._request("GET", f"/creditor_bank_accounts/{account_id}")
            account = result.get("creditor_bank_accounts", {})
            self._bank_accounts_cache[account_id] = account
            return account
        except GoCardlessAPIError:
            return {}

    def get_mandate(self, mandate_id: str) -> Dict:
        """Get mandate details (cached) - used to link payment to customer"""
        if mandate_id in self._mandates_cache:
            return self._mandates_cache[mandate_id]

        try:
            result = self._request("GET", f"/mandates/{mandate_id}")
            mandate = result.get("mandates", {})
            self._mandates_cache[mandate_id] = mandate
            return mandate
        except GoCardlessAPIError:
            return {}

    def list_mandates(
        self,
        status: str = "active",
        limit: int = 100,
        cursor: Optional[str] = None
    ) -> tuple[List[Dict], Optional[str]]:
        """
        List all mandates.

        Args:
            status: Filter by status (pending_submission, pending_customer_approval,
                    active, failed, cancelled, expired, consumed, blocked, suspended)
            limit: Number of results (max 500)
            cursor: Pagination cursor

        Returns:
            Tuple of (list of mandates, next cursor or None)
        """
        params = {"status": status, "limit": min(limit, 500)}
        if cursor:
            params["after"] = cursor

        result = self._request("GET", "/mandates", params=params)

        mandates = result.get("mandates", [])

        # Get pagination cursor
        meta = result.get("meta", {}).get("cursors", {})
        next_cursor = meta.get("after")

        return mandates, next_cursor

    def create_payment(
        self,
        amount_pence: int,
        mandate_id: str,
        description: Optional[str] = None,
        charge_date: Optional[str] = None,
        currency: str = "GBP",
        metadata: Optional[Dict[str, str]] = None,
        reference: Optional[str] = None,
        retry_if_possible: bool = True
    ) -> Dict:
        """
        Create a payment against a mandate.

        Args:
            amount_pence: Amount in pence (GBP) or smallest currency unit
            mandate_id: GoCardless mandate ID (MD000XXX)
            description: Payment description (shown to customer)
            charge_date: Date to charge (YYYY-MM-DD). If not provided, uses earliest possible.
            currency: Currency code (default GBP)
            metadata: Optional metadata dict
            reference: Optional reference (shown on bank statement)
            retry_if_possible: Whether to retry failed payments

        Returns:
            Created payment dict from GoCardless
        """
        payment_data = {
            "payments": {
                "amount": amount_pence,
                "currency": currency,
                "links": {
                    "mandate": mandate_id
                },
                "retry_if_possible": retry_if_possible
            }
        }

        if description:
            payment_data["payments"]["description"] = description

        if charge_date:
            payment_data["payments"]["charge_date"] = charge_date

        if reference:
            payment_data["payments"]["reference"] = reference

        if metadata:
            payment_data["payments"]["metadata"] = metadata

        result = self._request("POST", "/payments", data=payment_data)
        return result.get("payments", {})

    def cancel_payment(self, payment_id: str) -> Dict:
        """
        Cancel a pending payment.

        Args:
            payment_id: GoCardless payment ID (PM000XXX)

        Returns:
            Updated payment dict
        """
        result = self._request("POST", f"/payments/{payment_id}/actions/cancel")
        return result.get("payments", {})

    def list_payments(
        self,
        mandate_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        cursor: Optional[str] = None
    ) -> tuple[List[Dict], Optional[str]]:
        """
        List payments, optionally filtered.

        Args:
            mandate_id: Filter by mandate
            status: Filter by status
            limit: Number of results
            cursor: Pagination cursor

        Returns:
            Tuple of (list of payments, next cursor or None)
        """
        params = {"limit": min(limit, 500)}

        if mandate_id:
            params["mandate"] = mandate_id
        if status:
            params["status"] = status
        if cursor:
            params["after"] = cursor

        result = self._request("GET", "/payments", params=params)

        payments = result.get("payments", [])

        meta = result.get("meta", {}).get("cursors", {})
        next_cursor = meta.get("after")

        return payments, next_cursor

    # ============ Subscription Management ============

    def create_subscription(
        self,
        mandate_id: str,
        amount_pence: int,
        interval_unit: str,
        interval: int = 1,
        day_of_month: Optional[int] = None,
        name: Optional[str] = None,
        start_date: Optional[str] = None,
        count: Optional[int] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> Dict:
        """
        Create a subscription (recurring payment) against a mandate.

        Args:
            mandate_id: GoCardless mandate ID
            amount_pence: Amount per payment in pence
            interval_unit: weekly, monthly, or yearly
            interval: Number of interval_units between payments (e.g. 3 for quarterly)
            day_of_month: Day of month to charge (1-28, or -1 for last day). Monthly/yearly only.
            name: Human-readable name for the subscription
            start_date: First charge date (YYYY-MM-DD). Defaults to earliest possible.
            count: Total number of payments (omit for indefinite)
            metadata: Optional metadata dict

        Returns:
            Created subscription dict from GoCardless
        """
        sub_data: Dict[str, Any] = {
            "amount": amount_pence,
            "currency": "GBP",
            "interval_unit": interval_unit,
            "interval": interval,
            "links": {
                "mandate": mandate_id
            }
        }

        if day_of_month is not None:
            sub_data["day_of_month"] = day_of_month
        if name:
            sub_data["name"] = name
        if start_date:
            sub_data["start_date"] = start_date
        if count is not None:
            sub_data["count"] = count
        if metadata:
            sub_data["metadata"] = metadata

        result = self._request("POST", "/subscriptions", data={"subscriptions": sub_data})
        return result.get("subscriptions", {})

    def list_subscriptions(
        self,
        mandate_id: Optional[str] = None,
        customer_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        cursor: Optional[str] = None
    ) -> tuple[List[Dict], Optional[str]]:
        """
        List subscriptions, optionally filtered.

        Args:
            mandate_id: Filter by mandate
            customer_id: Filter by customer
            status: Filter by status (pending_customer_approval, active, paused,
                    cancelled, customer_approval_denied, ended, finished)
            limit: Number of results (max 500)
            cursor: Pagination cursor

        Returns:
            Tuple of (list of subscriptions, next cursor or None)
        """
        params: Dict[str, Any] = {"limit": min(limit, 500)}

        if mandate_id:
            params["mandate"] = mandate_id
        if customer_id:
            params["customer"] = customer_id
        if status:
            params["status"] = status
        if cursor:
            params["after"] = cursor

        result = self._request("GET", "/subscriptions", params=params)

        subscriptions = result.get("subscriptions", [])

        meta = result.get("meta", {}).get("cursors", {})
        next_cursor = meta.get("after")

        return subscriptions, next_cursor

    def get_subscription(self, subscription_id: str) -> Dict:
        """Get a specific subscription by ID."""
        result = self._request("GET", f"/subscriptions/{subscription_id}")
        return result.get("subscriptions", {})

    def update_subscription(
        self,
        subscription_id: str,
        name: Optional[str] = None,
        amount_pence: Optional[int] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> Dict:
        """
        Update a subscription. Only name, amount, and metadata are editable.

        Args:
            subscription_id: GoCardless subscription ID
            name: New name (optional)
            amount_pence: New amount in pence (optional)
            metadata: New metadata dict (optional)

        Returns:
            Updated subscription dict
        """
        sub_data: Dict[str, Any] = {}

        if name is not None:
            sub_data["name"] = name
        if amount_pence is not None:
            sub_data["amount"] = amount_pence
        if metadata is not None:
            sub_data["metadata"] = metadata

        result = self._request("PUT", f"/subscriptions/{subscription_id}",
                               data={"subscriptions": sub_data})
        return result.get("subscriptions", {})

    def pause_subscription(self, subscription_id: str) -> Dict:
        """Pause an active subscription."""
        result = self._request("POST", f"/subscriptions/{subscription_id}/actions/pause")
        return result.get("subscriptions", {})

    def resume_subscription(self, subscription_id: str) -> Dict:
        """Resume a paused subscription."""
        result = self._request("POST", f"/subscriptions/{subscription_id}/actions/resume")
        return result.get("subscriptions", {})

    def cancel_subscription(self, subscription_id: str) -> Dict:
        """Cancel a subscription. Cannot be undone."""
        result = self._request("POST", f"/subscriptions/{subscription_id}/actions/cancel")
        return result.get("subscriptions", {})

    def create_billing_request(
        self,
        customer_email: str,
        customer_name: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> Dict:
        """
        Create a billing request for setting up a new mandate.

        This is used when a customer doesn't have a mandate yet.
        The billing request includes customer details so GoCardless can
        pre-fill the authorisation form.

        Args:
            customer_email: Customer's email address
            customer_name: Customer's name (optional)
            description: Description for the mandate
            metadata: Optional metadata (e.g. opera_account)

        Returns:
            Billing request dict with id, status, links etc.
        """
        request_data = {
            "billing_requests": {
                "mandate_request": {
                    "scheme": "bacs"
                }
            }
        }

        if metadata:
            request_data["billing_requests"]["metadata"] = metadata

        result = self._request("POST", "/billing_requests", data=request_data)
        return result.get("billing_requests", {})

    def create_billing_request_flow(
        self,
        billing_request_id: str,
        redirect_url: Optional[str] = None,
        exit_url: Optional[str] = None,
    ) -> Dict:
        """
        Create a billing request flow — generates an authorisation URL
        that the customer visits to set up their Direct Debit mandate.

        Args:
            billing_request_id: The billing request ID (BRQ...)
            redirect_url: URL to redirect after completion (optional)
            exit_url: URL to redirect if customer exits (optional)

        Returns:
            Billing request flow dict with authorisation_url
        """
        flow_data: Dict[str, Any] = {
            "billing_request_flows": {
                "redirect_uri": redirect_url or "https://example.com/mandate-complete",
                "exit_uri": exit_url or "https://example.com/mandate-exit",
                "links": {
                    "billing_request": billing_request_id
                }
            }
        }

        result = self._request("POST", "/billing_request_flows", data=flow_data)
        return result.get("billing_request_flows", {})

    def get_billing_request(self, billing_request_id: str) -> Dict:
        """
        Get details of a billing request, including its current status
        and linked mandate/customer IDs once the customer has completed it.

        Returns:
            Billing request dict with status, links (mandate, customer) etc.
        """
        result = self._request("GET", f"/billing_requests/{billing_request_id}")
        return result.get("billing_requests", {})

    def get_payout_with_payments(self, payout_id: str) -> GoCardlessPayout:
        """
        Get a payout with all its payment details.

        Uses parallel fetching for payment/mandate/customer API calls
        to dramatically reduce total fetch time.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        payout = self.get_payout(payout_id)
        items = self.get_payout_items(payout_id)

        fees_vat = 0.0
        payment_ids = []

        for item in items:
            item_type = item.get("type", "")
            if item_type == "payment_paid_out":
                pid = item.get("links", {}).get("payment")
                if pid:
                    payment_ids.append(pid)
            elif item_type in ("gocardless_fee", "app_fee"):
                for tax in item.get("taxes", []):
                    fees_vat += abs(float(tax.get("amount", 0))) / 100

        # Phase 1: Fetch all payments in parallel
        payment_data_map = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self.get_payment, pid): pid for pid in payment_ids}
            for future in as_completed(futures):
                pid = futures[future]
                try:
                    payment_data_map[pid] = future.result()
                except Exception as e:
                    logger.warning(f"Failed to fetch payment {pid}: {e}")

        # Phase 2: Collect unique mandate IDs and fetch in parallel
        mandate_ids = set()
        for pd in payment_data_map.values():
            mid = pd.get("links", {}).get("mandate")
            if mid and mid not in self._mandates_cache:
                mandate_ids.add(mid)

        if mandate_ids:
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(self.get_mandate, mid): mid for mid in mandate_ids}
                for future in as_completed(futures):
                    try:
                        future.result()  # Result is cached by get_mandate()
                    except Exception:
                        pass

        # Phase 3: Collect unique customer IDs and fetch in parallel
        customer_ids = set()
        for pd in payment_data_map.values():
            mid = pd.get("links", {}).get("mandate")
            if mid:
                mandate = self._mandates_cache.get(mid, {})
                cid = mandate.get("links", {}).get("customer")
                if cid and cid not in self._customers_cache:
                    customer_ids.add(cid)

        if customer_ids:
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(self.get_customer, cid): cid for cid in customer_ids}
                for future in as_completed(futures):
                    try:
                        future.result()  # Result is cached by get_customer()
                    except Exception:
                        pass

        # Phase 4: Assemble GoCardlessPayment objects (all data now cached)
        payments = []
        for pid in payment_ids:
            pd = payment_data_map.get(pid)
            if not pd:
                continue

            customer_id = None
            customer_name = None
            mandate_id = pd.get("links", {}).get("mandate")
            if mandate_id:
                mandate = self._mandates_cache.get(mandate_id, {})
                customer_id = mandate.get("links", {}).get("customer")
                if customer_id:
                    customer = self._customers_cache.get(customer_id, {})
                    customer_name = customer.get("company_name") or \
                                  f"{customer.get('given_name', '')} {customer.get('family_name', '')}".strip()

            payments.append(GoCardlessPayment(
                id=pd.get("id"),
                amount=int(pd.get("amount", 0)) / 100,
                currency=pd.get("currency", "GBP"),
                status=pd.get("status"),
                charge_date=self._parse_date(pd.get("charge_date")),
                customer_name=customer_name,
                customer_id=customer_id,
                mandate_id=mandate_id,
                description=pd.get("description"),
                reference=pd.get("reference"),
                metadata=pd.get("metadata", {})
            ))

        payout.payments = payments
        payout.fees_vat = fees_vat

        return payout

    def _parse_payout(self, data: Dict) -> GoCardlessPayout:
        """Parse payout data from API response"""
        # Parse FX data for foreign currency payouts
        fx_data = data.get("fx", {}) or {}
        fx_amount_pence = fx_data.get("fx_amount")
        fx_amount = int(fx_amount_pence) / 100 if fx_amount_pence is not None else None
        fx_currency = fx_data.get("fx_currency")
        exchange_rate = fx_data.get("exchange_rate")

        # Look up creditor bank account for sort code / account number
        bank_account_number = None
        bank_sort_code = None
        cba_id = data.get("links", {}).get("creditor_bank_account")
        if cba_id:
            try:
                cba = self.get_creditor_bank_account(cba_id)
                bank_account_number = cba.get("account_number_ending") or cba.get("account_number")
                bank_sort_code = cba.get("bank_code")  # GoCardless uses bank_code for sort code
            except Exception:
                pass

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
            payments=[],
            fx_amount=fx_amount,
            fx_currency=fx_currency,
            exchange_rate=exchange_rate,
            bank_account_number=bank_account_number,
            bank_sort_code=bank_sort_code
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

    # Auto-detect sandbox/live from token prefix — overrides the setting
    # to prevent mismatch (sandbox token against live API or vice versa)
    if access_token.startswith("sandbox_"):
        sandbox = True
    elif access_token.startswith("live_"):
        sandbox = False
    else:
        sandbox = settings.get("api_sandbox", False)

    return GoCardlessClient(access_token=access_token, sandbox=sandbox)


class GoCardlessPartnerClient:
    """
    GoCardless Partner/Connect OAuth flow for onboarding new merchants.

    Uses GoCardless OAuth Connect to:
    1. Generate an authorisation URL for a new merchant to sign up
    2. Exchange the authorisation code for an access token
    3. Track the merchant's setup progress

    Requires Partner credentials (client_id, client_secret) from GoCardless.
    See: https://developer.gocardless.com/getting-started/partners/
    """

    SANDBOX_CONNECT_URL = "https://connect-sandbox.gocardless.com"
    LIVE_CONNECT_URL = "https://connect.gocardless.com"
    SANDBOX_API_URL = "https://api-sandbox.gocardless.com"
    LIVE_API_URL = "https://api.gocardless.com"

    def __init__(self, client_id: str, client_secret: str, sandbox: bool = False):
        self.client_id = client_id
        self.client_secret = client_secret
        self.sandbox = sandbox
        self.connect_url = self.SANDBOX_CONNECT_URL if sandbox else self.LIVE_CONNECT_URL
        self.api_url = self.SANDBOX_API_URL if sandbox else self.LIVE_API_URL

    def get_authorisation_url(
        self,
        redirect_uri: str,
        scope: str = "read_write",
        prefill_email: Optional[str] = None,
        prefill_company_name: Optional[str] = None,
        state: Optional[str] = None,
    ) -> str:
        """
        Generate the OAuth authorisation URL for a new merchant to sign up.

        The merchant visits this URL, creates their GoCardless account (or logs in),
        and authorises our app. GoCardless then redirects back to redirect_uri
        with an authorisation code.

        Args:
            redirect_uri: URL GoCardless redirects to after authorisation
            scope: OAuth scope (default: read_write)
            prefill_email: Pre-fill the merchant's email on signup form
            prefill_company_name: Pre-fill the merchant's company name
            state: Opaque state parameter returned in callback (for CSRF protection)

        Returns:
            Full authorisation URL string
        """
        import urllib.parse

        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "scope": scope,
            "redirect_uri": redirect_uri,
            "access_type": "offline",  # Get refresh token
        }
        if prefill_email:
            params["prefill[email]"] = prefill_email
        if prefill_company_name:
            params["prefill[company_name]"] = prefill_company_name
        if state:
            params["state"] = state

        return f"{self.connect_url}/oauth/authorize?{urllib.parse.urlencode(params)}"

    def exchange_authorisation_code(
        self,
        code: str,
        redirect_uri: str,
    ) -> Dict[str, Any]:
        """
        Exchange an authorisation code for an access token.

        Called after the merchant completes signup and GoCardless redirects
        back to our redirect_uri with ?code=XXX.

        Args:
            code: The authorisation code from the callback
            redirect_uri: Must match the redirect_uri used in the authorisation URL

        Returns:
            Dict with access_token, token_type, scope, organisation_id
        """
        url = f"{self.connect_url}/oauth/access_token"
        data = {
            "grant_type": "authorization_code",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": code,
            "redirect_uri": redirect_uri,
        }

        try:
            response = requests.post(url, json=data, timeout=30)
            if response.status_code != 200:
                error_msg = response.text
                try:
                    error_data = response.json()
                    error_msg = error_data.get("error_description", error_data.get("error", error_msg))
                except Exception:
                    pass
                raise GoCardlessAPIError(
                    f"Token exchange failed: {error_msg}",
                    response.status_code,
                    "oauth_error"
                )
            return response.json()
        except requests.exceptions.RequestException as e:
            raise GoCardlessAPIError(f"Token exchange request failed: {e}", error_type="connection_error")

    def get_organisation_info(self, access_token: str) -> Dict[str, Any]:
        """
        Get the merchant's organisation info using their access token.
        Verifies the token works and retrieves merchant details.

        Args:
            access_token: The merchant's access token from token exchange

        Returns:
            Dict with creditor details (name, id, etc.)
        """
        url = f"{self.api_url}/creditors"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "GoCardless-Version": "2015-07-06",
            "Content-Type": "application/json",
        }
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                raise GoCardlessAPIError(
                    f"Failed to get organisation info: {response.status_code}",
                    response.status_code,
                )
            data = response.json()
            creditors = data.get("creditors", [])
            if creditors:
                return creditors[0]
            return {}
        except requests.exceptions.RequestException as e:
            raise GoCardlessAPIError(f"Organisation info request failed: {e}", error_type="connection_error")


def create_partner_client_from_settings(settings: Dict) -> Optional[GoCardlessPartnerClient]:
    """
    Create a GoCardless Partner client from settings dictionary.

    Args:
        settings: Dict containing 'partner_client_id' and 'partner_client_secret'

    Returns:
        GoCardlessPartnerClient or None if partner credentials not configured
    """
    client_id = settings.get("partner_client_id")
    client_secret = settings.get("partner_client_secret")
    if not client_id or not client_secret:
        return None

    sandbox = settings.get("api_sandbox", False)
    return GoCardlessPartnerClient(client_id=client_id, client_secret=client_secret, sandbox=sandbox)
