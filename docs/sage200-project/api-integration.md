# Sage 200 API Integration Guide

## Overview

This document details the technical integration with the Sage 200 REST API for SQL RAG.

## Authentication

### OAuth 2.0 Flow

Sage 200 uses OAuth 2.0 with Authorization Code flow:

```
1. User authorizes app → Sage 200 authorization endpoint
2. Sage returns authorization code
3. App exchanges code for access token + refresh token
4. Access token used for API calls (expires in 1 hour)
5. Refresh token used to get new access token
```

### Endpoints

| Environment | Authorization URL | Token URL |
|-------------|-------------------|-----------|
| Production | `https://id.sage.com/authorize` | `https://id.sage.com/oauth/token` |

### Token Refresh

```python
def refresh_access_token(refresh_token: str, client_id: str, client_secret: str) -> dict:
    response = requests.post(
        "https://id.sage.com/oauth/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret
        }
    )
    return response.json()
```

## API Base URLs

| Version | Base URL |
|---------|----------|
| Sage 200 Standard | `https://api.columbus.sage.com/uk/sage200` |
| Sage 200 Professional | `https://api.columbus.sage.com/uk/sage200` |

## Key API Endpoints

### Nominal Ledger

#### Get Nominal Codes
```
GET /nominal_codes
```

Response:
```json
{
  "nominal_codes": [
    {
      "id": 12345,
      "code": "1001",
      "name": "Bank Current Account",
      "type": "Balance Sheet",
      "reporting_category": "Current Assets"
    }
  ]
}
```

#### Post Nominal Journal
```
POST /nominal_journals
```

Request:
```json
{
  "journal_type": "Standard",
  "journal_date": "2025-02-12",
  "reference": "GC-FEES-001",
  "narrative": "GoCardless Fees",
  "journal_lines": [
    {
      "nominal_code_id": 12345,
      "value": 10.50,
      "debit_credit": "Debit"
    },
    {
      "nominal_code_id": 12346,
      "value": 10.50,
      "debit_credit": "Credit"
    }
  ]
}
```

### Cash Book

#### Get Bank Accounts
```
GET /bank_accounts
```

Response:
```json
{
  "bank_accounts": [
    {
      "id": 1001,
      "account_name": "Current Account",
      "account_number": "12345678",
      "sort_code": "01-02-03",
      "nominal_code_id": 12345,
      "balance": 15234.56
    }
  ]
}
```

#### Get Bank Transactions
```
GET /bank_posted_transactions?bank_account_id=1001&transaction_date_from=2025-01-01
```

#### Post Bank Transaction
```
POST /bank_posted_transactions
```

Request:
```json
{
  "bank_account_id": 1001,
  "transaction_date": "2025-02-12",
  "transaction_type": "BankPayment",
  "reference": "BACS-001",
  "narrative": "Supplier Payment",
  "exchange_rate": 1.0,
  "lines": [
    {
      "nominal_code_id": 12346,
      "value": 500.00,
      "tax_code_id": 1
    }
  ]
}
```

Transaction Types:
- `BankReceipt` - Money in
- `BankPayment` - Money out
- `BankTransfer` - Between accounts

### Sales Ledger

#### Get Customers
```
GET /customers
```

#### Get Outstanding Invoices
```
GET /sales_invoices?customer_id=123&outstanding_only=true
```

#### Post Sales Receipt
```
POST /sales_receipts
```

Request:
```json
{
  "customer_id": 123,
  "bank_account_id": 1001,
  "transaction_date": "2025-02-12",
  "reference": "GC-BATCH-001",
  "exchange_rate": 1.0,
  "receipt_value": 1500.00
}
```

#### Allocate Receipt to Invoice
```
POST /sales_receipt_allocations
```

Request:
```json
{
  "sales_receipt_id": 456,
  "sales_invoice_id": 789,
  "allocation_value": 1500.00
}
```

### Purchase Ledger

#### Get Suppliers
```
GET /suppliers
```

#### Post Purchase Payment
```
POST /purchase_payments
```

Request:
```json
{
  "supplier_id": 456,
  "bank_account_id": 1001,
  "transaction_date": "2025-02-12",
  "reference": "BACS-002",
  "exchange_rate": 1.0,
  "payment_value": 2500.00
}
```

## Error Handling

### HTTP Status Codes

| Code | Meaning | Action |
|------|---------|--------|
| 200 | Success | Process response |
| 201 | Created | Process response |
| 400 | Bad Request | Check request body |
| 401 | Unauthorized | Refresh token |
| 403 | Forbidden | Check permissions |
| 404 | Not Found | Check ID/endpoint |
| 429 | Rate Limited | Wait and retry |
| 500 | Server Error | Retry with backoff |

### Error Response Format

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "The request was invalid",
    "details": [
      {
        "field": "transaction_date",
        "message": "Must be within open period"
      }
    ]
  }
}
```

## Rate Limiting

- **Limit**: 1000 requests per minute
- **Header**: `X-RateLimit-Remaining` shows remaining requests
- **Strategy**: Implement exponential backoff on 429 responses

## Pagination

Large result sets are paginated:

```
GET /customers?$top=100&$skip=0
```

Response includes:
```json
{
  "$total": 500,
  "$top": 100,
  "$skip": 0,
  "customers": [...]
}
```

## Filtering & Sorting

OData-style filtering:

```
GET /bank_posted_transactions?$filter=transaction_date ge 2025-01-01&$orderby=transaction_date desc
```

## Python Client Example

```python
class Sage200Client:
    BASE_URL = "https://api.columbus.sage.com/uk/sage200"

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.refresh_token = None
        self.token_expires = None

    def _ensure_token(self):
        """Refresh token if expired."""
        if not self.access_token or datetime.now() >= self.token_expires:
            self._refresh_token()

    def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make authenticated API request."""
        self._ensure_token()

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        response = requests.request(
            method,
            f"{self.BASE_URL}{endpoint}",
            headers=headers,
            **kwargs
        )

        if response.status_code == 429:
            time.sleep(60)
            return self._request(method, endpoint, **kwargs)

        response.raise_for_status()
        return response.json()

    def get_bank_accounts(self) -> list:
        """Get all bank accounts."""
        return self._request("GET", "/bank_accounts")["bank_accounts"]

    def post_bank_receipt(self, bank_account_id: int, date: str,
                          amount: float, reference: str,
                          nominal_code_id: int) -> dict:
        """Post a bank receipt."""
        return self._request("POST", "/bank_posted_transactions", json={
            "bank_account_id": bank_account_id,
            "transaction_date": date,
            "transaction_type": "BankReceipt",
            "reference": reference,
            "lines": [{
                "nominal_code_id": nominal_code_id,
                "value": amount
            }]
        })
```

## Testing

### Postman Collection

A Postman collection is available for testing API endpoints. Import and configure:

1. Set `client_id` and `client_secret` environment variables
2. Run OAuth flow to get tokens
3. Test individual endpoints

### Test Environment

- Use Sage 200 development/test instance
- Request development API credentials (not production)
- Create test data that mirrors production scenarios
