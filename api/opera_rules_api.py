"""
Opera Integration Rules API

Exposes the business rules, validation logic, and integration guidelines
for applications integrating with Pegasus Opera accounting system.

This API provides:
- Period posting rules (OPA, Real Time Update)
- Transaction type definitions and requirements
- Field formats and validation rules
- Control account mappings
- Amount conventions
- Double-entry requirements
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional, Dict, Any, List
from pydantic import BaseModel
from enum import Enum

router = APIRouter(prefix="/api/opera-rules", tags=["Opera Integration Rules"])


# ============================================================================
# MODELS
# ============================================================================

class TransactionType(str, Enum):
    SALES_RECEIPT = "sales_receipt"
    SALES_REFUND = "sales_refund"
    PURCHASE_PAYMENT = "purchase_payment"
    PURCHASE_REFUND = "purchase_refund"
    NOMINAL_PAYMENT = "nominal_payment"
    NOMINAL_RECEIPT = "nominal_receipt"


class LedgerType(str, Enum):
    SALES = "SL"
    PURCHASE = "PL"
    NOMINAL = "NL"
    CASHBOOK = "CB"


class AmountConvention(BaseModel):
    table: str
    unit: str  # "pence" or "pounds"
    description: str


class FieldRule(BaseModel):
    field: str
    max_length: int
    data_type: str
    required: bool
    description: str
    example: Optional[str] = None


class TransactionRule(BaseModel):
    type: str
    description: str
    cashbook_type_code: int
    amount_sign: str  # "positive" or "negative"
    tables_affected: List[str]
    double_entry: Dict[str, str]
    required_fields: List[str]
    optional_fields: List[str]


class PeriodRule(BaseModel):
    setting: str
    description: str
    when_enabled: str
    when_disabled: str


# ============================================================================
# AMOUNT CONVENTIONS
# ============================================================================

AMOUNT_CONVENTIONS = [
    AmountConvention(
        table="aentry",
        unit="pence",
        description="Cashbook header - amounts stored in pence (multiply by 100)"
    ),
    AmountConvention(
        table="atran",
        unit="pence",
        description="Cashbook transactions - amounts stored in pence"
    ),
    AmountConvention(
        table="ntran",
        unit="pounds",
        description="Nominal ledger transactions - amounts in pounds"
    ),
    AmountConvention(
        table="stran",
        unit="pounds",
        description="Sales ledger transactions - amounts in pounds"
    ),
    AmountConvention(
        table="ptran",
        unit="pounds",
        description="Purchase ledger transactions - amounts in pounds"
    ),
    AmountConvention(
        table="anoml",
        unit="pounds",
        description="Cashbook transfer file - amounts in pounds"
    ),
    AmountConvention(
        table="snoml",
        unit="pounds",
        description="Sales ledger transfer file - amounts in pounds"
    ),
    AmountConvention(
        table="pnoml",
        unit="pounds",
        description="Purchase ledger transfer file - amounts in pounds"
    ),
    AmountConvention(
        table="nbank",
        unit="pence",
        description="Bank master - current balance in pence"
    ),
]


# ============================================================================
# TRANSACTION RULES
# ============================================================================

TRANSACTION_RULES = {
    "sales_receipt": TransactionRule(
        type="sales_receipt",
        description="Receipt from customer - reduces customer balance, increases bank",
        cashbook_type_code=1,
        amount_sign="positive",
        tables_affected=["aentry", "atran", "stran", "anoml", "ntran", "nacnt", "sname", "nbank"],
        double_entry={
            "debit": "Bank Account (increases)",
            "credit": "Debtors Control (decreases customer balance)"
        },
        required_fields=["bank_code", "customer_account", "amount", "post_date", "reference"],
        optional_fields=["description", "invoice_ref", "discount"]
    ),
    "sales_refund": TransactionRule(
        type="sales_refund",
        description="Refund to customer - increases customer balance, decreases bank",
        cashbook_type_code=2,
        amount_sign="negative",
        tables_affected=["aentry", "atran", "stran", "anoml", "ntran", "nacnt", "sname", "nbank"],
        double_entry={
            "debit": "Debtors Control (increases customer balance)",
            "credit": "Bank Account (decreases)"
        },
        required_fields=["bank_code", "customer_account", "amount", "post_date", "reference"],
        optional_fields=["description", "original_invoice_ref"]
    ),
    "purchase_payment": TransactionRule(
        type="purchase_payment",
        description="Payment to supplier - reduces supplier balance, decreases bank",
        cashbook_type_code=3,
        amount_sign="negative",
        tables_affected=["aentry", "atran", "ptran", "anoml", "ntran", "nacnt", "pname", "nbank"],
        double_entry={
            "debit": "Creditors Control (reduces supplier balance)",
            "credit": "Bank Account (decreases)"
        },
        required_fields=["bank_code", "supplier_account", "amount", "post_date", "reference"],
        optional_fields=["description", "invoice_ref", "discount"]
    ),
    "purchase_refund": TransactionRule(
        type="purchase_refund",
        description="Refund from supplier - increases supplier balance, increases bank",
        cashbook_type_code=4,
        amount_sign="positive",
        tables_affected=["aentry", "atran", "ptran", "anoml", "ntran", "nacnt", "pname", "nbank"],
        double_entry={
            "debit": "Bank Account (increases)",
            "credit": "Creditors Control (increases supplier balance)"
        },
        required_fields=["bank_code", "supplier_account", "amount", "post_date", "reference"],
        optional_fields=["description", "original_invoice_ref"]
    ),
    "nominal_payment": TransactionRule(
        type="nominal_payment",
        description="Payment to nominal account (e.g., bank charges, fees)",
        cashbook_type_code=5,
        amount_sign="negative",
        tables_affected=["aentry", "atran", "anoml", "ntran", "nacnt", "nbank"],
        double_entry={
            "debit": "Expense Account (increases expense)",
            "credit": "Bank Account (decreases)"
        },
        required_fields=["bank_code", "nominal_account", "amount", "post_date", "reference"],
        optional_fields=["description", "vat_code", "vat_amount"]
    ),
    "nominal_receipt": TransactionRule(
        type="nominal_receipt",
        description="Receipt to nominal account (e.g., bank interest)",
        cashbook_type_code=6,
        amount_sign="positive",
        tables_affected=["aentry", "atran", "anoml", "ntran", "nacnt", "nbank"],
        double_entry={
            "debit": "Bank Account (increases)",
            "credit": "Income Account (increases income)"
        },
        required_fields=["bank_code", "nominal_account", "amount", "post_date", "reference"],
        optional_fields=["description"]
    ),
}


# ============================================================================
# PERIOD RULES
# ============================================================================

PERIOD_RULES = [
    PeriodRule(
        setting="Open Period Accounting (OPA)",
        description="Controls whether posting to non-current periods is allowed",
        when_enabled="Can post to any open period (past, current, or future) - checks nclndd for period status",
        when_disabled="Can only post to current period - past and future periods are blocked"
    ),
    PeriodRule(
        setting="Real Time Update (RTU)",
        description="Controls whether nominal ledger is updated immediately or via batch transfer",
        when_enabled="Transactions post to ntran immediately for current/past periods. Future periods go to transfer file.",
        when_disabled="All transactions go to transfer files (anoml/snoml/pnoml) only. Nominal updated at Period End."
    ),
]


# ============================================================================
# FIELD DEFINITIONS
# ============================================================================

FIELD_RULES = {
    "at_refer": FieldRule(
        field="at_refer",
        max_length=20,
        data_type="char",
        required=True,
        description="Transaction reference in cashbook",
        example="INV12345"
    ),
    "at_name": FieldRule(
        field="at_name",
        max_length=35,
        data_type="char",
        required=True,
        description="Name/description in cashbook transaction",
        example="Acme Corporation"
    ),
    "at_comment": FieldRule(
        field="at_comment",
        max_length=35,
        data_type="char",
        required=False,
        description="Additional comment on cashbook transaction",
        example="Payment for INV12345"
    ),
    "nt_trnref": FieldRule(
        field="nt_trnref",
        max_length=50,
        data_type="char",
        required=True,
        description="Transaction reference in nominal ledger - first 30 chars typically customer/supplier name",
        example="Acme Corporation              BACS       (RT)"
    ),
    "st_trref": FieldRule(
        field="st_trref",
        max_length=20,
        data_type="char",
        required=True,
        description="Transaction reference in sales ledger",
        example="REC001"
    ),
    "ae_entref": FieldRule(
        field="ae_entref",
        max_length=20,
        data_type="char",
        required=False,
        description="Entry reference in cashbook header",
        example="BACS"
    ),
}


# ============================================================================
# UNIQUE ID FORMAT
# ============================================================================

UNIQUE_ID_RULES = {
    "format": "_XXXXXXXXX",
    "description": "Opera unique IDs are underscore prefix followed by 9 base-36 characters",
    "example": "_7E00XM9II",
    "usage": "Used in at_unique, st_unique, nt_pstid, ax_unique fields for record linking"
}


# ============================================================================
# CONTROL ACCOUNTS
# ============================================================================

CONTROL_ACCOUNT_RULES = {
    "debtors_control": {
        "description": "Sales Ledger control account - represents total owed by customers",
        "source_primary": "sprfls.sc_dbtctrl",
        "source_fallback": "nparm.np_dca",
        "typical_code": "BB020",
        "usage": "Credit when posting sales receipts, Debit when posting sales refunds"
    },
    "creditors_control": {
        "description": "Purchase Ledger control account - represents total owed to suppliers",
        "source_primary": "sprfls.pc_crdctrl (via pparm)",
        "source_fallback": "nparm.np_cca",
        "typical_code": "CC020",
        "usage": "Debit when posting purchase payments, Credit when posting purchase refunds"
    }
}


# ============================================================================
# TRANSFER FILE RULES
# ============================================================================

TRANSFER_FILE_RULES = {
    "anoml": {
        "source": "Cashbook (aentry/atran)",
        "done_field": "ax_done",
        "done_values": {
            "Y": "Posted to nominal ledger (ntran exists)",
            " ": "Pending - will be processed at Period End"
        },
        "description": "Cashbook to Nominal transfer file"
    },
    "snoml": {
        "source": "Sales Ledger (stran)",
        "done_field": "sx_done",
        "done_values": {
            "Y": "Posted to nominal ledger",
            " ": "Pending"
        },
        "description": "Sales Ledger to Nominal transfer file"
    },
    "pnoml": {
        "source": "Purchase Ledger (ptran)",
        "done_field": "px_done",
        "done_values": {
            "Y": "Posted to nominal ledger",
            " ": "Pending"
        },
        "description": "Purchase Ledger to Nominal transfer file"
    }
}


# ============================================================================
# BALANCE UPDATE RULES
# ============================================================================

BALANCE_UPDATE_RULES = {
    "nacnt": {
        "description": "Nominal account balance table - MUST be updated after every ntran INSERT",
        "fields": {
            "na_ptddr": "Period to date debit",
            "na_ptdcr": "Period to date credit",
            "na_ytddr": "Year to date debit",
            "na_ytdcr": "Year to date credit",
            "na_balc01-12": "Period balance (01=Jan, 02=Feb, etc.)"
        },
        "rules": {
            "debit": "Add to na_ptddr, na_ytddr, na_balc{period}",
            "credit": "Add ABS value to na_ptdcr, na_ytdcr; Add signed value to na_balc{period}"
        }
    },
    "nbank": {
        "description": "Bank master balance - MUST be updated after cashbook transactions",
        "field": "nk_curbal",
        "unit": "pence",
        "rules": {
            "receipt": "Add amount (increases bank balance)",
            "payment": "Subtract amount (decreases bank balance)"
        }
    },
    "sname": {
        "description": "Customer master balance",
        "field": "sn_currbal",
        "rules": {
            "receipt": "Subtract amount (reduces amount owed)",
            "refund": "Add amount (increases amount owed)"
        }
    },
    "pname": {
        "description": "Supplier master balance",
        "field": "pn_currbal",
        "rules": {
            "payment": "Subtract amount (reduces amount owed)",
            "refund": "Add amount (increases amount owed)"
        }
    }
}


# ============================================================================
# API ENDPOINTS
# ============================================================================

@router.get("/amount-conventions")
async def get_amount_conventions() -> Dict[str, Any]:
    """
    Get amount storage conventions for each Opera table.

    Critical for correct posting - some tables store in pence, others in pounds.
    """
    return {
        "success": True,
        "description": "Amount storage conventions by table",
        "warning": "Mixing up pence/pounds will cause 100x errors in balances",
        "conventions": [conv.dict() for conv in AMOUNT_CONVENTIONS]
    }


@router.get("/transaction-types")
async def get_transaction_types() -> Dict[str, Any]:
    """
    Get all transaction type definitions with their rules.
    """
    return {
        "success": True,
        "description": "Transaction types and their posting rules",
        "types": {k: v.dict() for k, v in TRANSACTION_RULES.items()}
    }


@router.get("/transaction-types/{transaction_type}")
async def get_transaction_type(transaction_type: str) -> Dict[str, Any]:
    """
    Get rules for a specific transaction type.
    """
    if transaction_type not in TRANSACTION_RULES:
        raise HTTPException(status_code=404, detail=f"Transaction type '{transaction_type}' not found")

    return {
        "success": True,
        "transaction_type": transaction_type,
        "rules": TRANSACTION_RULES[transaction_type].dict()
    }


@router.get("/period-rules")
async def get_period_rules() -> Dict[str, Any]:
    """
    Get period posting rules (OPA and Real Time Update).
    """
    return {
        "success": True,
        "description": "Period posting rules - OPA and Real Time Update settings",
        "config_location": "Opera3SESystem.dbo.seqco",
        "fields": {
            "co_opanl": "Open Period Accounting flag",
            "co_rtupdnl": "Real Time Update flag"
        },
        "rules": [rule.dict() for rule in PERIOD_RULES],
        "posting_decision_logic": {
            "step_1": "Check OPA - if OFF, only current period allowed",
            "step_2": "If OPA ON, check nclndd for period status (0=Open, 1=Current, 2=Closed)",
            "step_3": "Check Real Time Update - determines if ntran created or just transfer file",
            "step_4": "For RTU ON + current/past period: post to ntran with done='Y'",
            "step_5": "For RTU ON + future period OR RTU OFF: transfer file only with done=' '"
        }
    }


@router.get("/field-rules")
async def get_field_rules() -> Dict[str, Any]:
    """
    Get field format and validation rules.
    """
    return {
        "success": True,
        "description": "Field formats and validation rules",
        "fields": {k: v.dict() for k, v in FIELD_RULES.items()}
    }


@router.get("/unique-id-format")
async def get_unique_id_format() -> Dict[str, Any]:
    """
    Get Opera unique ID format rules.
    """
    return {
        "success": True,
        "description": "Opera unique ID generation format",
        "rules": UNIQUE_ID_RULES,
        "generation_algorithm": {
            "step_1": "Get current timestamp in milliseconds",
            "step_2": "Combine with sequence number for uniqueness",
            "step_3": "Convert to base-36 (0-9, A-Z)",
            "step_4": "Pad to 9 characters, prefix with underscore"
        }
    }


@router.get("/control-accounts")
async def get_control_account_rules() -> Dict[str, Any]:
    """
    Get control account configuration rules.
    """
    return {
        "success": True,
        "description": "Control account configuration and usage",
        "accounts": CONTROL_ACCOUNT_RULES,
        "important": "Control accounts MUST match between sub-ledgers and nominal for balance checks to pass"
    }


@router.get("/transfer-files")
async def get_transfer_file_rules() -> Dict[str, Any]:
    """
    Get transfer file rules (anoml, snoml, pnoml).
    """
    return {
        "success": True,
        "description": "Transfer file rules for nominal ledger posting",
        "files": TRANSFER_FILE_RULES,
        "important": "done flag must match whether ntran entry exists - mismatch causes audit issues"
    }


@router.get("/balance-updates")
async def get_balance_update_rules() -> Dict[str, Any]:
    """
    Get balance update rules - CRITICAL for data integrity.
    """
    return {
        "success": True,
        "description": "Balance update rules - MUST be followed for every transaction",
        "warning": "Failing to update balances causes control account mismatches",
        "tables": BALANCE_UPDATE_RULES
    }


@router.get("/double-entry/{transaction_type}")
async def get_double_entry_rules(transaction_type: str) -> Dict[str, Any]:
    """
    Get double-entry accounting rules for a transaction type.
    """
    if transaction_type not in TRANSACTION_RULES:
        raise HTTPException(status_code=404, detail=f"Transaction type '{transaction_type}' not found")

    rule = TRANSACTION_RULES[transaction_type]

    return {
        "success": True,
        "transaction_type": transaction_type,
        "description": rule.description,
        "double_entry": rule.double_entry,
        "tables_affected": rule.tables_affected,
        "example": {
            "sales_receipt_1000": {
                "debit": {"account": "Bank (BC010)", "amount": 1000},
                "credit": {"account": "Debtors Control (BB020)", "amount": -1000}
            }
        } if transaction_type == "sales_receipt" else None
    }


@router.get("/validation-checklist")
async def get_validation_checklist() -> Dict[str, Any]:
    """
    Get pre-posting validation checklist.
    """
    return {
        "success": True,
        "description": "Validation checklist before posting transactions",
        "checklist": [
            {
                "check": "Period Validation",
                "description": "Verify post date is in an allowed period",
                "how": "Call get_period_posting_decision() with post_date and ledger_type"
            },
            {
                "check": "Account Exists",
                "description": "Verify customer/supplier/nominal account exists and is not stopped",
                "how": "Query sname/pname/nacnt with account code"
            },
            {
                "check": "Bank Account Valid",
                "description": "Verify bank account exists in nbank",
                "how": "Query nbank with bank_code"
            },
            {
                "check": "Control Account Configured",
                "description": "Verify control account is configured for the ledger",
                "how": "Call get_control_accounts() and verify result"
            },
            {
                "check": "Duplicate Check",
                "description": "Check for duplicate transactions",
                "how": "Query atran for matching date, amount, reference"
            },
            {
                "check": "Amount Sign",
                "description": "Verify amount sign matches transaction type",
                "how": "Receipts = positive, Payments = negative"
            }
        ]
    }


@router.get("/complete-rules")
async def get_complete_rules() -> Dict[str, Any]:
    """
    Get all Opera integration rules in a single response.
    """
    return {
        "success": True,
        "description": "Complete Opera integration rules",
        "version": "1.0",
        "last_updated": RULES_LAST_UPDATED,
        "amount_conventions": [conv.dict() for conv in AMOUNT_CONVENTIONS],
        "transaction_types": {k: v.dict() for k, v in TRANSACTION_RULES.items()},
        "period_rules": [rule.dict() for rule in PERIOD_RULES],
        "field_rules": {k: v.dict() for k, v in FIELD_RULES.items()},
        "unique_id_format": UNIQUE_ID_RULES,
        "control_accounts": CONTROL_ACCOUNT_RULES,
        "transfer_files": TRANSFER_FILE_RULES,
        "balance_updates": BALANCE_UPDATE_RULES,
        "knowledge_sources": KNOWLEDGE_SOURCES
    }


# ============================================================================
# AUTO-UPDATE FROM KNOWLEDGE FILES
# ============================================================================

import os
import re
from datetime import datetime

# Track when rules were last updated
RULES_LAST_UPDATED = datetime.now().isoformat()

# Knowledge source files
KNOWLEDGE_SOURCES = [
    "docs/opera_knowledge_base.md",
    "docs/opera_transaction_types.md",
    "docs/bank_statement_import.md",
    "docs/gocardless_integration.md"
]

# Dynamic rules extracted from knowledge files
DYNAMIC_RULES: Dict[str, Any] = {}


def extract_rules_from_markdown(filepath: str) -> Dict[str, Any]:
    """Extract rules and tables from markdown documentation."""
    rules = {
        "tables": [],
        "fields": [],
        "warnings": [],
        "examples": []
    }

    if not os.path.exists(filepath):
        return rules

    with open(filepath, 'r') as f:
        content = f.read()

    # Extract table references
    table_pattern = r'\b(aentry|atran|ntran|stran|ptran|anoml|snoml|pnoml|nacnt|nbank|sname|pname|nparm|nclndd|seqco|sprfls|pparm|atype)\b'
    rules["tables"] = list(set(re.findall(table_pattern, content, re.IGNORECASE)))

    # Extract field references (xx_fieldname pattern)
    field_pattern = r'\b([a-z]{2}_[a-z0-9_]+)\b'
    rules["fields"] = list(set(re.findall(field_pattern, content)))[:50]  # Limit to 50

    # Extract warnings/important notes
    warning_pattern = r'(?:IMPORTANT|WARNING|CRITICAL|NOTE)[:\s]*([^\n]+)'
    rules["warnings"] = re.findall(warning_pattern, content, re.IGNORECASE)[:20]

    return rules


def reload_rules_from_knowledge():
    """Reload rules from all knowledge source files."""
    global DYNAMIC_RULES, RULES_LAST_UPDATED

    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    all_tables = set()
    all_fields = set()
    all_warnings = []

    for source in KNOWLEDGE_SOURCES:
        filepath = os.path.join(base_path, source)
        extracted = extract_rules_from_markdown(filepath)
        all_tables.update(extracted["tables"])
        all_fields.update(extracted["fields"])
        all_warnings.extend(extracted["warnings"])

    DYNAMIC_RULES = {
        "referenced_tables": sorted(list(all_tables)),
        "referenced_fields": sorted(list(all_fields)),
        "important_notes": all_warnings[:30],
        "source_files": KNOWLEDGE_SOURCES
    }

    RULES_LAST_UPDATED = datetime.now().isoformat()
    return DYNAMIC_RULES


# Initial load
reload_rules_from_knowledge()


@router.post("/reload-rules")
async def reload_rules() -> Dict[str, Any]:
    """
    Reload rules from knowledge documentation files.

    Call this after updating documentation to refresh the rules API.
    """
    rules = reload_rules_from_knowledge()
    return {
        "success": True,
        "message": "Rules reloaded from knowledge files",
        "last_updated": RULES_LAST_UPDATED,
        "tables_found": len(rules["referenced_tables"]),
        "fields_found": len(rules["referenced_fields"]),
        "warnings_found": len(rules["important_notes"])
    }


@router.get("/dynamic-rules")
async def get_dynamic_rules() -> Dict[str, Any]:
    """
    Get dynamically extracted rules from knowledge documentation.

    These rules are automatically updated when /reload-rules is called.
    """
    return {
        "success": True,
        "description": "Rules extracted from knowledge documentation",
        "last_updated": RULES_LAST_UPDATED,
        "rules": DYNAMIC_RULES
    }


@router.get("/knowledge-sources")
async def get_knowledge_sources() -> Dict[str, Any]:
    """
    Get list of knowledge source files used for rules.
    """
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    sources = []
    for source in KNOWLEDGE_SOURCES:
        filepath = os.path.join(base_path, source)
        exists = os.path.exists(filepath)
        size = os.path.getsize(filepath) if exists else 0
        modified = datetime.fromtimestamp(os.path.getmtime(filepath)).isoformat() if exists else None

        sources.append({
            "file": source,
            "exists": exists,
            "size_bytes": size,
            "last_modified": modified
        })

    return {
        "success": True,
        "sources": sources,
        "rules_last_updated": RULES_LAST_UPDATED
    }
