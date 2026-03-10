"""
Opera 3 Write Agent Service

A FastAPI microservice that runs on the Opera 3 server alongside the FoxPro
data files. Handles all DBF writes with proper CDX index maintenance via the
Harbour DBFCDX bridge.

The main application proxies all Opera 3 write operations through this service,
ensuring data integrity (CDX indexes, VFP-compatible locking, proper memo handling).

Usage:
    uvicorn opera3_agent.service:app --host 0.0.0.0 --port 9000

Environment Variables:
    OPERA3_DATA_PATH    Path to Opera 3 data files (e.g. C:\\Apps\\O3 Server VFP)
    OPERA3_AGENT_PORT   Port to listen on (default: 9000)
    OPERA3_AGENT_KEY    Shared secret for authentication (optional)
"""

from __future__ import annotations

import os
import sys
import time
import logging
import platform
from datetime import date, datetime
from typing import Optional, List, Dict, Any
from pathlib import Path
from dataclasses import dataclass, field, asdict

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

# ============================================================
# Configuration
# ============================================================

OPERA3_DATA_PATH = os.environ.get("OPERA3_DATA_PATH", "")
OPERA3_AGENT_KEY = os.environ.get("OPERA3_AGENT_KEY", "")
AGENT_VERSION = "1.0.0"
START_TIME = time.time()


# ============================================================
# Pydantic Models (API request/response schemas)
# ============================================================

class ImportResult(BaseModel):
    """Standard import result matching Opera3ImportResult dataclass."""
    success: bool
    records_processed: int = 0
    records_imported: int = 0
    records_failed: int = 0
    entry_number: str = ""
    journal_number: int = 0
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    new_reconciled_balance: Optional[float] = None


class HealthResponse(BaseModel):
    status: str
    version: str
    uptime: float
    data_path: str
    data_path_exists: bool
    harbour_available: bool
    platform: str
    hostname: str


# -- Transaction request models --

class PurchasePaymentRequest(BaseModel):
    bank_account: str
    supplier_account: str
    amount_pounds: float
    reference: str
    post_date: str  # ISO format YYYY-MM-DD
    input_by: str = "IMPORT"
    creditors_control: Optional[str] = None
    payment_type: str = "Direct Cr"
    cbtype: Optional[str] = None
    validate_only: bool = False


class SalesReceiptRequest(BaseModel):
    bank_account: str
    customer_account: str
    amount_pounds: float
    reference: str
    post_date: str
    input_by: str = "IMPORT"
    debtors_control: Optional[str] = None
    receipt_type: str = "BACS"
    cbtype: Optional[str] = None
    validate_only: bool = False


class SalesRefundRequest(BaseModel):
    bank_account: str
    customer_account: str
    amount_pounds: float
    reference: str
    post_date: str
    input_by: str = "IMPORT"
    debtors_control: Optional[str] = None
    payment_method: str = "BACS"
    cbtype: Optional[str] = None
    validate_only: bool = False
    comment: str = ""


class PurchaseRefundRequest(BaseModel):
    bank_account: str
    supplier_account: str
    amount_pounds: float
    reference: str
    post_date: str
    input_by: str = "IMPORT"
    creditors_control: Optional[str] = None
    payment_type: str = "Direct Cr"
    cbtype: Optional[str] = None
    validate_only: bool = False
    comment: str = ""


class BankTransferRequest(BaseModel):
    source_bank: str
    dest_bank: str
    amount_pounds: float
    reference: str
    post_date: str
    comment: str = ""
    input_by: str = "SQLRAG"
    post_to_nominal: bool = True
    cbtype: Optional[str] = None


class NominalEntryRequest(BaseModel):
    bank_account: str
    nominal_account: str
    amount_pounds: float
    reference: str
    post_date: str
    description: str = ""
    input_by: str = "IMPORT"
    is_receipt: bool = False
    cbtype: Optional[str] = None
    validate_only: bool = False
    project_code: str = ""
    department_code: str = ""
    vat_code: str = ""


class GoCardlessBatchRequest(BaseModel):
    bank_account: str
    payments: List[Dict[str, Any]]
    post_date: str
    reference: str = "GoCardless"
    gocardless_fees: float = 0.0
    vat_on_fees: float = 0.0
    fees_nominal_account: Optional[str] = None
    fees_vat_code: str = "2"
    fees_payment_type: Optional[str] = None
    complete_batch: bool = False
    input_by: str = "GOCARDLS"
    cbtype: Optional[str] = None
    validate_only: bool = False
    auto_allocate: bool = False
    currency: Optional[str] = None
    destination_bank: Optional[str] = None
    transfer_cbtype: Optional[str] = None


class RecurringEntryRequest(BaseModel):
    bank_account: str
    entry_ref: str
    override_date: Optional[str] = None
    input_by: str = "RECUR"


class AutoAllocateReceiptRequest(BaseModel):
    customer_account: str
    receipt_ref: str
    receipt_amount: float
    allocation_date: str
    bank_account: str = ""
    description: Optional[str] = None


class AutoAllocatePaymentRequest(BaseModel):
    supplier_account: str
    payment_ref: str
    payment_amount: float
    allocation_date: str
    bank_account: str = ""
    description: Optional[str] = None


class ReconcileEntry(BaseModel):
    entry_number: Optional[str] = None
    ae_entry: Optional[str] = None
    line_number: int = 0
    statement_line: Optional[int] = None
    amount: Optional[float] = None


class MarkReconciledRequest(BaseModel):
    bank_account: str
    entries: List[Dict[str, Any]]
    statement_number: int
    statement_date: Optional[str] = None
    reconciliation_date: Optional[str] = None
    partial: bool = False


class DuplicateCheckRequest(BaseModel):
    bank_account: str
    transaction_date: str
    amount_pounds: float
    account_code: str = ""
    account_type: str = "nominal"
    date_tolerance_days: int = 1


# ============================================================
# Helper functions
# ============================================================

def parse_date(date_str: Optional[str]) -> Optional[date]:
    """Parse ISO date string to date object."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {date_str}. Expected YYYY-MM-DD")


def result_to_dict(result) -> dict:
    """Convert Opera3ImportResult (or dict) to serialisable dict."""
    if isinstance(result, dict):
        return result
    if hasattr(result, '__dataclass_fields__'):
        return asdict(result)
    # Fallback: convert known attributes
    return {
        "success": getattr(result, "success", False),
        "records_processed": getattr(result, "records_processed", 0),
        "records_imported": getattr(result, "records_imported", 0),
        "records_failed": getattr(result, "records_failed", 0),
        "entry_number": getattr(result, "entry_number", ""),
        "journal_number": getattr(result, "journal_number", 0),
        "errors": getattr(result, "errors", []),
        "warnings": getattr(result, "warnings", []),
        "new_reconciled_balance": getattr(result, "new_reconciled_balance", None),
    }


def get_importer():
    """Get or create the Opera3FoxProImport instance.

    This lazily imports and instantiates the importer so the service
    can start even if the data path isn't configured yet.
    """
    if not OPERA3_DATA_PATH:
        raise HTTPException(
            status_code=503,
            detail="OPERA3_DATA_PATH not configured. Set the environment variable and restart the service."
        )
    if not os.path.isdir(OPERA3_DATA_PATH):
        raise HTTPException(
            status_code=503,
            detail=f"Opera 3 data path does not exist: {OPERA3_DATA_PATH}"
        )

    # Import here to avoid circular imports and allow the service to start
    # even without the full sql_rag package during development
    try:
        from sql_rag.opera3_foxpro_import import Opera3FoxProImport
        return Opera3FoxProImport(OPERA3_DATA_PATH)
    except ImportError:
        # Fallback: try local import (when deployed standalone)
        try:
            from opera3_foxpro_import import Opera3FoxProImport
            return Opera3FoxProImport(OPERA3_DATA_PATH)
        except ImportError:
            raise HTTPException(
                status_code=503,
                detail="Opera3FoxProImport module not available. Check installation."
            )


# ============================================================
# Authentication
# ============================================================

async def verify_agent_key(x_agent_key: Optional[str] = Header(None)):
    """Verify the shared secret if configured."""
    if OPERA3_AGENT_KEY and x_agent_key != OPERA3_AGENT_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing agent key")


# ============================================================
# Application
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info(f"Opera 3 Write Agent v{AGENT_VERSION} starting")
    logger.info(f"Data path: {OPERA3_DATA_PATH}")
    logger.info(f"Platform: {platform.system()} {platform.machine()}")
    yield
    logger.info("Opera 3 Write Agent shutting down")


app = FastAPI(
    title="Opera 3 Write Agent",
    description="Handles all Opera 3 FoxPro DBF writes with CDX index maintenance",
    version=AGENT_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Health & Status Endpoints
# ============================================================

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint. Called by the main app every 30 seconds."""
    harbour_available = False
    try:
        from harbour_dbf import HarbourDBF
        harbour_available = True
    except (ImportError, Exception):
        pass

    return HealthResponse(
        status="ok",
        version=AGENT_VERSION,
        uptime=round(time.time() - START_TIME, 1),
        data_path=OPERA3_DATA_PATH,
        data_path_exists=os.path.isdir(OPERA3_DATA_PATH) if OPERA3_DATA_PATH else False,
        harbour_available=harbour_available,
        platform=f"{platform.system()} {platform.machine()}",
        hostname=platform.node(),
    )


@app.get("/status")
async def detailed_status():
    """Detailed status including table accessibility."""
    tables_accessible = {}
    if OPERA3_DATA_PATH and os.path.isdir(OPERA3_DATA_PATH):
        data_path = Path(OPERA3_DATA_PATH)
        for table_name in ["aentry", "atran", "ntran", "ptran", "stran",
                           "anoml", "nacnt", "nhist", "nbank", "nparm",
                           "pname", "sname", "atype", "palloc", "salloc",
                           "zvtran", "nvat", "arhead"]:
            dbf_path = data_path / f"{table_name}.dbf"
            if not dbf_path.exists():
                dbf_path = data_path / f"{table_name.upper()}.DBF"
            tables_accessible[table_name] = dbf_path.exists()

    return {
        "version": AGENT_VERSION,
        "uptime": round(time.time() - START_TIME, 1),
        "data_path": OPERA3_DATA_PATH,
        "tables": tables_accessible,
        "platform": f"{platform.system()} {platform.machine()}",
    }


# ============================================================
# Transaction Import Endpoints
# ============================================================

@app.post("/import/purchase-payment", dependencies=[Depends(verify_agent_key)])
async def import_purchase_payment(req: PurchasePaymentRequest):
    """Import a purchase payment (money out to supplier)."""
    importer = get_importer()
    try:
        result = importer.import_purchase_payment(
            bank_account=req.bank_account,
            supplier_account=req.supplier_account,
            amount_pounds=req.amount_pounds,
            reference=req.reference,
            post_date=parse_date(req.post_date),
            input_by=req.input_by,
            creditors_control=req.creditors_control,
            payment_type=req.payment_type,
            cbtype=req.cbtype,
            validate_only=req.validate_only,
        )
        return result_to_dict(result)
    except Exception as e:
        logger.error(f"import_purchase_payment failed: {e}", exc_info=True)
        return {"success": False, "errors": [str(e)]}


@app.post("/import/sales-receipt", dependencies=[Depends(verify_agent_key)])
async def import_sales_receipt(req: SalesReceiptRequest):
    """Import a sales receipt (money in from customer)."""
    importer = get_importer()
    try:
        result = importer.import_sales_receipt(
            bank_account=req.bank_account,
            customer_account=req.customer_account,
            amount_pounds=req.amount_pounds,
            reference=req.reference,
            post_date=parse_date(req.post_date),
            input_by=req.input_by,
            debtors_control=req.debtors_control,
            receipt_type=req.receipt_type,
            cbtype=req.cbtype,
            validate_only=req.validate_only,
        )
        return result_to_dict(result)
    except Exception as e:
        logger.error(f"import_sales_receipt failed: {e}", exc_info=True)
        return {"success": False, "errors": [str(e)]}


@app.post("/import/sales-refund", dependencies=[Depends(verify_agent_key)])
async def import_sales_refund(req: SalesRefundRequest):
    """Import a sales refund (money out to customer)."""
    importer = get_importer()
    try:
        result = importer.import_sales_refund(
            bank_account=req.bank_account,
            customer_account=req.customer_account,
            amount_pounds=req.amount_pounds,
            reference=req.reference,
            post_date=parse_date(req.post_date),
            input_by=req.input_by,
            debtors_control=req.debtors_control,
            payment_method=req.payment_method,
            cbtype=req.cbtype,
            validate_only=req.validate_only,
            comment=req.comment,
        )
        return result_to_dict(result)
    except Exception as e:
        logger.error(f"import_sales_refund failed: {e}", exc_info=True)
        return {"success": False, "errors": [str(e)]}


@app.post("/import/purchase-refund", dependencies=[Depends(verify_agent_key)])
async def import_purchase_refund(req: PurchaseRefundRequest):
    """Import a purchase refund (money in from supplier)."""
    importer = get_importer()
    try:
        result = importer.import_purchase_refund(
            bank_account=req.bank_account,
            supplier_account=req.supplier_account,
            amount_pounds=req.amount_pounds,
            reference=req.reference,
            post_date=parse_date(req.post_date),
            input_by=req.input_by,
            creditors_control=req.creditors_control,
            payment_type=req.payment_type,
            cbtype=req.cbtype,
            validate_only=req.validate_only,
            comment=req.comment,
        )
        return result_to_dict(result)
    except Exception as e:
        logger.error(f"import_purchase_refund failed: {e}", exc_info=True)
        return {"success": False, "errors": [str(e)]}


@app.post("/import/bank-transfer", dependencies=[Depends(verify_agent_key)])
async def import_bank_transfer(req: BankTransferRequest):
    """Import a bank transfer between two bank accounts."""
    importer = get_importer()
    try:
        result = importer.import_bank_transfer(
            source_bank=req.source_bank,
            dest_bank=req.dest_bank,
            amount_pounds=req.amount_pounds,
            reference=req.reference,
            post_date=parse_date(req.post_date),
            comment=req.comment,
            input_by=req.input_by,
            post_to_nominal=req.post_to_nominal,
            cbtype=req.cbtype,
        )
        return result_to_dict(result)
    except Exception as e:
        logger.error(f"import_bank_transfer failed: {e}", exc_info=True)
        return {"success": False, "errors": [str(e)]}


@app.post("/import/nominal-entry", dependencies=[Depends(verify_agent_key)])
async def import_nominal_entry(req: NominalEntryRequest):
    """Import a nominal entry (direct to nominal account)."""
    importer = get_importer()
    try:
        result = importer.import_nominal_entry(
            bank_account=req.bank_account,
            nominal_account=req.nominal_account,
            amount_pounds=req.amount_pounds,
            reference=req.reference,
            post_date=parse_date(req.post_date),
            description=req.description,
            input_by=req.input_by,
            is_receipt=req.is_receipt,
            cbtype=req.cbtype,
            validate_only=req.validate_only,
            project_code=req.project_code,
            department_code=req.department_code,
            vat_code=req.vat_code,
        )
        return result_to_dict(result)
    except Exception as e:
        logger.error(f"import_nominal_entry failed: {e}", exc_info=True)
        return {"success": False, "errors": [str(e)]}


@app.post("/import/gocardless-batch", dependencies=[Depends(verify_agent_key)])
async def import_gocardless_batch(req: GoCardlessBatchRequest):
    """Import a GoCardless batch of customer payments."""
    importer = get_importer()
    try:
        result = importer.import_gocardless_batch(
            bank_account=req.bank_account,
            payments=req.payments,
            post_date=parse_date(req.post_date),
            reference=req.reference,
            gocardless_fees=req.gocardless_fees,
            vat_on_fees=req.vat_on_fees,
            fees_nominal_account=req.fees_nominal_account,
            fees_vat_code=req.fees_vat_code,
            fees_payment_type=req.fees_payment_type,
            complete_batch=req.complete_batch,
            input_by=req.input_by,
            cbtype=req.cbtype,
            validate_only=req.validate_only,
            auto_allocate=req.auto_allocate,
            currency=req.currency,
            destination_bank=req.destination_bank,
            transfer_cbtype=req.transfer_cbtype,
        )
        return result_to_dict(result)
    except Exception as e:
        logger.error(f"import_gocardless_batch failed: {e}", exc_info=True)
        return {"success": False, "errors": [str(e)]}


@app.post("/import/recurring-entry", dependencies=[Depends(verify_agent_key)])
async def post_recurring_entry(req: RecurringEntryRequest):
    """Post a recurring entry from arhead/arline."""
    importer = get_importer()
    try:
        result = importer.post_recurring_entry(
            bank_account=req.bank_account,
            entry_ref=req.entry_ref,
            override_date=parse_date(req.override_date),
            input_by=req.input_by,
        )
        return result_to_dict(result)
    except Exception as e:
        logger.error(f"post_recurring_entry failed: {e}", exc_info=True)
        return {"success": False, "errors": [str(e)]}


# ============================================================
# Allocation Endpoints
# ============================================================

@app.post("/allocate/receipt", dependencies=[Depends(verify_agent_key)])
async def auto_allocate_receipt(req: AutoAllocateReceiptRequest):
    """Auto-allocate a customer receipt to invoices."""
    importer = get_importer()
    try:
        result = importer.auto_allocate_receipt(
            customer_account=req.customer_account,
            receipt_ref=req.receipt_ref,
            receipt_amount=req.receipt_amount,
            allocation_date=parse_date(req.allocation_date),
            bank_account=req.bank_account,
            description=req.description,
        )
        return result_to_dict(result)
    except Exception as e:
        logger.error(f"auto_allocate_receipt failed: {e}", exc_info=True)
        return {"success": False, "errors": [str(e)]}


@app.post("/allocate/payment", dependencies=[Depends(verify_agent_key)])
async def auto_allocate_payment(req: AutoAllocatePaymentRequest):
    """Auto-allocate a supplier payment to invoices."""
    importer = get_importer()
    try:
        result = importer.auto_allocate_payment(
            supplier_account=req.supplier_account,
            payment_ref=req.payment_ref,
            payment_amount=req.payment_amount,
            allocation_date=parse_date(req.allocation_date),
            bank_account=req.bank_account,
            description=req.description,
        )
        return result_to_dict(result)
    except Exception as e:
        logger.error(f"auto_allocate_payment failed: {e}", exc_info=True)
        return {"success": False, "errors": [str(e)]}


# ============================================================
# Reconciliation Endpoints
# ============================================================

@app.post("/reconcile/mark", dependencies=[Depends(verify_agent_key)])
async def mark_entries_reconciled(req: MarkReconciledRequest):
    """Mark cashbook entries as reconciled."""
    importer = get_importer()
    try:
        result = importer.mark_entries_reconciled(
            bank_account=req.bank_account,
            entries=req.entries,
            statement_number=req.statement_number,
            statement_date=parse_date(req.statement_date),
            reconciliation_date=parse_date(req.reconciliation_date),
            partial=req.partial,
        )
        return result_to_dict(result)
    except Exception as e:
        logger.error(f"mark_entries_reconciled failed: {e}", exc_info=True)
        return {"success": False, "errors": [str(e)]}


# ============================================================
# Duplicate Check Endpoint
# ============================================================

@app.post("/check/duplicate", dependencies=[Depends(verify_agent_key)])
async def check_duplicate_before_posting(req: DuplicateCheckRequest):
    """Check for duplicate transactions before posting."""
    importer = get_importer()
    try:
        result = importer.check_duplicate_before_posting(
            bank_account=req.bank_account,
            transaction_date=parse_date(req.transaction_date),
            amount_pounds=req.amount_pounds,
            account_code=req.account_code,
            account_type=req.account_type,
            date_tolerance_days=req.date_tolerance_days,
        )
        return result_to_dict(result)
    except Exception as e:
        logger.error(f"check_duplicate failed: {e}", exc_info=True)
        return {"success": False, "errors": [str(e)]}


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("OPERA3_AGENT_PORT", "9000"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info(f"Starting Opera 3 Write Agent on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
