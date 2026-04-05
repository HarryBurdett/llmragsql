"""
Opera 3 Agent Service

A FastAPI microservice that runs on the Opera 3 server alongside the FoxPro
data files. Single gateway for ALL Opera 3 access — reads and writes.

Writes: Proper CDX index maintenance via the Harbour DBFCDX bridge.
Reads: Generic table access via dbfread — any table, any filter.

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

from opera3_agent.write_ahead_log import WriteAheadLog, OperationStatus
from opera3_agent.transaction_safety import TransactionSafety

logger = logging.getLogger(__name__)

# ============================================================
# Configuration
# ============================================================

# Base path to Opera 3 installation (contains System/ folder and company data)
OPERA3_BASE_PATH = os.environ.get("OPERA3_BASE_PATH", os.environ.get("OPERA3_DATA_PATH", ""))
# Legacy: single company path (still supported for backward compat)
OPERA3_DATA_PATH = os.environ.get("OPERA3_DATA_PATH", "")
OPERA3_AGENT_KEY = os.environ.get("OPERA3_AGENT_KEY", "")
AGENT_VERSION = "1.2.0"
START_TIME = time.time()

# Company data paths discovered from seqco.dbf
# Key = company code (letter), Value = full path to data directory
_company_paths: Dict[str, str] = {}


def _discover_companies():
    """Discover all company datasets from seqco.dbf."""
    global _company_paths
    base = Path(OPERA3_BASE_PATH or OPERA3_DATA_PATH)
    if not base.exists():
        return

    system_path = base / "System"
    seqco_path = system_path / "seqco.dbf"
    if not seqco_path.exists():
        seqco_path = system_path / "SEQCO.DBF"
    if not seqco_path.exists():
        # No seqco — treat base path as single-company data path
        _company_paths["_default"] = str(base)
        logger.info(f"No seqco.dbf found — single company mode: {base}")
        return

    try:
        from dbfread import DBF
        dbf = DBF(str(seqco_path), encoding='cp1252', char_decode_errors='ignore')
        for record in dbf:
            code = ''
            name = ''
            subdir = ''
            for k, v in record.items():
                kl = k.lower()
                val = v.strip() if isinstance(v, str) else (v or '')
                if kl == 'co_code':
                    code = str(val).strip()
                elif kl == 'co_name':
                    name = str(val).strip()
                elif kl == 'co_subdir':
                    subdir = str(val).strip()

            if code and subdir:
                # Resolve subdir to actual path
                norm = subdir.replace("\\", "/").strip("/")
                # Try as relative to base
                candidate = base / norm
                if not candidate.exists():
                    # Try extracting relative portion after base path
                    base_str = str(base).replace("\\", "/").lower()
                    norm_lower = norm.lower()
                    if norm_lower.startswith(base_str.lstrip("/")):
                        relative = norm[len(base_str):].strip("/")
                        candidate = base / relative
                    else:
                        # Last resort: look for last path component
                        parts = norm.split("/")
                        candidate = base / parts[-1] if parts else base

                _company_paths[code] = str(candidate)
                logger.info(f"Company {code} ({name}): {candidate}")

        if not _company_paths:
            _company_paths["_default"] = str(base)
            logger.info(f"seqco.dbf empty — single company mode: {base}")

    except Exception as e:
        logger.warning(f"Could not read seqco.dbf: {e} — single company mode")
        _company_paths["_default"] = str(base)


def _resolve_data_path(company: Optional[str] = None) -> str:
    """Resolve the data path for a company code.

    Args:
        company: Company code letter (e.g. 'I', 'Z'). None = default/legacy.

    Returns:
        Full path to the company's data directory.

    Raises:
        HTTPException if company not found.
    """
    if not _company_paths:
        # Legacy mode — use OPERA3_DATA_PATH directly
        if OPERA3_DATA_PATH and os.path.isdir(OPERA3_DATA_PATH):
            return OPERA3_DATA_PATH
        raise HTTPException(status_code=503, detail="Opera 3 data path not configured")

    if company:
        company = company.strip().upper()
        if company in _company_paths:
            return _company_paths[company]
        raise HTTPException(status_code=404, detail=f"Company '{company}' not found. Available: {list(_company_paths.keys())}")

    # No company specified — use default or first
    if "_default" in _company_paths:
        return _company_paths["_default"]
    # Return first company as default
    return next(iter(_company_paths.values()))

# WAL database lives alongside the agent (NOT in Opera data)
WAL_DB_PATH = os.environ.get(
    "OPERA3_WAL_PATH",
    os.path.join(os.path.dirname(__file__), "opera3_wal.db"),
)

# Safety layer instances (initialised at startup)
wal: Optional[WriteAheadLog] = None
safety: Optional[TransactionSafety] = None


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

    # Check if writes are blocked due to failed compensation
    if safety and safety.writes_blocked:
        raise HTTPException(
            status_code=503,
            detail=(
                f"Writes are BLOCKED due to a data integrity issue: "
                f"{safety.block_reason}. "
                f"Manual intervention required before writes can resume."
            ),
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


def safe_import(operation_type: str, params: dict, import_fn) -> dict:
    """Execute an import operation wrapped with WAL + verification + compensation.

    This is the core safety wrapper. Every import endpoint calls this instead
    of directly invoking the importer.

    Flow:
    1. WAL: Record operation intent (PENDING)
    2. WAL: Mark IN_PROGRESS
    3. Execute the actual import
    4. If import reports failure → mark FAILED, return error
    5. WAL: Mark VERIFYING, run post-write verification
    6. If verification passes → mark COMPLETED, return success
    7. If verification fails → COMPENSATE, return error with details

    Args:
        operation_type: e.g. "purchase_payment"
        params: request parameters (for WAL and compensation)
        import_fn: callable that executes the actual import, returns result

    Returns:
        dict suitable for JSON response
    """
    # If WAL or safety not initialised, fall back to unprotected import
    if wal is None:
        logger.warning("WAL not initialised — executing unprotected import")
        result = import_fn()
        return result_to_dict(result)

    op_id = wal.begin_operation(operation_type, params)

    try:
        wal.mark_in_progress(op_id)

        # Execute the actual write operation
        result = import_fn()
        result_dict = result_to_dict(result)

        # If the import itself reported failure, no verification needed
        if not result_dict.get("success", False):
            wal.mark_failed(op_id, "; ".join(result_dict.get("errors", ["Unknown error"])))
            return result_dict

        # Post-write verification
        if safety is not None:
            wal.mark_verifying(op_id, result_dict)
            verification = safety.verify(operation_type, result_dict)
            wal.set_verification_details(op_id, verification.details)

            if verification.passed:
                wal.mark_completed(op_id, result_dict)
                return result_dict
            else:
                # VERIFICATION FAILED — this is serious
                logger.critical(
                    f"POST-WRITE VERIFICATION FAILED for {operation_type} "
                    f"[{op_id[:8]}]: {verification.details}"
                )
                wal.mark_failed(
                    op_id,
                    f"Verification failed: {verification.details}",
                )

                # Attempt compensation
                try:
                    wal.mark_compensating(op_id)
                    compensation = safety.compensate(
                        operation_type, params, result_dict
                    )
                    if compensation.success:
                        wal.mark_compensated(op_id, compensation.steps_completed)
                    else:
                        wal.mark_compensation_failed(
                            op_id, "; ".join(compensation.errors)
                        )
                except Exception as comp_err:
                    wal.mark_compensation_failed(op_id, str(comp_err))

                return {
                    "success": False,
                    "errors": [
                        f"Write verification failed: {verification.details}. "
                        "Changes have been rolled back where possible. "
                        "Check the agent WAL log for details."
                    ],
                    "wal_operation_id": op_id,
                }
        else:
            # No safety layer — mark as completed without verification
            wal.mark_completed(op_id, result_dict)
            return result_dict

    except HTTPException:
        # Let HTTP exceptions propagate (e.g. from get_importer)
        wal.mark_failed(op_id, "HTTP exception during import")
        raise
    except Exception as e:
        logger.error(
            f"Import {operation_type} [{op_id[:8]}] failed with exception: {e}",
            exc_info=True,
        )
        wal.mark_failed(op_id, str(e))
        return {"success": False, "errors": [str(e)], "wal_operation_id": op_id}


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
    """Startup and shutdown events with WAL initialisation and crash recovery."""
    global wal, safety

    logger.info(f"Opera 3 Agent v{AGENT_VERSION} starting")
    logger.info(f"Base path: {OPERA3_BASE_PATH or OPERA3_DATA_PATH}")
    logger.info(f"WAL path: {WAL_DB_PATH}")
    logger.info(f"Platform: {platform.system()} {platform.machine()}")

    # Discover company datasets
    _discover_companies()
    logger.info(f"Companies discovered: {list(_company_paths.keys())}")

    # Initialise Write-Ahead Log
    wal = WriteAheadLog(WAL_DB_PATH)
    logger.info("WAL initialised")

    # Initialise safety layer with first available data path
    first_path = next(iter(_company_paths.values()), OPERA3_DATA_PATH)
    if first_path and os.path.isdir(first_path):
        safety = TransactionSafety(first_path)
        logger.info("Transaction safety layer initialised")

        # Crash recovery — check for incomplete operations
        _run_crash_recovery(wal, safety)
    else:
        logger.warning(
            "No valid data path found — "
            "safety layer disabled until path is configured"
        )

    # Periodic WAL cleanup (remove old completed entries)
    wal.cleanup_old(days=90)

    yield

    logger.info("Opera 3 Write Agent shutting down")


def _run_crash_recovery(wal: WriteAheadLog, safety: TransactionSafety):
    """Check WAL for incomplete operations and handle them."""
    incomplete = wal.get_incomplete_operations()
    if not incomplete:
        logger.info("Crash recovery: no incomplete operations found")
        return

    logger.warning(
        f"Crash recovery: found {len(incomplete)} incomplete operation(s)"
    )

    for op in incomplete:
        logger.warning(
            f"  Recovering [{op.id[:8]}] {op.operation_type} "
            f"(status={op.status.value})"
        )
        try:
            recovery = safety.recover_operation(op)
            action = recovery.get("action", "unknown")

            if action == "marked_failed":
                wal.mark_failed(op.id, recovery.get("reason", "Crash recovery"))
            elif action == "marked_completed":
                wal.mark_completed(op.id, op.result)
            elif action == "compensated":
                wal.mark_compensated(
                    op.id,
                    recovery.get("compensation_steps", []),
                )
            elif action == "compensation_failed":
                wal.mark_compensation_failed(
                    op.id,
                    "; ".join(recovery.get("compensation_errors", ["Unknown"])),
                )
            else:
                wal.mark_failed(op.id, f"Recovery action: {action}")

            logger.warning(
                f"  [{op.id[:8]}] → {action}: {recovery.get('reason', '')}"
            )
        except Exception as e:
            logger.critical(
                f"  [{op.id[:8]}] Recovery failed: {e}", exc_info=True
            )
            wal.mark_compensation_failed(op.id, f"Recovery exception: {e}")


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

    # Include write-block status
    status = "ok"
    if safety and safety.writes_blocked:
        status = "blocked"

    return HealthResponse(
        status=status,
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
    post_date = parse_date(req.post_date)
    return safe_import(
        "purchase_payment",
        req.model_dump(),
        lambda: importer.import_purchase_payment(
            bank_account=req.bank_account,
            supplier_account=req.supplier_account,
            amount_pounds=req.amount_pounds,
            reference=req.reference,
            post_date=post_date,
            input_by=req.input_by,
            creditors_control=req.creditors_control,
            payment_type=req.payment_type,
            cbtype=req.cbtype,
            validate_only=req.validate_only,
        ),
    )


@app.post("/import/sales-receipt", dependencies=[Depends(verify_agent_key)])
async def import_sales_receipt(req: SalesReceiptRequest):
    """Import a sales receipt (money in from customer)."""
    importer = get_importer()
    post_date = parse_date(req.post_date)
    return safe_import(
        "sales_receipt",
        req.model_dump(),
        lambda: importer.import_sales_receipt(
            bank_account=req.bank_account,
            customer_account=req.customer_account,
            amount_pounds=req.amount_pounds,
            reference=req.reference,
            post_date=post_date,
            input_by=req.input_by,
            debtors_control=req.debtors_control,
            receipt_type=req.receipt_type,
            cbtype=req.cbtype,
            validate_only=req.validate_only,
        ),
    )


@app.post("/import/sales-refund", dependencies=[Depends(verify_agent_key)])
async def import_sales_refund(req: SalesRefundRequest):
    """Import a sales refund (money out to customer)."""
    importer = get_importer()
    post_date = parse_date(req.post_date)
    return safe_import(
        "sales_refund",
        req.model_dump(),
        lambda: importer.import_sales_refund(
            bank_account=req.bank_account,
            customer_account=req.customer_account,
            amount_pounds=req.amount_pounds,
            reference=req.reference,
            post_date=post_date,
            input_by=req.input_by,
            debtors_control=req.debtors_control,
            payment_method=req.payment_method,
            cbtype=req.cbtype,
            validate_only=req.validate_only,
            comment=req.comment,
        ),
    )


@app.post("/import/purchase-refund", dependencies=[Depends(verify_agent_key)])
async def import_purchase_refund(req: PurchaseRefundRequest):
    """Import a purchase refund (money in from supplier)."""
    importer = get_importer()
    post_date = parse_date(req.post_date)
    return safe_import(
        "purchase_refund",
        req.model_dump(),
        lambda: importer.import_purchase_refund(
            bank_account=req.bank_account,
            supplier_account=req.supplier_account,
            amount_pounds=req.amount_pounds,
            reference=req.reference,
            post_date=post_date,
            input_by=req.input_by,
            creditors_control=req.creditors_control,
            payment_type=req.payment_type,
            cbtype=req.cbtype,
            validate_only=req.validate_only,
            comment=req.comment,
        ),
    )


@app.post("/import/bank-transfer", dependencies=[Depends(verify_agent_key)])
async def import_bank_transfer(req: BankTransferRequest):
    """Import a bank transfer between two bank accounts."""
    importer = get_importer()
    post_date = parse_date(req.post_date)
    return safe_import(
        "bank_transfer",
        req.model_dump(),
        lambda: importer.import_bank_transfer(
            source_bank=req.source_bank,
            dest_bank=req.dest_bank,
            amount_pounds=req.amount_pounds,
            reference=req.reference,
            post_date=post_date,
            comment=req.comment,
            input_by=req.input_by,
            post_to_nominal=req.post_to_nominal,
            cbtype=req.cbtype,
        ),
    )


@app.post("/import/nominal-entry", dependencies=[Depends(verify_agent_key)])
async def import_nominal_entry(req: NominalEntryRequest):
    """Import a nominal entry (direct to nominal account)."""
    importer = get_importer()
    post_date = parse_date(req.post_date)
    return safe_import(
        "nominal_entry",
        req.model_dump(),
        lambda: importer.import_nominal_entry(
            bank_account=req.bank_account,
            nominal_account=req.nominal_account,
            amount_pounds=req.amount_pounds,
            reference=req.reference,
            post_date=post_date,
            description=req.description,
            input_by=req.input_by,
            is_receipt=req.is_receipt,
            cbtype=req.cbtype,
            validate_only=req.validate_only,
            project_code=req.project_code,
            department_code=req.department_code,
            vat_code=req.vat_code,
        ),
    )


@app.post("/import/gocardless-batch", dependencies=[Depends(verify_agent_key)])
async def import_gocardless_batch(req: GoCardlessBatchRequest):
    """Import a GoCardless batch of customer payments."""
    importer = get_importer()
    post_date = parse_date(req.post_date)
    return safe_import(
        "gocardless_batch",
        req.model_dump(),
        lambda: importer.import_gocardless_batch(
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
        ),
    )


@app.post("/import/recurring-entry", dependencies=[Depends(verify_agent_key)])
async def post_recurring_entry(req: RecurringEntryRequest):
    """Post a recurring entry from arhead/arline."""
    importer = get_importer()
    return safe_import(
        "recurring_entry",
        req.model_dump(),
        lambda: importer.post_recurring_entry(
            bank_account=req.bank_account,
            entry_ref=req.entry_ref,
            override_date=parse_date(req.override_date),
            input_by=req.input_by,
        ),
    )


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
# WAL Monitoring & Audit Endpoints
# ============================================================

@app.get("/wal/stats", dependencies=[Depends(verify_agent_key)])
async def wal_stats():
    """Get WAL statistics for monitoring."""
    if wal is None:
        return {"error": "WAL not initialised"}
    stats = wal.get_stats()
    stats["writes_blocked"] = safety.writes_blocked if safety else False
    stats["block_reason"] = safety.block_reason if safety else ""
    return stats


@app.get("/wal/recent", dependencies=[Depends(verify_agent_key)])
async def wal_recent(limit: int = 20):
    """Get recent WAL operations for audit."""
    if wal is None:
        return {"error": "WAL not initialised", "operations": []}
    ops = wal.get_recent_operations(limit=limit)
    return {
        "operations": [
            {
                "id": op.id,
                "type": op.operation_type,
                "status": op.status.value,
                "started_at": op.started_at,
                "completed_at": op.completed_at,
                "error": op.error_message,
                "verification": op.verification_details,
                "has_result": op.result is not None,
                "has_compensation": op.compensation_log is not None,
            }
            for op in ops
        ]
    }


@app.get("/wal/operation/{op_id}", dependencies=[Depends(verify_agent_key)])
async def wal_operation_detail(op_id: str):
    """Get full details of a specific WAL operation."""
    if wal is None:
        raise HTTPException(status_code=503, detail="WAL not initialised")
    op = wal.get_operation(op_id)
    if not op:
        raise HTTPException(status_code=404, detail="Operation not found")
    return {
        "id": op.id,
        "type": op.operation_type,
        "status": op.status.value,
        "params": op.params,
        "result": op.result,
        "snapshot": op.snapshot,
        "compensation_log": op.compensation_log,
        "verification": op.verification_details,
        "error": op.error_message,
        "started_at": op.started_at,
        "completed_at": op.completed_at,
    }


@app.post("/wal/unblock", dependencies=[Depends(verify_agent_key)])
async def wal_unblock():
    """Manually unblock writes after resolving a compensation failure.

    This should only be called after the data integrity issue has been
    manually resolved (e.g. using Opera Data Repair).
    """
    if safety is None:
        return {"success": False, "error": "Safety layer not initialised"}
    if not safety.writes_blocked:
        return {"success": True, "message": "Writes were not blocked"}
    reason = safety.block_reason
    safety.writes_blocked = False
    safety.block_reason = ""
    logger.warning(f"Writes UNBLOCKED manually (was: {reason})")
    return {"success": True, "message": f"Writes unblocked (was: {reason})"}


# ============================================================
# Generic Table Read
# ============================================================

class ReadRequest(BaseModel):
    """Generic table read request."""
    table: str = Field(..., description="Table name without extension (e.g. 'pname', 'ptran')")
    company: Optional[str] = Field(None, description="Company code (e.g. 'I', 'Z'). None = default.")
    fields: Optional[List[str]] = Field(None, description="Field names to return. None = all fields.")
    filter: Optional[Dict[str, Any]] = Field(None, description="Field=value filter pairs (exact match, AND logic)")
    filter_expr: Optional[str] = Field(None, description="Python expression filter (e.g. 'pt_trbal != 0'). Fields referenced by name.")
    limit: int = Field(10000, description="Maximum rows to return")
    order_by: Optional[str] = Field(None, description="Field name to sort by")


@app.post("/read", dependencies=[Depends(verify_agent_key)])
async def read_table(req: ReadRequest):
    """
    Generic read from any Opera 3 DBF table.

    Supports:
    - Field selection (return only specific columns)
    - Exact-match filtering (field=value pairs, AND logic)
    - Expression filtering (Python expression evaluated per row)
    - Row limit
    - Sort by field

    All reads are shared (no locking) — safe for concurrent access.
    """
    data_dir = _resolve_data_path(req.company)

    table_name = req.table.strip().lower()
    if not table_name.isalnum():
        raise HTTPException(status_code=400, detail="Invalid table name")

    # Find the DBF file (case-insensitive)
    data_path = Path(data_dir)
    dbf_path = data_path / f"{table_name}.dbf"
    if not dbf_path.exists():
        dbf_path = data_path / f"{table_name.upper()}.DBF"
    if not dbf_path.exists():
        # Try with mixed case
        for f in data_path.iterdir():
            if f.name.lower() == f"{table_name}.dbf":
                dbf_path = f
                break
    if not dbf_path.exists():
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    try:
        from dbfread import DBF

        table = DBF(str(dbf_path), encoding='latin-1', char_decode_errors='ignore')

        rows = []
        count = 0

        for record in table:
            if record.get('_deletion_flag'):
                continue  # Skip deleted records

            # Apply exact-match filter
            if req.filter:
                match = True
                for fld, val in req.filter.items():
                    rec_val = record.get(fld) or record.get(fld.lower()) or record.get(fld.upper())
                    if rec_val is None:
                        # Try case-insensitive field lookup
                        for k, v in record.items():
                            if k.lower() == fld.lower():
                                rec_val = v
                                break
                    # Compare: strip strings, handle types
                    if isinstance(rec_val, str):
                        rec_val = rec_val.strip()
                    if isinstance(val, str):
                        val = val.strip()
                    if rec_val != val:
                        match = False
                        break
                if not match:
                    continue

            # Apply expression filter (evaluated safely)
            if req.filter_expr:
                try:
                    # Build a clean dict with lowercase keys for the expression
                    expr_vars = {}
                    for k, v in record.items():
                        if k.startswith('_'):
                            continue
                        clean_val = v.strip() if isinstance(v, str) else v
                        expr_vars[k.lower()] = clean_val
                        expr_vars[k] = clean_val  # Also original case
                    if not eval(req.filter_expr, {"__builtins__": {}}, expr_vars):
                        continue
                except Exception:
                    continue  # Skip rows that fail the expression

            # Build output row
            row = {}
            for k, v in record.items():
                if k.startswith('_'):
                    continue
                if req.fields and k.lower() not in [f.lower() for f in req.fields]:
                    continue
                # Serialise values
                if isinstance(v, (date, datetime)):
                    row[k.lower()] = v.isoformat() if v else None
                elif isinstance(v, str):
                    row[k.lower()] = v.strip()
                elif isinstance(v, bytes):
                    row[k.lower()] = v.decode('latin-1', errors='ignore').strip()
                else:
                    row[k.lower()] = v

            rows.append(row)
            count += 1
            if count >= req.limit:
                break

        # Sort if requested
        if req.order_by:
            sort_key = req.order_by.lower()
            rows.sort(key=lambda r: r.get(sort_key) or '', reverse=False)

        return {
            "success": True,
            "table": table_name,
            "count": len(rows),
            "rows": rows,
        }

    except ImportError:
        raise HTTPException(status_code=503, detail="dbfread not installed — run: pip install dbfread")
    except Exception as e:
        logger.error(f"Error reading table {table_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/read/count", dependencies=[Depends(verify_agent_key)])
async def count_table(req: ReadRequest):
    """Count rows matching filter without returning data."""
    data_dir = _resolve_data_path(req.company)
    table_name = req.table.strip().lower()
    data_path = Path(data_dir)
    dbf_path = data_path / f"{table_name}.dbf"
    if not dbf_path.exists():
        dbf_path = data_path / f"{table_name.upper()}.DBF"
    if not dbf_path.exists():
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    try:
        from dbfread import DBF
        table = DBF(str(dbf_path), encoding='latin-1', char_decode_errors='ignore')
        count = 0
        for record in table:
            if record.get('_deletion_flag'):
                continue
            if req.filter:
                match = True
                for fld, val in req.filter.items():
                    rec_val = None
                    for k, v in record.items():
                        if k.lower() == fld.lower():
                            rec_val = v.strip() if isinstance(v, str) else v
                            break
                    if isinstance(val, str):
                        val = val.strip()
                    if rec_val != val:
                        match = False
                        break
                if not match:
                    continue
            count += 1
        return {"success": True, "table": table_name, "count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/companies", dependencies=[Depends(verify_agent_key)])
async def list_companies():
    """List all discovered company datasets."""
    companies = []
    for code, path in _company_paths.items():
        exists = os.path.isdir(path)
        companies.append({
            "code": code,
            "path": path,
            "accessible": exists,
        })
    return {"success": True, "companies": companies, "count": len(companies)}


@app.get("/tables", dependencies=[Depends(verify_agent_key)])
async def list_tables(company: Optional[str] = None):
    """List all available DBF tables for a company."""
    data_dir = _resolve_data_path(company)
    data_path = Path(data_dir)
    tables = []
    for f in sorted(data_path.iterdir()):
        if f.suffix.lower() == '.dbf' and not f.name.startswith('.'):
            tables.append({
                "name": f.stem.lower(),
                "size_bytes": f.stat().st_size,
            })
    return {"success": True, "tables": tables, "count": len(tables)}


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
