"""
Supplier Remittance Advice API routes.

Provides endpoints for generating and sending remittance advices based on
Opera purchase ledger payment data (ptran + palloc).

Works with both Opera SQL SE and Opera 3 (FoxPro) backends.
Remittance history is stored in the supplier_remittance_log SQLite table
via SupplierStatementDB.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================
# Request/Response Models
# ============================================================

class SendRemittanceRequest(BaseModel):
    payment_ref: str = Field(..., description="Payment reference (pt_trref)")
    remittance_text: Optional[str] = Field(
        default=None,
        description="Optional override for the remittance text body"
    )
    send_to: Optional[str] = Field(
        default=None,
        description="Optional override for recipient email address"
    )


# ============================================================
# Helpers
# ============================================================

def _format_payment_method(pt_trref: str) -> str:
    """
    Infer payment method from payment reference patterns.

    Opera does not store an explicit payment method field on ptran.
    We infer from the reference or default to 'BACS'.
    """
    ref_upper = (pt_trref or "").strip().upper()
    if "CHQ" in ref_upper or "CHEQUE" in ref_upper:
        return "Cheque"
    if "CHAPS" in ref_upper:
        return "CHAPS"
    if "DD" in ref_upper:
        return "Direct Debit"
    if "FP" in ref_upper or "FASTER" in ref_upper:
        return "Faster Payment"
    return "BACS"


def _format_date(d) -> str:
    """Format a date value to DD/MM/YYYY string."""
    if d is None:
        return ""
    if isinstance(d, str):
        # Try common SQL date formats
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"):
            try:
                return datetime.strptime(d.strip()[:10], fmt).strftime("%d/%m/%Y")
            except (ValueError, IndexError):
                continue
        return d.strip()[:10]
    if hasattr(d, "strftime"):
        return d.strftime("%d/%m/%Y")
    return str(d)


def _safe_float(val, default=0.0) -> float:
    """Safely convert a value to float."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _build_remittance_text(
    supplier_name: str,
    payment_date: str,
    payment_method: str,
    payment_ref: str,
    total_amount: float,
    invoices: list,
) -> str:
    """
    Build the remittance advice text from payment and allocation data.

    Returns a plain-text remittance advice document.
    """
    lines = []
    lines.append(f"REMITTANCE ADVICE")
    lines.append(f"")
    lines.append(f"To: {supplier_name}")
    lines.append(f"")
    lines.append(f"Payment Date: {payment_date}")
    lines.append(f"Payment Method: {payment_method}")
    lines.append(f"Payment Reference: {payment_ref}")
    lines.append(f"Total Amount: \u00a3{total_amount:,.2f}")
    lines.append(f"")
    lines.append(f"INVOICES PAID")

    if invoices:
        # Calculate column widths for alignment
        max_ref_len = max(len(str(inv.get("ref", ""))) for inv in invoices)
        max_ref_len = max(max_ref_len, 7)  # minimum "Invoice" width

        for inv in invoices:
            inv_ref = str(inv.get("ref", ""))
            inv_date = inv.get("date", "")
            inv_amount = _safe_float(inv.get("amount", 0))
            lines.append(
                f"Invoice {inv_ref:<{max_ref_len}} dated {inv_date}    \u00a3{inv_amount:,.2f}"
            )
    else:
        lines.append("(No allocation detail available)")

    lines.append(f"")
    lines.append(f"Total:                        \u00a3{total_amount:,.2f}")

    return "\n".join(lines)


def _get_contact_email(supplier_code: str) -> Optional[str]:
    """
    Get the best contact email for sending a remittance to a supplier.

    Priority:
    1. supplier_contacts_ext with is_payment_contact = 1
    2. pname.pn_email from Opera
    """
    from sql_rag.supplier_statement_db import get_supplier_statement_db

    # Check local contacts for a payment contact
    db = get_supplier_statement_db()
    contacts = db.get_contacts(supplier_code)
    for contact in contacts:
        if contact.get("is_payment_contact") and contact.get("email"):
            return contact["email"].strip()

    # Fallback: check Opera pname email
    from api.main import sql_connector
    if sql_connector:
        try:
            df = sql_connector.execute_query(f"""
                SELECT RTRIM(ISNULL(pn_email, '')) AS email
                FROM pname WITH (NOLOCK)
                WHERE RTRIM(pn_account) = '{supplier_code}'
            """)
            if df is not None and len(df) > 0:
                email = str(df.iloc[0]["email"]).strip()
                if email and "@" in email:
                    return email
        except Exception as e:
            logger.warning(f"Could not fetch pname email for {supplier_code}: {e}")

    return None


def _check_remittance_already_sent(supplier_code: str, payment_ref: str) -> bool:
    """Check if a remittance has already been sent for this payment."""
    from sql_rag.supplier_statement_db import get_supplier_statement_db

    db = get_supplier_statement_db()
    history = db.get_remittance_history(supplier_code=supplier_code, limit=100)
    for entry in history:
        if entry.get("payment_ref", "").strip() == payment_ref.strip():
            return True
    return False


# ============================================================
# Opera SQL SE Endpoints
# ============================================================

@router.get("/api/supplier-remittance/recent-payments")
async def get_recent_payments(
    days_back: int = Query(default=30, ge=1, le=365, description="Number of days to look back"),
    supplier_code: Optional[str] = Query(default=None, description="Filter by supplier account code"),
):
    """
    Get recent purchase ledger payments with allocation detail.

    Queries Opera ptran for payments (pt_trtype = 'P') within the specified
    date range, then joins palloc to get invoice allocation detail grouped
    by payment.
    """
    from api.main import sql_connector
    from sql_rag.supplier_statement_db import get_supplier_statement_db

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not available")

    try:
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        # Build WHERE clause
        where_clauses = [
            "pt_trtype = 'P'",
            f"pt_trdate >= '{cutoff_date}'",
        ]
        if supplier_code:
            where_clauses.append(f"RTRIM(pt_account) = '{supplier_code}'")

        where_sql = " AND ".join(where_clauses)

        # Get payments from ptran
        payments_df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(pt_account) AS supplier_code,
                RTRIM(pn_name) AS supplier_name,
                pt_trdate AS payment_date,
                RTRIM(pt_trref) AS payment_ref,
                pt_trvalue AS payment_amount,
                pt_payflag
            FROM ptran WITH (NOLOCK)
            INNER JOIN pname WITH (NOLOCK) ON RTRIM(pn_account) = RTRIM(pt_account)
            WHERE {where_sql}
            ORDER BY pt_trdate DESC, pt_account
        """)

        if payments_df is None or len(payments_df) == 0:
            return {"success": True, "payments": []}

        # Get remittance history for sent status
        db = get_supplier_statement_db()
        all_history = db.get_remittance_history(
            supplier_code=supplier_code, limit=500
        )
        sent_refs = set()
        for entry in all_history:
            key = f"{entry.get('supplier_code', '').strip()}|{entry.get('payment_ref', '').strip()}"
            sent_refs.add(key)

        # Group payments and get allocation detail
        payments = []
        for _, row in payments_df.iterrows():
            acct = str(row["supplier_code"]).strip()
            name = str(row["supplier_name"]).strip()
            pay_date = row["payment_date"]
            pay_ref = str(row["payment_ref"]).strip()
            pay_amount = abs(_safe_float(row["payment_amount"]))
            payflag = int(row["pt_payflag"] or 0)

            payment_method = _format_payment_method(pay_ref)

            # Get allocated invoices from palloc
            invoices = []
            if payflag > 0:
                alloc_df = sql_connector.execute_query(f"""
                    SELECT
                        RTRIM(pl_ref1) AS ref,
                        pl_date AS alloc_date,
                        pl_val AS alloc_amount,
                        pl_type
                    FROM palloc WITH (NOLOCK)
                    WHERE RTRIM(pl_account) = '{acct}'
                      AND pl_payflag = {payflag}
                      AND pl_type = 'I'
                    ORDER BY pl_date
                """)

                if alloc_df is not None and len(alloc_df) > 0:
                    for _, alloc_row in alloc_df.iterrows():
                        inv_ref = str(alloc_row["ref"]).strip()
                        inv_date = _format_date(alloc_row["alloc_date"])
                        inv_amount = abs(_safe_float(alloc_row["alloc_amount"]))

                        # Get the original invoice date from ptran
                        inv_detail_df = sql_connector.execute_query(f"""
                            SELECT pt_trdate
                            FROM ptran WITH (NOLOCK)
                            WHERE RTRIM(pt_account) = '{acct}'
                              AND RTRIM(pt_trref) = '{inv_ref}'
                              AND pt_trtype = 'I'
                        """)
                        if inv_detail_df is not None and len(inv_detail_df) > 0:
                            inv_date = _format_date(inv_detail_df.iloc[0]["pt_trdate"])

                        invoices.append({
                            "ref": inv_ref,
                            "date": inv_date,
                            "amount": inv_amount,
                        })

            # Check if remittance already sent
            sent_key = f"{acct}|{pay_ref}"
            remittance_sent = sent_key in sent_refs

            payments.append({
                "supplier_code": acct,
                "supplier_name": name,
                "payment_date": _format_date(pay_date),
                "payment_ref": pay_ref,
                "payment_method": payment_method,
                "total_amount": pay_amount,
                "invoices": invoices,
                "remittance_sent": remittance_sent,
            })

        return {"success": True, "payments": payments}

    except Exception as e:
        logger.error(f"Error fetching recent payments: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-remittance/{account}/generate")
async def generate_remittance(
    account: str,
    payment_ref: str = Query(..., description="Payment reference (pt_trref)"),
):
    """
    Generate remittance advice text for a specific payment.

    Looks up the payment and its allocations in ptran/palloc and builds
    a formatted remittance advice document.
    """
    from api.main import sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not available")

    try:
        # Get payment details
        payment_df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(pt_account) AS supplier_code,
                RTRIM(pn_name) AS supplier_name,
                pt_trdate AS payment_date,
                RTRIM(pt_trref) AS payment_ref,
                pt_trvalue AS payment_amount,
                pt_payflag
            FROM ptran WITH (NOLOCK)
            INNER JOIN pname WITH (NOLOCK) ON RTRIM(pn_account) = RTRIM(pt_account)
            WHERE RTRIM(pt_account) = '{account}'
              AND RTRIM(pt_trref) = '{payment_ref}'
              AND pt_trtype = 'P'
        """)

        if payment_df is None or len(payment_df) == 0:
            raise HTTPException(
                status_code=404,
                detail=f"Payment '{payment_ref}' not found for supplier '{account}'"
            )

        row = payment_df.iloc[0]
        supplier_name = str(row["supplier_name"]).strip()
        pay_date_raw = row["payment_date"]
        pay_date = _format_date(pay_date_raw)
        pay_ref = str(row["payment_ref"]).strip()
        total_amount = abs(_safe_float(row["payment_amount"]))
        payflag = int(row["pt_payflag"] or 0)
        payment_method = _format_payment_method(pay_ref)

        # Get allocated invoices
        invoices = []
        if payflag > 0:
            alloc_df = sql_connector.execute_query(f"""
                SELECT
                    RTRIM(pl_ref1) AS ref,
                    pl_date AS alloc_date,
                    pl_val AS alloc_amount,
                    pl_type
                FROM palloc WITH (NOLOCK)
                WHERE RTRIM(pl_account) = '{account}'
                  AND pl_payflag = {payflag}
                  AND pl_type = 'I'
                ORDER BY pl_date
            """)

            if alloc_df is not None and len(alloc_df) > 0:
                for _, alloc_row in alloc_df.iterrows():
                    inv_ref = str(alloc_row["ref"]).strip()
                    inv_amount = abs(_safe_float(alloc_row["alloc_amount"]))

                    # Get original invoice date from ptran
                    inv_detail_df = sql_connector.execute_query(f"""
                        SELECT pt_trdate
                        FROM ptran WITH (NOLOCK)
                        WHERE RTRIM(pt_account) = '{account}'
                          AND RTRIM(pt_trref) = '{inv_ref}'
                          AND pt_trtype = 'I'
                    """)
                    inv_date = ""
                    if inv_detail_df is not None and len(inv_detail_df) > 0:
                        inv_date = _format_date(inv_detail_df.iloc[0]["pt_trdate"])

                    invoices.append({
                        "ref": inv_ref,
                        "date": inv_date,
                        "amount": inv_amount,
                    })

        # Build remittance text
        text = _build_remittance_text(
            supplier_name=supplier_name,
            payment_date=pay_date,
            payment_method=payment_method,
            payment_ref=pay_ref,
            total_amount=total_amount,
            invoices=invoices,
        )

        return {
            "success": True,
            "remittance": {
                "supplier_code": account,
                "supplier_name": supplier_name,
                "payment_date": pay_date,
                "total": total_amount,
                "invoices": invoices,
                "text": text,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating remittance for {account}/{payment_ref}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-remittance/{account}/send")
async def send_remittance(account: str, body: SendRemittanceRequest):
    """
    Send a remittance advice to the supplier via email.

    Resolves the contact email from:
    1. body.send_to override
    2. supplier_contacts_ext (is_payment_contact)
    3. pname.pn_email

    Records the sent remittance in supplier_remittance_log and
    supplier_communications audit trail.
    """
    from api.main import sql_connector, email_storage
    from sql_rag.supplier_statement_db import get_supplier_statement_db

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not available")

    try:
        payment_ref = body.payment_ref.strip()

        # Resolve recipient email
        send_to = body.send_to.strip() if body.send_to else None
        if not send_to:
            send_to = _get_contact_email(account)
        if not send_to:
            raise HTTPException(
                status_code=400,
                detail=f"No email address found for supplier '{account}'. "
                       "Set a payment contact or provide a send_to address."
            )

        # Get payment details for the remittance
        payment_df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(pt_account) AS supplier_code,
                RTRIM(pn_name) AS supplier_name,
                pt_trdate AS payment_date,
                RTRIM(pt_trref) AS payment_ref,
                pt_trvalue AS payment_amount,
                pt_payflag
            FROM ptran WITH (NOLOCK)
            INNER JOIN pname WITH (NOLOCK) ON RTRIM(pn_account) = RTRIM(pt_account)
            WHERE RTRIM(pt_account) = '{account}'
              AND RTRIM(pt_trref) = '{payment_ref}'
              AND pt_trtype = 'P'
        """)

        if payment_df is None or len(payment_df) == 0:
            raise HTTPException(
                status_code=404,
                detail=f"Payment '{payment_ref}' not found for supplier '{account}'"
            )

        row = payment_df.iloc[0]
        supplier_name = str(row["supplier_name"]).strip()
        pay_date = _format_date(row["payment_date"])
        total_amount = abs(_safe_float(row["payment_amount"]))
        payflag = int(row["pt_payflag"] or 0)
        payment_method = _format_payment_method(payment_ref)

        # Get allocated invoices for the log
        invoices = []
        if payflag > 0:
            alloc_df = sql_connector.execute_query(f"""
                SELECT
                    RTRIM(pl_ref1) AS ref,
                    pl_date AS alloc_date,
                    pl_val AS alloc_amount
                FROM palloc WITH (NOLOCK)
                WHERE RTRIM(pl_account) = '{account}'
                  AND pl_payflag = {payflag}
                  AND pl_type = 'I'
                ORDER BY pl_date
            """)

            if alloc_df is not None and len(alloc_df) > 0:
                for _, alloc_row in alloc_df.iterrows():
                    inv_ref = str(alloc_row["ref"]).strip()
                    inv_amount = abs(_safe_float(alloc_row["alloc_amount"]))

                    inv_detail_df = sql_connector.execute_query(f"""
                        SELECT pt_trdate
                        FROM ptran WITH (NOLOCK)
                        WHERE RTRIM(pt_account) = '{account}'
                          AND RTRIM(pt_trref) = '{inv_ref}'
                          AND pt_trtype = 'I'
                    """)
                    inv_date = ""
                    if inv_detail_df is not None and len(inv_detail_df) > 0:
                        inv_date = _format_date(inv_detail_df.iloc[0]["pt_trdate"])

                    invoices.append({
                        "ref": inv_ref,
                        "date": inv_date,
                        "amount": inv_amount,
                    })

        # Build or use override remittance text
        if body.remittance_text:
            remittance_text = body.remittance_text
        else:
            remittance_text = _build_remittance_text(
                supplier_name=supplier_name,
                payment_date=pay_date,
                payment_method=payment_method,
                payment_ref=payment_ref,
                total_amount=total_amount,
                invoices=invoices,
            )

        # Send email via the /api/email/send infrastructure
        if not email_storage:
            raise HTTPException(status_code=503, detail="Email storage not initialized - cannot send email")

        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        providers = email_storage.get_all_providers()
        enabled_provider = next((p for p in providers if p.get("enabled")), None)
        if not enabled_provider:
            raise HTTPException(
                status_code=400,
                detail="No enabled email provider found. Configure an email provider first."
            )

        provider_type = enabled_provider.get("provider_type")
        config_json = enabled_provider.get("config_json", "{}")
        provider_config = json.loads(config_json) if config_json else {}

        # Determine SMTP settings
        import re as _re
        if provider_type == "gmail":
            smtp_server = "smtp.gmail.com"
            smtp_port = 587
            username = provider_config.get("email", "")
            password = provider_config.get("app_password", "") or provider_config.get("password", "")
        elif provider_type == "microsoft":
            smtp_server = "smtp.office365.com"
            smtp_port = 587
            username = provider_config.get("email", "")
            password = provider_config.get("password", "")
        elif provider_type == "imap":
            imap_server = provider_config.get("server", "")
            if _re.match(r"^\d+\.\d+\.\d+\.\d+$", imap_server):
                smtp_server = imap_server
                smtp_port = 587
            else:
                smtp_server = imap_server.replace("imap.", "smtp.").replace("imaps.", "smtp.")
                smtp_port = 587
            username = provider_config.get("username", "")
            password = provider_config.get("password", "")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported provider type: {provider_type}")

        if not username or not password:
            raise HTTPException(
                status_code=400,
                detail="Email provider credentials not configured properly"
            )

        # Determine From address
        from_email = provider_config.get("from_email") or provider_config.get("email")
        if not from_email:
            if "\\" in username:
                domain, user = username.split("\\", 1)
                from_email = f"{user}@{domain}.local"
            elif "@" in username:
                from_email = username
            else:
                from_email = username

        # Build HTML email body
        html_body = f"""<html>
<body style="font-family: Arial, sans-serif; color: #333;">
<h2>Remittance Advice</h2>
<pre style="font-family: Consolas, monospace; font-size: 14px; line-height: 1.5;">
{remittance_text}
</pre>
<hr style="border: 1px solid #ccc;">
<p style="font-size: 12px; color: #666;">
This remittance advice was generated automatically. If you have any queries,
please contact our accounts department.
</p>
</body>
</html>"""

        email_subject = f"Remittance Advice - {payment_ref} - {pay_date}"

        msg = MIMEMultipart()
        msg["From"] = from_email
        msg["To"] = send_to
        msg["Subject"] = email_subject
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            if smtp_port == 587:
                server.starttls()
                server.login(username, password)
            else:
                try:
                    server.login(username, password)
                except smtplib.SMTPException:
                    pass
            server.send_message(msg)

        sent_at = datetime.now().isoformat()

        logger.info(
            f"Remittance advice sent to {send_to} for {account} payment {payment_ref}"
        )

        # Record in supplier_remittance_log
        db = get_supplier_statement_db()
        db.log_remittance(
            supplier_code=account,
            payment_date=pay_date,
            payment_ref=payment_ref,
            payment_method=payment_method,
            total_amount=total_amount,
            invoice_count=len(invoices),
            invoices_json=json.dumps(invoices),
            sent_to=send_to,
            sent_by="system",
        )

        # Record in supplier_communications audit trail
        db.log_communication(
            supplier_code=account,
            direction="outbound",
            comm_type="remittance",
            email_subject=email_subject,
            email_body=remittance_text,
            sent_by="system",
        )

        return {
            "success": True,
            "sent_to": send_to,
            "sent_at": sent_at,
        }

    except HTTPException:
        raise
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"SMTP authentication failed sending remittance: {e}")
        return {
            "success": False,
            "error": "Email authentication failed. Check your credentials or app password.",
        }
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error sending remittance: {e}")
        return {"success": False, "error": f"Failed to send email: {str(e)}"}
    except Exception as e:
        logger.error(f"Error sending remittance for {account}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.get("/api/supplier-remittance/history")
async def get_remittance_history(
    supplier_code: Optional[str] = Query(default=None, description="Filter by supplier account code"),
    limit: int = Query(default=50, ge=1, le=500, description="Maximum results to return"),
):
    """
    Get remittance advice history from the local log.

    Returns previously sent remittance advices with timestamps, recipients,
    and invoice detail.
    """
    from sql_rag.supplier_statement_db import get_supplier_statement_db

    try:
        db = get_supplier_statement_db()
        history = db.get_remittance_history(
            supplier_code=supplier_code, limit=limit
        )

        # Parse invoices_json back to list for each entry
        for entry in history:
            invoices_raw = entry.get("invoices_json")
            if invoices_raw:
                try:
                    entry["invoices"] = json.loads(invoices_raw)
                except (json.JSONDecodeError, TypeError):
                    entry["invoices"] = []
            else:
                entry["invoices"] = []

        return {"success": True, "history": history}

    except Exception as e:
        logger.error(f"Error fetching remittance history: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# Opera 3 (FoxPro) Endpoints
# ============================================================

def _o3_get_str(record, field, default=""):
    """Get string from Opera 3 record (handles uppercase/lowercase field names)."""
    val = record.get(field.upper(), record.get(field.lower(), record.get(field, default)))
    if val is None:
        return default
    return str(val).strip()


def _o3_get_num(record, field, default=0.0):
    """Get numeric from Opera 3 record (handles uppercase/lowercase field names)."""
    val = record.get(field.upper(), record.get(field.lower(), record.get(field, default)))
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _o3_get_int(record, field, default=0):
    """Get integer from Opera 3 record (handles uppercase/lowercase field names)."""
    val = record.get(field.upper(), record.get(field.lower(), record.get(field, default)))
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _o3_get_date(record, field) -> Optional[str]:
    """Get date from Opera 3 record, formatted as DD/MM/YYYY."""
    val = record.get(field.upper(), record.get(field.lower(), record.get(field)))
    if val is None:
        return ""
    return _format_date(val)


@router.get("/api/opera3/supplier-remittance/recent-payments")
async def opera3_get_recent_payments(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    days_back: int = Query(default=30, ge=1, le=365, description="Number of days to look back"),
    supplier_code: Optional[str] = Query(default=None, description="Filter by supplier account code"),
):
    """
    Get recent purchase ledger payments from Opera 3 (FoxPro) with allocation detail.

    Reads ptran and palloc from Opera 3 DBF files and groups payments with
    their allocated invoices.
    """
    from sql_rag.opera3_foxpro import Opera3Reader
    from sql_rag.supplier_statement_db import get_supplier_statement_db

    try:
        reader = Opera3Reader(data_path)
        cutoff_date = datetime.now() - timedelta(days=days_back)

        # Build supplier name lookup
        pname_records = reader.read_table("pname")
        supplier_names = {}
        for rec in pname_records:
            acct = _o3_get_str(rec, "pn_account")
            name = _o3_get_str(rec, "pn_name")
            if acct:
                supplier_names[acct.upper()] = name

        # Read ptran for payments
        ptran_records = reader.read_table("ptran")
        payment_records = []

        for rec in ptran_records:
            trtype = _o3_get_str(rec, "pt_trtype")
            if trtype != "P":
                continue

            acct = _o3_get_str(rec, "pt_account")
            if supplier_code and acct.upper() != supplier_code.upper():
                continue

            # Parse date and check cutoff
            date_val = rec.get("PT_TRDATE", rec.get("pt_trdate", rec.get("Pt_trdate")))
            if date_val is None:
                continue

            if hasattr(date_val, "date"):
                tr_date = date_val if hasattr(date_val, "strftime") else None
            elif isinstance(date_val, str):
                tr_date = None
                for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
                    try:
                        tr_date = datetime.strptime(date_val.strip()[:10], fmt)
                        break
                    except ValueError:
                        continue
            else:
                tr_date = None

            if tr_date is None:
                continue

            # Handle both datetime and date objects for comparison
            if hasattr(tr_date, "hour"):
                if tr_date < cutoff_date:
                    continue
            else:
                if hasattr(cutoff_date, "date"):
                    if tr_date < cutoff_date.date():
                        continue
                else:
                    if tr_date < cutoff_date:
                        continue

            payment_records.append({
                "account": acct,
                "date": tr_date,
                "ref": _o3_get_str(rec, "pt_trref"),
                "amount": abs(_o3_get_num(rec, "pt_trvalue")),
                "payflag": _o3_get_int(rec, "pt_payflag"),
            })

        # Read palloc for allocation detail
        palloc_records = reader.read_table("palloc")

        # Index palloc by (account, payflag) for invoice allocations
        alloc_by_key = {}
        for rec in palloc_records:
            pl_type = _o3_get_str(rec, "pl_type")
            if pl_type != "I":
                continue
            acct = _o3_get_str(rec, "pl_account")
            payflag = _o3_get_int(rec, "pl_payflag")
            key = (acct.upper(), payflag)
            if key not in alloc_by_key:
                alloc_by_key[key] = []
            alloc_by_key[key].append({
                "ref": _o3_get_str(rec, "pl_ref1"),
                "date": _o3_get_date(rec, "pl_date"),
                "amount": abs(_o3_get_num(rec, "pl_val")),
            })

        # Index ptran invoices for getting original dates
        invoice_dates = {}
        for rec in ptran_records:
            trtype = _o3_get_str(rec, "pt_trtype")
            if trtype != "I":
                continue
            acct = _o3_get_str(rec, "pt_account")
            ref = _o3_get_str(rec, "pt_trref")
            date_val = rec.get("PT_TRDATE", rec.get("pt_trdate", rec.get("Pt_trdate")))
            invoice_dates[(acct.upper(), ref)] = _format_date(date_val)

        # Get remittance history for sent status
        db = get_supplier_statement_db()
        all_history = db.get_remittance_history(
            supplier_code=supplier_code, limit=500
        )
        sent_refs = set()
        for entry in all_history:
            key = f"{entry.get('supplier_code', '').strip()}|{entry.get('payment_ref', '').strip()}"
            sent_refs.add(key)

        # Build payment results
        payments = []
        for pay in payment_records:
            acct = pay["account"]
            pay_ref = pay["ref"]
            payflag = pay["payflag"]

            # Get allocations
            invoices = []
            if payflag > 0:
                alloc_key = (acct.upper(), payflag)
                raw_allocs = alloc_by_key.get(alloc_key, [])
                for alloc in raw_allocs:
                    inv_ref = alloc["ref"]
                    # Try to get original invoice date
                    inv_date_key = (acct.upper(), inv_ref)
                    inv_date = invoice_dates.get(inv_date_key, alloc["date"])
                    invoices.append({
                        "ref": inv_ref,
                        "date": inv_date,
                        "amount": alloc["amount"],
                    })

            sent_key = f"{acct}|{pay_ref}"
            remittance_sent = sent_key in sent_refs

            payments.append({
                "supplier_code": acct,
                "supplier_name": supplier_names.get(acct.upper(), acct),
                "payment_date": _format_date(pay["date"]),
                "payment_ref": pay_ref,
                "payment_method": _format_payment_method(pay_ref),
                "total_amount": pay["amount"],
                "invoices": invoices,
                "remittance_sent": remittance_sent,
            })

        # Sort by date descending
        payments.sort(key=lambda p: p["payment_date"], reverse=True)

        return {"success": True, "payments": payments}

    except Exception as e:
        logger.error(f"Error fetching Opera 3 recent payments: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/opera3/supplier-remittance/{account}/generate")
async def opera3_generate_remittance(
    account: str,
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    payment_ref: str = Query(..., description="Payment reference (pt_trref)"),
):
    """
    Generate remittance advice text for a specific Opera 3 payment.

    Reads ptran and palloc from Opera 3 DBF files to build the remittance
    advice document.
    """
    from sql_rag.opera3_foxpro import Opera3Reader

    try:
        reader = Opera3Reader(data_path)

        # Get supplier name
        pname_records = reader.read_table("pname")
        supplier_name = account
        for rec in pname_records:
            if _o3_get_str(rec, "pn_account").upper() == account.upper():
                supplier_name = _o3_get_str(rec, "pn_name")
                break

        # Find the payment in ptran
        ptran_records = reader.read_table("ptran")
        payment_rec = None
        for rec in ptran_records:
            if (_o3_get_str(rec, "pt_trtype") == "P"
                    and _o3_get_str(rec, "pt_account").upper() == account.upper()
                    and _o3_get_str(rec, "pt_trref") == payment_ref):
                payment_rec = rec
                break

        if payment_rec is None:
            raise HTTPException(
                status_code=404,
                detail=f"Payment '{payment_ref}' not found for supplier '{account}'"
            )

        pay_date = _o3_get_date(payment_rec, "pt_trdate")
        total_amount = abs(_o3_get_num(payment_rec, "pt_trvalue"))
        payflag = _o3_get_int(payment_rec, "pt_payflag")
        payment_method = _format_payment_method(payment_ref)

        # Get allocated invoices
        invoices = []
        if payflag > 0:
            palloc_records = reader.read_table("palloc")

            # Index ptran invoices for original dates
            invoice_dates = {}
            for rec in ptran_records:
                if (_o3_get_str(rec, "pt_trtype") == "I"
                        and _o3_get_str(rec, "pt_account").upper() == account.upper()):
                    inv_ref = _o3_get_str(rec, "pt_trref")
                    invoice_dates[inv_ref] = _o3_get_date(rec, "pt_trdate")

            for rec in palloc_records:
                if (_o3_get_str(rec, "pl_account").upper() == account.upper()
                        and _o3_get_int(rec, "pl_payflag") == payflag
                        and _o3_get_str(rec, "pl_type") == "I"):
                    inv_ref = _o3_get_str(rec, "pl_ref1")
                    inv_date = invoice_dates.get(inv_ref, _o3_get_date(rec, "pl_date"))
                    inv_amount = abs(_o3_get_num(rec, "pl_val"))
                    invoices.append({
                        "ref": inv_ref,
                        "date": inv_date,
                        "amount": inv_amount,
                    })

        # Build remittance text
        text = _build_remittance_text(
            supplier_name=supplier_name,
            payment_date=pay_date,
            payment_method=payment_method,
            payment_ref=payment_ref,
            total_amount=total_amount,
            invoices=invoices,
        )

        return {
            "success": True,
            "remittance": {
                "supplier_code": account,
                "supplier_name": supplier_name,
                "payment_date": pay_date,
                "total": total_amount,
                "invoices": invoices,
                "text": text,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error generating Opera 3 remittance for {account}/{payment_ref}: {e}",
            exc_info=True,
        )
        return {"success": False, "error": str(e)}
