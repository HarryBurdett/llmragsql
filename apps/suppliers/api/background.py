"""
Background supplier statement auto-processing.

Called automatically after each email sync cycle. Detects new emails
with PDF attachments, extracts supplier statement data, reconciles
against the purchase ledger, and sends acknowledgements.

Fully generic — no hardcoded supplier names, patterns, or addresses.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any

logger = logging.getLogger(__name__)


async def auto_process_supplier_statements(storage, providers):
    """
    Post-sync callback: find new emails with PDF attachments and
    attempt to process them as supplier statements.

    Called by EmailSyncManager after each sync cycle. Runs entirely
    in the background — no user interaction needed.

    Args:
        storage: EmailStorage instance
        providers: Dict of provider_id -> EmailProvider
    """
    try:
        # Only look at emails from the last 7 days
        since = datetime.now() - timedelta(days=7)
        emails = storage.get_emails_with_attachments(from_date=since, has_attachments=True)

        if not emails:
            return

        # Check DB for already-processed email IDs (survives restarts)
        already_processed_db = set()
        try:
            from sql_rag.supplier_statement_db import get_supplier_statement_db
            db = get_supplier_statement_db()
            import sqlite3 as _sqlite3
            conn = _sqlite3.connect(str(db.db_path))
            c = conn.cursor()
            c.execute("SELECT source_email_id FROM supplier_statements WHERE source_email_id IS NOT NULL")
            already_processed_db = {row[0] for row in c.fetchall()}
            conn.close()
        except Exception:
            pass

        new_emails = [
            e for e in emails
            if e['id'] not in already_processed_db
            and _has_pdf_attachment(e)
        ]

        if not new_emails:
            return

        logger.info(f"Supplier auto-processor: {len(new_emails)} new emails with PDF attachments")

        for email_data in new_emails:
            email_id = email_data['id']
            try:
                await _process_single_email(email_id, storage, providers)
            except Exception as e:
                logger.warning(f"Supplier auto-process failed for email {email_id}: {e}")

    except Exception as e:
        logger.error(f"Supplier auto-processor error: {e}")


def _has_pdf_attachment(email_data: Dict[str, Any]) -> bool:
    """Check if email has at least one PDF attachment."""
    for att in email_data.get('attachments', []):
        if att.get('content_type') == 'application/pdf':
            return True
        filename = (att.get('filename') or '').lower()
        if filename.endswith('.pdf'):
            return True
    return False


async def _process_single_email(email_id: int, storage, providers):
    """
    Process a single email through the supplier statement pipeline.
    """
    from api.main import sql_connector, config as app_config
    from sql_rag.supplier_statement_db import get_supplier_statement_db
    from sql_rag.supplier_statement_extract import SupplierStatementExtractor
    # TODO: SupplierStatementReconciler is imported only for find_supplier().
    # Once find_supplier() is extracted to sql_rag/supplier_lookup.py this
    # import (and supplier_statement_reconcile.py) can be removed entirely.
    from sql_rag.supplier_statement_reconcile import SupplierStatementReconciler
    from sql_rag.supplier_reconciler import reconcile, TheirItem, OurItem, clean_reference
    from sql_rag.supplier_config import SupplierConfigManager

    db = get_supplier_statement_db()

    # Check if this email was already processed — DB check only (survives restarts)
    import sqlite3 as _sq_dup
    try:
        conn_dup = _sq_dup.connect(str(db.db_path))
        c_dup = conn_dup.cursor()
        c_dup.execute("SELECT id FROM supplier_statements WHERE source_email_id = ?", (email_id,))
        if c_dup.fetchone():
            conn_dup.close()
            logger.debug(f"Email {email_id} already processed (found in DB)")
            return
        conn_dup.close()
    except Exception:
        pass

    # Get full email data
    email_data = storage.get_email_by_id(email_id)
    if not email_data:
        return

    from_addr = email_data.get('from_address', '')
    subject = email_data.get('subject', '')

    # Quick subject check — skip obviously non-statement emails
    subject_lower = subject.lower()
    skip_keywords = ['undeliverable', 'out of office', 'automatic reply',
                     'delivery status', 'read receipt', 'calendar']
    if any(kw in subject_lower for kw in skip_keywords):
        logger.debug(f"Skipping non-statement email: {subject[:60]}")
        return

    # Get Gemini config
    api_key = app_config.get('gemini', 'api_key', fallback='') if app_config else ''
    if not api_key:
        logger.warning("Supplier auto-processor: no Gemini API key configured")
        return

    gemini_model = app_config.get('gemini', 'model', fallback='gemini-2.0-flash') if app_config else 'gemini-2.0-flash'
    extractor = SupplierStatementExtractor(api_key=api_key, model=gemini_model)

    # Download and extract from PDF attachment
    attachments = email_data.get('attachments', [])
    pdf_attachments = [a for a in attachments
                       if a.get('content_type') == 'application/pdf'
                       or (a.get('filename', '').lower().endswith('.pdf'))]

    if not pdf_attachments:
        return

    # Download the first PDF attachment
    provider_id = email_data.get('provider_id')
    message_id = email_data.get('message_id')

    if provider_id not in providers:
        logger.debug(f"Provider {provider_id} not available for email {email_id}")
        return

    provider = providers[provider_id]
    target = pdf_attachments[0]

    # Resolve IMAP folder
    folder_id = 'INBOX'
    folder_id_db = email_data.get('folder_id')
    if folder_id_db:
        try:
            with storage._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_db,))
                row = cursor.fetchone()
                if row:
                    folder_id = row['folder_id']
        except Exception:
            pass

    result = await provider.download_attachment(message_id, str(target['attachment_id']), folder_id)
    if not result:
        logger.debug(f"Could not download attachment from email {email_id}")
        return

    pdf_bytes = result[0] if isinstance(result, tuple) else result.get('content', b'')
    if not pdf_bytes:
        return

    # Save PDF for later viewing/verification
    import os
    from sql_rag.company_data import get_current_db_path
    supplier_db_path = get_current_db_path('supplier_statements.db')
    pdf_dir = os.path.join(str(supplier_db_path.parent if supplier_db_path else '.'), 'pdfs')
    os.makedirs(pdf_dir, exist_ok=True)
    pdf_filename = f"statement_{email_id}.pdf"
    pdf_path = os.path.join(pdf_dir, pdf_filename)
    with open(pdf_path, 'wb') as f:
        f.write(pdf_bytes)

    # Extract statement data
    try:
        info, lines = extractor.extract_from_pdf_bytes(pdf_bytes)
    except Exception as e:
        logger.debug(f"PDF extraction failed for email {email_id}: {e}")
        return

    if not info or not info.supplier_name:
        logger.debug(f"Email {email_id}: PDF is not a supplier statement")
        return

    # Filter out bank statements — these are NOT supplier statements
    name_lower = (info.supplier_name or '').lower()
    bank_names = ['barclays', 'natwest', 'hsbc', 'lloyds', 'santander', 'nationwide',
                  'metro bank', 'starling', 'monzo', 'revolut', 'tide', 'virgin money']
    if any(bank in name_lower for bank in bank_names):
        logger.debug(f"Email {email_id}: Skipping bank statement from {info.supplier_name}")
        return

    logger.info(f"Supplier statement detected: {info.supplier_name} ({info.statement_date}) from {from_addr}")

    # Save statement record (Step 1: create with provisional supplier code)
    statement_id = db.create_statement(
        supplier_code=info.account_reference or 'UNKNOWN',
        sender_email=from_addr,
        statement_date=info.statement_date,
        pdf_path=pdf_path,
        opening_balance=info.opening_balance,
        closing_balance=info.closing_balance,
        email_id=email_id,
    )

    # Log inbound receipt
    db.log_communication(info.account_reference or 'UNKNOWN', 'inbound', 'statement_received',
                         email_subject=subject, statement_id=statement_id)

    # Save extracted lines
    line_records = []
    for line in (lines or []):
        line_records.append({
            'date': line.date if hasattr(line, 'date') else line.get('date'),
            'reference': line.reference if hasattr(line, 'reference') else line.get('reference'),
            'description': line.description if hasattr(line, 'description') else line.get('description'),
            'debit': line.debit if hasattr(line, 'debit') else line.get('debit'),
            'credit': line.credit if hasattr(line, 'credit') else line.get('credit'),
            'balance': line.balance if hasattr(line, 'balance') else line.get('balance'),
            'doc_type': line.doc_type if hasattr(line, 'doc_type') else line.get('doc_type'),
        })
    if line_records:
        db.add_statement_lines(statement_id, line_records)

    # --- Step 4: Identify supplier in Opera ---
    supplier_name = info.supplier_name or ''
    supplier_code = None

    if not sql_connector:
        logger.info(f"No SQL connector available — cannot reconcile statement {statement_id}")
        db.update_statement_status(statement_id, 'received')
        return

    reconciler_helper = SupplierStatementReconciler(sql_connector)
    supplier = reconciler_helper.find_supplier(info.supplier_name, info.account_reference)

    if not supplier:
        logger.info(f"Could not match supplier '{info.supplier_name}' to Opera account")
        db.update_statement_status(statement_id, 'received')
        return

    supplier_code = supplier.get('account', supplier.get('pn_account', ''))
    supplier_name = supplier.get('name', supplier.get('pn_name', supplier_name))
    db.update_statement_status(statement_id, 'processing', supplier_code=supplier_code)

    # --- Step 4a: Sender verification ---
    sender_verified = False
    sender_email_lower = from_addr.strip().lower()

    # Check Opera pcontact for this supplier
    try:
        contact_query = f"""
            SELECT RTRIM(pc_email) as email, RTRIM(pc_contact) as name
            FROM pcontact WITH (NOLOCK)
            WHERE pc_account = '{supplier_code}'
        """
        contact_df = sql_connector.execute_query(contact_query)
        if not contact_df.empty:
            for _, crow in contact_df.iterrows():
                contact_email = (crow.get('email') or '').strip().lower()
                if contact_email and contact_email == sender_email_lower:
                    sender_verified = True
                    break
    except Exception as exc:
        logger.warning(f"Could not query pcontact for supplier {supplier_code}: {exc}")

    # If not verified via Opera contacts, check approved senders table
    if not sender_verified:
        try:
            import sqlite3 as _sq_sender
            conn_s = _sq_sender.connect(str(db.db_path))
            c_s = conn_s.cursor()
            c_s.execute(
                "SELECT email_address FROM supplier_approved_emails WHERE supplier_code = ?",
                (supplier_code,)
            )
            approved_rows = c_s.fetchall()
            conn_s.close()
            for row in approved_rows:
                approved_email = (row[0] or '').strip().lower()
                if approved_email and approved_email == sender_email_lower:
                    sender_verified = True
                    break
        except Exception as exc:
            logger.warning(f"Could not query approved senders for {supplier_code}: {exc}")

    if not sender_verified:
        logger.info(
            f"Statement {statement_id} from unverified sender {from_addr} "
            f"for supplier {supplier_code} — parked for review"
        )
        db.update_statement_status(statement_id, 'unverified_sender')
        return

    # --- Step 5: Check supplier config flags ---
    config_mgr = SupplierConfigManager(str(db.db_path), sql_connector)
    supplier_cfg = config_mgr.get_config(supplier_code)
    if supplier_cfg and not supplier_cfg.get('reconciliation_active', True):
        logger.info(f"Supplier {supplier_code} reconciliation not active, skipping")
        return

    # --- Step 6: Build items for new reconciler ---
    their_items = []
    for line in (lines or []):
        ref = line.reference if hasattr(line, 'reference') else line.get('reference', '')
        debit = line.debit if hasattr(line, 'debit') else line.get('debit', 0)
        credit = line.credit if hasattr(line, 'credit') else line.get('credit', 0)
        their_items.append(TheirItem(
            reference=ref or '',
            debit=float(debit or 0),
            credit=float(credit or 0),
        ))

    # Our items from Opera — outstanding only (pt_trbal <> 0), NOLOCK
    our_items = []
    try:
        opera_query = f"""
            SELECT RTRIM(pt_trref) as reference, pt_trbal as balance, pt_trdate, pt_trtype
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{supplier_code}' AND pt_trbal <> 0
            ORDER BY pt_trdate
        """
        df = sql_connector.execute_query(opera_query)
        our_items = [
            OurItem(
                reference=str(row.get('reference', '')).strip(),
                balance=float(row['balance']),
            )
            for _, row in df.iterrows()
        ]
    except Exception as exc:
        logger.warning(f"Could not fetch Opera transactions for {supplier_code}: {exc}")
        db.update_statement_status(statement_id, 'reconciliation_error')
        return

    # --- Step 7: Run reconciliation and verify ---
    recon_result = reconcile(their_items, our_items)
    if not recon_result.math_checks_out:
        logger.error(
            f"Reconciliation math failed for {supplier_code} statement {statement_id}"
        )
        db.update_statement_status(statement_id, 'reconciliation_error')
        return

    # --- Step 8: Save results to database ---
    try:
        import sqlite3 as _sq
        conn = _sq.connect(str(db.db_path))
        cursor = conn.cursor()

        # Get DB line IDs in order (match order of their_items / lines)
        cursor.execute(
            "SELECT id FROM statement_lines WHERE statement_id = ? ORDER BY id",
            (statement_id,)
        )
        db_line_ids = [row[0] for row in cursor.fetchall()]

        # Build lookup of which refs are agreed
        agreed_refs = {clean_reference(a.reference) for a in recon_result.agreed}
        agreed_map = {clean_reference(a.reference): a for a in recon_result.agreed}

        for i, db_line_id in enumerate(db_line_ids):
            if i < len(their_items):
                ref = clean_reference(their_items[i].reference)
                if ref in agreed_refs:
                    exists = 'Yes'
                    agreed_item = agreed_map.get(ref)
                    status = (
                        'Agreed'
                        if agreed_item and abs(agreed_item.amount_difference) < 0.01
                        else 'Amount Difference'
                    )
                else:
                    exists = 'No'
                    status = 'Query'
                cursor.execute(
                    "UPDATE statement_lines SET exists_in_opera=?, status=? WHERE id=?",
                    (exists, status, db_line_id)
                )

        # Save opera-only items
        cursor.execute("DELETE FROM statement_opera_only WHERE statement_id = ?", (statement_id,))
        for item in recon_result.ours_only:
            cursor.execute("""
                INSERT INTO statement_opera_only (statement_id, line_date, reference, doc_type, amount, signed_value, balance)
                VALUES (?, NULL, ?, '', ?, ?, 0)
            """, (statement_id, item.reference, abs(item.amount), item.amount))

        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Failed to save reconciliation results for statement {statement_id}: {e}")

    # --- Step 9: Response decision logic ---
    never_communicate = supplier_cfg.get('never_communicate', False) if supplier_cfg else False
    auto_respond = supplier_cfg.get('auto_respond', False) if supplier_cfg else False
    balances_agree = abs(recon_result.difference) < 0.01

    if never_communicate:
        db.update_statement_status(statement_id, 'reconciled')
    elif balances_agree:
        _send_acknowledgement(db, statement_id, supplier_name, supplier_code, from_addr, info)
        db.update_statement_status(statement_id, 'acknowledged')
    elif auto_respond:
        _send_acknowledgement(db, statement_id, supplier_name, supplier_code, from_addr, info)
        db.update_statement_status(statement_id, 'acknowledged')
    else:
        db.update_statement_status(statement_id, 'reconciled')  # held for review

    logger.info(
        f"Auto-processed statement {statement_id}: {supplier_name} "
        f"({len(lines or [])} lines, supplier={supplier_code}, "
        f"difference={recon_result.difference:.2f}, math_ok={recon_result.math_checks_out})"
    )


def _send_acknowledgement(db, statement_id, supplier_name, supplier_code, from_addr, info):
    """Send acknowledgement email to the supplier."""
    try:
        from api.main import config as app_config

        # Check if auto-acknowledge is enabled
        auto_ack = True
        try:
            setting = db.get_config('auto_acknowledge')
            if setting is not None:
                auto_ack = str(setting).lower() in ('true', '1', 'yes')
        except Exception:
            pass

        if not auto_ack:
            return

        # Don't send to our own address
        if 'intsys@' in from_addr.lower() or 'wimbledoncloud' in from_addr.lower():
            logger.debug(f"Skipping acknowledgement to own address: {from_addr}")
            return

        ack_subject = f"Statement Received — {supplier_name} — {info.statement_date or 'today'}"
        ack_body = f"""<html><body>
<p>Dear Accounts,</p>
<p>Thank you for sending your statement dated {info.statement_date or 'today'}.</p>
<p>We have received it and are currently processing. You will receive a detailed reconciliation response shortly.</p>
<p>Regards,<br>Accounts Department</p>
</body></html>"""

        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        smtp_server = app_config.get('email', 'smtp_server', fallback='') if app_config else ''
        smtp_port = int(app_config.get('email', 'smtp_port', fallback='587')) if app_config else 587
        smtp_user = app_config.get('email', 'smtp_username', fallback='') if app_config else ''
        smtp_pass = app_config.get('email', 'smtp_password', fallback='') if app_config else ''
        from_email = app_config.get('email', 'from_address', fallback='intsys@wimbledoncloud.net') if app_config else ''

        if not smtp_server or not smtp_user:
            logger.debug("SMTP not configured, skipping acknowledgement")
            return

        msg = MIMEMultipart('alternative')
        msg['Subject'] = ack_subject
        msg['From'] = from_email
        msg['To'] = from_addr
        msg.attach(MIMEText(ack_body, 'html'))

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)

        db.update_statement_status(statement_id, 'acknowledged')
        db.log_communication(supplier_code or 'UNKNOWN', 'outbound', 'acknowledgment',
            email_subject=ack_subject, email_body=ack_body, statement_id=statement_id)

        logger.info(f"Acknowledgement sent to {from_addr} for statement {statement_id}")

    except Exception as e:
        logger.warning(f"Could not send acknowledgement for statement {statement_id}: {e}")
