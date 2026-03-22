"""
GoCardless Payment Requests Module

Manages GoCardless mandates linked to Opera customers and payment requests
for automated Direct Debit collection.

Database: SQLite (gocardless_payments.db) - local storage, separate from Opera
"""

import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class GCMandate:
    """GoCardless mandate linked to Opera customer."""
    id: int
    opera_account: str
    opera_name: Optional[str]
    gocardless_customer_id: Optional[str]
    mandate_id: str
    mandate_status: str
    scheme: str
    email: Optional[str]
    created_at: str
    updated_at: Optional[str]


@dataclass
class GCPaymentRequest:
    """Payment request sent to GoCardless."""
    id: int
    payment_id: Optional[str]
    mandate_id: str
    opera_account: str
    amount_pence: int
    currency: str
    charge_date: Optional[str]
    description: Optional[str]
    invoice_refs: List[str]
    status: str
    payout_id: Optional[str]
    opera_receipt_ref: Optional[str]
    error_message: Optional[str]
    created_at: str
    updated_at: Optional[str]

    @property
    def amount_pounds(self) -> float:
        return self.amount_pence / 100


class GoCardlessPaymentsDB:
    """
    Database manager for GoCardless mandates and payment requests.

    Uses SQLite for local storage - separate from Opera database.
    """

    DEFAULT_DB_PATH = Path(__file__).parent.parent / "gocardless_payments.db"

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or self._resolve_db_path()
        self._init_db()

    @staticmethod
    def _resolve_db_path() -> Path:
        """Resolve database path, using per-company path if available."""
        try:
            from sql_rag.company_data import get_current_db_path
            path = get_current_db_path("gocardless_payments.db")
            if path is not None:
                return path
        except ImportError:
            pass
        return GoCardlessPaymentsDB.DEFAULT_DB_PATH

    def _init_db(self):
        """Create database tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            # Mandates table - links Opera customers to GoCardless mandates
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS gocardless_mandates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    opera_account TEXT NOT NULL,
                    opera_name TEXT,
                    gocardless_name TEXT,
                    gocardless_customer_id TEXT,
                    mandate_id TEXT NOT NULL,
                    mandate_status TEXT DEFAULT 'active',
                    scheme TEXT DEFAULT 'bacs',
                    email TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT,
                    UNIQUE(opera_account, mandate_id)
                )
            ''')

            # Migration: add gocardless_name column if missing
            cursor.execute("PRAGMA table_info(gocardless_mandates)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'gocardless_name' not in columns:
                cursor.execute('ALTER TABLE gocardless_mandates ADD COLUMN gocardless_name TEXT')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_mandates_opera
                ON gocardless_mandates(opera_account)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_mandates_mandate
                ON gocardless_mandates(mandate_id)
            ''')

            # Payment requests table - tracks payments requested via GoCardless
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS gocardless_payment_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payment_id TEXT UNIQUE,
                    mandate_id TEXT NOT NULL,
                    opera_account TEXT NOT NULL,
                    amount_pence INTEGER NOT NULL,
                    currency TEXT DEFAULT 'GBP',
                    charge_date TEXT,
                    description TEXT,
                    invoice_refs TEXT,
                    status TEXT DEFAULT 'pending',
                    payout_id TEXT,
                    opera_receipt_ref TEXT,
                    error_message TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT
                )
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_requests_status
                ON gocardless_payment_requests(status)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_requests_opera
                ON gocardless_payment_requests(opera_account)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_requests_payment_id
                ON gocardless_payment_requests(payment_id)
            ''')

            # Subscriptions table - links Opera repeat documents to GoCardless subscriptions
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS gocardless_subscriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    subscription_id TEXT UNIQUE NOT NULL,
                    mandate_id TEXT NOT NULL,
                    opera_account TEXT,
                    opera_name TEXT,
                    source_doc TEXT,
                    amount_pence INTEGER NOT NULL,
                    currency TEXT DEFAULT 'GBP',
                    interval_unit TEXT NOT NULL,
                    interval_count INTEGER DEFAULT 1,
                    day_of_month INTEGER,
                    name TEXT,
                    status TEXT DEFAULT 'active',
                    start_date TEXT,
                    end_date TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT,
                    synced_at TEXT
                )
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_subscriptions_mandate
                ON gocardless_subscriptions(mandate_id)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_subscriptions_opera
                ON gocardless_subscriptions(opera_account)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_subscriptions_source_doc
                ON gocardless_subscriptions(source_doc)
            ''')

            # Junction table: subscription -> multiple Opera repeat documents
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS gocardless_subscription_documents (
                    subscription_id TEXT NOT NULL,
                    source_doc TEXT NOT NULL,
                    added_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (subscription_id, source_doc)
                )
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_sub_docs_subscription
                ON gocardless_subscription_documents(subscription_id)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_sub_docs_source_doc
                ON gocardless_subscription_documents(source_doc)
            ''')

            # Migrate any existing source_doc values to junction table
            cursor.execute('''
                INSERT OR IGNORE INTO gocardless_subscription_documents (subscription_id, source_doc)
                SELECT subscription_id, source_doc FROM gocardless_subscriptions
                WHERE source_doc IS NOT NULL AND source_doc != ''
            ''')

            # Mandate setup requests — tracks billing requests sent to customers
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS mandate_setup_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    opera_account TEXT NOT NULL,
                    opera_name TEXT,
                    customer_email TEXT NOT NULL,
                    billing_request_id TEXT,
                    billing_request_flow_id TEXT,
                    authorisation_url TEXT,
                    mandate_id TEXT,
                    gocardless_customer_id TEXT,
                    status TEXT DEFAULT 'pending',
                    status_detail TEXT,
                    email_sent_at TEXT,
                    mandate_active_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT
                )
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_setup_requests_opera
                ON mandate_setup_requests(opera_account)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_setup_requests_status
                ON mandate_setup_requests(status)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_setup_requests_billing_req
                ON mandate_setup_requests(billing_request_id)
            ''')

            # Partner signups — tracks GoCardless partner referral signups
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS gocardless_partner_signups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT,
                    company_email TEXT,
                    billing_request_id TEXT,
                    billing_request_flow_id TEXT,
                    authorisation_url TEXT,
                    status TEXT DEFAULT 'pending',
                    status_detail TEXT,
                    access_token_obtained INTEGER DEFAULT 0,
                    merchant_access_token TEXT,
                    merchant_organisation_id TEXT,
                    merchant_creditor_name TEXT,
                    merchant_app_url TEXT,
                    partner_referral_id TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    completed_at TEXT,
                    updated_at TEXT
                )
            ''')

            # Migrate: add merchant token columns if missing
            cursor.execute("PRAGMA table_info(gocardless_partner_signups)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            for col in ['merchant_access_token', 'merchant_organisation_id', 'merchant_creditor_name', 'merchant_app_url']:
                if col not in existing_cols:
                    cursor.execute(f'ALTER TABLE gocardless_partner_signups ADD COLUMN {col} TEXT')

            conn.commit()
            logger.info(f"GoCardless payments database initialized at {self.db_path}")
        finally:
            conn.close()

    # ============ Partner Signup Management ============

    def create_partner_signup(self, company_name: str, company_email: str,
                              billing_request_id: str = None, billing_request_flow_id: str = None,
                              authorisation_url: str = None) -> dict:
        """Create a new partner signup record."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO gocardless_partner_signups
                (company_name, company_email, billing_request_id, billing_request_flow_id, authorisation_url, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (company_name, company_email, billing_request_id, billing_request_flow_id,
                  authorisation_url, datetime.now().isoformat()))
            conn.commit()
            return {'id': cursor.lastrowid, 'status': 'pending'}
        finally:
            conn.close()

    def _signup_row_to_dict(self, row) -> dict:
        """Convert a partner signup row to a dictionary."""
        return {
            'id': row[0], 'company_name': row[1], 'company_email': row[2],
            'billing_request_id': row[3], 'billing_request_flow_id': row[4],
            'authorisation_url': row[5], 'status': row[6], 'status_detail': row[7],
            'access_token_obtained': bool(row[8]),
            'merchant_access_token': row[9], 'merchant_organisation_id': row[10],
            'merchant_creditor_name': row[11], 'merchant_app_url': row[12],
            'partner_referral_id': row[13],
            'created_at': row[14], 'completed_at': row[15], 'updated_at': row[16]
        }

    _SIGNUP_COLUMNS = '''id, company_name, company_email, billing_request_id, billing_request_flow_id,
                       authorisation_url, status, status_detail, access_token_obtained,
                       merchant_access_token, merchant_organisation_id, merchant_creditor_name,
                       merchant_app_url, partner_referral_id, created_at, completed_at, updated_at'''

    def get_latest_partner_signup(self) -> dict:
        """Get the most recent partner signup record."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(f'''
                SELECT {self._SIGNUP_COLUMNS}
                FROM gocardless_partner_signups
                ORDER BY id DESC LIMIT 1
            ''')
            row = cursor.fetchone()
            if not row:
                return None
            return self._signup_row_to_dict(row)
        finally:
            conn.close()

    def get_all_merchant_signups(self, status: str = None) -> list:
        """Get all partner signup records, optionally filtered by status."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            if status:
                cursor.execute(f'''
                    SELECT {self._SIGNUP_COLUMNS}
                    FROM gocardless_partner_signups
                    WHERE status = ?
                    ORDER BY id DESC
                ''', (status,))
            else:
                cursor.execute(f'''
                    SELECT {self._SIGNUP_COLUMNS}
                    FROM gocardless_partner_signups
                    ORDER BY id DESC
                ''')
            return [self._signup_row_to_dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_merchant_signup(self, signup_id: int) -> dict:
        """Get a specific partner signup by ID."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(f'''
                SELECT {self._SIGNUP_COLUMNS}
                FROM gocardless_partner_signups
                WHERE id = ?
            ''', (signup_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return self._signup_row_to_dict(row)
        finally:
            conn.close()

    def update_partner_signup(self, signup_id: int, **kwargs) -> bool:
        """Update a partner signup record."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            updates = []
            params = []
            for key, value in kwargs.items():
                updates.append(f'{key} = ?')
                params.append(value)
            updates.append('updated_at = ?')
            params.append(datetime.now().isoformat())
            params.append(signup_id)
            cursor.execute(f'''
                UPDATE gocardless_partner_signups SET {', '.join(updates)} WHERE id = ?
            ''', params)
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    # ============ Mandate Management ============

    def link_mandate(
        self,
        opera_account: str,
        mandate_id: str,
        opera_name: Optional[str] = None,
        gocardless_name: Optional[str] = None,
        gocardless_customer_id: Optional[str] = None,
        mandate_status: str = 'active',
        scheme: str = 'bacs',
        email: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Link a GoCardless mandate to an Opera customer.

        Returns the created/updated mandate record.
        """
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            # Remove any __UNLINKED__ placeholder for this mandate before linking
            if opera_account != '__UNLINKED__':
                cursor.execute('''
                    DELETE FROM gocardless_mandates
                    WHERE mandate_id = ? AND opera_account = '__UNLINKED__'
                ''', (mandate_id,))
                if cursor.rowcount > 0:
                    logger.info(f"Removed __UNLINKED__ placeholder for mandate {mandate_id}")

            # Check if this mandate is already linked
            cursor.execute('''
                SELECT id FROM gocardless_mandates
                WHERE opera_account = ? AND mandate_id = ?
            ''', (opera_account, mandate_id))

            existing = cursor.fetchone()

            if existing:
                # Update existing link
                cursor.execute('''
                    UPDATE gocardless_mandates SET
                        opera_name = COALESCE(?, opera_name),
                        gocardless_name = COALESCE(?, gocardless_name),
                        gocardless_customer_id = COALESCE(?, gocardless_customer_id),
                        mandate_status = ?,
                        scheme = ?,
                        email = COALESCE(?, email),
                        updated_at = ?
                    WHERE id = ?
                ''', (opera_name, gocardless_name, gocardless_customer_id, mandate_status, scheme,
                      email, datetime.utcnow().isoformat(), existing[0]))
                mandate_id_db = existing[0]
            else:
                # Insert new link
                cursor.execute('''
                    INSERT INTO gocardless_mandates
                    (opera_account, opera_name, gocardless_name, gocardless_customer_id, mandate_id,
                     mandate_status, scheme, email)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (opera_account, opera_name, gocardless_name, gocardless_customer_id, mandate_id,
                      mandate_status, scheme, email))
                mandate_id_db = cursor.lastrowid

            conn.commit()

            return self.get_mandate_by_id(mandate_id_db)
        finally:
            conn.close()

    def get_mandate_by_id(self, mandate_db_id: int) -> Optional[Dict[str, Any]]:
        """Get a mandate by database ID."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, opera_account, opera_name, gocardless_customer_id,
                       mandate_id, mandate_status, scheme, email, created_at, updated_at,
                       gocardless_name
                FROM gocardless_mandates WHERE id = ?
            ''', (mandate_db_id,))

            row = cursor.fetchone()
            if not row:
                return None

            return {
                'id': row[0],
                'opera_account': row[1],
                'opera_name': row[2],
                'gocardless_customer_id': row[3],
                'mandate_id': row[4],
                'mandate_status': row[5],
                'scheme': row[6],
                'email': row[7],
                'created_at': row[8],
                'updated_at': row[9],
                'gocardless_name': row[10]
            }
        finally:
            conn.close()

    def get_mandate_for_customer(self, opera_account: str) -> Optional[Dict[str, Any]]:
        """Get active mandate for an Opera customer."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, opera_account, opera_name, gocardless_customer_id,
                       mandate_id, mandate_status, scheme, email, created_at, updated_at,
                       gocardless_name
                FROM gocardless_mandates
                WHERE opera_account = ? AND mandate_status = 'active'
                ORDER BY created_at DESC LIMIT 1
            ''', (opera_account,))

            row = cursor.fetchone()
            if not row:
                return None

            return {
                'id': row[0],
                'opera_account': row[1],
                'opera_name': row[2],
                'gocardless_customer_id': row[3],
                'mandate_id': row[4],
                'mandate_status': row[5],
                'scheme': row[6],
                'email': row[7],
                'created_at': row[8],
                'updated_at': row[9],
                'gocardless_name': row[10]
            }
        finally:
            conn.close()

    def list_mandates(
        self,
        status: Optional[str] = None,
        opera_account: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List all mandates, optionally filtered."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            query = '''
                SELECT id, opera_account, opera_name, gocardless_customer_id,
                       mandate_id, mandate_status, scheme, email, created_at, updated_at,
                       gocardless_name
                FROM gocardless_mandates WHERE 1=1
            '''
            params = []

            if status:
                query += ' AND mandate_status = ?'
                params.append(status)

            if opera_account:
                query += ' AND opera_account = ?'
                params.append(opera_account)

            query += ' ORDER BY opera_account'

            cursor.execute(query, params)

            mandates = []
            for row in cursor.fetchall():
                mandates.append({
                    'id': row[0],
                    'opera_account': row[1],
                    'opera_name': row[2],
                    'gocardless_customer_id': row[3],
                    'mandate_id': row[4],
                    'mandate_status': row[5],
                    'scheme': row[6],
                    'email': row[7],
                    'created_at': row[8],
                    'updated_at': row[9],
                    'gocardless_name': row[10]
                })

            return mandates
        finally:
            conn.close()

    def update_mandate_status(self, mandate_id: str, status: str) -> bool:
        """Update the status of a mandate."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE gocardless_mandates
                SET mandate_status = ?, updated_at = ?
                WHERE mandate_id = ?
            ''', (status, datetime.utcnow().isoformat(), mandate_id))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def unlink_mandate(self, mandate_id: str) -> bool:
        """Remove mandate link (doesn't cancel in GoCardless)."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM gocardless_mandates WHERE mandate_id = ?', (mandate_id,))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    # ============ Payment Request Management ============

    def create_payment_request(
        self,
        mandate_id: str,
        opera_account: str,
        amount_pence: int,
        invoice_refs: List[str],
        payment_id: Optional[str] = None,
        charge_date: Optional[str] = None,
        description: Optional[str] = None,
        currency: str = 'GBP'
    ) -> Dict[str, Any]:
        """
        Create a payment request record.

        Returns the created payment request.
        """
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO gocardless_payment_requests
                (payment_id, mandate_id, opera_account, amount_pence, currency,
                 charge_date, description, invoice_refs, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            ''', (payment_id, mandate_id, opera_account, amount_pence, currency,
                  charge_date, description, json.dumps(invoice_refs)))

            request_id = cursor.lastrowid
            conn.commit()

            return self.get_payment_request(request_id)
        finally:
            conn.close()

    def update_payment_request(
        self,
        request_id: int,
        payment_id: Optional[str] = None,
        status: Optional[str] = None,
        charge_date: Optional[str] = None,
        payout_id: Optional[str] = None,
        opera_receipt_ref: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Update a payment request."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            updates = []
            params = []

            if payment_id is not None:
                updates.append('payment_id = ?')
                params.append(payment_id)

            if status is not None:
                updates.append('status = ?')
                params.append(status)

            if charge_date is not None:
                updates.append('charge_date = ?')
                params.append(charge_date)

            if payout_id is not None:
                updates.append('payout_id = ?')
                params.append(payout_id)

            if opera_receipt_ref is not None:
                updates.append('opera_receipt_ref = ?')
                params.append(opera_receipt_ref)

            if error_message is not None:
                updates.append('error_message = ?')
                params.append(error_message)

            if not updates:
                return self.get_payment_request(request_id)

            updates.append('updated_at = ?')
            params.append(datetime.utcnow().isoformat())
            params.append(request_id)

            cursor.execute(f'''
                UPDATE gocardless_payment_requests
                SET {', '.join(updates)}
                WHERE id = ?
            ''', params)

            conn.commit()
            return self.get_payment_request(request_id)
        finally:
            conn.close()

    def get_payment_request(self, request_id: int) -> Optional[Dict[str, Any]]:
        """Get a payment request by ID."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, payment_id, mandate_id, opera_account, amount_pence,
                       currency, charge_date, description, invoice_refs, status,
                       payout_id, opera_receipt_ref, error_message, created_at, updated_at
                FROM gocardless_payment_requests WHERE id = ?
            ''', (request_id,))

            row = cursor.fetchone()
            if not row:
                return None

            return self._row_to_payment_request(row)
        finally:
            conn.close()

    def get_payment_request_by_payment_id(self, payment_id: str) -> Optional[Dict[str, Any]]:
        """Get a payment request by GoCardless payment ID."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, payment_id, mandate_id, opera_account, amount_pence,
                       currency, charge_date, description, invoice_refs, status,
                       payout_id, opera_receipt_ref, error_message, created_at, updated_at
                FROM gocardless_payment_requests WHERE payment_id = ?
            ''', (payment_id,))

            row = cursor.fetchone()
            if not row:
                return None

            return self._row_to_payment_request(row)
        finally:
            conn.close()

    def list_payment_requests(
        self,
        status: Optional[str] = None,
        opera_account: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List payment requests, optionally filtered."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            query = '''
                SELECT id, payment_id, mandate_id, opera_account, amount_pence,
                       currency, charge_date, description, invoice_refs, status,
                       payout_id, opera_receipt_ref, error_message, created_at, updated_at
                FROM gocardless_payment_requests WHERE 1=1
            '''
            params = []

            if status:
                query += ' AND status = ?'
                params.append(status)

            if opera_account:
                query += ' AND opera_account = ?'
                params.append(opera_account)

            query += ' ORDER BY created_at DESC LIMIT ?'
            params.append(limit)

            cursor.execute(query, params)

            requests = []
            for row in cursor.fetchall():
                requests.append(self._row_to_payment_request(row))

            return requests
        finally:
            conn.close()

    def get_pending_requests(self) -> List[Dict[str, Any]]:
        """Get all payment requests that haven't been collected yet."""
        return self.list_payment_requests(status='pending') + \
               self.list_payment_requests(status='pending_submission') + \
               self.list_payment_requests(status='submitted') + \
               self.list_payment_requests(status='confirmed')

    def cancel_payment_request(self, request_id: int, error_message: str = 'Cancelled by user') -> bool:
        """Cancel a pending payment request."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE gocardless_payment_requests
                SET status = 'cancelled', error_message = ?, updated_at = ?
                WHERE id = ? AND status IN ('pending', 'pending_submission')
            ''', (error_message, datetime.utcnow().isoformat(), request_id))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def mark_request_paid_out(
        self,
        payment_id: str,
        payout_id: str,
        opera_receipt_ref: Optional[str] = None
    ) -> bool:
        """Mark a payment request as paid out (received)."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE gocardless_payment_requests
                SET status = 'paid_out', payout_id = ?, opera_receipt_ref = ?, updated_at = ?
                WHERE payment_id = ?
            ''', (payout_id, opera_receipt_ref, datetime.utcnow().isoformat(), payment_id))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def _row_to_payment_request(self, row) -> Dict[str, Any]:
        """Convert database row to payment request dict."""
        invoice_refs = []
        if row[8]:
            try:
                invoice_refs = json.loads(row[8])
            except json.JSONDecodeError:
                invoice_refs = [row[8]] if row[8] else []

        return {
            'id': row[0],
            'payment_id': row[1],
            'mandate_id': row[2],
            'opera_account': row[3],
            'amount_pence': row[4],
            'amount_pounds': row[4] / 100 if row[4] else 0,
            'amount_formatted': f"£{row[4] / 100:,.2f}" if row[4] else '£0.00',
            'currency': row[5],
            'charge_date': row[6],
            'description': row[7],
            'invoice_refs': invoice_refs,
            'status': row[9],
            'payout_id': row[10],
            'opera_receipt_ref': row[11],
            'error_message': row[12],
            'created_at': row[13],
            'updated_at': row[14]
        }

    # ============ Subscription Management ============

    _UNSET = object()  # sentinel for "not provided"

    def save_subscription(
        self,
        subscription_id: str,
        mandate_id: str,
        amount_pence: int,
        interval_unit: str,
        interval_count: int = 1,
        opera_account: Optional[str] = None,
        opera_name: Optional[str] = None,
        source_doc: Any = _UNSET,
        day_of_month: Optional[int] = None,
        name: Optional[str] = None,
        status: str = 'active',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Save or update a subscription record."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            cursor.execute(
                'SELECT id FROM gocardless_subscriptions WHERE subscription_id = ?',
                (subscription_id,))
            existing = cursor.fetchone()

            now = datetime.utcnow().isoformat()

            # Resolve source_doc: _UNSET means keep existing, None means clear, string means set
            source_doc_val = source_doc if source_doc is not self._UNSET else None
            source_doc_sql = '?' if source_doc is not self._UNSET else 'source_doc'

            if existing:
                cursor.execute(f'''
                    UPDATE gocardless_subscriptions SET
                        mandate_id = ?,
                        opera_account = COALESCE(?, opera_account),
                        opera_name = COALESCE(?, opera_name),
                        source_doc = {source_doc_sql},
                        amount_pence = ?,
                        interval_unit = ?,
                        interval_count = ?,
                        day_of_month = COALESCE(?, day_of_month),
                        name = COALESCE(?, name),
                        status = ?,
                        start_date = COALESCE(?, start_date),
                        end_date = COALESCE(?, end_date),
                        updated_at = ?,
                        synced_at = ?
                    WHERE subscription_id = ?
                ''', tuple(
                    [mandate_id, opera_account, opera_name] +
                    ([source_doc_val] if source_doc is not self._UNSET else []) +
                    [amount_pence, interval_unit, interval_count,
                     day_of_month, name, status, start_date, end_date,
                     now, now, subscription_id]))
            else:
                cursor.execute('''
                    INSERT INTO gocardless_subscriptions
                    (subscription_id, mandate_id, opera_account, opera_name, source_doc,
                     amount_pence, currency, interval_unit, interval_count, day_of_month,
                     name, status, start_date, end_date, synced_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'GBP', ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (subscription_id, mandate_id, opera_account, opera_name, source_doc_val,
                      amount_pence, interval_unit, interval_count, day_of_month,
                      name, status, start_date, end_date, now))

            # If source_doc was explicitly set, also add to junction table
            if source_doc is not self._UNSET and source_doc_val:
                cursor.execute('''
                    INSERT OR IGNORE INTO gocardless_subscription_documents
                    (subscription_id, source_doc, added_at)
                    VALUES (?, ?, ?)
                ''', (subscription_id, source_doc_val, now))

            conn.commit()
            return self.get_subscription(subscription_id)
        finally:
            conn.close()

    def get_subscription(self, subscription_id: str) -> Optional[Dict[str, Any]]:
        """Get a subscription by GoCardless subscription ID."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, subscription_id, mandate_id, opera_account, opera_name,
                       source_doc, amount_pence, currency, interval_unit, interval_count,
                       day_of_month, name, status, start_date, end_date,
                       created_at, updated_at, synced_at
                FROM gocardless_subscriptions WHERE subscription_id = ?
            ''', (subscription_id,))

            row = cursor.fetchone()
            if not row:
                return None

            # Fetch linked documents from junction table
            cursor.execute('''
                SELECT source_doc FROM gocardless_subscription_documents
                WHERE subscription_id = ? ORDER BY added_at
            ''', (subscription_id,))
            source_docs = [r[0] for r in cursor.fetchall()]

            return self._row_to_subscription(row, source_docs=source_docs)
        finally:
            conn.close()

    def get_subscription_by_source_doc(self, source_doc: str) -> Optional[Dict[str, Any]]:
        """Get a subscription by Opera source document reference (uses junction table)."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT s.id, s.subscription_id, s.mandate_id, s.opera_account, s.opera_name,
                       s.source_doc, s.amount_pence, s.currency, s.interval_unit, s.interval_count,
                       s.day_of_month, s.name, s.status, s.start_date, s.end_date,
                       s.created_at, s.updated_at, s.synced_at
                FROM gocardless_subscriptions s
                JOIN gocardless_subscription_documents d ON s.subscription_id = d.subscription_id
                WHERE d.source_doc = ? AND s.status != 'cancelled'
                ORDER BY s.created_at DESC LIMIT 1
            ''', (source_doc,))

            row = cursor.fetchone()
            if not row:
                return None

            # Fetch all linked documents for this subscription
            sub_id = row[1]
            cursor.execute('''
                SELECT source_doc FROM gocardless_subscription_documents
                WHERE subscription_id = ? ORDER BY added_at
            ''', (sub_id,))
            source_docs = [r[0] for r in cursor.fetchall()]

            return self._row_to_subscription(row, source_docs=source_docs)
        finally:
            conn.close()

    def list_subscriptions(
        self,
        status: Optional[str] = None,
        opera_account: Optional[str] = None,
        include_cancelled: bool = False
    ) -> List[Dict[str, Any]]:
        """List all subscriptions, optionally filtered. Excludes cancelled by default."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            query = '''
                SELECT id, subscription_id, mandate_id, opera_account, opera_name,
                       source_doc, amount_pence, currency, interval_unit, interval_count,
                       day_of_month, name, status, start_date, end_date,
                       created_at, updated_at, synced_at
                FROM gocardless_subscriptions WHERE 1=1
            '''
            params = []

            if status:
                query += ' AND status = ?'
                params.append(status)
            elif not include_cancelled:
                query += " AND status != 'cancelled'"

            if opera_account:
                query += ' AND opera_account = ?'
                params.append(opera_account)

            query += ' ORDER BY created_at DESC'

            cursor.execute(query, params)
            rows = cursor.fetchall()

            # Batch-fetch all linked documents for all subscriptions
            sub_ids = [row[1] for row in rows]
            docs_by_sub = {}
            if sub_ids:
                placeholders = ','.join('?' * len(sub_ids))
                cursor.execute(f'''
                    SELECT subscription_id, source_doc
                    FROM gocardless_subscription_documents
                    WHERE subscription_id IN ({placeholders})
                    ORDER BY added_at
                ''', sub_ids)
                for doc_row in cursor.fetchall():
                    docs_by_sub.setdefault(doc_row[0], []).append(doc_row[1])

            subscriptions = []
            for row in rows:
                sub_id = row[1]
                subscriptions.append(self._row_to_subscription(
                    row, source_docs=docs_by_sub.get(sub_id, [])))

            return subscriptions
        finally:
            conn.close()

    def update_subscription_status(self, subscription_id: str, status: str) -> bool:
        """Update the status of a subscription."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE gocardless_subscriptions
                SET status = ?, updated_at = ?
                WHERE subscription_id = ?
            ''', (status, datetime.utcnow().isoformat(), subscription_id))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    # ============ Subscription Document Links ============

    def get_subscription_documents(self, subscription_id: str) -> List[str]:
        """Get all Opera source documents linked to a subscription."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT source_doc FROM gocardless_subscription_documents
                WHERE subscription_id = ? ORDER BY added_at
            ''', (subscription_id,))
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

    def add_subscription_document(self, subscription_id: str, source_doc: str) -> bool:
        """Link an Opera repeat document to a subscription. Returns True if added."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO gocardless_subscription_documents
                (subscription_id, source_doc, added_at)
                VALUES (?, ?, ?)
            ''', (subscription_id, source_doc, datetime.utcnow().isoformat()))
            # Also keep legacy source_doc in sync (first linked doc)
            if cursor.rowcount > 0:
                cursor.execute('''
                    UPDATE gocardless_subscriptions
                    SET source_doc = COALESCE(source_doc, ?), updated_at = ?
                    WHERE subscription_id = ?
                ''', (source_doc, datetime.utcnow().isoformat(), subscription_id))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def remove_subscription_document(self, subscription_id: str, source_doc: str) -> bool:
        """Unlink an Opera repeat document from a subscription. Returns True if removed."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM gocardless_subscription_documents
                WHERE subscription_id = ? AND source_doc = ?
            ''', (subscription_id, source_doc))
            removed = cursor.rowcount > 0
            if removed:
                # Update legacy source_doc to first remaining or NULL
                cursor.execute('''
                    SELECT source_doc FROM gocardless_subscription_documents
                    WHERE subscription_id = ? ORDER BY added_at LIMIT 1
                ''', (subscription_id,))
                remaining = cursor.fetchone()
                cursor.execute('''
                    UPDATE gocardless_subscriptions
                    SET source_doc = ?, updated_at = ?
                    WHERE subscription_id = ?
                ''', (remaining[0] if remaining else None,
                      datetime.utcnow().isoformat(), subscription_id))
            conn.commit()
            return removed
        finally:
            conn.close()

    def get_subscriptions_by_source_doc(self, source_doc: str) -> List[Dict[str, Any]]:
        """Get all active subscriptions linked to a given Opera source document."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT s.id, s.subscription_id, s.mandate_id, s.opera_account, s.opera_name,
                       s.source_doc, s.amount_pence, s.currency, s.interval_unit, s.interval_count,
                       s.day_of_month, s.name, s.status, s.start_date, s.end_date,
                       s.created_at, s.updated_at, s.synced_at
                FROM gocardless_subscriptions s
                JOIN gocardless_subscription_documents d ON s.subscription_id = d.subscription_id
                WHERE d.source_doc = ? AND s.status != 'cancelled'
                ORDER BY s.created_at DESC
            ''', (source_doc,))
            return [self._row_to_subscription(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def _row_to_subscription(self, row, source_docs: List[str] = None) -> Dict[str, Any]:
        """Convert database row to subscription dict."""
        amount_pence = row[6] or 0
        interval_unit = row[8] or 'monthly'
        interval_count = row[9] or 1

        # Human-readable frequency
        if interval_unit == 'weekly' and interval_count == 1:
            frequency = 'Weekly'
        elif interval_unit == 'monthly' and interval_count == 1:
            frequency = 'Monthly'
        elif interval_unit == 'monthly' and interval_count == 3:
            frequency = 'Quarterly'
        elif interval_unit == 'yearly' and interval_count == 1:
            frequency = 'Annual'
        else:
            frequency = f"Every {interval_count} {interval_unit}"

        return {
            'id': row[0],
            'subscription_id': row[1],
            'mandate_id': row[2],
            'opera_account': row[3],
            'opera_name': row[4],
            'source_doc': row[5],  # Legacy: first linked doc
            'source_docs': source_docs if source_docs is not None else [],
            'amount_pence': amount_pence,
            'amount_pounds': amount_pence / 100,
            'amount_formatted': f"£{amount_pence / 100:,.2f}",
            'currency': row[7],
            'interval_unit': interval_unit,
            'interval_count': interval_count,
            'frequency': frequency,
            'day_of_month': row[10],
            'name': row[11],
            'status': row[12],
            'start_date': row[13],
            'end_date': row[14],
            'created_at': row[15],
            'updated_at': row[16],
            'synced_at': row[17]
        }

    # ============ Mandate Setup Requests ============

    def create_mandate_setup(
        self,
        opera_account: str,
        customer_email: str,
        opera_name: Optional[str] = None,
        billing_request_id: Optional[str] = None,
        billing_request_flow_id: Optional[str] = None,
        authorisation_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a mandate setup request record."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO mandate_setup_requests
                (opera_account, opera_name, customer_email, billing_request_id,
                 billing_request_flow_id, authorisation_url, status)
                VALUES (?, ?, ?, ?, ?, ?, 'pending')
            ''', (opera_account, opera_name, customer_email, billing_request_id,
                  billing_request_flow_id, authorisation_url))
            request_id = cursor.lastrowid
            conn.commit()
            return self.get_mandate_setup(request_id)
        finally:
            conn.close()

    def update_mandate_setup(
        self,
        setup_id: int,
        billing_request_id: Optional[str] = None,
        billing_request_flow_id: Optional[str] = None,
        authorisation_url: Optional[str] = None,
        mandate_id: Optional[str] = None,
        gocardless_customer_id: Optional[str] = None,
        status: Optional[str] = None,
        status_detail: Optional[str] = None,
        email_sent_at: Optional[str] = None,
        mandate_active_at: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Update a mandate setup request."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            updates = []
            params = []

            for field, value in [
                ('billing_request_id', billing_request_id),
                ('billing_request_flow_id', billing_request_flow_id),
                ('authorisation_url', authorisation_url),
                ('mandate_id', mandate_id),
                ('gocardless_customer_id', gocardless_customer_id),
                ('status', status),
                ('status_detail', status_detail),
                ('email_sent_at', email_sent_at),
                ('mandate_active_at', mandate_active_at),
            ]:
                if value is not None:
                    updates.append(f'{field} = ?')
                    params.append(value)

            if not updates:
                return self.get_mandate_setup(setup_id)

            updates.append('updated_at = ?')
            params.append(datetime.utcnow().isoformat())
            params.append(setup_id)

            cursor.execute(f'''
                UPDATE mandate_setup_requests
                SET {', '.join(updates)}
                WHERE id = ?
            ''', params)
            conn.commit()
            return self.get_mandate_setup(setup_id)
        finally:
            conn.close()

    def get_mandate_setup(self, setup_id: int) -> Optional[Dict[str, Any]]:
        """Get a mandate setup request by ID."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, opera_account, opera_name, customer_email, billing_request_id,
                       billing_request_flow_id, authorisation_url, mandate_id,
                       gocardless_customer_id, status, status_detail,
                       email_sent_at, mandate_active_at, created_at, updated_at
                FROM mandate_setup_requests WHERE id = ?
            ''', (setup_id,))
            row = cursor.fetchone()
            return self._row_to_mandate_setup(row) if row else None
        finally:
            conn.close()

    def get_mandate_setup_by_billing_request(self, billing_request_id: str) -> Optional[Dict[str, Any]]:
        """Get a mandate setup request by GoCardless billing request ID."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, opera_account, opera_name, customer_email, billing_request_id,
                       billing_request_flow_id, authorisation_url, mandate_id,
                       gocardless_customer_id, status, status_detail,
                       email_sent_at, mandate_active_at, created_at, updated_at
                FROM mandate_setup_requests WHERE billing_request_id = ?
            ''', (billing_request_id,))
            row = cursor.fetchone()
            return self._row_to_mandate_setup(row) if row else None
        finally:
            conn.close()

    def list_mandate_setups(
        self,
        status: Optional[str] = None,
        opera_account: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List mandate setup requests, optionally filtered."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            query = '''
                SELECT id, opera_account, opera_name, customer_email, billing_request_id,
                       billing_request_flow_id, authorisation_url, mandate_id,
                       gocardless_customer_id, status, status_detail,
                       email_sent_at, mandate_active_at, created_at, updated_at
                FROM mandate_setup_requests WHERE 1=1
            '''
            params = []

            if status:
                query += ' AND status = ?'
                params.append(status)
            if opera_account:
                query += ' AND opera_account = ?'
                params.append(opera_account)

            query += ' ORDER BY created_at DESC LIMIT ?'
            params.append(limit)

            cursor.execute(query, params)
            return [self._row_to_mandate_setup(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_pending_mandate_setups(self) -> List[Dict[str, Any]]:
        """Get all mandate setup requests that aren't completed or failed."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, opera_account, opera_name, customer_email, billing_request_id,
                       billing_request_flow_id, authorisation_url, mandate_id,
                       gocardless_customer_id, status, status_detail,
                       email_sent_at, mandate_active_at, created_at, updated_at
                FROM mandate_setup_requests
                WHERE status NOT IN ('completed', 'failed', 'cancelled')
                ORDER BY created_at DESC
            ''')
            return [self._row_to_mandate_setup(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def _row_to_mandate_setup(self, row) -> Dict[str, Any]:
        """Convert database row to mandate setup dict."""
        status = row[9] or 'pending'
        status_labels = {
            'pending': 'Awaiting Email',
            'email_sent': 'Email Sent',
            'authorisation_pending': 'Awaiting Customer',
            'mandate_created': 'Mandate Created',
            'mandate_active': 'Mandate Active',
            'completed': 'Completed',
            'failed': 'Failed',
            'cancelled': 'Cancelled',
        }
        return {
            'id': row[0],
            'opera_account': row[1],
            'opera_name': row[2],
            'customer_email': row[3],
            'billing_request_id': row[4],
            'billing_request_flow_id': row[5],
            'authorisation_url': row[6],
            'mandate_id': row[7],
            'gocardless_customer_id': row[8],
            'status': status,
            'status_label': status_labels.get(status, status.replace('_', ' ').title()),
            'status_detail': row[10],
            'email_sent_at': row[11],
            'mandate_active_at': row[12],
            'created_at': row[13],
            'updated_at': row[14],
        }

    # ============ Statistics ============

    def get_statistics(self) -> Dict[str, Any]:
        """Get summary statistics for dashboard."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            # Active mandates count
            cursor.execute('''
                SELECT COUNT(*) FROM gocardless_mandates WHERE mandate_status = 'active'
            ''')
            active_mandates = cursor.fetchone()[0]

            # Pending payments
            cursor.execute('''
                SELECT COUNT(*), COALESCE(SUM(amount_pence), 0)
                FROM gocardless_payment_requests
                WHERE status IN ('pending', 'pending_submission', 'submitted', 'confirmed')
            ''')
            pending_row = cursor.fetchone()
            pending_count = pending_row[0]
            pending_amount = pending_row[1] / 100

            # This month collected
            month_start = date.today().replace(day=1).isoformat()
            cursor.execute('''
                SELECT COUNT(*), COALESCE(SUM(amount_pence), 0)
                FROM gocardless_payment_requests
                WHERE status = 'paid_out' AND created_at >= ?
            ''', (month_start,))
            month_row = cursor.fetchone()
            month_count = month_row[0]
            month_amount = month_row[1] / 100

            # Failed payments (last 30 days)
            thirty_days_ago = (date.today() - timedelta(days=30)).isoformat()
            cursor.execute('''
                SELECT COUNT(*) FROM gocardless_payment_requests
                WHERE status = 'failed' AND created_at >= ?
            ''', (thirty_days_ago,))
            failed_count = cursor.fetchone()[0]

            return {
                'active_mandates': active_mandates,
                'pending_count': pending_count,
                'pending_amount': pending_amount,
                'pending_amount_formatted': f"£{pending_amount:,.2f}",
                'month_collected_count': month_count,
                'month_collected_amount': month_amount,
                'month_collected_formatted': f"£{month_amount:,.2f}",
                'failed_count_30d': failed_count
            }
        finally:
            conn.close()


# Singleton instance
_db_instance: Optional[GoCardlessPaymentsDB] = None


def get_payments_db() -> GoCardlessPaymentsDB:
    """Get or create the singleton database instance."""
    global _db_instance
    if _db_instance is None:
        _db_instance = GoCardlessPaymentsDB()
    return _db_instance


def reset_payments_db():
    """Reset the singleton so the next call picks up the current company's DB path."""
    global _db_instance
    _db_instance = None
