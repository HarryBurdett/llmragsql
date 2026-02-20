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

            conn.commit()
            logger.info(f"GoCardless payments database initialized at {self.db_path}")
        finally:
            conn.close()

    # ============ Mandate Management ============

    def link_mandate(
        self,
        opera_account: str,
        mandate_id: str,
        opera_name: Optional[str] = None,
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
                        gocardless_customer_id = COALESCE(?, gocardless_customer_id),
                        mandate_status = ?,
                        scheme = ?,
                        email = COALESCE(?, email),
                        updated_at = ?
                    WHERE id = ?
                ''', (opera_name, gocardless_customer_id, mandate_status, scheme,
                      email, datetime.utcnow().isoformat(), existing[0]))
                mandate_id_db = existing[0]
            else:
                # Insert new link
                cursor.execute('''
                    INSERT INTO gocardless_mandates
                    (opera_account, opera_name, gocardless_customer_id, mandate_id,
                     mandate_status, scheme, email)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (opera_account, opera_name, gocardless_customer_id, mandate_id,
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
                       mandate_id, mandate_status, scheme, email, created_at, updated_at
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
                'updated_at': row[9]
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
                       mandate_id, mandate_status, scheme, email, created_at, updated_at
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
                'updated_at': row[9]
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
                       mandate_id, mandate_status, scheme, email, created_at, updated_at
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
                    'updated_at': row[9]
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
