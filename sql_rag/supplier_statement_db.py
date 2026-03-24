"""
Supplier Statement Automation Database.

Local SQLite database for storing supplier statement processing data.
This is separate from Opera - it stores automation state and history.

Works with both Opera SQL SE and Opera 3 backends.
"""

import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class SupplierStatementDB:
    """
    SQLite database for supplier statement automation.

    Stores:
    - Processed statements and their status
    - Statement line items (extracted data)
    - Approved sender emails per supplier
    - Communication audit trail
    - Supplier field change audit (security)
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the database.

        Args:
            db_path: Path to SQLite database. Defaults to per-company or
                     supplier_statements.db in the project root.
        """
        if db_path:
            self.db_path = Path(db_path)
        else:
            self.db_path = self._resolve_db_path()

        self._init_db()

    @staticmethod
    def _resolve_db_path() -> Path:
        """Resolve database path, using per-company path if available."""
        try:
            from sql_rag.company_data import get_current_db_path
            path = get_current_db_path("supplier_statements.db")
            if path is not None:
                return path
        except ImportError:
            pass
        return Path(__file__).parent.parent / 'supplier_statements.db'

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Initialize database schema."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Processed statements
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supplier_statements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_code TEXT NOT NULL,
                statement_date DATE,
                received_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                sender_email TEXT,
                pdf_path TEXT,
                status TEXT DEFAULT 'received',
                opening_balance DECIMAL(15,2),
                closing_balance DECIMAL(15,2),
                currency TEXT DEFAULT 'GBP',
                acknowledged_at DATETIME,
                processed_at DATETIME,
                approved_by TEXT,
                approved_at DATETIME,
                sent_at DATETIME,
                response_email_id INTEGER,
                error_message TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Statement line items (extracted)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS statement_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                statement_id INTEGER NOT NULL REFERENCES supplier_statements(id),
                line_date DATE,
                reference TEXT,
                description TEXT,
                debit DECIMAL(15,2),
                credit DECIMAL(15,2),
                balance DECIMAL(15,2),
                doc_type TEXT,
                match_status TEXT,
                matched_ptran_id TEXT,
                query_type TEXT,
                query_sent_at DATETIME,
                query_resolved_at DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Approved sender emails per supplier
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supplier_approved_emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_code TEXT NOT NULL,
                email_address TEXT NOT NULL,
                email_domain TEXT,
                added_by TEXT,
                added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                verified BOOLEAN DEFAULT 0,
                UNIQUE(supplier_code, email_address)
            )
        """)

        # Communication audit trail
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supplier_communications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_code TEXT NOT NULL,
                statement_id INTEGER,
                direction TEXT,
                type TEXT,
                email_subject TEXT,
                email_body TEXT,
                sent_at DATETIME,
                sent_by TEXT,
                approved_by TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Supplier field change audit (security)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supplier_change_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_code TEXT NOT NULL,
                field_name TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                changed_by TEXT,
                changed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                alert_sent BOOLEAN DEFAULT 0,
                alert_sent_to TEXT,
                verified BOOLEAN DEFAULT 0,
                verified_by TEXT,
                verified_at DATETIME
            )
        """)

        # Configuration parameters
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supplier_automation_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE NOT NULL,
                value TEXT,
                description TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Per-supplier overrides
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supplier_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_code TEXT UNIQUE NOT NULL,
                acknowledgment_delay_minutes INTEGER,
                processing_priority INTEGER DEFAULT 0,
                custom_payment_terms TEXT,
                notes TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Supplier onboarding status
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supplier_onboarding (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_code TEXT NOT NULL UNIQUE,
                detected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                bank_verified BOOLEAN DEFAULT 0,
                bank_verified_by TEXT,
                bank_verified_at DATETIME,
                terms_confirmed BOOLEAN DEFAULT 0,
                senders_configured BOOLEAN DEFAULT 0,
                category TEXT DEFAULT 'standard',
                priority INTEGER DEFAULT 5,
                notes TEXT,
                completed_at DATETIME
            )
        """)

        # Contact extensions (automation-specific roles beyond Opera zcontacts)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supplier_contacts_ext (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_code TEXT NOT NULL,
                zcontact_id TEXT,
                name TEXT,
                role TEXT,
                email TEXT,
                phone TEXT,
                is_statement_contact BOOLEAN DEFAULT 0,
                is_payment_contact BOOLEAN DEFAULT 0,
                is_query_contact BOOLEAN DEFAULT 0,
                preferred_contact_method TEXT DEFAULT 'email',
                notes TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Remittance advice log
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supplier_remittance_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_code TEXT NOT NULL,
                payment_date DATE,
                payment_ref TEXT,
                payment_method TEXT,
                total_amount DECIMAL(15,2),
                invoice_count INTEGER DEFAULT 0,
                invoices_json TEXT,
                sent_to TEXT,
                sent_at DATETIME,
                sent_by TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Indexes for performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_statements_supplier
            ON supplier_statements(supplier_code)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_statements_status
            ON supplier_statements(status)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_statements_received
            ON supplier_statements(received_date)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_lines_statement
            ON statement_lines(statement_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_lines_match_status
            ON statement_lines(match_status)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_approved_emails_supplier
            ON supplier_approved_emails(supplier_code)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_change_audit_supplier
            ON supplier_change_audit(supplier_code)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_change_audit_verified
            ON supplier_change_audit(verified)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_onboarding_supplier
            ON supplier_onboarding(supplier_code)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_contacts_ext_supplier
            ON supplier_contacts_ext(supplier_code)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_remittance_supplier
            ON supplier_remittance_log(supplier_code)
        """)

        # Insert default configuration if not exists
        default_config = [
            # Immediate response settings
            ('auto_acknowledge', 'true', 'Automatically send acknowledgment when statement received'),
            ('auto_process', 'true', 'Automatically reconcile statement when received'),
            ('auto_respond_if_reconciled', 'true', 'Auto-send response if fully reconciled (no queries)'),
            ('auto_respond_with_queries', 'false', 'Auto-send response even if there are queries (requires approval if false)'),
            ('require_approval_above', '1000', 'Require manual approval for responses with variance above this amount'),

            # Timing settings
            ('acknowledgment_delay_minutes', '0', 'Delay before sending receipt acknowledgment (0 = immediate)'),
            ('processing_sla_hours', '24', 'Target time to process statement'),
            ('query_response_days', '5', 'Expected supplier response time'),
            ('follow_up_reminder_days', '7', 'Days before sending follow-up'),

            # Thresholds
            ('large_discrepancy_threshold', '500', 'Amount requiring manual review'),
            ('old_statement_threshold_days', '14', 'Days after which statement is considered old'),
            ('payment_notification_days', '90', 'Only notify payments made within this period'),

            # Notifications
            ('security_alert_recipients', '', 'Emails for bank detail change alerts (comma-separated)'),
            ('response_cc_email', '', 'CC email address for all sent responses'),

            # Remittance advice
            ('remittance_auto_send', 'false', 'Send remittance advice automatically after payment'),
            ('remittance_format', 'email', 'Remittance format: email or pdf'),
            ('remittance_cc', '', 'CC internal address on remittance'),

            # Onboarding
            ('onboarding_auto_detect', 'true', 'Auto-detect new suppliers from Opera'),
            ('onboarding_require_bank_verify', 'true', 'Require bank detail phone verification for new suppliers'),
        ]
        for key, value, description in default_config:
            cursor.execute("""
                INSERT OR IGNORE INTO supplier_automation_config (key, value, description)
                VALUES (?, ?, ?)
            """, (key, value, description))

        conn.commit()
        conn.close()
        logger.info(f"Supplier statement database initialized at {self.db_path}")

    # Statement CRUD operations
    def create_statement(
        self,
        supplier_code: str,
        sender_email: Optional[str] = None,
        statement_date: Optional[str] = None,
        pdf_path: Optional[str] = None,
        opening_balance: Optional[float] = None,
        closing_balance: Optional[float] = None,
        currency: str = 'GBP'
    ) -> int:
        """Create a new statement record."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO supplier_statements
            (supplier_code, sender_email, statement_date, pdf_path,
             opening_balance, closing_balance, currency, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'received')
        """, (supplier_code, sender_email, statement_date, pdf_path,
              opening_balance, closing_balance, currency))

        statement_id = cursor.lastrowid
        conn.commit()
        conn.close()

        logger.info(f"Created statement {statement_id} for supplier {supplier_code}")
        return statement_id

    def update_statement_status(
        self,
        statement_id: int,
        status: str,
        error_message: Optional[str] = None,
        **kwargs
    ):
        """Update statement status and optional fields."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Build update query dynamically
        updates = ['status = ?', 'updated_at = CURRENT_TIMESTAMP']
        params = [status]

        if error_message:
            updates.append('error_message = ?')
            params.append(error_message)

        # Handle timestamp fields based on status
        if status == 'acknowledged':
            updates.append('acknowledged_at = CURRENT_TIMESTAMP')
        elif status == 'reconciled':
            updates.append('processed_at = CURRENT_TIMESTAMP')
        elif status == 'approved':
            updates.append('approved_at = CURRENT_TIMESTAMP')
            if 'approved_by' in kwargs:
                updates.append('approved_by = ?')
                params.append(kwargs['approved_by'])
        elif status == 'sent':
            updates.append('sent_at = CURRENT_TIMESTAMP')

        # Additional kwargs
        for key, value in kwargs.items():
            if key not in ('approved_by',):  # Already handled
                updates.append(f'{key} = ?')
                params.append(value)

        params.append(statement_id)

        cursor.execute(f"""
            UPDATE supplier_statements
            SET {', '.join(updates)}
            WHERE id = ?
        """, params)

        conn.commit()
        conn.close()

    def get_statement(self, statement_id: int) -> Optional[Dict[str, Any]]:
        """Get a statement by ID."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM supplier_statements WHERE id = ?
        """, (statement_id,))

        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    def get_statements_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Get all statements with a given status."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM supplier_statements
            WHERE status = ?
            ORDER BY received_date DESC
        """, (status,))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    # Statement line operations
    def add_statement_lines(self, statement_id: int, lines: List[Dict[str, Any]]):
        """Add extracted lines to a statement."""
        conn = self._get_connection()
        cursor = conn.cursor()

        for line in lines:
            cursor.execute("""
                INSERT INTO statement_lines
                (statement_id, line_date, reference, description,
                 debit, credit, balance, doc_type, match_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'unmatched')
            """, (
                statement_id,
                line.get('date'),
                line.get('reference'),
                line.get('description'),
                line.get('debit'),
                line.get('credit'),
                line.get('balance'),
                line.get('doc_type')
            ))

        conn.commit()
        conn.close()

        logger.info(f"Added {len(lines)} lines to statement {statement_id}")

    def update_line_match_status(
        self,
        line_id: int,
        match_status: str,
        matched_ptran_id: Optional[str] = None,
        query_type: Optional[str] = None
    ):
        """Update the match status of a statement line."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE statement_lines
            SET match_status = ?, matched_ptran_id = ?, query_type = ?
            WHERE id = ?
        """, (match_status, matched_ptran_id, query_type, line_id))

        conn.commit()
        conn.close()

    def mark_query_sent(self, line_id: int):
        """Mark a query as sent for a line."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE statement_lines
            SET query_sent_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (line_id,))

        conn.commit()
        conn.close()

    def mark_query_resolved(self, line_id: int):
        """Mark a query as resolved."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE statement_lines
            SET query_resolved_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (line_id,))

        conn.commit()
        conn.close()

    # Approved emails
    def add_approved_email(
        self,
        supplier_code: str,
        email_address: str,
        added_by: Optional[str] = None,
        verified: bool = False
    ):
        """Add an approved sender email for a supplier."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Extract domain
        domain = email_address.split('@')[-1] if '@' in email_address else None

        cursor.execute("""
            INSERT OR REPLACE INTO supplier_approved_emails
            (supplier_code, email_address, email_domain, added_by, verified)
            VALUES (?, ?, ?, ?, ?)
        """, (supplier_code, email_address.lower(), domain, added_by, verified))

        conn.commit()
        conn.close()

    def is_email_approved(self, supplier_code: str, email_address: str) -> bool:
        """Check if an email address is approved for a supplier."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT 1 FROM supplier_approved_emails
            WHERE supplier_code = ? AND email_address = ?
        """, (supplier_code, email_address.lower()))

        result = cursor.fetchone() is not None
        conn.close()
        return result

    # Security audit
    def log_supplier_change(
        self,
        supplier_code: str,
        field_name: str,
        old_value: str,
        new_value: str,
        changed_by: Optional[str] = None
    ):
        """Log a change to a supplier's sensitive fields."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO supplier_change_audit
            (supplier_code, field_name, old_value, new_value, changed_by)
            VALUES (?, ?, ?, ?, ?)
        """, (supplier_code, field_name, old_value, new_value, changed_by))

        conn.commit()
        conn.close()

        logger.warning(
            f"SECURITY: Supplier {supplier_code} {field_name} changed "
            f"from '{old_value}' to '{new_value}' by {changed_by}"
        )

    def get_unverified_changes(self) -> List[Dict[str, Any]]:
        """Get all unverified supplier changes."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM supplier_change_audit
            WHERE verified = 0
            ORDER BY changed_at DESC
        """)

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def verify_change(self, change_id: int, verified_by: str):
        """Mark a supplier change as verified."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE supplier_change_audit
            SET verified = 1, verified_by = ?, verified_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (verified_by, change_id))

        conn.commit()
        conn.close()

    # Configuration
    def get_config(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get a configuration value."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT value FROM supplier_automation_config WHERE key = ?
        """, (key,))

        row = cursor.fetchone()
        conn.close()

        return row['value'] if row else default

    def set_config(self, key: str, value: str):
        """Set a configuration value."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE supplier_automation_config
            SET value = ?, updated_at = CURRENT_TIMESTAMP
            WHERE key = ?
        """, (value, key))

        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO supplier_automation_config (key, value)
                VALUES (?, ?)
            """, (key, value))

        conn.commit()
        conn.close()

    def get_all_config(self) -> Dict[str, str]:
        """Get all configuration values."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT key, value FROM supplier_automation_config")

        result = {row['key']: row['value'] for row in cursor.fetchall()}
        conn.close()

        return result

    # Onboarding operations
    def get_onboarding_status(self, supplier_code: str) -> Optional[Dict[str, Any]]:
        """Get onboarding status for a supplier."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM supplier_onboarding WHERE supplier_code = ?", (supplier_code,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def create_onboarding(self, supplier_code: str, category: str = 'standard', priority: int = 5) -> int:
        """Create onboarding record for a new supplier."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO supplier_onboarding (supplier_code, category, priority)
            VALUES (?, ?, ?)
        """, (supplier_code, category, priority))
        row_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return row_id

    def update_onboarding(self, supplier_code: str, **kwargs):
        """Update onboarding fields for a supplier."""
        conn = self._get_connection()
        cursor = conn.cursor()
        updates = []
        params = []
        for key, value in kwargs.items():
            updates.append(f'{key} = ?')
            params.append(value)
        if not updates:
            conn.close()
            return
        params.append(supplier_code)
        cursor.execute(f"""
            UPDATE supplier_onboarding SET {', '.join(updates)} WHERE supplier_code = ?
        """, params)
        conn.commit()
        conn.close()

    def get_pending_onboarding(self) -> List[Dict[str, Any]]:
        """Get all suppliers with incomplete onboarding."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM supplier_onboarding
            WHERE completed_at IS NULL
            ORDER BY priority DESC, detected_at ASC
        """)
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    # Contact extension operations
    def get_contacts(self, supplier_code: str) -> List[Dict[str, Any]]:
        """Get all contact extensions for a supplier."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM supplier_contacts_ext
            WHERE supplier_code = ?
            ORDER BY is_statement_contact DESC, is_payment_contact DESC, name
        """, (supplier_code,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def upsert_contact(self, supplier_code: str, contact_id: Optional[int] = None, **kwargs) -> int:
        """Create or update a contact extension."""
        conn = self._get_connection()
        cursor = conn.cursor()
        if contact_id:
            updates = ['updated_at = CURRENT_TIMESTAMP']
            params = []
            for key, value in kwargs.items():
                updates.append(f'{key} = ?')
                params.append(value)
            params.append(contact_id)
            cursor.execute(f"""
                UPDATE supplier_contacts_ext SET {', '.join(updates)} WHERE id = ?
            """, params)
            result_id = contact_id
        else:
            cols = ['supplier_code'] + list(kwargs.keys())
            placeholders = ', '.join(['?'] * len(cols))
            values = [supplier_code] + list(kwargs.values())
            cursor.execute(f"""
                INSERT INTO supplier_contacts_ext ({', '.join(cols)}) VALUES ({placeholders})
            """, values)
            result_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return result_id

    def delete_contact(self, contact_id: int):
        """Delete a contact extension."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM supplier_contacts_ext WHERE id = ?", (contact_id,))
        conn.commit()
        conn.close()

    def get_statement_contact(self, supplier_code: str) -> Optional[Dict[str, Any]]:
        """Get the designated statement contact for a supplier."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM supplier_contacts_ext
            WHERE supplier_code = ? AND is_statement_contact = 1
            LIMIT 1
        """, (supplier_code,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    # Remittance log operations
    def log_remittance(self, supplier_code: str, payment_date: str, payment_ref: str,
                       payment_method: str, total_amount: float, invoice_count: int,
                       invoices_json: str, sent_to: str, sent_by: Optional[str] = None) -> int:
        """Log a sent remittance advice."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO supplier_remittance_log
            (supplier_code, payment_date, payment_ref, payment_method, total_amount,
             invoice_count, invoices_json, sent_to, sent_at, sent_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        """, (supplier_code, payment_date, payment_ref, payment_method, total_amount,
              invoice_count, invoices_json, sent_to, sent_by))
        result_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return result_id

    def get_remittance_history(self, supplier_code: Optional[str] = None,
                               limit: int = 50) -> List[Dict[str, Any]]:
        """Get remittance advice history."""
        conn = self._get_connection()
        cursor = conn.cursor()
        if supplier_code:
            cursor.execute("""
                SELECT * FROM supplier_remittance_log
                WHERE supplier_code = ?
                ORDER BY sent_at DESC LIMIT ?
            """, (supplier_code, limit))
        else:
            cursor.execute("""
                SELECT * FROM supplier_remittance_log
                ORDER BY sent_at DESC LIMIT ?
            """, (limit,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    # Communication log
    def log_communication(
        self,
        supplier_code: str,
        direction: str,
        comm_type: str,
        email_subject: Optional[str] = None,
        email_body: Optional[str] = None,
        statement_id: Optional[int] = None,
        sent_by: Optional[str] = None,
        approved_by: Optional[str] = None
    ):
        """Log a communication with a supplier."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO supplier_communications
            (supplier_code, statement_id, direction, type, email_subject,
             email_body, sent_at, sent_by, approved_by)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
        """, (supplier_code, statement_id, direction, comm_type,
              email_subject, email_body, sent_by, approved_by))

        conn.commit()
        conn.close()


# Module-level instance for convenience
_db_instance: Optional[SupplierStatementDB] = None


def get_supplier_statement_db() -> SupplierStatementDB:
    """Get or create the supplier statement database instance."""
    global _db_instance
    if _db_instance is None:
        _db_instance = SupplierStatementDB()
    return _db_instance


def reset_supplier_statement_db():
    """Reset the singleton instance (used on company switch)."""
    global _db_instance
    _db_instance = None
