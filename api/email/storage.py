"""
SQLite storage for email data.
Handles persistence of emails, providers, folders, and sync logs.
"""

import sqlite3
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager

from .providers.base import EmailMessage, EmailFolder, ProviderType

logger = logging.getLogger(__name__)


class EmailStorage:
    """
    SQLite-based storage for email data.
    """

    def __init__(self, db_path: str = "email_data.db"):
        """
        Initialize email storage.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self._init_database()

    @contextmanager
    def _get_connection(self):
        """Get a database connection with context management."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def _init_database(self):
        """Initialize database schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Provider configurations
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS email_providers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    provider_type TEXT NOT NULL CHECK (provider_type IN ('microsoft', 'gmail', 'imap')),
                    config_json TEXT,
                    enabled INTEGER DEFAULT 1,
                    last_sync TEXT,
                    sync_status TEXT DEFAULT 'pending',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Folders to monitor
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS email_folders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider_id INTEGER NOT NULL,
                    folder_id TEXT NOT NULL,
                    folder_name TEXT NOT NULL,
                    monitored INTEGER DEFAULT 1,
                    last_sync TEXT,
                    unread_count INTEGER DEFAULT 0,
                    total_count INTEGER DEFAULT 0,
                    FOREIGN KEY (provider_id) REFERENCES email_providers(id) ON DELETE CASCADE,
                    UNIQUE(provider_id, folder_id)
                )
            """)

            # Email messages
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS emails (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider_id INTEGER NOT NULL,
                    message_id TEXT NOT NULL,
                    thread_id TEXT,
                    folder_id INTEGER,
                    from_address TEXT NOT NULL,
                    from_name TEXT,
                    to_addresses TEXT,
                    cc_addresses TEXT,
                    subject TEXT,
                    body_preview TEXT,
                    body_html TEXT,
                    body_text TEXT,
                    received_at TEXT NOT NULL,
                    sent_at TEXT,
                    is_read INTEGER DEFAULT 0,
                    is_flagged INTEGER DEFAULT 0,
                    has_attachments INTEGER DEFAULT 0,
                    category TEXT,
                    category_confidence REAL,
                    category_reason TEXT,
                    linked_account TEXT,
                    linked_at TEXT,
                    linked_by TEXT,
                    raw_headers TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (provider_id) REFERENCES email_providers(id) ON DELETE CASCADE,
                    FOREIGN KEY (folder_id) REFERENCES email_folders(id),
                    UNIQUE(provider_id, message_id)
                )
            """)

            # Email attachments
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS email_attachments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email_id INTEGER NOT NULL,
                    attachment_id TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    content_type TEXT,
                    size_bytes INTEGER,
                    FOREIGN KEY (email_id) REFERENCES emails(id) ON DELETE CASCADE
                )
            """)

            # Sync log
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sync_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider_id INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
                    emails_synced INTEGER DEFAULT 0,
                    error_message TEXT,
                    FOREIGN KEY (provider_id) REFERENCES email_providers(id) ON DELETE CASCADE
                )
            """)

            # GoCardless import tracking
            # Only marks emails as processed AFTER successful Opera import
            # Supports both email-based and API-based imports
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gocardless_imports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email_id INTEGER,
                    payout_id TEXT,
                    source TEXT DEFAULT 'email' CHECK (source IN ('email', 'api')),
                    bank_reference TEXT,
                    gross_amount REAL,
                    net_amount REAL,
                    gocardless_fees REAL,
                    vat_on_fees REAL,
                    payment_count INTEGER,
                    payments_json TEXT,
                    target_system TEXT NOT NULL CHECK (target_system IN ('opera_se', 'opera3')),
                    batch_ref TEXT,
                    import_date TEXT NOT NULL,
                    imported_by TEXT,
                    FOREIGN KEY (email_id) REFERENCES emails(id)
                )
            """)

            # Migration: Check if email_id is NOT NULL and recreate table if needed
            # This handles existing databases that had email_id as NOT NULL
            try:
                cursor.execute("PRAGMA table_info(gocardless_imports)")
                columns = cursor.fetchall()
                email_id_col = next((c for c in columns if c[1] == 'email_id'), None)
                needs_migration = email_id_col and email_id_col[2] == 'INTEGER' and email_id_col[3] == 1  # notnull=1

                if needs_migration:
                    logger.info("Migrating gocardless_imports table to allow NULL email_id")
                    # Backup existing data
                    cursor.execute("SELECT * FROM gocardless_imports")
                    existing_data = cursor.fetchall()

                    # Drop and recreate
                    cursor.execute("DROP TABLE gocardless_imports")
                    cursor.execute("""
                        CREATE TABLE gocardless_imports (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            email_id INTEGER,
                            payout_id TEXT,
                            source TEXT DEFAULT 'email' CHECK (source IN ('email', 'api')),
                            bank_reference TEXT,
                            gross_amount REAL,
                            net_amount REAL,
                            gocardless_fees REAL,
                            vat_on_fees REAL,
                            payment_count INTEGER,
                            payments_json TEXT,
                            target_system TEXT NOT NULL CHECK (target_system IN ('opera_se', 'opera3')),
                            batch_ref TEXT,
                            import_date TEXT NOT NULL,
                            imported_by TEXT,
                            FOREIGN KEY (email_id) REFERENCES emails(id)
                        )
                    """)
                    # Note: Existing data not migrated as columns changed significantly
                    logger.info("gocardless_imports table migrated successfully")
            except Exception as mig_err:
                logger.debug(f"Migration check: {mig_err}")

            # Add new columns if they don't exist (for existing databases)
            try:
                cursor.execute("ALTER TABLE gocardless_imports ADD COLUMN payout_id TEXT")
            except:
                pass
            try:
                cursor.execute("ALTER TABLE gocardless_imports ADD COLUMN source TEXT DEFAULT 'email'")
            except:
                pass
            try:
                cursor.execute("ALTER TABLE gocardless_imports ADD COLUMN gocardless_fees REAL")
            except:
                pass
            try:
                cursor.execute("ALTER TABLE gocardless_imports ADD COLUMN vat_on_fees REAL")
            except:
                pass
            try:
                cursor.execute("ALTER TABLE gocardless_imports ADD COLUMN payments_json TEXT")
            except:
                pass

            # Bank statement import tracking
            # Tracks bank statement imports from both email attachments and file uploads
            # Supports both email-based and file-based imports (like GoCardless)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bank_statement_imports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email_id INTEGER,
                    attachment_id TEXT,
                    source TEXT DEFAULT 'email' CHECK (source IN ('email', 'file')),
                    bank_code TEXT NOT NULL,
                    filename TEXT,
                    total_receipts REAL DEFAULT 0,
                    total_payments REAL DEFAULT 0,
                    transactions_imported INTEGER DEFAULT 0,
                    target_system TEXT NOT NULL DEFAULT 'opera_se' CHECK (target_system IN ('opera_se', 'opera3')),
                    import_date TEXT DEFAULT CURRENT_TIMESTAMP,
                    imported_by TEXT,
                    FOREIGN KEY (email_id) REFERENCES emails(id)
                )
            """)

            # Migration: Check if bank_statement_imports needs migration for new columns
            try:
                cursor.execute("PRAGMA table_info(bank_statement_imports)")
                columns = {c[1] for c in cursor.fetchall()}

                # Check if email_id is NOT NULL (needs migration to allow NULL for file imports)
                cursor.execute("PRAGMA table_info(bank_statement_imports)")
                cols_info = cursor.fetchall()
                email_id_col = next((c for c in cols_info if c[1] == 'email_id'), None)
                needs_migration = email_id_col and email_id_col[3] == 1  # notnull=1

                # Also check if source column exists
                if 'source' not in columns or 'target_system' not in columns or needs_migration:
                    logger.info("Migrating bank_statement_imports table for enhanced tracking")
                    # Backup existing data
                    cursor.execute("SELECT * FROM bank_statement_imports")
                    existing_data = cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description] if cursor.description else []

                    # Drop and recreate with new schema
                    cursor.execute("DROP TABLE bank_statement_imports")
                    cursor.execute("""
                        CREATE TABLE bank_statement_imports (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            email_id INTEGER,
                            attachment_id TEXT,
                            source TEXT DEFAULT 'email' CHECK (source IN ('email', 'file')),
                            bank_code TEXT NOT NULL,
                            filename TEXT,
                            total_receipts REAL DEFAULT 0,
                            total_payments REAL DEFAULT 0,
                            transactions_imported INTEGER DEFAULT 0,
                            target_system TEXT NOT NULL DEFAULT 'opera_se' CHECK (target_system IN ('opera_se', 'opera3')),
                            import_date TEXT DEFAULT CURRENT_TIMESTAMP,
                            imported_by TEXT,
                            FOREIGN KEY (email_id) REFERENCES emails(id)
                        )
                    """)

                    # Restore existing data with defaults for new columns
                    for row in existing_data:
                        row_dict = dict(zip(column_names, row)) if column_names else {}
                        cursor.execute("""
                            INSERT INTO bank_statement_imports
                            (email_id, attachment_id, source, bank_code, filename, transactions_imported, import_date, imported_by, target_system)
                            VALUES (?, ?, 'email', ?, ?, ?, ?, ?, 'opera_se')
                        """, (
                            row_dict.get('email_id'),
                            row_dict.get('attachment_id'),
                            row_dict.get('bank_code'),
                            row_dict.get('filename'),
                            row_dict.get('transactions_imported', 0),
                            row_dict.get('import_date'),
                            row_dict.get('imported_by')
                        ))
                    logger.info(f"Migrated {len(existing_data)} bank statement import records")
            except Exception as e:
                logger.warning(f"Bank statement import migration check: {e}")

            # Indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_from ON emails(from_address)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_received ON emails(received_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_category ON emails(category)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_linked ON emails(linked_account)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_emails_provider_msg ON emails(provider_id, message_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_gocardless_imports_email ON gocardless_imports(email_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bank_statement_imports_email ON bank_statement_imports(email_id)")

            logger.info(f"Email database initialized at {self.db_path}")

    # ==================== Provider Methods ====================

    def add_provider(
        self,
        name: str,
        provider_type: ProviderType,
        config: Dict[str, Any]
    ) -> int:
        """Add a new email provider configuration."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO email_providers (name, provider_type, config_json, enabled)
                VALUES (?, ?, ?, 1)
            """, (name, provider_type.value, json.dumps(config)))
            return cursor.lastrowid

    def update_provider(self, provider_id: int, **kwargs) -> bool:
        """Update provider configuration."""
        allowed_fields = {'name', 'config_json', 'enabled', 'last_sync', 'sync_status'}
        updates = {k: v for k, v in kwargs.items() if k in allowed_fields}

        if not updates:
            return False

        if 'config_json' in updates and isinstance(updates['config_json'], dict):
            updates['config_json'] = json.dumps(updates['config_json'])

        set_clause = ', '.join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [provider_id]

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"UPDATE email_providers SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                values
            )
            return cursor.rowcount > 0

    def get_provider(self, provider_id: int) -> Optional[Dict[str, Any]]:
        """Get provider by ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM email_providers WHERE id = ?", (provider_id,))
            row = cursor.fetchone()
            if row:
                result = dict(row)
                if result.get('config_json'):
                    result['config'] = json.loads(result['config_json'])
                return result
            return None

    def get_all_providers(self, enabled_only: bool = False) -> List[Dict[str, Any]]:
        """Get all providers."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT * FROM email_providers"
            if enabled_only:
                query += " WHERE enabled = 1"
            cursor.execute(query)

            results = []
            for row in cursor.fetchall():
                result = dict(row)
                if result.get('config_json'):
                    result['config'] = json.loads(result['config_json'])
                results.append(result)
            return results

    def delete_provider(self, provider_id: int) -> bool:
        """Delete a provider and all associated data."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM email_providers WHERE id = ?", (provider_id,))
            return cursor.rowcount > 0

    # ==================== Folder Methods ====================

    def add_folder(
        self,
        provider_id: int,
        folder_id: str,
        folder_name: str,
        monitored: bool = True
    ) -> int:
        """Add a folder for a provider."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO email_folders (provider_id, folder_id, folder_name, monitored)
                VALUES (?, ?, ?, ?)
            """, (provider_id, folder_id, folder_name, int(monitored)))
            return cursor.lastrowid

    def get_folders(self, provider_id: int, monitored_only: bool = False) -> List[Dict[str, Any]]:
        """Get folders for a provider."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT * FROM email_folders WHERE provider_id = ?"
            if monitored_only:
                query += " AND monitored = 1"
            cursor.execute(query, (provider_id,))
            return [dict(row) for row in cursor.fetchall()]

    def update_folder_sync(self, folder_id: int) -> None:
        """Update folder sync timestamp."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE email_folders SET last_sync = ? WHERE id = ?",
                (datetime.utcnow().isoformat(), folder_id)
            )

    # ==================== Email Methods ====================

    def store_email(
        self,
        provider_id: int,
        folder_db_id: int,
        email: EmailMessage
    ) -> int:
        """Store an email message. Returns email ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Check if email already exists
            cursor.execute(
                "SELECT id FROM emails WHERE provider_id = ? AND message_id = ?",
                (provider_id, email.message_id)
            )
            existing = cursor.fetchone()
            if existing:
                return existing['id']

            cursor.execute("""
                INSERT INTO emails (
                    provider_id, message_id, thread_id, folder_id,
                    from_address, from_name, to_addresses, cc_addresses,
                    subject, body_preview, body_html, body_text,
                    received_at, sent_at, is_read, is_flagged, has_attachments,
                    raw_headers
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                provider_id,
                email.message_id,
                email.thread_id,
                folder_db_id,
                email.from_address,
                email.from_name,
                json.dumps(email.to_addresses),
                json.dumps(email.cc_addresses),
                email.subject,
                email.body_preview[:500] if email.body_preview else None,
                email.body_html,
                email.body_text,
                email.received_at.isoformat() if email.received_at else None,
                email.sent_at.isoformat() if email.sent_at else None,
                int(email.is_read),
                int(email.is_flagged),
                int(email.has_attachments),
                json.dumps(email.raw_headers) if email.raw_headers else None
            ))

            email_id = cursor.lastrowid

            # Store attachments
            for att in email.attachments:
                cursor.execute("""
                    INSERT INTO email_attachments (email_id, attachment_id, filename, content_type, size_bytes)
                    VALUES (?, ?, ?, ?, ?)
                """, (email_id, att.attachment_id, att.filename, att.content_type, att.size_bytes))

            return email_id

    def get_emails(
        self,
        provider_id: Optional[int] = None,
        folder_id: Optional[int] = None,
        category: Optional[str] = None,
        linked_account: Optional[str] = None,
        is_read: Optional[bool] = None,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        search: Optional[str] = None,
        page: int = 1,
        page_size: int = 50
    ) -> Dict[str, Any]:
        """Get emails with filtering and pagination."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            conditions = []
            params = []

            if provider_id is not None:
                conditions.append("e.provider_id = ?")
                params.append(provider_id)

            if folder_id is not None:
                conditions.append("e.folder_id = ?")
                params.append(folder_id)

            if category is not None:
                conditions.append("e.category = ?")
                params.append(category)

            if linked_account is not None:
                conditions.append("e.linked_account = ?")
                params.append(linked_account)

            if is_read is not None:
                conditions.append("e.is_read = ?")
                params.append(int(is_read))

            if from_date is not None:
                conditions.append("e.received_at >= ?")
                params.append(from_date.isoformat())

            if to_date is not None:
                conditions.append("e.received_at <= ?")
                params.append(to_date.isoformat())

            if search:
                conditions.append("(e.subject LIKE ? OR e.from_address LIKE ? OR e.body_preview LIKE ?)")
                search_param = f"%{search}%"
                params.extend([search_param, search_param, search_param])

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            # Get total count
            cursor.execute(f"""
                SELECT COUNT(*) as total FROM emails e WHERE {where_clause}
            """, params)
            total = cursor.fetchone()['total']

            # Get paginated results
            offset = (page - 1) * page_size
            cursor.execute(f"""
                SELECT e.*, p.name as provider_name
                FROM emails e
                JOIN email_providers p ON e.provider_id = p.id
                WHERE {where_clause}
                ORDER BY e.received_at DESC
                LIMIT ? OFFSET ?
            """, params + [page_size, offset])

            emails = []
            for row in cursor.fetchall():
                email = dict(row)
                if email.get('to_addresses'):
                    email['to_addresses'] = json.loads(email['to_addresses'])
                if email.get('cc_addresses'):
                    email['cc_addresses'] = json.loads(email['cc_addresses'])
                emails.append(email)

            return {
                'emails': emails,
                'total': total,
                'page': page,
                'page_size': page_size,
                'total_pages': (total + page_size - 1) // page_size
            }

    def get_email_by_id(self, email_id: int) -> Optional[Dict[str, Any]]:
        """Get single email with full details."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT e.*, p.name as provider_name
                FROM emails e
                JOIN email_providers p ON e.provider_id = p.id
                WHERE e.id = ?
            """, (email_id,))
            row = cursor.fetchone()

            if not row:
                return None

            email = dict(row)
            if email.get('to_addresses'):
                email['to_addresses'] = json.loads(email['to_addresses'])
            if email.get('cc_addresses'):
                email['cc_addresses'] = json.loads(email['cc_addresses'])
            if email.get('raw_headers'):
                email['raw_headers'] = json.loads(email['raw_headers'])

            # Get attachments
            cursor.execute(
                "SELECT * FROM email_attachments WHERE email_id = ?",
                (email_id,)
            )
            email['attachments'] = [dict(row) for row in cursor.fetchall()]

            return email

    def get_unlinked_emails(self) -> List[Dict[str, Any]]:
        """Get emails that haven't been linked to a customer."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, from_address, from_name, subject
                FROM emails
                WHERE linked_account IS NULL
            """)
            return [dict(row) for row in cursor.fetchall()]

    def update_email_category(
        self,
        email_id: int,
        category: str,
        confidence: float,
        reason: Optional[str] = None
    ) -> bool:
        """Update email category from AI classification."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE emails
                SET category = ?, category_confidence = ?, category_reason = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (category, confidence, reason, email_id))
            return cursor.rowcount > 0

    def link_email_to_customer(
        self,
        email_id: int,
        account_code: str,
        linked_by: str = 'manual'
    ) -> bool:
        """Link an email to a customer account."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE emails
                SET linked_account = ?, linked_at = ?, linked_by = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (account_code, datetime.utcnow().isoformat(), linked_by, email_id))
            return cursor.rowcount > 0

    def unlink_email(self, email_id: int) -> bool:
        """Remove customer link from an email."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE emails
                SET linked_account = NULL, linked_at = NULL, linked_by = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (email_id,))
            return cursor.rowcount > 0

    def get_emails_by_customer(self, account_code: str) -> List[Dict[str, Any]]:
        """Get all emails linked to a customer account."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT e.*, p.name as provider_name
                FROM emails e
                JOIN email_providers p ON e.provider_id = p.id
                WHERE e.linked_account = ?
                ORDER BY e.received_at DESC
            """, (account_code,))

            emails = []
            for row in cursor.fetchall():
                email = dict(row)
                if email.get('to_addresses'):
                    email['to_addresses'] = json.loads(email['to_addresses'])
                emails.append(email)
            return emails

    # ==================== Sync Log Methods ====================

    def start_sync_log(self, provider_id: int) -> int:
        """Start a new sync log entry."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO sync_log (provider_id, started_at, status)
                VALUES (?, ?, 'running')
            """, (provider_id, datetime.utcnow().isoformat()))

            # Update provider sync status
            cursor.execute(
                "UPDATE email_providers SET sync_status = 'running' WHERE id = ?",
                (provider_id,)
            )

            return cursor.lastrowid

    def complete_sync_log(
        self,
        log_id: int,
        status: str,
        emails_synced: int = 0,
        error: Optional[str] = None
    ) -> None:
        """Complete a sync log entry."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE sync_log
                SET completed_at = ?, status = ?, emails_synced = ?, error_message = ?
                WHERE id = ?
            """, (datetime.utcnow().isoformat(), status, emails_synced, error, log_id))

            # Get provider_id and update status
            cursor.execute("SELECT provider_id FROM sync_log WHERE id = ?", (log_id,))
            row = cursor.fetchone()
            if row:
                cursor.execute("""
                    UPDATE email_providers
                    SET sync_status = ?, last_sync = ?
                    WHERE id = ?
                """, (status, datetime.utcnow().isoformat(), row['provider_id']))

    def get_sync_history(
        self,
        provider_id: Optional[int] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get sync history."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT * FROM sync_log"
            params = []

            if provider_id is not None:
                query += " WHERE provider_id = ?"
                params.append(provider_id)

            query += " ORDER BY started_at DESC LIMIT ?"
            params.append(limit)

            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    # ==================== Statistics ====================

    def get_category_stats(self) -> Dict[str, int]:
        """Get email count by category."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    COALESCE(category, 'uncategorized') as category,
                    COUNT(*) as count
                FROM emails
                GROUP BY category
            """)
            return {row['category']: row['count'] for row in cursor.fetchall()}

    def get_email_stats(self) -> Dict[str, Any]:
        """Get overall email statistics."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    COUNT(*) as total_emails,
                    SUM(CASE WHEN is_read = 0 THEN 1 ELSE 0 END) as unread_count,
                    SUM(CASE WHEN linked_account IS NOT NULL THEN 1 ELSE 0 END) as linked_count,
                    SUM(CASE WHEN category IS NOT NULL THEN 1 ELSE 0 END) as categorized_count
                FROM emails
            """)
            return dict(cursor.fetchone())

    # ==================== GoCardless Import Tracking ====================

    def record_gocardless_import(
        self,
        target_system: str,
        email_id: Optional[int] = None,
        payout_id: Optional[str] = None,
        source: str = 'email',
        bank_reference: Optional[str] = None,
        gross_amount: Optional[float] = None,
        net_amount: Optional[float] = None,
        gocardless_fees: Optional[float] = None,
        vat_on_fees: Optional[float] = None,
        payment_count: Optional[int] = None,
        payments_json: Optional[str] = None,
        batch_ref: Optional[str] = None,
        imported_by: Optional[str] = None
    ) -> int:
        """
        Record a successful GoCardless import into Opera.

        Only call this AFTER the batch has been successfully imported into Opera SE or Opera 3.
        This marks the payout as processed so it won't appear in future scans.

        Args:
            target_system: 'opera_se' or 'opera3'
            email_id: ID of the email (for email-based imports)
            payout_id: GoCardless payout ID (for API-based imports)
            source: 'email' or 'api'
            bank_reference: GoCardless bank reference (e.g., INTSYSUKLTD-R2VB7P)
            gross_amount: Total gross amount of the batch
            net_amount: Net amount after fees
            gocardless_fees: Total GoCardless fees
            vat_on_fees: VAT on GoCardless fees
            payment_count: Number of payments in the batch
            payments_json: JSON string of payment details
            batch_ref: Opera batch reference
            imported_by: User/system that performed the import

        Returns:
            ID of the import record
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO gocardless_imports
                (email_id, payout_id, source, target_system, bank_reference, gross_amount, net_amount,
                 gocardless_fees, vat_on_fees, payment_count, payments_json, batch_ref, import_date, imported_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                email_id, payout_id, source, target_system, bank_reference, gross_amount, net_amount,
                gocardless_fees, vat_on_fees, payment_count, payments_json, batch_ref,
                datetime.utcnow().isoformat(), imported_by
            ))
            logger.info(f"Recorded GoCardless import: payout_id={payout_id}, ref={bank_reference}, system={target_system}")
            return cursor.lastrowid

    def is_gocardless_payout_imported(self, payout_id: str, target_system: Optional[str] = None) -> bool:
        """
        Check if a GoCardless payout has been imported (by payout_id).

        Args:
            payout_id: GoCardless payout ID
            target_system: Optional - check specific system ('opera_se' or 'opera3')

        Returns:
            True if the payout has been imported
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if target_system:
                cursor.execute(
                    "SELECT 1 FROM gocardless_imports WHERE payout_id = ? AND target_system = ?",
                    (payout_id, target_system)
                )
            else:
                cursor.execute(
                    "SELECT 1 FROM gocardless_imports WHERE payout_id = ?",
                    (payout_id,)
                )
            return cursor.fetchone() is not None

    def is_gocardless_reference_imported(self, bank_reference: str, target_system: Optional[str] = None) -> bool:
        """
        Check if a GoCardless payout has been imported (by bank reference).

        Args:
            bank_reference: GoCardless bank reference (e.g., INTSYSUKLTD-R2VB7P)
            target_system: Optional - check specific system ('opera_se' or 'opera3')

        Returns:
            True if the payout has been imported
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if target_system:
                cursor.execute(
                    "SELECT 1 FROM gocardless_imports WHERE bank_reference = ? AND target_system = ?",
                    (bank_reference, target_system)
                )
            else:
                cursor.execute(
                    "SELECT 1 FROM gocardless_imports WHERE bank_reference = ?",
                    (bank_reference,)
                )
            return cursor.fetchone() is not None

    def is_gocardless_imported(self, email_id: int, target_system: Optional[str] = None) -> bool:
        """
        Check if a GoCardless email has been imported.

        Args:
            email_id: ID of the email to check
            target_system: Optional - check specific system ('opera_se' or 'opera3')
                          If None, checks if imported to any system

        Returns:
            True if the email has been imported
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if target_system:
                cursor.execute(
                    "SELECT 1 FROM gocardless_imports WHERE email_id = ? AND target_system = ?",
                    (email_id, target_system)
                )
            else:
                cursor.execute(
                    "SELECT 1 FROM gocardless_imports WHERE email_id = ?",
                    (email_id,)
                )
            return cursor.fetchone() is not None

    def get_imported_gocardless_email_ids(self, target_system: Optional[str] = None) -> List[int]:
        """
        Get list of email IDs that have been imported.

        Args:
            target_system: Optional - filter by system ('opera_se' or 'opera3')

        Returns:
            List of email IDs that have been imported
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if target_system:
                cursor.execute(
                    "SELECT DISTINCT email_id FROM gocardless_imports WHERE target_system = ?",
                    (target_system,)
                )
            else:
                cursor.execute("SELECT DISTINCT email_id FROM gocardless_imports")
            return [row['email_id'] for row in cursor.fetchall()]

    def get_gocardless_import_history(
        self,
        limit: int = 50,
        target_system: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get history of GoCardless imports.

        Args:
            limit: Maximum records to return
            target_system: Filter by Opera SE or Opera 3
            from_date: Filter from date (YYYY-MM-DD)
            to_date: Filter to date (YYYY-MM-DD)

        Returns list of import records with email details.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            query = """
                SELECT gi.*, e.subject as email_subject, e.received_at as email_date
                FROM gocardless_imports gi
                LEFT JOIN emails e ON gi.email_id = e.id
            """
            params = []
            conditions = []

            if target_system:
                conditions.append("gi.target_system = ?")
                params.append(target_system)

            if from_date:
                conditions.append("date(gi.import_date) >= date(?)")
                params.append(from_date)

            if to_date:
                conditions.append("date(gi.import_date) <= date(?)")
                params.append(to_date)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY gi.import_date DESC LIMIT ?"
            params.append(limit)

            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def clear_gocardless_import_history(
        self,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> int:
        """
        Clear GoCardless import history within a date range.

        Args:
            from_date: Clear from date (YYYY-MM-DD), inclusive
            to_date: Clear to date (YYYY-MM-DD), inclusive

        If no dates specified, clears ALL history.
        Returns number of records deleted.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            conditions = []
            params = []

            if from_date:
                conditions.append("date(import_date) >= date(?)")
                params.append(from_date)

            if to_date:
                conditions.append("date(import_date) <= date(?)")
                params.append(to_date)

            if conditions:
                query = f"DELETE FROM gocardless_imports WHERE {' AND '.join(conditions)}"
            else:
                query = "DELETE FROM gocardless_imports"

            cursor.execute(query, params)
            deleted_count = cursor.rowcount
            conn.commit()

            logger.info(f"Cleared {deleted_count} GoCardless import history records (from={from_date}, to={to_date})")
            return deleted_count

    def delete_gocardless_import_record(self, record_id: int) -> bool:
        """
        Delete a single import history record by ID.

        Used to allow re-importing a payout that was previously imported.
        Returns True if a record was deleted, False if not found.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM gocardless_imports WHERE id = ?", (record_id,))
            deleted = cursor.rowcount > 0
            conn.commit()
            if deleted:
                logger.info(f"Deleted GoCardless import record {record_id}")
            return deleted

    # ==================== Bank Statement Import Tracking ====================

    def record_bank_statement_import(
        self,
        bank_code: str,
        filename: str,
        transactions_imported: int,
        source: str = 'email',
        target_system: str = 'opera_se',
        email_id: Optional[int] = None,
        attachment_id: Optional[str] = None,
        total_receipts: float = 0,
        total_payments: float = 0,
        imported_by: Optional[str] = None
    ) -> int:
        """
        Record a successful bank statement import.

        Supports both email-based and file-based imports (mirroring GoCardless functionality).

        Only call this AFTER the transactions have been successfully imported into Opera.
        This marks the statement as processed so it won't appear in future scans (for emails).

        Args:
            bank_code: Opera bank account code used for import
            filename: Original filename of the statement
            transactions_imported: Number of transactions imported
            source: 'email' or 'file'
            target_system: 'opera_se' or 'opera3'
            email_id: ID of the email (if email import)
            attachment_id: ID of the attachment (if email import)
            total_receipts: Total receipts amount imported
            total_payments: Total payments amount imported
            imported_by: User/system that performed the import

        Returns:
            ID of the import record
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO bank_statement_imports
                (email_id, attachment_id, source, bank_code, filename, total_receipts, total_payments,
                 transactions_imported, target_system, import_date, imported_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                email_id, attachment_id, source, bank_code, filename,
                total_receipts, total_payments, transactions_imported,
                target_system, datetime.utcnow().isoformat(), imported_by
            ))
            logger.info(f"Recorded bank statement import: source={source}, bank={bank_code}, file={filename}, txns={transactions_imported}")
            return cursor.lastrowid

    def is_bank_statement_processed(
        self,
        email_id: int,
        attachment_id: str,
        bank_code: Optional[str] = None
    ) -> bool:
        """
        Check if a bank statement attachment has been imported.

        Args:
            email_id: ID of the email to check
            attachment_id: ID of the attachment to check
            bank_code: Optional - check specific bank code only

        Returns:
            True if the attachment has been imported
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if bank_code:
                cursor.execute(
                    "SELECT 1 FROM bank_statement_imports WHERE email_id = ? AND attachment_id = ? AND bank_code = ?",
                    (email_id, attachment_id, bank_code)
                )
            else:
                cursor.execute(
                    "SELECT 1 FROM bank_statement_imports WHERE email_id = ? AND attachment_id = ?",
                    (email_id, attachment_id)
                )
            return cursor.fetchone() is not None

    def get_processed_bank_statement_attachments(
        self,
        bank_code: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get list of attachment IDs that have been imported.

        Args:
            bank_code: Optional - filter by bank code

        Returns:
            List of dicts with email_id and attachment_id
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if bank_code:
                cursor.execute(
                    "SELECT DISTINCT email_id, attachment_id FROM bank_statement_imports WHERE bank_code = ?",
                    (bank_code,)
                )
            else:
                cursor.execute("SELECT DISTINCT email_id, attachment_id FROM bank_statement_imports")
            return [{'email_id': row['email_id'], 'attachment_id': row['attachment_id']} for row in cursor.fetchall()]

    def get_bank_statement_import_history(
        self,
        bank_code: Optional[str] = None,
        target_system: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get history of bank statement imports.

        Returns list of import records with email details (for email imports).
        Supports filtering by bank_code, target_system, and date range.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            query = """
                SELECT bsi.*, e.subject as email_subject, e.received_at as email_date,
                       e.from_address as email_from
                FROM bank_statement_imports bsi
                LEFT JOIN emails e ON bsi.email_id = e.id
            """
            conditions = []
            params = []

            if bank_code:
                conditions.append("bsi.bank_code = ?")
                params.append(bank_code)

            if target_system:
                conditions.append("bsi.target_system = ?")
                params.append(target_system)

            if from_date:
                conditions.append("date(bsi.import_date) >= date(?)")
                params.append(from_date)

            if to_date:
                conditions.append("date(bsi.import_date) <= date(?)")
                params.append(to_date)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY bsi.import_date DESC LIMIT ?"
            params.append(limit)

            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def delete_bank_statement_import_record(self, record_id: int) -> bool:
        """
        Delete a single bank statement import record by ID.

        Used to allow re-importing a statement that was previously imported.
        This removes the tracking record so the statement can be imported again.
        Does not affect Opera data - only the import tracking.

        Returns True if a record was deleted, False if not found.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM bank_statement_imports WHERE id = ?", (record_id,))
            deleted = cursor.rowcount > 0
            conn.commit()
            if deleted:
                logger.info(f"Deleted bank statement import record {record_id}")
            return deleted

    def clear_bank_statement_import_history(
        self,
        bank_code: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> int:
        """
        Clear bank statement import history within optional filters.

        Args:
            bank_code: Filter by bank code
            from_date: Clear from date (YYYY-MM-DD), inclusive
            to_date: Clear to date (YYYY-MM-DD), inclusive

        Returns number of records deleted.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            conditions = []
            params = []

            if bank_code:
                conditions.append("bank_code = ?")
                params.append(bank_code)

            if from_date:
                conditions.append("date(import_date) >= date(?)")
                params.append(from_date)

            if to_date:
                conditions.append("date(import_date) <= date(?)")
                params.append(to_date)

            if conditions:
                query = f"DELETE FROM bank_statement_imports WHERE {' AND '.join(conditions)}"
            else:
                query = "DELETE FROM bank_statement_imports"

            cursor.execute(query, params)
            deleted_count = cursor.rowcount
            conn.commit()

            logger.info(f"Cleared {deleted_count} bank statement import history records (bank={bank_code}, from={from_date}, to={to_date})")
            return deleted_count
