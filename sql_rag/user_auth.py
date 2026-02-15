"""
User Authentication Module for SQL RAG Application.

Handles user management, authentication, session management, and permissions.
Uses a separate SQLite database (users.db) for user data - NOT the Opera database.
"""

import sqlite3
import hashlib
import secrets
import os
import base64
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import logging

logger = logging.getLogger(__name__)


class UserAuth:
    """User authentication and authorization manager."""

    # Database path - separate from Opera database
    DB_PATH = Path(__file__).parent.parent / "users.db"

    # Session expiry in hours
    SESSION_EXPIRY_HOURS = 24

    # Encryption key for recoverable passwords (admin viewing)
    # In production, this should be in environment variables
    _ENCRYPTION_KEY = b'SqlRagUserAuth2024SecretKey!'

    # Module definitions for permissions
    MODULES = [
        'cashbook',      # Bank Reconciliation, GoCardless Import
        'payroll',       # Pension Export, Parameters
        'ap_automation', # Full AP Automation menu
        'utilities',     # Balance Check, User Activity
        'development',   # Opera SE, Archive
        'administration' # Company, Projects, Lock Monitor, Settings
    ]

    # Mapping from Opera NavGroups to SQL RAG modules
    # Opera controls WHAT data the user can see in Opera (and therefore what they should see in SQL RAG)
    # If user doesn't have NavGroupPayrollManagement in Opera, they shouldn't see payroll data in SQL RAG
    OPERA_NAVGROUP_TO_MODULE = {
        'NavGroupFinancials': 'cashbook',           # Financials = Cashbook, Bank Reconciliation
        'NavGroupPayrollManagement': 'payroll',     # Payroll Management = Payroll features
        'NavGroupSCM': 'ap_automation',             # Supply Chain Management = AP Automation
        'NavGroupReporter': 'utilities',            # Reporter = Utilities, Balance Check
        'NavGroupAdministration': 'administration', # Administration = Admin settings
        # NavGroupFavourites is user-specific UI, not a permission
    }

    def __init__(self):
        """Initialize the UserAuth system."""
        self._init_db()
        self._ensure_admin_user()

    def _init_db(self):
        """Create database tables if they don't exist."""
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Users table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    display_name TEXT,
                    email TEXT,
                    is_admin INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_login TEXT,
                    created_by TEXT,
                    default_company TEXT
                )
            ''')

            # Add default_company column if it doesn't exist (migration for existing DBs)
            cursor.execute("PRAGMA table_info(users)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'default_company' not in columns:
                cursor.execute('ALTER TABLE users ADD COLUMN default_company TEXT')

            # Add password_encrypted column for admin recovery
            if 'password_encrypted' not in columns:
                cursor.execute('ALTER TABLE users ADD COLUMN password_encrypted TEXT')

            # Module permissions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_permissions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    module TEXT NOT NULL,
                    has_access INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    UNIQUE(user_id, module)
                )
            ''')

            # Sessions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    token TEXT UNIQUE NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    expires_at TEXT NOT NULL,
                    license_id INTEGER,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (license_id) REFERENCES licenses(id)
                )
            ''')

            # Licenses table - for client licensing
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS licenses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_name TEXT UNIQUE NOT NULL,
                    opera_version TEXT NOT NULL DEFAULT 'SE',
                    max_users INTEGER DEFAULT 5,
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    notes TEXT
                )
            ''')

            # Add license_id to sessions if not exists (migration)
            cursor.execute("PRAGMA table_info(sessions)")
            session_columns = [col[1] for col in cursor.fetchall()]
            if 'license_id' not in session_columns:
                cursor.execute('ALTER TABLE sessions ADD COLUMN license_id INTEGER')

            # User companies table - which companies each user can access
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    company_id TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    UNIQUE(user_id, company_id)
                )
            ''')

            conn.commit()
            logger.info(f"User database initialized at {self.DB_PATH}")
        finally:
            conn.close()

    def _hash_password(self, password: str) -> str:
        """Hash a password using PBKDF2 with SHA256."""
        # Use a random salt
        salt = secrets.token_hex(32)
        # Hash with PBKDF2
        hash_obj = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt.encode('utf-8'),
            100000  # iterations
        )
        return f"{salt}:{hash_obj.hex()}"

    def _encrypt_password(self, password: str) -> str:
        """Encrypt password for admin recovery (simple XOR + base64)."""
        key = self._ENCRYPTION_KEY
        password_bytes = password.encode('utf-8')
        # XOR with repeating key
        encrypted = bytes([password_bytes[i] ^ key[i % len(key)] for i in range(len(password_bytes))])
        return base64.b64encode(encrypted).decode('utf-8')

    def _decrypt_password(self, encrypted: str) -> str:
        """Decrypt password for admin viewing."""
        try:
            key = self._ENCRYPTION_KEY
            encrypted_bytes = base64.b64decode(encrypted.encode('utf-8'))
            # XOR with repeating key (same operation decrypts)
            decrypted = bytes([encrypted_bytes[i] ^ key[i % len(key)] for i in range(len(encrypted_bytes))])
            return decrypted.decode('utf-8')
        except Exception as e:
            logger.error(f"Password decryption error: {e}")
            return "***"

    def _verify_password(self, password: str, password_hash: str) -> bool:
        """Verify a password against its hash."""
        try:
            salt, stored_hash = password_hash.split(':')
            hash_obj = hashlib.pbkdf2_hmac(
                'sha256',
                password.encode('utf-8'),
                salt.encode('utf-8'),
                100000
            )
            return hash_obj.hex() == stored_hash
        except Exception as e:
            logger.error(f"Password verification error: {e}")
            return False

    def _ensure_admin_user(self):
        """Create default admin user if not exists."""
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Check if admin user exists
            cursor.execute('SELECT id, password_encrypted FROM users WHERE username = ?', ('admin',))
            row = cursor.fetchone()
            if row is None:
                # Create admin user with password "Harry"
                password_hash = self._hash_password('Harry')
                password_encrypted = self._encrypt_password('Harry')
                cursor.execute('''
                    INSERT INTO users (username, password_hash, password_encrypted, display_name, is_admin, is_active, created_by)
                    VALUES (?, ?, ?, ?, 1, 1, 'system')
                ''', ('admin', password_hash, password_encrypted, 'Administrator'))
            elif row[1] is None:
                # Update existing admin user with encrypted password if missing
                password_encrypted = self._encrypt_password('Harry')
                cursor.execute('UPDATE users SET password_encrypted = ? WHERE username = ?', (password_encrypted, 'admin'))

                user_id = cursor.lastrowid

                # Grant all permissions to admin
                for module in self.MODULES:
                    cursor.execute('''
                        INSERT INTO user_permissions (user_id, module, has_access)
                        VALUES (?, ?, 1)
                    ''', (user_id, module))

                conn.commit()
                logger.info("Default admin user created (username: admin, password: Harry)")
        finally:
            conn.close()

    def sync_user_from_opera(
        self,
        opera_username: str,
        display_name: Optional[str] = None,
        email: Optional[str] = None,
        is_manager: bool = False,
        is_active: bool = True,
        preferred_company: Optional[str] = None,
        opera_permissions: Optional[Dict[str, bool]] = None,
        company_access: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Sync a user from Opera. Creates if not exists, updates Opera-controlled fields if exists.

        Opera is the master for: username, display_name, email, is_admin (manager), is_active,
                                  default_company, company_access
        Opera NavGroup permissions determine which SQL RAG modules the user can access.
        SQL RAG manages: password (local)

        Args:
            opera_username: Opera user ID
            display_name: Full name from Opera
            email: Email address
            is_manager: True if Opera manager flag set
            is_active: True if user is active in Opera (state != 2)
            preferred_company: Default company from Opera
            opera_permissions: Dict mapping SQL RAG module names to access booleans,
                               derived from Opera's seqnavgrps table

        Returns the user dict.
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Check if user exists (case-insensitive)
            cursor.execute('SELECT id FROM users WHERE LOWER(username) = LOWER(?)', (opera_username,))
            row = cursor.fetchone()

            # Build permissions from Opera NavGroups
            # If opera_permissions is provided, use it; otherwise fall back to defaults
            if opera_permissions is not None:
                # Opera permissions provided - use them
                # Managers get all permissions regardless
                if is_manager:
                    final_permissions = {module: True for module in self.MODULES}
                else:
                    # Start with opera_permissions for mapped modules
                    final_permissions = {module: False for module in self.MODULES}
                    for module, has_access in opera_permissions.items():
                        if module in self.MODULES:
                            final_permissions[module] = has_access
                    # Development module defaults to managers only
                    final_permissions['development'] = False
            else:
                # No Opera permissions provided - use defaults based on manager status
                final_permissions = {
                    'cashbook': True,
                    'payroll': is_manager,
                    'ap_automation': True,
                    'utilities': True,
                    'development': is_manager,
                    'administration': is_manager
                }

            if row:
                # Update Opera-controlled fields
                # Opera is the master for default_company - sync it from Opera
                user_id = row[0]
                cursor.execute('''
                    UPDATE users SET
                        display_name = COALESCE(?, display_name),
                        email = COALESCE(?, email),
                        is_admin = ?,
                        is_active = ?,
                        default_company = COALESCE(?, default_company)
                    WHERE id = ?
                ''', (display_name, email, 1 if is_manager else 0, 1 if is_active else 0,
                      preferred_company, user_id))

                # Update permissions from Opera NavGroups
                for module, has_access in final_permissions.items():
                    cursor.execute('''
                        INSERT OR REPLACE INTO user_permissions (user_id, module, has_access)
                        VALUES (?, ?, ?)
                    ''', (user_id, module, 1 if has_access else 0))

                # Update company access if provided from Opera
                if company_access is not None:
                    # Clear existing company access
                    cursor.execute('DELETE FROM user_companies WHERE user_id = ?', (user_id,))
                    # Insert new company access
                    for company_id in company_access:
                        cursor.execute('''
                            INSERT INTO user_companies (user_id, company_id)
                            VALUES (?, ?)
                        ''', (user_id, company_id))

                conn.commit()
                logger.info(f"Synced Opera user '{opera_username}' - updated with permissions: {final_permissions}, companies: {company_access}")
            else:
                # Create new user with default password = username
                password_hash = self._hash_password(opera_username)
                password_encrypted = self._encrypt_password(opera_username)

                cursor.execute('''
                    INSERT INTO users (username, password_hash, password_encrypted, display_name,
                                      email, is_admin, is_active, created_by, default_company)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'opera_sync', ?)
                ''', (opera_username, password_hash, password_encrypted, display_name or opera_username,
                      email, 1 if is_manager else 0, 1 if is_active else 0, preferred_company))

                user_id = cursor.lastrowid

                # Set permissions from Opera NavGroups
                for module, has_access in final_permissions.items():
                    cursor.execute('''
                        INSERT INTO user_permissions (user_id, module, has_access)
                        VALUES (?, ?, ?)
                    ''', (user_id, module, 1 if has_access else 0))

                # Set company access if provided from Opera
                if company_access is not None:
                    for company_id in company_access:
                        cursor.execute('''
                            INSERT INTO user_companies (user_id, company_id)
                            VALUES (?, ?)
                        ''', (user_id, company_id))

                conn.commit()
                logger.info(f"Synced Opera user '{opera_username}' - created new with permissions: {final_permissions}, companies: {company_access}")

            return self.get_user(user_id)
        finally:
            conn.close()

    @staticmethod
    def map_opera_navgroups_to_permissions(navgroups: Dict[str, bool]) -> Dict[str, bool]:
        """
        Map Opera NavGroup access to SQL RAG module permissions.

        Args:
            navgroups: Dict mapping Opera NavGroup names (e.g., 'NavGroupFinancials')
                      to access booleans

        Returns:
            Dict mapping SQL RAG module names to access booleans
        """
        permissions = {}
        for navgroup, has_access in navgroups.items():
            module = UserAuth.OPERA_NAVGROUP_TO_MODULE.get(navgroup)
            if module:
                permissions[module] = has_access
        return permissions

    def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticate a user with username and password.

        Returns user dict if successful, None if failed.

        Note: Call sync_user_from_opera() before this if using Opera as user master.
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Case-insensitive username lookup
            cursor.execute('''
                SELECT id, username, password_hash, display_name, email, is_admin, is_active, default_company
                FROM users WHERE LOWER(username) = LOWER(?)
            ''', (username,))

            row = cursor.fetchone()
            if row is None:
                logger.warning(f"Authentication failed: user '{username}' not found")
                return None

            user_id, username, password_hash, display_name, email, is_admin, is_active, default_company = row

            if not is_active:
                logger.warning(f"Authentication failed: user '{username}' is inactive")
                return None

            if not self._verify_password(password, password_hash):
                logger.warning(f"Authentication failed: incorrect password for '{username}'")
                return None

            # Update last login
            cursor.execute('''
                UPDATE users SET last_login = ? WHERE id = ?
            ''', (datetime.utcnow().isoformat(), user_id))
            conn.commit()

            logger.info(f"User '{username}' authenticated successfully")

            return {
                'id': user_id,
                'username': username,
                'display_name': display_name or username,
                'email': email,
                'is_admin': bool(is_admin),
                'default_company': default_company
            }
        finally:
            conn.close()

    def create_session(self, user_id: int) -> str:
        """
        Create a new session for a user.

        Returns the session token.
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Generate secure token
            token = secrets.token_urlsafe(32)

            # Calculate expiry
            expires_at = datetime.utcnow() + timedelta(hours=self.SESSION_EXPIRY_HOURS)

            cursor.execute('''
                INSERT INTO sessions (user_id, token, expires_at)
                VALUES (?, ?, ?)
            ''', (user_id, token, expires_at.isoformat()))

            conn.commit()

            # Clean up expired sessions
            self._cleanup_expired_sessions()

            return token
        finally:
            conn.close()

    def validate_session(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Validate a session token.

        Returns user dict if valid, None if invalid or expired.
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT s.user_id, s.expires_at, u.username, u.display_name, u.email, u.is_admin, u.is_active, u.default_company
                FROM sessions s
                JOIN users u ON s.user_id = u.id
                WHERE s.token = ?
            ''', (token,))

            row = cursor.fetchone()
            if row is None:
                return None

            user_id, expires_at, username, display_name, email, is_admin, is_active, default_company = row

            # Check expiry
            if datetime.fromisoformat(expires_at) < datetime.utcnow():
                # Session expired, delete it
                cursor.execute('DELETE FROM sessions WHERE token = ?', (token,))
                conn.commit()
                return None

            # Check user still active
            if not is_active:
                return None

            return {
                'id': user_id,
                'username': username,
                'display_name': display_name or username,
                'email': email,
                'is_admin': bool(is_admin),
                'default_company': default_company
            }
        finally:
            conn.close()

    def invalidate_session(self, token: str) -> bool:
        """
        Invalidate (delete) a session token.

        Returns True if session was found and deleted.
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM sessions WHERE token = ?', (token,))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def _cleanup_expired_sessions(self):
        """Remove expired sessions from the database."""
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM sessions WHERE expires_at < ?
            ''', (datetime.utcnow().isoformat(),))
            deleted = cursor.rowcount
            conn.commit()
            if deleted > 0:
                logger.info(f"Cleaned up {deleted} expired sessions")
        finally:
            conn.close()

    def get_user_permissions(self, user_id: int) -> Dict[str, bool]:
        """
        Get all module permissions for a user.

        Returns dict of {module: has_access}.
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Initialize all modules to False
            permissions = {module: False for module in self.MODULES}

            cursor.execute('''
                SELECT module, has_access FROM user_permissions WHERE user_id = ?
            ''', (user_id,))

            for module, has_access in cursor.fetchall():
                if module in permissions:
                    permissions[module] = bool(has_access)

            return permissions
        finally:
            conn.close()

    def create_user(
        self,
        username: str,
        password: str,
        display_name: Optional[str] = None,
        email: Optional[str] = None,
        is_admin: bool = False,
        permissions: Optional[Dict[str, bool]] = None,
        created_by: Optional[str] = None,
        default_company: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new user.

        Returns the created user dict.
        Raises ValueError if username already exists.
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Check if username exists
            cursor.execute('SELECT id FROM users WHERE username = ?', (username,))
            if cursor.fetchone() is not None:
                raise ValueError(f"Username '{username}' already exists")

            # Hash password and encrypt for admin recovery
            password_hash = self._hash_password(password)
            password_encrypted = self._encrypt_password(password)

            # Insert user
            cursor.execute('''
                INSERT INTO users (username, password_hash, password_encrypted, display_name, email, is_admin, is_active, created_by, default_company)
                VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
            ''', (username, password_hash, password_encrypted, display_name, email, 1 if is_admin else 0, created_by, default_company))

            user_id = cursor.lastrowid

            # Set permissions
            if permissions is None:
                permissions = {}

            # If admin, grant all permissions
            if is_admin:
                permissions = {module: True for module in self.MODULES}

            for module in self.MODULES:
                has_access = permissions.get(module, False)
                cursor.execute('''
                    INSERT INTO user_permissions (user_id, module, has_access)
                    VALUES (?, ?, ?)
                ''', (user_id, module, 1 if has_access else 0))

            conn.commit()

            logger.info(f"User '{username}' created by {created_by or 'unknown'}")

            return {
                'id': user_id,
                'username': username,
                'display_name': display_name or username,
                'email': email,
                'is_admin': is_admin,
                'is_active': True,
                'default_company': default_company
            }
        finally:
            conn.close()

    def update_user(
        self,
        user_id: int,
        username: Optional[str] = None,
        password: Optional[str] = None,
        display_name: Optional[str] = None,
        email: Optional[str] = None,
        is_admin: Optional[bool] = None,
        is_active: Optional[bool] = None,
        permissions: Optional[Dict[str, bool]] = None,
        default_company: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update an existing user.

        Returns the updated user dict.
        Raises ValueError if user not found or username conflict.
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Check user exists
            cursor.execute('SELECT id, username FROM users WHERE id = ?', (user_id,))
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"User with id {user_id} not found")

            current_username = row[1]

            # Check username uniqueness if changing
            if username is not None and username != current_username:
                cursor.execute('SELECT id FROM users WHERE username = ? AND id != ?', (username, user_id))
                if cursor.fetchone() is not None:
                    raise ValueError(f"Username '{username}' already exists")

            # Build update query
            updates = []
            params = []

            if username is not None:
                updates.append('username = ?')
                params.append(username)

            if password is not None:
                updates.append('password_hash = ?')
                params.append(self._hash_password(password))
                updates.append('password_encrypted = ?')
                params.append(self._encrypt_password(password))

            if display_name is not None:
                updates.append('display_name = ?')
                params.append(display_name)

            if email is not None:
                updates.append('email = ?')
                params.append(email)

            if is_admin is not None:
                updates.append('is_admin = ?')
                params.append(1 if is_admin else 0)

            if is_active is not None:
                updates.append('is_active = ?')
                params.append(1 if is_active else 0)

            # Handle default_company - update if provided
            if default_company is not None:
                updates.append('default_company = ?')
                params.append(default_company if default_company else None)

            if updates:
                params.append(user_id)
                cursor.execute(f'''
                    UPDATE users SET {', '.join(updates)} WHERE id = ?
                ''', params)

            # Update permissions if provided
            if permissions is not None:
                # If making admin, grant all permissions
                if is_admin:
                    permissions = {module: True for module in self.MODULES}

                for module, has_access in permissions.items():
                    if module in self.MODULES:
                        cursor.execute('''
                            INSERT OR REPLACE INTO user_permissions (user_id, module, has_access)
                            VALUES (?, ?, ?)
                        ''', (user_id, module, 1 if has_access else 0))

            conn.commit()

            # Fetch and return updated user
            cursor.execute('''
                SELECT id, username, display_name, email, is_admin, is_active, default_company
                FROM users WHERE id = ?
            ''', (user_id,))
            row = cursor.fetchone()

            return {
                'id': row[0],
                'username': row[1],
                'display_name': row[2] or row[1],
                'email': row[3],
                'is_admin': bool(row[4]),
                'is_active': bool(row[5]),
                'default_company': row[6]
            }
        finally:
            conn.close()

    def delete_user(self, user_id: int) -> bool:
        """
        Delete a user (soft delete - sets is_active to 0).

        Returns True if user was found and deactivated.
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Don't allow deleting the last admin
            cursor.execute('SELECT COUNT(*) FROM users WHERE is_admin = 1 AND is_active = 1')
            admin_count = cursor.fetchone()[0]

            cursor.execute('SELECT is_admin FROM users WHERE id = ?', (user_id,))
            row = cursor.fetchone()
            if row is None:
                return False

            if row[0] and admin_count <= 1:
                raise ValueError("Cannot deactivate the last admin user")

            cursor.execute('UPDATE users SET is_active = 0 WHERE id = ?', (user_id,))
            conn.commit()

            # Invalidate all sessions for this user
            cursor.execute('DELETE FROM sessions WHERE user_id = ?', (user_id,))
            conn.commit()

            return cursor.rowcount > 0
        finally:
            conn.close()

    def list_users(self) -> List[Dict[str, Any]]:
        """
        List all users (without password hashes).

        Returns list of user dicts.
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT id, username, display_name, email, is_admin, is_active, created_at, last_login, created_by, default_company
                FROM users
                ORDER BY username
            ''')

            users = []
            for row in cursor.fetchall():
                user_id = row[0]
                users.append({
                    'id': user_id,
                    'username': row[1],
                    'display_name': row[2] or row[1],
                    'email': row[3],
                    'is_admin': bool(row[4]),
                    'is_active': bool(row[5]),
                    'created_at': row[6],
                    'last_login': row[7],
                    'created_by': row[8],
                    'default_company': row[9],
                    'permissions': self.get_user_permissions(user_id),
                    'company_access': self.get_user_companies(user_id)
                })

            return users
        finally:
            conn.close()

    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a single user by ID.

        Returns user dict or None if not found.
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT id, username, display_name, email, is_admin, is_active, created_at, last_login, created_by, default_company
                FROM users WHERE id = ?
            ''', (user_id,))

            row = cursor.fetchone()
            if row is None:
                return None

            return {
                'id': row[0],
                'username': row[1],
                'display_name': row[2] or row[1],
                'email': row[3],
                'is_admin': bool(row[4]),
                'is_active': bool(row[5]),
                'created_at': row[6],
                'last_login': row[7],
                'created_by': row[8],
                'default_company': row[9],
                'permissions': self.get_user_permissions(row[0])
            }
        finally:
            conn.close()

    def get_user_password(self, user_id: int) -> Optional[str]:
        """
        Get decrypted password for a user (admin function only).

        Returns the decrypted password or None if not found/unavailable.
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            cursor.execute('SELECT password_encrypted FROM users WHERE id = ?', (user_id,))
            row = cursor.fetchone()

            if row is None or row[0] is None:
                return None

            return self._decrypt_password(row[0])
        finally:
            conn.close()

    # ============ License Management ============

    def create_license(
        self,
        client_name: str,
        opera_version: str = 'SE',
        max_users: int = 5,
        notes: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new client license.

        Args:
            client_name: Unique client/company name
            opera_version: 'SE' for Opera SQL SE, '3' for Opera 3 (FoxPro)
            max_users: Maximum concurrent users allowed
            notes: Optional notes about the license

        Returns the created license dict.
        Raises ValueError if client_name already exists.
        """
        if opera_version not in ('SE', '3'):
            raise ValueError("opera_version must be 'SE' or '3'")

        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Check if client name exists
            cursor.execute('SELECT id FROM licenses WHERE client_name = ?', (client_name,))
            if cursor.fetchone() is not None:
                raise ValueError(f"License for '{client_name}' already exists")

            cursor.execute('''
                INSERT INTO licenses (client_name, opera_version, max_users, notes)
                VALUES (?, ?, ?, ?)
            ''', (client_name, opera_version, max_users, notes))

            license_id = cursor.lastrowid
            conn.commit()

            logger.info(f"License created for client '{client_name}' (Opera {opera_version})")

            return {
                'id': license_id,
                'client_name': client_name,
                'opera_version': opera_version,
                'max_users': max_users,
                'is_active': True,
                'notes': notes
            }
        finally:
            conn.close()

    def update_license(
        self,
        license_id: int,
        client_name: Optional[str] = None,
        opera_version: Optional[str] = None,
        max_users: Optional[int] = None,
        is_active: Optional[bool] = None,
        notes: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update an existing license."""
        if opera_version is not None and opera_version not in ('SE', '3'):
            raise ValueError("opera_version must be 'SE' or '3'")

        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Check license exists
            cursor.execute('SELECT id FROM licenses WHERE id = ?', (license_id,))
            if cursor.fetchone() is None:
                raise ValueError(f"License with id {license_id} not found")

            # Check client name uniqueness if changing
            if client_name is not None:
                cursor.execute('SELECT id FROM licenses WHERE client_name = ? AND id != ?', (client_name, license_id))
                if cursor.fetchone() is not None:
                    raise ValueError(f"License for '{client_name}' already exists")

            # Build update query
            updates = []
            params = []

            if client_name is not None:
                updates.append('client_name = ?')
                params.append(client_name)

            if opera_version is not None:
                updates.append('opera_version = ?')
                params.append(opera_version)

            if max_users is not None:
                updates.append('max_users = ?')
                params.append(max_users)

            if is_active is not None:
                updates.append('is_active = ?')
                params.append(1 if is_active else 0)

            if notes is not None:
                updates.append('notes = ?')
                params.append(notes)

            if updates:
                params.append(license_id)
                cursor.execute(f'''
                    UPDATE licenses SET {', '.join(updates)} WHERE id = ?
                ''', params)
                conn.commit()

            # Return updated license
            return self.get_license(license_id)
        finally:
            conn.close()

    def get_license(self, license_id: int) -> Optional[Dict[str, Any]]:
        """Get a license by ID."""
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT id, client_name, opera_version, max_users, is_active, created_at, notes
                FROM licenses WHERE id = ?
            ''', (license_id,))

            row = cursor.fetchone()
            if row is None:
                return None

            return {
                'id': row[0],
                'client_name': row[1],
                'opera_version': row[2],
                'max_users': row[3],
                'is_active': bool(row[4]),
                'created_at': row[5],
                'notes': row[6]
            }
        finally:
            conn.close()

    def list_licenses(self, active_only: bool = False) -> List[Dict[str, Any]]:
        """List all licenses."""
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            query = '''
                SELECT id, client_name, opera_version, max_users, is_active, created_at, notes
                FROM licenses
            '''
            if active_only:
                query += ' WHERE is_active = 1'
            query += ' ORDER BY client_name'

            cursor.execute(query)

            licenses = []
            for row in cursor.fetchall():
                licenses.append({
                    'id': row[0],
                    'client_name': row[1],
                    'opera_version': row[2],
                    'max_users': row[3],
                    'is_active': bool(row[4]),
                    'created_at': row[5],
                    'notes': row[6]
                })

            return licenses
        finally:
            conn.close()

    def delete_license(self, license_id: int) -> bool:
        """Deactivate a license (soft delete)."""
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            cursor.execute('UPDATE licenses SET is_active = 0 WHERE id = ?', (license_id,))
            conn.commit()

            return cursor.rowcount > 0
        finally:
            conn.close()

    def get_active_session_count(self, license_id: int) -> int:
        """Get count of active sessions for a license."""
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT COUNT(*) FROM sessions
                WHERE license_id = ? AND expires_at > ?
            ''', (license_id, datetime.utcnow().isoformat()))

            return cursor.fetchone()[0]
        finally:
            conn.close()

    def create_session_with_license(self, user_id: int, license_id: int) -> str:
        """
        Create a new session for a user with a specific license.

        Returns the session token.
        Raises ValueError if license user limit is exceeded.
        """
        # Check license exists and is active
        license_data = self.get_license(license_id)
        if not license_data:
            raise ValueError(f"License with id {license_id} not found")
        if not license_data['is_active']:
            raise ValueError(f"License '{license_data['client_name']}' is not active")

        # Check user count
        active_count = self.get_active_session_count(license_id)
        if active_count >= license_data['max_users']:
            raise ValueError(f"License '{license_data['client_name']}' has reached maximum users ({license_data['max_users']})")

        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Generate secure token
            token = secrets.token_urlsafe(32)

            # Calculate expiry
            expires_at = datetime.utcnow() + timedelta(hours=self.SESSION_EXPIRY_HOURS)

            cursor.execute('''
                INSERT INTO sessions (user_id, token, expires_at, license_id)
                VALUES (?, ?, ?, ?)
            ''', (user_id, token, expires_at.isoformat(), license_id))

            conn.commit()

            # Clean up expired sessions
            self._cleanup_expired_sessions()

            return token
        finally:
            conn.close()

    def get_session_license(self, token: str) -> Optional[Dict[str, Any]]:
        """Get the license associated with a session token."""
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            cursor.execute('''
                SELECT l.id, l.client_name, l.opera_version, l.max_users, l.is_active
                FROM sessions s
                JOIN licenses l ON s.license_id = l.id
                WHERE s.token = ? AND s.expires_at > ?
            ''', (token, datetime.utcnow().isoformat()))

            row = cursor.fetchone()
            if row is None:
                return None

            return {
                'id': row[0],
                'client_name': row[1],
                'opera_version': row[2],
                'max_users': row[3],
                'is_active': bool(row[4])
            }
        finally:
            conn.close()

    # ==================== User Company Access Methods ====================

    def get_user_companies(self, user_id: int) -> List[str]:
        """
        Get list of company IDs a user has access to.

        Returns list of company IDs. Empty list means no restrictions (access to all).
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT company_id FROM user_companies WHERE user_id = ?
            ''', (user_id,))
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

    def set_user_companies(self, user_id: int, company_ids: List[str]) -> bool:
        """
        Set which companies a user can access.

        Replaces all existing company access for the user.
        Empty list means access to all companies (no restrictions).
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Delete existing company access
            cursor.execute('DELETE FROM user_companies WHERE user_id = ?', (user_id,))

            # Insert new company access
            for company_id in company_ids:
                cursor.execute('''
                    INSERT INTO user_companies (user_id, company_id)
                    VALUES (?, ?)
                ''', (user_id, company_id))

            conn.commit()
            logger.info(f"Updated company access for user {user_id}: {company_ids}")
            return True
        except Exception as e:
            logger.error(f"Error setting user companies: {e}")
            return False
        finally:
            conn.close()

    def add_user_company(self, user_id: int, company_id: str) -> bool:
        """Add access to a single company for a user."""
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO user_companies (user_id, company_id)
                VALUES (?, ?)
            ''', (user_id, company_id))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding user company: {e}")
            return False
        finally:
            conn.close()

    def remove_user_company(self, user_id: int, company_id: str) -> bool:
        """Remove access to a single company for a user."""
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM user_companies WHERE user_id = ? AND company_id = ?
            ''', (user_id, company_id))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error removing user company: {e}")
            return False
        finally:
            conn.close()

    def user_has_company_access(self, user_id: int, company_id: str) -> bool:
        """
        Check if a user has access to a specific company.

        Returns True if:
        - User has no company restrictions (user_companies table is empty for this user)
        - User has explicit access to this company
        - User is an admin
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Check if user is admin
            cursor.execute('SELECT is_admin FROM users WHERE id = ?', (user_id,))
            row = cursor.fetchone()
            if row and row[0]:
                return True  # Admins have access to all companies

            # Check if user has any company restrictions
            cursor.execute('SELECT COUNT(*) FROM user_companies WHERE user_id = ?', (user_id,))
            count = cursor.fetchone()[0]

            if count == 0:
                return True  # No restrictions means access to all

            # Check if user has specific access to this company
            cursor.execute('''
                SELECT 1 FROM user_companies WHERE user_id = ? AND company_id = ?
            ''', (user_id, company_id))
            return cursor.fetchone() is not None
        finally:
            conn.close()

    def get_user_accessible_companies(self, user_id: int, all_companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter a list of companies to only those the user can access.

        Args:
            user_id: The user's ID
            all_companies: List of company dicts with 'id' key

        Returns:
            Filtered list of companies the user can access
        """
        conn = sqlite3.connect(self.DB_PATH)
        try:
            cursor = conn.cursor()

            # Check if user is admin
            cursor.execute('SELECT is_admin FROM users WHERE id = ?', (user_id,))
            row = cursor.fetchone()
            if row and row[0]:
                return all_companies  # Admins have access to all companies

            # Get user's company restrictions
            user_companies = self.get_user_companies(user_id)

            if not user_companies:
                return all_companies  # No restrictions means access to all

            # Filter to only allowed companies
            return [c for c in all_companies if c.get('id') in user_companies]
        finally:
            conn.close()


# Singleton instance
_user_auth_instance: Optional[UserAuth] = None


def get_user_auth() -> UserAuth:
    """Get or create the singleton UserAuth instance."""
    global _user_auth_instance
    if _user_auth_instance is None:
        _user_auth_instance = UserAuth()
    return _user_auth_instance
