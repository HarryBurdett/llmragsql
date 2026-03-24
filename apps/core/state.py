"""
Core shared state — module-level globals used by all apps.

All apps import from here instead of api/main.py globals.
The _ensure_company_context() function in middleware sets these per-request.
"""

from typing import Optional, Dict, Any
import contextvars
import configparser

# Per-request company context
_request_company_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar('_request_company_id', default=None)

# Per-company resource registries
_company_sql_connectors: Dict[str, Any] = {}
_company_email_storages: Dict[str, Any] = {}
_company_data: Dict[str, Dict[str, Any]] = {}
_default_company_id: Optional[str] = None
_last_active_company_id: Optional[str] = None

# Active globals — set per-request by _ensure_company_context()
config: Optional[configparser.ConfigParser] = None
sql_connector = None  # Optional[SQLConnector]
email_storage = None  # Optional[EmailStorage]
vector_db = None
llm = None
current_company: Optional[Dict[str, Any]] = None
user_auth = None
email_sync_manager = None
email_categorizer = None
customer_linker = None
active_system_id: Optional[str] = None
