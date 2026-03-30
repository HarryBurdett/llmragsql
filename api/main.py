"""
FastAPI backend for SQL RAG application.
Provides REST API endpoints for database queries, RAG queries, and configuration management.
"""

import os
import sys
import logging
import configparser
import json
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager
from pathlib import Path

# Load .env file if it exists
from dotenv import load_dotenv
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path, override=True)

from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Body, Request
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Literal
from datetime import datetime, timedelta
import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sql_rag.sql_connector import SQLConnector
from sql_rag.vector_db import VectorDB
from sql_rag.llm import create_llm_instance

# Import RAG populator for deployment initialization
try:
    from scripts.populate_rag import RAGPopulator
    RAG_POPULATOR_AVAILABLE = True
except ImportError:
    RAG_POPULATOR_AVAILABLE = False

# User authentication
from sql_rag.user_auth import get_user_auth, UserAuth
from api.auth_middleware import AuthMiddleware, require_admin, get_current_user, get_user_permissions

# Email module imports
from api.email.storage import EmailStorage
from api.email.providers.base import ProviderType
from api.email.providers.imap import IMAPProvider
from api.email.categorizer import EmailCategorizer, CustomerLinker
from api.email.sync import EmailSyncManager

# Supplier statement extraction and reconciliation
from sql_rag.supplier_statement_extract import SupplierStatementExtractor
from sql_rag.supplier_statement_reconcile import SupplierStatementReconciler
from sql_rag.supplier_statement_db import SupplierStatementDB, get_supplier_statement_db, reset_supplier_statement_db
from sql_rag.company_data import (
    set_current_company_id, get_company_data_dir, get_company_db_path,
    get_company_chroma_dir, migrate_root_databases, detect_company_from_config,
    get_current_db_path
)

# SMB access for Opera 3 FoxPro
try:
    from sql_rag.smb_access import SMBFileManager, get_smb_manager, set_smb_manager
    SMB_AVAILABLE = True
except ImportError:
    SMB_AVAILABLE = False

# Opera integration rules API
from api.opera_rules_api import router as opera_rules_router

# Lock monitor routes (extracted from main.py)
from apps.lock_monitor.api.routes import router as lock_monitor_router

# Pension export routes (extracted from main.py)
from apps.pension_export.api.routes import router as pension_export_router

# Balance check routes (extracted from main.py)
from apps.balance_check.api.routes import router as balance_check_router

# Supplier routes (extracted from main.py)
from apps.suppliers.api.routes import router as suppliers_router
from apps.suppliers.api.routes_contacts import router as supplier_contacts_router
from apps.suppliers.api.routes_onboarding import router as supplier_onboarding_router
from apps.suppliers.api.routes_remittance import router as supplier_remittance_router
from apps.suppliers.api.routes_aged import router as supplier_aged_router

# Dashboard routes (extracted from main.py)
from apps.dashboards.api.routes import router as dashboards_router

# GoCardless routes (extracted from main.py)
from apps.gocardless.api.routes import router as gocardless_router
from apps.bank_reconcile.api.routes import router as bank_reconcile_router
from apps.transaction_snapshot.api.routes import router as transaction_snapshot_router

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# Also log to file for diagnostic access
_file_handler = logging.FileHandler('/Users/maccb/llmragsql/api_debug.log')
_file_handler.setLevel(logging.INFO)
_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(_file_handler)
# Also add to the opera_sql_import logger
logging.getLogger('sql_rag.opera_sql_import').addHandler(_file_handler)
logging.getLogger('api.auth_middleware').addHandler(_file_handler)
logging.getLogger('api.auth_middleware').setLevel(logging.INFO)

# --- Per-request company resolution ---
# Multiple users may be logged into different companies simultaneously.
# A contextvars token is set per-request by AuthMiddleware so that each
# async request sees the correct company_id from its session.
import contextvars
_request_company_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar('_request_company_id', default=None)

# Registry of per-company resources (sql_connector, email_storage, etc.)
# Keyed by company_id, lazily populated on first company switch or startup.
_company_sql_connectors: Dict[str, SQLConnector] = {}
_company_email_storages: Dict[str, 'EmailStorage'] = {}
_company_email_sync_managers: Dict[str, 'EmailSyncManager'] = {}
_company_data: Dict[str, Dict[str, Any]] = {}  # company_id -> company dict
_default_company_id: Optional[str] = None  # set on startup, used as fallback

def _get_active_company_id() -> Optional[str]:
    """Return the company_id for the current request (session-aware)."""
    req_co = _request_company_id.get(None)
    if req_co:
        return req_co
    return _default_company_id

# Track the last company whose globals we set, so _ensure_company_context
# can skip resets when called repeatedly with the same company.
_last_active_company_id: Optional[str] = None

def _ensure_company_context(company_id: str) -> None:
    """Set module-level globals to point at the given company's resources.

    This is safe because the server is single-threaded async (one uvicorn
    worker), so only one request is executing at a time.  The function is
    idempotent — if called with the same company twice in a row it is a
    no-op (skips singleton resets).
    """
    global sql_connector, email_storage, current_company, _last_active_company_id

    if not company_id:
        return

    changed = (company_id != _last_active_company_id)

    # 1. Swap sql_connector (create lazily if not yet registered)
    if company_id not in _company_sql_connectors:
        try:
            co_data = _company_data.get(company_id) or load_company(company_id)
            if co_data and co_data.get('database'):
                config["database"]["database"] = co_data['database']
                save_config(config)
                _company_sql_connectors[company_id] = SQLConnector(CONFIG_PATH)
                if co_data:
                    _company_data[company_id] = co_data
                logger.info(f"Created SQL connector for {company_id}")
        except Exception as e:
            logger.warning(f"Could not create SQL connector for {company_id}: {e}")
    if company_id in _company_sql_connectors:
        sql_connector = _company_sql_connectors[company_id]

    # 2. Set company_data's _current_company_id FIRST (drives get_current_db_path for singletons)
    set_current_company_id(company_id)

    # 3. Swap email_storage (create lazily if not yet registered)
    if company_id not in _company_email_storages:
        try:
            email_db = get_company_db_path(company_id, "email_data.db")
            if email_db:
                _company_email_storages[company_id] = EmailStorage(str(email_db))
                logger.info(f"Created email storage for company {company_id} at {email_db}")
        except Exception as e:
            logger.warning(f"Could not create email storage for {company_id}: {e}")
    if company_id in _company_email_storages:
        email_storage = _company_email_storages[company_id]

    # 4. Set current_company dict
    if company_id in _company_data:
        current_company = _company_data[company_id]

    # 5. Update email_sync_manager to use this company's storage and re-register providers
    if email_sync_manager and company_id in _company_email_storages:
        email_sync_manager.storage = _company_email_storages[company_id]
        if changed:
            # Re-register email providers for the new company
            email_sync_manager.providers.clear()
            try:
                company_storage = _company_email_storages[company_id]
                existing_providers = company_storage.get_all_providers(enabled_only=True)
                for provider_info in existing_providers:
                    pid = provider_info['id']
                    ptype = provider_info['provider_type']
                    pconfig = provider_info.get('config', {})
                    if pconfig and ptype == 'imap':
                        provider = IMAPProvider(pconfig)
                        email_sync_manager.register_provider(pid, provider)
                        logger.info(f"Re-registered IMAP provider {provider_info['name']} for {company_id}")
            except Exception as e:
                logger.warning(f"Could not re-register email providers for {company_id}: {e}")

    # 6. Update customer_linker connector
    if customer_linker and company_id in _company_sql_connectors:
        customer_linker.set_sql_connector(_company_sql_connectors[company_id])

    # 7. Update import_lock path
    if changed:
        try:
            from sql_rag.import_lock import set_db_path as set_import_lock_path
            import_lock_path = get_company_db_path(company_id, "import_locks.db")
            if import_lock_path:
                set_import_lock_path(import_lock_path)
        except Exception:
            pass

    # 8. Reset singletons only when the company actually changed
    if changed:
        try:
            reset_supplier_statement_db()
        except Exception:
            pass
        try:
            from sql_rag.pdf_extraction_cache import reset_extraction_cache
            reset_extraction_cache()
        except Exception:
            pass
        try:
            from sql_rag.gocardless_payments import reset_payments_db
            reset_payments_db()
        except Exception:
            pass
        try:
            from sql_rag.opera_config import clear_control_accounts_cache
            clear_control_accounts_cache()
        except Exception:
            pass
        if customer_linker:
            try:
                customer_linker.clear_cache()
            except Exception:
                pass
        logger.info(f"Company context switched to {company_id}")

    _last_active_company_id = company_id

    # Propagate globals to extracted route modules so bare names resolve
    _sync_route_modules()

def _sync_route_modules():
    """Push current globals into extracted route modules."""
    try:
        from apps.bank_reconcile.api.routes import _sync_from_main as _br_sync
        _br_sync()
    except (ImportError, AttributeError):
        pass
    try:
        from apps.gocardless.api.routes import _sync_from_main as _gc_sync
        _gc_sync()
    except (ImportError, AttributeError):
        pass

# Global instances — plain globals, set per-request by _ensure_company_context
config: Optional[configparser.ConfigParser] = None
sql_connector: Optional[SQLConnector] = None
vector_db: Optional[VectorDB] = None
llm = None
current_company: Optional[Dict[str, Any]] = None

# Email module global instances
email_storage: Optional[EmailStorage] = None
email_sync_manager: Optional[EmailSyncManager] = None
email_categorizer: Optional[EmailCategorizer] = None
customer_linker: Optional[CustomerLinker] = None

# User authentication instance
user_auth: Optional[UserAuth] = None

# Get the config path relative to the project root
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.ini")
COMPANIES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "companies")
SYSTEMS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "systems.json")

# Currently active system ID (set on startup and when switching)
active_system_id: Optional[str] = None

def load_config(config_path: str = None) -> configparser.ConfigParser:
    """Load configuration from file."""
    if config_path is None:
        config_path = CONFIG_PATH
    cfg = configparser.ConfigParser()
    if os.path.exists(config_path):
        cfg.read(config_path)
    return cfg

def load_companies() -> List[Dict[str, Any]]:
    """Load all company configurations from the companies directory."""
    companies = []
    if os.path.exists(COMPANIES_DIR):
        for filename in os.listdir(COMPANIES_DIR):
            if filename.endswith('.json'):
                filepath = os.path.join(COMPANIES_DIR, filename)
                try:
                    with open(filepath, 'r') as f:
                        company = json.load(f)
                        companies.append(company)
                except Exception as e:
                    logger.warning(f"Could not load company config {filename}: {e}")
    return companies

def load_company(company_id: str) -> Optional[Dict[str, Any]]:
    """Load a specific company configuration."""
    filepath = os.path.join(COMPANIES_DIR, f"{company_id}.json")
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            return json.load(f)
    return None

def _bank_lock_key(bank_code: str) -> str:
    """Build a company-scoped lock key for bank-level operations.
    Allows different Opera companies to operate independently."""
    company_id = current_company.get("id", "default") if current_company else "default"
    return f"{company_id}:{bank_code}"

def friendly_db_error(error: Exception) -> str:
    """Translate raw database/connection errors into clear, user-friendly messages."""
    msg = str(error)
    msg_lower = msg.lower()

    # Opera database locked or inaccessible (e.g. backup running, exclusive lock)
    if '4060' in msg or 'cannot open database' in msg_lower:
        return 'Opera database is currently unavailable — it may be locked by a backup or another process. Please try again in a few minutes.'

    # Login/authentication failure
    if '18456' in msg or 'login failed' in msg_lower:
        return 'Cannot connect to Opera — database login failed. Please check the connection settings.'

    # Connection timeout
    if 'timeout' in msg_lower and ('connection' in msg_lower or 'login' in msg_lower):
        return 'Connection to the Opera database timed out. The server may be busy or unreachable. Please try again shortly.'

    # Server not reachable / network error
    if 'server is not found' in msg_lower or 'network' in msg_lower or 'unreachable' in msg_lower or 'tcp provider' in msg_lower:
        return 'Cannot reach the Opera database server. Please check the network connection and try again.'

    # Connection reset / dropped
    if 'connection reset' in msg_lower or 'connection has been closed' in msg_lower or 'broken pipe' in msg_lower:
        return 'The database connection was interrupted. Please try again.'

    # Deadlock
    if '1205' in msg or 'deadlock' in msg_lower:
        return 'The operation was temporarily blocked by another user. Please try again in a moment.'

    # Lock timeout
    if 'lock request time out' in msg_lower or 'lock timeout' in msg_lower:
        return 'Opera is busy — another user or process is currently updating the same data. Please wait a moment and try again.'

    # Table not found
    if 'invalid object name' in msg_lower:
        return 'A required Opera table was not found. Please check the database connection is pointing to the correct Opera company.'

    # Duplicate key
    if 'duplicate' in msg_lower or 'unique constraint' in msg_lower or 'cannot insert' in msg_lower:
        return 'A duplicate record was detected — this entry may already exist in Opera.'

    # Foreign key violation
    if 'foreign key' in msg_lower:
        return 'Invalid account code — please verify the customer, supplier, or nominal account exists in Opera.'

    # Generic connection error from our connector layer
    if 'database connection failed' in msg_lower or 'query execution failed' in msg_lower:
        # Strip the wrapper and try to translate the inner error
        inner = msg.split(': ', 1)[-1] if ': ' in msg else msg
        inner_friendly = friendly_db_error(Exception(inner))
        if inner_friendly != inner:
            return inner_friendly
        return 'Cannot connect to the Opera database. Please check the connection and try again.'

    # Fallback — return a sanitised version (no raw SQL)
    return 'An unexpected database error occurred. Please try again or contact support.'

def save_config(cfg: configparser.ConfigParser, config_path: str = None):
    """Save configuration to file."""
    if config_path is None:
        config_path = CONFIG_PATH
    with open(config_path, "w") as f:
        cfg.write(f)

# ============ Systems (Named Config Profiles) ============

def load_systems() -> List[Dict[str, Any]]:
    """Load all system profiles. Auto-creates from config.ini if none exist."""
    if os.path.exists(SYSTEMS_PATH):
        try:
            with open(SYSTEMS_PATH, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load systems.json: {e}")
    # Auto-create a default system from the current config.ini
    systems = [_system_from_current_config("default", "Default System", True)]
    save_systems(systems)
    return systems

def save_systems(systems: List[Dict[str, Any]]):
    """Persist systems list to disk."""
    with open(SYSTEMS_PATH, 'w') as f:
        json.dump(systems, f, indent=2)

def _system_from_current_config(system_id: str, name: str, is_default: bool) -> Dict[str, Any]:
    """Build a system profile dict from the current config.ini values."""
    cfg = load_config()
    db = {}
    if cfg.has_section("database"):
        for key in ("type", "server", "port", "database", "username", "password",
                     "use_windows_auth", "pool_size", "max_overflow", "pool_timeout",
                     "connection_timeout", "command_timeout", "ssl", "trust_server_certificate"):
            db[key] = cfg.get("database", key, fallback="")
    opera = {}
    if cfg.has_section("opera"):
        for key in ("version", "opera3_server_path", "opera3_base_path"):
            opera[key] = cfg.get("opera", key, fallback="")
    return {
        "id": system_id,
        "name": name,
        "is_default": is_default,
        "database": db,
        "opera": opera,
    }

def apply_system_to_config(system: Dict[str, Any]):
    """Write a system profile's database/opera settings into config.ini and reload.

    CRITICAL: This function guarantees that active_system_id, config.ini, and
    sql_connector always stay in sync. On connection failure, all three are
    rolled back to their previous state.
    """
    global config, active_system_id

    if not config:
        config = load_config()

    # Snapshot previous state for rollback
    prev_system_id = active_system_id
    prev_db_settings = dict(config.items("database")) if config.has_section("database") else {}
    prev_opera_settings = dict(config.items("opera")) if config.has_section("opera") else {}
    co_id = _get_active_company_id() or _default_company_id
    prev_connector = _company_sql_connectors.get(co_id) if co_id else None

    # Apply database settings
    if "database" in system:
        if not config.has_section("database"):
            config.add_section("database")
        for key, value in system["database"].items():
            config["database"][key] = str(value)

    # Apply opera settings
    if "opera" in system:
        if not config.has_section("opera"):
            config.add_section("opera")
        for key, value in system["opera"].items():
            config["opera"][key] = str(value)

    save_config(config)
    active_system_id = system["id"]

    # If the system had empty settings, backfill from what's now in config.ini
    # so future activations restore the correct values
    if not system.get("database") or not system.get("opera"):
        _sync_active_system_config()

    # Reinitialize SQL connector for the active company
    try:
        new_connector = SQLConnector(CONFIG_PATH)
        co_id = _get_active_company_id() or _default_company_id
        if co_id:
            _company_sql_connectors[co_id] = new_connector
        logger.info(f"Switched to system: {system['name']} (id={system['id']})")
    except Exception as e:
        logger.warning(f"SQL connector reinit failed after system switch: {e} — rolling back")
        active_system_id = prev_system_id
        if prev_db_settings:
            for key, value in prev_db_settings.items():
                config["database"][key] = value
        if prev_opera_settings:
            for key, value in prev_opera_settings.items():
                config["opera"][key] = value
        save_config(config)
        # Restore previous connector
        if co_id and prev_connector:
            _company_sql_connectors[co_id] = prev_connector
        raise ConnectionError(f"Database connection failed: {e}") from e

def _sync_active_system_config():
    """Sync the active system's database/opera fields from the current config.ini.
    Called after Settings saves database or opera config so the system profile stays in sync."""
    if not active_system_id:
        return
    try:
        systems = load_systems()
        for s in systems:
            if s["id"] == active_system_id:
                # Read current database config from config.ini
                if config and config.has_section("database"):
                    s["database"] = {k: v for k, v in config.items("database")}
                if config and config.has_section("opera"):
                    s["opera"] = {k: v for k, v in config.items("opera")}
                save_systems(systems)
                break
    except Exception as e:
        logger.warning(f"Could not sync system config: {e}")

def get_active_system() -> Optional[Dict[str, Any]]:
    """Return the currently active system profile."""
    systems = load_systems()
    if active_system_id:
        for s in systems:
            if s["id"] == active_system_id:
                return s
    # Fall back to default
    for s in systems:
        if s.get("is_default"):
            return s
    return systems[0] if systems else None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global config, sql_connector, vector_db, llm, user_auth, current_company
    global email_storage, email_sync_manager, email_categorizer, customer_linker
    global active_system_id, _default_company_id

    # Startup
    logger.info("Starting SQL RAG API...")
    config = load_config()

    # Initialise active system from systems.json — match against config.ini server/database
    try:
        systems = load_systems()
        matched_sys = None
        if config and config.has_section("database"):
            cfg_server = config.get("database", "server", fallback="").strip().lower()
            cfg_db = config.get("database", "database", fallback="").strip().lower()
            if cfg_server and cfg_db:
                matched_sys = next(
                    (s for s in systems
                     if s.get("database", {}).get("server", "").strip().lower() == cfg_server
                     and s.get("database", {}).get("database", "").strip().lower() == cfg_db),
                    None
                )
        if matched_sys:
            active_system_id = matched_sys["id"]
            logger.info(f"Active system (matched from config.ini): {matched_sys['name']} (id={matched_sys['id']})")
        else:
            # CRITICAL: config.ini doesn't match any system — apply the default
            # system's settings to config.ini to guarantee sync
            default_sys = next((s for s in systems if s.get("is_default")), systems[0] if systems else None)
            if default_sys:
                logger.warning(
                    f"config.ini (server={config.get('database', 'server', fallback='?')}, "
                    f"db={config.get('database', 'database', fallback='?')}) does not match any "
                    f"system in systems.json — applying default system: {default_sys['name']}"
                )
                apply_system_to_config(default_sys)
    except Exception as e:
        logger.warning(f"Could not initialise systems: {e}")

    # Detect current company from config and set up per-company data directory
    try:
        company_id = detect_company_from_config(config)
        set_current_company_id(company_id)
        data_dir = get_company_data_dir(company_id)
        migrate_root_databases(company_id)
        logger.info(f"Per-company data directory: {data_dir}")

        # Set current_company global from detected company
        _default_company_id = company_id
        company_data_obj = load_company(company_id)
        if company_data_obj:
            current_company = company_data_obj
            _company_data[company_id] = company_data_obj
            logger.info(f"Current company set to: {company_data_obj.get('name', company_id)}")
    except Exception as e:
        logger.warning(f"Could not initialize per-company data: {e}")

    # Initialize user authentication
    try:
        user_auth = get_user_auth()
        logger.info("User authentication initialized")
    except Exception as e:
        logger.warning(f"Could not initialize user authentication: {e}")

    try:
        real_connector = SQLConnector(CONFIG_PATH)
        if _default_company_id:
            _company_sql_connectors[_default_company_id] = real_connector
        logger.info("SQL connector initialized")
    except Exception as e:
        logger.warning(f"Could not initialize SQL connector: {e}")

    try:
        vector_db = VectorDB(config)
        logger.info("Vector DB initialized")

        # Auto-populate RAG if database is empty and auto_populate is enabled
        auto_populate = config.get("system", "rag_auto_populate", fallback="true").lower() == "true"
        if auto_populate and RAG_POPULATOR_AVAILABLE:
            try:
                info = vector_db.get_collection_info()
                if info.get("vectors_count", 0) == 0:
                    logger.info("RAG database is empty - auto-populating with Opera knowledge...")
                    populator = RAGPopulator(config_path=CONFIG_PATH)
                    results = populator.populate_all(clear_first=False)
                    if results["success"]:
                        logger.info(f"RAG auto-population complete: {results['total_documents']} documents loaded")
                    else:
                        logger.warning(f"RAG auto-population failed: {results.get('error', 'Unknown error')}")
                else:
                    logger.info(f"RAG database already has {info.get('vectors_count')} documents")
            except Exception as e:
                logger.warning(f"Could not auto-populate RAG: {e}")
    except Exception as e:
        logger.warning(f"Could not initialize Vector DB: {e}")

    try:
        llm = create_llm_instance(config)
        logger.info("LLM initialized")
    except Exception as e:
        logger.warning(f"Could not initialize LLM: {e}")

    # Initialize email module
    try:
        from sql_rag.company_data import get_current_db_path
        email_db = get_current_db_path("email_data.db")
        if email_db is None:
            email_db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "email_data.db")
        real_email_storage = EmailStorage(str(email_db))
        if _default_company_id:
            _company_email_storages[_default_company_id] = real_email_storage
        logger.info("Email storage initialized")

        # Initialize categorizer with LLM
        email_categorizer = EmailCategorizer(llm)
        logger.info("Email categorizer initialized")

        # Initialize customer linker with the real SQL connector
        real_conn = _company_sql_connectors.get(_default_company_id) if _default_company_id else None
        customer_linker = CustomerLinker(real_conn)
        logger.info("Customer linker initialized")

        # Initialize sync manager with the real email storage instance
        email_sync_manager = EmailSyncManager(
            storage=real_email_storage,
            categorizer=email_categorizer,
            linker=customer_linker
        )

        # Register any enabled providers from config
        await _initialize_email_providers()

        logger.info("Email sync manager initialized")
    except Exception as e:
        logger.warning(f"Could not initialize email module: {e}")

    # Initialize supplier statement automation database
    try:
        supplier_statement_db = get_supplier_statement_db()
        logger.info(f"Supplier statement database initialized at {supplier_statement_db.db_path}")
    except Exception as e:
        logger.warning(f"Could not initialize supplier statement database: {e}")

    # Set the module-level globals to the initial company's resources
    if _default_company_id:
        _ensure_company_context(_default_company_id)
        logger.info(f"Initial company context set to {_default_company_id}")

    # Clean up stale SMB temp directories
    try:
        from sql_rag.smb_access import cleanup_stale_temp_dirs
        cleanup_stale_temp_dirs()
    except ImportError:
        pass

    # Auto-connect SMB for Opera 3 if configured
    if config and config.has_section("opera"):
        if (config.get("opera", "version", fallback="") == "opera3"
            and config.get("opera", "opera3_server_path", fallback="")
            and config.get("opera", "opera3_share_user", fallback="")
            and config.get("opera", "opera3_share_password", fallback="")):
            try:
                msg = _connect_opera3_smb(
                    config.get("opera", "opera3_server_path"),
                    config.get("opera", "opera3_share_user"),
                    config.get("opera", "opera3_share_password")
                )
                logger.info(f"Startup SMB: {msg}")
                # Set base_path in memory only (temp dir is ephemeral)
                smb = get_smb_manager()
                if smb and smb.get_local_base():
                    config["opera"]["opera3_base_path"] = str(smb.get_local_base())
            except Exception as e:
                logger.warning(f"Startup SMB connection failed: {e}")

    yield

    # Shutdown
    logger.info("Shutting down SQL RAG API...")
    if email_sync_manager:
        await email_sync_manager.stop_periodic_sync()

app = FastAPI(
    title="SQL RAG API",
    description="REST API for SQL RAG application - Query databases with natural language",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:5174", "http://localhost:3000", "http://127.0.0.1:5173", "http://127.0.0.1:5174"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Crakd.ai GoCardless signup page — standalone app served at /signup
@app.get("/signup", include_in_schema=False)
async def crakd_signup_page():
    signup_path = Path(__file__).parent.parent / "crakd-signup" / "index.html"
    if signup_path.exists():
        return FileResponse(signup_path, media_type="text/html")
    raise HTTPException(status_code=404, detail="Signup page not found")

# Global exception handler — translates raw database errors into friendly messages
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    from starlette.responses import JSONResponse
    error_str = str(exc)
    # Check if this looks like a database/connection error
    db_keywords = ['pyodbc', 'sqlalchemy', 'database', 'connection', 'login failed',
                   'cannot open', 'timeout', 'deadlock', 'lock request', 'network',
                   'tcp provider', 'broken pipe', '4060', '18456', '1205']
    if any(kw in error_str.lower() for kw in db_keywords):
        friendly = friendly_db_error(exc)
        logger.error(f"Database error (translated for user): {error_str}")
        return JSONResponse(status_code=503, content={"success": False, "error": friendly})
    # For non-DB errors, log and return generic message
    logger.error(f"Unhandled exception on {request.url.path}: {error_str}")
    return JSONResponse(status_code=500, content={"success": False, "error": "An unexpected error occurred. Please try again."})

# Auth middleware - added after CORS so CORS headers are always sent
# Note: Middleware is applied in reverse order, so this runs after CORS
@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Validate authentication tokens on protected routes."""
    from api.auth_middleware import PUBLIC_PATHS, PUBLIC_PREFIXES

    path = request.url.path

    # Skip auth for CORS preflight requests (OPTIONS method)
    if request.method == 'OPTIONS':
        return await call_next(request)

    # Skip auth for public paths
    if path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES):
        return await call_next(request)

    # Skip auth for non-API paths (static files, etc.)
    if not path.startswith('/api/'):
        return await call_next(request)

    # Get token from Authorization header or query parameter (for iframe/direct browser views)
    auth_header = request.headers.get('Authorization', '')
    token = auth_header.replace('Bearer ', '') if auth_header.startswith('Bearer ') else ''

    # Fallback: accept token as query parameter (needed for iframe src, PDF viewer, etc.)
    if not token:
        token = request.query_params.get('token', '')

    if not token:
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=401,
            content={'error': 'Not authenticated', 'detail': 'No authentication token provided'}
        )

    # Validate token
    if user_auth:
        user = user_auth.validate_session(token)
        if not user:
            from fastapi.responses import JSONResponse
            return JSONResponse(
                status_code=401,
                content={'error': 'Invalid or expired session', 'detail': 'Please log in again'}
            )

        # Attach user to request state
        request.state.user = user
        request.state.user_permissions = user_auth.get_user_permissions(user['id'])

        # Set company context from session — ensures every request uses the
        # correct company's sql_connector, email_storage, settings, etc.
        import sqlite3 as _sq3
        try:
            _conn = _sq3.connect(str(user_auth.DB_PATH))
            _row = _conn.execute('SELECT company_id FROM sessions WHERE token = ?', (token,)).fetchone()
            _conn.close()
            if _row and _row[0]:
                _ensure_company_context(_row[0])
                _request_company_id.set(_row[0])
        except Exception:
            pass

    return await call_next(request)

# Include Opera integration rules API router
app.include_router(opera_rules_router)

# Include lock monitor API router
app.include_router(lock_monitor_router)

# Include pension export API router
app.include_router(pension_export_router)

# Include balance check API router
app.include_router(balance_check_router)

# Include supplier API router
app.include_router(suppliers_router)
app.include_router(supplier_contacts_router)
app.include_router(supplier_onboarding_router)
app.include_router(supplier_remittance_router)
app.include_router(supplier_aged_router)

# Include dashboard API router
app.include_router(dashboards_router)

# Include GoCardless API router
app.include_router(gocardless_router)
app.include_router(bank_reconcile_router)
app.include_router(transaction_snapshot_router)

# ============ Pydantic Models ============

class SQLQueryRequest(BaseModel):
    query: str = Field(..., description="SQL query to execute")
    store_in_vector_db: bool = Field(False, description="Store results in vector database")

class SQLQueryResponse(BaseModel):
    success: bool
    data: List[Dict[str, Any]] = []
    columns: List[str] = []
    row_count: int = 0
    error: Optional[str] = None

class RAGQueryRequest(BaseModel):
    question: str = Field(..., description="Natural language question")
    num_results: int = Field(5, description="Number of similar results to retrieve")

class SQLToRAGRequest(BaseModel):
    description: str = Field(..., description="Natural language description of data to fetch and store")
    custom_sql: Optional[str] = Field(None, description="Optional: provide your own SQL instead of AI-generated")
    table_filter: Optional[List[str]] = Field(None, description="Optional: limit AI to specific tables")
    max_rows: int = Field(1000, description="Maximum rows to fetch and store")

class RAGQueryResponse(BaseModel):
    success: bool
    answer: str = ""
    sources: List[Dict[str, Any]] = []
    error: Optional[str] = None

class ConfigUpdateRequest(BaseModel):
    section: str
    key: str
    value: str

class ProviderConfig(BaseModel):
    provider: str
    api_key: Optional[str] = None
    model: str
    temperature: float = 0.2
    max_tokens: int = 1000
    ollama_url: Optional[str] = None  # For local Ollama running on network

class DatabaseConfig(BaseModel):
    type: str
    server: Optional[str] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    use_windows_auth: bool = False
    # Advanced MS SQL settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    connection_timeout: int = 30
    command_timeout: int = 60
    ssl: bool = False
    ssl_ca: Optional[str] = None
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    port: Optional[int] = None  # Default 1433 for MSSQL, 5432 for PostgreSQL, 3306 for MySQL

def _connect_opera3_smb(server_path: str, username: str, password: str) -> str:
    """
    Connect to an Opera 3 SMB share using smbprotocol.
    Creates an SMBFileManager singleton and returns the local temp base path.

    Returns:
        Status message with local path on success, or error message
    """
    if not SMB_AVAILABLE:
        return "smbprotocol not installed. Install with: pip install smbprotocol"

    try:
        # Disconnect existing manager if any
        existing = get_smb_manager()
        if existing is not None:
            set_smb_manager(None)

        # Create new manager from UNC path
        manager = SMBFileManager.from_unc_path(server_path, username, password)
        manager.connect()
        set_smb_manager(manager)

        # Auto-generate company configs from seqco.dbf
        _generate_opera3_company_configs()

        local_base = manager.get_local_base()
        logger.info(f"SMB connected, local cache: {local_base}")
        return f"Connected via SMB, local cache: {local_base}"

    except Exception as e:
        logger.error(f"SMB connection failed: {e}")
        return f"SMB connection failed: {e}"


def _generate_opera3_company_configs():
    """
    Read seqco.dbf from the SMB share and create/update companies/*.json
    for each Opera 3 company. Preserves user-added settings in existing configs.
    """
    smb = get_smb_manager()
    if smb is None or not smb.is_connected():
        logger.warning("Cannot generate Opera 3 company configs — SMB not connected")
        return

    try:
        from sql_rag.opera3_foxpro import Opera3System
        local_base = str(smb.get_local_base())
        system = Opera3System(local_base)
        companies = system.get_companies()

        if not companies:
            logger.warning("No companies found in seqco.dbf")
            return

        os.makedirs(COMPANIES_DIR, exist_ok=True)

        for co in companies:
            code = co.get("code", "").strip()
            if not code:
                continue

            company_id = f"o3_{code.lower()}"
            config_path = os.path.join(COMPANIES_DIR, f"{company_id}.json")

            # Load existing config to preserve user-added settings
            existing = {}
            if os.path.exists(config_path):
                try:
                    with open(config_path, 'r') as f:
                        existing = json.load(f)
                except Exception:
                    pass

            # Build/update config — preserve existing settings, update from seqco
            company_config = existing.copy()
            company_config["id"] = company_id
            company_config["name"] = co.get("name", code)
            company_config["opera_version"] = "3"
            company_config["opera3_company_code"] = code.upper()
            company_config["description"] = f"{code.upper()} - {co.get('name', '')}"

            # Extract relative data path from the full data_path
            # Opera3System.get_companies() already resolves CO_SUBDIR to a local path
            # We need the path relative to the SMB local base
            data_path = co.get("data_path", "")
            if data_path:
                try:
                    from pathlib import Path
                    rel = str(Path(data_path).relative_to(local_base))
                    company_config["opera3_data_path"] = rel
                except ValueError:
                    company_config["opera3_data_path"] = ""
            else:
                company_config["opera3_data_path"] = ""

            # Set defaults for settings if not already present
            if "settings" not in company_config:
                company_config["settings"] = {
                    "currency": "GBP",
                    "currency_symbol": "\u00a3",
                    "date_format": "DD/MM/YYYY"
                }

            with open(config_path, 'w') as f:
                json.dump(company_config, f, indent=2)

            logger.info(f"Generated company config: {config_path} ({company_config['name']})")

    except Exception as e:
        logger.error(f"Error generating Opera 3 company configs: {e}")


class OperaConfig(BaseModel):
    """Opera system configuration"""
    version: str = "sql_se"  # "sql_se" or "opera3"
    # Opera 3 specific settings
    opera3_server_path: Optional[str] = None  # UNC or network path to Opera 3 server
    opera3_base_path: Optional[str] = None  # Local path on the server
    opera3_company_code: Optional[str] = None  # Company code from seqco.dbf
    opera3_share_user: Optional[str] = None  # Windows share username
    opera3_share_password: Optional[str] = None  # Windows share password

class Opera3Company(BaseModel):
    """Opera 3 company information"""
    code: str
    name: str
    data_path: str

class TableInfo(BaseModel):
    schema_name: str
    table_name: str
    table_type: str

class ColumnInfo(BaseModel):
    column_name: str
    data_type: str
    is_nullable: bool
    column_default: Optional[str] = None

# ============ Authentication Models ============

class LoginRequest(BaseModel):
    """Login request with username and password."""
    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")
    license_id: Optional[int] = Field(None, description="License/client ID for this session")

class LoginResponse(BaseModel):
    """Login response with token and user info."""
    success: bool
    token: Optional[str] = None
    user: Optional[Dict[str, Any]] = None
    permissions: Optional[Dict[str, bool]] = None
    license: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class UserCreateRequest(BaseModel):
    """Request to create a new user."""
    username: str = Field(..., description="Username (unique)")
    password: str = Field(..., description="Password")
    display_name: Optional[str] = Field(None, description="Display name")
    email: Optional[str] = Field(None, description="Email address")
    is_admin: bool = Field(False, description="Is admin user")
    permissions: Optional[Dict[str, bool]] = Field(None, description="Module permissions")
    default_company: Optional[str] = Field(None, description="Default company ID on login")
    default_system: Optional[str] = Field(None, description="Default system profile ID on login")
    ui_mode: Optional[str] = Field(None, description="UI mode: 'classic' or 'launcher'")
    voice_enabled: Optional[bool] = Field(None, description="Voice control enabled")

class UserUpdateRequest(BaseModel):
    """Request to update a user."""
    username: Optional[str] = Field(None, description="Username (unique)")
    password: Optional[str] = Field(None, description="New password (optional)")
    display_name: Optional[str] = Field(None, description="Display name")
    email: Optional[str] = Field(None, description="Email address")
    is_admin: Optional[bool] = Field(None, description="Is admin user")
    is_active: Optional[bool] = Field(None, description="Is user active")
    permissions: Optional[Dict[str, bool]] = Field(None, description="Module permissions")
    default_company: Optional[str] = Field(None, description="Default company ID on login")
    default_system: Optional[str] = Field(None, description="Default system profile ID on login")
    ui_mode: Optional[str] = Field(None, description="UI mode: 'classic' or 'launcher'")
    voice_enabled: Optional[bool] = Field(None, description="Voice control enabled")

class UserResponse(BaseModel):
    """User information response."""
    id: int
    username: str
    display_name: str
    email: Optional[str] = None
    is_admin: bool
    is_active: bool
    permissions: Optional[Dict[str, bool]] = None
    created_at: Optional[str] = None
    last_login: Optional[str] = None
    created_by: Optional[str] = None

# ============ Health & Status Endpoints ============

@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "sql-rag-api"}

@app.get("/api/status")
async def get_status():
    """Get current system status."""
    return {
        "sql_connector": sql_connector is not None,
        "vector_db": vector_db is not None,
        "llm": llm is not None,
        "config_loaded": config is not None
    }

# ============ Authentication Endpoints ============

@app.post("/api/auth/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    """
    Authenticate user and return session token.

    This endpoint is public and does not require authentication.
    Opera is the master for users - we sync from Opera before authenticating.
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    # First, check if user exists in Opera and sync (Opera is king)
    # Sync from Opera — try SE (SQL) first, then Opera 3 (SMB) as fallback
    if sql_connector:
        try:
            opera_query = """
                SELECT [user], username, manager, email_addr, prefcomp, state, cos
                FROM Opera3SESystem.dbo.sequser
                WHERE LOWER([user]) = LOWER(:username)
            """
            result = sql_connector.execute_query(opera_query, params={'username': request.username})

            if result is not None and not result.empty:
                row = result.iloc[0]
                opera_user = row['user'].strip() if row['user'] else ''
                display_name = row['username'].strip() if row['username'] else opera_user
                is_manager = bool(row['manager'])
                email = row['email_addr'].strip() if row['email_addr'] else None
                pref_company = row['prefcomp'].strip() if row['prefcomp'] else None
                is_active = row['state'] in [0, 1]  # state 2 = deleted
                cos_string = row['cos'].strip() if row.get('cos') else None

                # Load companies for mapping
                companies = load_companies()

                # Map preferred company letter to company ID
                default_company = None
                if pref_company:
                    for co in companies:
                        db_name = co.get('database', '')
                        if db_name.endswith(pref_company):
                            default_company = co.get('id')
                            break

                # Parse cos field to get company access
                # cos is a string where each character (A-Z, 0-9) represents a company the user can access
                user_company_access = None
                if cos_string:
                    user_company_access = []
                    for char in cos_string:
                        # Find matching company config by database suffix
                        for co in companies:
                            db_name = co.get('database', '')
                            if db_name.endswith(char):
                                user_company_access.append(co.get('id'))
                                break
                    logger.info(f"Opera company access for '{opera_user}' from cos='{cos_string}': {user_company_access}")

                # Query Opera NavGroup permissions for this user
                # seqnavgrps stores navigation group access per user
                opera_permissions = None
                try:
                    navgrp_query = """
                        SELECT navgroup, enabled
                        FROM Opera3SESystem.dbo.seqnavgrps
                        WHERE LOWER([user]) = LOWER(:username)
                    """
                    navgrp_result = sql_connector.execute_query(navgrp_query, params={'username': opera_user})

                    if navgrp_result is not None and not navgrp_result.empty:
                        # Build navgroup dict
                        navgroups = {}
                        for _, navrow in navgrp_result.iterrows():
                            navgroup = navrow['navgroup'].strip() if navrow['navgroup'] else ''
                            # Check if enabled column exists and get its value
                            # Opera uses enabled=1 for access, enabled=0 for no access
                            enabled = bool(navrow.get('enabled', 1)) if 'enabled' in navrow else True
                            if navgroup:
                                navgroups[navgroup] = enabled

                        # Map Opera NavGroups to SQL RAG modules
                        opera_permissions = UserAuth.map_opera_navgroups_to_permissions(navgroups)
                        logger.info(f"Opera NavGroups for '{opera_user}': {navgroups} -> SQL RAG: {opera_permissions}")
                except Exception as navgrp_err:
                    # If seqnavgrps query fails, continue without Opera permissions
                    # Could be table doesn't exist or different structure
                    logger.warning(f"Could not query Opera NavGroups: {navgrp_err}")

                # Sync user from Opera (creates if not exists, updates if exists)
                user_auth.sync_user_from_opera(
                    opera_username=opera_user,
                    display_name=display_name,
                    email=email,
                    is_manager=is_manager,
                    is_active=is_active,
                    preferred_company=default_company,
                    opera_permissions=opera_permissions,
                    company_access=user_company_access
                )
                logger.info(f"Synced user '{opera_user}' from Opera before login")
        except Exception as e:
            # Log but don't fail - allow login to proceed with local user if Opera unavailable
            logger.warning(f"Could not sync user from Opera: {e}")
    elif get_smb_manager() is not None:
        # Opera 3: sync from sequser.dbf via SMB
        try:
            from sql_rag.opera3_foxpro import Opera3System
            smb = get_smb_manager()
            local_base = str(smb.get_local_base())
            system = Opera3System(local_base)

            users = system.read_sequser(username=request.username)
            if users:
                row = users[0]
                opera_user = row['user']
                display_name = row['username'] or opera_user
                is_manager = row['manager']
                email = row['email_addr'] or None
                pref_company_letter = row['prefcomp']
                cos_string = row['cos']

                # Load companies for mapping
                companies = load_companies()

                # Map preferred company letter to company ID
                # Opera 3 uses opera3_company_code field instead of database suffix
                default_company = None
                if pref_company_letter:
                    for co in companies:
                        if co.get('opera3_company_code', '').upper() == pref_company_letter.upper():
                            default_company = co.get('id')
                            break

                # Parse cos field to get company access
                # cos is a string where each character represents a company the user can access
                user_company_access = None
                if cos_string:
                    user_company_access = []
                    for char in cos_string:
                        for co in companies:
                            if co.get('opera3_company_code', '').upper() == char.upper():
                                user_company_access.append(co.get('id'))
                                break
                    logger.info(f"Opera 3 company access for '{opera_user}' from cos='{cos_string}': {user_company_access}")

                # Read NavGroup permissions
                opera_permissions = None
                try:
                    navgroups = system.read_seqnavgrps(opera_user)
                    if navgroups:
                        opera_permissions = UserAuth.map_opera_navgroups_to_permissions(navgroups)
                        logger.info(f"Opera 3 NavGroups for '{opera_user}': {navgroups} -> SQL RAG: {opera_permissions}")
                except Exception as navgrp_err:
                    logger.warning(f"Could not read Opera 3 NavGroups: {navgrp_err}")

                # Sync user from Opera 3 (creates if not exists, updates if exists)
                user_auth.sync_user_from_opera(
                    opera_username=opera_user,
                    display_name=display_name,
                    email=email,
                    is_manager=is_manager,
                    is_active=True,
                    preferred_company=default_company,
                    opera_permissions=opera_permissions,
                    company_access=user_company_access
                )
                logger.info(f"Synced user '{opera_user}' from Opera 3 sequser.dbf before login")
        except Exception as e:
            logger.warning(f"Could not sync from Opera 3 sequser.dbf: {e} — continuing with local auth")

    # Authenticate user
    user = user_auth.authenticate(request.username, request.password)
    if not user:
        return LoginResponse(success=False, error="Invalid username or password")

    # Enforce single active session — block if user is already logged in
    # First clean up expired sessions, then check for active ones
    try:
        import sqlite3
        conn = sqlite3.connect(user_auth.DB_PATH)
        cursor = conn.cursor()
        # Clean up expired sessions first
        cursor.execute("DELETE FROM sessions WHERE expires_at <= datetime('now')")
        conn.commit()
        # Now check for genuinely active sessions
        cursor.execute(
            "SELECT COUNT(*) FROM sessions WHERE user_id = ? AND expires_at > datetime('now')",
            (user['id'],)
        )
        active_count = cursor.fetchone()[0]
        conn.close()
        if active_count > 0:
            return LoginResponse(success=False, error=f"User '{request.username}' is already logged in. Please log out from the other session first.")
    except Exception as e:
        logger.warning(f"Could not check active sessions: {e}")

    # Create session (with license if provided)
    license_data = None
    try:
        if request.license_id:
            token = user_auth.create_session_with_license(user['id'], request.license_id)
            license_data = user_auth.get_license(request.license_id)
        else:
            token = user_auth.create_session(user['id'])
    except ValueError as e:
        return LoginResponse(success=False, error=str(e))

    # Get permissions
    permissions = user_auth.get_user_permissions(user['id'])

    return LoginResponse(
        success=True,
        token=token,
        user=user,
        permissions=permissions,
        license=license_data
    )

@app.post("/api/auth/logout")
async def logout(request: Request):
    """
    Logout and invalidate the current session.
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    # Get token from header
    auth_header = request.headers.get('Authorization', '')
    token = auth_header.replace('Bearer ', '') if auth_header.startswith('Bearer ') else ''

    if token:
        user_auth.invalidate_session(token)

    return {"success": True, "message": "Logged out successfully"}

@app.get("/api/auth/me")
async def get_current_user_info(request: Request):
    """
    Get current user info and permissions.
    """
    user = getattr(request.state, 'user', None)
    permissions = getattr(request.state, 'user_permissions', {})

    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")

    return {
        "success": True,
        "user": user,
        "permissions": permissions
    }

@app.get("/api/auth/preferences")
async def get_user_preferences(request: Request):
    """Get current user's preferences."""
    user = getattr(request.state, 'user', None)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")

    return {
        "success": True,
        "ui_mode": user.get('ui_mode', 'classic'),
        "voice_enabled": user.get('voice_enabled', False),
        "default_company": user.get('default_company'),
        "default_system": user.get('default_system'),
    }

@app.put("/api/auth/preferences")
async def update_user_preferences(request: Request):
    """Update current user's preferences."""
    user = getattr(request.state, 'user', None)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")

    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    body = await request.json()
    ui_mode = body.get('ui_mode')
    voice_enabled = body.get('voice_enabled')
    default_company = body.get('default_company')
    default_system = body.get('default_system')

    # Validate ui_mode
    if ui_mode is not None and ui_mode not in ('classic', 'launcher'):
        return {"success": False, "error": "ui_mode must be 'classic' or 'launcher'"}

    try:
        updated = user_auth.update_user(
            user_id=user['id'],
            ui_mode=ui_mode,
            voice_enabled=voice_enabled,
            default_company=default_company,
            default_system=default_system
        )
        return {
            "success": True,
            "ui_mode": updated.get('ui_mode', 'classic'),
            "voice_enabled": updated.get('voice_enabled', False),
            "default_company": updated.get('default_company'),
            "default_system": updated.get('default_system'),
        }
    except Exception as e:
        logger.error(f"Failed to update preferences: {e}")
        return {"success": False, "error": str(e)}

@app.get("/api/auth/modules")
async def get_available_modules():
    """
    Get list of available modules for permissions.
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    return {
        "success": True,
        "modules": [
            {"id": "cashbook", "name": "Cashbook", "description": "Bank Reconciliation, GoCardless Import"},
            {"id": "payroll", "name": "Payroll", "description": "Pension Export, Parameters"},
            {"id": "ap_automation", "name": "AP Automation", "description": "Supplier Statement Automation"},
            {"id": "utilities", "name": "Utilities", "description": "Balance Check, User Activity"},
            {"id": "development", "name": "Development", "description": "Opera SE, Archive"},
            {"id": "administration", "name": "Administration", "description": "Company, Projects, Lock Monitor, Settings"},
        ]
    }

@app.get("/api/auth/user-default-company")
async def get_user_default_company(username: str):
    """
    Get a user's default company and default system by username.
    Public endpoint for login page to pre-select company and system dropdowns.
    """
    if not user_auth:
        return {"default_company": None, "default_system": None}

    # Query the users table for the default_company and default_system
    import sqlite3
    try:
        conn = sqlite3.connect(user_auth.DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            'SELECT default_company, default_system FROM users WHERE LOWER(username) = LOWER(?)',
            (username,)
        )
        row = cursor.fetchone()
        conn.close()

        result = {"default_company": None, "default_system": None}
        if row:
            if row[0]:
                result["default_company"] = row[0]
            if row[1]:
                result["default_system"] = row[1]
        return result
    except Exception as e:
        logger.warning(f"Error getting user defaults: {e}")

    return {"default_company": None, "default_system": None}

@app.get("/api/companies/list")
async def get_companies_list():
    """
    Get list of available companies.
    Public endpoint for login page company dropdown (no auth required).
    """
    companies = load_companies()
    # Return simplified company list (just id, name, description)
    return {
        "companies": [
            {
                "id": c.get("id"),
                "name": c.get("name"),
                "description": c.get("description", "")
            }
            for c in companies
        ]
    }

# ============ Admin User Management Endpoints ============

@app.get("/api/admin/users")
async def list_users(request: Request):
    """
    List all users. Admin only.
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    user = getattr(request.state, 'user', None)
    if not user or not user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    users = user_auth.list_users()
    return {"success": True, "users": users}

@app.post("/api/admin/users")
async def create_user(request: Request, user_data: UserCreateRequest):
    """
    Create a new user. Admin only.
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        new_user = user_auth.create_user(
            username=user_data.username,
            password=user_data.password,
            display_name=user_data.display_name,
            email=user_data.email,
            is_admin=user_data.is_admin,
            permissions=user_data.permissions,
            created_by=current_user.get('username'),
            default_company=user_data.default_company,
            ui_mode=user_data.ui_mode,
            voice_enabled=user_data.voice_enabled
        )
        return {"success": True, "user": new_user}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/admin/users/{user_id}")
async def get_user(request: Request, user_id: int):
    """
    Get a specific user. Admin only.
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    user = user_auth.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {"success": True, "user": user}

@app.put("/api/admin/users/{user_id}")
async def update_user(request: Request, user_id: int, user_data: UserUpdateRequest):
    """
    Update a user. Admin only.
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        updated_user = user_auth.update_user(
            user_id=user_id,
            username=user_data.username,
            password=user_data.password,
            display_name=user_data.display_name,
            email=user_data.email,
            is_admin=user_data.is_admin,
            is_active=user_data.is_active,
            permissions=user_data.permissions,
            default_company=user_data.default_company,
            default_system=user_data.default_system,
            ui_mode=user_data.ui_mode,
            voice_enabled=user_data.voice_enabled
        )
        return {"success": True, "user": updated_user}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/admin/users/{user_id}")
async def delete_user(request: Request, user_id: int):
    """
    Deactivate a user. Admin only.
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    # Prevent self-deletion
    if current_user.get('id') == user_id:
        raise HTTPException(status_code=400, detail="Cannot deactivate your own account")

    try:
        success = user_auth.delete_user(user_id)
        if not success:
            raise HTTPException(status_code=404, detail="User not found")
        return {"success": True, "message": "User deactivated"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/admin/users/{user_id}/password")
async def get_user_password(request: Request, user_id: int):
    """
    Get decrypted password for a user. Admin only.
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    password = user_auth.get_user_password(user_id)
    if password is None:
        raise HTTPException(status_code=404, detail="Password not available")

    return {"success": True, "password": password}

@app.get("/api/admin/users/{user_id}/companies")
async def get_user_companies(request: Request, user_id: int):
    """
    Get list of companies a user has access to. Admin only.
    Empty list means access to all companies (no restrictions).
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    companies = user_auth.get_user_companies(user_id)
    return {"success": True, "companies": companies, "has_restrictions": len(companies) > 0}

@app.put("/api/admin/users/{user_id}/companies")
async def set_user_companies(request: Request, user_id: int):
    """
    Set which companies a user can access. Admin only.
    Empty list means access to all companies (no restrictions).
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    data = await request.json()
    company_ids = data.get('companies', [])

    if not isinstance(company_ids, list):
        raise HTTPException(status_code=400, detail="companies must be a list")

    success = user_auth.set_user_companies(user_id, company_ids)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update user companies")

    return {"success": True, "companies": company_ids}

@app.post("/api/admin/users/sync-from-opera")
async def sync_users_from_opera(request: Request):
    """
    Sync users from Opera SE system database.
    Creates/updates SQL RAG users for each Opera user.
    Opera NavGroup permissions determine which SQL RAG modules the user can access.
    Admin only.
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    if not sql_connector:
        return {"success": False, "error": "Database not connected"}

    try:
        # Query Opera users from the system database
        query = """
            SELECT [user], username, manager, email_addr, prefcomp, state, cos
            FROM Opera3SESystem.dbo.sequser
            WHERE state <> 2
            ORDER BY [user]
        """

        result = sql_connector.execute_query(query)
        if result is None or result.empty:
            return {"success": True, "message": "No Opera users found", "created": [], "updated": [], "errors": []}

        # Query all NavGroup permissions at once for efficiency
        navgrp_query = """
            SELECT [user], navgroup, enabled
            FROM Opera3SESystem.dbo.seqnavgrps
        """
        navgrp_result = None
        navgrp_by_user = {}
        try:
            navgrp_result = sql_connector.execute_query(navgrp_query)
            if navgrp_result is not None and not navgrp_result.empty:
                # Build navgroups dict keyed by lowercase username
                for _, navrow in navgrp_result.iterrows():
                    nav_user = navrow['user'].strip().lower() if navrow['user'] else ''
                    navgroup = navrow['navgroup'].strip() if navrow['navgroup'] else ''
                    enabled = bool(navrow.get('enabled', 1)) if 'enabled' in navrow else True
                    if nav_user and navgroup:
                        if nav_user not in navgrp_by_user:
                            navgrp_by_user[nav_user] = {}
                        navgrp_by_user[nav_user][navgroup] = enabled
                logger.info(f"Loaded NavGroup permissions for {len(navgrp_by_user)} users")
        except Exception as navgrp_err:
            logger.warning(f"Could not query Opera NavGroups: {navgrp_err}")

        created = []
        updated = []
        errors = []

        # Get existing SQL RAG users
        existing_users = {u['username'].lower(): u for u in user_auth.list_users()}

        for _, row in result.iterrows():
            opera_user = row['user'].strip() if row['user'] else ''
            display_name = row['username'].strip() if row['username'] else opera_user
            is_manager = bool(row['manager'])
            email = row['email_addr'].strip() if row['email_addr'] else None
            pref_company = row['prefcomp'].strip() if row['prefcomp'] else None
            is_active = row['state'] in [0, 1] if 'state' in row else True

            if not opera_user:
                continue

            # Get cos field for company access
            cos_string = row['cos'].strip() if row.get('cos') else None

            # Load companies for mapping
            companies = load_companies()

            # Map preferred company letter to company ID
            default_company = None
            if pref_company:
                # Try to find matching company config
                for co in companies:
                    db_name = co.get('database', '')
                    if db_name.endswith(pref_company):
                        default_company = co.get('id')
                        break

            # Parse cos field for company access
            # cos is a string where each character (A-Z, 0-9) represents a company the user can access
            user_company_access = None
            if cos_string:
                user_company_access = []
                for char in cos_string:
                    # Find matching company config by database suffix
                    for co in companies:
                        db_name = co.get('database', '')
                        if db_name.endswith(char):
                            user_company_access.append(co.get('id'))
                            break
                if user_company_access:
                    logger.info(f"Opera company access for '{opera_user}' from cos='{cos_string}': {user_company_access}")

            # Get NavGroup permissions for this user
            opera_permissions = None
            user_navgroups = navgrp_by_user.get(opera_user.lower(), {})
            if user_navgroups:
                opera_permissions = UserAuth.map_opera_navgroups_to_permissions(user_navgroups)

            # Sync user from Opera (creates if not exists, updates if exists)
            try:
                is_new = opera_user.lower() not in existing_users
                synced_user = user_auth.sync_user_from_opera(
                    opera_username=opera_user,
                    display_name=display_name,
                    email=email,
                    is_manager=is_manager,
                    is_active=is_active,
                    preferred_company=default_company,
                    opera_permissions=opera_permissions,
                    company_access=user_company_access
                )

                user_info = {
                    'username': opera_user,
                    'display_name': display_name,
                    'is_admin': is_manager,
                    'default_company': default_company,
                    'permissions': opera_permissions or {}
                }

                if is_new:
                    created.append(user_info)
                    logger.info(f"Created user from Opera: {opera_user}")
                else:
                    updated.append(user_info)
                    logger.info(f"Updated user from Opera: {opera_user}")
            except ValueError as e:
                errors.append(f"{opera_user}: {str(e)}")

        return {
            "success": True,
            "message": f"Synced {len(created)} new users, updated {len(updated)} existing users from Opera",
            "created": created,
            "updated": updated,
            "errors": errors
        }

    except Exception as e:
        logger.error(f"Error syncing users from Opera: {e}")
        return {"success": False, "error": str(e)}

@app.get("/api/admin/users/opera-users")
async def list_opera_users(request: Request):
    """
    List users from Opera SE system database (preview before sync).
    Includes NavGroup permissions that will be mapped to SQL RAG modules.
    Admin only.
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    if not sql_connector:
        return {"success": False, "error": "Database not connected"}

    try:
        query = """
            SELECT [user], username, manager, email_addr, prefcomp
            FROM Opera3SESystem.dbo.sequser
            WHERE state <> 2
            ORDER BY [user]
        """

        result = sql_connector.execute_query(query)
        if result is None or result.empty:
            return {"success": True, "users": [], "navgroup_mapping": UserAuth.OPERA_NAVGROUP_TO_MODULE}

        # Query all NavGroup permissions at once for efficiency
        navgrp_by_user = {}
        try:
            navgrp_query = """
                SELECT [user], navgroup, enabled
                FROM Opera3SESystem.dbo.seqnavgrps
            """
            navgrp_result = sql_connector.execute_query(navgrp_query)
            if navgrp_result is not None and not navgrp_result.empty:
                for _, navrow in navgrp_result.iterrows():
                    nav_user = navrow['user'].strip().lower() if navrow['user'] else ''
                    navgroup = navrow['navgroup'].strip() if navrow['navgroup'] else ''
                    enabled = bool(navrow.get('enabled', 1)) if 'enabled' in navrow else True
                    if nav_user and navgroup:
                        if nav_user not in navgrp_by_user:
                            navgrp_by_user[nav_user] = {}
                        navgrp_by_user[nav_user][navgroup] = enabled
        except Exception as navgrp_err:
            logger.warning(f"Could not query Opera NavGroups: {navgrp_err}")

        # Get existing SQL RAG users for comparison
        existing_users = {u['username'].lower() for u in user_auth.list_users()}

        users = []
        for _, row in result.iterrows():
            opera_user = row['user'].strip() if row['user'] else ''
            if not opera_user:
                continue

            # Get NavGroup permissions for this user
            user_navgroups = navgrp_by_user.get(opera_user.lower(), {})
            opera_permissions = UserAuth.map_opera_navgroups_to_permissions(user_navgroups) if user_navgroups else None

            users.append({
                'username': opera_user,
                'display_name': row['username'].strip() if row['username'] else opera_user,
                'is_manager': bool(row['manager']),
                'email': row['email_addr'].strip() if row['email_addr'] else None,
                'preferred_company': row['prefcomp'].strip() if row['prefcomp'] else None,
                'exists_in_sqlrag': opera_user.lower() in existing_users,
                'opera_navgroups': user_navgroups,
                'mapped_permissions': opera_permissions
            })

        return {
            "success": True,
            "users": users,
            "navgroup_mapping": UserAuth.OPERA_NAVGROUP_TO_MODULE
        }

    except Exception as e:
        logger.error(f"Error listing Opera users: {e}")
        return {"success": False, "error": str(e)}

# ============ License Management Endpoints ============

class LicenseCreate(BaseModel):
    """Model for creating a license."""
    client_name: str = Field(..., description="Client/company name")
    opera_version: str = Field(default="SE", description="Opera version: 'SE' or '3'")
    max_users: int = Field(default=5, description="Maximum concurrent users")
    notes: Optional[str] = Field(None, description="Optional notes")

class LicenseUpdate(BaseModel):
    """Model for updating a license."""
    client_name: Optional[str] = None
    opera_version: Optional[str] = None
    max_users: Optional[int] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None

@app.get("/api/licenses")
async def get_licenses_public():
    """
    Get list of active licenses for login dropdown.
    Public endpoint - no auth required.
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    licenses = user_auth.list_licenses(active_only=True)
    return {"licenses": licenses}

@app.get("/api/admin/licenses")
async def get_licenses(request: Request):
    """
    Get all licenses (admin only).
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    licenses = user_auth.list_licenses(active_only=False)
    return {"licenses": licenses}

@app.post("/api/admin/licenses")
async def create_license(request: Request, license_data: LicenseCreate):
    """
    Create a new license (admin only).
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        license = user_auth.create_license(
            client_name=license_data.client_name,
            opera_version=license_data.opera_version,
            max_users=license_data.max_users,
            notes=license_data.notes
        )
        return {"success": True, "license": license}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/admin/licenses/{license_id}")
async def get_license(request: Request, license_id: int):
    """
    Get a specific license (admin only).
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    license = user_auth.get_license(license_id)
    if not license:
        raise HTTPException(status_code=404, detail=f"License {license_id} not found")

    # Get active session count
    license['active_sessions'] = user_auth.get_active_session_count(license_id)

    return {"license": license}

@app.put("/api/admin/licenses/{license_id}")
async def update_license(request: Request, license_id: int, license_data: LicenseUpdate):
    """
    Update a license (admin only).
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        license = user_auth.update_license(
            license_id=license_id,
            client_name=license_data.client_name,
            opera_version=license_data.opera_version,
            max_users=license_data.max_users,
            is_active=license_data.is_active,
            notes=license_data.notes
        )
        return {"success": True, "license": license}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/admin/licenses/{license_id}")
async def delete_license(request: Request, license_id: int):
    """
    Deactivate a license (admin only).
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    current_user = getattr(request.state, 'user', None)
    if not current_user or not current_user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    success = user_auth.delete_license(license_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"License {license_id} not found")

    return {"success": True, "message": "License deactivated"}

@app.get("/api/session/license")
async def get_session_license(request: Request):
    """
    Get the license associated with the current session.
    """
    if not user_auth:
        raise HTTPException(status_code=500, detail="Authentication system not initialized")

    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        return {"license": None}

    license = user_auth.get_session_license(token)
    return {"license": license}

# ============ Projects Endpoints ============

@app.get("/api/projects")
async def get_projects():
    """Get list of development projects to track."""
    import json
    projects_file = Path(__file__).parent.parent / "docs" / "projects.json"
    if projects_file.exists():
        with open(projects_file, 'r') as f:
            return json.load(f)
    return []

@app.get("/api/projects/{project_id}")
async def get_project(project_id: str):
    """Get a specific project by ID."""
    import json
    projects_file = Path(__file__).parent.parent / "docs" / "projects.json"
    if projects_file.exists():
        with open(projects_file, 'r') as f:
            projects = json.load(f)
            for project in projects:
                if project.get("id") == project_id:
                    # Also try to load the doc content if it exists
                    if project.get("doc_link"):
                        doc_path = Path(__file__).parent.parent / project["doc_link"].lstrip("/")
                        if doc_path.exists():
                            with open(doc_path, 'r') as df:
                                project["doc_content"] = df.read()
                    return project
    raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found")

@app.post("/api/projects")
async def create_project(project: Dict[str, Any]):
    """Create or update a project."""
    import json
    from datetime import date
    projects_file = Path(__file__).parent.parent / "docs" / "projects.json"

    projects = []
    if projects_file.exists():
        with open(projects_file, 'r') as f:
            projects = json.load(f)

    # Check if project exists
    existing_idx = None
    for i, p in enumerate(projects):
        if p.get("id") == project.get("id"):
            existing_idx = i
            break

    project["updated"] = date.today().isoformat()
    if existing_idx is not None:
        projects[existing_idx] = project
    else:
        project["created"] = date.today().isoformat()
        projects.append(project)

    with open(projects_file, 'w') as f:
        json.dump(projects, f, indent=2)

    return {"success": True, "project": project}

# ============ Configuration Endpoints ============

@app.get("/api/config")
async def get_config():
    """Get current configuration (sensitive values masked)."""
    if not config:
        raise HTTPException(status_code=500, detail="Configuration not loaded")

    result = {}
    for section in config.sections():
        result[section] = {}
        for key, value in config.items(section):
            # Mask sensitive values
            if "key" in key.lower() or "password" in key.lower():
                result[section][key] = "***" if value else ""
            else:
                result[section][key] = value
    return result

@app.get("/api/config/providers")
async def get_available_providers():
    """Get list of available LLM providers."""
    return {
        "providers": [
            {"id": "local", "name": "Local (Ollama)", "requires_api_key": False},
            {"id": "openai", "name": "OpenAI", "requires_api_key": True},
            {"id": "anthropic", "name": "Anthropic Claude", "requires_api_key": True},
            {"id": "gemini", "name": "Google Gemini", "requires_api_key": True},
            {"id": "groq", "name": "Groq", "requires_api_key": True},
        ]
    }

@app.get("/api/config/models/{provider}")
async def get_provider_models(provider: str):
    """Get available models for a provider."""
    models_map = {
        "local": ["llama3:latest", "llama3:8b", "llama3:70b", "mistral:7b-instruct-v0.2", "codellama:7b", "phi:3"],
        "openai": ["gpt-4o", "gpt-4-turbo", "gpt-4", "gpt-3.5-turbo"],
        "anthropic": ["claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022", "claude-3-opus-20240229", "claude-3-sonnet-20240229", "claude-3-haiku-20240307"],
        "gemini": ["gemini-3-pro", "gemini-3-flash", "gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash"],
        "groq": ["llama-3.3-70b-versatile", "llama-3.1-70b-versatile", "llama-3.1-8b-instant", "mixtral-8x7b-32768", "gemma2-9b-it"],
    }

    if provider not in models_map:
        raise HTTPException(status_code=404, detail=f"Unknown provider: {provider}")

    return {"provider": provider, "models": models_map[provider]}

@app.post("/api/config/llm")
async def update_llm_config(provider_config: ProviderConfig):
    """Update LLM configuration."""
    global config, llm

    if not config:
        config = load_config()

    # Update models section
    if not config.has_section("models"):
        config.add_section("models")
    config["models"]["provider"] = provider_config.provider

    # Update provider-specific section
    if provider_config.provider != "local":
        if not config.has_section(provider_config.provider):
            config.add_section(provider_config.provider)
        if provider_config.api_key:
            config[provider_config.provider]["api_key"] = provider_config.api_key
        config[provider_config.provider]["model"] = provider_config.model
    else:
        config["models"]["llm_model"] = provider_config.model
        # Save Ollama URL if provided
        if provider_config.ollama_url:
            config["models"]["llm_api_url"] = provider_config.ollama_url

    # Update system settings
    if not config.has_section("system"):
        config.add_section("system")
    config["system"]["temperature"] = str(provider_config.temperature)
    config["system"]["max_token_limit"] = str(provider_config.max_tokens)

    # Save config
    save_config(config)

    # Reinitialize LLM
    try:
        llm = create_llm_instance(config)
        return {"success": True, "message": "LLM configuration updated"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/config/database")
async def update_database_config(db_config: DatabaseConfig):
    """Update database configuration."""
    global config

    if not config:
        config = load_config()

    if not config.has_section("database"):
        config.add_section("database")

    config["database"]["type"] = db_config.type
    if db_config.server:
        config["database"]["server"] = db_config.server
    if db_config.database:
        config["database"]["database"] = db_config.database
    if db_config.username:
        config["database"]["username"] = db_config.username
    if db_config.password:
        config["database"]["password"] = db_config.password
    config["database"]["use_windows_auth"] = str(db_config.use_windows_auth)

    # Advanced connection settings
    config["database"]["pool_size"] = str(db_config.pool_size)
    config["database"]["max_overflow"] = str(db_config.max_overflow)
    config["database"]["pool_timeout"] = str(db_config.pool_timeout)
    config["database"]["connection_timeout"] = str(db_config.connection_timeout)
    config["database"]["command_timeout"] = str(db_config.command_timeout)
    config["database"]["ssl"] = str(db_config.ssl).lower()
    if db_config.ssl_ca:
        config["database"]["ssl_ca"] = db_config.ssl_ca
    if db_config.ssl_cert:
        config["database"]["ssl_cert"] = db_config.ssl_cert
    if db_config.ssl_key:
        config["database"]["ssl_key"] = db_config.ssl_key
    if db_config.port:
        config["database"]["port"] = str(db_config.port)

    save_config(config)

    # Sync the active system profile with the updated config
    _sync_active_system_config()

    # Reinitialize SQL connector for the active company
    try:
        new_conn = SQLConnector(CONFIG_PATH)
        co = _get_active_company_id() or _default_company_id
        if co:
            _company_sql_connectors[co] = new_conn
        return {"success": True, "message": "Database configuration updated"}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ============ Opera Configuration Endpoints ============

@app.get("/api/config/opera")
async def get_opera_config():
    """Get current Opera configuration."""
    if not config:
        raise HTTPException(status_code=500, detail="Configuration not loaded")

    opera_section = {}
    if config.has_section("opera"):
        for key, value in config.items("opera"):
            opera_section[key] = value

    return {
        "version": opera_section.get("version", "sql_se"),
        "opera3_server_path": opera_section.get("opera3_server_path", ""),
        "opera3_base_path": opera_section.get("opera3_base_path", ""),
        "opera3_company_code": opera_section.get("opera3_company_code", ""),
    }

@app.post("/api/config/opera")
async def update_opera_config(opera_config: OperaConfig):
    """Update Opera configuration."""
    global config

    if not config:
        config = load_config()

    if not config.has_section("opera"):
        config.add_section("opera")

    config["opera"]["version"] = opera_config.version

    if opera_config.opera3_server_path is not None:
        config["opera"]["opera3_server_path"] = opera_config.opera3_server_path
    if opera_config.opera3_base_path:
        config["opera"]["opera3_base_path"] = opera_config.opera3_base_path
    if opera_config.opera3_company_code:
        config["opera"]["opera3_company_code"] = opera_config.opera3_company_code
    if opera_config.opera3_share_user is not None:
        config["opera"]["opera3_share_user"] = opera_config.opera3_share_user
    if opera_config.opera3_share_password is not None:
        config["opera"]["opera3_share_password"] = opera_config.opera3_share_password

    save_config(config)

    # Auto-connect SMB if Opera 3 with server path and credentials
    smb_message = ""
    if (opera_config.version == "opera3"
        and opera_config.opera3_server_path
        and opera_config.opera3_share_user
        and opera_config.opera3_share_password):
        try:
            smb_message = _connect_opera3_smb(
                opera_config.opera3_server_path,
                opera_config.opera3_share_user,
                opera_config.opera3_share_password
            )
            # Set opera3_base_path in memory only (not persisted to disk)
            # — the temp dir is ephemeral and will be recreated on each startup
            smb = get_smb_manager()
            if smb is not None and smb.get_local_base():
                config["opera"]["opera3_base_path"] = str(smb.get_local_base())
        except Exception as e:
            smb_message = f"SMB connection failed: {e}"

    # Sync the active system profile with the updated config
    _sync_active_system_config()

    return {"success": True, "message": f"Opera configuration updated. {smb_message}".strip()}

@app.get("/api/config/opera/companies")
async def get_opera3_companies():
    """Get list of Opera 3 companies (requires opera3_base_path to be configured)."""
    if not config:
        raise HTTPException(status_code=500, detail="Configuration not loaded")

    if not config.has_section("opera"):
        return {"companies": [], "error": "Opera not configured"}

    base_path = config.get("opera", "opera3_base_path", fallback="")
    if not base_path:
        return {"companies": [], "error": "Opera 3 base path not configured"}

    try:
        from sql_rag.opera3_foxpro import Opera3System
        system = Opera3System(base_path)
        companies = system.get_companies()
        return {
            "companies": [
                {"code": c["code"], "name": c["name"], "data_path": c.get("data_path", "")}
                for c in companies
            ]
        }
    except ImportError:
        return {"companies": [], "error": "dbfread package not installed (required for Opera 3)"}
    except FileNotFoundError as e:
        return {"companies": [], "error": f"Path not found: {str(e)}"}
    except Exception as e:
        return {"companies": [], "error": str(e)}

@app.post("/api/config/opera/test")
async def test_opera_connection(opera_config: OperaConfig):
    """Test Opera connection with the provided configuration."""
    if opera_config.version == "sql_se":
        # Test SQL Server connection (use existing database config)
        if sql_connector:
            try:
                # Simple query to test connection
                df = sql_connector.execute_query("SELECT 1 as test")
                return {"success": True, "message": "SQL Server connection successful"}
            except Exception as e:
                return {"success": False, "error": friendly_db_error(e)}
        else:
            return {"success": False, "error": "SQL connector not initialized"}
    else:
        # Test Opera 3 connection
        # Try SMB connection first if credentials provided
        if (opera_config.opera3_server_path
            and opera_config.opera3_share_user
            and opera_config.opera3_share_password):
            try:
                smb_msg = _connect_opera3_smb(
                    opera_config.opera3_server_path,
                    opera_config.opera3_share_user,
                    opera_config.opera3_share_password
                )
                smb = get_smb_manager()
                if smb is not None and smb.is_connected():
                    test_base_path = str(smb.get_local_base())
                else:
                    return {"success": False, "error": smb_msg}
            except Exception as e:
                return {"success": False, "error": f"SMB connection failed: {e}"}
        elif opera_config.opera3_base_path:
            test_base_path = opera_config.opera3_base_path
        else:
            return {"success": False, "error": "Opera 3 base path or server path not provided"}

        try:
            from sql_rag.opera3_foxpro import Opera3System, Opera3Reader
            system = Opera3System(test_base_path)

            # Test getting companies
            companies = system.get_companies()
            if not companies:
                return {"success": False, "error": "No companies found in Opera 3 installation"}

            # If company code provided, test reading from it
            if opera_config.opera3_company_code:
                reader = system.get_reader_for_company(opera_config.opera3_company_code)
                # Try to read suppliers to verify data access
                suppliers = reader.read_table("pname", limit=1)
                return {
                    "success": True,
                    "message": f"Opera 3 connection successful. Found {len(companies)} companies.",
                    "companies_count": len(companies)
                }
            else:
                return {
                    "success": True,
                    "message": f"Opera 3 path valid. Found {len(companies)} companies.",
                    "companies_count": len(companies)
                }

        except ImportError:
            return {"success": False, "error": "dbfread package not installed (required for Opera 3)"}
        except FileNotFoundError as e:
            return {"success": False, "error": f"Path not found: {str(e)}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

# ============ Systems (Named Config Profiles) ============

@app.get("/api/systems")
async def get_systems():
    """List all configured systems and which is active."""
    systems = load_systems()
    return {
        "systems": systems,
        "active_system_id": active_system_id,
    }

@app.get("/api/systems/active")
async def get_active_system_endpoint():
    """Get the currently active system profile, verified against the live database connection.

    CRITICAL: This endpoint confirms that what the UI displays matches what
    the backend is actually connected to. If there is any mismatch, it is
    reported so the user is never misled about which database they are working in.
    """
    system = get_active_system()

    # Verify the live connection matches what the system profile claims
    verified = False
    actual_server = None
    actual_db = None
    mismatch_warning = None

    if sql_connector:
        try:
            df = sql_connector.execute_query("SELECT @@SERVERNAME as sname, DB_NAME() as dbname")
            if df is not None and len(df) > 0:
                actual_server = str(df.iloc[0]["sname"]).strip()
                actual_db = str(df.iloc[0]["dbname"]).strip()

                expected_db = system.get("database", {}).get("database", "").strip() if system else ""

                if actual_db.lower() == expected_db.lower():
                    verified = True
                else:
                    mismatch_warning = (
                        f"MISMATCH: System '{system.get('name', '?')}' expects database "
                        f"'{expected_db}' but connected to '{actual_db}' on server '{actual_server}'"
                    )
                    logger.error(f"Connection verification FAILED: {mismatch_warning}")
        except Exception as e:
            logger.warning(f"Could not verify database connection: {e}")

    response = {"system": system, "verified": verified}
    if actual_server:
        response["actual_server"] = actual_server
    if actual_db:
        response["actual_database"] = actual_db
    if mismatch_warning:
        response["mismatch_warning"] = mismatch_warning
    return response

class SystemCreate(BaseModel):
    name: str
    database: Dict[str, Any] = {}
    opera: Dict[str, Any] = {}
    is_default: bool = False

@app.post("/api/systems")
async def create_system(body: SystemCreate):
    """Create a new system profile."""
    systems = load_systems()

    # Generate a simple ID from the name
    system_id = body.name.lower().replace(" ", "_").replace("-", "_")
    # Ensure unique
    existing_ids = {s["id"] for s in systems}
    base_id = system_id
    counter = 1
    while system_id in existing_ids:
        system_id = f"{base_id}_{counter}"
        counter += 1

    # If this is marked as default, clear default from others
    if body.is_default:
        for s in systems:
            s["is_default"] = False

    new_system = {
        "id": system_id,
        "name": body.name,
        "is_default": body.is_default,
        "database": body.database,
        "opera": body.opera,
    }
    systems.append(new_system)
    save_systems(systems)

    return {"success": True, "system": new_system}

@app.put("/api/systems/{system_id}")
async def update_system(system_id: str, body: SystemCreate):
    """Update an existing system profile."""
    systems = load_systems()
    target = None
    for s in systems:
        if s["id"] == system_id:
            target = s
            break

    if not target:
        raise HTTPException(status_code=404, detail="System not found")

    # If setting as default, clear others
    if body.is_default:
        for s in systems:
            s["is_default"] = False

    target["name"] = body.name
    target["is_default"] = body.is_default
    target["database"] = body.database
    target["opera"] = body.opera

    save_systems(systems)

    # If this is the active system, re-apply config
    if active_system_id == system_id:
        apply_system_to_config(target)

    return {"success": True, "system": target}

@app.delete("/api/systems/{system_id}")
async def delete_system(system_id: str):
    """Delete a system profile. Cannot delete the last one."""
    systems = load_systems()

    if len(systems) <= 1:
        return {"success": False, "error": "Cannot delete the only system."}

    if system_id == active_system_id:
        return {"success": False, "error": "Cannot delete the currently active system. Switch to another system first."}

    systems = [s for s in systems if s["id"] != system_id]

    # Ensure at least one default
    if not any(s.get("is_default") for s in systems):
        systems[0]["is_default"] = True

    save_systems(systems)
    return {"success": True}

@app.post("/api/systems/{system_id}/activate")
async def activate_system(system_id: str):
    """Switch to a different system. Updates config.ini and reinitialises the SQL connector."""
    systems = load_systems()
    target = None
    for s in systems:
        if s["id"] == system_id:
            target = s
            break

    if not target:
        raise HTTPException(status_code=404, detail="System not found")

    try:
        apply_system_to_config(target)
    except ConnectionError as e:
        return JSONResponse(
            status_code=503,
            content={
                "success": False,
                "error": friendly_db_error(str(e)),
                "detail": f"Switched configuration to {target['name']} but the database connection failed. "
                          "Please check the server address and credentials in Installation settings.",
            },
        )

    return {"success": True, "message": f"Switched to {target['name']}"}

# ============ Company Management Endpoints ============

@app.get("/api/companies")
async def get_companies(request: Request):
    """Get list of available companies, filtered by session's license opera_version and user access."""
    companies = load_companies()

    # Filter by license's opera_version if session has a license
    if user_auth:
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if token:
            license_data = user_auth.get_session_license(token)
            if license_data and license_data.get('opera_version'):
                opera_version = license_data['opera_version']
                # Filter companies to only those matching the license's Opera version
                companies = [
                    c for c in companies
                    if c.get('opera_version', 'SE') == opera_version
                ]

            # Filter by user's company access
            current_user = getattr(request.state, 'user', None)
            if current_user:
                companies = user_auth.get_user_accessible_companies(
                    current_user['id'],
                    companies
                )

    return {
        "companies": companies,
        "current_company": current_company
    }

@app.post("/api/companies/discover")
async def discover_opera_companies():
    """
    Auto-discover Opera companies from both SQL Server (Opera SE) and FoxPro (Opera 3).
    Creates config files for any new companies found.
    """
    discovered_se = []
    discovered_o3 = []
    created = []
    existing = []
    errors = []

    os.makedirs(COMPANIES_DIR, exist_ok=True)

    # Load existing company configs to check for duplicates by database
    existing_configs = load_companies()
    existing_databases = {c.get('database'): c.get('id') for c in existing_configs if c.get('database')}
    existing_o3_codes = {c.get('opera3_company_code'): c.get('id') for c in existing_configs if c.get('opera3_company_code')}

    # ========== Discover Opera SE companies from SQL Server ==========
    if sql_connector:
        try:
            query = """
                SELECT name FROM sys.databases
                WHERE name LIKE 'Opera3SECompany00%'
                AND state_desc = 'ONLINE'
                ORDER BY name
            """

            from sqlalchemy import text as sa_text
            with sql_connector.engine.connect() as conn:
                result = conn.execute(sa_text(query))
                databases = [row[0] for row in result]

            for db_name in databases:
                company_letter = db_name.replace('Opera3SECompany00', '')
                company_id = f"se_{company_letter.lower()}"

                # Check if this database already has a config (by any ID)
                existing_id = existing_databases.get(db_name)
                if existing_id:
                    config_path = os.path.join(COMPANIES_DIR, f"{existing_id}.json")
                else:
                    config_path = os.path.join(COMPANIES_DIR, f"{company_id}.json")

                # Try to get company name from the database
                company_name = f"Company {company_letter}"
                try:
                    temp_query = f"SELECT TOP 1 sy_coname FROM [{db_name}].dbo.syspar"
                    with sql_connector.engine.connect() as conn:
                        result = conn.execute(sa_text(temp_query))
                        row = result.fetchone()
                        if row and row[0]:
                            company_name = row[0].strip()
                except Exception as e:
                    logger.warning(f"Could not read company name from {db_name}: {e}")

                config_exists = existing_id is not None or os.path.exists(config_path)
                discovered_se.append({
                    "database": db_name,
                    "letter": company_letter,
                    "name": company_name,
                    "opera_version": "SE",
                    "config_exists": config_exists,
                    "config_id": existing_id or company_id
                })

                if not config_exists:
                    new_config = {
                        "id": company_id,
                        "name": company_name,
                        "database": db_name,
                        "opera_version": "SE",
                        "description": f"{company_name} (Opera SE)",
                        "settings": {
                            "currency": "GBP",
                            "currency_symbol": "\u00a3",
                            "date_format": "DD/MM/YYYY",
                            "financial_year_start_month": 1
                        },
                        "payroll": {"pension_provider": "", "pension_export_folder": ""},
                        "dashboard_config": {
                            "default_year": 2026,
                            "show_margin_analysis": True,
                            "show_customer_lifecycle": True,
                            "revenue_categories_field": "sg_group",
                            "margin_categories_field": "sg_group"
                        },
                        "modules": {
                            "debtors_control": True,
                            "creditors_control": True,
                            "sales_dashboards": True,
                            "trial_balance": True,
                            "email_integration": True
                        }
                    }

                    with open(config_path, 'w') as f:
                        json.dump(new_config, f, indent=2)
                    created.append(f"{company_name} (SE)")
                    logger.info(f"Created config for Opera SE company: {company_name}")
                else:
                    existing.append(f"{company_name} (SE)")

        except Exception as e:
            errors.append(f"Opera SE discovery error: {str(e)}")
            logger.error(f"Error discovering Opera SE companies: {e}")

    # ========== Discover Opera 3 companies from FoxPro ==========
    if config and config.has_option("opera", "opera3_base_path"):
        opera3_base = config.get("opera", "opera3_base_path")
        if opera3_base and os.path.exists(opera3_base):
            try:
                from sql_rag.opera3_foxpro import Opera3System
                system = Opera3System(opera3_base)
                o3_companies = system.get_companies()

                for co in o3_companies:
                    company_code = co.get("code", "").strip()
                    company_name = co.get("name", f"Company {company_code}").strip()
                    data_path = co.get("data_path", "")

                    company_id = f"o3_{company_code.lower()}"
                    config_path = os.path.join(COMPANIES_DIR, f"{company_id}.json")

                    existing_o3_id = existing_o3_codes.get(company_code)
                    if existing_o3_id:
                        config_path = os.path.join(COMPANIES_DIR, f"{existing_o3_id}.json")

                    config_exists = existing_o3_id is not None or os.path.exists(config_path)
                    discovered_o3.append({
                        "code": company_code,
                        "name": company_name,
                        "data_path": data_path,
                        "opera_version": "3",
                        "config_exists": config_exists,
                        "config_id": existing_o3_id or company_id
                    })

                    if not config_exists:
                        new_config = {
                            "id": company_id,
                            "name": company_name,
                            "opera3_company_code": company_code,
                            "opera3_data_path": data_path,
                            "opera_version": "3",
                            "description": f"{company_name} (Opera 3)",
                            "settings": {
                                "currency": "GBP",
                                "currency_symbol": "\u00a3",
                                "date_format": "DD/MM/YYYY",
                                "financial_year_start_month": 1
                            },
                            "payroll": {"pension_provider": "", "pension_export_folder": ""},
                            "dashboard_config": {
                                "default_year": 2026,
                                "show_margin_analysis": True,
                                "show_customer_lifecycle": True,
                                "revenue_categories_field": "sg_group",
                                "margin_categories_field": "sg_group"
                            },
                            "modules": {
                                "debtors_control": True,
                                "creditors_control": True,
                                "sales_dashboards": True,
                                "trial_balance": True,
                                "email_integration": True
                            }
                        }

                        with open(config_path, 'w') as f:
                            json.dump(new_config, f, indent=2)
                        created.append(f"{company_name} (O3)")
                        logger.info(f"Created config for Opera 3 company: {company_name}")
                    else:
                        existing.append(f"{company_name} (O3)")

            except Exception as e:
                errors.append(f"Opera 3 discovery error: {str(e)}")
                logger.error(f"Error discovering Opera 3 companies: {e}")

    total_discovered = len(discovered_se) + len(discovered_o3)
    return {
        "success": len(errors) == 0,
        "opera_se": discovered_se,
        "opera_3": discovered_o3,
        "created": created,
        "existing": existing,
        "errors": errors,
        "message": f"Found {len(discovered_se)} Opera SE and {len(discovered_o3)} Opera 3 companies. Created {len(created)} new configs."
    }

@app.get("/api/companies/current")
async def get_current_company():
    """Get the currently active company."""
    global current_company
    if not current_company:
        # Try to determine current company from database name
        if config and config.has_option("database", "database"):
            db_name = config.get("database", "database")
            companies = load_companies()
            for company in companies:
                if company.get("database") == db_name:
                    current_company = company
                    break

    # Include per-company data directory info
    from sql_rag.company_data import get_current_company_id, get_company_data_dir
    company_id = get_current_company_id()
    data_dir = str(get_company_data_dir(company_id)) if company_id else None

    return {
        "company": current_company,
        "company_id": company_id,
        "data_directory": data_dir
    }

@app.post("/api/companies/switch/{company_id}")
async def switch_company(request: Request, company_id: str):
    """Switch the current user's session to a different company/database.

    Creates per-company SQL connector and email storage if they don't
    already exist, and records the company_id on the user's session so
    subsequent requests automatically use the correct resources.
    """
    global current_company, config, vector_db, _default_company_id

    # Load the company configuration
    company = load_company(company_id)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{company_id}' not found")

    # Check if user has access to this company
    if user_auth:
        current_user = getattr(request.state, 'user', None)
        if current_user and not user_auth.user_has_company_access(current_user['id'], company_id):
            raise HTTPException(status_code=403, detail=f"You don't have access to company '{company.get('name', company_id)}'")

    # ---- Opera 3: set data path, no SQL connector ----
    opera_version = company.get("opera_version", "SE")
    if opera_version in ("3", "Opera 3"):
        smb = get_smb_manager()
        if smb is None or not smb.is_connected():
            raise HTTPException(status_code=503, detail="Opera 3 SMB connection not available")

        data_path_rel = company.get("opera3_data_path", "")
        local_base = smb.get_local_base()
        if data_path_rel:
            opera3_data_path = str(Path(local_base) / data_path_rel)
        else:
            opera3_data_path = str(local_base)

        # Update process-level defaults
        _default_company_id = company_id
        _company_data[company_id] = company

        # Store Opera 3 data path in config (memory only)
        if config:
            if not config.has_section("opera"):
                config.add_section("opera")
            config["opera"]["opera3_base_path"] = opera3_data_path

        # Don't null sql_connector — it's needed when user switches back to SE
        # Opera 3 endpoints check for SMB manager, SE endpoints check for sql_connector

        # Save company to user's session
        auth_header = request.headers.get('Authorization', '')
        session_token = auth_header.replace('Bearer ', '') if auth_header.startswith('Bearer ') else ''
        if session_token and user_auth:
            user_auth.set_session_company(session_token, company_id)

        # Switch per-company data directory
        data_dir = get_company_data_dir(company_id)
        migrate_root_databases(company_id)
        logger.info(f"Per-company data directory: {data_dir}")

        # Create / reuse per-company email storage
        if company_id not in _company_email_storages:
            email_db = get_company_db_path(company_id, "email_data.db")
            _company_email_storages[company_id] = EmailStorage(str(email_db))
            logger.info(f"Created email storage for company {company_id}")

        # Set module-level globals
        _ensure_company_context(company_id)

        # Re-register email providers
        if email_sync_manager:
            email_sync_manager.providers.clear()
            try:
                await _initialize_email_providers()
            except Exception as e:
                logger.warning(f"Could not re-register email providers for {company_id}: {e}")

        # Reinitialize VectorDB
        try:
            chroma_dir = str(get_company_chroma_dir(company_id))
            vector_db = VectorDB(config, persist_dir=chroma_dir)
        except Exception as e:
            logger.warning(f"Could not reinitialize VectorDB on company switch: {e}")

        _sync_active_system_config()

        logger.info(f"Switched to Opera 3 company {company_id} ({company['name']}) at {opera3_data_path}")
        return {
            "success": True,
            "message": f"Switched to {company['name']}",
            "company": company
        }

    # ---- SE: existing SQL connector logic ----
    # Get the database name for this company
    database_name = company.get("database")
    if not database_name:
        raise HTTPException(status_code=400, detail="Company has no database configured")

    # Update config with new database (needed for SQLConnector init)
    if not config:
        config = load_config()

    old_database = config.get("database", "database", fallback="")
    config["database"]["database"] = database_name
    save_config(config)

    try:
        # --- Create / reuse per-company SQL connector ---
        if company_id not in _company_sql_connectors:
            _company_sql_connectors[company_id] = SQLConnector(CONFIG_PATH)
            logger.info(f"Created SQL connector for company {company_id}")
        else:
            # Connector exists — verify it's still alive, recreate if not
            try:
                _company_sql_connectors[company_id].execute_query("SELECT 1")
            except Exception:
                _company_sql_connectors[company_id] = SQLConnector(CONFIG_PATH)
                logger.info(f"Recreated SQL connector for company {company_id}")

        # Update process-level defaults (used by unauthenticated endpoints)
        _default_company_id = company_id
        _company_data[company_id] = company

        # Save company to user's session so per-request lookups work
        auth_header = request.headers.get('Authorization', '')
        session_token = auth_header.replace('Bearer ', '') if auth_header.startswith('Bearer ') else ''
        if session_token and user_auth:
            user_auth.set_session_company(session_token, company_id)

        # Switch per-company data directory
        data_dir = get_company_data_dir(company_id)
        migrate_root_databases(company_id)
        logger.info(f"Per-company data directory: {data_dir}")

        # --- Create / reuse per-company email storage ---
        if company_id not in _company_email_storages:
            email_db = get_company_db_path(company_id, "email_data.db")
            _company_email_storages[company_id] = EmailStorage(str(email_db))
            logger.info(f"Created email storage for company {company_id}")

        # Set module-level globals and reset singletons via the helper
        _ensure_company_context(company_id)

        # Re-register email providers for the new company
        if email_sync_manager:
            email_sync_manager.providers.clear()
            try:
                await _initialize_email_providers()
                logger.info(f"Email sync manager updated for company {company_id}")
            except Exception as e:
                logger.warning(f"Could not re-register email providers for {company_id}: {e}")

        # Reinitialize VectorDB with new company's ChromaDB directory
        try:
            chroma_dir = str(get_company_chroma_dir(company_id))
            vector_db = VectorDB(config, persist_dir=chroma_dir)
            logger.info(f"VectorDB reinitialized for company {company_id}")
        except Exception as e:
            logger.warning(f"Could not reinitialize VectorDB on company switch: {e}")

        _sync_active_system_config()

        logger.info(f"Switched to {database_name} ({company['name']})")
        return {
            "success": True,
            "message": f"Switched to {company['name']}",
            "company": company
        }
    except Exception as e:
        # Rollback config on failure
        config["database"]["database"] = old_database
        save_config(config)
        logger.error(f"Failed to switch company: {e}")
        raise HTTPException(status_code=500, detail=friendly_db_error(e))

@app.post("/api/companies/scan-learned-data")
async def scan_learned_data(request: Request, body: dict = Body(...)):
    """Scan a source SQL RAG installation for importable learned data.

    Body: { "source_path": "/path/to/other/llmragsql", "company_id": "intsys" }
    """
    source_path = body.get("source_path", "").strip()
    company_id = body.get("company_id", "").strip()

    if not source_path:
        raise HTTPException(status_code=400, detail="source_path is required")
    if not company_id:
        raise HTTPException(status_code=400, detail="company_id is required")

    from sql_rag.company_data import scan_source_installation
    result = scan_source_installation(source_path, company_id)

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result

@app.post("/api/companies/import-learned-data")
async def api_import_learned_data(request: Request, body: dict = Body(...)):
    """Import learned data from a source installation into the current company.

    Body: {
        "source_path": "/path/to/other/llmragsql",
        "source_company_id": "intsys",
        "databases": ["bank_patterns.db", "bank_aliases.db"]
    }
    """
    source_path = body.get("source_path", "").strip()
    source_company_id = body.get("source_company_id", "").strip()
    databases = body.get("databases", [])

    if not source_path:
        raise HTTPException(status_code=400, detail="source_path is required")
    if not source_company_id:
        raise HTTPException(status_code=400, detail="source_company_id is required")
    if not databases:
        raise HTTPException(status_code=400, detail="At least one database must be selected")

    from sql_rag.company_data import import_learned_data, get_current_company_id

    target_company_id = get_current_company_id()
    if not target_company_id:
        raise HTTPException(status_code=400, detail="No active company — switch to a company first")

    result = import_learned_data(source_path, source_company_id, target_company_id, databases)

    if result["errors"] and not result["imported"]:
        raise HTTPException(status_code=400, detail="; ".join(result["errors"]))

    return result

@app.get("/api/companies/{company_id}")
async def get_company_config(company_id: str):
    """Get configuration for a specific company."""
    company = load_company(company_id)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{company_id}' not found")
    return {"company": company}

@app.post("/api/companies/reset-after-restore")
async def reset_after_opera_restore():
    """
    Clear transactional state after an Opera database restore.

    Removes import records, ignored transactions, and caches that reference
    Opera data. Preserves learned intelligence (patterns, aliases).
    """
    from sql_rag.company_data import get_current_company_id, reset_after_opera_restore as do_reset

    company_id = get_current_company_id()
    if not company_id:
        return {"success": False, "error": "No company selected"}

    try:
        result = do_reset(company_id)
        return {
            "success": True,
            "company_id": company_id,
            **result
        }
    except Exception as e:
        logger.error(f"Reset after restore failed: {e}")
        return {"success": False, "error": str(e)}

# ============ Admin System Reset Endpoints ============

def _get_system_reset_counts(target_company_id: str = None) -> Dict[str, Any]:
    """Get record counts for each resettable category."""
    import sqlite3
    from sql_rag.company_data import get_current_company_id, get_company_data_dir

    company_id = target_company_id or get_current_company_id()
    if not company_id:
        return {}

    data_dir = get_company_data_dir(company_id)
    counts = {}

    def _count_table(db_path: str, table: str) -> int:
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except Exception:
            return 0

    # email_data.db tables
    email_db = str(data_dir / "email_data.db")
    counts["bank_statement_imports"] = _count_table(email_db, "bank_statement_imports")
    counts["bank_statement_transactions"] = _count_table(email_db, "bank_statement_transactions")
    counts["gocardless_imports"] = _count_table(email_db, "gocardless_imports")
    counts["ignored_bank_transactions"] = _count_table(email_db, "ignored_bank_transactions")

    # bank_patterns.db
    patterns_db = str(data_dir / "bank_patterns.db")
    counts["bank_import_patterns"] = _count_table(patterns_db, "bank_import_patterns")

    # bank_aliases.db
    aliases_db = str(data_dir / "bank_aliases.db")
    counts["bank_import_aliases"] = _count_table(aliases_db, "bank_import_aliases")
    counts["ai_suggestions"] = _count_table(aliases_db, "ai_suggestions")
    counts["repeat_entry_aliases"] = _count_table(aliases_db, "repeat_entry_aliases")

    # pdf_extraction_cache.db
    cache_db = str(data_dir / "pdf_extraction_cache.db")
    counts["extraction_cache"] = _count_table(cache_db, "extraction_cache")

    return counts

def _execute_system_reset(action: str, target_company_id: str = None) -> Dict[str, int]:
    """Execute a system reset action. Returns dict of table -> deleted count."""
    import sqlite3
    from sql_rag.company_data import get_current_company_id, get_company_data_dir

    company_id = target_company_id or get_current_company_id()
    if not company_id:
        raise ValueError("No company selected")

    data_dir = get_company_data_dir(company_id)
    deleted = {}

    def _clear_tables(db_path: str, tables: list) -> Dict[str, int]:
        result = {}
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
                    count = cursor.fetchone()[0]
                    cursor.execute(f"DELETE FROM [{table}]")
                    result[table] = count
                except sqlite3.OperationalError:
                    result[table] = 0
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error clearing tables in {db_path}: {e}")
        return result

    email_db = str(data_dir / "email_data.db")
    patterns_db = str(data_dir / "bank_patterns.db")
    aliases_db = str(data_dir / "bank_aliases.db")
    cache_db = str(data_dir / "pdf_extraction_cache.db")

    if action in ("bank_imports", "full_reset"):
        deleted.update(_clear_tables(email_db, ["bank_statement_transactions", "bank_statement_imports"]))

    if action in ("gocardless_imports", "full_reset"):
        deleted.update(_clear_tables(email_db, ["gocardless_imports"]))

    if action in ("ignored_transactions", "full_reset"):
        deleted.update(_clear_tables(email_db, ["ignored_bank_transactions"]))

    if action in ("learned_patterns", "full_reset"):
        deleted.update(_clear_tables(patterns_db, ["bank_import_patterns"]))

    if action in ("learned_aliases", "full_reset"):
        deleted.update(_clear_tables(aliases_db, ["bank_import_aliases", "ai_suggestions", "repeat_entry_aliases"]))

    if action in ("pdf_cache", "full_reset"):
        deleted.update(_clear_tables(cache_db, ["extraction_cache"]))

    return deleted

@app.get("/api/admin/system-reset/counts")
async def get_system_reset_counts(request: Request, company_id: str = None):
    """
    Get record counts for each resettable category. Admin only.
    Optional company_id query param to target a specific company.
    """
    user = getattr(request.state, 'user', None)
    if not user or not user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        counts = _get_system_reset_counts(target_company_id=company_id)
        return {"success": True, "counts": counts}
    except Exception as e:
        logger.error(f"Failed to get system reset counts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/admin/system-reset")
async def execute_system_reset(request: Request):
    """
    Execute system reset action(s). Admin only.
    Accepts 'actions' (list) or 'action' (single string) for backwards compatibility.
    Optional company_id in body to target a specific company.
    """
    user = getattr(request.state, 'user', None)
    if not user or not user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    body = await request.json()
    target_company_id = body.get("company_id")

    valid_actions = [
        "bank_imports", "gocardless_imports", "ignored_transactions",
        "learned_patterns", "learned_aliases", "pdf_cache", "full_reset"
    ]

    # Support both 'actions' (list) and 'action' (single) for backwards compat
    actions = body.get("actions") or ([body.get("action")] if body.get("action") else [])
    if not actions:
        raise HTTPException(status_code=400, detail="No actions specified")
    for a in actions:
        if a not in valid_actions:
            raise HTTPException(status_code=400, detail=f"Invalid action '{a}'. Must be one of: {', '.join(valid_actions)}")

    try:
        all_deleted = {}
        for action in actions:
            deleted = _execute_system_reset(action, target_company_id=target_company_id)
            all_deleted.update(deleted)
        total = sum(all_deleted.values())
        logger.info(f"System reset {actions} for company '{target_company_id or 'current'}' by {user.get('username', 'unknown')}: {total} records deleted")
        return {
            "success": True,
            "actions": actions,
            "records_deleted": all_deleted,
            "total_deleted": total
        }
    except Exception as e:
        logger.error(f"System reset {actions} failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============ Nominal Balance Recalculation ============

@app.post("/api/admin/recalculate-nominal-balances")
async def recalculate_nominal_balances(request: Request):
    """
    Recalculate nhist and nacnt PTD/YTD from ntran (source of truth).

    Fixes any discrepancies between nhist/nacnt accumulators and actual
    ntran transaction totals. Pass dry_run=true to preview without writing.
    """
    user = getattr(request.state, 'user', None)
    if not user or not user.get('is_admin'):
        raise HTTPException(status_code=403, detail="Admin access required")

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    body = await request.json() if request.headers.get('content-type', '').startswith('application/json') else {}
    dry_run = body.get("dry_run", False)

    try:
        engine = sql_connector.engine

        # Get current financial year and period
        with engine.connect() as conn:
            nparm_row = conn.execute(text(
                "SELECT np_year, np_perno FROM nparm WITH (NOLOCK)"
            )).fetchone()
            if not nparm_row:
                raise HTTPException(status_code=500, detail="Cannot determine financial year from nparm")
            fin_year = int(nparm_row[0])
            cur_period = int(nparm_row[1])

        # Phase 1: Get ntran totals per account+period (source of truth)
        ntran_sql = f"""
            SELECT
                RTRIM(nt_acnt) as account,
                nt_period as period,
                SUM(CASE WHEN nt_value >= 0 THEN nt_value ELSE 0 END) as dr_total,
                SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) as cr_total,
                SUM(nt_value) as net_bal
            FROM ntran WITH (NOLOCK)
            WHERE nt_year = {fin_year}
            GROUP BY RTRIM(nt_acnt), nt_period
            ORDER BY RTRIM(nt_acnt), nt_period
        """
        df_ntran = sql_connector.execute_query(ntran_sql)

        if df_ntran.empty:
            return {"success": True, "message": f"No ntran records for year {fin_year}", "changes": []}

        # Phase 2: Get nacnt records for type/subtype lookup
        nacnt_sql = """
            SELECT RTRIM(na_acnt) as account, na_type, na_subt,
                   na_ptddr, na_ptdcr, na_ytddr, na_ytdcr
            FROM nacnt WITH (NOLOCK)
        """
        df_nacnt = sql_connector.execute_query(nacnt_sql)
        nacnt_lookup = {}
        for _, row in df_nacnt.iterrows():
            acnt = str(row['account']).strip()
            nacnt_lookup[acnt] = row

        # Phase 3: Get all nhist records for the year
        nhist_sql = f"""
            SELECT id, RTRIM(nh_nacnt) as account, nh_ntype, nh_nsubt,
                   RTRIM(nh_ncntr) as centre, nh_period,
                   nh_ptddr, nh_ptdcr, nh_bal
            FROM nhist WITH (NOLOCK)
            WHERE nh_year = {fin_year}
        """
        df_nhist = sql_connector.execute_query(nhist_sql)

        # Build nhist lookup: (account, period) -> list of rows
        nhist_lookup = {}
        for _, row in df_nhist.iterrows():
            key = (str(row['account']).strip(), int(row['nh_period']))
            if key not in nhist_lookup:
                nhist_lookup[key] = []
            nhist_lookup[key].append(row)

        # Phase 4: Calculate correct YTD/PTD per account
        account_ytd = {}
        account_ptd = {}

        for _, row in df_ntran.iterrows():
            acnt = str(row['account']).strip()
            period = int(row['period'])
            dr = float(row['dr_total'] or 0)
            cr = float(row['cr_total'] or 0)

            if acnt not in account_ytd:
                account_ytd[acnt] = {'dr': 0.0, 'cr': 0.0}

            account_ytd[acnt]['dr'] += dr
            account_ytd[acnt]['cr'] += cr

            if period == cur_period:
                account_ptd[acnt] = {'dr': dr, 'cr': cr}

        changes = []
        nhist_updates = 0
        nhist_inserts = 0
        nacnt_updates = 0

        ctx = engine.begin() if not dry_run else engine.connect()
        with ctx as conn:
            # Phase 5: Fix nhist for each account+period
            for _, row in df_ntran.iterrows():
                acnt = str(row['account']).strip()
                period = int(row['period'])
                correct_dr = round(float(row['dr_total'] or 0), 2)
                correct_cr = round(float(row['cr_total'] or 0), 2)
                correct_bal = round(float(row['net_bal'] or 0), 2)

                # nhist stores nh_ptdcr as NEGATIVE
                correct_nhist_cr = round(-correct_cr, 2)

                nacnt_info = nacnt_lookup.get(acnt)
                if nacnt_info is None:
                    continue

                na_type = str(nacnt_info['na_type']).strip()
                na_subt = str(nacnt_info['na_subt']).strip()

                key = (acnt, period)
                existing = nhist_lookup.get(key, [])

                # Find primary row matching nacnt type/subtype
                target_row = None
                for erow in existing:
                    if (str(erow['nh_ntype']).strip() == na_type and
                            str(erow['nh_nsubt']).strip() == na_subt):
                        target_row = erow
                        break
                # Fallback to first row if no type match
                if target_row is None and existing:
                    target_row = existing[0]

                if target_row is not None:
                    cur_dr = round(float(target_row['nh_ptddr'] or 0), 2)
                    cur_cr = round(float(target_row['nh_ptdcr'] or 0), 2)
                    cur_bal = round(float(target_row['nh_bal'] or 0), 2)

                    if (abs(cur_dr - correct_dr) > 0.005 or
                            abs(cur_cr - correct_nhist_cr) > 0.005 or
                            abs(cur_bal - correct_bal) > 0.005):

                        if not dry_run:
                            row_id = int(target_row['id'])
                            conn.execute(text(f"""
                                UPDATE nhist WITH (ROWLOCK)
                                SET nh_ptddr = {correct_dr},
                                    nh_ptdcr = {correct_nhist_cr},
                                    nh_bal = {correct_bal},
                                    datemodified = GETDATE()
                                WHERE id = {row_id}
                            """))

                        nhist_updates += 1
                        changes.append({
                            "type": "nhist_update",
                            "account": acnt,
                            "period": period,
                            "old": {"dr": cur_dr, "cr": cur_cr, "bal": cur_bal},
                            "new": {"dr": correct_dr, "cr": correct_nhist_cr, "bal": correct_bal}
                        })
                else:
                    # No nhist row exists — insert (must get id from nextid table)
                    if not dry_run:
                        next_id_row = conn.execute(text(
                            "SELECT nextid FROM nextid WITH (UPDLOCK, ROWLOCK) WHERE RTRIM(tablename) = 'nhist'"
                        )).fetchone()
                        if next_id_row:
                            nhist_id = int(next_id_row[0])
                            conn.execute(text(f"UPDATE nextid WITH (ROWLOCK) SET nextid = {nhist_id + 1} WHERE RTRIM(tablename) = 'nhist'"))
                        else:
                            nhist_id = 1
                        conn.execute(text(f"""
                            INSERT INTO nhist (
                                id, nh_rectype, nh_ntype, nh_nsubt, nh_nacnt, nh_ncntr,
                                nh_job, nh_project, nh_year, nh_period,
                                nh_bal, nh_budg, nh_rbudg, nh_ptddr, nh_ptdcr, nh_fbal,
                                datecreated, datemodified, state
                            ) VALUES (
                                {nhist_id}, 1, '{na_type}', '{na_subt}', '{acnt:<8}', '    ',
                                '        ', '        ', {fin_year}, {period},
                                {correct_bal}, 0, 0, {correct_dr}, {correct_nhist_cr}, 0,
                                GETDATE(), GETDATE(), 1
                            )
                        """))

                    nhist_inserts += 1
                    changes.append({
                        "type": "nhist_insert",
                        "account": acnt,
                        "period": period,
                        "values": {"dr": correct_dr, "cr": correct_nhist_cr, "bal": correct_bal}
                    })

            # Phase 6: Fix nacnt PTD/YTD
            for acnt, ytd in account_ytd.items():
                nacnt_info = nacnt_lookup.get(acnt)
                if nacnt_info is None:
                    continue

                correct_ytd_dr = round(ytd['dr'], 2)
                correct_ytd_cr = round(ytd['cr'], 2)
                ptd = account_ptd.get(acnt, {'dr': 0.0, 'cr': 0.0})
                correct_ptd_dr = round(ptd['dr'], 2)
                correct_ptd_cr = round(ptd['cr'], 2)

                cur_ytd_dr = round(float(nacnt_info['na_ytddr'] or 0), 2)
                cur_ytd_cr = round(float(nacnt_info['na_ytdcr'] or 0), 2)
                cur_ptd_dr = round(float(nacnt_info['na_ptddr'] or 0), 2)
                cur_ptd_cr = round(float(nacnt_info['na_ptdcr'] or 0), 2)

                if (abs(cur_ytd_dr - correct_ytd_dr) > 0.005 or
                        abs(cur_ytd_cr - correct_ytd_cr) > 0.005 or
                        abs(cur_ptd_dr - correct_ptd_dr) > 0.005 or
                        abs(cur_ptd_cr - correct_ptd_cr) > 0.005):

                    if not dry_run:
                        conn.execute(text(f"""
                            UPDATE nacnt WITH (ROWLOCK)
                            SET na_ptddr = {correct_ptd_dr},
                                na_ptdcr = {correct_ptd_cr},
                                na_ytddr = {correct_ytd_dr},
                                na_ytdcr = {correct_ytd_cr},
                                datemodified = GETDATE()
                            WHERE RTRIM(na_acnt) = '{acnt}'
                        """))

                    nacnt_updates += 1
                    changes.append({
                        "type": "nacnt_update",
                        "account": acnt,
                        "old": {"ptd_dr": cur_ptd_dr, "ptd_cr": cur_ptd_cr,
                                "ytd_dr": cur_ytd_dr, "ytd_cr": cur_ytd_cr},
                        "new": {"ptd_dr": correct_ptd_dr, "ptd_cr": correct_ptd_cr,
                                "ytd_dr": correct_ytd_dr, "ytd_cr": correct_ytd_cr}
                    })

        return {
            "success": True,
            "dry_run": dry_run,
            "year": fin_year,
            "current_period": cur_period,
            "summary": {
                "nhist_updates": nhist_updates,
                "nhist_inserts": nhist_inserts,
                "nacnt_updates": nacnt_updates,
                "total_changes": nhist_updates + nhist_inserts + nacnt_updates,
                "accounts_checked": len(account_ytd)
            },
            "changes": changes
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to recalculate nominal balances: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ============ Database Endpoints ============

@app.get("/api/database/tables", response_model=List[TableInfo])
async def get_tables():
    """Get list of tables in the database."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        tables = sql_connector.get_tables()
        # Convert DataFrame to list of dicts if needed
        if hasattr(tables, 'to_dict'):
            tables = tables.to_dict('records')
        return [TableInfo(**t) for t in tables]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/database/tables/{table_name}/columns", response_model=List[ColumnInfo])
async def get_table_columns(table_name: str, schema_name: str = ""):
    """Get columns for a specific table."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        columns = sql_connector.get_columns(table_name, schema_name)
        # Convert DataFrame to list of dicts if needed
        if hasattr(columns, 'to_dict'):
            columns = columns.to_dict('records')
        return [ColumnInfo(**c) for c in columns]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/database/query", response_model=SQLQueryResponse)
async def execute_query(request: SQLQueryRequest):
    """Execute a SQL query."""
    # Backend validation - check for empty query
    if not request.query or not request.query.strip():
        return SQLQueryResponse(success=False, error="SQL query is required. Please enter a query to execute.")

    if not sql_connector:
        return SQLQueryResponse(success=False, error="SQL connector not initialized")

    try:
        result = sql_connector.execute_query(request.query)

        # Convert DataFrame to list of dicts if needed
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        if result and len(result) > 0:
            columns = list(result[0].keys())
            data = [dict(row) for row in result]

            # Store in vector DB if requested
            if request.store_in_vector_db and vector_db:
                try:
                    texts = [" ".join(f"{k}: {v}" for k, v in row.items()) for row in data]
                    vector_db.store_vectors(texts, [{"source": "sql_query", "query": request.query, **row} for row in data])
                except Exception as e:
                    logger.warning(f"Failed to store in vector DB: {e}")

            return SQLQueryResponse(
                success=True,
                data=data,
                columns=columns,
                row_count=len(data)
            )
        else:
            return SQLQueryResponse(success=True, data=[], columns=[], row_count=0)

    except Exception as e:
        return SQLQueryResponse(success=False, error=str(e))

# ============ RAG Endpoints ============

@app.post("/api/rag/query", response_model=RAGQueryResponse)
async def rag_query(request: RAGQueryRequest):
    """Query the RAG system with natural language."""
    # Backend validation - check for empty question
    if not request.question or not request.question.strip():
        return RAGQueryResponse(success=False, error="Question is required. Please enter a question.")

    if not vector_db:
        return RAGQueryResponse(success=False, error="Vector database not initialized")
    if not llm:
        return RAGQueryResponse(success=False, error="LLM not initialized")

    try:
        # Search for similar content
        results = vector_db.search_similar(request.question, limit=request.num_results)

        if not results:
            return RAGQueryResponse(
                success=True,
                answer="I don't have any relevant information to answer your question. Please ingest some data first.",
                sources=[]
            )

        # Generate answer using LLM
        answer = llm.process_rag_query(request.question, results)

        # Format sources
        sources = [{"score": r["score"], "text": r["payload"].get("text", "")[:200]} for r in results]

        return RAGQueryResponse(
            success=True,
            answer=answer,
            sources=sources
        )

    except Exception as e:
        return RAGQueryResponse(success=False, error=str(e))

@app.post("/api/rag/generate-sql")
async def generate_sql(question: str = Query(..., description="Natural language question")):
    """Generate SQL from natural language question."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")
    if not llm:
        raise HTTPException(status_code=503, detail="LLM not initialized")

    try:
        # Get schema information
        tables = sql_connector.get_tables()
        # Convert DataFrame to list of dicts if needed
        if hasattr(tables, 'to_dict'):
            tables = tables.to_dict('records')
        schema_info = []

        for table in tables[:10]:  # Limit to first 10 tables
            columns = sql_connector.get_columns(table["table_name"], table.get("schema_name", ""))
            if hasattr(columns, 'to_dict'):
                columns = columns.to_dict('records')
            col_info = ", ".join([f"{c['column_name']} ({c['data_type']})" for c in columns])
            schema_info.append(f"Table {table['table_name']}: {col_info}")

        schema_str = "\n".join(schema_info)

        prompt = f"""Given the following database schema:
{schema_str}

Generate a SQL query to answer this question: {question}

Return ONLY the SQL query, no explanations."""

        sql = llm.get_completion(prompt)

        return {"success": True, "sql": sql.strip()}

    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/rag/stats")
async def get_vector_stats():
    """Get vector database statistics."""
    if not vector_db:
        raise HTTPException(status_code=503, detail="Vector database not initialized")

    try:
        info = vector_db.get_collection_info()
        return {"success": True, "stats": info}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/rag/ingest")
async def ingest_data(texts: List[str], metadata: Optional[List[Dict[str, Any]]] = None):
    """Ingest data into the vector database."""
    if not vector_db:
        raise HTTPException(status_code=503, detail="Vector database not initialized")

    try:
        if metadata is None:
            metadata = [{"source": "manual_ingest"} for _ in texts]

        vector_db.store_vectors(texts, metadata)
        return {"success": True, "message": f"Ingested {len(texts)} documents"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/rag/ingest-from-sql")
async def ingest_from_sql(request: SQLToRAGRequest):
    """
    AI-powered SQL to RAG ingestion.
    Describe what data you want in natural language, AI generates SQL,
    executes it against the database, and stores results in ChromaDB.
    """
    # Backend validation - check for empty description (unless custom SQL provided)
    if not request.custom_sql and (not request.description or not request.description.strip()):
        return {"success": False, "error": "Description is required. Please describe what data you want to extract."}

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")
    if not vector_db:
        raise HTTPException(status_code=503, detail="Vector database not initialized")
    if not llm and not request.custom_sql:
        raise HTTPException(status_code=503, detail="LLM not initialized (required for SQL generation)")

    try:
        # Step 1: Generate or use provided SQL
        if request.custom_sql:
            sql_query = request.custom_sql
            logger.info(f"Using custom SQL: {sql_query[:100]}...")
        else:
            # Get schema information for AI
            tables = sql_connector.get_tables()
            # Convert DataFrame to list of dicts if needed
            if hasattr(tables, 'to_dict'):
                tables = tables.to_dict('records')

            # Filter tables if specified
            if request.table_filter:
                tables = [t for t in tables if t["table_name"] in request.table_filter]

            schema_info = []
            for table in tables[:20]:  # Limit schema context
                columns = sql_connector.get_columns(table["table_name"], table.get("schema_name", ""))
                if hasattr(columns, 'to_dict'):
                    columns = columns.to_dict('records')
                col_info = ", ".join([f"{c['column_name']} ({c['data_type']})" for c in columns[:30]])
                schema_info.append(f"Table {table.get('schema_name', 'dbo')}.{table['table_name']}: {col_info}")

            schema_str = "\n".join(schema_info)

            prompt = f"""You are a SQL expert. Given the following database schema:

{schema_str}

Generate a SQL query to retrieve the following data: {request.description}

Requirements:
- Return ONLY the SQL query, no explanations or markdown
- Limit results to {request.max_rows} rows using TOP or LIMIT clause
- Select meaningful columns that would be useful for semantic search
- Join related tables if it provides better context

SQL Query:"""

            sql_query = llm.get_completion(prompt).strip()

            # Check if LLM returned an error instead of SQL
            if sql_query.lower().startswith("error:") or "429" in sql_query or "quota" in sql_query.lower():
                return {"success": False, "error": f"LLM API error: {sql_query[:200]}. Please use custom SQL instead."}

            # Clean up the SQL (remove markdown if present)
            if sql_query.startswith("```"):
                sql_query = sql_query.split("```")[1]
                if sql_query.startswith("sql"):
                    sql_query = sql_query[3:]
            sql_query = sql_query.strip()

            # Validate it looks like SQL
            if not any(sql_query.upper().startswith(kw) for kw in ["SELECT", "WITH"]):
                return {"success": False, "error": f"LLM did not return valid SQL. Got: {sql_query[:100]}. Please use custom SQL instead."}

            logger.info(f"AI generated SQL: {sql_query[:200]}...")

        # Step 2: Execute the SQL query
        result = sql_connector.execute_query(sql_query)

        # Convert DataFrame to list of dicts if needed
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        if not result or len(result) == 0:
            return {
                "success": True,
                "message": "Query executed but returned no results",
                "sql_used": sql_query,
                "rows_ingested": 0
            }

        # Step 3: Convert results to text documents for RAG
        data = [dict(row) for row in result]

        # Create semantic text representation for each row
        texts = []
        metadata_list = []

        for row in data:
            # Create a readable text representation
            text_parts = []
            for key, value in row.items():
                if value is not None and str(value).strip():
                    text_parts.append(f"{key}: {value}")

            text = " | ".join(text_parts)
            texts.append(text)

            # Create metadata for filtering/retrieval
            meta = {
                "source": "mssql_ingestion",
                "query_description": request.description[:200],
                "sql_query": sql_query[:500],
            }
            # Add row data as metadata (with type conversion)
            for key, value in row.items():
                if isinstance(value, (str, int, float, bool)) or value is None:
                    meta[f"col_{key}"] = value
                else:
                    meta[f"col_{key}"] = str(value)

            metadata_list.append(meta)

        # Step 4: Store in ChromaDB
        vector_db.store_vectors(texts, metadata_list)

        logger.info(f"Successfully ingested {len(texts)} rows into ChromaDB")

        return {
            "success": True,
            "message": f"Successfully ingested {len(texts)} rows from MS SQL into RAG database",
            "sql_used": sql_query,
            "rows_ingested": len(texts),
            "sample_data": data[:3] if len(data) > 3 else data  # Return sample
        }

    except Exception as e:
        logger.error(f"SQL to RAG ingestion failed: {e}")
        return {"success": False, "error": str(e)}

@app.post("/api/rag/load-credit-control")
async def load_credit_control_data():
    """Load all credit control data using predefined table mappings."""
    import json
    import os

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")
    if not vector_db:
        raise HTTPException(status_code=503, detail="Vector database not initialized")

    try:
        # Load the table mapping
        mapping_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "table_mapping.json")
        if not os.path.exists(mapping_path):
            return {"success": False, "error": "table_mapping.json not found"}

        with open(mapping_path, "r") as f:
            mapping = json.load(f)

        # Clear existing data
        vector_db.client.delete_collection(vector_db.collection_name)
        vector_db.collection = vector_db.client.get_or_create_collection(
            name=vector_db.collection_name,
            metadata={"hnsw:space": "cosine"}
        )

        total_ingested = 0
        results = []

        # Load each table from the mapping
        tables = mapping.get("credit_control_tables", {})
        for table_key, table_info in tables.items():
            try:
                sql = table_info.get("sql_template", "")
                if not sql:
                    continue

                result = sql_connector.execute_query(sql)
                if hasattr(result, 'to_dict'):
                    result = result.to_dict('records')

                if result:
                    texts = [" | ".join(f"{k}: {v}" for k, v in row.items() if v is not None) for row in result]
                    metadata = [{"source": table_info["table"], "type": "master_data"} for _ in texts]
                    vector_db.store_vectors(texts, metadata)
                    total_ingested += len(texts)
                    results.append({"table": table_info["table"], "rows": len(texts)})
            except Exception as e:
                results.append({"table": table_info.get("table", table_key), "error": str(e)})

        # Load pre-computed credit control queries
        queries = mapping.get("credit_control_queries", {})
        for query_key, query_info in queries.items():
            try:
                sql = query_info.get("sql", "")
                if not sql:
                    continue

                result = sql_connector.execute_query(sql)
                if hasattr(result, 'to_dict'):
                    result = result.to_dict('records')

                if result:
                    texts = [" | ".join(f"{k}: {v}" for k, v in row.items() if v is not None) for row in result]
                    metadata = [{"source": query_key, "type": "credit_control_query"} for _ in texts]
                    vector_db.store_vectors(texts, metadata)
                    total_ingested += len(texts)
                    results.append({"query": query_key, "rows": len(texts)})
            except Exception as e:
                results.append({"query": query_key, "error": str(e)})

        # Load data dictionary
        dictionary_docs = [
            "SNAME is the Customer Master table with columns: sn_account=Account Code, sn_name=Customer Name, sn_currbal=Current Balance, sn_crlim=Credit Limit, sn_stop=Account On Stop, sn_lastinv=Last Invoice Date, sn_lastrec=Last Receipt Date",
            "STRAN is the Sales Transactions table. Transaction types: I=Invoice, C=Credit Note, R=Receipt. Key columns: st_account=Customer Account, st_trdate=Date, st_trvalue=Value, st_trbal=Outstanding Balance",
            "CREDIT CONTROL RULES: Customer is OVER CREDIT LIMIT when current_balance > credit_limit. Account ON STOP means on_stop=True. OVERDUE means due_date passed and outstanding > 0"
        ]
        vector_db.store_vectors(dictionary_docs, [{"source": "data_dictionary", "type": "reference"} for _ in dictionary_docs])
        total_ingested += len(dictionary_docs)

        return {
            "success": True,
            "message": f"Loaded {total_ingested} documents for credit control",
            "details": results
        }

    except Exception as e:
        logger.error(f"Failed to load credit control data: {e}")
        return {"success": False, "error": str(e)}

@app.get("/api/rag/clear")
async def clear_vector_db():
    """Clear all data from the vector database."""
    if not vector_db:
        raise HTTPException(status_code=503, detail="Vector database not initialized")

    try:
        # Delete and recreate collection
        vector_db.client.delete_collection(vector_db.collection_name)
        vector_db.collection = vector_db.client.get_or_create_collection(
            name=vector_db.collection_name,
            metadata={"hnsw:space": "cosine"}
        )
        return {"success": True, "message": "Vector database cleared"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/rag/populate")
async def populate_rag_database(
    clear_first: bool = Query(True, description="Clear existing data before populating"),
    include_data_structures: bool = Query(True, description="Include Opera data structures documentation"),
    data_structures_path: Optional[str] = Query(None, description="Custom path to data structures file")
):
    """
    Populate the RAG database with Opera knowledge.

    This endpoint loads:
    - Markdown knowledge files from docs/
    - Opera data structures documentation
    - Business rules and conventions
    - SQL query examples
    - Table reference summaries

    Use this endpoint during deployment to initialize the knowledge base.
    """
    if not RAG_POPULATOR_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="RAG Populator not available. Ensure scripts/populate_rag.py exists."
        )

    if not vector_db:
        raise HTTPException(status_code=503, detail="Vector database not initialized")

    try:
        populator = RAGPopulator(config_path=CONFIG_PATH)

        # If custom data structures path provided, use it
        if data_structures_path and include_data_structures:
            ds_path = Path(data_structures_path)
            if not ds_path.exists():
                return {
                    "success": False,
                    "error": f"Data structures file not found: {data_structures_path}"
                }

        results = populator.populate_all(clear_first=clear_first)

        return {
            "success": results["success"],
            "message": f"RAG database populated with {results['total_documents']} documents",
            "total_documents": results["total_documents"],
            "sources": results.get("sources", {}),
            "error": results.get("error")
        }

    except Exception as e:
        logger.error(f"Failed to populate RAG database: {e}")
        return {"success": False, "error": str(e)}

@app.get("/api/rag/populate/status")
async def get_rag_population_status():
    """
    Get the current status of the RAG database.

    Returns:
    - Whether RAG populator is available
    - Current document count
    - Last population timestamp (if tracked)
    """
    if not vector_db:
        return {
            "available": False,
            "populator_available": RAG_POPULATOR_AVAILABLE,
            "error": "Vector database not initialized"
        }

    try:
        info = vector_db.get_collection_info()
        return {
            "available": True,
            "populator_available": RAG_POPULATOR_AVAILABLE,
            "document_count": info.get("vectors_count", 0),
            "collection_name": info.get("name"),
            "status": info.get("status"),
            "needs_population": info.get("vectors_count", 0) == 0
        }
    except Exception as e:
        return {
            "available": False,
            "populator_available": RAG_POPULATOR_AVAILABLE,
            "error": str(e)
        }

# ============ Credit Control Query (Live SQL) ============

class CreditControlQueryRequest(BaseModel):
    question: str = Field(..., description="Natural language credit control question")

# ============ Credit Control Agent Query Definitions ============
# Organized into categories matching the agent requirements
# Order matters - more specific patterns should be checked first

CREDIT_CONTROL_QUERIES = [
    # ========== PRE-ACTION CHECKS (run before every contact) ==========
    {
        "name": "unallocated_cash",
        "category": "pre_action",
        "keywords": ["unallocated cash", "unapplied cash", "unallocated payment", "cash on account", "credit balance"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS receipt_ref, st.st_trdate AS receipt_date,
                  ABS(st.st_trbal) AS unallocated_amount, sn.sn_teleno AS phone
                  FROM stran WITH (NOLOCK) st
                  JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'R' AND st.st_trbal < 0
                  ORDER BY ABS(st.st_trbal) DESC""",
        "description": "Unallocated cash/payments on customer accounts",
        "param_sql": """SELECT st.st_trref AS receipt_ref, st.st_trdate AS receipt_date,
                       ABS(st.st_trbal) AS unallocated_amount
                       FROM stran WITH (NOLOCK) st WHERE st.st_account = '{account}'
                       AND st.st_trtype = 'R' AND st.st_trbal < 0"""
    },
    {
        "name": "pending_credit_notes",
        "category": "pre_action",
        "keywords": ["pending credit", "credit note", "credit notes", "credits pending", "unapplied credit"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS credit_ref, st.st_trdate AS credit_date,
                  ABS(st.st_trbal) AS credit_amount, st.st_memo AS reason
                  FROM stran WITH (NOLOCK) st
                  JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'C' AND st.st_trbal < 0
                  ORDER BY st.st_trdate DESC""",
        "description": "Pending/unallocated credit notes",
        "param_sql": """SELECT st.st_trref AS credit_ref, st.st_trdate AS credit_date,
                       ABS(st.st_trbal) AS credit_amount, st.st_memo AS reason
                       FROM stran WITH (NOLOCK) st WHERE st.st_account = '{account}'
                       AND st.st_trtype = 'C' AND st.st_trbal < 0"""
    },
    {
        "name": "disputed_invoices",
        "category": "pre_action",
        "keywords": ["dispute", "disputed", "in dispute", "query", "queried"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS invoice, st.st_trdate AS invoice_date,
                  st.st_trbal AS outstanding, st.st_memo AS dispute_reason,
                  sn.sn_teleno AS phone
                  FROM stran WITH (NOLOCK) st
                  JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                  WHERE st.st_dispute = 1 AND st.st_trbal > 0
                  ORDER BY st.st_trbal DESC""",
        "description": "Invoices flagged as disputed",
        "param_sql": """SELECT st.st_trref AS invoice, st.st_trdate AS invoice_date,
                       st.st_trbal AS outstanding, st.st_memo AS dispute_reason
                       FROM stran WITH (NOLOCK) st WHERE st.st_account = '{account}'
                       AND st.st_dispute = 1 AND st.st_trbal > 0"""
    },
    {
        "name": "account_status",
        "category": "pre_action",
        "keywords": ["account status", "status check", "can we contact", "on hold", "do not contact", "insolvency", "dormant"],
        "sql": """SELECT sn_account AS account, sn_name AS customer,
                  sn_stop AS on_stop, sn_dormant AS dormant,
                  sn_currbal AS balance, sn_crlim AS credit_limit,
                  sn_crdscor AS credit_score, sn_crdnotes AS credit_notes,
                  sn_memo AS account_memo, sn_priorty AS priority,
                  sn_cmgroup AS credit_group
                  FROM sname WHERE sn_stop = 1 OR sn_dormant = 1
                  ORDER BY sn_currbal DESC""",
        "description": "Accounts with stop/hold/dormant flags",
        "param_sql": """SELECT sn_stop AS on_stop, sn_dormant AS dormant,
                       sn_crdscor AS credit_score, sn_crdnotes AS credit_notes,
                       sn_memo AS account_memo, sn_priorty AS priority
                       FROM sname WHERE sn_account = '{account}'"""
    },

    # ========== LEDGER STATE ==========
    {
        "name": "overdue_by_age",
        "category": "ledger",
        "keywords": ["overdue by age", "aged overdue", "aging bucket", "age bracket", "days overdue breakdown"],
        "sql": """SELECT
                  sn.sn_account AS account, sn.sn_name AS customer,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 0 AND 7 THEN st.st_trbal ELSE 0 END) AS days_1_7,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 8 AND 14 THEN st.st_trbal ELSE 0 END) AS days_8_14,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 15 AND 21 THEN st.st_trbal ELSE 0 END) AS days_15_21,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 22 AND 30 THEN st.st_trbal ELSE 0 END) AS days_22_30,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 31 AND 45 THEN st.st_trbal ELSE 0 END) AS days_31_45,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) > 45 THEN st.st_trbal ELSE 0 END) AS days_45_plus,
                  SUM(st.st_trbal) AS total_overdue
                  FROM stran WITH (NOLOCK) st
                  JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'I' AND st.st_trbal > 0 AND st.st_dueday < GETDATE()
                  GROUP BY sn.sn_account, sn.sn_name
                  HAVING SUM(st.st_trbal) > 0
                  ORDER BY SUM(st.st_trbal) DESC""",
        "description": "Overdue invoices broken down by age bracket"
    },
    {
        "name": "customer_balance_aging",
        "category": "ledger",
        "keywords": ["balance aging", "ageing summary", "customer aging", "outstanding balance", "balance breakdown"],
        "sql": """SELECT sn.sn_account AS account, sn.sn_name AS customer,
                  sn.sn_currbal AS total_balance, sn.sn_crlim AS credit_limit,
                  sh.si_current AS current, sh.si_period1 AS period_1,
                  sh.si_period2 AS period_2, sh.si_period3 AS period_3,
                  sh.si_period4 AS period_4, sh.si_period5 AS period_5,
                  sh.si_avgdays AS avg_days_to_pay
                  FROM sname sn
                  LEFT JOIN shist sh ON sn.sn_account = sh.si_account
                  WHERE sn.sn_currbal > 0
                  ORDER BY sn.sn_currbal DESC""",
        "description": "Customer balance with aging periods"
    },
    {
        "name": "invoice_lookup",
        "category": "ledger",
        "keywords": ["invoice detail", "invoice lookup", "find invoice", "invoice number", "specific invoice"],
        "sql": None,  # Dynamic - needs invoice number extraction
        "description": "Look up specific invoice details",
        "param_sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                       st.st_trref AS invoice, st.st_custref AS your_ref,
                       st.st_trdate AS invoice_date, st.st_dueday AS due_date,
                       st.st_trvalue AS original_value, st.st_trbal AS outstanding,
                       st.st_dispute AS disputed, st.st_memo AS memo,
                       st.st_payday AS promise_date,
                       DATEDIFF(day, st.st_dueday, GETDATE()) AS days_overdue
                       FROM stran WITH (NOLOCK) st
                       JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                       WHERE st.st_trref LIKE '%{invoice}%' AND st.st_trtype = 'I'"""
    },
    {
        "name": "overdue_invoices",
        "category": "ledger",
        "keywords": ["overdue", "past due", "late invoice", "unpaid invoice", "outstanding invoice"],
        "sql": """SELECT TOP 50 st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS invoice, st.st_trdate AS invoice_date, st.st_dueday AS due_date,
                  st.st_trbal AS outstanding, DATEDIFF(day, st.st_dueday, GETDATE()) AS days_overdue,
                  st.st_dispute AS disputed, sn.sn_teleno AS phone
                  FROM stran st WITH (NOLOCK) JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'I' AND st.st_trbal > 0 AND st.st_dueday < GETDATE()
                  ORDER BY days_overdue DESC""",
        "description": "Overdue invoices ordered by days overdue"
    },

    # ========== CUSTOMER CONTEXT ==========
    {
        "name": "customer_master",
        "category": "customer",
        "keywords": ["customer details", "customer master", "contact details", "customer info", "customer record"],
        "sql": None,  # Dynamic - needs customer name/account extraction
        "description": "Customer master record with all details",
        "param_sql": """SELECT sn_account AS account, sn_name AS customer,
                       sn_addr1 AS address1, sn_addr2 AS address2, sn_addr3 AS city,
                       sn_addr4 AS county, sn_pstcode AS postcode,
                       sn_teleno AS phone, sn_email AS email,
                       sn_contact AS contact, sn_contac2 AS contact2,
                       sn_currbal AS balance, sn_crlim AS credit_limit,
                       sn_stop AS on_stop, sn_dormant AS dormant,
                       sn_custype AS customer_type, sn_region AS region,
                       sn_priorty AS priority, sn_cmgroup AS credit_group,
                       sn_lastinv AS last_invoice, sn_lastrec AS last_payment,
                       sn_memo AS memo, sn_crdnotes AS credit_notes
                       FROM sname WHERE sn_account = '{account}'
                       OR sn_name LIKE '%{customer}%'"""
    },
    {
        "name": "payment_history",
        "category": "customer",
        "keywords": ["payment history", "payment pattern", "how they pay", "days to pay", "payment behaviour", "payment behavior"],
        "sql": """SELECT TOP 20 st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS receipt_ref, st.st_trdate AS payment_date,
                  ABS(st.st_trvalue) AS amount, sh.si_avgdays AS avg_days_to_pay
                  FROM stran WITH (NOLOCK) st
                  JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                  LEFT JOIN shist sh ON st.st_account = sh.si_account
                  WHERE st.st_trtype = 'R'
                  ORDER BY st.st_trdate DESC""",
        "description": "Customer payment history and patterns",
        "param_sql": """SELECT st.st_trref AS receipt_ref, st.st_trdate AS payment_date,
                       ABS(st.st_trvalue) AS amount,
                       (SELECT TOP 1 si_avgdays FROM shist WHERE si_account = '{account}') AS avg_days_to_pay
                       FROM stran WITH (NOLOCK) st WHERE st.st_account = '{account}'
                       AND st.st_trtype = 'R' ORDER BY st.st_trdate DESC"""
    },
    {
        "name": "customer_notes",
        "category": "customer",
        "keywords": ["customer notes", "memo", "notes", "comments", "history notes", "contact log"],
        "sql": """SELECT zn.zn_account AS account, sn.sn_name AS customer,
                  zn.zn_subject AS subject, zn.zn_note AS note,
                  zn.sq_date AS note_date, zn.sq_user AS created_by,
                  zn.zn_actreq AS action_required, zn.zn_actdate AS action_date,
                  zn.zn_actcomp AS action_complete, zn.zn_priority AS priority
                  FROM znotes zn
                  JOIN sname sn WITH (NOLOCK) ON zn.zn_account = sn.sn_account
                  WHERE zn.zn_module = 'SL'
                  ORDER BY zn.sq_date DESC""",
        "description": "Customer notes and contact history",
        "param_sql": """SELECT zn.zn_subject AS subject, zn.zn_note AS note,
                       zn.sq_date AS note_date, zn.sq_user AS created_by,
                       zn.zn_actreq AS action_required, zn.zn_actdate AS action_date,
                       zn.zn_actcomp AS action_complete
                       FROM znotes zn WHERE zn.zn_account = '{account}'
                       AND zn.zn_module = 'SL' ORDER BY zn.sq_date DESC"""
    },
    {
        "name": "customer_segments",
        "category": "customer",
        "keywords": ["segment", "customer type", "strategic", "watch list", "customer category", "priority"],
        "sql": """SELECT sn_account AS account, sn_name AS customer,
                  sn_custype AS customer_type, sn_region AS region,
                  sn_priorty AS priority, sn_cmgroup AS credit_group,
                  sn_currbal AS balance, sn_crlim AS credit_limit,
                  sn_crdscor AS credit_score
                  FROM sname WHERE sn_currbal > 0
                  ORDER BY sn_priorty DESC, sn_currbal DESC""",
        "description": "Customer segments and priority flags"
    },

    # ========== PROMISE TRACKING ==========
    {
        "name": "promises_due",
        "category": "promise",
        "keywords": ["promise due", "promises today", "payment promise", "promised to pay", "ptp", "promise overdue"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS invoice, st.st_trbal AS outstanding,
                  st.st_payday AS promise_date,
                  DATEDIFF(day, st.st_payday, GETDATE()) AS days_since_promise,
                  sn.sn_teleno AS phone, sn.sn_contact AS contact
                  FROM stran WITH (NOLOCK) st
                  JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                  WHERE st.st_payday IS NOT NULL
                  AND st.st_payday <= GETDATE()
                  AND st.st_trbal > 0
                  ORDER BY st.st_payday ASC""",
        "description": "Promises to pay due today or overdue"
    },
    {
        "name": "broken_promises",
        "category": "promise",
        "keywords": ["broken promise", "missed promise", "promise count", "failed promise", "promise history"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  COUNT(*) AS broken_promise_count,
                  SUM(st.st_trbal) AS total_outstanding,
                  MIN(st.st_payday) AS oldest_promise,
                  MAX(st.st_payday) AS latest_promise
                  FROM stran WITH (NOLOCK) st
                  JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                  WHERE st.st_payday IS NOT NULL
                  AND st.st_payday < GETDATE()
                  AND st.st_trbal > 0
                  GROUP BY st.st_account, sn.sn_name
                  HAVING COUNT(*) > 1
                  ORDER BY COUNT(*) DESC""",
        "description": "Customers with multiple broken promises",
        "param_sql": """SELECT COUNT(*) AS broken_promise_count,
                       SUM(st_trbal) AS total_outstanding
                       FROM stran WITH (NOLOCK) WHERE st_account = '{account}'
                       AND st_payday IS NOT NULL AND st_payday < GETDATE()
                       AND st_trbal > 0"""
    },

    # ========== MONITORING / REPORTING ==========
    {
        "name": "over_credit_limit",
        "category": "monitoring",
        "keywords": ["over credit", "exceed credit", "over limit", "exceeded limit", "above credit", "credit limit"],
        "sql": """SELECT sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                  sn_crlim AS credit_limit, (sn_currbal - sn_crlim) AS over_by,
                  sn_ordrbal AS orders_pending, sn_teleno AS phone, sn_contact AS contact,
                  sn_stop AS on_stop
                  FROM sname WHERE sn_currbal > sn_crlim AND sn_crlim > 0
                  ORDER BY (sn_currbal - sn_crlim) DESC""",
        "description": "Customers over their credit limit"
    },
    {
        "name": "unallocated_cash_old",
        "category": "monitoring",
        "keywords": ["old unallocated", "unallocated over 7 days", "reconciliation", "cash reconciliation", "old cash"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS receipt_ref, st.st_trdate AS receipt_date,
                  ABS(st.st_trbal) AS unallocated_amount,
                  DATEDIFF(day, st.st_trdate, GETDATE()) AS days_unallocated,
                  sn.sn_teleno AS phone
                  FROM stran WITH (NOLOCK) st
                  JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'R' AND st.st_trbal < 0
                  AND DATEDIFF(day, st.st_trdate, GETDATE()) > 7
                  ORDER BY DATEDIFF(day, st.st_trdate, GETDATE()) DESC""",
        "description": "Unallocated cash older than 7 days (needs reconciliation)"
    },
    {
        "name": "accounts_on_stop",
        "category": "monitoring",
        "keywords": ["on stop", "stopped", "account stop", "credit stop"],
        "sql": """SELECT sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                  sn_crlim AS credit_limit, sn_teleno AS phone, sn_contact AS contact,
                  sn_memo AS memo, sn_crdnotes AS credit_notes
                  FROM sname WHERE sn_stop = 1
                  ORDER BY sn_currbal DESC""",
        "description": "Accounts currently on stop"
    },
    {
        "name": "top_debtors",
        "category": "monitoring",
        "keywords": ["owes most", "top debtor", "highest balance", "biggest debt", "most money", "largest balance", "owe us"],
        "sql": """SELECT TOP 20 sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                  sn_crlim AS credit_limit, sn_lastrec AS last_payment, sn_teleno AS phone,
                  CASE WHEN sn_stop = 1 THEN 'ON STOP'
                       WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER LIMIT'
                       ELSE 'OK' END AS status
                  FROM sname WHERE sn_currbal > 0 ORDER BY sn_currbal DESC""",
        "description": "Top 20 customers by outstanding balance"
    },
    {
        "name": "recent_payments",
        "category": "monitoring",
        "keywords": ["recent payment", "last payment", "receipts", "paid recently", "who paid"],
        "sql": """SELECT TOP 20 st.st_account AS account, sn.sn_name AS customer,
                  st.st_trdate AS payment_date, st.st_trref AS reference,
                  ABS(st.st_trvalue) AS amount
                  FROM stran st WITH (NOLOCK) JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'R' ORDER BY st.st_trdate DESC""",
        "description": "Recent payments received"
    },
    {
        "name": "aged_debt",
        "category": "monitoring",
        "keywords": ["aged debt", "aging", "debt age", "how old", "debt summary"],
        "sql": """SELECT sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                  sn_lastrec AS last_payment, DATEDIFF(day, sn_lastrec, GETDATE()) AS days_since_payment,
                  CASE WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER LIMIT'
                       WHEN sn_stop = 1 THEN 'ON STOP' ELSE 'OK' END AS status
                  FROM sname WHERE sn_currbal > 0 ORDER BY sn_currbal DESC""",
        "description": "Aged debt summary with payment history"
    },

    # ========== LOOKUP (Dynamic) ==========
    {
        "name": "customer_lookup",
        "category": "lookup",
        "keywords": ["details for", "info for", "lookup", "find customer", "about customer", "tell me about"],
        "sql": None,  # Dynamic - needs customer name extraction
        "description": "Look up specific customer details"
    }
]

# Helper function to get query by name (for programmatic API access)
def get_query_by_name(name: str) -> dict:
    """Get a specific query definition by name."""
    for q in CREDIT_CONTROL_QUERIES:
        if q["name"] == name:
            return q
    return None

# API endpoint to list all available queries
# @app.get("/api/credit-control/queries")  # Moved to apps/dashboards/api/routes.py
async def _old_list_credit_control_queries():
    """List all available credit control query types."""
    queries = []
    for q in CREDIT_CONTROL_QUERIES:
        queries.append({
            "name": q["name"],
            "category": q.get("category", "general"),
            "description": q["description"],
            "keywords": q["keywords"],
            "has_param_sql": "param_sql" in q
        })
    return {"queries": queries}

# API endpoint for parameterized queries (for agent use)
# @app.post("/api/credit-control/query-param")  # Moved to apps/dashboards/api/routes.py
async def _old_credit_control_query_param(query_name: str, account: str = None, customer: str = None, invoice: str = None):
    """
    Execute a parameterized credit control query.
    For use by the credit control agent with specific parameters.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    query_def = get_query_by_name(query_name)
    if not query_def:
        raise HTTPException(status_code=404, detail=f"Query '{query_name}' not found")

    if "param_sql" not in query_def:
        raise HTTPException(status_code=400, detail=f"Query '{query_name}' does not support parameters")

    try:
        sql = query_def["param_sql"]
        if account:
            sql = sql.replace("{account}", account)
        if customer:
            sql = sql.replace("{customer}", customer)
        if invoice:
            sql = sql.replace("{invoice}", invoice)

        result = sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        return {
            "success": True,
            "query_name": query_name,
            "category": query_def.get("category", "general"),
            "description": query_def["description"],
            "data": result or [],
            "count": len(result) if result else 0
        }
    except Exception as e:
        logger.error(f"Parameterized query failed: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/credit-control/dashboard")  # Moved to apps/dashboards/api/routes.py
async def _old_credit_control_dashboard():
    """
    Get summary dashboard data for credit control.
    Returns key metrics in a single API call.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        metrics = {}

        # Total outstanding balance
        result = sql_connector.execute_query(
            "SELECT COUNT(*) AS count, SUM(sn_currbal) AS total FROM sname WHERE sn_currbal > 0"
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["total_debt"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Total Outstanding"
            }

        # Over credit limit
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(sn_currbal - sn_crlim) AS total
               FROM sname WHERE sn_currbal > sn_crlim AND sn_crlim > 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["over_credit_limit"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Over Credit Limit"
            }

        # Accounts on stop
        result = sql_connector.execute_query(
            "SELECT COUNT(*) AS count, SUM(sn_currbal) AS total FROM sname WHERE sn_stop = 1"
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["accounts_on_stop"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Accounts On Stop"
            }

        # Overdue invoices
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(st_trbal) AS total
               FROM stran WITH (NOLOCK) WHERE st_trtype = 'I' AND st_trbal > 0 AND st_dueday < GETDATE()"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["overdue_invoices"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Overdue Invoices"
            }

        # Recent payments (last 7 days)
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(ABS(st_trvalue)) AS total
               FROM stran WITH (NOLOCK) WHERE st_trtype = 'R' AND st_trdate >= DATEADD(day, -7, GETDATE())"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["recent_payments"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Payments (7 days)"
            }

        # Promises due today or overdue
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(st_trbal) AS total
               FROM stran WITH (NOLOCK) WHERE st_payday IS NOT NULL
               AND st_payday <= GETDATE() AND st_trbal > 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["promises_due"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Promises Due"
            }

        # Disputed invoices
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(st_trbal) AS total
               FROM stran WITH (NOLOCK) WHERE st_dispute = 1 AND st_trbal > 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["disputed"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "In Dispute"
            }

        # Unallocated cash
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(ABS(st_trbal)) AS total
               FROM stran WITH (NOLOCK) WHERE st_trtype = 'R' AND st_trbal < 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["unallocated_cash"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Unallocated Cash"
            }

        # Priority actions - customers needing attention
        priority_result = sql_connector.execute_query(
            """SELECT TOP 10 sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                      sn_crlim AS credit_limit, sn_teleno AS phone, sn_contact AS contact,
                      CASE WHEN sn_stop = 1 THEN 'ON_STOP'
                           WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER_LIMIT'
                           ELSE 'HIGH_BALANCE' END AS priority_reason
               FROM sname
               WHERE sn_currbal > 0 AND (sn_stop = 1 OR sn_currbal > sn_crlim)
               ORDER BY sn_currbal DESC"""
        )
        if hasattr(priority_result, 'to_dict'):
            priority_result = priority_result.to_dict('records')

        return {
            "success": True,
            "metrics": metrics,
            "priority_actions": priority_result or []
        }

    except Exception as e:
        logger.error(f"Dashboard query failed: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/credit-control/debtors-report")  # Moved to apps/dashboards/api/routes.py
async def _old_credit_control_debtors_report():
    """
    Get aged debtors report with balance breakdown by aging period.
    Columns: Account, Customer, Balance, Current, 1 Month, 2 Month, 3 Month+
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Query joins sname (customer) with shist (aging history) to get aged balances
        # si_age = 1 is the most recent aging period
        result = sql_connector.execute_query("""
            SELECT
                sn.sn_account AS account,
                sn.sn_name AS customer,
                sn.sn_currbal AS balance,
                ISNULL(sh.si_current, 0) AS current_period,
                ISNULL(sh.si_period1, 0) AS month_1,
                ISNULL(sh.si_period2, 0) AS month_2,
                ISNULL(sh.si_period3, 0) + ISNULL(sh.si_period4, 0) + ISNULL(sh.si_period5, 0) AS month_3_plus,
                sn.sn_crlim AS credit_limit,
                sn.sn_teleno AS phone,
                sn.sn_contact AS contact,
                sn.sn_stop AS on_stop
            FROM sname sn
            LEFT JOIN shist sh ON sn.sn_account = sh.si_account AND sh.si_age = 1
            WHERE sn.sn_currbal <> 0
            ORDER BY sn.sn_account
        """)

        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        # Calculate totals
        totals = {
            "balance": sum(r.get("balance", 0) or 0 for r in result),
            "current": sum(r.get("current_period", 0) or 0 for r in result),
            "month_1": sum(r.get("month_1", 0) or 0 for r in result),
            "month_2": sum(r.get("month_2", 0) or 0 for r in result),
            "month_3_plus": sum(r.get("month_3_plus", 0) or 0 for r in result),
        }

        return {
            "success": True,
            "data": result or [],
            "count": len(result) if result else 0,
            "totals": totals
        }

    except Exception as e:
        logger.error(f"Debtors report query failed: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/nominal/trial-balance")  # Moved to apps/dashboards/api/routes.py
async def _old_nominal_trial_balance(year: int = 2026):
    """
    Get summary trial balance from the nominal ledger.
    Returns account balances with debit/credit columns for the specified year.
    Only includes transactions dated within the specified year.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Query the ntran (nominal transactions) table filtered by year
        # Join with nacnt to get account descriptions
        # Sum all transactions for each account within the specified year
        result = sql_connector.execute_query(f"""
            SELECT
                RTRIM(t.nt_acnt) AS account_code,
                RTRIM(n.na_desc) AS description,
                RTRIM(t.nt_type) AS account_type,
                RTRIM(t.nt_subt) AS subtype,
                0 AS opening_balance,
                ISNULL(SUM(t.nt_value), 0) AS ytd_movement,
                CASE
                    WHEN ISNULL(SUM(t.nt_value), 0) > 0 THEN ISNULL(SUM(t.nt_value), 0)
                    ELSE 0
                END AS debit,
                CASE
                    WHEN ISNULL(SUM(t.nt_value), 0) < 0 THEN ABS(ISNULL(SUM(t.nt_value), 0))
                    ELSE 0
                END AS credit
            FROM ntran WITH (NOLOCK) t
            LEFT JOIN nacnt n ON RTRIM(t.nt_acnt) = RTRIM(n.na_acnt)
            WHERE t.nt_year = {year}
            GROUP BY t.nt_acnt, n.na_desc, t.nt_type, t.nt_subt
            HAVING ISNULL(SUM(t.nt_value), 0) <> 0
            ORDER BY t.nt_acnt
        """)

        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        # Calculate totals
        total_debit = sum(r.get("debit", 0) or 0 for r in result)
        total_credit = sum(r.get("credit", 0) or 0 for r in result)

        # Group by account type for summary
        type_summary = {}
        type_names = {
            'A': 'Fixed Assets',
            'B': 'Current Assets',
            'C': 'Current Liabilities',
            'D': 'Capital & Reserves',
            'E': 'Sales',
            'F': 'Cost of Sales',
            'G': 'Overheads',
            'H': 'Other'
        }
        for r in result:
            atype = r.get("account_type", "?").strip()
            if atype not in type_summary:
                type_summary[atype] = {
                    "name": type_names.get(atype, f"Type {atype}"),
                    "debit": 0,
                    "credit": 0,
                    "count": 0
                }
            type_summary[atype]["debit"] += r.get("debit", 0) or 0
            type_summary[atype]["credit"] += r.get("credit", 0) or 0
            type_summary[atype]["count"] += 1

        return {
            "success": True,
            "year": year,
            "data": result or [],
            "count": len(result) if result else 0,
            "totals": {
                "debit": total_debit,
                "credit": total_credit,
                "difference": total_debit - total_credit
            },
            "by_type": type_summary
        }

    except Exception as e:
        logger.error(f"Trial balance query failed: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/nominal/statutory-accounts")  # Moved to apps/dashboards/api/routes.py
async def _old_nominal_statutory_accounts(year: int = 2026):
    """
    Generate UK statutory accounts (P&L and Balance Sheet) from ntran.
    Returns formatted accounts following UK GAAP structure.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Query ntran for all transactions in the year, grouped by account type/subtype
        result = sql_connector.execute_query(f"""
            SELECT
                RTRIM(t.nt_type) AS account_type,
                RTRIM(t.nt_subt) AS subtype,
                RTRIM(t.nt_acnt) AS account_code,
                RTRIM(n.na_desc) AS description,
                ISNULL(SUM(t.nt_value), 0) AS value
            FROM ntran WITH (NOLOCK) t
            LEFT JOIN nacnt n ON RTRIM(t.nt_acnt) = RTRIM(n.na_acnt)
            WHERE t.nt_year = {year}
            GROUP BY t.nt_type, t.nt_subt, t.nt_acnt, n.na_desc
            HAVING ISNULL(SUM(t.nt_value), 0) <> 0
            ORDER BY t.nt_type, t.nt_subt, t.nt_acnt
        """)

        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        # UK Statutory Account Structure
        # P&L: E=Sales (Credit), F=Cost of Sales (Debit), G=Overheads (Debit)
        # Balance Sheet: A=Fixed Assets, B=Current Assets, C=Current Liabilities, D=Capital & Reserves

        # Build P&L
        turnover = []
        cost_of_sales = []
        administrative_expenses = []
        other_income = []

        # Build Balance Sheet
        fixed_assets = []
        current_assets = []
        current_liabilities = []
        capital_reserves = []

        for r in result:
            atype = (r.get("account_type") or "").strip()
            value = r.get("value", 0) or 0
            item = {
                "code": r.get("account_code", "").strip(),
                "description": r.get("description", ""),
                "value": value
            }

            if atype == "E":  # Sales (typically credit/negative in ledger)
                turnover.append(item)
            elif atype == "F":  # Cost of Sales
                cost_of_sales.append(item)
            elif atype == "G":  # Overheads/Admin
                administrative_expenses.append(item)
            elif atype == "H":  # Other
                other_income.append(item)
            elif atype == "A":  # Fixed Assets
                fixed_assets.append(item)
            elif atype == "B":  # Current Assets
                current_assets.append(item)
            elif atype == "C":  # Current Liabilities
                current_liabilities.append(item)
            elif atype == "D":  # Capital & Reserves
                capital_reserves.append(item)

        # Calculate P&L totals (Sales are negative/credit, so negate for display)
        total_turnover = -sum(i["value"] for i in turnover)  # Negate credits
        total_cos = sum(i["value"] for i in cost_of_sales)
        gross_profit = total_turnover - total_cos

        total_admin = sum(i["value"] for i in administrative_expenses)
        total_other = -sum(i["value"] for i in other_income)  # Negate if credit
        operating_profit = gross_profit - total_admin + total_other

        # Calculate Balance Sheet totals
        total_fixed = sum(i["value"] for i in fixed_assets)
        total_current_assets = sum(i["value"] for i in current_assets)
        total_current_liab = -sum(i["value"] for i in current_liabilities)  # Liabilities are credits
        net_current_assets = total_current_assets - total_current_liab
        total_assets_less_liab = total_fixed + net_current_assets
        total_capital = -sum(i["value"] for i in capital_reserves)  # Capital is credit

        return {
            "success": True,
            "year": year,
            "profit_and_loss": {
                "turnover": {
                    "items": turnover,
                    "total": total_turnover
                },
                "cost_of_sales": {
                    "items": cost_of_sales,
                    "total": total_cos
                },
                "gross_profit": gross_profit,
                "administrative_expenses": {
                    "items": administrative_expenses,
                    "total": total_admin
                },
                "other_operating_income": {
                    "items": other_income,
                    "total": total_other
                },
                "operating_profit": operating_profit,
                "profit_before_tax": operating_profit,
                "profit_after_tax": operating_profit  # Simplified - no tax calc
            },
            "balance_sheet": {
                "fixed_assets": {
                    "items": fixed_assets,
                    "total": total_fixed
                },
                "current_assets": {
                    "items": current_assets,
                    "total": total_current_assets
                },
                "current_liabilities": {
                    "items": current_liabilities,
                    "total": total_current_liab
                },
                "net_current_assets": net_current_assets,
                "total_assets_less_current_liabilities": total_assets_less_liab,
                "capital_and_reserves": {
                    "items": capital_reserves,
                    "total": total_capital
                }
            }
        }

    except Exception as e:
        logger.error(f"Statutory accounts query failed: {e}")
        return {"success": False, "error": str(e)}

# @app.post("/api/credit-control/query")  # Moved to apps/dashboards/api/routes.py
async def _old_credit_control_query(request: CreditControlQueryRequest):
    """
    Answer credit control questions using LIVE SQL queries.
    More accurate than RAG for precise financial data.
    Supports all 15+ credit control agent query types.
    """
    # Backend validation - check for empty question
    if not request.question or not request.question.strip():
        return {
            "success": False,
            "error": "Question is required. Please enter a credit control question.",
            "description": "Validation Error",
            "count": 0,
            "data": []
        }

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    question = request.question.lower()

    try:
        # Find matching query based on keywords (list is ordered by priority)
        matched_query = None
        for query_info in CREDIT_CONTROL_QUERIES:
            if any(kw in question for kw in query_info["keywords"]):
                matched_query = query_info
                break

        # Handle dynamic lookups that need parameter extraction
        if matched_query and matched_query["name"] == "customer_lookup":
            # Extract customer name from question
            words = request.question.split()
            search_terms = [w for w in words if len(w) > 2 and w.lower() not in
                          ["customer", "account", "find", "lookup", "details", "for", "info", "what", "about", "the", "tell", "me"]]
            if search_terms:
                search = "%".join(search_terms)
                sql = f"""SELECT sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                          sn_crlim AS credit_limit, sn_stop AS on_stop, sn_dormant AS dormant,
                          sn_teleno AS phone, sn_email AS email, sn_contact AS contact,
                          sn_lastrec AS last_payment, sn_lastinv AS last_invoice,
                          sn_priorty AS priority, sn_cmgroup AS credit_group,
                          sn_memo AS memo, sn_crdnotes AS credit_notes
                          FROM sname WHERE sn_name LIKE '%{search}%' OR sn_account LIKE '%{search}%'"""
            else:
                return {"success": False, "error": "Could not extract customer name from question"}

        elif matched_query and matched_query["name"] == "invoice_lookup":
            # Extract invoice number from question
            import re
            invoice_match = re.search(r'([A-Z0-9]{4,})', request.question.upper())
            if invoice_match:
                invoice = invoice_match.group(1)
                sql = f"""SELECT st.st_account AS account, sn.sn_name AS customer,
                         st.st_trref AS invoice, st.st_custref AS your_ref,
                         st.st_trdate AS invoice_date, st.st_dueday AS due_date,
                         st.st_trvalue AS original_value, st.st_trbal AS outstanding,
                         st.st_dispute AS disputed, st.st_memo AS memo,
                         st.st_payday AS promise_date,
                         DATEDIFF(day, st.st_dueday, GETDATE()) AS days_overdue
                         FROM stran WITH (NOLOCK) st
                         JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                         WHERE st.st_trref LIKE '%{invoice}%' AND st.st_trtype = 'I'"""
            else:
                return {"success": False, "error": "Could not extract invoice number from question"}

        elif matched_query and matched_query["name"] == "customer_master":
            # Extract customer name/account from question
            words = request.question.split()
            search_terms = [w for w in words if len(w) > 2 and w.lower() not in
                          ["customer", "master", "details", "record", "for", "info", "get", "show"]]
            if search_terms:
                search = "%".join(search_terms)
                sql = matched_query.get("param_sql", "").replace("{account}", search).replace("{customer}", search)
            else:
                # Return all customers if no specific one mentioned
                sql = """SELECT TOP 50 sn_account AS account, sn_name AS customer,
                        sn_currbal AS balance, sn_crlim AS credit_limit,
                        sn_teleno AS phone, sn_email AS email
                        FROM sname WHERE sn_currbal > 0 ORDER BY sn_currbal DESC"""

        elif matched_query:
            sql = matched_query["sql"]
        else:
            # Default: return summary of problem accounts
            sql = """SELECT TOP 20 sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                     sn_crlim AS credit_limit,
                     CASE WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER LIMIT'
                          WHEN sn_stop = 1 THEN 'ON STOP' ELSE 'OK' END AS status
                     FROM sname WHERE sn_currbal > 0 ORDER BY sn_currbal DESC"""
            matched_query = {"name": "summary", "category": "general", "description": "Summary of accounts with balances"}

        # Execute the SQL
        result = sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        if not result:
            return {
                "success": True,
                "query_type": matched_query["name"] if matched_query else "unknown",
                "category": matched_query.get("category", "general") if matched_query else "general",
                "description": matched_query["description"] if matched_query else "No matching query",
                "data": [],
                "count": 0,
                "summary": "No results found"
            }

        # Generate a summary based on query type
        query_name = matched_query["name"] if matched_query else "unknown"
        count = len(result)

        # Custom summaries for different query types
        if query_name == "top_debtors":
            total = sum(r.get("balance", 0) or 0 for r in result)
            summary = f"Top {count} debtors owe a total of £{total:,.2f}. Highest: {result[0]['customer'].strip()} (£{result[0]['balance']:,.2f})"
        elif query_name == "over_credit_limit":
            total_over = sum(r.get("over_by", 0) or 0 for r in result)
            summary = f"{count} customers are over their credit limit by a total of £{total_over:,.2f}"
        elif query_name == "accounts_on_stop":
            total_value = sum(r.get("balance", 0) or 0 for r in result)
            summary = f"{count} accounts on stop with total debt of £{total_value:,.2f}"
        elif query_name == "overdue_invoices":
            total_overdue = sum(r.get("outstanding", 0) or 0 for r in result)
            summary = f"{count} overdue invoices totaling £{total_overdue:,.2f}"
        elif query_name == "unallocated_cash":
            total_unalloc = sum(r.get("unallocated_amount", 0) or 0 for r in result)
            summary = f"{count} receipts with £{total_unalloc:,.2f} unallocated cash"
        elif query_name == "pending_credit_notes":
            total_credits = sum(r.get("credit_amount", 0) or 0 for r in result)
            summary = f"{count} pending credit notes totaling £{total_credits:,.2f}"
        elif query_name == "disputed_invoices":
            total_disputed = sum(r.get("outstanding", 0) or 0 for r in result)
            summary = f"{count} invoices in dispute totaling £{total_disputed:,.2f}"
        elif query_name == "promises_due":
            total_promised = sum(r.get("outstanding", 0) or 0 for r in result)
            summary = f"{count} promises due/overdue totaling £{total_promised:,.2f}"
        elif query_name == "broken_promises":
            total_broken = sum(r.get("broken_promise_count", 0) or 0 for r in result)
            summary = f"{count} customers with {total_broken} total broken promises"
        elif query_name == "unallocated_cash_old":
            total_old = sum(r.get("unallocated_amount", 0) or 0 for r in result)
            summary = f"{count} receipts with £{total_old:,.2f} unallocated for over 7 days - needs reconciliation"
        elif query_name == "overdue_by_age":
            total = sum(r.get("total_overdue", 0) or 0 for r in result)
            summary = f"{count} customers with £{total:,.2f} overdue debt across all age brackets"
        elif query_name == "customer_balance_aging":
            total = sum(r.get("total_balance", 0) or 0 for r in result)
            summary = f"{count} customers with £{total:,.2f} total outstanding balance"
        elif query_name == "recent_payments":
            total_paid = sum(r.get("amount", 0) or 0 for r in result)
            summary = f"{count} recent payments totaling £{total_paid:,.2f}"
        elif query_name == "account_status":
            summary = f"{count} accounts with stop, hold, or dormant flags"
        elif query_name == "customer_notes":
            summary = f"{count} notes/contact log entries found"
        else:
            summary = f"Found {count} records"

        return {
            "success": True,
            "query_type": query_name,
            "category": matched_query.get("category", "general") if matched_query else "general",
            "description": matched_query["description"] if matched_query else "Custom query",
            "data": result,
            "count": count,
            "summary": summary,
            "sql_used": sql
        }

    except Exception as e:
        logger.error(f"Credit control query failed: {e}")
        return {"success": False, "error": str(e)}

# ============ Cashflow Forecast Endpoint ============

# @app.get("/api/cashflow/forecast")  # Moved to apps/dashboards/api/routes.py
async def _old_cashflow_forecast(years_history: int = 3):
    """
    Generate cashflow forecast based on historical transaction patterns.
    Analyzes receipts (money in) and payments (money out) to predict monthly cashflow.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from datetime import datetime
        from collections import defaultdict

        current_year = datetime.now().year
        current_month = datetime.now().month

        # First, get the date range of available data
        date_range_sql = """
            SELECT
                MIN(st_trdate) as min_date,
                MAX(st_trdate) as max_date,
                DATEDIFF(year, MIN(st_trdate), MAX(st_trdate)) + 1 as years_span
            FROM stran
            WHERE st_trtype = 'R'
        """
        date_range = sql_connector.execute_query(date_range_sql)
        if hasattr(date_range, 'to_dict'):
            date_range = date_range.to_dict('records')

        # Use all available data if years_history covers more than available
        # Or limit to requested years from the most recent data
        use_all_data = True  # Default to using all available historical data

        # Get historical receipts by month (from sales ledger)
        if use_all_data:
            receipts_sql = """
                SELECT
                    YEAR(st_trdate) AS year,
                    MONTH(st_trdate) AS month,
                    COUNT(*) AS transaction_count,
                    SUM(ABS(st_trvalue)) AS total_amount
                FROM stran
                WHERE st_trtype = 'R'
                GROUP BY YEAR(st_trdate), MONTH(st_trdate)
                ORDER BY year, month
            """
        else:
            receipts_sql = f"""
                SELECT
                    YEAR(st_trdate) AS year,
                    MONTH(st_trdate) AS month,
                    COUNT(*) AS transaction_count,
                    SUM(ABS(st_trvalue)) AS total_amount
                FROM stran
                WHERE st_trtype = 'R'
                AND st_trdate >= DATEADD(year, -{years_history}, GETDATE())
                GROUP BY YEAR(st_trdate), MONTH(st_trdate)
                ORDER BY year, month
            """
        receipts_result = sql_connector.execute_query(receipts_sql)
        if hasattr(receipts_result, 'to_dict'):
            receipts_result = receipts_result.to_dict('records')

        # Get historical payments by month (from purchase ledger)
        if use_all_data:
            payments_sql = """
                SELECT
                    YEAR(pt_trdate) AS year,
                    MONTH(pt_trdate) AS month,
                    COUNT(*) AS transaction_count,
                    SUM(ABS(pt_trvalue)) AS total_amount
                FROM ptran
                WHERE pt_trtype = 'P'
                GROUP BY YEAR(pt_trdate), MONTH(pt_trdate)
                ORDER BY year, month
            """
        else:
            payments_sql = f"""
                SELECT
                    YEAR(pt_trdate) AS year,
                    MONTH(pt_trdate) AS month,
                    COUNT(*) AS transaction_count,
                    SUM(ABS(pt_trvalue)) AS total_amount
                FROM ptran
                WHERE pt_trtype = 'P'
                AND pt_trdate >= DATEADD(year, -{years_history}, GETDATE())
                GROUP BY YEAR(pt_trdate), MONTH(pt_trdate)
                ORDER BY year, month
            """
        payments_result = sql_connector.execute_query(payments_sql)
        if hasattr(payments_result, 'to_dict'):
            payments_result = payments_result.to_dict('records')

        # Get payroll history (weekly pay converted to monthly estimates)
        # whist uses tax year format (1819 = tax year 2018/19, April to April)
        # wh_period is the week number (1-52)
        payroll_sql = """
            SELECT
                wh_year as tax_year,
                wh_period as week,
                SUM(CAST(wh_net AS FLOAT)) as net_pay,
                SUM(CAST(wh_erni AS FLOAT)) as employer_ni
            FROM whist
            GROUP BY wh_year, wh_period
            ORDER BY wh_year DESC, wh_period DESC
        """
        payroll_result = sql_connector.execute_query(payroll_sql)
        if hasattr(payroll_result, 'to_dict'):
            payroll_result = payroll_result.to_dict('records')

        # Get recurring expenses from nominal ledger by period
        # Focus on key expense categories that represent cash outflows
        expenses_sql = """
            SELECT
                nt_year as year,
                nt_period as period,
                SUM(CASE WHEN nt_acnt LIKE 'W%' THEN CAST(nt_value AS FLOAT) ELSE 0 END) as payroll_related,
                SUM(CASE WHEN nt_acnt IN ('Q125') THEN CAST(nt_value AS FLOAT) ELSE 0 END) as rent_rates,
                SUM(CASE WHEN nt_acnt IN ('Q120') THEN CAST(nt_value AS FLOAT) ELSE 0 END) as utilities,
                SUM(CASE WHEN nt_acnt LIKE 'Q13%' THEN CAST(nt_value AS FLOAT) ELSE 0 END) as insurance,
                SUM(CASE WHEN nt_acnt IN ('U230') THEN CAST(nt_value AS FLOAT) ELSE 0 END) as loan_interest
            FROM ntran
            WHERE RTRIM(nt_type) IN ('45', 'H')  -- Expenses / Overheads (H is Opera letter code)
            GROUP BY nt_year, nt_period
            ORDER BY nt_year DESC, nt_period DESC
        """
        expenses_result = sql_connector.execute_query(expenses_sql)
        if hasattr(expenses_result, 'to_dict'):
            expenses_result = expenses_result.to_dict('records')

        # Build historical data structure
        historical_receipts = defaultdict(list)  # month -> [amounts]
        historical_payments = defaultdict(list)  # month -> [amounts]
        historical_payroll = defaultdict(list)  # month -> [amounts]
        historical_recurring = defaultdict(list)  # month -> [amounts]

        for r in receipts_result or []:
            month = int(r['month'])
            amount = float(r['total_amount'] or 0)
            historical_receipts[month].append(amount)

        for p in payments_result or []:
            month = int(p['month'])
            amount = float(p['total_amount'] or 0)
            historical_payments[month].append(amount)

        # Process payroll data - convert weekly to monthly
        # Tax year 1819 starts April 2018, 1718 starts April 2017, etc.
        # Week 1-4 = April (month 4), Week 5-8 = May (month 5), etc.
        for pay in payroll_result or []:
            week = int(pay['week'])
            net_pay = float(pay['net_pay'] or 0)
            employer_ni = float(pay['employer_ni'] or 0)
            total_payroll = net_pay + employer_ni

            # Convert week to month (approximate)
            # Week 1-4 = Month 4 (April), Week 5-8 = Month 5 (May), etc.
            month_offset = (week - 1) // 4
            month = ((month_offset + 3) % 12) + 1  # Tax year starts in April

            historical_payroll[month].append(total_payroll)

        # Process recurring expenses - ntran uses calendar periods (1-12)
        for exp in expenses_result or []:
            period = int(exp['period'])
            if 1 <= period <= 12:
                rent = float(exp['rent_rates'] or 0)
                utilities = float(exp['utilities'] or 0)
                insurance = float(exp['insurance'] or 0)
                loan_interest = float(exp['loan_interest'] or 0)
                total_recurring = rent + utilities + insurance + loan_interest
                if total_recurring > 0:
                    historical_recurring[period].append(total_recurring)

        # Calculate averages and build forecast for current year
        forecast = []
        month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December']

        annual_receipts_total = 0
        annual_payments_total = 0
        annual_payroll_total = 0
        annual_recurring_total = 0

        for month in range(1, 13):
            receipts_history = historical_receipts.get(month, [])
            payments_history = historical_payments.get(month, [])
            payroll_history = historical_payroll.get(month, [])
            recurring_history = historical_recurring.get(month, [])

            avg_receipts = sum(receipts_history) / len(receipts_history) if receipts_history else 0
            avg_payments = sum(payments_history) / len(payments_history) if payments_history else 0
            avg_payroll = sum(payroll_history) / len(payroll_history) if payroll_history else 0
            avg_recurring = sum(recurring_history) / len(recurring_history) if recurring_history else 0

            # Total expected payments includes purchase payments + payroll + recurring expenses
            total_expected_payments = avg_payments + avg_payroll + avg_recurring
            net_cashflow = avg_receipts - total_expected_payments

            # Determine if this is actual or forecast
            is_actual = month < current_month
            is_current = month == current_month

            annual_receipts_total += avg_receipts
            annual_payments_total += avg_payments
            annual_payroll_total += avg_payroll
            annual_recurring_total += avg_recurring

            forecast.append({
                "month": month,
                "month_name": month_names[month],
                "expected_receipts": round(avg_receipts, 2),
                "expected_payments": round(total_expected_payments, 2),
                "purchase_payments": round(avg_payments, 2),
                "payroll": round(avg_payroll, 2),
                "recurring_expenses": round(avg_recurring, 2),
                "net_cashflow": round(net_cashflow, 2),
                "receipts_data_points": len(receipts_history),
                "payments_data_points": len(payments_history) + len(payroll_history) + len(recurring_history),
                "status": "actual" if is_actual else ("current" if is_current else "forecast")
            })

        # Get actual YTD data for comparison
        ytd_receipts_sql = f"""
            SELECT SUM(ABS(st_trvalue)) AS total
            FROM stran
            WHERE st_trtype = 'R' AND YEAR(st_trdate) = {current_year}
        """
        ytd_receipts = sql_connector.execute_query(ytd_receipts_sql)
        if hasattr(ytd_receipts, 'to_dict'):
            ytd_receipts = ytd_receipts.to_dict('records')
        ytd_receipts_value = float(ytd_receipts[0]['total'] or 0) if ytd_receipts else 0

        ytd_payments_sql = f"""
            SELECT SUM(ABS(pt_trvalue)) AS total
            FROM ptran
            WHERE pt_trtype = 'P' AND YEAR(pt_trdate) = {current_year}
        """
        ytd_payments = sql_connector.execute_query(ytd_payments_sql)
        if hasattr(ytd_payments, 'to_dict'):
            ytd_payments = ytd_payments.to_dict('records')
        ytd_payments_value = float(ytd_payments[0]['total'] or 0) if ytd_payments else 0

        # Get bank balances from nominal ledger using nbank to identify bank accounts
        bank_sql = """
            SELECT n.na_acnt AS account,
                   n.na_desc AS description,
                   (ISNULL(n.na_ytddr, 0) - ISNULL(n.na_ytdcr, 0)) AS balance
            FROM nacnt n WITH (NOLOCK)
            INNER JOIN nbank b WITH (NOLOCK) ON RTRIM(n.na_acnt) = RTRIM(b.nk_acnt)
            ORDER BY n.na_acnt
        """
        bank_result = sql_connector.execute_query(bank_sql)
        if hasattr(bank_result, 'to_dict'):
            bank_result = bank_result.to_dict('records')

        total_bank_balance = sum(float(b['balance'] or 0) for b in bank_result or [])

        # Total expected payments includes all categories
        total_annual_payments = annual_payments_total + annual_payroll_total + annual_recurring_total

        return {
            "success": True,
            "forecast_year": current_year,
            "years_of_history": years_history,
            "monthly_forecast": forecast,
            "summary": {
                "annual_expected_receipts": round(annual_receipts_total, 2),
                "annual_expected_payments": round(total_annual_payments, 2),
                "annual_purchase_payments": round(annual_payments_total, 2),
                "annual_payroll": round(annual_payroll_total, 2),
                "annual_recurring_expenses": round(annual_recurring_total, 2),
                "annual_expected_net": round(annual_receipts_total - total_annual_payments, 2),
                "ytd_actual_receipts": round(ytd_receipts_value, 2),
                "ytd_actual_payments": round(ytd_payments_value, 2),
                "ytd_actual_net": round(ytd_receipts_value - ytd_payments_value, 2),
                "current_bank_balance": round(total_bank_balance, 2)
            },
            "bank_accounts": bank_result or []
        }

    except Exception as e:
        logger.error(f"Cashflow forecast failed: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/cashflow/history")  # Moved to apps/dashboards/api/routes.py
async def _old_cashflow_history():
    """
    Get detailed cashflow history by year and month.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get all receipts by year/month
        receipts_sql = """
            SELECT
                YEAR(st_trdate) AS year,
                MONTH(st_trdate) AS month,
                COUNT(*) AS count,
                SUM(ABS(st_trvalue)) AS total
            FROM stran
            WHERE st_trtype = 'R'
            GROUP BY YEAR(st_trdate), MONTH(st_trdate)
            ORDER BY year DESC, month DESC
        """
        receipts = sql_connector.execute_query(receipts_sql)
        if hasattr(receipts, 'to_dict'):
            receipts = receipts.to_dict('records')

        # Get all payments by year/month
        payments_sql = """
            SELECT
                YEAR(pt_trdate) AS year,
                MONTH(pt_trdate) AS month,
                COUNT(*) AS count,
                SUM(ABS(pt_trvalue)) AS total
            FROM ptran
            WHERE pt_trtype = 'P'
            GROUP BY YEAR(pt_trdate), MONTH(pt_trdate)
            ORDER BY year DESC, month DESC
        """
        payments = sql_connector.execute_query(payments_sql)
        if hasattr(payments, 'to_dict'):
            payments = payments.to_dict('records')

        # Combine into yearly summary
        yearly_data = {}
        for r in receipts or []:
            year = int(r['year'])
            month = int(r['month'])
            if year not in yearly_data:
                yearly_data[year] = {"receipts": {}, "payments": {}}
            yearly_data[year]["receipts"][month] = float(r['total'] or 0)

        for p in payments or []:
            year = int(p['year'])
            month = int(p['month'])
            if year not in yearly_data:
                yearly_data[year] = {"receipts": {}, "payments": {}}
            yearly_data[year]["payments"][month] = float(p['total'] or 0)

        # Format output
        history = []
        for year in sorted(yearly_data.keys(), reverse=True):
            year_receipts = sum(yearly_data[year]["receipts"].values())
            year_payments = sum(yearly_data[year]["payments"].values())
            history.append({
                "year": year,
                "total_receipts": round(year_receipts, 2),
                "total_payments": round(year_payments, 2),
                "net_cashflow": round(year_receipts - year_payments, 2),
                "monthly_receipts": yearly_data[year]["receipts"],
                "monthly_payments": yearly_data[year]["payments"]
            })

        return {
            "success": True,
            "history": history
        }

    except Exception as e:
        logger.error(f"Cashflow history failed: {e}")
        return {"success": False, "error": str(e)}

# ============ LLM Test Endpoint ============

@app.post("/api/llm/test")
async def test_llm(prompt: str = Query("Hello, how are you?", description="Test prompt")):
    """Test the LLM connection."""
    if not llm:
        raise HTTPException(status_code=503, detail="LLM not initialized")

    try:
        response = llm.get_completion(prompt)
        return {"success": True, "response": response}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ============ Email Module Helper Functions ============

async def _initialize_email_providers():
    """Initialize email providers from config and database."""
    global email_storage, email_sync_manager

    if not email_storage or not email_sync_manager:
        return

    # Load email config from company JSON (authoritative source)
    # If the company file has email settings, use them to ensure the DB provider
    # always has the correct config (survives DB resets and password mis-saves)
    company_email_config = None
    try:
        co_id = _get_active_company_id()
        if co_id:
            co_data = load_company(co_id)
            if co_data and co_data.get('email'):
                company_email_config = co_data['email']
                logger.info(f"Found email config in company '{co_id}' JSON: server={company_email_config.get('server')}")
    except Exception:
        pass

    # Register existing providers from the database
    try:
        existing_providers = email_storage.get_all_providers(enabled_only=True)
        for provider_info in existing_providers:
            provider_id = provider_info['id']
            provider_type = provider_info['provider_type']
            provider_config = provider_info.get('config', {})

            # If company JSON has email config, use it to fill missing/empty DB fields
            if company_email_config and provider_type == 'imap':
                for key in ('server', 'port', 'username', 'password', 'use_ssl'):
                    company_val = company_email_config.get(key)
                    if company_val and not provider_config.get(key):
                        provider_config[key] = company_val
                        logger.info(f"Backfilled email provider {key} from company JSON")

            if not provider_config or not provider_config.get('server'):
                logger.warning(f"Provider {provider_id} ({provider_info['name']}) has no config, skipping")
                continue

            try:
                if provider_type == 'imap':
                    provider = IMAPProvider(provider_config)
                    email_sync_manager.register_provider(provider_id, provider)
                    logger.info(f"Registered IMAP provider: {provider_info['name']} (id={provider_id})")
                # Add other provider types here as needed (microsoft, gmail)
            except Exception as e:
                logger.warning(f"Could not register provider {provider_info['name']}: {e}")
    except Exception as e:
        logger.warning(f"Error loading providers from database: {e}")

    # Also check for IMAP provider in config.ini (legacy support)
    if config and config.has_section('email_imap') and config.getboolean('email_imap', 'enabled', fallback=False):
        try:
            imap_config = {
                'server': config.get('email_imap', 'server', fallback=''),
                'port': config.getint('email_imap', 'port', fallback=993),
                'username': config.get('email_imap', 'username', fallback=''),
                'password': config.get('email_imap', 'password', fallback=''),
                'use_ssl': config.getboolean('email_imap', 'use_ssl', fallback=True),
            }

            if imap_config['server'] and imap_config['username']:
                # Check if provider already exists in DB
                providers = email_storage.get_all_providers()
                imap_provider_db = next((p for p in providers if p['provider_type'] == 'imap'), None)

                if not imap_provider_db:
                    provider_id = email_storage.add_provider(
                        name='IMAP',
                        provider_type=ProviderType.IMAP,
                        config=imap_config
                    )
                    provider = IMAPProvider(imap_config)
                    email_sync_manager.register_provider(provider_id, provider)
                    logger.info("IMAP provider from config.ini registered")
        except Exception as e:
            logger.warning(f"Could not initialize IMAP provider from config: {e}")

# ============ Email Pydantic Models ============

class EmailProviderCreate(BaseModel):
    name: str
    provider_type: Literal['microsoft', 'gmail', 'imap']
    # IMAP config
    server: Optional[str] = None
    port: Optional[int] = 993
    username: Optional[str] = None
    password: Optional[str] = None
    use_ssl: bool = True
    # Microsoft config
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    # Gmail config
    credentials_json: Optional[str] = None
    # Common
    user_email: Optional[str] = None

class EmailProviderResponse(BaseModel):
    id: int
    name: str
    provider_type: str
    enabled: bool
    last_sync: Optional[str]
    sync_status: str

class EmailListParams(BaseModel):
    provider_id: Optional[int] = None
    folder_id: Optional[int] = None
    category: Optional[str] = None
    linked_account: Optional[str] = None
    is_read: Optional[bool] = None
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    search: Optional[str] = None
    page: int = 1
    page_size: int = 50

class EmailLinkRequest(BaseModel):
    account_code: str

class CategoryUpdateRequest(BaseModel):
    category: Literal['payment', 'query', 'complaint', 'order', 'other']
    reason: Optional[str] = None

# ============ Email API Endpoints ============

@app.get("/api/email/providers")
async def list_email_providers():
    """List all configured email providers."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        providers = email_storage.get_all_providers()
        # Remove sensitive config data
        for p in providers:
            if 'config' in p:
                del p['config']
            if 'config_json' in p:
                del p['config_json']
        return {"success": True, "providers": providers}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/email/providers/{provider_id}")
async def get_email_provider(provider_id: int):
    """Get a single email provider's configuration (for editing)."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        provider = email_storage.get_provider(provider_id)
        if not provider:
            return {"success": False, "error": "Provider not found"}

        # Return provider with config for editing
        # Remove sensitive fields (passwords/secrets) for security
        config = provider.get('config', {}).copy()
        if 'password' in config:
            del config['password']
        if 'client_secret' in config:
            del config['client_secret']

        return {
            "success": True,
            "provider": {
                "id": provider['id'],
                "name": provider['name'],
                "provider_type": provider['provider_type'],
                "enabled": provider['enabled'],
                "config": config
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/email/providers")
async def add_email_provider(provider: EmailProviderCreate):
    """Add a new email provider."""
    import sqlite3

    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        # Validate required fields
        if not provider.name or not provider.name.strip():
            return {"success": False, "error": "Provider name is required"}

        # Build config based on provider type
        if provider.provider_type == 'imap':
            if not provider.server:
                return {"success": False, "error": "IMAP server is required"}
            if not provider.username:
                return {"success": False, "error": "IMAP username is required"}
            if not provider.password:
                return {"success": False, "error": "IMAP password is required"}
            provider_config = {
                'server': provider.server,
                'port': provider.port or 993,
                'username': provider.username,
                'password': provider.password,
                'use_ssl': provider.use_ssl,
            }
        elif provider.provider_type == 'microsoft':
            if not provider.tenant_id:
                return {"success": False, "error": "Tenant ID is required for Microsoft provider"}
            if not provider.client_id:
                return {"success": False, "error": "Client ID is required for Microsoft provider"}
            if not provider.client_secret:
                return {"success": False, "error": "Client Secret is required for Microsoft provider"}
            if not provider.user_email:
                return {"success": False, "error": "User email is required for Microsoft provider"}
            provider_config = {
                'tenant_id': provider.tenant_id,
                'client_id': provider.client_id,
                'client_secret': provider.client_secret,
                'user_email': provider.user_email,
            }
        elif provider.provider_type == 'gmail':
            provider_config = {
                'credentials_json': provider.credentials_json,
                'user_email': provider.user_email,
            }
        else:
            return {"success": False, "error": f"Invalid provider type: {provider.provider_type}"}

        provider_id = email_storage.add_provider(
            name=provider.name.strip(),
            provider_type=ProviderType(provider.provider_type),
            config=provider_config
        )

        # Register provider with sync manager if IMAP
        if provider.provider_type == 'imap' and email_sync_manager:
            imap_provider = IMAPProvider(provider_config)
            email_sync_manager.register_provider(provider_id, imap_provider)

        return {"success": True, "provider_id": provider_id}
    except sqlite3.IntegrityError as e:
        if 'UNIQUE constraint failed' in str(e):
            return {"success": False, "error": f"A provider with the name '{provider.name}' already exists. Please use a different name."}
        return {"success": False, "error": f"Database error: {e}"}
    except Exception as e:
        logger.error(f"Failed to add email provider: {e}")
        return {"success": False, "error": str(e)}

@app.delete("/api/email/providers/{provider_id}")
async def delete_email_provider(provider_id: int):
    """Delete an email provider."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        if email_sync_manager:
            email_sync_manager.unregister_provider(provider_id)

        result = email_storage.delete_provider(provider_id)
        return {"success": result}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.put("/api/email/providers/{provider_id}")
async def update_email_provider(provider_id: int, provider: EmailProviderCreate):
    """Update an existing email provider's configuration."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        # Get existing provider to verify it exists
        existing = email_storage.get_provider(provider_id)
        if not existing:
            return {"success": False, "error": "Provider not found"}

        existing_config = existing.get('config', {})

        # Build config based on provider type
        # Keep existing password/secret if not provided (for security - we don't send passwords back to frontend)
        if provider.provider_type == 'imap':
            provider_config = {
                'server': provider.server,
                'port': provider.port or 993,
                'username': provider.username,
                'password': provider.password if provider.password else existing_config.get('password', ''),
                'use_ssl': provider.use_ssl
            }
        elif provider.provider_type == 'microsoft':
            provider_config = {
                'tenant_id': provider.tenant_id,
                'client_id': provider.client_id,
                'client_secret': provider.client_secret if provider.client_secret else existing_config.get('client_secret', ''),
                'user_email': provider.user_email
            }
        else:
            provider_config = {}

        # Update provider in database
        import json
        email_storage.update_provider(
            provider_id,
            name=provider.name.strip(),
            config_json=json.dumps(provider_config)
        )

        # Re-register provider with sync manager if IMAP
        if provider.provider_type == 'imap' and email_sync_manager:
            # Unregister old provider first
            email_sync_manager.unregister_provider(provider_id)
            # Register with new config
            imap_provider = IMAPProvider(provider_config)
            email_sync_manager.register_provider(provider_id, imap_provider)
            logger.info(f"Updated and re-registered IMAP provider: {provider.name}")

        return {"success": True, "message": "Provider updated successfully"}
    except Exception as e:
        logger.error(f"Failed to update email provider: {e}")
        return {"success": False, "error": str(e)}

@app.post("/api/email/providers/{provider_id}/test")
async def test_email_provider(provider_id: int):
    """Test connection to an email provider."""
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email module not initialized")

    try:
        if provider_id not in email_sync_manager.providers:
            return {"success": False, "error": "Provider not registered"}

        provider = email_sync_manager.providers[provider_id]
        result = await provider.test_connection()
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/email/providers/{provider_id}/folders")
async def list_email_folders(provider_id: int):
    """List folders for an email provider."""
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email module not initialized")

    try:
        if provider_id not in email_sync_manager.providers:
            return {"success": False, "error": "Provider not registered"}

        provider = email_sync_manager.providers[provider_id]
        if not provider.is_authenticated:
            await provider.authenticate()

        folders = await provider.list_folders()

        # Also get stored folder info
        stored_folders = email_storage.get_folders(provider_id)
        stored_map = {f['folder_id']: f for f in stored_folders}

        result = []
        for folder in folders:
            folder_dict = folder.to_dict()
            if folder.folder_id in stored_map:
                folder_dict['monitored'] = bool(stored_map[folder.folder_id].get('monitored', False))
                folder_dict['db_id'] = stored_map[folder.folder_id]['id']
            else:
                folder_dict['monitored'] = False
                folder_dict['db_id'] = None
            result.append(folder_dict)

        return {"success": True, "folders": result}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/email/messages")
async def list_emails(
    provider_id: Optional[int] = None,
    folder_id: Optional[int] = None,
    category: Optional[str] = None,
    linked_account: Optional[str] = None,
    is_read: Optional[bool] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    search: Optional[str] = None,
    page: int = 1,
    page_size: int = 50
):
    """List emails with filtering and pagination."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        # Parse dates
        from_dt = datetime.fromisoformat(from_date) if from_date else None
        to_dt = datetime.fromisoformat(to_date) if to_date else None

        result = email_storage.get_emails(
            provider_id=provider_id,
            folder_id=folder_id,
            category=category,
            linked_account=linked_account,
            is_read=is_read,
            from_date=from_dt,
            to_date=to_dt,
            search=search,
            page=page,
            page_size=page_size
        )

        # Add customer names for linked emails
        if customer_linker:
            for email in result['emails']:
                if email.get('linked_account'):
                    name = customer_linker.get_customer_name(email['linked_account'])
                    email['linked_customer_name'] = name

        return {"success": True, **result}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/email/messages/{email_id}")
async def get_email_detail(email_id: int):
    """Get full email details."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        email = email_storage.get_email_by_id(email_id)
        if not email:
            raise HTTPException(status_code=404, detail="Email not found")

        # Add customer name if linked
        if email.get('linked_account') and customer_linker:
            email['linked_customer_name'] = customer_linker.get_customer_name(email['linked_account'])

        return {"success": True, "email": email}
    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/email/sync")
async def trigger_email_sync(request: Request, provider_id: Optional[int] = None):
    """Trigger email sync (all providers or specific one)."""
    if not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email sync manager not initialized")

    try:
        if provider_id:
            result = await email_sync_manager.sync_provider(provider_id)
        else:
            result = await email_sync_manager.sync_all_providers()
        return {"success": True, "result": result}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/email/sync/status")
async def get_sync_status():
    """Get email sync status."""
    if not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email sync manager not initialized")

    try:
        status = email_sync_manager.get_sync_status()
        return {"success": True, **status}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/email/sync/log")
async def get_sync_history(provider_id: Optional[int] = None, limit: int = 20):
    """Get sync history log."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        history = email_storage.get_sync_history(provider_id=provider_id, limit=limit)
        return {"success": True, "history": history}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.put("/api/email/messages/{email_id}/category")
async def update_email_category(email_id: int, request: CategoryUpdateRequest):
    """Update email category."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        result = email_storage.update_email_category(
            email_id=email_id,
            category=request.category,
            confidence=1.0,  # Manual override
            reason=request.reason or "Manual classification"
        )
        return {"success": result}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/email/messages/{email_id}/link")
async def link_email_to_customer(email_id: int, request: EmailLinkRequest):
    """Link an email to a customer account."""
    # Backend validation - check for empty account code
    if not request.account_code or not request.account_code.strip():
        return {"success": False, "error": "Account code is required. Please enter a customer account code."}

    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        result = email_storage.link_email_to_customer(
            email_id=email_id,
            account_code=request.account_code.strip(),
            linked_by='manual'
        )
        return {"success": result}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.delete("/api/email/messages/{email_id}/link")
async def unlink_email_from_customer(email_id: int):
    """Remove customer link from an email."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        result = email_storage.unlink_email(email_id)
        return {"success": result}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/email/by-customer/{account_code}")
async def get_emails_by_customer(account_code: str):
    """Get all emails linked to a customer account."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        emails = email_storage.get_emails_by_customer(account_code)
        return {"success": True, "emails": emails, "count": len(emails)}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/email/messages/{email_id}/attachments/{attachment_id}/download")
async def download_email_attachment(email_id: int, attachment_id: str, save_to: Optional[str] = None):
    """
    Download an email attachment.

    Args:
        email_id: The database email ID
        attachment_id: The attachment ID from the attachment list
        save_to: Optional path to save the file (if not provided, returns file info and content path)

    Returns:
        File content or save path
    """
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email module not initialized")

    try:
        # Get the email to find provider and message info
        email = email_storage.get_email_by_id(email_id)
        if not email:
            raise HTTPException(status_code=404, detail="Email not found")

        provider_id = email.get('provider_id')
        message_id = email.get('message_id')

        logger.info(f"Downloading attachment: email_id={email_id}, provider_id={provider_id}, message_id={message_id}")
        logger.info(f"Registered providers: {list(email_sync_manager.providers.keys())}")

        if provider_id not in email_sync_manager.providers:
            raise HTTPException(status_code=404, detail=f"Email provider {provider_id} not found. Available: {list(email_sync_manager.providers.keys())}")

        provider = email_sync_manager.providers[provider_id]

        # Find the attachment metadata
        attachments = email.get('attachments', [])
        attachment_meta = next(
            (a for a in attachments if str(a.get('attachment_id')) == str(attachment_id)),
            None
        )
        if not attachment_meta:
            raise HTTPException(status_code=404, detail=f"Attachment {attachment_id} not found in attachments: {attachments}")

        # Get folder_id - need to look it up from database
        folder_id_db = email.get('folder_id')
        folder_id = 'INBOX'  # Default
        if folder_id_db:
            # Look up folder name from email_folders table
            with email_storage._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_db,))
                row = cursor.fetchone()
                if row:
                    folder_id = row['folder_id']

        logger.info(f"Downloading from folder: {folder_id}, attachment_id: {attachment_id}")

        # Download the attachment
        try:
            result = await provider.download_attachment(message_id, attachment_id, folder_id)
            logger.info(f"Download result: {type(result)}, is None: {result is None}")
        except Exception as dl_err:
            logger.error(f"Download exception: {dl_err}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Download exception: {str(dl_err)}")

        if not result:
            raise HTTPException(status_code=404, detail=f"Failed to download attachment from provider. message_id={message_id}, folder={folder_id}")

        content, filename, content_type = result

        # Save to specified path or temp location
        import tempfile
        import os

        if save_to:
            # Save to user-specified path
            save_path = save_to
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
        else:
            # Save to temp directory
            temp_dir = tempfile.gettempdir()
            save_path = os.path.join(temp_dir, f"email_attachment_{email_id}_{filename}")

        with open(save_path, 'wb') as f:
            f.write(content)

        return {
            "success": True,
            "filename": filename,
            "content_type": content_type,
            "size_bytes": len(content),
            "saved_to": save_path
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading attachment: {e}")
        return {"success": False, "error": str(e)}

@app.get("/api/email/messages/{email_id}/attachments/{attachment_id}/view")
async def view_email_attachment(email_id: int, attachment_id: str):
    """Serve an email attachment directly for browser viewing (e.g. PDF in iframe)."""
    from fastapi.responses import Response

    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email module not initialized")

    email = email_storage.get_email_by_id(email_id)
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")

    provider_id = email.get('provider_id')
    if provider_id not in email_sync_manager.providers:
        raise HTTPException(status_code=404, detail="Email provider not connected")

    provider = email_sync_manager.providers[provider_id]
    attachment_meta = next(
        (a for a in email.get('attachments', []) if str(a.get('attachment_id')) == str(attachment_id)),
        None
    )
    if not attachment_meta:
        raise HTTPException(status_code=404, detail="Attachment not found")

    folder_id_db = email.get('folder_id')
    folder_id = 'INBOX'
    if folder_id_db:
        with email_storage._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_db,))
            row = cursor.fetchone()
            if row:
                folder_id = row['folder_id']

    result = await provider.download_attachment(email.get('message_id'), attachment_id, folder_id)
    if not result:
        raise HTTPException(status_code=404, detail="Failed to download attachment")

    content, filename, content_type = result
    return Response(
        content=content,
        media_type=content_type or 'application/octet-stream',
        headers={"Content-Disposition": f'inline; filename="{filename}"'}
    )

@app.get("/api/email/debug/imap-search")
async def debug_imap_search(provider_id: int, message_id: str, folder: str = 'INBOX'):
    """Debug endpoint to test IMAP search."""
    if not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email sync manager not initialized")

    if provider_id not in email_sync_manager.providers:
        return {
            "success": False,
            "error": f"Provider {provider_id} not in providers",
            "available_providers": list(email_sync_manager.providers.keys())
        }

    provider = email_sync_manager.providers[provider_id]

    try:
        import asyncio
        loop = asyncio.get_event_loop()

        # Ensure connection
        if not provider._connection:
            await provider.authenticate()

        # Select folder
        status, count = await loop.run_in_executor(
            None, lambda: provider._connection.select(folder, readonly=True)
        )

        # Search by Message-ID
        search_criteria = f'(HEADER Message-ID "<{message_id}>")'
        status2, msg_ids = await loop.run_in_executor(
            None, lambda: provider._connection.search(None, search_criteria)
        )

        # Get all emails
        status3, all_ids = await loop.run_in_executor(
            None, lambda: provider._connection.search(None, 'ALL')
        )

        total_emails = len(all_ids[0].split()) if all_ids[0] else 0

        # Get Message-IDs of last 10 emails
        sample_msg_ids = []
        if all_ids[0]:
            for imap_id in all_ids[0].split()[-10:]:
                try:
                    s, headers = await loop.run_in_executor(
                        None, lambda mid=imap_id: provider._connection.fetch(mid, '(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID SUBJECT)])')
                    )
                    if s == 'OK' and headers[0]:
                        header_data = headers[0][1] if isinstance(headers[0], tuple) else headers[0]
                        if isinstance(header_data, bytes):
                            sample_msg_ids.append({
                                'imap_id': imap_id.decode() if isinstance(imap_id, bytes) else str(imap_id),
                                'headers': header_data.decode('utf-8', errors='replace')
                            })
                except Exception as e:
                    sample_msg_ids.append({'error': str(e)})

        return {
            "success": True,
            "folder": folder,
            "folder_select_status": status,
            "search_criteria": search_criteria,
            "search_status": status2,
            "found_msg_ids": msg_ids[0].decode() if isinstance(msg_ids[0], bytes) else str(msg_ids[0]) if msg_ids[0] else None,
            "total_emails_in_folder": total_emails,
            "sample_recent_emails": sample_msg_ids
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/email/stats")
async def get_email_stats():
    """Get email statistics."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        stats = email_storage.get_email_stats()
        category_stats = email_storage.get_category_stats()
        return {
            "success": True,
            "stats": stats,
            "categories": category_stats
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/email/messages/{email_id}/categorize")
async def categorize_single_email(email_id: int):
    """Manually trigger AI categorization for an email."""
    if not email_storage or not email_categorizer:
        raise HTTPException(status_code=503, detail="Email module not initialized")

    try:
        email = email_storage.get_email_by_id(email_id)
        if not email:
            raise HTTPException(status_code=404, detail="Email not found")

        result = email_categorizer.categorize(
            subject=email.get('subject', ''),
            from_address=email.get('from_address', ''),
            body=email.get('body_text') or email.get('body_preview', '')
        )

        email_storage.update_email_category(
            email_id=email_id,
            category=result['category'],
            confidence=result['confidence'],
            reason=result.get('reason')
        )

        return {"success": True, "categorization": result}
    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "error": str(e)}

class SendEmailRequest(BaseModel):
    """Request model for sending email."""
    to: str = Field(..., description="Recipient email address")
    subject: str = Field(..., description="Email subject")
    body: str = Field(..., description="Email body (HTML supported)")
    attachments: Optional[List[str]] = Field(default=None, description="List of file paths to attach")
    from_email: Optional[str] = Field(default=None, description="Override from address (for external relay)")

@app.post("/api/email/send")
async def send_email(request: SendEmailRequest):
    """
    Send an email using the configured IMAP provider's SMTP settings.

    Uses the first enabled email provider's credentials for sending.
    Most email services (Gmail, Microsoft, IMAP providers) use the same
    credentials for both receiving (IMAP) and sending (SMTP).

    Args:
        to: Recipient email address
        subject: Email subject
        body: Email body (HTML supported)
        attachments: Optional list of file paths to attach

    Returns:
        Success status and message ID
    """
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders

    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        # Get first enabled email provider
        providers = email_storage.get_all_providers()
        enabled_provider = next((p for p in providers if p.get('enabled')), None)

        if not enabled_provider:
            raise HTTPException(status_code=400, detail="No enabled email provider found. Configure an email provider first.")

        provider_type = enabled_provider.get('provider_type')
        config_json = enabled_provider.get('config_json', '{}')
        provider_config = json.loads(config_json) if config_json else {}

        # Determine SMTP settings based on provider type
        if provider_type == 'gmail':
            smtp_server = 'smtp.gmail.com'
            smtp_port = 587
            username = provider_config.get('email', '')
            # For Gmail, need app password
            password = provider_config.get('app_password', '') or provider_config.get('password', '')
        elif provider_type == 'microsoft':
            smtp_server = 'smtp.office365.com'
            smtp_port = 587
            username = provider_config.get('email', '')
            password = provider_config.get('password', '')
        elif provider_type == 'imap':
            # For IMAP, derive SMTP from IMAP server
            imap_server = provider_config.get('server', '')
            # If it's an IP address, use the same for SMTP
            import re
            if re.match(r'^\d+\.\d+\.\d+\.\d+$', imap_server):
                smtp_server = imap_server
                smtp_port = 587  # Use 587 with STARTTLS for authenticated relay
            else:
                smtp_server = imap_server.replace('imap.', 'smtp.').replace('imaps.', 'smtp.')
                smtp_port = 587
            username = provider_config.get('username', '')
            password = provider_config.get('password', '')
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported provider type: {provider_type}")

        if not username or not password:
            raise HTTPException(status_code=400, detail="Email provider credentials not configured properly")

        # Determine From address (handle domain\user format)
        # Check for explicit from_email in request first (for external relay)
        if request.from_email:
            from_address = request.from_email
        else:
            # Check for explicit email in config first
            from_address = provider_config.get('email') or provider_config.get('from_email')
            if not from_address:
                if '\\' in username:
                    # Convert domain\user to user@domain.local
                    domain, user = username.split('\\', 1)
                    from_address = f"{user}@{domain}.local"
                elif '@' in username:
                    from_address = username
                else:
                    from_address = username

        # Create message
        msg = MIMEMultipart()
        msg['From'] = from_address
        msg['To'] = request.to
        msg['Subject'] = request.subject

        # Add body
        msg.attach(MIMEText(request.body, 'html'))

        # Add attachments
        if request.attachments:
            for filepath in request.attachments:
                if not os.path.exists(filepath):
                    logger.warning(f"Attachment not found: {filepath}")
                    continue

                filename = os.path.basename(filepath)
                with open(filepath, 'rb') as f:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                    msg.attach(part)

        # Send email
        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            # For port 587, use STARTTLS; for port 25 (internal), try without
            if smtp_port == 587:
                server.starttls()
                server.login(username, password)
            else:
                # Internal server - try with auth first, fallback to no auth
                try:
                    server.login(username, password)
                except smtplib.SMTPException:
                    pass  # Some internal servers don't require auth
            server.send_message(msg)

        logger.info(f"Email sent successfully to {request.to}")

        return {
            "success": True,
            "message": f"Email sent to {request.to}",
            "from": from_address,
            "to": request.to,
            "subject": request.subject
        }

    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"SMTP authentication failed: {e}")
        raise HTTPException(status_code=401, detail="Email authentication failed. Check your credentials or app password.")
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send email: {str(e)}")
    except Exception as e:
        logger.error(f"Error sending email: {e}")
        raise HTTPException(status_code=500, detail=f"Error sending email: {str(e)}")

def df_to_records(df):
    """Convert DataFrame to list of dicts."""
    if hasattr(df, 'to_dict'):
        return df.to_dict('records')
    return df

# @app.get("/api/dashboard/available-years")  # Moved to apps/dashboards/api/routes.py
async def _old_get_available_years():
    """Get years with transaction data and determine the best default year."""
    try:
        # Get years from nominal transactions - support both E/F codes and numeric 30/35 codes
        # Note: RTRIM needed as nt_type may have trailing spaces
        df = sql_connector.execute_query("""
            SELECT DISTINCT nt_year as year,
                   COUNT(*) as transaction_count,
                   SUM(CASE
                       WHEN RTRIM(nt_type) = 'E' THEN ABS(nt_value)
                       WHEN RTRIM(nt_type) = '30' THEN ABS(nt_value)
                       ELSE 0
                   END) as revenue
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year >= 2015
            GROUP BY nt_year
            ORDER BY nt_year DESC
        """)
        data = df_to_records(df)

        # Find the most recent year with meaningful data
        years_with_data = []
        latest_year_with_data = None
        for row in data:
            year = int(row['year'])
            revenue = float(row['revenue'] or 0)
            if revenue > 1000:  # Has meaningful revenue
                years_with_data.append({
                    "year": year,
                    "transaction_count": row['transaction_count'],
                    "revenue": round(revenue, 2)
                })
                if latest_year_with_data is None:
                    latest_year_with_data = year

        return {
            "success": True,
            "years": years_with_data,
            "default_year": latest_year_with_data or 2024,
            "current_company": current_company.get("name") if current_company else None
        }
    except Exception as e:
        return {"success": False, "error": str(e), "years": [], "default_year": 2024}

# @app.get("/api/dashboard/sales-categories")  # Moved to apps/dashboards/api/routes.py
async def _old_get_sales_categories():
    """Get sales categories/segments for the current company's data."""
    try:
        # Try to get categories from invoice lines with analysis codes
        df = sql_connector.execute_query("""
            SELECT
                COALESCE(NULLIF(RTRIM(it_anal), ''), 'Uncategorised') as category,
                COUNT(*) as line_count,
                SUM(it_value) / 100.0 as total_value
            FROM itran
            WHERE it_value > 0
            GROUP BY COALESCE(NULLIF(RTRIM(it_anal), ''), 'Uncategorised')
            HAVING SUM(it_value) > 0
            ORDER BY SUM(it_value) DESC
        """)
        data = df_to_records(df)

        if data:
            return {
                "success": True,
                "source": "invoice_lines",
                "categories": data
            }

        # Fallback to stock groups if no analysis codes
        df = sql_connector.execute_query("""
            SELECT
                COALESCE(sg.sg_desc, 'Other') as category,
                COUNT(*) as line_count,
                SUM(it.it_value) / 100.0 as total_value
            FROM itran it
            LEFT JOIN cname cn ON it.it_prodcode = cn.cn_prodcode
            LEFT JOIN sgroup sg ON cn.cn_catag = sg.sg_group
            WHERE it.it_value > 0
            GROUP BY COALESCE(sg.sg_desc, 'Other')
            HAVING SUM(it.it_value) > 0
            ORDER BY SUM(it.it_value) DESC
        """)
        data = df_to_records(df)

        return {
            "success": True,
            "source": "stock_groups",
            "categories": data if data else []
        }
    except Exception as e:
        return {"success": False, "error": str(e), "categories": []}

# @app.get("/api/dashboard/ceo-kpis")  # Moved to apps/dashboards/api/routes.py
async def _old_get_ceo_kpis(year: int = 2026):
    """Get CEO-level KPIs: MTD, QTD, YTD sales, growth, customer metrics."""
    try:
        from datetime import datetime as dt

        current_date = dt.now()
        current_month = current_date.month
        current_quarter = (current_month - 1) // 3 + 1

        # Get current year and previous year sales - support both E/F and 30/35 type codes
        # Note: RTRIM needed as nt_type may have trailing spaces
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'E' THEN -nt_value
                    WHEN RTRIM(nt_type) = '30' THEN -nt_value
                    ELSE 0
                END) as revenue,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'F' THEN nt_value
                    WHEN RTRIM(nt_type) = '35' THEN nt_value
                    ELSE 0
                END) as cost_of_sales
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year IN ({year}, {year - 1})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Process data
        current_year_data = {}
        prev_year_data = {}
        for row in data:
            y = int(row['nt_year'])
            p = int(row['nt_period']) if row['nt_period'] else 0
            if y == year:
                current_year_data[p] = row['revenue'] or 0
            else:
                prev_year_data[p] = row['revenue'] or 0

        # Calculate MTD, QTD, YTD
        ytd = sum(current_year_data.get(p, 0) for p in range(1, current_month + 1))
        mtd = current_year_data.get(current_month, 0)

        quarter_start = (current_quarter - 1) * 3 + 1
        qtd = sum(current_year_data.get(p, 0) for p in range(quarter_start, current_month + 1))

        # Previous year comparisons
        ytd_prev = sum(prev_year_data.get(p, 0) for p in range(1, current_month + 1))
        yoy_growth = ((ytd - ytd_prev) / ytd_prev * 100) if ytd_prev != 0 else 0

        # Rolling averages (use previous year data for full year average)
        all_months = sorted(prev_year_data.keys())
        monthly_values = [prev_year_data[m] for m in all_months if m > 0]
        avg_3m = sum(monthly_values[-3:]) / min(3, len(monthly_values)) if monthly_values else 0
        avg_6m = sum(monthly_values[-6:]) / min(6, len(monthly_values)) if monthly_values else 0
        avg_12m = sum(monthly_values[-12:]) / min(12, len(monthly_values)) if monthly_values else 0

        # Active customers (had transactions this year)
        cust_df = sql_connector.execute_query(f"""
            SELECT COUNT(DISTINCT st_account) as active_customers
            FROM stran
            WHERE st_trtype = 'I' AND YEAR(st_trdate) = {year}
        """)
        cust_data = df_to_records(cust_df)
        active_customers = cust_data[0]['active_customers'] if cust_data else 0

        # Revenue per customer
        rev_per_cust = ytd / active_customers if active_customers > 0 else 0

        return {
            "success": True,
            "kpis": {
                "mtd": round(mtd, 2),
                "qtd": round(qtd, 2),
                "ytd": round(ytd, 2),
                "yoy_growth_percent": round(yoy_growth, 1),
                "avg_monthly_3m": round(avg_3m, 2),
                "avg_monthly_6m": round(avg_6m, 2),
                "avg_monthly_12m": round(avg_12m, 2),
                "active_customers": active_customers,
                "revenue_per_customer": round(rev_per_cust, 2),
                "year": year,
                "month": current_month,
                "quarter": current_quarter
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# @app.get("/api/dashboard/revenue-over-time")  # Moved to apps/dashboards/api/routes.py
async def _old_get_revenue_over_time(year: int = 2026):
    """Get monthly revenue breakdown by category."""
    try:
        # Get monthly totals - simpler approach that works across company types
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period as month,
                SUM(-nt_value) as revenue
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', '30')
            AND nt_year IN ({year}, {year - 1})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Organize data by year and month
        data_by_year = {year: {}, year - 1: {}}

        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            rev = row['revenue'] or 0
            data_by_year[y][m] = rev

        # Build monthly series
        months = []
        for m in range(1, 13):
            month_data = {
                "month": m,
                "month_name": ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][m],
                "current_total": data_by_year[year].get(m, 0),
                "previous_total": data_by_year[year - 1].get(m, 0)
            }
            months.append(month_data)

        return {
            "success": True,
            "year": year,
            "months": months
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# @app.get("/api/dashboard/revenue-composition")  # Moved to apps/dashboards/api/routes.py
async def _old_get_revenue_composition(year: int = 2026):
    """Get revenue breakdown by category with comparison to previous year."""
    try:
        # Get revenue by nominal account description - works for all company types
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                COALESCE(NULLIF(RTRIM(na.na_subt), ''), 'Other') as category,
                SUM(-nt_value) as revenue
            FROM ntran WITH (NOLOCK) nt
            LEFT JOIN nacnt na ON RTRIM(nt.nt_acnt) = RTRIM(na.na_acnt) AND na.na_year = nt.nt_year
            WHERE RTRIM(nt.nt_type) IN ('E', '30')
            AND nt.nt_year IN ({year}, {year - 1})
            GROUP BY nt.nt_year, COALESCE(NULLIF(RTRIM(na.na_subt), ''), 'Other')
            ORDER BY SUM(-nt_value) DESC
        """)
        data = df_to_records(df)

        current_year = {}
        prev_year = {}

        for row in data:
            y = int(row['nt_year'])
            cat = row['category']
            rev = row['revenue'] or 0
            if y == year:
                current_year[cat] = rev
            else:
                prev_year[cat] = rev

        # Calculate totals and percentages
        current_total = sum(current_year.values())
        prev_total = sum(prev_year.values())

        categories = []
        all_cats = set(current_year.keys()) | set(prev_year.keys())

        for cat in sorted(all_cats, key=lambda x: current_year.get(x, 0), reverse=True):
            curr = current_year.get(cat, 0)
            prev = prev_year.get(cat, 0)
            categories.append({
                "category": cat,
                "current_year": round(curr, 2),
                "previous_year": round(prev, 2),
                "current_percent": round(curr / current_total * 100, 1) if current_total else 0,
                "previous_percent": round(prev / prev_total * 100, 1) if prev_total else 0,
                "change_percent": round((curr - prev) / prev * 100, 1) if prev else 0
            })

        return {
            "success": True,
            "year": year,
            "current_total": round(current_total, 2),
            "previous_total": round(prev_total, 2),
            "categories": categories
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# @app.get("/api/dashboard/top-customers")  # Moved to apps/dashboards/api/routes.py
async def _old_get_top_customers(year: int = 2026, limit: int = 20):
    """Get top customers by revenue with trends."""
    try:
        df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(t.st_account) as account_code,
                RTRIM(s.sn_name) as customer_name,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trvalue ELSE 0 END) as current_year,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year - 1} THEN t.st_trvalue ELSE 0 END) as previous_year,
                COUNT(DISTINCT CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trref END) as invoice_count
            FROM stran WITH (NOLOCK) t
            INNER JOIN sname WITH (NOLOCK) s ON RTRIM(t.st_account) = RTRIM(s.sn_account)
            WHERE t.st_trtype = 'I'
            AND YEAR(t.st_trdate) IN ({year}, {year - 1})
            GROUP BY t.st_account, s.sn_name
            HAVING SUM(CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trvalue ELSE 0 END) > 0
            ORDER BY SUM(CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trvalue ELSE 0 END) DESC
        """)
        data = df_to_records(df)

        total_revenue = sum(row['current_year'] or 0 for row in data)

        customers = []
        cumulative = 0
        for row in data[:limit]:
            curr = row['current_year'] or 0
            prev = row['previous_year'] or 0
            cumulative += curr

            trend = 'stable'
            if prev > 0:
                change = (curr - prev) / prev * 100
                if change > 10:
                    trend = 'up'
                elif change < -10:
                    trend = 'down'

            customers.append({
                "account_code": row['account_code'],
                "customer_name": row['customer_name'],
                "current_year": round(curr, 2),
                "previous_year": round(prev, 2),
                "percent_of_total": round(curr / total_revenue * 100, 1) if total_revenue else 0,
                "cumulative_percent": round(cumulative / total_revenue * 100, 1) if total_revenue else 0,
                "invoice_count": row['invoice_count'],
                "trend": trend,
                "change_percent": round((curr - prev) / prev * 100, 1) if prev else 0
            })

        return {
            "success": True,
            "year": year,
            "total_revenue": round(total_revenue, 2),
            "customers": customers
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# @app.get("/api/dashboard/customer-concentration")  # Moved to apps/dashboards/api/routes.py
async def _old_get_customer_concentration(year: int = 2026):
    """Get customer concentration analysis."""
    try:
        df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(st_account) as account_code,
                SUM(st_trvalue) as revenue
            FROM stran
            WHERE st_trtype = 'I' AND YEAR(st_trdate) = {year}
            GROUP BY st_account
            ORDER BY SUM(st_trvalue) DESC
        """)
        customers = df_to_records(df)
        total_revenue = sum(c['revenue'] or 0 for c in customers)
        total_customers = len(customers)

        # Calculate concentration metrics
        top_1 = customers[0]['revenue'] if customers else 0
        top_3 = sum(c['revenue'] or 0 for c in customers[:3])
        top_5 = sum(c['revenue'] or 0 for c in customers[:5])
        top_10 = sum(c['revenue'] or 0 for c in customers[:10])

        concentration = {
            "total_customers": total_customers,
            "total_revenue": round(total_revenue, 2),
            "top_1_percent": round(top_1 / total_revenue * 100, 1) if total_revenue else 0,
            "top_3_percent": round(top_3 / total_revenue * 100, 1) if total_revenue else 0,
            "top_5_percent": round(top_5 / total_revenue * 100, 1) if total_revenue else 0,
            "top_10_percent": round(top_10 / total_revenue * 100, 1) if total_revenue else 0,
            "risk_level": "low"
        }

        # Set risk flag
        if concentration["top_5_percent"] > 50:
            concentration["risk_level"] = "high"
        elif concentration["top_5_percent"] > 40:
            concentration["risk_level"] = "medium"

        return {
            "success": True,
            "year": year,
            "concentration": concentration
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# @app.get("/api/dashboard/customer-lifecycle")  # Moved to apps/dashboards/api/routes.py
async def _old_get_customer_lifecycle(year: int = 2026):
    """Get customer lifecycle analysis - new, lost, by age band."""
    try:
        # Get first transaction date per customer
        df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(st_account) as account_code,
                MIN(st_trdate) as first_transaction,
                MAX(st_trdate) as last_transaction,
                SUM(CASE WHEN YEAR(st_trdate) = {year} THEN st_trvalue ELSE 0 END) as current_revenue,
                SUM(CASE WHEN YEAR(st_trdate) = {year - 1} THEN st_trvalue ELSE 0 END) as prev_revenue
            FROM stran
            WHERE st_trtype = 'I'
            GROUP BY st_account
        """)
        data = df_to_records(df)

        new_customers = 0
        lost_customers = 0
        age_bands = {
            "less_than_1_year": {"count": 0, "revenue": 0},
            "1_to_3_years": {"count": 0, "revenue": 0},
            "3_to_5_years": {"count": 0, "revenue": 0},
            "over_5_years": {"count": 0, "revenue": 0}
        }

        for row in data:
            first = row['first_transaction']
            curr_rev = row['current_revenue'] or 0
            prev_rev = row['prev_revenue'] or 0

            if not first:
                continue

            # New customer (first transaction this year)
            if first.year == year:
                new_customers += 1

            # Lost/dormant (had revenue last year, none this year)
            if prev_rev > 0 and curr_rev == 0:
                lost_customers += 1

            # Age bands (for active customers)
            if curr_rev > 0:
                years_active = year - first.year
                if years_active < 1:
                    age_bands["less_than_1_year"]["count"] += 1
                    age_bands["less_than_1_year"]["revenue"] += curr_rev
                elif years_active < 3:
                    age_bands["1_to_3_years"]["count"] += 1
                    age_bands["1_to_3_years"]["revenue"] += curr_rev
                elif years_active < 5:
                    age_bands["3_to_5_years"]["count"] += 1
                    age_bands["3_to_5_years"]["revenue"] += curr_rev
                else:
                    age_bands["over_5_years"]["count"] += 1
                    age_bands["over_5_years"]["revenue"] += curr_rev

        # Round revenue figures
        for band in age_bands.values():
            band["revenue"] = round(band["revenue"], 2)

        return {
            "success": True,
            "year": year,
            "new_customers": new_customers,
            "lost_customers": lost_customers,
            "age_bands": age_bands
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# @app.get("/api/dashboard/margin-by-category")  # Moved to apps/dashboards/api/routes.py
async def _old_get_margin_by_category(year: int = 2026):
    """Get gross margin analysis by revenue category."""
    try:
        # Get total revenue and COS - works for all company types
        # Note: RTRIM needed as nt_type may have trailing spaces
        df = sql_connector.execute_query(f"""
            SELECT
                'Total' as category,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'E' THEN -nt_value
                    WHEN RTRIM(nt_type) = '30' THEN -nt_value
                    ELSE 0
                END) as revenue,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'F' THEN nt_value
                    WHEN RTRIM(nt_type) = '35' THEN nt_value
                    ELSE 0
                END) as cost_of_sales
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year = {year}
        """)
        data = df_to_records(df)

        # Consolidate by category (some categories appear twice due to E and F codes)
        categories = {}
        for row in data:
            cat = row['category']
            if cat not in categories:
                categories[cat] = {"revenue": 0, "cost_of_sales": 0}
            categories[cat]["revenue"] += row['revenue'] or 0
            categories[cat]["cost_of_sales"] += row['cost_of_sales'] or 0

        # Calculate margins
        margins = []
        for cat, data in categories.items():
            rev = data["revenue"]
            cos = data["cost_of_sales"]
            gp = rev - cos
            gp_pct = (gp / rev * 100) if rev > 0 else 0

            margins.append({
                "category": cat,
                "revenue": round(rev, 2),
                "cost_of_sales": round(cos, 2),
                "gross_profit": round(gp, 2),
                "gross_margin_percent": round(gp_pct, 1)
            })

        margins.sort(key=lambda x: x['revenue'], reverse=True)

        # Total
        total_rev = sum(m['revenue'] for m in margins)
        total_cos = sum(m['cost_of_sales'] for m in margins)
        total_gp = total_rev - total_cos

        return {
            "success": True,
            "year": year,
            "categories": margins,
            "totals": {
                "revenue": round(total_rev, 2),
                "cost_of_sales": round(total_cos, 2),
                "gross_profit": round(total_gp, 2),
                "gross_margin_percent": round(total_gp / total_rev * 100, 1) if total_rev else 0
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# ============================================================
# Finance Dashboard Endpoints
# ============================================================

# @app.get("/api/dashboard/finance-summary")  # Moved to apps/dashboards/api/routes.py
async def _old_get_finance_summary(year: int = 2024):
    """Get financial summary: P&L overview, Balance Sheet summary, Key ratios."""
    try:
        # Get P&L summary by type - using ntran for YTD values by year
        # Note: RTRIM needed as nt_type may have trailing spaces
        pl_df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(nt_type) as type,
                CASE RTRIM(nt_type)
                    WHEN '30' THEN 'Sales'
                    WHEN 'E' THEN 'Sales'
                    WHEN '35' THEN 'Cost of Sales'
                    WHEN 'F' THEN 'Cost of Sales'
                    WHEN '40' THEN 'Other Income'
                    WHEN 'G' THEN 'Other Income'
                    WHEN '45' THEN 'Overheads'
                    WHEN 'H' THEN 'Overheads'
                    ELSE 'Other'
                END as type_name,
                SUM(nt_value) as ytd_movement
            FROM ntran
            WHERE RTRIM(nt_type) IN ('30', '35', '40', '45', 'E', 'F', 'G', 'H')
            AND nt_year = {year}
            GROUP BY RTRIM(nt_type)
            ORDER BY RTRIM(nt_type)
        """)
        pl_data = df_to_records(pl_df)

        # Aggregate P&L - handle both letter and number codes
        sales = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('30', 'E'))
        cos = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('35', 'F'))
        other_income = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('40', 'G'))
        overheads = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('45', 'H'))

        gross_profit = -sales - cos  # Sales are negative (credits)
        operating_profit = gross_profit + (-other_income) - overheads

        # Get Balance Sheet summary from nacnt using YTD dr/cr fields
        # Opera uses letter codes: A=Fixed Assets, B=Current Assets, C=Current Liabilities, D=Capital
        bs_df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(na_type) as type,
                CASE RTRIM(na_type)
                    WHEN 'A' THEN 'Fixed Assets'
                    WHEN '05' THEN 'Fixed Assets'
                    WHEN 'B' THEN 'Current Assets'
                    WHEN '10' THEN 'Current Assets'
                    WHEN 'C' THEN 'Current Liabilities'
                    WHEN '15' THEN 'Current Liabilities'
                    WHEN 'D' THEN 'Capital & Reserves'
                    WHEN '20' THEN 'Long Term Liabilities'
                    WHEN '25' THEN 'Capital & Reserves'
                    ELSE 'Other'
                END as type_name,
                SUM(na_ytddr - na_ytdcr) as balance
            FROM nacnt
            WHERE RTRIM(na_type) IN ('A', 'B', 'C', 'D', '05', '10', '15', '20', '25')
            GROUP BY RTRIM(na_type)
            ORDER BY RTRIM(na_type)
        """)
        bs_data = df_to_records(bs_df)

        bs_summary = {}
        for row in bs_data:
            bs_summary[row['type_name']] = round(row['balance'] or 0, 2)

        # Calculate key financial figures
        fixed_assets = bs_summary.get('Fixed Assets', 0)
        current_assets = bs_summary.get('Current Assets', 0)
        current_liabilities = abs(bs_summary.get('Current Liabilities', 0))
        net_current_assets = current_assets - current_liabilities

        # Ratios
        current_ratio = current_assets / current_liabilities if current_liabilities > 0 else 0
        gross_margin = (gross_profit / -sales * 100) if sales != 0 else 0
        operating_margin = (operating_profit / -sales * 100) if sales != 0 else 0

        return {
            "success": True,
            "year": year,
            "profit_and_loss": {
                "sales": round(-sales, 2),
                "cost_of_sales": round(cos, 2),
                "gross_profit": round(gross_profit, 2),
                "other_income": round(-other_income, 2),
                "overheads": round(overheads, 2),
                "operating_profit": round(operating_profit, 2)
            },
            "balance_sheet": {
                "fixed_assets": fixed_assets,
                "current_assets": current_assets,
                "current_liabilities": current_liabilities,
                "net_current_assets": round(net_current_assets, 2),
                "total_assets": round(fixed_assets + current_assets, 2)
            },
            "ratios": {
                "gross_margin_percent": round(gross_margin, 1),
                "operating_margin_percent": round(operating_margin, 1),
                "current_ratio": round(current_ratio, 2)
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# @app.get("/api/dashboard/finance-monthly")  # Moved to apps/dashboards/api/routes.py
async def _old_get_finance_monthly(year: int = 2024):
    """Get monthly P&L breakdown for finance view."""
    try:
        # Support both E/F/H codes and numeric 30/35/45 codes
        # Note: RTRIM needed as nt_type may have trailing spaces
        df = sql_connector.execute_query(f"""
            SELECT
                nt_period as month,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'E' THEN -nt_value
                    WHEN RTRIM(nt_type) = '30' THEN -nt_value
                    ELSE 0
                END) as revenue,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'F' THEN nt_value
                    WHEN RTRIM(nt_type) = '35' THEN nt_value
                    ELSE 0
                END) as cost_of_sales,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'H' THEN nt_value
                    WHEN RTRIM(nt_type) = '45' THEN nt_value
                    ELSE 0
                END) as overheads
            FROM ntran
            WHERE nt_year = {year}
            AND RTRIM(nt_type) IN ('E', 'F', 'H', '30', '35', '45')
            GROUP BY nt_period
            ORDER BY nt_period
        """)
        data = df_to_records(df)

        month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        months = []
        ytd_revenue = 0
        ytd_cos = 0
        ytd_overheads = 0

        for row in data:
            m = int(row['month']) if row['month'] else 0
            if m < 1 or m > 12:
                continue

            revenue = row['revenue'] or 0
            cos = row['cost_of_sales'] or 0
            overheads = row['overheads'] or 0
            gross_profit = revenue - cos
            net_profit = gross_profit - overheads

            ytd_revenue += revenue
            ytd_cos += cos
            ytd_overheads += overheads

            months.append({
                "month": m,
                "month_name": month_names[m],
                "revenue": round(revenue, 2),
                "cost_of_sales": round(cos, 2),
                "gross_profit": round(gross_profit, 2),
                "overheads": round(overheads, 2),
                "net_profit": round(net_profit, 2),
                "gross_margin_percent": round(gross_profit / revenue * 100, 1) if revenue > 0 else 0
            })

        return {
            "success": True,
            "year": year,
            "months": months,
            "ytd": {
                "revenue": round(ytd_revenue, 2),
                "cost_of_sales": round(ytd_cos, 2),
                "gross_profit": round(ytd_revenue - ytd_cos, 2),
                "overheads": round(ytd_overheads, 2),
                "net_profit": round(ytd_revenue - ytd_cos - ytd_overheads, 2)
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# @app.get("/api/dashboard/sales-by-product")  # Moved to apps/dashboards/api/routes.py
async def _old_get_sales_by_product(year: int = 2024):
    """Get sales breakdown by product category - adapts to company data structure."""

    # Analysis code description mapping for Company Z style codes
    analysis_code_descriptions = {
        'SALE': 'Vehicle Sales',
        'ACCE': 'Accessories',
        'CONS': 'Consumables',
        'MAIN': 'Maintenance',
        'SERV': 'Services',
        'LCON': 'Lease Contracts',
        'MCON': 'Maintenance Contracts',
        'CONT': 'Contracts',
    }

    def get_category_description(code: str) -> str:
        """Get a human-readable description for an analysis code."""
        if not code:
            return 'Other'
        code = code.strip()
        # Try to match the prefix (first 4 chars)
        prefix = code[:4].upper() if len(code) >= 4 else code.upper()
        if prefix in analysis_code_descriptions:
            # Include the suffix number if present
            suffix = code[4:].strip() if len(code) > 4 else ''
            base_desc = analysis_code_descriptions[prefix]
            if suffix:
                return f"{base_desc} ({suffix})"
            return base_desc
        return code  # Return the code itself if no mapping found

    def get_nominal_descriptions() -> dict:
        """Get nominal account descriptions from database."""
        try:
            desc_df = sql_connector.execute_query("""
                SELECT RTRIM(na_acnt) as acnt, RTRIM(na_desc) as descr
                FROM nacnt
                WHERE RTRIM(na_type) = 'E' OR na_acnt LIKE 'E%'
                GROUP BY na_acnt, na_desc
            """)
            desc_data = df_to_records(desc_df)
            return {row['acnt']: row['descr'] for row in desc_data if row.get('acnt')}
        except Exception:
            return {}

    try:
        # Get nominal account descriptions (for E codes like E1010, E1020)
        nominal_descriptions = get_nominal_descriptions()

        # Try using itran with it_doc and it_lineval (common to both schemas)
        try:
            df = sql_connector.execute_query(f"""
                SELECT
                    COALESCE(NULLIF(RTRIM(it.it_anal), ''), 'Other') as category_code,
                    COUNT(DISTINCT it.it_doc) as invoice_count,
                    COUNT(*) as line_count,
                    SUM(it.it_lineval) / 100.0 as total_value
                FROM itran it
                WHERE YEAR(it.it_date) = {year}
                AND it.it_lineval > 0
                GROUP BY COALESCE(NULLIF(RTRIM(it.it_anal), ''), 'Other')
                HAVING SUM(it.it_lineval) > 0
                ORDER BY SUM(it.it_lineval) DESC
            """)
            data = df_to_records(df)
            # Add descriptions - check both Company Z mapping and nominal ledger
            for row in data:
                code = row.get('category_code', '')
                # First check if it's a Company Z style code (SALE01, ACCE02, etc.)
                if code and len(code) >= 4 and code[:4].upper() in analysis_code_descriptions:
                    row['category'] = get_category_description(code)
                # Otherwise check nominal ledger descriptions (E codes like E1010)
                elif code in nominal_descriptions:
                    row['category'] = nominal_descriptions[code]
                else:
                    row['category'] = code
        except Exception:
            data = None

        if not data:
            # Fallback: Try using it_value instead of it_lineval
            try:
                df = sql_connector.execute_query(f"""
                    SELECT
                        COALESCE(NULLIF(RTRIM(it.it_anal), ''), 'Other') as category_code,
                        COUNT(DISTINCT ih.ih_invno) as invoice_count,
                        COUNT(*) as line_count,
                        SUM(it.it_value) / 100.0 as total_value
                    FROM itran it
                    JOIN ihead ih ON it.it_invno = ih.ih_invno
                    WHERE YEAR(ih.ih_invdat) = {year}
                    AND it.it_value > 0
                    GROUP BY COALESCE(NULLIF(RTRIM(it.it_anal), ''), 'Other')
                    HAVING SUM(it.it_value) > 0
                    ORDER BY SUM(it.it_value) DESC
                """)
                data = df_to_records(df)
                # Add descriptions from nominal ledger
                for row in data:
                    code = row.get('category_code', '')
                    row['category'] = nominal_descriptions.get(code, code)
            except Exception:
                data = None

        if not data:
            # Fallback: try nominal ledger categories
            df = sql_connector.execute_query(f"""
                SELECT
                    CASE
                        WHEN nt_acnt LIKE 'E1%' THEN 'Primary Sales'
                        WHEN nt_acnt LIKE 'E2%' THEN 'Secondary Sales'
                        WHEN nt_acnt LIKE 'E3%' THEN 'Services'
                        WHEN nt_acnt LIKE 'E4%' THEN 'Other Revenue'
                        ELSE 'Miscellaneous'
                    END as category,
                    COUNT(*) as line_count,
                    SUM(-nt_value) as total_value
                FROM ntran
                WHERE RTRIM(nt_type) = 'E'
                AND nt_year = {year}
                GROUP BY CASE
                    WHEN nt_acnt LIKE 'E1%' THEN 'Primary Sales'
                    WHEN nt_acnt LIKE 'E2%' THEN 'Secondary Sales'
                    WHEN nt_acnt LIKE 'E3%' THEN 'Services'
                    WHEN nt_acnt LIKE 'E4%' THEN 'Other Revenue'
                    ELSE 'Miscellaneous'
                END
                ORDER BY SUM(-nt_value) DESC
            """)
            data = df_to_records(df)

        total_value = sum(row.get('total_value', 0) or 0 for row in data)

        categories = []
        for row in data:
            value = row.get('total_value', 0) or 0
            categories.append({
                "category": row.get('category', 'Unknown'),
                "category_code": row.get('category_code', ''),
                "invoice_count": row.get('invoice_count', 0),
                "line_count": row.get('line_count', 0),
                "value": round(value, 2),
                "percent_of_total": round(value / total_value * 100, 1) if total_value > 0 else 0
            })

        return {
            "success": True,
            "year": year,
            "categories": categories,
            "total_value": round(total_value, 2)
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

class ReconcileEntriesRequest(BaseModel):
    """Request body for marking entries as reconciled."""
    entries: List[dict]  # Each entry: {"entry_number": "P100008036", "statement_line": 10}
    statement_number: int
    statement_date: Optional[str] = None  # YYYY-MM-DD format
    reconciliation_date: Optional[str] = None  # YYYY-MM-DD format

@app.get("/api/file/view")
async def view_file(path: str):
    """
    Serve a file for viewing (e.g., PDF preview).

    Args:
        path: Path to the file

    Returns:
        The file content with appropriate content type
    """
    from fastapi.responses import FileResponse
    import mimetypes

    try:
        file_path = Path(path)

        # If path is just a filename (no directory), search bank statement folders
        if not file_path.exists() and not file_path.is_absolute() and '/' not in path and '\\' not in path:
            try:
                settings = _load_company_settings()
                base_folder = settings.get("bank_statements_base_folder", "")
                if base_folder:
                    base = Path(base_folder)
                    # Search all bank subfolders and archive subfolders
                    for search_root in [base, base / 'archive']:
                        if search_root.exists():
                            for subfolder in search_root.iterdir():
                                if subfolder.is_dir():
                                    candidate = subfolder / path
                                    if candidate.exists():
                                        file_path = candidate
                                        break
                        if file_path.exists():
                            break
            except Exception:
                pass

        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"PDF file not found: {path}")

        if not file_path.is_file():
            raise HTTPException(status_code=400, detail="Path is not a file")

        # Get content type
        content_type, _ = mimetypes.guess_type(str(file_path))
        if content_type is None:
            content_type = "application/octet-stream"

        from fastapi.responses import Response
        content_bytes = file_path.read_bytes()
        return Response(
            content=content_bytes,
            media_type=content_type,
            headers={
                "Content-Disposition": f"inline",
                "X-Frame-Options": "SAMEORIGIN",
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to serve file {path}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/statement-files")
async def list_statement_files(bank_folder: Optional[str] = None):
    """
    List PDF statement files available for processing.

    Args:
        bank_folder: Optional bank folder name (barclays, hsbc, lloyds, natwest).
                    If not provided, lists files from all bank folders.

    Returns:
        List of PDF files with path, filename, size, modified date, and status
        Status includes: is_imported, is_reconciled (separate states)
    """
    import os
    from pathlib import Path
    from datetime import datetime

    base_path = Path("/Users/maccb/Downloads/bank-statements")
    bank_folders = ["barclays", "hsbc", "lloyds", "natwest"]

    if bank_folder:
        if bank_folder.lower() not in bank_folders:
            return {"success": False, "error": f"Unknown bank folder: {bank_folder}"}
        folders_to_scan = [base_path / bank_folder.lower()]
    else:
        folders_to_scan = [base_path / f for f in bank_folders]

    files = []
    imported_count = 0
    reconciled_count = 0

    for folder in folders_to_scan:
        if not folder.exists():
            continue

        for file_path in folder.iterdir():
            if file_path.is_file() and file_path.suffix.lower() == '.pdf':
                stat = file_path.stat()
                filename = file_path.name

                # Get detailed status for this file
                status = email_storage.get_statement_status(filename)

                if status['is_imported']:
                    imported_count += 1
                if status['is_reconciled']:
                    reconciled_count += 1

                files.append({
                    "path": str(file_path),
                    "filename": filename,
                    "folder": folder.name,
                    "size": stat.st_size,
                    "size_formatted": f"{stat.st_size / 1024:.1f} KB",
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "modified_formatted": datetime.fromtimestamp(stat.st_mtime).strftime("%d %b %Y %H:%M"),
                    # Import status
                    "is_imported": status['is_imported'],
                    "import_date": status['import_date'],
                    "import_bank": status['bank_code'],
                    "transactions_imported": status['transactions_imported'],
                    # Reconciliation status
                    "is_reconciled": status['is_reconciled'],
                    "reconciled_date": status['reconciled_date'],
                    "reconciled_count": status['reconciled_count']
                })

    # Sort by modified date descending (newest first)
    files.sort(key=lambda x: x["modified"], reverse=True)

    return {
        "success": True,
        "files": files,
        "count": len(files),
        "imported_count": imported_count,
        "reconciled_count": reconciled_count
    }

async def _old_get_executive_summary(year: int = 2026):
    """
    Executive Summary KPIs - what a Sales Director should see first.
    Provides at-a-glance performance metrics with like-for-like comparisons.
    """
    from datetime import datetime as dt

    try:
        current_date = dt.now()
        current_month = current_date.month
        current_quarter = (current_month - 1) // 3 + 1

        # Get monthly revenue data for both years with type breakdown
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period as month,
                SUM(CASE WHEN RTRIM(nt_type) IN ('E', '30') THEN -nt_value ELSE 0 END) as revenue
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year IN ({year}, {year - 1}, {year - 2})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Organize by year and month
        revenue_by_year = {}
        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            if y not in revenue_by_year:
                revenue_by_year[y] = {}
            revenue_by_year[y][m] = float(row['revenue'] or 0)

        # Calculate KPIs
        curr_year = revenue_by_year.get(year, {})
        prev_year = revenue_by_year.get(year - 1, {})

        # Current month vs same month last year
        curr_month_rev = curr_year.get(current_month, 0)
        prev_month_rev = prev_year.get(current_month, 0)
        month_yoy_change = ((curr_month_rev - prev_month_rev) / prev_month_rev * 100) if prev_month_rev else 0

        # Current quarter vs same quarter last year
        quarter_months = list(range((current_quarter - 1) * 3 + 1, current_quarter * 3 + 1))
        curr_qtd = sum(curr_year.get(m, 0) for m in quarter_months if m <= current_month)
        prev_qtd = sum(prev_year.get(m, 0) for m in quarter_months if m <= current_month)
        quarter_yoy_change = ((curr_qtd - prev_qtd) / prev_qtd * 100) if prev_qtd else 0

        # YTD comparison (same months as current year)
        curr_ytd = sum(curr_year.get(m, 0) for m in range(1, current_month + 1))
        prev_ytd = sum(prev_year.get(m, 0) for m in range(1, current_month + 1))
        ytd_yoy_change = ((curr_ytd - prev_ytd) / prev_ytd * 100) if prev_ytd else 0

        # Rolling 12 months (last 12 complete months)
        rolling_12 = 0
        prev_rolling_12 = 0
        for i in range(12):
            m = current_month - i
            y = year if m > 0 else year - 1
            m = m if m > 0 else m + 12
            rolling_12 += revenue_by_year.get(y, {}).get(m, 0)
            # Previous period
            py = y - 1
            prev_rolling_12 += revenue_by_year.get(py, {}).get(m, 0)

        rolling_12_change = ((rolling_12 - prev_rolling_12) / prev_rolling_12 * 100) if prev_rolling_12 else 0

        # Monthly run rate (based on last 3 months)
        recent_months = []
        for i in range(3):
            m = current_month - i
            y = year if m > 0 else year - 1
            m = m if m > 0 else m + 12
            recent_months.append(revenue_by_year.get(y, {}).get(m, 0))
        monthly_run_rate = sum(recent_months) / len(recent_months) if recent_months else 0
        annual_run_rate = monthly_run_rate * 12

        # Full year projection
        months_elapsed = current_month
        projected_full_year = (curr_ytd / months_elapsed * 12) if months_elapsed > 0 else 0
        prev_full_year = sum(prev_year.get(m, 0) for m in range(1, 13))
        projection_vs_prior = ((projected_full_year - prev_full_year) / prev_full_year * 100) if prev_full_year else 0

        return {
            "success": True,
            "year": year,
            "period": {
                "current_month": current_month,
                "current_quarter": current_quarter,
                "months_elapsed": months_elapsed
            },
            "kpis": {
                # Current month metrics
                "current_month": {
                    "value": round(curr_month_rev, 2),
                    "prior_year": round(prev_month_rev, 2),
                    "yoy_change_percent": round(month_yoy_change, 1),
                    "trend": "up" if month_yoy_change > 0 else "down" if month_yoy_change < 0 else "flat"
                },
                # Quarter metrics
                "quarter_to_date": {
                    "value": round(curr_qtd, 2),
                    "prior_year": round(prev_qtd, 2),
                    "yoy_change_percent": round(quarter_yoy_change, 1),
                    "trend": "up" if quarter_yoy_change > 0 else "down" if quarter_yoy_change < 0 else "flat"
                },
                # YTD metrics
                "year_to_date": {
                    "value": round(curr_ytd, 2),
                    "prior_year": round(prev_ytd, 2),
                    "yoy_change_percent": round(ytd_yoy_change, 1),
                    "trend": "up" if ytd_yoy_change > 0 else "down" if ytd_yoy_change < 0 else "flat"
                },
                # Rolling 12 months
                "rolling_12_months": {
                    "value": round(rolling_12, 2),
                    "prior_period": round(prev_rolling_12, 2),
                    "change_percent": round(rolling_12_change, 1),
                    "trend": "up" if rolling_12_change > 0 else "down" if rolling_12_change < 0 else "flat"
                },
                # Run rate and projections
                "monthly_run_rate": round(monthly_run_rate, 2),
                "annual_run_rate": round(annual_run_rate, 2),
                "projected_full_year": round(projected_full_year, 2),
                "prior_full_year": round(prev_full_year, 2),
                "projection_vs_prior_percent": round(projection_vs_prior, 1)
            }
        }
    except Exception as e:
        logger.error(f"Executive summary error: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/dashboard/revenue-by-category-detailed")  # Moved to apps/dashboards/api/routes.py
async def _old_get_revenue_by_category_detailed(year: int = 2026):
    """
    Detailed revenue breakdown by sales category with YoY comparison.
    Categories: Recurring (Support/AMC), Consultancy, Cloud/Hosting, Software Resale.
    """
    try:
        # Get revenue by nominal account subtype - provides category breakdown
        df = sql_connector.execute_query(f"""
            SELECT
                nt.nt_year,
                nt.nt_period as month,
                COALESCE(NULLIF(RTRIM(na.na_subt), ''),
                    CASE
                        WHEN nt.nt_acnt LIKE 'E1%' OR nt.nt_acnt LIKE '30%' THEN 'Primary Revenue'
                        WHEN nt.nt_acnt LIKE 'E2%' THEN 'Secondary Revenue'
                        WHEN nt.nt_acnt LIKE 'E3%' THEN 'Service Revenue'
                        ELSE 'Other Revenue'
                    END
                ) as category,
                SUM(-nt.nt_value) as revenue
            FROM ntran WITH (NOLOCK) nt
            LEFT JOIN nacnt na ON RTRIM(nt.nt_acnt) = RTRIM(na.na_acnt) AND na.na_year = nt.nt_year
            WHERE RTRIM(nt.nt_type) IN ('E', '30')
            AND nt.nt_year IN ({year}, {year - 1})
            GROUP BY nt.nt_year, nt.nt_period,
                COALESCE(NULLIF(RTRIM(na.na_subt), ''),
                    CASE
                        WHEN nt.nt_acnt LIKE 'E1%' OR nt.nt_acnt LIKE '30%' THEN 'Primary Revenue'
                        WHEN nt.nt_acnt LIKE 'E2%' THEN 'Secondary Revenue'
                        WHEN nt.nt_acnt LIKE 'E3%' THEN 'Service Revenue'
                        ELSE 'Other Revenue'
                    END
                )
            ORDER BY nt.nt_year, nt.nt_period
        """)
        data = df_to_records(df)

        # Aggregate by category and year
        category_totals = {}
        monthly_by_category = {}

        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            cat = row['category']
            rev = float(row['revenue'] or 0)

            if cat not in category_totals:
                category_totals[cat] = {year: 0, year - 1: 0}
                monthly_by_category[cat] = {year: {}, year - 1: {}}

            category_totals[cat][y] = category_totals[cat].get(y, 0) + rev
            monthly_by_category[cat][y][m] = monthly_by_category[cat][y].get(m, 0) + rev

        # Build response
        categories = []
        total_current = 0
        total_previous = 0

        for cat, totals in sorted(category_totals.items(), key=lambda x: -x[1].get(year, 0)):
            curr = totals.get(year, 0)
            prev = totals.get(year - 1, 0)
            total_current += curr
            total_previous += prev

            change_pct = ((curr - prev) / prev * 100) if prev else 0

            # Monthly trend for this category
            monthly_trend = []
            for m in range(1, 13):
                monthly_trend.append({
                    "month": m,
                    "current": monthly_by_category[cat][year].get(m, 0),
                    "previous": monthly_by_category[cat][year - 1].get(m, 0)
                })

            categories.append({
                "category": cat,
                "current_year": round(curr, 2),
                "previous_year": round(prev, 2),
                "change_amount": round(curr - prev, 2),
                "change_percent": round(change_pct, 1),
                "percent_of_total": round(curr / total_current * 100, 1) if total_current else 0,
                "trend": "up" if change_pct > 5 else "down" if change_pct < -5 else "stable",
                "monthly_trend": monthly_trend
            })

        return {
            "success": True,
            "year": year,
            "summary": {
                "total_current": round(total_current, 2),
                "total_previous": round(total_previous, 2),
                "total_change_percent": round((total_current - total_previous) / total_previous * 100, 1) if total_previous else 0
            },
            "categories": categories
        }
    except Exception as e:
        logger.error(f"Revenue by category error: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/dashboard/new-vs-existing-revenue")  # Moved to apps/dashboards/api/routes.py
async def _old_get_new_vs_existing_revenue(year: int = 2026):
    """
    Split revenue between new business (first invoice this year or last year)
    vs existing customers (invoiced in prior years).
    Critical for understanding growth sources.
    """
    try:
        # Get customer first transaction dates and revenue by year
        df = sql_connector.execute_query(f"""
            WITH CustomerHistory AS (
                SELECT
                    RTRIM(st_account) as account,
                    MIN(st_trdate) as first_invoice_date,
                    SUM(CASE WHEN YEAR(st_trdate) = {year} THEN st_trvalue ELSE 0 END) as current_year_rev,
                    SUM(CASE WHEN YEAR(st_trdate) = {year - 1} THEN st_trvalue ELSE 0 END) as prev_year_rev,
                    SUM(CASE WHEN YEAR(st_trdate) < {year - 1} THEN st_trvalue ELSE 0 END) as older_rev
                FROM stran
                WHERE st_trtype = 'I'
                GROUP BY st_account
            )
            SELECT
                account,
                first_invoice_date,
                current_year_rev,
                prev_year_rev,
                older_rev
            FROM CustomerHistory
            WHERE current_year_rev > 0 OR prev_year_rev > 0
        """)
        data = df_to_records(df)

        # Classify customers
        new_customers_current = {"count": 0, "revenue": 0}  # First invoice this year
        new_customers_prev = {"count": 0, "revenue": 0}     # First invoice last year (still "new-ish")
        existing_customers = {"count": 0, "revenue": 0}     # Invoiced before last year

        for row in data:
            first_date = row['first_invoice_date']
            curr_rev = float(row['current_year_rev'] or 0)

            if first_date:
                first_year = first_date.year if hasattr(first_date, 'year') else int(str(first_date)[:4])

                if first_year == year and curr_rev > 0:
                    new_customers_current["count"] += 1
                    new_customers_current["revenue"] += curr_rev
                elif first_year == year - 1 and curr_rev > 0:
                    new_customers_prev["count"] += 1
                    new_customers_prev["revenue"] += curr_rev
                elif curr_rev > 0:
                    existing_customers["count"] += 1
                    existing_customers["revenue"] += curr_rev

        total_revenue = new_customers_current["revenue"] + new_customers_prev["revenue"] + existing_customers["revenue"]
        total_customers = new_customers_current["count"] + new_customers_prev["count"] + existing_customers["count"]

        return {
            "success": True,
            "year": year,
            "summary": {
                "total_revenue": round(total_revenue, 2),
                "total_customers": total_customers
            },
            "new_business": {
                "this_year": {
                    "customers": new_customers_current["count"],
                    "revenue": round(new_customers_current["revenue"], 2),
                    "percent_of_total": round(new_customers_current["revenue"] / total_revenue * 100, 1) if total_revenue else 0,
                    "avg_per_customer": round(new_customers_current["revenue"] / new_customers_current["count"], 2) if new_customers_current["count"] else 0
                },
                "last_year_acquired": {
                    "customers": new_customers_prev["count"],
                    "revenue": round(new_customers_prev["revenue"], 2),
                    "percent_of_total": round(new_customers_prev["revenue"] / total_revenue * 100, 1) if total_revenue else 0,
                    "avg_per_customer": round(new_customers_prev["revenue"] / new_customers_prev["count"], 2) if new_customers_prev["count"] else 0
                }
            },
            "existing_business": {
                "customers": existing_customers["count"],
                "revenue": round(existing_customers["revenue"], 2),
                "percent_of_total": round(existing_customers["revenue"] / total_revenue * 100, 1) if total_revenue else 0,
                "avg_per_customer": round(existing_customers["revenue"] / existing_customers["count"], 2) if existing_customers["count"] else 0
            }
        }
    except Exception as e:
        logger.error(f"New vs existing revenue error: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/dashboard/customer-churn-analysis")  # Moved to apps/dashboards/api/routes.py
async def _old_get_customer_churn_analysis(year: int = 2026):
    """
    Customer churn and retention analysis.
    Shows customers lost, at risk, and retention rate.
    """
    try:
        df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(t.st_account) as account,
                RTRIM(s.sn_name) as customer_name,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trvalue ELSE 0 END) as current_year,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year - 1} THEN t.st_trvalue ELSE 0 END) as prev_year,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year - 2} THEN t.st_trvalue ELSE 0 END) as two_years_ago,
                MAX(t.st_trdate) as last_invoice_date
            FROM stran WITH (NOLOCK) t
            INNER JOIN sname WITH (NOLOCK) s ON RTRIM(t.st_account) = RTRIM(s.sn_account)
            WHERE t.st_trtype = 'I'
            AND YEAR(t.st_trdate) >= {year - 2}
            GROUP BY t.st_account, s.sn_name
        """)
        data = df_to_records(df)

        churned = []  # Had revenue last year, none this year
        at_risk = []  # Revenue dropped >50% vs last year
        growing = []  # Revenue up vs last year
        stable = []   # Revenue within +/- 20%
        declining = []  # Revenue down 20-50%

        total_churned_revenue = 0
        total_at_risk_revenue = 0

        for row in data:
            curr = float(row['current_year'] or 0)
            prev = float(row['prev_year'] or 0)

            if prev > 0 and curr == 0:
                # Churned customer
                churned.append({
                    "account": row['account'],
                    "customer_name": row['customer_name'],
                    "last_year_revenue": round(prev, 2),
                    "last_invoice": str(row['last_invoice_date'])[:10] if row['last_invoice_date'] else None
                })
                total_churned_revenue += prev
            elif prev > 0 and curr > 0:
                change_pct = (curr - prev) / prev * 100

                if change_pct < -50:
                    at_risk.append({
                        "account": row['account'],
                        "customer_name": row['customer_name'],
                        "current_revenue": round(curr, 2),
                        "previous_revenue": round(prev, 2),
                        "change_percent": round(change_pct, 1)
                    })
                    total_at_risk_revenue += prev - curr  # Revenue at risk
                elif change_pct > 20:
                    growing.append({
                        "account": row['account'],
                        "customer_name": row['customer_name'],
                        "current_revenue": round(curr, 2),
                        "previous_revenue": round(prev, 2),
                        "change_percent": round(change_pct, 1)
                    })
                elif change_pct < -20:
                    declining.append({
                        "account": row['account'],
                        "customer_name": row['customer_name'],
                        "current_revenue": round(curr, 2),
                        "previous_revenue": round(prev, 2),
                        "change_percent": round(change_pct, 1)
                    })
                else:
                    stable.append({
                        "account": row['account'],
                        "customer_name": row['customer_name'],
                        "current_revenue": round(curr, 2)
                    })

        # Sort by revenue impact
        churned.sort(key=lambda x: -x['last_year_revenue'])
        at_risk.sort(key=lambda x: x['change_percent'])
        growing.sort(key=lambda x: -x['change_percent'])

        # Calculate retention rate
        prev_year_customers = len([d for d in data if (d['prev_year'] or 0) > 0])
        retained_customers = prev_year_customers - len(churned)
        retention_rate = (retained_customers / prev_year_customers * 100) if prev_year_customers else 0

        return {
            "success": True,
            "year": year,
            "summary": {
                "retention_rate": round(retention_rate, 1),
                "churned_count": len(churned),
                "churned_revenue": round(total_churned_revenue, 2),
                "at_risk_count": len(at_risk),
                "at_risk_revenue": round(total_at_risk_revenue, 2),
                "growing_count": len(growing),
                "stable_count": len(stable),
                "declining_count": len(declining)
            },
            "churned_customers": churned[:10],  # Top 10 by revenue
            "at_risk_customers": at_risk[:10],   # Top 10 most at risk
            "growing_customers": growing[:10]    # Top 10 growing
        }
    except Exception as e:
        logger.error(f"Churn analysis error: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/dashboard/forward-indicators")  # Moved to apps/dashboards/api/routes.py
async def _old_get_forward_indicators(year: int = 2026):
    """
    Forward-looking indicators: run rate, recurring revenue base, risk flags.
    Helps predict future performance.
    """
    from datetime import datetime as dt

    try:
        current_date = dt.now()
        current_month = current_date.month

        # Get monthly revenue for trend analysis
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period as month,
                SUM(CASE WHEN RTRIM(nt_type) IN ('E', '30') THEN -nt_value ELSE 0 END) as revenue
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', '30')
            AND nt_year IN ({year}, {year - 1})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Organize data
        revenue_by_month = {year: {}, year - 1: {}}
        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            revenue_by_month[y][m] = float(row['revenue'] or 0)

        # Calculate various run rates
        last_3_months = sum(revenue_by_month[year].get(m, 0) for m in range(max(1, current_month - 2), current_month + 1))
        avg_3_month = last_3_months / min(3, current_month)

        last_6_months = sum(revenue_by_month[year].get(m, 0) for m in range(max(1, current_month - 5), current_month + 1))
        avg_6_month = last_6_months / min(6, current_month)

        ytd = sum(revenue_by_month[year].get(m, 0) for m in range(1, current_month + 1))
        avg_ytd = ytd / current_month if current_month > 0 else 0

        # Trend direction (comparing recent 3 months vs prior 3 months)
        recent_3 = sum(revenue_by_month[year].get(m, 0) for m in range(max(1, current_month - 2), current_month + 1))
        prior_3_start = max(1, current_month - 5)
        prior_3_end = max(1, current_month - 2)
        prior_3 = sum(revenue_by_month[year].get(m, 0) for m in range(prior_3_start, prior_3_end))

        trend_direction = "accelerating" if recent_3 > prior_3 * 1.1 else "decelerating" if recent_3 < prior_3 * 0.9 else "stable"

        # Previous year same period comparison
        prev_ytd = sum(revenue_by_month[year - 1].get(m, 0) for m in range(1, current_month + 1))
        prev_full_year = sum(revenue_by_month[year - 1].get(m, 0) for m in range(1, 13))

        # Projections
        projection_conservative = avg_ytd * 12  # Based on YTD average
        projection_optimistic = avg_3_month * 12  # Based on recent trend
        projection_midpoint = (projection_conservative + projection_optimistic) / 2

        # Risk flags
        risk_flags = []

        # Check for declining trend
        if trend_direction == "decelerating":
            risk_flags.append({
                "type": "trend",
                "severity": "medium",
                "message": "Revenue trend is decelerating vs prior quarter"
            })

        # Check for underperformance vs prior year
        if ytd < prev_ytd * 0.9:
            risk_flags.append({
                "type": "yoy_performance",
                "severity": "high" if ytd < prev_ytd * 0.8 else "medium",
                "message": f"YTD revenue {round((1 - ytd/prev_ytd) * 100, 1)}% below prior year"
            })

        # Check for projection below prior year
        if projection_midpoint < prev_full_year * 0.95:
            risk_flags.append({
                "type": "projection",
                "severity": "medium",
                "message": f"Full year projection tracking below prior year"
            })

        return {
            "success": True,
            "year": year,
            "current_month": current_month,
            "run_rates": {
                "monthly_3m_avg": round(avg_3_month, 2),
                "monthly_6m_avg": round(avg_6_month, 2),
                "monthly_ytd_avg": round(avg_ytd, 2),
                "annual_3m_basis": round(avg_3_month * 12, 2),
                "annual_6m_basis": round(avg_6_month * 12, 2),
                "annual_ytd_basis": round(avg_ytd * 12, 2)
            },
            "trend": {
                "direction": trend_direction,
                "recent_3_months": round(recent_3, 2),
                "prior_3_months": round(prior_3, 2)
            },
            "projections": {
                "conservative": round(projection_conservative, 2),
                "optimistic": round(projection_optimistic, 2),
                "midpoint": round(projection_midpoint, 2),
                "prior_year_actual": round(prev_full_year, 2),
                "vs_prior_year_percent": round((projection_midpoint - prev_full_year) / prev_full_year * 100, 1) if prev_full_year else 0
            },
            "risk_flags": risk_flags,
            "risk_level": "high" if any(f['severity'] == 'high' for f in risk_flags) else "medium" if risk_flags else "low"
        }
    except Exception as e:
        logger.error(f"Forward indicators error: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/dashboard/monthly-comparison")  # Moved to apps/dashboards/api/routes.py
async def _old_get_monthly_comparison(year: int = 2026):
    """
    Detailed month-by-month comparison: current year vs same month last year.
    Shows seasonality patterns and identifies anomalies.
    """
    try:
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period as month,
                SUM(CASE WHEN RTRIM(nt_type) IN ('E', '30') THEN -nt_value ELSE 0 END) as revenue,
                SUM(CASE WHEN RTRIM(nt_type) IN ('F', '35') THEN nt_value ELSE 0 END) as cost_of_sales
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year IN ({year}, {year - 1}, {year - 2})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Organize by year
        by_year = {year: {}, year - 1: {}, year - 2: {}}
        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            by_year[y][m] = {
                "revenue": float(row['revenue'] or 0),
                "cost_of_sales": float(row['cost_of_sales'] or 0)
            }

        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        months = []
        ytd_current = 0
        ytd_previous = 0

        for m in range(1, 13):
            curr = by_year[year].get(m, {"revenue": 0, "cost_of_sales": 0})
            prev = by_year[year - 1].get(m, {"revenue": 0, "cost_of_sales": 0})
            two_yr = by_year[year - 2].get(m, {"revenue": 0, "cost_of_sales": 0})

            curr_rev = curr["revenue"]
            prev_rev = prev["revenue"]
            two_yr_rev = two_yr["revenue"]

            ytd_current += curr_rev
            ytd_previous += prev_rev

            yoy_change = ((curr_rev - prev_rev) / prev_rev * 100) if prev_rev else 0

            # Calculate gross margin
            curr_gp = curr_rev - curr["cost_of_sales"]
            curr_margin = (curr_gp / curr_rev * 100) if curr_rev else 0

            months.append({
                "month": m,
                "month_name": month_names[m - 1],
                "current_year": round(curr_rev, 2),
                "previous_year": round(prev_rev, 2),
                "two_years_ago": round(two_yr_rev, 2),
                "yoy_change_amount": round(curr_rev - prev_rev, 2),
                "yoy_change_percent": round(yoy_change, 1),
                "gross_profit": round(curr_gp, 2),
                "gross_margin_percent": round(curr_margin, 1),
                "ytd_current": round(ytd_current, 2),
                "ytd_previous": round(ytd_previous, 2),
                "ytd_variance": round(ytd_current - ytd_previous, 2)
            })

        return {
            "success": True,
            "year": year,
            "months": months,
            "totals": {
                "current_year": round(sum(m["current_year"] for m in months), 2),
                "previous_year": round(sum(m["previous_year"] for m in months), 2),
                "two_years_ago": round(sum(m["two_years_ago"] for m in months), 2)
            }
        }
    except Exception as e:
        logger.error(f"Monthly comparison error: {e}")
        return {"success": False, "error": str(e)}

# =============================================================================
# OPERA COM AUTOMATION ENDPOINTS
# =============================================================================

# Import Opera COM module (gracefully handles non-Windows environments)
try:
    from sql_rag.opera_com import (
        OperaCOM, get_opera_connection, ImportType, ImportResult, OperaCOMError
    )
    OPERA_COM_AVAILABLE = True
except ImportError:
    OPERA_COM_AVAILABLE = False
    logger.info("Opera COM module not available (requires Windows with pywin32)")

class OperaImportRequest(BaseModel):
    """Request model for Opera import operations"""
    import_type: str = Field(..., description="Type of import: sales_invoices, purchase_invoices, nominal_journals, customers, suppliers, products, sales_orders, purchase_orders")
    file_path: str = Field(..., description="Path to the import file on the server")
    company_code: str = Field(default="", description="Opera company code (uses current if blank)")
    validate_only: bool = Field(default=False, description="Only validate, don't import")
    options: Dict[str, Any] = Field(default={}, description="Additional import options")

class OperaPostRequest(BaseModel):
    """Request model for Opera posting operations"""
    post_type: str = Field(..., description="Type of posting: sales_invoices, purchase_invoices, nominal_journals")
    batch_ref: Optional[str] = Field(default=None, description="Specific batch to post (all if blank)")
    company_code: str = Field(default="", description="Opera company code (uses current if blank)")

@app.get("/api/opera/status")
async def get_opera_status():
    """
    Check if Opera COM automation is available on this server.
    Returns availability status and system information.
    """
    import platform

    return {
        "success": True,
        "opera_com_available": OPERA_COM_AVAILABLE,
        "platform": platform.system(),
        "is_windows": platform.system() == "Windows",
        "message": "Opera COM automation is available" if OPERA_COM_AVAILABLE else "Opera COM automation requires Windows with Opera 3 and pywin32 installed",
        "supported_imports": [
            "sales_invoices",
            "purchase_invoices",
            "nominal_journals",
            "customers",
            "suppliers",
            "products",
            "sales_orders",
            "purchase_orders"
        ] if OPERA_COM_AVAILABLE else []
    }

@app.post("/api/opera/import")
async def opera_import(request: OperaImportRequest):
    """
    Import data into Opera 3 using COM automation.

    This endpoint allows importing various types of data:
    - Sales/Purchase Invoices
    - Nominal Journal Entries
    - Customer/Supplier records
    - Product records
    - Sales/Purchase Orders

    The import uses Opera's native import functions which:
    - Validate data against Opera's business rules
    - Maintain audit trails
    - Update all related ledgers correctly
    """
    if not OPERA_COM_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="Opera COM automation not available. Requires Windows server with Opera 3 and pywin32 installed."
        )

    try:
        # Validate import type
        try:
            import_type = ImportType(request.import_type)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid import type: {request.import_type}. Valid types: {[t.value for t in ImportType]}"
            )

        # Get Opera connection
        opera = get_opera_connection()

        # Connect to company if specified or use current
        company = request.company_code or (current_company.get("code") if current_company else None)
        if not company:
            raise HTTPException(
                status_code=400,
                detail="No company specified and no current company selected"
            )

        if not opera.connected or opera.company != company:
            if not opera.connect(company):
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to connect to Opera company: {company}"
                )

        # Prepare options
        options = request.options.copy()
        options["validate_only"] = request.validate_only

        # Execute import
        result = opera.import_from_file(import_type, request.file_path, options)

        return {
            "success": result.success,
            "import_type": request.import_type,
            "file_path": request.file_path,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "warnings": result.warnings
        }

    except OperaCOMError as e:
        logger.error(f"Opera COM error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Opera import error: {e}")
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")

@app.post("/api/opera/validate")
async def opera_validate(request: OperaImportRequest):
    """
    Validate an import file without actually importing.
    Returns validation results including any errors or warnings.
    """
    request.validate_only = True
    return await opera_import(request)

@app.post("/api/opera/post")
async def opera_post(request: OperaPostRequest):
    """
    Post transactions to the nominal ledger via Opera COM.

    Supported posting types:
    - sales_invoices: Post sales invoices to nominal
    - purchase_invoices: Post purchase invoices to nominal
    - nominal_journals: Post nominal journal entries
    """
    if not OPERA_COM_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="Opera COM automation not available. Requires Windows server with Opera 3 and pywin32 installed."
        )

    try:
        # Get Opera connection
        opera = get_opera_connection()

        # Connect to company
        company = request.company_code or (current_company.get("code") if current_company else None)
        if not company:
            raise HTTPException(
                status_code=400,
                detail="No company specified and no current company selected"
            )

        if not opera.connected or opera.company != company:
            if not opera.connect(company):
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to connect to Opera company: {company}"
                )

        # Execute posting
        if request.post_type == "sales_invoices":
            result = opera.post_sales_invoices(request.batch_ref)
        elif request.post_type == "purchase_invoices":
            result = opera.post_purchase_invoices(request.batch_ref)
        elif request.post_type == "nominal_journals":
            result = opera.post_nominal_journals(request.batch_ref)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid post type: {request.post_type}. Valid types: sales_invoices, purchase_invoices, nominal_journals"
            )

        return {
            "success": result,
            "post_type": request.post_type,
            "batch_ref": request.batch_ref,
            "message": "Posting completed successfully" if result else "Posting failed"
        }

    except OperaCOMError as e:
        logger.error(f"Opera COM error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Opera post error: {e}")
        raise HTTPException(status_code=500, detail=f"Posting failed: {str(e)}")

@app.get("/api/opera/company-info")
async def get_opera_company_info():
    """
    Get information about the currently connected Opera company.
    """
    if not OPERA_COM_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="Opera COM automation not available"
        )

    try:
        opera = get_opera_connection()

        if not opera.connected:
            return {
                "success": False,
                "connected": False,
                "message": "Not connected to Opera"
            }

        info = opera.get_company_info()
        return {
            "success": True,
            "connected": True,
            **info
        }

    except OperaCOMError as e:
        logger.error(f"Opera COM error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# OPERA SQL SE IMPORT ENDPOINTS (Works from any platform!)
# =============================================================================

from sql_rag.opera_sql_import import OperaSQLImport, ImportType as SQLImportType, get_opera_sql_import

class OperaSQLImportRequest(BaseModel):
    """Request model for Opera SQL SE import operations"""
    import_type: str = Field(..., description="Type: customers, suppliers, nominal_journals, sales_invoices")
    data: List[Dict[str, Any]] = Field(default=[], description="Data records to import")
    file_path: Optional[str] = Field(default=None, description="Or path to CSV file")
    field_mapping: Optional[Dict[str, str]] = Field(default=None, description="CSV column to Opera field mapping")
    validate_only: bool = Field(default=False, description="Only validate, don't import")
    update_existing: bool = Field(default=False, description="Update existing records")

@app.get("/api/opera-sql/status")
async def get_opera_sql_status():
    """
    Check Opera SQL SE import capabilities.
    This works from any platform - no Windows required!
    """
    if not sql_connector:
        return {
            "success": False,
            "available": False,
            "message": "No database connection"
        }

    try:
        importer = get_opera_sql_import(sql_connector)
        capabilities = importer.discover_import_capabilities()

        return {
            "success": True,
            "available": True,
            "platform_independent": True,
            "message": "Opera SQL SE import available - works from any platform!",
            "capabilities": capabilities,
            "supported_imports": [
                "customers",
                "suppliers",
                "nominal_journals",
                "sales_invoices",
                "purchase_invoices"
            ]
        }
    except Exception as e:
        logger.error(f"Error checking Opera SQL status: {e}")
        return {
            "success": False,
            "available": False,
            "error": str(e)
        }

@app.get("/api/opera-sql/stored-procedures")
async def get_opera_stored_procedures():
    """
    List ALL stored procedures in the Opera SQL SE database.
    This helps discover if Pegasus has provided import procedures we should use.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query("""
            SELECT
                s.name as schema_name,
                p.name as procedure_name,
                p.type_desc,
                p.create_date,
                p.modify_date
            FROM sys.procedures p
            JOIN sys.schemas s ON p.schema_id = s.schema_id
            ORDER BY s.name, p.name
        """)

        procedures = df.to_dict('records') if not df.empty else []

        # Group by schema for easier reading
        by_schema = {}
        for proc in procedures:
            schema = proc['schema_name']
            if schema not in by_schema:
                by_schema[schema] = []
            by_schema[schema].append(proc['procedure_name'])

        return {
            "success": True,
            "total_count": len(procedures),
            "by_schema": by_schema,
            "all_procedures": procedures,
            "note": "Look for procedures with 'import', 'post', 'process' in their names - these may be Opera's standard routines"
        }
    except Exception as e:
        logger.error(f"Error listing procedures: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/opera-sql/procedure-definition/{procedure_name}")
async def get_procedure_definition(procedure_name: str):
    """
    Get the SQL definition of a stored procedure.
    Useful for understanding what Opera's import procedures do.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query(f"""
            SELECT
                OBJECT_NAME(object_id) as procedure_name,
                definition
            FROM sys.sql_modules
            WHERE OBJECT_NAME(object_id) = '{procedure_name}'
        """)

        if df.empty:
            raise HTTPException(status_code=404, detail=f"Procedure '{procedure_name}' not found")

        return {
            "success": True,
            "procedure_name": procedure_name,
            "definition": df.iloc[0]['definition']
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting procedure definition: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/opera-sql/table-schema/{table_name}")
async def get_opera_table_schema(table_name: str):
    """
    Get the schema for an Opera SQL SE table.
    Useful for understanding required fields before importing.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        importer = get_opera_sql_import(sql_connector)
        schema = importer.get_table_schema(table_name)

        return {
            "success": True,
            "table": table_name,
            "columns": schema
        }
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/opera-sql/import")
async def opera_sql_import(request: OperaSQLImportRequest):
    """
    Import data into Opera SQL SE.

    This endpoint works from ANY platform (Mac, Linux, Windows) because
    it uses direct SQL Server access rather than COM automation.

    Supported import types:
    - customers: Customer master records
    - suppliers: Supplier master records
    - nominal_journals: Nominal ledger journal entries
    - sales_invoices: Sales invoice headers and lines
    - purchase_invoices: Purchase invoice headers and lines

    You can either:
    1. Send data directly in the 'data' field
    2. Specify a 'file_path' to a CSV file on the server

    Example request:
    {
        "import_type": "customers",
        "data": [
            {"account": "CUST001", "name": "Test Customer", "email": "test@example.com"}
        ],
        "validate_only": true
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        # Validate import type
        try:
            import_type = SQLImportType(request.import_type)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid import type: {request.import_type}. Valid: customers, suppliers, nominal_journals, sales_invoices, purchase_invoices"
            )

        importer = get_opera_sql_import(sql_connector)

        # Import from file or data
        if request.file_path:
            result = importer.import_from_csv(
                import_type,
                request.file_path,
                field_mapping=request.field_mapping,
                validate_only=request.validate_only
            )
        elif request.data:
            # Route to appropriate method
            if import_type == SQLImportType.CUSTOMERS:
                result = importer.import_customers(
                    request.data,
                    update_existing=request.update_existing,
                    validate_only=request.validate_only
                )
            elif import_type == SQLImportType.NOMINAL_JOURNALS:
                result = importer.import_nominal_journals(
                    request.data,
                    validate_only=request.validate_only
                )
            elif import_type == SQLImportType.SALES_INVOICES:
                result = importer.import_sales_invoices(
                    request.data,
                    validate_only=request.validate_only
                )
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Import type {import_type.value} not yet implemented for direct data"
                )
        else:
            raise HTTPException(
                status_code=400,
                detail="Must provide either 'data' or 'file_path'"
            )

        return {
            "success": result.success,
            "import_type": request.import_type,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "warnings": result.warnings
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Opera SQL import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/opera-sql/validate")
async def opera_sql_validate(request: OperaSQLImportRequest):
    """
    Validate import data without actually importing.
    Same as /import with validate_only=true
    """
    request.validate_only = True
    return await opera_sql_import(request)

# =============================================================================
# SALES RECEIPT IMPORT - Replicates Opera's exact pattern
# =============================================================================

class SalesReceiptRequest(BaseModel):
    """Request model for importing a sales receipt"""
    bank_account: str  # Required - Opera bank account code
    customer_account: str  # Required - customer account code
    amount: float  # Amount in POUNDS (e.g., 100.00)
    reference: str = ""  # Your reference (e.g., invoice number)
    post_date: str  # Posting date YYYY-MM-DD
    input_by: str = "IMPORT"  # User code for audit trail
    sales_ledger_control: Optional[str] = None  # Loaded from config if not specified
    validate_only: bool = False  # If True, only validate without inserting

class SalesReceiptBatchRequest(BaseModel):
    """Request model for importing multiple sales receipts"""
    receipts: List[dict]  # List of receipt dictionaries
    validate_only: bool = False

@app.post("/api/opera-sql/sales-receipt")
async def import_sales_receipt(request: SalesReceiptRequest):
    """
    Import a single sales receipt into Opera SQL SE.

    This replicates the EXACT pattern Opera uses when a user manually
    enters a sales receipt, creating records in:
    - aentry (Cashbook Entry Header)
    - atran (Cashbook Transaction)
    - ntran (Nominal Ledger - 2 rows for double-entry bookkeeping)

    Example request:
    {
        "bank_account": "BC010",
        "customer_account": "A046",
        "amount": 100.00,
        "reference": "inv12345",
        "post_date": "2026-01-31",
        "input_by": "TEST",
        "validate_only": false
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime

        importer = get_opera_sql_import(sql_connector)

        # Parse the date
        post_date = datetime.strptime(request.post_date, '%Y-%m-%d').date()

        result = importer.import_sales_receipt(
            bank_account=request.bank_account,
            customer_account=request.customer_account,
            amount_pounds=request.amount,
            reference=request.reference,
            post_date=post_date,
            input_by=request.input_by,
            sales_ledger_control=request.sales_ledger_control,
            validate_only=request.validate_only
        )

        return {
            "success": result.success,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "details": result.warnings  # Contains entry number, journal number, amount
        }

    except Exception as e:
        logger.error(f"Sales receipt import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/opera-sql/sales-receipts-batch")
async def import_sales_receipts_batch(request: SalesReceiptBatchRequest):
    """
    Import multiple sales receipts into Opera SQL SE.

    Example request:
    {
        "receipts": [
            {
                "bank_account": "BC010",
                "customer_account": "A046",
                "amount": 100.00,
                "reference": "inv12345",
                "post_date": "2026-01-31"
            },
            {
                "bank_account": "BC010",
                "customer_account": "A047",
                "amount": 250.00,
                "reference": "inv12346",
                "post_date": "2026-01-31"
            }
        ],
        "validate_only": false
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        importer = get_opera_sql_import(sql_connector)

        result = importer.import_sales_receipts_batch(
            receipts=request.receipts,
            validate_only=request.validate_only
        )

        return {
            "success": result.success,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "details": result.warnings
        }

    except Exception as e:
        logger.error(f"Sales receipts batch import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# PURCHASE PAYMENT IMPORT
# =============================================================================

class PurchasePaymentRequest(BaseModel):
    """Request model for importing a purchase payment"""
    bank_account: str
    supplier_account: str
    amount: float
    reference: str = ""
    post_date: str
    input_by: str = "IMPORT"
    creditors_control: Optional[str] = None  # Loaded from config if not specified
    payment_type: str = "Direct Cr"  # Payment type description
    validate_only: bool = False

@app.post("/api/opera-sql/purchase-payment")
async def import_purchase_payment(request: PurchasePaymentRequest):
    """
    Import a single purchase payment into Opera SQL SE.

    This replicates the pattern Opera uses when a user manually
    enters a supplier payment, creating records in:
    - aentry (Cashbook Entry Header)
    - atran (Cashbook Transaction)
    - ntran (Nominal Ledger - 2 rows for double-entry bookkeeping)

    Example request:
    {
        "bank_account": "BC010",
        "supplier_account": "P001",
        "amount": 500.00,
        "reference": "PAY12345",
        "post_date": "2026-01-31",
        "input_by": "IMPORT",
        "validate_only": false
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime

        importer = get_opera_sql_import(sql_connector)
        post_date = datetime.strptime(request.post_date, '%Y-%m-%d').date()

        result = importer.import_purchase_payment(
            bank_account=request.bank_account,
            supplier_account=request.supplier_account,
            amount_pounds=request.amount,
            reference=request.reference,
            post_date=post_date,
            input_by=request.input_by,
            creditors_control=request.creditors_control,
            payment_type=request.payment_type,
            validate_only=request.validate_only
        )

        return {
            "success": result.success,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "details": result.warnings
        }

    except Exception as e:
        logger.error(f"Purchase payment import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# SALES INVOICE IMPORT (Replicates Opera's exact pattern)
# Creates: stran, ntran (3), nhist
# =============================================================================

class SalesInvoiceRequest(BaseModel):
    """Request model for importing a sales invoice to Opera SQL SE"""
    customer_account: str
    invoice_number: str
    net_amount: float
    vat_amount: float = 0.0
    post_date: str
    customer_ref: str = ""  # Customer's reference (PO number etc)
    sales_nominal: Optional[str] = None  # Sales P&L account (looked up from Opera if not provided)
    vat_nominal: Optional[str] = None  # VAT output account (looked up from ztax if not provided)
    debtors_control: Optional[str] = None  # Loaded from config if not specified
    department: str = "U999"  # Department code
    payment_days: int = 14  # Days until payment due
    input_by: str = "IMPORT"
    description: str = ""
    validate_only: bool = False

@app.post("/api/opera-sql/sales-invoice")
async def import_sales_invoice(request: SalesInvoiceRequest):
    """
    Import a sales invoice into Opera SQL SE.

    This replicates the EXACT pattern Opera uses when a user manually
    enters a sales invoice, creating records in:
    - stran (Sales Ledger Transaction)
    - ntran (Nominal Ledger - 3 rows for double-entry)
    - nhist (Nominal History for P&L account)

    Example request:
    {
        "customer_account": "A046",
        "invoice_number": "INV001",
        "net_amount": 100.00,
        "vat_amount": 20.00,
        "post_date": "2026-01-31",
        "customer_ref": "PO12345",
        "sales_nominal": "(looked up from Opera config if omitted)",
        "vat_nominal": "(looked up from ztax if omitted)",
        "department": "U999",
        "payment_days": 14,
        "validate_only": false
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime

        importer = get_opera_sql_import(sql_connector)
        post_date = datetime.strptime(request.post_date, '%Y-%m-%d').date()

        # Look up sales_nominal and vat_nominal from Opera config if not provided
        sales_nominal = request.sales_nominal
        vat_nominal = request.vat_nominal
        if not sales_nominal:
            try:
                sp_config = importer.get_sales_processing_config()
                sales_nominal = sp_config.get('bank_nominal', '') or ''
            except Exception:
                sales_nominal = ''
        if not vat_nominal and request.vat_amount > 0:
            try:
                vat_info = importer.get_vat_rate('1', 'S', post_date)
                vat_nominal = vat_info.get('nominal', '') or ''
            except Exception:
                vat_nominal = ''

        result = importer.import_sales_invoice(
            customer_account=request.customer_account,
            invoice_number=request.invoice_number,
            net_amount=request.net_amount,
            vat_amount=request.vat_amount,
            post_date=post_date,
            customer_ref=request.customer_ref,
            sales_nominal=sales_nominal or '',
            vat_nominal=vat_nominal or '',
            debtors_control=request.debtors_control,
            department=request.department,
            payment_days=request.payment_days,
            input_by=request.input_by,
            description=request.description,
            validate_only=request.validate_only
        )

        return {
            "success": result.success,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "details": result.warnings
        }

    except Exception as e:
        logger.error(f"Sales invoice import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# PURCHASE INVOICE POSTING IMPORT
# =============================================================================

class PurchaseInvoicePostingRequest(BaseModel):
    """Request model for importing a purchase invoice posting"""
    supplier_account: str
    invoice_number: str
    net_amount: float
    vat_amount: float = 0.0
    post_date: str
    nominal_account: Optional[str] = None  # Purchase cost account (looked up if not provided)
    vat_account: Optional[str] = None  # Looked up from ztax if not specified
    vat_code: str = "1"  # Standard VAT code
    purchase_ledger_control: Optional[str] = None  # Loaded from config if not specified
    input_by: str = "IMPORT"
    description: str = ""
    validate_only: bool = False

@app.post("/api/opera-sql/purchase-invoice")
async def import_purchase_invoice(request: PurchaseInvoicePostingRequest):
    """
    Import a purchase invoice posting into Opera SQL SE.

    Creates nominal ledger entries:
    - Credit: Purchase Ledger Control (we owe supplier)
    - Debit: Expense Account (cost incurred)
    - Debit: VAT Account (VAT reclaimable)

    Example request:
    {
        "supplier_account": "P001",
        "invoice_number": "PINV001",
        "net_amount": 500.00,
        "vat_amount": 100.00,
        "post_date": "2026-01-31",
        "nominal_account": "(required - expense account code)",
        "validate_only": false
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime

        importer = get_opera_sql_import(sql_connector)
        post_date = datetime.strptime(request.post_date, '%Y-%m-%d').date()

        # Validate nominal account is provided — it's the expense code and varies per invoice
        nominal_account = request.nominal_account or ''
        if not nominal_account.strip():
            raise HTTPException(status_code=400, detail="Expense nominal account is required for purchase invoices")

        result = importer.import_purchase_invoice_posting(
            supplier_account=request.supplier_account,
            invoice_number=request.invoice_number,
            net_amount=request.net_amount,
            vat_amount=request.vat_amount,
            post_date=post_date,
            nominal_account=nominal_account,
            vat_account=request.vat_account,
            vat_code=request.vat_code,
            purchase_ledger_control=request.purchase_ledger_control,
            input_by=request.input_by,
            description=request.description,
            validate_only=request.validate_only
        )

        return {
            "success": result.success,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "details": result.warnings
        }

    except Exception as e:
        logger.error(f"Purchase invoice import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# NOMINAL JOURNAL IMPORT
# =============================================================================

class NominalJournalLine(BaseModel):
    account: str
    amount: float
    description: str = ""

class NominalJournalRequest(BaseModel):
    """Request model for importing a nominal journal"""
    lines: List[NominalJournalLine]
    reference: str
    post_date: str
    input_by: str = "IMPORT"
    description: str = ""
    validate_only: bool = False

@app.post("/api/opera-sql/nominal-journal")
async def import_nominal_journal(request: NominalJournalRequest):
    """
    Import a nominal journal into Opera SQL SE.

    The journal MUST balance (total of all amounts = 0).
    Positive amounts = Debit, Negative amounts = Credit.

    Example request:
    {
        "lines": [
            {"account": "GA010", "amount": 100.00, "description": "Sales"},
            {"account": "BB020", "amount": -100.00, "description": "Control"}
        ],
        "reference": "JNL001",
        "post_date": "2026-01-31",
        "validate_only": false
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime

        importer = get_opera_sql_import(sql_connector)
        post_date = datetime.strptime(request.post_date, '%Y-%m-%d').date()

        # Convert Pydantic models to dicts
        lines = [{"account": l.account, "amount": l.amount, "description": l.description} for l in request.lines]

        result = importer.import_nominal_journal(
            lines=lines,
            reference=request.reference,
            post_date=post_date,
            input_by=request.input_by,
            description=request.description,
            validate_only=request.validate_only
        )

        return {
            "success": result.success,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "details": result.warnings
        }

    except Exception as e:
        logger.error(f"Nominal journal import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# BANK STATEMENT IMPORT
# =============================================================================

class BankImportPreviewResponse(BaseModel):
    """Response model for bank import preview"""
    success: bool
    filename: str
    total_transactions: int
    to_import: int
    already_posted: int
    skipped: int
    transactions: List[Dict[str, Any]]
    errors: List[str]

@app.get("/api/opera-sql/bank-accounts")
async def get_bank_accounts():
    """
    Get list of available bank accounts from Opera.

    Returns all bank accounts configured in Opera's nbank table,
    including account codes, descriptions, and current balances.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        accounts = BankStatementImport.get_available_bank_accounts()
        return {
            "success": True,
            "bank_accounts": accounts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/opera-sql/bank-accounts")
async def get_bank_accounts():
    """
    Get list of available bank accounts from Opera.

    Returns bank account codes, descriptions, sort codes, account numbers and balances.
    Use this to determine which bank_code to use for imports.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        accounts = BankStatementImport.get_available_bank_accounts()
        return {
            "success": True,
            "count": len(accounts),
            "accounts": accounts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/opera-sql/bank-import/preview")
async def preview_bank_import(
    filepath: str = Query(..., description="Path to CSV file"),
    bank_code: str = Query(..., description="Opera bank account code")
):
    """
    Preview what would be imported from a bank statement CSV.

    Returns matched transactions, already posted, and skipped items.
    Use this to review before actual import.
    """
    # Backend validation - check for empty filepath
    if not filepath or not filepath.strip():
        return {
            "success": False,
            "filename": "",
            "total_transactions": 0,
            "matched_receipts": [],
            "matched_payments": [],
            "already_posted": [],
            "skipped": [],
            "errors": ["CSV file path is required. Please enter the path to your bank statement CSV file."]
        }

    import os
    if not os.path.exists(filepath):
        return {
            "success": False,
            "filename": filepath,
            "total_transactions": 0,
            "matched_receipts": [],
            "matched_payments": [],
            "already_posted": [],
            "skipped": [],
            "errors": [f"File not found: {filepath}. Please check the file path."]
        }

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        importer = BankStatementImport(bank_code=bank_code, sql_connector=sql_connector)
        result = importer.preview_file(filepath)

        # Categorize transactions for frontend display
        matched_receipts = []
        matched_payments = []
        already_posted = []
        skipped = []

        for txn in result.transactions:
            txn_data = {
                "row": txn.row_number,
                "date": txn.date.isoformat(),
                "amount": txn.amount,
                "name": txn.name,
                "reference": txn.reference,
                "account": txn.matched_account,
                "match_score": txn.match_score if txn.match_score else 0,
                "reason": txn.skip_reason
            }

            if txn.action == 'sales_receipt':
                matched_receipts.append(txn_data)
            elif txn.action == 'purchase_payment':
                matched_payments.append(txn_data)
            elif txn.skip_reason and 'Already' in txn.skip_reason:
                already_posted.append(txn_data)
            else:
                # Skipped for other reasons (no match, ambiguous, etc.)
                skipped.append(txn_data)

        return {
            "success": True,
            "filename": result.filename,
            "total_transactions": result.total_transactions,
            "matched_receipts": matched_receipts,
            "matched_payments": matched_payments,
            "already_posted": already_posted,
            "skipped": skipped,
            "errors": result.errors
        }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {filepath}")
    except Exception as e:
        logger.error(f"Bank import preview error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/opera-sql/bank-import/import")
async def import_bank_statement(
    filepath: str = Query(..., description="Path to CSV file"),
    bank_code: str = Query(..., description="Opera bank account code")
):
    """
    Import matched transactions from a bank statement CSV.

    Only imports:
    - Sales receipts (customer payments)
    - Purchase payments (supplier payments)

    Skips:
    - Already posted transactions
    - Direct debits, standing orders, card payments
    - Transactions matching both customer and supplier (ambiguous)
    - Unmatched transactions
    """
    # Backend validation - check for empty filepath
    import os
    if not filepath or not filepath.strip():
        return {
            "success": False,
            "error": "CSV file path is required. Please enter the path to your bank statement CSV file."
        }

    if not os.path.exists(filepath):
        return {
            "success": False,
            "error": f"File not found: {filepath}. Please check the file path."
        }

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        importer = BankStatementImport(bank_code=bank_code, sql_connector=sql_connector)
        result = importer.import_file(filepath)

        # Format imported transactions for response
        imported = []
        for txn in result.transactions:
            if txn.imported:
                imported.append({
                    "date": txn.date.isoformat(),
                    "amount": txn.abs_amount,
                    "type": txn.action,
                    "account": txn.matched_account,
                    "name": txn.matched_name
                })

        return {
            "success": result.imported_transactions > 0 or result.matched_transactions == 0,
            "total_transactions": result.total_transactions,
            "imported": result.imported_transactions,
            "already_posted": result.already_posted,
            "skipped": result.skipped_transactions,
            "imported_transactions": imported,
            "errors": result.errors
        }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {filepath}")
    except Exception as e:
        logger.error(f"Bank import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/opera-sql/bank-import/audit")
async def bank_import_audit_report(
    filepath: str = Query(..., description="Path to CSV file"),
    bank_code: str = Query(..., description="Opera bank account code"),
    format: str = Query("json", description="Output format: json or text")
):
    """
    Generate an audit report of bank statement import.

    Returns detailed breakdown of:
    - Imported transactions with matched accounts
    - Not imported transactions with reasons
    - Summary by skip reason category
    - Total amounts

    Use format=text for a printable report.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        importer = BankStatementImport(bank_code=bank_code, sql_connector=sql_connector)
        result = importer.preview_file(filepath)

        if format == "text":
            return {
                "success": True,
                "report": importer.get_audit_report_text(result)
            }
        else:
            return {
                "success": True,
                **importer.generate_audit_report(result)
            }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {filepath}")
    except Exception as e:
        logger.error(f"Bank import audit error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# OPERA TRANSACTION ANALYSIS - Learn from manual entries
# =============================================================================

# Store snapshots for comparison
_table_snapshots: Dict[str, Any] = {}

@app.get("/api/opera-sql/snapshot/take")
async def take_table_snapshot(
    label: str = "before",
    tables: str = "ntran,sltrn,cbtrn"
):
    """
    Take a snapshot of Opera tables BEFORE you enter a transaction manually.

    Usage:
    1. Call this endpoint with label="before"
    2. Enter your transaction in Opera
    3. Call with label="after"
    4. Call /api/opera-sql/snapshot/compare to see what changed

    Default tables monitored:
    - ntran: Nominal transactions
    - sltrn: Sales ledger transactions
    - pltrn: Purchase ledger transactions
    - audit: Audit trail (if exists)
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    # Parse comma-separated tables string into list
    table_list = [t.strip() for t in tables.split(",") if t.strip()]

    try:
        snapshot = {
            "label": label,
            "timestamp": datetime.now().isoformat(),
            "tables": {}
        }

        for table in table_list:
            try:
                # Get row count and max ID/date
                df = sql_connector.execute_query(f"""
                    SELECT
                        COUNT(*) as row_count,
                        MAX(CAST(NEWID() AS VARCHAR(36))) as snapshot_id
                    FROM {table}
                """)

                # Get recent rows (last 50)
                df_recent = sql_connector.execute_query(f"""
                    SELECT TOP 50 * FROM {table}
                    ORDER BY 1 DESC
                """)

                snapshot["tables"][table] = {
                    "row_count": int(df.iloc[0]['row_count']) if not df.empty else 0,
                    "recent_rows": df_recent.to_dict('records') if not df_recent.empty else []
                }
            except Exception as e:
                snapshot["tables"][table] = {"error": str(e)}

        _table_snapshots[label] = snapshot

        return {
            "success": True,
            "label": label,
            "timestamp": snapshot["timestamp"],
            "tables_captured": list(snapshot["tables"].keys()),
            "row_counts": {t: snapshot["tables"][t].get("row_count", "error") for t in table_list},
            "message": f"Snapshot '{label}' taken. Now enter your transaction in Opera, then take another snapshot with label='after'"
        }

    except Exception as e:
        logger.error(f"Snapshot error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/opera-sql/snapshot/compare")
async def compare_snapshots(before_label: str = "before", after_label: str = "after"):
    """
    Compare two snapshots to see what Opera changed when you entered a transaction.

    This shows you exactly what tables and fields Opera populates,
    so we can replicate it for imports.
    """
    if before_label not in _table_snapshots:
        raise HTTPException(status_code=400, detail=f"No snapshot with label '{before_label}'. Take a snapshot first.")
    if after_label not in _table_snapshots:
        raise HTTPException(status_code=400, detail=f"No snapshot with label '{after_label}'. Take a snapshot first.")

    before = _table_snapshots[before_label]
    after = _table_snapshots[after_label]

    comparison = {
        "before_timestamp": before["timestamp"],
        "after_timestamp": after["timestamp"],
        "changes": {}
    }

    for table in after["tables"]:
        if table not in before["tables"]:
            comparison["changes"][table] = {"status": "new_table"}
            continue

        before_count = before["tables"][table].get("row_count", 0)
        after_count = after["tables"][table].get("row_count", 0)

        if before_count != after_count:
            # Find new rows
            before_rows = before["tables"][table].get("recent_rows", [])
            after_rows = after["tables"][table].get("recent_rows", [])

            # Simple comparison - rows in after but not in before
            new_rows = []
            if after_count > before_count:
                new_rows = after_rows[:after_count - before_count]

            comparison["changes"][table] = {
                "rows_added": after_count - before_count,
                "before_count": before_count,
                "after_count": after_count,
                "new_rows": new_rows
            }
        else:
            comparison["changes"][table] = {"status": "no_change", "row_count": after_count}

    return {
        "success": True,
        **comparison,
        "summary": {
            "tables_changed": [t for t, c in comparison["changes"].items() if c.get("rows_added", 0) > 0],
            "total_rows_added": sum(c.get("rows_added", 0) for c in comparison["changes"].values())
        }
    }

@app.get("/api/opera-sql/snapshot/new-rows/{table}")
async def get_new_rows_detail(table: str, before_label: str = "before", after_label: str = "after"):
    """
    Get detailed view of new rows added to a specific table.
    Shows all columns and values so you can see exactly what Opera populated.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    if before_label not in _table_snapshots or after_label not in _table_snapshots:
        raise HTTPException(status_code=400, detail="Missing snapshots. Take before/after snapshots first.")

    before_count = _table_snapshots[before_label]["tables"].get(table, {}).get("row_count", 0)
    after_count = _table_snapshots[after_label]["tables"].get(table, {}).get("row_count", 0)

    rows_added = after_count - before_count

    if rows_added <= 0:
        return {
            "success": True,
            "table": table,
            "rows_added": 0,
            "message": "No new rows in this table"
        }

    try:
        # Get the newest rows
        df = sql_connector.execute_query(f"""
            SELECT TOP {rows_added} * FROM {table}
            ORDER BY 1 DESC
        """)

        new_rows = df.to_dict('records') if not df.empty else []

        # Get column info
        schema_df = sql_connector.execute_query(f"""
            SELECT
                c.name as column_name,
                t.name as data_type,
                c.is_nullable
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            WHERE c.object_id = OBJECT_ID('{table}')
            ORDER BY c.column_id
        """)

        columns = schema_df.to_dict('records') if not schema_df.empty else []

        return {
            "success": True,
            "table": table,
            "rows_added": rows_added,
            "columns": columns,
            "new_rows": new_rows,
            "note": "These are the exact rows Opera created. Use these field names and values as a template for imports."
        }

    except Exception as e:
        logger.error(f"Error getting new rows: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# BANK IMPORT ALIAS MANAGEMENT
# =============================================================================

@app.get("/api/bank-aliases")
async def get_bank_aliases(ledger_type: Optional[str] = Query(None, description="Filter by ledger type: 'S' for supplier, 'C' for customer")):
    """
    Get all bank import aliases.

    Aliases are learned matches between bank statement names and Opera accounts.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_aliases import BankAliasManager

        manager = BankAliasManager()
        aliases = manager.get_all_aliases()

        if ledger_type:
            aliases = [a for a in aliases if a.ledger_type == ledger_type.upper()]

        return {
            "success": True,
            "count": len(aliases),
            "aliases": [
                {
                    "id": a.id,
                    "bank_name": a.bank_name,
                    "ledger_type": a.ledger_type,
                    "ledger_type_name": "Supplier" if a.ledger_type == 'S' else "Customer",
                    "account_code": a.account_code,
                    "account_name": a.account_name,
                    "match_score": round(a.match_score * 100) if a.match_score else None,
                    "use_count": a.use_count,
                    "last_used": a.last_used.isoformat() if a.last_used else None,
                    "created_date": a.created_date.isoformat() if a.created_date else None,
                    "created_by": a.created_by
                }
                for a in aliases
            ]
        }

    except Exception as e:
        logger.error(f"Error getting aliases: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/bank-aliases/statistics")
async def get_bank_alias_statistics():
    """
    Get statistics about bank import aliases.

    Returns counts, usage statistics, and most frequently used aliases.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_aliases import BankAliasManager

        manager = BankAliasManager()
        stats = manager.get_statistics()

        return {
            "success": True,
            "statistics": {
                "total_aliases": stats.get('total_aliases', 0),
                "active_aliases": stats.get('active_aliases', 0),
                "supplier_aliases": stats.get('supplier_aliases', 0),
                "customer_aliases": stats.get('customer_aliases', 0),
                "total_uses": stats.get('total_uses', 0),
                "avg_match_score": round(stats.get('avg_match_score', 0) * 100) if stats.get('avg_match_score') else 0,
                "last_used": stats.get('last_used').isoformat() if stats.get('last_used') else None
            }
        }

    except Exception as e:
        logger.error(f"Error getting alias statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class CreateAliasRequest(BaseModel):
    """Request model for creating a bank alias"""
    bank_name: str
    ledger_type: str  # 'S' or 'C'
    account_code: str
    account_name: Optional[str] = None

@app.post("/api/bank-aliases")
async def create_bank_alias(request: CreateAliasRequest):
    """
    Manually create a bank import alias.

    Use this to add aliases for bank names that fuzzy matching struggles with.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    if request.ledger_type.upper() not in ('S', 'C'):
        raise HTTPException(status_code=400, detail="ledger_type must be 'S' (supplier) or 'C' (customer)")

    try:
        from sql_rag.bank_aliases import BankAliasManager

        manager = BankAliasManager()
        success = manager.save_alias(
            bank_name=request.bank_name,
            ledger_type=request.ledger_type.upper(),
            account_code=request.account_code,
            match_score=1.0,  # Manual aliases get perfect score
            account_name=request.account_name,
            created_by='MANUAL'
        )

        if success:
            return {
                "success": True,
                "message": f"Alias created: '{request.bank_name}' -> {request.account_code}"
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to create alias")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating alias: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/bank-aliases")
async def delete_bank_alias(
    bank_name: str = Query(..., description="Bank name to delete"),
    ledger_type: str = Query(..., description="'S' for supplier, 'C' for customer")
):
    """
    Delete a bank import alias.

    This performs a soft delete (deactivates the alias).
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    if ledger_type.upper() not in ('S', 'C'):
        raise HTTPException(status_code=400, detail="ledger_type must be 'S' (supplier) or 'C' (customer)")

    try:
        from sql_rag.bank_aliases import BankAliasManager

        manager = BankAliasManager()
        success = manager.delete_alias(bank_name, ledger_type.upper())

        if success:
            return {
                "success": True,
                "message": f"Alias deleted: '{bank_name}'"
            }
        else:
            return {
                "success": False,
                "message": f"Alias not found: '{bank_name}'"
            }

    except Exception as e:
        logger.error(f"Error deleting alias: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/bank-aliases/for-account/{account_code}")
async def get_aliases_for_account(account_code: str):
    """
    Get all aliases that map to a specific Opera account.

    Useful for seeing what bank names have been matched to an account.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_aliases import BankAliasManager

        manager = BankAliasManager()
        aliases = manager.get_aliases_for_account(account_code)

        return {
            "success": True,
            "account_code": account_code,
            "count": len(aliases),
            "aliases": [
                {
                    "bank_name": a.bank_name,
                    "ledger_type": a.ledger_type,
                    "use_count": a.use_count,
                    "last_used": a.last_used.isoformat() if a.last_used else None
                }
                for a in aliases
            ]
        }

    except Exception as e:
        logger.error(f"Error getting aliases for account: {e}")
        raise HTTPException(status_code=500, detail=str(e))

TYPE_DESCRIPTIONS = {
    1: "Nominal Payment",
    2: "Nominal Receipt",
    3: "Sales Refund",
    4: "Sales Receipt",
    5: "Purchase Payment",
    6: "Purchase Refund",
}

FREQ_DESCRIPTIONS = {
    "D": "Daily",
    "W": "Weekly",
    "M": "Monthly",
    "Q": "Quarterly",
    "Y": "Yearly",
}

@app.get("/api/opera3/agent/status")
async def opera3_agent_status():
    """
    Check the Opera 3 Write Agent service status.
    Returns whether the agent is available and its health info.
    The frontend uses this to show a status indicator and block writes if unavailable.
    """
    try:
        from sql_rag.opera3_agent_client import Opera3AgentClient, Opera3AgentUnavailable

        # Get agent URL from company settings or environment
        import os
        agent_url = os.environ.get("OPERA3_AGENT_URL", "")
        agent_key = os.environ.get("OPERA3_AGENT_KEY", "")

        # Also check company-level config
        if not agent_url and current_company:
            agent_url = current_company.get("opera3_agent_url", "")
            agent_key = agent_key or current_company.get("opera3_agent_key", "")

        if not agent_url:
            return {
                "available": False,
                "configured": False,
                "message": "Opera 3 Write Agent not configured. Set OPERA3_AGENT_URL or configure in company settings.",
            }

        client = Opera3AgentClient(
            base_url=agent_url,
            agent_key=agent_key,
            health_check_interval=0,  # One-shot check, no background thread
        )
        available = client.is_available()
        info = client.get_health_info()

        return {
            "available": available,
            "configured": True,
            "url": agent_url,
            "info": info.get("info", {}),
            "message": "Opera 3 Write Agent is online" if available else "Opera 3 Write Agent is not responding",
        }

    except Exception as e:
        logger.error(f"Error checking Opera 3 agent status: {e}")
        return {
            "available": False,
            "configured": False,
            "error": str(e),
        }

@app.get("/api/recurring-entries/config")
async def get_recurring_entries_config():
    """Get recurring entries processing mode setting (per-company)."""
    try:
        settings = _load_company_settings()
        mode = settings.get("recurring_entries_mode", "process")
        if mode not in ("process", "warn"):
            mode = "process"
        return {"success": True, "mode": mode}
    except Exception as e:
        logger.error(f"Error reading recurring entries config: {e}")
        return {"success": True, "mode": "process"}

@app.put("/api/recurring-entries/config")
async def update_recurring_entries_config(
    mode: str = Query(..., description="Mode: 'process' or 'warn'")
):
    """Update recurring entries processing mode setting (per-company)."""
    if mode not in ("process", "warn"):
        return {"success": False, "error": "Mode must be 'process' or 'warn'"}
    try:
        settings = _load_company_settings()
        settings["recurring_entries_mode"] = mode
        if _save_company_settings(settings):
            return {"success": True, "mode": mode}
        return {"success": False, "error": "Failed to save settings"}
    except Exception as e:
        logger.error(f"Error saving recurring entries config: {e}")
        return {"success": False, "error": str(e)}

@app.get("/api/recurring-entries/check/{bank_code}")
async def check_recurring_entries(bank_code: str):
    """
    Check for due recurring entries for a bank account.
    Returns entries that are active and have ae_nxtpost <= today.
    Each entry includes period validation (can_post flag).
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.opera_config import get_period_posting_decision
        from datetime import date as date_type

        mode = _load_company_settings().get("recurring_entries_mode", "process")

        query = f"""
            SELECT
                h.ae_entry, h.ae_type, h.ae_desc,
                h.ae_freq, h.ae_every, h.ae_nxtpost, h.ae_lstpost,
                h.ae_posted, h.ae_topost, h.ae_vatanal,
                l.at_line, RTRIM(l.at_account) as at_account, RTRIM(l.at_cbtype) as at_cbtype,
                l.at_value, RTRIM(l.at_entref) as at_entref, l.at_comment,
                RTRIM(l.at_project) as at_project, RTRIM(l.at_job) as at_job,
                l.at_vatcde, l.at_vatval,
                (SELECT COUNT(*) FROM arline l2 WITH (NOLOCK)
                 WHERE l2.at_entry = h.ae_entry AND l2.at_acnt = h.ae_acnt) as line_count
            FROM arhead h WITH (NOLOCK)
            JOIN arline l WITH (NOLOCK) ON l.at_entry = h.ae_entry AND l.at_acnt = h.ae_acnt
            WHERE RTRIM(h.ae_acnt) = '{bank_code}'
              AND (h.ae_topost = 0 OR h.ae_posted < h.ae_topost)
              AND h.ae_nxtpost <= GETDATE()
            ORDER BY h.ae_nxtpost ASC
        """
        df = sql_connector.execute_query(query)

        if df is None or len(df) == 0:
            return {
                "success": True,
                "mode": mode,
                "entries": [],
                "total_due": 0,
                "postable_count": 0,
                "blocked_count": 0
            }

        # Look up nominal account descriptions
        account_descs = {}
        try:
            accounts = set(str(r.get("at_account", "")).strip() for _, r in df.iterrows() if str(r.get("at_account", "")).strip())
            if accounts:
                acct_list = ",".join(f"'{a}'" for a in accounts)
                desc_query = f"SELECT RTRIM(na_acnt) as code, RTRIM(na_desc) as description FROM nacnt WITH (NOLOCK) WHERE RTRIM(na_acnt) IN ({acct_list})"
                desc_df = sql_connector.execute_query(desc_query)
                if desc_df is not None:
                    for _, r in desc_df.iterrows():
                        account_descs[str(r.get("code", "")).strip()] = str(r.get("description", "")).strip()
        except Exception:
            pass

        # Also look up supplier/customer names for purchase/sales types
        try:
            supplier_accounts = set()
            customer_accounts = set()
            for _, r in df.iterrows():
                ae_type = int(r.get("ae_type", 0))
                acct = str(r.get("at_account", "")).strip()
                if ae_type in (5, 6) and acct:
                    supplier_accounts.add(acct)
                elif ae_type in (3, 4) and acct:
                    customer_accounts.add(acct)
            if supplier_accounts:
                acct_list = ",".join(f"'{a}'" for a in supplier_accounts)
                sdf = sql_connector.execute_query(f"SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as description FROM pname WITH (NOLOCK) WHERE RTRIM(pn_account) IN ({acct_list})")
                if sdf is not None:
                    for _, r in sdf.iterrows():
                        account_descs[str(r.get("code", "")).strip()] = str(r.get("description", "")).strip()
            if customer_accounts:
                acct_list = ",".join(f"'{a}'" for a in customer_accounts)
                cdf = sql_connector.execute_query(f"SELECT RTRIM(sn_acnt) as code, RTRIM(sn_name) as description FROM sname WITH (NOLOCK) WHERE RTRIM(sn_acnt) IN ({acct_list})")
                if cdf is not None:
                    for _, r in cdf.iterrows():
                        account_descs[str(r.get("code", "")).strip()] = str(r.get("description", "")).strip()
        except Exception:
            pass

        # Group rows by ae_entry to handle multi-line entries (JOIN produces 1 row per arline)
        grouped = {}
        for _, row in df.iterrows():
            entry_ref = str(row.get("ae_entry", "")).strip()
            if entry_ref not in grouped:
                grouped[entry_ref] = {
                    'header': row,
                    'lines': []
                }
            grouped[entry_ref]['lines'].append(row)

        entries = []
        postable_count = 0
        blocked_count = 0
        today = date_type.today()

        # Helper: generate all outstanding posting dates from ae_nxtpost to today
        def _outstanding_dates(nxt_post_date, freq_code, every, posted, total):
            from dateutil.relativedelta import relativedelta
            dates = []
            if not nxt_post_date:
                return dates
            current = nxt_post_date
            max_remaining = (total - posted) if total > 0 else 24  # cap for safety
            while current <= today and len(dates) < max_remaining:
                dates.append(current)
                fu = freq_code.upper().strip()
                if fu == 'D':
                    current = current + timedelta(days=every)
                elif fu == 'W':
                    current = current + timedelta(weeks=every)
                elif fu == 'M':
                    current = current + relativedelta(months=every)
                elif fu == 'Q':
                    current = current + relativedelta(months=3 * every)
                elif fu == 'Y':
                    current = current + relativedelta(years=every)
                else:
                    current = current + relativedelta(months=every)
            return dates

        for entry_ref, group in grouped.items():
            row = group['header']
            lines = group['lines']
            ae_type = int(row.get("ae_type", 0))
            nxt_post = row.get("ae_nxtpost")
            nxt_post_date = None
            if nxt_post:
                if isinstance(nxt_post, str):
                    nxt_post_date = date_type.fromisoformat(str(nxt_post)[:10])
                else:
                    nxt_post_date = nxt_post.date() if hasattr(nxt_post, 'date') else nxt_post

            # Sum amount across ALL lines
            total_amount_pence = sum(abs(int(l.get("at_value", 0))) for l in lines)
            total_amount_pounds = round(total_amount_pence / 100.0, 2)
            line_count = len(lines)
            freq = str(row.get("ae_freq", "")).strip()
            ae_every = int(row.get("ae_every", 1) or 1)
            ae_posted = int(row.get("ae_posted", 0))
            ae_topost = int(row.get("ae_topost", 0))

            # Build per-line detail
            line_details = []
            for l in lines:
                l_acct = str(l.get("at_account", "")).strip()
                l_vat_code = str(l.get("at_vatcde", "")).strip()
                l_vat_val = int(l.get("at_vatval", 0))
                line_details.append({
                    "account": l_acct,
                    "account_desc": account_descs.get(l_acct, ""),
                    "amount_pence": int(l.get("at_value", 0)),
                    "amount_pounds": round(abs(int(l.get("at_value", 0))) / 100.0, 2),
                    "vat_code": l_vat_code,
                    "vat_amount_pence": l_vat_val,
                    "project": str(l.get("at_project", "")).strip(),
                    "department": str(l.get("at_job", "")).strip(),
                    "comment": str(l.get("at_comment", "")).strip(),
                })

            # Use first line for backward-compatible single-line fields
            first_line = lines[0]
            account = str(first_line.get("at_account", "")).strip()
            vat_code = str(first_line.get("at_vatcde", "")).strip()
            vat_val = int(first_line.get("at_vatval", 0))

            # Generate all outstanding posting dates
            outstanding = _outstanding_dates(nxt_post_date, freq, ae_every, ae_posted, ae_topost)
            if not outstanding:
                outstanding = [nxt_post_date] if nxt_post_date else []

            description = str(row.get("ae_desc", "")).strip() or str(first_line.get("at_entref", "")).strip()

            for post_dt in outstanding:
                # Determine if this specific date can be posted
                can_post = True
                blocked_reason = None

                # Check unsupported types
                if ae_type not in (1, 2, 3, 4, 5, 6):
                    can_post = False
                    blocked_reason = f"Type {ae_type} ({TYPE_DESCRIPTIONS.get(ae_type, 'Unknown')}) — process in Opera"

                # Check period for this specific date
                elif post_dt:
                    try:
                        ledger_type = 'SL' if ae_type in (3, 4) else ('PL' if ae_type in (5, 6) else 'NL')
                        decision = get_period_posting_decision(sql_connector, post_dt, ledger_type)
                        if not decision.can_post:
                            can_post = False
                            blocked_reason = decision.error_message or "Period is blocked"
                    except Exception as pe:
                        can_post = False
                        blocked_reason = f"Period validation error: {pe}"

                if can_post:
                    postable_count += 1
                else:
                    blocked_count += 1

                # Use composite key: entry_ref:YYYY-MM-DD for multi-date entries
                post_date_str = post_dt.strftime('%Y-%m-%d') if post_dt else None
                composite_ref = f"{entry_ref}:{post_date_str}" if len(outstanding) > 1 else entry_ref

                entries.append({
                    "entry_ref": composite_ref,
                    "base_entry_ref": entry_ref,
                    "type": ae_type,
                    "type_desc": TYPE_DESCRIPTIONS.get(ae_type, f"Type {ae_type}"),
                    "description": description,
                    "account": account,
                    "account_desc": account_descs.get(account, ""),
                    "cbtype": str(first_line.get("at_cbtype", "")).strip(),
                    "amount_pence": total_amount_pence,
                    "amount_pounds": total_amount_pounds,
                    "next_post_date": post_date_str,
                    "posted_count": ae_posted,
                    "total_posts": ae_topost,
                    "frequency": FREQ_DESCRIPTIONS.get(freq, freq),
                    "project": str(first_line.get("at_project", "")).strip(),
                    "department": str(first_line.get("at_job", "")).strip(),
                    "can_post": can_post,
                    "blocked_reason": blocked_reason,
                    "comment": str(first_line.get("at_comment", "")).strip(),
                    "vat_code": vat_code,
                    "vat_amount_pence": vat_val,
                    "line_count": line_count,
                    "lines": line_details,
                })

        return {
            "success": True,
            "mode": mode,
            "entries": entries,
            "total_due": len(entries),
            "postable_count": postable_count,
            "blocked_count": blocked_count
        }

    except Exception as e:
        logger.error(f"Error checking recurring entries: {e}")
        return {"success": False, "error": str(e)}

@app.post("/api/recurring-entries/post")
async def post_recurring_entries(request: Request):
    """
    Post selected recurring entries to Opera SQL SE.

    Request body:
    {
        "bank_code": "BC010",
        "entries": [
            {"entry_ref": "REC0000053", "override_date": "2026-02-20"},
            {"entry_ref": "REC0000053:2026-01-17", "override_date": null},
            {"entry_ref": "REC0000042", "override_date": null}
        ]
    }

    Composite entry_ref format (entry_ref:YYYY-MM-DD) is used when a recurring
    entry has multiple outstanding dates.  The date portion becomes the
    override_date for posting if no explicit override_date is provided.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        from datetime import date as date_type

        body = await request.json()
        bank_code = body.get("bank_code", "").strip()
        entries = body.get("entries", [])

        if not bank_code:
            return {"success": False, "error": "bank_code is required"}
        if not entries:
            return {"success": False, "error": "No entries to post"}

        importer = OperaSQLImport(sql_connector)
        results = []
        posted_count = 0
        failed_count = 0

        for entry in entries:
            raw_ref = entry.get("entry_ref", "").strip()
            override_str = entry.get("override_date")

            # Parse composite key (entry_ref:YYYY-MM-DD) from multi-date entries
            if ':' in raw_ref:
                entry_ref, date_part = raw_ref.rsplit(':', 1)
                # The date from composite key IS the override_date unless explicit
                override_str = override_str or date_part
            else:
                entry_ref = raw_ref

            override_date = None
            if override_str:
                try:
                    override_date = date_type.fromisoformat(override_str)
                except (ValueError, TypeError):
                    results.append({"entry_ref": raw_ref, "success": False, "error": f"Invalid date: {override_str}"})
                    failed_count += 1
                    continue

            result = importer.post_recurring_entry(
                bank_account=bank_code,
                entry_ref=entry_ref,
                override_date=override_date
            )

            if result.success:
                posted_count += 1
                results.append({
                    "entry_ref": raw_ref,
                    "success": True,
                    "message": f"Posted {result.entry_number or 'entry'}",
                    "entry_number": result.entry_number,
                    "warnings": result.warnings
                })
            else:
                failed_count += 1
                results.append({
                    "entry_ref": raw_ref,
                    "success": False,
                    "error": "; ".join(result.errors) if result.errors else "Unknown error"
                })

        return {
            "success": posted_count > 0 or failed_count == 0,
            "results": results,
            "posted_count": posted_count,
            "failed_count": failed_count
        }

    except Exception as e:
        logger.error(f"Error posting recurring entries: {e}")
        return {"success": False, "error": str(e)}

# ============================================================
# Bank Statement Email Scanning Endpoints
# ============================================================

# Bank statement detection patterns
BANK_STATEMENT_PATTERNS = {
    'barclays': {
        'sender_patterns': ['@barclays.co.uk', '@barclays.com', 'barclays'],
        'filename_patterns': ['barclays', 'bcb_statement'],
    },
    'lloyds': {
        'sender_patterns': ['@lloydsbank.co.uk', '@lloydsbank.com', 'lloyds'],
        'filename_patterns': ['lloyds', 'lbg_statement'],
    },
    'hsbc': {
        'sender_patterns': ['@hsbc.co.uk', '@hsbc.com', 'hsbc'],
        'filename_patterns': ['hsbc'],
    },
    'natwest': {
        'sender_patterns': ['@natwest.com', 'natwest'],
        'filename_patterns': ['natwest'],
    },
    'santander': {
        'sender_patterns': ['@santander.co.uk', 'santander'],
        'filename_patterns': ['santander'],
    },
    'tide': {
        'sender_patterns': ['@tide.co', '@tidebank.co.uk', 'tide'],
        'filename_patterns': ['tide'],
    },
    'monzo': {
        'sender_patterns': ['@monzo.com', 'monzo'],
        'filename_patterns': ['monzo'],
    },
    'starling': {
        'sender_patterns': ['@starlingbank.com', 'starling'],
        'filename_patterns': ['starling'],
    },
    'nationwide': {
        'sender_patterns': ['@nationwide.co.uk', 'nationwide'],
        'filename_patterns': ['nationwide'],
    },
    'rbs': {
        'sender_patterns': ['@rbs.co.uk', 'royal bank of scotland'],
        'filename_patterns': ['rbs'],
    },
    'tsb': {
        'sender_patterns': ['@tsb.co.uk', 'tsb'],
        'filename_patterns': ['tsb'],
    },
    'metro': {
        'sender_patterns': ['@metrobankonline.co.uk', 'metro bank'],
        'filename_patterns': ['metro'],
    },
    'revolut': {
        'sender_patterns': ['@revolut.com', 'revolut'],
        'filename_patterns': ['revolut'],
    },
}

# Allowed bank statement file extensions
BANK_STATEMENT_EXTENSIONS = {'.csv', '.ofx', '.qif', '.mt940', '.sta', '.pdf'}

# Allowed content types for bank statements
BANK_STATEMENT_CONTENT_TYPES = {
    'text/csv', 'application/csv', 'text/plain',
    'application/vnd.ms-excel', 'application/ofx', 'application/pdf',
    'application/x-ofx', 'application/qif'
}

def detect_bank_from_email(from_address: str, filename: str, subject: str = '') -> Optional[str]:
    """Detect which bank a statement might be from based on sender, filename, and subject."""
    from_lower = from_address.lower() if from_address else ''
    filename_lower = filename.lower() if filename else ''
    subject_lower = subject.lower() if subject else ''

    for bank_name, patterns in BANK_STATEMENT_PATTERNS.items():
        # Check sender patterns
        for pattern in patterns['sender_patterns']:
            pattern_lower = pattern.lower()
            if pattern_lower in from_lower:
                return bank_name

        # Check filename patterns
        for pattern in patterns['filename_patterns']:
            pattern_lower = pattern.lower()
            if pattern_lower in filename_lower:
                return bank_name

        # Check subject (e.g. subject="Tide" with generic attachment.pdf)
        for pattern in patterns['filename_patterns']:
            pattern_lower = pattern.lower()
            if pattern_lower in subject_lower:
                return bank_name

    return None

def extract_statement_number_from_filename(filename: str, subject: str = None) -> tuple:
    """
    Extract statement date from filename or subject for ordering.

    Returns (sort_key, display_date) where:
    - sort_key: tuple for sorting (year, month, day, sequence)
    - display_date: human-readable date identifier
    """
    import re
    from datetime import datetime

    if not filename:
        return ((9999, 99, 99, 0), None)

    # Combine filename and subject for searching
    search_text = filename.lower()
    if subject:
        search_text = f"{search_text} {subject.lower()}"

    base_name = filename.lower().rsplit('.', 1)[0]  # Remove extension

    month_names = {
        'jan': 1, 'january': 1, 'feb': 2, 'february': 2, 'mar': 3, 'march': 3,
        'apr': 4, 'april': 4, 'may': 5, 'jun': 6, 'june': 6, 'jul': 7, 'july': 7,
        'aug': 8, 'august': 8, 'sep': 9, 'september': 9, 'oct': 10, 'october': 10,
        'nov': 11, 'november': 11, 'dec': 12, 'december': 12
    }

    # Ordered month abbreviations for lookup by number
    month_abbrs = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

    # Pattern 1: DD-MMM-YY or DD/MMM/YY (e.g., "08-JAN-26", "30-jan-26")
    for month_name, month_num in month_names.items():
        pattern = rf'(\d{{1,2}})[-/\s]({month_name})[-/\s](\d{{2,4}})'
        m = re.search(pattern, search_text, re.IGNORECASE)
        if m:
            day = int(m.group(1))
            year = int(m.group(3))
            if year < 100:
                year = 2000 + year if year < 50 else 1900 + year
            return ((year, month_num, day, 0), f"{day:02d}-{month_abbrs[month_num-1]}-{year}")

    # Pattern 2: DD/MM/YYYY or DD-MM-YYYY (e.g., "02/02/2026", "30/09/2025")
    date_dmy = re.search(r'(\d{1,2})[/-](\d{1,2})[/-](20\d{2})', search_text)
    if date_dmy:
        day, month, year = int(date_dmy.group(1)), int(date_dmy.group(2)), int(date_dmy.group(3))
        if 1 <= month <= 12 and 1 <= day <= 31:
            return ((year, month, day, 0), f"{day:02d}-{month_abbrs[month-1]}-{year}")

    # Pattern 3: YYYY-MM-DD (ISO format)
    date_iso = re.search(r'(20\d{2})[-/](\d{1,2})[-/](\d{1,2})', search_text)
    if date_iso:
        year, month, day = int(date_iso.group(1)), int(date_iso.group(2)), int(date_iso.group(3))
        if 1 <= month <= 12 and 1 <= day <= 31:
            return ((year, month, day, 0), f"{day:02d}-{month_abbrs[month-1]}-{year}")

    # Pattern 4: Month name + year (e.g., "jan2026", "january 2026")
    for month_name, month_num in month_names.items():
        pattern = rf'({month_name})[-_\s]?(20\d{{2}})'
        m = re.search(pattern, search_text)
        if m:
            year = int(m.group(2))
            return ((year, month_num, 1, 0), f"01-{month_abbrs[month_num-1]}-{year}")

    # Pattern 5: YYYY-MM (e.g., "2026-01")
    date_ym = re.search(r'(20\d{2})[-_](\d{2})', search_text)
    if date_ym:
        year, month = int(date_ym.group(1)), int(date_ym.group(2))
        if 1 <= month <= 12:
            return ((year, month, 1, 0), f"01-{month_abbrs[month-1]}-{year}")

    # No date pattern found - use filename hash for consistent ordering
    return ((9999, 99, 99, hash(base_name) % 1000), None)

def is_bank_statement_attachment(filename: str, content_type: str, from_address: str = None, subject: str = None) -> bool:
    """
    Check if an attachment is likely a bank statement (not supplier statement).

    Bank statements typically have:
    - Filenames with bank account numbers (8 digits) or sort codes (XX-XX-XX)
    - Keywords like "statement" with bank-related context
    - From known bank senders
    """
    import re

    if not filename:
        return False

    filename_lower = filename.lower()
    from_lower = (from_address or '').lower()
    subject_lower = (subject or '').lower()

    # Check extension first - must be a valid statement format
    ext = '.' + filename_lower.split('.')[-1] if '.' in filename_lower else ''
    if ext not in BANK_STATEMENT_EXTENSIONS:
        # Also check content type
        if not (content_type and content_type.lower() in BANK_STATEMENT_CONTENT_TYPES):
            return False

    # Check if from a known bank
    bank_senders = ['barclays', 'lloyds', 'hsbc', 'natwest', 'santander', 'nationwide', 'rbs', 'tsb', 'metro', 'tide', 'monzo', 'starling', 'revolut']
    is_from_bank = any(bank in from_lower for bank in bank_senders)

    # Check for bank statement patterns in filename
    # Pattern 1: Contains "statement" AND has bank account number (8 digits) or sort code pattern
    has_statement_keyword = 'statement' in filename_lower
    has_account_number = bool(re.search(r'\b\d{8}\b', filename_lower))  # 8-digit account number
    has_sort_code = bool(re.search(r'\d{2}[-\s]?\d{2}[-\s]?\d{2}', filename_lower))  # Sort code pattern

    # Pattern 2: Known bank name in filename
    has_bank_in_filename = any(bank in filename_lower for bank in bank_senders)

    # Pattern 3: Bank statement keywords in subject
    bank_subject_patterns = ['bank statement', 'account statement', 'your statement']
    has_bank_subject = any(pattern in subject_lower for pattern in bank_subject_patterns)

    # Pattern 4: Known bank name in subject (e.g. subject="Tide" with attachment.pdf)
    has_bank_in_subject = any(bank in subject_lower for bank in bank_senders)

    # Accept if:
    # 1. From a known bank sender, OR
    # 2. Filename has "statement" AND (account number OR sort code), OR
    # 3. Bank name in filename, OR
    # 4. Subject indicates bank statement, OR
    # 5. Filename has "statement" and is a PDF (broad catch — bank match filters later), OR
    # 6. Bank name in subject and attachment is PDF (e.g. "Tide" subject with attachment.pdf)
    if is_from_bank:
        return True
    if has_statement_keyword and (has_account_number or has_sort_code):
        return True
    if has_bank_in_filename:
        return True
    if has_bank_subject:
        return True
    if has_statement_keyword and ext == '.pdf':
        return True
    if has_bank_in_subject and ext == '.pdf':
        return True

    return False

_COMPANY_SETTINGS_FILENAME = "company_settings.json"

def _compute_similarity_key(name: str, memo: str = "") -> str:
    """Extract a pattern key from transaction name for grouping similar unmatched items.
    Strips variable parts (reference numbers, dates) to find the common pattern."""
    import re as _re
    text = (name or "").strip()
    if not text:
        return ""
    # Normalise PPY references: "PPY041063/1571" → "PPY/1571"
    ppy_match = _re.match(r'PPY\d+(/\d+)', text)
    if ppy_match:
        return f"PPY{ppy_match.group(1)}"
    # Strip trailing 4+ digit reference numbers: "30JAN A/C 5456" → "30JAN A/C"
    text = _re.sub(r'\s+\d{4,}\s*$', '', text)
    # Strip FP date references: "FP 27/02/26 0259" → ""
    text = _re.sub(r'\bFP\s+\d{2}/\d{2}/\d{2,4}\s*\d*\b', '', text)
    # Strip long numeric/alphanumeric references (18+ chars)
    text = _re.sub(r'\b[A-Z0-9]{18,}\b', '', text)
    # Clean up whitespace
    text = _re.sub(r'\s+', ' ', text).strip()
    return text if text else (name or "").strip()

def _get_bank_subfolder_name(bank_code: str, bank_description: str = "") -> str:
    """Derive a bank subfolder name from Opera bank code and description.
    Returns e.g. 'BB010-barclays-current' from code='BB010', desc='Barclays Current'."""
    import re as _re
    code = (bank_code or "").strip()
    desc = (bank_description or "").strip()
    if desc:
        # Sanitise description for folder name: lowercase, replace non-alphanum with hyphen
        safe_desc = _re.sub(r'[^a-z0-9]+', '-', desc.lower()).strip('-')
        return f"{code}-{safe_desc}"
    return code

def _ensure_bank_statement_folders(base_folder: str, archive_folder: str, bank_code: str, bank_description: str = "") -> tuple:
    """Ensure bank-specific subfolders exist under base and archive. Returns (input_path, archive_path)."""
    subfolder = _get_bank_subfolder_name(bank_code, bank_description)
    input_path = Path(base_folder) / subfolder
    archive_path = Path(archive_folder) / subfolder
    input_path.mkdir(parents=True, exist_ok=True)
    archive_path.mkdir(parents=True, exist_ok=True)
    return input_path, archive_path

def _load_company_settings() -> dict:
    """Load per-company settings from data/{company_id}/company_settings.json."""
    defaults = {
        "bank_statements_base_folder": "",
        "bank_statements_archive_folder": "",
        "recurring_entries_mode": "process",
    }

    company_id = _get_active_company_id()
    if company_id:
        company_path = get_company_db_path(company_id, _COMPANY_SETTINGS_FILENAME)
    else:
        company_path = get_current_db_path(_COMPANY_SETTINGS_FILENAME)
    if company_path and company_path.exists():
        try:
            with open(company_path) as f:
                saved = json.load(f)
                # Merge with defaults so new keys are always present
                merged = dict(defaults)
                merged.update(saved)
                return merged
        except Exception:
            pass

    return defaults

def _save_company_settings(settings: dict) -> bool:
    """Save per-company settings."""
    company_id = _get_active_company_id()
    if company_id:
        company_path = get_company_db_path(company_id, _COMPANY_SETTINGS_FILENAME)
    else:
        company_path = get_current_db_path(_COMPANY_SETTINGS_FILENAME)
    if not company_path:
        # Fallback: save to project root (shouldn't normally happen)
        company_path = Path(__file__).parent.parent / _COMPANY_SETTINGS_FILENAME

    try:
        company_path.parent.mkdir(parents=True, exist_ok=True)
        with open(company_path, 'w') as f:
            json.dump(settings, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Failed to save company settings: {e}")
        return False

@app.get("/api/nominal/advanced-config")
async def get_advanced_nominal_config_endpoint():
    """Get company-level Advanced Nominal settings (project/department enabled)."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.opera_config import get_advanced_nominal_config
        config = get_advanced_nominal_config(sql_connector)
        return {"success": True, **config}
    except Exception as e:
        logger.error(f"Error fetching advanced nominal config: {e}")
        return {"success": True, "project_enabled": False, "department_enabled": False}

@app.get("/api/nominal/projects")
async def get_project_codes():
    """Get project codes for dropdown selection from nproj table."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query("""
            SELECT RTRIM(nr_project) as nr_project, RTRIM(ISNULL(nr_desc, '')) as nr_desc
            FROM nproj WITH (NOLOCK)
            ORDER BY nr_project
        """)

        if df is None or len(df) == 0:
            return {"success": True, "projects": []}

        projects = [
            {"code": row['nr_project'].strip(), "description": row['nr_desc'].strip() if row['nr_desc'] else ''}
            for _, row in df.iterrows()
        ]
        return {"success": True, "projects": projects}
    except Exception as e:
        logger.debug(f"Could not read nproj table (may not exist): {e}")
        return {"success": True, "projects": []}

@app.get("/api/nominal/departments")
async def get_department_codes():
    """Get department codes for dropdown selection from njob table."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query("""
            SELECT RTRIM(no_job) as no_job, RTRIM(ISNULL(no_desc, '')) as no_desc
            FROM njob WITH (NOLOCK)
            ORDER BY no_job
        """)

        if df is None or len(df) == 0:
            return {"success": True, "departments": []}

        departments = [
            {"code": row['no_job'].strip(), "description": row['no_desc'].strip() if row['no_desc'] else ''}
            for _, row in df.iterrows()
        ]
        return {"success": True, "departments": departments}
    except Exception as e:
        logger.debug(f"Could not read njob table (may not exist): {e}")
        return {"success": True, "departments": []}

@app.get("/api/import-locks")
async def get_import_locks_status():
    """Get all currently active bank-level import locks (diagnostic)."""
    from sql_rag.import_lock import get_active_locks
    return {"locks": get_active_locks()}

# ============================================================
# Opera 3 FoxPro API Endpoints
# ============================================================
# These endpoints mirror the SQL SE endpoints but read directly from
# Opera 3 FoxPro DBF files. Requires data_path parameter pointing to
# the Opera 3 company data folder.

from sql_rag.opera3_data_provider import Opera3DataProvider

def _get_opera3_provider(data_path: str) -> Opera3DataProvider:
    """Get or create Opera3DataProvider for the given data path."""
    return Opera3DataProvider(data_path)

# @app.get("/api/opera3/credit-control/dashboard")  # Moved to apps/dashboards/api/routes.py
async def _old_opera3_credit_control_dashboard(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """
    Get credit control dashboard from Opera 3 FoxPro data.
    Mirrors /api/credit-control/dashboard but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        metrics = provider.get_credit_control_metrics()
        priority_actions = provider.get_priority_customers(limit=10)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "metrics": metrics,
            "priority_actions": priority_actions
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 credit control dashboard failed: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/opera3/credit-control/debtors-report")  # Moved to apps/dashboards/api/routes.py
async def _old_opera3_debtors_report(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """
    Get aged debtors report from Opera 3 FoxPro data.
    Mirrors /api/credit-control/debtors-report but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        data = provider.get_customer_aging()

        # Calculate totals
        totals = {
            "balance": sum(r.get("balance", 0) or 0 for r in data),
            "current": sum(r.get("current", 0) or 0 for r in data),
            "month_1": sum(r.get("month1", 0) or 0 for r in data),
            "month_2": sum(r.get("month2", 0) or 0 for r in data),
            "month_3_plus": sum(r.get("month3_plus", 0) or 0 for r in data),
        }

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "data": data,
            "count": len(data),
            "totals": totals
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 debtors report failed: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/opera3/nominal/trial-balance")  # Moved to apps/dashboards/api/routes.py
async def _old_opera3_trial_balance(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    year: int = Query(2026, description="Financial year")
):
    """
    Get trial balance from Opera 3 FoxPro data.
    Mirrors /api/nominal/trial-balance but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        data = provider.get_nominal_trial_balance(year)

        # Calculate totals
        total_debit = sum(r.get("debit", 0) or 0 for r in data)
        total_credit = sum(r.get("credit", 0) or 0 for r in data)

        # Group by account type for summary
        type_names = {
            'A': 'Fixed Assets',
            'B': 'Current Assets',
            'C': 'Current Liabilities',
            'D': 'Capital & Reserves',
            'E': 'Sales',
            'F': 'Cost of Sales',
            'G': 'Overheads',
            'H': 'Other'
        }
        type_summary = {}
        for r in data:
            atype = (r.get("account_type") or "?").strip()
            if atype not in type_summary:
                type_summary[atype] = {
                    "name": type_names.get(atype, f"Type {atype}"),
                    "debit": 0,
                    "credit": 0,
                    "count": 0
                }
            type_summary[atype]["debit"] += r.get("debit", 0) or 0
            type_summary[atype]["credit"] += r.get("credit", 0) or 0
            type_summary[atype]["count"] += 1

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "year": year,
            "data": data,
            "count": len(data),
            "totals": {
                "debit": total_debit,
                "credit": total_credit,
                "difference": total_debit - total_credit
            },
            "by_type": type_summary
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 trial balance failed: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/opera3/dashboard/finance-summary")  # Moved to apps/dashboards/api/routes.py
async def _old_opera3_finance_summary(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    year: int = Query(2024, description="Financial year")
):
    """
    Get financial summary from Opera 3 FoxPro data.
    Mirrors /api/dashboard/finance-summary but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        summary = provider.get_finance_summary(year)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "year": year,
            **summary
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 finance summary failed: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/opera3/dashboard/finance-monthly")  # Moved to apps/dashboards/api/routes.py
async def _old_opera3_finance_monthly(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    year: int = Query(2024, description="Financial year")
):
    """
    Get monthly P&L breakdown from Opera 3 FoxPro data.
    Mirrors /api/dashboard/finance-monthly but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        months = provider.get_nominal_monthly(year)

        # Calculate YTD totals
        ytd_revenue = sum(m['revenue'] for m in months)
        ytd_cos = sum(m['cost_of_sales'] for m in months)
        ytd_overheads = sum(m['overheads'] for m in months)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "year": year,
            "months": months,
            "ytd": {
                "revenue": round(ytd_revenue, 2),
                "cost_of_sales": round(ytd_cos, 2),
                "gross_profit": round(ytd_revenue - ytd_cos, 2),
                "overheads": round(ytd_overheads, 2),
                "net_profit": round(ytd_revenue - ytd_cos - ytd_overheads, 2)
            }
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 finance monthly failed: {e}")
        return {"success": False, "error": str(e)}

# @app.get("/api/opera3/dashboard/executive-summary")  # Moved to apps/dashboards/api/routes.py
async def _old_opera3_executive_summary(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    year: int = Query(2026, description="Financial year")
):
    """
    Get executive KPIs from Opera 3 FoxPro data.
    Mirrors /api/dashboard/executive-summary but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        summary = provider.get_executive_summary(year)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "year": year,
            **summary
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 executive summary failed: {e}")
        return {"success": False, "error": str(e)}

@app.get("/api/opera3/creditors/dashboard")
async def opera3_creditors_dashboard(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """
    Get creditors dashboard from Opera 3 FoxPro data.
    Mirrors /api/creditors/dashboard but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        metrics = provider.get_creditors_metrics()
        top_suppliers = provider.get_top_suppliers(limit=10)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "metrics": metrics,
            "top_suppliers": top_suppliers
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 creditors dashboard failed: {e}")
        return {"success": False, "error": str(e)}

@app.get("/api/opera3/creditors/report")
async def opera3_creditors_report(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """
    Get aged creditors report from Opera 3 FoxPro data.
    Mirrors /api/creditors/report but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        data = provider.get_supplier_aging()

        # Calculate totals
        totals = {
            "balance": sum(r.get("balance", 0) or 0 for r in data),
            "current": sum(r.get("current", 0) or 0 for r in data),
            "month_1": sum(r.get("month1", 0) or 0 for r in data),
            "month_2": sum(r.get("month2", 0) or 0 for r in data),
            "month_3_plus": sum(r.get("month3_plus", 0) or 0 for r in data),
        }

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "data": data,
            "count": len(data),
            "totals": totals
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 creditors report failed: {e}")
        return {"success": False, "error": str(e)}

@app.get("/api/opera3/customers")
async def opera3_get_customers(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    active_only: bool = Query(True, description="Only return active customers")
):
    """
    Get customer master list from Opera 3 FoxPro data.
    """
    try:
        provider = _get_opera3_provider(data_path)
        customers = provider.get_customers(active_only=active_only)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "data": customers,
            "count": len(customers)
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 customers query failed: {e}")
        return {"success": False, "error": str(e)}

@app.get("/api/opera3/suppliers")
async def opera3_get_suppliers(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    active_only: bool = Query(True, description="Only return active suppliers")
):
    """
    Get supplier master list from Opera 3 FoxPro data.
    """
    try:
        provider = _get_opera3_provider(data_path)
        suppliers = provider.get_suppliers(active_only=active_only)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "data": suppliers,
            "count": len(suppliers)
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 suppliers query failed: {e}")
        return {"success": False, "error": str(e)}

@app.get("/api/opera3/nominal-accounts")
async def opera3_get_nominal_accounts(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """
    Get nominal account master list from Opera 3 FoxPro data.
    """
    try:
        provider = _get_opera3_provider(data_path)
        accounts = provider.get_nominal_accounts()

        # Enrich with project/department flags from nacnt
        try:
            from sql_rag.opera3_foxpro import Opera3Reader
            reader = Opera3Reader(data_path)
            nacnt_records = reader.read_table("nacnt")
            nacnt_map = {}
            for r in nacnt_records:
                acnt = (r.get('NA_ACNT', r.get('na_acnt', '')) or '').strip()
                if acnt:
                    nacnt_map[acnt] = r
            for acc in accounts:
                r = nacnt_map.get(acc.get('account', ''), {})
                acc['allow_project'] = int(r.get('NA_ALLWPRJ', r.get('na_allwprj', 0)) or 0)
                acc['allow_department'] = int(r.get('NA_ALLWJOB', r.get('na_allwjob', 0)) or 0)
                acc['default_project'] = (r.get('NA_PROJECT', r.get('na_project', '')) or '').strip()
                acc['default_department'] = (r.get('NA_JOB', r.get('na_job', '')) or '').strip()
        except Exception as enrich_err:
            logger.debug(f"Could not enrich Opera 3 nominal accounts with project/dept flags: {enrich_err}")

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "data": accounts,
            "count": len(accounts)
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 nominal accounts query failed: {e}")
        return {"success": False, "error": str(e)}

@app.get("/api/opera3/nominal/advanced-config")
async def opera3_get_advanced_nominal_config(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """Get company-level Advanced Nominal settings (project/department enabled) for Opera 3."""
    try:
        from sql_rag.opera3_config import Opera3Config
        config = Opera3Config(data_path)
        result = config.get_advanced_nominal_config()
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"Error fetching Opera 3 advanced nominal config: {e}")
        return {"success": True, "project_enabled": False, "department_enabled": False}

@app.get("/api/opera3/nominal/projects")
async def opera3_get_project_codes(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """Get project codes for dropdown selection from nproj table (Opera 3)."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        records = reader.read_table("nproj")

        projects = [
            {
                "code": (r.get('NR_PROJECT', r.get('nr_project', '')) or '').strip(),
                "description": (r.get('NR_DESC', r.get('nr_desc', '')) or '').strip(),
            }
            for r in records
            if (r.get('NR_PROJECT', r.get('nr_project', '')) or '').strip()
        ]
        return {"success": True, "projects": projects}
    except Exception as e:
        logger.debug(f"Could not read nproj from Opera 3 (may not exist): {e}")
        return {"success": True, "projects": []}

@app.get("/api/opera3/nominal/departments")
async def opera3_get_department_codes(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """Get department codes for dropdown selection from njob table (Opera 3)."""
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(data_path)
        records = reader.read_table("njob")

        departments = [
            {
                "code": (r.get('NO_JOB', r.get('no_job', '')) or '').strip(),
                "description": (r.get('NO_DESC', r.get('no_desc', '')) or '').strip(),
            }
            for r in records
            if (r.get('NO_JOB', r.get('no_job', '')) or '').strip()
        ]
        return {"success": True, "departments": departments}
    except Exception as e:
        logger.debug(f"Could not read njob from Opera 3 (may not exist): {e}")
        return {"success": True, "departments": []}

def _o3_get_str(record, field, default=''):
    """Get string from Opera 3 record (handles uppercase/lowercase field names)."""
    val = record.get(field.upper(), record.get(field.lower(), record.get(field, default)))
    if val is None:
        return default
    return str(val).strip()

def _o3_get_num(record, field, default=0):
    """Get number from Opera 3 record."""
    val = record.get(field.upper(), record.get(field.lower(), record.get(field, None)))
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default

def _o3_get_int(record, field, default=0):
    """Get integer from Opera 3 record."""
    return int(_o3_get_num(record, field, default))

from sql_rag.gocardless_payments import get_payments_db, GoCardlessPaymentsDB

def _complete_mandate_setup(payments_db, client, setup, mandate_id, gc_customer_id):
    """
    When a mandate setup completes (mandate becomes active):
    1. Link mandate to Opera customer in local DB
    2. Set sn_analsys='GC' on the Opera customer
    """
    opera_account = setup['opera_account']
    opera_name = setup.get('opera_name', '')
    email = setup.get('customer_email', '')

    try:
        # Get customer details from GoCardless
        gc_name = None
        if gc_customer_id:
            try:
                gc_customer = client.get_customer(gc_customer_id)
                gc_name = gc_customer.get("company_name") or gc_customer.get("given_name", "")
                if not email:
                    email = gc_customer.get("email", "")
            except Exception:
                pass

        # Get mandate details
        mandate_status = "active"
        scheme = "bacs"
        try:
            mandate_detail = client.get_mandate(mandate_id)
            mandate_status = mandate_detail.get("status", "active")
            scheme = mandate_detail.get("scheme", "bacs")
        except Exception:
            pass

        # Link mandate in local DB
        payments_db.link_mandate(
            opera_account=opera_account,
            mandate_id=mandate_id,
            opera_name=opera_name,
            gocardless_name=gc_name,
            gocardless_customer_id=gc_customer_id,
            mandate_status=mandate_status,
            scheme=scheme,
            email=email
        )
        logger.info(f"Mandate {mandate_id} linked to {opera_account} via setup flow")

        # Set sn_analsys='GC' in Opera
        if sql_connector:
            try:
                from sqlalchemy import text
                with sql_connector.engine.connect() as conn:
                    conn.execute(text("""
                        UPDATE sname WITH (ROWLOCK)
                        SET sn_analsys = 'GC'
                        WHERE LTRIM(RTRIM(sn_account)) = :account
                        AND (sn_analsys IS NULL OR LTRIM(RTRIM(sn_analsys)) = ''
                             OR LTRIM(RTRIM(UPPER(sn_analsys))) != 'GC')
                    """), {"account": opera_account.strip()})
                    conn.commit()
                    logger.info(f"Set sn_analsys='GC' for customer {opera_account}")
            except Exception as e:
                logger.warning(f"Could not update sn_analsys for {opera_account}: {e}")

    except Exception as e:
        logger.error(f"Error completing mandate setup for {opera_account}: {e}")

def _complete_mandate_setup_opera3(payments_db, client, setup, mandate_id, gc_customer_id):
    """
    When a mandate setup completes for Opera 3:
    1. Link mandate to Opera customer in local DB
    2. Set sn_analsys='GC' in FoxPro
    """
    opera_account = setup['opera_account']
    opera_name = setup.get('opera_name', '')
    email = setup.get('customer_email', '')

    try:
        gc_name = None
        if gc_customer_id:
            try:
                gc_customer = client.get_customer(gc_customer_id)
                gc_name = gc_customer.get("company_name") or gc_customer.get("given_name", "")
            except Exception:
                pass

        mandate_status = "active"
        scheme = "bacs"
        try:
            mandate_detail = client.get_mandate(mandate_id)
            mandate_status = mandate_detail.get("status", "active")
            scheme = mandate_detail.get("scheme", "bacs")
        except Exception:
            pass

        payments_db.link_mandate(
            opera_account=opera_account,
            mandate_id=mandate_id,
            opera_name=opera_name,
            gocardless_name=gc_name,
            gocardless_customer_id=gc_customer_id,
            mandate_status=mandate_status,
            scheme=scheme,
            email=email
        )
        logger.info(f"Mandate {mandate_id} linked to {opera_account} via setup flow (Opera 3)")

        # Set sn_analsys='GC' in FoxPro
        try:
            from sql_rag.opera3_foxpro import Opera3FoxPro
            foxpro = Opera3FoxPro()
            foxpro.update_field('sname', 'sn_account', opera_account.strip(), 'sn_analsys', 'GC')
            logger.info(f"Set sn_analsys='GC' in FoxPro for customer {opera_account}")
        except Exception as e:
            logger.warning(f"Could not update sn_analsys in FoxPro for {opera_account}: {e}")

    except Exception as e:
        logger.error(f"Error completing mandate setup for {opera_account} (Opera 3): {e}")

FREQUENCY_MAP = {
    'W': ('weekly', 1),
    'M': ('monthly', 1),
    'Q': ('monthly', 3),
    'A': ('yearly', 1),
}

@app.get("/api/opera3/bank-accounts")
async def opera3_get_bank_accounts(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Get list of available bank accounts from Opera 3's nbank table.
    """
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        reader = Opera3Reader(data_path)
        nbank_records = reader.read_table('nbank')

        accounts = []
        for row in nbank_records:
            is_petty = row.get('nk_petty', 0) == 1
            accounts.append({
                "code": (row.get('nk_acnt') or '').strip(),
                "description": (row.get('nk_desc') or '').strip(),
                "sort_code": (row.get('nk_sort') or '').strip(),
                "account_number": (row.get('nk_number') or '').strip(),
                "balance": (row.get('nk_curbal', 0) or 0) / 100.0,
                "type": "Petty Cash" if is_petty else "Bank Account"
            })

        # Sort by code
        accounts.sort(key=lambda x: x['code'])

        return {
            "success": True,
            "count": len(accounts),
            "accounts": accounts
        }

    except Exception as e:
        logger.error(f"Opera 3 bank accounts error: {e}")
        return {"success": False, "error": str(e)}

# =============================================================================
# USER ACTIVITY MONITORING
# =============================================================================

@app.get("/api/user-activity")
async def get_user_activity(
    start_date: str = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(None, description="End date (YYYY-MM-DD)"),
    user_filter: str = Query(None, description="Filter by specific user code")
):
    """
    Get user activity summary from Opera - transactions posted by user, type, and date.
    Tracks activity from ntran (nominal ledger), atran (cashbook), and aentry tables.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Default to last 30 days if no dates provided
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        result = {
            "success": True,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            },
            "users": [],
            "summary": {
                "total_users": 0,
                "total_transactions": 0,
                "by_type": {}
            },
            "daily_activity": [],
            "hourly_distribution": []
        }

        # Build user filter clause
        user_clause = ""
        if user_filter:
            user_clause = f"AND RTRIM(nt_inp) = '{user_filter}'"

        # ===========================================
        # 1. NOMINAL LEDGER ACTIVITY (ntran)
        # ===========================================

        # Get transaction counts by user
        ntran_user_sql = f"""
            SELECT
                RTRIM(nt_inp) AS user_code,
                COUNT(*) AS transaction_count,
                SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS total_debits,
                SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS total_credits,
                MIN(datecreated) AS first_activity,
                MAX(datecreated) AS last_activity
            FROM ntran WITH (NOLOCK)
            WHERE datecreated >= '{start_date}'
              AND datecreated < DATEADD(day, 1, '{end_date}')
              AND nt_inp IS NOT NULL
              AND RTRIM(nt_inp) != ''
              {user_clause}
            GROUP BY RTRIM(nt_inp)
            ORDER BY transaction_count DESC
        """
        ntran_users = sql_connector.execute_query(ntran_user_sql)
        if hasattr(ntran_users, 'to_dict'):
            ntran_users = ntran_users.to_dict('records')

        # Get transaction counts by type per user
        ntran_type_sql = f"""
            SELECT
                RTRIM(nt_inp) AS user_code,
                nt_posttyp AS post_type,
                COUNT(*) AS count,
                SUM(ABS(nt_value)) AS total_value
            FROM ntran WITH (NOLOCK)
            WHERE datecreated >= '{start_date}'
              AND datecreated < DATEADD(day, 1, '{end_date}')
              AND nt_inp IS NOT NULL
              AND RTRIM(nt_inp) != ''
              {user_clause}
            GROUP BY RTRIM(nt_inp), nt_posttyp
            ORDER BY RTRIM(nt_inp), count DESC
        """
        ntran_types = sql_connector.execute_query(ntran_type_sql)
        if hasattr(ntran_types, 'to_dict'):
            ntran_types = ntran_types.to_dict('records')

        # Map post types to task descriptions (business operations)
        post_type_names = {
            'I': 'Sales Invoices Entered',
            'R': 'Sales Receipts Posted',
            'C': 'Sales Credit Notes Entered',
            'P': 'Purchase Payments Posted',
            'S': 'Purchase Invoices Entered',
            'D': 'Purchase Credit Notes Entered',
            'J': 'Journals Posted',
            'B': 'Bank Transactions Posted',
            'V': 'VAT Entries',
            'Y': 'Year End Adjustments',
            '': 'Other Entries'
        }

        # Build user activity data
        users_dict = {}
        for row in ntran_users or []:
            user_code = (row.get('user_code') or '').strip()
            if not user_code:
                continue

            users_dict[user_code] = {
                "user_code": user_code,
                "nominal_transactions": int(row.get('transaction_count') or 0),
                "cashbook_entries": 0,
                "total_debits": round(float(row.get('total_debits') or 0), 2),
                "total_credits": round(float(row.get('total_credits') or 0), 2),
                "first_activity": str(row.get('first_activity') or ''),
                "last_activity": str(row.get('last_activity') or ''),
                "by_type": {}
            }

        # Add type breakdowns
        for row in ntran_types or []:
            user_code = (row.get('user_code') or '').strip()
            if not user_code or user_code not in users_dict:
                continue

            post_type = (row.get('post_type') or '').strip()
            type_name = post_type_names.get(post_type, f'Type {post_type}')

            users_dict[user_code]["by_type"][type_name] = {
                "count": int(row.get('count') or 0),
                "total_value": round(float(row.get('total_value') or 0), 2)
            }

        # ===========================================
        # 2. CASHBOOK ACTIVITY (atran/aentry)
        # ===========================================

        # Build user filter for cashbook
        cb_user_clause = ""
        if user_filter:
            cb_user_clause = f"AND RTRIM(at_inputby) = '{user_filter}'"

        atran_user_sql = f"""
            SELECT
                RTRIM(at_inputby) AS user_code,
                COUNT(*) AS transaction_count,
                SUM(CASE WHEN at_value > 0 THEN at_value ELSE 0 END) / 100.0 AS total_receipts,
                SUM(CASE WHEN at_value < 0 THEN ABS(at_value) ELSE 0 END) / 100.0 AS total_payments,
                MIN(datecreated) AS first_activity,
                MAX(datecreated) AS last_activity
            FROM atran WITH (NOLOCK)
            WHERE datecreated >= '{start_date}'
              AND datecreated < DATEADD(day, 1, '{end_date}')
              AND at_inputby IS NOT NULL
              AND RTRIM(at_inputby) != ''
              {cb_user_clause}
            GROUP BY RTRIM(at_inputby)
            ORDER BY transaction_count DESC
        """
        atran_users = sql_connector.execute_query(atran_user_sql)
        if hasattr(atran_users, 'to_dict'):
            atran_users = atran_users.to_dict('records')

        # Merge cashbook data into users
        for row in atran_users or []:
            user_code = (row.get('user_code') or '').strip()
            if not user_code:
                continue

            if user_code not in users_dict:
                users_dict[user_code] = {
                    "user_code": user_code,
                    "nominal_transactions": 0,
                    "cashbook_entries": 0,
                    "total_debits": 0,
                    "total_credits": 0,
                    "first_activity": str(row.get('first_activity') or ''),
                    "last_activity": str(row.get('last_activity') or ''),
                    "by_type": {}
                }

            users_dict[user_code]["cashbook_entries"] = int(row.get('transaction_count') or 0)
            users_dict[user_code]["by_type"]["Cashbook Receipts"] = {
                "count": int(row.get('transaction_count') or 0),
                "total_value": round(float(row.get('total_receipts') or 0), 2)
            }
            users_dict[user_code]["by_type"]["Cashbook Payments"] = {
                "count": int(row.get('transaction_count') or 0),
                "total_value": round(float(row.get('total_payments') or 0), 2)
            }

            # Update activity times if cashbook activity is earlier/later
            cb_first = str(row.get('first_activity') or '')
            cb_last = str(row.get('last_activity') or '')
            if cb_first and (not users_dict[user_code]["first_activity"] or cb_first < users_dict[user_code]["first_activity"]):
                users_dict[user_code]["first_activity"] = cb_first
            if cb_last and (not users_dict[user_code]["last_activity"] or cb_last > users_dict[user_code]["last_activity"]):
                users_dict[user_code]["last_activity"] = cb_last

        # ===========================================
        # 3. DAILY ACTIVITY PATTERN
        # ===========================================

        daily_sql = f"""
            SELECT
                CONVERT(date, datecreated) AS activity_date,
                COUNT(*) AS transaction_count
            FROM ntran WITH (NOLOCK)
            WHERE datecreated >= '{start_date}'
              AND datecreated < DATEADD(day, 1, '{end_date}')
              AND nt_inp IS NOT NULL
              AND RTRIM(nt_inp) != ''
              {user_clause}
            GROUP BY CONVERT(date, datecreated)
            ORDER BY activity_date
        """
        daily_result = sql_connector.execute_query(daily_sql)
        if hasattr(daily_result, 'to_dict'):
            daily_result = daily_result.to_dict('records')

        daily_activity = []
        for row in daily_result or []:
            daily_activity.append({
                "date": str(row.get('activity_date') or ''),
                "count": int(row.get('transaction_count') or 0)
            })

        # ===========================================
        # 4. HOURLY DISTRIBUTION
        # ===========================================

        hourly_sql = f"""
            SELECT
                DATEPART(hour, datecreated) AS hour,
                COUNT(*) AS transaction_count
            FROM ntran WITH (NOLOCK)
            WHERE datecreated >= '{start_date}'
              AND datecreated < DATEADD(day, 1, '{end_date}')
              AND nt_inp IS NOT NULL
              AND RTRIM(nt_inp) != ''
              {user_clause}
            GROUP BY DATEPART(hour, datecreated)
            ORDER BY hour
        """
        hourly_result = sql_connector.execute_query(hourly_sql)
        if hasattr(hourly_result, 'to_dict'):
            hourly_result = hourly_result.to_dict('records')

        hourly_distribution = []
        for row in hourly_result or []:
            hour = int(row.get('hour') or 0)
            hourly_distribution.append({
                "hour": hour,
                "label": f"{hour:02d}:00",
                "count": int(row.get('transaction_count') or 0)
            })

        # ===========================================
        # 5. BUILD SUMMARY
        # ===========================================

        # Calculate totals
        total_transactions = 0
        by_type_totals = {}

        for user_data in users_dict.values():
            total_transactions += user_data["nominal_transactions"] + user_data["cashbook_entries"]
            for type_name, type_data in user_data["by_type"].items():
                if type_name not in by_type_totals:
                    by_type_totals[type_name] = {"count": 0, "total_value": 0}
                by_type_totals[type_name]["count"] += type_data["count"]
                by_type_totals[type_name]["total_value"] += type_data["total_value"]

        # Sort users by total activity
        users_list = sorted(
            users_dict.values(),
            key=lambda x: x["nominal_transactions"] + x["cashbook_entries"],
            reverse=True
        )

        result["users"] = users_list
        result["summary"]["total_users"] = len(users_list)
        result["summary"]["total_transactions"] = total_transactions
        result["summary"]["by_type"] = by_type_totals
        result["daily_activity"] = daily_activity
        result["hourly_distribution"] = hourly_distribution

        return result

    except Exception as e:
        logger.error(f"User activity error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/user-activity/users")
async def get_user_list():
    """
    Get list of all users who have posted transactions in Opera.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get unique users from ntran
        users_sql = """
            SELECT DISTINCT RTRIM(nt_inp) AS user_code
            FROM ntran WITH (NOLOCK)
            WHERE nt_inp IS NOT NULL
              AND RTRIM(nt_inp) != ''
            ORDER BY user_code
        """
        users_result = sql_connector.execute_query(users_sql)
        if hasattr(users_result, 'to_dict'):
            users_result = users_result.to_dict('records')

        users = [row['user_code'].strip() for row in (users_result or []) if row.get('user_code')]

        return {
            "success": True,
            "count": len(users),
            "users": users
        }

    except Exception as e:
        logger.error(f"User list error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/user-activity/discover-fields")
async def discover_user_fields():
    """
    Discover all tables and columns in the database that might track user activity.
    Searches for columns with patterns like: *_inp*, *_user*, *_oper*, *_by, *cruser*, etc.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Query information_schema to find all columns that might contain user data
        discovery_sql = """
            SELECT
                t.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.TABLES t
            INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
            WHERE t.TABLE_TYPE = 'BASE TABLE'
              AND (
                  c.COLUMN_NAME LIKE '%_inp%'
                  OR c.COLUMN_NAME LIKE '%_user%'
                  OR c.COLUMN_NAME LIKE '%_oper%'
                  OR c.COLUMN_NAME LIKE '%cruser%'
                  OR c.COLUMN_NAME LIKE '%inputby%'
                  OR c.COLUMN_NAME LIKE '%_by'
                  OR c.COLUMN_NAME LIKE '%operator%'
                  OR c.COLUMN_NAME LIKE '%created%'
                  OR c.COLUMN_NAME LIKE '%modified%'
              )
            ORDER BY t.TABLE_NAME, c.COLUMN_NAME
        """
        discovery_result = sql_connector.execute_query(discovery_sql)
        if hasattr(discovery_result, 'to_dict'):
            discovery_result = discovery_result.to_dict('records')

        # Group by table
        tables = {}
        for row in discovery_result or []:
            table_name = row['TABLE_NAME']
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append({
                "column": row['COLUMN_NAME'],
                "type": row['DATA_TYPE'],
                "max_length": row['CHARACTER_MAXIMUM_LENGTH']
            })

        return {
            "success": True,
            "table_count": len(tables),
            "tables": tables
        }

    except Exception as e:
        logger.error(f"Discovery error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/user-activity/all-modules")
async def get_all_module_activity(
    start_date: str = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(None, description="End date (YYYY-MM-DD)"),
    user_filter: str = Query(None, description="Filter by specific user code")
):
    """
    Get user activity across ALL Opera modules:
    - Nominal Ledger (ntran)
    - Cashbook (atran/aentry)
    - Sales Ledger (stran via datecreated)
    - Purchase Ledger (ptran via datecreated)
    - SOP - Sales Order Processing
    - POP - Purchase Order Processing
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Default to last 30 days if no dates provided
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        result = {
            "success": True,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            },
            "modules": {}
        }

        # Helper to build user filter
        def make_user_clause(field_name):
            if user_filter:
                return f"AND RTRIM({field_name}) = '{user_filter}'"
            return ""

        # ===========================================
        # 1. NOMINAL LEDGER (ntran - nt_inp field)
        # ===========================================
        try:
            nl_sql = f"""
                SELECT
                    RTRIM(nt_inp) AS user_code,
                    COUNT(*) AS count,
                    SUM(ABS(nt_value)) AS total_value,
                    MIN(datecreated) AS first_activity,
                    MAX(datecreated) AS last_activity
                FROM ntran WITH (NOLOCK)
                WHERE datecreated >= '{start_date}'
                  AND datecreated < DATEADD(day, 1, '{end_date}')
                  AND nt_inp IS NOT NULL
                  AND RTRIM(nt_inp) != ''
                  {make_user_clause('nt_inp')}
                GROUP BY RTRIM(nt_inp)
                ORDER BY count DESC
            """
            nl_result = sql_connector.execute_query(nl_sql)
            if hasattr(nl_result, 'to_dict'):
                nl_result = nl_result.to_dict('records')
            result["modules"]["nominal_ledger"] = {
                "table": "ntran",
                "user_field": "nt_inp",
                "users": [{
                    "user": r.get('user_code', '').strip(),
                    "count": int(r.get('count') or 0),
                    "value": round(float(r.get('total_value') or 0), 2),
                    "first": str(r.get('first_activity') or ''),
                    "last": str(r.get('last_activity') or '')
                } for r in (nl_result or [])]
            }
        except Exception as e:
            result["modules"]["nominal_ledger"] = {"error": str(e)}

        # ===========================================
        # 2. CASHBOOK (atran - at_inputby field)
        # ===========================================
        try:
            cb_sql = f"""
                SELECT
                    RTRIM(at_inputby) AS user_code,
                    COUNT(*) AS count,
                    SUM(ABS(at_value)) / 100.0 AS total_value,
                    MIN(datecreated) AS first_activity,
                    MAX(datecreated) AS last_activity
                FROM atran WITH (NOLOCK)
                WHERE datecreated >= '{start_date}'
                  AND datecreated < DATEADD(day, 1, '{end_date}')
                  AND at_inputby IS NOT NULL
                  AND RTRIM(at_inputby) != ''
                  {make_user_clause('at_inputby')}
                GROUP BY RTRIM(at_inputby)
                ORDER BY count DESC
            """
            cb_result = sql_connector.execute_query(cb_sql)
            if hasattr(cb_result, 'to_dict'):
                cb_result = cb_result.to_dict('records')
            result["modules"]["cashbook"] = {
                "table": "atran",
                "user_field": "at_inputby",
                "users": [{
                    "user": r.get('user_code', '').strip(),
                    "count": int(r.get('count') or 0),
                    "value": round(float(r.get('total_value') or 0), 2),
                    "first": str(r.get('first_activity') or ''),
                    "last": str(r.get('last_activity') or '')
                } for r in (cb_result or [])]
            }
        except Exception as e:
            result["modules"]["cashbook"] = {"error": str(e)}

        # ===========================================
        # 3. SALES LEDGER (stran - check for st_inp or use datecreated)
        # ===========================================
        try:
            # First try to check if st_inp exists
            sl_check_sql = """
                SELECT TOP 1 * FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'stran' AND COLUMN_NAME LIKE 'st_%inp%'
            """
            sl_check = sql_connector.execute_query(sl_check_sql)
            if hasattr(sl_check, 'to_dict'):
                sl_check = sl_check.to_dict('records')

            if sl_check:
                # Use the user field
                user_col = sl_check[0]['COLUMN_NAME']
                sl_sql = f"""
                    SELECT
                        RTRIM({user_col}) AS user_code,
                        COUNT(*) AS count,
                        SUM(ABS(st_trvalue)) AS total_value,
                        MIN(datecreated) AS first_activity,
                        MAX(datecreated) AS last_activity
                    FROM stran WITH (NOLOCK)
                    WHERE datecreated >= '{start_date}'
                      AND datecreated < DATEADD(day, 1, '{end_date}')
                      AND {user_col} IS NOT NULL
                      AND RTRIM({user_col}) != ''
                    GROUP BY RTRIM({user_col})
                    ORDER BY count DESC
                """
                sl_result = sql_connector.execute_query(sl_sql)
                if hasattr(sl_result, 'to_dict'):
                    sl_result = sl_result.to_dict('records')
                result["modules"]["sales_ledger"] = {
                    "table": "stran",
                    "user_field": user_col,
                    "users": [{
                        "user": r.get('user_code', '').strip(),
                        "count": int(r.get('count') or 0),
                        "value": round(float(r.get('total_value') or 0), 2),
                        "first": str(r.get('first_activity') or ''),
                        "last": str(r.get('last_activity') or '')
                    } for r in (sl_result or [])]
                }
            else:
                # Just count by date
                sl_sql = f"""
                    SELECT
                        COUNT(*) AS count,
                        SUM(ABS(st_trvalue)) AS total_value,
                        MIN(datecreated) AS first_activity,
                        MAX(datecreated) AS last_activity
                    FROM stran WITH (NOLOCK)
                    WHERE datecreated >= '{start_date}'
                      AND datecreated < DATEADD(day, 1, '{end_date}')
                """
                sl_result = sql_connector.execute_query(sl_sql)
                if hasattr(sl_result, 'to_dict'):
                    sl_result = sl_result.to_dict('records')
                row = sl_result[0] if sl_result else {}
                result["modules"]["sales_ledger"] = {
                    "table": "stran",
                    "user_field": "none - tracked via ntran",
                    "total_count": int(row.get('count') or 0),
                    "total_value": round(float(row.get('total_value') or 0), 2)
                }
        except Exception as e:
            result["modules"]["sales_ledger"] = {"error": str(e)}

        # ===========================================
        # 4. PURCHASE LEDGER (ptran - check for pt_inp or use datecreated)
        # ===========================================
        try:
            pl_check_sql = """
                SELECT TOP 1 * FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'ptran' AND COLUMN_NAME LIKE 'pt_%inp%'
            """
            pl_check = sql_connector.execute_query(pl_check_sql)
            if hasattr(pl_check, 'to_dict'):
                pl_check = pl_check.to_dict('records')

            if pl_check:
                user_col = pl_check[0]['COLUMN_NAME']
                pl_sql = f"""
                    SELECT
                        RTRIM({user_col}) AS user_code,
                        COUNT(*) AS count,
                        SUM(ABS(pt_trvalue)) AS total_value,
                        MIN(datecreated) AS first_activity,
                        MAX(datecreated) AS last_activity
                    FROM ptran WITH (NOLOCK)
                    WHERE datecreated >= '{start_date}'
                      AND datecreated < DATEADD(day, 1, '{end_date}')
                      AND {user_col} IS NOT NULL
                      AND RTRIM({user_col}) != ''
                    GROUP BY RTRIM({user_col})
                    ORDER BY count DESC
                """
                pl_result = sql_connector.execute_query(pl_sql)
                if hasattr(pl_result, 'to_dict'):
                    pl_result = pl_result.to_dict('records')
                result["modules"]["purchase_ledger"] = {
                    "table": "ptran",
                    "user_field": user_col,
                    "users": [{
                        "user": r.get('user_code', '').strip(),
                        "count": int(r.get('count') or 0),
                        "value": round(float(r.get('total_value') or 0), 2),
                        "first": str(r.get('first_activity') or ''),
                        "last": str(r.get('last_activity') or '')
                    } for r in (pl_result or [])]
                }
            else:
                pl_sql = f"""
                    SELECT
                        COUNT(*) AS count,
                        SUM(ABS(pt_trvalue)) AS total_value,
                        MIN(datecreated) AS first_activity,
                        MAX(datecreated) AS last_activity
                    FROM ptran WITH (NOLOCK)
                    WHERE datecreated >= '{start_date}'
                      AND datecreated < DATEADD(day, 1, '{end_date}')
                """
                pl_result = sql_connector.execute_query(pl_sql)
                if hasattr(pl_result, 'to_dict'):
                    pl_result = pl_result.to_dict('records')
                row = pl_result[0] if pl_result else {}
                result["modules"]["purchase_ledger"] = {
                    "table": "ptran",
                    "user_field": "none - tracked via ntran",
                    "total_count": int(row.get('count') or 0),
                    "total_value": round(float(row.get('total_value') or 0), 2)
                }
        except Exception as e:
            result["modules"]["purchase_ledger"] = {"error": str(e)}

        # ===========================================
        # 5. SALES ORDER PROCESSING (SOP)
        # Check for sorder, sorhed, sohist tables
        # ===========================================
        sop_tables = ['sorder', 'sorhed', 'sohist', 'soline']
        for sop_table in sop_tables:
            try:
                # Check if table exists and find user columns
                sop_check_sql = f"""
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{sop_table}'
                    AND (COLUMN_NAME LIKE '%_inp%' OR COLUMN_NAME LIKE '%_user%' OR COLUMN_NAME LIKE '%_oper%' OR COLUMN_NAME LIKE '%inputby%')
                """
                sop_check = sql_connector.execute_query(sop_check_sql)
                if hasattr(sop_check, 'to_dict'):
                    sop_check = sop_check.to_dict('records')

                if sop_check:
                    user_col = sop_check[0]['COLUMN_NAME']
                    sop_sql = f"""
                        SELECT
                            RTRIM({user_col}) AS user_code,
                            COUNT(*) AS count,
                            MIN(datecreated) AS first_activity,
                            MAX(datecreated) AS last_activity
                        FROM {sop_table} WITH (NOLOCK)
                        WHERE datecreated >= '{start_date}'
                          AND datecreated < DATEADD(day, 1, '{end_date}')
                          AND {user_col} IS NOT NULL
                          AND RTRIM({user_col}) != ''
                        GROUP BY RTRIM({user_col})
                        ORDER BY count DESC
                    """
                    sop_result = sql_connector.execute_query(sop_sql)
                    if hasattr(sop_result, 'to_dict'):
                        sop_result = sop_result.to_dict('records')
                    result["modules"][f"sop_{sop_table}"] = {
                        "table": sop_table,
                        "user_field": user_col,
                        "users": [{
                            "user": r.get('user_code', '').strip(),
                            "count": int(r.get('count') or 0),
                            "first": str(r.get('first_activity') or ''),
                            "last": str(r.get('last_activity') or '')
                        } for r in (sop_result or [])]
                    }
            except Exception:
                pass  # Table may not exist

        # ===========================================
        # 6. PURCHASE ORDER PROCESSING (POP)
        # Check for porder, porhed, pohist tables
        # ===========================================
        pop_tables = ['porder', 'porhed', 'pohist', 'poline']
        for pop_table in pop_tables:
            try:
                pop_check_sql = f"""
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{pop_table}'
                    AND (COLUMN_NAME LIKE '%_inp%' OR COLUMN_NAME LIKE '%_user%' OR COLUMN_NAME LIKE '%_oper%' OR COLUMN_NAME LIKE '%inputby%')
                """
                pop_check = sql_connector.execute_query(pop_check_sql)
                if hasattr(pop_check, 'to_dict'):
                    pop_check = pop_check.to_dict('records')

                if pop_check:
                    user_col = pop_check[0]['COLUMN_NAME']
                    pop_sql = f"""
                        SELECT
                            RTRIM({user_col}) AS user_code,
                            COUNT(*) AS count,
                            MIN(datecreated) AS first_activity,
                            MAX(datecreated) AS last_activity
                        FROM {pop_table} WITH (NOLOCK)
                        WHERE datecreated >= '{start_date}'
                          AND datecreated < DATEADD(day, 1, '{end_date}')
                          AND {user_col} IS NOT NULL
                          AND RTRIM({user_col}) != ''
                        GROUP BY RTRIM({user_col})
                        ORDER BY count DESC
                    """
                    pop_result = sql_connector.execute_query(pop_sql)
                    if hasattr(pop_result, 'to_dict'):
                        pop_result = pop_result.to_dict('records')
                    result["modules"][f"pop_{pop_table}"] = {
                        "table": pop_table,
                        "user_field": user_col,
                        "users": [{
                            "user": r.get('user_code', '').strip(),
                            "count": int(r.get('count') or 0),
                            "first": str(r.get('first_activity') or ''),
                            "last": str(r.get('last_activity') or '')
                        } for r in (pop_result or [])]
                    }
            except Exception:
                pass  # Table may not exist

        # ===========================================
        # 7. DYNAMICALLY CHECK ALL TABLES FOR USER COLUMNS
        # ===========================================
        try:
            all_user_cols_sql = """
                SELECT DISTINCT t.TABLE_NAME, c.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLES t
                INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                  AND (
                      c.COLUMN_NAME LIKE '%_inp'
                      OR c.COLUMN_NAME LIKE '%_inp_%'
                      OR c.COLUMN_NAME LIKE '%inputby%'
                      OR c.COLUMN_NAME LIKE '%cruser%'
                  )
                  AND c.DATA_TYPE IN ('char', 'varchar', 'nchar', 'nvarchar')
                ORDER BY t.TABLE_NAME
            """
            all_cols = sql_connector.execute_query(all_user_cols_sql)
            if hasattr(all_cols, 'to_dict'):
                all_cols = all_cols.to_dict('records')

            discovered_tables = {}
            for row in all_cols or []:
                table = row['TABLE_NAME']
                col = row['COLUMN_NAME']
                if table not in discovered_tables:
                    discovered_tables[table] = []
                discovered_tables[table].append(col)

            result["discovered_user_columns"] = discovered_tables

        except Exception as e:
            result["discovered_user_columns"] = {"error": str(e)}

        return result

    except Exception as e:
        logger.error(f"All modules activity error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# STOCK MODULE - Product Master, Stock Levels, Transactions
# =============================================================================
# Documentation: docs/opera-modules/stock/README.md
# Part of the Opera Modules Modernization Project
# =============================================================================

@app.get("/api/stock/products")
async def get_stock_products(
    search: str = Query("", description="Search by reference, description, or alt codes"),
    category: str = Query("", description="Filter by category code"),
    profile: str = Query("", description="Filter by profile code"),
    warehouse: str = Query("", description="Filter to show only products with stock in this warehouse"),
    include_zero_stock: bool = Query(True, description="Include products with zero stock"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """
    List and search stock products.

    Returns product master data from cname table with aggregated stock levels.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        # Build WHERE clause
        conditions = ["1=1"]

        if search:
            search_term = search.replace("'", "''")
            conditions.append(f"""(
                cn_ref LIKE '%{search_term}%' OR
                cn_desc LIKE '%{search_term}%' OR
                cn_alt1 LIKE '%{search_term}%' OR
                cn_alt2 LIKE '%{search_term}%' OR
                cn_cat LIKE '%{search_term}%'
            )""")

        if category:
            conditions.append(f"cn_catag = '{category.replace(chr(39), chr(39)+chr(39))}'")

        if profile:
            conditions.append(f"cn_fact = '{profile.replace(chr(39), chr(39)+chr(39))}'")

        if not include_zero_stock:
            conditions.append("cn_instock > 0")

        where_clause = " AND ".join(conditions)

        # Main query - product master with stock summary
        query = f"""
            SELECT
                cn.cn_ref AS ref,
                RTRIM(cn.cn_desc) AS description,
                RTRIM(cn.cn_catag) AS category,
                RTRIM(cn.cn_fact) AS profile,
                cn.cn_cost AS cost_price,
                cn.cn_sell AS sell_price,
                cn.cn_instock AS total_in_stock,
                cn.cn_freest AS free_stock,
                cn.cn_alloc AS allocated,
                cn.cn_onorder AS on_order,
                cn.cn_saleord AS on_sales_order,
                RTRIM(cn.cn_alt1) AS alt_code_1,
                RTRIM(cn.cn_alt2) AS alt_code_2,
                RTRIM(cn.cn_anal) AS analysis_code,
                cn.cn_lastiss AS last_issued,
                cn.cn_lastrct AS last_received
            FROM cname cn
            WHERE {where_clause}
            ORDER BY cn.cn_ref
            OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"products": [], "count": 0, "offset": offset, "limit": limit}

        # Convert to list of dicts
        products = result.to_dict('records')

        # Clean up the data
        for p in products:
            p['ref'] = p['ref'].strip() if p['ref'] else ''
            # Convert dates to strings
            if p.get('last_issued') and pd.notna(p['last_issued']):
                p['last_issued'] = str(p['last_issued'])[:10]
            else:
                p['last_issued'] = None
            if p.get('last_received') and pd.notna(p['last_received']):
                p['last_received'] = str(p['last_received'])[:10]
            else:
                p['last_received'] = None

        # Get total count for pagination
        count_query = f"SELECT COUNT(*) as cnt FROM cname cn WHERE {where_clause}"
        count_result = sql_connector.execute_query(count_query)
        total_count = int(count_result.iloc[0]['cnt']) if count_result is not None and len(count_result) > 0 else 0

        return {
            "products": products,
            "count": total_count,
            "offset": offset,
            "limit": limit
        }

    except Exception as e:
        logger.error(f"Error fetching stock products: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stock/products/{ref}")
async def get_stock_product_detail(ref: str):
    """
    Get detailed product information including stock levels by warehouse.

    Returns:
    - Product master data from cname
    - Profile details from cfact
    - Category details from ccatg
    - Stock levels per warehouse from cstwh
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        # Get product master
        product_query = f"""
            SELECT
                cn.cn_ref AS ref,
                RTRIM(cn.cn_desc) AS description,
                RTRIM(cn.cn_catag) AS category_code,
                RTRIM(cn.cn_fact) AS profile_code,
                cn.cn_cost AS cost_price,
                cn.cn_lcost AS last_cost_price,
                cn.cn_sell AS sell_price,
                cn.cn_ssell AS sale_price,
                cn.cn_nsell AS next_price,
                cn.cn_ndate AS next_price_date,
                cn.cn_instock AS total_in_stock,
                cn.cn_freest AS free_stock,
                cn.cn_alloc AS allocated,
                cn.cn_onorder AS on_order,
                cn.cn_saleord AS on_sales_order,
                RTRIM(cn.cn_alt1) AS alt_code_1,
                RTRIM(cn.cn_alt2) AS alt_code_2,
                RTRIM(cn.cn_alt3) AS alt_code_3,
                RTRIM(cn.cn_cat) AS search_ref_1,
                RTRIM(cn.cn_cat2) AS search_ref_2,
                RTRIM(cn.cn_anal) AS sales_analysis_code,
                RTRIM(cn.cn_panal) AS purchase_analysis_code,
                RTRIM(cn.cn_discmat) AS discount_matrix,
                cn.cn_unitw AS unit_weight,
                cn.cn_unitv AS unit_volume,
                cn.cn_lastiss AS last_issued,
                cn.cn_lastrct AS last_received,
                cn.cn_sett AS settlement_discount,
                cn.cn_line AS line_discount,
                cn.cn_over AS overall_discount,
                RTRIM(cn.cn_comcode) AS commodity_code,
                RTRIM(cn.cn_cntorig) AS country_of_origin,
                RTRIM(cg.cg_desc) AS category_name,
                RTRIM(cf.cf_name) AS profile_name,
                cf.cf_stock AS is_stocked,
                cf.cf_batch AS is_batch_tracked,
                cf.cf_serial AS is_serial_tracked,
                cf.cf_fifo AS is_fifo,
                cf.cf_aver AS is_average_costed
            FROM cname cn
            LEFT JOIN ccatg cg ON cn.cn_catag = cg.cg_code
            LEFT JOIN cfact cf ON cn.cn_fact = cf.cf_code
            WHERE cn.cn_ref = '{ref.replace(chr(39), chr(39)+chr(39))}'
        """

        product_result = sql_connector.execute_query(product_query)

        if product_result is None or len(product_result) == 0:
            raise HTTPException(status_code=404, detail=f"Product not found: {ref}")

        product = product_result.iloc[0].to_dict()

        # Clean up string fields
        for key in product:
            if isinstance(product[key], str):
                product[key] = product[key].strip()
            # Convert dates
            if key in ['last_issued', 'last_received', 'next_price_date']:
                if product[key] and pd.notna(product[key]):
                    product[key] = str(product[key])[:10]
                else:
                    product[key] = None
            # Convert booleans
            if key in ['is_stocked', 'is_batch_tracked', 'is_serial_tracked', 'is_fifo',
                      'is_average_costed', 'settlement_discount', 'line_discount', 'overall_discount']:
                product[key] = bool(product[key]) if product[key] is not None else False

        # Get stock levels by warehouse
        stock_query = f"""
            SELECT
                cs.cs_whar AS warehouse_code,
                RTRIM(cw.cw_desc) AS warehouse_name,
                cs.cs_instock AS in_stock,
                cs.cs_freest AS free_stock,
                cs.cs_alloc AS allocated,
                cs.cs_order AS on_order,
                cs.cs_saleord AS on_sales_order,
                cs.cs_woalloc AS works_order_allocated,
                cs.cs_workord AS on_works_order,
                cs.cs_cost AS cost_price,
                cs.cs_sell AS sell_price,
                RTRIM(cs.cs_bin) AS bin_location,
                cs.cs_reordl AS reorder_level,
                cs.cs_reordq AS reorder_quantity,
                cs.cs_minst AS minimum_stock,
                cs.cs_lastiss AS last_issued,
                cs.cs_lastrct AS last_received
            FROM cstwh cs
            LEFT JOIN cware cw ON cs.cs_whar = cw.cw_code
            WHERE cs.cs_ref = '{ref.replace(chr(39), chr(39)+chr(39))}'
            ORDER BY cs.cs_whar
        """

        stock_result = sql_connector.execute_query(stock_query)

        warehouses = []
        if stock_result is not None and len(stock_result) > 0:
            for _, row in stock_result.iterrows():
                wh = row.to_dict()
                wh['warehouse_code'] = wh['warehouse_code'].strip() if wh['warehouse_code'] else ''
                if wh.get('last_issued') and pd.notna(wh['last_issued']):
                    wh['last_issued'] = str(wh['last_issued'])[:10]
                else:
                    wh['last_issued'] = None
                if wh.get('last_received') and pd.notna(wh['last_received']):
                    wh['last_received'] = str(wh['last_received'])[:10]
                else:
                    wh['last_received'] = None
                warehouses.append(wh)

        return {
            "product": product,
            "stock_by_warehouse": warehouses
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching product detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stock/warehouses")
async def get_stock_warehouses():
    """
    List all stock warehouses.

    Returns warehouse master data from cware table.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        query = """
            SELECT
                cw_code AS code,
                RTRIM(cw_desc) AS name,
                RTRIM(cw_addr1) AS address_1,
                RTRIM(cw_addr2) AS address_2,
                RTRIM(cw_addr3) AS address_3,
                RTRIM(cw_addr4) AS address_4,
                RTRIM(cw_post) AS postcode,
                RTRIM(cw_ctact) AS contact,
                RTRIM(cw_phone) AS phone,
                RTRIM(cw_fax) AS fax,
                cw_rec AS block_receipts,
                cw_iss AS block_issues,
                cw_ret AS block_returns,
                cw_tranf AS block_transfers
            FROM cware
            ORDER BY cw_code
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"warehouses": []}

        warehouses = result.to_dict('records')

        # Clean up and convert booleans
        for wh in warehouses:
            wh['code'] = wh['code'].strip() if wh['code'] else ''
            for key in ['block_receipts', 'block_issues', 'block_returns', 'block_transfers']:
                wh[key] = bool(wh[key]) if wh[key] is not None else False

        return {"warehouses": warehouses}

    except Exception as e:
        logger.error(f"Error fetching warehouses: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stock/products/{ref}/transactions")
async def get_stock_transactions(
    ref: str,
    warehouse: str = Query("", description="Filter by warehouse code"),
    trans_type: str = Query("", description="Filter by transaction type (R=Receipt, I=Issue, etc.)"),
    date_from: str = Query("", description="Start date (YYYY-MM-DD)"),
    date_to: str = Query("", description="End date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """
    Get stock transaction history for a product.

    Returns transactions from ctran table showing stock movements.

    Transaction types (CT_TYPE):
    - R = Receipt
    - I = Issue
    - T = Transfer
    - A = Adjustment
    - S = Sale
    - P = Purchase Return
    - W = Works Order Issue
    - M = Works Order Receipt (Manufacture)
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        # Build WHERE clause
        conditions = [f"ct.ct_ref = '{ref.replace(chr(39), chr(39)+chr(39))}'"]

        if warehouse:
            conditions.append(f"ct.ct_loc = '{warehouse.replace(chr(39), chr(39)+chr(39))}'")

        if trans_type:
            conditions.append(f"ct.ct_type = '{trans_type.replace(chr(39), chr(39)+chr(39))}'")

        if date_from:
            conditions.append(f"ct.ct_date >= '{date_from}'")

        if date_to:
            conditions.append(f"ct.ct_date <= '{date_to}'")

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                ct.ct_ref AS stock_ref,
                ct.ct_loc AS warehouse,
                ct.ct_type AS trans_type,
                ct.ct_date AS trans_date,
                ct.ct_crdate AS created_date,
                ct.ct_quan AS quantity,
                ct.ct_moved AS quantity_moved,
                RTRIM(ct.ct_referen) AS reference,
                RTRIM(ct.ct_account) AS account,
                ct.ct_cost AS cost_value,
                ct.ct_sell AS sell_value,
                RTRIM(ct.ct_comnt) AS comment,
                RTRIM(ct.ct_ref2) AS reference_2,
                ct.ct_time AS trans_time,
                RTRIM(cw.cw_desc) AS warehouse_name
            FROM ctran ct
            LEFT JOIN cware cw ON ct.ct_loc = cw.cw_code
            WHERE {where_clause}
            ORDER BY ct.ct_date DESC, ct.ct_time DESC
            OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"transactions": [], "count": 0, "offset": offset, "limit": limit}

        transactions = result.to_dict('records')

        # Transaction type descriptions
        type_descriptions = {
            'R': 'Receipt',
            'I': 'Issue',
            'T': 'Transfer',
            'A': 'Adjustment',
            'S': 'Sale',
            'P': 'Purchase Return',
            'W': 'Works Order Issue',
            'M': 'Manufacture Receipt'
        }

        # Clean up the data
        for t in transactions:
            t['stock_ref'] = t['stock_ref'].strip() if t['stock_ref'] else ''
            t['warehouse'] = t['warehouse'].strip() if t['warehouse'] else ''
            t['trans_type'] = t['trans_type'].strip() if t['trans_type'] else ''
            t['trans_type_desc'] = type_descriptions.get(t['trans_type'], 'Unknown')
            if t.get('trans_date') and pd.notna(t['trans_date']):
                t['trans_date'] = str(t['trans_date'])[:10]
            else:
                t['trans_date'] = None
            if t.get('created_date') and pd.notna(t['created_date']):
                t['created_date'] = str(t['created_date'])[:10]
            else:
                t['created_date'] = None

        # Get total count
        count_query = f"SELECT COUNT(*) as cnt FROM ctran ct WHERE {where_clause}"
        count_result = sql_connector.execute_query(count_query)
        total_count = int(count_result.iloc[0]['cnt']) if count_result is not None and len(count_result) > 0 else 0

        return {
            "transactions": transactions,
            "count": total_count,
            "offset": offset,
            "limit": limit
        }

    except Exception as e:
        logger.error(f"Error fetching stock transactions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stock/categories")
async def get_stock_categories():
    """
    List all stock categories.

    Returns category master data from ccatg table.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        query = """
            SELECT
                cg_code AS code,
                RTRIM(cg_desc) AS description
            FROM ccatg
            ORDER BY cg_code
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"categories": []}

        categories = result.to_dict('records')
        for c in categories:
            c['code'] = c['code'].strip() if c['code'] else ''

        return {"categories": categories}

    except Exception as e:
        logger.error(f"Error fetching stock categories: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stock/profiles")
async def get_stock_profiles():
    """
    List all stock profiles.

    Profiles control product behavior (stocked, costing method, traceability, etc.)
    Returns profile data from cfact table.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        query = """
            SELECT
                cf_code AS code,
                RTRIM(cf_name) AS name,
                cf_stock AS is_stocked,
                cf_batch AS is_batch_tracked,
                cf_serial AS is_serial_tracked,
                cf_fifo AS is_fifo,
                cf_aver AS is_average_costed,
                cf_labour AS is_labour,
                cf_factor AS factor,
                RTRIM(cf_desc) AS unit_description,
                cf_dps AS quantity_decimals,
                cf_selldps AS price_decimals
            FROM cfact
            ORDER BY cf_code
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"profiles": []}

        profiles = result.to_dict('records')

        for p in profiles:
            p['code'] = p['code'].strip() if p['code'] else ''
            # Convert booleans
            for key in ['is_stocked', 'is_batch_tracked', 'is_serial_tracked',
                       'is_fifo', 'is_average_costed', 'is_labour']:
                p[key] = bool(p[key]) if p[key] is not None else False

        return {"profiles": profiles}

    except Exception as e:
        logger.error(f"Error fetching stock profiles: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stock/warehouse/{warehouse_code}/stock")
async def get_warehouse_stock(
    warehouse_code: str,
    include_zero_stock: bool = Query(False, description="Include products with zero stock"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """
    Get all stock in a specific warehouse.

    Returns stock levels from cstwh for the specified warehouse.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        # Build WHERE clause
        conditions = [f"cs.cs_whar = '{warehouse_code.replace(chr(39), chr(39)+chr(39))}'"]

        if not include_zero_stock:
            conditions.append("cs.cs_instock > 0")

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                cs.cs_ref AS stock_ref,
                RTRIM(cn.cn_desc) AS description,
                cs.cs_whar AS warehouse,
                cs.cs_instock AS in_stock,
                cs.cs_freest AS free_stock,
                cs.cs_alloc AS allocated,
                cs.cs_order AS on_order,
                cs.cs_saleord AS on_sales_order,
                cs.cs_cost AS cost_price,
                cs.cs_sell AS sell_price,
                RTRIM(cs.cs_bin) AS bin_location,
                cs.cs_reordl AS reorder_level,
                cs.cs_reordq AS reorder_quantity,
                cs.cs_lastiss AS last_issued,
                cs.cs_lastrct AS last_received
            FROM cstwh cs
            LEFT JOIN cname cn ON cs.cs_ref = cn.cn_ref
            WHERE {where_clause}
            ORDER BY cs.cs_ref
            OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"stock": [], "count": 0, "warehouse": warehouse_code, "offset": offset, "limit": limit}

        stock_items = result.to_dict('records')

        for s in stock_items:
            s['stock_ref'] = s['stock_ref'].strip() if s['stock_ref'] else ''
            if s.get('last_issued') and pd.notna(s['last_issued']):
                s['last_issued'] = str(s['last_issued'])[:10]
            else:
                s['last_issued'] = None
            if s.get('last_received') and pd.notna(s['last_received']):
                s['last_received'] = str(s['last_received'])[:10]
            else:
                s['last_received'] = None

        # Get total count
        count_query = f"SELECT COUNT(*) as cnt FROM cstwh cs WHERE {where_clause}"
        count_result = sql_connector.execute_query(count_query)
        total_count = int(count_result.iloc[0]['cnt']) if count_result is not None and len(count_result) > 0 else 0

        return {
            "stock": stock_items,
            "count": total_count,
            "warehouse": warehouse_code,
            "offset": offset,
            "limit": limit
        }

    except Exception as e:
        logger.error(f"Error fetching warehouse stock: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------------------------
# STOCK WRITE OPERATIONS
# -----------------------------------------------------------------------------

@app.post("/api/stock/adjustments")
async def create_stock_adjustment(
    stock_ref: str = Query(..., description="Product stock reference"),
    warehouse: str = Query(..., description="Warehouse code"),
    quantity: float = Query(..., description="Adjustment quantity (+/-)"),
    reason: str = Query("Adjustment", description="Reason for adjustment"),
    reference: str = Query("", description="Optional reference"),
    adjust_date: str = Query("", description="Adjustment date (YYYY-MM-DD, defaults to today)")
):
    """
    Create a stock adjustment (increase or decrease).

    - Positive quantity adds stock
    - Negative quantity removes stock
    - Creates audit trail in ctran
    - Updates cstwh (warehouse) and cname (product) stock levels
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        from datetime import date as date_type

        # Parse date
        if adjust_date:
            try:
                parsed_date = date_type.fromisoformat(adjust_date[:10])
            except ValueError:
                return {"success": False, "error": f"Invalid date format: {adjust_date}"}
        else:
            parsed_date = None  # Will default to today

        opera_import = OperaSQLImport(sql_connector)
        result = opera_import.import_stock_adjustment(
            stock_ref=stock_ref.strip(),
            warehouse=warehouse.strip(),
            quantity=quantity,
            reason=reason[:35] if reason else "Adjustment",
            reference=reference[:10] if reference else "",
            adjust_date=parsed_date,
            input_by="SQLRAG"
        )

        if result.success:
            return {
                "success": True,
                "stock_ref": stock_ref,
                "warehouse": warehouse,
                "quantity": quantity,
                "transaction_id": result.transaction_ref,
                "details": result.warnings
            }
        else:
            return {
                "success": False,
                "error": result.errors[0] if result.errors else "Unknown error"
            }

    except Exception as e:
        logger.error(f"Error creating stock adjustment: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}

@app.post("/api/stock/transfers")
async def create_stock_transfer(
    stock_ref: str = Query(..., description="Product stock reference"),
    from_warehouse: str = Query(..., description="Source warehouse code"),
    to_warehouse: str = Query(..., description="Destination warehouse code"),
    quantity: float = Query(..., gt=0, description="Quantity to transfer (must be positive)"),
    reason: str = Query("Transfer", description="Reason for transfer"),
    reference: str = Query("", description="Optional reference"),
    transfer_date: str = Query("", description="Transfer date (YYYY-MM-DD, defaults to today)")
):
    """
    Create a stock transfer between warehouses.

    - Quantity must be positive
    - Creates 2 ctran records (issue from source, receipt to destination)
    - Updates cstwh for both warehouses
    - Company-wide stock (cname) unchanged
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        from datetime import date as date_type

        # Validate different warehouses
        if from_warehouse.strip() == to_warehouse.strip():
            return {"success": False, "error": "Source and destination warehouse must be different"}

        # Parse date
        if transfer_date:
            try:
                parsed_date = date_type.fromisoformat(transfer_date[:10])
            except ValueError:
                return {"success": False, "error": f"Invalid date format: {transfer_date}"}
        else:
            parsed_date = None

        opera_import = OperaSQLImport(sql_connector)
        result = opera_import.import_stock_transfer(
            stock_ref=stock_ref.strip(),
            from_warehouse=from_warehouse.strip(),
            to_warehouse=to_warehouse.strip(),
            quantity=quantity,
            reason=reason[:35] if reason else "Transfer",
            reference=reference[:10] if reference else "",
            transfer_date=parsed_date,
            input_by="SQLRAG"
        )

        if result.success:
            return {
                "success": True,
                "stock_ref": stock_ref,
                "from_warehouse": from_warehouse,
                "to_warehouse": to_warehouse,
                "quantity": quantity,
                "transaction_ids": result.transaction_ref,
                "details": result.warnings
            }
        else:
            return {
                "success": False,
                "error": result.errors[0] if result.errors else "Unknown error"
            }

    except Exception as e:
        logger.error(f"Error creating stock transfer: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}

# =============================================================================
# SOP MODULE - Sales Order Processing
# =============================================================================
# Documentation: docs/opera-modules/sop/README.md
# =============================================================================

@app.get("/api/sop/documents")
async def get_sop_documents(
    status: str = Query("", description="Filter by status (Q=Quote, O=Order, D=Delivery, I=Invoice, C=Credit)"),
    account: str = Query("", description="Filter by customer account"),
    date_from: str = Query("", description="Start date (YYYY-MM-DD)"),
    date_to: str = Query("", description="End date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """
    List SOP documents (quotes, orders, deliveries, invoices, credits).
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        conditions = ["1=1"]

        if status:
            conditions.append(f"ih_docstat = '{status.replace(chr(39), chr(39)+chr(39))}'")
        if account:
            conditions.append(f"ih_account = '{account.replace(chr(39), chr(39)+chr(39))}'")
        if date_from:
            conditions.append(f"ih_date >= '{date_from}'")
        if date_to:
            conditions.append(f"ih_date <= '{date_to}'")

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                ih_doc AS document,
                RTRIM(ih_account) AS account,
                RTRIM(ih_name) AS customer_name,
                ih_date AS document_date,
                ih_docstat AS status,
                RTRIM(ih_sorder) AS sales_order,
                RTRIM(ih_invoice) AS invoice,
                RTRIM(ih_deliv) AS delivery,
                RTRIM(ih_credit) AS credit_note,
                RTRIM(ih_custref) AS customer_ref,
                ih_exvat AS net_value,
                ih_vat AS vat_value,
                (ih_exvat + ih_vat) AS gross_value,
                RTRIM(ih_loc) AS warehouse,
                RTRIM(ih_fcurr) AS currency
            FROM ihead
            WHERE {where_clause}
            ORDER BY ih_date DESC, ih_doc DESC
            OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"documents": [], "count": 0, "offset": offset, "limit": limit}

        documents = result.to_dict('records')

        status_map = {'Q': 'Quote', 'P': 'Proforma', 'O': 'Order', 'D': 'Delivery', 'I': 'Invoice', 'C': 'Credit', 'U': 'Undefined'}

        for doc in documents:
            doc['document'] = doc['document'].strip() if doc['document'] else ''
            doc['status_desc'] = status_map.get(doc.get('status', '').strip(), 'Unknown')
            if doc.get('document_date') and pd.notna(doc['document_date']):
                doc['document_date'] = str(doc['document_date'])[:10]
            else:
                doc['document_date'] = None

        count_query = f"SELECT COUNT(*) as cnt FROM ihead WHERE {where_clause}"
        count_result = sql_connector.execute_query(count_query)
        total_count = int(count_result.iloc[0]['cnt']) if count_result is not None and len(count_result) > 0 else 0

        return {"documents": documents, "count": total_count, "offset": offset, "limit": limit}

    except Exception as e:
        logger.error(f"Error fetching SOP documents: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sop/documents/{doc_number}")
async def get_sop_document_detail(doc_number: str):
    """
    Get SOP document detail with lines.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        # Get header
        header_query = f"""
            SELECT
                ih_doc AS document,
                RTRIM(ih_account) AS account,
                RTRIM(ih_name) AS customer_name,
                RTRIM(ih_addr1) AS address_1,
                RTRIM(ih_addr2) AS address_2,
                RTRIM(ih_addr3) AS address_3,
                RTRIM(ih_addr4) AS address_4,
                ih_date AS document_date,
                ih_docstat AS status,
                RTRIM(ih_sorder) AS sales_order,
                RTRIM(ih_invoice) AS invoice,
                RTRIM(ih_deliv) AS delivery,
                RTRIM(ih_credit) AS credit_note,
                RTRIM(ih_custref) AS customer_ref,
                ih_exvat AS net_value,
                ih_vat AS vat_value,
                ih_odisc AS overall_discount,
                RTRIM(ih_loc) AS warehouse,
                RTRIM(ih_fcurr) AS currency,
                ih_orddate AS order_date,
                ih_deldate AS delivery_date,
                ih_invdate AS invoice_date,
                RTRIM(ih_narr1) AS narrative_1,
                RTRIM(ih_narr2) AS narrative_2
            FROM ihead
            WHERE ih_doc = '{doc_number.replace(chr(39), chr(39)+chr(39))}'
        """

        header_result = sql_connector.execute_query(header_query)

        if header_result is None or len(header_result) == 0:
            raise HTTPException(status_code=404, detail=f"Document not found: {doc_number}")

        header = header_result.iloc[0].to_dict()
        for key in header:
            if isinstance(header[key], str):
                header[key] = header[key].strip()
            if key in ['document_date', 'order_date', 'delivery_date', 'invoice_date']:
                if header[key] and pd.notna(header[key]):
                    header[key] = str(header[key])[:10]
                else:
                    header[key] = None

        # Get lines
        lines_query = f"""
            SELECT
                il_recno AS line_number,
                RTRIM(il_stock) AS stock_ref,
                RTRIM(il_desc) AS description,
                il_quan AS quantity,
                il_sell AS unit_price,
                il_value AS line_value,
                il_disc AS discount_percent,
                il_cost AS cost_price,
                RTRIM(il_vatcode) AS vat_code,
                il_vatrate AS vat_rate,
                RTRIM(il_cwcode) AS warehouse,
                RTRIM(il_anal) AS analysis_code
            FROM iline
            WHERE il_doc = '{doc_number.replace(chr(39), chr(39)+chr(39))}'
            ORDER BY il_recno
        """

        lines_result = sql_connector.execute_query(lines_query)
        lines = []
        if lines_result is not None and len(lines_result) > 0:
            lines = lines_result.to_dict('records')
            for line in lines:
                for key in line:
                    if isinstance(line[key], str):
                        line[key] = line[key].strip()

        return {"header": header, "lines": lines}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching SOP document detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sop/orders/open")
async def get_open_orders(
    account: str = Query("", description="Filter by customer account"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """
    Get open sales orders (status = 'O').
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        conditions = ["ih_docstat = 'O'"]
        if account:
            conditions.append(f"ih_account = '{account.replace(chr(39), chr(39)+chr(39))}'")

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                ih_doc AS document,
                RTRIM(ih_account) AS account,
                RTRIM(ih_name) AS customer_name,
                ih_date AS document_date,
                ih_orddate AS order_date,
                RTRIM(ih_sorder) AS sales_order,
                RTRIM(ih_custref) AS customer_ref,
                ih_exvat AS net_value,
                ih_vat AS vat_value,
                RTRIM(ih_loc) AS warehouse
            FROM ihead
            WHERE {where_clause}
            ORDER BY ih_orddate, ih_doc
            OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"orders": [], "count": 0, "offset": offset, "limit": limit}

        orders = result.to_dict('records')
        for order in orders:
            order['document'] = order['document'].strip() if order['document'] else ''
            for key in ['document_date', 'order_date']:
                if order.get(key) and pd.notna(order[key]):
                    order[key] = str(order[key])[:10]
                else:
                    order[key] = None

        count_query = f"SELECT COUNT(*) as cnt FROM ihead WHERE {where_clause}"
        count_result = sql_connector.execute_query(count_query)
        total_count = int(count_result.iloc[0]['cnt']) if count_result is not None and len(count_result) > 0 else 0

        return {"orders": orders, "count": total_count, "offset": offset, "limit": limit}

    except Exception as e:
        logger.error(f"Error fetching open orders: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------------------------
# SOP Write Operations
# -----------------------------------------------------------------------------

@app.post("/api/sop/quotes")
async def create_sales_quote(
    customer_account: str = Query(..., description="Customer account code"),
    customer_ref: str = Query("", description="Customer's reference"),
    warehouse: str = Query("MAIN", description="Default warehouse"),
    expiry_days: int = Query(30, description="Days until quote expires"),
    notes: str = Query("", description="Notes/narration"),
    lines: str = Query(..., description="JSON array of line items: [{stock_ref, description, quantity, price, vat_code}]")
):
    """
    Create a new sales quote.

    Line items JSON format:
    ```json
    [
        {"stock_ref": "WIDGET001", "description": "Widget", "quantity": 10, "price": 99.99, "vat_code": "S"},
        {"description": "Service Item", "quantity": 1, "price": 500.00, "vat_code": "S"}
    ]
    ```
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        import json

        # Parse lines JSON
        try:
            line_items = json.loads(lines)
            if not isinstance(line_items, list):
                return {"success": False, "error": "Lines must be a JSON array"}
        except json.JSONDecodeError as je:
            return {"success": False, "error": f"Invalid JSON in lines: {je}"}

        opera_import = OperaSQLImport(sql_connector)
        result = opera_import.import_sales_quote(
            customer_account=customer_account.strip(),
            lines=line_items,
            customer_ref=customer_ref[:20] if customer_ref else "",
            warehouse=warehouse.strip(),
            expiry_days=expiry_days,
            notes=notes[:60] if notes else "",
            input_by="SQLRAG"
        )

        if result.success:
            return {
                "success": True,
                "quote_number": result.transaction_ref,
                "document_number": result.entry_number,
                "details": result.warnings
            }
        else:
            return {
                "success": False,
                "error": result.errors[0] if result.errors else "Unknown error"
            }

    except Exception as e:
        logger.error(f"Error creating sales quote: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}

@app.post("/api/sop/quotes/{document_no}/convert")
async def convert_quote_to_order(
    document_no: str,
    order_date: str = Query("", description="Order date (YYYY-MM-DD, defaults to today)")
):
    """
    Convert a quote to a sales order.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        from datetime import date as date_type

        # Parse date
        if order_date:
            try:
                parsed_date = date_type.fromisoformat(order_date[:10])
            except ValueError:
                return {"success": False, "error": f"Invalid date format: {order_date}"}
        else:
            parsed_date = None

        opera_import = OperaSQLImport(sql_connector)
        result = opera_import.convert_quote_to_order(
            document_no=document_no.strip(),
            order_date=parsed_date,
            input_by="SQLRAG"
        )

        if result.success:
            return {
                "success": True,
                "order_number": result.transaction_ref,
                "document_number": result.entry_number,
                "details": result.warnings
            }
        else:
            return {
                "success": False,
                "error": result.errors[0] if result.errors else "Unknown error"
            }

    except Exception as e:
        logger.error(f"Error converting quote to order: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}

@app.post("/api/sop/orders")
async def create_sales_order(
    customer_account: str = Query(..., description="Customer account code"),
    customer_ref: str = Query("", description="Customer's reference"),
    warehouse: str = Query("MAIN", description="Default warehouse"),
    auto_allocate: bool = Query(False, description="Automatically allocate available stock"),
    notes: str = Query("", description="Notes/narration"),
    lines: str = Query(..., description="JSON array of line items")
):
    """
    Create a new sales order directly (bypassing quote stage).
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        import json

        # Parse lines JSON
        try:
            line_items = json.loads(lines)
            if not isinstance(line_items, list):
                return {"success": False, "error": "Lines must be a JSON array"}
        except json.JSONDecodeError as je:
            return {"success": False, "error": f"Invalid JSON in lines: {je}"}

        opera_import = OperaSQLImport(sql_connector)
        result = opera_import.import_sales_order(
            customer_account=customer_account.strip(),
            lines=line_items,
            customer_ref=customer_ref[:20] if customer_ref else "",
            warehouse=warehouse.strip(),
            auto_allocate=auto_allocate,
            notes=notes[:60] if notes else "",
            input_by="SQLRAG"
        )

        if result.success:
            return {
                "success": True,
                "order_number": result.transaction_ref,
                "document_number": result.entry_number,
                "details": result.warnings
            }
        else:
            return {
                "success": False,
                "error": result.errors[0] if result.errors else "Unknown error"
            }

    except Exception as e:
        logger.error(f"Error creating sales order: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}

@app.post("/api/sop/orders/{document_no}/allocate")
async def allocate_order_stock(
    document_no: str,
    line_no: int = Query(None, description="Specific line number to allocate (all lines if omitted)")
):
    """
    Allocate available stock to an order's lines.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport

        opera_import = OperaSQLImport(sql_connector)
        result = opera_import.allocate_order_stock(
            document_no=document_no.strip(),
            line_no=line_no,
            input_by="SQLRAG"
        )

        if result.success:
            return {
                "success": True,
                "document_number": result.entry_number,
                "lines_processed": result.records_processed,
                "lines_allocated": result.records_imported,
                "details": result.warnings
            }
        else:
            return {
                "success": False,
                "error": result.errors[0] if result.errors else "Unknown error"
            }

    except Exception as e:
        logger.error(f"Error allocating stock: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}

@app.post("/api/sop/orders/{document_no}/invoice")
async def create_sales_invoice(
    document_no: str,
    invoice_date: str = Query("", description="Invoice date (YYYY-MM-DD, defaults to today)"),
    post_to_nominal: bool = Query(True, description="Post to nominal ledger"),
    issue_stock: bool = Query(True, description="Issue stock (reduce stock levels)")
):
    """
    Create an invoice from a sales order.

    This is the most complex SOP operation - creates records in:
    - stran (Sales Ledger)
    - snoml (Transfer File)
    - ntran (Nominal Ledger, if post_to_nominal=True)
    - nacnt (Balance updates)
    - zvtran (VAT Analysis)
    - nvat (VAT Return Tracking)
    - ctran (Stock transactions, if issue_stock=True)
    - cstwh/cname (Stock levels)
    - sname (Customer balance)

    Company options (iparm) are respected for stock update behavior.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        from datetime import date as date_type

        # Parse date
        if invoice_date:
            try:
                parsed_date = date_type.fromisoformat(invoice_date[:10])
            except ValueError:
                return {"success": False, "error": f"Invalid date format: {invoice_date}"}
        else:
            parsed_date = None

        opera_import = OperaSQLImport(sql_connector)
        result = opera_import.invoice_sales_order(
            document_no=document_no.strip(),
            invoice_date=parsed_date,
            post_to_nominal=post_to_nominal,
            issue_stock=issue_stock,
            input_by="SQLRAG"
        )

        if result.success:
            return {
                "success": True,
                "invoice_number": result.transaction_ref,
                "document_number": result.entry_number,
                "details": result.warnings
            }
        else:
            return {
                "success": False,
                "error": result.errors[0] if result.errors else "Unknown error"
            }

    except Exception as e:
        logger.error(f"Error creating invoice: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}

@app.get("/api/sop/customers")
async def get_sop_customers(
    search: str = Query("", description="Search by account or name"),
    limit: int = Query(50, ge=1, le=200)
):
    """
    Get customer accounts for SOP order entry.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        search_condition = ""
        if search:
            safe_search = search.replace("'", "''")
            safe_nospace = safe_search.replace(" ", "")
            search_condition = f"""AND (
                sn_account LIKE '%{safe_search}%'
                OR UPPER(sn_name) LIKE UPPER('%{safe_search}%')
                OR UPPER(REPLACE(sn_name, ' ', '')) LIKE UPPER('%{safe_nospace}%')
            )"""

        query = f"""
            SELECT TOP {limit}
                RTRIM(sn_account) AS account,
                RTRIM(sn_name) AS name,
                RTRIM(sn_addr1) AS address1,
                RTRIM(sn_pstcode) AS postcode,
                sn_currbal AS balance
            FROM sname
            WHERE sn_account != '' {search_condition}
            ORDER BY sn_name
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"customers": []}

        customers = result.to_dict('records')
        return {"customers": customers}

    except Exception as e:
        logger.error(f"Error fetching customers: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# POP MODULE - Purchase Order Processing
# =============================================================================
# Documentation: docs/opera-modules/pop/README.md
# =============================================================================

@app.get("/api/pop/orders")
async def get_purchase_orders(
    account: str = Query("", description="Filter by supplier account"),
    status: str = Query("", description="Filter: open, cancelled, all"),
    date_from: str = Query("", description="Start date (YYYY-MM-DD)"),
    date_to: str = Query("", description="End date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """
    List purchase orders from dohead table.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        conditions = ["1=1"]

        if account:
            conditions.append(f"dc_account = '{account.replace(chr(39), chr(39)+chr(39))}'")
        if status == 'open':
            conditions.append("(dc_cancel = 0 OR dc_cancel IS NULL)")
        elif status == 'cancelled':
            conditions.append("dc_cancel = 1")
        if date_from:
            conditions.append(f"sq_crdate >= '{date_from}'")
        if date_to:
            conditions.append(f"sq_crdate <= '{date_to}'")

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                dc_ref AS po_number,
                RTRIM(dc_account) AS supplier_account,
                RTRIM(pn.pn_name) AS supplier_name,
                dc_totval AS total_value,
                dc_odisc AS overall_discount,
                RTRIM(dc_cwcode) AS warehouse,
                dc_cancel AS is_cancelled,
                dc_printed AS is_printed,
                RTRIM(dc_currcy) AS currency,
                sq_crdate AS created_date,
                RTRIM(dc_ref2) AS reference
            FROM dohead
            LEFT JOIN pname WITH (NOLOCK) pn ON dc_account = pn.pn_account
            WHERE {where_clause}
            ORDER BY sq_crdate DESC, dc_ref DESC
            OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"orders": [], "count": 0, "offset": offset, "limit": limit}

        orders = result.to_dict('records')
        for order in orders:
            order['po_number'] = order['po_number'].strip() if order['po_number'] else ''
            order['is_cancelled'] = bool(order.get('is_cancelled'))
            order['is_printed'] = bool(order.get('is_printed'))
            if order.get('created_date') and pd.notna(order['created_date']):
                order['created_date'] = str(order['created_date'])[:10]
            else:
                order['created_date'] = None

        count_query = f"SELECT COUNT(*) as cnt FROM dohead WHERE {where_clause}"
        count_result = sql_connector.execute_query(count_query)
        total_count = int(count_result.iloc[0]['cnt']) if count_result is not None and len(count_result) > 0 else 0

        return {"orders": orders, "count": total_count, "offset": offset, "limit": limit}

    except Exception as e:
        logger.error(f"Error fetching purchase orders: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pop/orders/{po_number}")
async def get_purchase_order_detail(po_number: str):
    """
    Get purchase order detail with lines.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        # Get header
        header_query = f"""
            SELECT
                dc_ref AS po_number,
                RTRIM(dc_account) AS supplier_account,
                RTRIM(pn.pn_name) AS supplier_name,
                dc_totval AS total_value,
                dc_odisc AS overall_discount,
                RTRIM(dc_cwcode) AS warehouse,
                RTRIM(dc_delnam) AS delivery_name,
                RTRIM(dc_delad1) AS delivery_address_1,
                RTRIM(dc_delad2) AS delivery_address_2,
                RTRIM(dc_delad3) AS delivery_address_3,
                RTRIM(dc_delad4) AS delivery_address_4,
                RTRIM(dc_deladpc) AS delivery_postcode,
                RTRIM(dc_contact) AS contact,
                dc_cancel AS is_cancelled,
                dc_printed AS is_printed,
                RTRIM(dc_currcy) AS currency,
                dc_exrate AS exchange_rate,
                sq_crdate AS created_date,
                RTRIM(dc_ref2) AS reference,
                RTRIM(dc_narr1) AS narrative_1,
                RTRIM(dc_narr2) AS narrative_2
            FROM dohead
            LEFT JOIN pname WITH (NOLOCK) pn ON dc_account = pn.pn_account
            WHERE dc_ref = '{po_number.replace(chr(39), chr(39)+chr(39))}'
        """

        header_result = sql_connector.execute_query(header_query)

        if header_result is None or len(header_result) == 0:
            raise HTTPException(status_code=404, detail=f"Purchase order not found: {po_number}")

        header = header_result.iloc[0].to_dict()
        header['po_number'] = header['po_number'].strip() if header['po_number'] else ''
        header['is_cancelled'] = bool(header.get('is_cancelled'))
        header['is_printed'] = bool(header.get('is_printed'))
        if header.get('created_date') and pd.notna(header['created_date']):
            header['created_date'] = str(header['created_date'])[:10]
        else:
            header['created_date'] = None

        # Get lines
        lines_query = f"""
            SELECT
                do_dcline AS line_number,
                RTRIM(do_cnref) AS stock_ref,
                RTRIM(do_supref) AS supplier_ref,
                RTRIM(do_desc) AS description,
                do_reqqty AS quantity,
                do_price AS unit_price,
                do_value AS line_value,
                do_discp AS discount_percent,
                RTRIM(do_cwcode) AS warehouse,
                do_reqdat AS required_date,
                RTRIM(do_ledger) AS ledger_account,
                RTRIM(do_jcstdoc) AS job_number,
                RTRIM(do_jphase) AS job_phase
            FROM doline
            WHERE do_dcref = '{po_number.replace(chr(39), chr(39)+chr(39))}'
            ORDER BY do_dcline
        """

        lines_result = sql_connector.execute_query(lines_query)
        lines = []
        if lines_result is not None and len(lines_result) > 0:
            lines = lines_result.to_dict('records')
            for line in lines:
                for key in line:
                    if isinstance(line[key], str):
                        line[key] = line[key].strip()
                if line.get('required_date') and pd.notna(line['required_date']):
                    line['required_date'] = str(line['required_date'])[:10]
                else:
                    line['required_date'] = None

        return {"header": header, "lines": lines}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching PO detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pop/grns")
async def get_grns(
    account: str = Query("", description="Filter by supplier account"),
    date_from: str = Query("", description="Start date (YYYY-MM-DD)"),
    date_to: str = Query("", description="End date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """
    List Goods Received Notes from cghead table.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        conditions = ["1=1"]
        if date_from:
            conditions.append(f"ch_date >= '{date_from}'")
        if date_to:
            conditions.append(f"ch_date <= '{date_to}'")

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                ch_ref AS grn_number,
                ch_date AS grn_date,
                RTRIM(ch_dref) AS delivery_ref,
                ch_delchg AS delivery_charge,
                ch_vat AS vat_on_delivery,
                RTRIM(ch_user) AS received_by,
                ch_status AS status,
                sq_crdate AS created_date
            FROM cghead
            WHERE {where_clause}
            ORDER BY ch_date DESC, ch_ref DESC
            OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"grns": [], "count": 0, "offset": offset, "limit": limit}

        grns = result.to_dict('records')
        for grn in grns:
            grn['grn_number'] = grn['grn_number'].strip() if grn['grn_number'] else ''
            for key in ['grn_date', 'created_date']:
                if grn.get(key) and pd.notna(grn[key]):
                    grn[key] = str(grn[key])[:10]
                else:
                    grn[key] = None

        count_query = f"SELECT COUNT(*) as cnt FROM cghead WHERE {where_clause}"
        count_result = sql_connector.execute_query(count_query)
        total_count = int(count_result.iloc[0]['cnt']) if count_result is not None and len(count_result) > 0 else 0

        return {"grns": grns, "count": total_count, "offset": offset, "limit": limit}

    except Exception as e:
        logger.error(f"Error fetching GRNs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pop/grns/{grn_number}")
async def get_grn_detail(grn_number: str):
    """
    Get GRN detail with lines.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        # Get header
        header_query = f"""
            SELECT
                ch_ref AS grn_number,
                ch_date AS grn_date,
                RTRIM(ch_dref) AS delivery_ref,
                ch_delchg AS delivery_charge,
                ch_vat AS vat_on_delivery,
                RTRIM(ch_user) AS received_by,
                ch_status AS status,
                sq_crdate AS created_date,
                ch_time AS grn_time
            FROM cghead
            WHERE ch_ref = '{grn_number.replace(chr(39), chr(39)+chr(39))}'
        """

        header_result = sql_connector.execute_query(header_query)

        if header_result is None or len(header_result) == 0:
            raise HTTPException(status_code=404, detail=f"GRN not found: {grn_number}")

        header = header_result.iloc[0].to_dict()
        header['grn_number'] = header['grn_number'].strip() if header['grn_number'] else ''
        for key in ['grn_date', 'created_date']:
            if header.get(key) and pd.notna(header[key]):
                header[key] = str(header[key])[:10]
            else:
                header[key] = None

        # Get lines
        lines_query = f"""
            SELECT
                ci_line AS line_number,
                RTRIM(ci_account) AS supplier_account,
                RTRIM(pn.pn_name) AS supplier_name,
                RTRIM(ci_cnref) AS stock_ref,
                RTRIM(ci_supref) AS supplier_ref,
                RTRIM(ci_desc) AS description,
                ci_qtyrcv AS quantity_received,
                ci_qtyrel AS quantity_released,
                ci_qtyret AS quantity_returned,
                ci_qtymat AS quantity_matched,
                ci_cost AS unit_cost,
                ci_value AS line_value,
                RTRIM(ci_bkware) AS warehouse,
                RTRIM(ci_dcref) AS po_reference,
                ci_dcline AS po_line
            FROM cgline
            LEFT JOIN pname WITH (NOLOCK) pn ON ci_account = pn.pn_account
            WHERE ci_chref = '{grn_number.replace(chr(39), chr(39)+chr(39))}'
            ORDER BY ci_line
        """

        lines_result = sql_connector.execute_query(lines_query)
        lines = []
        if lines_result is not None and len(lines_result) > 0:
            lines = lines_result.to_dict('records')
            for line in lines:
                for key in line:
                    if isinstance(line[key], str):
                        line[key] = line[key].strip()

        return {"header": header, "lines": lines}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching GRN detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------------------------
# POP Write Operations
# -----------------------------------------------------------------------------

@app.get("/api/pop/suppliers")
async def get_suppliers_for_pop(
    search: str = Query("", description="Search term (account or name)")
):
    """
    Search suppliers for PO entry.
    Returns matching suppliers from pname table.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        search_term = search.replace("'", "''")
        query = f"""
            SELECT TOP 20
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS name,
                RTRIM(pn_addr1) AS address1,
                RTRIM(pn_pstcode) AS postcode,
                RTRIM(pn_teleno) AS phone
            FROM pname WITH (NOLOCK)
            WHERE (pn_account LIKE '%{search_term}%' OR pn_name LIKE '%{search_term}%')
            ORDER BY pn_name
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"suppliers": []}

        suppliers = result.to_dict('records')
        return {"suppliers": suppliers}

    except Exception as e:
        logger.error(f"Error searching suppliers: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/pop/orders")
async def create_purchase_order(
    supplier_account: str = Query(..., description="Supplier account code"),
    lines: str = Query(..., description="JSON array of lines"),
    warehouse: str = Query("", description="Default warehouse"),
    reference: str = Query("", description="External reference"),
    narrative: str = Query("", description="PO narrative"),
    po_date: str = Query("", description="PO date YYYY-MM-DD (default today)")
):
    """
    Create a new purchase order.

    Lines JSON format:
    [
        {
            "stock_ref": "WIDGET001",
            "supplier_ref": "SUP-REF",
            "description": "Widget",
            "quantity": 10,
            "unit_price": 5.99,
            "discount_percent": 0,
            "warehouse": "MAIN"
        }
    ]
    """
    global sql_connector, opera_import

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")
    if not opera_import:
        raise HTTPException(status_code=500, detail="Opera import not initialized")

    try:
        # Parse lines JSON
        try:
            lines_data = json.loads(lines)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Invalid lines JSON: {e}")

        if not lines_data or len(lines_data) == 0:
            raise HTTPException(status_code=400, detail="At least one line is required")

        # Parse date
        order_date = None
        if po_date:
            try:
                order_date = datetime.strptime(po_date, '%Y-%m-%d').date()
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

        # Create PO
        result = opera_import.import_purchase_order(
            supplier_account=supplier_account,
            lines=lines_data,
            po_date=order_date,
            warehouse=warehouse or None,
            reference=reference,
            narrative=narrative
        )

        if not result.success:
            raise HTTPException(status_code=400, detail=result.errors[0] if result.errors else "Failed to create PO")

        return {
            "success": True,
            "po_number": result.transaction_ref,
            "message": f"Purchase order {result.transaction_ref} created successfully",
            "details": result.warnings
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating purchase order: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/pop/orders/{po_number}/receive")
async def receive_purchase_order(
    po_number: str,
    lines: str = Query("", description="JSON array of lines to receive (optional - receives all if empty)"),
    delivery_ref: str = Query("", description="Carrier delivery reference"),
    grn_date: str = Query("", description="GRN date YYYY-MM-DD (default today)")
):
    """
    Receive goods against a purchase order.

    If lines is empty, receives all outstanding quantities.

    Lines JSON format (optional):
    [
        {
            "line_number": 1,
            "quantity": 10,
            "unit_cost": 5.50  // optional cost override
        }
    ]
    """
    global sql_connector, opera_import

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")
    if not opera_import:
        raise HTTPException(status_code=500, detail="Opera import not initialized")

    try:
        # Parse lines JSON if provided
        lines_to_receive = None
        if lines and lines.strip():
            try:
                lines_to_receive = json.loads(lines)
            except json.JSONDecodeError as e:
                raise HTTPException(status_code=400, detail=f"Invalid lines JSON: {e}")

        # Parse date
        receive_date = None
        if grn_date:
            try:
                receive_date = datetime.strptime(grn_date, '%Y-%m-%d').date()
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

        # Receive PO
        result = opera_import.receive_po_lines(
            po_number=po_number,
            lines_to_receive=lines_to_receive,
            grn_date=receive_date,
            delivery_ref=delivery_ref
        )

        if not result.success:
            raise HTTPException(status_code=400, detail=result.errors[0] if result.errors else "Failed to receive PO")

        return {
            "success": True,
            "grn_number": result.transaction_ref,
            "message": f"GRN {result.transaction_ref} created for PO {po_number}",
            "details": result.warnings
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error receiving purchase order: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/pop/grns")
async def create_grn(
    lines: str = Query(..., description="JSON array of GRN lines"),
    delivery_ref: str = Query("", description="Carrier delivery reference"),
    grn_date: str = Query("", description="GRN date YYYY-MM-DD (default today)"),
    update_stock: bool = Query(True, description="Update stock levels")
):
    """
    Create a Goods Received Note (ad-hoc, not linked to PO).

    Lines JSON format:
    [
        {
            "stock_ref": "WIDGET001",
            "supplier_account": "SUP001",
            "supplier_ref": "SUP-REF",
            "description": "Widget",
            "quantity": 10,
            "unit_cost": 5.50,
            "warehouse": "MAIN"
        }
    ]
    """
    global sql_connector, opera_import

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")
    if not opera_import:
        raise HTTPException(status_code=500, detail="Opera import not initialized")

    try:
        # Parse lines JSON
        try:
            lines_data = json.loads(lines)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Invalid lines JSON: {e}")

        if not lines_data or len(lines_data) == 0:
            raise HTTPException(status_code=400, detail="At least one line is required")

        # Parse date
        receive_date = None
        if grn_date:
            try:
                receive_date = datetime.strptime(grn_date, '%Y-%m-%d').date()
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

        # Create GRN
        result = opera_import.create_grn(
            lines=lines_data,
            grn_date=receive_date,
            delivery_ref=delivery_ref,
            update_stock=update_stock
        )

        if not result.success:
            raise HTTPException(status_code=400, detail=result.errors[0] if result.errors else "Failed to create GRN")

        return {
            "success": True,
            "grn_number": result.transaction_ref,
            "message": f"GRN {result.transaction_ref} created successfully",
            "details": result.warnings
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating GRN: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pop/orders/{po_number}/outstanding")
async def get_po_outstanding(po_number: str):
    """
    Get outstanding lines on a purchase order (not fully received).
    Useful for partial receipts.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        # Get PO header
        header_query = f"""
            SELECT dc_ref, RTRIM(dc_account) AS supplier_account,
                   RTRIM(pn.pn_name) AS supplier_name, dc_cancel
            FROM dohead
            LEFT JOIN pname WITH (NOLOCK) pn ON dc_account = pn.pn_account
            WHERE dc_ref = '{po_number.replace(chr(39), chr(39)+chr(39))}'
        """

        header_result = sql_connector.execute_query(header_query)
        if header_result is None or len(header_result) == 0:
            raise HTTPException(status_code=404, detail=f"Purchase order not found: {po_number}")

        header = header_result.iloc[0]
        if header.get('dc_cancel'):
            raise HTTPException(status_code=400, detail=f"Purchase order {po_number} is cancelled")

        # Get outstanding lines
        lines_query = f"""
            SELECT
                do_dcline AS line_number,
                RTRIM(do_cnref) AS stock_ref,
                RTRIM(do_supref) AS supplier_ref,
                RTRIM(do_desc) AS description,
                RTRIM(do_cwcode) AS warehouse,
                do_reqqty AS quantity_ordered,
                do_recqty AS quantity_received,
                (do_reqqty - ISNULL(do_recqty, 0)) AS quantity_outstanding,
                do_price AS unit_price
            FROM doline
            WHERE do_dcref = '{po_number.replace(chr(39), chr(39)+chr(39))}'
              AND (do_reqqty - ISNULL(do_recqty, 0)) > 0
            ORDER BY do_dcline
        """

        lines_result = sql_connector.execute_query(lines_query)
        lines = []
        if lines_result is not None and len(lines_result) > 0:
            lines = lines_result.to_dict('records')
            for line in lines:
                for key in line:
                    if isinstance(line[key], str):
                        line[key] = line[key].strip()

        return {
            "po_number": po_number,
            "supplier_account": header['supplier_account'],
            "supplier_name": header['supplier_name'],
            "outstanding_lines": lines,
            "total_lines_outstanding": len(lines)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching PO outstanding: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# BOM MODULE - Bill of Materials / Works Orders
# =============================================================================
# Documentation: docs/opera-modules/bom/README.md
# =============================================================================

@app.get("/api/bom/assemblies")
async def get_assemblies(
    search: str = Query("", description="Search by assembly reference or description"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """
    List assembly structures - products that have components defined.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        conditions = ["1=1"]
        if search:
            search_term = search.replace("'", "''")
            conditions.append(f"(cv_assembl LIKE '%{search_term}%' OR cn.cn_desc LIKE '%{search_term}%')")

        where_clause = " AND ".join(conditions)

        # Get distinct assemblies from cstruc
        query = f"""
            SELECT DISTINCT
                cv_assembl AS assembly_ref,
                RTRIM(cn.cn_desc) AS description,
                RTRIM(cn.cn_catag) AS category,
                cn.cn_cost AS cost_price,
                cn.cn_sell AS sell_price,
                (SELECT COUNT(*) FROM cstruc cs2 WHERE cs2.cv_assembl = cstruc.cv_assembl) AS component_count
            FROM cstruc
            LEFT JOIN cname cn ON cstruc.cv_assembl = cn.cn_ref
            WHERE {where_clause}
            ORDER BY cv_assembl
            OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"assemblies": [], "count": 0, "offset": offset, "limit": limit}

        assemblies = result.to_dict('records')
        for asm in assemblies:
            asm['assembly_ref'] = asm['assembly_ref'].strip() if asm['assembly_ref'] else ''

        count_query = f"SELECT COUNT(DISTINCT cv_assembl) as cnt FROM cstruc LEFT JOIN cname cn ON cstruc.cv_assembl = cn.cn_ref WHERE {where_clause}"
        count_result = sql_connector.execute_query(count_query)
        total_count = int(count_result.iloc[0]['cnt']) if count_result is not None and len(count_result) > 0 else 0

        return {"assemblies": assemblies, "count": total_count, "offset": offset, "limit": limit}

    except Exception as e:
        logger.error(f"Error fetching assemblies: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/bom/assemblies/{assembly_ref}")
async def get_assembly_structure(assembly_ref: str):
    """
    Get assembly structure (bill of materials) - list of components.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        # Get assembly details
        assembly_query = f"""
            SELECT
                cn_ref AS ref,
                RTRIM(cn_desc) AS description,
                RTRIM(cn_catag) AS category,
                RTRIM(cn_fact) AS profile,
                cn_cost AS cost_price,
                cn_sell AS sell_price
            FROM cname
            WHERE cn_ref = '{assembly_ref.replace(chr(39), chr(39)+chr(39))}'
        """

        assembly_result = sql_connector.execute_query(assembly_query)
        if assembly_result is None or len(assembly_result) == 0:
            raise HTTPException(status_code=404, detail=f"Assembly not found: {assembly_ref}")

        assembly = assembly_result.iloc[0].to_dict()
        assembly['ref'] = assembly['ref'].strip() if assembly['ref'] else ''

        # Get components
        components_query = f"""
            SELECT
                cv_compone AS component_ref,
                RTRIM(cn.cn_desc) AS description,
                cv_coquant AS quantity,
                cv_seqno AS sequence,
                cv_subassm AS is_sub_assembly,
                cv_phassm AS is_phantom,
                RTRIM(cv_loc) AS warehouse,
                cn.cn_cost AS cost_price,
                cn.cn_instock AS in_stock,
                cn.cn_freest AS free_stock
            FROM cstruc
            LEFT JOIN cname cn ON cstruc.cv_compone = cn.cn_ref
            WHERE cv_assembl = '{assembly_ref.replace(chr(39), chr(39)+chr(39))}'
            ORDER BY cv_seqno, cv_compone
        """

        components_result = sql_connector.execute_query(components_query)
        components = []
        if components_result is not None and len(components_result) > 0:
            components = components_result.to_dict('records')
            for comp in components:
                comp['component_ref'] = comp['component_ref'].strip() if comp['component_ref'] else ''
                comp['is_sub_assembly'] = bool(comp.get('is_sub_assembly'))
                comp['is_phantom'] = bool(comp.get('is_phantom'))

        return {"assembly": assembly, "components": components}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching assembly structure: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/bom/works-orders")
async def get_works_orders(
    status: str = Query("", description="Filter by status"),
    assembly: str = Query("", description="Filter by assembly reference"),
    date_from: str = Query("", description="Start date (YYYY-MM-DD)"),
    date_to: str = Query("", description="End date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """
    List works orders from chead table.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        conditions = ["1=1"]

        if assembly:
            conditions.append(f"cx_assembl = '{assembly.replace(chr(39), chr(39)+chr(39))}'")
        if status:
            conditions.append(f"cx_wostat = '{status.replace(chr(39), chr(39)+chr(39))}'")
        if date_from:
            conditions.append(f"cx_orddate >= '{date_from}'")
        if date_to:
            conditions.append(f"cx_orddate <= '{date_to}'")

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                cx_ref AS works_order,
                RTRIM(cx_assembl) AS assembly_ref,
                RTRIM(cx_desc) AS description,
                cx_ordqty AS quantity_ordered,
                cx_madeqty AS quantity_made,
                cx_wipqty AS quantity_wip,
                cx_alloqty AS quantity_allocated,
                cx_orddate AS order_date,
                cx_duedate AS due_date,
                cx_cmpdate AS completed_date,
                cx_wostat AS status,
                cx_cancel AS is_cancelled,
                RTRIM(cx_cwcode) AS warehouse,
                cx_totval AS total_value,
                cx_matval AS material_value,
                cx_labval AS labour_value,
                RTRIM(cx_soref) AS sales_order_ref,
                RTRIM(cx_jcstdoc) AS job_number
            FROM chead
            WHERE {where_clause}
            ORDER BY cx_orddate DESC, cx_ref DESC
            OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"works_orders": [], "count": 0, "offset": offset, "limit": limit}

        works_orders = result.to_dict('records')
        for wo in works_orders:
            wo['works_order'] = wo['works_order'].strip() if wo['works_order'] else ''
            wo['is_cancelled'] = bool(wo.get('is_cancelled'))
            for key in ['order_date', 'due_date', 'completed_date']:
                if wo.get(key) and pd.notna(wo[key]):
                    wo[key] = str(wo[key])[:10]
                else:
                    wo[key] = None

        count_query = f"SELECT COUNT(*) as cnt FROM chead WHERE {where_clause}"
        count_result = sql_connector.execute_query(count_query)
        total_count = int(count_result.iloc[0]['cnt']) if count_result is not None and len(count_result) > 0 else 0

        return {"works_orders": works_orders, "count": total_count, "offset": offset, "limit": limit}

    except Exception as e:
        logger.error(f"Error fetching works orders: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/bom/works-orders/{wo_number}")
async def get_works_order_detail(wo_number: str):
    """
    Get works order detail with component lines.
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        # Get header
        header_query = f"""
            SELECT
                cx_ref AS works_order,
                RTRIM(cx_assembl) AS assembly_ref,
                RTRIM(cx_desc) AS description,
                cx_ordqty AS quantity_ordered,
                cx_madeqty AS quantity_made,
                cx_wipqty AS quantity_wip,
                cx_alloqty AS quantity_allocated,
                cx_discqty AS quantity_discarded,
                cx_orddate AS order_date,
                cx_duedate AS due_date,
                cx_cmpdate AS completed_date,
                cx_wostat AS status,
                cx_cancel AS is_cancelled,
                RTRIM(cx_cwcode) AS warehouse,
                cx_totval AS total_value,
                cx_matval AS material_value,
                cx_labval AS labour_value,
                cx_matcost AS material_cost,
                cx_labcost AS labour_cost,
                cx_price AS assembly_cost_price,
                RTRIM(cx_soref) AS sales_order_ref,
                RTRIM(cx_saleacc) AS sales_account,
                RTRIM(cx_cusnam) AS customer_name,
                RTRIM(cx_jcstdoc) AS job_number,
                RTRIM(cx_jphase) AS job_phase
            FROM chead
            WHERE cx_ref = '{wo_number.replace(chr(39), chr(39)+chr(39))}'
        """

        header_result = sql_connector.execute_query(header_query)

        if header_result is None or len(header_result) == 0:
            raise HTTPException(status_code=404, detail=f"Works order not found: {wo_number}")

        header = header_result.iloc[0].to_dict()
        header['works_order'] = header['works_order'].strip() if header['works_order'] else ''
        header['is_cancelled'] = bool(header.get('is_cancelled'))
        for key in ['order_date', 'due_date', 'completed_date']:
            if header.get(key) and pd.notna(header[key]):
                header[key] = str(header[key])[:10]
            else:
                header[key] = None

        # Get component lines
        lines_query = f"""
            SELECT
                cy_lineno AS line_number,
                RTRIM(cy_cnref) AS component_ref,
                RTRIM(cy_desc) AS description,
                cy_reqqty AS quantity_required,
                cy_alloqty AS quantity_allocated,
                cy_wipqty AS quantity_wip,
                cy_cmplqty AS quantity_completed,
                cy_fromst AS quantity_from_stock,
                cy_tomake AS quantity_to_make,
                RTRIM(cy_cwcode) AS warehouse,
                cy_price AS price,
                cy_value AS line_value,
                cy_labour AS is_labour,
                cy_stock AS is_stocked,
                cy_subassm AS sub_assembly_flag,
                cy_phassm AS is_phantom,
                cy_issdate AS issue_date
            FROM cline
            WHERE cy_cxref = '{wo_number.replace(chr(39), chr(39)+chr(39))}'
            ORDER BY cy_lineno
        """

        lines_result = sql_connector.execute_query(lines_query)
        lines = []
        if lines_result is not None and len(lines_result) > 0:
            lines = lines_result.to_dict('records')
            for line in lines:
                for key in line:
                    if isinstance(line[key], str):
                        line[key] = line[key].strip()
                line['is_labour'] = bool(line.get('is_labour'))
                line['is_stocked'] = bool(line.get('is_stocked'))
                line['is_phantom'] = bool(line.get('is_phantom'))
                if line.get('issue_date') and pd.notna(line['issue_date']):
                    line['issue_date'] = str(line['issue_date'])[:10]
                else:
                    line['issue_date'] = None

        return {"header": header, "lines": lines}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching works order detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/bom/where-used/{component_ref}")
async def get_where_used(component_ref: str):
    """
    Find all assemblies that use a specific component (where-used enquiry).
    """
    global sql_connector

    if not sql_connector:
        raise HTTPException(status_code=500, detail="Database not connected")

    try:
        query = f"""
            SELECT
                cv_assembl AS assembly_ref,
                RTRIM(cn.cn_desc) AS assembly_description,
                cv_coquant AS quantity_per_assembly,
                cv_seqno AS sequence
            FROM cstruc
            LEFT JOIN cname cn ON cstruc.cv_assembl = cn.cn_ref
            WHERE cv_compone = '{component_ref.replace(chr(39), chr(39)+chr(39))}'
            ORDER BY cv_assembl
        """

        result = sql_connector.execute_query(query)

        if result is None or len(result) == 0:
            return {"component_ref": component_ref, "used_in": []}

        used_in = result.to_dict('records')
        for item in used_in:
            item['assembly_ref'] = item['assembly_ref'].strip() if item['assembly_ref'] else ''

        return {"component_ref": component_ref, "used_in": used_in}

    except Exception as e:
        logger.error(f"Error fetching where-used: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
