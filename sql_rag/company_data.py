"""
Per-Company Data Directory Management

Manages isolated data directories for each Opera installation/company.
Each company gets its own set of SQLite databases and ChromaDB directory
so that switching between installations preserves learned data.

Directory structure:
    data/{company_id}/
        bank_patterns.db
        bank_aliases.db
        supplier_statements.db
        email_data.db
        gocardless_payments.db
        pdf_extraction_cache.db
        import_locks.db
        lock_monitor.db
        chroma_db/

Shared (stays in project root):
    users.db — cross-company authentication
"""

import logging
import shutil
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Base data directory (project_root/data/)
BASE_DATA_DIR = Path(__file__).parent.parent / "data"

# Project root for locating legacy root-level databases
PROJECT_ROOT = Path(__file__).parent.parent

# Current active company ID (set by api/main.py on startup and company switch)
_current_company_id: Optional[str] = None

# Databases that should be per-company (all except users.db)
PER_COMPANY_DATABASES = [
    "bank_patterns.db",
    "bank_aliases.db",
    "supplier_statements.db",
    "email_data.db",
    "gocardless_payments.db",
    "pdf_extraction_cache.db",
    "import_locks.db",
    "lock_monitor.db",
]

# Databases that stay shared in project root
SHARED_DATABASES = [
    "users.db",
]


def get_current_company_id() -> Optional[str]:
    """Return the currently active company ID."""
    return _current_company_id


def set_current_company_id(company_id: str):
    """Set the currently active company ID. Called on startup and company switch."""
    global _current_company_id
    _current_company_id = company_id
    logger.info(f"Active company set to: {company_id}")


def get_company_data_dir(company_id: str) -> Path:
    """
    Return the data directory for a company, creating it if needed.

    Args:
        company_id: Company identifier (e.g., 'intsys', 'cloudsis', 'z_demo')

    Returns:
        Path to data/{company_id}/
    """
    data_dir = BASE_DATA_DIR / company_id
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def get_company_db_path(company_id: str, db_name: str) -> Path:
    """
    Return the path to a specific database for a company.

    Args:
        company_id: Company identifier
        db_name: Database filename (e.g., 'bank_patterns.db')

    Returns:
        Path to data/{company_id}/{db_name}
    """
    data_dir = get_company_data_dir(company_id)
    return data_dir / db_name


def get_company_chroma_dir(company_id: str) -> Path:
    """
    Return the ChromaDB persist directory for a company.

    Args:
        company_id: Company identifier

    Returns:
        Path to data/{company_id}/chroma_db/
    """
    data_dir = get_company_data_dir(company_id)
    chroma_dir = data_dir / "chroma_db"
    chroma_dir.mkdir(parents=True, exist_ok=True)
    return chroma_dir


def get_current_db_path(db_name: str) -> Optional[Path]:
    """
    Return the path to a database for the currently active company.

    Returns None if no company is set (caller should fall back to legacy path).

    Args:
        db_name: Database filename (e.g., 'bank_patterns.db')

    Returns:
        Path to data/{current_company}/{db_name}, or None if no company set
    """
    if _current_company_id is None:
        return None
    return get_company_db_path(_current_company_id, db_name)


def migrate_root_databases(company_id: str):
    """
    One-time migration: copy root-level .db files into data/{company_id}/
    if they don't already exist there.

    This preserves existing learned data when first switching to per-company
    directories. Root-level files are NOT deleted (kept as backups).

    Args:
        company_id: Company identifier to migrate data for
    """
    data_dir = get_company_data_dir(company_id)
    migrated = []

    for db_name in PER_COMPANY_DATABASES:
        source = PROJECT_ROOT / db_name
        dest = data_dir / db_name

        if source.exists() and not dest.exists():
            try:
                shutil.copy2(str(source), str(dest))
                migrated.append(db_name)
                logger.info(f"Migrated {db_name} to {dest}")
            except Exception as e:
                logger.error(f"Failed to migrate {db_name}: {e}")

    # Also migrate chroma_db directory if it exists at root
    source_chroma = PROJECT_ROOT / "chroma_db"
    dest_chroma = data_dir / "chroma_db"

    if source_chroma.exists() and source_chroma.is_dir() and not dest_chroma.exists():
        try:
            shutil.copytree(str(source_chroma), str(dest_chroma))
            migrated.append("chroma_db/")
            logger.info(f"Migrated chroma_db/ to {dest_chroma}")
        except Exception as e:
            logger.error(f"Failed to migrate chroma_db/: {e}")

    if migrated:
        logger.info(f"Migration complete for {company_id}: {', '.join(migrated)}")
    else:
        logger.info(f"No migration needed for {company_id} (all files already present or no root files)")


def detect_company_from_config(config) -> str:
    """
    Determine the current company ID from config.ini database name.

    Searches companies/*.json for a matching database field.
    Falls back to 'default' if no match found.

    Args:
        config: ConfigParser instance with database settings

    Returns:
        Company ID string
    """
    import json
    import os

    if not config or not config.has_option("database", "database"):
        return "default"

    db_name = config.get("database", "database")
    companies_dir = PROJECT_ROOT / "companies"

    if companies_dir.exists():
        for filename in os.listdir(companies_dir):
            if filename.endswith(".json"):
                filepath = companies_dir / filename
                try:
                    with open(filepath, "r") as f:
                        company = json.load(f)
                        if company.get("database") == db_name:
                            company_id = company.get("id", filename.replace(".json", ""))
                            logger.info(f"Detected company '{company_id}' from database '{db_name}'")
                            return company_id
                except Exception as e:
                    logger.warning(f"Could not read company config {filename}: {e}")

    logger.warning(f"No company found for database '{db_name}', using 'default'")
    return "default"
