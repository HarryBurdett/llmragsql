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
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

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

# Databases containing learned/intelligence data (recommended for import)
IMPORTABLE_DATABASES = {
    "bank_patterns.db": {
        "description": "Learned transaction type and account assignments",
        "default_selected": True,
        "table": "bank_import_patterns",
    },
    "bank_aliases.db": {
        "description": "Bank name to account code mappings",
        "default_selected": True,
        "table": "bank_import_aliases",
    },
    "supplier_statements.db": {
        "description": "Supplier statement processing history",
        "default_selected": False,
        "table": "supplier_statements",
    },
}


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


def _count_db_records(db_path: Path, table_name: str) -> Optional[int]:
    """Count records in a SQLite table. Returns None if table doesn't exist."""
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return None


def scan_source_installation(source_path: str, company_id: str) -> Dict[str, Any]:
    """
    Scan a source SQL RAG installation for importable learned data.

    Looks in two locations (in order of preference):
      1. source_path/data/{company_id}/  (per-company structure)
      2. source_path/                     (legacy root-level)

    Args:
        source_path: Path to the root of another SQL RAG installation
        company_id: Company ID to look for in the source

    Returns:
        Dict with 'source_location', 'databases' list, and any 'errors'
    """
    source = Path(source_path)

    if not source.exists():
        return {"error": f"Source path does not exist: {source_path}", "databases": []}

    if not source.is_dir():
        return {"error": f"Source path is not a directory: {source_path}", "databases": []}

    # Determine where to look: per-company dir first, then root
    per_company_dir = source / "data" / company_id
    source_dir = None
    source_location = None

    if per_company_dir.exists() and per_company_dir.is_dir():
        source_dir = per_company_dir
        source_location = f"data/{company_id}/"
    else:
        # Check root level for legacy layout
        has_any = any((source / db_name).exists() for db_name in IMPORTABLE_DATABASES)
        if has_any:
            source_dir = source
            source_location = "(root level — legacy layout)"

    if source_dir is None:
        # Also check if other company dirs exist to help the user
        data_dir = source / "data"
        available_companies = []
        if data_dir.exists():
            available_companies = [
                d.name for d in data_dir.iterdir()
                if d.is_dir() and any((d / db).exists() for db in IMPORTABLE_DATABASES)
            ]

        msg = f"No learned data found for company '{company_id}' at {source_path}"
        if available_companies:
            msg += f". Available companies: {', '.join(available_companies)}"
        return {"error": msg, "databases": [], "available_companies": available_companies}

    # Scan for importable databases
    databases = []
    for db_name, info in IMPORTABLE_DATABASES.items():
        db_file = source_dir / db_name
        if db_file.exists():
            size_bytes = db_file.stat().st_size
            record_count = _count_db_records(db_file, info["table"])

            databases.append({
                "name": db_name,
                "description": info["description"],
                "default_selected": info["default_selected"],
                "size_bytes": size_bytes,
                "size_display": _format_file_size(size_bytes),
                "record_count": record_count,
                "source_file": str(db_file),
            })

    return {
        "source_path": source_path,
        "source_location": source_location,
        "company_id": company_id,
        "databases": databases,
    }


def import_learned_data(
    source_path: str,
    source_company_id: str,
    target_company_id: str,
    databases: List[str],
) -> Dict[str, Any]:
    """
    Copy selected databases from a source installation to the target company dir.

    Backs up existing target databases before overwriting (.bak).

    Args:
        source_path: Path to the root of the source SQL RAG installation
        source_company_id: Company ID in the source installation
        target_company_id: Company ID to import into (current company)
        databases: List of database filenames to import (e.g., ['bank_patterns.db'])

    Returns:
        Dict with 'imported' list, 'backed_up' list, and any 'errors'
    """
    source = Path(source_path)
    imported = []
    backed_up = []
    errors = []

    # Validate databases requested
    valid_dbs = set(IMPORTABLE_DATABASES.keys())
    for db_name in databases:
        if db_name not in valid_dbs:
            errors.append(f"'{db_name}' is not an importable database")

    if errors:
        return {"imported": [], "backed_up": [], "errors": errors}

    # Determine source directory
    per_company_dir = source / "data" / source_company_id
    if per_company_dir.exists() and per_company_dir.is_dir():
        source_dir = per_company_dir
    elif any((source / db).exists() for db in databases):
        source_dir = source
    else:
        return {
            "imported": [],
            "backed_up": [],
            "errors": [f"No learned data found for company '{source_company_id}' at {source_path}"]
        }

    # Ensure target directory exists
    target_dir = get_company_data_dir(target_company_id)

    for db_name in databases:
        source_file = source_dir / db_name
        target_file = target_dir / db_name

        if not source_file.exists():
            errors.append(f"Source file not found: {source_file}")
            continue

        try:
            # Backup existing target if it exists
            if target_file.exists():
                backup_file = target_dir / f"{db_name}.bak"
                shutil.copy2(str(target_file), str(backup_file))
                backed_up.append(db_name)
                logger.info(f"Backed up {target_file} to {backup_file}")

            # Copy source to target
            shutil.copy2(str(source_file), str(target_file))

            info = IMPORTABLE_DATABASES[db_name]
            record_count = _count_db_records(target_file, info["table"])
            imported.append({
                "name": db_name,
                "record_count": record_count,
                "size_display": _format_file_size(target_file.stat().st_size),
            })
            logger.info(f"Imported {db_name} ({record_count} records) from {source_file} to {target_file}")

        except Exception as e:
            errors.append(f"Failed to import {db_name}: {str(e)}")
            logger.error(f"Failed to import {db_name}: {e}")

    return {
        "imported": imported,
        "backed_up": backed_up,
        "errors": errors,
        "target_directory": str(target_dir),
    }


def _format_file_size(size_bytes: int) -> str:
    """Format file size for display."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
