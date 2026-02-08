"""
File Archive Module

Handles archiving of processed import files (bank statements, GoCardless exports, etc.)
with metadata tracking and organized folder structure.
"""

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)

# Base paths
DOWNLOADS_BASE = Path("/Users/maccb/Downloads")
ARCHIVE_LOG_FILE = DOWNLOADS_BASE / "archive_log.json"

# Import type configurations
IMPORT_CONFIGS = {
    "bank-statement": {
        "source_dirs": [
            DOWNLOADS_BASE / "bank-statements" / "barclays",
            DOWNLOADS_BASE / "bank-statements" / "hsbc",
            DOWNLOADS_BASE / "bank-statements" / "lloyds",
            DOWNLOADS_BASE / "bank-statements" / "natwest",
        ],
        "archive_dir": DOWNLOADS_BASE / "bank-statements" / "archive",
    },
    "gocardless": {
        "source_dirs": [DOWNLOADS_BASE / "gocardless"],
        "archive_dir": DOWNLOADS_BASE / "gocardless" / "archive",
    },
    "invoice": {
        "source_dirs": [DOWNLOADS_BASE / "invoices"],
        "archive_dir": DOWNLOADS_BASE / "invoices" / "archive",
    },
}


def load_archive_log() -> List[Dict[str, Any]]:
    """Load the archive log from disk."""
    if ARCHIVE_LOG_FILE.exists():
        try:
            with open(ARCHIVE_LOG_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []
    return []


def save_archive_log(log: List[Dict[str, Any]]) -> None:
    """Save the archive log to disk."""
    with open(ARCHIVE_LOG_FILE, 'w') as f:
        json.dump(log, f, indent=2, default=str)


def archive_file(
    file_path: str,
    import_type: str,
    metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Archive a processed import file.

    Args:
        file_path: Path to the file to archive
        import_type: Type of import ('bank-statement', 'gocardless', 'invoice')
        metadata: Optional metadata about the import (transactions, matches, etc.)

    Returns:
        Dict with archive result
    """
    source_path = Path(file_path)

    if not source_path.exists():
        return {"success": False, "error": f"File not found: {file_path}"}

    if import_type not in IMPORT_CONFIGS:
        return {"success": False, "error": f"Unknown import type: {import_type}"}

    config = IMPORT_CONFIGS[import_type]
    archive_base = config["archive_dir"]

    # Create YYYY-MM subfolder
    now = datetime.now()
    archive_subfolder = archive_base / now.strftime("%Y-%m")
    archive_subfolder.mkdir(parents=True, exist_ok=True)

    # Destination path
    dest_path = archive_subfolder / source_path.name

    # Handle duplicate filenames
    if dest_path.exists():
        stem = source_path.stem
        suffix = source_path.suffix
        counter = 1
        while dest_path.exists():
            dest_path = archive_subfolder / f"{stem}_{counter}{suffix}"
            counter += 1

    try:
        # Move the file
        shutil.move(str(source_path), str(dest_path))

        # Log the archive action
        log_entry = {
            "archived_at": now.isoformat(),
            "original_path": str(source_path),
            "archive_path": str(dest_path),
            "import_type": import_type,
            "filename": source_path.name,
            "metadata": metadata or {}
        }

        log = load_archive_log()
        log.append(log_entry)
        save_archive_log(log)

        logger.info(f"Archived {source_path.name} to {dest_path}")

        return {
            "success": True,
            "message": f"Archived to {dest_path}",
            "archive_path": str(dest_path),
            "original_path": str(source_path)
        }

    except Exception as e:
        logger.error(f"Failed to archive {file_path}: {e}")
        return {"success": False, "error": str(e)}


def get_archive_history(
    import_type: Optional[str] = None,
    limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Get archive history, optionally filtered by import type.

    Args:
        import_type: Filter by type, or None for all
        limit: Maximum entries to return

    Returns:
        List of archive log entries (newest first)
    """
    log = load_archive_log()

    if import_type:
        log = [e for e in log if e.get("import_type") == import_type]

    # Sort by date descending
    log.sort(key=lambda x: x.get("archived_at", ""), reverse=True)

    return log[:limit]


def get_pending_files(import_type: str) -> List[Dict[str, Any]]:
    """
    Get list of files pending in source directories (not yet archived).

    Args:
        import_type: Type of import to check

    Returns:
        List of file info dicts
    """
    if import_type not in IMPORT_CONFIGS:
        return []

    config = IMPORT_CONFIGS[import_type]
    files = []

    for source_dir in config["source_dirs"]:
        if not source_dir.exists():
            continue

        for file_path in source_dir.iterdir():
            if file_path.is_file() and not file_path.name.startswith('.'):
                files.append({
                    "path": str(file_path),
                    "filename": file_path.name,
                    "folder": source_dir.name,
                    "size": file_path.stat().st_size,
                    "modified": datetime.fromtimestamp(
                        file_path.stat().st_mtime
                    ).isoformat()
                })

    # Sort by modified date descending
    files.sort(key=lambda x: x.get("modified", ""), reverse=True)

    return files


def is_file_archived(file_path: str) -> bool:
    """Check if a file has already been archived."""
    log = load_archive_log()
    return any(e.get("original_path") == file_path for e in log)
