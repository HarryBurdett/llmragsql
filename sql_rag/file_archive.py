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


def restore_file(archive_path: str) -> Dict[str, Any]:
    """
    Restore an archived file back to its original location.

    Args:
        archive_path: Current path of the archived file

    Returns:
        Dict with restore result
    """
    archived = Path(archive_path)

    if not archived.exists():
        return {"success": False, "error": f"Archived file not found: {archive_path}"}

    # Find the log entry to get the original path
    log = load_archive_log()
    entry = None
    entry_index = None
    for i, e in enumerate(log):
        if e.get("archive_path") == archive_path:
            entry = e
            entry_index = i
            break

    if entry is None:
        return {"success": False, "error": "No archive record found for this file"}

    original_path = Path(entry["original_path"])

    # Ensure original directory exists
    original_path.parent.mkdir(parents=True, exist_ok=True)

    # Handle case where a new file exists at the original location
    dest = original_path
    if dest.exists():
        stem = original_path.stem
        suffix = original_path.suffix
        counter = 1
        while dest.exists():
            dest = original_path.parent / f"{stem}_restored_{counter}{suffix}"
            counter += 1

    try:
        shutil.move(str(archived), str(dest))

        # Update log entry
        log[entry_index]["restored_at"] = datetime.now().isoformat()
        log[entry_index]["restored_to"] = str(dest)
        save_archive_log(log)

        logger.info(f"Restored {archived.name} to {dest}")

        return {
            "success": True,
            "message": f"Restored to {dest}",
            "restored_path": str(dest),
            "original_path": str(original_path),
        }

    except Exception as e:
        logger.error(f"Failed to restore {archive_path}: {e}")
        return {"success": False, "error": str(e)}


# Bank detection patterns for email attachments
BANK_PATTERNS = {
    "barclays": {
        "domains": ["barclays.co.uk", "barclays.com", "barclaysbank.co.uk"],
        "keywords": ["barclays", "barclays bank"],
        "folder": DOWNLOADS_BASE / "bank-statements" / "barclays",
    },
    "hsbc": {
        "domains": ["hsbc.co.uk", "hsbc.com", "hsbcnet.com"],
        "keywords": ["hsbc", "hsbc uk"],
        "folder": DOWNLOADS_BASE / "bank-statements" / "hsbc",
    },
    "lloyds": {
        "domains": ["lloydsbank.co.uk", "lloydsbank.com", "lloydstsb.com"],
        "keywords": ["lloyds", "lloyds bank", "lloyds tsb"],
        "folder": DOWNLOADS_BASE / "bank-statements" / "lloyds",
    },
    "natwest": {
        "domains": ["natwest.com", "natwest.co.uk", "nwolb.com"],
        "keywords": ["natwest", "national westminster"],
        "folder": DOWNLOADS_BASE / "bank-statements" / "natwest",
    },
}


def detect_bank_from_email(
    sender_email: str,
    subject: str = "",
    filename: str = ""
) -> Optional[str]:
    """
    Detect which bank an email statement belongs to.

    Args:
        sender_email: Email sender address
        subject: Email subject line
        filename: Attachment filename

    Returns:
        Bank identifier (e.g., 'barclays') or None if not detected
    """
    sender_lower = sender_email.lower()
    subject_lower = subject.lower()
    filename_lower = filename.lower()

    for bank_id, patterns in BANK_PATTERNS.items():
        # Check sender domain
        for domain in patterns["domains"]:
            if domain in sender_lower:
                logger.info(f"Detected {bank_id} from sender domain: {sender_email}")
                return bank_id

        # Check subject and filename for keywords
        for keyword in patterns["keywords"]:
            if keyword in subject_lower or keyword in filename_lower:
                logger.info(f"Detected {bank_id} from keyword '{keyword}'")
                return bank_id

    return None


def save_email_attachment(
    attachment_data: bytes,
    filename: str,
    sender_email: str,
    subject: str = "",
    bank_hint: Optional[str] = None
) -> Dict[str, Any]:
    """
    Save an email attachment to the appropriate bank folder.

    Args:
        attachment_data: The file content as bytes
        filename: Original filename
        sender_email: Email sender (used for bank detection)
        subject: Email subject (used for bank detection)
        bank_hint: Optional explicit bank identifier

    Returns:
        Dict with saved file info
    """
    # Detect bank
    bank_id = bank_hint or detect_bank_from_email(sender_email, subject, filename)

    if bank_id and bank_id in BANK_PATTERNS:
        dest_folder = BANK_PATTERNS[bank_id]["folder"]
    else:
        # Default to generic bank-statements folder
        dest_folder = DOWNLOADS_BASE / "bank-statements" / "unsorted"
        logger.warning(f"Could not detect bank for {filename}, saving to unsorted folder")

    # Ensure folder exists
    dest_folder.mkdir(parents=True, exist_ok=True)

    # Create destination path
    dest_path = dest_folder / filename

    # Handle duplicate filenames
    if dest_path.exists():
        stem = Path(filename).stem
        suffix = Path(filename).suffix
        counter = 1
        while dest_path.exists():
            dest_path = dest_folder / f"{stem}_{counter}{suffix}"
            counter += 1

    # Write file
    try:
        with open(dest_path, 'wb') as f:
            f.write(attachment_data)

        logger.info(f"Saved email attachment to {dest_path}")

        return {
            "success": True,
            "path": str(dest_path),
            "bank": bank_id,
            "folder": str(dest_folder),
            "filename": dest_path.name
        }
    except Exception as e:
        logger.error(f"Failed to save attachment: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def get_bank_statement_folder(bank_id: str) -> Optional[Path]:
    """Get the folder path for a specific bank's statements."""
    if bank_id in BANK_PATTERNS:
        return BANK_PATTERNS[bank_id]["folder"]
    return None
