"""
PDF Extraction Cache

Caches Gemini AI extraction results for bank statement PDFs in SQLite.
Cache key is SHA256 hash of PDF bytes — same PDF always returns cached result.
"""

import hashlib
import json
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Default cache database location (next to other SQLite DBs)
DEFAULT_CACHE_DB = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'pdf_extraction_cache.db')


class PDFExtractionCache:
    """SQLite-backed cache for PDF extraction results."""

    def __init__(self, db_path: str = DEFAULT_CACHE_DB):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Create cache table if it doesn't exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS extraction_cache (
                    pdf_hash TEXT PRIMARY KEY,
                    statement_info_json TEXT NOT NULL,
                    transactions_json TEXT NOT NULL,
                    transaction_count INTEGER NOT NULL,
                    extracted_at TEXT NOT NULL,
                    model_name TEXT,
                    file_size INTEGER
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_cache_extracted_at
                ON extraction_cache(extracted_at)
            """)

    @staticmethod
    def hash_pdf(pdf_bytes: bytes) -> str:
        """Compute SHA256 hash of PDF bytes."""
        return hashlib.sha256(pdf_bytes).hexdigest()

    def get(self, pdf_hash: str) -> Optional[Tuple[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Look up cached extraction result.

        Args:
            pdf_hash: SHA256 hash of the PDF bytes

        Returns:
            Tuple of (statement_info_dict, transactions_list) or None if not cached
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT statement_info_json, transactions_json FROM extraction_cache WHERE pdf_hash = ?",
                    (pdf_hash,)
                ).fetchone()
                if row:
                    statement_info = json.loads(row['statement_info_json'])
                    transactions = json.loads(row['transactions_json'])
                    logger.info(f"Cache HIT for pdf_hash={pdf_hash[:12]}... ({len(transactions)} transactions)")
                    return statement_info, transactions
        except Exception as e:
            logger.warning(f"Cache lookup error: {e}")
        return None

    def put(
        self,
        pdf_hash: str,
        statement_info: Dict[str, Any],
        transactions: List[Dict[str, Any]],
        model_name: str = '',
        file_size: int = 0
    ):
        """
        Store extraction result in cache.

        Args:
            pdf_hash: SHA256 hash of the PDF bytes
            statement_info: Statement info dict (serializable)
            transactions: List of transaction dicts (serializable)
            model_name: Gemini model used for extraction
            file_size: Size of the PDF in bytes
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO extraction_cache
                    (pdf_hash, statement_info_json, transactions_json, transaction_count,
                     extracted_at, model_name, file_size)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    pdf_hash,
                    json.dumps(statement_info, default=str),
                    json.dumps(transactions, default=str),
                    len(transactions),
                    datetime.utcnow().isoformat(),
                    model_name,
                    file_size
                ))
                logger.info(f"Cache STORE for pdf_hash={pdf_hash[:12]}... ({len(transactions)} transactions)")
        except Exception as e:
            logger.warning(f"Cache store error: {e}")

    def clear(self):
        """Clear all cached entries."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM extraction_cache")
            logger.info("PDF extraction cache cleared")

    def stats(self) -> Dict[str, Any]:
        """Return cache statistics."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("""
                SELECT COUNT(*) as entries,
                       SUM(file_size) as total_bytes,
                       MIN(extracted_at) as oldest,
                       MAX(extracted_at) as newest
                FROM extraction_cache
            """).fetchone()
            return {
                'entries': row['entries'],
                'total_bytes': row['total_bytes'] or 0,
                'oldest': row['oldest'],
                'newest': row['newest']
            }


# Singleton instance
_cache_instance = None


def get_extraction_cache(db_path: str = DEFAULT_CACHE_DB) -> PDFExtractionCache:
    """Get or create the singleton cache instance."""
    global _cache_instance
    if _cache_instance is None or _cache_instance.db_path != db_path:
        _cache_instance = PDFExtractionCache(db_path)
    return _cache_instance
