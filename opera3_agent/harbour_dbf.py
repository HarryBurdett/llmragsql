"""
Harbour DBF/CDX Bridge - Python ctypes wrapper

Provides a Python interface to the Harbour DBFCDX shared library for reading
and writing FoxPro DBF files with full CDX compound index maintenance.

Thread Safety:
    The Harbour VM is single-threaded. All calls to this class MUST be
    serialised via the internal lock. The HarbourDBF class handles this
    automatically - callers do not need to worry about thread safety.

Usage:
    db = HarbourDBF()
    db.open("/path/to/pname.dbf", "PNAME")
    db.set_order("PNAME", "PNACCOUNT")
    if db.seek("PNAME", "SUP001"):
        name = db.get_field("PNAME", "PN_NAME")
    db.close("PNAME")
    db.shutdown()
"""

import ctypes
import sys
import os
import threading
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import date, datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class HarbourDBFError(Exception):
    """Raised when a Harbour DBF operation fails."""
    pass


class HarbourDBF:
    """Python interface to Harbour DBFCDX operations via shared library.

    All methods are thread-safe via an internal lock (Harbour VM is single-threaded).
    """

    def __init__(self, lib_path: str = None):
        """Initialise the Harbour DBF bridge.

        Args:
            lib_path: Path to the shared library (without extension).
                      Auto-detects platform extension if not specified.
        """
        self._lock = threading.Lock()
        self._lib = self._load_library(lib_path)
        self._setup_prototypes()
        self._initialized = False

        # Initialise Harbour VM and DBFCDX driver
        with self._lock:
            rc = self._lib.hb_dbf_init()
            if rc != 0:
                raise HarbourDBFError(f"Failed to initialise Harbour VM (rc={rc})")
            self._initialized = True
            logger.info("Harbour DBFCDX bridge initialised")

    def _load_library(self, lib_path: str = None) -> ctypes.CDLL:
        """Load the platform-specific shared library."""
        if lib_path is None:
            # Look in the same directory as this file
            base_dir = Path(__file__).parent
            lib_path = str(base_dir / "harbour" / "libdbfbridge")

        # Determine platform extension
        if sys.platform == "darwin":
            extensions = [".dylib", ".so"]
        elif sys.platform == "win32":
            extensions = [".dll"]
        else:
            extensions = [".so"]

        # Try with explicit extension first
        for ext in extensions:
            full_path = lib_path + ext
            if os.path.exists(full_path):
                try:
                    return ctypes.CDLL(full_path)
                except OSError as e:
                    logger.warning(f"Failed to load {full_path}: {e}")
                    continue

        # Try without extension (let OS resolve)
        try:
            return ctypes.CDLL(lib_path)
        except OSError as e:
            raise HarbourDBFError(
                f"Could not load Harbour library from '{lib_path}'. "
                f"Build it first: cd opera3_agent/harbour && ./build.sh"
            ) from e

    def _setup_prototypes(self):
        """Declare C function signatures for type safety."""
        L = self._lib

        # Init/Quit
        L.hb_dbf_init.restype = ctypes.c_int
        L.hb_dbf_init.argtypes = []
        L.hb_dbf_quit.restype = None
        L.hb_dbf_quit.argtypes = []

        # Open/Close
        L.hb_dbf_open.restype = ctypes.c_int
        L.hb_dbf_open.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        L.hb_dbf_open_exclusive.restype = ctypes.c_int
        L.hb_dbf_open_exclusive.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        L.hb_dbf_close.restype = ctypes.c_int
        L.hb_dbf_close.argtypes = [ctypes.c_char_p]
        L.hb_dbf_close_all.restype = ctypes.c_int
        L.hb_dbf_close_all.argtypes = []

        # Record operations
        L.hb_dbf_append.restype = ctypes.c_int
        L.hb_dbf_append.argtypes = [ctypes.c_char_p]

        # Replace field values
        L.hb_dbf_replace_c.restype = ctypes.c_int
        L.hb_dbf_replace_c.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]
        L.hb_dbf_replace_n.restype = ctypes.c_int
        L.hb_dbf_replace_n.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_double]
        L.hb_dbf_replace_d.restype = ctypes.c_int
        L.hb_dbf_replace_d.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]
        L.hb_dbf_replace_l.restype = ctypes.c_int
        L.hb_dbf_replace_l.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
        L.hb_dbf_replace_m.restype = ctypes.c_int
        L.hb_dbf_replace_m.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]

        # Locking
        L.hb_dbf_unlock.restype = ctypes.c_int
        L.hb_dbf_unlock.argtypes = [ctypes.c_char_p]
        L.hb_dbf_unlock_all.restype = ctypes.c_int
        L.hb_dbf_unlock_all.argtypes = []
        L.hb_dbf_rlock.restype = ctypes.c_int
        L.hb_dbf_rlock.argtypes = [ctypes.c_char_p]
        L.hb_dbf_flock.restype = ctypes.c_int
        L.hb_dbf_flock.argtypes = [ctypes.c_char_p]

        # Navigation
        L.hb_dbf_goto_top.restype = ctypes.c_int
        L.hb_dbf_goto_top.argtypes = [ctypes.c_char_p]
        L.hb_dbf_goto_bottom.restype = ctypes.c_int
        L.hb_dbf_goto_bottom.argtypes = [ctypes.c_char_p]
        L.hb_dbf_goto_record.restype = ctypes.c_int
        L.hb_dbf_goto_record.argtypes = [ctypes.c_char_p, ctypes.c_int]
        L.hb_dbf_skip.restype = ctypes.c_int
        L.hb_dbf_skip.argtypes = [ctypes.c_char_p, ctypes.c_int]
        L.hb_dbf_eof.restype = ctypes.c_int
        L.hb_dbf_eof.argtypes = [ctypes.c_char_p]
        L.hb_dbf_bof.restype = ctypes.c_int
        L.hb_dbf_bof.argtypes = [ctypes.c_char_p]
        L.hb_dbf_recno.restype = ctypes.c_int
        L.hb_dbf_recno.argtypes = [ctypes.c_char_p]
        L.hb_dbf_reccount.restype = ctypes.c_int
        L.hb_dbf_reccount.argtypes = [ctypes.c_char_p]

        # Index operations
        L.hb_dbf_seek.restype = ctypes.c_int
        L.hb_dbf_seek.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        L.hb_dbf_seek_n.restype = ctypes.c_int
        L.hb_dbf_seek_n.argtypes = [ctypes.c_char_p, ctypes.c_int]
        L.hb_dbf_set_order.restype = ctypes.c_int
        L.hb_dbf_set_order.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        L.hb_dbf_order.restype = ctypes.c_char_p
        L.hb_dbf_order.argtypes = [ctypes.c_char_p]
        L.hb_dbf_tag_count.restype = ctypes.c_int
        L.hb_dbf_tag_count.argtypes = [ctypes.c_char_p]
        L.hb_dbf_tag_name.restype = ctypes.c_char_p
        L.hb_dbf_tag_name.argtypes = [ctypes.c_char_p, ctypes.c_int]
        L.hb_dbf_reindex.restype = ctypes.c_int
        L.hb_dbf_reindex.argtypes = [ctypes.c_char_p]

        # Field access
        L.hb_dbf_get_field.restype = ctypes.c_char_p
        L.hb_dbf_get_field.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        L.hb_dbf_get_field_n.restype = ctypes.c_double
        L.hb_dbf_get_field_n.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        L.hb_dbf_fcount.restype = ctypes.c_int
        L.hb_dbf_fcount.argtypes = [ctypes.c_char_p]
        L.hb_dbf_fname.restype = ctypes.c_char_p
        L.hb_dbf_fname.argtypes = [ctypes.c_char_p, ctypes.c_int]
        L.hb_dbf_ftype.restype = ctypes.c_char_p
        L.hb_dbf_ftype.argtypes = [ctypes.c_char_p, ctypes.c_char_p]

        # Utility
        L.hb_dbf_flush.restype = ctypes.c_int
        L.hb_dbf_flush.argtypes = [ctypes.c_char_p]
        L.hb_dbf_flush_all.restype = ctypes.c_int
        L.hb_dbf_flush_all.argtypes = []
        L.hb_dbf_is_open.restype = ctypes.c_int
        L.hb_dbf_is_open.argtypes = [ctypes.c_char_p]

    # ================================================================
    # Encoding helpers
    # ================================================================

    @staticmethod
    def _e(s: str) -> Optional[bytes]:
        """Encode string to bytes for C (FoxPro uses cp1252/latin-1)."""
        if s is None:
            return None
        return s.encode("latin-1", errors="replace")

    @staticmethod
    def _d(b: bytes) -> str:
        """Decode bytes from C to Python string."""
        if b is None:
            return ""
        if isinstance(b, bytes):
            return b.decode("latin-1")
        return str(b)

    # ================================================================
    # Table operations
    # ================================================================

    def open(self, filepath: str, alias: str = "WORK") -> None:
        """Open a DBF table in shared mode. CDX auto-opens if present."""
        with self._lock:
            rc = self._lib.hb_dbf_open(self._e(filepath), self._e(alias))
            if rc != 0:
                raise HarbourDBFError(f"Failed to open '{filepath}' as '{alias}' — file may be locked or missing")

    def open_exclusive(self, filepath: str, alias: str = "WORK") -> None:
        """Open a DBF table in exclusive mode (for PACK/REINDEX)."""
        with self._lock:
            rc = self._lib.hb_dbf_open_exclusive(self._e(filepath), self._e(alias))
            if rc != 0:
                raise HarbourDBFError(f"Failed to open '{filepath}' exclusively — file may be in use")

    def close(self, alias: str = "WORK") -> None:
        """Close a workarea."""
        with self._lock:
            self._lib.hb_dbf_close(self._e(alias))

    def close_all(self) -> None:
        """Close all open workareas."""
        with self._lock:
            self._lib.hb_dbf_close_all()

    def is_open(self, alias: str) -> bool:
        """Check if a workarea alias is open."""
        with self._lock:
            return self._lib.hb_dbf_is_open(self._e(alias)) == 1

    # ================================================================
    # Record operations
    # ================================================================

    def append_blank(self, alias: str = "WORK") -> None:
        """Append a blank record. Auto-locks it. CDX indexes auto-update."""
        with self._lock:
            rc = self._lib.hb_dbf_append(self._e(alias))
            if rc != 0:
                raise HarbourDBFError(f"Failed to append record to '{alias}' — table may be locked")

    def append(self, alias: str, data: Dict[str, Any]) -> None:
        """Append a record with field values. CDX indexes auto-update.

        Args:
            alias: Table alias
            data: Dict of field_name -> value
        """
        with self._lock:
            a = self._e(alias)

            rc = self._lib.hb_dbf_append(a)
            if rc != 0:
                raise HarbourDBFError(f"Failed to append to '{alias}'")

            self._set_fields_locked(a, data)
            self._lib.hb_dbf_unlock(a)

    def replace(self, alias: str, field: str, value: Any) -> None:
        """Replace a field value on the current record.

        The record must be locked (via append or rlock).
        """
        with self._lock:
            self._replace_field(self._e(alias), field, value)

    def replace_fields(self, alias: str, data: Dict[str, Any]) -> None:
        """Replace multiple field values on the current record."""
        with self._lock:
            self._set_fields_locked(self._e(alias), data)

    def _replace_field(self, alias_bytes: bytes, field: str, value: Any) -> None:
        """Internal: replace a single field (caller holds lock)."""
        f = self._e(field.upper())

        if isinstance(value, str):
            self._lib.hb_dbf_replace_c(alias_bytes, f, self._e(value))
        elif isinstance(value, bool):
            self._lib.hb_dbf_replace_l(alias_bytes, f, 1 if value else 0)
        elif isinstance(value, (int, float)):
            self._lib.hb_dbf_replace_n(alias_bytes, f, float(value))
        elif isinstance(value, (date, datetime)):
            date_str = value.strftime("%Y%m%d")
            self._lib.hb_dbf_replace_d(alias_bytes, f, self._e(date_str))
        elif value is None:
            pass  # Leave as default/blank
        else:
            raise TypeError(f"Unsupported type {type(value)} for field '{field}'")

    def _set_fields_locked(self, alias_bytes: bytes, data: Dict[str, Any]) -> None:
        """Internal: set multiple fields on current record (caller holds lock)."""
        for field, value in data.items():
            self._replace_field(alias_bytes, field, value)

    # ================================================================
    # Locking
    # ================================================================

    def rlock(self, alias: str = "WORK") -> bool:
        """Lock the current record. Returns True on success."""
        with self._lock:
            return self._lib.hb_dbf_rlock(self._e(alias)) == 0

    def flock(self, alias: str = "WORK") -> bool:
        """Lock the entire file. Returns True on success. Use sparingly."""
        with self._lock:
            return self._lib.hb_dbf_flock(self._e(alias)) == 0

    def unlock(self, alias: str = "WORK") -> None:
        """Commit and unlock the current record."""
        with self._lock:
            self._lib.hb_dbf_unlock(self._e(alias))

    def unlock_all(self) -> None:
        """Commit and unlock all records in all workareas."""
        with self._lock:
            self._lib.hb_dbf_unlock_all()

    def flush(self, alias: str = "WORK") -> None:
        """Flush buffers to disk for this workarea."""
        with self._lock:
            self._lib.hb_dbf_flush(self._e(alias))

    def flush_all(self) -> None:
        """Flush all workareas to disk."""
        with self._lock:
            self._lib.hb_dbf_flush_all()

    # ================================================================
    # Navigation
    # ================================================================

    def goto_top(self, alias: str = "WORK") -> None:
        """Go to the first record."""
        with self._lock:
            self._lib.hb_dbf_goto_top(self._e(alias))

    def goto_bottom(self, alias: str = "WORK") -> None:
        """Go to the last record."""
        with self._lock:
            self._lib.hb_dbf_goto_bottom(self._e(alias))

    def goto_record(self, alias: str, recno: int) -> None:
        """Go to a specific record number."""
        with self._lock:
            self._lib.hb_dbf_goto_record(self._e(alias), recno)

    def skip(self, alias: str = "WORK", n: int = 1) -> bool:
        """Skip n records. Returns True if NOT at EOF."""
        with self._lock:
            return self._lib.hb_dbf_skip(self._e(alias), n) == 0

    def eof(self, alias: str = "WORK") -> bool:
        """Check if at end of file."""
        with self._lock:
            return self._lib.hb_dbf_eof(self._e(alias)) == 1

    def bof(self, alias: str = "WORK") -> bool:
        """Check if at beginning of file."""
        with self._lock:
            return self._lib.hb_dbf_bof(self._e(alias)) == 1

    def recno(self, alias: str = "WORK") -> int:
        """Get current record number."""
        with self._lock:
            return self._lib.hb_dbf_recno(self._e(alias))

    def reccount(self, alias: str = "WORK") -> int:
        """Get total record count."""
        with self._lock:
            return self._lib.hb_dbf_reccount(self._e(alias))

    # ================================================================
    # Index operations
    # ================================================================

    def seek(self, alias: str, key: str) -> bool:
        """Seek on the active index tag. Returns True if found."""
        with self._lock:
            return self._lib.hb_dbf_seek(self._e(alias), self._e(key)) == 0

    def seek_n(self, alias: str, key: int) -> bool:
        """Seek a numeric key on the active index tag."""
        with self._lock:
            return self._lib.hb_dbf_seek_n(self._e(alias), key) == 0

    def set_order(self, alias: str, tag_name: str) -> None:
        """Set the active CDX index tag. Empty string = natural order."""
        with self._lock:
            self._lib.hb_dbf_set_order(self._e(alias), self._e(tag_name))

    def order(self, alias: str = "WORK") -> str:
        """Get the current active index tag name."""
        with self._lock:
            return self._d(self._lib.hb_dbf_order(self._e(alias)))

    def tag_count(self, alias: str = "WORK") -> int:
        """Get the number of index tags."""
        with self._lock:
            return self._lib.hb_dbf_tag_count(self._e(alias))

    def tag_name(self, alias: str, pos: int) -> str:
        """Get index tag name by ordinal position (1-based)."""
        with self._lock:
            return self._d(self._lib.hb_dbf_tag_name(self._e(alias), pos))

    def list_tags(self, alias: str = "WORK") -> List[str]:
        """List all index tag names for this table."""
        with self._lock:
            count = self._lib.hb_dbf_tag_count(self._e(alias))
            a = self._e(alias)
            tags = []
            for i in range(1, count + 1):
                name = self._d(self._lib.hb_dbf_tag_name(a, i))
                tags.append(name)
            return tags

    def reindex(self, alias: str = "WORK") -> None:
        """Rebuild all index tags. Table should be opened exclusively."""
        with self._lock:
            rc = self._lib.hb_dbf_reindex(self._e(alias))
            if rc != 0:
                raise HarbourDBFError(f"Reindex failed for '{alias}'")

    # ================================================================
    # Field access
    # ================================================================

    def get_field(self, alias: str, field: str) -> str:
        """Get field value as string."""
        with self._lock:
            return self._d(self._lib.hb_dbf_get_field(self._e(alias), self._e(field.upper())))

    def get_field_n(self, alias: str, field: str) -> float:
        """Get numeric field value."""
        with self._lock:
            return self._lib.hb_dbf_get_field_n(self._e(alias), self._e(field.upper()))

    def get_field_stripped(self, alias: str, field: str) -> str:
        """Get field value as stripped string (no trailing spaces)."""
        return self.get_field(alias, field).rstrip()

    def scatter(self, alias: str = "WORK", strip: bool = True) -> Dict[str, Any]:
        """Read all fields of current record as a dict.

        Args:
            alias: Table alias
            strip: Strip trailing spaces from string fields
        """
        with self._lock:
            a = self._e(alias)
            count = self._lib.hb_dbf_fcount(a)
            record = {}
            for i in range(1, count + 1):
                name = self._d(self._lib.hb_dbf_fname(a, i))
                ftype = self._d(self._lib.hb_dbf_ftype(a, self._e(name)))

                if ftype == "N":
                    record[name] = self._lib.hb_dbf_get_field_n(a, self._e(name))
                else:
                    val = self._d(self._lib.hb_dbf_get_field(a, self._e(name)))
                    if strip and ftype in ("C", "M"):
                        val = val.rstrip()
                    record[name] = val

            return record

    def fcount(self, alias: str = "WORK") -> int:
        """Get field count."""
        with self._lock:
            return self._lib.hb_dbf_fcount(self._e(alias))

    def fname(self, alias: str, pos: int) -> str:
        """Get field name by position (1-based)."""
        with self._lock:
            return self._d(self._lib.hb_dbf_fname(self._e(alias), pos))

    def ftype(self, alias: str, field: str) -> str:
        """Get field type (C/N/D/L/M)."""
        with self._lock:
            return self._d(self._lib.hb_dbf_ftype(self._e(alias), self._e(field.upper())))

    # ================================================================
    # Context managers
    # ================================================================

    @contextmanager
    def table(self, filepath: str, alias: str = "WORK", exclusive: bool = False):
        """Context manager: open table, yield, close on exit.

        Usage:
            with db.table("/path/to/pname.dbf", "PNAME") as alias:
                db.seek(alias, "SUP001")
        """
        if exclusive:
            self.open_exclusive(filepath, alias)
        else:
            self.open(filepath, alias)
        try:
            yield alias
        finally:
            self.close(alias)

    @contextmanager
    def record_lock(self, alias: str = "WORK"):
        """Context manager: lock current record, unlock on exit.

        Usage:
            with db.record_lock("PNAME"):
                db.replace("PNAME", "PN_CURRBAL", 1234.56)
        """
        if not self.rlock(alias):
            raise HarbourDBFError(f"Failed to lock record in '{alias}'")
        try:
            yield
        finally:
            self.unlock(alias)

    # ================================================================
    # Lifecycle
    # ================================================================

    def shutdown(self):
        """Shut down the Harbour VM. Call once at application exit."""
        if self._initialized:
            with self._lock:
                try:
                    self._lib.hb_dbf_close_all()
                except Exception:
                    pass
                try:
                    self._lib.hb_dbf_quit()
                except Exception:
                    pass
                self._initialized = False
                logger.info("Harbour DBFCDX bridge shut down")

    def __del__(self):
        try:
            self.shutdown()
        except Exception:
            pass

    def __repr__(self):
        return f"<HarbourDBF initialized={self._initialized}>"
