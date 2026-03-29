# SMB Access to Opera 3 FoxPro Folders — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace OS-level SMB mounting with direct Python SMB access (`smbprotocol`) so macOS can read/write Opera 3 FoxPro DBF files on a Windows server.

**Architecture:** A new `SMBFileManager` singleton handles SMB connections and file caching. `Opera3Reader` and `Opera3FoxProImport` check the singleton internally in their file-path resolution methods — so the 30+ existing callers across the codebase require zero changes. The API config endpoints create/destroy the singleton when Opera 3 SMB settings change.

**Tech Stack:** `smbprotocol` (pure Python SMBv2/v3), `dbfread` (read), `dbf` (write)

**Spec:** `docs/superpowers/specs/2026-03-29-smb-foxpro-access-design.md`

**Spec Deviations (justified):**
1. **Singleton instead of pass-through** — spec says pass `smb_manager` through each layer. Plan uses a module-level singleton instead, avoiding changes to 30+ callers.
2. **No `download_file_set()`** — spec lists bulk download method. Plan uses on-demand download per file via `_get_dbf_path()`, which is simpler. Bulk download is a future optimization if latency matters.
3. **Non-atomic uploads** — spec claims "all uploads succeed or none do". True atomic multi-file upload over SMB is not possible. Plan uploads sequentially; if one fails, prior uploads remain on server (acceptable — old versions are replaced).
4. **Temp path not persisted** — spec says `opera3_base_path` is auto-set to temp dir. Plan sets it in memory only (not saved to config file), since temp dirs are ephemeral and recreated on startup.

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `sql_rag/smb_access.py` | Create | `SMBFileManager` class + module-level singleton (`get_smb_manager`, `set_smb_manager`) |
| `sql_rag/opera3_foxpro.py` | Modify | `Opera3Reader._get_dbf_path()` and `Opera3System` use SMB singleton for on-demand download |
| `sql_rag/opera3_foxpro_import.py` | Modify | `Opera3FoxProImport._open_table()` downloads via SMB, `_close_all_tables()` uploads modified files |
| `api/main.py` | Modify | Replace `_mount_opera3_share()` with SMB manager creation; update config/test/companies endpoints |
| `requirements.txt` | Modify | Add `smbprotocol` |

**Files that do NOT change** (key design decision — singleton pattern avoids this):
- `sql_rag/opera3_data_provider.py`
- `sql_rag/opera_data_provider.py`
- `sql_rag/bank_import_opera3.py`
- `apps/gocardless/api/routes.py` (30+ `Opera3Reader(data_path)` calls)
- `apps/balance_check/api/routes.py`
- `apps/lock_monitor/api/routes.py`
- Frontend (`Installations.tsx` — settings already exist)

---

### Task 1: Add `smbprotocol` dependency

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add smbprotocol to requirements.txt**

Add after the `pytest` line:

```
# SMB/Network share access (Opera 3 FoxPro over network)
smbprotocol>=1.13.0
```

- [ ] **Step 2: Install the package**

Run: `pip install smbprotocol`

- [ ] **Step 3: Verify import works**

Run: `python -c "import smbclient; print('smbprotocol OK')"`
Expected: `smbprotocol OK`

- [ ] **Step 4: Commit**

```bash
git add requirements.txt
git commit -m "Add smbprotocol dependency for Opera 3 SMB access"
```

---

### Task 2: Create `SMBFileManager` core module

**Files:**
- Create: `sql_rag/smb_access.py`

- [ ] **Step 1: Create the module with SMBFileManager class**

Create `sql_rag/smb_access.py` with the full implementation:

```python
"""
SMB File Manager for Opera 3 FoxPro Access

Provides direct Python SMB access to Opera 3 FoxPro DBF files on Windows servers.
Replaces OS-level mount commands (mount_smbfs/mount -t cifs) which are unreliable on macOS.

Uses smbprotocol/smbclient for SMBv2/v3 access. Downloads DBF files (+ .cdx/.fpt companions)
to a local temp directory for reading, uploads modified files back after writes.

USAGE:
    from sql_rag.smb_access import SMBFileManager, set_smb_manager, get_smb_manager

    # Create and register singleton
    mgr = SMBFileManager(server="172.17.172.214", share="O3 Server VFP", username="user", password="pass")
    mgr.connect()
    set_smb_manager(mgr)

    # Now Opera3Reader and Opera3FoxProImport automatically use it
    # via get_smb_manager() in their file-path resolution methods
"""

import os
import stat
import time
import shutil
import logging
import tempfile
from pathlib import Path
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

try:
    import smbclient
    from smbprotocol.exceptions import SMBException, SMBConnectionClosed, SMBResponseException
    SMB_AVAILABLE = True
except ImportError:
    SMB_AVAILABLE = False
    logger.warning("smbprotocol not installed. Install with: pip install smbprotocol")


class SMBFileManager:
    """
    Manages SMB connections and local file caching for Opera 3 FoxPro DBF access.

    Downloads DBF files (and their .cdx/.fpt companions) to a local temp directory.
    Uploads modified files back to the SMB share after writes.
    Provides caching with configurable TTL to avoid redundant downloads.
    """

    # Companion file extensions that travel with .dbf files
    COMPANION_EXTENSIONS = ['.cdx', '.CDX', '.fpt', '.FPT']

    def __init__(self, server: str, share: str, username: str, password: str,
                 base_path: str = "", cache_ttl: int = 30):
        """
        Args:
            server: SMB server hostname or IP (e.g., "172.17.172.214")
            share: Share name (e.g., "O3 Server VFP")
            username: SMB username
            password: SMB password
            base_path: Subfolder within the share (e.g., "" or "subfolder/path")
            cache_ttl: Seconds before a cached file is considered stale (default 30)
        """
        if not SMB_AVAILABLE:
            raise ImportError("smbprotocol required. Install with: pip install smbprotocol")

        self.server = server
        self.share = share
        self.username = username
        self.password = password
        self.base_path = base_path.strip("/\\")
        self.cache_ttl = cache_ttl

        self._connected = False
        self._local_base: Optional[Path] = None
        self._cache_times: Dict[str, float] = {}  # remote_path -> download timestamp
        self._modified_files: set = set()  # tracks locally modified file paths

    @property
    def smb_base(self) -> str:
        """Base SMB path for file operations."""
        base = f"\\\\{self.server}\\{self.share}"
        if self.base_path:
            base += f"\\{self.base_path.replace('/', '\\')}"
        return base

    def connect(self) -> bool:
        """
        Establish SMB connection and create local temp directory.

        Returns:
            True if connection successful

        Raises:
            ConnectionError: If cannot connect to SMB server
        """
        try:
            # Register credentials with smbclient
            smbclient.register_session(
                self.server,
                username=self.username,
                password=self.password
            )

            # Test connection by listing the share root
            test_path = f"\\\\{self.server}\\{self.share}"
            smbclient.listdir(test_path)

            # Create local temp directory with restrictive permissions
            self._local_base = Path(tempfile.mkdtemp(prefix="opera3_smb_"))
            os.chmod(str(self._local_base), stat.S_IRWXU)  # 0o700

            self._connected = True
            logger.info(f"SMB connected to \\\\{self.server}\\{self.share}, "
                       f"local cache: {self._local_base}")
            return True

        except Exception as e:
            self._connected = False
            raise ConnectionError(
                f"Cannot connect to Opera 3 server at {self.server} — "
                f"check credentials and network. Error: {e}"
            ) from e

    def disconnect(self):
        """Disconnect and clean up local temp directory."""
        if self._local_base and self._local_base.exists():
            try:
                shutil.rmtree(str(self._local_base))
                logger.info(f"Cleaned up temp directory: {self._local_base}")
            except Exception as e:
                logger.warning(f"Failed to clean temp directory: {e}")

        self._connected = False
        self._local_base = None
        self._cache_times.clear()
        self._modified_files.clear()

    def is_connected(self) -> bool:
        """Check if SMB connection is active."""
        if not self._connected:
            return False
        # Verify connection is still alive
        try:
            test_path = f"\\\\{self.server}\\{self.share}"
            smbclient.listdir(test_path)
            return True
        except Exception:
            self._connected = False
            return False

    def _reconnect(self):
        """Attempt to reconnect after a dropped connection."""
        logger.info("Attempting SMB reconnection...")
        try:
            smbclient.register_session(
                self.server,
                username=self.username,
                password=self.password
            )
            test_path = f"\\\\{self.server}\\{self.share}"
            smbclient.listdir(test_path)
            self._connected = True
            logger.info("SMB reconnection successful")
        except Exception as e:
            raise ConnectionError(
                f"SMB reconnection failed to {self.server}: {e}"
            ) from e

    def _ensure_connected(self):
        """Ensure we have an active connection, reconnecting if needed."""
        if not self._connected:
            raise ConnectionError("SMB not connected. Call connect() first.")
        try:
            # Use stat (lighter than listdir) to verify connection
            test_path = f"\\\\{self.server}\\{self.share}"
            smbclient.stat(test_path)
        except Exception:
            self._reconnect()

    def _remote_path(self, relative_path: str) -> str:
        """Convert a relative path to full SMB path."""
        clean = relative_path.replace("/", "\\").strip("\\")
        return f"{self.smb_base}\\{clean}"

    def _local_path(self, relative_path: str) -> Path:
        """Convert a relative path to local temp path."""
        clean = relative_path.replace("\\", "/").strip("/")
        return self._local_base / clean

    def list_dir(self, relative_path: str = "") -> List[str]:
        """
        List files in a remote directory.

        Args:
            relative_path: Path relative to the share base

        Returns:
            List of filenames
        """
        self._ensure_connected()
        remote = self._remote_path(relative_path) if relative_path else self.smb_base
        try:
            entries = smbclient.listdir(remote)
            return [e for e in entries if e not in ('.', '..')]
        except Exception as e:
            raise IOError(f"Cannot list directory {remote}: {e}") from e

    def download_file(self, relative_path: str, force_fresh: bool = False) -> Path:
        """
        Download a file from SMB to local temp directory.
        Also downloads companion files (.cdx, .fpt) if they exist.

        Args:
            relative_path: Path relative to share base (e.g., "Company00A/data/pname.dbf")
            force_fresh: Bypass cache and always re-download

        Returns:
            Path to local copy of the file
        """
        self._ensure_connected()

        local = self._local_path(relative_path)
        cache_key = relative_path.lower()

        # Check cache
        if not force_fresh and local.exists():
            cached_time = self._cache_times.get(cache_key, 0)
            if time.time() - cached_time < self.cache_ttl:
                return local

        # Ensure local directory exists
        local.parent.mkdir(parents=True, exist_ok=True)

        # Download the main file
        remote = self._remote_path(relative_path)
        try:
            self._download_single_file(remote, local)
            self._cache_times[cache_key] = time.time()
            logger.debug(f"Downloaded {relative_path} ({local.stat().st_size} bytes)")
        except Exception as e:
            raise IOError(f"Cannot download {relative_path}: {e}") from e

        # Download companion files (.cdx, .fpt)
        self._download_companions(relative_path)

        return local

    def _download_single_file(self, remote_path: str, local_path: Path):
        """Download a single file from SMB."""
        with smbclient.open_file(remote_path, mode='rb') as remote_f:
            with open(str(local_path), 'wb') as local_f:
                while True:
                    chunk = remote_f.read(65536)
                    if not chunk:
                        break
                    local_f.write(chunk)

    def _download_companions(self, relative_path: str):
        """Download companion files (.cdx, .fpt) for a DBF file."""
        base = relative_path.rsplit('.', 1)[0] if '.' in relative_path else relative_path

        for ext in self.COMPANION_EXTENSIONS:
            companion_rel = f"{base}{ext}"
            companion_remote = self._remote_path(companion_rel)
            companion_local = self._local_path(companion_rel)

            try:
                # Check if companion exists on remote
                smbclient.stat(companion_remote)
                # Download it
                self._download_single_file(companion_remote, companion_local)
                logger.debug(f"Downloaded companion {companion_rel}")
            except (OSError, SMBResponseException):
                # Companion doesn't exist — that's fine
                pass

    def upload_file(self, relative_path: str):
        """
        Upload a local file back to SMB share.
        Also uploads companion files (.cdx, .fpt) if they exist locally.

        Args:
            relative_path: Path relative to share base
        """
        self._ensure_connected()

        local = self._local_path(relative_path)
        if not local.exists():
            raise FileNotFoundError(f"Local file not found: {local}")

        remote = self._remote_path(relative_path)
        try:
            self._upload_single_file(local, remote)
            logger.debug(f"Uploaded {relative_path} ({local.stat().st_size} bytes)")
        except Exception as e:
            raise IOError(f"Cannot upload {relative_path}: {e}") from e

        # Upload companion files
        self._upload_companions(relative_path)

    def _upload_single_file(self, local_path: Path, remote_path: str):
        """Upload a single file to SMB."""
        with open(str(local_path), 'rb') as local_f:
            with smbclient.open_file(remote_path, mode='wb') as remote_f:
                while True:
                    chunk = local_f.read(65536)
                    if not chunk:
                        break
                    remote_f.write(chunk)

    def _upload_companions(self, relative_path: str):
        """Upload companion files (.cdx, .fpt) if they exist locally."""
        base = relative_path.rsplit('.', 1)[0] if '.' in relative_path else relative_path

        for ext in self.COMPANION_EXTENSIONS:
            companion_rel = f"{base}{ext}"
            companion_local = self._local_path(companion_rel)
            if companion_local.exists():
                companion_remote = self._remote_path(companion_rel)
                try:
                    self._upload_single_file(companion_local, companion_remote)
                    logger.debug(f"Uploaded companion {companion_rel}")
                except Exception as e:
                    logger.warning(f"Failed to upload companion {companion_rel}: {e}")

    def upload_modified_files(self, relative_paths: List[str]):
        """
        Batch upload all modified files back to SMB.
        Used by Opera3FoxProImport._close_all_tables().

        NOTE: True atomic multi-file upload over SMB is not possible.
        Files are uploaded sequentially. If upload N fails, files 1..N-1
        are already on the server. This is acceptable because:
        - The remote files had OLD versions before, so partial upload
          is better than no upload (data already written locally)
        - The user can retry to complete the upload

        Args:
            relative_paths: List of relative paths to upload
        """
        errors = []
        for rel_path in relative_paths:
            try:
                self.upload_file(rel_path)
            except Exception as e:
                errors.append(f"{rel_path}: {e}")

        if errors:
            raise IOError(
                f"Failed to upload {len(errors)} file(s): " + "; ".join(errors)
            )

    def invalidate(self, relative_path: str):
        """Remove a file from the local cache so next access re-downloads."""
        cache_key = relative_path.lower()
        self._cache_times.pop(cache_key, None)

    def get_local_base(self) -> Optional[Path]:
        """Get the local temp directory base path (for use as opera3_base_path)."""
        return self._local_base

    def resolve_dbf_path(self, data_path: Path, table_name: str) -> Optional[Path]:
        """
        Download a DBF file from SMB and return the local path.
        Called by Opera3Reader._get_dbf_path() and Opera3FoxProImport._get_dbf_path().

        Figures out the relative path from the local base to construct the SMB path.

        Args:
            data_path: The local data_path the reader/importer was initialized with
            table_name: Table name (e.g., "pname")

        Returns:
            Local path to the downloaded DBF file, or None if not found on SMB
        """
        if not self._connected or not self._local_base:
            return None

        # Calculate relative path from local base
        try:
            rel_dir = data_path.relative_to(self._local_base)
        except ValueError:
            # data_path is not under our temp directory — SMB not applicable
            return None

        # Try different case combinations
        for name_variant in [f"{table_name.lower()}.dbf", f"{table_name.upper()}.DBF"]:
            rel_path = str(rel_dir / name_variant).replace("\\", "/")
            remote = self._remote_path(rel_path)
            try:
                smbclient.stat(remote)
                # File exists on remote — download it
                return self.download_file(rel_path)
            except (OSError, SMBResponseException):
                continue

        # Try glob-like search on the remote directory
        try:
            remote_dir = self._remote_path(str(rel_dir))
            entries = smbclient.listdir(remote_dir)
            for entry in entries:
                if entry.lower() == f"{table_name.lower()}.dbf":
                    rel_path = str(rel_dir / entry).replace("\\", "/")
                    return self.download_file(rel_path)
        except Exception:
            pass

        return None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False

    @classmethod
    def from_unc_path(cls, unc_path: str, username: str, password: str,
                      cache_ttl: int = 30) -> 'SMBFileManager':
        """
        Create an SMBFileManager from a UNC path like \\\\server\\share\\subfolder.

        Args:
            unc_path: UNC path (e.g., "\\\\172.17.172.214\\O3 Server VFP\\subfolder")
            username: SMB username
            password: SMB password
            cache_ttl: Cache TTL in seconds

        Returns:
            Configured SMBFileManager (not yet connected)
        """
        clean = unc_path.replace('\\', '/')
        parts = [p for p in clean.split('/') if p]
        if len(parts) < 2:
            raise ValueError(f"Invalid UNC path: {unc_path}. Expected \\\\server\\share")

        server = parts[0]
        share = parts[1]
        base_path = '/'.join(parts[2:]) if len(parts) > 2 else ''

        return cls(server=server, share=share, username=username,
                   password=password, base_path=base_path, cache_ttl=cache_ttl)


# =========================================================================
# Module-level singleton
# =========================================================================

_smb_manager: Optional[SMBFileManager] = None


def get_smb_manager() -> Optional[SMBFileManager]:
    """Get the active SMB file manager singleton, or None if not configured."""
    return _smb_manager


def set_smb_manager(manager: Optional[SMBFileManager]):
    """Set or clear the active SMB file manager singleton."""
    global _smb_manager
    # Disconnect old manager if replacing
    if _smb_manager is not None and _smb_manager is not manager:
        try:
            _smb_manager.disconnect()
        except Exception:
            pass
    _smb_manager = manager
```

- [ ] **Step 2: Verify module imports cleanly**

Run: `cd /Users/maccb/llmragsql && python -c "from sql_rag.smb_access import SMBFileManager, get_smb_manager, set_smb_manager; print('OK')"`
Expected: `OK` (or a warning about smbprotocol if not installed yet)

- [ ] **Step 3: Commit**

```bash
git add sql_rag/smb_access.py
git commit -m "Add SMBFileManager for direct Python SMB access to Opera 3"
```

---

### Task 3: Integrate SMB into `Opera3Reader`

**Files:**
- Modify: `sql_rag/opera3_foxpro.py`

The key change is in `_get_dbf_path()` — before raising `FileNotFoundError`, check the SMB singleton. Also update `Opera3System` to download `seqco.dbf` via SMB. Update the singleton factories to set `data_path` from SMB when active.

- [ ] **Step 1: Add SMB import at top of file**

After the existing imports (line 27), add:

```python
from sql_rag.smb_access import get_smb_manager
```

Wrap in try/except for when smb_access module isn't available:

```python
try:
    from sql_rag.smb_access import get_smb_manager
except ImportError:
    def get_smb_manager():
        return None
```

- [ ] **Step 2: Modify `Opera3Reader.__init__` to relax path existence check when SMB is active**

Currently line 129-130:
```python
if not self.data_path.exists():
    logger.warning(f"Opera 3 data path does not exist: {data_path}")
```

Change to:
```python
if not self.data_path.exists():
    smb = get_smb_manager()
    if smb is None:
        logger.warning(f"Opera 3 data path does not exist: {data_path}")
    else:
        # SMB mode — create local directory structure
        self.data_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"SMB mode: created local cache directory {data_path}")
```

- [ ] **Step 3: Modify `Opera3Reader._get_dbf_path()` to use SMB fallback**

Currently (lines 348-368), the method tries local paths and raises `FileNotFoundError`. Add SMB fallback before the final raise:

```python
def _get_dbf_path(self, table_name: str) -> Path:
    """Get the path to a DBF file, downloading from SMB if needed."""
    # Try lowercase first
    dbf_path = self.data_path / f"{table_name.lower()}.dbf"
    if dbf_path.exists():
        return dbf_path

    # Try uppercase
    dbf_path = self.data_path / f"{table_name.upper()}.DBF"
    if dbf_path.exists():
        return dbf_path

    # Try mixed case
    for f in self.data_path.glob("*.dbf"):
        if f.stem.lower() == table_name.lower():
            return f
    for f in self.data_path.glob("*.DBF"):
        if f.stem.lower() == table_name.lower():
            return f

    # SMB fallback — download from remote if available
    smb = get_smb_manager()
    if smb is not None:
        local_path = smb.resolve_dbf_path(self.data_path, table_name)
        if local_path is not None:
            return local_path

    raise FileNotFoundError(f"Table not found: {table_name}")
```

- [ ] **Step 4: Modify `_get_table_info()` to use `_get_dbf_path()`**

Currently `_get_table_info()` (lines 176-219) does its own path resolution (`self.data_path / f"{table_name}.dbf"`) and does NOT go through `_get_dbf_path()`. This means it will fail under SMB. Fix by delegating to `_get_dbf_path()`:

Replace lines 181-187:
```python
dbf_path = self.data_path / f"{table_name}.dbf"
if not dbf_path.exists():
    # Try uppercase
    dbf_path = self.data_path / f"{table_name.upper()}.DBF"

if not dbf_path.exists():
    raise FileNotFoundError(f"Table not found: {table_name}")
```

With:
```python
dbf_path = self._get_dbf_path(table_name)
```

This reuses the same resolution logic (lowercase, uppercase, mixed case, SMB fallback).

- [ ] **Step 5: Modify `Opera3System.__init__` to create local dirs when SMB active**

Add after line 497 (`self.encoding = encoding`):

```python
if not self.system_path.exists():
    smb = get_smb_manager()
    if smb is not None:
        self.system_path.mkdir(parents=True, exist_ok=True)
```

- [ ] **Step 6: Modify `Opera3System.get_companies` to download seqco.dbf via SMB**

Before the `FileNotFoundError` raise (around line 518), add SMB download:

```python
if not seqco_path.exists():
    # Try uppercase
    seqco_path = self.system_path / "SEQCO.DBF"

if not seqco_path.exists():
    # SMB fallback — try downloading
    smb = get_smb_manager()
    if smb is not None:
        try:
            rel_dir = self.system_path.relative_to(smb.get_local_base())
            for name in ["seqco.dbf", "SEQCO.DBF"]:
                rel_path = str(rel_dir / name).replace("\\", "/")
                try:
                    downloaded = smb.download_file(rel_path)
                    seqco_path = downloaded
                    break
                except (IOError, OSError):
                    continue
        except (ValueError, AttributeError):
            pass

if not seqco_path.exists():
    raise FileNotFoundError(f"Company file not found: {seqco_path}")
```

- [ ] **Step 7: Verify the module still imports and reader works with local paths**

Run: `cd /Users/maccb/llmragsql && python -c "from sql_rag.opera3_foxpro import Opera3Reader, Opera3System; print('OK')"`
Expected: `OK`

- [ ] **Step 8: Commit**

```bash
git add sql_rag/opera3_foxpro.py
git commit -m "Add SMB fallback to Opera3Reader and Opera3System file resolution"
```

---

### Task 4: Integrate SMB into `Opera3FoxProImport`

**Files:**
- Modify: `sql_rag/opera3_foxpro_import.py`

The import module needs to: (1) download DBF files via SMB before opening, (2) upload all modified files back when closing.

- [ ] **Step 1: Add SMB import at top of file**

After the existing imports (around line 38), add:

```python
try:
    from sql_rag.smb_access import get_smb_manager
except ImportError:
    def get_smb_manager():
        return None
```

- [ ] **Step 2: Add modified-tables tracking to `__init__`**

After `self._financial_year_cache = None` (line 233), add:

```python
self._modified_tables: List[str] = []  # table names modified during this session
```

- [ ] **Step 3: Modify `Opera3FoxProImport.__init__` to relax path check when SMB active**

Currently line 235-236:
```python
if not self.data_path.exists():
    raise FileNotFoundError(f"Opera 3 data path not found: {data_path}")
```

Change to:
```python
if not self.data_path.exists():
    smb = get_smb_manager()
    if smb is not None:
        self.data_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"SMB mode: created local directory {data_path}")
    else:
        raise FileNotFoundError(f"Opera 3 data path not found: {data_path}")
```

- [ ] **Step 4: Modify `_get_dbf_path()` to use SMB fallback**

Same pattern as Opera3Reader. After the existing search loops and before the `raise FileNotFoundError`, add:

```python
# SMB fallback
smb = get_smb_manager()
if smb is not None:
    local_path = smb.resolve_dbf_path(self.data_path, table_name)
    if local_path is not None:
        return local_path

raise FileNotFoundError(f"Table not found: {table_name}")
```

- [ ] **Step 5: Modify `_open_table()` to track opened tables**

After `table.open(dbf.READ_WRITE)` (line 355), no change needed for download — `_get_dbf_path()` already handles that. But track the table name for upload:

After `self._table_cache[table_name] = table` (line 356), the table is already tracked. We just need to track which tables get modified. Rather than tracking in `_open_table`, track in the actual write methods. But since every open table could be modified, the simplest approach is to track all opened tables.

Replace the current `_open_table` (lines 348-357) with:

```python
def _open_table(self, table_name: str) -> Any:
    """Open a DBF table for reading/writing"""
    if table_name in self._table_cache:
        return self._table_cache[table_name]

    dbf_path = self._get_dbf_path(table_name)
    table = dbf.Table(str(dbf_path), codepage=self.encoding)
    table.open(dbf.READ_WRITE)
    self._table_cache[table_name] = table

    # Track for SMB upload on close
    if table_name not in self._modified_tables:
        self._modified_tables.append(table_name)

    return table
```

- [ ] **Step 6: Modify `_close_all_tables()` to upload via SMB**

Replace the current implementation (lines 359-366):

```python
def _close_all_tables(self):
    """Close all open tables and upload modified files to SMB if active."""
    for table in self._table_cache.values():
        try:
            table.close()
        except Exception:
            pass

    # Upload modified tables back to SMB
    # Note: fcntl.flock() locking is local-only — it prevents concurrent local
    # Python processes from colliding but does NOT lock on the SMB server.
    smb = get_smb_manager()
    if smb is not None and self._modified_tables:
        try:
            # Compute relative dir once (same for all tables)
            try:
                rel_dir = self.data_path.relative_to(smb.get_local_base())
            except (ValueError, AttributeError):
                rel_dir = None  # Not an SMB-managed path

            rel_paths = []
            if rel_dir is not None:
                for table_name in self._modified_tables:
                    for name_variant in [f"{table_name.lower()}.dbf", f"{table_name.upper()}.DBF"]:
                        local_file = self.data_path / name_variant
                        if local_file.exists():
                            rel_path = str(rel_dir / name_variant).replace("\\", "/")
                            rel_paths.append(rel_path)
                            break
            if rel_paths:
                smb.upload_modified_files(rel_paths)
                logger.info(f"Uploaded {len(rel_paths)} modified tables to SMB")
        except Exception as e:
            logger.error(f"Failed to upload modified tables to SMB: {e}")
            raise IOError(
                f"Transaction saved locally but failed to upload to server: {e}. "
                f"Please retry the operation."
            ) from e

    self._table_cache.clear()
    self._modified_tables.clear()
```

- [ ] **Step 7: Verify the module still imports**

Run: `cd /Users/maccb/llmragsql && python -c "from sql_rag.opera3_foxpro_import import Opera3FoxProImport; print('OK')"`
Expected: `OK`

- [ ] **Step 8: Commit**

```bash
git add sql_rag/opera3_foxpro_import.py
git commit -m "Add SMB download/upload to Opera3FoxProImport table operations"
```

---

### Task 5: Update API config endpoints

**Files:**
- Modify: `api/main.py`

Replace `_mount_opera3_share()` with SMB manager creation. Update config, test, and companies endpoints.

- [ ] **Step 1: Add SMB import near top of api/main.py**

Near the other sql_rag imports, add:

```python
try:
    from sql_rag.smb_access import SMBFileManager, get_smb_manager, set_smb_manager
    SMB_AVAILABLE = True
except ImportError:
    SMB_AVAILABLE = False
```

- [ ] **Step 2: Replace `_mount_opera3_share()` with `_connect_opera3_smb()`**

Replace the entire `_mount_opera3_share` function (lines 872-919) with:

```python
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

        local_base = manager.get_local_base()
        logger.info(f"SMB connected, local cache: {local_base}")
        return f"Connected via SMB, local cache: {local_base}"

    except Exception as e:
        logger.error(f"SMB connection failed: {e}")
        return f"SMB connection failed: {e}"
```

- [ ] **Step 3: Update `update_opera_config()` endpoint**

Replace the mount logic (lines 2125-2138). Change:

```python
    # Auto-mount SMB share if Opera 3 with server path and credentials
    mount_message = ""
    if (opera_config.version == "opera3"
        and opera_config.opera3_server_path
        and opera_config.opera3_share_user
        and opera_config.opera3_share_password):
        try:
            mount_message = _mount_opera3_share(
                opera_config.opera3_server_path,
                opera_config.opera3_share_user,
                opera_config.opera3_share_password
            )
        except Exception as e:
            mount_message = f"Share mount failed: {e}"
```

To:

```python
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
```

And update the return message:
```python
    return {"success": True, "message": f"Opera configuration updated. {smb_message}".strip()}
```

- [ ] **Step 4: Update `get_opera3_companies()` endpoint**

The current endpoint reads `opera3_base_path` from config. When SMB is active, this already points to the local temp dir (set in step 3). But the `Opera3System` also needs the SMB singleton to download `seqco.dbf`. Since we modified `Opera3System` in Task 3 to check the singleton, this should work automatically.

No changes needed to the endpoint logic — just verify it works.

- [ ] **Step 5: Update `test_opera_connection()` endpoint**

Replace the Opera 3 test section (lines 2190-2201) to support SMB:

```python
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
                    # Use SMB local base as the path
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
            companies = system.get_companies()
            if not companies:
                return {"success": False, "error": "No companies found in Opera 3 installation"}
```

(Continue with the rest of the existing test logic, replacing references to `opera_config.opera3_base_path` with `test_base_path`.)

- [ ] **Step 6: Add SMB auto-connect on startup**

In the app startup/lifespan code, after config is loaded, add SMB connection if configured. Find where `load_config()` is called during startup and add after it:

```python
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
```

- [ ] **Step 7: Commit**

```bash
git add api/main.py
git commit -m "Replace OS-level SMB mount with smbprotocol SMBFileManager in API"
```

---

### Task 6: Clean up stale temp directories on startup

**Files:**
- Modify: `sql_rag/smb_access.py`

- [ ] **Step 1: Add cleanup function**

Add to `smb_access.py` after the singleton functions:

```python
def cleanup_stale_temp_dirs(max_age_hours: int = 24):
    """Remove stale opera3_smb temp directories older than max_age_hours."""
    import glob
    temp_dir = tempfile.gettempdir()
    cutoff = time.time() - (max_age_hours * 3600)

    for path in glob.glob(os.path.join(temp_dir, "opera3_smb_*")):
        try:
            if os.path.isdir(path) and os.path.getmtime(path) < cutoff:
                shutil.rmtree(path)
                logger.info(f"Cleaned stale temp dir: {path}")
        except Exception as e:
            logger.debug(f"Could not clean {path}: {e}")
```

- [ ] **Step 2: Call cleanup on startup in api/main.py**

In the startup code (same area as Task 5 Step 6), add before the SMB connect:

```python
# Clean up stale SMB temp directories
try:
    from sql_rag.smb_access import cleanup_stale_temp_dirs
    cleanup_stale_temp_dirs()
except ImportError:
    pass
```

- [ ] **Step 3: Commit**

```bash
git add sql_rag/smb_access.py api/main.py
git commit -m "Add stale temp directory cleanup on startup"
```

---

### Task 7: End-to-end verification

- [ ] **Step 1: Verify app starts without SMB configured**

Run: `cd /Users/maccb/llmragsql && python -c "from sql_rag.smb_access import get_smb_manager; assert get_smb_manager() is None; print('No SMB singleton — OK')"`

- [ ] **Step 2: Verify SMBFileManager can be created from UNC path**

Run: `cd /Users/maccb/llmragsql && python -c "
from sql_rag.smb_access import SMBFileManager
mgr = SMBFileManager.from_unc_path(r'\\\\172.17.172.214\\O3 Server VFP', 'user', 'pass')
print(f'Server: {mgr.server}, Share: {mgr.share}')
print('UNC parsing OK')
"`
Expected: `Server: 172.17.172.214, Share: O3 Server VFP`

- [ ] **Step 3: Verify Opera3Reader still works with local paths (no regression)**

Run: `cd /Users/maccb/llmragsql && python -c "from sql_rag.opera3_foxpro import Opera3Reader; print('Reader import OK')"`

- [ ] **Step 4: Verify Opera3FoxProImport still works with local paths (no regression)**

Run: `cd /Users/maccb/llmragsql && python -c "from sql_rag.opera3_foxpro_import import Opera3FoxProImport; print('Import module OK')"`

- [ ] **Step 5: Verify API imports cleanly**

Run: `cd /Users/maccb/llmragsql && python -c "import api.main; print('API import OK')"`

- [ ] **Step 6: Test actual SMB connection** (requires network access to Windows server)

From the Installations page in the UI:
1. Enter the server path (e.g., `\\172.17.172.214\O3 Server VFP`)
2. Enter SMB username and password
3. Click "Test Connection"
4. Verify companies are listed
5. Select a company and verify data loads

- [ ] **Step 7: Final commit if any fixes needed**

```bash
git add -A
git commit -m "SMB FoxPro access: end-to-end verification fixes"
```
