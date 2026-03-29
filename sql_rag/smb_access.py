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
            bp = self.base_path.replace('/', '\\')
            base += f"\\{bp}"
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
            smbclient.stat(test_path)
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
            smbclient.stat(test_path)
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
