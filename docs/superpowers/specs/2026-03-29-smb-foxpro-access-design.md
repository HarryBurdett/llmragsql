# SMB Access to Opera 3 FoxPro Folders

**Date:** 2026-03-29
**Status:** Approved

## Problem

The application runs on macOS and needs to read/write Opera 3 FoxPro DBF files hosted on a Windows server via SMB. The current approach uses OS-level mount commands (`mount_smbfs` on macOS, `mount -t cifs` on Linux) which is unreliable on macOS and requires elevated permissions.

## Solution

Replace OS-level SMB mounting with direct Python SMB access using the `smbprotocol` library. A new `SMBFileManager` class downloads DBF files to a local temp directory for reading, and uploads modified files back after writes. This is transparent to the rest of the codebase — `Opera3Reader` and write paths continue working with local file paths.

## Design

### New Module: `sql_rag/smb_access.py`

**`SMBFileManager` class:**

```
SMBFileManager
├── __init__(server, share, username, password, base_path="")
├── connect() → bool
├── disconnect()
├── list_dir(remote_path) → list[str]
├── download_file(remote_path, force_fresh=False) → Path  # returns local temp path
├── download_file_set(remote_paths) → dict[str, Path]     # bulk download with associated files
├── upload_file(local_path, remote_path)                   # writes back to SMB
├── upload_modified_files(file_paths: list)                # batch upload all modified files
├── invalidate(filename)                                   # clear cache for a specific file
├── is_connected() → bool
└── context manager support (__enter__/__exit__)
```

**Configuration:** Uses existing `OperaConfig` fields — no new settings needed:
- `opera3_server_path` → parsed to extract server + share name (e.g., `\\172.17.172.214\O3 Server VFP` → server=`172.17.172.214`, share=`O3 Server VFP`)
- `opera3_share_user` → SMB username
- `opera3_share_password` → SMB password

**`opera3_base_path` handling:** When SMB is active, `opera3_base_path` is automatically set to the local temp directory path. This keeps existing code that reads `opera3_base_path` working without changes. The field is derived, not user-configured, when SMB mode is active.

### Associated File Handling

FoxPro DBF files have companion files that must travel together:
- `.cdx` — compound index files
- `.fpt` — memo field files

When downloading or uploading a `.dbf` file, the SMB manager automatically includes any associated `.cdx` and `.fpt` files with the same base name. This is handled transparently in `download_file()` and `upload_file()`.

### Caching Strategy

- Temp directory created via `tempfile.mkdtemp(prefix="opera3_smb_", dir="/tmp")` with `0o700` permissions (restricts access to current user)
- Mirrors remote directory structure locally
- Default TTL: 30 seconds (configurable) — skip re-download if file is fresh
- `force_fresh=True` parameter bypasses cache (for critical reads like duplicate detection, balance checks)
- Writes invalidate cache for that file immediately
- Full cache clear on disconnect or config change
- Stale temp directories (>24h) cleaned on app startup

### Integration: `Opera3Reader`

`Opera3Reader.__init__` gains an optional `smb_manager` parameter:

```python
Opera3Reader(data_path, smb_manager=None)
```

- `_get_dbf_path()` checks `smb_manager` first — if present, downloads the file (+ .cdx/.fpt) and returns the local temp path
- If no `smb_manager`, works exactly as today (local path)

### Integration: `Opera3System`

Same pattern — accepts optional `smb_manager`. Uses it to download `System/seqco.dbf` for company discovery.

### Integration: Write Paths (`opera3_foxpro_import.py`)

**Critical design: batch write, not per-file context manager.**

`Opera3FoxProImport` opens multiple tables simultaneously (up to 11 during a single import), caches them open via `_open_table()`, appends/updates records, then calls `_close_all_tables()` in a `finally` block.

The SMB integration hooks into this existing pattern:

1. **`_open_table(table_name)`** — calls `smb_manager.download_file()` to get a local path, then opens the table via the `dbf` library as before
2. **`_close_all_tables()`** — after closing all local table handles, calls `smb_manager.upload_modified_files()` to batch-upload all modified files back to the SMB share
3. **Atomicity** — if any upload fails, no partial state on the server. The remote files remain unchanged (old versions). The user retries the operation. Local temp files are cleaned up.

```python
# In _open_table():
if self.smb_manager:
    local_path = self.smb_manager.download_file(f"data/{table_name}.dbf")
    table = dbf.Table(str(local_path))
else:
    table = dbf.Table(str(self.data_path / f"{table_name}.dbf"))

# In _close_all_tables():
for table in self._open_tables.values():
    table.close()
if self.smb_manager:
    self.smb_manager.upload_modified_files(self._modified_tables)
```

### Integration: API (`api/main.py`)

- `_mount_opera3_share()` replaced by `SMBFileManager` instantiation
- SMB manager stored as singleton (per company), reused across requests
- `POST /api/config/opera` creates/recreates SMB manager when credentials change
- `POST /api/config/opera/test` tests SMB connectivity directly
- `GET /api/config/opera/companies` uses SMB manager for company discovery

### Error Handling

- Connection failures: clear message ("Cannot connect to Opera 3 server at X — check credentials and network")
- Mid-request disconnection: retry once, then fail with actionable error
- Write upload failures: no partial state on server — all uploads succeed or none do
- All errors surfaced via existing `friendly_db_error()` pattern
- Connection health: `is_connected()` detects stale connections; auto-reconnects on next operation

### Logging

All SMB operations are logged:
- File downloads/uploads with file sizes and durations
- Connection events (connect, disconnect, reconnect)
- Cache hits/misses
- Upload failures with details

### Architecture Diagram

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ Installations│────▶│  POST /config/   │────▶│  SMBFileManager   │
│  Settings UI │     │  opera           │     │  (smbprotocol)    │
│  (existing)  │     │  (replaces mount)│     │                   │
└─────────────┘     └──────────────────┘     │  connect()        │
                                              │  download_file()  │
┌─────────────┐     ┌──────────────────┐     │  upload_file()    │
│Opera3Reader │────▶│  _get_dbf_path() │────▶│  cache (TTL 30s)  │
│Opera3System │     │  checks smb_mgr  │     └────────┬──────────┘
└─────────────┘     └──────────────────┘              │
                                                       ▼
┌──────────────┐    ┌──────────────────┐     ┌──────────────────┐
│Opera3FoxPro  │───▶│  _open_table()   │────▶│  /tmp/opera3_smb  │
│  Import      │    │  download first   │     │  (local cache,    │
│              │───▶│  _close_all()    │────▶│   0o700 perms)    │
│              │    │  upload modified  │     └──────────────────┘
└──────────────┘    └──────────────────┘
```

## Files to Create

| File | Purpose |
|------|---------|
| `sql_rag/smb_access.py` | `SMBFileManager` class |

## Files to Modify

| File | Change |
|------|--------|
| `sql_rag/opera3_foxpro.py` | `Opera3Reader` and `Opera3System` accept optional `smb_manager` |
| `sql_rag/opera3_foxpro_import.py` | `_open_table()` downloads via SMB, `_close_all_tables()` uploads modified files |
| `sql_rag/opera3_data_provider.py` | Pass `smb_manager` through to `Opera3Reader` |
| `sql_rag/opera_data_provider.py` | Factory `create_data_provider()` supports `smb_manager` kwarg |
| `sql_rag/bank_import_opera3.py` | `Opera3FoxProImport` instantiation passes `smb_manager` |
| `api/main.py` | Replace `_mount_opera3_share()` with `SMBFileManager`, update config/test/companies endpoints, set `opera3_base_path` to temp dir when SMB active |
| `requirements.txt` | Add `smbprotocol` |

## Files Unchanged

- Frontend (`Installations.tsx`) — settings fields already exist
- All SQL SE code paths

## Locking Considerations

The existing `fcntl.flock()` locking in `Opera3FoxProImport` is local-only — it prevents concurrent access from multiple Python processes on the same machine. It does NOT prevent another machine from accessing the same remote files via SMB simultaneously.

For the initial implementation, this is acceptable because:
- Opera 3 is typically single-user or few-users
- The download-modify-upload pattern is short-lived
- Opera's own application handles its locking separately

If concurrent SMB write access becomes a concern, a future enhancement could use `smbprotocol`'s file open modes to acquire SMB-level locks on remote files during writes.

## Dependencies

- `smbprotocol` (pip install) — pure Python, SMBv2/v3 support
