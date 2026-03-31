# Opera 3 Write Agent — Production Design Specification

**Date:** 2026-03-31
**Status:** Approved
**Priority:** Critical — blocks all Opera 3 write operations

---

## 1. Problem

Opera 3 stores data as FoxPro DBF files on a Windows server. Our application runs on macOS and accesses these files via SMB (network share). The download-modify-upload pattern over SMB is NOT safe for multi-user environments:

- Two users can download the same file simultaneously
- Changes from one user overwrite the other's changes
- No file-level or record-level locking across the network
- Risk of data corruption, partial writes, and lost transactions

**This is unacceptable for a finance system.** All Opera 3 write operations are currently blocked.

## 2. Solution

A **self-contained Write Agent** service that runs directly on the Opera 3 Windows server alongside the DBF files. It:

- Writes locally to FoxPro files with proper locking (no network transfer)
- Uses Harbour DBFCDX for correct index maintenance
- Exposes a REST API that our application calls remotely
- Handles concurrent access safely via record-level locking
- Includes crash recovery via Write-Ahead Log
- Verifies every write and compensates on failure

**Reading remains via SMB** (safe for multi-user). Only writes go through the Agent.

## 3. Architecture

```
┌──────────────────┐         HTTP/REST          ┌─────────────────────────┐
│  Main Application │ ──────────────────────────▶│  Opera 3 Write Agent    │
│  (macOS)          │         port 9000          │  (Windows Server)       │
│                   │◀──────────────────────────│                         │
│  SMB read-only   ─┤         results            │  Local DBF access       │
│  for data viewing │                            │  Harbour DBFCDX locking │
└──────────────────┘                             │  Write-Ahead Log        │
                                                  │  Post-write verification│
         ┌──────────┐                             └────────┬────────────────┘
         │ Opera 3  │                                       │
         │ Users    │── direct FoxPro access ───────────────┤
         └──────────┘                                       │
                                                             ▼
                                                  ┌─────────────────────┐
                                                  │  Opera 3 DBF Files  │
                                                  │  (Local Filesystem) │
                                                  └─────────────────────┘
```

## 4. Deployment Package

**Self-contained zip** (`opera3-write-agent-setup.zip`) — no prerequisites required on the server.

```
opera3-write-agent/
├── install.bat              — One-click installer
├── uninstall.bat            — Clean removal
├── python/                  — Embedded Python 3.11 runtime
├── agent/                   — Write Agent service code
│   ├── service.py           — FastAPI application
│   ├── harbour_dbf.py       — Harbour DBFCDX Python wrapper
│   ├── transaction_safety.py — Verification & compensation
│   ├── write_ahead_log.py   — Crash recovery audit trail
│   └── opera3_config.py     — Opera 3 configuration reader
├── harbour/
│   └── libdbfbridge.dll     — Pre-compiled Harbour DBFCDX library
├── nssm.exe                 — Windows Service wrapper (Non-Sucking Service Manager)
└── README.txt               — Quick start guide
```

### Installation Process (`install.bat`)

1. Auto-detect Opera 3 data path (scans `C:\Apps\O3 Server VFP`, registry, common locations)
2. Prompt user to confirm/enter path if not found
3. Install Python dependencies into embedded Python
4. Generate random agent key (shared secret)
5. Register as Windows Service (`Opera3WriteAgent`) via NSSM
6. Configure automatic startup on boot
7. Start the service
8. Run health check to confirm operational
9. Display: URL, port, and agent key for entering in the Installations page

### Uninstallation (`uninstall.bat`)

1. Stop the service
2. Remove from Windows Services
3. Optionally remove the installation folder

## 5. Windows Service

- **Service Name:** `Opera3WriteAgent`
- **Start Type:** Automatic (starts on boot)
- **Port:** 9000 (configurable)
- **Resources:** Lightweight — idles when no requests, uses ~50MB RAM
- **Logging:** Writes to `opera3-write-agent/logs/agent.log`
- **Recovery:** Windows Service restarts automatically on crash

## 6. API Endpoints

### Authentication
All endpoints require `X-Agent-Key` header matching the shared secret configured during installation.

### Transaction Import Endpoints

| Endpoint | Purpose | Tables Written |
|----------|---------|----------------|
| `POST /import/purchase-payment` | Payment to supplier | aentry, atran, ntran (2x), ptran, pname, nbank, nacnt, nhist, anoml, atype |
| `POST /import/sales-receipt` | Receipt from customer | aentry, atran, ntran (2x), stran, sname, nbank, nacnt, nhist, anoml, atype |
| `POST /import/sales-refund` | Refund to customer | aentry, atran, ntran (2x), stran, sname, nbank, nacnt, nhist, anoml, atype |
| `POST /import/purchase-refund` | Refund from supplier | aentry, atran, ntran (2x), ptran, pname, nbank, nacnt, nhist, anoml, atype |
| `POST /import/bank-transfer` | Inter-bank transfer | aentry (2x), atran (2x), ntran (2x), anoml (2x), nacnt (2x), nbank (2x), atype |
| `POST /import/nominal-entry` | Direct nominal posting | aentry, atran, ntran (2x), anoml, nacnt, nhist, nbank, atype |
| `POST /import/gocardless-batch` | GoCardless batch | Multiple aentry, atran, stran, ntran, anoml, nacnt, nbank, sname, zvtran, nvat |
| `POST /import/recurring-entry` | Recurring entries | aentry, atran, ntran, anoml, stran/ptran, nacnt, nbank |

### Allocation Endpoints

| Endpoint | Purpose |
|----------|---------|
| `POST /allocate/receipt` | Auto-allocate customer receipt to invoices |
| `POST /allocate/payment` | Auto-allocate supplier payment to invoices |

### Reconciliation Endpoints

| Endpoint | Purpose |
|----------|---------|
| `POST /reconcile/mark` | Mark cashbook entries as reconciled |
| `POST /check/duplicate` | Pre-posting duplicate detection |

### Monitoring Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Health check — called by main app periodically |
| `GET /status` | Table accessibility and lock status |
| `GET /wal/stats` | Write-Ahead Log statistics |
| `GET /wal/recent` | Recent operations |

## 7. Locking Strategy — CRITICAL

### Principles
1. **Record-level locks (RLOCK)** not table-level locks (FLOCK) — other Opera users continue working
2. **Check for existing locks before writing** — if locked, wait with timeout
3. **5-second lock timeout** — return clear error if lock can't be acquired, never force
4. **Release locks immediately** after each table write
5. **Consistent lock order** — always: aentry → atran → ntran → ptran/stran → nacnt → nbank → anoml
6. **Prepare ALL data before acquiring any lock** — lookups, validations, ID generation done first

### Lock Lifecycle
```
1. PREPARE: Read config, generate IDs, validate accounts, calculate amounts
   (NO locks held — safe to take as long as needed)

2. WRITE: Open tables → RLOCK → write → unlock → close
   (MILLISECONDS — absolute minimum duration)

3. VERIFY: Read back written records to confirm integrity
   (NO locks held — read with NOLOCK equivalent)
```

### Interaction with Opera Users
- If an Opera user is entering a transaction and has records locked, our Write Agent WAITS (up to 5s)
- If our Write Agent has records locked, Opera WAITS briefly
- Lock duration is milliseconds — Opera users will not notice
- If timeout exceeded: "Table X is locked by another user — please try again"

### Deadlock Prevention
- All table locks acquired in the SAME ORDER every time
- Prevents circular wait between Opera and Write Agent
- If deadlock detected: release all locks, wait 100ms, retry once

## 8. Transaction Safety — 3 Layers

### Layer 1: Write-Ahead Log (WAL)
- Every operation logged BEFORE writing to Opera
- If agent crashes mid-write, WAL enables recovery on restart
- SQLite database (separate from Opera data)
- Operation states: PENDING → IN_PROGRESS → VERIFYING → COMPLETED / FAILED

### Layer 2: Post-Write Verification
- After every import, reads back ALL expected records
- Confirms: correct table, correct values, correct count
- If verification fails → triggers compensation

### Layer 3: Compensation (Rollback)
- Soft-deletes any records that were partially written
- Ordered: reverse of write order (ntran → atran → aentry)
- Logs all compensation actions for audit
- If compensation fails → BLOCKS further writes until manual review

## 9. Performance Requirement

**Tables must be opened, written, and closed as fast as possible.**

- ALL data preparation happens BEFORE acquiring locks:
  - Journal numbers from nparm
  - Entry numbers from atype
  - Unique IDs generated
  - Control accounts looked up from sprfls/pprfls
  - Period calculated from nclndd
  - Account validation completed
  - Dormant check done

- The actual WRITE phase (lock → write → unlock) must complete in **milliseconds**
- Opera users must experience **zero noticeable delay**
- Under normal conditions, a complete transaction import takes < 500ms total

## 10. Configuration in Main App

### Installations Page — New Fields

Under the Opera 3 section:
- **Write Agent URL**: `http://172.17.172.214:9000`
- **Write Agent Key**: (shared secret from installation)
- **Test Connection** button — pings `/health` endpoint
- **Status indicator**: Connected / Disconnected / Error

### Help Text on Installations Page

```
Opera 3 Write Agent

The Write Agent is a service that runs on the Opera 3 server to safely
post transactions to the FoxPro database. It must be installed on the
same server as the Opera 3 data files.

Setup:
1. Copy the opera3-write-agent folder to the Opera 3 server
2. Run install.bat as Administrator
3. The installer will display the URL and Key
4. Enter the URL and Key below
5. Click Test Connection to verify

The Write Agent:
- Runs as a Windows Service (starts automatically)
- Uses proper FoxPro file locking (safe for multi-user)
- Does not interfere with Opera 3 users
- Includes crash recovery and audit logging

Without the Write Agent, Opera 3 is read-only in this application.
Bank statement viewing, reporting, and analysis work without it.
Posting transactions, importing, and reconciliation require it.
```

### App Behaviour Without Write Agent

- **Reading**: Works via SMB (viewing, reporting, analysis, scanning)
- **Writing**: Blocked with clear message — "Opera 3 Write Agent is not running"
- **Health check**: On app startup, checked periodically, and before any write
- **Warning banner**: Shown on any page that requires write access

## 11. Existing Code Status

The Write Agent code is **already substantially built** in `opera3_agent/`:

| Component | Status | Notes |
|-----------|--------|-------|
| `service.py` | Built | All endpoints implemented |
| `harbour_dbf.py` | Built | Python wrapper complete |
| `harbour/dbfbridge.prg` | Built | Harbour source, needs compilation |
| `transaction_safety.py` | Built | Verification + compensation |
| `write_ahead_log.py` | Built | SQLite WAL complete |
| `opera3_agent_client.py` | Built | HTTP client for main app |
| `opera3_write_provider.py` | Built | Routes agent vs direct access |
| `deploy.ps1` | Built | PowerShell deployment script |
| `harbour/libdbfbridge.dll` | NOT BUILT | Needs compilation on Windows |
| Embedded Python package | NOT BUILT | Needs creation |
| `install.bat` | Partial | Needs completion for self-contained deployment |
| Integration with Installations page | NOT BUILT | URL/key fields needed |
| Health check integration | NOT BUILT | App needs to check agent on startup |

## 12. What Needs To Be Done

### Phase 1: Package & Deploy
1. Create self-contained deployment package with embedded Python
2. Compile Harbour DBFCDX library for Windows (pre-compile the DLL)
3. Complete `install.bat` for one-click setup
4. Test deployment on 172.17.172.214

### Phase 2: Integration
5. Add Write Agent URL/Key fields to Installations page
6. Add help text to Installations page
7. Add health check on app startup — warn if agent unavailable
8. Route all Opera 3 writes through the agent client
9. Show "Write Agent not running" on pages that need write access

### Phase 3: Verification
10. Test every posting method with Transaction Snapshot tool
11. Test concurrent access (Opera user + Write Agent simultaneously)
12. Test crash recovery (kill agent mid-transaction, restart)
13. Test lock timeout behaviour
14. Verify against Opera 3 data integrity

## 13. Considerations Summary

| Consideration | How Addressed |
|---------------|---------------|
| Multi-user safety | Record-level locking via Harbour RLOCK |
| No interference with Opera users | 5s timeout, consistent lock order, millisecond write duration |
| Data integrity | WAL + post-write verification + compensation |
| No partial records | Verification catches missing records, compensation removes them |
| Crash recovery | WAL scanned on startup, incomplete operations recovered |
| Deployment simplicity | Self-contained zip, one-click install, no prerequisites |
| Configuration | URL/Key in Installations page with test button |
| Monitoring | Health endpoint, WAL stats, status checks |
| Performance | All data prepared before locks, write phase in milliseconds |
| Dormant accounts | Checked before posting (pre-existing check) |
| Duplicate detection | Reference + description + sign checks (pre-existing) |
| Audit trail | WAL retains all operations for compliance |
| Index maintenance | Harbour DBFCDX maintains CDX indexes automatically |
| Balance updates | nacnt, nhist, nbank, sname/pname all updated correctly |
| Opera SE parity | Same posting methods, same validation, same result format |
