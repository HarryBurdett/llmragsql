# Opera 3 Write Agent — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy a production-ready Write Agent for Opera 3 that safely handles all FoxPro writes with proper locking, and integrate it into the main application.

**Architecture:** Two-phase plan. Plan A (integration) adds Write Agent configuration to the app, health checks, and write routing — executable now from macOS. Plan B (deployment) creates the self-contained package, compiles Harbour, and deploys to the Windows server — requires Windows access.

**Tech Stack:** Python FastAPI (agent), Harbour DBFCDX (FoxPro locking), NSSM (Windows Service), httpx (client), React/TypeScript (frontend)

**Spec:** `docs/superpowers/specs/2026-03-31-opera3-write-agent-design.md`

---

# PLAN A: Integration (macOS — execute now)

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `frontend/src/pages/Installations.tsx` | Modify | Add Write Agent URL/Key fields, test button, help text |
| `frontend/src/api/client.ts` | Modify | Add Write Agent config type fields |
| `api/main.py` | Modify | Store/load Write Agent settings, health check on startup, agent status endpoint |
| `apps/bank_reconcile/api/routes.py` | Modify | Route Opera 3 writes through agent client, show warning if unavailable |
| `sql_rag/opera3_write_provider.py` | Modify (if needed) | Configure agent from app settings instead of env vars |

---

### Task A1: Add Write Agent fields to OperaConfig and Installations page

**Files:**
- Modify: `frontend/src/pages/Installations.tsx`
- Modify: `frontend/src/api/client.ts`
- Modify: `api/main.py`

- [ ] **Step 1: Add fields to OperaConfig model in api/main.py**

Find the `OperaConfig` Pydantic model (around line 922) and add:

```python
class OperaConfig(BaseModel):
    """Opera system configuration"""
    version: str = "sql_se"
    opera3_server_path: Optional[str] = None
    opera3_base_path: Optional[str] = None
    opera3_company_code: Optional[str] = None
    opera3_share_user: Optional[str] = None
    opera3_share_password: Optional[str] = None
    # Write Agent settings
    opera3_agent_url: Optional[str] = None      # e.g., http://172.17.172.214:9000
    opera3_agent_key: Optional[str] = None      # Shared secret
```

- [ ] **Step 2: Update update_opera_config endpoint to save agent settings**

In the `update_opera_config()` endpoint, add after the existing config saves:

```python
    if opera_config.opera3_agent_url is not None:
        config["opera"]["opera3_agent_url"] = opera_config.opera3_agent_url
    if opera_config.opera3_agent_key is not None:
        config["opera"]["opera3_agent_key"] = opera_config.opera3_agent_key
```

- [ ] **Step 3: Update get_opera_config to return agent settings**

In the `get_opera_config()` endpoint, add to the return dict:

```python
    "opera3_agent_url": opera_section.get("opera3_agent_url", ""),
    "opera3_agent_key": opera_section.get("opera3_agent_key", ""),
```

- [ ] **Step 4: Add fields to frontend OperaConfig type**

In `frontend/src/api/client.ts`, find the `OperaConfig` interface and add:

```typescript
export interface OperaConfig {
    version: string;
    opera3_server_path?: string;
    opera3_base_path?: string;
    opera3_company_code?: string;
    opera3_share_user?: string;
    opera3_share_password?: string;
    opera3_agent_url?: string;    // NEW
    opera3_agent_key?: string;    // NEW
}
```

- [ ] **Step 5: Add fields to SystemFormState in Installations.tsx**

Add to `SystemFormState` interface:

```typescript
    opera3AgentUrl: string;
    opera3AgentKey: string;
```

Add to `systemToForm`:
```typescript
    opera3AgentUrl: sys.opera?.opera3_agent_url || '',
    opera3AgentKey: sys.opera?.opera3_agent_key || '',
```

Add to `formToSystemData`:
```typescript
    opera: {
        ...existing,
        opera3_agent_url: form.opera3AgentUrl,
        opera3_agent_key: form.opera3AgentKey,
    }
```

- [ ] **Step 6: Add Write Agent UI section to Installations.tsx**

After the existing Opera 3 Connection section (Share Username/Password fields), add a new section that only shows when `operaVersion === 'opera3'`:

```tsx
{/* Write Agent */}
<div className="space-y-3 mt-4 pt-4 border-t border-gray-100">
    <h4 className="text-sm font-medium text-gray-700 flex items-center gap-1.5">
        Write Agent
    </h4>
    <p className="text-xs text-gray-500">
        The Write Agent runs on the Opera 3 server to safely post transactions.
        Without it, Opera 3 is read-only. See help below for setup instructions.
    </p>
    <div className="grid grid-cols-2 gap-3">
        <div>
            <label className="label">Write Agent URL</label>
            <input type="text" className="input" placeholder="http://172.17.172.214:9000"
                value={form.opera3AgentUrl}
                onChange={(e) => updateForm({ opera3AgentUrl: e.target.value })} />
        </div>
        <div>
            <label className="label">Write Agent Key</label>
            <input type="password" className="input" placeholder="Shared secret from install"
                value={form.opera3AgentKey}
                onChange={(e) => updateForm({ opera3AgentKey: e.target.value })} />
        </div>
    </div>
</div>
```

- [ ] **Step 7: Add save of agent settings in handleSaveSettings**

In the `updateOperaConfig` call within `handleSaveSettings`, add the agent fields:

```typescript
await apiClient.updateOperaConfig({
    ...existing fields,
    opera3_agent_url: form.opera3AgentUrl,
    opera3_agent_key: form.opera3AgentKey,
});
```

- [ ] **Step 8: Commit**

```bash
git add api/main.py frontend/src/pages/Installations.tsx frontend/src/api/client.ts
git commit -m "Add Write Agent URL/Key settings to Installations page and API"
```

---

### Task A2: Add Test Connection button and Help text

**Files:**
- Modify: `frontend/src/pages/Installations.tsx`
- Modify: `api/main.py`

- [ ] **Step 1: Add agent health check endpoint**

In `api/main.py`, add a new endpoint:

```python
@app.post("/api/config/opera/test-agent")
async def test_write_agent(opera_config: OperaConfig):
    """Test connection to Opera 3 Write Agent."""
    if not opera_config.opera3_agent_url:
        return {"success": False, "error": "Write Agent URL not provided"}

    try:
        import httpx
        headers = {}
        if opera_config.opera3_agent_key:
            headers["X-Agent-Key"] = opera_config.opera3_agent_key

        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{opera_config.opera3_agent_url.rstrip('/')}/health", headers=headers)
            if response.status_code == 200:
                data = response.json()
                return {
                    "success": True,
                    "message": f"Write Agent connected — {data.get('status', 'OK')}",
                    "details": data
                }
            elif response.status_code == 401:
                return {"success": False, "error": "Authentication failed — check the Agent Key"}
            else:
                return {"success": False, "error": f"Agent returned status {response.status_code}"}
    except httpx.ConnectError:
        return {"success": False, "error": f"Cannot connect to {opera_config.opera3_agent_url} — is the Write Agent running?"}
    except httpx.TimeoutException:
        return {"success": False, "error": "Connection timed out — check the URL and ensure the agent is running"}
    except Exception as e:
        return {"success": False, "error": str(e)}
```

- [ ] **Step 2: Add Test Connection button in UI**

After the Write Agent URL/Key fields:

```tsx
<button
    onClick={async () => {
        try {
            const res = await authFetch('/api/config/opera/test-agent', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    opera3_agent_url: form.opera3AgentUrl,
                    opera3_agent_key: form.opera3AgentKey,
                })
            });
            const data = await res.json();
            if (data.success) {
                setMessage({ type: 'success', text: data.message });
            } else {
                setMessage({ type: 'error', text: data.error });
            }
        } catch {
            setMessage({ type: 'error', text: 'Failed to test Write Agent connection' });
        }
    }}
    className="px-3 py-1.5 text-sm bg-blue-50 text-blue-700 rounded hover:bg-blue-100"
>
    Test Connection
</button>
```

- [ ] **Step 3: Add Help section**

After the Write Agent section, add collapsible help:

```tsx
<details className="mt-3">
    <summary className="text-sm font-medium text-blue-600 cursor-pointer hover:text-blue-800">
        Setup Instructions
    </summary>
    <div className="mt-2 p-3 bg-blue-50 rounded text-sm text-gray-700 space-y-2">
        <p><strong>The Write Agent</strong> is a service that runs on the Opera 3 server to safely post transactions to the FoxPro database.</p>
        <ol className="list-decimal list-inside space-y-1">
            <li>Copy the <code>opera3-write-agent</code> folder to the Opera 3 server</li>
            <li>Run <code>install.bat</code> as Administrator</li>
            <li>The installer displays the URL and Key</li>
            <li>Enter the URL and Key above</li>
            <li>Click Test Connection to verify</li>
        </ol>
        <p className="text-gray-500 text-xs mt-2">
            Without the Write Agent, Opera 3 is read-only. Viewing, reporting, and analysis work via SMB.
            Posting transactions, importing, and reconciliation require the Write Agent.
        </p>
    </div>
</details>
```

- [ ] **Step 4: Commit**

```bash
git add api/main.py frontend/src/pages/Installations.tsx
git commit -m "Add Write Agent test connection button and setup help text"
```

---

### Task A3: Health check on startup and agent status banner

**Files:**
- Modify: `api/main.py`

- [ ] **Step 1: Add agent health check on startup**

In the `lifespan()` function, after the SMB auto-connect block, add:

```python
    # Check Opera 3 Write Agent if configured
    if config and config.has_section("opera"):
        agent_url = config.get("opera", "opera3_agent_url", fallback="")
        agent_key = config.get("opera", "opera3_agent_key", fallback="")
        if agent_url:
            try:
                import httpx
                headers = {"X-Agent-Key": agent_key} if agent_key else {}
                response = httpx.get(f"{agent_url.rstrip('/')}/health", headers=headers, timeout=5.0)
                if response.status_code == 200:
                    logger.info(f"Opera 3 Write Agent connected: {agent_url}")
                else:
                    logger.warning(f"Opera 3 Write Agent returned {response.status_code}: {agent_url}")
            except Exception as e:
                logger.warning(f"Opera 3 Write Agent not available at {agent_url}: {e}")
```

- [ ] **Step 2: Add agent status endpoint**

```python
@app.get("/api/config/opera/agent-status")
async def get_agent_status():
    """Check if Write Agent is currently available."""
    if not config or not config.has_section("opera"):
        return {"configured": False, "available": False}

    agent_url = config.get("opera", "opera3_agent_url", fallback="")
    if not agent_url:
        return {"configured": False, "available": False}

    try:
        import httpx
        agent_key = config.get("opera", "opera3_agent_key", fallback="")
        headers = {"X-Agent-Key": agent_key} if agent_key else {}
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{agent_url.rstrip('/')}/health", headers=headers)
            return {
                "configured": True,
                "available": response.status_code == 200,
                "url": agent_url,
                "details": response.json() if response.status_code == 200 else None,
                "error": f"Status {response.status_code}" if response.status_code != 200 else None
            }
    except Exception as e:
        return {"configured": True, "available": False, "url": agent_url, "error": str(e)}
```

- [ ] **Step 3: Configure agent client from app settings**

In the startup code, after checking the agent health, configure the write provider:

```python
            if response.status_code == 200:
                logger.info(f"Opera 3 Write Agent connected: {agent_url}")
                # Configure the write provider to use the agent
                try:
                    from sql_rag.opera3_write_provider import configure_agent
                    configure_agent(agent_url=agent_url, agent_key=agent_key, required=True)
                    logger.info("Opera 3 write provider configured to use Write Agent")
                except ImportError:
                    logger.warning("opera3_write_provider not available")
```

- [ ] **Step 4: Commit**

```bash
git add api/main.py
git commit -m "Add Write Agent health check on startup and agent status endpoint"
```

---

### Task A4: Route Opera 3 writes through agent and show warnings

**Files:**
- Modify: `apps/bank_reconcile/api/routes.py`

- [ ] **Step 1: Add agent availability check helper**

Near the top of the routes file, add:

```python
def _check_opera3_write_agent() -> tuple:
    """Check if Opera 3 Write Agent is available. Returns (available, error_message)."""
    try:
        from sql_rag.opera3_write_provider import is_agent_available
        available, info = is_agent_available()
        if available:
            return True, None
        return False, "Opera 3 Write Agent is not running. Posting transactions requires the Write Agent service on the Opera 3 server."
    except ImportError:
        return False, "Opera 3 Write Agent client not configured."
```

- [ ] **Step 2: Add agent check to Opera 3 import endpoints**

In the Opera 3 import endpoints (around line 10186 for `opera3_import_from_pdf`), add at the start:

```python
    # Check Write Agent is available for Opera 3 writes
    available, agent_error = _check_opera3_write_agent()
    if not available:
        return {"success": False, "error": agent_error}
```

Apply this to all Opera 3 write endpoints.

- [ ] **Step 3: Commit**

```bash
git add apps/bank_reconcile/api/routes.py
git commit -m "Route Opera 3 writes through agent, block if unavailable"
```

---

# PLAN B: Deployment Package (Windows Server)

---

### Task B1: Create self-contained deployment package structure

**Files:**
- Create: `opera3_agent/package/build_package.py`

- [ ] **Step 1: Create package builder script**

This script runs on any machine and creates the deployment zip. It:
1. Downloads embedded Python for Windows
2. Copies agent code
3. Copies pre-compiled Harbour DLL (must exist)
4. Downloads NSSM
5. Creates install.bat and uninstall.bat
6. Zips everything

```python
"""
Build the self-contained Opera 3 Write Agent deployment package.

Run: python build_package.py
Output: opera3-write-agent-setup.zip

Prerequisites:
- harbour/libdbfbridge.dll must be pre-compiled (from Windows)
"""
# ... full implementation ...
```

- [ ] **Step 2: Create install.bat**

```batch
@echo off
REM Opera 3 Write Agent - One Click Installer
REM Run as Administrator

echo ============================================
echo Opera 3 Write Agent Installer
echo ============================================
echo.

REM Check admin
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Please run as Administrator
    pause
    exit /b 1
)

REM Detect Opera 3 path
set "DATA_PATH="
if exist "C:\Apps\O3 Server VFP" set "DATA_PATH=C:\Apps\O3 Server VFP"
if exist "D:\Apps\O3 Server VFP" set "DATA_PATH=D:\Apps\O3 Server VFP"

if "%DATA_PATH%"=="" (
    set /p DATA_PATH="Enter Opera 3 data path: "
)

echo Using data path: %DATA_PATH%

REM Install dependencies
echo Installing Python dependencies...
python\python.exe -m pip install -r agent\requirements.txt --quiet

REM Generate agent key
for /f %%i in ('python\python.exe -c "import secrets; print(secrets.token_urlsafe(32))"') do set AGENT_KEY=%%i

REM Set environment variables
setx OPERA3_DATA_PATH "%DATA_PATH%" /M
setx OPERA3_AGENT_KEY "%AGENT_KEY%" /M
setx OPERA3_AGENT_PORT "9000" /M

REM Install as Windows Service
echo Installing Windows Service...
nssm.exe install Opera3WriteAgent "%~dp0python\python.exe" -m uvicorn agent.service:app --host 0.0.0.0 --port 9000
nssm.exe set Opera3WriteAgent AppDirectory "%~dp0"
nssm.exe set Opera3WriteAgent Start SERVICE_AUTO_START
nssm.exe set Opera3WriteAgent AppStdout "%~dp0logs\agent.log"
nssm.exe set Opera3WriteAgent AppStderr "%~dp0logs\agent_error.log"

REM Start service
echo Starting service...
mkdir logs 2>nul
nssm.exe start Opera3WriteAgent

REM Health check
timeout /t 3 /nobreak >nul
python\python.exe -c "import httpx; r=httpx.get('http://localhost:9000/health'); print('Health:', r.json())" 2>nul
if %errorlevel% equ 0 (
    echo.
    echo ============================================
    echo SUCCESS - Write Agent is running
    echo ============================================
    echo.
    echo URL:  http://%COMPUTERNAME%:9000
    echo Key:  %AGENT_KEY%
    echo.
    echo Enter these in the Installations page of your application.
    echo ============================================
) else (
    echo.
    echo WARNING: Health check failed. Check logs\agent_error.log
)

pause
```

- [ ] **Step 3: Create uninstall.bat**

```batch
@echo off
echo Stopping and removing Opera 3 Write Agent service...
nssm.exe stop Opera3WriteAgent
nssm.exe remove Opera3WriteAgent confirm
echo Service removed.
pause
```

- [ ] **Step 4: Commit**

```bash
git add opera3_agent/package/
git commit -m "Create self-contained deployment package builder and installer scripts"
```

---

### Task B2: Compile Harbour DBFCDX library

**Files:**
- Pre-compiled: `opera3_agent/harbour/libdbfbridge.dll`

This task must be done ON a Windows machine with Harbour compiler installed.

- [ ] **Step 1: Install Harbour on Windows**

Download from https://harbour.github.io/ and install.

- [ ] **Step 2: Compile the bridge**

```batch
cd opera3_agent\harbour
hbmk2 dbfbridge.prg -shared -o libdbfbridge.dll
```

- [ ] **Step 3: Verify the DLL loads**

```python
import ctypes
lib = ctypes.CDLL("libdbfbridge.dll")
print("Harbour DLL loaded successfully")
```

- [ ] **Step 4: Copy DLL into package**

The DLL goes into `opera3_agent/harbour/libdbfbridge.dll` and is included in the deployment zip.

- [ ] **Step 5: Commit**

```bash
git add opera3_agent/harbour/libdbfbridge.dll
git commit -m "Add pre-compiled Harbour DBFCDX library for Windows"
```

---

### Task B3: Deploy to Windows server

- [ ] **Step 1: Copy package to server**

Copy `opera3-write-agent-setup.zip` to `172.17.172.214`

- [ ] **Step 2: Run installer**

```batch
install.bat
```

Note the URL and Key displayed.

- [ ] **Step 3: Configure in main app**

Go to Installations page → Opera 3 section → enter Write Agent URL and Key → Test Connection.

- [ ] **Step 4: Verify all posting methods**

Use the Transaction Snapshot tool to test each transaction type:
1. Sales Receipt
2. Purchase Payment
3. Sales Refund
4. Purchase Refund
5. Bank Transfer
6. Nominal Entry
7. GoCardless Batch

For each: take before snapshot, enter in Opera, take after snapshot, compare with Write Agent result.

- [ ] **Step 5: Test concurrent access**

Have an Opera user entering transactions while the Write Agent posts. Verify:
- No lock timeouts
- No data corruption
- Both transactions post correctly

- [ ] **Step 6: Test crash recovery**

1. Start a transaction via the Write Agent
2. Kill the agent service mid-write
3. Restart the service
4. Check WAL shows recovery
5. Verify Opera data is consistent

---

### Task B4: End-to-end verification

- [ ] **Step 1: Verify all Opera 3 apps work through Write Agent**

Test the full bank reconciliation flow:
1. Scan emails/folders for Opera 3 bank
2. Analyse statement
3. Import transactions (goes through Write Agent)
4. Reconcile

- [ ] **Step 2: Verify GoCardless import through Write Agent**

- [ ] **Step 3: Run Transaction Snapshot for each posting type**

Build the complete Transaction Library for Opera 3.

- [ ] **Step 4: Compare with Opera SE patterns**

Ensure Opera 3 postings match Opera SE table/field patterns.
