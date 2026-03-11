#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Opera 3 Write Agent - One-Click Deployment Script

.DESCRIPTION
    Installs everything needed for the Opera 3 Write Agent on a Windows server:
    1. Downloads and installs Harbour compiler (for CDX index maintenance)
    2. Compiles the DBF/CDX bridge library
    3. Downloads NSSM (for Windows Service management)
    4. Installs Python dependencies
    5. Installs and starts the agent as a Windows Service
    6. Verifies the service is healthy

    Run this on the Opera 3 server (where the .DBF files live).

.PARAMETER DataPath
    Path to Opera 3 data files. Default: C:\Apps\O3 Server VFP

.PARAMETER Port
    Agent service port. Default: 9000

.PARAMETER SkipHarbour
    Skip Harbour download/compile if libdbfbridge.dll already exists.

.EXAMPLE
    .\deploy.ps1
    .\deploy.ps1 -DataPath "D:\Opera3\Data"
    .\deploy.ps1 -DataPath "C:\Apps\O3 Server VFP" -Port 9000
#>

param(
    [string]$DataPath = "",
    [int]$Port = 9000,
    [string]$ServiceName = "",
    [switch]$SkipHarbour
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"  # Speed up Invoke-WebRequest

# ============================================================
# Configuration
# ============================================================

$AGENT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$PROJECT_DIR = Split-Path -Parent $AGENT_DIR
$HARBOUR_DIR = Join-Path $AGENT_DIR "harbour"
$INSTALLER_DIR = Join-Path $AGENT_DIR "installer"
$LOGS_DIR = Join-Path $AGENT_DIR "logs"
$TOOLS_DIR = Join-Path $AGENT_DIR "tools"

# Service name is derived from data path to support multiple instances on same server.
# Override with -ServiceName parameter if needed.
$HARBOUR_RELEASE_API = "https://api.github.com/repos/vszakats/hb/releases/latest"
$NSSM_URL = "https://nssm.cc/release/nssm-2.24.zip"

# ============================================================
# Helper functions
# ============================================================

function Write-Header($text) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  $text" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Step($number, $text) {
    Write-Host "[$number] $text" -ForegroundColor Yellow
}

function Write-OK($text) {
    Write-Host "  OK: $text" -ForegroundColor Green
}

function Write-Warn($text) {
    Write-Host "  WARNING: $text" -ForegroundColor DarkYellow
}

function Write-Fail($text) {
    Write-Host "  FAILED: $text" -ForegroundColor Red
}

function Test-Command($cmd) {
    try { Get-Command $cmd -ErrorAction Stop | Out-Null; return $true }
    catch { return $false }
}

# ============================================================
# Step 0: Validate environment
# ============================================================

Write-Header "Opera 3 Write Agent Deployment"

Write-Host "Agent directory: $AGENT_DIR"
Write-Host "Project directory: $PROJECT_DIR"
Write-Host ""

# Ensure we're on Windows
if ($env:OS -ne "Windows_NT") {
    Write-Fail "This script must be run on Windows (the Opera 3 server)."
    exit 1
}

# Find Opera 3 data path
if (-not $DataPath) {
    # Check common locations
    $commonPaths = @(
        "C:\Apps\O3 Server VFP",
        "C:\Opera3\Data",
        "D:\Apps\O3 Server VFP",
        "C:\Program Files\Pegasus\Opera 3\Data"
    )
    foreach ($p in $commonPaths) {
        if (Test-Path (Join-Path $p "aentry.dbf")) {
            $DataPath = $p
            Write-OK "Found Opera 3 data at: $DataPath"
            break
        }
        if (Test-Path (Join-Path $p "AENTRY.DBF")) {
            $DataPath = $p
            Write-OK "Found Opera 3 data at: $DataPath"
            break
        }
    }

    if (-not $DataPath) {
        $DataPath = Read-Host "Enter the path to Opera 3 data files (e.g. C:\Apps\O3 Server VFP)"
    }
}

if (-not (Test-Path $DataPath)) {
    Write-Fail "Data path does not exist: $DataPath"
    exit 1
}

# Verify DBF files exist
$hasDBF = (Test-Path (Join-Path $DataPath "aentry.dbf")) -or (Test-Path (Join-Path $DataPath "AENTRY.DBF"))
if (-not $hasDBF) {
    Write-Warn "No Opera 3 DBF files found at $DataPath"
    $continue = Read-Host "Continue anyway? (Y/N)"
    if ($continue -ne "Y") { exit 1 }
}

# Derive service name from data path if not explicitly provided
if (-not $ServiceName) {
    # Create a short, unique suffix from the data path
    $pathHash = [System.BitConverter]::ToString(
        [System.Security.Cryptography.SHA256]::Create().ComputeHash(
            [System.Text.Encoding]::UTF8.GetBytes($DataPath.ToLower().TrimEnd('\'))
        )
    ).Replace("-", "").Substring(0, 6)
    $ServiceName = "Opera3Agent_$pathHash"
}
$SERVICE_NAME = $ServiceName

Write-Host "Data path:     $DataPath"
Write-Host "Port:          $Port"
Write-Host "Service name:  $SERVICE_NAME"
Write-Host ""

# Create directories
foreach ($dir in @($LOGS_DIR, $TOOLS_DIR)) {
    if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
}

# ============================================================
# Step 1: Python
# ============================================================

Write-Step 1 "Checking Python..."

$pythonExe = $null

# Check for system Python
if (Test-Command "python") {
    $pythonExe = (Get-Command python).Source
    $version = & python --version 2>&1
    Write-OK "Found Python: $version ($pythonExe)"
} elseif (Test-Command "python3") {
    $pythonExe = (Get-Command python3).Source
    $version = & python3 --version 2>&1
    Write-OK "Found Python: $version ($pythonExe)"
} else {
    # Check for embedded Python
    $embedded = Join-Path $AGENT_DIR "python-embed\python.exe"
    if (Test-Path $embedded) {
        $pythonExe = $embedded
        Write-OK "Found embedded Python: $pythonExe"
    } else {
        Write-Fail "Python not found. Install Python 3.10+ from https://python.org"
        exit 1
    }
}

# Install Python dependencies
Write-Step 1 "Installing Python dependencies..."
$reqFile = Join-Path $AGENT_DIR "requirements.txt"
& $pythonExe -m pip install -r $reqFile --quiet 2>&1 | Out-Null
Write-OK "Python dependencies installed"

# ============================================================
# Step 2: Harbour compiler
# ============================================================

$bridgeDll = Join-Path $HARBOUR_DIR "libdbfbridge.dll"

if ($SkipHarbour -and (Test-Path $bridgeDll)) {
    Write-Step 2 "Skipping Harbour (bridge DLL already exists)"
    Write-OK "Found: $bridgeDll"
} else {
    Write-Step 2 "Setting up Harbour compiler..."

    $hbmk2Found = Test-Command "hbmk2"

    if (-not $hbmk2Found) {
        # Check tools directory
        $localHbmk2 = Join-Path $TOOLS_DIR "harbour\bin\hbmk2.exe"
        if (Test-Path $localHbmk2) {
            $env:PATH = "$(Split-Path $localHbmk2);$env:PATH"
            $hbmk2Found = $true
            Write-OK "Found local Harbour: $localHbmk2"
        }
    }

    if (-not $hbmk2Found) {
        Write-Host "  Downloading Harbour compiler from GitHub..." -ForegroundColor Gray

        try {
            # Query GitHub API for latest release
            $headers = @{ "User-Agent" = "Opera3WriteAgent/1.0" }
            $release = Invoke-RestMethod -Uri $HARBOUR_RELEASE_API -Headers $headers

            # Find Windows 64-bit asset
            $asset = $release.assets | Where-Object {
                $_.name -match "win" -and $_.name -match "(mingw64|msvc64)" -and $_.name -match "\.zip$"
            } | Select-Object -First 1

            if (-not $asset) {
                # Fallback: try any Windows zip
                $asset = $release.assets | Where-Object {
                    $_.name -match "win.*\.zip$"
                } | Select-Object -First 1
            }

            if ($asset) {
                $zipPath = Join-Path $TOOLS_DIR "harbour.zip"
                $extractPath = Join-Path $TOOLS_DIR "harbour"

                Write-Host "  Downloading: $($asset.name) ($([math]::Round($asset.size / 1MB, 1)) MB)..." -ForegroundColor Gray
                Invoke-WebRequest -Uri $asset.browser_download_url -OutFile $zipPath -Headers $headers

                Write-Host "  Extracting..." -ForegroundColor Gray
                if (Test-Path $extractPath) { Remove-Item -Recurse -Force $extractPath }
                Expand-Archive -Path $zipPath -DestinationPath $extractPath -Force

                # Find hbmk2 in extracted files
                $hbmk2 = Get-ChildItem -Path $extractPath -Recurse -Filter "hbmk2.exe" | Select-Object -First 1
                if ($hbmk2) {
                    $env:PATH = "$($hbmk2.DirectoryName);$env:PATH"
                    $hbmk2Found = $true
                    Write-OK "Harbour installed: $($hbmk2.FullName)"
                }

                Remove-Item -Force $zipPath -ErrorAction SilentlyContinue
            } else {
                Write-Warn "Could not find Windows build in latest Harbour release"
            }
        } catch {
            Write-Warn "Failed to download Harbour: $_"
        }
    }

    if (-not $hbmk2Found) {
        Write-Host ""
        Write-Host "  Harbour compiler not found. Please install manually:" -ForegroundColor Red
        Write-Host "    1. Download from: https://github.com/vszakats/hb/releases" -ForegroundColor Red
        Write-Host "    2. Extract to: $TOOLS_DIR\harbour\" -ForegroundColor Red
        Write-Host "    3. Re-run this script" -ForegroundColor Red
        Write-Host ""
        Write-Host "  OR: If you have a pre-compiled libdbfbridge.dll, place it in:" -ForegroundColor Yellow
        Write-Host "    $HARBOUR_DIR" -ForegroundColor Yellow
        Write-Host "  Then re-run with: .\deploy.ps1 -SkipHarbour" -ForegroundColor Yellow
        exit 1
    }

    # Compile the bridge
    Write-Step 2 "Compiling Harbour DBF/CDX bridge..."
    Push-Location $HARBOUR_DIR
    try {
        $buildOutput = & hbmk2 -hbdynvm dbfbridge.prg -o libdbfbridge 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Fail "Harbour compilation failed:"
            Write-Host $buildOutput
            exit 1
        }

        if (Test-Path $bridgeDll) {
            $size = [math]::Round((Get-Item $bridgeDll).Length / 1KB, 0)
            Write-OK "Compiled: libdbfbridge.dll (${size} KB)"
        } else {
            Write-Fail "Compilation produced no DLL output"
            exit 1
        }
    } finally {
        Pop-Location
    }
}

# ============================================================
# Step 3: NSSM (Windows Service Manager)
# ============================================================

Write-Step 3 "Setting up NSSM..."

$nssmExe = Join-Path $INSTALLER_DIR "nssm.exe"

if (-not (Test-Path $nssmExe)) {
    # Check tools directory
    $toolNssm = Get-ChildItem -Path $TOOLS_DIR -Recurse -Filter "nssm.exe" -ErrorAction SilentlyContinue |
        Where-Object { $_.DirectoryName -match "win64" -or $_.DirectoryName -match "64" } |
        Select-Object -First 1

    if (-not $toolNssm) {
        $toolNssm = Get-ChildItem -Path $TOOLS_DIR -Recurse -Filter "nssm.exe" -ErrorAction SilentlyContinue |
            Select-Object -First 1
    }

    if ($toolNssm) {
        Copy-Item $toolNssm.FullName -Destination $nssmExe
        Write-OK "Found NSSM in tools directory"
    } elseif (Test-Command "nssm") {
        $nssmExe = (Get-Command nssm).Source
        Write-OK "Found system NSSM: $nssmExe"
    } else {
        Write-Host "  Downloading NSSM..." -ForegroundColor Gray
        try {
            $nssmZip = Join-Path $TOOLS_DIR "nssm.zip"
            $nssmExtract = Join-Path $TOOLS_DIR "nssm"

            Invoke-WebRequest -Uri $NSSM_URL -OutFile $nssmZip
            Expand-Archive -Path $nssmZip -DestinationPath $nssmExtract -Force

            $found = Get-ChildItem -Path $nssmExtract -Recurse -Filter "nssm.exe" |
                Where-Object { $_.DirectoryName -match "win64" } |
                Select-Object -First 1

            if (-not $found) {
                $found = Get-ChildItem -Path $nssmExtract -Recurse -Filter "nssm.exe" | Select-Object -First 1
            }

            if ($found) {
                Copy-Item $found.FullName -Destination $nssmExe
                Write-OK "NSSM downloaded and installed"
            }

            Remove-Item -Force $nssmZip -ErrorAction SilentlyContinue
        } catch {
            Write-Fail "Failed to download NSSM: $_"
            Write-Host "  Download manually from https://nssm.cc/ and place nssm.exe in:" -ForegroundColor Red
            Write-Host "    $INSTALLER_DIR" -ForegroundColor Red
            exit 1
        }
    }
}

if (-not (Test-Path $nssmExe)) {
    Write-Fail "NSSM not available"
    exit 1
}

Write-OK "NSSM ready: $nssmExe"

# ============================================================
# Step 4: Generate agent key
# ============================================================

Write-Step 4 "Generating agent key..."

$agentKey = & $pythonExe -c "import secrets; print(secrets.token_urlsafe(32))" 2>&1
$agentKey = $agentKey.Trim()
Write-OK "Agent key generated"

# ============================================================
# Step 5: Install Windows Service
# ============================================================

Write-Step 5 "Installing Windows Service..."

# Stop and remove existing service
$existingStatus = & $nssmExe status $SERVICE_NAME 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  Stopping existing service..." -ForegroundColor Gray
    & $nssmExe stop $SERVICE_NAME 2>&1 | Out-Null
    Start-Sleep -Seconds 2
    Write-Host "  Removing existing service..." -ForegroundColor Gray
    & $nssmExe remove $SERVICE_NAME confirm 2>&1 | Out-Null
    Start-Sleep -Seconds 1
}

# Install the service
& $nssmExe install $SERVICE_NAME $pythonExe "-m" "uvicorn" "opera3_agent.service:app" "--host" "0.0.0.0" "--port" "$Port"

# Configure service
& $nssmExe set $SERVICE_NAME AppDirectory $PROJECT_DIR
& $nssmExe set $SERVICE_NAME DisplayName "Opera 3 Write Agent"
& $nssmExe set $SERVICE_NAME Description "Handles Opera 3 FoxPro DBF writes with CDX index maintenance, WAL, and post-write verification"
& $nssmExe set $SERVICE_NAME Start SERVICE_AUTO_START

# Environment variables
& $nssmExe set $SERVICE_NAME AppEnvironmentExtra "OPERA3_DATA_PATH=$DataPath" "OPERA3_AGENT_KEY=$agentKey" "OPERA3_AGENT_PORT=$Port"

# Auto-restart on failure (5-second delay)
& $nssmExe set $SERVICE_NAME AppRestartDelay 5000
& $nssmExe set $SERVICE_NAME AppThrottle 10000

# Logging with rotation (10MB max)
& $nssmExe set $SERVICE_NAME AppStdout (Join-Path $LOGS_DIR "agent.log")
& $nssmExe set $SERVICE_NAME AppStderr (Join-Path $LOGS_DIR "agent-error.log")
& $nssmExe set $SERVICE_NAME AppStdoutCreationDisposition 4
& $nssmExe set $SERVICE_NAME AppStderrCreationDisposition 4
& $nssmExe set $SERVICE_NAME AppRotateFiles 1
& $nssmExe set $SERVICE_NAME AppRotateBytes 10485760

Write-OK "Service installed: $SERVICE_NAME"

# ============================================================
# Step 6: Start and verify
# ============================================================

Write-Step 6 "Starting service..."

& $nssmExe start $SERVICE_NAME 2>&1 | Out-Null

# Wait for startup with progress
$maxWait = 10
$healthy = $false
for ($i = 1; $i -le $maxWait; $i++) {
    Start-Sleep -Seconds 1
    Write-Host "  Waiting for startup... ($i/${maxWait}s)" -ForegroundColor Gray -NoNewline
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:${Port}/health" -TimeoutSec 2 -ErrorAction Stop
        if ($response.status -eq "ok") {
            $healthy = $true
            Write-Host ""
            break
        }
    } catch {
        Write-Host "`r" -NoNewline
    }
}

# ============================================================
# Step 7: Save config and report
# ============================================================

# Determine this server's IP for remote connection
$serverIP = (Get-NetIPAddress -AddressFamily IPv4 |
    Where-Object { $_.IPAddress -ne "127.0.0.1" -and $_.PrefixOrigin -ne "WellKnown" } |
    Select-Object -First 1).IPAddress

if (-not $serverIP) { $serverIP = "THIS_SERVER_IP" }

# Save config file
$configFile = Join-Path $AGENT_DIR "agent-config.ini"
@"
[OperaWriteAgent]
data_path=$DataPath
agent_key=$agentKey
port=$Port
server_ip=$serverIP
installed=$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
version=1.1.0
"@ | Set-Content -Path $configFile

if ($healthy) {
    Write-Header "DEPLOYMENT SUCCESSFUL"

    Write-Host "  Service:     $SERVICE_NAME" -ForegroundColor White
    Write-Host "  Status:      Running" -ForegroundColor Green
    Write-Host "  URL:         http://${serverIP}:${Port}" -ForegroundColor White
    Write-Host "  Data path:   $DataPath" -ForegroundColor White
    Write-Host "  Agent key:   $agentKey" -ForegroundColor White
    Write-Host "  Logs:        $LOGS_DIR" -ForegroundColor White
    Write-Host "  WAL:         $(Join-Path $AGENT_DIR 'opera3_wal.db')" -ForegroundColor White
    Write-Host ""
    Write-Host "  The service starts automatically on boot" -ForegroundColor Gray
    Write-Host "  and restarts automatically on failure." -ForegroundColor Gray
    Write-Host ""
    Write-Host "  ----------------------------------------" -ForegroundColor Cyan
    Write-Host "  CONFIGURE THE APP WITH THESE SETTINGS:" -ForegroundColor Cyan
    Write-Host "  ----------------------------------------" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  OPERA3_AGENT_URL = http://${serverIP}:${Port}" -ForegroundColor Yellow
    Write-Host "  OPERA3_AGENT_KEY = $agentKey" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Or set in company settings in the UI." -ForegroundColor Gray
    Write-Host ""

    # Health details
    Write-Host "  Agent health:" -ForegroundColor Gray
    Write-Host "    Version:  $($response.version)" -ForegroundColor Gray
    Write-Host "    Platform: $($response.platform)" -ForegroundColor Gray
    Write-Host "    Harbour:  $(if ($response.harbour_available) { 'Available' } else { 'Not loaded (will use on first write)' })" -ForegroundColor Gray
    Write-Host ""

    # Save connection details to a text file for easy copy
    $connFile = Join-Path $AGENT_DIR "connection-details.txt"
    @"
Opera 3 Write Agent - Connection Details
=========================================
Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')

Add these to your application environment or company settings:

  OPERA3_AGENT_URL = http://${serverIP}:${Port}
  OPERA3_AGENT_KEY = $agentKey

Service management:
  Start:   nssm start $SERVICE_NAME
  Stop:    nssm stop $SERVICE_NAME
  Status:  nssm status $SERVICE_NAME
  Logs:    $LOGS_DIR

Health check:
  curl http://${serverIP}:${Port}/health
"@ | Set-Content -Path $connFile
    Write-Host "  Connection details saved to: $connFile" -ForegroundColor Gray

} else {
    Write-Header "SERVICE INSTALLED (health check pending)"

    Write-Warn "Service installed but health check didn't respond in ${maxWait}s."
    Write-Host "  This may be normal on first start. Check logs:" -ForegroundColor Gray
    Write-Host "    $LOGS_DIR\agent-error.log" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Service status:" -ForegroundColor Gray
    & $nssmExe status $SERVICE_NAME
    Write-Host ""
    Write-Host "  Agent key: $agentKey" -ForegroundColor Yellow
    Write-Host "  URL: http://${serverIP}:${Port}" -ForegroundColor Yellow
}

Write-Host ""
