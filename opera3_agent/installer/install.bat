@echo off
:: Opera 3 Write Agent - Windows Service Installer
::
:: This script installs the Opera 3 Write Agent as a Windows Service
:: that starts automatically and restarts on failure.
::
:: Usage:
::   install.bat                        Interactive (prompts for data path)
::   install.bat "C:\Apps\O3 Server VFP"  Non-interactive
::
:: Requirements:
::   - Administrator privileges
::   - Python 3.10+ installed (or embedded Python in this package)
::   - NSSM (included in this package)

setlocal enabledelayedexpansion

echo ========================================
echo  Opera 3 Write Agent - Service Installer
echo ========================================
echo.

:: Check for admin privileges
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: This installer must be run as Administrator.
    echo Right-click and select "Run as administrator".
    pause
    exit /b 1
)

:: Determine install directory (where this script lives)
set "INSTALL_DIR=%~dp0.."
set "INSTALL_DIR=%INSTALL_DIR:\installer=\%"
pushd "%INSTALL_DIR%"
set "INSTALL_DIR=%CD%"
popd

:: Get Opera 3 data path
if "%~1"=="" (
    echo Enter the path to Opera 3 data files:
    echo   Example: C:\Apps\O3 Server VFP
    echo.
    set /p OPERA3_PATH="Path: "
) else (
    set "OPERA3_PATH=%~1"
)

:: Validate data path
if not exist "%OPERA3_PATH%" (
    echo ERROR: Path does not exist: %OPERA3_PATH%
    pause
    exit /b 1
)

:: Check for DBF files
if not exist "%OPERA3_PATH%\aentry.dbf" (
    if not exist "%OPERA3_PATH%\AENTRY.DBF" (
        echo WARNING: No Opera 3 DBF files found at %OPERA3_PATH%
        echo Continue anyway? (Y/N^)
        set /p CONTINUE="Choice: "
        if /i not "!CONTINUE!"=="Y" exit /b 1
    )
)

:: Find Python
set "PYTHON_EXE="

:: Check for embedded Python first
if exist "%INSTALL_DIR%\python-embed\python.exe" (
    set "PYTHON_EXE=%INSTALL_DIR%\python-embed\python.exe"
    echo Found embedded Python: %PYTHON_EXE%
    goto :found_python
)

:: Check system Python
where python >nul 2>&1
if %errorlevel% equ 0 (
    for /f "tokens=*" %%i in ('where python') do (
        set "PYTHON_EXE=%%i"
        goto :found_python
    )
)

where python3 >nul 2>&1
if %errorlevel% equ 0 (
    for /f "tokens=*" %%i in ('where python3') do (
        set "PYTHON_EXE=%%i"
        goto :found_python
    )
)

echo ERROR: Python not found. Install Python 3.10+ or include embedded Python.
pause
exit /b 1

:found_python
echo Using Python: %PYTHON_EXE%
echo.

:: Check for NSSM
set "NSSM_EXE=%INSTALL_DIR%\installer\nssm.exe"
if not exist "%NSSM_EXE%" (
    :: Try system NSSM
    where nssm >nul 2>&1
    if %errorlevel% equ 0 (
        for /f "tokens=*" %%i in ('where nssm') do set "NSSM_EXE=%%i"
    ) else (
        echo ERROR: NSSM not found. Download from https://nssm.cc/
        echo Place nssm.exe in: %INSTALL_DIR%\installer\
        pause
        exit /b 1
    )
)
echo Using NSSM: %NSSM_EXE%
echo.

:: Install Python dependencies
echo Installing Python dependencies...
"%PYTHON_EXE%" -m pip install fastapi uvicorn httpx pydantic dbfread >nul 2>&1
if %errorlevel% neq 0 (
    echo WARNING: pip install failed. Dependencies may already be installed.
)

:: Build Harbour DLL (for CDX index maintenance on writes)
echo.
echo Checking Harbour DLL...
set "DLL_PATH=%INSTALL_DIR%\opera3_agent\harbour\libdbfbridge.dll"
if exist "%DLL_PATH%" (
    echo Harbour DLL already exists: %DLL_PATH%
) else (
    echo Harbour DLL not found — attempting to build...

    :: Check if Harbour is installed
    where hbmk2 >nul 2>&1
    if %errorlevel% equ 0 (
        echo Found Harbour compiler. Building DLL...
        pushd "%INSTALL_DIR%\opera3_agent\harbour"
        hbmk2 dbfbridge.prg -shared -o libdbfbridge.dll
        if exist "libdbfbridge.dll" (
            echo SUCCESS: Harbour DLL built.
        ) else (
            echo WARNING: DLL build failed. Write operations may not maintain CDX indexes.
            echo Read operations will work fine without it.
        )
        popd
    ) else (
        echo Harbour not found on PATH.
        echo.
        echo The Harbour DLL is optional — needed only for CDX index maintenance on writes.
        echo Read operations work without it.
        echo.
        echo To add write support later:
        echo   1. Install Harbour from https://harbour.github.io/
        echo   2. Run: cd "%INSTALL_DIR%\opera3_agent\harbour"
        echo   3. Run: hbmk2 dbfbridge.prg -shared -o libdbfbridge.dll
        echo   4. Restart the service: net stop Opera3Agent ^&^& net start Opera3Agent
    )
)
echo.

:: Generate agent key
set "AGENT_KEY="
for /f "tokens=*" %%i in ('"%PYTHON_EXE%" -c "import secrets; print(secrets.token_urlsafe(32))"') do (
    set "AGENT_KEY=%%i"
)

:: Stop existing service if running
echo Checking for existing service...
"%NSSM_EXE%" status Opera3Agent >nul 2>&1
if %errorlevel% equ 0 (
    echo Stopping existing service...
    "%NSSM_EXE%" stop Opera3Agent >nul 2>&1
    timeout /t 2 >nul
    echo Removing existing service...
    "%NSSM_EXE%" remove Opera3Agent confirm >nul 2>&1
    timeout /t 1 >nul
)

:: Install the service
echo.
echo Installing Opera 3 Write Agent service...

"%NSSM_EXE%" install Opera3Agent "%PYTHON_EXE%" "-m" "uvicorn" "opera3_agent.service:app" "--host" "0.0.0.0" "--port" "9000"
"%NSSM_EXE%" set Opera3Agent AppDirectory "%INSTALL_DIR%\.."
"%NSSM_EXE%" set Opera3Agent DisplayName "Opera 3 Agent"
"%NSSM_EXE%" set Opera3Agent Description "Gateway for all Opera 3 FoxPro data access — reads and writes"
"%NSSM_EXE%" set Opera3Agent Start SERVICE_AUTO_START

:: Environment variables
"%NSSM_EXE%" set Opera3Agent AppEnvironmentExtra "OPERA3_DATA_PATH=%OPERA3_PATH%" "OPERA3_AGENT_KEY=%AGENT_KEY%"

:: Restart on failure
"%NSSM_EXE%" set Opera3Agent AppRestartDelay 5000
"%NSSM_EXE%" set Opera3Agent AppThrottle 10000

:: Logging
"%NSSM_EXE%" set Opera3Agent AppStdout "%INSTALL_DIR%\logs\agent.log"
"%NSSM_EXE%" set Opera3Agent AppStderr "%INSTALL_DIR%\logs\agent-error.log"
"%NSSM_EXE%" set Opera3Agent AppStdoutCreationDisposition 4
"%NSSM_EXE%" set Opera3Agent AppStderrCreationDisposition 4
"%NSSM_EXE%" set Opera3Agent AppRotateFiles 1
"%NSSM_EXE%" set Opera3Agent AppRotateBytes 10485760

:: Create logs directory
if not exist "%INSTALL_DIR%\logs" mkdir "%INSTALL_DIR%\logs"

:: Start the service
echo Starting service...
"%NSSM_EXE%" start Opera3Agent

:: Wait for startup
echo Waiting for service to start...
timeout /t 3 >nul

:: Verify
echo.
echo Checking service health...
curl -s http://localhost:9000/health >nul 2>&1
if %errorlevel% equ 0 (
    echo.
    echo ========================================
    echo  SUCCESS: Opera 3 Agent installed
    echo ========================================
    echo.
    echo  Service:    Opera3Agent
    echo  URL:        http://localhost:9000
    echo  Data path:  %OPERA3_PATH%
    echo  Agent key:  %AGENT_KEY%
    echo  Logs:       %INSTALL_DIR%\logs\
    echo.
    echo  The service will start automatically on boot
    echo  and restart automatically on failure.
    echo.
    echo  IMPORTANT: Save the agent key above.
    echo  Configure it in the main app as:
    echo    OPERA3_AGENT_URL=http://THIS_SERVER_IP:9000
    echo    OPERA3_AGENT_KEY=%AGENT_KEY%
    echo.
) else (
    echo.
    echo WARNING: Service installed but health check failed.
    echo Check logs at: %INSTALL_DIR%\logs\agent-error.log
    echo.
    echo Service status:
    "%NSSM_EXE%" status Opera3Agent
)

:: Save config for reference
echo [Opera3Agent] > "%INSTALL_DIR%\agent-config.ini"
echo data_path=%OPERA3_PATH% >> "%INSTALL_DIR%\agent-config.ini"
echo agent_key=%AGENT_KEY% >> "%INSTALL_DIR%\agent-config.ini"
echo port=9000 >> "%INSTALL_DIR%\agent-config.ini"
echo installed=%DATE% %TIME% >> "%INSTALL_DIR%\agent-config.ini"

pause
