@echo off
:: Opera 3 Write Agent - Windows Service Uninstaller
::
:: Stops and removes the OperaWriteAgent Windows Service.
:: Does NOT delete files - only removes the service registration.

setlocal

echo ========================================
echo  Opera 3 Write Agent - Uninstaller
echo ========================================
echo.

:: Check for admin privileges
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: This must be run as Administrator.
    pause
    exit /b 1
)

set "INSTALL_DIR=%~dp0.."
set "NSSM_EXE=%~dp0nssm.exe"

if not exist "%NSSM_EXE%" (
    where nssm >nul 2>&1
    if %errorlevel% equ 0 (
        for /f "tokens=*" %%i in ('where nssm') do set "NSSM_EXE=%%i"
    ) else (
        echo ERROR: NSSM not found.
        pause
        exit /b 1
    )
)

:: Check if service exists
"%NSSM_EXE%" status OperaWriteAgent >nul 2>&1
if %errorlevel% neq 0 (
    echo Service OperaWriteAgent is not installed.
    pause
    exit /b 0
)

:: Stop the service
echo Stopping OperaWriteAgent service...
"%NSSM_EXE%" stop OperaWriteAgent >nul 2>&1
timeout /t 3 >nul

:: Remove the service
echo Removing OperaWriteAgent service...
"%NSSM_EXE%" remove OperaWriteAgent confirm

echo.
echo Service removed. Files remain at:
echo   %INSTALL_DIR%
echo.
echo To fully remove, delete the opera3_agent directory.
pause
