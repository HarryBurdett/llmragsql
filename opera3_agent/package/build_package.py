"""
Opera 3 Write Agent — Deployment Package Builder

Creates a self-contained zip file that can be deployed on any Windows server
with Opera 3 installed. No prerequisites needed on the target server.

The package includes:
- Embedded Python 3.11 runtime (Windows x64)
- All Python dependencies pre-installed
- Write Agent service code
- NSSM (Windows Service wrapper)
- install.bat / uninstall.bat
- Pre-compiled Harbour DLL (if available)

Usage:
    python build_package.py

Output:
    opera3-write-agent-setup.zip

Note: The Harbour DLL (libdbfbridge.dll) must be compiled on Windows.
If not present, the package will include a compile script instead.
"""

import os
import sys
import shutil
import subprocess
import zipfile
import urllib.request
import tempfile
from pathlib import Path

# Configuration
PYTHON_VERSION = "3.11.9"
PYTHON_EMBED_URL = f"https://www.python.org/ftp/python/{PYTHON_VERSION}/python-{PYTHON_VERSION}-embed-amd64.zip"
NSSM_URL = "https://nssm.cc/release/nssm-2.24.zip"
PACKAGE_NAME = "opera3-write-agent-setup"

# Paths
SCRIPT_DIR = Path(__file__).parent
AGENT_DIR = SCRIPT_DIR.parent
PROJECT_DIR = AGENT_DIR.parent
OUTPUT_DIR = SCRIPT_DIR / "dist"
BUILD_DIR = SCRIPT_DIR / "build" / PACKAGE_NAME


def clean():
    """Clean previous build artifacts."""
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Build directory: {BUILD_DIR}")


def download_file(url: str, dest: Path, desc: str):
    """Download a file with progress indication."""
    print(f"Downloading {desc}...")
    print(f"  URL: {url}")
    try:
        urllib.request.urlretrieve(url, str(dest))
        print(f"  Saved to: {dest} ({dest.stat().st_size / 1024 / 1024:.1f} MB)")
    except Exception as e:
        print(f"  ERROR: {e}")
        print(f"  Download manually and place at: {dest}")
        return False
    return True


def setup_embedded_python():
    """Download and set up embedded Python for Windows."""
    python_dir = BUILD_DIR / "python"
    python_dir.mkdir(exist_ok=True)

    # Download embedded Python
    zip_path = SCRIPT_DIR / "build" / f"python-{PYTHON_VERSION}-embed.zip"
    if not zip_path.exists():
        if not download_file(PYTHON_EMBED_URL, zip_path, f"Python {PYTHON_VERSION} (Windows embedded)"):
            return False

    # Extract
    print("Extracting Python...")
    import zipfile
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(python_dir)

    # Enable pip in embedded Python by modifying the ._pth file
    pth_files = list(python_dir.glob("python*._pth"))
    for pth in pth_files:
        content = pth.read_text()
        if "#import site" in content:
            content = content.replace("#import site", "import site")
            pth.write_text(content)
            print(f"  Enabled site-packages in {pth.name}")

    # Download get-pip.py
    getpip_path = python_dir / "get-pip.py"
    if not getpip_path.exists():
        download_file("https://bootstrap.pypa.io/get-pip.py", getpip_path, "get-pip.py")

    print("  Note: pip will be installed on first run via install.bat")
    return True


def setup_nssm():
    """Download NSSM (Windows Service wrapper)."""
    nssm_dir = BUILD_DIR / "installer"
    nssm_dir.mkdir(exist_ok=True)

    zip_path = SCRIPT_DIR / "build" / "nssm.zip"
    if not zip_path.exists():
        if not download_file(NSSM_URL, zip_path, "NSSM (Windows Service Manager)"):
            return False

    # Extract the 64-bit exe
    print("Extracting NSSM...")
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            if member.endswith("win64/nssm.exe"):
                data = zf.read(member)
                nssm_path = nssm_dir / "nssm.exe"
                nssm_path.write_bytes(data)
                print(f"  Extracted: {nssm_path}")
                return True

    print("  ERROR: Could not find nssm.exe in archive")
    return False


def copy_agent_code():
    """Copy the Write Agent source code."""
    agent_dest = BUILD_DIR / "opera3_agent"
    agent_dest.mkdir(exist_ok=True)

    # Copy Python source files
    for filename in [
        "__init__.py",
        "service.py",
        "harbour_dbf.py",
        "transaction_safety.py",
        "write_ahead_log.py",
        "requirements.txt",
    ]:
        src = AGENT_DIR / filename
        if src.exists():
            shutil.copy2(src, agent_dest / filename)
            print(f"  Copied: {filename}")

    # Copy opera3_config if it exists
    config_src = PROJECT_DIR / "sql_rag" / "opera3_config.py"
    if config_src.exists():
        shutil.copy2(config_src, agent_dest / "opera3_config.py")
        print(f"  Copied: opera3_config.py (from sql_rag/)")

    # Copy opera3_foxpro_import for the actual posting logic
    import_src = PROJECT_DIR / "sql_rag" / "opera3_foxpro_import.py"
    if import_src.exists():
        shutil.copy2(import_src, agent_dest / "opera3_foxpro_import.py")
        print(f"  Copied: opera3_foxpro_import.py (from sql_rag/)")

    # Copy opera3_foxpro (reader) — needed for reading tables
    reader_src = PROJECT_DIR / "sql_rag" / "opera3_foxpro.py"
    if reader_src.exists():
        shutil.copy2(reader_src, agent_dest / "opera3_foxpro.py")
        print(f"  Copied: opera3_foxpro.py (from sql_rag/)")

    # Copy Harbour bridge
    harbour_dest = agent_dest / "harbour"
    harbour_dest.mkdir(exist_ok=True)
    harbour_src = AGENT_DIR / "harbour"
    if harbour_src.exists():
        for f in harbour_src.iterdir():
            shutil.copy2(f, harbour_dest / f.name)
            print(f"  Copied harbour: {f.name}")

    return True


def copy_installer():
    """Copy installer scripts."""
    installer_src = AGENT_DIR / "installer"
    installer_dest = BUILD_DIR / "installer"
    installer_dest.mkdir(exist_ok=True)

    for f in installer_src.iterdir():
        if f.name != "nssm.exe":  # NSSM handled separately
            shutil.copy2(f, installer_dest / f.name)
            print(f"  Copied installer: {f.name}")

    # Copy install.bat to root for easy access
    install_bat = installer_dest / "install.bat"
    if install_bat.exists():
        shutil.copy2(install_bat, BUILD_DIR / "install.bat")
        print(f"  Copied install.bat to package root")

    uninstall_bat = installer_dest / "uninstall.bat"
    if uninstall_bat.exists():
        shutil.copy2(uninstall_bat, BUILD_DIR / "uninstall.bat")
        print(f"  Copied uninstall.bat to package root")

    return True


def create_readme():
    """Create a README for the package."""
    readme = BUILD_DIR / "README.txt"
    readme.write_text("""
Opera 3 Write Agent — Quick Start
===================================

This service safely writes to Opera 3 FoxPro (DBF) files with proper
file locking. It runs on the Opera 3 server alongside the data files.

INSTALLATION
============

1. Copy this entire folder to the Opera 3 server
2. Right-click install.bat and select "Run as administrator"
3. Follow the prompts (enter Opera 3 data path if not auto-detected)
4. Save the URL and Key displayed at the end
5. Enter these in the main application:
   Installations > Opera 3 > Write Agent section

REQUIREMENTS
============
- Windows Server (same machine as Opera 3 data files)
- Administrator privileges for installation
- Port 9000 open (or configure a different port)

SERVICE MANAGEMENT
==================
The agent installs as a Windows Service called "OperaWriteAgent".
- Starts automatically on boot
- Restarts automatically on crash
- Logs at: logs\\agent.log

To stop:     net stop OperaWriteAgent
To start:    net start OperaWriteAgent
To remove:   uninstall.bat (as Administrator)

TROUBLESHOOTING
===============
- Check logs\\agent-error.log for errors
- Ensure the Opera 3 data path is correct
- Ensure port 9000 is not blocked by firewall
- Ensure no other service is using port 9000
""")
    print("  Created README.txt")
    return True


def create_zip():
    """Create the final deployment zip."""
    zip_path = OUTPUT_DIR / f"{PACKAGE_NAME}.zip"
    print(f"\nCreating deployment package: {zip_path}")

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(BUILD_DIR):
            for file in files:
                file_path = Path(root) / file
                arcname = file_path.relative_to(BUILD_DIR.parent)
                zf.write(file_path, arcname)

    size_mb = zip_path.stat().st_size / 1024 / 1024
    print(f"Package created: {zip_path} ({size_mb:.1f} MB)")
    return zip_path


def main():
    print("=" * 60)
    print("Opera 3 Write Agent — Package Builder")
    print("=" * 60)
    print()

    clean()

    print("\n[1/5] Setting up embedded Python...")
    if not setup_embedded_python():
        print("WARNING: Embedded Python setup failed — package may need system Python")

    print("\n[2/5] Setting up NSSM...")
    if not setup_nssm():
        print("WARNING: NSSM download failed — include manually")

    print("\n[3/5] Copying agent code...")
    copy_agent_code()

    print("\n[4/5] Copying installer...")
    copy_installer()
    create_readme()

    print("\n[5/5] Creating zip package...")
    zip_path = create_zip()

    print()
    print("=" * 60)
    print("DONE")
    print("=" * 60)
    print(f"\nDeployment package: {zip_path}")
    print()
    print("Next steps:")
    print("1. Copy the zip to the Opera 3 Windows server")
    print("2. Extract to a permanent location (e.g., C:\\Opera3WriteAgent)")
    print("3. Right-click install.bat → Run as administrator")
    print("4. Enter the URL and Key in the main app's Installations page")
    print()

    # Check for Harbour DLL
    harbour_dll = AGENT_DIR / "harbour" / "libdbfbridge.dll"
    if not harbour_dll.exists():
        print("NOTE: Harbour DLL (libdbfbridge.dll) not found.")
        print("You must compile it on the Windows server:")
        print("  1. Install Harbour from https://harbour.github.io/")
        print("  2. cd opera3_agent\\harbour")
        print("  3. hbmk2 dbfbridge.prg -shared -o libdbfbridge.dll")
        print()


if __name__ == "__main__":
    main()
