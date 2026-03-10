#!/bin/bash
# Build the Harbour DBF/CDX bridge shared library
#
# Prerequisites:
#   macOS:   brew install harbour (or build from https://github.com/vszakats/hb)
#   Linux:   apt-get install harbour (or build from source)
#   Windows: Install harbour and ensure hbmk2 is in PATH
#
# Usage:
#   ./build.sh          # Build for current platform
#   ./build.sh clean    # Remove build artefacts
#   ./build.sh test     # Build and run basic test

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ "$1" = "clean" ]; then
    echo "Cleaning build artefacts..."
    rm -f libdbfbridge.dylib libdbfbridge.so libdbfbridge.dll
    rm -f dbfbridge.c dbfbridge.o
    rm -f *.ppo *.o
    echo "Done."
    exit 0
fi

# Check for hbmk2
if ! command -v hbmk2 &> /dev/null; then
    echo "Error: hbmk2 not found."
    echo ""
    echo "Install Harbour:"
    echo "  macOS:   brew install harbour"
    echo "           OR build from https://github.com/vszakats/hb"
    echo "  Linux:   sudo apt-get install harbour"
    echo "  Windows: Download from https://github.com/vszakats/hb/releases"
    exit 1
fi

echo "Building Harbour DBF/CDX bridge..."
echo "Platform: $(uname -s) $(uname -m)"
echo ""

# Build self-contained shared library with embedded Harbour VM
hbmk2 -hbdynvm dbfbridge.prg -o libdbfbridge

# Verify output
if [ -f "libdbfbridge.dylib" ]; then
    echo ""
    echo "Built: libdbfbridge.dylib (macOS)"
    ls -lh libdbfbridge.dylib
elif [ -f "libdbfbridge.so" ]; then
    echo ""
    echo "Built: libdbfbridge.so (Linux)"
    ls -lh libdbfbridge.so
elif [ -f "libdbfbridge.dll" ]; then
    echo ""
    echo "Built: libdbfbridge.dll (Windows)"
    ls -lh libdbfbridge.dll
else
    echo "Error: No shared library produced."
    exit 1
fi

if [ "$1" = "test" ]; then
    echo ""
    echo "Running basic integration test..."
    cd "$SCRIPT_DIR/.."
    python3 -c "
from harbour_dbf import HarbourDBF
db = HarbourDBF('harbour/libdbfbridge')
print('Harbour VM initialised successfully')
db.shutdown()
print('Harbour VM shut down cleanly')
print('TEST PASSED')
"
fi

echo ""
echo "Done. Copy the library to opera3_agent/ for deployment."
