"""
Opera 3 Write Agent

A microservice that handles all Opera 3 FoxPro DBF writes with proper
CDX index maintenance via the Harbour DBFCDX bridge.

Components:
    service.py      - FastAPI write service (runs on Opera 3 server)
    harbour_dbf.py  - Python ctypes wrapper for Harbour shared library
    harbour/        - Harbour source and build scripts
    installer/      - Windows service installer
"""

__version__ = "1.0.0"
