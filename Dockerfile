# Multi-stage Dockerfile for SQL RAG applications
# =================================================
#
# Single source of truth for all application containers. Pick a
# target with `--target <name>`:
#
#   bank-reconcile    Bank statement reconciliation
#   gocardless        Direct Debit payout import
#   suppliers         Supplier statement reconciliation
#   balance-check     Internal Opera balance reconciliation
#   core-email        Shared IMAP poller + email storage
#   core-opera-se     Opera SQL connection gateway
#   core-opera3       Opera 3 read gateway
#   monolith          All apps in one container (Phase A; default)
#
# Build:
#   docker build --target bank-reconcile -t sqlrag/bank-reconcile .
#   docker build --target monolith -t sqlrag/monolith .
#
# Run:
#   docker run -p 8000:8000 --env-file .env sqlrag/bank-reconcile
#
# All containers expect env-var configuration. See
# docs/sam-migration/env-var-contract.md for the full list.

# =============================================================
# Stage: base — system dependencies + Python
# =============================================================
FROM python:3.11-slim-bookworm AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System packages:
#   - curl: ODBC driver install + healthcheck
#   - gnupg, apt-transport-https: ODBC driver repo trust
#   - unixodbc, unixodbc-dev: pyodbc runtime + build
#   - build-essential: pyodbc build (removed in build-deps stage)
#   - cifs-utils: optional SMB mount (for Opera 3 file shares)
#   - tini: PID 1 signal forwarding
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        apt-transport-https \
        ca-certificates \
        unixodbc \
        unixodbc-dev \
        cifs-utils \
        tini \
    && rm -rf /var/lib/apt/lists/*

# Microsoft ODBC Driver 18 for SQL Server (Opera SE connection)
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# =============================================================
# Stage: build-deps — install Python deps in a venv
# =============================================================
FROM base AS build-deps

# Need build-essential to compile pyodbc + a few native deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Use a venv at /opt/venv so we can copy it into the final stages
# without dragging build-essential along.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pip install fastapi uvicorn[standard] python-multipart sqlalchemy

# =============================================================
# Stage: app-base — common runtime layer for all app targets
# =============================================================
FROM base AS app-base

# Bring in the pre-built venv (no compilers in the runtime image)
COPY --from=build-deps /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Application code
COPY api ./api
COPY apps ./apps
COPY sql_rag ./sql_rag
COPY scripts ./scripts

# Per-company state lives in a mounted volume; create the mount point
RUN mkdir -p /app/data /app/archive

# Run as a non-root user (security)
RUN useradd --create-home --shell /bin/bash --uid 1001 sqlrag \
    && chown -R sqlrag:sqlrag /app
USER sqlrag

EXPOSE 8000

# tini handles SIGTERM/SIGINT forwarding to uvicorn
ENTRYPOINT ["/usr/bin/tini", "--"]

# Healthcheck: every container must respond to /healthz on the
# uvicorn port. SAM may override; this is the local-dev default.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:8000/healthz || exit 1

# =============================================================
# Per-app targets
# =============================================================
# Each target sets:
#   - APP_NAME env var (logged at startup, used by health endpoint)
#   - INSTALLED_APPS env var (which routers register; comma-separated)
#   - CMD pointing at the relevant ASGI app
#
# All targets share the same image layer up to app-base. Only the
# CMD differs. SAM can pick the target it wants per service.
# =============================================================

# -------- bank-reconcile --------
FROM app-base AS bank-reconcile
ENV APP_NAME=bank-reconcile \
    INSTALLED_APPS=bank_reconcile
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]

# -------- gocardless --------
FROM app-base AS gocardless
ENV APP_NAME=gocardless \
    INSTALLED_APPS=gocardless
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]

# -------- suppliers --------
FROM app-base AS suppliers
ENV APP_NAME=suppliers \
    INSTALLED_APPS=suppliers
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]

# -------- balance-check --------
FROM app-base AS balance-check
ENV APP_NAME=balance-check \
    INSTALLED_APPS=balance_check
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]

# -------- core-email --------
# Shared IMAP poller + email storage + query API. Owned by core
# during Phase B; can be replaced by SAM equivalent in Phase C.
FROM app-base AS core-email
ENV APP_NAME=core-email \
    INSTALLED_APPS=core_email
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]

# -------- monolith (default — Phase A) --------
# All apps in one container. Same shape as today. Default target
# when no --target is specified.
FROM app-base AS monolith
ENV APP_NAME=monolith \
    INSTALLED_APPS=bank_reconcile,gocardless,suppliers,balance_check
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
