"""Adapter factory — env-var-driven port → adapter selection.

Apps call the get_*() functions from here; the factory decides
which adapter implementation to construct based on environment.

Selection logic (per port):

  1. If `SAM_ENABLED=true`:
       Use the SAM-specific adapter for that port (Phase C — not
       implemented yet; falls through to HTTP/local for now).
  2. If `<PORT>_URL` is set (e.g. `CORE_OPERA_SE_URL`):
       Use the HTTP adapter pointing at that URL (Phase B+).
  3. Otherwise:
       Use the local in-process adapter (default).

For Phase B today, only #3 is wired. SAM and HTTP adapters can be
added without touching call sites.
"""
from __future__ import annotations

import logging
from functools import lru_cache
from typing import TYPE_CHECKING

from apps.core.env_config import env_bool, env_str

if TYPE_CHECKING:
    from apps.core.ports import (
        AuthPort,
        CompanyContextPort,
        EmailStoragePort,
        EmailSyncPort,
        Opera3ReaderPort,
        Opera3WriterPort,
        OperaSQLPort,
        SMTPPort,
    )

logger = logging.getLogger(__name__)


# =====================================================================
# OperaSQL
# =====================================================================


def get_opera_sql() -> "OperaSQLPort":
    """Return the OperaSQL adapter for this process.

    Phase B: in-process by default. Set CORE_OPERA_SE_URL to enable
    the HTTP adapter (when it lands).
    """
    if env_bool('SAM_ENABLED') and env_str('SAM_OPERA_SQL_URL'):
        # Phase C placeholder — fall through to local for now.
        logger.debug("SAM_ENABLED but SAM OperaSQL adapter not implemented")
    if env_str('CORE_OPERA_SE_URL'):
        # Phase B HTTP adapter not implemented yet — fall through.
        logger.debug("CORE_OPERA_SE_URL set but HTTP OperaSQL adapter "
                     "not implemented")
    from apps.core.adapters.local.opera_sql import LocalOperaSQLAdapter
    return LocalOperaSQLAdapter()


# =====================================================================
# EmailStorage
# =====================================================================


def get_email_storage() -> "EmailStoragePort":
    if env_str('CORE_EMAIL_URL'):
        # Phase B HTTP adapter — not implemented yet.
        logger.debug("CORE_EMAIL_URL set but HTTP EmailStorage adapter "
                     "not implemented")
    from apps.core.adapters.local.email_storage import LocalEmailStorageAdapter
    return LocalEmailStorageAdapter()


# =====================================================================
# Opera3Reader
# =====================================================================


def get_opera3_reader(data_path: str | None = None) -> "Opera3ReaderPort":
    if env_str('CORE_OPERA3_URL'):
        logger.debug("CORE_OPERA3_URL set but HTTP Opera3Reader adapter "
                     "not implemented")
    from apps.core.adapters.local.opera3_reader import LocalOpera3ReaderAdapter
    return LocalOpera3ReaderAdapter(data_path)


def get_opera3_data_provider(data_path: str):
    """Return an Opera3DataProvider for the given Opera 3 data path.

    Convenience factory for routes that use the higher-level
    Opera3DataProvider (credit-control metrics, priority customers,
    aged debt summaries, etc.) rather than the lower-level reader.

    When SAM provides an equivalent Opera 3 data service, this
    factory would route to its HTTP adapter. Today: thin wrapper
    over the in-process provider, no behaviour change.
    """
    from sql_rag.opera3_data_provider import Opera3DataProvider
    return Opera3DataProvider(data_path)


# =====================================================================
# Opera3Writer (Write Agent — stays Windows-native)
# =====================================================================


def get_opera3_writer() -> "Opera3WriterPort":
    """Return the Opera 3 writer adapter (HTTP client to the Agent).

    🆕 SAM-hosted deployments: the Opera 3 Agent has been expanded to
    handle reads + writes. URL comes from `OPERA3_AGENT_URL` env var
    (per-tenant; SAM populates it).

    Standalone / legacy deployments: `OPERA3_WRITE_AGENT_URL` points
    at the customer-deployed Windows agent for writes only (reads use
    direct DBF access via Opera3ReaderPort).

    The adapter prefers `OPERA3_AGENT_URL` when set, falling back to
    the legacy variable — single adapter, two configurations.
    """
    from apps.core.adapters.local.opera3_writer import LocalOpera3WriterAdapter
    return LocalOpera3WriterAdapter()


# =====================================================================
# EmailSync
# =====================================================================


def get_email_sync() -> "EmailSyncPort":
    if env_str('CORE_EMAIL_URL'):
        logger.debug("CORE_EMAIL_URL set but HTTP EmailSync adapter "
                     "not implemented")
    from apps.core.adapters.local.email_sync import LocalEmailSyncAdapter
    return LocalEmailSyncAdapter()


# =====================================================================
# SMTP
# =====================================================================


def get_smtp() -> "SMTPPort":
    from apps.core.adapters.local.smtp import LocalSMTPAdapter
    return LocalSMTPAdapter()


# =====================================================================
# Auth
# =====================================================================


def get_auth() -> "AuthPort":
    if env_bool('SAM_ENABLED') and env_str('AUTH_JWT_PUBLIC_KEY'):
        # Phase C: SAM-issued JWT adapter — not implemented yet.
        logger.debug("SAM auth not yet implemented; using local adapter")
    if env_str('CORE_AUTH_URL'):
        logger.debug("CORE_AUTH_URL set but HTTP Auth adapter not "
                     "implemented")
    from apps.core.adapters.local.auth import LocalAuthAdapter
    return LocalAuthAdapter()


# =====================================================================
# CompanyContext (per-request tenant + system context)
# =====================================================================


def get_company_context() -> "CompanyContextPort":
    """Return the per-request company context adapter.

    SAM Phase C: read tenant + system from SAM-issued JWT instead
    of contextvars set by our auth middleware.
    """
    if env_bool('SAM_ENABLED') and env_str('AUTH_JWT_PUBLIC_KEY'):
        logger.debug("SAM-aware CompanyContext not yet implemented; "
                     "using local adapter")
    from apps.core.adapters.local.company_context import LocalCompanyContextAdapter
    return LocalCompanyContextAdapter()


# =====================================================================
# Convenience: for tests / debugging
# =====================================================================


def list_adapter_selection() -> dict[str, str]:
    """Return which adapter would be used per port. Useful for
    diagnostic endpoints."""
    return {
        'opera_sql': type(get_opera_sql()).__name__,
        'email_storage': type(get_email_storage()).__name__,
        'opera3_reader': type(get_opera3_reader()).__name__,
        'opera3_writer': type(get_opera3_writer()).__name__,
        'email_sync': type(get_email_sync()).__name__,
        'smtp': type(get_smtp()).__name__,
        'auth': type(get_auth()).__name__,
        'company_context': type(get_company_context()).__name__,
    }
