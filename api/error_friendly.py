"""Friendly database / connection error translator.

Lives in its own module so the route packages (apps/bank_reconcile,
apps/suppliers, apps/gocardless) can import it without forming a
circular dependency with `api.main` (which imports those packages
on startup).

`api.main` re-exports `friendly_db_error` for backwards compatibility
with code paths that still import from there.
"""
from __future__ import annotations


def friendly_db_error(error: Exception) -> str:
    """Translate raw database/connection errors into clear, user-friendly messages.

    Per CLAUDE.md "Error Message Clarity" rule. Routes MUST wrap any raw
    `Exception` returned in API responses with this helper instead of
    leaking pyodbc / SQLAlchemy stack-trace strings to the UI.
    """
    msg = str(error)
    msg_lower = msg.lower()

    # Opera database locked or inaccessible (e.g. backup running, exclusive lock)
    if '4060' in msg or 'cannot open database' in msg_lower:
        return (
            'Opera database is currently unavailable — it may be locked '
            'by a backup or another process. Please try again in a few minutes.'
        )

    # Login/authentication failure
    if '18456' in msg or 'login failed' in msg_lower:
        return (
            'Cannot connect to Opera — database login failed. '
            'Please check the connection settings.'
        )

    # Connection timeout
    if 'timeout' in msg_lower and ('connection' in msg_lower or 'login' in msg_lower):
        return (
            'Connection to the Opera database timed out. The server may '
            'be busy or unreachable. Please try again shortly.'
        )

    # Server not reachable / network error
    if (
        'server is not found' in msg_lower
        or 'network' in msg_lower
        or 'unreachable' in msg_lower
        or 'tcp provider' in msg_lower
    ):
        return (
            'Cannot reach the Opera database server. Please check the '
            'network connection and try again.'
        )

    # Connection reset / dropped
    if (
        'connection reset' in msg_lower
        or 'connection has been closed' in msg_lower
        or 'broken pipe' in msg_lower
    ):
        return 'The database connection was interrupted. Please try again.'

    # Deadlock
    if '1205' in msg or 'deadlock' in msg_lower:
        return (
            'The operation was temporarily blocked by another user. '
            'Please try again in a moment.'
        )

    # Lock timeout
    if 'lock request time out' in msg_lower or 'lock timeout' in msg_lower:
        return (
            'Opera is busy — another user or process is currently updating '
            'the same data. Please wait a moment and try again.'
        )

    # Table not found
    if 'invalid object name' in msg_lower:
        return (
            'A required Opera table was not found. Please check the '
            'database connection is pointing to the correct Opera company.'
        )

    # Duplicate key
    if (
        'duplicate' in msg_lower
        or 'unique constraint' in msg_lower
        or 'cannot insert' in msg_lower
    ):
        return (
            'A duplicate record was detected — this entry may already '
            'exist in Opera.'
        )

    # Foreign key violation
    if 'foreign key' in msg_lower:
        return (
            'Invalid account code — please verify the customer, supplier, '
            'or nominal account exists in Opera.'
        )

    # Generic connection error from our connector layer
    if (
        'database connection failed' in msg_lower
        or 'query execution failed' in msg_lower
    ):
        inner = msg.split(': ', 1)[-1] if ': ' in msg else msg
        inner_friendly = friendly_db_error(Exception(inner))
        if inner_friendly != inner:
            return inner_friendly
        return 'Cannot connect to the Opera database. Please check the connection and try again.'

    # Fallback — return a sanitised version (no raw SQL)
    return 'An unexpected database error occurred. Please try again or contact support.'
