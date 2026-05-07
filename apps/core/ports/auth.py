"""Auth port — validate request authentication.

The local adapter wraps today's session-based auth (sessions stored
in installations.db). The HTTP adapter calls a core-auth service.
The SAM adapter validates SAM-issued JWTs against
AUTH_JWT_PUBLIC_KEY.

Auth in the apps is currently middleware — apps don't usually call
the auth port directly. The port exists so per-app SAM migration
can swap the validation mechanism without touching middleware
plumbing in api/main.py.
"""
from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable


@runtime_checkable
class AuthPort(Protocol):
    """Validate a request's auth credentials.

    Implementations:
      - Local: session cookie → installations.db user record
      - HTTP:  delegates to core-auth via API
      - SAM:   validates JWT against AUTH_JWT_PUBLIC_KEY
    """

    def validate(
        self,
        token_or_cookie: str,
    ) -> Optional[dict[str, Any]]:
        """Return user dict if valid, None if not.

        User dict shape:
          {'username': str, 'company_id': str, 'roles': list[str]}
        """
        ...

    def get_current_user(self) -> Optional[dict[str, Any]]:
        """Return the user for the active request context, or None.

        Backed by contextvars in the local adapter — middleware sets
        the user at request entry.
        """
        ...
