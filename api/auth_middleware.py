"""
Authentication Middleware for FastAPI.

Validates session tokens and attaches user info to request state.
"""

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from typing import Callable, Set
import logging

logger = logging.getLogger(__name__)


# Paths that don't require authentication
PUBLIC_PATHS: Set[str] = {
    '/api/auth/login',
    '/api/health',
    '/docs',
    '/openapi.json',
    '/redoc',
}

# Path prefixes that don't require authentication
PUBLIC_PREFIXES: tuple = (
    '/docs',
    '/openapi',
    '/redoc',
)


class AuthMiddleware(BaseHTTPMiddleware):
    """Middleware to validate authentication tokens on protected routes."""

    def __init__(self, app, user_auth):
        super().__init__(app)
        self.user_auth = user_auth

    async def dispatch(self, request: Request, call_next: Callable):
        """Process the request, validating authentication if needed."""
        path = request.url.path

        # Skip auth for public paths
        if path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES):
            return await call_next(request)

        # Skip auth for non-API paths (static files, etc.)
        if not path.startswith('/api/'):
            return await call_next(request)

        # Get token from Authorization header
        auth_header = request.headers.get('Authorization', '')
        token = auth_header.replace('Bearer ', '') if auth_header.startswith('Bearer ') else ''

        if not token:
            logger.warning(f"No auth token provided for {request.method} {path}")
            return JSONResponse(
                status_code=401,
                content={'error': 'Not authenticated', 'detail': 'No authentication token provided'}
            )

        # Validate token
        user = self.user_auth.validate_session(token)
        if not user:
            logger.warning(f"Invalid or expired token for {request.method} {path}")
            return JSONResponse(
                status_code=401,
                content={'error': 'Invalid or expired session', 'detail': 'Please log in again'}
            )

        # Attach user to request state
        request.state.user = user
        request.state.user_permissions = self.user_auth.get_user_permissions(user['id'])

        return await call_next(request)


def require_admin(request: Request) -> bool:
    """Check if the current user is an admin. Raises 403 if not."""
    user = getattr(request.state, 'user', None)
    if not user or not user.get('is_admin'):
        return False
    return True


def get_current_user(request: Request) -> dict:
    """Get the current authenticated user from request state."""
    return getattr(request.state, 'user', None)


def get_user_permissions(request: Request) -> dict:
    """Get the current user's permissions from request state."""
    return getattr(request.state, 'user_permissions', {})
