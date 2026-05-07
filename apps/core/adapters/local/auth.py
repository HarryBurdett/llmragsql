"""Local Auth adapter — wraps the in-process session-based auth."""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class LocalAuthAdapter:
    """Wraps the in-process user_auth singleton."""

    def validate(self, token_or_cookie: str) -> Optional[dict[str, Any]]:
        try:
            from apps.core import state
            if state.user_auth is None:
                return None
            user = state.user_auth.validate_session(token_or_cookie)
            return user
        except Exception as e:
            logger.warning(f"Auth validate failed: {e}")
            return None

    def get_current_user(self) -> Optional[dict[str, Any]]:
        try:
            from apps.core import state
            # The middleware in api/main.py stores the user under
            # request.state.user; the local adapter peeks via the
            # contextvar in apps.core.state.
            company_id = state._request_company_id.get()
            if company_id and state.current_company:
                return {
                    'company_id': company_id,
                    'username': (state.current_company.get('username')
                                 if isinstance(state.current_company, dict)
                                 else None),
                    'roles': [],
                }
            return None
        except Exception:
            return None
