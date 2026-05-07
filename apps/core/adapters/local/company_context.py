"""Local CompanyContext adapter — wraps the contextvars-based
in-process state set by the auth middleware."""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class LocalCompanyContextAdapter:
    """Reads per-request company context from apps.core.state.

    The middleware in api/main.py sets these per-request via
    `_ensure_company_context()`. Apps read them through this
    adapter without knowing which middleware populates them.
    """

    def get_company_id(self) -> Optional[str]:
        try:
            from apps.core import state
            cid = state._request_company_id.get()
            if cid:
                return cid
        except Exception:
            pass
        try:
            from api.main import _request_company_id
            return _request_company_id.get()
        except Exception:
            return None

    def get_company(self) -> Optional[dict[str, Any]]:
        try:
            from apps.core import state
            if state.current_company is not None:
                return state.current_company
        except Exception:
            pass
        try:
            from api.main import current_company
            return current_company
        except Exception:
            return None

    def get_active_system_id(self) -> Optional[str]:
        try:
            from apps.core import state
            if state.active_system_id is not None:
                return state.active_system_id
        except Exception:
            pass
        try:
            from api.main import active_system_id
            return active_system_id
        except Exception:
            return None
