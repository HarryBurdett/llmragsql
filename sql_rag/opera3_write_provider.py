"""
Opera 3 Write Provider — Automatic routing for Opera 3 write operations.

Returns either an Opera3AgentClient (remote write agent with CDX index
maintenance) or a direct Opera3FoxProImport instance, depending on
configuration and agent availability.

Usage:
    from sql_rag.opera3_write_provider import get_opera3_writer, is_agent_available

    # Get a writer — automatically selects agent or direct
    writer = get_opera3_writer(data_path)
    result = writer.import_sales_receipt(...)

    # Check agent status (for UI indicators)
    available, info = is_agent_available()

Both Opera3AgentClient and Opera3FoxProImport share the same method signatures,
so callers don't need to know which implementation is active.

Configuration:
    Set these environment variables (or pass via company settings):
    - OPERA3_AGENT_URL:  Agent service URL (e.g., http://opera3-server:9000)
    - OPERA3_AGENT_KEY:  Shared secret for agent authentication
    - OPERA3_AGENT_REQUIRED: Set to "1" to BLOCK writes when agent is offline
                             (recommended for production)
"""

from __future__ import annotations

import os
import logging
import threading
import time
from typing import Optional, Dict, Any, Tuple, Union

logger = logging.getLogger(__name__)

# ============================================================
# Module-level singleton for the agent client
# ============================================================

_agent_client = None
_agent_lock = threading.Lock()
_agent_config: Dict[str, str] = {}


def configure_agent(
    agent_url: str = "",
    agent_key: str = "",
    required: bool = False,
):
    """Configure the agent connection.

    Call this once at application startup (e.g., after loading company settings).
    Can also be called again when switching companies.

    Args:
        agent_url: Agent service URL (e.g., http://opera3-server:9000)
        agent_key: Shared secret for authentication
        required: If True, block writes when agent is offline
    """
    global _agent_client, _agent_config

    with _agent_lock:
        _agent_config = {
            "url": agent_url,
            "key": agent_key,
            "required": "1" if required else "0",
        }
        _agent_client = None  # Force re-creation on next use

        if agent_url:
            logger.info(f"Opera 3 Write Agent configured: {agent_url}")
        else:
            logger.info("Opera 3 Write Agent not configured — using direct writes")


def _get_agent_url() -> str:
    """Get the agent URL from config or environment."""
    return _agent_config.get("url", "") or os.environ.get("OPERA3_AGENT_URL", "")


def _get_agent_key() -> str:
    """Get the agent key from config or environment."""
    return _agent_config.get("key", "") or os.environ.get("OPERA3_AGENT_KEY", "")


def _is_agent_required() -> bool:
    """Check if the agent is required (block writes when offline)."""
    config_val = _agent_config.get("required", "")
    env_val = os.environ.get("OPERA3_AGENT_REQUIRED", "")
    return config_val == "1" or env_val == "1"


def _get_or_create_client():
    """Get or create the singleton agent client."""
    global _agent_client

    agent_url = _get_agent_url()
    if not agent_url:
        return None

    with _agent_lock:
        if _agent_client is None:
            try:
                from sql_rag.opera3_agent_client import Opera3AgentClient
                _agent_client = Opera3AgentClient(
                    base_url=agent_url,
                    agent_key=_get_agent_key(),
                    timeout=30.0,
                    health_check_interval=30.0,
                )
                logger.info(f"Opera 3 agent client created for {agent_url}")
            except Exception as e:
                logger.warning(f"Failed to create agent client: {e}")
                return None
        return _agent_client


# ============================================================
# Public API
# ============================================================

def is_agent_available() -> Tuple[bool, Dict[str, Any]]:
    """Check if the Opera 3 Write Agent is available.

    Returns:
        Tuple of (available: bool, info: dict with status details)
    """
    agent_url = _get_agent_url()
    if not agent_url:
        return False, {
            "configured": False,
            "message": "Opera 3 Write Agent not configured",
        }

    client = _get_or_create_client()
    if client is None:
        return False, {
            "configured": True,
            "url": agent_url,
            "message": "Failed to create agent client",
        }

    available = client.is_available()
    health = client.get_health_info()

    return available, {
        "configured": True,
        "available": available,
        "url": agent_url,
        "info": health.get("info", {}),
        "message": "Online" if available else "Offline — not responding",
    }


def get_opera3_writer(data_path: str = ""):
    """Get an Opera 3 writer instance.

    Automatically selects the best available method:
    1. If agent is configured and available → Opera3AgentClient (safe, CDX-maintained)
    2. If agent is configured but offline AND required → raises error
    3. If agent is not configured or offline (not required) → direct Opera3FoxProImport

    Args:
        data_path: Path to Opera 3 data files (used for direct fallback)

    Returns:
        An object with the same interface as Opera3FoxProImport

    Raises:
        Opera3AgentRequired: If agent is required but offline
    """
    client = _get_or_create_client()

    # Try agent first
    if client is not None:
        if client.is_available():
            logger.debug("Using Opera 3 Write Agent (remote)")
            return client
        else:
            if _is_agent_required():
                raise Opera3AgentRequired(
                    "Opera 3 Write Agent is required but not available. "
                    "Ensure the service is running on the Opera 3 server. "
                    "Imports are blocked until the agent is online."
                )
            logger.warning(
                "Opera 3 Write Agent is configured but offline. "
                "Falling back to direct writes (CDX indexes will NOT be maintained)."
            )

    # Fallback to direct writes
    if not data_path:
        raise ValueError("data_path is required when agent is not available")

    try:
        from sql_rag.opera3_foxpro_import import Opera3FoxProImport
        logger.debug(f"Using direct Opera 3 writes to {data_path}")
        return Opera3FoxProImport(data_path)
    except ImportError:
        raise ImportError(
            "Neither Opera 3 Write Agent nor Opera3FoxProImport is available. "
            "Install the dbf package or configure the Write Agent."
        )


def get_opera3_writer_or_error(data_path: str = ""):
    """Like get_opera3_writer but returns (writer, error_message) tuple.

    Convenience method for API endpoints that need to return errors as JSON.

    Returns:
        Tuple of (writer, None) on success or (None, error_message) on failure
    """
    try:
        writer = get_opera3_writer(data_path)
        return writer, None
    except Opera3AgentRequired as e:
        return None, str(e)
    except Exception as e:
        return None, f"Failed to initialise Opera 3 writer: {e}"


# ============================================================
# Exceptions
# ============================================================

class Opera3AgentRequired(Exception):
    """Raised when the agent is required but not available."""
    pass
