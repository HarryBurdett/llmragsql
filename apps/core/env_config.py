"""Environment-variable-first configuration loader.

Phase A — Step 1 of the SAM-readiness work. This module replaces the
`config = configparser.ConfigParser(); config.read('config.ini')`
pattern with a loader that reads env vars first, falls back to
`config.ini` for development.

Why a configparser-shaped output?
------------------------------------
The codebase has ~250 sites that call `config.get('section', 'key')`
or `config.getint(...)`. Returning a populated `ConfigParser` lets us
roll out env-var support without touching any of those call sites.
Each call site keeps its existing API; we just change where the
values come from.

Env-var convention
-------------------
Each `[section] key = value` in `config.ini` maps to env var
`SECTION_KEY` (uppercased). For example:
    [database]      → DATABASE_*
        server = X  →   DATABASE_SERVER=X
        port = 1433 →   DATABASE_PORT=1433

Precedence (highest first):
    1. Env var (set in container, docker-compose, SAM, shell)
    2. config.ini (development convenience)
    3. Whatever default the call site passes (e.g. fallback=...)

Dev workflow
-------------
Drop a `config.ini` in the repo root as today. The loader reads it
into env vars at startup so subsequent `config.get(...)` calls work
identically.

Container workflow
-------------------
Don't ship `config.ini`. Set env vars instead — via docker-compose
`environment:` block, `.env` file, or SAM-injected secrets.

SAM workflow
-------------
SAM populates env vars (or mounts them as files that get read into
env vars at startup). The application code keeps reading from env
vars — only the source changes.
"""
from __future__ import annotations

import configparser
import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# Section names known to map from configparser sections to env var
# prefixes. New sections can be added here without hunting through
# call sites — anything matching SECTION_KEY in the environment will
# be picked up automatically.
KNOWN_SECTIONS = (
    'database',
    'email',
    'email_imap',
    'email_microsoft',  # MS Graph email provider
    'email_gmail',      # Gmail provider
    'gemini',
    'openai',
    'anthropic',
    'groq',
    'models',
    'opera',
    'system',
    'ui',
    'gocardless',
    'auth',
    'sam',              # SAM platform integration
)


def _config_ini_path() -> Path:
    """The optional config.ini at the repo root.

    Override with CONFIG_INI_PATH env var when running outside the
    source tree (containers point this at a mounted file or skip it
    entirely).
    """
    override = os.environ.get('CONFIG_INI_PATH')
    if override:
        return Path(override)
    return Path(__file__).parent.parent.parent / 'config.ini'


def _section_key_to_env(section: str, key: str) -> str:
    """Compute the env var name for a [section] key combination.

    Convention: SECTION_KEY in upper-case. Section name is included
    even when it would create a redundant prefix (e.g. EMAIL_EMAIL_*
    is avoided by section names that don't repeat the key).
    """
    return f"{section}_{key}".upper()


def load_config_ini_into_env() -> int:
    """Read `config.ini` (if present) and populate env vars for any
    keys that aren't already set in the environment.

    Returns the number of vars populated. Safe to call multiple
    times — only fills in missing env vars (env > config.ini).
    """
    path = _config_ini_path()
    if not path.exists():
        logger.info("No config.ini found at %s; relying on env vars only", path)
        return 0

    cp = configparser.ConfigParser()
    cp.read(path)
    populated = 0
    for section in cp.sections():
        for key in cp[section]:
            env_name = _section_key_to_env(section, key)
            if env_name in os.environ:
                continue
            os.environ[env_name] = cp[section][key]
            populated += 1
    if populated:
        logger.info("Populated %d env vars from %s", populated, path)
    return populated


def get_config(*, fresh: bool = False) -> configparser.ConfigParser:
    """Return a ConfigParser populated from env vars (and optionally
    config.ini as a development fallback).

    The returned object is a drop-in replacement for the existing
    `configparser.ConfigParser()` calls — all `config.get(...)`,
    `config.getint(...)`, `config.has_section(...)` calls keep working.

    Args:
        fresh: If True, rebuild from current env. Default False uses
        a cached value (faster, recommended for the lifetime of an
        HTTP request — reload on startup or explicit refresh).
    """
    global _CACHED_CONFIG
    if not fresh and _CACHED_CONFIG is not None:
        return _CACHED_CONFIG

    # Step 1: Pre-populate env vars from config.ini if any exist
    load_config_ini_into_env()

    # Step 2: Build a ConfigParser entirely from env vars matching
    # any of the known section prefixes. Fields that aren't set
    # remain absent (existing call sites use fallback= to handle
    # missing values).
    cp = configparser.ConfigParser()
    for section in KNOWN_SECTIONS:
        section_prefix = section.upper() + '_'
        section_keys: dict[str, str] = {}
        for env_name, value in os.environ.items():
            if env_name.startswith(section_prefix):
                key = env_name[len(section_prefix):].lower()
                if key:
                    section_keys[key] = value
        if section_keys:
            cp.add_section(section)
            for key, value in section_keys.items():
                cp.set(section, key, value)

    _CACHED_CONFIG = cp
    return cp


_CACHED_CONFIG: Optional[configparser.ConfigParser] = None


def reload_config() -> configparser.ConfigParser:
    """Force a config rebuild from current env. Useful in tests."""
    global _CACHED_CONFIG
    _CACHED_CONFIG = None
    return get_config()


# ---------------------------------------------------------------
# Direct env-var helpers — preferred for new code
# ---------------------------------------------------------------
#
# Existing call sites use `config.get('section', 'key', fallback=X)`.
# New code should prefer these helpers — they're more explicit about
# which env var is being read and they fail loudly when required
# variables are missing.


def env_str(key: str, default: Optional[str] = None) -> Optional[str]:
    """Read a string env var with optional default."""
    return os.environ.get(key, default)


def env_required(key: str) -> str:
    """Read a string env var; raise if not set."""
    val = os.environ.get(key)
    if not val:
        raise RuntimeError(
            f"Required environment variable {key} is not set. "
            f"For docker-compose, add it to the service's environment block. "
            f"For SAM, ensure the platform provides it."
        )
    return val


def env_int(key: str, default: int = 0) -> int:
    """Read an integer env var. Empty string → default."""
    val = os.environ.get(key)
    if val is None or val.strip() == '':
        return default
    try:
        return int(val)
    except ValueError as e:
        raise RuntimeError(
            f"Env var {key}={val!r} is not a valid integer ({e})"
        ) from e


def env_float(key: str, default: float = 0.0) -> float:
    val = os.environ.get(key)
    if val is None or val.strip() == '':
        return default
    try:
        return float(val)
    except ValueError as e:
        raise RuntimeError(
            f"Env var {key}={val!r} is not a valid float ({e})"
        ) from e


def env_bool(key: str, default: bool = False) -> bool:
    """Read a boolean env var. Accepts: true/false, 1/0, yes/no, on/off
    (case-insensitive)."""
    val = os.environ.get(key)
    if val is None:
        return default
    val_lower = val.strip().lower()
    if val_lower in ('true', '1', 'yes', 'on'):
        return True
    if val_lower in ('false', '0', 'no', 'off', ''):
        return False
    raise RuntimeError(
        f"Env var {key}={val!r} is not a valid boolean "
        "(expected true/false, 1/0, yes/no, on/off)"
    )
