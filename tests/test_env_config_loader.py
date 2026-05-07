"""Tests for the env-var-first config loader.

Phase A — Step 1 of the SAM-readiness work pinned by these tests:
  - Env var > config.ini > built-in default precedence
  - configparser-shape return value (drop-in replacement)
  - Typed env helpers raise on bad input, accept defaults sensibly
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from apps.core.env_config import (
    KNOWN_SECTIONS,
    env_bool,
    env_float,
    env_int,
    env_required,
    env_str,
    get_config,
    load_config_ini_into_env,
    reload_config,
)


@pytest.fixture(autouse=True)
def _reset_config_state():
    """Reset cached config + clean known env vars between tests.

    Also redirects CONFIG_INI_PATH to a non-existent file so tests
    don't accidentally load the developer's real config.ini at the
    repo root. Individual tests that want to test config.ini handling
    override CONFIG_INI_PATH explicitly.
    """
    # Snapshot known-section env vars + CONFIG_INI_PATH
    snapshot = {}
    for section in KNOWN_SECTIONS:
        prefix = section.upper() + '_'
        for k in list(os.environ.keys()):
            if k.startswith(prefix):
                snapshot[k] = os.environ[k]
                del os.environ[k]
    if 'CONFIG_INI_PATH' in os.environ:
        snapshot['CONFIG_INI_PATH'] = os.environ['CONFIG_INI_PATH']

    # Point at a deliberately-missing path so the loader is a no-op
    # by default. Tests that want to test config.ini set their own.
    os.environ['CONFIG_INI_PATH'] = '/nonexistent/test/config.ini'

    reload_config()
    yield

    # Restore env vars
    for section in KNOWN_SECTIONS:
        prefix = section.upper() + '_'
        for k in list(os.environ.keys()):
            if k.startswith(prefix):
                del os.environ[k]
    if 'CONFIG_INI_PATH' in os.environ:
        del os.environ['CONFIG_INI_PATH']
    for k, v in snapshot.items():
        os.environ[k] = v
    reload_config()


# =====================================================================
# Env vars → ConfigParser
# =====================================================================


def test_env_var_populates_configparser_section():
    """An env var SECTION_KEY appears as cp.get('section', 'key')."""
    os.environ['DATABASE_SERVER'] = 'sql.example.com'
    cp = reload_config()
    assert cp.has_section('database')
    assert cp.get('database', 'server') == 'sql.example.com'


def test_multiple_keys_in_same_section():
    os.environ['DATABASE_SERVER'] = 'host'
    os.environ['DATABASE_PORT'] = '1433'
    os.environ['DATABASE_DATABASE'] = 'opera'
    cp = reload_config()
    assert cp.get('database', 'server') == 'host'
    assert cp.get('database', 'port') == '1433'
    assert cp.get('database', 'database') == 'opera'


def test_unknown_section_prefix_ignored():
    """Env var that doesn't match any known section prefix is ignored."""
    os.environ['SOMEUNKNOWN_FOO'] = 'bar'
    cp = reload_config()
    assert not cp.has_section('someunknown')


def test_getint_works_via_configparser():
    """Existing call sites use getint() — values must be parseable
    even though stored as strings."""
    os.environ['DATABASE_PORT'] = '1433'
    cp = reload_config()
    assert cp.getint('database', 'port') == 1433


def test_getboolean_works_via_configparser():
    os.environ['DATABASE_USE_WINDOWS_AUTH'] = 'False'
    cp = reload_config()
    assert cp.getboolean('database', 'use_windows_auth') is False
    os.environ['DATABASE_USE_WINDOWS_AUTH'] = 'True'
    cp = reload_config()
    assert cp.getboolean('database', 'use_windows_auth') is True


def test_fallback_returned_when_env_missing():
    """Existing call sites use config.get(..., fallback=X). When the
    env var isn't set, the section may not exist — fallback fires."""
    cp = reload_config()
    assert cp.get('gemini', 'api_key', fallback='') == ''


def test_all_known_sections_can_be_populated():
    """Every section listed in KNOWN_SECTIONS must round-trip an env var."""
    for section in KNOWN_SECTIONS:
        env_name = f"{section.upper()}_TEST_KEY"
        os.environ[env_name] = 'value'
    cp = reload_config()
    for section in KNOWN_SECTIONS:
        assert cp.has_section(section), f"section {section} not populated"
        assert cp.get(section, 'test_key') == 'value'
    # Cleanup is handled by the autouse fixture


# =====================================================================
# Precedence: env var > config.ini > caller-default
# =====================================================================


def test_env_var_wins_over_config_ini(tmp_path):
    """If both env var and config.ini are set, env var wins."""
    config_ini = tmp_path / 'config.ini'
    config_ini.write_text("[database]\nserver = from-config-ini\n")
    os.environ['DATABASE_SERVER'] = 'from-env-var'
    os.environ['CONFIG_INI_PATH'] = str(config_ini)
    try:
        cp = reload_config()
        assert cp.get('database', 'server') == 'from-env-var'
    finally:
        del os.environ['CONFIG_INI_PATH']


def test_config_ini_used_when_env_var_missing(tmp_path):
    """If env var is unset, config.ini supplies the value."""
    config_ini = tmp_path / 'config.ini'
    config_ini.write_text("[database]\nserver = from-config-ini\n")
    os.environ['CONFIG_INI_PATH'] = str(config_ini)
    try:
        cp = reload_config()
        assert cp.get('database', 'server') == 'from-config-ini'
    finally:
        del os.environ['CONFIG_INI_PATH']


def test_load_config_ini_into_env_skips_existing_env_var(tmp_path):
    """load_config_ini_into_env must NOT overwrite an env var that's
    already set — preserves env-wins-over-file precedence."""
    config_ini = tmp_path / 'config.ini'
    config_ini.write_text("[database]\nserver = from-file\n")
    os.environ['DATABASE_SERVER'] = 'from-env'
    os.environ['CONFIG_INI_PATH'] = str(config_ini)
    try:
        load_config_ini_into_env()
        assert os.environ['DATABASE_SERVER'] == 'from-env'
    finally:
        del os.environ['CONFIG_INI_PATH']


def test_load_config_ini_into_env_returns_count(tmp_path):
    """The loader returns the number of env vars it populated."""
    config_ini = tmp_path / 'config.ini'
    config_ini.write_text(
        "[database]\nserver = X\nport = 1433\n"
        "[gemini]\napi_key = key123\n"
    )
    os.environ['CONFIG_INI_PATH'] = str(config_ini)
    try:
        count = load_config_ini_into_env()
        assert count == 3
    finally:
        del os.environ['CONFIG_INI_PATH']


def test_no_config_ini_returns_zero():
    os.environ['CONFIG_INI_PATH'] = '/nonexistent/path/config.ini'
    try:
        assert load_config_ini_into_env() == 0
    finally:
        del os.environ['CONFIG_INI_PATH']


# =====================================================================
# env_str / env_int / env_bool / env_float / env_required
# =====================================================================


def test_env_str_returns_value_or_default():
    os.environ['SOMEKEY'] = 'hello'
    assert env_str('SOMEKEY') == 'hello'
    assert env_str('NOTSET', 'fallback') == 'fallback'
    assert env_str('NOTSET') is None


def test_env_int_parses_value():
    os.environ['MYINT'] = '42'
    assert env_int('MYINT') == 42


def test_env_int_default_for_missing():
    assert env_int('NOTSET', 99) == 99


def test_env_int_default_for_empty_string():
    """Empty env var should fall back to default (Docker often
    forwards literal empty strings for unset values)."""
    os.environ['EMPTYINT'] = ''
    assert env_int('EMPTYINT', 99) == 99


def test_env_int_raises_on_bad_value():
    os.environ['BADINT'] = 'not a number'
    with pytest.raises(RuntimeError, match='not a valid integer'):
        env_int('BADINT')


def test_env_float_parses_value():
    os.environ['MYFLOAT'] = '3.14'
    assert env_float('MYFLOAT') == 3.14


def test_env_float_raises_on_bad_value():
    os.environ['BADFLOAT'] = 'pi'
    with pytest.raises(RuntimeError, match='not a valid float'):
        env_float('BADFLOAT')


@pytest.mark.parametrize("val,expected", [
    ('true', True), ('TRUE', True), ('True', True),
    ('1', True), ('yes', True), ('on', True),
    ('false', False), ('FALSE', False),
    ('0', False), ('no', False), ('off', False),
    ('', False),
])
def test_env_bool_accepts_common_values(val, expected):
    os.environ['MYBOOL'] = val
    assert env_bool('MYBOOL') == expected


def test_env_bool_default_for_missing():
    assert env_bool('NOTSET', default=True) is True
    assert env_bool('NOTSET', default=False) is False


def test_env_bool_raises_on_garbage():
    os.environ['GARBAGE'] = 'maybe'
    with pytest.raises(RuntimeError, match='not a valid boolean'):
        env_bool('GARBAGE')


def test_env_required_raises_on_missing():
    with pytest.raises(RuntimeError, match='Required environment variable'):
        env_required('NEVER_SET_AT_ALL')


def test_env_required_returns_value_when_set():
    os.environ['REQUIRED_KEY'] = 'value'
    assert env_required('REQUIRED_KEY') == 'value'


def test_env_required_treats_empty_as_missing():
    """Production-deployed containers sometimes pass empty strings
    for unset values. Required vars must reject empty just like
    None — a service started with DATABASE_SERVER='' would fail
    silently otherwise."""
    os.environ['EMPTY_REQUIRED'] = ''
    with pytest.raises(RuntimeError):
        env_required('EMPTY_REQUIRED')


# =====================================================================
# Cache behaviour
# =====================================================================


def test_get_config_cached_until_reload():
    os.environ['DATABASE_SERVER'] = 'first'
    # Reload to seed cache with 'first' (autouse fixture cleared it)
    cp1 = reload_config()
    os.environ['DATABASE_SERVER'] = 'second'
    cp2 = get_config()
    assert cp1 is cp2  # same cached instance
    assert cp2.get('database', 'server') == 'first'  # stale on purpose


def test_reload_config_picks_up_new_env():
    os.environ['DATABASE_SERVER'] = 'first'
    cp1 = reload_config()
    os.environ['DATABASE_SERVER'] = 'second'
    cp2 = reload_config()
    assert cp2.get('database', 'server') == 'second'


def test_get_config_fresh_flag_bypasses_cache():
    os.environ['DATABASE_SERVER'] = 'first'
    cp1 = get_config()
    os.environ['DATABASE_SERVER'] = 'second'
    cp2 = get_config(fresh=True)
    assert cp2.get('database', 'server') == 'second'
