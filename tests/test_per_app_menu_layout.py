"""Pin the per-app menu layout: each app owns its Settings + Cleardown.

Per-app settings used to live in the Admin > Module Setup section.
After Phase B + the SAM-readiness work, settings live INSIDE each
app's own menu (settings live where the work lives). This matches
the per-app independence we built into the runtime.

These tests source-inspect frontend/src/components/Layout.tsx
to pin the new menu shape so a future edit can't quietly drop
settings or move them back to Admin without the test failing.

What's checked:
  - Each app menu has a 'Setup' section (alongside its 'Workflow')
  - Cashbook menu's Setup contains Settings + Routines Cleardown
  - GoCardless menu's Setup contains Settings + Cleardown
  - Suppliers menu's Setup contains Settings + Cleardown
  - Admin menu no longer has bank-rec / gocardless / supplier
    settings paths under Module Setup
  - System-wide items remain in Admin (Application Settings,
    Lock Monitor, Date & Company, Installations, Users, Licenses)
"""
from __future__ import annotations

from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
LAYOUT_TSX = REPO / "frontend" / "src" / "components" / "Layout.tsx"
APP_TSX = REPO / "frontend" / "src" / "App.tsx"


@pytest.fixture(scope="module")
def layout_src() -> str:
    return LAYOUT_TSX.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def app_src() -> str:
    return APP_TSX.read_text(encoding="utf-8")


# ====================================================================
# Per-app Setup section presence
# ====================================================================


def test_cashbook_menu_has_setup_section_with_settings(layout_src):
    cashbook_block = _extract_block(layout_src, "const cashbookMenu")
    assert "heading: 'Setup'" in cashbook_block
    assert "/cashbook/options" in cashbook_block
    assert "label: 'Settings'" in cashbook_block


def test_cashbook_menu_setup_contains_routines_cleardown(layout_src):
    cashbook_block = _extract_block(layout_src, "const cashbookMenu")
    assert "/cashbook/routines-cleardown" in cashbook_block
    assert "Routines Cleardown" in cashbook_block


def test_gocardless_menu_has_setup_section(layout_src):
    gc_block = _extract_block(layout_src, "const gocardlessMenu")
    assert "heading: 'Setup'" in gc_block
    assert "/cashbook/gocardless-settings" in gc_block
    assert "label: 'Settings'" in gc_block


def test_gocardless_menu_setup_contains_cleardown(layout_src):
    gc_block = _extract_block(layout_src, "const gocardlessMenu")
    assert "/cashbook/gocardless-cleardown" in gc_block
    assert "label: 'Cleardown'" in gc_block


def test_suppliers_menu_has_setup_section(layout_src):
    sup_block = _extract_block(layout_src, "const suppliersMenu")
    assert "heading: 'Setup'" in sup_block
    assert "/supplier/settings" in sup_block
    assert "label: 'Settings'" in sup_block


def test_suppliers_menu_setup_contains_cleardown(layout_src):
    sup_block = _extract_block(layout_src, "const suppliersMenu")
    assert "/supplier/cleardown" in sup_block
    assert "label: 'Cleardown'" in sup_block


# ====================================================================
# Admin menu: per-app settings have moved out
# ====================================================================


def test_admin_module_setup_does_not_contain_bank_rec_settings(layout_src):
    """Bank Rec Settings moved to Cashbook menu — must not appear
    under Admin > Module Setup any more."""
    admin_block = _extract_block(layout_src, "const getAdminMenu")
    setup_section = _extract_section(admin_block, "Module Setup") or \
                    _extract_section(admin_block, "System Setup") or \
                    admin_block
    assert "label: 'Bank Rec Settings'" not in setup_section


def test_admin_module_setup_does_not_contain_gocardless_settings(layout_src):
    admin_block = _extract_block(layout_src, "const getAdminMenu")
    setup_section = _extract_section(admin_block, "Module Setup") or \
                    _extract_section(admin_block, "System Setup") or \
                    admin_block
    assert "label: 'GoCardless Settings'" not in setup_section


def test_admin_module_setup_does_not_contain_supplier_settings(layout_src):
    admin_block = _extract_block(layout_src, "const getAdminMenu")
    setup_section = _extract_section(admin_block, "Module Setup") or \
                    _extract_section(admin_block, "System Setup") or \
                    admin_block
    assert "label: 'Supplier Settings'" not in setup_section


# ====================================================================
# Admin menu: system-wide items stay
# ====================================================================


def test_admin_keeps_company_switching(layout_src):
    """Date & Company stays in Admin — it's deployment/system context."""
    admin_block = _extract_block(layout_src, "const getAdminMenu")
    assert "/admin/company" in admin_block
    assert "label: 'Date & Company'" in admin_block


def test_admin_keeps_installations(layout_src):
    admin_block = _extract_block(layout_src, "const getAdminMenu")
    assert "/admin/installations" in admin_block


def test_admin_keeps_users_and_licenses_for_admins(layout_src):
    """Users + Licenses are admin-only system-level concerns."""
    admin_block = _extract_block(layout_src, "const getAdminMenu")
    assert "/admin/users" in admin_block
    assert "/admin/licenses" in admin_block


def test_admin_keeps_application_settings(layout_src):
    """Application Settings = global cross-app config, stays in Admin."""
    admin_block = _extract_block(layout_src, "const getAdminMenu")
    assert "/settings" in admin_block
    assert "label: 'Application Settings'" in admin_block


def test_admin_keeps_lock_monitor(layout_src):
    """Lock Monitor = system-level diagnostic, stays in Admin."""
    admin_block = _extract_block(layout_src, "const getAdminMenu")
    assert "/admin/lock-monitor" in admin_block


# ====================================================================
# App.tsx route registration
# ====================================================================


def test_per_app_cleardown_routes_registered(app_src):
    """Each app's cleardown route maps to SystemReset with the
    matching appFilter prop."""
    assert 'path="/cashbook/routines-cleardown"' in app_src
    assert 'appFilter="bank_reconcile"' in app_src

    assert 'path="/cashbook/gocardless-cleardown"' in app_src
    assert 'appFilter="gocardless"' in app_src

    assert 'path="/supplier/cleardown"' in app_src
    assert 'appFilter="suppliers"' in app_src


def test_settings_routes_registered_at_app_paths(app_src):
    """The settings pages live at /cashbook/options, /cashbook/gocardless-
    settings, /supplier/settings — i.e. under the app's own URL prefix,
    not under /admin/*."""
    assert '/cashbook/options' in app_src
    assert '/cashbook/gocardless-settings' in app_src
    assert '/supplier/settings' in app_src


# ====================================================================
# Helpers
# ====================================================================


def _extract_block(src: str, marker: str) -> str:
    """Grab the curly-brace block following a `const X = ` declaration.
    Returns the substring from `marker` to the matching closing brace."""
    start = src.find(marker)
    if start < 0:
        raise AssertionError(f"Marker {marker!r} not found in source")
    # Find the opening brace of the object literal
    brace_start = src.find('{', start)
    if brace_start < 0:
        raise AssertionError(f"No opening brace after {marker!r}")
    depth = 0
    for i in range(brace_start, len(src)):
        c = src[i]
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                return src[start:i + 1]
    raise AssertionError(f"Unterminated block for {marker!r}")


def _extract_section(block: str, heading_label: str) -> str | None:
    """Within a menu block, find the section whose heading matches
    `heading_label` and return that section's source."""
    needle = f"heading: '{heading_label}'"
    idx = block.find(needle)
    if idx < 0:
        return None
    # Backtrack to the section's opening brace
    brace = block.rfind('{', 0, idx)
    if brace < 0:
        return None
    depth = 0
    for i in range(brace, len(block)):
        c = block[i]
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                return block[brace:i + 1]
    return None
