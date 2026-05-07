"""Shared types + helpers for per-app data-integrity health checks.

Each app exposes `GET /api/{app}/health-check` returning a
`HealthCheckResult` shaped exactly the same way. SAM (when ready)
fans out across all apps and aggregates the results into one
upgrade-readiness dashboard.

Why per-app
-----------
Only the app knows what its own data references and what could
break (e.g. bank-rec's bank_aliases.db references nbank.nk_acnt
codes; gocardless references sname.sn_account codes; etc.).
SAM can't have that knowledge — so the verification logic stays
inside each app and SAM just orchestrates.

When this is useful
-------------------
1. Post-upgrade verification (Opera 3 → Opera SE migration)
2. Periodic data-integrity audit
3. Diagnostic when something looks wrong (orphan codes, broken
   patterns)
4. Pre-flight before bulk operations (e.g. before re-running
   pattern learning)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


Severity = Literal['info', 'warning', 'error']


@dataclass
class HealthCheckItem:
    """One named check within an app's health report.

    Each check answers a specific question — e.g. "Do all bank
    codes in bank_aliases.db still exist in nbank?" — and reports
    its findings.

    Fields
    ------
    name              Short label (e.g. "Bank codes in aliases")
    description       Human-readable explanation of what was checked
    passed            True if everything is fine, False if orphans
                      or other issues were found
    total_checked     How many records were inspected
    orphan_count      How many were problematic
    orphans           Up to MAX_ORPHANS_RETURNED examples (for the
                      UI to render a drill-down). Each is a dict
                      describing the problematic record.
    severity          'info' / 'warning' / 'error' — controls
                      whether the overall report fails the check
    """
    name: str
    description: str
    passed: bool
    total_checked: int = 0
    orphan_count: int = 0
    orphans: list[dict[str, Any]] = field(default_factory=list)
    severity: Severity = 'warning'


@dataclass
class HealthCheckResult:
    """Top-level health report for one app.

    Fields
    ------
    app                Stable app identifier (e.g. 'bank_reconcile')
    healthy            True iff every error-severity check passed
                      AND no warning-severity check failed catastrophically
    summary            One-line human-readable summary
    checks             List of individual check results
    metadata           Optional extra context (timestamp, system_type,
                      total record counts, etc.)
    """
    app: str
    healthy: bool
    summary: str
    checks: list[HealthCheckItem] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_response_dict(self) -> dict[str, Any]:
        """Serialise to the standardised JSON response shape that
        every app's health-check endpoint returns. SAM aggregates
        across these dicts."""
        return {
            'app': self.app,
            'healthy': self.healthy,
            'summary': self.summary,
            'checks': [
                {
                    'name': c.name,
                    'description': c.description,
                    'passed': c.passed,
                    'total_checked': c.total_checked,
                    'orphan_count': c.orphan_count,
                    'orphans': c.orphans,
                    'severity': c.severity,
                }
                for c in self.checks
            ],
            'metadata': self.metadata,
        }


# Cap orphan-list size in the JSON response so a tenant with
# thousands of orphan codes doesn't OOM the browser.
MAX_ORPHANS_RETURNED = 50


def derive_overall_healthy(checks: list[HealthCheckItem]) -> bool:
    """An app is 'healthy' iff:
      - Every error-severity check passed, AND
      - At most a few warning-severity checks failed (we surface
        warnings but they don't fail the overall report)
    Info-severity checks never affect the overall result.
    """
    for check in checks:
        if check.severity == 'error' and not check.passed:
            return False
    return True


def summarise(app: str, checks: list[HealthCheckItem]) -> str:
    """Build a one-line human summary."""
    failed_errors = [c for c in checks if c.severity == 'error' and not c.passed]
    failed_warnings = [c for c in checks if c.severity == 'warning' and not c.passed]
    if failed_errors:
        return (f"{app}: {len(failed_errors)} error(s), "
                f"{len(failed_warnings)} warning(s)")
    if failed_warnings:
        return (f"{app}: passed with {len(failed_warnings)} warning(s)")
    return f"{app}: all {len(checks)} check(s) passed"
