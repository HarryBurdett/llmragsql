"""Verify the api_debug.log file handler is attached to ALL the loggers we
care about for production diagnosis.

The previous setup only attached the handler to two loggers:
    - sql_rag.opera_sql_import
    - api.auth_middleware

That meant warnings such as

    "import-from-pdf: email_storage is None - bank_statement_imports will NOT
     be created..."

(emitted by ``apps.bank_reconcile.api.routes``) and

    "BANK_IMPORT_DEBUG: Importing SALES_RECEIPT..."

(emitted by ``sql_rag.bank_import``) were silently dropped from the log
file, making it impossible to debug missing-row bugs after the fact.

This is unacceptable for a finance system: every import-related warning
must land in the durable log file. These tests pin that contract.
"""

import logging
from pathlib import Path

API_MAIN_FILE = Path(__file__).parent.parent / "api" / "main.py"

# Logger names that emit information critical for debugging bank-rec
# correctness. If any of these is dropped from api_debug.log we lose the
# ability to investigate finance bugs after the fact.
REQUIRED_LOGGERS = (
    "apps.bank_reconcile.api.routes",
    "apps.bank_reconcile",
    "sql_rag.bank_import",
    "sql_rag.bank_import_opera3",
    "sql_rag.opera_sql_import",
    "sql_rag.opera3_foxpro_import",
    "api.email.storage",  # emits "Recorded bank statement import:..." log — must be captured
    "api.auth_middleware",
    "apps.gocardless.api.routes",
    "apps.suppliers.api.routes",
)


def test_api_main_attaches_file_handler_to_required_loggers():
    """api/main.py must register every required logger with a file handler
    pointing at api_debug.log.

    Imports api.main (which has heavy import-time side effects but is the
    only way to verify what handlers are actually wired up at runtime).
    """
    import api.main  # noqa: F401  - import for side effects

    missing = []
    for name in REQUIRED_LOGGERS:
        lg = logging.getLogger(name)
        # Walk up the logger chain to also count handlers attached to
        # ancestor loggers when propagation is on (the default).
        attached_paths = []
        cur = lg
        while cur is not None:
            for h in cur.handlers:
                if isinstance(h, logging.FileHandler):
                    attached_paths.append(h.baseFilename)
            cur = cur.parent if cur.propagate else None

        if not any(p.endswith("api_debug.log") for p in attached_paths):
            missing.append((name, attached_paths))

    assert not missing, (
        "api/main.py is missing the api_debug.log file handler for these "
        f"loggers: {missing}. Without it, their warnings (including critical "
        "finance-correctness warnings) are silently dropped."
    )


def test_required_loggers_propagate_at_info_level():
    """Each required logger must allow INFO-level records through.

    A common foot-gun is to attach the handler but leave the child
    logger's effective level at WARNING (inherited from the root). This
    test asserts that, with our config, an INFO record made by each
    target logger is not blocked by the logger's own level.
    """
    # Import api.main - this triggers the logging setup. Acceptable in
    # tests; the module is import-safe (its FastAPI app object is built
    # at import time but starts no servers).
    import api.main  # noqa: F401  - import for side effects

    for name in REQUIRED_LOGGERS:
        lg = logging.getLogger(name)
        eff = lg.getEffectiveLevel()
        assert eff <= logging.INFO, (
            f"Logger {name!r} has effective level {logging.getLevelName(eff)} "
            f"which blocks INFO records. api_debug.log will miss diagnostics."
        )


def test_file_handler_path_matches_documented_location():
    """The file handler writes to the path that humans/scripts grep.

    If someone moves the log file, every triage runbook breaks silently.
    Pin the location.
    """
    src = API_MAIN_FILE.read_text(encoding="utf-8")
    assert "api_debug.log" in src, (
        "api/main.py must write the file handler to api_debug.log so "
        "operations runbooks and `grep api_debug.log` keep working."
    )
