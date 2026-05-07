# Phase B Status — Ports & Adapters

Phase B of the SAM-readiness work: per-app runtime independence
via ports/adapters. Apps no longer import shared singletons
directly from `api.main`; they obtain implementations through a
factory that can swap them per-environment.

## What's done

### Foundation (Phase B.1)

- **Ports** (interfaces) defined in `apps/core/ports/`:
  - `OperaSQLPort` — Opera SQL Server queries
  - `EmailStoragePort` — email metadata + attachments store
  - `Opera3ReaderPort` — FoxPro DBF read access
  - `Opera3WriterPort` — Windows Write Agent HTTP client
  - `EmailSyncPort` — IMAP poller control
  - `SMTPPort` — outbound email
  - `AuthPort` — request authentication

- **Local adapters** wrap the existing in-process state in
  `apps/core/adapters/local/`. Behaviour is identical to today.

- **Adapter factory** (`apps/core/adapters/factory.py`) selects
  the right adapter based on env vars. Default = local. Future
  HTTP / SAM adapters plug in here without touching call sites.

- **24 unit tests** pin the port satisfaction, factory
  selection, and adapter contracts.

### Migrations (Phase B.2)

- **`sql_connector` migrated** at 91 call sites across 11 apps:
  - `bank_reconcile`, `gocardless`, `suppliers` (multiple files),
    `balance_check`, `dashboards`, `pension_export`, `sop`,
    `transaction_snapshot`
  - Pattern: `from api.main import sql_connector` →
    `from apps.core.adapters.factory import get_opera_sql; sql_connector = get_opera_sql()`

- **`email_storage` migrated** at 5 call sites in `suppliers`:
  - Pattern: `from api.main import email_storage` →
    `from apps.core.adapters.factory import get_email_storage; email_storage = get_email_storage()`

- **`email_sync_manager` migrated** at 3 call sites in `suppliers`.

- **Truthiness preserved**: adapters implement `__bool__()` so
  the legacy `if not sql_connector:` / `if not email_storage:`
  fail-fast pattern continues to work. The adapter is truthy
  iff the underlying singleton is resolvable.

## What's intentionally unmigrated

These imports remain on `from api.main import ...`:

- `_get_opera3_provider` (8 sites) — Opera 3 provider lookup
  helper. Could become a port helper; for now reading via
  api.main is fine.
- `config` (10 sites) — already env-var-aware via Phase A.
  Apps just need the ConfigParser object; migrating to a
  port-based getter is a polish improvement, not a structural one.
- `current_company`, `active_system_id`, `_request_company_id`
  — per-request context. These conceptually belong in a
  `CompanyContextPort` but the migration is mechanical when SAM
  specifics are known (does SAM provide tenant context, or do
  we keep our own?).
- `_company_sql_connectors` — connector registry (2 sites in
  transaction_snapshot for company switching). Internals.
- `COMPANIES_DIR` — path constant. Trivial.

## How to swap adapters at runtime

Today:
```bash
# All defaults — uses LocalOperaSQLAdapter, LocalEmailStorageAdapter, etc.
docker compose up
```

When apps are split into containers (Phase B-final):
```bash
# bank_reconcile uses HTTP adapters pointing at core-email + core-opera-se
CORE_EMAIL_URL=http://core-email:8000 \
CORE_OPERA_SE_URL=http://core-opera-se:8000 \
docker compose up
```

When SAM is wired up (Phase C):
```bash
# SAM-aware adapters validate tokens against AUTH_JWT_PUBLIC_KEY,
# look up services via SAM_SERVICE_REGISTRY_URL, etc.
SAM_ENABLED=true \
SAM_AUTH_URL=https://sam.example.com/auth \
SAM_SERVICE_REGISTRY_URL=https://sam.example.com/registry \
AUTH_JWT_PUBLIC_KEY=<...> \
docker compose up   # (or whatever SAM uses to launch)
```

The HTTP and SAM adapters don't exist yet — they're the next
chunk of work. The factory has TODO log lines that mention them.

## Adding a new adapter

Example: SAM email service.

1. Write the adapter:

```python
# apps/core/adapters/sam/email_storage.py

class SAMEmailStorageAdapter:
    def __init__(self, base_url: str, jwt_token: str):
        self._base_url = base_url
        self._jwt = jwt_token

    def get_emails(self, **kwargs):
        return requests.get(
            f"{self._base_url}/emails",
            params=kwargs,
            headers={"Authorization": f"Bearer {self._jwt}"},
        ).json()

    # ... implement EmailStoragePort interface
```

2. Wire it up in `apps/core/adapters/factory.py`:

```python
def get_email_storage() -> EmailStoragePort:
    if env_bool('SAM_ENABLED') and env_str('SAM_EMAIL_URL'):
        from apps.core.adapters.sam.email_storage import SAMEmailStorageAdapter
        return SAMEmailStorageAdapter(
            base_url=env_required('SAM_EMAIL_URL'),
            jwt_token=env_required('SAM_AUTH_TOKEN'),
        )
    if env_str('CORE_EMAIL_URL'):
        from apps.core.adapters.http.email_storage import HTTPEmailStorageAdapter
        return HTTPEmailStorageAdapter(env_required('CORE_EMAIL_URL'))
    from apps.core.adapters.local.email_storage import LocalEmailStorageAdapter
    return LocalEmailStorageAdapter()
```

3. App code is unchanged. Tests pass. SAM merge done for that port.

## Test coverage

| Suite | Count | Note |
|---|---|---|
| `test_ports_and_adapters.py` | 27 | Port satisfaction, factory, adapters, truthiness |
| Existing F1-F9 audit suite | 505 | Continues passing through ports |
| **Total** | **532** | (was 460 at session start) |

## Phase B.3 (next, not yet started)

- **Per-app database ownership**: today `bank_aliases.db` lives
  at the repo root and is read by both `bank_reconcile` and
  `gocardless` directly. Phase B.3 moves it under
  `apps/bank_reconcile/` (logical owner) and adds a
  `BankAliasesPort` so `gocardless` can read via the port.
- **HTTP adapter implementations**: needed when apps actually
  run in separate containers (today's `monolith` target keeps
  them in one process; the port machinery is in place but the
  HTTP adapters aren't necessary yet).
- **Per-app OpenAPI specs**: lock the cross-app HTTP contracts
  before Phase C / SAM merge. Each app exposes its OpenAPI at
  `/openapi.json` already; the work is reviewing + freezing
  the contract surface.

## Phase C (SAM merge — when SAM specifics are known)

- Write `apps/core/adapters/sam/*.py` adapters for each port
  SAM provides (probably auth, possibly email, possibly Opera
  SQL gateway).
- Wire them into the factory.
- Rename env vars if SAM uses different conventions (alias in
  `apps/core/env_config.py`).
- Update `docs/sam-migration/migration-checklist.md` with
  SAM-specific cutover steps.

This is **one adapter file per port** — small, isolated, no
app code changes.
