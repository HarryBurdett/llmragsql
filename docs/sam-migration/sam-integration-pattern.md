# SAM Integration Pattern

How the apps plug into SAM's parameter store, auth, and tenant
configuration once SAM is implemented. Read this alongside
[`env-var-contract.md`](./env-var-contract.md) and
[`phase-b-status.md`](./phase-b-status.md).

## TL;DR

The Phase A + B work was designed so SAM integration is **adapter
implementations + factory wiring** — never app code changes. When SAM
is ready, you write one Python file per port that talks to SAM's
APIs, then flip `SAM_ENABLED=true`. Every `get_opera_sql().execute_query(...)`
call site continues to work; under the hood, the adapter now resolves
the tenant's Opera SQL credentials from SAM instead of from local env vars.

## What SAM provides

When SAM is the source of truth, it provides:

| Category | Examples | Per-tenant? |
|---|---|---|
| **User identity + roles** | username, password (or SSO), role assignments | Yes |
| **Tenant configuration** | Opera version (SE/3), enabled apps, locale | Yes |
| **Database credentials** | Opera SQL host/db/user/password | Yes |
| **Opera 3 Agent URL** | SAM's expanded Opera 3 Agent (single endpoint for reads + writes — replaces the legacy DBF-share + Windows-write-agent pair) | Yes |
| **Email credentials** | IMAP server/port/user/password, SMTP equivalent | Yes |
| **AI credentials** | Gemini API key (or alternative provider) | Could be shared or per-tenant |
| **Payment integration** | GoCardless access token, webhook secret | Yes (per-tenant) |
| **Service URLs** | core-email service, SAM Opera 3 Agent (per-tenant) | Often shared infrastructure |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          SAM Platform                               │
│  ┌──────────┐  ┌────────────┐  ┌────────────┐  ┌──────────────┐    │
│  │  Auth    │  │  Tenants/  │  │ Config /   │  │  Service     │    │
│  │ (JWT)    │  │  Users     │  │ Secrets    │  │  Registry    │    │
│  └────┬─────┘  └─────┬──────┘  └─────┬──────┘  └──────┬───────┘    │
└───────┼──────────────┼───────────────┼────────────────┼────────────┘
        │              │               │                │
        │ JWT          │ tenant_id     │ HTTP GET       │ resolve URL
        ▼              ▼               ▼                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Our apps (bank_reconcile / gocardless / suppliers / ...)          │
│                                                                     │
│  SAM-aware adapters (Phase C) ← selected by factory when           │
│      SAM_ENABLED=true            SAM_ENABLED=true                  │
│        │                                                            │
│        ▼                                                            │
│  apps/core/adapters/factory.py                                     │
│        ▲                                                            │
│        │ (same call sites as today; app code unchanged)            │
│  Application business logic                                         │
└─────────────────────────────────────────────────────────────────────┘
```

## How each parameter is sourced

| Parameter | Where in SAM | How our app reads it |
|---|---|---|
| **User identity** | SAM Auth — JWT issued at login | `SAMAuthAdapter.validate(token)` |
| **Active tenant** | JWT claim `tenant_id` | `SAMCompanyContextAdapter.get_company_id()` |
| **System type** (SE/3) | SAM tenant config: `tenant.opera_version` | `SAMConfigClient.get('opera.version', tenant_id)` |
| **Opera SQL creds** | SAM secrets: `tenant/{id}/database/*` | `SAMOperaSQLAdapter` per-tenant cache |
| **Opera 3 path + Agent URL** | SAM tenant config: `tenant.opera3.*` | `SAMOpera3ReaderAdapter` per-tenant lookup |
| **Email IMAP/SMTP** | SAM secrets: `tenant/{id}/email/*` | `SAMEmailStorageAdapter` per-tenant lookup |
| **Gemini API key** | SAM secrets (deployment-wide or per-tenant) | `SAMConfigClient.get('gemini.api_key')` |
| **GoCardless token** | SAM secrets: `tenant/{id}/gocardless/*` | `SAMGoCardlessAdapter` per-tenant |
| **GoCardless environment** | **Deployment-level** (sandbox vs live) | Env var — NEVER per-tenant (real-money risk) |

## Request lifecycle

```
1. Browser → SAM:  POST /sam/auth/login {user, password}
2. SAM → Browser:  JWT (signed, contains tenant_id, system_type, roles)
3. Browser → app:  GET /api/bank-import/scan-emails
                   Header: Authorization: Bearer <JWT>

4. App middleware:
   - SAMAuthAdapter.validate(JWT) → {tenant_id, system_type, roles}
   - Set CompanyContextPort for this request

5. Route handler runs UNCHANGED:
   - get_opera_sql().execute_query(...)
        └→ SAMOperaSQLAdapter reads tenant_id from CompanyContextPort
           └→ Looks up tenant creds (cached)
              └→ Returns the right connector
   - get_email_storage().get_emails(...)
        └→ SAMEmailStorageAdapter calls SAM's email service for tenant
6. Response → Browser
```

App code is **identical** to today. Adapters change.

## The 3 migration steps

### Step 1: SAM config client (one-time)

```python
# apps/core/adapters/sam/config_client.py

import httpx
from typing import Any


class SAMConfigClient:
    """Cached HTTP client for SAM's config/secrets service."""

    def __init__(self, base_url: str, auth_token: str):
        self.base_url = base_url
        self.auth_token = auth_token
        self._cache: dict[tuple[str, str], dict] = {}

    def get_section(self, section: str, tenant_id: str) -> dict[str, Any]:
        """Fetch a config section for a tenant.
        e.g. get_section('database', 't_123') → {server, port, ...}
        """
        key = (tenant_id, section)
        if key not in self._cache:
            r = httpx.get(
                f"{self.base_url}/api/v1/tenants/{tenant_id}/config/{section}",
                headers={"Authorization": f"Bearer {self.auth_token}"},
                timeout=10,
            )
            r.raise_for_status()
            self._cache[key] = r.json()
        return self._cache[key]

    def invalidate(self, tenant_id: str | None = None):
        """Drop cached values. Pass tenant_id to invalidate one
        tenant; pass None to clear everything."""
        if tenant_id is None:
            self._cache = {}
        else:
            self._cache = {k: v for k, v in self._cache.items() if k[0] != tenant_id}
```

### Step 2: SAM-aware adapter per port

```python
# apps/core/adapters/sam/opera_sql.py

from sql_rag.sql_connector import SQLConnector


class SAMOperaSQLAdapter:
    """Per-tenant Opera SQL connector pool sourced from SAM."""

    def __init__(self, sam_client):
        self._sam = sam_client
        self._connectors: dict[str, SQLConnector] = {}

    def __bool__(self) -> bool:
        return True   # always available; per-tenant resolution at call time

    def execute_query(self, sql, params=None):
        from apps.core.adapters.factory import get_company_context
        tenant_id = get_company_context().get_company_id()
        if not tenant_id:
            raise RuntimeError("No active tenant — cannot resolve Opera SQL")
        return self._connector_for(tenant_id).execute_query(sql, params)

    def execute_non_query(self, sql, params=None):
        from apps.core.adapters.factory import get_company_context
        tenant_id = get_company_context().get_company_id()
        return self._connector_for(tenant_id).execute_query(sql, params)

    def _connector_for(self, tenant_id: str) -> SQLConnector:
        if tenant_id not in self._connectors:
            cfg = self._sam.get_section('database', tenant_id)
            self._connectors[tenant_id] = SQLConnector(
                # SAM provides the credentials; our SQLConnector handles
                # pooling, NOLOCK / ROWLOCK hints, etc. — unchanged.
                server=cfg['server'],
                database=cfg['database'],
                username=cfg['username'],
                password=cfg['password'],
                pool_size=cfg.get('pool_size', 5),
                # ... rest of the params
            )
        return self._connectors[tenant_id]
```

### Step 3: Factory wiring

```python
# apps/core/adapters/factory.py — modify get_opera_sql()

def get_opera_sql() -> "OperaSQLPort":
    if env_bool('SAM_ENABLED'):
        from apps.core.adapters.sam.config_client import SAMConfigClient
        from apps.core.adapters.sam.opera_sql import SAMOperaSQLAdapter
        client = _get_sam_client()   # singleton, see below
        return SAMOperaSQLAdapter(client)
    if env_str('CORE_OPERA_SE_URL'):
        # Phase B HTTP adapter
        ...
    from apps.core.adapters.local.opera_sql import LocalOperaSQLAdapter
    return LocalOperaSQLAdapter()


@lru_cache(maxsize=1)
def _get_sam_client() -> "SAMConfigClient":
    from apps.core.adapters.sam.config_client import SAMConfigClient
    return SAMConfigClient(
        base_url=env_required('SAM_CONFIG_URL'),
        auth_token=env_required('SAM_SERVICE_TOKEN'),
    )
```

That's it. No app code changes. Every `get_opera_sql().execute_query(...)`
call site keeps working — the adapter underneath now resolves the
right connector for the active tenant via SAM.

## Auth integration (special case)

Auth is the one port where the **request middleware** changes too,
not just the adapter — because middleware extracts the user from
the request.

```python
# api/main.py middleware — modified to validate SAM JWTs

@app.middleware("http")
async def auth_middleware(request, call_next):
    if env_bool('SAM_ENABLED'):
        # SAM mode: validate inbound JWT
        token = request.headers.get('authorization', '').removeprefix('Bearer ')
        from apps.core.adapters.factory import get_auth
        user = get_auth().validate(token)
        if user:
            # Set CompanyContextPort backing values from JWT claims
            from apps.core import state
            state._request_company_id.set(user['tenant_id'])
            state.current_company = user.get('company') or {'id': user['tenant_id']}
            state.active_system_id = user.get('system_id')
    else:
        # Legacy session-cookie mode (today)
        ...
    return await call_next(request)
```

The middleware is the only place that needs a code change to
support SAM JWTs. Adapters and route handlers don't change.

## Per-tenant credential caching

The `SAMOperaSQLAdapter._connectors` dict above caches one
`SQLConnector` per tenant indefinitely. This is fine for small N
(<100 tenants); for large multi-tenant SaaS you'd want:

- TTL eviction (drop after N minutes)
- LRU eviction (cap to N tenants in memory)
- Webhook from SAM to invalidate when credentials rotate

Add these in the SAM adapter — they don't affect app code.

## What stays SAM-agnostic

These don't move to SAM and are NOT per-tenant:

- **Deployment-level config**: `LOG_LEVEL`, `OPERA_VERSION` (per-deployment override only),
  `GOCARDLESS_ENVIRONMENT` (sandbox vs live — SAFETY-CRITICAL: never per-tenant)
- **App boundaries**: which routers register (`INSTALLED_APPS` env var)
- **Container metadata**: `APP_NAME`, port

These remain plain env vars set by the deployment manifest, regardless
of SAM.

## Questions for the SAM team (lock these in before Phase C)

These shape the SAM adapter implementations. None require app
code changes — only the adapter files.

1. **Auth shape**: JWT? OAuth2? Session cookie? What's in the
   token claims (tenant_id? roles? system_type?)
2. **Config API shape**: REST? gRPC? URL pattern for "give me
   tenant X's email config"? Is it cacheable? TTL?
3. **Secret rotation**: Does SAM push notifications on secret
   change, or do we re-fetch on every request, or use a TTL?
4. **Service-to-service auth**: How do our containers authenticate
   to SAM's config API? mTLS? Service token? Workload identity?
5. **Tenant routing**: Does SAM put `tenant_id` in the URL path?
   In a header? In the JWT only? (Affects middleware.)
6. **Operational mode**: One app instance serves all tenants
   (multi-tenant)? Or one instance per tenant (single-tenant
   deployment)?
7. **Opera 3 Agent** (now SAM-hosted, expanded): How does SAM
   tell apps which Opera 3 Agent endpoint to use per tenant?
   (Single env var `OPERA3_AGENT_URL`, populated per-tenant by SAM's
   secret store, is the expected shape.) The agent now handles both
   reads and writes, so no SMB / DBF-share configuration is needed
   on our containers.

## Migration checklist for SAM-day

- [ ] Implement `apps/core/adapters/sam/config_client.py`
- [ ] Implement `apps/core/adapters/sam/auth.py` (JWT validation)
- [ ] Implement `apps/core/adapters/sam/company_context.py`
      (read tenant from JWT)
- [ ] Implement `apps/core/adapters/sam/opera_sql.py`
- [ ] Implement `apps/core/adapters/sam/email_storage.py`
      (or skip if SAM provides its own equivalent)
- [ ] Implement `apps/core/adapters/sam/opera3_reader.py`
- [ ] Update auth middleware in `api/main.py` to handle JWTs when
      `SAM_ENABLED=true`
- [ ] Wire all adapters into `apps/core/adapters/factory.py`
- [ ] Set SAM env vars in deployment:
      `SAM_ENABLED=true`, `SAM_CONFIG_URL=...`,
      `SAM_AUTH_URL=...`, `SAM_SERVICE_TOKEN=...`,
      `AUTH_JWT_PUBLIC_KEY=...`
- [ ] Smoke-test each app's `/healthz` from inside SAM
- [ ] Run one full workflow per app (bank scan, gocardless import,
      supplier reconcile, balance check)

This is one focused engineering effort: ~6 adapter files + 1
middleware update + factory wiring + SAM-side env vars. **Zero
app code changes**.

## Related documentation

- [`env-var-contract.md`](./env-var-contract.md) — every env var the apps consume
- [`phase-b-status.md`](./phase-b-status.md) — current ports/adapters status
- [`migration-checklist.md`](./migration-checklist.md) — per-app SAM migration checklist
- [`apps/`](./apps/) — per-app dependency details
