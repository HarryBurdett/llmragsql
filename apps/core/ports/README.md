# Ports & Adapters

Phase B of the SAM-readiness work. Each app's external dependencies
go through **ports** (interface definitions in `apps/core/ports/`)
and pluggable **adapters** (implementations in `apps/core/adapters/`).

## Why

When apps move to SAM:
- The provider of each dependency may change (SAM provides email,
  auth, Opera SQL gateway)
- The transport may change (in-process today, HTTP tomorrow, gRPC
  someday)

Without ports, swapping any of those means touching every call site
in every app. With ports, swapping means writing one new adapter and
flipping a config flag.

## Layout

```
apps/core/
  ports/                    Interface definitions (protocols)
    opera_sql.py
    opera3_reader.py
    opera3_writer.py
    email_storage.py
    email_sync.py
    auth.py
    smtp.py
  adapters/
    factory.py              Returns the right adapter per port
    local/                  In-process implementations (today's
                            behaviour; default in Phase A + B)
      opera_sql.py
      ...
    http/                   HTTP-client implementations (used when
                            apps run in separate containers)
      opera_sql.py
      ...
```

## How apps use ports

```python
# Old (cross-app import, runtime-coupled):
from api.main import sql_connector

result = sql_connector.execute_query("SELECT ...")

# New (port-based, runtime-decoupled):
from apps.core.adapters.factory import get_opera_sql

opera_sql = get_opera_sql()
result = opera_sql.execute_query("SELECT ...")
```

The factory reads env vars (`OPERA_SE_GATEWAY_URL`, `SAM_ENABLED`,
etc.) to decide which adapter to construct.

## Adapter selection

Default = local (in-process). Adapters are swapped via env var:

| Env var | Behaviour |
|---|---|
| (unset) or `false` | Use the local in-process adapter |
| `CORE_OPERA_SE_URL=http://...` | Use HTTP adapter pointing at that URL |
| `SAM_ENABLED=true` + `SAM_*_URL` | Use SAM-aware adapter |

## Migration strategy

Phase B is **additive**: ports + local adapters are added alongside
the existing direct imports. Existing call sites keep working. New
code uses ports. Migration of existing call sites happens
incrementally.

The local adapters are thin wrappers around today's in-process
state — they re-export the same `sql_connector`, `email_storage`,
etc., but through a stable interface.

## SAM migration

When SAM specifics are known, write SAM-specific adapters:

```python
# apps/core/adapters/sam/opera_sql.py

class SAMOperaSQLAdapter:
    """Calls SAM's Opera SQL gateway service."""
    def execute_query(self, sql, params=None):
        # HTTP call to SAM-provided URL
        ...
```

Wire it up in `apps/core/adapters/factory.py`:

```python
def get_opera_sql() -> OperaSQLPort:
    if env_bool('SAM_ENABLED'):
        return SAMOperaSQLAdapter(env_required('SAM_OPERA_SQL_URL'))
    if env_str('CORE_OPERA_SE_URL'):
        return HTTPOperaSQLAdapter(env_required('CORE_OPERA_SE_URL'))
    return LocalOperaSQLAdapter()  # Default
```

No app code changes — only the factory.
