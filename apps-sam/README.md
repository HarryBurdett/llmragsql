# SAM plugins

TypeScript port of the four Python apps in `apps/`. Each plugin
exposes the same HTTP endpoints as its Python counterpart and ships
a React frontend lifted from `frontend/src/pages/`.

| Plugin | Description | Backend tests | Frontend LOC |
| --- | --- | --- | --- |
| [bank-reconcile](bank-reconcile/) | Bank statement reconciliation | 434 | 5,400 (BankStatementReconcile.tsx) |
| [gocardless](gocardless/) | GoCardless Direct Debit import | 503 | 2,500 (GoCardlessImport.tsx) |
| [suppliers](suppliers/) | Supplier statement reconciliation | 212 | 3,000 (4 supplier pages) |
| [balance-check](balance-check/) | Internal Opera control reconciliation | 32 | 2,400 (4 balance pages) |
| [shared](shared/) | Opera helpers + posting primitives used by all four | (n/a) | (n/a) |

**Total**: 1,181 backend tests passing, ~13,300 lines of frontend
code ported.

Read each plugin's `README.md` for ctx contract, defaults, and
config keys.

## Deploying and maintaining SAM plugins

Two reference documents cover everything you need:

- **[DEPLOY-TO-SAM.md](DEPLOY-TO-SAM.md)** — first-time deployment.
  Extract plugins from this monorepo, push to GitHub, register in SAM
  Central, install on the SAM host, configure each plugin, migrate
  data from the legacy Python apps.
- **[MAINTAIN-SAM-PLUGINS.md](MAINTAIN-SAM-PLUGINS.md)** — everything
  after deployment. Ship fixes, ship features, roll back releases,
  debug production issues, monitor health, add new endpoints or new
  plugins.

The earlier handoff docs (`EMBEDDING.md`, `OPERATOR-SETUP.md`,
`MIGRATION.md`) have been retired and replaced by these two.

## Workspace commands

```sh
npm install                    # installs all workspaces
npm test                       # runs every plugin's vitest suite
npm run lint                   # tsc --noEmit across all plugins
npm run build                  # tsc + vite for every plugin
```

Per-plugin commands work too:

```sh
cd bank-reconcile
npm test
npm run lint
cd frontend && npm run build   # produces UMD bundle in dist/
```

## Conventions

**Endpoint parity**. Every Python URL has a 1:1 SAM equivalent at
the same path. Both Opera SE (`/api/...`) and Opera 3
(`/api/opera3/...`) prefixes resolve to the same handler — the
opera-3 mirror is a one-line path-rewrite middleware in each
router.

**Adapter pattern**. Plugin routers cast `ctx as unknown as
{ adapterName?: T }` to look up SAM-injected adapters. When an
adapter is missing, plugins fall back to a built-in default where
one exists (filesystem, LLM, email-ingest) or return 503 with a
clear message identifying which key on ctx must be wired.

**Faithful ports**. Service files comment with line-number
references back to the Python source — when behaviour drifts, the
diff is auditable. Tests use chained-Knex-builder mocks so they
run without a database; the migration smoke test
(`tests/migrations.test.ts` in each plugin) is the one place real
SQLite is used.

## Frontend infrastructure

Each plugin frontend ships with:

- React 18 (provided by SAM as external)
- `@tanstack/react-query` (per-plugin QueryClient)
- `lucide-react` icons
- Tailwind 3 scoped via `important: '.<plugin>-app'` so utilities
  don't leak into the host CSS
- `api-shim.ts` — adapts SAM's `context.api.fetch` onto the
  axios-style `apiClient` legacy pages expect, including
  `X-Opera-Company` header injection
- `router-shim.tsx` (suppliers, balance-check) — minimal
  `useNavigate` / `useSearchParams` / `Link` since SAM hosts
  routing
