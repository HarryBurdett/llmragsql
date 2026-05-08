# SAM Rewrite — TypeScript Plugins

Live, ongoing rewrite of the four Python apps into native TypeScript
SAM plugins. Replaces the SAM-merge-via-Docker-containers plan that
was based on a wrong assumption about how SAM accommodates apps.

## Why we're rewriting

SAM (`https://github.com/jonathangintsys/aisam.git` — cloned locally
at `~/opera-knowledge-ref/`) is a **TypeScript / Node.js plugin host**.
Plugins are loaded **in-process** via `import()` and must export a
factory function returning an Express Router. There is no support
for Python sidecars or external HTTP services.

For a finance product targeting **10s of production deployments**,
running natively as a SAM plugin is the bar. Sidecar workarounds
would mean two languages, two deploy units, and a non-standard
integration that the SAM team would have to support indefinitely.

## Scope

Four backend apps + frontend get rewritten:

| App | Rewrite priority | Notes |
|---|---|---|
| `balance-check` | First (smallest, read-only) | Establishes the SAM plugin pattern |
| `gocardless` | Second | Posting + VAT, well-defined workflow |
| `bank-reconcile` | Third | Largest workflow, but well-tested |
| `suppliers` | Fourth | Incomplete in Python — finish in TypeScript |

Frontend (React) repackages as SAM `frontend-only` plugins — minimal
changes to the React code; mostly build-tooling changes.

## What's NOT being rewritten

| Component | Why |
|---|---|
| Opera knowledge base | Already at `https://github.com/jonathangintsys/aisam.git` (`~/opera-knowledge-ref/packages/opera-knowledge/`) — central, language-agnostic |
| Opera business data | Lives in customer's Opera SQL Server, never duplicated |
| Opera 3 access | SAM hosts the Opera 3 Agent over HTTP; we just call it |
| Email plumbing | SAM provides `ctx.emailIngest` and `ctx.email.send()` — we don't reimplement IMAP/Graph |
| User authentication | SAM's middleware populates `req.user` before plugin code runs |
| Secrets management | SAM injects per-app config via the `AppContext` factory parameter |

About 30-40% of our current Python code is glue for IMAP/Graph/auth/
multi-tenancy. **That code isn't being translated — it's being
deleted**, replaced by SAM-provided primitives. The actual business
logic translation is smaller than line-count suggests.

## Reference architecture

Each rewritten app follows this shape:

```
apps-sam/<app-name>/
├── manifest.json          ← SAM plugin manifest (matches plugin-authoring §8)
├── package.json           ← npm package, ESM, scripts
├── tsconfig.json          ← TypeScript config; emits ESM to dist/
├── src/
│   ├── index.ts           ← Default export = AppBackendFactory
│   ├── router.ts          ← Express router with all endpoints
│   ├── services/          ← Business logic ports (one file per concern)
│   ├── queries/           ← Opera SQL queries (Knex builders)
│   ├── types.ts           ← TypeScript types specific to this app
│   └── posting/           ← Opera posting helpers (apps that write)
├── tests/                 ← Vitest tests (mirrors src/ structure)
├── db/migrations/         ← (if separateDatabase=true) Knex migrations
└── README.md              ← Per-app overview
```

Plus a shared package:

```
apps-sam/shared/
├── package.json
├── tsconfig.json
└── src/
    ├── opera/             ← Opera SQL helpers (control-account lookups,
    │                        period status, etc. — used by every app)
    ├── posting/           ← Common posting primitives (id allocation,
    │                        VAT tracking, anoml/snoml/pnoml writers)
    ├── types.ts           ← Cross-app types
    └── index.ts
```

## SAM plugin contract — what each plugin must do

From `~/opera-knowledge-ref/docs/plugin-authoring.md` (1.0, April 2026):

1. **Factory function** is the default export. Receives `AppContext`
   (`appId`, `tenantId`, `config`, `operaType`, `db`, `eventBus`,
   `logger`). Returns an Express `Router`. SAM mounts it under
   `/api/apps/<appId>/*`.

2. **Auth is pre-validated.** SAM's middleware populates `req.user`
   from the JWT (HS256, claims include `userId`, `email`, `role`,
   `tenantId`, `userType`, `permissions`, `appId`, `appRole`) before
   our router runs. We don't validate tokens — we read `req.user`.

3. **Tenant identification** comes from `ctx.tenantId` and (per request)
   `req.user.tenantId`. Per-request company comes from the
   `X-Opera-Company` header and is attached as `req.operaCompany` by
   SAM's `resolveCompany` middleware.

4. **Database access:**
   - `ctx.db.sam` — SAM's database (treat as read-only by convention)
   - `ctx.db.operaSystem` — Opera3SESystem connection (Knex pool)
   - `ctx.db.getCompanyDb(code)` — per-company Opera database (Knex pool)
   - If `manifest.backend.separateDatabase: true`, SAM provisions a
     dedicated MSSQL database and exposes it via env vars
     (`APP_DB_*`); we open it ourselves with Knex.

5. **Email:**
   - `ctx.emailIngest.listMyMailboxes()`
   - `ctx.emailIngest.registerHandler(mailboxId, fn)`
   - `ctx.emailIngest.fetchAttachment(msg, attachmentId)`
   - `ctx.emailIngest.getAttachmentText(msg, attachmentId, opts?)`
   - `ctx.email.send({...})`

6. **Standalone-mode gating.** Anything that's only relevant to local
   dev (`express.static`, terminal 404s, Bearer-only auth, etc.) is
   gated by `process.env.SAM_PLUGIN_MODE !== 'true'`.

## Rewrite progress

Tracked in [`progress.md`](./progress.md).

## Reference implementations

The Python apps stay alive at `apps/` during the rewrite. They are
the **specification** — every TypeScript port is validated by:

1. Reading the corresponding Python code as the spec
2. Porting the test (Pytest → Vitest) and confirming it passes
3. Running the Python and TypeScript versions side by side against
   the same Opera test data and diffing outputs

Once full parity is confirmed for an app, the Python version can be
retired.

## Estimated timeline

| Phase | Calendar weeks |
|---|---|
| Shared TypeScript foundation | 2 |
| `balance-check` rewrite + parity validation | 2 |
| `gocardless` rewrite + parity validation | 3 |
| `bank-reconcile` rewrite + parity validation | 4 |
| `suppliers` finished in TypeScript | 3 |
| Frontend plugins (4 apps) | 1-2 |
| Integration tests against SAM tenant | 2 |
| Buffer | 2-3 |
| **Total** | **~3-4 months** |

## Next steps in this session

1. ✅ Set up `apps-sam/` structure
2. ✅ Document the rewrite plan
3. Build `balance-check` foundation (manifest, package.json, tsconfig, factory)
4. Port the first endpoint (`/api/reconcile/summary` — read-only, simplest)
5. Port the matching test
6. Verify it builds and tests pass
7. Commit + push
