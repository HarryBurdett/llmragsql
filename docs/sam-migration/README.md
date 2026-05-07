# SAM Platform Migration — Application Manifest

This directory documents how the SQL RAG applications expect to be
hosted, what they consume, and what they provide. It exists so the
SAM platform integration team can plug the apps into SAM without
having to read application source code.

## Status

**Phase A (in progress)**: applications run as one process today;
all configuration is now externalised via env vars. Containerisable
on a single host.

**Phase B (next)**: each application becomes its own container with
its own database; they communicate via HTTP.

**Phase C (SAM merge)**: SAM provides connections, secrets, and
shared services. The apps are pointed at SAM-provided URLs/values
via env vars. No app code changes — only what populates env vars.

## Application catalogue

| App | Purpose | Owns | Reads from |
|---|---|---|---|
| [bank-reconcile](./apps/bank-reconcile.md) | Bank statement scan + reconcile + Opera posting | `bank_aliases.db`, `bank_patterns.db`, statement-tracking SQLite | Opera SQL, IMAP, Gemini |
| [gocardless](./apps/gocardless.md) | Direct Debit payout import | `gocardless_payments.db` | Opera SQL, IMAP, GoCardless API, Gemini |
| [suppliers](./apps/suppliers.md) | Supplier statement reconciliation | `supplier_extraction_cache.db`, `supplier_statements.db` | Opera SQL, IMAP, SMTP, Gemini |
| [balance-check](./apps/balance-check.md) | Internal Opera balance reconciliation | (read-only, no own state) | Opera SQL |
| [core-email](./apps/core-email.md) | Shared IMAP poller + email storage | `email_data.db` | IMAP server |
| [core-opera-se](./apps/core-opera-se.md) | Opera SQL connection gateway | (stateless) | Opera SQL Server |
| [core-opera3](./apps/core-opera3.md) | Opera 3 read gateway | (stateless) | Opera 3 file share, Write Agent |

## Documents

- [`env-var-contract.md`](./env-var-contract.md) — every env var the apps consume, what it's for, who needs it
- [`dependency-graph.md`](./dependency-graph.md) — which app depends on which other (apps + core services)
- [`health-checks.md`](./health-checks.md) — health/readiness endpoint per app
- [`deployment-shapes.md`](./deployment-shapes.md) — single-tenant on-prem, multi-tenant SaaS, SAM-hosted
- [`migration-checklist.md`](./migration-checklist.md) — per-app checklist for the SAM merge
- [`apps/`](./apps/) — per-app details

## Migration philosophy

**Late binding to SAM.** Apps don't know they're running on SAM.
They read env vars and call HTTP URLs. SAM populates the env vars
and routes the URLs to its own services.

**Adapters for SAM equivalents.** Each app's external dependencies
are behind interfaces (`apps/{name}/ports/*.py`). When SAM provides
an equivalent service (auth, email, opera-sql), we add a SAM-specific
adapter and switch one config flag per app.

**No SAM-specific code in apps.** App business logic must stay
SAM-independent. SAM-specific behaviour goes in adapters or in
deployment manifests.

## Contact

For SAM integration questions: charlieb@intsysuk.com
