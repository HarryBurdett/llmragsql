# Migration Checklist (SAM Merge)

Per-app checklist for migrating each application into the SAM
platform. Run through this list once SAM specifics are known.

## Per-app universal checklist

For **every** app being migrated:

- [ ] Confirm the app's required env vars are listed in
      [`env-var-contract.md`](./env-var-contract.md). If any are
      missing, document them.
- [ ] Map each env var to its SAM-provided source:
      - SAM secret? Direct env var? Config-API call?
- [ ] Verify SAM's name for each value (e.g. SAM might call it
      `OPERA_DB_HOST` whereas we call it `DATABASE_SERVER`).
      Add an alias adapter in `apps/core/env_config.py` if needed
      so apps don't change.
- [ ] Confirm volume / persistent-storage strategy for the app's
      SQLite databases (or migrate to SAM-provided Postgres).
- [ ] Health/readiness probe path: `/healthz` works today; confirm
      SAM uses this or change to SAM's convention.
- [ ] Logging format: today's apps log to stdout with Python's
      default formatter. If SAM expects JSON / structured logs,
      add a log adapter.
- [ ] Authentication: confirm whether SAM provides JWTs and we
      validate, or we keep `core-auth`. Set `AUTH_JWT_PUBLIC_KEY`
      env var or remove `core-auth` accordingly.
- [ ] Inter-service URLs: confirm SAM's service discovery
      mechanism. Today's `CORE_EMAIL_URL` etc. need pointing at
      SAM's equivalent.

## Per-app specific checklist

### bank-reconcile

- [ ] `core-email` URL → SAM's email service URL (if SAM has one)
- [ ] Statement-tracking SQLite (`email_data.db` for bank-rec rows
      specifically) — migrate or volume-mount
- [ ] `bank_aliases.db` and `bank_patterns.db` — bank-reconcile
      owns these; verify they migrate cleanly
- [ ] PDF extraction cache (`pdf_extraction_cache.db`) — keep
      in-app (small, app-private cache)
- [ ] If Opera 3 is in scope: confirm `OPERA3_WRITE_AGENT_URL`
      points at the customer's Windows Write Agent
- [ ] Confirm IMAP credentials are SAM-provided (`EMAIL_IMAP_*`)

### gocardless

- [ ] All bank-reconcile prerequisites apply
- [ ] `GOCARDLESS_ACCESS_TOKEN` — SAM provides per-customer (each
      tenant has their own GoCardless account)
- [ ] `GOCARDLESS_ENVIRONMENT` — sandbox / live (set per
      deployment, NOT per tenant)
- [ ] `GOCARDLESS_WEBHOOK_SECRET` — for inbound webhook validation
- [ ] `gocardless_payments.db` — migrate or volume-mount

### suppliers

- [ ] All bank-reconcile prerequisites apply
- [ ] SMTP credentials (`EMAIL_SMTP_*`) — required for remittance
- [ ] `supplier_extraction_cache.db` and `supplier_statements.db` —
      app-private, volume mount
- [ ] Confirm any per-supplier sync schedules survive the move

### balance-check

- [ ] No SQLite — read-only against Opera SQL
- [ ] Confirm SQL connection pool size + timeouts are appropriate
      under SAM's load model

### core-email

- [ ] If SAM provides an email/document ingestion service:
      - [ ] Replace `core-email` with a SAM adapter in each app
      - [ ] Drop the `core-email` container from the deployment
- [ ] Otherwise:
      - [ ] Migrate `email_data.db` to volume / Postgres
      - [ ] Confirm IMAP poller schedule (default: every 5 min)
            is appropriate

## Frontend

- [ ] `VITE_API_BASE_URL` env var at build time → SAM's gateway URL
- [ ] Confirm SPA routing (client-side) works through SAM's ingress
      (the nginx prod target uses `try_files $uri /index.html`)
- [ ] Authentication flow: if SAM uses SSO, swap the login UI for
      a redirect to SAM's auth service

## Cutover runbook (general shape)

1. Deploy the SAM-target Docker images alongside the current
   deployment (parallel run, no traffic).
2. Configure SAM env vars per the checklists above.
3. Smoke test each app's `/healthz` from inside SAM.
4. Smoke test one workflow per app:
   - bank-reconcile: scan inbox → preview → import → reconcile
     → complete
   - gocardless: scan → import batch → reconcile
   - suppliers: scan → reconcile statement
   - balance-check: load creditors / debtors / VAT
5. Switch DNS / SAM ingress to point at the new deployment.
6. Monitor for 24 hours.
7. Decommission the old deployment.

## Rollback

- SAM redirects ingress back to the previous deployment.
- The old deployment kept its data volumes during cutover, so no
  data loss.
- Apps don't know which deployment they're in — both read env vars,
  both work.

## Post-migration cleanup

- [ ] Remove `config.ini` from any image (it's a dev convenience —
      shouldn't ship to SAM-hosted)
- [ ] Remove docker-compose.yml from production (SAM has its own
      orchestration)
- [ ] Update each app's README to point at SAM-specific docs
- [ ] Archive `docs/sam-migration/` once migration is stable
