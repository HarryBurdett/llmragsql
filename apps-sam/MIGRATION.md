# Data migration: Python SQLite → SAM MSSQL

When the SAM plugins replace the Python apps, the per-company SQLite
databases under `data/<company>/*/*.db` need their contents migrated
into SAM's per-app MSSQL databases.

## What gets migrated (and what doesn't)

| Plugin | Table | Intsys rows | CloudSiS rows | Migrate? | Why |
| --- | --- | --- | --- | --- | --- |
| **bank-reconcile** | `bank_import_aliases` | 161 | 1 | ✅ | Months of learned payee→customer/supplier mappings |
| | `bank_import_patterns` | 95 | 7 | ✅ | Pattern-learning state |
| | `deferred_transactions` | 10 | 0 | ✅ | Operator-deferred items |
| | `extraction_cache` | 8 | 12 | ❌ | Rebuilds from PDFs on next scan |
| | `import_locks` | runtime | runtime | ❌ | Ephemeral runtime state |
| | `match_config`, `duplicate_overrides` | 0 | 0 | ❌ | Empty in source |
| **gocardless** | `gocardless_settings.json` | (config) | (config) | ✅ partial | Most fields safe; **api_access_token + partner_* deliberately NOT migrated** — re-enter as sandbox in SAM |
| | `gocardless_mandates` | 39 | 35 | ✅ | External IDs — can't easily rebuild |
| | `gocardless_payment_requests` | 27 | 3 | ⚠ | Optional — only useful if there are pending requests |
| | `gocardless_subscriptions` | 78 | 66 | ⚠ | Optional — same |
| | `gocardless_subscription_documents` | 21 | 44 | ⚠ | Optional |
| | `gocardless_partner_signups` | 9 | 0 | ⚠ | Optional |
| | `mandate_setup_requests` | 1 | 0 | ⚠ | Optional |
| **suppliers** | `supplier_statements` | 9 | 5 | ✅ | Historical statements |
| | `statement_lines` | 75 | 12 | ⚠ | Optional — rebuild on next process |
| | `supplier_change_audit` | 264 | 22 | ❌ | Audit trail — leave in Python for compliance reference |
| | other supplier tables | varies | varies | ❌ | Not needed for first install |
| **balance-check** | (none) | — | — | — | Read-only plugin, no per-app data |

The script ships with the ✅ tables wired. ⚠ are easy to add — say so
if you want them in the first run.

## Prerequisites

- Plugins already installed by SAM (the per-app MSSQL DBs exist with
  Knex migrations applied). If they don't exist yet, run the install
  first — the script only INSERTs data, it doesn't run DDL.
- Network access from your migration machine to SAM's MSSQL Server.
- The Python `data/<company>/` folder available on the migration
  machine (if SAM is on a different host, copy or mount the folder).

## Usage

### One-off setup

```sh
cd apps-sam/scripts/migrate-from-python
npm install
```

### Dry run

Always dry-run first to see the row counts and verify connection
details before writing anything:

```sh
npm run migrate -- \
  --company intsys \
  --plugin bank-reconcile \
  --data-root /Users/maccb/llmragsql/data \
  --target-host <sam-mssql-host> \
  --target-port 1433 \
  --target-user sa \
  --target-password '<password>' \
  --target-db ai_sam_app_bank_reconcile \
  --dry-run
```

Output looks like:

```
  bank-reconcile from /Users/maccb/llmragsql/data/intsys/bank_reconcile
    [dry-run] bank_import_aliases: 161 row(s)
    [dry-run] bank_import_patterns: 95 row(s)
    [dry-run] deferred_transactions: 10 row(s)
  ✓ Dry run complete — re-run without --dry-run to apply.
```

### Apply

Drop `--dry-run` to actually write:

```sh
npm run migrate -- \
  --company intsys \
  --plugin bank-reconcile \
  --target-host <host> --target-user sa --target-password '<pw>' \
  --target-db ai_sam_app_bank_reconcile
```

The script uses MERGE / ON CONFLICT semantics where the SAM schema
has unique keys, so re-running is safe.

## Recipe for both companies × all 3 plugins

Sequence to run:

```sh
# Set common env vars once
export DATA_ROOT=/Users/maccb/llmragsql/data
export TARGET_HOST=<sam-mssql-host>
export TARGET_PORT=1433
export TARGET_USER=sa
export TARGET_PASSWORD='<password>'

# === Intsys ===
TARGET_DB=ai_sam_app_bank_reconcile npm run migrate -- --company intsys --plugin bank-reconcile --dry-run
TARGET_DB=ai_sam_app_bank_reconcile npm run migrate -- --company intsys --plugin bank-reconcile

TARGET_DB=ai_sam_app_gocardless     npm run migrate -- --company intsys --plugin gocardless --dry-run
TARGET_DB=ai_sam_app_gocardless     npm run migrate -- --company intsys --plugin gocardless

TARGET_DB=ai_sam_app_suppliers      npm run migrate -- --company intsys --plugin suppliers --dry-run
TARGET_DB=ai_sam_app_suppliers      npm run migrate -- --company intsys --plugin suppliers

# === CloudSiS ===
# Same three commands with --company cloudsis
```

balance-check has no per-app DB so no migration needed.

## Important: GoCardless secrets

The script **deliberately does not migrate** the GoCardless
`api_access_token`, `api_sandbox`, or `partner_*` fields from
`gocardless_settings.json`.

Reason: the live token is in the JSON. Per project policy ("DO NOT
make live API requests"), the SAM install should start with a
**sandbox** token. Re-enter via the GoCardless plugin's Settings
page.

The other settings (default batch type, fees nominal, exclusions,
subscription tag, etc.) are migrated since they're configuration not
secrets.

## Verifying after migration

After running for a plugin, point your SAM portal at the plugin and:

- **bank-reconcile**: open Settings → Aliases. Should show ~160
  aliases for Intsys, ~1 for CloudSiS.
- **gocardless**: open Mandates page. Should show ~39 / ~35 mandates.
  Then check Settings — should have the safe fields populated; the
  API token field should be empty (re-enter sandbox).
- **suppliers**: open Statement History (or whatever the relevant
  page is). Should show 9 / 5 historical statements.

## Rollback

The migration only INSERTs. To roll back, drop and re-create the
per-app DB and re-run the SAM Knex migrations:

```sql
-- On SAM MSSQL:
DROP DATABASE ai_sam_app_bank_reconcile;
```

Then trigger a SAM plugin reinstall (Central → Apps → Sync). SAM
recreates the DB and runs migrations from scratch.

Pre-production caveat: this is fine because there's no client data
to lose. After go-live you'd want a backup-first protocol.
