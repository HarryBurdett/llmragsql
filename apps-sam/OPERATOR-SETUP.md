# Operator setup checklist

Print this. Tick boxes as you go. Each phase is independent — you
can stop and resume.

---

## Phase 1: Extract & push (one-off, all 4 plugins, ~30 min)

### Pre-conditions
- [ ] Working directory clean (`git status` in this repo shows nothing pending)
- [ ] Logged into GitHub with `gh` CLI or SSH access to `intsysuk` org

### Extract everything
```sh
chmod +x apps-sam/scripts/*.sh
./apps-sam/scripts/extract-all.sh
```
Should print 4 ✓ lines and `~/sam-plugins-staging/sam-*` paths.

- [ ] All 4 plugins extracted to `~/sam-plugins-staging/`
- [ ] Each shows `tests passed`, `lint clean`, `build ok`

### Create GitHub repos (one-off)
For each plugin, create a private repo under `intsysuk`:
- [ ] `intsysuk/sam-balance-check`
- [ ] `intsysuk/sam-bank-reconcile`
- [ ] `intsysuk/sam-gocardless`
- [ ] `intsysuk/sam-suppliers`

Don't initialise (no README, no .gitignore — extraction provides them).

### Push to GitHub
```sh
./apps-sam/scripts/push-to-github.sh balance-check
./apps-sam/scripts/push-to-github.sh bank-reconcile
./apps-sam/scripts/push-to-github.sh gocardless
./apps-sam/scripts/push-to-github.sh suppliers
```

- [ ] All 4 repos contain `main` branch + `v1.0.0` tag

---

## Phase 2: SAM Central registration (~10 min)

In SAM Central admin UI, for each plugin:

### Apps catalogue
- [ ] balance-check
  - app_id: `balance-check`
  - git_url: `https://github.com/intsysuk/sam-balance-check.git`
  - default_version: `v1.0.0`
- [ ] bank-reconcile
  - app_id: `bank-reconcile`
  - git_url: `https://github.com/intsysuk/sam-bank-reconcile.git`
  - default_version: `v1.0.0`
- [ ] gocardless
  - app_id: `gocardless`
  - git_url: `https://github.com/intsysuk/sam-gocardless.git`
  - default_version: `v1.0.0`
- [ ] suppliers
  - app_id: `suppliers`
  - git_url: `https://github.com/intsysuk/sam-suppliers.git`
  - default_version: `v1.0.0`

### License assignment
On the IntSys client license:
- [ ] Assign `balance-check` `v1.0.0`
- [ ] Assign `bank-reconcile` `v1.0.0`
- [ ] Assign `gocardless` `v1.0.0`
- [ ] Assign `suppliers` `v1.0.0`

### GitHub PAT
- [ ] Confirmed SAM Central's GitHub PAT has read access to the new repos
  (already configured at deploy — only needed for new repo access)

---

## Phase 3: Trigger install on SAM host (~5 min)

On your SAM-running machine (Mac via Docker):

```sh
# Force a license check / app sync now (instead of waiting for the cron)
curl -X POST http://localhost/api/admin/app-updates/check-updates \
  -H "Cookie: sam_session=<paste-from-browser>"
```

Or in SAM Admin UI: Apps → Sync now.

Watch the logs:
```sh
docker logs -f ai-sam | grep -E "GitInstall|PluginLoader"
```

- [ ] See `[GitInstall] Cloning ...` for each of the 4 repos
- [ ] See `[GitInstall] Running npm ci ...`
- [ ] See `[GitInstall] Running npm run build ...`
- [ ] See `[GitInstall] Success: <appId> @ <sha>` for each
- [ ] See `[PluginLoader] Loaded: <appId>@1.0.0` for each

If any step errors, see `apps-sam/EMBEDDING.md § Step 9` for common
causes.

---

## Phase 4: Smoke test

```sh
./apps-sam/scripts/post-install-smoke.sh http://localhost <sam-session-cookie>
```

- [ ] All 4 plugins green

If anything red, the script prints the response body — usually shows
which adapter is missing or which DB query failed.

---

## Phase 5: Per-plugin runtime configuration

### Opera SE connection

If you're already running SAM, this is already configured (it's how
SAM connects to your Opera SE database today). Verify:

- [ ] SAM Admin → Opera Connections → connection exists, status `active`
- [ ] SAM Admin → Opera Companies → both your company datasets are listed

If either is wrong, see `~/opera-knowledge-ref/DEPLOYMENT-GUIDE.md`
section "Opera Setup Wizard".

### Microsoft Graph (email)

Same — already configured if you receive bank statements / GoCardless
notifications today. Verify:

- [ ] SAM Admin → Email Settings → status `active`
- [ ] SAM Admin → Email Mailboxes → expected mailboxes listed,
      `is_active = true`

### Plugin-specific configuration

#### balance-check
Nothing to configure. Open the page, see the data.

- [ ] Balance Check page renders 4 tabs (Creditors, Debtors,
      Trial Balance, Cashbook)
- [ ] Each tab shows numbers from your live Opera

#### bank-reconcile
- [ ] SAM Admin → Email Mailboxes → bank-statements mailbox →
      `owner_app_id` = `bank-reconcile`
- [ ] Bank Reconciliation → Settings → set base folder path (if you
      drop PDFs into a watched folder; skip if email-only)
- [ ] Open Bank Reconciliation → bank dropdown shows your accounts

#### gocardless
- [ ] SAM Admin → Email Mailboxes → GoCardless payouts mailbox →
      `owner_app_id` = `gocardless`
- [ ] GoCardless → Settings → environment = `sandbox`
- [ ] GoCardless → Settings → access_token = `<sandbox token>`
- [ ] GoCardless → Settings → company_reference = `<your GC company ID>`
- [ ] GoCardless → Settings → default_batch_type = e.g. `BC`
- [ ] Test connection (button on Settings page) → succeeds

⚠️ **Do NOT set `environment: live` until smoke-tested in sandbox.**

#### suppliers
- [ ] SAM Admin → Email Mailboxes → supplier-statements mailbox →
      `owner_app_id` = `suppliers`
- [ ] Suppliers → Settings → response email templates configured
      (defaults are fine to start)

---

## Phase 6: First end-to-end test per plugin

### balance-check
- [ ] Click each of the 4 tabs. Numbers match what you see in Opera
      directly.

### bank-reconcile
- [ ] Drop a recent bank statement PDF into the watched folder OR
      verify it appears in the email scan
- [ ] Click through the 5-stage workflow: Select → Review → Import → Reconcile → Complete
- [ ] One full statement reconciled

### gocardless
- [ ] Trigger a GoCardless sandbox payout that lands in the mailbox
- [ ] Run scan-emails — payout appears
- [ ] Match payments to customers
- [ ] Post the batch (sandbox mode!)
- [ ] Verify Opera receipts created

### suppliers
- [ ] Email a supplier statement to the watched mailbox
- [ ] Verify it appears in the queue
- [ ] Open the statement → AI extracts line items
- [ ] Reconcile against ptran

---

## Troubleshooting

If a plugin doesn't load: see `apps-sam/EMBEDDING.md` Step 9.
If a plugin loads but shows 503 errors: the corresponding adapter
isn't configured. Check Phase 5.
If a plugin shows 500 errors: the SQL query failed against Opera.
Check `docker logs ai-sam` for the actual error.

For everything else, reproduce in browser devtools and paste the
failing request URL + response into the next chat session.
