# SAM brief — enable a 2nd (Opera 3 demo) connection on the live Intsys UK SAM

**Date:** 2026-07-13 · **From:** Harry / Charlie · **For:** Jonathan (on return)
**Goal:** Harry needs to test the current v2 apps (bank-reconcile 2.7.29, gocardless 2.0.30) against the **Opera 3 VFP demo on 172.17.172.214** *inside the live Intsys UK SAM* (Docker on the Intsys server, LAN 172.17.173.130, SAM v1.10.0).

Everything on the .214 side is ready and verified today: sidecar `:8443` healthy, write agent `:9000` healthy at **2.6.12**, `.214` firewall rules already extended to admit the live SAM's IP (172.17.173.130) on both ports. The blocker is entirely SAM-platform-side, detailed below. **No live data was touched** — all attempts failed safely at the API gate.

---

## 1. What happens today (evidence)

Creating an Opera 3 connection for the Intsys UK tenant (which already has "Opera SE Production") returns:

```
HTTP 422 { error: "v1_plugins_block_multi_connection",
  message: "Cannot add a second connection while plugins … are still on the legacy contract." }
```

That's `checkV1PluginGate` in `packages/backend/src/routes/admin/connections.ts` — it refuses a 2nd active connection while any *loaded* plugin manifest has `samContextVersion ?? 1 < 2`.

**Apps on the live tenant, by their ACTIVE version's manifest:**

| Legacy (v1) — trigger the gate | V2 — multi-connection-safe |
|---|---|
| apautomation 1.24.3 | apassist 0.9.2 |
| eshop 1.0.0 ⚠ *no `app_versions` manifest row at all* | bank-reconcile 2.7.29 |
| finance-hub 1.3.56 | document-hub 1.3.0 |
| hr-system 1.4.16 | esign 1.3.0 |
| opera-copilot 1.0.5 | gocardless 2.0.30 |
| router 1.4.4 | |

⚠ The `eshop` anomaly (installed at 1.0.0 with no matching `app_versions` row) is worth a look independently — the loader can't even read its contract version.

## 2. Why the gate is right (for now): the runtime is single-connection

The gate is not the real problem — it's honest. SAM 1.10.0's runtime resolves **one connection globally**, so a 2nd row would be non-deterministic for *every* app, v2 included:

- `packages/backend/src/plugins/loader.ts:423`
  `opera_connections.where({ is_active: true }).first()` — no tenant filter, **no ordering**. Whichever row SQL Server returns first is what all v1 plugins bind to at load.
- `packages/backend/src/plugins/loader.ts:420` — `tenants.where({ is_active: true }).first()` (same pattern).
- `packages/backend/src/plugins/context.ts:76` and `:135` — per-request context resolution is also single-connection (`resolveOperaSystem` takes the first active `opera-se` row; the operaAgent/operaType resolution takes the first active row full stop).

So with two rows, finance-hub/apautomation (which **write** to Opera) could silently bind to the .214 demo instead of .99 production on any restart. Also note: deactivating apps doesn't dodge this — the loader loads from `app_registry` and ignores `app_installations.is_active`, so "switch the legacy apps off" is not an available mitigation.

## 3. The ask — platform changes (in order)

**A. Deterministic binding for the legacy path (small, safety-critical).**
Give the single-connection resolution a stable order — e.g. `orderBy('created_at', 'asc')` (oldest = the original production connection) or an explicit `is_primary` flag on `opera_connections`. Apply to `loader.ts:420/:423` and `context.ts:76/:135`. This alone makes a 2nd connection *safe for the legacy apps* (they keep binding to Opera SE Production forever).

**B. Per-company connection resolution for v2 apps (the real feature).**
`opera_companies` already carries `connection_id`. For samContextVersion 2 apps, resolve the request's context **from the company's connection** (company → connection → type/agent/facade) instead of the global first-active row. Then bank-reconcile/gocardless can serve .99 companies via SE and .214 companies via the sidecar/agent in the same instance — which is exactly Harry's test scenario. (The plugin scheduler is already per-connection — `pluginScheduler.ts` enumerates connections — so it's ahead of the request path here.)

**C. Relax `checkV1PluginGate` accordingly.**
Once A is in, the gate can allow additional connections while v1 plugins are present (they're pinned to the primary). If you'd rather keep it strict until the 6 legacy apps are upgraded to `samContextVersion: 2`, that works too — but A+B are the minimum for Harry's live test.

**D. Merge Charlie's two local `ai-sam` branches (prerequisite for Opera 3 *reads* on live).**
These are on Charlie's Mac (`/Users/maccb/ai-sam`), deliberately not pushed to your repo without your say-so — say the word and Charlie pushes them for PR review. Without the first one, every app read screen on an opera-3 connection 500s (proven, then fixed, during the dev end-to-end on 2026-07-12).

`fix/opera3-dataroot-folder` — 8 commits, 47 regression tests, all facade (`packages/backend/src/opera/foxpro-facade.ts`):
```
4f61a1e fix(opera3): companyFolder '' when co_subdir points at the Data root
e39fcff fix(opera3): strip MSSQL table hints (WITH (NOLOCK)/UPDLOCK/…) in the facade rewrite
dacd636 fix(opera3): translate MSSQL TOP for VFP (Knex .first() emitted top (1) → sidecar 500)
7f0508b feat(opera3): callable facade — Knex query-builder support on opera-3
7a71975 fix(opera3): facade raw() is dual-purpose like Knex's (fragment + awaitable)
d13dbc0 fix(opera3): hoist ORDER BY expressions into aliased select columns
08e8a5c fix(opera3): recurse ISNULL args — nested ISNULL(ISNULL(x,y),z) left the inner untranslated
023d25a fix(opera3): no-op MSSQL session SET statements in the facade (VFP 500s on them)
```

`feat/opera-agent-unmark` — 1 commit (`b4cf550`): `packages/backend/src/opera/agent.ts` gains `unmarkReconciled` (`POST /reconcile/unmark`, agent ≥ 2.6.10) so bank-rec's reverse-reconcile works on opera-3. Independent of the branch above; both are against `origin/main` 234fcd0.

**E. Harden the plugin scheduler against unhandled rejections.**
During dev testing, a rejected promise from a background job crashed the whole SAM process. Charlie has a local dev-only guard (not committed, not the proper fix); the durable fix is a catch/log at the scheduler boundary so one bad job can't take the platform down. This matters more once opera-3 connections exist — background jobs against a sidecar are a new failure surface.

**F. Rebuild + redeploy the live Docker, restart.**
All of the above only reaches the live box via your image rebuild + container restart on the Intsys server. (Harry can restart the container; the rebuild/deploy is yours.)

## 4. After deploy — creating the connection (5 minutes)

Admin → Connections → Add → **Opera 3 (VFP)** on the live SAM, Intsys UK tenant (`FCF93C5F-F1CC-4AA9-B5A5-D1B1D52E2367`):

| Field | Value |
|---|---|
| Name / Slug | `Opera 3 Demo (.214)` / `o3-demo` |
| Sidecar URL | `http://172.17.172.214:8443` |
| Sidecar secret | (in `C:\Apps\AI-SAM-Sidecar\appsettings.json` on .214 — Harry/Charlie have it) |
| System folder | `C:\Apps\O3 Server VFP\System` |
| Data root | `C:\Apps\O3 Server VFP\Data` |
| Agent URL / key | `http://172.17.172.214:9000` / (Harry/Charlie have it) |
| App DB | host `172.17.172.99`, port `1433`, user `n8n`, prefix **`ai_sam_app_o3`** (must differ from the SE connection's `ai_sam_app` — the API enforces per-connection app-DB namespaces) |

Then **Sync companies** → expect A (Emergency Lighting), P (Flannery), Z (Orion demo — Harry's test company).

## 5. Verification checklist

1. `GET /api/health` on the live SAM → new version, healthy.
2. Existing Opera SE Production connection untouched: `last_check_status = ok`, SE companies still resolve, finance-hub/apautomation still bound to `.99` **after a restart** (this is the point of §3A — check it deliberately).
3. New o3-demo connection: health check ok; companies A/P/Z synced and enable-able.
4. bank-reconcile on company Z (o3-demo): screens load (facade fixes working — this is §3D's proof), statement flows read correctly.
5. gocardless on company Z: Import/Requests screens load.
6. Optional write proof (agent writes are gated by `OPERA3_WRITES_ENABLED` on the .214 agent; already enabled for the demo): post a cashbook entry on Z via bank-rec → `R…` entry lands in FoxPro `aentry`/`atran` (Charlie has done this end-to-end on dev SAM — 2026-07-12, entries R100000285/286).

## 6. Related known items (not blockers for this, but same territory)

- **Opera-3 duplicate-check blindness**: `ae_remove = 0` comparisons don't match VFP LOGICAL fields via the sidecar — needs type-aware coercion sidecar-side (`jonathangintsys/ai-sam-vfp-sidecar`).
- Agent `nominal-entry` verb lacks `cost_centre` (opera3-write-agent — ours, we'll handle).
- Golden-mastering of the gocardless-batch / recurring / allocate / reconcile agent verbs is still pending (snapshot-tool presets are in place for capture).
- The SAM crakd.ai branding brief from 2026-07-12 (`docs/2026-07-12-sam-crakd-branding-brief.md`) is still queued for you as well.

— Charlie
