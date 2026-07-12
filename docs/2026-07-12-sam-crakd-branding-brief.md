# Brief: crakd.ai copyright / IP notices in the SAM platform

**For:** Jonathan
**From:** Harry (drafted by Claude, 2026-07-12)
**Repo:** `jonathangintsys/aisam`

## Context

All apps (bank-reconcile 2.7.29+, gocardless 2.0.29+) now carry crakd.ai
intellectual-property notices: a subtle per-screen footer, a full notice in
Settings → About, a proprietary `LICENSE`, `"license": "UNLICENSED"` in the
package.jsons, and a `copyright` field in `manifest.json` (so the packaged
`.sap` artefact itself is marked). Harry wants the **SAM platform to carry
and manage the same** — the portal chrome is SAM's, so the platform-level
notice is a SAM change. Style requirement: **subtle but clear** — one small
muted line, no screen clutter.

Reference implementation to copy from (identical pattern in both apps):
- `bank-rec` commit `8d26cc29` / `gocardless` commit `dd11ef9f`
  (branch `feat/opera-write-library`)
- Component: `frontend/src/CopyrightFooter.tsx` in either app.

## Canonical wording (copy-paste verbatim)

**Footer (every screen):**

```
© {currentYear} crakd.ai — All rights reserved.
```

**Full notice (About/legal surfaces):**

```
© {currentYear} crakd.ai. All rights reserved. This software — including its
design, code, workflows and documentation — is the intellectual property of
crakd.ai and is protected by copyright and other intellectual property laws.
It is licensed for use, not sold. Unauthorised copying, distribution,
modification or reverse engineering is prohibited.
```

**Trademark acknowledgement (where Opera is referenced):**

```
Pegasus Opera 3 and Opera SE are trademarks of Pegasus Software. This product
is an independent integration and is not affiliated with or endorsed by
Pegasus Software.
```

## Changes requested (smallest-first)

### 1. One shared branding constant

Add `packages/shared/src/branding.ts` (exported from the shared index):

```ts
export const BRANDING = {
  owner: 'crakd.ai',
  footer: (year: number) => `© ${year} crakd.ai — All rights reserved.`,
  notice: '…full notice above…',
  trademarks: '…trademark line above…',
} as const;
```

One source of truth; everything below imports it. (Optionally also expose it
on an existing public config/bootstrap endpoint so SPAs read it at runtime —
nice-to-have, not required.)

### 2. Portal + admin chrome footers

Render the footer line (tiny, muted, centred, e.g. `text-[11px] text-gray-400
text-center py-2`) at the bottom of:

- `packages/portal/src/components/layout/PortalLayout.tsx` (user portal)
- `packages/frontend/src/components/layout/AppLayout.tsx`
- `packages/frontend/src/components/layout/AdminLayout.tsx`
- `packages/frontend/src/plugins/AppShell.tsx` — **skip if the hosted plugin
  area already shows the app's own footer** (bank-rec/gocardless render their
  own; doubling it up would clutter — check visually).

Use `new Date().getFullYear()` so the year never goes stale.

### 3. Login pages

One small line under the login card on:
- `packages/portal/src/pages/Login.tsx`
- `packages/frontend/src/pages/Login.tsx`

This is the highest-value single placement — it marks the platform before any
session exists.

### 4. Admin → Apps page: surface each app's manifest copyright

`packages/frontend/src/pages/admin/AppsPage.tsx`: the `.sap` manifests now
carry a `copyright` string (e.g. bank-reconcile 2.7.29+). Where app details
render (version/status), show it as one muted line when present. Treat as
optional per-manifest — older `.sap`s won't have it.

### 5. Repo/package markings

- `LICENSE` file at the aisam repo root — copy the apps' LICENSE text
  (proprietary, crakd.ai; see bank-rec/LICENSE) if crakd.ai is also the
  platform's IP owner — **confirm with Harry**, since the platform's
  ownership may differ from the apps'.
- `"license": "UNLICENSED"` in each package.json under `packages/*` that
  currently has no license field.

### 6. Optional future (not now)

If SAM later serves branding through the plugin context (e.g.
`ctx.branding`), the apps can drop their local constants and consume it —
single-source across the whole estate. Not needed for this pass; the apps
are self-sufficient.

## Acceptance

- Every portal/admin screen shows the footer line; login pages marked.
- No layout shift or clutter (line sits below content, muted).
- Year renders current.
- Apps page shows manifest copyright for 2.7.29+/2.0.29+ uploads.
