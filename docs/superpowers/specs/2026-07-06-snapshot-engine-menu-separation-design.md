# Snapshot Tool — Separate Opera SE and Opera 3 into distinct menus

> Design — 2026-07-06

## Problem

The Transaction Snapshot tool (`apps/transaction_snapshot`) is a single page
(Utilities → Developer Tools → "Transaction Snapshot") that handles both Opera
engines through one screen. The engine is chosen *implicitly*: paste an Opera 3
DBF folder path and it runs FoxPro mode, otherwise it runs Opera SE (SQL).

This is hard to reason about and, worse, it **mislabels captures**. Opera 3 in
its SQL Server Edition flavour (e.g. company `z_demo` → database
`Opera3SECompany00Z`) is read through the *same SQL code path* as genuine Opera
SE, so its captures are stamped `source: "opera_se"` and land in the SE library —
indistinguishable from real Opera SE work.

The user wants to **snapshot Opera 3 as a first-class, clearly-separated
activity** and never again mix SE and Opera 3 captures.

## Goal

Clean, explicit separation of Opera SE and Opera 3 in both the workflow and the
stored library, driven by a deliberate user choice rather than inference.

## Key decision — the menu is the declaration

The engine a capture belongs to is determined by **which menu the user entered**,
not by the read mechanism underneath. Entering via the "Opera 3" menu stamps every
capture as Opera 3, whether it read SQL-SE or FoxPro. This makes the label a
deliberate choice, impossible to mislabel, and decouples two concepts that are
currently conflated:

- **Logical engine** (`engine`): `opera_se` | `opera_3` — what the user declares.
- **Read mechanism** (`source`): how the data was physically read — SQL (active
  company database) or FoxPro (DBF files). Informational only.

## Design

### 1. Menu & routes

`frontend/src/components/Layout.tsx` — replace the single Developer-Tools item
with two:

| Label | Route |
|-------|-------|
| Snapshot — Opera SE | `/utilities/transaction-snapshot/opera-se` |
| Snapshot — Opera 3  | `/utilities/transaction-snapshot/opera-3`  |

`frontend/src/App.tsx` — two routes render the same component with an `engine`
prop. The legacy path `/utilities/transaction-snapshot` **redirects** to the
Opera SE route so existing bookmarks/links keep working.

### 2. Component

`frontend/src/pages/TransactionSnapshot.tsx` gains one prop:
`engine: 'opera_se' | 'opera_3'`. The prop drives three things:

- **Capture controls**
  - *Opera SE:* snapshots the active SQL company. No DBF field.
  - *Opera 3:* a source sub-picker — **SQL-SE (active company)** or **FoxPro (DBF
    folder path + optional file filter)**. The existing `opera3Path` /
    `opera3Filter` fields are shown only when FoxPro is selected.
- **Library view** — shows *only* this engine's captures. The old
  `all / SE / Opera 3` filter buttons are removed (redundant once the page is
  engine-scoped).
- **Backend calls** — every capture sends `engine=<prop>`.

A read-only line shows the active company and its configured `opera_version`. On
the Opera 3 page in SQL mode, if the active company does not look like Opera 3 it
shows a **soft warning** — not a hard block, because the menu choice is
authoritative (Option 1).

### 3. Backend contract — `/before`, `/after`

`apps/transaction_snapshot/api/routes.py`:

- `/before` gains an explicit **`engine=opera_se|opera_3`** query param (the
  logical engine).
- **Read mechanism stays inferred from `data_path`:** a non-empty `data_path` →
  FoxPro; otherwise SQL against the active company. `engine` is independent of
  this.
- **Guard:** `engine=opera_se` combined with a non-empty `data_path` is
  contradictory (SE is always SQL) → HTTP 400 with a clear message.
- The saved snapshot/meta records **both** `engine` (the tag) and the read
  mechanism (`source`).
- **Library subfolder is chosen by `engine`**, not by `source`. This is the fix
  for the `z_demo` mislabelling: an Opera-3-over-SQL capture now correctly lands
  in `opera_3/`.
- `/after` reads `engine` from the persisted meta (exactly as it already reuses
  `source` / `data_path`), so a BEFORE/AFTER pair can never straddle engines.

### 4. Library tagging

- `_iter_library_files()` additionally yields the **subfolder** each file came
  from.
- The library listing endpoint attaches an authoritative `engine` field derived
  from the subfolder (`opera_se/` → `opera_se`, `opera_3/` → `opera_3`), falling
  back to the stored `source` for legacy flat-root entries written before the
  2026-05-12 reorg.
- The frontend filters the library by this `engine` field.

### 5. Cross-engine Compare

Retained as a **secondary, per-entry action** (compare an Opera SE capture against
its Opera 3 twin of the same transaction) — directly useful for the upcoming
write-feature evaluation. It moves off the removed filter bar into a per-entry
control.

## Out of scope

- **Migrating legacy captures.** Existing entries stay where they are. The one
  historically mislabelled `z_demo` Opera-3-over-SQL capture remains under
  `opera_se/`; no auto-migration. Re-tagging historical entries is a separate,
  optional task.

## Testing / verification

Per repo rules (`CLAUDE.md`: test the real endpoint; new backend tests run against
MSSQL, not SQLite):

- **Backend** (live `Opera3SECompany00Z`):
  - `/before?engine=opera_3` with **no** `data_path` → SQL read; saved entry
    stamped `engine=opera_3`; file written under `opera_3/`.
  - `/before?engine=opera_se&data_path=<something>` → HTTP 400.
  - `/after` inherits `engine` from meta; refuses an engine mismatch.
- **Frontend**:
  - `/opera-se` route → SE capture controls, SE-only library.
  - `/opera-3` route → source sub-picker, Opera-3-only library.
  - Legacy `/utilities/transaction-snapshot` → redirects to `/opera-se`.

## Files touched

- `frontend/src/components/Layout.tsx` — two menu items.
- `frontend/src/App.tsx` — two routes + redirect.
- `frontend/src/pages/TransactionSnapshot.tsx` — `engine` prop; engine-scoped
  controls & library; remove filter bar.
- `apps/transaction_snapshot/api/routes.py` — `engine` param on `/before`;
  engine-driven library subfolder + tag; meta persistence; `/after` reuse;
  `_iter_library_files` subfolder tagging; listing endpoint `engine` field.
