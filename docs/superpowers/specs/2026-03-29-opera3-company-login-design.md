# Opera 3 Company Selection, Login Sync & User Access

**Date:** 2026-03-29
**Status:** Approved

## Problem

The Opera 3 (FoxPro/SMB) version does not support company selection at login, user access control per company, or default company preferences. The SE version does all of this by reading from `Opera3SESystem.dbo.sequser` and generating company configs. Opera 3 users should have an identical experience.

## Goal

When the installation is Opera 3, the login flow, company selection, company switching, user access control, and default company behaviour must be identical to the SE version. The user should not be able to tell the difference.

## Solution

Three pieces:

### 1. Auto-generate Company Configs from seqco.dbf

**When:** On SMB connect (app startup + config save from Installations page). Called from `_connect_opera3_smb()` after a successful connection — a new helper `_generate_opera3_company_configs()` reads seqco.dbf and writes/updates the JSON files.

**Process:**
1. Read `System/seqco.dbf` via SMB (shared read access)
2. For each company record, create/update `companies/o3_{letter}.json`:

```json
{
  "id": "o3_z",
  "name": "Orion Vehicles Leasing",
  "opera_version": "3",
  "opera3_company_code": "Z",
  "description": "Z - Orion Vehicles Leasing",
  "opera3_data_path": "Data",
  "settings": {
    "currency": "GBP",
    "currency_symbol": "£",
    "date_format": "DD/MM/YYYY"
  }
}
```

- `id` = `o3_{lowercase letter}` from `CO_CODE` (prefixed to avoid collision with SE company IDs which use `se_{letter}`)
- `name` = from `CO_NAME`
- `opera_version` = `"3"` (matches existing codebase convention — SE uses `"SE"`, Opera 3 uses `"3"`)
- `opera3_company_code` = uppercase company code letter (e.g., `Z`) — used for COS/PREFCOMP mapping
- `opera3_data_path` = relative SMB path extracted from `CO_SUBDIR` (e.g., `Data`, `DATA/P`)
- No `database` field (SE-only)

**Update rules:**
- If JSON already exists, update `name`, `description`, `opera3_data_path`, `opera3_company_code` from seqco
- Preserve any user-added settings (email, modules, payroll, dashboard_config)
- Companies removed from seqco.dbf are NOT auto-deleted (safety)

### 2. Login Sync from sequser.dbf

**Mirrors the SE login sync exactly.** During login (`POST /api/auth/login`), if the active installation is Opera 3 (detected by `config.get("opera", "version") == "opera3"` or presence of SMB manager):

1. Read `System/sequser.dbf` via SMB (shared read access)
2. Find user by matching `USER` field (case-insensitive) against login username
3. Extract from the matched record:
   - `PREFCOMP` → default company (e.g., `Z` → company id `o3_z`)
   - `COS` → accessible companies (e.g., `ZPA` → split each letter, map to `['o3_z', 'o3_p', 'o3_a']`)
   - `MANAGER` → admin/is_manager flag
   - `USERNAME` → display name
4. Read `System/seqnavgrps.dbf` — match user by `USER` field, extract NavGroup boolean flags, map to SQL RAG modules (same NavGroup→module mapping as SE)
5. Call existing `user_auth.sync_user_from_opera()` with extracted data to update:
   - `users.default_company` from PREFCOMP
   - `user_companies` entries from COS
   - `user_permissions` from NavGroups
   - Admin status from MANAGER

**COS → Company ID mapping:**
SE maps COS characters by matching against the `database` field suffix. Opera 3 has no `database` field. Instead, match each COS character against `opera3_company_code` in the company configs:
```
COS = "ZPA" → for each char, find company where opera3_company_code == char
Z → o3_z, P → o3_p, A → o3_a
```
Same logic for PREFCOMP → default_company.

**Fallback:** If SMB is unavailable or sequser.dbf is locked, fall back to local `users.db` credentials — same as SE does when SQL Server is unreachable. Note: on first-ever login before any sync, the user must exist in `users.db` already (default admin or manually created).

**Data format differences from SE:**

| Field | SE (SQL) | Opera 3 (DBF) |
|-------|----------|----------------|
| Username | `us_userid` | `USER` |
| Display name | `us_name` | `USERNAME` |
| Default company | `prefcomp` | `PREFCOMP` |
| Company access | `cos` | `COS` |
| Manager flag | `manager` | `MANAGER` |
| NavGroups table | `seqnavgrps` (SQL) | `System/seqnavgrps.dbf` |

Same data, different field names and access method.

**seqnavgrps.dbf fields** (to be verified during implementation — read the DBF field list and map to the SE SQL column equivalents).

### 3. Company Switch for Opera 3

`POST /api/companies/switch/{company_id}` — the existing endpoint has a hard requirement for a `database` field and will reject Opera 3 companies with a 400 error. An Opera 3 branch is needed:

1. Load company config from `companies/{company_id}.json`
2. Check `opera_version` — if `"3"`, take the Opera 3 path:
   - Skip SQL connector creation entirely
   - Resolve the data path: SMB local base + `opera3_data_path` from config
   - Set this as the active data path for `Opera3Reader` and `Opera3FoxProImport`
3. If `opera_version == "SE"`, take the existing SE path (unchanged)
4. Common housekeeping for both paths:
   - Update session company in `users.db`
   - Update per-company data directory (`data/{company_id}/`)
   - Swap email storage, clear caches

**No SQL connector swap for Opera 3** — the equivalent action is pointing the data path at the correct company folder on the SMB share.

## Branching Logic

The login endpoint and company switch endpoint need to detect whether the active installation is Opera 3 or SE. The branching condition:

```python
opera_version = config.get("opera", "version", fallback="sql_se")
if opera_version == "opera3" and get_smb_manager() is not None:
    # Opera 3 path — read from DBF via SMB
else:
    # SE path — query SQL Server (existing code)
```

## Files to Modify

| File | Change |
|------|--------|
| `api/main.py` | `_generate_opera3_company_configs()` helper called after SMB connect; Opera 3 branch in login sync; Opera 3 branch in company switch (skip SQL, set data path) |
| `sql_rag/opera3_foxpro.py` | Helper functions to read sequser.dbf and seqnavgrps.dbf fields via SMB |
| `sql_rag/user_auth.py` | Works as-is — `sync_user_from_opera()` accepts generic Python types, is Opera-version-agnostic |

## Files Unchanged

| File | Why |
|------|-----|
| `frontend/src/pages/Login.tsx` | Already fetches company list, default company, handles selection |
| `frontend/src/components/CompanySelector.tsx` | Already works with any company from `/api/companies` |
| `users.db` schema | Same tables: users, user_companies, user_permissions, sessions |
| `sql_rag/smb_access.py` | Already provides all needed file access (with shared read) |

## User Experience

Identical to SE:
1. Login page shows company dropdown populated from `companies/*.json` (filtered by opera_version matching installation)
2. Username entered → default company pre-selected from `PREFCOMP`
3. Company dropdown shows only companies the user has access to (from `COS`)
4. After login, company switcher in header works the same way
5. Switching company changes the active data folder, refreshes data
