# Opera 3 Company Selection, Login Sync & User Access — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Opera 3 login, company selection, and user access work identically to the SE version — reading from FoxPro DBF files over SMB instead of SQL Server.

**Architecture:** Three changes to `api/main.py`: (1) auto-generate `companies/*.json` from `seqco.dbf` on SMB connect, (2) add Opera 3 branch in login to sync from `sequser.dbf`/`seqnavgrps.dbf`, (3) add Opera 3 branch in company switch to set data path instead of SQL connector. Helper functions in `opera3_foxpro.py` read the system DBF files.

**Tech Stack:** Python, FastAPI, dbfread, smbprotocol (already installed), SQLite (users.db)

**Spec:** `docs/superpowers/specs/2026-03-29-opera3-company-login-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `sql_rag/opera3_foxpro.py` | Modify | Add `read_sequser()` and `read_seqnavgrps()` helpers to `Opera3System` |
| `api/main.py` | Modify | `_generate_opera3_company_configs()` helper; Opera 3 branch in `login()`; Opera 3 branch in `switch_company()` |

**Files unchanged:**
- `frontend/src/pages/Login.tsx` — already fetches `/api/companies/list` and `/api/auth/user-default-company`
- `frontend/src/components/CompanySelector.tsx` — already works with any company
- `sql_rag/user_auth.py` — `sync_user_from_opera()` is Opera-version-agnostic
- `sql_rag/smb_access.py` — already provides shared-read file access

---

### Task 1: Add sequser/seqnavgrps reader helpers to Opera3System

**Files:**
- Modify: `sql_rag/opera3_foxpro.py`

- [ ] **Step 1: Add `read_sequser()` method to `Opera3System`**

After the existing `read_system_table()` method (around line 640), add:

```python
def read_sequser(self, username: str = None) -> List[Dict[str, Any]]:
    """
    Read user records from System/sequser.dbf.

    Args:
        username: If provided, return only the matching user (case-insensitive)

    Returns:
        List of user dicts with normalised keys:
        {user, username, manager, prefcomp, cos, email_addr, state}
    """
    reader = Opera3Reader(str(self.system_path), encoding=self.encoding)
    results = []
    try:
        for record in reader.iter_table("sequser"):
            user_id = record.get("USER", "").strip()
            if not user_id:
                continue
            if username and user_id.upper() != username.upper():
                continue

            results.append({
                "user": user_id,
                "username": record.get("USERNAME", "").strip(),
                "manager": bool(record.get("MANAGER", False)),
                "prefcomp": record.get("PREFCOMP", "").strip(),
                "cos": record.get("COS", "").strip(),
                "email_addr": record.get("EMAIL_ADDR", "").strip() if record.get("EMAIL_ADDR") else "",
                "state": 0,  # Opera 3 sequser has no state field — assume active
            })

            if username:
                break  # Found the user, no need to continue

    except FileNotFoundError:
        logger.warning("sequser.dbf not found in System folder")
    except Exception as e:
        logger.warning(f"Error reading sequser.dbf: {e}")

    return results
```

- [ ] **Step 2: Add `read_seqnavgrps()` method to `Opera3System`**

After `read_sequser()`, add:

```python
def read_seqnavgrps(self, username: str) -> Dict[str, bool]:
    """
    Read NavGroup permissions for a user from System/seqnavgrps.dbf.

    Opera 3 format: one row per navgroup the user has access to.
    Field COMMANDID = navgroup name (e.g., 'NavGroupFinancials').
    Presence of a row = enabled.

    Args:
        username: The Opera user ID to look up

    Returns:
        Dict mapping navgroup names to True (all present = enabled)
    """
    reader = Opera3Reader(str(self.system_path), encoding=self.encoding)
    navgroups = {}
    try:
        for record in reader.iter_table("seqnavgrps"):
            user_id = record.get("USER", "").strip()
            if user_id.upper() != username.upper():
                continue
            command_id = record.get("COMMANDID", "").strip()
            if command_id:
                navgroups[command_id] = True
    except FileNotFoundError:
        logger.warning("seqnavgrps.dbf not found in System folder")
    except Exception as e:
        logger.warning(f"Error reading seqnavgrps.dbf: {e}")

    return navgroups
```

- [ ] **Step 3: Verify COMMANDID values match NavGroup naming convention**

The `COMMANDID` values in `seqnavgrps.dbf` must match the `NavGroup*` keys in `UserAuth.OPERA_NAVGROUP_TO_MODULE` (e.g., `NavGroupFinancials`). From our earlier exploration, the values ARE in this format. If they differ, a mapping layer would be needed. Add a log warning in `read_seqnavgrps()` for unrecognised COMMANDID values:

```python
                if command_id:
                    navgroups[command_id] = True
                    # Warn if this doesn't match any known NavGroup
                    from sql_rag.user_auth import UserAuth
                    if command_id not in UserAuth.OPERA_NAVGROUP_TO_MODULE:
                        logger.debug(f"Unrecognised NavGroup in seqnavgrps.dbf: {command_id}")
```

- [ ] **Step 4: Verify imports**

Run: `source venv/bin/activate && python -c "from sql_rag.opera3_foxpro import Opera3System; print('OK')"`
Expected: `OK`

- [ ] **Step 5: Commit**

```bash
git add sql_rag/opera3_foxpro.py
git commit -m "Add sequser and seqnavgrps reader helpers to Opera3System"
```

---

### Task 2: Auto-generate company configs from seqco.dbf

**Files:**
- Modify: `api/main.py`

- [ ] **Step 1: Add `_generate_opera3_company_configs()` helper**

Add this function near the existing `_connect_opera3_smb()` function (around line 930):

```python
def _generate_opera3_company_configs():
    """
    Read seqco.dbf from the SMB share and create/update companies/*.json
    for each Opera 3 company. Preserves user-added settings in existing configs.
    """
    smb = get_smb_manager()
    if smb is None or not smb.is_connected():
        logger.warning("Cannot generate Opera 3 company configs — SMB not connected")
        return

    try:
        from sql_rag.opera3_foxpro import Opera3System
        local_base = str(smb.get_local_base())
        system = Opera3System(local_base)
        companies = system.get_companies()

        if not companies:
            logger.warning("No companies found in seqco.dbf")
            return

        os.makedirs(COMPANIES_DIR, exist_ok=True)

        for co in companies:
            code = co.get("code", "").strip()
            if not code:
                continue

            company_id = f"o3_{code.lower()}"
            config_path = os.path.join(COMPANIES_DIR, f"{company_id}.json")

            # Load existing config to preserve user-added settings
            existing = {}
            if os.path.exists(config_path):
                try:
                    with open(config_path, 'r') as f:
                        existing = json.load(f)
                except Exception:
                    pass

            # Build/update config — preserve existing settings, update from seqco
            company_config = existing.copy()
            company_config["id"] = company_id
            company_config["name"] = co.get("name", code)
            company_config["opera_version"] = "3"
            company_config["opera3_company_code"] = code.upper()
            company_config["description"] = f"{code.upper()} - {co.get('name', '')}"

            # Resolve data path relative to SMB share
            subdir = co.get("subdir", "")
            if subdir:
                norm = subdir.replace("\\", "/").strip("/")
                share_lower = smb.share.lower().replace("\\", "/")
                norm_lower = norm.lower()
                idx = norm_lower.find(share_lower)
                if idx >= 0:
                    relative = norm[idx + len(share_lower):].strip("/")
                    company_config["opera3_data_path"] = relative if relative else ""
                else:
                    # Fallback: look for "Data" or "DATA" in path
                    parts = norm.split("/")
                    for i, p in enumerate(parts):
                        if p.lower() == "data":
                            company_config["opera3_data_path"] = "/".join(parts[i:])
                            break
                    else:
                        company_config["opera3_data_path"] = ""
            else:
                company_config["opera3_data_path"] = ""

            # Set defaults for settings if not already present
            if "settings" not in company_config:
                company_config["settings"] = {
                    "currency": "GBP",
                    "currency_symbol": "£",
                    "date_format": "DD/MM/YYYY"
                }

            with open(config_path, 'w') as f:
                json.dump(company_config, f, indent=2)

            logger.info(f"Generated company config: {config_path} ({company_config['name']})")

    except Exception as e:
        logger.error(f"Error generating Opera 3 company configs: {e}")
```

- [ ] **Step 2: Call `_generate_opera3_company_configs()` after SMB connect**

In `_connect_opera3_smb()`, after `set_smb_manager(manager)` and before the return, add:

```python
        # Auto-generate company configs from seqco.dbf
        _generate_opera3_company_configs()
```

- [ ] **Step 3: Verify — restart API and check companies are generated**

After the API auto-reloads, check:
```bash
ls companies/o3_*.json
cat companies/o3_z.json | python3 -m json.tool
```

Expected: `o3_z.json`, `o3_p.json`, `o3_a.json` with correct names and data paths.

- [ ] **Step 4: Commit**

```bash
git add api/main.py
git commit -m "Auto-generate Opera 3 company configs from seqco.dbf on SMB connect"
```

---

### Task 3: Opera 3 login sync from sequser.dbf

**Files:**
- Modify: `api/main.py`

- [ ] **Step 1: Add Opera 3 sync branch in `login()` endpoint**

In the `login()` function (line 1041), the existing code checks `if sql_connector:` to sync from SE. Add an Opera 3 branch. After the existing `except` block for the SE sync (around line 1140), add:

```python
    # Opera 3: sync from sequser.dbf if SMB is available
    elif get_smb_manager() is not None:
        try:
            from sql_rag.opera3_foxpro import Opera3System
            smb = get_smb_manager()
            local_base = str(smb.get_local_base())
            system = Opera3System(local_base)

            users = system.read_sequser(username=request.username)
            if users:
                row = users[0]
                opera_user = row['user']
                display_name = row['username'] or opera_user
                is_manager = row['manager']
                email = row['email_addr'] or None
                pref_company_letter = row['prefcomp']
                cos_string = row['cos']

                # Load companies for mapping
                companies = load_companies()

                # Map preferred company letter to company ID
                default_company = None
                if pref_company_letter:
                    for co in companies:
                        if co.get('opera3_company_code', '').upper() == pref_company_letter.upper():
                            default_company = co.get('id')
                            break

                # Parse cos field to get company access
                user_company_access = None
                if cos_string:
                    user_company_access = []
                    for char in cos_string:
                        for co in companies:
                            if co.get('opera3_company_code', '').upper() == char.upper():
                                user_company_access.append(co.get('id'))
                                break
                    logger.info(f"Opera 3 company access for '{opera_user}' from cos='{cos_string}': {user_company_access}")

                # Read NavGroup permissions
                opera_permissions = None
                try:
                    navgroups = system.read_seqnavgrps(opera_user)
                    if navgroups:
                        opera_permissions = UserAuth.map_opera_navgroups_to_permissions(navgroups)
                        logger.info(f"Opera 3 NavGroups for '{opera_user}': {navgroups} -> SQL RAG: {opera_permissions}")
                except Exception as navgrp_err:
                    logger.warning(f"Could not read Opera 3 NavGroups: {navgrp_err}")

                # Sync user (creates if not exists, updates if exists)
                user_auth.sync_user_from_opera(
                    opera_username=opera_user,
                    display_name=display_name,
                    email=email,
                    is_manager=is_manager,
                    is_active=True,
                    preferred_company=default_company,
                    opera_permissions=opera_permissions,
                    company_access=user_company_access
                )
                logger.info(f"Synced user '{opera_user}' from Opera 3 sequser.dbf before login")
        except Exception as e:
            logger.warning(f"Could not sync from Opera 3 sequser.dbf: {e} — continuing with local auth")
```

Note: The existing code structure is:
```python
if sql_connector:
    try:
        # SE sync ...
    except Exception as e:
        # log warning, continue
```

Change `if sql_connector:` to:
```python
if sql_connector:
    # ... existing SE sync code unchanged ...
elif get_smb_manager() is not None:
    # ... Opera 3 sync code above ...
```

- [ ] **Step 2: Verify login works — test with Opera 3 user**

Log in via the API with a known Opera 3 user:
```bash
curl -s -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"ADMIN","password":"Harry"}' | python3 -m json.tool
```

Check the API logs for the sync message. Then verify user-default-company:
```bash
curl -s "http://localhost:8000/api/auth/user-default-company?username=ADMIN" | python3 -m json.tool
```

Expected: `default_company` should be `o3_z` (from ADMIN's PREFCOMP=Z).

- [ ] **Step 3: Commit**

```bash
git add api/main.py
git commit -m "Add Opera 3 login sync from sequser.dbf and seqnavgrps.dbf"
```

---

### Task 4: Opera 3 branch in company switch

**Files:**
- Modify: `api/main.py`

- [ ] **Step 1: Add Opera 3 branch in `switch_company()` endpoint**

In `switch_company()` (line 2689), the current code at line 2710-2713 does:
```python
    database_name = company.get("database")
    if not database_name:
        raise HTTPException(status_code=400, detail="Company has no database configured")
```

Replace lines 2710-2790 with a branching structure:

```python
    opera_version = company.get("opera_version", "SE")

    if opera_version == "3":
        # ---- Opera 3 company switch: set data path, no SQL connector ----
        smb = get_smb_manager()
        if smb is None or not smb.is_connected():
            raise HTTPException(status_code=503, detail="Opera 3 SMB connection not available")

        data_path_rel = company.get("opera3_data_path", "")
        local_base = smb.get_local_base()
        if data_path_rel:
            opera3_data_path = str(local_base / data_path_rel)
        else:
            opera3_data_path = str(local_base)

        # Update process-level defaults
        _default_company_id = company_id
        _company_data[company_id] = company

        # Store the Opera 3 data path in config (memory only)
        if config and config.has_section("opera"):
            config["opera"]["opera3_base_path"] = opera3_data_path

        # Save company to user's session
        auth_header = request.headers.get('Authorization', '')
        session_token = auth_header.replace('Bearer ', '') if auth_header.startswith('Bearer ') else ''
        if session_token and user_auth:
            user_auth.set_session_company(session_token, company_id)

        # Switch per-company data directory
        data_dir = get_company_data_dir(company_id)
        migrate_root_databases(company_id)
        logger.info(f"Per-company data directory: {data_dir}")

        # Create / reuse per-company email storage
        if company_id not in _company_email_storages:
            email_db = get_company_db_path(company_id, "email_data.db")
            _company_email_storages[company_id] = EmailStorage(str(email_db))
            logger.info(f"Created email storage for company {company_id}")

        # Set module-level globals and null out SQL connector
        # (prevents stale SE connector being used while Opera 3 is active)
        _ensure_company_context(company_id)
        sql_connector = None

        # Re-register email providers
        if email_sync_manager:
            email_sync_manager.providers.clear()
            try:
                await _initialize_email_providers()
            except Exception as e:
                logger.warning(f"Could not re-register email providers for {company_id}: {e}")

        # Reinitialize VectorDB
        try:
            chroma_dir = str(get_company_chroma_dir(company_id))
            vector_db = VectorDB(config, persist_dir=chroma_dir)
        except Exception as e:
            logger.warning(f"Could not reinitialize VectorDB on company switch: {e}")

        _sync_active_system_config()

        logger.info(f"Switched to Opera 3 company {company_id} ({company['name']}) at {opera3_data_path}")
        return {
            "success": True,
            "message": f"Switched to {company['name']}",
            "company": company
        }

    else:
        # ---- SE company switch: existing SQL connector logic ----
        database_name = company.get("database")
        if not database_name:
            raise HTTPException(status_code=400, detail="Company has no database configured")

        # ... rest of existing SE switch code unchanged ...
```

The existing SE code (lines 2715-2790) goes inside the `else:` block, unchanged.

- [ ] **Step 2: Verify company switch works**

After logging in, switch to an Opera 3 company:
```bash
TOKEN=<from login>
curl -s -X POST http://localhost:8000/api/companies/switch/o3_z \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

Expected: `{"success": true, "message": "Switched to Orion Vehicles Leasing", ...}`

- [ ] **Step 3: Commit**

```bash
git add api/main.py
git commit -m "Add Opera 3 branch in company switch — set data path instead of SQL connector"
```

---

### Task 5: End-to-end verification

- [ ] **Step 1: Verify company configs were generated**

```bash
ls companies/o3_*.json
```
Expected: `o3_a.json o3_p.json o3_z.json`

- [ ] **Step 2: Verify company list endpoint includes Opera 3 companies**

```bash
curl -s http://localhost:8000/api/companies/list | python3 -m json.tool
```
Expected: Should include `o3_z`, `o3_p`, `o3_a` alongside any SE companies.

- [ ] **Step 3: Verify login sync populates default company**

```bash
curl -s "http://localhost:8000/api/auth/user-default-company?username=ADMIN"
```
Expected: `{"default_company": "o3_z", ...}`

- [ ] **Step 4: Verify full login + company switch flow**

1. Log in via UI login page
2. Verify company dropdown shows Opera 3 companies (Z, P, A)
3. Verify default company is pre-selected
4. After login, switch company via the header dropdown
5. Verify data loads from the correct company folder

- [ ] **Step 5: Commit any fixes**

```bash
git add -A
git commit -m "Opera 3 company login: end-to-end verification fixes"
```
