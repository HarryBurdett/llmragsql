"""
Supplier Contact Management API routes.

Provides endpoints for reading Opera zcontacts data and managing local
contact extensions (role flags, preferred contact method, notes).

Works with both Opera SQL SE and Opera 3 (FoxPro) backends.
Local extensions are stored in the supplier_contacts_ext SQLite table
via SupplierStatementDB.

Also provides write-back endpoints for Opera zcontacts (create, update, delete)
and security/automation extension fields on local contacts.
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================================
# Request/Response Models
# ============================================================

class ContactCreateRequest(BaseModel):
    name: str
    role: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    is_statement_contact: bool = False
    is_payment_contact: bool = False
    is_query_contact: bool = False
    preferred_contact_method: str = "email"
    notes: Optional[str] = None
    zcontact_id: Optional[str] = None


class ContactUpdateRequest(BaseModel):
    name: Optional[str] = None
    role: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    is_statement_contact: Optional[bool] = None
    is_payment_contact: Optional[bool] = None
    is_query_contact: Optional[bool] = None
    preferred_contact_method: Optional[str] = None
    notes: Optional[str] = None
    zcontact_id: Optional[str] = None


class ContactRolesRequest(BaseModel):
    is_statement_contact: bool
    is_payment_contact: bool
    is_query_contact: bool


class OperaContactCreateRequest(BaseModel):
    """Request body for creating a new Opera zcontact."""
    name: str
    title: Optional[str] = None
    role: Optional[str] = None
    department: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    mobile: Optional[str] = None
    fax: Optional[str] = None
    # Security/automation extension fields (stored locally, not in Opera)
    verified_sender: Optional[bool] = None
    verified_by: Optional[str] = None
    verified_date: Optional[datetime] = None
    authorised_bank_changes: Optional[bool] = None
    security_clearance: Optional[str] = None  # 'standard', 'elevated', 'director'
    verification_phone: Optional[str] = None
    last_verified: Optional[datetime] = None


class OperaContactUpdateRequest(BaseModel):
    """Request body for updating an existing Opera zcontact."""
    name: Optional[str] = None
    title: Optional[str] = None
    role: Optional[str] = None
    department: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    mobile: Optional[str] = None
    fax: Optional[str] = None
    # Security/automation extension fields (stored locally, not in Opera)
    verified_sender: Optional[bool] = None
    verified_by: Optional[str] = None
    verified_date: Optional[datetime] = None
    authorised_bank_changes: Optional[bool] = None
    security_clearance: Optional[str] = None  # 'standard', 'elevated', 'director'
    verification_phone: Optional[str] = None
    last_verified: Optional[datetime] = None


# ============================================================
# Helper: ensure supplier_contacts_ext has security columns
# ============================================================

_security_columns_ensured = False


def _ensure_security_columns(db):
    """
    Add security/automation extension columns to supplier_contacts_ext
    if they don't already exist. Runs once per process.
    """
    global _security_columns_ensured
    if _security_columns_ensured:
        return

    conn = db._get_connection()
    cursor = conn.cursor()

    # Check existing columns
    cursor.execute("PRAGMA table_info(supplier_contacts_ext)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    new_columns = [
        ("verified_sender", "BOOLEAN DEFAULT 0"),
        ("verified_by", "TEXT"),
        ("verified_date", "DATETIME"),
        ("authorised_bank_changes", "BOOLEAN DEFAULT 0"),
        ("security_clearance", "TEXT DEFAULT 'standard'"),
        ("verification_phone", "TEXT"),
        ("last_verified", "DATETIME"),
    ]

    for col_name, col_def in new_columns:
        if col_name not in existing_cols:
            try:
                cursor.execute(f"ALTER TABLE supplier_contacts_ext ADD COLUMN {col_name} {col_def}")
            except Exception:
                pass  # Column may already exist from another process

    conn.commit()
    conn.close()
    _security_columns_ensured = True


def _upsert_security_extensions(db, supplier_code: str, zcontact_id: str, body):
    """
    Create or update security/automation fields in supplier_contacts_ext
    for a given Opera zcontact.
    """
    _ensure_security_columns(db)

    # Check if extension already exists for this zcontact_id
    conn = db._get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id FROM supplier_contacts_ext WHERE supplier_code = ? AND zcontact_id = ?",
        (supplier_code, str(zcontact_id))
    )
    row = cursor.fetchone()
    conn.close()

    kwargs = {}

    # Map body fields that have values
    if hasattr(body, 'name') and body.name is not None:
        kwargs['name'] = body.name
    if hasattr(body, 'email') and body.email is not None:
        kwargs['email'] = body.email
    if hasattr(body, 'phone') and body.phone is not None:
        kwargs['phone'] = body.phone
    if hasattr(body, 'role') and body.role is not None:
        kwargs['role'] = body.role

    # Security fields
    if body.verified_sender is not None:
        kwargs['verified_sender'] = 1 if body.verified_sender else 0
    if body.verified_by is not None:
        kwargs['verified_by'] = body.verified_by
    if body.verified_date is not None:
        kwargs['verified_date'] = body.verified_date.isoformat()
    if body.authorised_bank_changes is not None:
        kwargs['authorised_bank_changes'] = 1 if body.authorised_bank_changes else 0
    if body.security_clearance is not None:
        if body.security_clearance not in ('standard', 'elevated', 'director'):
            raise ValueError(f"Invalid security_clearance: {body.security_clearance}")
        kwargs['security_clearance'] = body.security_clearance
    if body.verification_phone is not None:
        kwargs['verification_phone'] = body.verification_phone
    if body.last_verified is not None:
        kwargs['last_verified'] = body.last_verified.isoformat()

    if not kwargs:
        return None

    kwargs['zcontact_id'] = str(zcontact_id)

    if row:
        # Update existing
        ext_id = db.upsert_contact(supplier_code, contact_id=row[0], **kwargs)
    else:
        # Create new local extension linked to this Opera zcontact
        ext_id = db.upsert_contact(supplier_code, contact_id=None, **kwargs)

    return ext_id


# ============================================================
# Helper: merge Opera zcontacts with local extensions
# ============================================================

def _merge_contacts(opera_contacts: list, local_extensions: list) -> list:
    """
    Merge Opera zcontacts rows with local supplier_contacts_ext rows.

    For each Opera contact, check if a local extension exists (matched by
    zcontact_id or email). If so, overlay the local role flags. Local-only
    contacts (no matching Opera record) are appended at the end.

    Returns a combined list with Opera fields plus local role flags.
    """
    # Index local extensions by zcontact_id and email for fast lookup
    ext_by_zcontact_id = {}
    ext_by_email = {}
    used_ext_ids = set()

    for ext in local_extensions:
        if ext.get("zcontact_id"):
            ext_by_zcontact_id[str(ext["zcontact_id"])] = ext
        if ext.get("email"):
            ext_by_email[ext["email"].lower()] = ext

    merged = []

    for oc in opera_contacts:
        contact = dict(oc)
        matched_ext = None

        # Try matching by zcontact_id first (from Opera's row identity)
        zc_id = str(contact.get("zc_id", "")).strip()
        if zc_id and zc_id in ext_by_zcontact_id:
            matched_ext = ext_by_zcontact_id[zc_id]

        # Fallback: match by email
        if matched_ext is None:
            oc_email = (contact.get("zc_email") or "").strip().lower()
            if oc_email and oc_email in ext_by_email:
                matched_ext = ext_by_email[oc_email]

        # Overlay local extension fields
        if matched_ext:
            used_ext_ids.add(matched_ext["id"])
            contact["local_extension_id"] = matched_ext["id"]
            contact["is_statement_contact"] = bool(matched_ext.get("is_statement_contact", 0))
            contact["is_payment_contact"] = bool(matched_ext.get("is_payment_contact", 0))
            contact["is_query_contact"] = bool(matched_ext.get("is_query_contact", 0))
            contact["preferred_contact_method"] = matched_ext.get("preferred_contact_method", "email")
            contact["notes"] = matched_ext.get("notes")
        else:
            contact["local_extension_id"] = None
            contact["is_statement_contact"] = False
            contact["is_payment_contact"] = False
            contact["is_query_contact"] = False
            contact["preferred_contact_method"] = "email"
            contact["notes"] = None

        merged.append(contact)

    # Append local-only contacts (not matched to any Opera zcontact)
    for ext in local_extensions:
        if ext["id"] not in used_ext_ids:
            merged.append({
                "source": "local",
                "zc_account": ext.get("supplier_code", ""),
                "zc_name": ext.get("name", ""),
                "zc_title": "",
                "zc_email": ext.get("email", ""),
                "zc_phone": ext.get("phone", ""),
                "zc_mobile": "",
                "zc_role": ext.get("role", ""),
                "zc_dept": "",
                "zc_id": "",
                "local_extension_id": ext["id"],
                "is_statement_contact": bool(ext.get("is_statement_contact", 0)),
                "is_payment_contact": bool(ext.get("is_payment_contact", 0)),
                "is_query_contact": bool(ext.get("is_query_contact", 0)),
                "preferred_contact_method": ext.get("preferred_contact_method", "email"),
                "notes": ext.get("notes"),
            })

    return merged


# ============================================================
# Opera SQL SE Endpoints
# ============================================================

@router.get("/api/supplier-contacts/{account}")
async def get_supplier_contacts(account: str):
    """
    Get contacts for a supplier account.

    Reads from Opera zcontacts table (SQL SE) and merges with local
    supplier_contacts_ext extensions for role flags.
    """
    from api.main import sql_connector
    from sql_rag.supplier_statement_db import SupplierStatementDB

    try:
        opera_contacts = []

        # Read Opera zcontacts via sql_connector
        if sql_connector:
            try:
                df = sql_connector.execute_query(f"""
                    SELECT
                        id,
                        RTRIM(zc_account) AS zc_account,
                        RTRIM(zc_contact) AS zc_name,
                        RTRIM(ISNULL(zc_title, '')) AS zc_title,
                        RTRIM(ISNULL(zc_fornam, '')) AS zc_forename,
                        RTRIM(ISNULL(zc_surname, '')) AS zc_surname,
                        RTRIM(ISNULL(zc_pos, '')) AS zc_role,
                        RTRIM(ISNULL(zc_email, '')) AS zc_email,
                        RTRIM(ISNULL(zc_phone, '')) AS zc_phone,
                        RTRIM(ISNULL(zc_mobile, '')) AS zc_mobile,
                        RTRIM(ISNULL(zc_fax, '')) AS zc_fax,
                        RTRIM(ISNULL(zc_module, '')) AS zc_module,
                        RTRIM(ISNULL(zc_attr1, '')) AS zc_attr1,
                        RTRIM(ISNULL(zc_attr2, '')) AS zc_attr2,
                        RTRIM(ISNULL(zc_attr3, '')) AS zc_attr3,
                        RTRIM(ISNULL(zc_attr4, '')) AS zc_attr4,
                        RTRIM(ISNULL(zc_attr5, '')) AS zc_attr5,
                        RTRIM(ISNULL(zc_attr6, '')) AS zc_attr6,
                        CAST(id AS VARCHAR(50)) AS zc_id
                    FROM zcontacts WITH (NOLOCK)
                    WHERE zc_account = '{account}'
                      AND zc_module = 'P'
                    ORDER BY zc_contact
                """)
                if df is not None and hasattr(df, 'to_dict'):
                    opera_contacts = df.to_dict('records')
                elif df is not None:
                    opera_contacts = list(df)
            except Exception as e:
                # zcontacts table may not exist in all Opera installations
                logger.warning(f"Could not read zcontacts for {account}: {e}")

        # Read local extensions
        db = SupplierStatementDB()
        local_extensions = db.get_contacts(account)

        # Merge
        contacts = _merge_contacts(opera_contacts, local_extensions)

        return {
            "success": True,
            "account": account,
            "contacts": contacts,
            "opera_count": len(opera_contacts),
            "local_count": len(local_extensions),
        }

    except Exception as e:
        logger.error(f"Error getting contacts for {account}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.post("/api/supplier-contacts/{account}")
async def create_supplier_contact(account: str, body: ContactCreateRequest):
    """
    Create a local contact extension for a supplier.

    This creates a record in supplier_contacts_ext (local SQLite), not in
    Opera's zcontacts table. Used for automation-specific role flags.
    """
    from sql_rag.supplier_statement_db import SupplierStatementDB

    try:
        db = SupplierStatementDB()

        kwargs = {
            "name": body.name,
            "role": body.role,
            "email": body.email,
            "phone": body.phone,
            "is_statement_contact": 1 if body.is_statement_contact else 0,
            "is_payment_contact": 1 if body.is_payment_contact else 0,
            "is_query_contact": 1 if body.is_query_contact else 0,
            "preferred_contact_method": body.preferred_contact_method,
            "notes": body.notes,
        }
        if body.zcontact_id:
            kwargs["zcontact_id"] = body.zcontact_id

        contact_id = db.upsert_contact(account, contact_id=None, **kwargs)

        # Read back the created contact
        contacts = db.get_contacts(account)
        created = next((c for c in contacts if c["id"] == contact_id), None)

        return {
            "success": True,
            "contact_id": contact_id,
            "contact": created,
        }

    except Exception as e:
        logger.error(f"Error creating contact for {account}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.put("/api/supplier-contacts/{account}/{contact_id}")
async def update_supplier_contact(account: str, contact_id: int, body: ContactUpdateRequest):
    """
    Update an existing local contact extension.

    Only updates fields that are provided (non-None) in the request body.
    """
    from sql_rag.supplier_statement_db import SupplierStatementDB

    try:
        db = SupplierStatementDB()

        # Verify the contact exists and belongs to this account
        existing = db.get_contacts(account)
        match = next((c for c in existing if c["id"] == contact_id), None)
        if not match:
            return {
                "success": False,
                "error": f"Contact {contact_id} not found for account {account}",
            }

        # Build kwargs from non-None fields
        kwargs = {}
        if body.name is not None:
            kwargs["name"] = body.name
        if body.role is not None:
            kwargs["role"] = body.role
        if body.email is not None:
            kwargs["email"] = body.email
        if body.phone is not None:
            kwargs["phone"] = body.phone
        if body.is_statement_contact is not None:
            kwargs["is_statement_contact"] = 1 if body.is_statement_contact else 0
        if body.is_payment_contact is not None:
            kwargs["is_payment_contact"] = 1 if body.is_payment_contact else 0
        if body.is_query_contact is not None:
            kwargs["is_query_contact"] = 1 if body.is_query_contact else 0
        if body.preferred_contact_method is not None:
            kwargs["preferred_contact_method"] = body.preferred_contact_method
        if body.notes is not None:
            kwargs["notes"] = body.notes
        if body.zcontact_id is not None:
            kwargs["zcontact_id"] = body.zcontact_id

        if not kwargs:
            return {"success": False, "error": "No fields to update"}

        db.upsert_contact(account, contact_id=contact_id, **kwargs)

        # Read back updated contact
        contacts = db.get_contacts(account)
        updated = next((c for c in contacts if c["id"] == contact_id), None)

        return {
            "success": True,
            "contact_id": contact_id,
            "contact": updated,
        }

    except Exception as e:
        logger.error(f"Error updating contact {contact_id} for {account}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.delete("/api/supplier-contacts/{account}/{contact_id}")
async def delete_supplier_contact(account: str, contact_id: int):
    """
    Delete a local contact extension.

    This does NOT delete the Opera zcontacts record -- only the local
    automation extension in supplier_contacts_ext.
    """
    from sql_rag.supplier_statement_db import SupplierStatementDB

    try:
        db = SupplierStatementDB()

        # Verify the contact exists and belongs to this account
        existing = db.get_contacts(account)
        match = next((c for c in existing if c["id"] == contact_id), None)
        if not match:
            return {
                "success": False,
                "error": f"Contact {contact_id} not found for account {account}",
            }

        db.delete_contact(contact_id)

        return {
            "success": True,
            "message": f"Contact {contact_id} deleted for account {account}",
        }

    except Exception as e:
        logger.error(f"Error deleting contact {contact_id} for {account}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.put("/api/supplier-contacts/{account}/{contact_id}/roles")
async def update_supplier_contact_roles(account: str, contact_id: int, body: ContactRolesRequest):
    """
    Update just the role flags on a local contact extension.

    This is a convenience endpoint for toggling statement/payment/query
    contact roles without touching other fields.
    """
    from sql_rag.supplier_statement_db import SupplierStatementDB

    try:
        db = SupplierStatementDB()

        # Verify the contact exists and belongs to this account
        existing = db.get_contacts(account)
        match = next((c for c in existing if c["id"] == contact_id), None)
        if not match:
            return {
                "success": False,
                "error": f"Contact {contact_id} not found for account {account}",
            }

        db.upsert_contact(
            account,
            contact_id=contact_id,
            is_statement_contact=1 if body.is_statement_contact else 0,
            is_payment_contact=1 if body.is_payment_contact else 0,
            is_query_contact=1 if body.is_query_contact else 0,
        )

        # Read back updated contact
        contacts = db.get_contacts(account)
        updated = next((c for c in contacts if c["id"] == contact_id), None)

        return {
            "success": True,
            "contact_id": contact_id,
            "contact": updated,
        }

    except Exception as e:
        logger.error(f"Error updating roles for contact {contact_id}, account {account}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# Opera 3 (FoxPro) Endpoint
# ============================================================

def _o3_get_str(record, field, default=''):
    """Get string from Opera 3 record (handles uppercase/lowercase field names)."""
    val = record.get(field.upper(), record.get(field.lower(), record.get(field, default)))
    if val is None:
        return default
    return str(val).strip()


@router.get("/api/opera3/supplier-contacts/{account}")
async def opera3_get_supplier_contacts(
    account: str,
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
):
    """
    Get contacts for a supplier account from Opera 3 (FoxPro).

    Reads zcontacts from Opera 3 DBF files and merges with local
    supplier_contacts_ext extensions.
    """
    from sql_rag.supplier_statement_db import SupplierStatementDB

    try:
        opera_contacts = []

        # Read Opera 3 zcontacts via FoxPro reader
        try:
            from sql_rag.opera3_foxpro import Opera3Reader
            reader = Opera3Reader(data_path)
            records = reader.read_table("zcontacts")

            for rec in records:
                rec_account = _o3_get_str(rec, 'zc_account')
                if rec_account.upper() == account.upper():
                    opera_contacts.append({
                        "zc_account": rec_account,
                        "zc_name": _o3_get_str(rec, 'zc_name'),
                        "zc_title": _o3_get_str(rec, 'zc_title'),
                        "zc_email": _o3_get_str(rec, 'zc_email'),
                        "zc_phone": _o3_get_str(rec, 'zc_phone'),
                        "zc_mobile": _o3_get_str(rec, 'zc_mobile'),
                        "zc_role": _o3_get_str(rec, 'zc_role'),
                        "zc_dept": _o3_get_str(rec, 'zc_dept'),
                        "zc_id": _o3_get_str(rec, 'zc_id'),
                    })

            # Sort by name
            opera_contacts.sort(key=lambda c: c.get("zc_name", ""))

        except Exception as e:
            # zcontacts table may not exist in all Opera 3 installations
            logger.warning(f"Could not read Opera 3 zcontacts for {account}: {e}")

        # Read local extensions
        db = SupplierStatementDB()
        local_extensions = db.get_contacts(account)

        # Merge
        contacts = _merge_contacts(opera_contacts, local_extensions)

        return {
            "success": True,
            "account": account,
            "contacts": contacts,
            "opera_count": len(opera_contacts),
            "local_count": len(local_extensions),
        }

    except Exception as e:
        logger.error(f"Error getting Opera 3 contacts for {account}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# Opera SQL SE Write-Back Endpoints (zcontacts)
# ============================================================

@router.post("/api/supplier-contacts/{account}/opera")
async def create_opera_contact(account: str, body: OperaContactCreateRequest):
    """
    Create a NEW contact in Opera's zcontacts table.

    Gets the next id from the nextid table (required for Opera SQL SE),
    inserts the contact record, logs the change to the audit table,
    and optionally stores security/automation extension fields locally.
    """
    from api.main import sql_connector
    from sql_rag.supplier_statement_db import SupplierStatementDB
    from sqlalchemy import text

    if not sql_connector:
        return {"success": False, "error": "Opera SQL SE is not connected"}

    try:
        new_id = None
        created_contact = None

        with sql_connector.engine.connect() as conn:
            # Step 1: Get and increment nextid for zcontacts
            conn.execute(text(
                "UPDATE nextid WITH (ROWLOCK) SET nextid = nextid + 1, datemodified = GETDATE() "
                "WHERE RTRIM(tablename) = 'zcontacts'"
            ))
            result = conn.execute(text(
                "SELECT nextid - 1 as new_id FROM nextid "
                "WHERE RTRIM(tablename) = 'zcontacts'"
            ))
            row = result.fetchone()
            if not row:
                conn.rollback()
                return {
                    "success": False,
                    "error": "Could not allocate id from nextid table for zcontacts"
                }
            new_id = row[0]

            # Step 2: Insert into zcontacts
            conn.execute(text("""
                INSERT INTO zcontacts WITH (ROWLOCK)
                    (id, zc_module, zc_account, zc_contact, zc_title, zc_fornam,
                     zc_surname, zc_pos, zc_email, zc_phone, zc_mobile, zc_fax,
                     datecreated, datemodified, state)
                VALUES
                    (:id, 'P', :account, :contact, :title, :forename,
                     :surname, :position, :email, :phone, :mobile, :fax,
                     GETDATE(), GETDATE(), 1)
            """), {
                "id": new_id,
                "account": account,
                "contact": body.name or "",
                "title": body.title or "",
                "forename": body.name.split()[0] if body.name and ' ' in body.name else (body.name or ""),
                "surname": ' '.join(body.name.split()[1:]) if body.name and ' ' in body.name else "",
                "position": body.role or "",
                "email": body.email or "",
                "phone": body.phone or "",
                "mobile": body.mobile or "",
                "fax": body.fax or "",
            })

            # Step 3: Read back the created contact
            result = conn.execute(text("""
                SELECT
                    CAST(id AS VARCHAR(50)) AS zc_id,
                    RTRIM(zc_account) AS zc_account,
                    RTRIM(zc_contact) AS zc_name,
                    RTRIM(ISNULL(zc_title, '')) AS zc_title,
                    RTRIM(ISNULL(zc_email, '')) AS zc_email,
                    RTRIM(ISNULL(zc_phone, '')) AS zc_phone,
                    RTRIM(ISNULL(zc_mobile, '')) AS zc_mobile,
                    RTRIM(ISNULL(zc_pos, '')) AS zc_role,
                    RTRIM(ISNULL(zc_module, '')) AS zc_module,
                    RTRIM(ISNULL(zc_fax, '')) AS zc_fax
                FROM zcontacts WITH (NOLOCK)
                WHERE id = :id
            """), {"id": new_id})
            row = result.fetchone()
            if row:
                created_contact = dict(row._mapping)

            conn.commit()

        # Step 4: Log to audit
        db = SupplierStatementDB()
        db.log_supplier_change(
            supplier_code=account,
            field_name="zcontact_created",
            old_value="",
            new_value=f"id={new_id}, name={body.name}",
            changed_by="api"
        )

        # Step 5: Store security/automation extension fields locally if provided
        ext_id = None
        has_security_fields = any([
            body.verified_sender is not None,
            body.verified_by is not None,
            body.verified_date is not None,
            body.authorised_bank_changes is not None,
            body.security_clearance is not None,
            body.verification_phone is not None,
            body.last_verified is not None,
        ])
        if has_security_fields:
            try:
                ext_id = _upsert_security_extensions(db, account, new_id, body)
            except Exception as ext_err:
                logger.warning(
                    f"Opera contact {new_id} created but local extension failed: {ext_err}"
                )

        return {
            "success": True,
            "contact_id": new_id,
            "contact": created_contact,
            "local_extension_id": ext_id,
        }

    except Exception as e:
        logger.error(f"Error creating Opera contact for {account}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@router.put("/api/supplier-contacts/{account}/opera/{contact_id}")
async def update_opera_contact(account: str, contact_id: int, body: OperaContactUpdateRequest):
    """
    Update an existing contact in Opera's zcontacts table.

    Updates only the fields provided in the request body. Logs all
    changes to the audit table. Security/automation fields are stored
    in the local supplier_contacts_ext table.
    """
    from api.main import sql_connector
    from sql_rag.supplier_statement_db import SupplierStatementDB
    from sqlalchemy import text

    if not sql_connector:
        return {"success": False, "error": "Opera SQL SE is not connected"}

    try:
        # Build SET clause dynamically from provided fields
        field_map = {
            "name": ("zc_contact", body.name),
            "title": ("zc_title", body.title),
            "role": ("zc_pos", body.role),
            "email": ("zc_email", body.email),
            "phone": ("zc_phone", body.phone),
            "mobile": ("zc_mobile", body.mobile),
            "fax": ("zc_fax", body.fax),
        }

        set_clauses = []
        params = {"contact_id": contact_id}
        old_values = {}

        with sql_connector.engine.connect() as conn:
            # Read current values for audit logging
            result = conn.execute(text("""
                SELECT
                    RTRIM(ISNULL(zc_contact, '')) AS zc_name,
                    RTRIM(ISNULL(zc_title, '')) AS zc_title,
                    RTRIM(ISNULL(zc_pos, '')) AS zc_role,
                    RTRIM(ISNULL(zc_module, '')) AS zc_module,
                    RTRIM(ISNULL(zc_email, '')) AS zc_email,
                    RTRIM(ISNULL(zc_phone, '')) AS zc_phone,
                    RTRIM(ISNULL(zc_mobile, '')) AS zc_mobile,
                    RTRIM(ISNULL(zc_fax, '')) AS zc_fax
                FROM zcontacts WITH (NOLOCK)
                WHERE id = :contact_id
            """), {"contact_id": contact_id})
            existing = result.fetchone()
            if not existing:
                return {
                    "success": False,
                    "error": f"Opera contact with id {contact_id} not found"
                }
            old_values = dict(existing._mapping)

            # Build update
            for field_key, (col_name, value) in field_map.items():
                if value is not None:
                    set_clauses.append(f"{col_name} = :{field_key}")
                    params[field_key] = value

            if not set_clauses:
                # No Opera fields to update -- check if only security fields
                pass
            else:
                update_sql = (
                    f"UPDATE zcontacts WITH (ROWLOCK) "
                    f"SET {', '.join(set_clauses)} "
                    f"WHERE id = :contact_id"
                )
                conn.execute(text(update_sql), params)
                conn.commit()

            # Read back updated contact
            result = conn.execute(text("""
                SELECT
                    CAST(id AS VARCHAR(50)) AS zc_id,
                    RTRIM(zc_account) AS zc_account,
                    RTRIM(zc_contact) AS zc_name,
                    RTRIM(ISNULL(zc_title, '')) AS zc_title,
                    RTRIM(ISNULL(zc_email, '')) AS zc_email,
                    RTRIM(ISNULL(zc_phone, '')) AS zc_phone,
                    RTRIM(ISNULL(zc_mobile, '')) AS zc_mobile,
                    RTRIM(ISNULL(zc_pos, '')) AS zc_role,
                    RTRIM(ISNULL(zc_module, '')) AS zc_module,
                    RTRIM(ISNULL(zc_fax, '')) AS zc_fax
                FROM zcontacts WITH (NOLOCK)
                WHERE id = :contact_id
            """), {"contact_id": contact_id})
            updated_contact = None
            row = result.fetchone()
            if row:
                updated_contact = dict(row._mapping)

        # Audit log for each changed Opera field
        db = SupplierStatementDB()
        for field_key, (col_name, value) in field_map.items():
            if value is not None:
                old_val = old_values.get(col_name, "")
                if str(old_val).strip() != str(value).strip():
                    db.log_supplier_change(
                        supplier_code=account,
                        field_name=f"zcontact.{col_name}",
                        old_value=str(old_val).strip(),
                        new_value=str(value).strip(),
                        changed_by="api"
                    )

        # Store security/automation extension fields locally if provided
        ext_id = None
        has_security_fields = any([
            body.verified_sender is not None,
            body.verified_by is not None,
            body.verified_date is not None,
            body.authorised_bank_changes is not None,
            body.security_clearance is not None,
            body.verification_phone is not None,
            body.last_verified is not None,
        ])
        if has_security_fields:
            try:
                ext_id = _upsert_security_extensions(db, account, contact_id, body)
            except Exception as ext_err:
                logger.warning(
                    f"Opera contact {contact_id} updated but local extension failed: {ext_err}"
                )

        return {
            "success": True,
            "contact_id": contact_id,
            "contact": updated_contact,
            "local_extension_id": ext_id,
        }

    except Exception as e:
        logger.error(
            f"Error updating Opera contact {contact_id} for {account}: {e}",
            exc_info=True
        )
        return {"success": False, "error": str(e)}


@router.delete("/api/supplier-contacts/{account}/opera/{contact_id}")
async def delete_opera_contact(account: str, contact_id: int):
    """
    Delete a contact from Opera's zcontacts table.

    Logs the deletion to the audit table and removes any associated
    local security extension.
    """
    from api.main import sql_connector
    from sql_rag.supplier_statement_db import SupplierStatementDB
    from sqlalchemy import text

    if not sql_connector:
        return {"success": False, "error": "Opera SQL SE is not connected"}

    try:
        deleted_name = ""

        with sql_connector.engine.connect() as conn:
            # Read current record for audit logging
            result = conn.execute(text("""
                SELECT
                    RTRIM(ISNULL(zc_contact, '')) AS zc_name,
                    RTRIM(ISNULL(zc_email, '')) AS zc_email,
                    RTRIM(zc_account) AS zc_account
                FROM zcontacts WITH (NOLOCK)
                WHERE id = :contact_id
            """), {"contact_id": contact_id})
            existing = result.fetchone()
            if not existing:
                return {
                    "success": False,
                    "error": f"Opera contact with id {contact_id} not found"
                }

            existing_data = dict(existing._mapping)
            deleted_name = existing_data.get("zc_name", "")

            # Verify it belongs to the correct account
            if existing_data.get("zc_account", "").strip().upper() != account.strip().upper():
                return {
                    "success": False,
                    "error": f"Contact {contact_id} does not belong to account {account}"
                }

            # Delete from Opera
            conn.execute(text(
                "DELETE FROM zcontacts WITH (ROWLOCK) WHERE id = :contact_id"
            ), {"contact_id": contact_id})
            conn.commit()

        # Audit log
        db = SupplierStatementDB()
        db.log_supplier_change(
            supplier_code=account,
            field_name="zcontact_deleted",
            old_value=f"id={contact_id}, name={deleted_name}",
            new_value="",
            changed_by="api"
        )

        # Clean up any local extension linked to this Opera contact
        try:
            conn_local = db._get_connection()
            cursor = conn_local.cursor()
            cursor.execute(
                "DELETE FROM supplier_contacts_ext "
                "WHERE supplier_code = ? AND zcontact_id = ?",
                (account, str(contact_id))
            )
            conn_local.commit()
            conn_local.close()
        except Exception as ext_err:
            logger.warning(
                f"Opera contact {contact_id} deleted but local extension cleanup failed: {ext_err}"
            )

        return {
            "success": True,
            "message": f"Opera contact {contact_id} ({deleted_name}) deleted from account {account}",
        }

    except Exception as e:
        logger.error(
            f"Error deleting Opera contact {contact_id} for {account}: {e}",
            exc_info=True
        )
        return {"success": False, "error": str(e)}
