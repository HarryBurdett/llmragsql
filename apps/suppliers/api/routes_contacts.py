"""
Supplier Contact Management API routes.

Provides endpoints for reading Opera zcontacts data and managing local
contact extensions (role flags, preferred contact method, notes).

Works with both Opera SQL SE and Opera 3 (FoxPro) backends.
Local extensions are stored in the supplier_contacts_ext SQLite table
via SupplierStatementDB.
"""

import logging
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
                        RTRIM(zc_account) AS zc_account,
                        RTRIM(zc_name) AS zc_name,
                        RTRIM(ISNULL(zc_title, '')) AS zc_title,
                        RTRIM(ISNULL(zc_email, '')) AS zc_email,
                        RTRIM(ISNULL(zc_phone, '')) AS zc_phone,
                        RTRIM(ISNULL(zc_mobile, '')) AS zc_mobile,
                        RTRIM(ISNULL(zc_role, '')) AS zc_role,
                        RTRIM(ISNULL(zc_dept, '')) AS zc_dept,
                        CAST(zc_id AS VARCHAR(50)) AS zc_id
                    FROM zcontacts WITH (NOLOCK)
                    WHERE zc_account = '{account}'
                    ORDER BY zc_name
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
