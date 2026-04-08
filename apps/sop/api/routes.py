"""
SOP Batch Processing API — replicate Opera's batch document progression.
Evaluation build — read operations safe, write operations replicate exact Opera patterns.
"""
from fastapi import APIRouter, Body, Query, HTTPException
from typing import Optional, List, Dict
from datetime import date, datetime
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

# Per-request globals (synced from api/main.py)
sql_connector = None


def _get_sql():
    """Get the SQL connector — synced per-request from main app."""
    from api.main import sql_connector as main_sql
    return main_sql


# ============================================================
# Read-only endpoints (safe for evaluation)
# ============================================================

@router.get("/api/sop/config")
async def get_sop_config():
    """Get SOP configuration from iparm — shows available progressions."""
    sql = _get_sql()
    if not sql:
        return {"success": False, "error": "Database not connected"}

    try:
        from sql_rag.sop_batch_processor import SOPBatchProcessor
        processor = SOPBatchProcessor(sql)
        config = processor.load_config()
        progressions = processor.get_available_progressions()

        return {
            "success": True,
            "config": {
                "delivery_enabled": config.delivery_enabled,
                "picking_enabled": config.picking_enabled,
                "stock_update_at": config.stock_update_at,
                "can_order_to_invoice": config.can_order_to_invoice,
                "can_delivery_to_invoice": config.can_delivery_to_invoice,
                "address_refresh": config.address_refresh,
                "sequences": {
                    "next_quote": config.next_quote,
                    "next_proforma": config.next_proforma,
                    "next_order": config.next_order,
                    "next_delivery": config.next_delivery,
                    "next_invoice": config.next_invoice,
                    "next_credit": config.next_credit,
                },
            },
            "progressions": progressions,
        }
    except Exception as e:
        logger.error(f"Error loading SOP config: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/sop/documents")
async def list_sop_documents(
    status: str = Query(..., description="Document status: Q, P, O, D, U, I"),
    due_date_to: Optional[str] = Query(None, description="Filter due date up to (YYYY-MM-DD)"),
    number_from: Optional[str] = Query(None, description="Reference number from"),
    number_to: Optional[str] = Query(None, description="Reference number to"),
    priority_from: Optional[int] = Query(0, description="Priority from"),
    priority_to: Optional[int] = Query(9, description="Priority to"),
):
    """List documents at a given status, ready for batch progression."""
    sql = _get_sql()
    if not sql:
        return {"success": False, "error": "Database not connected"}

    try:
        from sql_rag.sop_batch_processor import SOPBatchProcessor
        processor = SOPBatchProcessor(sql)

        filters = {
            'priority_from': priority_from,
            'priority_to': priority_to,
        }
        if due_date_to:
            filters['due_date_to'] = due_date_to
        if number_from:
            filters['number_from'] = number_from
        if number_to:
            filters['number_to'] = number_to

        docs = processor.list_documents(status, filters)

        # Summary
        total_ex_vat = sum(d['ex_vat'] for d in docs)
        total_vat = sum(d['vat'] for d in docs)

        return {
            "success": True,
            "documents": docs,
            "count": len(docs),
            "summary": {
                "total_ex_vat": total_ex_vat,
                "total_vat": total_vat,
                "total": total_ex_vat + total_vat,
            },
        }
    except Exception as e:
        logger.error(f"Error listing SOP documents: {e}")
        return {"success": False, "error": str(e)}


@router.get("/api/sop/document/{doc_id}/lines")
async def get_document_lines(doc_id: int):
    """Get line items for a specific document."""
    sql = _get_sql()
    if not sql:
        return {"success": False, "error": "Database not connected"}

    try:
        # Get doc number from id
        doc_df = sql.execute_query(f"SELECT ih_doc FROM ihead WITH (NOLOCK) WHERE id = {doc_id}")
        if doc_df is None or doc_df.empty:
            return {"success": False, "error": "Document not found"}

        doc_number = doc_df.iloc[0]['ih_doc'].strip()

        lines_df = sql.execute_query(f"""
            SELECT it_recno, it_stock, it_desc, it_quan, it_price, it_disc,
                   it_exvat, it_vatval, it_lineval, it_vat, it_vatpct,
                   it_cwcode, it_anal, it_cost, it_memo, it_status
            FROM itran WITH (NOLOCK)
            WHERE it_doc = '{doc_number}'
            ORDER BY it_recno
        """)

        if lines_df is None or lines_df.empty:
            return {"success": True, "lines": [], "count": 0}

        lines = []
        for _, r in lines_df.iterrows():
            lines.append({
                'line_no': int(r['it_recno'] or 0),
                'stock_code': str(r['it_stock'] or '').strip(),
                'description': str(r['it_desc'] or '').strip(),
                'quantity': float(r['it_quan'] or 0),
                'price': float(r['it_price'] or 0),
                'discount': float(r['it_disc'] or 0),
                'ex_vat': float(r['it_exvat'] or 0),
                'vat': float(r['it_vatval'] or 0),
                'line_total': float(r['it_lineval'] or 0),
                'vat_code': str(r['it_vat'] or '').strip(),
                'vat_rate': float(r['it_vatpct'] or 0),
                'warehouse': str(r['it_cwcode'] or '').strip(),
                'analysis': str(r['it_anal'] or '').strip(),
                'cost': float(r['it_cost'] or 0),
                'memo': str(r['it_memo'] or '').strip(),
                'status': str(r['it_status'] or '').strip(),
            })

        return {"success": True, "lines": lines, "count": len(lines)}
    except Exception as e:
        logger.error(f"Error getting document lines: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Write endpoints (batch progression)
# ============================================================

@router.post("/api/sop/progress")
async def progress_documents(
    doc_ids: List[int] = Body(..., description="List of ihead.id values to progress"),
    from_status: str = Body(..., description="Current status (Q, P, O, D)"),
    to_status: str = Body(..., description="Target status (P, O, D, I)"),
    posting_date: Optional[str] = Body(None, description="Posting date YYYY-MM-DD (default today)"),
):
    """
    Progress selected documents to the next stage.
    Replicates exact Opera batch processing behaviour from snapshots.
    """
    sql = _get_sql()
    if not sql:
        return {"success": False, "error": "Database not connected"}

    try:
        from sql_rag.sop_batch_processor import SOPBatchProcessor
        processor = SOPBatchProcessor(sql)

        parsed_date = date.today()
        if posting_date:
            try:
                parsed_date = datetime.strptime(posting_date, '%Y-%m-%d').date()
            except ValueError:
                return {"success": False, "error": "Invalid date format. Use YYYY-MM-DD"}

        results = processor.progress_documents(doc_ids, from_status, to_status, parsed_date)

        succeeded = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        return {
            "success": len(failed) == 0,
            "total": len(results),
            "succeeded": len(succeeded),
            "failed": len(failed),
            "results": [
                {
                    "doc_number": r.doc_number,
                    "from_status": r.from_status,
                    "to_status": r.to_status,
                    "assigned_number": r.assigned_number,
                    "success": r.success,
                    "error": r.error,
                    "tables_updated": r.tables_updated,
                }
                for r in results
            ],
        }
    except Exception as e:
        logger.error(f"Error progressing documents: {e}")
        return {"success": False, "error": str(e)}
