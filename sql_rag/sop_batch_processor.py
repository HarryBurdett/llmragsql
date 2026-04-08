"""
SOP Batch Processor — replicates Opera's Sales Order Processing batch progression.

Progresses documents through stages: Quote → Proforma → Order → Delivery → Invoice.
Progression chain is parameter-driven from iparm — not hardcoded.

All write patterns derived from before/after transaction snapshots taken from Opera.
Each transition replicates exactly what Opera does — same tables, same fields, same values.
"""
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# Document status codes and their progression order
DOC_STATUSES = {
    'Q': 'Quote',
    'P': 'Proforma',
    'O': 'Order',
    'D': 'Delivery',
    'U': 'Despatched',
    'I': 'Invoice',
    'C': 'Credit',
}


@dataclass
class SOPConfig:
    """SOP parameters loaded from iparm — controls the progression chain."""
    delivery_enabled: bool = True       # ip_delivry
    picking_enabled: bool = False        # ip_picking
    stock_update_at: str = 'D'          # ip_updst: O/D/I
    can_order_to_delivery: bool = False  # ip_cnor2dl
    can_order_to_invoice: bool = False   # ip_cnor2in
    can_delivery_to_invoice: bool = True # ip_cndl2in
    address_refresh: bool = True         # ip_adar
    immediate_print: bool = True         # ip_immedpr

    # Current sequence counters
    next_doc: str = ''
    next_quote: str = ''
    next_proforma: str = ''
    next_order: str = ''
    next_delivery: str = ''
    next_invoice: str = ''
    next_credit: str = ''


@dataclass
class ProgressionResult:
    """Result of progressing a single document."""
    success: bool
    doc_number: str = ''
    from_status: str = ''
    to_status: str = ''
    assigned_number: str = ''  # e.g. PRO00010, ORD01098
    error: str = ''
    tables_updated: List[str] = None

    def __post_init__(self):
        if self.tables_updated is None:
            self.tables_updated = []


class SOPBatchProcessor:
    """
    Batch processor for SOP document progression.
    Reads configuration from iparm and applies the correct writes for each transition.
    """

    def __init__(self, sql_connector):
        self.sql = sql_connector
        self._config: Optional[SOPConfig] = None

    def load_config(self) -> SOPConfig:
        """Load SOP configuration from iparm."""
        df = self.sql.execute_query("SELECT * FROM iparm WITH (NOLOCK)")
        if df is None or df.empty:
            raise ValueError("iparm table not found or empty")

        r = df.iloc[0]
        self._config = SOPConfig(
            delivery_enabled=str(r.get('ip_delivry', 'Y')).strip().upper() == 'Y',
            picking_enabled=str(r.get('ip_picking', 'N')).strip().upper() == 'Y',
            stock_update_at=str(r.get('ip_updst', 'D')).strip().upper(),
            can_order_to_delivery=str(r.get('ip_cnor2dl', 'N')).strip().upper() == 'Y',
            can_order_to_invoice=str(r.get('ip_cnor2in', 'N')).strip().upper() == 'Y',
            can_delivery_to_invoice=str(r.get('ip_cndl2in', 'Y')).strip().upper() == 'Y',
            address_refresh=str(r.get('ip_adar', 'Y')).strip().upper() == 'Y',
            immediate_print=str(r.get('ip_immedpr', 'Y')).strip().upper() == 'Y',
            next_doc=str(r.get('ip_docno', '')).strip(),
            next_quote=str(r.get('ip_quotno', '')).strip(),
            next_proforma=str(r.get('ip_profno', '')).strip(),
            next_order=str(r.get('ip_orderno', '')).strip(),
            next_delivery=str(r.get('ip_deliv', '')).strip(),
            next_invoice=str(r.get('ip_invno', '')).strip(),
            next_credit=str(r.get('ip_credno', '')).strip(),
        )
        return self._config

    @property
    def config(self) -> SOPConfig:
        if self._config is None:
            self.load_config()
        return self._config

    def get_available_progressions(self) -> List[Dict]:
        """Return which progressions are available based on iparm settings."""
        cfg = self.config
        progressions = [
            {'from': 'Q', 'from_label': 'Quote', 'to': 'P', 'to_label': 'Proforma', 'available': True},
        ]

        if cfg.delivery_enabled:
            progressions.append({'from': 'P', 'from_label': 'Proforma', 'to': 'O', 'to_label': 'Order', 'available': True})
            progressions.append({'from': 'O', 'from_label': 'Order', 'to': 'D', 'to_label': 'Delivery', 'available': True})
            progressions.append({'from': 'D', 'from_label': 'Delivery', 'to': 'I', 'to_label': 'Invoice', 'available': cfg.can_delivery_to_invoice})
        else:
            progressions.append({'from': 'P', 'from_label': 'Proforma', 'to': 'O', 'to_label': 'Order', 'available': True})
            progressions.append({'from': 'O', 'from_label': 'Order', 'to': 'I', 'to_label': 'Invoice', 'available': True})

        return [p for p in progressions if p['available']]

    def list_documents(self, status: str, filters: Dict = None) -> List[Dict]:
        """
        List documents at a given status, ready for progression.

        Args:
            status: Document status code (Q, P, O, D, U)
            filters: Optional filters — quote_from, quote_to, due_date_to, priority_from, priority_to
        """
        filters = filters or {}

        where = ["ih_docstat = :status"]
        params = {'status': status}

        if filters.get('due_date_to'):
            where.append("ih_due <= :due_to")
            params['due_to'] = filters['due_date_to']

        if filters.get('priority_from') is not None:
            where.append("ih_priorty >= :pri_from")
            params['pri_from'] = filters['priority_from']

        if filters.get('priority_to') is not None:
            where.append("ih_priorty <= :pri_to")
            params['pri_to'] = filters['priority_to']

        # Quote/order number range filter
        if status == 'Q':
            num_field = 'ih_quotat'
        elif status == 'P':
            num_field = 'ih_proform'
        elif status == 'O':
            num_field = 'ih_sorder'
        elif status in ('D', 'U'):
            num_field = 'ih_deliv'
        else:
            num_field = 'ih_doc'

        if filters.get('number_from'):
            where.append(f"{num_field} >= :num_from")
            params['num_from'] = filters['number_from']
        if filters.get('number_to'):
            where.append(f"{num_field} <= :num_to")
            params['num_to'] = filters['number_to']

        where_clause = ' AND '.join(where)

        df = self.sql.execute_query(f"""
            SELECT h.id, h.ih_doc, h.ih_quotat, h.ih_proform, h.ih_sorder, h.ih_deliv,
                   h.ih_invoice, h.ih_account, h.ih_name, h.ih_custref, h.ih_date,
                   h.ih_due, h.ih_exvat, h.ih_vat, h.ih_loc, h.ih_route, h.ih_docstat,
                   h.ih_priorty,
                   (SELECT COUNT(*) FROM itran WITH (NOLOCK) WHERE it_doc = h.ih_doc) as line_count
            FROM ihead h WITH (NOLOCK)
            WHERE {where_clause}
            ORDER BY h.ih_doc
        """, params=params)

        if df is None or df.empty:
            return []

        docs = []
        for _, r in df.iterrows():
            total = (r['ih_exvat'] or 0) + (r['ih_vat'] or 0)
            docs.append({
                'id': int(r['id']),
                'doc': str(r['ih_doc']).strip(),
                'quote': str(r['ih_quotat'] or '').strip(),
                'proforma': str(r['ih_proform'] or '').strip(),
                'order': str(r['ih_sorder'] or '').strip(),
                'delivery': str(r['ih_deliv'] or '').strip(),
                'invoice': str(r['ih_invoice'] or '').strip(),
                'account': str(r['ih_account']).strip(),
                'name': str(r['ih_name']).strip(),
                'cust_ref': str(r['ih_custref'] or '').strip(),
                'date': r['ih_date'].isoformat()[:10] if hasattr(r['ih_date'], 'isoformat') else str(r['ih_date'])[:10] if r['ih_date'] else '',
                'due_date': r['ih_due'].isoformat()[:10] if hasattr(r['ih_due'], 'isoformat') else str(r['ih_due'])[:10] if r['ih_due'] else '',
                'ex_vat': float(r['ih_exvat'] or 0),
                'vat': float(r['ih_vat'] or 0),
                'total': total,
                'warehouse': str(r['ih_loc'] or '').strip(),
                'route': str(r['ih_route'] or '').strip(),
                'status': str(r['ih_docstat']).strip(),
                'priority': int(r['ih_priorty'] or 1),
                'line_count': int(r['line_count'] or 0),
            })
        return docs

    def progress_documents(self, doc_ids: List[int], from_status: str, to_status: str,
                           posting_date: date = None) -> List[ProgressionResult]:
        """
        Progress a batch of documents from one status to the next.

        Args:
            doc_ids: List of ihead.id values to progress
            from_status: Current status (Q, P, O, D)
            to_status: Target status (P, O, D, I)
            posting_date: Date for the progression (defaults to today)
        """
        if posting_date is None:
            posting_date = date.today()

        transition = f"{from_status}_{to_status}"
        handler = {
            'Q_P': self._progress_quote_to_proforma,
            'P_O': self._progress_proforma_to_order,
        }.get(transition)

        if not handler:
            return [ProgressionResult(
                success=False,
                error=f"Progression {DOC_STATUSES.get(from_status, from_status)} → {DOC_STATUSES.get(to_status, to_status)} not yet implemented"
            )]

        results = []
        for doc_id in doc_ids:
            try:
                result = handler(doc_id, posting_date)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to progress doc id {doc_id}: {e}")
                results.append(ProgressionResult(
                    success=False,
                    doc_number=str(doc_id),
                    from_status=from_status,
                    to_status=to_status,
                    error=str(e),
                ))
        return results

    # ================================================================
    # Stage transitions — each replicates exact Opera snapshot pattern
    # ================================================================

    def _progress_quote_to_proforma(self, ihead_id: int, posting_date: date) -> ProgressionResult:
        """
        Quote → Proforma (Q → P)
        Snapshot: sop_batch_process_a_quote_to_proforma_20260408_235230.json

        Tables affected:
        - ihead: UPDATE ih_docstat Q→P, ih_proform, ih_prodate, ih_revpro, ih_prtstat, ih_fcval, ih_fcvat, address refresh
        - iparm: UPDATE ip_profno increment
        - zpool: INSERT print spool entry
        - nextid: UPDATE for zpool
        """
        from sqlalchemy import text

        with self.sql.engine.begin() as conn:
            # Get the document
            result = conn.execute(text(
                "SELECT ih_doc, ih_account, ih_docstat, ih_quotat FROM ihead WITH (NOLOCK) WHERE id = :id"
            ), {'id': ihead_id})
            doc = result.fetchone()
            if not doc:
                return ProgressionResult(success=False, error=f"Document id {ihead_id} not found")
            if doc[2].strip() != 'Q':
                return ProgressionResult(success=False, doc_number=doc[0].strip(),
                                         error=f"Document is status '{doc[2].strip()}', expected 'Q'")

            doc_number = doc[0].strip()
            account = doc[1].strip()
            quote_number = doc[3].strip()

            # Get next proforma number from iparm (with lock)
            result = conn.execute(text(
                "SELECT ip_profno FROM iparm WITH (UPDLOCK, ROWLOCK)"
            ))
            proforma_no = result.fetchone()[0].strip()

            # Increment proforma counter
            next_proforma = self._increment_reference(proforma_no)
            conn.execute(text(
                "UPDATE iparm WITH (ROWLOCK) SET ip_profno = :next, datemodified = GETDATE()"
            ), {'next': next_proforma})

            # Address refresh from customer master (if ip_adar = Y)
            addr_updates = ""
            if self.config.address_refresh:
                addr_result = conn.execute(text(
                    "SELECT sn_addr1, sn_addr2, sn_addr3, sn_addr4 FROM sname WITH (NOLOCK) WHERE RTRIM(sn_account) = :acct"
                ), {'acct': account})
                addr = addr_result.fetchone()
                if addr:
                    addr_updates = ", ih_addr1 = :a1, ih_addr2 = :a2, ih_addr3 = :a3, ih_addr4 = :a4"

            # Update ihead
            sql_str = f"""
                UPDATE ihead WITH (ROWLOCK)
                SET ih_docstat = 'P',
                    ih_proform = :proforma,
                    ih_prodate = :prodate,
                    ih_revpro = 'A',
                    ih_prtstat = 'P',
                    ih_fcval = '0',
                    ih_fcvat = '0',
                    datemodified = GETDATE()
                    {addr_updates}
                WHERE id = :id
            """
            params = {
                'proforma': proforma_no,
                'prodate': posting_date,
                'id': ihead_id,
            }
            if addr_updates and addr:
                params.update({
                    'a1': addr[0], 'a2': addr[1], 'a3': addr[2], 'a4': addr[3],
                })
            conn.execute(text(sql_str), params)

            # Insert zpool (print spool) — get nextid for zpool
            zpool_id = self._get_next_id(conn, 'zpool')
            conn.execute(text("""
                INSERT INTO zpool (id, sp_file, sp_desc, sp_ctime, sp_cdate, sp_cby,
                                   sp_printer, sp_repwide, sp_rephite, sp_platfrm,
                                   datecreated, datemodified, state)
                VALUES (:id, 'PROFORMA', :desc, :ctime, :cdate, 'IMPORT',
                        'PDF:', 0, 0, '32BIT', GETDATE(), GETDATE(), 1)
            """), {
                'id': zpool_id,
                'desc': f'Proforma {proforma_no}',
                'ctime': datetime.now().strftime('%H:%M'),
                'cdate': datetime.now().date(),
            })

        logger.info(f"Progressed {doc_number} ({quote_number}) from Quote to Proforma {proforma_no}")
        return ProgressionResult(
            success=True,
            doc_number=doc_number,
            from_status='Q',
            to_status='P',
            assigned_number=proforma_no,
            tables_updated=['ihead', 'iparm', 'zpool', 'nextid'],
        )

    def _progress_proforma_to_order(self, ihead_id: int, posting_date: date) -> ProgressionResult:
        """
        Proforma → Order (P → O)
        Snapshot: sop_batch_processing_20260408_233206.json

        Tables affected:
        - ihead: UPDATE ih_docstat P→O, ih_sorder, ih_orddate, ih_revord, ih_fcval, ih_fcvat
        - itran: UPDATE it_dteallc (allocation date)
        - iparm: UPDATE ip_orderno increment
        - sname: UPDATE sn_ordrbal (add order value)
        - cname: UPDATE cn_saleord = 1 (stock items have sales order)
        - cstwh: UPDATE cs_saleord = 1, cs_lastiss (stock warehouse)
        - zpool: INSERT print spool entry
        - nextid: UPDATE for zpool
        """
        from sqlalchemy import text

        with self.sql.engine.begin() as conn:
            # Get the document
            result = conn.execute(text(
                "SELECT ih_doc, ih_account, ih_docstat, ih_proform, ih_exvat, ih_vat FROM ihead WITH (NOLOCK) WHERE id = :id"
            ), {'id': ihead_id})
            doc = result.fetchone()
            if not doc:
                return ProgressionResult(success=False, error=f"Document id {ihead_id} not found")
            if doc[2].strip() != 'P':
                return ProgressionResult(success=False, doc_number=doc[0].strip(),
                                         error=f"Document is status '{doc[2].strip()}', expected 'P'")

            doc_number = doc[0].strip()
            account = doc[1].strip()
            proforma_number = doc[3].strip() if doc[3] else ''
            order_value = float(doc[4] or 0) + float(doc[5] or 0)

            # Get next order number from iparm (with lock)
            result = conn.execute(text(
                "SELECT ip_orderno FROM iparm WITH (UPDLOCK, ROWLOCK)"
            ))
            order_no = result.fetchone()[0].strip()

            # Increment order counter
            next_order = self._increment_reference(order_no)
            conn.execute(text(
                "UPDATE iparm WITH (ROWLOCK) SET ip_orderno = :next, datemodified = GETDATE()"
            ), {'next': next_order})

            # Update ihead — progress P to O
            conn.execute(text("""
                UPDATE ihead WITH (ROWLOCK)
                SET ih_docstat = 'O',
                    ih_sorder = :order_no,
                    ih_orddate = :orddate,
                    ih_revord = 'A',
                    ih_fcval = '0',
                    ih_fcvat = '0',
                    datemodified = GETDATE()
                WHERE id = :id
            """), {
                'order_no': order_no,
                'orddate': posting_date,
                'id': ihead_id,
            })

            # Update itran — set allocation date on all lines for this document
            conn.execute(text("""
                UPDATE itran WITH (ROWLOCK)
                SET it_dteallc = :allocdate, datemodified = GETDATE()
                WHERE it_doc = :doc
            """), {'allocdate': posting_date, 'doc': doc_number})

            # Update sname — add to order balance
            conn.execute(text("""
                UPDATE sname WITH (ROWLOCK)
                SET sn_ordrbal = sn_ordrbal + :val, datemodified = GETDATE()
                WHERE RTRIM(sn_account) = :acct
            """), {'val': order_value, 'acct': account})

            # Update cname and cstwh for stock items on this order
            # Get stock codes from itran lines
            stock_result = conn.execute(text("""
                SELECT DISTINCT it_stock, it_cwcode FROM itran WITH (NOLOCK)
                WHERE it_doc = :doc AND RTRIM(it_stock) != ''
            """), {'doc': doc_number})
            stock_items = stock_result.fetchall()

            for item in stock_items:
                stock_code = item[0].strip() if item[0] else ''
                warehouse = item[1].strip() if item[1] else ''
                if stock_code:
                    # Update cname (stock item master) — set sales order flag
                    conn.execute(text("""
                        UPDATE cname WITH (ROWLOCK)
                        SET cn_saleord = 1, datemodified = GETDATE()
                        WHERE RTRIM(cn_ref) = :stock
                    """), {'stock': stock_code})

                    # Update cstwh (stock warehouse) — set sales order flag and last issue date
                    if warehouse:
                        conn.execute(text("""
                            UPDATE cstwh WITH (ROWLOCK)
                            SET cs_saleord = 1, cs_lastiss = :issdate, datemodified = GETDATE()
                            WHERE RTRIM(cs_stock) = :stock AND RTRIM(cs_whouse) = :wh
                        """), {'stock': stock_code, 'wh': warehouse, 'issdate': posting_date})

            # Insert zpool (print spool)
            zpool_id = self._get_next_id(conn, 'zpool')
            conn.execute(text("""
                INSERT INTO zpool (id, sp_file, sp_desc, sp_ctime, sp_cdate, sp_cby,
                                   sp_printer, sp_repwide, sp_rephite, sp_platfrm,
                                   datecreated, datemodified, state)
                VALUES (:id, 'ORDER', :desc, :ctime, :cdate, 'IMPORT',
                        'PDF:', 0, 0, '32BIT', GETDATE(), GETDATE(), 1)
            """), {
                'id': zpool_id,
                'desc': f'Order {order_no}',
                'ctime': datetime.now().strftime('%H:%M'),
                'cdate': datetime.now().date(),
            })

        logger.info(f"Progressed {doc_number} from Proforma to Order {order_no}")
        return ProgressionResult(
            success=True,
            doc_number=doc_number,
            from_status='P',
            to_status='O',
            assigned_number=order_no,
            tables_updated=['ihead', 'itran', 'iparm', 'sname', 'cname', 'cstwh', 'zpool', 'nextid'],
        )

    # ================================================================
    # Helpers
    # ================================================================

    @staticmethod
    def _increment_reference(ref: str) -> str:
        """Increment a reference number like PRO00010 → PRO00011."""
        prefix = ''
        num_str = ''
        for i, c in enumerate(ref):
            if c.isdigit():
                prefix = ref[:i]
                num_str = ref[i:]
                break
        if not num_str:
            return ref  # Can't parse

        num = int(num_str) + 1
        return f"{prefix}{str(num).zfill(len(num_str))}"

    @staticmethod
    def _get_next_id(conn, table_name: str) -> int:
        """Get and increment the next ID from the nextid table."""
        from sqlalchemy import text
        result = conn.execute(text("""
            SELECT id, nextid FROM nextid WITH (UPDLOCK, ROWLOCK)
            WHERE RTRIM(tablename) = :tbl
        """), {'tbl': table_name})
        row = result.fetchone()
        if row:
            new_id = int(row[1]) + 1
            conn.execute(text("""
                UPDATE nextid WITH (ROWLOCK) SET nextid = :new_id, datemodified = GETDATE()
                WHERE id = :id
            """), {'new_id': new_id, 'id': row[0]})
            return new_id
        else:
            # Fallback: MAX(id) + 1
            result = conn.execute(text(f"SELECT MAX(id) FROM [{table_name}] WITH (NOLOCK)"))
            max_id = result.fetchone()[0] or 0
            return int(max_id) + 1
