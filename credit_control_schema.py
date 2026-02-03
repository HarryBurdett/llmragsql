"""
Credit Control Schema Map
Defines all tables, columns, and pre-built SQL queries for credit control
"""

# Table definitions with business-friendly names
TABLES = {
    "customers": {
        "table": "sname",
        "description": "Customer master data",
        "columns": {
            "sn_account": "Account Code",
            "sn_name": "Customer Name",
            "sn_addr1": "Address Line 1",
            "sn_addr2": "Address Line 2",
            "sn_addr3": "Town",
            "sn_addr4": "County",
            "sn_pstcode": "Postcode",
            "sn_teleno": "Phone",
            "sn_email": "Email",
            "sn_contact": "Contact Name",
            "sn_currbal": "Current Balance",
            "sn_crlim": "Credit Limit",
            "sn_ordrbal": "Order Balance",
            "sn_stop": "Account Stopped",
            "sn_lastinv": "Last Invoice Date",
            "sn_lastrec": "Last Receipt Date",
            "sn_trnover": "Turnover",
        }
    },
    "sales_transactions": {
        "table": "stran",
        "description": "Sales ledger transactions (invoices, credits, receipts)",
        "columns": {
            "st_account": "Account Code",
            "st_trdate": "Transaction Date",
            "st_trref": "Our Reference",
            "st_custref": "Customer Reference",
            "st_trtype": "Type (I=Invoice, C=Credit, R=Receipt)",
            "st_trvalue": "Gross Value",
            "st_vatval": "VAT Value",
            "st_trbal": "Outstanding Balance",
            "st_dueday": "Due Date",
        }
    },
    "payment_allocations": {
        "table": "salloc",
        "description": "How payments are allocated to invoices",
        "columns": {
            "al_account": "Account Code",
            "al_date": "Allocation Date",
            "al_ref1": "Document Reference",
            "al_ref2": "Allocated To",
            "al_type": "Type (R=Receipt, I=Invoice)",
            "al_val": "Allocated Amount",
        }
    },
    "suppliers": {
        "table": "pname",
        "description": "Supplier master data",
        "columns": {
            "pn_account": "Supplier Code",
            "pn_name": "Supplier Name",
            "pn_currbal": "Balance Owed",
            "pn_teleno": "Phone",
            "pn_email": "Email",
        }
    },
    "purchase_transactions": {
        "table": "ptran",
        "description": "Purchase ledger transactions",
        "columns": {
            "pt_account": "Supplier Code",
            "pt_trdate": "Transaction Date",
            "pt_trref": "Our Reference",
            "pt_trvalue": "Value",
            "pt_trbal": "Outstanding",
        }
    },
    "bank_accounts": {
        "table": "nbank",
        "description": "Bank account details",
        "columns": {
            "nk_acnt": "Account Code",
            "nk_desc": "Bank Name",
            "nk_sort": "Sort Code",
            "nk_number": "Account Number",
        }
    }
}

# Pre-built SQL queries for common credit control scenarios
CREDIT_CONTROL_QUERIES = {
    "all_customers": {
        "description": "All customer master data",
        "sql": """
            SELECT sn_account AS account_code, sn_name AS customer_name,
                   sn_addr1 AS address, sn_pstcode AS postcode,
                   sn_teleno AS phone, sn_email AS email, sn_contact AS contact,
                   sn_currbal AS current_balance, sn_crlim AS credit_limit,
                   sn_stop AS account_stopped, sn_lastinv AS last_invoice,
                   sn_lastrec AS last_payment
            FROM sname
        """
    },
    "customers_over_credit_limit": {
        "description": "Customers who have exceeded their credit limit",
        "sql": """
            SELECT sn_account AS account_code, sn_name AS customer_name,
                   sn_teleno AS phone, sn_email AS email, sn_contact AS contact,
                   sn_currbal AS current_balance, sn_crlim AS credit_limit,
                   (sn_currbal - sn_crlim) AS amount_over_limit,
                   sn_stop AS account_stopped
            FROM sname
            WHERE sn_currbal > sn_crlim AND sn_crlim > 0
            ORDER BY (sn_currbal - sn_crlim) DESC
        """
    },
    "customers_on_stop": {
        "description": "Customers with accounts on stop",
        "sql": """
            SELECT sn_account AS account_code, sn_name AS customer_name,
                   sn_teleno AS phone, sn_contact AS contact,
                   sn_currbal AS current_balance, sn_crlim AS credit_limit
            FROM sname
            WHERE sn_stop = 1
        """
    },
    "outstanding_invoices": {
        "description": "All unpaid invoices with customer details",
        "sql": """
            SELECT st.st_account AS account_code, sn.sn_name AS customer_name,
                   sn.sn_teleno AS phone, sn.sn_email AS email,
                   st.st_trdate AS invoice_date, st.st_trref AS invoice_number,
                   st.st_trvalue AS invoice_amount, st.st_trbal AS outstanding,
                   st.st_dueday AS due_date
            FROM stran st
            JOIN sname sn ON st.st_account = sn.sn_account
            WHERE st.st_trtype = 'I' AND st.st_trbal > 0
            ORDER BY st.st_trbal DESC
        """
    },
    "overdue_invoices": {
        "description": "Invoices past their due date",
        "sql": """
            SELECT st.st_account AS account_code, sn.sn_name AS customer_name,
                   sn.sn_teleno AS phone, sn.sn_contact AS contact,
                   st.st_trref AS invoice_number, st.st_trvalue AS invoice_amount,
                   st.st_trbal AS outstanding, st.st_dueday AS due_date,
                   DATEDIFF(day, st.st_dueday, GETDATE()) AS days_overdue
            FROM stran st
            JOIN sname sn ON st.st_account = sn.sn_account
            WHERE st.st_trtype = 'I' AND st.st_trbal > 0
                  AND st.st_dueday < GETDATE()
            ORDER BY DATEDIFF(day, st.st_dueday, GETDATE()) DESC
        """
    },
    "recent_payments": {
        "description": "Recent customer payments received",
        "sql": """
            SELECT st.st_account AS account_code, sn.sn_name AS customer_name,
                   st.st_trdate AS payment_date, st.st_trref AS reference,
                   ABS(st.st_trvalue) AS payment_amount
            FROM stran st
            JOIN sname sn ON st.st_account = sn.sn_account
            WHERE st.st_trtype = 'R'
            ORDER BY st.st_trdate DESC
        """
    },
    "aged_debt_summary": {
        "description": "Customer balances with aging information",
        "sql": """
            SELECT sn_account AS account_code, sn_name AS customer_name,
                   sn_contact AS contact, sn_teleno AS phone, sn_email AS email,
                   sn_currbal AS total_owed, sn_crlim AS credit_limit,
                   sn_lastrec AS last_payment_date,
                   DATEDIFF(day, sn_lastrec, GETDATE()) AS days_since_payment,
                   CASE
                       WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER LIMIT'
                       WHEN sn_stop = 1 THEN 'ON STOP'
                       WHEN DATEDIFF(day, sn_lastrec, GETDATE()) > 90 THEN 'HIGH RISK'
                       WHEN DATEDIFF(day, sn_lastrec, GETDATE()) > 60 THEN 'MEDIUM RISK'
                       ELSE 'OK'
                   END AS risk_status
            FROM sname
            WHERE sn_currbal > 0
            ORDER BY sn_currbal DESC
        """
    },
    "top_debtors": {
        "description": "Customers who owe the most money",
        "sql": """
            SELECT TOP 20 sn_account AS account_code, sn_name AS customer_name,
                   sn_teleno AS phone, sn_contact AS contact,
                   sn_currbal AS amount_owed, sn_crlim AS credit_limit,
                   sn_lastrec AS last_payment
            FROM sname
            WHERE sn_currbal > 0
            ORDER BY sn_currbal DESC
        """
    },
    "payment_history": {
        "description": "Full payment allocation history",
        "sql": """
            SELECT al.al_account AS account_code, sn.sn_name AS customer_name,
                   al.al_date AS date, al.al_ref1 AS document,
                   al.al_ref2 AS allocated_to, al.al_val AS amount,
                   CASE al.al_type
                       WHEN 'R' THEN 'Receipt'
                       WHEN 'I' THEN 'Invoice'
                       WHEN 'C' THEN 'Credit Note'
                       ELSE al.al_type
                   END AS type
            FROM salloc al
            JOIN sname sn ON al.al_account = sn.sn_account
            ORDER BY al.al_date DESC
        """
    },
    "supplier_balances": {
        "description": "What we owe to suppliers",
        "sql": """
            SELECT pn_account AS supplier_code, pn_name AS supplier_name,
                   pn_teleno AS phone, pn_currbal AS balance_owed
            FROM pname
            WHERE pn_currbal <> 0
            ORDER BY pn_currbal DESC
        """
    }
}

def get_query(query_name: str) -> str:
    """Get a pre-built SQL query by name"""
    if query_name in CREDIT_CONTROL_QUERIES:
        return CREDIT_CONTROL_QUERIES[query_name]["sql"].strip()
    return None

def list_queries() -> list:
    """List all available pre-built queries"""
    return [(name, q["description"]) for name, q in CREDIT_CONTROL_QUERIES.items()]
