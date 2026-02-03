#!/usr/bin/env python3
"""
Credit Control Data Loader
Loads data from MS SQL into the RAG database using predefined table mappings.
No AI-generated SQL - uses reliable, tested queries.
"""

import json
import requests
import sys

API_BASE = "http://localhost:8000/api"

def load_mapping():
    """Load the table mapping configuration."""
    with open("table_mapping.json", "r") as f:
        return json.load(f)

def clear_rag():
    """Clear the RAG database."""
    print("Clearing RAG database...")
    response = requests.get(f"{API_BASE}/rag/clear")
    print(f"  {response.json().get('message', 'Done')}")

def ingest_from_sql(description: str, sql: str, max_rows: int = 2000):
    """Ingest data using a predefined SQL query."""
    response = requests.post(
        f"{API_BASE}/rag/ingest-from-sql",
        json={
            "description": description,
            "custom_sql": f"{sql} ",  # Custom SQL bypasses AI generation
            "max_rows": max_rows
        },
        timeout=120
    )
    result = response.json()
    if result.get("success"):
        print(f"  ✓ {result.get('rows_ingested', 0)} rows ingested")
    else:
        print(f"  ✗ Error: {result.get('error', 'Unknown error')}")
    return result

def ingest_text_documents(texts: list, metadata: list = None):
    """Ingest text documents directly."""
    response = requests.post(
        f"{API_BASE}/rag/ingest",
        json={"texts": texts, "metadata": metadata}
    )
    return response.json()

def load_credit_control_data():
    """Load all credit control data using the mapping."""
    mapping = load_mapping()

    # Clear existing data
    clear_rag()

    print("\n=== Loading Credit Control Tables ===\n")

    # Load each table from the mapping
    tables = mapping["credit_control_tables"]
    for table_key, table_info in tables.items():
        print(f"Loading {table_info['description']} ({table_info['table']})...")
        ingest_from_sql(
            description=table_info["description"],
            sql=table_info["sql_template"],
            max_rows=5000
        )

    print("\n=== Loading Pre-computed Credit Control Queries ===\n")

    # Load pre-computed credit control queries
    queries = mapping["credit_control_queries"]
    for query_key, query_info in queries.items():
        print(f"Loading {query_info['description']}...")
        ingest_from_sql(
            description=query_info["description"],
            sql=query_info["sql"],
            max_rows=1000
        )

    print("\n=== Loading Data Dictionary ===\n")

    # Load data dictionary for context
    dictionary_docs = [
        f"TABLE REFERENCE: {tables['customers']['table'].upper()} is the Customer Master table. " +
        "Contains customer names, addresses, contact details, current balances, credit limits. " +
        "Key fields: " + ", ".join([f"{k} = {v}" for k, v in tables['customers']['columns'].items()]),

        f"TABLE REFERENCE: {tables['sales_transactions']['table'].upper()} is the Sales Transactions table. " +
        "Contains invoices, credit notes, and receipts. " +
        "Transaction types: I=Invoice, C=Credit Note, R=Receipt. " +
        "Key fields: " + ", ".join([f"{k} = {v}" for k, v in tables['sales_transactions']['columns'].items()]),

        f"TABLE REFERENCE: {tables['payment_allocations']['table'].upper()} shows how payments are allocated to invoices. " +
        "Key fields: " + ", ".join([f"{k} = {v}" for k, v in tables['payment_allocations']['columns'].items()]),

        "CREDIT CONTROL RULES: " +
        "A customer is OVER CREDIT LIMIT when current_balance > credit_limit. " +
        "An account ON STOP (on_stop=True) means no further credit should be given. " +
        "OVERDUE means invoice due_date has passed and outstanding > 0. " +
        "Transaction types: I=Invoice (we are owed), C=Credit Note (reduction), R=Receipt (payment received).",

        "HOW TO IDENTIFY PROBLEM ACCOUNTS: " +
        "1. Check if current_balance exceeds credit_limit " +
        "2. Check days_since_payment - long time = concern " +
        "3. Check if account is on_stop " +
        "4. Check for overdue invoices (due_date < today and outstanding > 0)"
    ]

    print("Loading business rules and data dictionary...")
    result = ingest_text_documents(
        texts=dictionary_docs,
        metadata=[{"source": "data_dictionary", "type": "reference"} for _ in dictionary_docs]
    )
    print(f"  ✓ {len(dictionary_docs)} reference documents ingested")

    # Get final stats
    print("\n=== Summary ===\n")
    response = requests.get(f"{API_BASE}/rag/stats")
    stats = response.json().get("stats", {})
    print(f"Total documents in RAG: {stats.get('vectors_count', 'Unknown')}")

if __name__ == "__main__":
    load_credit_control_data()
