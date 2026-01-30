"""
FastAPI backend for SQL RAG application.
Provides REST API endpoints for database queries, RAG queries, and configuration management.
"""

import os
import sys
import logging
import configparser
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Literal
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sql_rag.sql_connector import SQLConnector
from sql_rag.vector_db import VectorDB
from sql_rag.llm import create_llm_instance

# Email module imports
from api.email.storage import EmailStorage
from api.email.providers.base import ProviderType
from api.email.providers.imap import IMAPProvider
from api.email.categorizer import EmailCategorizer, CustomerLinker
from api.email.sync import EmailSyncManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
config: Optional[configparser.ConfigParser] = None
sql_connector: Optional[SQLConnector] = None
vector_db: Optional[VectorDB] = None
llm = None

# Email module global instances
email_storage: Optional[EmailStorage] = None
email_sync_manager: Optional[EmailSyncManager] = None
email_categorizer: Optional[EmailCategorizer] = None
customer_linker: Optional[CustomerLinker] = None


# Get the config path relative to the project root
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.ini")


def load_config(config_path: str = None) -> configparser.ConfigParser:
    """Load configuration from file."""
    if config_path is None:
        config_path = CONFIG_PATH
    cfg = configparser.ConfigParser()
    if os.path.exists(config_path):
        cfg.read(config_path)
    return cfg


def save_config(cfg: configparser.ConfigParser, config_path: str = None):
    """Save configuration to file."""
    if config_path is None:
        config_path = CONFIG_PATH
    with open(config_path, "w") as f:
        cfg.write(f)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global config, sql_connector, vector_db, llm
    global email_storage, email_sync_manager, email_categorizer, customer_linker

    # Startup
    logger.info("Starting SQL RAG API...")
    config = load_config()

    try:
        sql_connector = SQLConnector(CONFIG_PATH)
        logger.info("SQL connector initialized")
    except Exception as e:
        logger.warning(f"Could not initialize SQL connector: {e}")

    try:
        vector_db = VectorDB(config)
        logger.info("Vector DB initialized")
    except Exception as e:
        logger.warning(f"Could not initialize Vector DB: {e}")

    try:
        llm = create_llm_instance(config)
        logger.info("LLM initialized")
    except Exception as e:
        logger.warning(f"Could not initialize LLM: {e}")

    # Initialize email module
    try:
        db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "email_data.db")
        email_storage = EmailStorage(db_path)
        logger.info("Email storage initialized")

        # Initialize categorizer with LLM
        email_categorizer = EmailCategorizer(llm)
        logger.info("Email categorizer initialized")

        # Initialize customer linker with SQL connector
        customer_linker = CustomerLinker(sql_connector)
        logger.info("Customer linker initialized")

        # Initialize sync manager
        email_sync_manager = EmailSyncManager(
            storage=email_storage,
            categorizer=email_categorizer,
            linker=customer_linker
        )

        # Register any enabled providers from config
        await _initialize_email_providers()

        logger.info("Email sync manager initialized")
    except Exception as e:
        logger.warning(f"Could not initialize email module: {e}")

    yield

    # Shutdown
    logger.info("Shutting down SQL RAG API...")
    if email_sync_manager:
        await email_sync_manager.stop_periodic_sync()


app = FastAPI(
    title="SQL RAG API",
    description="REST API for SQL RAG application - Query databases with natural language",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============ Pydantic Models ============

class SQLQueryRequest(BaseModel):
    query: str = Field(..., description="SQL query to execute")
    store_in_vector_db: bool = Field(False, description="Store results in vector database")


class SQLQueryResponse(BaseModel):
    success: bool
    data: List[Dict[str, Any]] = []
    columns: List[str] = []
    row_count: int = 0
    error: Optional[str] = None


class RAGQueryRequest(BaseModel):
    question: str = Field(..., description="Natural language question")
    num_results: int = Field(5, description="Number of similar results to retrieve")


class SQLToRAGRequest(BaseModel):
    description: str = Field(..., description="Natural language description of data to fetch and store")
    custom_sql: Optional[str] = Field(None, description="Optional: provide your own SQL instead of AI-generated")
    table_filter: Optional[List[str]] = Field(None, description="Optional: limit AI to specific tables")
    max_rows: int = Field(1000, description="Maximum rows to fetch and store")


class RAGQueryResponse(BaseModel):
    success: bool
    answer: str = ""
    sources: List[Dict[str, Any]] = []
    error: Optional[str] = None


class ConfigUpdateRequest(BaseModel):
    section: str
    key: str
    value: str


class ProviderConfig(BaseModel):
    provider: str
    api_key: Optional[str] = None
    model: str
    temperature: float = 0.2
    max_tokens: int = 1000
    ollama_url: Optional[str] = None  # For local Ollama running on network


class DatabaseConfig(BaseModel):
    type: str
    server: Optional[str] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    use_windows_auth: bool = False
    # Advanced MS SQL settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    connection_timeout: int = 30
    command_timeout: int = 60
    ssl: bool = False
    ssl_ca: Optional[str] = None
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    port: Optional[int] = None  # Default 1433 for MSSQL, 5432 for PostgreSQL, 3306 for MySQL


class TableInfo(BaseModel):
    schema_name: str
    table_name: str
    table_type: str


class ColumnInfo(BaseModel):
    column_name: str
    data_type: str
    is_nullable: bool
    column_default: Optional[str] = None


# ============ Health & Status Endpoints ============

@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "sql-rag-api"}


@app.get("/api/status")
async def get_status():
    """Get current system status."""
    return {
        "sql_connector": sql_connector is not None,
        "vector_db": vector_db is not None,
        "llm": llm is not None,
        "config_loaded": config is not None
    }


# ============ Configuration Endpoints ============

@app.get("/api/config")
async def get_config():
    """Get current configuration (sensitive values masked)."""
    if not config:
        raise HTTPException(status_code=500, detail="Configuration not loaded")

    result = {}
    for section in config.sections():
        result[section] = {}
        for key, value in config.items(section):
            # Mask sensitive values
            if "key" in key.lower() or "password" in key.lower():
                result[section][key] = "***" if value else ""
            else:
                result[section][key] = value
    return result


@app.get("/api/config/providers")
async def get_available_providers():
    """Get list of available LLM providers."""
    return {
        "providers": [
            {"id": "local", "name": "Local (Ollama)", "requires_api_key": False},
            {"id": "openai", "name": "OpenAI", "requires_api_key": True},
            {"id": "anthropic", "name": "Anthropic Claude", "requires_api_key": True},
            {"id": "gemini", "name": "Google Gemini", "requires_api_key": True},
            {"id": "groq", "name": "Groq", "requires_api_key": True},
        ]
    }


@app.get("/api/config/models/{provider}")
async def get_provider_models(provider: str):
    """Get available models for a provider."""
    models_map = {
        "local": ["llama3:latest", "llama3:8b", "llama3:70b", "mistral:7b-instruct-v0.2", "codellama:7b", "phi:3"],
        "openai": ["gpt-4o", "gpt-4-turbo", "gpt-4", "gpt-3.5-turbo"],
        "anthropic": ["claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022", "claude-3-opus-20240229", "claude-3-sonnet-20240229", "claude-3-haiku-20240307"],
        "gemini": ["gemini-3-pro", "gemini-3-flash", "gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash"],
        "groq": ["llama-3.3-70b-versatile", "llama-3.1-70b-versatile", "llama-3.1-8b-instant", "mixtral-8x7b-32768", "gemma2-9b-it"],
    }

    if provider not in models_map:
        raise HTTPException(status_code=404, detail=f"Unknown provider: {provider}")

    return {"provider": provider, "models": models_map[provider]}


@app.post("/api/config/llm")
async def update_llm_config(provider_config: ProviderConfig):
    """Update LLM configuration."""
    global config, llm

    if not config:
        config = load_config()

    # Update models section
    if not config.has_section("models"):
        config.add_section("models")
    config["models"]["provider"] = provider_config.provider

    # Update provider-specific section
    if provider_config.provider != "local":
        if not config.has_section(provider_config.provider):
            config.add_section(provider_config.provider)
        if provider_config.api_key:
            config[provider_config.provider]["api_key"] = provider_config.api_key
        config[provider_config.provider]["model"] = provider_config.model
    else:
        config["models"]["llm_model"] = provider_config.model
        # Save Ollama URL if provided
        if provider_config.ollama_url:
            config["models"]["llm_api_url"] = provider_config.ollama_url

    # Update system settings
    if not config.has_section("system"):
        config.add_section("system")
    config["system"]["temperature"] = str(provider_config.temperature)
    config["system"]["max_token_limit"] = str(provider_config.max_tokens)

    # Save config
    save_config(config)

    # Reinitialize LLM
    try:
        llm = create_llm_instance(config)
        return {"success": True, "message": "LLM configuration updated"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/config/database")
async def update_database_config(db_config: DatabaseConfig):
    """Update database configuration."""
    global config, sql_connector

    if not config:
        config = load_config()

    if not config.has_section("database"):
        config.add_section("database")

    config["database"]["type"] = db_config.type
    if db_config.server:
        config["database"]["server"] = db_config.server
    if db_config.database:
        config["database"]["database"] = db_config.database
    if db_config.username:
        config["database"]["username"] = db_config.username
    if db_config.password:
        config["database"]["password"] = db_config.password
    config["database"]["use_windows_auth"] = str(db_config.use_windows_auth)

    # Advanced connection settings
    config["database"]["pool_size"] = str(db_config.pool_size)
    config["database"]["max_overflow"] = str(db_config.max_overflow)
    config["database"]["pool_timeout"] = str(db_config.pool_timeout)
    config["database"]["connection_timeout"] = str(db_config.connection_timeout)
    config["database"]["command_timeout"] = str(db_config.command_timeout)
    config["database"]["ssl"] = str(db_config.ssl).lower()
    if db_config.ssl_ca:
        config["database"]["ssl_ca"] = db_config.ssl_ca
    if db_config.ssl_cert:
        config["database"]["ssl_cert"] = db_config.ssl_cert
    if db_config.ssl_key:
        config["database"]["ssl_key"] = db_config.ssl_key
    if db_config.port:
        config["database"]["port"] = str(db_config.port)

    save_config(config)

    # Reinitialize SQL connector
    try:
        sql_connector = SQLConnector(config)
        return {"success": True, "message": "Database configuration updated"}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============ Database Endpoints ============

@app.get("/api/database/tables", response_model=List[TableInfo])
async def get_tables():
    """Get list of tables in the database."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        tables = sql_connector.get_tables()
        # Convert DataFrame to list of dicts if needed
        if hasattr(tables, 'to_dict'):
            tables = tables.to_dict('records')
        return [TableInfo(**t) for t in tables]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/database/tables/{table_name}/columns", response_model=List[ColumnInfo])
async def get_table_columns(table_name: str, schema_name: str = ""):
    """Get columns for a specific table."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        columns = sql_connector.get_columns(table_name, schema_name)
        # Convert DataFrame to list of dicts if needed
        if hasattr(columns, 'to_dict'):
            columns = columns.to_dict('records')
        return [ColumnInfo(**c) for c in columns]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/database/query", response_model=SQLQueryResponse)
async def execute_query(request: SQLQueryRequest):
    """Execute a SQL query."""
    if not sql_connector:
        return SQLQueryResponse(success=False, error="SQL connector not initialized")

    try:
        result = sql_connector.execute_query(request.query)

        # Convert DataFrame to list of dicts if needed
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        if result and len(result) > 0:
            columns = list(result[0].keys())
            data = [dict(row) for row in result]

            # Store in vector DB if requested
            if request.store_in_vector_db and vector_db:
                try:
                    texts = [" ".join(f"{k}: {v}" for k, v in row.items()) for row in data]
                    vector_db.store_vectors(texts, [{"source": "sql_query", "query": request.query, **row} for row in data])
                except Exception as e:
                    logger.warning(f"Failed to store in vector DB: {e}")

            return SQLQueryResponse(
                success=True,
                data=data,
                columns=columns,
                row_count=len(data)
            )
        else:
            return SQLQueryResponse(success=True, data=[], columns=[], row_count=0)

    except Exception as e:
        return SQLQueryResponse(success=False, error=str(e))


# ============ RAG Endpoints ============

@app.post("/api/rag/query", response_model=RAGQueryResponse)
async def rag_query(request: RAGQueryRequest):
    """Query the RAG system with natural language."""
    if not vector_db:
        return RAGQueryResponse(success=False, error="Vector database not initialized")
    if not llm:
        return RAGQueryResponse(success=False, error="LLM not initialized")

    try:
        # Search for similar content
        results = vector_db.search_similar(request.question, limit=request.num_results)

        if not results:
            return RAGQueryResponse(
                success=True,
                answer="I don't have any relevant information to answer your question. Please ingest some data first.",
                sources=[]
            )

        # Generate answer using LLM
        answer = llm.process_rag_query(request.question, results)

        # Format sources
        sources = [{"score": r["score"], "text": r["payload"].get("text", "")[:200]} for r in results]

        return RAGQueryResponse(
            success=True,
            answer=answer,
            sources=sources
        )

    except Exception as e:
        return RAGQueryResponse(success=False, error=str(e))


@app.post("/api/rag/generate-sql")
async def generate_sql(question: str = Query(..., description="Natural language question")):
    """Generate SQL from natural language question."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")
    if not llm:
        raise HTTPException(status_code=503, detail="LLM not initialized")

    try:
        # Get schema information
        tables = sql_connector.get_tables()
        # Convert DataFrame to list of dicts if needed
        if hasattr(tables, 'to_dict'):
            tables = tables.to_dict('records')
        schema_info = []

        for table in tables[:10]:  # Limit to first 10 tables
            columns = sql_connector.get_columns(table["table_name"], table.get("schema_name", ""))
            if hasattr(columns, 'to_dict'):
                columns = columns.to_dict('records')
            col_info = ", ".join([f"{c['column_name']} ({c['data_type']})" for c in columns])
            schema_info.append(f"Table {table['table_name']}: {col_info}")

        schema_str = "\n".join(schema_info)

        prompt = f"""Given the following database schema:
{schema_str}

Generate a SQL query to answer this question: {question}

Return ONLY the SQL query, no explanations."""

        sql = llm.get_completion(prompt)

        return {"success": True, "sql": sql.strip()}

    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/rag/stats")
async def get_vector_stats():
    """Get vector database statistics."""
    if not vector_db:
        raise HTTPException(status_code=503, detail="Vector database not initialized")

    try:
        info = vector_db.get_collection_info()
        return {"success": True, "stats": info}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/rag/ingest")
async def ingest_data(texts: List[str], metadata: Optional[List[Dict[str, Any]]] = None):
    """Ingest data into the vector database."""
    if not vector_db:
        raise HTTPException(status_code=503, detail="Vector database not initialized")

    try:
        if metadata is None:
            metadata = [{"source": "manual_ingest"} for _ in texts]

        vector_db.store_vectors(texts, metadata)
        return {"success": True, "message": f"Ingested {len(texts)} documents"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/rag/ingest-from-sql")
async def ingest_from_sql(request: SQLToRAGRequest):
    """
    AI-powered SQL to RAG ingestion.
    Describe what data you want in natural language, AI generates SQL,
    executes it against the database, and stores results in ChromaDB.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")
    if not vector_db:
        raise HTTPException(status_code=503, detail="Vector database not initialized")
    if not llm and not request.custom_sql:
        raise HTTPException(status_code=503, detail="LLM not initialized (required for SQL generation)")

    try:
        # Step 1: Generate or use provided SQL
        if request.custom_sql:
            sql_query = request.custom_sql
            logger.info(f"Using custom SQL: {sql_query[:100]}...")
        else:
            # Get schema information for AI
            tables = sql_connector.get_tables()
            # Convert DataFrame to list of dicts if needed
            if hasattr(tables, 'to_dict'):
                tables = tables.to_dict('records')

            # Filter tables if specified
            if request.table_filter:
                tables = [t for t in tables if t["table_name"] in request.table_filter]

            schema_info = []
            for table in tables[:20]:  # Limit schema context
                columns = sql_connector.get_columns(table["table_name"], table.get("schema_name", ""))
                if hasattr(columns, 'to_dict'):
                    columns = columns.to_dict('records')
                col_info = ", ".join([f"{c['column_name']} ({c['data_type']})" for c in columns[:30]])
                schema_info.append(f"Table {table.get('schema_name', 'dbo')}.{table['table_name']}: {col_info}")

            schema_str = "\n".join(schema_info)

            prompt = f"""You are a SQL expert. Given the following database schema:

{schema_str}

Generate a SQL query to retrieve the following data: {request.description}

Requirements:
- Return ONLY the SQL query, no explanations or markdown
- Limit results to {request.max_rows} rows using TOP or LIMIT clause
- Select meaningful columns that would be useful for semantic search
- Join related tables if it provides better context

SQL Query:"""

            sql_query = llm.get_completion(prompt).strip()

            # Check if LLM returned an error instead of SQL
            if sql_query.lower().startswith("error:") or "429" in sql_query or "quota" in sql_query.lower():
                return {"success": False, "error": f"LLM API error: {sql_query[:200]}. Please use custom SQL instead."}

            # Clean up the SQL (remove markdown if present)
            if sql_query.startswith("```"):
                sql_query = sql_query.split("```")[1]
                if sql_query.startswith("sql"):
                    sql_query = sql_query[3:]
            sql_query = sql_query.strip()

            # Validate it looks like SQL
            if not any(sql_query.upper().startswith(kw) for kw in ["SELECT", "WITH"]):
                return {"success": False, "error": f"LLM did not return valid SQL. Got: {sql_query[:100]}. Please use custom SQL instead."}

            logger.info(f"AI generated SQL: {sql_query[:200]}...")

        # Step 2: Execute the SQL query
        result = sql_connector.execute_query(sql_query)

        # Convert DataFrame to list of dicts if needed
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        if not result or len(result) == 0:
            return {
                "success": True,
                "message": "Query executed but returned no results",
                "sql_used": sql_query,
                "rows_ingested": 0
            }

        # Step 3: Convert results to text documents for RAG
        data = [dict(row) for row in result]

        # Create semantic text representation for each row
        texts = []
        metadata_list = []

        for row in data:
            # Create a readable text representation
            text_parts = []
            for key, value in row.items():
                if value is not None and str(value).strip():
                    text_parts.append(f"{key}: {value}")

            text = " | ".join(text_parts)
            texts.append(text)

            # Create metadata for filtering/retrieval
            meta = {
                "source": "mssql_ingestion",
                "query_description": request.description[:200],
                "sql_query": sql_query[:500],
            }
            # Add row data as metadata (with type conversion)
            for key, value in row.items():
                if isinstance(value, (str, int, float, bool)) or value is None:
                    meta[f"col_{key}"] = value
                else:
                    meta[f"col_{key}"] = str(value)

            metadata_list.append(meta)

        # Step 4: Store in ChromaDB
        vector_db.store_vectors(texts, metadata_list)

        logger.info(f"Successfully ingested {len(texts)} rows into ChromaDB")

        return {
            "success": True,
            "message": f"Successfully ingested {len(texts)} rows from MS SQL into RAG database",
            "sql_used": sql_query,
            "rows_ingested": len(texts),
            "sample_data": data[:3] if len(data) > 3 else data  # Return sample
        }

    except Exception as e:
        logger.error(f"SQL to RAG ingestion failed: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/rag/load-credit-control")
async def load_credit_control_data():
    """Load all credit control data using predefined table mappings."""
    import json
    import os

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")
    if not vector_db:
        raise HTTPException(status_code=503, detail="Vector database not initialized")

    try:
        # Load the table mapping
        mapping_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "table_mapping.json")
        if not os.path.exists(mapping_path):
            return {"success": False, "error": "table_mapping.json not found"}

        with open(mapping_path, "r") as f:
            mapping = json.load(f)

        # Clear existing data
        vector_db.client.delete_collection(vector_db.collection_name)
        vector_db.collection = vector_db.client.get_or_create_collection(
            name=vector_db.collection_name,
            metadata={"hnsw:space": "cosine"}
        )

        total_ingested = 0
        results = []

        # Load each table from the mapping
        tables = mapping.get("credit_control_tables", {})
        for table_key, table_info in tables.items():
            try:
                sql = table_info.get("sql_template", "")
                if not sql:
                    continue

                result = sql_connector.execute_query(sql)
                if hasattr(result, 'to_dict'):
                    result = result.to_dict('records')

                if result:
                    texts = [" | ".join(f"{k}: {v}" for k, v in row.items() if v is not None) for row in result]
                    metadata = [{"source": table_info["table"], "type": "master_data"} for _ in texts]
                    vector_db.store_vectors(texts, metadata)
                    total_ingested += len(texts)
                    results.append({"table": table_info["table"], "rows": len(texts)})
            except Exception as e:
                results.append({"table": table_info.get("table", table_key), "error": str(e)})

        # Load pre-computed credit control queries
        queries = mapping.get("credit_control_queries", {})
        for query_key, query_info in queries.items():
            try:
                sql = query_info.get("sql", "")
                if not sql:
                    continue

                result = sql_connector.execute_query(sql)
                if hasattr(result, 'to_dict'):
                    result = result.to_dict('records')

                if result:
                    texts = [" | ".join(f"{k}: {v}" for k, v in row.items() if v is not None) for row in result]
                    metadata = [{"source": query_key, "type": "credit_control_query"} for _ in texts]
                    vector_db.store_vectors(texts, metadata)
                    total_ingested += len(texts)
                    results.append({"query": query_key, "rows": len(texts)})
            except Exception as e:
                results.append({"query": query_key, "error": str(e)})

        # Load data dictionary
        dictionary_docs = [
            "SNAME is the Customer Master table with columns: sn_account=Account Code, sn_name=Customer Name, sn_currbal=Current Balance, sn_crlim=Credit Limit, sn_stop=Account On Stop, sn_lastinv=Last Invoice Date, sn_lastrec=Last Receipt Date",
            "STRAN is the Sales Transactions table. Transaction types: I=Invoice, C=Credit Note, R=Receipt. Key columns: st_account=Customer Account, st_trdate=Date, st_trvalue=Value, st_trbal=Outstanding Balance",
            "CREDIT CONTROL RULES: Customer is OVER CREDIT LIMIT when current_balance > credit_limit. Account ON STOP means on_stop=True. OVERDUE means due_date passed and outstanding > 0"
        ]
        vector_db.store_vectors(dictionary_docs, [{"source": "data_dictionary", "type": "reference"} for _ in dictionary_docs])
        total_ingested += len(dictionary_docs)

        return {
            "success": True,
            "message": f"Loaded {total_ingested} documents for credit control",
            "details": results
        }

    except Exception as e:
        logger.error(f"Failed to load credit control data: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/rag/clear")
async def clear_vector_db():
    """Clear all data from the vector database."""
    if not vector_db:
        raise HTTPException(status_code=503, detail="Vector database not initialized")

    try:
        # Delete and recreate collection
        vector_db.client.delete_collection(vector_db.collection_name)
        vector_db.collection = vector_db.client.get_or_create_collection(
            name=vector_db.collection_name,
            metadata={"hnsw:space": "cosine"}
        )
        return {"success": True, "message": "Vector database cleared"}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============ Credit Control Query (Live SQL) ============

class CreditControlQueryRequest(BaseModel):
    question: str = Field(..., description="Natural language credit control question")


# ============ Credit Control Agent Query Definitions ============
# Organized into categories matching the agent requirements
# Order matters - more specific patterns should be checked first

CREDIT_CONTROL_QUERIES = [
    # ========== PRE-ACTION CHECKS (run before every contact) ==========
    {
        "name": "unallocated_cash",
        "category": "pre_action",
        "keywords": ["unallocated cash", "unapplied cash", "unallocated payment", "cash on account", "credit balance"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS receipt_ref, st.st_trdate AS receipt_date,
                  ABS(st.st_trbal) AS unallocated_amount, sn.sn_teleno AS phone
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'R' AND st.st_trbal < 0
                  ORDER BY ABS(st.st_trbal) DESC""",
        "description": "Unallocated cash/payments on customer accounts",
        "param_sql": """SELECT st.st_trref AS receipt_ref, st.st_trdate AS receipt_date,
                       ABS(st.st_trbal) AS unallocated_amount
                       FROM stran st WHERE st.st_account = '{account}'
                       AND st.st_trtype = 'R' AND st.st_trbal < 0"""
    },
    {
        "name": "pending_credit_notes",
        "category": "pre_action",
        "keywords": ["pending credit", "credit note", "credit notes", "credits pending", "unapplied credit"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS credit_ref, st.st_trdate AS credit_date,
                  ABS(st.st_trbal) AS credit_amount, st.st_memo AS reason
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'C' AND st.st_trbal < 0
                  ORDER BY st.st_trdate DESC""",
        "description": "Pending/unallocated credit notes",
        "param_sql": """SELECT st.st_trref AS credit_ref, st.st_trdate AS credit_date,
                       ABS(st.st_trbal) AS credit_amount, st.st_memo AS reason
                       FROM stran st WHERE st.st_account = '{account}'
                       AND st.st_trtype = 'C' AND st.st_trbal < 0"""
    },
    {
        "name": "disputed_invoices",
        "category": "pre_action",
        "keywords": ["dispute", "disputed", "in dispute", "query", "queried"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS invoice, st.st_trdate AS invoice_date,
                  st.st_trbal AS outstanding, st.st_memo AS dispute_reason,
                  sn.sn_teleno AS phone
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_dispute = 1 AND st.st_trbal > 0
                  ORDER BY st.st_trbal DESC""",
        "description": "Invoices flagged as disputed",
        "param_sql": """SELECT st.st_trref AS invoice, st.st_trdate AS invoice_date,
                       st.st_trbal AS outstanding, st.st_memo AS dispute_reason
                       FROM stran st WHERE st.st_account = '{account}'
                       AND st.st_dispute = 1 AND st.st_trbal > 0"""
    },
    {
        "name": "account_status",
        "category": "pre_action",
        "keywords": ["account status", "status check", "can we contact", "on hold", "do not contact", "insolvency", "dormant"],
        "sql": """SELECT sn_account AS account, sn_name AS customer,
                  sn_stop AS on_stop, sn_dormant AS dormant,
                  sn_currbal AS balance, sn_crlim AS credit_limit,
                  sn_crdscor AS credit_score, sn_crdnotes AS credit_notes,
                  sn_memo AS account_memo, sn_priorty AS priority,
                  sn_cmgroup AS credit_group
                  FROM sname WHERE sn_stop = 1 OR sn_dormant = 1
                  ORDER BY sn_currbal DESC""",
        "description": "Accounts with stop/hold/dormant flags",
        "param_sql": """SELECT sn_stop AS on_stop, sn_dormant AS dormant,
                       sn_crdscor AS credit_score, sn_crdnotes AS credit_notes,
                       sn_memo AS account_memo, sn_priorty AS priority
                       FROM sname WHERE sn_account = '{account}'"""
    },

    # ========== LEDGER STATE ==========
    {
        "name": "overdue_by_age",
        "category": "ledger",
        "keywords": ["overdue by age", "aged overdue", "aging bucket", "age bracket", "days overdue breakdown"],
        "sql": """SELECT
                  sn.sn_account AS account, sn.sn_name AS customer,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 0 AND 7 THEN st.st_trbal ELSE 0 END) AS days_1_7,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 8 AND 14 THEN st.st_trbal ELSE 0 END) AS days_8_14,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 15 AND 21 THEN st.st_trbal ELSE 0 END) AS days_15_21,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 22 AND 30 THEN st.st_trbal ELSE 0 END) AS days_22_30,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) BETWEEN 31 AND 45 THEN st.st_trbal ELSE 0 END) AS days_31_45,
                  SUM(CASE WHEN DATEDIFF(day, st.st_dueday, GETDATE()) > 45 THEN st.st_trbal ELSE 0 END) AS days_45_plus,
                  SUM(st.st_trbal) AS total_overdue
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'I' AND st.st_trbal > 0 AND st.st_dueday < GETDATE()
                  GROUP BY sn.sn_account, sn.sn_name
                  HAVING SUM(st.st_trbal) > 0
                  ORDER BY SUM(st.st_trbal) DESC""",
        "description": "Overdue invoices broken down by age bracket"
    },
    {
        "name": "customer_balance_aging",
        "category": "ledger",
        "keywords": ["balance aging", "ageing summary", "customer aging", "outstanding balance", "balance breakdown"],
        "sql": """SELECT sn.sn_account AS account, sn.sn_name AS customer,
                  sn.sn_currbal AS total_balance, sn.sn_crlim AS credit_limit,
                  sh.si_current AS current, sh.si_period1 AS period_1,
                  sh.si_period2 AS period_2, sh.si_period3 AS period_3,
                  sh.si_period4 AS period_4, sh.si_period5 AS period_5,
                  sh.si_avgdays AS avg_days_to_pay
                  FROM sname sn
                  LEFT JOIN shist sh ON sn.sn_account = sh.si_account
                  WHERE sn.sn_currbal > 0
                  ORDER BY sn.sn_currbal DESC""",
        "description": "Customer balance with aging periods"
    },
    {
        "name": "invoice_lookup",
        "category": "ledger",
        "keywords": ["invoice detail", "invoice lookup", "find invoice", "invoice number", "specific invoice"],
        "sql": None,  # Dynamic - needs invoice number extraction
        "description": "Look up specific invoice details",
        "param_sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                       st.st_trref AS invoice, st.st_custref AS your_ref,
                       st.st_trdate AS invoice_date, st.st_dueday AS due_date,
                       st.st_trvalue AS original_value, st.st_trbal AS outstanding,
                       st.st_dispute AS disputed, st.st_memo AS memo,
                       st.st_payday AS promise_date,
                       DATEDIFF(day, st.st_dueday, GETDATE()) AS days_overdue
                       FROM stran st
                       JOIN sname sn ON st.st_account = sn.sn_account
                       WHERE st.st_trref LIKE '%{invoice}%' AND st.st_trtype = 'I'"""
    },
    {
        "name": "overdue_invoices",
        "category": "ledger",
        "keywords": ["overdue", "past due", "late invoice", "unpaid invoice", "outstanding invoice"],
        "sql": """SELECT TOP 50 st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS invoice, st.st_trdate AS invoice_date, st.st_dueday AS due_date,
                  st.st_trbal AS outstanding, DATEDIFF(day, st.st_dueday, GETDATE()) AS days_overdue,
                  st.st_dispute AS disputed, sn.sn_teleno AS phone
                  FROM stran st JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'I' AND st.st_trbal > 0 AND st.st_dueday < GETDATE()
                  ORDER BY days_overdue DESC""",
        "description": "Overdue invoices ordered by days overdue"
    },

    # ========== CUSTOMER CONTEXT ==========
    {
        "name": "customer_master",
        "category": "customer",
        "keywords": ["customer details", "customer master", "contact details", "customer info", "customer record"],
        "sql": None,  # Dynamic - needs customer name/account extraction
        "description": "Customer master record with all details",
        "param_sql": """SELECT sn_account AS account, sn_name AS customer,
                       sn_addr1 AS address1, sn_addr2 AS address2, sn_addr3 AS city,
                       sn_addr4 AS county, sn_pstcode AS postcode,
                       sn_teleno AS phone, sn_email AS email,
                       sn_contact AS contact, sn_contac2 AS contact2,
                       sn_currbal AS balance, sn_crlim AS credit_limit,
                       sn_stop AS on_stop, sn_dormant AS dormant,
                       sn_custype AS customer_type, sn_region AS region,
                       sn_priorty AS priority, sn_cmgroup AS credit_group,
                       sn_lastinv AS last_invoice, sn_lastrec AS last_payment,
                       sn_memo AS memo, sn_crdnotes AS credit_notes
                       FROM sname WHERE sn_account = '{account}'
                       OR sn_name LIKE '%{customer}%'"""
    },
    {
        "name": "payment_history",
        "category": "customer",
        "keywords": ["payment history", "payment pattern", "how they pay", "days to pay", "payment behaviour", "payment behavior"],
        "sql": """SELECT TOP 20 st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS receipt_ref, st.st_trdate AS payment_date,
                  ABS(st.st_trvalue) AS amount, sh.si_avgdays AS avg_days_to_pay
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  LEFT JOIN shist sh ON st.st_account = sh.si_account
                  WHERE st.st_trtype = 'R'
                  ORDER BY st.st_trdate DESC""",
        "description": "Customer payment history and patterns",
        "param_sql": """SELECT st.st_trref AS receipt_ref, st.st_trdate AS payment_date,
                       ABS(st.st_trvalue) AS amount,
                       (SELECT TOP 1 si_avgdays FROM shist WHERE si_account = '{account}') AS avg_days_to_pay
                       FROM stran st WHERE st.st_account = '{account}'
                       AND st.st_trtype = 'R' ORDER BY st.st_trdate DESC"""
    },
    {
        "name": "customer_notes",
        "category": "customer",
        "keywords": ["customer notes", "memo", "notes", "comments", "history notes", "contact log"],
        "sql": """SELECT zn.zn_account AS account, sn.sn_name AS customer,
                  zn.zn_subject AS subject, zn.zn_note AS note,
                  zn.sq_date AS note_date, zn.sq_user AS created_by,
                  zn.zn_actreq AS action_required, zn.zn_actdate AS action_date,
                  zn.zn_actcomp AS action_complete, zn.zn_priority AS priority
                  FROM znotes zn
                  JOIN sname sn ON zn.zn_account = sn.sn_account
                  WHERE zn.zn_module = 'SL'
                  ORDER BY zn.sq_date DESC""",
        "description": "Customer notes and contact history",
        "param_sql": """SELECT zn.zn_subject AS subject, zn.zn_note AS note,
                       zn.sq_date AS note_date, zn.sq_user AS created_by,
                       zn.zn_actreq AS action_required, zn.zn_actdate AS action_date,
                       zn.zn_actcomp AS action_complete
                       FROM znotes zn WHERE zn.zn_account = '{account}'
                       AND zn.zn_module = 'SL' ORDER BY zn.sq_date DESC"""
    },
    {
        "name": "customer_segments",
        "category": "customer",
        "keywords": ["segment", "customer type", "strategic", "watch list", "customer category", "priority"],
        "sql": """SELECT sn_account AS account, sn_name AS customer,
                  sn_custype AS customer_type, sn_region AS region,
                  sn_priorty AS priority, sn_cmgroup AS credit_group,
                  sn_currbal AS balance, sn_crlim AS credit_limit,
                  sn_crdscor AS credit_score
                  FROM sname WHERE sn_currbal > 0
                  ORDER BY sn_priorty DESC, sn_currbal DESC""",
        "description": "Customer segments and priority flags"
    },

    # ========== PROMISE TRACKING ==========
    {
        "name": "promises_due",
        "category": "promise",
        "keywords": ["promise due", "promises today", "payment promise", "promised to pay", "ptp", "promise overdue"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS invoice, st.st_trbal AS outstanding,
                  st.st_payday AS promise_date,
                  DATEDIFF(day, st.st_payday, GETDATE()) AS days_since_promise,
                  sn.sn_teleno AS phone, sn.sn_contact AS contact
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_payday IS NOT NULL
                  AND st.st_payday <= GETDATE()
                  AND st.st_trbal > 0
                  ORDER BY st.st_payday ASC""",
        "description": "Promises to pay due today or overdue"
    },
    {
        "name": "broken_promises",
        "category": "promise",
        "keywords": ["broken promise", "missed promise", "promise count", "failed promise", "promise history"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  COUNT(*) AS broken_promise_count,
                  SUM(st.st_trbal) AS total_outstanding,
                  MIN(st.st_payday) AS oldest_promise,
                  MAX(st.st_payday) AS latest_promise
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_payday IS NOT NULL
                  AND st.st_payday < GETDATE()
                  AND st.st_trbal > 0
                  GROUP BY st.st_account, sn.sn_name
                  HAVING COUNT(*) > 1
                  ORDER BY COUNT(*) DESC""",
        "description": "Customers with multiple broken promises",
        "param_sql": """SELECT COUNT(*) AS broken_promise_count,
                       SUM(st_trbal) AS total_outstanding
                       FROM stran WHERE st_account = '{account}'
                       AND st_payday IS NOT NULL AND st_payday < GETDATE()
                       AND st_trbal > 0"""
    },

    # ========== MONITORING / REPORTING ==========
    {
        "name": "over_credit_limit",
        "category": "monitoring",
        "keywords": ["over credit", "exceed credit", "over limit", "exceeded limit", "above credit", "credit limit"],
        "sql": """SELECT sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                  sn_crlim AS credit_limit, (sn_currbal - sn_crlim) AS over_by,
                  sn_ordrbal AS orders_pending, sn_teleno AS phone, sn_contact AS contact,
                  sn_stop AS on_stop
                  FROM sname WHERE sn_currbal > sn_crlim AND sn_crlim > 0
                  ORDER BY (sn_currbal - sn_crlim) DESC""",
        "description": "Customers over their credit limit"
    },
    {
        "name": "unallocated_cash_old",
        "category": "monitoring",
        "keywords": ["old unallocated", "unallocated over 7 days", "reconciliation", "cash reconciliation", "old cash"],
        "sql": """SELECT st.st_account AS account, sn.sn_name AS customer,
                  st.st_trref AS receipt_ref, st.st_trdate AS receipt_date,
                  ABS(st.st_trbal) AS unallocated_amount,
                  DATEDIFF(day, st.st_trdate, GETDATE()) AS days_unallocated,
                  sn.sn_teleno AS phone
                  FROM stran st
                  JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'R' AND st.st_trbal < 0
                  AND DATEDIFF(day, st.st_trdate, GETDATE()) > 7
                  ORDER BY DATEDIFF(day, st.st_trdate, GETDATE()) DESC""",
        "description": "Unallocated cash older than 7 days (needs reconciliation)"
    },
    {
        "name": "accounts_on_stop",
        "category": "monitoring",
        "keywords": ["on stop", "stopped", "account stop", "credit stop"],
        "sql": """SELECT sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                  sn_crlim AS credit_limit, sn_teleno AS phone, sn_contact AS contact,
                  sn_memo AS memo, sn_crdnotes AS credit_notes
                  FROM sname WHERE sn_stop = 1
                  ORDER BY sn_currbal DESC""",
        "description": "Accounts currently on stop"
    },
    {
        "name": "top_debtors",
        "category": "monitoring",
        "keywords": ["owes most", "top debtor", "highest balance", "biggest debt", "most money", "largest balance", "owe us"],
        "sql": """SELECT TOP 20 sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                  sn_crlim AS credit_limit, sn_lastrec AS last_payment, sn_teleno AS phone,
                  CASE WHEN sn_stop = 1 THEN 'ON STOP'
                       WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER LIMIT'
                       ELSE 'OK' END AS status
                  FROM sname WHERE sn_currbal > 0 ORDER BY sn_currbal DESC""",
        "description": "Top 20 customers by outstanding balance"
    },
    {
        "name": "recent_payments",
        "category": "monitoring",
        "keywords": ["recent payment", "last payment", "receipts", "paid recently", "who paid"],
        "sql": """SELECT TOP 20 st.st_account AS account, sn.sn_name AS customer,
                  st.st_trdate AS payment_date, st.st_trref AS reference,
                  ABS(st.st_trvalue) AS amount
                  FROM stran st JOIN sname sn ON st.st_account = sn.sn_account
                  WHERE st.st_trtype = 'R' ORDER BY st.st_trdate DESC""",
        "description": "Recent payments received"
    },
    {
        "name": "aged_debt",
        "category": "monitoring",
        "keywords": ["aged debt", "aging", "debt age", "how old", "debt summary"],
        "sql": """SELECT sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                  sn_lastrec AS last_payment, DATEDIFF(day, sn_lastrec, GETDATE()) AS days_since_payment,
                  CASE WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER LIMIT'
                       WHEN sn_stop = 1 THEN 'ON STOP' ELSE 'OK' END AS status
                  FROM sname WHERE sn_currbal > 0 ORDER BY sn_currbal DESC""",
        "description": "Aged debt summary with payment history"
    },

    # ========== LOOKUP (Dynamic) ==========
    {
        "name": "customer_lookup",
        "category": "lookup",
        "keywords": ["details for", "info for", "lookup", "find customer", "about customer", "tell me about"],
        "sql": None,  # Dynamic - needs customer name extraction
        "description": "Look up specific customer details"
    }
]


# Helper function to get query by name (for programmatic API access)
def get_query_by_name(name: str) -> dict:
    """Get a specific query definition by name."""
    for q in CREDIT_CONTROL_QUERIES:
        if q["name"] == name:
            return q
    return None


# API endpoint to list all available queries
@app.get("/api/credit-control/queries")
async def list_credit_control_queries():
    """List all available credit control query types."""
    queries = []
    for q in CREDIT_CONTROL_QUERIES:
        queries.append({
            "name": q["name"],
            "category": q.get("category", "general"),
            "description": q["description"],
            "keywords": q["keywords"],
            "has_param_sql": "param_sql" in q
        })
    return {"queries": queries}


# API endpoint for parameterized queries (for agent use)
@app.post("/api/credit-control/query-param")
async def credit_control_query_param(query_name: str, account: str = None, customer: str = None, invoice: str = None):
    """
    Execute a parameterized credit control query.
    For use by the credit control agent with specific parameters.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    query_def = get_query_by_name(query_name)
    if not query_def:
        raise HTTPException(status_code=404, detail=f"Query '{query_name}' not found")

    if "param_sql" not in query_def:
        raise HTTPException(status_code=400, detail=f"Query '{query_name}' does not support parameters")

    try:
        sql = query_def["param_sql"]
        if account:
            sql = sql.replace("{account}", account)
        if customer:
            sql = sql.replace("{customer}", customer)
        if invoice:
            sql = sql.replace("{invoice}", invoice)

        result = sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        return {
            "success": True,
            "query_name": query_name,
            "category": query_def.get("category", "general"),
            "description": query_def["description"],
            "data": result or [],
            "count": len(result) if result else 0
        }
    except Exception as e:
        logger.error(f"Parameterized query failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/credit-control/dashboard")
async def credit_control_dashboard():
    """
    Get summary dashboard data for credit control.
    Returns key metrics in a single API call.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        metrics = {}

        # Total outstanding balance
        result = sql_connector.execute_query(
            "SELECT COUNT(*) AS count, SUM(sn_currbal) AS total FROM sname WHERE sn_currbal > 0"
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["total_debt"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Total Outstanding"
            }

        # Over credit limit
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(sn_currbal - sn_crlim) AS total
               FROM sname WHERE sn_currbal > sn_crlim AND sn_crlim > 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["over_credit_limit"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Over Credit Limit"
            }

        # Accounts on stop
        result = sql_connector.execute_query(
            "SELECT COUNT(*) AS count, SUM(sn_currbal) AS total FROM sname WHERE sn_stop = 1"
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["accounts_on_stop"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Accounts On Stop"
            }

        # Overdue invoices
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(st_trbal) AS total
               FROM stran WHERE st_trtype = 'I' AND st_trbal > 0 AND st_dueday < GETDATE()"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["overdue_invoices"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Overdue Invoices"
            }

        # Recent payments (last 7 days)
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(ABS(st_trvalue)) AS total
               FROM stran WHERE st_trtype = 'R' AND st_trdate >= DATEADD(day, -7, GETDATE())"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["recent_payments"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Payments (7 days)"
            }

        # Promises due today or overdue
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(st_trbal) AS total
               FROM stran WHERE st_payday IS NOT NULL
               AND st_payday <= GETDATE() AND st_trbal > 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["promises_due"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Promises Due"
            }

        # Disputed invoices
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(st_trbal) AS total
               FROM stran WHERE st_dispute = 1 AND st_trbal > 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["disputed"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "In Dispute"
            }

        # Unallocated cash
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(ABS(st_trbal)) AS total
               FROM stran WHERE st_trtype = 'R' AND st_trbal < 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["unallocated_cash"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Unallocated Cash"
            }

        # Priority actions - customers needing attention
        priority_result = sql_connector.execute_query(
            """SELECT TOP 10 sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                      sn_crlim AS credit_limit, sn_teleno AS phone, sn_contact AS contact,
                      CASE WHEN sn_stop = 1 THEN 'ON_STOP'
                           WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER_LIMIT'
                           ELSE 'HIGH_BALANCE' END AS priority_reason
               FROM sname
               WHERE sn_currbal > 0 AND (sn_stop = 1 OR sn_currbal > sn_crlim)
               ORDER BY sn_currbal DESC"""
        )
        if hasattr(priority_result, 'to_dict'):
            priority_result = priority_result.to_dict('records')

        return {
            "success": True,
            "metrics": metrics,
            "priority_actions": priority_result or []
        }

    except Exception as e:
        logger.error(f"Dashboard query failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/credit-control/debtors-report")
async def credit_control_debtors_report():
    """
    Get aged debtors report with balance breakdown by aging period.
    Columns: Account, Customer, Balance, Current, 1 Month, 2 Month, 3 Month+
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Query joins sname (customer) with shist (aging history) to get aged balances
        # si_age = 1 is the most recent aging period
        result = sql_connector.execute_query("""
            SELECT
                sn.sn_account AS account,
                sn.sn_name AS customer,
                sn.sn_currbal AS balance,
                ISNULL(sh.si_current, 0) AS current_period,
                ISNULL(sh.si_period1, 0) AS month_1,
                ISNULL(sh.si_period2, 0) AS month_2,
                ISNULL(sh.si_period3, 0) + ISNULL(sh.si_period4, 0) + ISNULL(sh.si_period5, 0) AS month_3_plus,
                sn.sn_crlim AS credit_limit,
                sn.sn_teleno AS phone,
                sn.sn_contact AS contact,
                sn.sn_stop AS on_stop
            FROM sname sn
            LEFT JOIN shist sh ON sn.sn_account = sh.si_account AND sh.si_age = 1
            WHERE sn.sn_currbal <> 0
            ORDER BY sn.sn_account
        """)

        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        # Calculate totals
        totals = {
            "balance": sum(r.get("balance", 0) or 0 for r in result),
            "current": sum(r.get("current_period", 0) or 0 for r in result),
            "month_1": sum(r.get("month_1", 0) or 0 for r in result),
            "month_2": sum(r.get("month_2", 0) or 0 for r in result),
            "month_3_plus": sum(r.get("month_3_plus", 0) or 0 for r in result),
        }

        return {
            "success": True,
            "data": result or [],
            "count": len(result) if result else 0,
            "totals": totals
        }

    except Exception as e:
        logger.error(f"Debtors report query failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/nominal/trial-balance")
async def nominal_trial_balance(year: int = 2026):
    """
    Get summary trial balance from the nominal ledger.
    Returns account balances with debit/credit columns for the specified year.
    Only includes transactions dated within the specified year.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Query the ntran (nominal transactions) table filtered by year
        # Join with nacnt to get account descriptions
        # Sum all transactions for each account within the specified year
        result = sql_connector.execute_query(f"""
            SELECT
                RTRIM(t.nt_acnt) AS account_code,
                RTRIM(n.na_desc) AS description,
                RTRIM(t.nt_type) AS account_type,
                RTRIM(t.nt_subt) AS subtype,
                0 AS opening_balance,
                ISNULL(SUM(t.nt_value), 0) AS ytd_movement,
                CASE
                    WHEN ISNULL(SUM(t.nt_value), 0) > 0 THEN ISNULL(SUM(t.nt_value), 0)
                    ELSE 0
                END AS debit,
                CASE
                    WHEN ISNULL(SUM(t.nt_value), 0) < 0 THEN ABS(ISNULL(SUM(t.nt_value), 0))
                    ELSE 0
                END AS credit
            FROM ntran t
            LEFT JOIN nacnt n ON RTRIM(t.nt_acnt) = RTRIM(n.na_acnt)
            WHERE t.nt_year = {year}
            GROUP BY t.nt_acnt, n.na_desc, t.nt_type, t.nt_subt
            HAVING ISNULL(SUM(t.nt_value), 0) <> 0
            ORDER BY t.nt_acnt
        """)

        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        # Calculate totals
        total_debit = sum(r.get("debit", 0) or 0 for r in result)
        total_credit = sum(r.get("credit", 0) or 0 for r in result)

        # Group by account type for summary
        type_summary = {}
        type_names = {
            'A': 'Fixed Assets',
            'B': 'Current Assets',
            'C': 'Current Liabilities',
            'D': 'Capital & Reserves',
            'E': 'Sales',
            'F': 'Cost of Sales',
            'G': 'Overheads',
            'H': 'Other'
        }
        for r in result:
            atype = r.get("account_type", "?").strip()
            if atype not in type_summary:
                type_summary[atype] = {
                    "name": type_names.get(atype, f"Type {atype}"),
                    "debit": 0,
                    "credit": 0,
                    "count": 0
                }
            type_summary[atype]["debit"] += r.get("debit", 0) or 0
            type_summary[atype]["credit"] += r.get("credit", 0) or 0
            type_summary[atype]["count"] += 1

        return {
            "success": True,
            "year": year,
            "data": result or [],
            "count": len(result) if result else 0,
            "totals": {
                "debit": total_debit,
                "credit": total_credit,
                "difference": total_debit - total_credit
            },
            "by_type": type_summary
        }

    except Exception as e:
        logger.error(f"Trial balance query failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/nominal/statutory-accounts")
async def nominal_statutory_accounts(year: int = 2026):
    """
    Generate UK statutory accounts (P&L and Balance Sheet) from ntran.
    Returns formatted accounts following UK GAAP structure.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Query ntran for all transactions in the year, grouped by account type/subtype
        result = sql_connector.execute_query(f"""
            SELECT
                RTRIM(t.nt_type) AS account_type,
                RTRIM(t.nt_subt) AS subtype,
                RTRIM(t.nt_acnt) AS account_code,
                RTRIM(n.na_desc) AS description,
                ISNULL(SUM(t.nt_value), 0) AS value
            FROM ntran t
            LEFT JOIN nacnt n ON RTRIM(t.nt_acnt) = RTRIM(n.na_acnt)
            WHERE t.nt_year = {year}
            GROUP BY t.nt_type, t.nt_subt, t.nt_acnt, n.na_desc
            HAVING ISNULL(SUM(t.nt_value), 0) <> 0
            ORDER BY t.nt_type, t.nt_subt, t.nt_acnt
        """)

        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        # UK Statutory Account Structure
        # P&L: E=Sales (Credit), F=Cost of Sales (Debit), G=Overheads (Debit)
        # Balance Sheet: A=Fixed Assets, B=Current Assets, C=Current Liabilities, D=Capital & Reserves

        # Build P&L
        turnover = []
        cost_of_sales = []
        administrative_expenses = []
        other_income = []

        # Build Balance Sheet
        fixed_assets = []
        current_assets = []
        current_liabilities = []
        capital_reserves = []

        for r in result:
            atype = (r.get("account_type") or "").strip()
            value = r.get("value", 0) or 0
            item = {
                "code": r.get("account_code", "").strip(),
                "description": r.get("description", ""),
                "value": value
            }

            if atype == "E":  # Sales (typically credit/negative in ledger)
                turnover.append(item)
            elif atype == "F":  # Cost of Sales
                cost_of_sales.append(item)
            elif atype == "G":  # Overheads/Admin
                administrative_expenses.append(item)
            elif atype == "H":  # Other
                other_income.append(item)
            elif atype == "A":  # Fixed Assets
                fixed_assets.append(item)
            elif atype == "B":  # Current Assets
                current_assets.append(item)
            elif atype == "C":  # Current Liabilities
                current_liabilities.append(item)
            elif atype == "D":  # Capital & Reserves
                capital_reserves.append(item)

        # Calculate P&L totals (Sales are negative/credit, so negate for display)
        total_turnover = -sum(i["value"] for i in turnover)  # Negate credits
        total_cos = sum(i["value"] for i in cost_of_sales)
        gross_profit = total_turnover - total_cos

        total_admin = sum(i["value"] for i in administrative_expenses)
        total_other = -sum(i["value"] for i in other_income)  # Negate if credit
        operating_profit = gross_profit - total_admin + total_other

        # Calculate Balance Sheet totals
        total_fixed = sum(i["value"] for i in fixed_assets)
        total_current_assets = sum(i["value"] for i in current_assets)
        total_current_liab = -sum(i["value"] for i in current_liabilities)  # Liabilities are credits
        net_current_assets = total_current_assets - total_current_liab
        total_assets_less_liab = total_fixed + net_current_assets
        total_capital = -sum(i["value"] for i in capital_reserves)  # Capital is credit

        return {
            "success": True,
            "year": year,
            "profit_and_loss": {
                "turnover": {
                    "items": turnover,
                    "total": total_turnover
                },
                "cost_of_sales": {
                    "items": cost_of_sales,
                    "total": total_cos
                },
                "gross_profit": gross_profit,
                "administrative_expenses": {
                    "items": administrative_expenses,
                    "total": total_admin
                },
                "other_operating_income": {
                    "items": other_income,
                    "total": total_other
                },
                "operating_profit": operating_profit,
                "profit_before_tax": operating_profit,
                "profit_after_tax": operating_profit  # Simplified - no tax calc
            },
            "balance_sheet": {
                "fixed_assets": {
                    "items": fixed_assets,
                    "total": total_fixed
                },
                "current_assets": {
                    "items": current_assets,
                    "total": total_current_assets
                },
                "current_liabilities": {
                    "items": current_liabilities,
                    "total": total_current_liab
                },
                "net_current_assets": net_current_assets,
                "total_assets_less_current_liabilities": total_assets_less_liab,
                "capital_and_reserves": {
                    "items": capital_reserves,
                    "total": total_capital
                }
            }
        }

    except Exception as e:
        logger.error(f"Statutory accounts query failed: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/credit-control/query")
async def credit_control_query(request: CreditControlQueryRequest):
    """
    Answer credit control questions using LIVE SQL queries.
    More accurate than RAG for precise financial data.
    Supports all 15+ credit control agent query types.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    question = request.question.lower()

    try:
        # Find matching query based on keywords (list is ordered by priority)
        matched_query = None
        for query_info in CREDIT_CONTROL_QUERIES:
            if any(kw in question for kw in query_info["keywords"]):
                matched_query = query_info
                break

        # Handle dynamic lookups that need parameter extraction
        if matched_query and matched_query["name"] == "customer_lookup":
            # Extract customer name from question
            words = request.question.split()
            search_terms = [w for w in words if len(w) > 2 and w.lower() not in
                          ["customer", "account", "find", "lookup", "details", "for", "info", "what", "about", "the", "tell", "me"]]
            if search_terms:
                search = "%".join(search_terms)
                sql = f"""SELECT sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                          sn_crlim AS credit_limit, sn_stop AS on_stop, sn_dormant AS dormant,
                          sn_teleno AS phone, sn_email AS email, sn_contact AS contact,
                          sn_lastrec AS last_payment, sn_lastinv AS last_invoice,
                          sn_priorty AS priority, sn_cmgroup AS credit_group,
                          sn_memo AS memo, sn_crdnotes AS credit_notes
                          FROM sname WHERE sn_name LIKE '%{search}%' OR sn_account LIKE '%{search}%'"""
            else:
                return {"success": False, "error": "Could not extract customer name from question"}

        elif matched_query and matched_query["name"] == "invoice_lookup":
            # Extract invoice number from question
            import re
            invoice_match = re.search(r'([A-Z0-9]{4,})', request.question.upper())
            if invoice_match:
                invoice = invoice_match.group(1)
                sql = f"""SELECT st.st_account AS account, sn.sn_name AS customer,
                         st.st_trref AS invoice, st.st_custref AS your_ref,
                         st.st_trdate AS invoice_date, st.st_dueday AS due_date,
                         st.st_trvalue AS original_value, st.st_trbal AS outstanding,
                         st.st_dispute AS disputed, st.st_memo AS memo,
                         st.st_payday AS promise_date,
                         DATEDIFF(day, st.st_dueday, GETDATE()) AS days_overdue
                         FROM stran st
                         JOIN sname sn ON st.st_account = sn.sn_account
                         WHERE st.st_trref LIKE '%{invoice}%' AND st.st_trtype = 'I'"""
            else:
                return {"success": False, "error": "Could not extract invoice number from question"}

        elif matched_query and matched_query["name"] == "customer_master":
            # Extract customer name/account from question
            words = request.question.split()
            search_terms = [w for w in words if len(w) > 2 and w.lower() not in
                          ["customer", "master", "details", "record", "for", "info", "get", "show"]]
            if search_terms:
                search = "%".join(search_terms)
                sql = matched_query.get("param_sql", "").replace("{account}", search).replace("{customer}", search)
            else:
                # Return all customers if no specific one mentioned
                sql = """SELECT TOP 50 sn_account AS account, sn_name AS customer,
                        sn_currbal AS balance, sn_crlim AS credit_limit,
                        sn_teleno AS phone, sn_email AS email
                        FROM sname WHERE sn_currbal > 0 ORDER BY sn_currbal DESC"""

        elif matched_query:
            sql = matched_query["sql"]
        else:
            # Default: return summary of problem accounts
            sql = """SELECT TOP 20 sn_account AS account, sn_name AS customer, sn_currbal AS balance,
                     sn_crlim AS credit_limit,
                     CASE WHEN sn_currbal > sn_crlim AND sn_crlim > 0 THEN 'OVER LIMIT'
                          WHEN sn_stop = 1 THEN 'ON STOP' ELSE 'OK' END AS status
                     FROM sname WHERE sn_currbal > 0 ORDER BY sn_currbal DESC"""
            matched_query = {"name": "summary", "category": "general", "description": "Summary of accounts with balances"}

        # Execute the SQL
        result = sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        if not result:
            return {
                "success": True,
                "query_type": matched_query["name"] if matched_query else "unknown",
                "category": matched_query.get("category", "general") if matched_query else "general",
                "description": matched_query["description"] if matched_query else "No matching query",
                "data": [],
                "count": 0,
                "summary": "No results found"
            }

        # Generate a summary based on query type
        query_name = matched_query["name"] if matched_query else "unknown"
        count = len(result)

        # Custom summaries for different query types
        if query_name == "top_debtors":
            total = sum(r.get("balance", 0) or 0 for r in result)
            summary = f"Top {count} debtors owe a total of £{total:,.2f}. Highest: {result[0]['customer'].strip()} (£{result[0]['balance']:,.2f})"
        elif query_name == "over_credit_limit":
            total_over = sum(r.get("over_by", 0) or 0 for r in result)
            summary = f"{count} customers are over their credit limit by a total of £{total_over:,.2f}"
        elif query_name == "accounts_on_stop":
            total_value = sum(r.get("balance", 0) or 0 for r in result)
            summary = f"{count} accounts on stop with total debt of £{total_value:,.2f}"
        elif query_name == "overdue_invoices":
            total_overdue = sum(r.get("outstanding", 0) or 0 for r in result)
            summary = f"{count} overdue invoices totaling £{total_overdue:,.2f}"
        elif query_name == "unallocated_cash":
            total_unalloc = sum(r.get("unallocated_amount", 0) or 0 for r in result)
            summary = f"{count} receipts with £{total_unalloc:,.2f} unallocated cash"
        elif query_name == "pending_credit_notes":
            total_credits = sum(r.get("credit_amount", 0) or 0 for r in result)
            summary = f"{count} pending credit notes totaling £{total_credits:,.2f}"
        elif query_name == "disputed_invoices":
            total_disputed = sum(r.get("outstanding", 0) or 0 for r in result)
            summary = f"{count} invoices in dispute totaling £{total_disputed:,.2f}"
        elif query_name == "promises_due":
            total_promised = sum(r.get("outstanding", 0) or 0 for r in result)
            summary = f"{count} promises due/overdue totaling £{total_promised:,.2f}"
        elif query_name == "broken_promises":
            total_broken = sum(r.get("broken_promise_count", 0) or 0 for r in result)
            summary = f"{count} customers with {total_broken} total broken promises"
        elif query_name == "unallocated_cash_old":
            total_old = sum(r.get("unallocated_amount", 0) or 0 for r in result)
            summary = f"{count} receipts with £{total_old:,.2f} unallocated for over 7 days - needs reconciliation"
        elif query_name == "overdue_by_age":
            total = sum(r.get("total_overdue", 0) or 0 for r in result)
            summary = f"{count} customers with £{total:,.2f} overdue debt across all age brackets"
        elif query_name == "customer_balance_aging":
            total = sum(r.get("total_balance", 0) or 0 for r in result)
            summary = f"{count} customers with £{total:,.2f} total outstanding balance"
        elif query_name == "recent_payments":
            total_paid = sum(r.get("amount", 0) or 0 for r in result)
            summary = f"{count} recent payments totaling £{total_paid:,.2f}"
        elif query_name == "account_status":
            summary = f"{count} accounts with stop, hold, or dormant flags"
        elif query_name == "customer_notes":
            summary = f"{count} notes/contact log entries found"
        else:
            summary = f"Found {count} records"

        return {
            "success": True,
            "query_type": query_name,
            "category": matched_query.get("category", "general") if matched_query else "general",
            "description": matched_query["description"] if matched_query else "Custom query",
            "data": result,
            "count": count,
            "summary": summary,
            "sql_used": sql
        }

    except Exception as e:
        logger.error(f"Credit control query failed: {e}")
        return {"success": False, "error": str(e)}


# ============ Cashflow Forecast Endpoint ============

@app.get("/api/cashflow/forecast")
async def cashflow_forecast(years_history: int = 3):
    """
    Generate cashflow forecast based on historical transaction patterns.
    Analyzes receipts (money in) and payments (money out) to predict monthly cashflow.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from datetime import datetime
        from collections import defaultdict

        current_year = datetime.now().year
        current_month = datetime.now().month

        # First, get the date range of available data
        date_range_sql = """
            SELECT
                MIN(st_trdate) as min_date,
                MAX(st_trdate) as max_date,
                DATEDIFF(year, MIN(st_trdate), MAX(st_trdate)) + 1 as years_span
            FROM stran
            WHERE st_trtype = 'R'
        """
        date_range = sql_connector.execute_query(date_range_sql)
        if hasattr(date_range, 'to_dict'):
            date_range = date_range.to_dict('records')

        # Use all available data if years_history covers more than available
        # Or limit to requested years from the most recent data
        use_all_data = True  # Default to using all available historical data

        # Get historical receipts by month (from sales ledger)
        if use_all_data:
            receipts_sql = """
                SELECT
                    YEAR(st_trdate) AS year,
                    MONTH(st_trdate) AS month,
                    COUNT(*) AS transaction_count,
                    SUM(ABS(st_trvalue)) AS total_amount
                FROM stran
                WHERE st_trtype = 'R'
                GROUP BY YEAR(st_trdate), MONTH(st_trdate)
                ORDER BY year, month
            """
        else:
            receipts_sql = f"""
                SELECT
                    YEAR(st_trdate) AS year,
                    MONTH(st_trdate) AS month,
                    COUNT(*) AS transaction_count,
                    SUM(ABS(st_trvalue)) AS total_amount
                FROM stran
                WHERE st_trtype = 'R'
                AND st_trdate >= DATEADD(year, -{years_history}, GETDATE())
                GROUP BY YEAR(st_trdate), MONTH(st_trdate)
                ORDER BY year, month
            """
        receipts_result = sql_connector.execute_query(receipts_sql)
        if hasattr(receipts_result, 'to_dict'):
            receipts_result = receipts_result.to_dict('records')

        # Get historical payments by month (from purchase ledger)
        if use_all_data:
            payments_sql = """
                SELECT
                    YEAR(pt_trdate) AS year,
                    MONTH(pt_trdate) AS month,
                    COUNT(*) AS transaction_count,
                    SUM(ABS(pt_trvalue)) AS total_amount
                FROM ptran
                WHERE pt_trtype = 'P'
                GROUP BY YEAR(pt_trdate), MONTH(pt_trdate)
                ORDER BY year, month
            """
        else:
            payments_sql = f"""
                SELECT
                    YEAR(pt_trdate) AS year,
                    MONTH(pt_trdate) AS month,
                    COUNT(*) AS transaction_count,
                    SUM(ABS(pt_trvalue)) AS total_amount
                FROM ptran
                WHERE pt_trtype = 'P'
                AND pt_trdate >= DATEADD(year, -{years_history}, GETDATE())
                GROUP BY YEAR(pt_trdate), MONTH(pt_trdate)
                ORDER BY year, month
            """
        payments_result = sql_connector.execute_query(payments_sql)
        if hasattr(payments_result, 'to_dict'):
            payments_result = payments_result.to_dict('records')

        # Get payroll history (weekly pay converted to monthly estimates)
        # whist uses tax year format (1819 = tax year 2018/19, April to April)
        # wh_period is the week number (1-52)
        payroll_sql = """
            SELECT
                wh_year as tax_year,
                wh_period as week,
                SUM(CAST(wh_net AS FLOAT)) as net_pay,
                SUM(CAST(wh_erni AS FLOAT)) as employer_ni
            FROM whist
            GROUP BY wh_year, wh_period
            ORDER BY wh_year DESC, wh_period DESC
        """
        payroll_result = sql_connector.execute_query(payroll_sql)
        if hasattr(payroll_result, 'to_dict'):
            payroll_result = payroll_result.to_dict('records')

        # Get recurring expenses from nominal ledger by period
        # Focus on key expense categories that represent cash outflows
        expenses_sql = """
            SELECT
                nt_year as year,
                nt_period as period,
                SUM(CASE WHEN nt_acnt LIKE 'W%' THEN CAST(nt_value AS FLOAT) ELSE 0 END) as payroll_related,
                SUM(CASE WHEN nt_acnt IN ('Q125') THEN CAST(nt_value AS FLOAT) ELSE 0 END) as rent_rates,
                SUM(CASE WHEN nt_acnt IN ('Q120') THEN CAST(nt_value AS FLOAT) ELSE 0 END) as utilities,
                SUM(CASE WHEN nt_acnt LIKE 'Q13%' THEN CAST(nt_value AS FLOAT) ELSE 0 END) as insurance,
                SUM(CASE WHEN nt_acnt IN ('U230') THEN CAST(nt_value AS FLOAT) ELSE 0 END) as loan_interest
            FROM ntran
            WHERE nt_type = '45'  -- Expenses / Overheads
            GROUP BY nt_year, nt_period
            ORDER BY nt_year DESC, nt_period DESC
        """
        expenses_result = sql_connector.execute_query(expenses_sql)
        if hasattr(expenses_result, 'to_dict'):
            expenses_result = expenses_result.to_dict('records')

        # Build historical data structure
        historical_receipts = defaultdict(list)  # month -> [amounts]
        historical_payments = defaultdict(list)  # month -> [amounts]
        historical_payroll = defaultdict(list)  # month -> [amounts]
        historical_recurring = defaultdict(list)  # month -> [amounts]

        for r in receipts_result or []:
            month = int(r['month'])
            amount = float(r['total_amount'] or 0)
            historical_receipts[month].append(amount)

        for p in payments_result or []:
            month = int(p['month'])
            amount = float(p['total_amount'] or 0)
            historical_payments[month].append(amount)

        # Process payroll data - convert weekly to monthly
        # Tax year 1819 starts April 2018, 1718 starts April 2017, etc.
        # Week 1-4 = April (month 4), Week 5-8 = May (month 5), etc.
        for pay in payroll_result or []:
            week = int(pay['week'])
            net_pay = float(pay['net_pay'] or 0)
            employer_ni = float(pay['employer_ni'] or 0)
            total_payroll = net_pay + employer_ni

            # Convert week to month (approximate)
            # Week 1-4 = Month 4 (April), Week 5-8 = Month 5 (May), etc.
            month_offset = (week - 1) // 4
            month = ((month_offset + 3) % 12) + 1  # Tax year starts in April

            historical_payroll[month].append(total_payroll)

        # Process recurring expenses - ntran uses calendar periods (1-12)
        for exp in expenses_result or []:
            period = int(exp['period'])
            if 1 <= period <= 12:
                rent = float(exp['rent_rates'] or 0)
                utilities = float(exp['utilities'] or 0)
                insurance = float(exp['insurance'] or 0)
                loan_interest = float(exp['loan_interest'] or 0)
                total_recurring = rent + utilities + insurance + loan_interest
                if total_recurring > 0:
                    historical_recurring[period].append(total_recurring)

        # Calculate averages and build forecast for current year
        forecast = []
        month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December']

        annual_receipts_total = 0
        annual_payments_total = 0
        annual_payroll_total = 0
        annual_recurring_total = 0

        for month in range(1, 13):
            receipts_history = historical_receipts.get(month, [])
            payments_history = historical_payments.get(month, [])
            payroll_history = historical_payroll.get(month, [])
            recurring_history = historical_recurring.get(month, [])

            avg_receipts = sum(receipts_history) / len(receipts_history) if receipts_history else 0
            avg_payments = sum(payments_history) / len(payments_history) if payments_history else 0
            avg_payroll = sum(payroll_history) / len(payroll_history) if payroll_history else 0
            avg_recurring = sum(recurring_history) / len(recurring_history) if recurring_history else 0

            # Total expected payments includes purchase payments + payroll + recurring expenses
            total_expected_payments = avg_payments + avg_payroll + avg_recurring
            net_cashflow = avg_receipts - total_expected_payments

            # Determine if this is actual or forecast
            is_actual = month < current_month
            is_current = month == current_month

            annual_receipts_total += avg_receipts
            annual_payments_total += avg_payments
            annual_payroll_total += avg_payroll
            annual_recurring_total += avg_recurring

            forecast.append({
                "month": month,
                "month_name": month_names[month],
                "expected_receipts": round(avg_receipts, 2),
                "expected_payments": round(total_expected_payments, 2),
                "purchase_payments": round(avg_payments, 2),
                "payroll": round(avg_payroll, 2),
                "recurring_expenses": round(avg_recurring, 2),
                "net_cashflow": round(net_cashflow, 2),
                "receipts_data_points": len(receipts_history),
                "payments_data_points": len(payments_history) + len(payroll_history) + len(recurring_history),
                "status": "actual" if is_actual else ("current" if is_current else "forecast")
            })

        # Get actual YTD data for comparison
        ytd_receipts_sql = f"""
            SELECT SUM(ABS(st_trvalue)) AS total
            FROM stran
            WHERE st_trtype = 'R' AND YEAR(st_trdate) = {current_year}
        """
        ytd_receipts = sql_connector.execute_query(ytd_receipts_sql)
        if hasattr(ytd_receipts, 'to_dict'):
            ytd_receipts = ytd_receipts.to_dict('records')
        ytd_receipts_value = float(ytd_receipts[0]['total'] or 0) if ytd_receipts else 0

        ytd_payments_sql = f"""
            SELECT SUM(ABS(pt_trvalue)) AS total
            FROM ptran
            WHERE pt_trtype = 'P' AND YEAR(pt_trdate) = {current_year}
        """
        ytd_payments = sql_connector.execute_query(ytd_payments_sql)
        if hasattr(ytd_payments, 'to_dict'):
            ytd_payments = ytd_payments.to_dict('records')
        ytd_payments_value = float(ytd_payments[0]['total'] or 0) if ytd_payments else 0

        # Get bank balances
        bank_sql = "SELECT nk_acnt AS account, nk_desc AS description, nk_curbal AS balance FROM nbank"
        bank_result = sql_connector.execute_query(bank_sql)
        if hasattr(bank_result, 'to_dict'):
            bank_result = bank_result.to_dict('records')

        total_bank_balance = sum(float(b['balance'] or 0) for b in bank_result or [])

        # Total expected payments includes all categories
        total_annual_payments = annual_payments_total + annual_payroll_total + annual_recurring_total

        return {
            "success": True,
            "forecast_year": current_year,
            "years_of_history": years_history,
            "monthly_forecast": forecast,
            "summary": {
                "annual_expected_receipts": round(annual_receipts_total, 2),
                "annual_expected_payments": round(total_annual_payments, 2),
                "annual_purchase_payments": round(annual_payments_total, 2),
                "annual_payroll": round(annual_payroll_total, 2),
                "annual_recurring_expenses": round(annual_recurring_total, 2),
                "annual_expected_net": round(annual_receipts_total - total_annual_payments, 2),
                "ytd_actual_receipts": round(ytd_receipts_value, 2),
                "ytd_actual_payments": round(ytd_payments_value, 2),
                "ytd_actual_net": round(ytd_receipts_value - ytd_payments_value, 2),
                "current_bank_balance": round(total_bank_balance, 2)
            },
            "bank_accounts": bank_result or []
        }

    except Exception as e:
        logger.error(f"Cashflow forecast failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/cashflow/history")
async def cashflow_history():
    """
    Get detailed cashflow history by year and month.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get all receipts by year/month
        receipts_sql = """
            SELECT
                YEAR(st_trdate) AS year,
                MONTH(st_trdate) AS month,
                COUNT(*) AS count,
                SUM(ABS(st_trvalue)) AS total
            FROM stran
            WHERE st_trtype = 'R'
            GROUP BY YEAR(st_trdate), MONTH(st_trdate)
            ORDER BY year DESC, month DESC
        """
        receipts = sql_connector.execute_query(receipts_sql)
        if hasattr(receipts, 'to_dict'):
            receipts = receipts.to_dict('records')

        # Get all payments by year/month
        payments_sql = """
            SELECT
                YEAR(pt_trdate) AS year,
                MONTH(pt_trdate) AS month,
                COUNT(*) AS count,
                SUM(ABS(pt_trvalue)) AS total
            FROM ptran
            WHERE pt_trtype = 'P'
            GROUP BY YEAR(pt_trdate), MONTH(pt_trdate)
            ORDER BY year DESC, month DESC
        """
        payments = sql_connector.execute_query(payments_sql)
        if hasattr(payments, 'to_dict'):
            payments = payments.to_dict('records')

        # Combine into yearly summary
        yearly_data = {}
        for r in receipts or []:
            year = int(r['year'])
            month = int(r['month'])
            if year not in yearly_data:
                yearly_data[year] = {"receipts": {}, "payments": {}}
            yearly_data[year]["receipts"][month] = float(r['total'] or 0)

        for p in payments or []:
            year = int(p['year'])
            month = int(p['month'])
            if year not in yearly_data:
                yearly_data[year] = {"receipts": {}, "payments": {}}
            yearly_data[year]["payments"][month] = float(p['total'] or 0)

        # Format output
        history = []
        for year in sorted(yearly_data.keys(), reverse=True):
            year_receipts = sum(yearly_data[year]["receipts"].values())
            year_payments = sum(yearly_data[year]["payments"].values())
            history.append({
                "year": year,
                "total_receipts": round(year_receipts, 2),
                "total_payments": round(year_payments, 2),
                "net_cashflow": round(year_receipts - year_payments, 2),
                "monthly_receipts": yearly_data[year]["receipts"],
                "monthly_payments": yearly_data[year]["payments"]
            })

        return {
            "success": True,
            "history": history
        }

    except Exception as e:
        logger.error(f"Cashflow history failed: {e}")
        return {"success": False, "error": str(e)}


# ============ LLM Test Endpoint ============

@app.post("/api/llm/test")
async def test_llm(prompt: str = Query("Hello, how are you?", description="Test prompt")):
    """Test the LLM connection."""
    if not llm:
        raise HTTPException(status_code=503, detail="LLM not initialized")

    try:
        response = llm.get_completion(prompt)
        return {"success": True, "response": response}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============ Email Module Helper Functions ============

async def _initialize_email_providers():
    """Initialize email providers from config."""
    global email_storage, email_sync_manager

    if not email_storage or not email_sync_manager or not config:
        return

    # Check for IMAP provider
    if config.has_section('email_imap') and config.getboolean('email_imap', 'enabled', fallback=False):
        try:
            imap_config = {
                'server': config.get('email_imap', 'server', fallback=''),
                'port': config.getint('email_imap', 'port', fallback=993),
                'username': config.get('email_imap', 'username', fallback=''),
                'password': config.get('email_imap', 'password', fallback=''),
                'use_ssl': config.getboolean('email_imap', 'use_ssl', fallback=True),
            }

            if imap_config['server'] and imap_config['username']:
                # Check if provider already exists in DB
                providers = email_storage.get_all_providers()
                imap_provider_db = next((p for p in providers if p['provider_type'] == 'imap'), None)

                if not imap_provider_db:
                    provider_id = email_storage.add_provider(
                        name='IMAP',
                        provider_type=ProviderType.IMAP,
                        config=imap_config
                    )
                else:
                    provider_id = imap_provider_db['id']

                provider = IMAPProvider(imap_config)
                email_sync_manager.register_provider(provider_id, provider)
                logger.info("IMAP provider registered")
        except Exception as e:
            logger.warning(f"Could not initialize IMAP provider: {e}")


# ============ Email Pydantic Models ============

class EmailProviderCreate(BaseModel):
    name: str
    provider_type: Literal['microsoft', 'gmail', 'imap']
    # IMAP config
    server: Optional[str] = None
    port: Optional[int] = 993
    username: Optional[str] = None
    password: Optional[str] = None
    use_ssl: bool = True
    # Microsoft config
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    # Gmail config
    credentials_json: Optional[str] = None
    # Common
    user_email: Optional[str] = None


class EmailProviderResponse(BaseModel):
    id: int
    name: str
    provider_type: str
    enabled: bool
    last_sync: Optional[str]
    sync_status: str


class EmailListParams(BaseModel):
    provider_id: Optional[int] = None
    folder_id: Optional[int] = None
    category: Optional[str] = None
    linked_account: Optional[str] = None
    is_read: Optional[bool] = None
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    search: Optional[str] = None
    page: int = 1
    page_size: int = 50


class EmailLinkRequest(BaseModel):
    account_code: str


class CategoryUpdateRequest(BaseModel):
    category: Literal['payment', 'query', 'complaint', 'order', 'other']
    reason: Optional[str] = None


# ============ Email API Endpoints ============

@app.get("/api/email/providers")
async def list_email_providers():
    """List all configured email providers."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        providers = email_storage.get_all_providers()
        # Remove sensitive config data
        for p in providers:
            if 'config' in p:
                del p['config']
            if 'config_json' in p:
                del p['config_json']
        return {"success": True, "providers": providers}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/email/providers")
async def add_email_provider(provider: EmailProviderCreate):
    """Add a new email provider."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        # Build config based on provider type
        if provider.provider_type == 'imap':
            provider_config = {
                'server': provider.server,
                'port': provider.port,
                'username': provider.username,
                'password': provider.password,
                'use_ssl': provider.use_ssl,
            }
        elif provider.provider_type == 'microsoft':
            provider_config = {
                'tenant_id': provider.tenant_id,
                'client_id': provider.client_id,
                'client_secret': provider.client_secret,
                'user_email': provider.user_email,
            }
        elif provider.provider_type == 'gmail':
            provider_config = {
                'credentials_json': provider.credentials_json,
                'user_email': provider.user_email,
            }
        else:
            return {"success": False, "error": "Invalid provider type"}

        provider_id = email_storage.add_provider(
            name=provider.name,
            provider_type=ProviderType(provider.provider_type),
            config=provider_config
        )

        # Register provider with sync manager if IMAP
        if provider.provider_type == 'imap' and email_sync_manager:
            imap_provider = IMAPProvider(provider_config)
            email_sync_manager.register_provider(provider_id, imap_provider)

        return {"success": True, "provider_id": provider_id}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.delete("/api/email/providers/{provider_id}")
async def delete_email_provider(provider_id: int):
    """Delete an email provider."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        if email_sync_manager:
            email_sync_manager.unregister_provider(provider_id)

        result = email_storage.delete_provider(provider_id)
        return {"success": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/email/providers/{provider_id}/test")
async def test_email_provider(provider_id: int):
    """Test connection to an email provider."""
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email module not initialized")

    try:
        if provider_id not in email_sync_manager.providers:
            return {"success": False, "error": "Provider not registered"}

        provider = email_sync_manager.providers[provider_id]
        result = await provider.test_connection()
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/email/providers/{provider_id}/folders")
async def list_email_folders(provider_id: int):
    """List folders for an email provider."""
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email module not initialized")

    try:
        if provider_id not in email_sync_manager.providers:
            return {"success": False, "error": "Provider not registered"}

        provider = email_sync_manager.providers[provider_id]
        if not provider.is_authenticated:
            await provider.authenticate()

        folders = await provider.list_folders()

        # Also get stored folder info
        stored_folders = email_storage.get_folders(provider_id)
        stored_map = {f['folder_id']: f for f in stored_folders}

        result = []
        for folder in folders:
            folder_dict = folder.to_dict()
            if folder.folder_id in stored_map:
                folder_dict['monitored'] = bool(stored_map[folder.folder_id].get('monitored', False))
                folder_dict['db_id'] = stored_map[folder.folder_id]['id']
            else:
                folder_dict['monitored'] = False
                folder_dict['db_id'] = None
            result.append(folder_dict)

        return {"success": True, "folders": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/email/messages")
async def list_emails(
    provider_id: Optional[int] = None,
    folder_id: Optional[int] = None,
    category: Optional[str] = None,
    linked_account: Optional[str] = None,
    is_read: Optional[bool] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    search: Optional[str] = None,
    page: int = 1,
    page_size: int = 50
):
    """List emails with filtering and pagination."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        # Parse dates
        from_dt = datetime.fromisoformat(from_date) if from_date else None
        to_dt = datetime.fromisoformat(to_date) if to_date else None

        result = email_storage.get_emails(
            provider_id=provider_id,
            folder_id=folder_id,
            category=category,
            linked_account=linked_account,
            is_read=is_read,
            from_date=from_dt,
            to_date=to_dt,
            search=search,
            page=page,
            page_size=page_size
        )

        # Add customer names for linked emails
        if customer_linker:
            for email in result['emails']:
                if email.get('linked_account'):
                    name = customer_linker.get_customer_name(email['linked_account'])
                    email['linked_customer_name'] = name

        return {"success": True, **result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/email/messages/{email_id}")
async def get_email_detail(email_id: int):
    """Get full email details."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        email = email_storage.get_email_by_id(email_id)
        if not email:
            raise HTTPException(status_code=404, detail="Email not found")

        # Add customer name if linked
        if email.get('linked_account') and customer_linker:
            email['linked_customer_name'] = customer_linker.get_customer_name(email['linked_account'])

        return {"success": True, "email": email}
    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/email/sync")
async def trigger_email_sync(provider_id: Optional[int] = None):
    """Trigger email sync (all providers or specific one)."""
    if not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email sync manager not initialized")

    try:
        if provider_id:
            result = await email_sync_manager.sync_provider(provider_id)
        else:
            result = await email_sync_manager.sync_all_providers()
        return {"success": True, "result": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/email/sync/status")
async def get_sync_status():
    """Get email sync status."""
    if not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email sync manager not initialized")

    try:
        status = email_sync_manager.get_sync_status()
        return {"success": True, **status}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/email/sync/log")
async def get_sync_history(provider_id: Optional[int] = None, limit: int = 20):
    """Get sync history log."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        history = email_storage.get_sync_history(provider_id=provider_id, limit=limit)
        return {"success": True, "history": history}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.put("/api/email/messages/{email_id}/category")
async def update_email_category(email_id: int, request: CategoryUpdateRequest):
    """Update email category."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        result = email_storage.update_email_category(
            email_id=email_id,
            category=request.category,
            confidence=1.0,  # Manual override
            reason=request.reason or "Manual classification"
        )
        return {"success": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/email/messages/{email_id}/link")
async def link_email_to_customer(email_id: int, request: EmailLinkRequest):
    """Link an email to a customer account."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        result = email_storage.link_email_to_customer(
            email_id=email_id,
            account_code=request.account_code,
            linked_by='manual'
        )
        return {"success": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.delete("/api/email/messages/{email_id}/link")
async def unlink_email_from_customer(email_id: int):
    """Remove customer link from an email."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        result = email_storage.unlink_email(email_id)
        return {"success": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/email/by-customer/{account_code}")
async def get_emails_by_customer(account_code: str):
    """Get all emails linked to a customer account."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        emails = email_storage.get_emails_by_customer(account_code)
        return {"success": True, "emails": emails, "count": len(emails)}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/email/stats")
async def get_email_stats():
    """Get email statistics."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        stats = email_storage.get_email_stats()
        category_stats = email_storage.get_category_stats()
        return {
            "success": True,
            "stats": stats,
            "categories": category_stats
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/email/messages/{email_id}/categorize")
async def categorize_single_email(email_id: int):
    """Manually trigger AI categorization for an email."""
    if not email_storage or not email_categorizer:
        raise HTTPException(status_code=503, detail="Email module not initialized")

    try:
        email = email_storage.get_email_by_id(email_id)
        if not email:
            raise HTTPException(status_code=404, detail="Email not found")

        result = email_categorizer.categorize(
            subject=email.get('subject', ''),
            from_address=email.get('from_address', ''),
            body=email.get('body_text') or email.get('body_preview', '')
        )

        email_storage.update_email_category(
            email_id=email_id,
            category=result['category'],
            confidence=result['confidence'],
            reason=result.get('reason')
        )

        return {"success": True, "categorization": result}
    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============================================================
# Creditors Control API Endpoints (Purchase Ledger)
# ============================================================

@app.get("/api/creditors/dashboard")
async def creditors_dashboard():
    """
    Get creditors control dashboard with key metrics.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        metrics = {}

        # Total creditors balance
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(pn_currbal) AS total
               FROM pname WHERE pn_currbal <> 0"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["total_creditors"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Total Outstanding"
            }

        # Overdue invoices (past due date)
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(pt_trbal) AS total
               FROM ptran WHERE pt_trtype = 'I' AND pt_trbal > 0 AND pt_dueday < GETDATE()"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["overdue_invoices"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Overdue Invoices"
            }

        # Due within 7 days
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(pt_trbal) AS total
               FROM ptran WHERE pt_trtype = 'I' AND pt_trbal > 0
               AND pt_dueday >= GETDATE() AND pt_dueday < DATEADD(day, 7, GETDATE())"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["due_7_days"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Due in 7 Days"
            }

        # Due within 30 days
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(pt_trbal) AS total
               FROM ptran WHERE pt_trtype = 'I' AND pt_trbal > 0
               AND pt_dueday >= GETDATE() AND pt_dueday < DATEADD(day, 30, GETDATE())"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["due_30_days"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Due in 30 Days"
            }

        # Recent payments made (last 7 days)
        result = sql_connector.execute_query(
            """SELECT COUNT(*) AS count, SUM(ABS(pt_trvalue)) AS total
               FROM ptran WHERE pt_trtype = 'P' AND pt_trdate >= DATEADD(day, -7, GETDATE())"""
        )
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result:
            metrics["recent_payments"] = {
                "value": float(result[0]["total"] or 0),
                "count": result[0]["count"] or 0,
                "label": "Payments (7 days)"
            }

        # Top suppliers by balance
        priority_result = sql_connector.execute_query(
            """SELECT TOP 10
                   RTRIM(pn_account) AS account,
                   RTRIM(pn_name) AS supplier,
                   pn_currbal AS balance,
                   pn_teleno AS phone,
                   pn_contact AS contact
               FROM pname
               WHERE pn_currbal > 0
               ORDER BY pn_currbal DESC"""
        )
        if hasattr(priority_result, 'to_dict'):
            priority_result = priority_result.to_dict('records')

        return {
            "success": True,
            "metrics": metrics,
            "top_suppliers": priority_result or []
        }

    except Exception as e:
        logger.error(f"Creditors dashboard query failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/creditors/report")
async def creditors_report():
    """
    Get aged creditors report with balance breakdown by aging period.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        result = sql_connector.execute_query("""
            SELECT
                RTRIM(pn.pn_account) AS account,
                RTRIM(pn.pn_name) AS supplier,
                pn.pn_currbal AS balance,
                ISNULL(ph.pi_current, 0) AS current_period,
                ISNULL(ph.pi_period1, 0) AS month_1,
                ISNULL(ph.pi_period2, 0) AS month_2,
                ISNULL(ph.pi_period3, 0) + ISNULL(ph.pi_period4, 0) + ISNULL(ph.pi_period5, 0) AS month_3_plus,
                pn.pn_teleno AS phone,
                pn.pn_contact AS contact
            FROM pname pn
            LEFT JOIN phist ph ON pn.pn_account = ph.pi_account AND ph.pi_age = 1
            WHERE pn.pn_currbal <> 0
            ORDER BY pn.pn_account
        """)

        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        # Calculate totals
        totals = {
            "balance": sum(r.get("balance", 0) or 0 for r in result),
            "current": sum(r.get("current_period", 0) or 0 for r in result),
            "month_1": sum(r.get("month_1", 0) or 0 for r in result),
            "month_2": sum(r.get("month_2", 0) or 0 for r in result),
            "month_3_plus": sum(r.get("month_3_plus", 0) or 0 for r in result),
        }

        return {
            "success": True,
            "data": result or [],
            "count": len(result) if result else 0,
            "totals": totals
        }

    except Exception as e:
        logger.error(f"Creditors report query failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/creditors/supplier/{account}")
async def get_supplier_details(account: str):
    """
    Get detailed information for a specific supplier.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get supplier header info
        supplier = sql_connector.execute_query(f"""
            SELECT
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS supplier_name,
                RTRIM(pn_addr1) AS address1,
                RTRIM(pn_addr2) AS address2,
                RTRIM(pn_addr3) AS address3,
                RTRIM(pn_addr4) AS address4,
                RTRIM(pn_pstcode) AS postcode,
                RTRIM(pn_teleno) AS phone,
                RTRIM(pn_contact) AS contact,
                RTRIM(pn_email) AS email,
                pn_currbal AS balance,
                pn_trnover AS turnover_ytd
            FROM pname
            WHERE pn_account = '{account}'
        """)

        if hasattr(supplier, 'to_dict'):
            supplier = supplier.to_dict('records')

        if not supplier:
            raise HTTPException(status_code=404, detail="Supplier not found")

        return {
            "success": True,
            "supplier": supplier[0]
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Supplier details query failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/creditors/supplier/{account}/transactions")
async def get_supplier_transactions(account: str, include_paid: bool = False):
    """
    Get outstanding transactions for a specific supplier.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        balance_filter = "" if include_paid else "AND pt_trbal <> 0"

        transactions = sql_connector.execute_query(f"""
            SELECT
                RTRIM(pt_account) AS account,
                pt_trdate AS date,
                RTRIM(pt_trref) AS reference,
                CASE pt_trtype
                    WHEN 'I' THEN 'Invoice'
                    WHEN 'C' THEN 'Credit Note'
                    WHEN 'P' THEN 'Payment'
                    WHEN 'J' THEN 'Journal'
                    ELSE pt_trtype
                END AS type,
                RTRIM(pt_supref) AS description,
                pt_trvalue AS value,
                pt_trbal AS balance,
                pt_dueday AS due_date,
                CASE
                    WHEN pt_trtype = 'I' AND pt_trbal > 0 AND pt_dueday < GETDATE()
                    THEN DATEDIFF(day, pt_dueday, GETDATE())
                    ELSE 0
                END AS days_overdue
            FROM ptran
            WHERE pt_account = '{account}'
            {balance_filter}
            ORDER BY pt_trdate DESC, pt_trref
        """)

        if hasattr(transactions, 'to_dict'):
            transactions = transactions.to_dict('records')

        # Calculate totals
        total_invoices = sum(t['value'] for t in transactions if t.get('type') == 'Invoice')
        total_credits = sum(abs(t['value']) for t in transactions if t.get('type') == 'Credit Note')
        total_payments = sum(abs(t['value']) for t in transactions if t.get('type') == 'Payment')
        balance = sum(t.get('balance', 0) or 0 for t in transactions)

        return {
            "success": True,
            "transactions": transactions or [],
            "count": len(transactions) if transactions else 0,
            "summary": {
                "total_invoices": total_invoices,
                "total_credits": total_credits,
                "total_payments": total_payments,
                "balance": balance
            }
        }

    except Exception as e:
        logger.error(f"Supplier transactions query failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/creditors/supplier/{account}/statement")
async def get_supplier_statement(account: str, from_date: str = None, to_date: str = None):
    """
    Generate a supplier statement showing outstanding transactions only.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from datetime import datetime

        # Get supplier info
        supplier = sql_connector.execute_query(f"""
            SELECT
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS supplier_name,
                RTRIM(pn_addr1) AS address1,
                RTRIM(pn_addr2) AS address2,
                RTRIM(pn_addr3) AS address3,
                RTRIM(pn_addr4) AS address4,
                RTRIM(pn_pstcode) AS postcode,
                pn_currbal AS current_balance
            FROM pname
            WHERE pn_account = '{account}'
        """)

        if hasattr(supplier, 'to_dict'):
            supplier = supplier.to_dict('records')

        if not supplier:
            raise HTTPException(status_code=404, detail="Supplier not found")

        # No opening balance for outstanding-only statement
        opening_balance = 0.0

        # Get outstanding transactions only (where balance is not zero)
        transactions = sql_connector.execute_query(f"""
            SELECT
                pt_trdate AS date,
                RTRIM(pt_trref) AS reference,
                CASE pt_trtype
                    WHEN 'I' THEN 'Invoice'
                    WHEN 'C' THEN 'Credit Note'
                    WHEN 'P' THEN 'Payment'
                    WHEN 'J' THEN 'Journal'
                    ELSE pt_trtype
                END AS type,
                RTRIM(pt_supref) AS description,
                CASE WHEN pt_trtype IN ('I', 'J') AND pt_trvalue > 0 THEN pt_trvalue ELSE 0 END AS debit,
                CASE WHEN pt_trtype IN ('C', 'P') OR pt_trvalue < 0 THEN ABS(pt_trvalue) ELSE 0 END AS credit,
                pt_trbal AS balance,
                pt_dueday AS due_date
            FROM ptran
            WHERE pt_account = '{account}'
            AND pt_trbal <> 0
            ORDER BY pt_trdate, pt_trref
        """)

        if hasattr(transactions, 'to_dict'):
            transactions = transactions.to_dict('records')

        # Calculate running balance from outstanding balances
        running_balance = 0.0
        for t in transactions:
            running_balance += t.get('balance', 0) or 0
            t['running_balance'] = running_balance

        # Calculate totals
        total_debits = sum(t.get('debit', 0) or 0 for t in transactions)
        total_credits = sum(t.get('credit', 0) or 0 for t in transactions)
        total_outstanding = sum(t.get('balance', 0) or 0 for t in transactions)

        return {
            "success": True,
            "supplier": supplier[0],
            "period": {
                "from_date": None,
                "to_date": datetime.now().strftime('%Y-%m-%d')
            },
            "opening_balance": opening_balance,
            "transactions": transactions or [],
            "totals": {
                "debits": total_debits,
                "credits": total_credits,
                "outstanding": total_outstanding
            },
            "closing_balance": total_outstanding
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Supplier statement query failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/creditors/search")
async def search_suppliers(query: str):
    """
    Search for suppliers by account code or name.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        results = sql_connector.execute_query(f"""
            SELECT TOP 20
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS supplier_name,
                pn_currbal AS balance,
                pn_teleno AS phone
            FROM pname
            WHERE pn_account LIKE '%{query}%' OR pn_name LIKE '%{query}%'
            ORDER BY pn_name
        """)

        if hasattr(results, 'to_dict'):
            results = results.to_dict('records')

        return {
            "success": True,
            "suppliers": results or [],
            "count": len(results) if results else 0
        }

    except Exception as e:
        logger.error(f"Supplier search failed: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Live Opera Dashboards API Endpoints
# ============================================================

def df_to_records(df):
    """Convert DataFrame to list of dicts."""
    if hasattr(df, 'to_dict'):
        return df.to_dict('records')
    return df

@app.get("/api/dashboard/ceo-kpis")
async def get_ceo_kpis(year: int = 2026):
    """Get CEO-level KPIs: MTD, QTD, YTD sales, growth, customer metrics."""
    try:
        from datetime import datetime as dt

        current_date = dt.now()
        current_month = current_date.month
        current_quarter = (current_month - 1) // 3 + 1

        # Get current year and previous year sales
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period,
                SUM(CASE WHEN nt_type = 'E' THEN -nt_value ELSE 0 END) as revenue,
                SUM(CASE WHEN nt_type = 'F' THEN nt_value ELSE 0 END) as cost_of_sales
            FROM ntran
            WHERE nt_type IN ('E', 'F')
            AND nt_year IN ({year}, {year - 1})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Process data
        current_year_data = {}
        prev_year_data = {}
        for row in data:
            y = int(row['nt_year'])
            p = int(row['nt_period']) if row['nt_period'] else 0
            if y == year:
                current_year_data[p] = row['revenue'] or 0
            else:
                prev_year_data[p] = row['revenue'] or 0

        # Calculate MTD, QTD, YTD
        ytd = sum(current_year_data.get(p, 0) for p in range(1, current_month + 1))
        mtd = current_year_data.get(current_month, 0)

        quarter_start = (current_quarter - 1) * 3 + 1
        qtd = sum(current_year_data.get(p, 0) for p in range(quarter_start, current_month + 1))

        # Previous year comparisons
        ytd_prev = sum(prev_year_data.get(p, 0) for p in range(1, current_month + 1))
        yoy_growth = ((ytd - ytd_prev) / ytd_prev * 100) if ytd_prev != 0 else 0

        # Rolling averages (use previous year data for full year average)
        all_months = sorted(prev_year_data.keys())
        monthly_values = [prev_year_data[m] for m in all_months if m > 0]
        avg_3m = sum(monthly_values[-3:]) / min(3, len(monthly_values)) if monthly_values else 0
        avg_6m = sum(monthly_values[-6:]) / min(6, len(monthly_values)) if monthly_values else 0
        avg_12m = sum(monthly_values[-12:]) / min(12, len(monthly_values)) if monthly_values else 0

        # Active customers (had transactions this year)
        cust_df = sql_connector.execute_query(f"""
            SELECT COUNT(DISTINCT st_account) as active_customers
            FROM stran
            WHERE st_trtype = 'I' AND YEAR(st_trdate) = {year}
        """)
        cust_data = df_to_records(cust_df)
        active_customers = cust_data[0]['active_customers'] if cust_data else 0

        # Revenue per customer
        rev_per_cust = ytd / active_customers if active_customers > 0 else 0

        return {
            "success": True,
            "kpis": {
                "mtd": round(mtd, 2),
                "qtd": round(qtd, 2),
                "ytd": round(ytd, 2),
                "yoy_growth_percent": round(yoy_growth, 1),
                "avg_monthly_3m": round(avg_3m, 2),
                "avg_monthly_6m": round(avg_6m, 2),
                "avg_monthly_12m": round(avg_12m, 2),
                "active_customers": active_customers,
                "revenue_per_customer": round(rev_per_cust, 2),
                "year": year,
                "month": current_month,
                "quarter": current_quarter
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/revenue-over-time")
async def get_revenue_over_time(year: int = 2026):
    """Get monthly revenue breakdown by category."""
    try:
        # Define revenue categories based on account codes
        category_mapping = """
            CASE
                WHEN nt_acnt LIKE 'E10%' OR nt_acnt LIKE 'E9%' THEN 'Support Contracts'
                WHEN nt_acnt LIKE 'E30%' OR nt_acnt LIKE 'E60%' THEN 'Hosting'
                WHEN nt_acnt LIKE 'E1040' OR nt_acnt LIKE 'E2040' OR nt_acnt LIKE 'E1050' THEN 'Consultancy/Development'
                WHEN nt_acnt LIKE 'E1030' OR nt_acnt LIKE 'E2030' OR nt_acnt LIKE 'E5100' THEN 'Training'
                WHEN nt_acnt LIKE 'E1000' OR nt_acnt LIKE 'E2000' OR nt_acnt LIKE 'E7000' THEN 'Software Licences'
                ELSE 'Other'
            END
        """

        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period as month,
                {category_mapping} as category,
                SUM(-nt_value) as revenue
            FROM ntran
            WHERE nt_type = 'E' AND nt_year IN ({year}, {year - 1})
            GROUP BY nt_year, nt_period, {category_mapping}
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Organize data by year and month
        data_by_year = {year: {}, year - 1: {}}
        categories = set()

        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            cat = row['category']
            rev = row['revenue'] or 0

            categories.add(cat)
            if m not in data_by_year[y]:
                data_by_year[y][m] = {}
            data_by_year[y][m][cat] = rev

        # Build monthly series
        months = []
        for m in range(1, 13):
            month_data = {
                "month": m,
                "month_name": ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][m],
                "current_year": data_by_year[year].get(m, {}),
                "previous_year": data_by_year[year - 1].get(m, {}),
                "current_total": sum(data_by_year[year].get(m, {}).values()),
                "previous_total": sum(data_by_year[year - 1].get(m, {}).values())
            }
            months.append(month_data)

        return {
            "success": True,
            "year": year,
            "categories": sorted(list(categories)),
            "months": months
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/revenue-composition")
async def get_revenue_composition(year: int = 2026):
    """Get revenue breakdown by category with comparison to previous year."""
    try:
        # More detailed category mapping
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                CASE
                    WHEN nt_acnt IN ('E1010', 'E1020', 'E1025', 'E2010', 'E2020', 'E7010', 'E7020', 'E9030') THEN 'Support Contracts'
                    WHEN nt_acnt LIKE 'E30%' OR nt_acnt LIKE 'E60%' OR nt_acnt = 'E9020' THEN 'Hosting/Cloud'
                    WHEN nt_acnt IN ('E1040', 'E2040', 'E1050', 'E7040', 'E1015', 'E6300') THEN 'Consultancy/Dev'
                    WHEN nt_acnt IN ('E1030', 'E2030', 'E5100', 'E7030') THEN 'Training'
                    WHEN nt_acnt IN ('E1000', 'E2000', 'E7000', 'E4000', 'E5500', 'E9000', 'E9010') THEN 'Software/Licences'
                    WHEN nt_acnt IN ('E5000', 'E5200', 'E8000') THEN 'Subscriptions'
                    ELSE 'Other'
                END as category,
                SUM(-nt_value) as revenue
            FROM ntran
            WHERE nt_type = 'E' AND nt_year IN ({year}, {year - 1})
            GROUP BY nt_year,
                CASE
                    WHEN nt_acnt IN ('E1010', 'E1020', 'E1025', 'E2010', 'E2020', 'E7010', 'E7020', 'E9030') THEN 'Support Contracts'
                    WHEN nt_acnt LIKE 'E30%' OR nt_acnt LIKE 'E60%' OR nt_acnt = 'E9020' THEN 'Hosting/Cloud'
                    WHEN nt_acnt IN ('E1040', 'E2040', 'E1050', 'E7040', 'E1015', 'E6300') THEN 'Consultancy/Dev'
                    WHEN nt_acnt IN ('E1030', 'E2030', 'E5100', 'E7030') THEN 'Training'
                    WHEN nt_acnt IN ('E1000', 'E2000', 'E7000', 'E4000', 'E5500', 'E9000', 'E9010') THEN 'Software/Licences'
                    WHEN nt_acnt IN ('E5000', 'E5200', 'E8000') THEN 'Subscriptions'
                    ELSE 'Other'
                END
            ORDER BY revenue DESC
        """)
        data = df_to_records(df)

        current_year = {}
        prev_year = {}

        for row in data:
            y = int(row['nt_year'])
            cat = row['category']
            rev = row['revenue'] or 0
            if y == year:
                current_year[cat] = rev
            else:
                prev_year[cat] = rev

        # Calculate totals and percentages
        current_total = sum(current_year.values())
        prev_total = sum(prev_year.values())

        categories = []
        all_cats = set(current_year.keys()) | set(prev_year.keys())

        for cat in sorted(all_cats, key=lambda x: current_year.get(x, 0), reverse=True):
            curr = current_year.get(cat, 0)
            prev = prev_year.get(cat, 0)
            categories.append({
                "category": cat,
                "current_year": round(curr, 2),
                "previous_year": round(prev, 2),
                "current_percent": round(curr / current_total * 100, 1) if current_total else 0,
                "previous_percent": round(prev / prev_total * 100, 1) if prev_total else 0,
                "change_percent": round((curr - prev) / prev * 100, 1) if prev else 0
            })

        return {
            "success": True,
            "year": year,
            "current_total": round(current_total, 2),
            "previous_total": round(prev_total, 2),
            "categories": categories
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/top-customers")
async def get_top_customers(year: int = 2026, limit: int = 20):
    """Get top customers by revenue with trends."""
    try:
        df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(t.st_account) as account_code,
                RTRIM(s.sn_name) as customer_name,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trvalue ELSE 0 END) as current_year,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year - 1} THEN t.st_trvalue ELSE 0 END) as previous_year,
                COUNT(DISTINCT CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trref END) as invoice_count
            FROM stran t
            INNER JOIN sname s ON RTRIM(t.st_account) = RTRIM(s.sn_account)
            WHERE t.st_trtype = 'I'
            AND YEAR(t.st_trdate) IN ({year}, {year - 1})
            GROUP BY t.st_account, s.sn_name
            HAVING SUM(CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trvalue ELSE 0 END) > 0
            ORDER BY SUM(CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trvalue ELSE 0 END) DESC
        """)
        data = df_to_records(df)

        total_revenue = sum(row['current_year'] or 0 for row in data)

        customers = []
        cumulative = 0
        for row in data[:limit]:
            curr = row['current_year'] or 0
            prev = row['previous_year'] or 0
            cumulative += curr

            trend = 'stable'
            if prev > 0:
                change = (curr - prev) / prev * 100
                if change > 10:
                    trend = 'up'
                elif change < -10:
                    trend = 'down'

            customers.append({
                "account_code": row['account_code'],
                "customer_name": row['customer_name'],
                "current_year": round(curr, 2),
                "previous_year": round(prev, 2),
                "percent_of_total": round(curr / total_revenue * 100, 1) if total_revenue else 0,
                "cumulative_percent": round(cumulative / total_revenue * 100, 1) if total_revenue else 0,
                "invoice_count": row['invoice_count'],
                "trend": trend,
                "change_percent": round((curr - prev) / prev * 100, 1) if prev else 0
            })

        return {
            "success": True,
            "year": year,
            "total_revenue": round(total_revenue, 2),
            "customers": customers
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/customer-concentration")
async def get_customer_concentration(year: int = 2026):
    """Get customer concentration analysis."""
    try:
        df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(st_account) as account_code,
                SUM(st_trvalue) as revenue
            FROM stran
            WHERE st_trtype = 'I' AND YEAR(st_trdate) = {year}
            GROUP BY st_account
            ORDER BY SUM(st_trvalue) DESC
        """)
        customers = df_to_records(df)
        total_revenue = sum(c['revenue'] or 0 for c in customers)
        total_customers = len(customers)

        # Calculate concentration metrics
        top_1 = customers[0]['revenue'] if customers else 0
        top_3 = sum(c['revenue'] or 0 for c in customers[:3])
        top_5 = sum(c['revenue'] or 0 for c in customers[:5])
        top_10 = sum(c['revenue'] or 0 for c in customers[:10])

        concentration = {
            "total_customers": total_customers,
            "total_revenue": round(total_revenue, 2),
            "top_1_percent": round(top_1 / total_revenue * 100, 1) if total_revenue else 0,
            "top_3_percent": round(top_3 / total_revenue * 100, 1) if total_revenue else 0,
            "top_5_percent": round(top_5 / total_revenue * 100, 1) if total_revenue else 0,
            "top_10_percent": round(top_10 / total_revenue * 100, 1) if total_revenue else 0,
            "risk_level": "low"
        }

        # Set risk flag
        if concentration["top_5_percent"] > 50:
            concentration["risk_level"] = "high"
        elif concentration["top_5_percent"] > 40:
            concentration["risk_level"] = "medium"

        return {
            "success": True,
            "year": year,
            "concentration": concentration
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/customer-lifecycle")
async def get_customer_lifecycle(year: int = 2026):
    """Get customer lifecycle analysis - new, lost, by age band."""
    try:
        # Get first transaction date per customer
        df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(st_account) as account_code,
                MIN(st_trdate) as first_transaction,
                MAX(st_trdate) as last_transaction,
                SUM(CASE WHEN YEAR(st_trdate) = {year} THEN st_trvalue ELSE 0 END) as current_revenue,
                SUM(CASE WHEN YEAR(st_trdate) = {year - 1} THEN st_trvalue ELSE 0 END) as prev_revenue
            FROM stran
            WHERE st_trtype = 'I'
            GROUP BY st_account
        """)
        data = df_to_records(df)

        new_customers = 0
        lost_customers = 0
        age_bands = {
            "less_than_1_year": {"count": 0, "revenue": 0},
            "1_to_3_years": {"count": 0, "revenue": 0},
            "3_to_5_years": {"count": 0, "revenue": 0},
            "over_5_years": {"count": 0, "revenue": 0}
        }

        for row in data:
            first = row['first_transaction']
            curr_rev = row['current_revenue'] or 0
            prev_rev = row['prev_revenue'] or 0

            if not first:
                continue

            # New customer (first transaction this year)
            if first.year == year:
                new_customers += 1

            # Lost/dormant (had revenue last year, none this year)
            if prev_rev > 0 and curr_rev == 0:
                lost_customers += 1

            # Age bands (for active customers)
            if curr_rev > 0:
                years_active = year - first.year
                if years_active < 1:
                    age_bands["less_than_1_year"]["count"] += 1
                    age_bands["less_than_1_year"]["revenue"] += curr_rev
                elif years_active < 3:
                    age_bands["1_to_3_years"]["count"] += 1
                    age_bands["1_to_3_years"]["revenue"] += curr_rev
                elif years_active < 5:
                    age_bands["3_to_5_years"]["count"] += 1
                    age_bands["3_to_5_years"]["revenue"] += curr_rev
                else:
                    age_bands["over_5_years"]["count"] += 1
                    age_bands["over_5_years"]["revenue"] += curr_rev

        # Round revenue figures
        for band in age_bands.values():
            band["revenue"] = round(band["revenue"], 2)

        return {
            "success": True,
            "year": year,
            "new_customers": new_customers,
            "lost_customers": lost_customers,
            "age_bands": age_bands
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/margin-by-category")
async def get_margin_by_category(year: int = 2026):
    """Get gross margin analysis by revenue category."""
    try:
        # Get revenue and costs by category
        df = sql_connector.execute_query(f"""
            SELECT
                CASE
                    WHEN nt_acnt LIKE 'E10%' OR nt_acnt LIKE 'E20%' OR nt_acnt LIKE 'E70%' OR nt_acnt LIKE 'E90%' THEN 'Opera/Software'
                    WHEN nt_acnt LIKE 'E30%' OR nt_acnt LIKE 'E60%' THEN 'Hosting/Cloud'
                    WHEN nt_acnt LIKE 'E50%' THEN 'Zahara/Autoinvoicing'
                    WHEN nt_acnt LIKE 'E40%' OR nt_acnt LIKE 'E80%' THEN 'Other Products'
                    WHEN nt_acnt LIKE 'F10%' OR nt_acnt LIKE 'F20%' OR nt_acnt LIKE 'F70%' OR nt_acnt LIKE 'F90%' THEN 'Opera/Software'
                    WHEN nt_acnt LIKE 'F30%' OR nt_acnt LIKE 'F60%' THEN 'Hosting/Cloud'
                    WHEN nt_acnt LIKE 'F50%' THEN 'Zahara/Autoinvoicing'
                    WHEN nt_acnt LIKE 'F40%' OR nt_acnt LIKE 'F80%' THEN 'Other Products'
                    ELSE 'General'
                END as category,
                SUM(CASE WHEN nt_type = 'E' THEN -nt_value ELSE 0 END) as revenue,
                SUM(CASE WHEN nt_type = 'F' THEN nt_value ELSE 0 END) as cost_of_sales
            FROM ntran
            WHERE nt_type IN ('E', 'F') AND nt_year = {year}
            GROUP BY
                CASE
                    WHEN nt_acnt LIKE 'E10%' OR nt_acnt LIKE 'E20%' OR nt_acnt LIKE 'E70%' OR nt_acnt LIKE 'E90%' THEN 'Opera/Software'
                    WHEN nt_acnt LIKE 'E30%' OR nt_acnt LIKE 'E60%' THEN 'Hosting/Cloud'
                    WHEN nt_acnt LIKE 'E50%' THEN 'Zahara/Autoinvoicing'
                    WHEN nt_acnt LIKE 'E40%' OR nt_acnt LIKE 'E80%' THEN 'Other Products'
                    WHEN nt_acnt LIKE 'F10%' OR nt_acnt LIKE 'F20%' OR nt_acnt LIKE 'F70%' OR nt_acnt LIKE 'F90%' THEN 'Opera/Software'
                    WHEN nt_acnt LIKE 'F30%' OR nt_acnt LIKE 'F60%' THEN 'Hosting/Cloud'
                    WHEN nt_acnt LIKE 'F50%' THEN 'Zahara/Autoinvoicing'
                    WHEN nt_acnt LIKE 'F40%' OR nt_acnt LIKE 'F80%' THEN 'Other Products'
                    ELSE 'General'
                END
        """)
        data = df_to_records(df)

        # Consolidate by category (some categories appear twice due to E and F codes)
        categories = {}
        for row in data:
            cat = row['category']
            if cat not in categories:
                categories[cat] = {"revenue": 0, "cost_of_sales": 0}
            categories[cat]["revenue"] += row['revenue'] or 0
            categories[cat]["cost_of_sales"] += row['cost_of_sales'] or 0

        # Calculate margins
        margins = []
        for cat, data in categories.items():
            rev = data["revenue"]
            cos = data["cost_of_sales"]
            gp = rev - cos
            gp_pct = (gp / rev * 100) if rev > 0 else 0

            margins.append({
                "category": cat,
                "revenue": round(rev, 2),
                "cost_of_sales": round(cos, 2),
                "gross_profit": round(gp, 2),
                "gross_margin_percent": round(gp_pct, 1)
            })

        margins.sort(key=lambda x: x['revenue'], reverse=True)

        # Total
        total_rev = sum(m['revenue'] for m in margins)
        total_cos = sum(m['cost_of_sales'] for m in margins)
        total_gp = total_rev - total_cos

        return {
            "success": True,
            "year": year,
            "categories": margins,
            "totals": {
                "revenue": round(total_rev, 2),
                "cost_of_sales": round(total_cos, 2),
                "gross_profit": round(total_gp, 2),
                "gross_margin_percent": round(total_gp / total_rev * 100, 1) if total_rev else 0
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
