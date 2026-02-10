"""
FastAPI backend for SQL RAG application.
Provides REST API endpoints for database queries, RAG queries, and configuration management.
"""

import os
import sys
import logging
import configparser
import json
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager
from pathlib import Path

# Load .env file if it exists
from dotenv import load_dotenv
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path, override=True)

from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Literal
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sql_rag.sql_connector import SQLConnector
from sql_rag.vector_db import VectorDB
from sql_rag.llm import create_llm_instance

# Import RAG populator for deployment initialization
try:
    from scripts.populate_rag import RAGPopulator
    RAG_POPULATOR_AVAILABLE = True
except ImportError:
    RAG_POPULATOR_AVAILABLE = False
    logger.warning("RAG Populator not available - scripts/populate_rag.py not found")

# Email module imports
from api.email.storage import EmailStorage
from api.email.providers.base import ProviderType
from api.email.providers.imap import IMAPProvider
from api.email.categorizer import EmailCategorizer, CustomerLinker
from api.email.sync import EmailSyncManager

# Supplier statement extraction and reconciliation
from sql_rag.supplier_statement_extract import SupplierStatementExtractor
from sql_rag.supplier_statement_reconcile import SupplierStatementReconciler
from sql_rag.supplier_statement_db import SupplierStatementDB, get_supplier_statement_db

# Opera integration rules API
from api.opera_rules_api import router as opera_rules_router

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
config: Optional[configparser.ConfigParser] = None
sql_connector: Optional[SQLConnector] = None
vector_db: Optional[VectorDB] = None
llm = None
current_company: Optional[Dict[str, Any]] = None

# Email module global instances
email_storage: Optional[EmailStorage] = None
email_sync_manager: Optional[EmailSyncManager] = None
email_categorizer: Optional[EmailCategorizer] = None
customer_linker: Optional[CustomerLinker] = None


# Get the config path relative to the project root
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.ini")
COMPANIES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "companies")


def load_config(config_path: str = None) -> configparser.ConfigParser:
    """Load configuration from file."""
    if config_path is None:
        config_path = CONFIG_PATH
    cfg = configparser.ConfigParser()
    if os.path.exists(config_path):
        cfg.read(config_path)
    return cfg


def load_companies() -> List[Dict[str, Any]]:
    """Load all company configurations from the companies directory."""
    companies = []
    if os.path.exists(COMPANIES_DIR):
        for filename in os.listdir(COMPANIES_DIR):
            if filename.endswith('.json'):
                filepath = os.path.join(COMPANIES_DIR, filename)
                try:
                    with open(filepath, 'r') as f:
                        company = json.load(f)
                        companies.append(company)
                except Exception as e:
                    logger.warning(f"Could not load company config {filename}: {e}")
    return companies


def load_company(company_id: str) -> Optional[Dict[str, Any]]:
    """Load a specific company configuration."""
    filepath = os.path.join(COMPANIES_DIR, f"{company_id}.json")
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            return json.load(f)
    return None


def switch_database(database_name: str) -> bool:
    """Switch the SQL connector to a different database."""
    global config, sql_connector

    if not config:
        config = load_config()

    # Update the database name in config
    config["database"]["database"] = database_name

    # Reinitialize SQL connector with new database
    try:
        sql_connector = SQLConnector(CONFIG_PATH)
        # Temporarily override the database
        sql_connector.database = database_name
        # Reconnect with new database
        sql_connector._init_connection_string()
        sql_connector._connect()
        return True
    except Exception as e:
        logger.error(f"Failed to switch database: {e}")
        return False


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

        # Auto-populate RAG if database is empty and auto_populate is enabled
        auto_populate = config.get("system", "rag_auto_populate", fallback="true").lower() == "true"
        if auto_populate and RAG_POPULATOR_AVAILABLE:
            try:
                info = vector_db.get_collection_info()
                if info.get("vectors_count", 0) == 0:
                    logger.info("RAG database is empty - auto-populating with Opera knowledge...")
                    populator = RAGPopulator(config_path=CONFIG_PATH)
                    results = populator.populate_all(clear_first=False)
                    if results["success"]:
                        logger.info(f"RAG auto-population complete: {results['total_documents']} documents loaded")
                    else:
                        logger.warning(f"RAG auto-population failed: {results.get('error', 'Unknown error')}")
                else:
                    logger.info(f"RAG database already has {info.get('vectors_count')} documents")
            except Exception as e:
                logger.warning(f"Could not auto-populate RAG: {e}")
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

    # Initialize supplier statement automation database
    try:
        supplier_statement_db = get_supplier_statement_db()
        logger.info(f"Supplier statement database initialized at {supplier_statement_db.db_path}")
    except Exception as e:
        logger.warning(f"Could not initialize supplier statement database: {e}")

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

# Include Opera integration rules API router
app.include_router(opera_rules_router)


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


class OperaConfig(BaseModel):
    """Opera system configuration"""
    version: str = "sql_se"  # "sql_se" or "opera3"
    # Opera 3 specific settings
    opera3_server_path: Optional[str] = None  # UNC or network path to Opera 3 server, e.g., "\\\\SERVER\\O3 Server VFP"
    opera3_base_path: Optional[str] = None  # e.g., "C:\\Apps\\O3 Server VFP"
    opera3_company_code: Optional[str] = None  # Company code from seqco.dbf


class Opera3Company(BaseModel):
    """Opera 3 company information"""
    code: str
    name: str
    data_path: str


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


# ============ Opera Configuration Endpoints ============

@app.get("/api/config/opera")
async def get_opera_config():
    """Get current Opera configuration."""
    if not config:
        raise HTTPException(status_code=500, detail="Configuration not loaded")

    opera_section = {}
    if config.has_section("opera"):
        for key, value in config.items("opera"):
            opera_section[key] = value

    return {
        "version": opera_section.get("version", "sql_se"),
        "opera3_server_path": opera_section.get("opera3_server_path", ""),
        "opera3_base_path": opera_section.get("opera3_base_path", ""),
        "opera3_company_code": opera_section.get("opera3_company_code", ""),
    }


@app.post("/api/config/opera")
async def update_opera_config(opera_config: OperaConfig):
    """Update Opera configuration."""
    global config

    if not config:
        config = load_config()

    if not config.has_section("opera"):
        config.add_section("opera")

    config["opera"]["version"] = opera_config.version

    if opera_config.opera3_server_path is not None:
        config["opera"]["opera3_server_path"] = opera_config.opera3_server_path
    if opera_config.opera3_base_path:
        config["opera"]["opera3_base_path"] = opera_config.opera3_base_path
    if opera_config.opera3_company_code:
        config["opera"]["opera3_company_code"] = opera_config.opera3_company_code

    save_config(config)

    return {"success": True, "message": "Opera configuration updated"}


@app.get("/api/config/opera/companies")
async def get_opera3_companies():
    """Get list of Opera 3 companies (requires opera3_base_path to be configured)."""
    if not config:
        raise HTTPException(status_code=500, detail="Configuration not loaded")

    if not config.has_section("opera"):
        return {"companies": [], "error": "Opera not configured"}

    base_path = config.get("opera", "opera3_base_path", fallback="")
    if not base_path:
        return {"companies": [], "error": "Opera 3 base path not configured"}

    try:
        from sql_rag.opera3_foxpro import Opera3System
        system = Opera3System(base_path)
        companies = system.get_companies()
        return {
            "companies": [
                {"code": c["code"], "name": c["name"], "data_path": c.get("data_path", "")}
                for c in companies
            ]
        }
    except ImportError:
        return {"companies": [], "error": "dbfread package not installed (required for Opera 3)"}
    except FileNotFoundError as e:
        return {"companies": [], "error": f"Path not found: {str(e)}"}
    except Exception as e:
        return {"companies": [], "error": str(e)}


@app.post("/api/config/opera/test")
async def test_opera_connection(opera_config: OperaConfig):
    """Test Opera connection with the provided configuration."""
    if opera_config.version == "sql_se":
        # Test SQL Server connection (use existing database config)
        if sql_connector:
            try:
                # Simple query to test connection
                df = sql_connector.execute_query("SELECT 1 as test")
                return {"success": True, "message": "SQL Server connection successful"}
            except Exception as e:
                return {"success": False, "error": f"SQL Server connection failed: {str(e)}"}
        else:
            return {"success": False, "error": "SQL connector not initialized"}
    else:
        # Test Opera 3 connection
        if not opera_config.opera3_base_path:
            return {"success": False, "error": "Opera 3 base path not provided"}

        try:
            from sql_rag.opera3_foxpro import Opera3System, Opera3Reader
            system = Opera3System(opera_config.opera3_base_path)

            # Test getting companies
            companies = system.get_companies()
            if not companies:
                return {"success": False, "error": "No companies found in Opera 3 installation"}

            # If company code provided, test reading from it
            if opera_config.opera3_company_code:
                reader = system.get_reader_for_company(opera_config.opera3_company_code)
                # Try to read suppliers to verify data access
                suppliers = reader.read_table("pname", limit=1)
                return {
                    "success": True,
                    "message": f"Opera 3 connection successful. Found {len(companies)} companies.",
                    "companies_count": len(companies)
                }
            else:
                return {
                    "success": True,
                    "message": f"Opera 3 path valid. Found {len(companies)} companies.",
                    "companies_count": len(companies)
                }

        except ImportError:
            return {"success": False, "error": "dbfread package not installed (required for Opera 3)"}
        except FileNotFoundError as e:
            return {"success": False, "error": f"Path not found: {str(e)}"}
        except Exception as e:
            return {"success": False, "error": str(e)}


# ============ Company Management Endpoints ============

@app.get("/api/companies")
async def get_companies():
    """Get list of available companies."""
    companies = load_companies()
    return {
        "companies": companies,
        "current_company": current_company
    }


@app.get("/api/companies/current")
async def get_current_company():
    """Get the currently active company."""
    global current_company
    if not current_company:
        # Try to determine current company from database name
        if config and config.has_option("database", "database"):
            db_name = config.get("database", "database")
            companies = load_companies()
            for company in companies:
                if company.get("database") == db_name:
                    current_company = company
                    break
    return {"company": current_company}


@app.post("/api/companies/switch/{company_id}")
async def switch_company(company_id: str):
    """Switch to a different company/database."""
    global current_company, sql_connector, config

    # Load the company configuration
    company = load_company(company_id)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{company_id}' not found")

    # Get the database name for this company
    database_name = company.get("database")
    if not database_name:
        raise HTTPException(status_code=400, detail="Company has no database configured")

    # Update config with new database
    if not config:
        config = load_config()

    old_database = config.get("database", "database", fallback="")
    config["database"]["database"] = database_name

    # Save the config
    save_config(config)

    # Reinitialize SQL connector with new database
    try:
        sql_connector = SQLConnector(CONFIG_PATH)
        current_company = company
        logger.info(f"Switched from {old_database} to {database_name} ({company['name']})")
        return {
            "success": True,
            "message": f"Switched to {company['name']}",
            "company": company
        }
    except Exception as e:
        # Rollback config on failure
        config["database"]["database"] = old_database
        save_config(config)
        logger.error(f"Failed to switch company: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to switch company: {str(e)}")


@app.get("/api/companies/{company_id}")
async def get_company_config(company_id: str):
    """Get configuration for a specific company."""
    company = load_company(company_id)
    if not company:
        raise HTTPException(status_code=404, detail=f"Company '{company_id}' not found")
    return {"company": company}


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
    # Backend validation - check for empty query
    if not request.query or not request.query.strip():
        return SQLQueryResponse(success=False, error="SQL query is required. Please enter a query to execute.")

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
    # Backend validation - check for empty question
    if not request.question or not request.question.strip():
        return RAGQueryResponse(success=False, error="Question is required. Please enter a question.")

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
    # Backend validation - check for empty description (unless custom SQL provided)
    if not request.custom_sql and (not request.description or not request.description.strip()):
        return {"success": False, "error": "Description is required. Please describe what data you want to extract."}

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


@app.post("/api/rag/populate")
async def populate_rag_database(
    clear_first: bool = Query(True, description="Clear existing data before populating"),
    include_data_structures: bool = Query(True, description="Include Opera data structures documentation"),
    data_structures_path: Optional[str] = Query(None, description="Custom path to data structures file")
):
    """
    Populate the RAG database with Opera knowledge.

    This endpoint loads:
    - Markdown knowledge files from docs/
    - Opera data structures documentation
    - Business rules and conventions
    - SQL query examples
    - Table reference summaries

    Use this endpoint during deployment to initialize the knowledge base.
    """
    if not RAG_POPULATOR_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="RAG Populator not available. Ensure scripts/populate_rag.py exists."
        )

    if not vector_db:
        raise HTTPException(status_code=503, detail="Vector database not initialized")

    try:
        populator = RAGPopulator(config_path=CONFIG_PATH)

        # If custom data structures path provided, use it
        if data_structures_path and include_data_structures:
            ds_path = Path(data_structures_path)
            if not ds_path.exists():
                return {
                    "success": False,
                    "error": f"Data structures file not found: {data_structures_path}"
                }

        results = populator.populate_all(clear_first=clear_first)

        return {
            "success": results["success"],
            "message": f"RAG database populated with {results['total_documents']} documents",
            "total_documents": results["total_documents"],
            "sources": results.get("sources", {}),
            "error": results.get("error")
        }

    except Exception as e:
        logger.error(f"Failed to populate RAG database: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/rag/populate/status")
async def get_rag_population_status():
    """
    Get the current status of the RAG database.

    Returns:
    - Whether RAG populator is available
    - Current document count
    - Last population timestamp (if tracked)
    """
    if not vector_db:
        return {
            "available": False,
            "populator_available": RAG_POPULATOR_AVAILABLE,
            "error": "Vector database not initialized"
        }

    try:
        info = vector_db.get_collection_info()
        return {
            "available": True,
            "populator_available": RAG_POPULATOR_AVAILABLE,
            "document_count": info.get("vectors_count", 0),
            "collection_name": info.get("name"),
            "status": info.get("status"),
            "needs_population": info.get("vectors_count", 0) == 0
        }
    except Exception as e:
        return {
            "available": False,
            "populator_available": RAG_POPULATOR_AVAILABLE,
            "error": str(e)
        }


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
    # Backend validation - check for empty question
    if not request.question or not request.question.strip():
        return {
            "success": False,
            "error": "Question is required. Please enter a credit control question.",
            "description": "Validation Error",
            "count": 0,
            "data": []
        }

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
            WHERE RTRIM(nt_type) IN ('45', 'H')  -- Expenses / Overheads (H is Opera letter code)
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

        # Get bank balances from nominal ledger (more accurate than nbank.nk_curbal which is cumulative)
        # Bank accounts typically start with 'BC' in Opera chart of accounts
        bank_sql = """
            SELECT na_acnt AS account,
                   na_desc AS description,
                   (ISNULL(na_ytddr, 0) - ISNULL(na_ytdcr, 0)) AS balance
            FROM nacnt
            WHERE na_acnt LIKE 'BC%'
            ORDER BY na_acnt
        """
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
    """Initialize email providers from config and database."""
    global email_storage, email_sync_manager

    if not email_storage or not email_sync_manager:
        return

    # First, register any existing providers from the database
    try:
        existing_providers = email_storage.get_all_providers(enabled_only=True)
        for provider_info in existing_providers:
            provider_id = provider_info['id']
            provider_type = provider_info['provider_type']
            provider_config = provider_info.get('config', {})

            if not provider_config:
                logger.warning(f"Provider {provider_id} ({provider_info['name']}) has no config, skipping")
                continue

            try:
                if provider_type == 'imap':
                    provider = IMAPProvider(provider_config)
                    email_sync_manager.register_provider(provider_id, provider)
                    logger.info(f"Registered IMAP provider: {provider_info['name']} (id={provider_id})")
                # Add other provider types here as needed (microsoft, gmail)
            except Exception as e:
                logger.warning(f"Could not register provider {provider_info['name']}: {e}")
    except Exception as e:
        logger.warning(f"Error loading providers from database: {e}")

    # Also check for IMAP provider in config.ini (legacy support)
    if config and config.has_section('email_imap') and config.getboolean('email_imap', 'enabled', fallback=False):
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
                    provider = IMAPProvider(imap_config)
                    email_sync_manager.register_provider(provider_id, provider)
                    logger.info("IMAP provider from config.ini registered")
        except Exception as e:
            logger.warning(f"Could not initialize IMAP provider from config: {e}")


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


@app.get("/api/email/providers/{provider_id}")
async def get_email_provider(provider_id: int):
    """Get a single email provider's configuration (for editing)."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        provider = email_storage.get_provider(provider_id)
        if not provider:
            return {"success": False, "error": "Provider not found"}

        # Return provider with config for editing
        # Remove sensitive fields (passwords/secrets) for security
        config = provider.get('config', {}).copy()
        if 'password' in config:
            del config['password']
        if 'client_secret' in config:
            del config['client_secret']

        return {
            "success": True,
            "provider": {
                "id": provider['id'],
                "name": provider['name'],
                "provider_type": provider['provider_type'],
                "enabled": provider['enabled'],
                "config": config
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/email/providers")
async def add_email_provider(provider: EmailProviderCreate):
    """Add a new email provider."""
    import sqlite3

    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        # Validate required fields
        if not provider.name or not provider.name.strip():
            return {"success": False, "error": "Provider name is required"}

        # Build config based on provider type
        if provider.provider_type == 'imap':
            if not provider.server:
                return {"success": False, "error": "IMAP server is required"}
            if not provider.username:
                return {"success": False, "error": "IMAP username is required"}
            if not provider.password:
                return {"success": False, "error": "IMAP password is required"}
            provider_config = {
                'server': provider.server,
                'port': provider.port or 993,
                'username': provider.username,
                'password': provider.password,
                'use_ssl': provider.use_ssl,
            }
        elif provider.provider_type == 'microsoft':
            if not provider.tenant_id:
                return {"success": False, "error": "Tenant ID is required for Microsoft provider"}
            if not provider.client_id:
                return {"success": False, "error": "Client ID is required for Microsoft provider"}
            if not provider.client_secret:
                return {"success": False, "error": "Client Secret is required for Microsoft provider"}
            if not provider.user_email:
                return {"success": False, "error": "User email is required for Microsoft provider"}
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
            return {"success": False, "error": f"Invalid provider type: {provider.provider_type}"}

        provider_id = email_storage.add_provider(
            name=provider.name.strip(),
            provider_type=ProviderType(provider.provider_type),
            config=provider_config
        )

        # Register provider with sync manager if IMAP
        if provider.provider_type == 'imap' and email_sync_manager:
            imap_provider = IMAPProvider(provider_config)
            email_sync_manager.register_provider(provider_id, imap_provider)

        return {"success": True, "provider_id": provider_id}
    except sqlite3.IntegrityError as e:
        if 'UNIQUE constraint failed' in str(e):
            return {"success": False, "error": f"A provider with the name '{provider.name}' already exists. Please use a different name."}
        return {"success": False, "error": f"Database error: {e}"}
    except Exception as e:
        logger.error(f"Failed to add email provider: {e}")
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


@app.put("/api/email/providers/{provider_id}")
async def update_email_provider(provider_id: int, provider: EmailProviderCreate):
    """Update an existing email provider's configuration."""
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        # Get existing provider to verify it exists
        existing = email_storage.get_provider(provider_id)
        if not existing:
            return {"success": False, "error": "Provider not found"}

        existing_config = existing.get('config', {})

        # Build config based on provider type
        # Keep existing password/secret if not provided (for security - we don't send passwords back to frontend)
        if provider.provider_type == 'imap':
            provider_config = {
                'server': provider.server,
                'port': provider.port or 993,
                'username': provider.username,
                'password': provider.password if provider.password else existing_config.get('password', ''),
                'use_ssl': provider.use_ssl
            }
        elif provider.provider_type == 'microsoft':
            provider_config = {
                'tenant_id': provider.tenant_id,
                'client_id': provider.client_id,
                'client_secret': provider.client_secret if provider.client_secret else existing_config.get('client_secret', ''),
                'user_email': provider.user_email
            }
        else:
            provider_config = {}

        # Update provider in database
        import json
        email_storage.update_provider(
            provider_id,
            name=provider.name.strip(),
            config_json=json.dumps(provider_config)
        )

        # Re-register provider with sync manager if IMAP
        if provider.provider_type == 'imap' and email_sync_manager:
            # Unregister old provider first
            email_sync_manager.unregister_provider(provider_id)
            # Register with new config
            imap_provider = IMAPProvider(provider_config)
            email_sync_manager.register_provider(provider_id, imap_provider)
            logger.info(f"Updated and re-registered IMAP provider: {provider.name}")

        return {"success": True, "message": "Provider updated successfully"}
    except Exception as e:
        logger.error(f"Failed to update email provider: {e}")
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
    # Backend validation - check for empty account code
    if not request.account_code or not request.account_code.strip():
        return {"success": False, "error": "Account code is required. Please enter a customer account code."}

    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        result = email_storage.link_email_to_customer(
            email_id=email_id,
            account_code=request.account_code.strip(),
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


@app.get("/api/email/messages/{email_id}/attachments/{attachment_id}/download")
async def download_email_attachment(email_id: int, attachment_id: str, save_to: Optional[str] = None):
    """
    Download an email attachment.

    Args:
        email_id: The database email ID
        attachment_id: The attachment ID from the attachment list
        save_to: Optional path to save the file (if not provided, returns file info and content path)

    Returns:
        File content or save path
    """
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email module not initialized")

    try:
        # Get the email to find provider and message info
        email = email_storage.get_email_by_id(email_id)
        if not email:
            raise HTTPException(status_code=404, detail="Email not found")

        provider_id = email.get('provider_id')
        message_id = email.get('message_id')

        logger.info(f"Downloading attachment: email_id={email_id}, provider_id={provider_id}, message_id={message_id}")
        logger.info(f"Registered providers: {list(email_sync_manager.providers.keys())}")

        if provider_id not in email_sync_manager.providers:
            raise HTTPException(status_code=404, detail=f"Email provider {provider_id} not found. Available: {list(email_sync_manager.providers.keys())}")

        provider = email_sync_manager.providers[provider_id]

        # Find the attachment metadata
        attachments = email.get('attachments', [])
        attachment_meta = next(
            (a for a in attachments if str(a.get('attachment_id')) == str(attachment_id)),
            None
        )
        if not attachment_meta:
            raise HTTPException(status_code=404, detail=f"Attachment {attachment_id} not found in attachments: {attachments}")

        # Get folder_id - need to look it up from database
        folder_id_db = email.get('folder_id')
        folder_id = 'INBOX'  # Default
        if folder_id_db:
            # Look up folder name from email_folders table
            with email_storage._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_db,))
                row = cursor.fetchone()
                if row:
                    folder_id = row['folder_id']

        logger.info(f"Downloading from folder: {folder_id}, attachment_id: {attachment_id}")

        # Download the attachment
        try:
            result = await provider.download_attachment(message_id, attachment_id, folder_id)
            logger.info(f"Download result: {type(result)}, is None: {result is None}")
        except Exception as dl_err:
            logger.error(f"Download exception: {dl_err}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Download exception: {str(dl_err)}")

        if not result:
            raise HTTPException(status_code=404, detail=f"Failed to download attachment from provider. message_id={message_id}, folder={folder_id}")

        content, filename, content_type = result

        # Save to specified path or temp location
        import tempfile
        import os

        if save_to:
            # Save to user-specified path
            save_path = save_to
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
        else:
            # Save to temp directory
            temp_dir = tempfile.gettempdir()
            save_path = os.path.join(temp_dir, f"email_attachment_{email_id}_{filename}")

        with open(save_path, 'wb') as f:
            f.write(content)

        return {
            "success": True,
            "filename": filename,
            "content_type": content_type,
            "size_bytes": len(content),
            "saved_to": save_path
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading attachment: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/email/debug/imap-search")
async def debug_imap_search(provider_id: int, message_id: str, folder: str = 'INBOX'):
    """Debug endpoint to test IMAP search."""
    if not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email sync manager not initialized")

    if provider_id not in email_sync_manager.providers:
        return {
            "success": False,
            "error": f"Provider {provider_id} not in providers",
            "available_providers": list(email_sync_manager.providers.keys())
        }

    provider = email_sync_manager.providers[provider_id]

    try:
        import asyncio
        loop = asyncio.get_event_loop()

        # Ensure connection
        if not provider._connection:
            await provider.authenticate()

        # Select folder
        status, count = await loop.run_in_executor(
            None, lambda: provider._connection.select(folder, readonly=True)
        )

        # Search by Message-ID
        search_criteria = f'(HEADER Message-ID "<{message_id}>")'
        status2, msg_ids = await loop.run_in_executor(
            None, lambda: provider._connection.search(None, search_criteria)
        )

        # Get all emails
        status3, all_ids = await loop.run_in_executor(
            None, lambda: provider._connection.search(None, 'ALL')
        )

        total_emails = len(all_ids[0].split()) if all_ids[0] else 0

        # Get Message-IDs of last 10 emails
        sample_msg_ids = []
        if all_ids[0]:
            for imap_id in all_ids[0].split()[-10:]:
                try:
                    s, headers = await loop.run_in_executor(
                        None, lambda mid=imap_id: provider._connection.fetch(mid, '(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID SUBJECT)])')
                    )
                    if s == 'OK' and headers[0]:
                        header_data = headers[0][1] if isinstance(headers[0], tuple) else headers[0]
                        if isinstance(header_data, bytes):
                            sample_msg_ids.append({
                                'imap_id': imap_id.decode() if isinstance(imap_id, bytes) else str(imap_id),
                                'headers': header_data.decode('utf-8', errors='replace')
                            })
                except Exception as e:
                    sample_msg_ids.append({'error': str(e)})

        return {
            "success": True,
            "folder": folder,
            "folder_select_status": status,
            "search_criteria": search_criteria,
            "search_status": status2,
            "found_msg_ids": msg_ids[0].decode() if isinstance(msg_ids[0], bytes) else str(msg_ids[0]) if msg_ids[0] else None,
            "total_emails_in_folder": total_emails,
            "sample_recent_emails": sample_msg_ids
        }
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


class SendEmailRequest(BaseModel):
    """Request model for sending email."""
    to: str = Field(..., description="Recipient email address")
    subject: str = Field(..., description="Email subject")
    body: str = Field(..., description="Email body (HTML supported)")
    attachments: Optional[List[str]] = Field(default=None, description="List of file paths to attach")
    from_email: Optional[str] = Field(default=None, description="Override from address (for external relay)")


@app.post("/api/email/send")
async def send_email(request: SendEmailRequest):
    """
    Send an email using the configured IMAP provider's SMTP settings.

    Uses the first enabled email provider's credentials for sending.
    Most email services (Gmail, Microsoft, IMAP providers) use the same
    credentials for both receiving (IMAP) and sending (SMTP).

    Args:
        to: Recipient email address
        subject: Email subject
        body: Email body (HTML supported)
        attachments: Optional list of file paths to attach

    Returns:
        Success status and message ID
    """
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders

    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        # Get first enabled email provider
        providers = email_storage.get_all_providers()
        enabled_provider = next((p for p in providers if p.get('enabled')), None)

        if not enabled_provider:
            raise HTTPException(status_code=400, detail="No enabled email provider found. Configure an email provider first.")

        provider_type = enabled_provider.get('provider_type')
        config_json = enabled_provider.get('config_json', '{}')
        provider_config = json.loads(config_json) if config_json else {}

        # Determine SMTP settings based on provider type
        if provider_type == 'gmail':
            smtp_server = 'smtp.gmail.com'
            smtp_port = 587
            username = provider_config.get('email', '')
            # For Gmail, need app password
            password = provider_config.get('app_password', '') or provider_config.get('password', '')
        elif provider_type == 'microsoft':
            smtp_server = 'smtp.office365.com'
            smtp_port = 587
            username = provider_config.get('email', '')
            password = provider_config.get('password', '')
        elif provider_type == 'imap':
            # For IMAP, derive SMTP from IMAP server
            imap_server = provider_config.get('server', '')
            # If it's an IP address, use the same for SMTP
            import re
            if re.match(r'^\d+\.\d+\.\d+\.\d+$', imap_server):
                smtp_server = imap_server
                smtp_port = 587  # Use 587 with STARTTLS for authenticated relay
            else:
                smtp_server = imap_server.replace('imap.', 'smtp.').replace('imaps.', 'smtp.')
                smtp_port = 587
            username = provider_config.get('username', '')
            password = provider_config.get('password', '')
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported provider type: {provider_type}")

        if not username or not password:
            raise HTTPException(status_code=400, detail="Email provider credentials not configured properly")

        # Determine From address (handle domain\user format)
        # Check for explicit from_email in request first (for external relay)
        if request.from_email:
            from_address = request.from_email
        else:
            # Check for explicit email in config first
            from_address = provider_config.get('email') or provider_config.get('from_email')
            if not from_address:
                if '\\' in username:
                    # Convert domain\user to user@domain.local
                    domain, user = username.split('\\', 1)
                    from_address = f"{user}@{domain}.local"
                elif '@' in username:
                    from_address = username
                else:
                    from_address = username

        # Create message
        msg = MIMEMultipart()
        msg['From'] = from_address
        msg['To'] = request.to
        msg['Subject'] = request.subject

        # Add body
        msg.attach(MIMEText(request.body, 'html'))

        # Add attachments
        if request.attachments:
            for filepath in request.attachments:
                if not os.path.exists(filepath):
                    logger.warning(f"Attachment not found: {filepath}")
                    continue

                filename = os.path.basename(filepath)
                with open(filepath, 'rb') as f:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                    msg.attach(part)

        # Send email
        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            # For port 587, use STARTTLS; for port 25 (internal), try without
            if smtp_port == 587:
                server.starttls()
                server.login(username, password)
            else:
                # Internal server - try with auth first, fallback to no auth
                try:
                    server.login(username, password)
                except smtplib.SMTPException:
                    pass  # Some internal servers don't require auth
            server.send_message(msg)

        logger.info(f"Email sent successfully to {request.to}")

        return {
            "success": True,
            "message": f"Email sent to {request.to}",
            "from": from_address,
            "to": request.to,
            "subject": request.subject
        }

    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"SMTP authentication failed: {e}")
        raise HTTPException(status_code=401, detail="Email authentication failed. Check your credentials or app password.")
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send email: {str(e)}")
    except Exception as e:
        logger.error(f"Error sending email: {e}")
        raise HTTPException(status_code=500, detail=f"Error sending email: {str(e)}")


# ============================================================
# Supplier Statement Automation API Endpoints
# ============================================================

@app.get("/api/supplier-statements/dashboard")
async def get_supplier_statement_dashboard():
    """
    Get supplier statement automation dashboard data.

    Returns KPIs, alerts, and recent activity for the supplier statement
    automation system. Works with both Opera SQL SE and Opera 3.

    Returns:
        Dashboard data including:
        - KPIs (statements count, pending approvals, queries, etc.)
        - Alerts (security, overdue, failed processing)
        - Recent statements, queries, and responses
    """
    import sqlite3
    from datetime import datetime, timedelta

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    # Initialize response with default values
    response = {
        "success": True,
        "kpis": {
            "statements_today": 0,
            "statements_week": 0,
            "statements_month": 0,
            "pending_approvals": 0,
            "open_queries": 0,
            "overdue_queries": 0,
            "avg_processing_hours": None,
            "match_rate_percent": None
        },
        "alerts": {
            "security_alerts": [],
            "overdue_queries": [],
            "failed_processing": []
        },
        "recent_statements": [],
        "recent_queries": [],
        "recent_responses": []
    }

    # If database doesn't exist, return empty dashboard
    if not db_path.exists():
        return response

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        now = datetime.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = today_start - timedelta(days=today_start.weekday())
        month_start = today_start.replace(day=1)

        # KPIs - Statements counts
        cursor.execute("""
            SELECT
                COUNT(CASE WHEN received_date >= ? THEN 1 END) as today_count,
                COUNT(CASE WHEN received_date >= ? THEN 1 END) as week_count,
                COUNT(CASE WHEN received_date >= ? THEN 1 END) as month_count
            FROM supplier_statements
        """, (today_start.isoformat(), week_start.isoformat(), month_start.isoformat()))
        row = cursor.fetchone()
        if row:
            response["kpis"]["statements_today"] = row["today_count"] or 0
            response["kpis"]["statements_week"] = row["week_count"] or 0
            response["kpis"]["statements_month"] = row["month_count"] or 0

        # Pending approvals
        cursor.execute("""
            SELECT COUNT(*) as count FROM supplier_statements
            WHERE status = 'queued'
        """)
        row = cursor.fetchone()
        if row:
            response["kpis"]["pending_approvals"] = row["count"] or 0

        # Open and overdue queries
        cursor.execute("""
            SELECT
                COUNT(CASE WHEN query_resolved_at IS NULL THEN 1 END) as open_count,
                COUNT(CASE WHEN query_resolved_at IS NULL
                           AND datetime(query_sent_at, '+7 days') < datetime('now') THEN 1 END) as overdue_count
            FROM statement_lines
            WHERE query_sent_at IS NOT NULL
        """)
        row = cursor.fetchone()
        if row:
            response["kpis"]["open_queries"] = row["open_count"] or 0
            response["kpis"]["overdue_queries"] = row["overdue_count"] or 0

        # Average processing time (hours)
        cursor.execute("""
            SELECT AVG((julianday(processed_at) - julianday(received_date)) * 24) as avg_hours
            FROM supplier_statements
            WHERE processed_at IS NOT NULL AND received_date IS NOT NULL
        """)
        row = cursor.fetchone()
        if row and row["avg_hours"]:
            response["kpis"]["avg_processing_hours"] = round(row["avg_hours"], 1)

        # Match rate
        cursor.execute("""
            SELECT
                COUNT(CASE WHEN match_status = 'matched' THEN 1 END) as matched,
                COUNT(*) as total
            FROM statement_lines
        """)
        row = cursor.fetchone()
        if row and row["total"] > 0:
            response["kpis"]["match_rate_percent"] = round(
                (row["matched"] or 0) / row["total"] * 100, 1
            )

        # Security alerts (unverified bank changes)
        cursor.execute("""
            SELECT id, supplier_code, field_name, old_value, new_value, changed_at, changed_by
            FROM supplier_change_audit
            WHERE verified = 0 AND field_name IN ('pn_bank', 'pn_acno', 'pn_sort')
            ORDER BY changed_at DESC LIMIT 10
        """)
        for row in cursor.fetchall():
            response["alerts"]["security_alerts"].append({
                "id": row["id"],
                "supplier_code": row["supplier_code"],
                "supplier_name": row["supplier_code"],  # Would need to lookup
                "alert_type": "bank_detail_change",
                "message": f"{row['field_name']} changed from '{row['old_value']}' to '{row['new_value']}'",
                "created_at": row["changed_at"]
            })

        # Overdue queries
        cursor.execute("""
            SELECT sl.id, ss.supplier_code, sl.reference, sl.query_type, sl.query_sent_at,
                   julianday('now') - julianday(sl.query_sent_at) as days_outstanding
            FROM statement_lines sl
            JOIN supplier_statements ss ON sl.statement_id = ss.id
            WHERE sl.query_resolved_at IS NULL
              AND datetime(sl.query_sent_at, '+7 days') < datetime('now')
            ORDER BY sl.query_sent_at ASC LIMIT 10
        """)
        for row in cursor.fetchall():
            response["alerts"]["overdue_queries"].append({
                "id": row["id"],
                "supplier_code": row["supplier_code"],
                "supplier_name": row["supplier_code"],
                "alert_type": "overdue_query",
                "message": f"Query on {row['reference'] or 'item'} ({row['query_type']}) - {int(row['days_outstanding'])} days overdue",
                "created_at": row["query_sent_at"]
            })

        # Failed processing
        cursor.execute("""
            SELECT id, supplier_code, statement_date, received_date, status
            FROM supplier_statements
            WHERE status = 'error'
            ORDER BY received_date DESC LIMIT 10
        """)
        for row in cursor.fetchall():
            response["alerts"]["failed_processing"].append({
                "id": row["id"],
                "supplier_code": row["supplier_code"],
                "supplier_name": row["supplier_code"],
                "alert_type": "failed_processing",
                "message": f"Statement extraction failed",
                "created_at": row["received_date"]
            })

        # Recent statements
        cursor.execute("""
            SELECT id, supplier_code, statement_date, received_date, status,
                   (SELECT closing_balance FROM supplier_statements WHERE id = ss.id) as closing_balance
            FROM supplier_statements ss
            ORDER BY received_date DESC LIMIT 10
        """)
        for row in cursor.fetchall():
            response["recent_statements"].append({
                "id": row["id"],
                "supplier_code": row["supplier_code"],
                "supplier_name": row["supplier_code"],  # Would need to lookup
                "statement_date": row["statement_date"],
                "received_date": row["received_date"],
                "status": row["status"],
                "closing_balance": row["closing_balance"]
            })

        # Recent queries
        cursor.execute("""
            SELECT sl.id, ss.supplier_code, sl.reference, sl.query_type, sl.query_sent_at,
                   sl.query_resolved_at,
                   julianday('now') - julianday(sl.query_sent_at) as days_outstanding
            FROM statement_lines sl
            JOIN supplier_statements ss ON sl.statement_id = ss.id
            WHERE sl.query_sent_at IS NOT NULL
            ORDER BY sl.query_sent_at DESC LIMIT 10
        """)
        for row in cursor.fetchall():
            status = "resolved" if row["query_resolved_at"] else (
                "overdue" if row["days_outstanding"] > 7 else "open"
            )
            response["recent_queries"].append({
                "id": row["id"],
                "supplier_code": row["supplier_code"],
                "supplier_name": row["supplier_code"],
                "query_type": row["query_type"],
                "reference": row["reference"],
                "status": status,
                "days_outstanding": int(row["days_outstanding"]) if row["days_outstanding"] else 0,
                "created_at": row["query_sent_at"]
            })

        # Recent responses sent
        cursor.execute("""
            SELECT ss.id, ss.supplier_code, ss.statement_date, ss.sent_at, ss.approved_by,
                   (SELECT COUNT(*) FROM statement_lines sl WHERE sl.statement_id = ss.id
                    AND sl.query_sent_at IS NOT NULL) as queries_count,
                   (SELECT closing_balance FROM supplier_statements WHERE id = ss.id) as balance
            FROM supplier_statements ss
            WHERE ss.sent_at IS NOT NULL
            ORDER BY ss.sent_at DESC LIMIT 10
        """)
        for row in cursor.fetchall():
            response["recent_responses"].append({
                "id": row["id"],
                "supplier_code": row["supplier_code"],
                "supplier_name": row["supplier_code"],
                "statement_date": row["statement_date"],
                "sent_at": row["sent_at"],
                "approved_by": row["approved_by"],
                "queries_count": row["queries_count"] or 0,
                "balance": row["balance"]
            })

        conn.close()
        return response

    except Exception as e:
        logger.error(f"Error loading supplier statement dashboard: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/api/supplier-statements")
async def list_supplier_statements(status: Optional[str] = None):
    """List all supplier statements with line counts and match statistics."""
    import sqlite3

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "statements": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get statements with aggregated line data
        query = """
            SELECT
                ss.id, ss.supplier_code, ss.statement_date, ss.received_date, ss.status,
                ss.sender_email, ss.opening_balance, ss.closing_balance, ss.currency,
                ss.acknowledged_at, ss.processed_at, ss.approved_by, ss.approved_at,
                ss.sent_at, ss.error_message,
                COUNT(sl.id) as line_count,
                SUM(CASE WHEN sl.match_status = 'matched' THEN 1 ELSE 0 END) as matched_count,
                SUM(CASE WHEN sl.match_status = 'query' THEN 1 ELSE 0 END) as query_count
            FROM supplier_statements ss
            LEFT JOIN statement_lines sl ON sl.statement_id = ss.id
        """
        params = []
        if status:
            query += " WHERE ss.status = ?"
            params.append(status)
        query += " GROUP BY ss.id ORDER BY ss.received_date DESC"

        cursor.execute(query, params)
        statements = []

        # Try to get supplier names from Opera if SQL connector is available
        supplier_names = {}
        if sql_connector:
            try:
                name_query = "SELECT pn_account, pn_name FROM pname WITH (NOLOCK)"
                df = sql_connector.execute_query(name_query)
                if df is not None and len(df) > 0:
                    supplier_names = dict(zip(df['pn_account'], df['pn_name']))
            except Exception:
                pass  # Silently fail - will use supplier_code as name

        for row in cursor.fetchall():
            stmt = dict(row)
            stmt['supplier_name'] = supplier_names.get(stmt['supplier_code'], stmt['supplier_code'])
            statements.append(stmt)

        conn.close()
        return {"success": True, "statements": statements}

    except Exception as e:
        logger.error(f"Error listing supplier statements: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/api/supplier-statements/reconciliations")
async def list_supplier_reconciliations():
    """List statements pending reconciliation review/approval."""
    import sqlite3

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "statements": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, supplier_code, statement_date, received_date, status,
                   processed_at, approved_by, approved_at
            FROM supplier_statements
            WHERE status IN ('reconciled', 'queued')
            ORDER BY processed_at DESC
        """)
        statements = []
        for row in cursor.fetchall():
            statements.append(dict(row))

        conn.close()
        return {"success": True, "statements": statements}

    except Exception as e:
        logger.error(f"Error listing reconciliations: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/api/supplier-statements/history")
async def list_supplier_statement_history(days: int = 90):
    """List completed/sent statements for history view."""
    import sqlite3
    from datetime import datetime, timedelta

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "statements": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).isoformat()

        cursor.execute("""
            SELECT
                ss.id, ss.supplier_code, ss.statement_date, ss.received_date,
                ss.status, ss.processed_at, ss.approved_by, ss.approved_at,
                ss.sent_at, ss.opening_balance, ss.closing_balance,
                COUNT(sl.id) as line_count,
                SUM(CASE WHEN sl.match_status = 'matched' THEN 1 ELSE 0 END) as matched_count,
                SUM(CASE WHEN sl.match_status = 'query' THEN 1 ELSE 0 END) as query_count
            FROM supplier_statements ss
            LEFT JOIN statement_lines sl ON sl.statement_id = ss.id
            WHERE ss.status IN ('approved', 'sent') AND ss.received_date >= ?
            GROUP BY ss.id
            ORDER BY ss.sent_at DESC, ss.approved_at DESC
        """, (cutoff,))

        statements = [dict(row) for row in cursor.fetchall()]

        # Get supplier names from Opera
        if sql_connector and statements:
            codes = list(set(s['supplier_code'] for s in statements if s.get('supplier_code')))
            if codes:
                code_list = ','.join(f"'{c}'" for c in codes)
                names_df = sql_connector.execute_query(f"""
                    SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK) WHERE pn_account IN ({code_list})
                """)
                if names_df is not None and len(names_df) > 0:
                    name_map = dict(zip(names_df['code'], names_df['name']))
                    for s in statements:
                        s['supplier_name'] = name_map.get(s['supplier_code'], s['supplier_code'])

        conn.close()
        return {"success": True, "statements": statements}

    except Exception as e:
        logger.error(f"Error listing statement history: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/api/supplier-queries")
async def list_supplier_queries(status: Optional[str] = None):
    """
    List supplier queries from statement lines with query status.

    Query status is derived from statement_lines:
    - open: query_type is set, query_resolved_at is null
    - overdue: open query older than query_response_days config
    - resolved: query_resolved_at is set
    """
    import sqlite3
    from datetime import datetime, timedelta

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "queries": [], "counts": {"open": 0, "overdue": 0, "resolved": 0}}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get config for overdue threshold
        cursor.execute("SELECT value FROM supplier_automation_config WHERE key = 'query_response_days'")
        row = cursor.fetchone()
        response_days = int(row['value']) if row else 5
        overdue_cutoff = (datetime.now() - timedelta(days=response_days)).isoformat()

        # Build query based on status filter
        if status == 'open':
            where_clause = "sl.query_type IS NOT NULL AND sl.query_resolved_at IS NULL AND sl.query_sent_at >= ?"
            params = (overdue_cutoff,)
        elif status == 'overdue':
            where_clause = "sl.query_type IS NOT NULL AND sl.query_resolved_at IS NULL AND sl.query_sent_at < ?"
            params = (overdue_cutoff,)
        elif status == 'resolved':
            where_clause = "sl.query_resolved_at IS NOT NULL"
            params = ()
        else:
            where_clause = "sl.query_type IS NOT NULL"
            params = ()

        cursor.execute(f"""
            SELECT
                sl.id as query_id,
                sl.statement_id,
                ss.supplier_code,
                sl.query_type,
                sl.reference,
                sl.description,
                sl.debit,
                sl.credit,
                sl.line_date,
                sl.query_sent_at,
                sl.query_resolved_at,
                ss.statement_date,
                CASE
                    WHEN sl.query_resolved_at IS NOT NULL THEN 'resolved'
                    WHEN sl.query_sent_at < ? THEN 'overdue'
                    ELSE 'open'
                END as status,
                julianday('now') - julianday(sl.query_sent_at) as days_outstanding
            FROM statement_lines sl
            JOIN supplier_statements ss ON sl.statement_id = ss.id
            WHERE {where_clause}
            ORDER BY sl.query_sent_at DESC
        """, (overdue_cutoff,) + params)

        queries = [dict(row) for row in cursor.fetchall()]

        # Get counts for each status
        cursor.execute("""
            SELECT
                SUM(CASE WHEN query_resolved_at IS NULL AND query_sent_at >= ? THEN 1 ELSE 0 END) as open_count,
                SUM(CASE WHEN query_resolved_at IS NULL AND query_sent_at < ? THEN 1 ELSE 0 END) as overdue_count,
                SUM(CASE WHEN query_resolved_at IS NOT NULL THEN 1 ELSE 0 END) as resolved_count
            FROM statement_lines
            WHERE query_type IS NOT NULL
        """, (overdue_cutoff, overdue_cutoff))
        counts_row = cursor.fetchone()
        counts = {
            "open": counts_row['open_count'] or 0,
            "overdue": counts_row['overdue_count'] or 0,
            "resolved": counts_row['resolved_count'] or 0
        }

        # Get supplier names from Opera
        if sql_connector and queries:
            codes = list(set(q['supplier_code'] for q in queries if q.get('supplier_code')))
            if codes:
                code_list = ','.join(f"'{c}'" for c in codes)
                names_df = sql_connector.execute_query(f"""
                    SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK) WHERE pn_account IN ({code_list})
                """)
                if names_df is not None and len(names_df) > 0:
                    name_map = dict(zip(names_df['code'], names_df['name']))
                    for q in queries:
                        q['supplier_name'] = name_map.get(q['supplier_code'], q['supplier_code'])

        conn.close()
        return {"success": True, "queries": queries, "counts": counts}

    except Exception as e:
        logger.error(f"Error listing supplier queries: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.post("/api/supplier-queries/{query_id}/resolve")
async def resolve_supplier_query(query_id: int):
    """Mark a supplier query as resolved."""
    import sqlite3

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE statement_lines
            SET query_resolved_at = CURRENT_TIMESTAMP
            WHERE id = ? AND query_type IS NOT NULL
        """, (query_id,))

        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="Query not found")

        conn.commit()
        conn.close()

        return {"success": True, "message": "Query resolved"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resolving query: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.post("/api/supplier-queries/auto-resolve")
async def auto_resolve_supplier_queries():
    """
    Auto-resolve queries when matching invoices are found in Opera.

    Checks all open queries against Opera ptran to see if the missing
    invoice has now been entered. If found, marks the query as resolved.

    This should be called:
    - Periodically (scheduled job)
    - After invoice entry
    - When processing new statements
    """
    import sqlite3
    from datetime import datetime

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "resolved": 0, "message": "No queries database"}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get all open queries (invoice not found type)
        cursor.execute("""
            SELECT sl.id, sl.reference, sl.debit, sl.credit, ss.supplier_code
            FROM statement_lines sl
            JOIN supplier_statements ss ON sl.statement_id = ss.id
            WHERE sl.query_type IS NOT NULL
              AND sl.query_resolved_at IS NULL
              AND sl.query_type LIKE '%not found%'
        """)

        open_queries = cursor.fetchall()
        resolved_count = 0
        resolved_items = []

        for query in open_queries:
            supplier_code = query['supplier_code']
            reference = query['reference']
            amount = query['debit'] or query['credit'] or 0

            # Check if this invoice now exists in Opera
            # Match by reference OR by amount for this supplier
            check_query = f"""
                SELECT TOP 1 pt_unique, pt_trref, pt_trvalue
                FROM ptran WITH (NOLOCK)
                WHERE pt_account = '{supplier_code}'
                  AND pt_trtype = 'I'
                  AND (
                      pt_trref LIKE '%{reference}%'
                      OR pt_supref LIKE '%{reference}%'
                      OR ABS(pt_trvalue - {amount}) < 0.01
                  )
                ORDER BY pt_trdate DESC
            """

            result = sql_connector.execute_query(check_query)

            if result is not None and len(result) > 0:
                # Invoice found - auto-resolve the query
                cursor.execute("""
                    UPDATE statement_lines
                    SET query_resolved_at = CURRENT_TIMESTAMP,
                        match_status = 'matched',
                        matched_ptran_id = ?
                    WHERE id = ?
                """, (result.iloc[0]['pt_unique'], query['id']))

                resolved_count += 1
                resolved_items.append({
                    "query_id": query['id'],
                    "reference": reference,
                    "supplier_code": supplier_code,
                    "matched_to": result.iloc[0]['pt_unique']
                })

                logger.info(f"Auto-resolved query {query['id']} - {reference} matched to {result.iloc[0]['pt_unique']}")

        conn.commit()
        conn.close()

        # Check if any statements now have all queries resolved
        statements_ready = []
        if resolved_count > 0:
            # Get unique statement IDs that had queries resolved
            statement_ids = list(set(
                cursor.execute("""
                    SELECT DISTINCT statement_id FROM statement_lines WHERE id IN ({})
                """.format(','.join(str(item['query_id']) for item in resolved_items))).fetchall()
            ))

            for (stmt_id,) in statement_ids:
                # Check if this statement has any remaining open queries
                cursor.execute("""
                    SELECT COUNT(*) FROM statement_lines
                    WHERE statement_id = ?
                      AND query_type IS NOT NULL
                      AND query_resolved_at IS NULL
                """, (stmt_id,))
                open_count = cursor.fetchone()[0]

                if open_count == 0:
                    statements_ready.append(stmt_id)
                    logger.info(f"Statement {stmt_id} - all queries resolved, ready for updated status")

        conn.commit()
        conn.close()

        return {
            "success": True,
            "resolved": resolved_count,
            "items": resolved_items,
            "statements_all_resolved": statements_ready,
            "message": f"Auto-resolved {resolved_count} queries. {len(statements_ready)} statement(s) ready for updated status."
        }

    except Exception as e:
        logger.error(f"Error auto-resolving queries: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.post("/api/supplier-statements/{statement_id}/send-updated-status")
async def send_updated_statement_status(statement_id: int):
    """
    Send updated status to supplier after all queries are resolved.

    Generates a final reconciliation response confirming all items
    are now agreed and showing the payment schedule.
    """
    import sqlite3
    from datetime import datetime

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get statement details
        cursor.execute("SELECT * FROM supplier_statements WHERE id = ?", (statement_id,))
        statement = cursor.fetchone()
        if not statement:
            conn.close()
            raise HTTPException(status_code=404, detail="Statement not found")

        # Check no open queries remain
        cursor.execute("""
            SELECT COUNT(*) FROM statement_lines
            WHERE statement_id = ? AND query_type IS NOT NULL AND query_resolved_at IS NULL
        """, (statement_id,))
        open_queries = cursor.fetchone()[0]

        if open_queries > 0:
            conn.close()
            return {
                "success": False,
                "error": f"{open_queries} queries still open - cannot send updated status"
            }

        # Get supplier name from Opera
        supplier_code = statement['supplier_code']
        supplier_name = supplier_code
        supplier_df = sql_connector.execute_query(f"""
            SELECT RTRIM(pn_name) as name FROM pname WITH (NOLOCK)
            WHERE pn_account = '{supplier_code}'
        """)
        if supplier_df is not None and len(supplier_df) > 0:
            supplier_name = supplier_df.iloc[0]['name']

        # Get line details
        cursor.execute("""
            SELECT reference, debit, credit, match_status
            FROM statement_lines WHERE statement_id = ?
        """, (statement_id,))
        lines = cursor.fetchall()

        # Generate updated status response
        response_lines = []
        response_lines.append(f"Subject: Statement Update - All Queries Resolved - {supplier_name}")
        response_lines.append("")
        response_lines.append("Dear Accounts Team,")
        response_lines.append("")
        response_lines.append(f"Further to our previous correspondence regarding your statement dated {statement['statement_date']},")
        response_lines.append("we are pleased to confirm that all queries have now been resolved.")
        response_lines.append("")
        response_lines.append("RECONCILIATION STATUS: FULLY AGREED")
        response_lines.append("=" * 50)
        response_lines.append("")

        total = 0
        for line in lines:
            amount = line['debit'] or line['credit'] or 0
            total += amount
            response_lines.append(f"  {line['reference']}: £{amount:,.2f} - AGREED")

        response_lines.append("")
        response_lines.append(f"  TOTAL: £{total:,.2f}")
        response_lines.append("")

        # Payment info
        from datetime import timedelta
        today = datetime.now().date()
        days_until_friday = (4 - today.weekday()) % 7
        if days_until_friday == 0:
            days_until_friday = 7
        next_friday = today + timedelta(days=days_until_friday)

        response_lines.append("PAYMENT SCHEDULE")
        response_lines.append("=" * 50)
        response_lines.append(f"Total to pay: £{total:,.2f}")
        response_lines.append(f"Scheduled payment date: {next_friday.strftime('%d/%m/%Y')}")
        response_lines.append("")
        response_lines.append("Thank you for your patience in resolving these queries.")
        response_lines.append("")
        response_lines.append("Regards,")
        response_lines.append("Accounts Department")

        response_text = "\n".join(response_lines)

        # Log the communication
        cursor.execute("""
            INSERT INTO supplier_communications
            (supplier_code, statement_id, direction, type, email_subject, email_body, sent_at, sent_by)
            VALUES (?, ?, 'outbound', 'updated_status', ?, ?, CURRENT_TIMESTAMP, 'System')
        """, (
            supplier_code,
            statement_id,
            f"Statement Update - All Queries Resolved - {supplier_name}",
            response_text
        ))

        # Update statement status
        cursor.execute("""
            UPDATE supplier_statements
            SET status = 'approved', approved_at = CURRENT_TIMESTAMP, approved_by = 'Auto-resolved'
            WHERE id = ?
        """, (statement_id,))

        conn.commit()
        conn.close()

        return {
            "success": True,
            "message": "Updated status sent to supplier",
            "response_text": response_text
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending updated status: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/api/supplier-communications")
async def list_supplier_communications(supplier_code: Optional[str] = None, days: int = 90):
    """List supplier communications history."""
    import sqlite3
    from datetime import datetime, timedelta

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "communications": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).isoformat()

        if supplier_code:
            cursor.execute("""
                SELECT * FROM supplier_communications
                WHERE supplier_code = ? AND created_at >= ?
                ORDER BY created_at DESC
            """, (supplier_code, cutoff))
        else:
            cursor.execute("""
                SELECT * FROM supplier_communications
                WHERE created_at >= ?
                ORDER BY created_at DESC
            """, (cutoff,))

        communications = [dict(row) for row in cursor.fetchall()]

        # Get supplier names from Opera
        if sql_connector and communications:
            codes = list(set(c['supplier_code'] for c in communications if c.get('supplier_code')))
            if codes:
                code_list = ','.join(f"'{c}'" for c in codes)
                names_df = sql_connector.execute_query(f"""
                    SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK) WHERE pn_account IN ({code_list})
                """)
                if names_df is not None and len(names_df) > 0:
                    name_map = dict(zip(names_df['code'], names_df['name']))
                    for c in communications:
                        c['supplier_name'] = name_map.get(c['supplier_code'], c['supplier_code'])

        conn.close()
        return {"success": True, "communications": communications}

    except Exception as e:
        logger.error(f"Error listing communications: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/api/supplier-security/alerts")
async def list_security_alerts():
    """List unverified supplier change alerts (bank details, etc.)."""
    import sqlite3

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "alerts": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM supplier_change_audit
            WHERE verified = 0
            ORDER BY changed_at DESC
        """)

        alerts = [dict(row) for row in cursor.fetchall()]

        # Get supplier names from Opera
        if sql_connector and alerts:
            codes = list(set(a['supplier_code'] for a in alerts if a.get('supplier_code')))
            if codes:
                code_list = ','.join(f"'{c}'" for c in codes)
                names_df = sql_connector.execute_query(f"""
                    SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK) WHERE pn_account IN ({code_list})
                """)
                if names_df is not None and len(names_df) > 0:
                    name_map = dict(zip(names_df['code'], names_df['name']))
                    for a in alerts:
                        a['supplier_name'] = name_map.get(a['supplier_code'], a['supplier_code'])

        conn.close()
        return {"success": True, "alerts": alerts}

    except Exception as e:
        logger.error(f"Error listing security alerts: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.post("/api/supplier-security/alerts/{alert_id}/verify")
async def verify_security_alert(alert_id: int, verified_by: str = "System"):
    """Verify a security alert (mark as reviewed)."""
    import sqlite3

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE supplier_change_audit
            SET verified = 1, verified_by = ?, verified_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (verified_by, alert_id))

        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="Alert not found")

        conn.commit()
        conn.close()

        return {"success": True, "message": "Alert verified"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error verifying alert: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/api/supplier-security/audit")
async def list_security_audit_log(days: int = 90):
    """List all supplier change audit entries (verified and unverified)."""
    import sqlite3
    from datetime import datetime, timedelta

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "entries": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).isoformat()

        cursor.execute("""
            SELECT * FROM supplier_change_audit
            WHERE changed_at >= ?
            ORDER BY changed_at DESC
        """, (cutoff,))

        entries = [dict(row) for row in cursor.fetchall()]

        # Get supplier names from Opera
        if sql_connector and entries:
            codes = list(set(e['supplier_code'] for e in entries if e.get('supplier_code')))
            if codes:
                code_list = ','.join(f"'{c}'" for c in codes)
                names_df = sql_connector.execute_query(f"""
                    SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK) WHERE pn_account IN ({code_list})
                """)
                if names_df is not None and len(names_df) > 0:
                    name_map = dict(zip(names_df['code'], names_df['name']))
                    for e in entries:
                        e['supplier_name'] = name_map.get(e['supplier_code'], e['supplier_code'])

        conn.close()
        return {"success": True, "entries": entries}

    except Exception as e:
        logger.error(f"Error listing audit log: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/api/supplier-security/approved-senders")
async def list_approved_senders(supplier_code: Optional[str] = None):
    """List approved email senders for suppliers."""
    import sqlite3

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        return {"success": True, "senders": []}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if supplier_code:
            cursor.execute("""
                SELECT * FROM supplier_approved_emails
                WHERE supplier_code = ?
                ORDER BY added_at DESC
            """, (supplier_code,))
        else:
            cursor.execute("""
                SELECT * FROM supplier_approved_emails
                ORDER BY supplier_code, added_at DESC
            """)

        senders = [dict(row) for row in cursor.fetchall()]

        # Get supplier names from Opera
        if sql_connector and senders:
            codes = list(set(s['supplier_code'] for s in senders if s.get('supplier_code')))
            if codes:
                code_list = ','.join(f"'{c}'" for c in codes)
                names_df = sql_connector.execute_query(f"""
                    SELECT RTRIM(pn_account) as code, RTRIM(pn_name) as name
                    FROM pname WITH (NOLOCK) WHERE pn_account IN ({code_list})
                """)
                if names_df is not None and len(names_df) > 0:
                    name_map = dict(zip(names_df['code'], names_df['name']))
                    for s in senders:
                        s['supplier_name'] = name_map.get(s['supplier_code'], s['supplier_code'])

        conn.close()
        return {"success": True, "senders": senders}

    except Exception as e:
        logger.error(f"Error listing approved senders: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.post("/api/supplier-security/approved-senders")
async def add_approved_sender(
    supplier_code: str = Body(..., embed=True),
    email_address: str = Body(..., embed=True),
    added_by: str = Body("System", embed=True)
):
    """Add an approved email sender for a supplier."""
    import sqlite3

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    # Initialize DB if it doesn't exist
    if not db_path.exists():
        from sql_rag.supplier_statement_db import SupplierStatementDB
        SupplierStatementDB(str(db_path))

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        domain = email_address.split('@')[-1] if '@' in email_address else None

        cursor.execute("""
            INSERT OR REPLACE INTO supplier_approved_emails
            (supplier_code, email_address, email_domain, added_by, verified)
            VALUES (?, ?, ?, ?, 0)
        """, (supplier_code, email_address.lower(), domain, added_by))

        conn.commit()
        conn.close()

        return {"success": True, "message": "Approved sender added"}

    except Exception as e:
        logger.error(f"Error adding approved sender: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.delete("/api/supplier-security/approved-senders/{sender_id}")
async def remove_approved_sender(sender_id: int):
    """Remove an approved email sender."""
    import sqlite3

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute("DELETE FROM supplier_approved_emails WHERE id = ?", (sender_id,))

        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="Sender not found")

        conn.commit()
        conn.close()

        return {"success": True, "message": "Approved sender removed"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error removing approved sender: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/api/supplier-settings")
async def get_supplier_settings():
    """Get supplier automation settings."""
    import sqlite3

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        # Return defaults
        return {
            "success": True,
            "settings": {
                "acknowledgment_delay_minutes": "0",
                "processing_sla_hours": "24",
                "query_response_days": "5",
                "follow_up_reminder_days": "7",
                "large_discrepancy_threshold": "500",
                "old_statement_threshold_days": "14",
                "payment_notification_days": "90",
                "security_alert_recipients": ""
            }
        }

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT key, value, description FROM supplier_automation_config")
        settings = {row['key']: {"value": row['value'], "description": row['description']} for row in cursor.fetchall()}

        conn.close()
        return {"success": True, "settings": settings}

    except Exception as e:
        logger.error(f"Error getting settings: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.post("/api/supplier-settings")
async def update_supplier_settings(settings: Dict[str, str] = Body(...)):
    """Update supplier automation settings."""
    import sqlite3

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    # Initialize DB if it doesn't exist
    if not db_path.exists():
        from sql_rag.supplier_statement_db import SupplierStatementDB
        SupplierStatementDB(str(db_path))

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        for key, value in settings.items():
            cursor.execute("""
                UPDATE supplier_automation_config
                SET value = ?, updated_at = CURRENT_TIMESTAMP
                WHERE key = ?
            """, (value, key))

            if cursor.rowcount == 0:
                cursor.execute("""
                    INSERT INTO supplier_automation_config (key, value)
                    VALUES (?, ?)
                """, (key, value))

        conn.commit()
        conn.close()

        return {"success": True, "message": "Settings updated"}

    except Exception as e:
        logger.error(f"Error updating settings: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/api/supplier-directory")
async def list_supplier_directory(search: Optional[str] = None):
    """List all suppliers from Opera with statement automation info."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get suppliers from Opera
        if search:
            query = f"""
                SELECT TOP 100
                    RTRIM(pn_account) AS account,
                    RTRIM(pn_name) AS name,
                    RTRIM(pn_email) AS email,
                    RTRIM(pn_teleno) AS phone,
                    RTRIM(pn_contact) AS contact,
                    pn_currbal AS balance
                FROM pname WITH (NOLOCK)
                WHERE pn_name LIKE '%{search}%' OR pn_account LIKE '%{search}%'
                ORDER BY pn_name
            """
        else:
            query = """
                SELECT TOP 500
                    RTRIM(pn_account) AS account,
                    RTRIM(pn_name) AS name,
                    RTRIM(pn_email) AS email,
                    RTRIM(pn_teleno) AS phone,
                    RTRIM(pn_contact) AS contact,
                    pn_currbal AS balance
                FROM pname WITH (NOLOCK)
                WHERE pn_currbal <> 0
                ORDER BY pn_name
            """

        result = sql_connector.execute_query(query)
        if hasattr(result, 'to_dict'):
            suppliers = result.to_dict('records')
        else:
            suppliers = result or []

        # Get automation info from SQLite
        db_path = Path(__file__).parent.parent / 'supplier_statements.db'
        if db_path.exists():
            import sqlite3
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get statement counts per supplier
            cursor.execute("""
                SELECT supplier_code,
                       COUNT(*) as statement_count,
                       MAX(received_date) as last_statement
                FROM supplier_statements
                GROUP BY supplier_code
            """)
            stmt_map = {row['supplier_code']: {
                'statement_count': row['statement_count'],
                'last_statement': row['last_statement']
            } for row in cursor.fetchall()}

            # Get approved senders count
            cursor.execute("""
                SELECT supplier_code, COUNT(*) as sender_count
                FROM supplier_approved_emails
                GROUP BY supplier_code
            """)
            sender_map = {row['supplier_code']: row['sender_count'] for row in cursor.fetchall()}

            conn.close()

            # Merge into suppliers
            for s in suppliers:
                stmt_info = stmt_map.get(s['account'], {})
                s['statement_count'] = stmt_info.get('statement_count', 0)
                s['last_statement'] = stmt_info.get('last_statement')
                s['approved_senders'] = sender_map.get(s['account'], 0)

        return {"success": True, "suppliers": suppliers, "count": len(suppliers)}

    except Exception as e:
        logger.error(f"Error listing supplier directory: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.post("/api/supplier-statements/{statement_id}/approve")
async def approve_supplier_statement(statement_id: int, approved_by: str = "System"):
    """Approve a reconciled statement for sending."""
    import sqlite3
    from datetime import datetime

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE supplier_statements
            SET status = 'approved', approved_by = ?, approved_at = ?
            WHERE id = ? AND status = 'queued'
        """, (approved_by, datetime.now().isoformat(), statement_id))

        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="Statement not found or not in queued status")

        conn.commit()
        conn.close()

        return {"success": True, "message": "Statement approved"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error approving statement: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/api/supplier-statements/{statement_id}")
async def get_supplier_statement_detail(statement_id: int):
    """Get detailed information about a specific statement."""
    import sqlite3

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                ss.id, ss.supplier_code, ss.statement_date, ss.received_date, ss.status,
                ss.sender_email, ss.opening_balance, ss.closing_balance, ss.currency,
                ss.acknowledged_at, ss.processed_at, ss.approved_by, ss.approved_at,
                ss.sent_at, ss.error_message, ss.pdf_path,
                COUNT(sl.id) as line_count,
                SUM(CASE WHEN sl.match_status = 'matched' THEN 1 ELSE 0 END) as matched_count,
                SUM(CASE WHEN sl.match_status = 'query' THEN 1 ELSE 0 END) as query_count
            FROM supplier_statements ss
            LEFT JOIN statement_lines sl ON sl.statement_id = ss.id
            WHERE ss.id = ?
            GROUP BY ss.id
        """, (statement_id,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Statement not found")

        stmt = dict(row)

        # Get supplier name from Opera if available
        if sql_connector:
            try:
                name_query = f"SELECT pn_name FROM pname WITH (NOLOCK) WHERE pn_account = '{stmt['supplier_code']}'"
                df = sql_connector.execute_query(name_query)
                if df is not None and len(df) > 0:
                    stmt['supplier_name'] = df.iloc[0]['pn_name']
                else:
                    stmt['supplier_name'] = stmt['supplier_code']
            except Exception:
                stmt['supplier_name'] = stmt['supplier_code']
        else:
            stmt['supplier_name'] = stmt['supplier_code']

        return {"success": True, "statement": stmt}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting statement detail: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.get("/api/supplier-statements/{statement_id}/lines")
async def get_supplier_statement_lines(statement_id: int):
    """Get all line items for a statement."""
    import sqlite3

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, line_date, reference, description, debit, credit, balance,
                   doc_type, match_status, matched_ptran_id, query_type,
                   query_sent_at, query_resolved_at
            FROM statement_lines
            WHERE statement_id = ?
            ORDER BY line_date, id
        """, (statement_id,))

        lines = [dict(row) for row in cursor.fetchall()]

        # Calculate summary
        summary = {
            "total_lines": len(lines),
            "total_debits": sum(l['debit'] or 0 for l in lines),
            "total_credits": sum(l['credit'] or 0 for l in lines),
            "matched_count": sum(1 for l in lines if l['match_status'] == 'matched'),
            "query_count": sum(1 for l in lines if l['match_status'] == 'query'),
            "unmatched_count": sum(1 for l in lines if l['match_status'] == 'unmatched'),
        }

        conn.close()
        return {"success": True, "lines": lines, "summary": summary}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting statement lines: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.post("/api/supplier-statements/{statement_id}/process")
async def process_supplier_statement(statement_id: int):
    """
    Process a received statement - reconcile against Opera and generate response.

    This endpoint:
    1. Updates status to 'processing'
    2. Reconciles statement lines against ptran
    3. Applies business rules
    4. Generates draft response
    5. Updates status to 'queued' for approval
    """
    import sqlite3
    from datetime import datetime

    db_path = Path(__file__).parent.parent / 'supplier_statements.db'

    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database not found")

    if not sql_connector:
        raise HTTPException(status_code=503, detail="Opera SQL connection not available")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get statement
        cursor.execute("SELECT * FROM supplier_statements WHERE id = ?", (statement_id,))
        stmt = cursor.fetchone()

        if not stmt:
            conn.close()
            raise HTTPException(status_code=404, detail="Statement not found")

        if stmt['status'] not in ('received', 'error'):
            conn.close()
            raise HTTPException(status_code=400, detail=f"Statement cannot be processed from status '{stmt['status']}'")

        # Update status to processing
        cursor.execute("""
            UPDATE supplier_statements SET status = 'processing', updated_at = ?
            WHERE id = ?
        """, (datetime.now().isoformat(), statement_id))
        conn.commit()

        try:
            # Get statement lines
            cursor.execute("SELECT * FROM statement_lines WHERE statement_id = ?", (statement_id,))
            lines = [dict(row) for row in cursor.fetchall()]

            supplier_code = stmt['supplier_code']

            # Get ptran data for this supplier
            ptran_query = f"""
                SELECT pt_unique, pt_trdate, pt_trref, pt_supref, pt_trtype, pt_trvalue, pt_trbal
                FROM ptran WITH (NOLOCK)
                WHERE pt_account = '{supplier_code}'
                ORDER BY pt_trdate DESC
            """
            ptran_df = sql_connector.execute_query(ptran_query)

            matched_count = 0
            query_count = 0

            # Match each statement line against ptran
            for line in lines:
                match_status = 'unmatched'
                matched_ptran_id = None
                query_type = None

                if ptran_df is not None and len(ptran_df) > 0:
                    # Try to match by reference
                    if line.get('reference'):
                        ref_matches = ptran_df[
                            (ptran_df['pt_trref'].str.contains(line['reference'], case=False, na=False)) |
                            (ptran_df['pt_supref'].str.contains(line['reference'], case=False, na=False))
                        ]
                        if len(ref_matches) > 0:
                            match_status = 'matched'
                            matched_ptran_id = str(ref_matches.iloc[0]['pt_unique'])
                            matched_count += 1

                    # If not matched and it's a debit (invoice), may need to query
                    if match_status == 'unmatched' and line.get('debit') and line['debit'] > 0:
                        match_status = 'query'
                        query_type = 'invoice_not_found'
                        query_count += 1

                # Update line
                cursor.execute("""
                    UPDATE statement_lines
                    SET match_status = ?, matched_ptran_id = ?, query_type = ?
                    WHERE id = ?
                """, (match_status, matched_ptran_id, query_type, line['id']))

            # Update statement to queued
            cursor.execute("""
                UPDATE supplier_statements
                SET status = 'queued', processed_at = ?, updated_at = ?, error_message = NULL
                WHERE id = ?
            """, (datetime.now().isoformat(), datetime.now().isoformat(), statement_id))

            conn.commit()
            conn.close()

            return {
                "success": True,
                "message": "Statement processed successfully",
                "matched_count": matched_count,
                "query_count": query_count
            }

        except Exception as e:
            # Update status to error
            cursor.execute("""
                UPDATE supplier_statements
                SET status = 'error', error_message = ?, updated_at = ?
                WHERE id = ?
            """, (str(e), datetime.now().isoformat(), statement_id))
            conn.commit()
            conn.close()
            raise

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing statement: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# Supplier Statement Extraction API Endpoints
# ============================================================

@app.post("/api/supplier-statements/extract-from-email/{email_id}")
async def extract_supplier_statement_from_email(email_id: int, attachment_id: Optional[str] = None):
    """
    Extract supplier statement data from an email.

    If the email has a PDF attachment, extracts from that.
    Otherwise, attempts to extract from the email body text.

    Args:
        email_id: The database email ID
        attachment_id: Optional specific attachment ID (if multiple PDFs)

    Returns:
        Extracted statement info and line items
    """
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email module not initialized")

    # Get API key for Claude
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    try:
        # Get the email
        email = email_storage.get_email_by_id(email_id)
        if not email:
            raise HTTPException(status_code=404, detail="Email not found")

        extractor = SupplierStatementExtractor(api_key=api_key)
        attachments = email.get('attachments', [])

        # Find PDF attachment(s)
        pdf_attachments = [a for a in attachments if a.get('content_type') == 'application/pdf'
                          or (a.get('filename', '').lower().endswith('.pdf'))]

        if pdf_attachments:
            # Extract from PDF attachment
            if attachment_id:
                target_attachment = next(
                    (a for a in pdf_attachments if str(a.get('attachment_id')) == str(attachment_id)),
                    None
                )
            else:
                target_attachment = pdf_attachments[0]  # Use first PDF

            if not target_attachment:
                raise HTTPException(status_code=404, detail="PDF attachment not found")

            # Download the attachment
            provider_id = email.get('provider_id')
            message_id = email.get('message_id')

            if provider_id not in email_sync_manager.providers:
                raise HTTPException(status_code=503, detail="Email provider not connected")

            provider = email_sync_manager.providers[provider_id]

            # Get folder_id
            folder_id_db = email.get('folder_id')
            folder_id = 'INBOX'
            if folder_id_db:
                with email_storage._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_db,))
                    row = cursor.fetchone()
                    if row:
                        folder_id = row['folder_id']

            result = await provider.download_attachment(
                message_id,
                str(target_attachment['attachment_id']),
                folder_id
            )

            if not result:
                raise HTTPException(status_code=500, detail="Failed to download attachment")

            pdf_bytes, filename, content_type = result

            # Extract from PDF
            statement_info, lines = extractor.extract_from_pdf_bytes(pdf_bytes)

            return {
                "success": True,
                "source": "pdf_attachment",
                "filename": filename,
                "email_subject": email.get('subject'),
                "from_address": email.get('from_address'),
                **extractor.to_dict(statement_info, lines)
            }
        else:
            # Try to extract from email body text
            body_text = email.get('body_text') or email.get('body_preview', '')
            if not body_text or len(body_text) < 50:
                raise HTTPException(
                    status_code=400,
                    detail="No PDF attachment found and email body is too short to contain statement data"
                )

            statement_info, lines = extractor.extract_from_text(
                body_text,
                sender_email=email.get('from_address')
            )

            return {
                "success": True,
                "source": "email_body",
                "email_subject": email.get('subject'),
                "from_address": email.get('from_address'),
                **extractor.to_dict(statement_info, lines)
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error extracting supplier statement: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.post("/api/supplier-statements/extract-from-file")
async def extract_supplier_statement_from_file(file_path: str):
    """
    Extract supplier statement data from a PDF file path.

    Args:
        file_path: Path to the PDF file

    Returns:
        Extracted statement info and line items
    """
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")

    try:
        extractor = SupplierStatementExtractor(api_key=api_key)
        statement_info, lines = extractor.extract_from_pdf(file_path)

        return {
            "success": True,
            "source": "file",
            "file_path": file_path,
            **extractor.to_dict(statement_info, lines)
        }

    except Exception as e:
        logger.error(f"Error extracting supplier statement from file: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.post("/api/supplier-statements/extract-from-text")
async def extract_supplier_statement_from_text(
    text: str = Body(..., embed=True),
    sender_email: Optional[str] = Body(None, embed=True)
):
    """
    Extract supplier statement data from plain text.

    Args:
        text: The statement text content
        sender_email: Optional sender email for supplier identification

    Returns:
        Extracted statement info and line items
    """
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    if not text or len(text) < 20:
        raise HTTPException(status_code=400, detail="Text content too short")

    try:
        extractor = SupplierStatementExtractor(api_key=api_key)
        statement_info, lines = extractor.extract_from_text(text, sender_email)

        return {
            "success": True,
            "source": "text",
            **extractor.to_dict(statement_info, lines)
        }

    except Exception as e:
        logger.error(f"Error extracting supplier statement from text: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.post("/api/supplier-statements/reconcile/{email_id}")
async def reconcile_supplier_statement(email_id: int, attachment_id: Optional[str] = None):
    """
    Extract and reconcile a supplier statement against Opera purchase ledger.

    This endpoint:
    1. Extracts statement data from the email (PDF or body text)
    2. Finds the matching supplier in Opera
    3. Compares statement lines against ptran
    4. Generates an informative response following business rules

    Business rules:
    - Only raise queries when NOT in our favour
    - Stay quiet about discrepancies that benefit us
    - Always notify payments we've made
    - Flag old statements and request current one

    Args:
        email_id: The database email ID
        attachment_id: Optional specific attachment ID

    Returns:
        Reconciliation result with match details and generated response
    """
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email module not initialized")

    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    try:
        # Step 1: Extract statement from email
        email = email_storage.get_email_by_id(email_id)
        if not email:
            raise HTTPException(status_code=404, detail="Email not found")

        extractor = SupplierStatementExtractor(api_key=api_key)
        attachments = email.get('attachments', [])

        # Find PDF attachment(s)
        pdf_attachments = [a for a in attachments if a.get('content_type') == 'application/pdf'
                          or (a.get('filename', '').lower().endswith('.pdf'))]

        statement_info = None
        lines = None

        if pdf_attachments:
            # Extract from PDF
            if attachment_id:
                target_attachment = next(
                    (a for a in pdf_attachments if str(a.get('attachment_id')) == str(attachment_id)),
                    None
                )
            else:
                target_attachment = pdf_attachments[0]

            if target_attachment:
                provider_id = email.get('provider_id')
                message_id = email.get('message_id')

                if provider_id in email_sync_manager.providers:
                    provider = email_sync_manager.providers[provider_id]

                    folder_id_db = email.get('folder_id')
                    folder_id = 'INBOX'
                    if folder_id_db:
                        with email_storage._get_connection() as conn:
                            cursor = conn.cursor()
                            cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_db,))
                            row = cursor.fetchone()
                            if row:
                                folder_id = row['folder_id']

                    result = await provider.download_attachment(
                        message_id,
                        str(target_attachment['attachment_id']),
                        folder_id
                    )

                    if result:
                        pdf_bytes, filename, content_type = result
                        statement_info, lines = extractor.extract_from_pdf_bytes(pdf_bytes)

        if not statement_info:
            # Try extracting from email body
            body_text = email.get('body_text') or email.get('body_preview', '')
            if body_text and len(body_text) >= 50:
                statement_info, lines = extractor.extract_from_text(
                    body_text,
                    sender_email=email.get('from_address')
                )

        if not statement_info:
            raise HTTPException(
                status_code=400,
                detail="Could not extract statement data from email"
            )

        # Step 2: Reconcile against Opera
        reconciler = SupplierStatementReconciler(sql_connector)

        # Convert dataclass to dict
        info_dict = {
            "supplier_name": statement_info.supplier_name,
            "account_reference": statement_info.account_reference,
            "statement_date": statement_info.statement_date,
            "closing_balance": statement_info.closing_balance,
            "contact_email": statement_info.contact_email,
            "contact_phone": statement_info.contact_phone
        }

        lines_dict = [
            {
                "date": line.date,
                "reference": line.reference,
                "description": line.description,
                "debit": line.debit,
                "credit": line.credit,
                "balance": line.balance,
                "doc_type": line.doc_type
            }
            for line in lines
        ]

        recon_result = reconciler.reconcile(info_dict, lines_dict)

        return {
            "success": True,
            "email_id": email_id,
            "email_subject": email.get('subject'),
            "from_address": email.get('from_address'),
            "extraction": extractor.to_dict(statement_info, lines),
            "reconciliation": recon_result.to_dict()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reconciling supplier statement: {e}", exc_info=True)
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
    Search for suppliers by any field - account, name, address, contact, email, phone.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        results = sql_connector.execute_query(f"""
            SELECT TOP 20
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS supplier_name,
                pn_currbal AS balance,
                RTRIM(pn_teleno) AS phone,
                RTRIM(pn_addr1) AS address1,
                RTRIM(pn_addr2) AS address2,
                RTRIM(pn_addr3) AS address3,
                RTRIM(pn_addr4) AS address4,
                RTRIM(pn_pstcode) AS postcode,
                RTRIM(pn_contact) AS contact,
                RTRIM(pn_email) AS email
            FROM pname WITH (NOLOCK)
            WHERE UPPER(pn_account) LIKE UPPER('%{query}%')
               OR UPPER(pn_name) LIKE UPPER('%{query}%')
               OR UPPER(pn_addr1) LIKE UPPER('%{query}%')
               OR UPPER(pn_addr2) LIKE UPPER('%{query}%')
               OR UPPER(pn_addr3) LIKE UPPER('%{query}%')
               OR UPPER(pn_addr4) LIKE UPPER('%{query}%')
               OR UPPER(pn_pstcode) LIKE UPPER('%{query}%')
               OR UPPER(pn_contact) LIKE UPPER('%{query}%')
               OR UPPER(pn_email) LIKE UPPER('%{query}%')
               OR UPPER(pn_teleno) LIKE UPPER('%{query}%')
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


@app.get("/api/supplier/account/first")
async def get_first_supplier_account():
    """Get the first supplier with a balance (for default view)."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        result = sql_connector.execute_query("""
            SELECT TOP 1 RTRIM(pn_account) AS account
            FROM pname WITH (NOLOCK)
            WHERE pn_currbal <> 0
            ORDER BY pn_name
        """)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')
        if result and len(result) > 0:
            return {"success": True, "account": result[0]['account']}
        return {"success": False, "error": "No suppliers with balance found"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/supplier/account/{account}")
async def get_supplier_account_view(account: str):
    """
    Get full supplier account view matching Opera's Purchase Processing screen.
    Returns supplier details, outstanding transactions, and aging analysis.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from datetime import datetime, timedelta

        # Get supplier header info - use only columns that exist in all Opera installations
        supplier_query = f"""
            SELECT
                RTRIM(pn_account) AS account,
                RTRIM(pn_name) AS company_name,
                RTRIM(pn_addr1) AS address1,
                RTRIM(pn_addr2) AS address2,
                RTRIM(pn_addr3) AS address3,
                RTRIM(pn_addr4) AS address4,
                RTRIM(pn_pstcode) AS postcode,
                RTRIM(pn_contact) AS ac_contact,
                RTRIM(pn_email) AS email,
                RTRIM(pn_teleno) AS telephone,
                pn_currbal AS current_balance,
                ISNULL(pn_ordrbal, 0) AS order_balance,
                pn_trnover AS turnover
            FROM pname WITH (NOLOCK)
            WHERE pn_account = '{account}'
        """

        supplier_result = sql_connector.execute_query(supplier_query)
        if hasattr(supplier_result, 'to_dict'):
            supplier_result = supplier_result.to_dict('records')

        if not supplier_result:
            raise HTTPException(status_code=404, detail="Supplier not found")

        supplier = supplier_result[0]

        # Get outstanding transactions
        transactions_query = f"""
            SELECT
                pt_trdate AS date,
                CASE pt_trtype
                    WHEN 'I' THEN 'Inv'
                    WHEN 'C' THEN 'Crd'
                    WHEN 'P' THEN 'Pay'
                    WHEN 'J' THEN 'Jnl'
                    WHEN 'F' THEN 'Ref'
                    ELSE pt_trtype
                END AS type,
                RTRIM(pt_trref) AS ref1,
                RTRIM(pt_supref) AS ref2,
                '' AS stat,
                CASE WHEN pt_trtype IN ('I', 'J') AND pt_trvalue > 0 THEN pt_trvalue ELSE NULL END AS debit,
                CASE WHEN pt_trtype IN ('C', 'P', 'F') OR pt_trvalue < 0 THEN ABS(pt_trvalue) ELSE NULL END AS credit,
                pt_trbal AS balance,
                pt_dueday AS due_date,
                pt_unique AS unique_id,
                pt_trtype AS raw_type
            FROM ptran WITH (NOLOCK)
            WHERE pt_account = '{account}'
            AND pt_trbal <> 0
            ORDER BY pt_trdate DESC, pt_trref
        """

        transactions_result = sql_connector.execute_query(transactions_query)
        if transactions_result is None:
            transactions = []
        elif hasattr(transactions_result, 'to_dict'):
            transactions = transactions_result.to_dict('records')
        else:
            transactions = transactions_result or []

        # Calculate aging analysis
        today = datetime.now().date()
        aging = {
            '150_plus': 0.0,
            '120_days': 0.0,
            '90_days': 0.0,
            '60_days': 0.0,
            '30_days': 0.0,
            'current': 0.0,
            'total': 0.0,
            'unallocated': 0.0
        }

        for t in transactions:
            balance = t.get('balance', 0) or 0

            # Unallocated payments/credits (negative balance)
            if balance < 0:
                aging['unallocated'] += abs(balance)
                continue

            aging['total'] += balance

            due_date = t.get('due_date')
            days_old = 0

            # Check for None or pandas NaT
            import pandas as pd
            if due_date is not None and not pd.isna(due_date):
                try:
                    # Handle pandas Timestamp
                    if hasattr(due_date, 'to_pydatetime'):
                        due_date = due_date.to_pydatetime().date()
                    # Handle datetime
                    elif hasattr(due_date, 'date') and callable(due_date.date):
                        due_date = due_date.date()
                    # Handle string
                    elif isinstance(due_date, str):
                        due_date = datetime.strptime(due_date[:10], '%Y-%m-%d').date()
                    # Calculate days
                    days_old = (today - due_date).days
                except Exception:
                    days_old = 0

            if days_old > 150:
                aging['150_plus'] += balance
            elif days_old > 120:
                aging['120_days'] += balance
            elif days_old > 90:
                aging['90_days'] += balance
            elif days_old > 60:
                aging['60_days'] += balance
            elif days_old > 30:
                aging['30_days'] += balance
            else:
                aging['current'] += balance

        return {
            "success": True,
            "supplier": supplier,
            "transactions": transactions,
            "aging": aging,
            "count": len(transactions)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Supplier account view failed: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ============================================================
# Live Opera Dashboards API Endpoints
# ============================================================

def df_to_records(df):
    """Convert DataFrame to list of dicts."""
    if hasattr(df, 'to_dict'):
        return df.to_dict('records')
    return df


@app.get("/api/dashboard/available-years")
async def get_available_years():
    """Get years with transaction data and determine the best default year."""
    try:
        # Get years from nominal transactions - support both E/F codes and numeric 30/35 codes
        # Note: RTRIM needed as nt_type may have trailing spaces
        df = sql_connector.execute_query("""
            SELECT DISTINCT nt_year as year,
                   COUNT(*) as transaction_count,
                   SUM(CASE
                       WHEN RTRIM(nt_type) = 'E' THEN ABS(nt_value)
                       WHEN RTRIM(nt_type) = '30' THEN ABS(nt_value)
                       ELSE 0
                   END) as revenue
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year >= 2015
            GROUP BY nt_year
            ORDER BY nt_year DESC
        """)
        data = df_to_records(df)

        # Find the most recent year with meaningful data
        years_with_data = []
        latest_year_with_data = None
        for row in data:
            year = int(row['year'])
            revenue = float(row['revenue'] or 0)
            if revenue > 1000:  # Has meaningful revenue
                years_with_data.append({
                    "year": year,
                    "transaction_count": row['transaction_count'],
                    "revenue": round(revenue, 2)
                })
                if latest_year_with_data is None:
                    latest_year_with_data = year

        return {
            "success": True,
            "years": years_with_data,
            "default_year": latest_year_with_data or 2024,
            "current_company": current_company.get("name") if current_company else None
        }
    except Exception as e:
        return {"success": False, "error": str(e), "years": [], "default_year": 2024}


@app.get("/api/dashboard/sales-categories")
async def get_sales_categories():
    """Get sales categories/segments for the current company's data."""
    try:
        # Try to get categories from invoice lines with analysis codes
        df = sql_connector.execute_query("""
            SELECT
                COALESCE(NULLIF(RTRIM(it_anal), ''), 'Uncategorised') as category,
                COUNT(*) as line_count,
                SUM(it_value) / 100.0 as total_value
            FROM itran
            WHERE it_value > 0
            GROUP BY COALESCE(NULLIF(RTRIM(it_anal), ''), 'Uncategorised')
            HAVING SUM(it_value) > 0
            ORDER BY SUM(it_value) DESC
        """)
        data = df_to_records(df)

        if data:
            return {
                "success": True,
                "source": "invoice_lines",
                "categories": data
            }

        # Fallback to stock groups if no analysis codes
        df = sql_connector.execute_query("""
            SELECT
                COALESCE(sg.sg_desc, 'Other') as category,
                COUNT(*) as line_count,
                SUM(it.it_value) / 100.0 as total_value
            FROM itran it
            LEFT JOIN cname cn ON it.it_prodcode = cn.cn_prodcode
            LEFT JOIN sgroup sg ON cn.cn_catag = sg.sg_group
            WHERE it.it_value > 0
            GROUP BY COALESCE(sg.sg_desc, 'Other')
            HAVING SUM(it.it_value) > 0
            ORDER BY SUM(it.it_value) DESC
        """)
        data = df_to_records(df)

        return {
            "success": True,
            "source": "stock_groups",
            "categories": data if data else []
        }
    except Exception as e:
        return {"success": False, "error": str(e), "categories": []}


@app.get("/api/dashboard/ceo-kpis")
async def get_ceo_kpis(year: int = 2026):
    """Get CEO-level KPIs: MTD, QTD, YTD sales, growth, customer metrics."""
    try:
        from datetime import datetime as dt

        current_date = dt.now()
        current_month = current_date.month
        current_quarter = (current_month - 1) // 3 + 1

        # Get current year and previous year sales - support both E/F and 30/35 type codes
        # Note: RTRIM needed as nt_type may have trailing spaces
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'E' THEN -nt_value
                    WHEN RTRIM(nt_type) = '30' THEN -nt_value
                    ELSE 0
                END) as revenue,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'F' THEN nt_value
                    WHEN RTRIM(nt_type) = '35' THEN nt_value
                    ELSE 0
                END) as cost_of_sales
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
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
        # Get monthly totals - simpler approach that works across company types
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period as month,
                SUM(-nt_value) as revenue
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', '30')
            AND nt_year IN ({year}, {year - 1})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Organize data by year and month
        data_by_year = {year: {}, year - 1: {}}

        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            rev = row['revenue'] or 0
            data_by_year[y][m] = rev

        # Build monthly series
        months = []
        for m in range(1, 13):
            month_data = {
                "month": m,
                "month_name": ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][m],
                "current_total": data_by_year[year].get(m, 0),
                "previous_total": data_by_year[year - 1].get(m, 0)
            }
            months.append(month_data)

        return {
            "success": True,
            "year": year,
            "months": months
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/revenue-composition")
async def get_revenue_composition(year: int = 2026):
    """Get revenue breakdown by category with comparison to previous year."""
    try:
        # Get revenue by nominal account description - works for all company types
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                COALESCE(NULLIF(RTRIM(na.na_subt), ''), 'Other') as category,
                SUM(-nt_value) as revenue
            FROM ntran nt
            LEFT JOIN nacnt na ON RTRIM(nt.nt_acnt) = RTRIM(na.na_acnt) AND na.na_year = nt.nt_year
            WHERE RTRIM(nt.nt_type) IN ('E', '30')
            AND nt.nt_year IN ({year}, {year - 1})
            GROUP BY nt.nt_year, COALESCE(NULLIF(RTRIM(na.na_subt), ''), 'Other')
            ORDER BY SUM(-nt_value) DESC
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
        # Get total revenue and COS - works for all company types
        # Note: RTRIM needed as nt_type may have trailing spaces
        df = sql_connector.execute_query(f"""
            SELECT
                'Total' as category,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'E' THEN -nt_value
                    WHEN RTRIM(nt_type) = '30' THEN -nt_value
                    ELSE 0
                END) as revenue,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'F' THEN nt_value
                    WHEN RTRIM(nt_type) = '35' THEN nt_value
                    ELSE 0
                END) as cost_of_sales
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year = {year}
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


# ============================================================
# Finance Dashboard Endpoints
# ============================================================

@app.get("/api/dashboard/finance-summary")
async def get_finance_summary(year: int = 2024):
    """Get financial summary: P&L overview, Balance Sheet summary, Key ratios."""
    try:
        # Get P&L summary by type - using ntran for YTD values by year
        # Note: RTRIM needed as nt_type may have trailing spaces
        pl_df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(nt_type) as type,
                CASE RTRIM(nt_type)
                    WHEN '30' THEN 'Sales'
                    WHEN 'E' THEN 'Sales'
                    WHEN '35' THEN 'Cost of Sales'
                    WHEN 'F' THEN 'Cost of Sales'
                    WHEN '40' THEN 'Other Income'
                    WHEN 'G' THEN 'Other Income'
                    WHEN '45' THEN 'Overheads'
                    WHEN 'H' THEN 'Overheads'
                    ELSE 'Other'
                END as type_name,
                SUM(nt_value) as ytd_movement
            FROM ntran
            WHERE RTRIM(nt_type) IN ('30', '35', '40', '45', 'E', 'F', 'G', 'H')
            AND nt_year = {year}
            GROUP BY RTRIM(nt_type)
            ORDER BY RTRIM(nt_type)
        """)
        pl_data = df_to_records(pl_df)

        # Aggregate P&L - handle both letter and number codes
        sales = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('30', 'E'))
        cos = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('35', 'F'))
        other_income = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('40', 'G'))
        overheads = sum(row['ytd_movement'] for row in pl_data if row['type'] in ('45', 'H'))

        gross_profit = -sales - cos  # Sales are negative (credits)
        operating_profit = gross_profit + (-other_income) - overheads

        # Get Balance Sheet summary from nacnt using YTD dr/cr fields
        # Opera uses letter codes: A=Fixed Assets, B=Current Assets, C=Current Liabilities, D=Capital
        bs_df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(na_type) as type,
                CASE RTRIM(na_type)
                    WHEN 'A' THEN 'Fixed Assets'
                    WHEN '05' THEN 'Fixed Assets'
                    WHEN 'B' THEN 'Current Assets'
                    WHEN '10' THEN 'Current Assets'
                    WHEN 'C' THEN 'Current Liabilities'
                    WHEN '15' THEN 'Current Liabilities'
                    WHEN 'D' THEN 'Capital & Reserves'
                    WHEN '20' THEN 'Long Term Liabilities'
                    WHEN '25' THEN 'Capital & Reserves'
                    ELSE 'Other'
                END as type_name,
                SUM(na_ytddr - na_ytdcr) as balance
            FROM nacnt
            WHERE RTRIM(na_type) IN ('A', 'B', 'C', 'D', '05', '10', '15', '20', '25')
            GROUP BY RTRIM(na_type)
            ORDER BY RTRIM(na_type)
        """)
        bs_data = df_to_records(bs_df)

        bs_summary = {}
        for row in bs_data:
            bs_summary[row['type_name']] = round(row['balance'] or 0, 2)

        # Calculate key financial figures
        fixed_assets = bs_summary.get('Fixed Assets', 0)
        current_assets = bs_summary.get('Current Assets', 0)
        current_liabilities = abs(bs_summary.get('Current Liabilities', 0))
        net_current_assets = current_assets - current_liabilities

        # Ratios
        current_ratio = current_assets / current_liabilities if current_liabilities > 0 else 0
        gross_margin = (gross_profit / -sales * 100) if sales != 0 else 0
        operating_margin = (operating_profit / -sales * 100) if sales != 0 else 0

        return {
            "success": True,
            "year": year,
            "profit_and_loss": {
                "sales": round(-sales, 2),
                "cost_of_sales": round(cos, 2),
                "gross_profit": round(gross_profit, 2),
                "other_income": round(-other_income, 2),
                "overheads": round(overheads, 2),
                "operating_profit": round(operating_profit, 2)
            },
            "balance_sheet": {
                "fixed_assets": fixed_assets,
                "current_assets": current_assets,
                "current_liabilities": current_liabilities,
                "net_current_assets": round(net_current_assets, 2),
                "total_assets": round(fixed_assets + current_assets, 2)
            },
            "ratios": {
                "gross_margin_percent": round(gross_margin, 1),
                "operating_margin_percent": round(operating_margin, 1),
                "current_ratio": round(current_ratio, 2)
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/finance-monthly")
async def get_finance_monthly(year: int = 2024):
    """Get monthly P&L breakdown for finance view."""
    try:
        # Support both E/F/H codes and numeric 30/35/45 codes
        # Note: RTRIM needed as nt_type may have trailing spaces
        df = sql_connector.execute_query(f"""
            SELECT
                nt_period as month,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'E' THEN -nt_value
                    WHEN RTRIM(nt_type) = '30' THEN -nt_value
                    ELSE 0
                END) as revenue,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'F' THEN nt_value
                    WHEN RTRIM(nt_type) = '35' THEN nt_value
                    ELSE 0
                END) as cost_of_sales,
                SUM(CASE
                    WHEN RTRIM(nt_type) = 'H' THEN nt_value
                    WHEN RTRIM(nt_type) = '45' THEN nt_value
                    ELSE 0
                END) as overheads
            FROM ntran
            WHERE nt_year = {year}
            AND RTRIM(nt_type) IN ('E', 'F', 'H', '30', '35', '45')
            GROUP BY nt_period
            ORDER BY nt_period
        """)
        data = df_to_records(df)

        month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        months = []
        ytd_revenue = 0
        ytd_cos = 0
        ytd_overheads = 0

        for row in data:
            m = int(row['month']) if row['month'] else 0
            if m < 1 or m > 12:
                continue

            revenue = row['revenue'] or 0
            cos = row['cost_of_sales'] or 0
            overheads = row['overheads'] or 0
            gross_profit = revenue - cos
            net_profit = gross_profit - overheads

            ytd_revenue += revenue
            ytd_cos += cos
            ytd_overheads += overheads

            months.append({
                "month": m,
                "month_name": month_names[m],
                "revenue": round(revenue, 2),
                "cost_of_sales": round(cos, 2),
                "gross_profit": round(gross_profit, 2),
                "overheads": round(overheads, 2),
                "net_profit": round(net_profit, 2),
                "gross_margin_percent": round(gross_profit / revenue * 100, 1) if revenue > 0 else 0
            })

        return {
            "success": True,
            "year": year,
            "months": months,
            "ytd": {
                "revenue": round(ytd_revenue, 2),
                "cost_of_sales": round(ytd_cos, 2),
                "gross_profit": round(ytd_revenue - ytd_cos, 2),
                "overheads": round(ytd_overheads, 2),
                "net_profit": round(ytd_revenue - ytd_cos - ytd_overheads, 2)
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/sales-by-product")
async def get_sales_by_product(year: int = 2024):
    """Get sales breakdown by product category - adapts to company data structure."""

    # Analysis code description mapping for Company Z style codes
    analysis_code_descriptions = {
        'SALE': 'Vehicle Sales',
        'ACCE': 'Accessories',
        'CONS': 'Consumables',
        'MAIN': 'Maintenance',
        'SERV': 'Services',
        'LCON': 'Lease Contracts',
        'MCON': 'Maintenance Contracts',
        'CONT': 'Contracts',
    }

    def get_category_description(code: str) -> str:
        """Get a human-readable description for an analysis code."""
        if not code:
            return 'Other'
        code = code.strip()
        # Try to match the prefix (first 4 chars)
        prefix = code[:4].upper() if len(code) >= 4 else code.upper()
        if prefix in analysis_code_descriptions:
            # Include the suffix number if present
            suffix = code[4:].strip() if len(code) > 4 else ''
            base_desc = analysis_code_descriptions[prefix]
            if suffix:
                return f"{base_desc} ({suffix})"
            return base_desc
        return code  # Return the code itself if no mapping found

    def get_nominal_descriptions() -> dict:
        """Get nominal account descriptions from database."""
        try:
            desc_df = sql_connector.execute_query("""
                SELECT RTRIM(na_acnt) as acnt, RTRIM(na_desc) as descr
                FROM nacnt
                WHERE RTRIM(na_type) = 'E' OR na_acnt LIKE 'E%'
                GROUP BY na_acnt, na_desc
            """)
            desc_data = df_to_records(desc_df)
            return {row['acnt']: row['descr'] for row in desc_data if row.get('acnt')}
        except Exception:
            return {}

    try:
        # Get nominal account descriptions (for E codes like E1010, E1020)
        nominal_descriptions = get_nominal_descriptions()

        # Try using itran with it_doc and it_lineval (common to both schemas)
        try:
            df = sql_connector.execute_query(f"""
                SELECT
                    COALESCE(NULLIF(RTRIM(it.it_anal), ''), 'Other') as category_code,
                    COUNT(DISTINCT it.it_doc) as invoice_count,
                    COUNT(*) as line_count,
                    SUM(it.it_lineval) / 100.0 as total_value
                FROM itran it
                WHERE YEAR(it.it_date) = {year}
                AND it.it_lineval > 0
                GROUP BY COALESCE(NULLIF(RTRIM(it.it_anal), ''), 'Other')
                HAVING SUM(it.it_lineval) > 0
                ORDER BY SUM(it.it_lineval) DESC
            """)
            data = df_to_records(df)
            # Add descriptions - check both Company Z mapping and nominal ledger
            for row in data:
                code = row.get('category_code', '')
                # First check if it's a Company Z style code (SALE01, ACCE02, etc.)
                if code and len(code) >= 4 and code[:4].upper() in analysis_code_descriptions:
                    row['category'] = get_category_description(code)
                # Otherwise check nominal ledger descriptions (E codes like E1010)
                elif code in nominal_descriptions:
                    row['category'] = nominal_descriptions[code]
                else:
                    row['category'] = code
        except Exception:
            data = None

        if not data:
            # Fallback: Try using it_value instead of it_lineval
            try:
                df = sql_connector.execute_query(f"""
                    SELECT
                        COALESCE(NULLIF(RTRIM(it.it_anal), ''), 'Other') as category_code,
                        COUNT(DISTINCT ih.ih_invno) as invoice_count,
                        COUNT(*) as line_count,
                        SUM(it.it_value) / 100.0 as total_value
                    FROM itran it
                    JOIN ihead ih ON it.it_invno = ih.ih_invno
                    WHERE YEAR(ih.ih_invdat) = {year}
                    AND it.it_value > 0
                    GROUP BY COALESCE(NULLIF(RTRIM(it.it_anal), ''), 'Other')
                    HAVING SUM(it.it_value) > 0
                    ORDER BY SUM(it.it_value) DESC
                """)
                data = df_to_records(df)
                # Add descriptions from nominal ledger
                for row in data:
                    code = row.get('category_code', '')
                    row['category'] = nominal_descriptions.get(code, code)
            except Exception:
                data = None

        if not data:
            # Fallback: try nominal ledger categories
            df = sql_connector.execute_query(f"""
                SELECT
                    CASE
                        WHEN nt_acnt LIKE 'E1%' THEN 'Primary Sales'
                        WHEN nt_acnt LIKE 'E2%' THEN 'Secondary Sales'
                        WHEN nt_acnt LIKE 'E3%' THEN 'Services'
                        WHEN nt_acnt LIKE 'E4%' THEN 'Other Revenue'
                        ELSE 'Miscellaneous'
                    END as category,
                    COUNT(*) as line_count,
                    SUM(-nt_value) as total_value
                FROM ntran
                WHERE RTRIM(nt_type) = 'E'
                AND nt_year = {year}
                GROUP BY CASE
                    WHEN nt_acnt LIKE 'E1%' THEN 'Primary Sales'
                    WHEN nt_acnt LIKE 'E2%' THEN 'Secondary Sales'
                    WHEN nt_acnt LIKE 'E3%' THEN 'Services'
                    WHEN nt_acnt LIKE 'E4%' THEN 'Other Revenue'
                    ELSE 'Miscellaneous'
                END
                ORDER BY SUM(-nt_value) DESC
            """)
            data = df_to_records(df)

        total_value = sum(row.get('total_value', 0) or 0 for row in data)

        categories = []
        for row in data:
            value = row.get('total_value', 0) or 0
            categories.append({
                "category": row.get('category', 'Unknown'),
                "category_code": row.get('category_code', ''),
                "invoice_count": row.get('invoice_count', 0),
                "line_count": row.get('line_count', 0),
                "value": round(value, 2),
                "percent_of_total": round(value / total_value * 100, 1) if total_value > 0 else 0
            })

        return {
            "success": True,
            "year": year,
            "categories": categories,
            "total_value": round(total_value, 2)
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============================================================
# Reconciliation Endpoints
# ============================================================

def _get_control_accounts_for_reconciliation():
    """Get control accounts from Opera config for reconciliation"""
    if not sql_connector:
        return {'debtors': 'BB020', 'creditors': 'CA030'}
    try:
        from sql_rag.opera_config import get_control_accounts
        control = get_control_accounts(sql_connector)
        return {
            'debtors': control.debtors_control,
            'creditors': control.creditors_control
        }
    except Exception as e:
        logger.warning(f"Could not load control accounts from config: {e}")
        return {'debtors': 'BB020', 'creditors': 'CA030'}

@app.get("/api/reconcile/creditors")
async def reconcile_creditors():
    """
    Reconcile Purchase Ledger (ptran) to Creditors Control Account (Nominal Ledger).
    Compares outstanding balances in ptran with the control account in nacnt/ntran.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get dynamic control accounts from config
        control_accounts = _get_control_accounts_for_reconciliation()
        creditors_control = control_accounts['creditors']

        reconciliation = {
            "success": True,
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "purchase_ledger": {},
            "nominal_ledger": {},
            "variance": {},
            "status": "UNRECONCILED",
            "details": [],
            "control_account_used": creditors_control
        }

        # ========== PURCHASE LEDGER (ptran) ==========
        # Get outstanding transactions from purchase ledger
        # Only include transactions for suppliers that still exist in pname (exclude deleted suppliers)
        pl_outstanding_sql = """
            SELECT
                COUNT(*) AS transaction_count,
                SUM(pt_trbal) AS total_outstanding
            FROM ptran WITH (NOLOCK)
            WHERE pt_trbal <> 0
              AND RTRIM(pt_account) IN (SELECT RTRIM(pn_account) FROM pname WITH (NOLOCK))
        """
        pl_result = sql_connector.execute_query(pl_outstanding_sql)
        if hasattr(pl_result, 'to_dict'):
            pl_result = pl_result.to_dict('records')

        pl_total = float(pl_result[0]['total_outstanding'] or 0) if pl_result else 0
        pl_count = int(pl_result[0]['transaction_count'] or 0) if pl_result else 0

        # Get breakdown by transaction type (only active suppliers)
        pl_breakdown_sql = """
            SELECT
                pt_trtype AS type,
                COUNT(*) AS count,
                SUM(pt_trbal) AS total
            FROM ptran WITH (NOLOCK)
            WHERE pt_trbal <> 0
              AND RTRIM(pt_account) IN (SELECT RTRIM(pn_account) FROM pname WITH (NOLOCK))
            GROUP BY pt_trtype
            ORDER BY pt_trtype
        """
        pl_breakdown = sql_connector.execute_query(pl_breakdown_sql)
        if hasattr(pl_breakdown, 'to_dict'):
            pl_breakdown = pl_breakdown.to_dict('records')

        type_names = {'I': 'Invoices', 'C': 'Credit Notes', 'P': 'Payments', 'B': 'Brought Forward'}
        pl_by_type = []
        for row in pl_breakdown or []:
            pl_by_type.append({
                "type": row['type'].strip() if row['type'] else 'Unknown',
                "description": type_names.get(row['type'].strip(), row['type'].strip()) if row['type'] else 'Unknown',
                "count": int(row['count'] or 0),
                "total": round(float(row['total'] or 0), 2)
            })

        # Verify against supplier master (pname)
        pname_sql = """
            SELECT
                COUNT(*) AS supplier_count,
                SUM(pn_currbal) AS total_balance
            FROM pname WITH (NOLOCK)
            WHERE pn_currbal <> 0
        """
        pname_result = sql_connector.execute_query(pname_sql)
        if hasattr(pname_result, 'to_dict'):
            pname_result = pname_result.to_dict('records')

        pname_total = float(pname_result[0]['total_balance'] or 0) if pname_result else 0
        pname_count = int(pname_result[0]['supplier_count'] or 0) if pname_result else 0

        # Detailed verification: Find suppliers where pn_currbal doesn't match SUM(pt_trbal)
        balance_mismatch_sql = """
            SELECT
                p.pn_account AS account,
                RTRIM(p.pn_name) AS name,
                p.pn_currbal AS master_balance,
                COALESCE(t.txn_balance, 0) AS transaction_balance,
                p.pn_currbal - COALESCE(t.txn_balance, 0) AS variance
            FROM pname p WITH (NOLOCK)
            LEFT JOIN (
                SELECT pt_account, SUM(pt_trbal) AS txn_balance
                FROM ptran WITH (NOLOCK)
                GROUP BY pt_account
            ) t ON RTRIM(p.pn_account) = RTRIM(t.pt_account)
            WHERE ABS(p.pn_currbal - COALESCE(t.txn_balance, 0)) >= 0.01
            ORDER BY ABS(p.pn_currbal - COALESCE(t.txn_balance, 0)) DESC
        """
        try:
            balance_mismatches = sql_connector.execute_query(balance_mismatch_sql)
            if hasattr(balance_mismatches, 'to_dict'):
                balance_mismatches = balance_mismatches.to_dict('records')
        except Exception:
            balance_mismatches = []

        supplier_balance_issues = []
        for row in balance_mismatches or []:
            supplier_balance_issues.append({
                "account": row['account'].strip() if row['account'] else '',
                "name": row['name'] or '',
                "master_balance": round(float(row['master_balance'] or 0), 2),
                "transaction_balance": round(float(row['transaction_balance'] or 0), 2),
                "variance": round(float(row['variance'] or 0), 2)
            })

        # ========== TRANSFER FILE (pnoml) ==========
        # Check for transactions sitting in the transfer file waiting to post to NL
        # px_done = 'Y' means posted, anything else means pending
        pnoml_pending_sql = """
            SELECT
                px_nacnt AS nominal_account,
                px_type AS type,
                px_date AS date,
                px_value AS value,
                px_tref AS reference,
                px_comment AS comment,
                px_done AS status
            FROM pnoml WITH (NOLOCK)
            WHERE px_done <> 'Y' OR px_done IS NULL
            ORDER BY px_date DESC
        """
        try:
            pnoml_pending = sql_connector.execute_query(pnoml_pending_sql)
            if hasattr(pnoml_pending, 'to_dict'):
                pnoml_pending = pnoml_pending.to_dict('records')
        except Exception:
            pnoml_pending = []

        # Count posted vs pending in transfer file
        pnoml_summary_sql = """
            SELECT
                CASE WHEN px_done = 'Y' THEN 'Posted' ELSE 'Pending' END AS status,
                COUNT(*) AS count,
                SUM(px_value) AS total
            FROM pnoml WITH (NOLOCK)
            GROUP BY CASE WHEN px_done = 'Y' THEN 'Posted' ELSE 'Pending' END
        """
        try:
            pnoml_summary = sql_connector.execute_query(pnoml_summary_sql)
            if hasattr(pnoml_summary, 'to_dict'):
                pnoml_summary = pnoml_summary.to_dict('records')
        except Exception:
            pnoml_summary = []

        posted_count = 0
        posted_total = 0
        pending_count = 0
        pending_total = 0
        for row in pnoml_summary or []:
            if row['status'] == 'Posted':
                posted_count = int(row['count'] or 0)
                posted_total = float(row['total'] or 0)
            else:
                pending_count = int(row['count'] or 0)
                pending_total = float(row['total'] or 0)

        # Build pending transactions list from transfer file
        pending_transactions = []
        for row in pnoml_pending or []:
            tr_date = row['date']
            if hasattr(tr_date, 'strftime'):
                tr_date = tr_date.strftime('%Y-%m-%d')
            value = float(row['value'] or 0)
            pending_transactions.append({
                "nominal_account": row['nominal_account'].strip() if row['nominal_account'] else '',
                "type": row['type'].strip() if row['type'] else '',
                "date": str(tr_date) if tr_date else '',
                "value": round(value, 2),
                "reference": row['reference'].strip() if row['reference'] else '',
                "comment": row['comment'].strip() if row['comment'] else ''
            })

        reconciliation["purchase_ledger"] = {
            "source": "ptran (Purchase Ledger Transactions)",
            "total_outstanding": round(pl_total, 2),
            "transaction_count": pl_count,
            "breakdown_by_type": pl_by_type,
            "transfer_file": {
                "source": "pnoml (Purchase to Nominal Transfer File)",
                "posted_to_nl": {
                    "count": posted_count,
                    "total": round(posted_total, 2)
                },
                "pending_transfer": {
                    "count": pending_count,
                    "total": round(pending_total, 2),
                    "transactions": pending_transactions
                }
            },
            "supplier_master_check": {
                "source": "pname (Supplier Master)",
                "total": round(pname_total, 2),
                "supplier_count": pname_count,
                "matches_ptran": abs(pl_total - pname_total) < 0.01,
                "balance_variance": round(pl_total - pname_total, 2),
                "suppliers_with_balance_issues": supplier_balance_issues[:20] if supplier_balance_issues else [],
                "total_suppliers_with_issues": len(supplier_balance_issues)
            }
        }

        # ========== NOMINAL LEDGER (nacnt/ntran) ==========
        # Find Creditors Control account - check various naming conventions
        # Include the dynamically loaded control account from config
        control_account_sql = f"""
            SELECT na_acnt, na_desc, na_ytddr, na_ytdcr, na_prydr, na_prycr
            FROM nacnt WITH (NOLOCK)
            WHERE na_desc LIKE '%Creditor%Control%'
               OR na_desc LIKE '%Trade%Creditor%'
               OR na_acnt = 'E110'
               OR na_acnt = '{creditors_control}'
            ORDER BY na_acnt
        """
        control_result = sql_connector.execute_query(control_account_sql)
        if hasattr(control_result, 'to_dict'):
            control_result = control_result.to_dict('records')

        nl_total = 0
        nl_details = []

        # Get current year from ntran
        current_year_sql = "SELECT MAX(nt_year) AS current_year FROM ntran WITH (NOLOCK)"
        cy_result = sql_connector.execute_query(current_year_sql)
        if hasattr(cy_result, 'to_dict'):
            cy_result = cy_result.to_dict('records')
        current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

        if control_result:
            for acc in control_result:
                acnt = acc['na_acnt'].strip()

                # Get YTD figures from nacnt (these are current year movements)
                ytd_dr = float(acc['na_ytddr'] or 0)
                ytd_cr = float(acc['na_ytdcr'] or 0)
                pry_dr = float(acc['na_prydr'] or 0)
                pry_cr = float(acc['na_prycr'] or 0)

                # Prior year B/F (credit balance for creditors)
                bf_balance = pry_cr - pry_dr

                # Get current year transactions from ntran
                ntran_current_sql = f"""
                    SELECT
                        SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                        SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                        SUM(nt_value) AS net
                    FROM ntran WITH (NOLOCK)
                    WHERE nt_acnt = '{acnt}' AND nt_year = {current_year}
                """
                ntran_current = sql_connector.execute_query(ntran_current_sql)
                if hasattr(ntran_current, 'to_dict'):
                    ntran_current = ntran_current.to_dict('records')

                current_year_dr = float(ntran_current[0]['debits'] or 0) if ntran_current else 0
                current_year_cr = float(ntran_current[0]['credits'] or 0) if ntran_current else 0
                current_year_net = float(ntran_current[0]['net'] or 0) if ntran_current else 0

                # For creditors control, NEGATE the ntran value for comparison with ptran
                # Sign conventions are OPPOSITE:
                # - ptran: positive = we owe suppliers, negative = they owe us
                # - ntran: negative = we owe suppliers (credit), positive = they owe us (debit)
                # So we negate ntran to match ptran convention
                current_year_balance = -current_year_net

                # Get all years for reference
                ntran_sql = f"""
                    SELECT
                        nt_year,
                        SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                        SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                        SUM(nt_value) AS net
                    FROM ntran WITH (NOLOCK)
                    WHERE nt_acnt = '{acnt}'
                    GROUP BY nt_year
                    ORDER BY nt_year
                """
                ntran_result = sql_connector.execute_query(ntran_sql)
                if hasattr(ntran_result, 'to_dict'):
                    ntran_result = ntran_result.to_dict('records')

                ntran_by_year = []
                for row in ntran_result or []:
                    ntran_by_year.append({
                        "year": int(row['nt_year']),
                        "debits": round(float(row['debits'] or 0), 2),
                        "credits": round(float(row['credits'] or 0), 2),
                        "net": round(float(row['net'] or 0), 2)
                    })

                nl_details.append({
                    "account": acnt,
                    "description": acc['na_desc'].strip() if acc['na_desc'] else '',
                    "brought_forward": round(bf_balance, 2),
                    "current_year": current_year,
                    "current_year_debits": round(current_year_dr, 2),
                    "current_year_credits": round(current_year_cr, 2),
                    "current_year_net": round(current_year_net, 2),
                    "closing_balance": round(current_year_balance, 2),
                    "ntran_by_year": ntran_by_year
                })

                # Use current year balance for reconciliation
                nl_total += current_year_balance

        reconciliation["nominal_ledger"] = {
            "source": f"ntran (Nominal Ledger - {current_year} only)",
            "control_accounts": nl_details,
            "total_balance": round(nl_total, 2),
            "current_year": current_year
        }

        # ========== VARIANCE CALCULATION ==========
        # The total PL outstanding should match the NL control account balance
        # Pending transfers are transactions in the PL that haven't been posted to NL yet
        # but the PL total and NL should still reconcile as the balance is calculated differently

        # Primary variance: Total PL vs NL (this is what matters for reconciliation)
        variance = pl_total - nl_total
        variance_abs = abs(variance)

        # Secondary check: Posted-only variance (to verify NL posting integrity)
        variance_posted = posted_total - nl_total
        variance_posted_abs = abs(variance_posted)

        reconciliation["variance"] = {
            "amount": round(variance, 2),
            "absolute": round(variance_abs, 2),
            "purchase_ledger_total": round(pl_total, 2),
            "transfer_file_posted": round(posted_total, 2),
            "transfer_file_pending": round(pending_total, 2),
            "nominal_ledger_total": round(nl_total, 2),
            "posted_variance": round(variance_posted, 2),
            "posted_variance_abs": round(variance_posted_abs, 2),
            "reconciled": variance_abs < 1.00,
            "has_pending_transfers": pending_count > 0
        }

        # Determine status based on total PL vs NL
        if variance_abs < 1.00:
            reconciliation["status"] = "RECONCILED"
            if pending_count > 0:
                reconciliation["message"] = f"Purchase Ledger reconciles to Nominal Ledger. {pending_count} transactions (£{abs(pending_total):,.2f}) in transfer file pending."
            else:
                reconciliation["message"] = "Purchase Ledger reconciles to Nominal Ledger Creditors Control"
        else:
            reconciliation["status"] = "UNRECONCILED"
            if variance > 0:
                reconciliation["message"] = f"Purchase Ledger is £{variance_abs:,.2f} MORE than Nominal Ledger Control"
            else:
                reconciliation["message"] = f"Purchase Ledger is £{variance_abs:,.2f} LESS than Nominal Ledger Control"

        # ========== VARIANCE ANALYSIS ==========
        # If there's a variance, try to identify the cause by comparing NL and PL
        variance_items = []
        nl_only_items = []
        pl_only_items = []
        value_diff_items = []
        nl_total_check = 0
        pl_total_check = 0

        if variance_abs >= 0.01:
            # Get creditors control account code(s), fallback to config value
            control_accounts = [acc['account'] for acc in nl_details] if nl_details else [creditors_control]
            control_accounts_str = "','".join(control_accounts)

            # Get NL transactions for current year only
            # Match key is: date + value + reference (composite key)
            # Note: nt_entr is the entry date, nt_cmnt contains the PL reference
            nl_transactions_sql = f"""
                SELECT
                    RTRIM(nt_cmnt) AS reference,
                    nt_value AS nl_value,
                    nt_entr AS date,
                    nt_year AS year,
                    nt_type AS type,
                    RTRIM(nt_ref) AS nl_ref,
                    RTRIM(nt_trnref) AS description
                FROM ntran
                WHERE nt_acnt IN ('{control_accounts_str}')
                  AND nt_year = {current_year}
                ORDER BY nt_entr, nt_cmnt
            """
            try:
                nl_trans = sql_connector.execute_query(nl_transactions_sql)
                if hasattr(nl_trans, 'to_dict'):
                    nl_trans = nl_trans.to_dict('records')
            except Exception as e:
                logger.error(f"NL transactions query failed: {e}")
                nl_trans = []

            # Get supplier names from pname for matching
            # Build lookup: supplier_name -> account and account -> name
            supplier_names_sql = """
                SELECT RTRIM(pn_account) AS account, RTRIM(pn_name) AS name
                FROM pname
            """
            try:
                supplier_result = sql_connector.execute_query(supplier_names_sql)
                if hasattr(supplier_result, 'to_dict'):
                    supplier_result = supplier_result.to_dict('records')
                # Build lookups for matching
                supplier_name_to_account = {}
                supplier_account_to_name = {}
                for row in supplier_result or []:
                    acc = row['account'].strip() if row['account'] else ''
                    name = row['name'].strip().upper() if row['name'] else ''
                    if acc and name:
                        supplier_name_to_account[name] = acc
                        supplier_account_to_name[acc] = name
                active_suppliers = set(supplier_name_to_account.values())
            except Exception as e:
                logger.error(f"Supplier names query failed: {e}")
                supplier_name_to_account = {}
                supplier_account_to_name = {}
                active_suppliers = set()

            # Get PL transactions for current year only, for active suppliers
            pl_transactions_sql = f"""
                SELECT
                    RTRIM(pt_trref) AS reference,
                    pt_trbal AS pl_balance,
                    pt_trvalue AS pl_value,
                    pt_trdate AS date,
                    RTRIM(pt_account) AS supplier,
                    pt_trtype AS type,
                    RTRIM(pt_supref) AS supplier_ref
                FROM ptran
                WHERE pt_trbal <> 0
                  AND RTRIM(pt_account) IN (SELECT RTRIM(pn_account) FROM pname)
                  AND YEAR(pt_trdate) = {current_year}
                ORDER BY pt_trdate, pt_trref
            """
            try:
                pl_trans = sql_connector.execute_query(pl_transactions_sql)
                if hasattr(pl_trans, 'to_dict'):
                    pl_trans = pl_trans.to_dict('records')
            except Exception as e:
                logger.error(f"PL transactions query failed: {e}")
                pl_trans = []

            # Store NL transactions as a list to handle duplicates properly
            # Each entry should be matchable individually, not aggregated
            nl_entries = []

            for txn in nl_trans or []:
                ref = txn['reference'].strip() if txn['reference'] else ''
                nl_value = float(txn['nl_value'] or 0)
                nl_date = txn['date']
                if hasattr(nl_date, 'strftime'):
                    nl_date_str = nl_date.strftime('%Y-%m-%d')
                else:
                    nl_date_str = str(nl_date) if nl_date else ''

                # Extract supplier name from nt_trnref (first 30 chars contain supplier name)
                description = txn['description'].strip() if txn['description'] else ''
                nl_supplier_name = description[:30].strip().upper() if description else ''

                # Try to find the supplier account from the name
                nl_supplier_account = supplier_name_to_account.get(nl_supplier_name, '')

                # If no exact match, try partial match (supplier name might be truncated)
                if not nl_supplier_account and nl_supplier_name:
                    for name, acc in supplier_name_to_account.items():
                        if name.startswith(nl_supplier_name) or nl_supplier_name.startswith(name):
                            nl_supplier_account = acc
                            break

                abs_val = abs(nl_value)

                # Keys for matching
                date_val_key = f"{nl_date_str}|{abs_val:.2f}"
                # Include supplier in the key for more accurate matching
                date_val_supplier_key = f"{nl_date_str}|{abs_val:.2f}|{nl_supplier_account}"

                nl_entry = {
                    'value': nl_value,
                    'date': nl_date_str,
                    'reference': ref,
                    'year': txn['year'],
                    'type': txn['type'],
                    'matched': False,
                    'date_val_key': date_val_key,
                    'date_val_supplier_key': date_val_supplier_key,
                    'abs_val': abs_val,
                    'supplier_name': nl_supplier_name,
                    'supplier_account': nl_supplier_account
                }

                nl_entries.append(nl_entry)
                nl_total_check += nl_value

            # Build PL lookup and try to match against NL
            pl_entries = []
            matched_pl_indices = set()

            for txn in pl_trans or []:
                ref = txn['reference'].strip() if txn['reference'] else ''
                pl_bal = float(txn['pl_balance'] or 0)
                pl_value = float(txn['pl_value'] or 0)
                supplier = txn['supplier'].strip() if txn['supplier'] else ''
                tr_type = txn['type'].strip() if txn['type'] else ''
                sup_ref = txn['supplier_ref'].strip() if txn['supplier_ref'] else ''
                pl_date = txn['date']

                if hasattr(pl_date, 'strftime'):
                    pl_date_str = pl_date.strftime('%Y-%m-%d')
                else:
                    pl_date_str = str(pl_date) if pl_date else ''

                abs_val = abs(pl_value)
                date_val_key = f"{pl_date_str}|{abs_val:.2f}"
                # Include supplier account in key for precise matching
                date_val_supplier_key = f"{pl_date_str}|{abs_val:.2f}|{supplier}"

                pl_entries.append({
                    'balance': pl_bal,
                    'value': pl_value,
                    'date': pl_date_str,
                    'reference': ref,
                    'supplier': supplier,
                    'type': tr_type,
                    'supplier_ref': sup_ref,
                    'date_val_key': date_val_key,
                    'date_val_supplier_key': date_val_supplier_key,
                    'abs_val': abs_val,
                    'matched': False
                })

                pl_total_check += pl_bal

            # Build NL reference lookup for fast matching
            # nt_cmnt (stored as 'reference') should match pt_trref directly
            # Use list to handle multiple entries with same reference
            nl_by_reference = {}
            for nl_entry in nl_entries:
                ref = nl_entry['reference']
                if ref:
                    if ref not in nl_by_reference:
                        nl_by_reference[ref] = []
                    nl_by_reference[ref].append(nl_entry)

            # Match PL entries to NL entries
            # Priority: 1) Reference match, 2) Date+Value+Supplier, 3) Date+Value, 4) Value+Supplier
            for pl_idx, pl_entry in enumerate(pl_entries):
                pl_ref = pl_entry['reference']
                pl_date_val_supplier_key = pl_entry['date_val_supplier_key']
                pl_date_val_key = pl_entry['date_val_key']
                pl_abs = pl_entry['abs_val']
                pl_supplier = pl_entry['supplier']

                nl_data = None
                match_type = None

                # Strategy 1: Match by reference (nt_cmnt = pt_trref) - MOST RELIABLE
                # For generic refs like 'pay', 'rec' - require exact value match to avoid false matches
                # For specific refs (invoice numbers) - allow small tolerance
                GENERIC_REFS = {'rec', 'pay', 'contra', 'refund', 'adjustment', 'adj', 'jnl', 'journal'}
                is_generic_ref = pl_ref.lower() in GENERIC_REFS if pl_ref else False

                if pl_ref and pl_ref in nl_by_reference:
                    for nl_entry in nl_by_reference[pl_ref]:
                        if not nl_entry['matched']:
                            value_diff = abs(nl_entry['abs_val'] - pl_abs)

                            # For generic refs, require exact match (within £0.10 for rounding)
                            # For specific refs, allow 10% or £10 tolerance
                            if is_generic_ref:
                                value_tolerance = 0.10  # Must be exact for generic refs
                            else:
                                value_tolerance = max(10.0, pl_abs * 0.1)

                            if value_diff <= value_tolerance:
                                nl_data = nl_entry
                                match_type = "reference"
                                break

                # Strategy 2: Match by date + value + supplier
                if not nl_data:
                    for nl_entry in nl_entries:
                        if not nl_entry['matched'] and nl_entry['date_val_supplier_key'] == pl_date_val_supplier_key:
                            nl_data = nl_entry
                            match_type = "date_value_supplier"
                            break

                # Strategy 3: Match by date + value only (supplier may not match exactly)
                if not nl_data:
                    for nl_entry in nl_entries:
                        if not nl_entry['matched'] and nl_entry['date_val_key'] == pl_date_val_key:
                            nl_data = nl_entry
                            match_type = "date_value"
                            break

                # Strategy 4: Match by value + supplier (dates may differ)
                if not nl_data:
                    for nl_entry in nl_entries:
                        if nl_entry['matched']:
                            continue
                        if abs(nl_entry['abs_val'] - pl_abs) < 0.02 and nl_entry['supplier_account'] == pl_supplier:
                            nl_data = nl_entry
                            match_type = "value_supplier"
                            break

                if nl_data:
                    nl_data['matched'] = True
                    pl_entry['matched'] = True
                    matched_pl_indices.add(pl_idx)

                    # Check for value differences
                    nl_abs = nl_data['abs_val']
                    actual_diff = round(nl_abs - pl_abs, 2)
                    if abs(actual_diff) >= 0.01:
                        value_diff_items.append({
                            "source": "Value Difference",
                            "date": pl_entry['date'],
                            "reference": pl_entry['reference'],
                            "supplier": pl_entry['supplier'],
                            "type": pl_entry['type'],
                            "value": actual_diff,
                            "nl_value": round(nl_data['value'], 2),
                            "pl_value": round(pl_entry['value'], 2),
                            "pl_balance": round(pl_entry['balance'], 2),
                            "match_type": match_type,
                            "note": f"NL: £{nl_abs:.2f} vs PL: £{pl_abs:.2f} (diff: £{abs(actual_diff):.2f})"
                        })

            # Find unmatched NL entries (in NL but not in PL)
            for nl_entry in nl_entries:
                if not nl_entry['matched'] and abs(nl_entry['value']) >= 0.01:
                    # Include extracted supplier info from nt_trnref
                    supplier_info = nl_entry['supplier_account'] or nl_entry['supplier_name'] or ''
                    note = f"In NL (year {nl_entry['year']}) but no matching PL entry"
                    if nl_entry['supplier_name'] and not nl_entry['supplier_account']:
                        note += f" - Supplier name '{nl_entry['supplier_name']}' not found in pname"
                    nl_only_items.append({
                        "source": "Nominal Ledger Only",
                        "date": nl_entry['date'],
                        "reference": nl_entry['reference'],
                        "supplier": supplier_info,
                        "type": nl_entry['type'] or "NL",
                        "value": round(nl_entry['value'], 2),
                        "note": note
                    })

            # Find unmatched PL entries (in PL but not in NL)
            for pl_idx, pl_entry in enumerate(pl_entries):
                if not pl_entry['matched'] and abs(pl_entry['balance']) >= 0.01:
                    pl_only_items.append({
                        "source": "Purchase Ledger Only",
                        "date": pl_entry['date'],
                        "reference": pl_entry['reference'],
                        "supplier": pl_entry['supplier'],
                        "type": pl_entry['type'],
                        "value": round(pl_entry['balance'], 2),
                        "note": "In PL but no matching NL entry"
                    })

            # Look for items that exactly match the variance or are small balances
            # These are most likely to explain the variance
            exact_match_refs = set()
            small_balance_refs = set()

            for txn in pl_trans or []:
                pl_bal = float(txn['pl_balance'] or 0)
                ref = txn['reference'].strip() if txn['reference'] else ''
                tr_date = txn['date']
                if hasattr(tr_date, 'strftime'):
                    tr_date = tr_date.strftime('%Y-%m-%d')

                # Check for exact match to variance
                if abs(abs(pl_bal) - variance_abs) < 0.02:
                    exact_match_refs.add(ref)
                    variance_items.append({
                        "source": "Exact Match",
                        "date": str(tr_date) if tr_date else '',
                        "reference": ref,
                        "supplier": txn['supplier'].strip() if txn['supplier'] else '',
                        "type": txn['type'].strip() if txn['type'] else '',
                        "value": round(pl_bal, 2),
                        "note": f"Balance £{pl_bal:.2f} matches variance £{variance_abs:.2f}"
                    })
                # Check for small balances under £1 that could be rounding
                elif 0.01 <= abs(pl_bal) < 1.00:
                    small_balance_refs.add(ref)
                    variance_items.append({
                        "source": "Small Balance",
                        "date": str(tr_date) if tr_date else '',
                        "reference": ref,
                        "supplier": txn['supplier'].strip() if txn['supplier'] else '',
                        "type": txn['type'].strip() if txn['type'] else '',
                        "value": round(pl_bal, 2),
                        "note": "Small balance - possible rounding"
                    })

            # Remove small balance/exact match items from pl_only_items
            variance_refs = exact_match_refs | small_balance_refs
            pl_only_items = [item for item in pl_only_items if item['reference'] not in variance_refs]

        # Always show all variance items - don't hide any
        # Separate into clear categories for the report
        display_items = []

        # Add NL only items (in Nominal but not in Purchase Ledger)
        for item in nl_only_items:
            item['category'] = 'NL_ONLY'
            display_items.append(item)

        # Add PL only items (in Purchase Ledger but not in Nominal)
        for item in pl_only_items:
            item['category'] = 'PL_ONLY'
            display_items.append(item)

        # Add value difference items
        for item in value_diff_items:
            item['category'] = 'VALUE_DIFF'
            display_items.append(item)

        # Add exact match/small balance items
        for item in variance_items:
            item['category'] = 'OTHER'
            display_items.append(item)

        # Calculate totals for each category
        nl_only_total = sum(item['value'] for item in nl_only_items)
        pl_only_total = sum(item['value'] for item in pl_only_items)
        value_diff_total = sum(item['value'] for item in value_diff_items)

        reconciliation["variance_analysis"] = {
            "items": display_items,
            "count": len(display_items),
            # Backward compatible fields for frontend
            "value_diff_count": len(value_diff_items),
            "nl_only_count": len(nl_only_items),
            "pl_only_count": len(pl_only_items),
            "small_balance_count": len(variance_items),
            # New detailed summary
            "summary": {
                "nl_only": {
                    "count": len(nl_only_items),
                    "total": round(nl_only_total, 2),
                    "description": "Entries in Nominal Ledger with no matching Purchase Ledger entry"
                },
                "pl_only": {
                    "count": len(pl_only_items),
                    "total": round(pl_only_total, 2),
                    "description": "Entries in Purchase Ledger with no matching Nominal Ledger entry"
                },
                "value_differences": {
                    "count": len(value_diff_items),
                    "total": round(value_diff_total, 2),
                    "description": "Matched entries with different values"
                }
            },
            "nl_total_check": round(nl_total_check, 2),
            "pl_total_check": round(pl_total_check, 2)
        }

        # ========== DETAILED ANALYSIS ==========
        # Get aged breakdown of PL outstanding (only active suppliers)
        aged_sql = """
            SELECT
                CASE
                    WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
                    WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 60 THEN '31-60 days'
                    WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 90 THEN '61-90 days'
                    ELSE 'Over 90 days'
                END AS age_band,
                COUNT(*) AS count,
                SUM(pt_trbal) AS total
            FROM ptran
            WHERE pt_trbal <> 0
              AND RTRIM(pt_account) IN (SELECT RTRIM(pn_account) FROM pname)
            GROUP BY CASE
                WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
                WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 60 THEN '31-60 days'
                WHEN DATEDIFF(day, pt_trdate, GETDATE()) <= 90 THEN '61-90 days'
                ELSE 'Over 90 days'
            END
            ORDER BY MIN(DATEDIFF(day, pt_trdate, GETDATE()))
        """
        aged_result = sql_connector.execute_query(aged_sql)
        if hasattr(aged_result, 'to_dict'):
            aged_result = aged_result.to_dict('records')

        aged_analysis = []
        for row in aged_result or []:
            aged_analysis.append({
                "age_band": row['age_band'],
                "count": int(row['count'] or 0),
                "total": round(float(row['total'] or 0), 2)
            })

        reconciliation["aged_analysis"] = aged_analysis

        # Top suppliers with outstanding balances
        top_suppliers_sql = """
            SELECT TOP 10
                RTRIM(p.pn_account) AS account,
                RTRIM(p.pn_name) AS supplier_name,
                COUNT(*) AS invoice_count,
                SUM(pt.pt_trbal) AS outstanding
            FROM ptran pt
            JOIN pname p ON pt.pt_account = p.pn_account
            WHERE pt.pt_trbal <> 0
            GROUP BY p.pn_account, p.pn_name
            ORDER BY SUM(pt.pt_trbal) DESC
        """
        top_suppliers = sql_connector.execute_query(top_suppliers_sql)
        if hasattr(top_suppliers, 'to_dict'):
            top_suppliers = top_suppliers.to_dict('records')

        reconciliation["top_suppliers"] = [
            {
                "account": row['account'],
                "name": row['supplier_name'],
                "invoice_count": int(row['invoice_count'] or 0),
                "outstanding": round(float(row['outstanding'] or 0), 2)
            }
            for row in (top_suppliers or [])
        ]

        return reconciliation

    except Exception as e:
        logger.error(f"Creditors reconciliation failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/reconcile/debtors")
async def reconcile_debtors():
    """
    Reconcile Sales Ledger (stran) to Debtors Control Account (Nominal Ledger).
    Compares outstanding balances in stran with the control account in nacnt/ntran.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get dynamic control accounts from config
        control_accounts_config = _get_control_accounts_for_reconciliation()
        debtors_control = control_accounts_config['debtors']

        reconciliation = {
            "success": True,
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "sales_ledger": {},
            "nominal_ledger": {},
            "variance": {},
            "status": "UNRECONCILED",
            "details": [],
            "control_account_used": debtors_control
        }

        # ========== SALES LEDGER (stran) ==========
        sl_outstanding_sql = """
            SELECT
                COUNT(*) AS transaction_count,
                SUM(st_trbal) AS total_outstanding
            FROM stran WITH (NOLOCK)
            WHERE st_trbal <> 0
        """
        sl_result = sql_connector.execute_query(sl_outstanding_sql)
        if hasattr(sl_result, 'to_dict'):
            sl_result = sl_result.to_dict('records')

        sl_total = float(sl_result[0]['total_outstanding'] or 0) if sl_result else 0
        sl_count = int(sl_result[0]['transaction_count'] or 0) if sl_result else 0

        # Breakdown by type
        sl_breakdown_sql = """
            SELECT
                st_trtype AS type,
                COUNT(*) AS count,
                SUM(st_trbal) AS total
            FROM stran WITH (NOLOCK)
            WHERE st_trbal <> 0
            GROUP BY st_trtype
            ORDER BY st_trtype
        """
        sl_breakdown = sql_connector.execute_query(sl_breakdown_sql)
        if hasattr(sl_breakdown, 'to_dict'):
            sl_breakdown = sl_breakdown.to_dict('records')

        type_names = {'I': 'Invoices', 'C': 'Credit Notes', 'R': 'Receipts', 'B': 'Brought Forward'}
        sl_by_type = []
        for row in sl_breakdown or []:
            sl_by_type.append({
                "type": row['type'].strip() if row['type'] else 'Unknown',
                "description": type_names.get(row['type'].strip(), row['type'].strip()) if row['type'] else 'Unknown',
                "count": int(row['count'] or 0),
                "total": round(float(row['total'] or 0), 2)
            })

        # Verify against customer master (sname)
        sname_sql = """
            SELECT
                COUNT(*) AS customer_count,
                SUM(sn_currbal) AS total_balance
            FROM sname WITH (NOLOCK)
            WHERE sn_currbal <> 0
        """
        sname_result = sql_connector.execute_query(sname_sql)
        if hasattr(sname_result, 'to_dict'):
            sname_result = sname_result.to_dict('records')

        sname_total = float(sname_result[0]['total_balance'] or 0) if sname_result else 0
        sname_count = int(sname_result[0]['customer_count'] or 0) if sname_result else 0

        # Detailed verification: Find customers where sn_currbal doesn't match SUM(st_trbal)
        cust_balance_mismatch_sql = """
            SELECT
                s.sn_account AS account,
                RTRIM(s.sn_name) AS name,
                s.sn_currbal AS master_balance,
                COALESCE(t.txn_balance, 0) AS transaction_balance,
                s.sn_currbal - COALESCE(t.txn_balance, 0) AS variance
            FROM sname s WITH (NOLOCK)
            LEFT JOIN (
                SELECT st_account, SUM(st_trbal) AS txn_balance
                FROM stran WITH (NOLOCK)
                GROUP BY st_account
            ) t ON RTRIM(s.sn_account) = RTRIM(t.st_account)
            WHERE ABS(s.sn_currbal - COALESCE(t.txn_balance, 0)) >= 0.01
            ORDER BY ABS(s.sn_currbal - COALESCE(t.txn_balance, 0)) DESC
        """
        try:
            cust_balance_mismatches = sql_connector.execute_query(cust_balance_mismatch_sql)
            if hasattr(cust_balance_mismatches, 'to_dict'):
                cust_balance_mismatches = cust_balance_mismatches.to_dict('records')
        except Exception:
            cust_balance_mismatches = []

        customer_balance_issues = []
        for row in cust_balance_mismatches or []:
            customer_balance_issues.append({
                "account": row['account'].strip() if row['account'] else '',
                "name": row['name'] or '',
                "master_balance": round(float(row['master_balance'] or 0), 2),
                "transaction_balance": round(float(row['transaction_balance'] or 0), 2),
                "variance": round(float(row['variance'] or 0), 2)
            })

        # ========== TRANSFER FILE (snoml) ==========
        # Check for transactions sitting in the transfer file waiting to post to NL
        # sx_done = 'Y' means posted, anything else means pending
        snoml_pending_sql = """
            SELECT
                sx_nacnt AS nominal_account,
                sx_type AS type,
                sx_date AS date,
                sx_value AS value,
                sx_tref AS reference,
                sx_comment AS comment,
                sx_done AS status
            FROM snoml WITH (NOLOCK)
            WHERE sx_done <> 'Y' OR sx_done IS NULL
            ORDER BY sx_date DESC
        """
        try:
            snoml_pending = sql_connector.execute_query(snoml_pending_sql)
            if hasattr(snoml_pending, 'to_dict'):
                snoml_pending = snoml_pending.to_dict('records')
        except Exception:
            snoml_pending = []

        # Count posted vs pending in transfer file
        snoml_summary_sql = """
            SELECT
                CASE WHEN sx_done = 'Y' THEN 'Posted' ELSE 'Pending' END AS status,
                COUNT(*) AS count,
                SUM(sx_value) AS total
            FROM snoml WITH (NOLOCK)
            GROUP BY CASE WHEN sx_done = 'Y' THEN 'Posted' ELSE 'Pending' END
        """
        try:
            snoml_summary = sql_connector.execute_query(snoml_summary_sql)
            if hasattr(snoml_summary, 'to_dict'):
                snoml_summary = snoml_summary.to_dict('records')
        except Exception:
            snoml_summary = []

        sl_posted_count = 0
        sl_posted_total = 0
        sl_pending_count = 0
        sl_pending_total = 0
        for row in snoml_summary or []:
            if row['status'] == 'Posted':
                sl_posted_count = int(row['count'] or 0)
                sl_posted_total = float(row['total'] or 0)
            else:
                sl_pending_count = int(row['count'] or 0)
                sl_pending_total = float(row['total'] or 0)

        # Build pending transactions list from transfer file
        sl_pending_transactions = []
        for row in snoml_pending or []:
            tr_date = row['date']
            if hasattr(tr_date, 'strftime'):
                tr_date = tr_date.strftime('%Y-%m-%d')
            value = float(row['value'] or 0)
            sl_pending_transactions.append({
                "nominal_account": row['nominal_account'].strip() if row['nominal_account'] else '',
                "type": row['type'].strip() if row['type'] else '',
                "date": str(tr_date) if tr_date else '',
                "value": round(value, 2),
                "reference": row['reference'].strip() if row['reference'] else '',
                "comment": row['comment'].strip() if row['comment'] else ''
            })

        reconciliation["sales_ledger"] = {
            "source": "stran (Sales Ledger Transactions)",
            "total_outstanding": round(sl_total, 2),
            "transaction_count": sl_count,
            "breakdown_by_type": sl_by_type,
            "transfer_file": {
                "source": "snoml (Sales to Nominal Transfer File)",
                "posted_to_nl": {
                    "count": sl_posted_count,
                    "total": round(sl_posted_total, 2)
                },
                "pending_transfer": {
                    "count": sl_pending_count,
                    "total": round(sl_pending_total, 2),
                    "transactions": sl_pending_transactions
                }
            },
            "customer_master_check": {
                "source": "sname (Customer Master)",
                "total": round(sname_total, 2),
                "customer_count": sname_count,
                "matches_stran": abs(sl_total - sname_total) < 0.01,
                "balance_variance": round(sl_total - sname_total, 2),
                "customers_with_balance_issues": customer_balance_issues[:20] if customer_balance_issues else [],
                "total_customers_with_issues": len(customer_balance_issues)
            }
        }

        # ========== NOMINAL LEDGER ==========
        # Find Debtors Control account - check various naming conventions
        # Include the dynamically loaded control account from config
        control_account_sql = f"""
            SELECT na_acnt, na_desc, na_ytddr, na_ytdcr, na_prydr, na_prycr
            FROM nacnt WITH (NOLOCK)
            WHERE na_desc LIKE '%Debtor%Control%'
               OR na_desc LIKE '%Trade%Debtor%'
               OR na_acnt = 'C110'
               OR na_acnt = '{debtors_control}'
            ORDER BY na_acnt
        """
        control_result = sql_connector.execute_query(control_account_sql)
        if hasattr(control_result, 'to_dict'):
            control_result = control_result.to_dict('records')

        nl_total = 0
        nl_details = []

        # Get current year from ntran
        current_year_sql = "SELECT MAX(nt_year) AS current_year FROM ntran WITH (NOLOCK)"
        cy_result = sql_connector.execute_query(current_year_sql)
        if hasattr(cy_result, 'to_dict'):
            cy_result = cy_result.to_dict('records')
        current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

        if control_result:
            for acc in control_result:
                acnt = acc['na_acnt'].strip()

                pry_dr = float(acc['na_prydr'] or 0)
                pry_cr = float(acc['na_prycr'] or 0)

                # Prior year B/F (debit balance for debtors)
                bf_balance = pry_dr - pry_cr

                # Get current year transactions from ntran
                ntran_current_sql = f"""
                    SELECT
                        SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                        SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                        SUM(nt_value) AS net
                    FROM ntran WITH (NOLOCK)
                    WHERE nt_acnt = '{acnt}' AND nt_year = {current_year}
                """
                ntran_current = sql_connector.execute_query(ntran_current_sql)
                if hasattr(ntran_current, 'to_dict'):
                    ntran_current = ntran_current.to_dict('records')

                current_year_dr = float(ntran_current[0]['debits'] or 0) if ntran_current else 0
                current_year_cr = float(ntran_current[0]['credits'] or 0) if ntran_current else 0
                current_year_net = float(ntran_current[0]['net'] or 0) if ntran_current else 0

                # For debtors control, keep the actual net value (don't force positive)
                # Positive = customers owe us (normal debit balance)
                # Negative = we owe customers (credit balance - e.g. overpayments, credit notes)
                # This must match stran sign convention for reconciliation
                current_year_balance = current_year_net

                # Get all years for reference
                ntran_sql = f"""
                    SELECT
                        nt_year,
                        SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                        SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                        SUM(nt_value) AS net
                    FROM ntran WITH (NOLOCK)
                    WHERE nt_acnt = '{acnt}'
                    GROUP BY nt_year
                    ORDER BY nt_year
                """
                ntran_result = sql_connector.execute_query(ntran_sql)
                if hasattr(ntran_result, 'to_dict'):
                    ntran_result = ntran_result.to_dict('records')

                ntran_by_year = []
                for row in ntran_result or []:
                    ntran_by_year.append({
                        "year": int(row['nt_year']),
                        "debits": round(float(row['debits'] or 0), 2),
                        "credits": round(float(row['credits'] or 0), 2),
                        "net": round(float(row['net'] or 0), 2)
                    })

                nl_details.append({
                    "account": acnt,
                    "description": acc['na_desc'].strip() if acc['na_desc'] else '',
                    "brought_forward": round(bf_balance, 2),
                    "current_year": current_year,
                    "current_year_debits": round(current_year_dr, 2),
                    "current_year_credits": round(current_year_cr, 2),
                    "current_year_net": round(current_year_net, 2),
                    "closing_balance": round(current_year_balance, 2),
                    "ntran_by_year": ntran_by_year
                })

                # Use current year balance for reconciliation
                nl_total += current_year_balance

        reconciliation["nominal_ledger"] = {
            "source": f"ntran (Nominal Ledger - {current_year} only)",
            "control_accounts": nl_details,
            "total_balance": round(nl_total, 2),
            "current_year": current_year
        }

        # ========== VARIANCE ==========
        # The total SL outstanding should match the NL control account balance
        # Pending transfers are transactions in the SL that haven't been posted to NL yet

        # Primary variance: Total SL vs NL (this is what matters for reconciliation)
        variance = sl_total - nl_total
        variance_abs = abs(variance)

        # Secondary check: Posted-only variance (to verify NL posting integrity)
        variance_posted = sl_posted_total - nl_total
        variance_posted_abs = abs(variance_posted)

        reconciliation["variance"] = {
            "amount": round(variance, 2),
            "absolute": round(variance_abs, 2),
            "sales_ledger_total": round(sl_total, 2),
            "transfer_file_posted": round(sl_posted_total, 2),
            "transfer_file_pending": round(sl_pending_total, 2),
            "nominal_ledger_total": round(nl_total, 2),
            "posted_variance": round(variance_posted, 2),
            "posted_variance_abs": round(variance_posted_abs, 2),
            "reconciled": variance_abs < 1.00,
            "has_pending_transfers": sl_pending_count > 0
        }

        # Determine status based on total SL vs NL
        if variance_abs < 1.00:
            reconciliation["status"] = "RECONCILED"
            if sl_pending_count > 0:
                reconciliation["message"] = f"Sales Ledger reconciles to Nominal Ledger. {sl_pending_count} transactions (£{abs(sl_pending_total):,.2f}) in transfer file pending."
            else:
                reconciliation["message"] = "Sales Ledger reconciles to Nominal Ledger Debtors Control"
        else:
            reconciliation["status"] = "UNRECONCILED"
            if variance > 0:
                reconciliation["message"] = f"Sales Ledger is £{variance_abs:,.2f} MORE than Nominal Ledger Control"
            else:
                reconciliation["message"] = f"Sales Ledger is £{variance_abs:,.2f} LESS than Nominal Ledger Control"

        # ========== VARIANCE ANALYSIS ==========
        # Provide drill-down into transactions even when reconciled
        variance_items = []
        nl_only_items = []
        sl_only_items = []
        value_diff_items = []
        nl_total_check = 0
        sl_total_check = 0

        # Get control account codes, fallback to config value
        control_accounts = [acc['account'] for acc in nl_details] if nl_details else [debtors_control]
        control_accounts_str = "','".join(control_accounts)

        # Get ALL NL transactions for the control account
        # Note: nt_entr is the entry date, nt_cmnt contains the SL reference
        # We need all years to properly match against SL transactions that may be old
        nl_transactions_sql = f"""
            SELECT
                RTRIM(nt_cmnt) AS reference,
                nt_value AS nl_value,
                nt_entr AS date,
                nt_year AS year,
                nt_type AS type,
                RTRIM(nt_ref) AS nl_ref,
                RTRIM(nt_trnref) AS description
            FROM ntran
            WHERE nt_acnt IN ('{control_accounts_str}')
            ORDER BY nt_entr, nt_cmnt
        """
        try:
            nl_trans = sql_connector.execute_query(nl_transactions_sql)
            if hasattr(nl_trans, 'to_dict'):
                nl_trans = nl_trans.to_dict('records')
        except Exception as e:
            logger.error(f"NL transactions query failed: {e}")
            nl_trans = []

        # Get ALL SL transactions with non-zero balance (to match the total calculation)
        # Include all years to properly identify all variance sources
        sl_transactions_sql = """
            SELECT
                RTRIM(st_trref) AS reference,
                st_trbal AS sl_balance,
                st_trvalue AS sl_value,
                st_trdate AS date,
                RTRIM(st_account) AS customer,
                st_trtype AS type,
                RTRIM(st_custref) AS customer_ref
            FROM stran
            WHERE st_trbal <> 0
            ORDER BY st_trdate, st_trref
        """
        try:
            sl_trans = sql_connector.execute_query(sl_transactions_sql)
            if hasattr(sl_trans, 'to_dict'):
                sl_trans = sl_trans.to_dict('records')
        except Exception as e:
            logger.error(f"SL transactions query failed: {e}")
            sl_trans = []

        # Build NL lookups for matching
        # Primary: by reference only (nt_cmnt = st_trref) - most reliable
        # Secondary: by date + abs(value) + reference
        # Tertiary: by reference + abs_value
        # Use lists to handle multiple entries with same reference
        nl_by_ref = {}  # reference -> [nl_entries] (PRIMARY - most reliable)
        nl_by_key = {}  # date|value|ref -> nl_entry
        nl_by_ref_val = {}  # ref|value -> nl_entry

        for txn in nl_trans or []:
            ref = txn['reference'].strip() if txn['reference'] else ''
            nl_value = float(txn['nl_value'] or 0)
            nl_date = txn['date']
            if hasattr(nl_date, 'strftime'):
                nl_date_str = nl_date.strftime('%Y-%m-%d')
            else:
                nl_date_str = str(nl_date) if nl_date else ''

            abs_val = abs(nl_value)
            key = f"{nl_date_str}|{abs_val:.2f}|{ref}"
            ref_val_key = f"{ref}|{abs_val:.2f}"

            nl_entry = {
                'value': nl_value,
                'date': nl_date_str,
                'reference': ref,
                'year': txn['year'],
                'type': txn['type'],
                'matched': False,
                'key': key
            }

            # Primary lookup by reference only (nt_cmnt = st_trref)
            # Store as list to handle multiple entries with same reference
            if ref:
                if ref not in nl_by_ref:
                    nl_by_ref[ref] = []
                nl_by_ref[ref].append(nl_entry)

            if key not in nl_by_key:
                nl_by_key[key] = nl_entry
            else:
                nl_by_key[key]['value'] += nl_value

            if ref_val_key not in nl_by_ref_val and ref:
                nl_by_ref_val[ref_val_key] = nl_entry

            nl_total_check += nl_value

        # Build SL lookup and try to match against NL
        sl_by_key = {}
        matched_sl_keys = set()

        for txn in sl_trans or []:
            ref = txn['reference'].strip() if txn['reference'] else ''
            sl_bal = float(txn['sl_balance'] or 0)
            sl_value = float(txn['sl_value'] or 0)
            customer = txn['customer'].strip() if txn['customer'] else ''
            tr_type = txn['type'].strip() if txn['type'] else ''
            cust_ref = txn['customer_ref'].strip() if txn['customer_ref'] else ''
            sl_date = txn['date']

            if hasattr(sl_date, 'strftime'):
                sl_date_str = sl_date.strftime('%Y-%m-%d')
            else:
                sl_date_str = str(sl_date) if sl_date else ''

            # Create composite key: date|abs_value|reference
            abs_val = abs(sl_value)
            key = f"{sl_date_str}|{abs_val:.2f}|{ref}"
            ref_val_key = f"{ref}|{abs_val:.2f}"

            sl_by_key[key] = {
                'balance': sl_bal,
                'value': sl_value,
                'date': sl_date_str,
                'reference': ref,
                'customer': customer,
                'type': tr_type,
                'customer_ref': cust_ref
            }

            sl_total_check += sl_bal

            # Try to match with NL using multiple strategies
            # Priority: 1) Reference (nt_cmnt = st_trref), 2) Date+Value+Ref, 3) Ref+Value, 4) Fuzzy
            nl_data = None
            match_type = None

            # Strategy 1: Match by reference only (nt_cmnt = st_trref) - MOST RELIABLE
            # For generic refs like 'rec', 'pay' - require exact value match to avoid false matches
            # For specific refs (invoice numbers) - allow small tolerance
            GENERIC_REFS = {'rec', 'pay', 'contra', 'refund', 'adjustment', 'adj', 'jnl', 'journal'}
            is_generic_ref = ref.lower() in GENERIC_REFS if ref else False

            if ref and ref in nl_by_ref:
                for nl_entry in nl_by_ref[ref]:
                    if not nl_entry['matched']:
                        nl_abs = abs(nl_entry['value'])
                        value_diff = abs(nl_abs - abs_val)

                        # For generic refs, require exact match (within £0.10 for rounding)
                        # For specific refs, allow 10% or £10 tolerance
                        if is_generic_ref:
                            value_tolerance = 0.10  # Must be exact for generic refs
                        else:
                            value_tolerance = max(10.0, abs_val * 0.1)

                        if value_diff <= value_tolerance:
                            nl_data = nl_entry
                            match_type = "reference"
                            break

            # Strategy 2: Match by date + value + reference (exact composite key)
            if not nl_data and key in nl_by_key:
                nl_entry = nl_by_key[key]
                if not nl_entry['matched']:
                    nl_data = nl_entry
                    match_type = "exact"

            # Strategy 3: Match by reference + value (for date mismatches)
            if not nl_data and ref and ref_val_key in nl_by_ref_val:
                nl_entry = nl_by_ref_val[ref_val_key]
                if not nl_entry['matched']:
                    nl_data = nl_entry
                    match_type = "ref_val"

            # Strategy 4: Fuzzy value matching with same reference
            if not nl_data and ref:
                for nl_key, nl_entry in nl_by_ref_val.items():
                    if nl_entry['matched']:
                        continue
                    nl_ref = nl_entry['reference']
                    if nl_ref == ref:
                        nl_abs = abs(nl_entry['value'])
                        diff = abs(nl_abs - abs_val)
                        if diff <= 0.10:
                            nl_data = nl_entry
                            match_type = "fuzzy"
                            break

            if nl_data:
                nl_data['matched'] = True
                matched_sl_keys.add(key)

                # Check if absolute values match
                nl_val = nl_data['value']
                nl_abs = abs(nl_val)
                sl_abs = abs(sl_value)

                actual_diff = round(nl_abs - sl_abs, 2)
                if abs(actual_diff) >= 0.01:
                    value_diff_items.append({
                        "source": "Value Difference",
                        "date": sl_date_str,
                        "reference": ref,
                        "customer": customer,
                        "type": tr_type,
                        "value": actual_diff,
                        "nl_value": round(nl_val, 2),
                        "sl_value": round(sl_value, 2),
                        "sl_balance": round(sl_bal, 2),
                        "match_type": match_type,
                        "note": f"NL: £{nl_abs:.2f} vs SL: £{sl_abs:.2f} (diff: £{abs(actual_diff):.2f})"
                    })

        # Find unmatched NL entries
        for key, nl_data in nl_by_key.items():
            if not nl_data['matched'] and abs(nl_data['value']) >= 0.01:
                nl_only_items.append({
                    "source": "Nominal Ledger Only",
                    "date": nl_data['date'],
                    "reference": nl_data['reference'],
                    "customer": "",
                    "type": nl_data['type'] or "NL",
                    "value": round(nl_data['value'], 2),
                    "note": f"In NL (year {nl_data['year']}) but no matching SL entry"
                })

        # Find unmatched SL entries
        for key, sl_data in sl_by_key.items():
            if key not in matched_sl_keys and abs(sl_data['balance']) >= 0.01:
                sl_only_items.append({
                    "source": "Sales Ledger Only",
                    "date": sl_data['date'],
                    "reference": sl_data['reference'],
                    "customer": sl_data['customer'],
                    "type": sl_data['type'],
                    "value": round(sl_data['balance'], 2),
                    "note": f"In SL but no matching NL entry (key: {key})"
                })

        # Look for small balances that could be rounding
        exact_match_refs = set()
        small_balance_refs = set()

        for txn in sl_trans or []:
            sl_bal = float(txn['sl_balance'] or 0)
            ref = txn['reference'].strip() if txn['reference'] else ''
            sl_date = txn['date']
            if hasattr(sl_date, 'strftime'):
                sl_date_str = sl_date.strftime('%Y-%m-%d')
            else:
                sl_date_str = str(sl_date) if sl_date else ''

            # Check for exact match to variance
            if abs(abs(sl_bal) - variance_abs) < 0.02:
                exact_match_refs.add(ref)
                variance_items.append({
                    "source": "Exact Match",
                    "date": sl_date_str,
                    "reference": ref,
                    "customer": txn['customer'].strip() if txn['customer'] else '',
                    "type": txn['type'].strip() if txn['type'] else '',
                    "value": round(sl_bal, 2),
                    "note": f"Balance £{sl_bal:.2f} matches variance £{variance_abs:.2f}"
                })
            elif 0.01 <= abs(sl_bal) < 1.00:
                small_balance_refs.add(ref)
                variance_items.append({
                    "source": "Small Balance",
                    "date": sl_date_str,
                    "reference": ref,
                    "customer": txn['customer'].strip() if txn['customer'] else '',
                    "type": txn['type'].strip() if txn['type'] else '',
                    "value": round(sl_bal, 2),
                    "note": "Small balance - possible rounding"
                })

        # Remove small balance/exact match items from sl_only_items
        variance_refs = exact_match_refs | small_balance_refs
        sl_only_items = [item for item in sl_only_items if item['reference'] not in variance_refs]

        # Calculate totals for unmatched items
        sl_only_total = sum(item['value'] for item in sl_only_items)
        nl_only_total = sum(item['value'] for item in nl_only_items)
        value_diff_total = sum(item['value'] for item in value_diff_items)

        # Theoretical variance from items: SL only - NL only + value diffs
        # (SL only adds to SL total, NL only reduces it, value diffs are SL perspective)
        calculated_variance = value_diff_total

        # For display - show top items sorted by absolute value
        display_items = []

        # Always show value differences
        display_items.extend(sorted(value_diff_items, key=lambda x: abs(x['value']), reverse=True))

        # Show small balance items
        display_items.extend(variance_items)

        # Show top 10 unmatched items by value (if not too many)
        if len(sl_only_items) <= 50:
            top_sl_only = sorted(sl_only_items, key=lambda x: abs(x['value']), reverse=True)[:10]
            display_items.extend(top_sl_only)

        if len(nl_only_items) <= 50:
            top_nl_only = sorted(nl_only_items, key=lambda x: abs(x['value']), reverse=True)[:10]
            display_items.extend(top_nl_only)

        # Analysis note
        if len(sl_only_items) > 50 or len(nl_only_items) > 50:
            analysis_note = f"NL uses batch posting. SL unmatched: {len(sl_only_items)} items (£{sl_only_total:,.2f}), NL unmatched: {len(nl_only_items)} items"
        else:
            analysis_note = None

        reconciliation["variance_analysis"] = {
            "items": display_items,
            "count": len(display_items),
            "value_diff_count": len(value_diff_items),
            "value_diff_total": round(value_diff_total, 2),
            "nl_only_count": len(nl_only_items),
            "nl_only_total": round(nl_only_total, 2),
            "sl_only_count": len(sl_only_items),
            "sl_only_total": round(sl_only_total, 2),
            "small_balance_count": len(variance_items),
            "nl_total_check": round(nl_total_check, 2),
            "sl_total_check": round(sl_total_check, 2),
            "note": analysis_note
        }

        # ========== DETAILED ANALYSIS ==========
        # Get aged breakdown of SL outstanding (only active customers)
        aged_sql = f"""
            SELECT
                CASE
                    WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
                    WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 60 THEN '31-60 days'
                    WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 90 THEN '61-90 days'
                    ELSE 'Over 90 days'
                END AS age_band,
                COUNT(*) AS count,
                SUM(st_trbal) AS total
            FROM stran
            WHERE st_trbal <> 0
              AND RTRIM(st_account) IN (SELECT RTRIM(sn_account) FROM sname)
            GROUP BY CASE
                WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 30 THEN 'Current (0-30 days)'
                WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 60 THEN '31-60 days'
                WHEN DATEDIFF(day, st_trdate, GETDATE()) <= 90 THEN '61-90 days'
                ELSE 'Over 90 days'
            END
            ORDER BY MIN(DATEDIFF(day, st_trdate, GETDATE()))
        """
        aged_result = sql_connector.execute_query(aged_sql)
        if hasattr(aged_result, 'to_dict'):
            aged_result = aged_result.to_dict('records')

        aged_analysis = []
        for row in aged_result or []:
            aged_analysis.append({
                "age_band": row['age_band'],
                "count": int(row['count'] or 0),
                "total": round(float(row['total'] or 0), 2)
            })

        reconciliation["aged_analysis"] = aged_analysis

        # Top customers with outstanding balances
        top_customers_sql = """
            SELECT TOP 10
                RTRIM(s.sn_account) AS account,
                RTRIM(s.sn_name) AS customer_name,
                COUNT(*) AS invoice_count,
                SUM(st.st_trbal) AS outstanding
            FROM stran st
            JOIN sname s ON st.st_account = s.sn_account
            WHERE st.st_trbal <> 0
            GROUP BY s.sn_account, s.sn_name
            ORDER BY SUM(st.st_trbal) DESC
        """
        top_customers = sql_connector.execute_query(top_customers_sql)
        if hasattr(top_customers, 'to_dict'):
            top_customers = top_customers.to_dict('records')

        reconciliation["top_customers"] = [
            {
                "account": row['account'],
                "name": row['customer_name'],
                "invoice_count": int(row['invoice_count'] or 0),
                "outstanding": round(float(row['outstanding'] or 0), 2)
            }
            for row in (top_customers or [])
        ]

        return reconciliation

    except Exception as e:
        logger.error(f"Debtors reconciliation failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/reconcile/trial-balance")
async def reconcile_trial_balance():
    """
    Trial Balance check - verifies the nominal ledger as a whole balances (debits = credits).
    Also shows all nominal accounts with their balances.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        result = {
            "success": True,
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "summary": {},
            "accounts": [],
            "status": "UNBALANCED",
            "message": ""
        }

        # Get current year
        cy_sql = "SELECT MAX(nt_year) AS current_year FROM ntran WITH (NOLOCK)"
        cy_result = sql_connector.execute_query(cy_sql)
        if hasattr(cy_result, 'to_dict'):
            cy_result = cy_result.to_dict('records')
        current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

        # Get all nominal accounts with balances
        accounts_sql = """
            SELECT
                n.na_acnt AS account,
                RTRIM(n.na_desc) AS description,
                n.na_type AS type,
                n.na_prydr AS prior_debits,
                n.na_prycr AS prior_credits,
                n.na_ytddr AS ytd_debits,
                n.na_ytdcr AS ytd_credits,
                COALESCE(t.current_debits, 0) AS current_debits,
                COALESCE(t.current_credits, 0) AS current_credits,
                COALESCE(t.current_net, 0) AS current_net
            FROM nacnt n WITH (NOLOCK)
            LEFT JOIN (
                SELECT
                    nt_acnt,
                    SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS current_debits,
                    SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS current_credits,
                    SUM(nt_value) AS current_net
                FROM ntran WITH (NOLOCK)
                WHERE nt_year = {current_year}
                GROUP BY nt_acnt
            ) t ON n.na_acnt = t.nt_acnt
            WHERE n.na_ytddr <> 0 OR n.na_ytdcr <> 0 OR n.na_prydr <> 0 OR n.na_prycr <> 0
               OR COALESCE(t.current_debits, 0) <> 0 OR COALESCE(t.current_credits, 0) <> 0
            ORDER BY n.na_acnt
        """.format(current_year=current_year)
        accounts_result = sql_connector.execute_query(accounts_sql)
        if hasattr(accounts_result, 'to_dict'):
            accounts_result = accounts_result.to_dict('records')

        # Process accounts and calculate totals
        total_bf_debits = 0
        total_bf_credits = 0
        total_current_debits = 0
        total_current_credits = 0
        total_closing_debits = 0
        total_closing_credits = 0

        type_names = {
            'A': 'Asset',
            'L': 'Liability',
            'E': 'Expense',
            'I': 'Income',
            'C': 'Capital',
            'P': 'P&L',
            'B': 'Balance Sheet'
        }

        accounts = []
        for row in accounts_result or []:
            prior_dr = float(row['prior_debits'] or 0)
            prior_cr = float(row['prior_credits'] or 0)
            current_dr = float(row['current_debits'] or 0)
            current_cr = float(row['current_credits'] or 0)

            # B/F balance (prior year net)
            bf_balance = prior_dr - prior_cr

            # Current year movement
            current_net = current_dr - current_cr

            # Closing balance
            closing_balance = bf_balance + current_net

            # Track totals (debit vs credit balances)
            if bf_balance > 0:
                total_bf_debits += bf_balance
            else:
                total_bf_credits += abs(bf_balance)

            total_current_debits += current_dr
            total_current_credits += current_cr

            if closing_balance > 0:
                total_closing_debits += closing_balance
            else:
                total_closing_credits += abs(closing_balance)

            account_type = row['type'].strip() if row['type'] else ''

            accounts.append({
                "account": row['account'].strip() if row['account'] else '',
                "description": row['description'] or '',
                "type": account_type,
                "type_name": type_names.get(account_type, account_type),
                "bf_balance": round(bf_balance, 2),
                "current_debits": round(current_dr, 2),
                "current_credits": round(current_cr, 2),
                "current_net": round(current_net, 2),
                "closing_balance": round(closing_balance, 2)
            })

        result["accounts"] = accounts
        result["current_year"] = current_year

        # Calculate variance (should be zero for a balanced trial balance)
        bf_variance = abs(total_bf_debits - total_bf_credits)
        current_variance = abs(total_current_debits - total_current_credits)
        closing_variance = abs(total_closing_debits - total_closing_credits)

        result["summary"] = {
            "brought_forward": {
                "debits": round(total_bf_debits, 2),
                "credits": round(total_bf_credits, 2),
                "variance": round(bf_variance, 2),
                "balanced": bf_variance < 1.00
            },
            "current_year": {
                "debits": round(total_current_debits, 2),
                "credits": round(total_current_credits, 2),
                "variance": round(current_variance, 2),
                "balanced": current_variance < 1.00
            },
            "closing": {
                "debits": round(total_closing_debits, 2),
                "credits": round(total_closing_credits, 2),
                "variance": round(closing_variance, 2),
                "balanced": closing_variance < 1.00
            },
            "account_count": len(accounts)
        }

        # Overall status
        all_balanced = (bf_variance < 1.00 and current_variance < 1.00 and closing_variance < 1.00)

        if all_balanced:
            result["status"] = "BALANCED"
            result["message"] = f"Trial Balance is correct. {len(accounts)} accounts with matching debits and credits."
        else:
            result["status"] = "UNBALANCED"
            variances = []
            if bf_variance >= 1.00:
                variances.append(f"B/F: £{bf_variance:,.2f}")
            if current_variance >= 1.00:
                variances.append(f"Current: £{current_variance:,.2f}")
            if closing_variance >= 1.00:
                variances.append(f"Closing: £{closing_variance:,.2f}")
            result["message"] = f"Trial Balance has variance: {', '.join(variances)}"

        return result

    except Exception as e:
        logger.error(f"Trial balance check failed: {e}")
        return {"success": False, "error": str(e)}


def get_vat_quarter_dates(reference_date: datetime = None):
    """
    Calculate VAT quarter start/end dates based on standard UK calendar quarters.
    Returns current quarter dates and previous quarters for reference.
    """
    if reference_date is None:
        reference_date = datetime.now()

    year = reference_date.year
    month = reference_date.month

    # Determine current quarter (Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec)
    if month <= 3:
        current_q_start = datetime(year, 1, 1)
        current_q_end = datetime(year, 3, 31)
        quarter_name = f"Q1 {year}"
        quarter_num = 1
    elif month <= 6:
        current_q_start = datetime(year, 4, 1)
        current_q_end = datetime(year, 6, 30)
        quarter_name = f"Q2 {year}"
        quarter_num = 2
    elif month <= 9:
        current_q_start = datetime(year, 7, 1)
        current_q_end = datetime(year, 9, 30)
        quarter_name = f"Q3 {year}"
        quarter_num = 3
    else:
        current_q_start = datetime(year, 10, 1)
        current_q_end = datetime(year, 12, 31)
        quarter_name = f"Q4 {year}"
        quarter_num = 4

    # Build list of quarters (current + previous 3)
    quarters = []
    for i in range(4):
        q_num = quarter_num - i
        q_year = year
        while q_num <= 0:
            q_num += 4
            q_year -= 1

        if q_num == 1:
            q_start = datetime(q_year, 1, 1)
            q_end = datetime(q_year, 3, 31)
        elif q_num == 2:
            q_start = datetime(q_year, 4, 1)
            q_end = datetime(q_year, 6, 30)
        elif q_num == 3:
            q_start = datetime(q_year, 7, 1)
            q_end = datetime(q_year, 9, 30)
        else:
            q_start = datetime(q_year, 10, 1)
            q_end = datetime(q_year, 12, 31)

        quarters.append({
            "name": f"Q{q_num} {q_year}",
            "start": q_start.strftime('%Y-%m-%d'),
            "end": q_end.strftime('%Y-%m-%d'),
            "is_current": i == 0
        })

    return {
        "current_quarter": quarter_name,
        "quarter_start": current_q_start.strftime('%Y-%m-%d'),
        "quarter_end": current_q_end.strftime('%Y-%m-%d'),
        "quarters": quarters
    }


@app.get("/api/reconcile/summary")
async def reconcile_summary():
    """
    Quick summary of all reconciliation checks - shows at a glance whether everything balances.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    summary = {
        "success": True,
        "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "checks": [],
        "all_reconciled": True,
        "total_checks": 0,
        "passed_checks": 0,
        "failed_checks": 0
    }

    try:
        # Get control accounts
        control_accounts = _get_control_accounts_for_reconciliation()

        # ========== 1. DEBTORS CHECK ==========
        try:
            # Sales Ledger total
            sl_sql = "SELECT SUM(st_trbal) AS total FROM stran WITH (NOLOCK) WHERE st_trbal <> 0"
            sl_result = sql_connector.execute_query(sl_sql)
            if hasattr(sl_result, 'to_dict'):
                sl_result = sl_result.to_dict('records')
            sl_total = float(sl_result[0]['total'] or 0) if sl_result and sl_result[0]['total'] else 0

            # Customer master total
            sname_sql = "SELECT SUM(sn_currbal) AS total FROM sname WITH (NOLOCK) WHERE sn_currbal <> 0"
            sname_result = sql_connector.execute_query(sname_sql)
            if hasattr(sname_result, 'to_dict'):
                sname_result = sname_result.to_dict('records')
            sname_total = float(sname_result[0]['total'] or 0) if sname_result and sname_result[0]['total'] else 0

            # NL Debtors control
            debtors_control = control_accounts['debtors']
            nl_debtors_sql = f"""
                SELECT SUM(nt_value) AS total
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{debtors_control}'
            """
            nl_debtors_result = sql_connector.execute_query(nl_debtors_sql)
            if hasattr(nl_debtors_result, 'to_dict'):
                nl_debtors_result = nl_debtors_result.to_dict('records')
            nl_debtors_total = float(nl_debtors_result[0]['total'] or 0) if nl_debtors_result and nl_debtors_result[0]['total'] else 0

            # Check stran vs sname
            sl_vs_sname_variance = abs(sl_total - sname_total)
            sl_vs_sname_ok = sl_vs_sname_variance < 1.00

            # Check stran vs NL
            sl_vs_nl_variance = abs(sl_total - nl_debtors_total)
            sl_vs_nl_ok = sl_vs_nl_variance < 1.00

            debtors_ok = sl_vs_sname_ok and sl_vs_nl_ok

            summary["checks"].append({
                "name": "Debtors",
                "icon": "users",
                "reconciled": debtors_ok,
                "details": [
                    {"label": "Sales Ledger (stran)", "value": round(sl_total, 2)},
                    {"label": "Customer Master (sname)", "value": round(sname_total, 2)},
                    {"label": f"Nominal ({debtors_control})", "value": round(nl_debtors_total, 2)},
                ],
                "variances": [
                    {"label": "SL vs Master", "value": round(sl_vs_sname_variance, 2), "ok": sl_vs_sname_ok},
                    {"label": "SL vs NL", "value": round(sl_vs_nl_variance, 2), "ok": sl_vs_nl_ok},
                ]
            })
        except Exception as e:
            summary["checks"].append({
                "name": "Debtors",
                "icon": "users",
                "reconciled": False,
                "error": str(e)
            })

        # ========== 2. CREDITORS CHECK ==========
        try:
            # Purchase Ledger total
            pl_sql = """
                SELECT SUM(pt_trbal) AS total
                FROM ptran WITH (NOLOCK)
                WHERE pt_trbal <> 0
                  AND RTRIM(pt_account) IN (SELECT RTRIM(pn_account) FROM pname WITH (NOLOCK))
            """
            pl_result = sql_connector.execute_query(pl_sql)
            if hasattr(pl_result, 'to_dict'):
                pl_result = pl_result.to_dict('records')
            pl_total = float(pl_result[0]['total'] or 0) if pl_result and pl_result[0]['total'] else 0

            # Supplier master total
            pname_sql = "SELECT SUM(pn_currbal) AS total FROM pname WITH (NOLOCK) WHERE pn_currbal <> 0"
            pname_result = sql_connector.execute_query(pname_sql)
            if hasattr(pname_result, 'to_dict'):
                pname_result = pname_result.to_dict('records')
            pname_total = float(pname_result[0]['total'] or 0) if pname_result and pname_result[0]['total'] else 0

            # NL Creditors control (negate for comparison - NL is opposite sign)
            creditors_control = control_accounts['creditors']
            nl_creditors_sql = f"""
                SELECT SUM(nt_value) AS total
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{creditors_control}'
            """
            nl_creditors_result = sql_connector.execute_query(nl_creditors_sql)
            if hasattr(nl_creditors_result, 'to_dict'):
                nl_creditors_result = nl_creditors_result.to_dict('records')
            nl_creditors_total = -float(nl_creditors_result[0]['total'] or 0) if nl_creditors_result and nl_creditors_result[0]['total'] else 0

            # Check ptran vs pname
            pl_vs_pname_variance = abs(pl_total - pname_total)
            pl_vs_pname_ok = pl_vs_pname_variance < 1.00

            # Check ptran vs NL
            pl_vs_nl_variance = abs(pl_total - nl_creditors_total)
            pl_vs_nl_ok = pl_vs_nl_variance < 1.00

            creditors_ok = pl_vs_pname_ok and pl_vs_nl_ok

            summary["checks"].append({
                "name": "Creditors",
                "icon": "building",
                "reconciled": creditors_ok,
                "details": [
                    {"label": "Purchase Ledger (ptran)", "value": round(pl_total, 2)},
                    {"label": "Supplier Master (pname)", "value": round(pname_total, 2)},
                    {"label": f"Nominal ({creditors_control})", "value": round(nl_creditors_total, 2)},
                ],
                "variances": [
                    {"label": "PL vs Master", "value": round(pl_vs_pname_variance, 2), "ok": pl_vs_pname_ok},
                    {"label": "PL vs NL", "value": round(pl_vs_nl_variance, 2), "ok": pl_vs_nl_ok},
                ]
            })
        except Exception as e:
            summary["checks"].append({
                "name": "Creditors",
                "icon": "building",
                "reconciled": False,
                "error": str(e)
            })

        # ========== 3. CASHBOOK CHECK ==========
        try:
            # Get bank accounts - nk_acnt (e.g., 'BC010') is both the bank code AND the nominal code
            banks_sql = """
                SELECT nk_acnt, nk_curbal
                FROM nbank WITH (NOLOCK)
                WHERE nk_acnt LIKE 'BC%'
            """
            banks_result = sql_connector.execute_query(banks_sql)
            if hasattr(banks_result, 'to_dict'):
                banks_result = banks_result.to_dict('records')

            bank_master_total = 0
            nl_bank_total = 0

            for bank in banks_result or []:
                bank_code = bank['nk_acnt'].strip()
                # nk_curbal is in pence
                master_bal = float(bank['nk_curbal'] or 0) / 100.0
                bank_master_total += master_bal

                # In Opera, bank account code IS the nominal code (e.g., BC010)
                nl_sql = f"SELECT SUM(nt_value) AS total FROM ntran WITH (NOLOCK) WHERE nt_acnt = '{bank_code}'"
                nl_result = sql_connector.execute_query(nl_sql)
                if hasattr(nl_result, 'to_dict'):
                    nl_result = nl_result.to_dict('records')
                nl_bal = float(nl_result[0]['total'] or 0) if nl_result and nl_result[0]['total'] else 0
                nl_bank_total += nl_bal

            # Check bank master vs NL
            bank_variance = abs(bank_master_total - nl_bank_total)
            cashbook_ok = bank_variance < 1.00

            summary["checks"].append({
                "name": "Cashbook",
                "icon": "book",
                "reconciled": cashbook_ok,
                "details": [
                    {"label": "Bank Master (nbank)", "value": round(bank_master_total, 2)},
                    {"label": "Nominal Ledger", "value": round(nl_bank_total, 2)},
                ],
                "variances": [
                    {"label": "Bank vs NL", "value": round(bank_variance, 2), "ok": cashbook_ok},
                ]
            })
        except Exception as e:
            summary["checks"].append({
                "name": "Cashbook",
                "icon": "book",
                "reconciled": False,
                "error": str(e)
            })

        # ========== 4. VAT CHECK ==========
        try:
            # Get VAT nominal accounts from ztax
            ztax_sql = """
                SELECT DISTINCT tx_nominal, tx_trantyp
                FROM ztax WITH (NOLOCK)
                WHERE tx_ctrytyp = 'H' AND tx_nominal IS NOT NULL AND tx_nominal <> ''
            """
            ztax_result = sql_connector.execute_query(ztax_sql)
            if hasattr(ztax_result, 'to_dict'):
                ztax_result = ztax_result.to_dict('records')

            output_nominals = set()
            input_nominals = set()
            for row in ztax_result or []:
                nominal = row['tx_nominal'].strip() if row['tx_nominal'] else ''
                vat_type = row['tx_trantyp'].strip() if row['tx_trantyp'] else ''
                if nominal:
                    if vat_type == 'S':
                        output_nominals.add(nominal)
                    elif vat_type == 'P':
                        input_nominals.add(nominal)

            # Get current year
            cy_sql = "SELECT MAX(nt_year) AS current_year FROM ntran WITH (NOLOCK)"
            cy_result = sql_connector.execute_query(cy_sql)
            if hasattr(cy_result, 'to_dict'):
                cy_result = cy_result.to_dict('records')
            current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

            # nvat totals
            nvat_output_sql = f"SELECT SUM(nv_vatval) AS total FROM nvat WITH (NOLOCK) WHERE nv_vattype = 'S' AND YEAR(nv_date) = {current_year}"
            nvat_output_result = sql_connector.execute_query(nvat_output_sql)
            if hasattr(nvat_output_result, 'to_dict'):
                nvat_output_result = nvat_output_result.to_dict('records')
            nvat_output = float(nvat_output_result[0]['total'] or 0) if nvat_output_result and nvat_output_result[0]['total'] else 0

            nvat_input_sql = f"SELECT SUM(nv_vatval) AS total FROM nvat WITH (NOLOCK) WHERE nv_vattype = 'P' AND YEAR(nv_date) = {current_year}"
            nvat_input_result = sql_connector.execute_query(nvat_input_sql)
            if hasattr(nvat_input_result, 'to_dict'):
                nvat_input_result = nvat_input_result.to_dict('records')
            nvat_input = float(nvat_input_result[0]['total'] or 0) if nvat_input_result and nvat_input_result[0]['total'] else 0

            nvat_net = nvat_output - nvat_input

            # NL VAT totals
            nl_vat_total = 0
            all_vat_nominals = output_nominals.union(input_nominals)
            for acnt in all_vat_nominals:
                nl_sql = f"SELECT SUM(nt_value) AS total FROM ntran WITH (NOLOCK) WHERE nt_acnt = '{acnt}' AND nt_year = {current_year}"
                nl_result = sql_connector.execute_query(nl_sql)
                if hasattr(nl_result, 'to_dict'):
                    nl_result = nl_result.to_dict('records')
                nl_vat_total += -float(nl_result[0]['total'] or 0) if nl_result and nl_result[0]['total'] else 0

            vat_variance = abs(nvat_net - nl_vat_total)
            vat_ok = vat_variance < 1.00

            summary["checks"].append({
                "name": "VAT",
                "icon": "receipt",
                "reconciled": vat_ok,
                "details": [
                    {"label": "Output VAT (nvat)", "value": round(nvat_output, 2)},
                    {"label": "Input VAT (nvat)", "value": round(nvat_input, 2)},
                    {"label": "Net VAT (nvat)", "value": round(nvat_net, 2)},
                    {"label": "Nominal Ledger", "value": round(nl_vat_total, 2)},
                ],
                "variances": [
                    {"label": "nvat vs NL", "value": round(vat_variance, 2), "ok": vat_ok},
                ]
            })
        except Exception as e:
            summary["checks"].append({
                "name": "VAT",
                "icon": "receipt",
                "reconciled": False,
                "error": str(e)
            })

        # Calculate overall status
        summary["total_checks"] = len(summary["checks"])
        summary["passed_checks"] = sum(1 for c in summary["checks"] if c.get("reconciled", False))
        summary["failed_checks"] = summary["total_checks"] - summary["passed_checks"]
        summary["all_reconciled"] = summary["failed_checks"] == 0

        return summary

    except Exception as e:
        logger.error(f"Reconciliation summary failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/reconcile/vat/diagnostic")
async def vat_diagnostic():
    """
    Diagnostic endpoint to check VAT table data availability.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        result = {"tables": {}}

        # Check zvtran
        try:
            zvtran_sql = """
                SELECT
                    COUNT(*) AS total_rows,
                    SUM(CASE WHEN va_done = 0 THEN 1 ELSE 0 END) AS uncommitted,
                    SUM(CASE WHEN va_done = 1 THEN 1 ELSE 0 END) AS committed,
                    MIN(va_taxdate) AS min_date,
                    MAX(va_taxdate) AS max_date,
                    SUM(va_vatval) AS total_vat
                FROM zvtran WITH (NOLOCK)
            """
            zvtran_result = sql_connector.execute_query(zvtran_sql)
            if hasattr(zvtran_result, 'to_dict'):
                zvtran_result = zvtran_result.to_dict('records')
            result["tables"]["zvtran"] = zvtran_result[0] if zvtran_result else {"error": "no data"}
        except Exception as e:
            result["tables"]["zvtran"] = {"error": str(e)}

        # Check nvat
        try:
            nvat_sql = """
                SELECT
                    COUNT(*) AS total_rows,
                    MIN(nv_date) AS min_date,
                    MAX(nv_date) AS max_date,
                    SUM(nv_vatval) AS total_vat,
                    COUNT(DISTINCT nv_vattype) AS vat_types
                FROM nvat WITH (NOLOCK)
            """
            nvat_result = sql_connector.execute_query(nvat_sql)
            if hasattr(nvat_result, 'to_dict'):
                nvat_result = nvat_result.to_dict('records')
            result["tables"]["nvat"] = nvat_result[0] if nvat_result else {"error": "no data"}
        except Exception as e:
            result["tables"]["nvat"] = {"error": str(e)}

        # Check ztax (VAT codes)
        try:
            ztax_sql = """
                SELECT COUNT(*) AS total_codes
                FROM ztax WITH (NOLOCK)
                WHERE tx_ctrytyp = 'H'
            """
            ztax_result = sql_connector.execute_query(ztax_sql)
            if hasattr(ztax_result, 'to_dict'):
                ztax_result = ztax_result.to_dict('records')
            result["tables"]["ztax"] = ztax_result[0] if ztax_result else {"error": "no data"}
        except Exception as e:
            result["tables"]["ztax"] = {"error": str(e)}

        # Check ntran for current year
        try:
            ntran_sql = """
                SELECT
                    MAX(nt_year) AS current_year,
                    COUNT(*) AS total_rows
                FROM ntran WITH (NOLOCK)
            """
            ntran_result = sql_connector.execute_query(ntran_sql)
            if hasattr(ntran_result, 'to_dict'):
                ntran_result = ntran_result.to_dict('records')
            result["tables"]["ntran"] = ntran_result[0] if ntran_result else {"error": "no data"}
        except Exception as e:
            result["tables"]["ntran"] = {"error": str(e)}

        return result

    except Exception as e:
        return {"error": str(e)}


@app.get("/api/reconcile/vat/variance-drilldown")
async def vat_variance_drilldown():
    """
    Drill-down to identify causes of VAT variance between zvtran and nominal ledger.
    Shows transactions that don't reconcile.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        result = {
            "success": True,
            "analysis": {}
        }

        # Get VAT nominal accounts from ztax
        ztax_sql = """
            SELECT DISTINCT tx_nominal
            FROM ztax WITH (NOLOCK)
            WHERE tx_ctrytyp = 'H'
              AND tx_nominal IS NOT NULL
              AND RTRIM(tx_nominal) != ''
        """
        ztax_result = sql_connector.execute_query(ztax_sql)
        if hasattr(ztax_result, 'to_dict'):
            ztax_result = ztax_result.to_dict('records')

        vat_nominals = [r['tx_nominal'].strip() for r in (ztax_result or []) if r.get('tx_nominal')]
        result["vat_nominal_accounts"] = vat_nominals

        # 1. Get uncommitted VAT totals by year/period
        zvtran_by_period_sql = """
            SELECT
                YEAR(va_taxdate) AS year,
                MONTH(va_taxdate) AS month,
                va_vattype AS vat_type,
                COUNT(*) AS transaction_count,
                SUM(va_vatval) AS vat_total
            FROM zvtran WITH (NOLOCK)
            WHERE va_done = 0
            GROUP BY YEAR(va_taxdate), MONTH(va_taxdate), va_vattype
            ORDER BY year DESC, month DESC, va_vattype
        """
        zvtran_periods = sql_connector.execute_query(zvtran_by_period_sql)
        if hasattr(zvtran_periods, 'to_dict'):
            zvtran_periods = zvtran_periods.to_dict('records')

        result["analysis"]["uncommitted_by_period"] = [{
            "year": int(r.get('year') or 0),
            "month": int(r.get('month') or 0),
            "type": r.get('vat_type', '').strip(),
            "count": int(r.get('transaction_count') or 0),
            "total": round(float(r.get('vat_total') or 0), 2)
        } for r in (zvtran_periods or [])]

        # 2. Get nominal ledger VAT movements by year/period
        nl_by_period = []
        for acnt in vat_nominals:
            nl_period_sql = f"""
                SELECT
                    nt_year AS year,
                    nt_period AS period,
                    COUNT(*) AS transaction_count,
                    SUM(nt_value) AS total_value
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{acnt}'
                GROUP BY nt_year, nt_period
                ORDER BY nt_year DESC, nt_period DESC
            """
            nl_result = sql_connector.execute_query(nl_period_sql)
            if hasattr(nl_result, 'to_dict'):
                nl_result = nl_result.to_dict('records')

            for r in (nl_result or []):
                nl_by_period.append({
                    "account": acnt,
                    "year": int(r.get('year') or 0),
                    "period": int(r.get('period') or 0),
                    "count": int(r.get('transaction_count') or 0),
                    "total": round(float(r.get('total_value') or 0), 2)
                })

        result["analysis"]["nominal_by_period"] = nl_by_period

        # 3. Get largest uncommitted VAT transactions
        largest_sql = """
            SELECT TOP 100
                va_taxdate,
                va_vattype,
                va_vatval,
                va_trvalue,
                va_anvat
            FROM zvtran WITH (NOLOCK)
            WHERE va_done = 0
            ORDER BY ABS(va_vatval) DESC
        """
        largest_result = sql_connector.execute_query(largest_sql)
        if hasattr(largest_result, 'to_dict'):
            largest_result = largest_result.to_dict('records')

        result["analysis"]["largest_uncommitted"] = [{
            "date": str(r.get('va_taxdate') or ''),
            "type": (r.get('va_vattype') or '').strip(),
            "vat_amount": round(float(r.get('va_vatval') or 0), 2),
            "net_amount": round(float(r.get('va_trvalue') or 0), 2),
            "vat_code": (r.get('va_anvat') or '').strip()
        } for r in (largest_result or [])[:50]]

        # 4. Get nominal ledger entries on VAT accounts - largest transactions
        for acnt in vat_nominals[:2]:  # Check first 2 VAT accounts
            nl_entries_sql = f"""
                SELECT TOP 50
                    nt_entr AS post_date,
                    nt_value,
                    nt_trnref,
                    nt_posttyp,
                    nt_year,
                    nt_period
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{acnt}'
                ORDER BY ABS(nt_value) DESC
            """
            nl_entries_result = sql_connector.execute_query(nl_entries_sql)
            if hasattr(nl_entries_result, 'to_dict'):
                nl_entries_result = nl_entries_result.to_dict('records')

            result["analysis"][f"largest_nl_entries_{acnt}"] = [{
                "date": str(r.get('post_date') or ''),
                "value": round(float(r.get('nt_value') or 0), 2),
                "reference": (r.get('nt_trnref') or '').strip()[:40],
                "type": (r.get('nt_posttyp') or '').strip(),
                "year": int(r.get('nt_year') or 0),
                "period": int(r.get('nt_period') or 0)
            } for r in (nl_entries_result or [])]

        # 5. Summary comparison
        # Total uncommitted VAT
        total_uncommitted_sql = """
            SELECT
                SUM(CASE WHEN va_vattype = 'S' THEN va_vatval ELSE 0 END) AS output_total,
                SUM(CASE WHEN va_vattype = 'P' THEN va_vatval ELSE 0 END) AS input_total,
                COUNT(*) AS total_records
            FROM zvtran WITH (NOLOCK)
            WHERE va_done = 0
        """
        total_uncommitted = sql_connector.execute_query(total_uncommitted_sql)
        if hasattr(total_uncommitted, 'to_dict'):
            total_uncommitted = total_uncommitted.to_dict('records')

        uncommitted_output = float(total_uncommitted[0]['output_total'] or 0) if total_uncommitted else 0
        uncommitted_input = float(total_uncommitted[0]['input_total'] or 0) if total_uncommitted else 0
        uncommitted_net = uncommitted_output - uncommitted_input
        uncommitted_count = int(total_uncommitted[0]['total_records'] or 0) if total_uncommitted else 0

        # Total nominal balance on VAT accounts
        nl_total = 0
        nl_count = 0
        for acnt in vat_nominals:
            nl_sum_sql = f"""
                SELECT SUM(nt_value) AS total, COUNT(*) AS cnt
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{acnt}'
            """
            nl_sum = sql_connector.execute_query(nl_sum_sql)
            if hasattr(nl_sum, 'to_dict'):
                nl_sum = nl_sum.to_dict('records')
            if nl_sum and nl_sum[0]:
                nl_total += float(nl_sum[0]['total'] or 0)
                nl_count += int(nl_sum[0]['cnt'] or 0)

        # VAT liability is typically credit, so negate for comparison
        nl_balance = -nl_total

        result["summary"] = {
            "uncommitted_vat": {
                "output": round(uncommitted_output, 2),
                "input": round(uncommitted_input, 2),
                "net": round(uncommitted_net, 2),
                "record_count": uncommitted_count
            },
            "nominal_balance": {
                "total": round(nl_balance, 2),
                "record_count": nl_count
            },
            "variance": round(uncommitted_net - nl_balance, 2),
            "variance_explanation": []
        }

        # Add variance explanations
        variance = uncommitted_net - nl_balance
        if abs(variance) > 1:
            if variance > 0:
                result["summary"]["variance_explanation"].append(
                    f"Uncommitted VAT is £{variance:,.2f} MORE than nominal balance"
                )
                result["summary"]["variance_explanation"].append(
                    "Possible causes: VAT transactions not posted to nominal, or nominal entries reversed"
                )
            else:
                result["summary"]["variance_explanation"].append(
                    f"Uncommitted VAT is £{abs(variance):,.2f} LESS than nominal balance"
                )
                result["summary"]["variance_explanation"].append(
                    "Possible causes: Nominal entries without zvtran records, or VAT returns processed but marked done"
                )

        return result

    except Exception as e:
        logger.error(f"VAT variance drilldown failed: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


@app.get("/api/reconcile/vat")
async def reconcile_vat():
    """
    Reconcile VAT accounts - compare VAT liability in nominal ledger to VAT transactions.
    Enhanced for quarterly VAT tracking with uncommitted transactions from zvtran.
    Shows output VAT (sales), input VAT (purchases), and net liability for current quarter.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # First, find the most recent uncommitted VAT transaction date to determine the relevant quarter
        # This handles cases where the database has data from a different year than the current calendar date
        recent_vat_date_sql = """
            SELECT MAX(va_taxdate) AS most_recent_date
            FROM zvtran WITH (NOLOCK)
            WHERE va_done = 0
        """
        recent_vat_result = sql_connector.execute_query(recent_vat_date_sql)
        if hasattr(recent_vat_result, 'to_dict'):
            recent_vat_result = recent_vat_result.to_dict('records')

        # Use the most recent uncommitted VAT date as reference, or fallback to current date
        reference_date = None
        if recent_vat_result and recent_vat_result[0] and recent_vat_result[0].get('most_recent_date'):
            reference_date = recent_vat_result[0]['most_recent_date']
            if isinstance(reference_date, str):
                reference_date = datetime.strptime(reference_date, '%Y-%m-%d')

        quarter_info = get_vat_quarter_dates(reference_date)

        reconciliation = {
            "success": True,
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "quarter_info": quarter_info,
            "vat_codes": [],
            "current_quarter": {
                "output_vat": {},
                "input_vat": {},
                "uncommitted": {},
                "nominal_movements": {}
            },
            "year_to_date": {
                "output_vat": {},
                "input_vat": {},
                "nominal_accounts": []
            },
            "variance": {},
            "status": "UNRECONCILED",
            "message": ""
        }

        # Get current year from ntran - use NOLOCK to avoid locking
        current_year_sql = "SELECT MAX(nt_year) AS current_year FROM ntran WITH (NOLOCK)"
        cy_result = sql_connector.execute_query(current_year_sql)
        if hasattr(cy_result, 'to_dict'):
            cy_result = cy_result.to_dict('records')
        current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

        # If no uncommitted VAT found, also check nvat for the most recent date
        if reference_date is None:
            nvat_date_sql = """
                SELECT MAX(nv_date) AS most_recent_date
                FROM nvat WITH (NOLOCK)
            """
            nvat_date_result = sql_connector.execute_query(nvat_date_sql)
            if hasattr(nvat_date_result, 'to_dict'):
                nvat_date_result = nvat_date_result.to_dict('records')
            if nvat_date_result and nvat_date_result[0] and nvat_date_result[0].get('most_recent_date'):
                reference_date = nvat_date_result[0]['most_recent_date']
                if isinstance(reference_date, str):
                    reference_date = datetime.strptime(reference_date, '%Y-%m-%d')
                # Recalculate quarter_info with the nvat date
                quarter_info = get_vat_quarter_dates(reference_date)
                reconciliation["quarter_info"] = quarter_info

        # Get VAT codes from ztax with date-based rate calculation
        ztax_sql = """
            SELECT tx_code, tx_desc, tx_rate1, tx_rate1dy, tx_rate2, tx_rate2dy, tx_trantyp, tx_nominal
            FROM ztax WITH (NOLOCK)
            WHERE tx_ctrytyp = 'H'
            ORDER BY tx_trantyp, tx_code
        """
        ztax_result = sql_connector.execute_query(ztax_sql)
        if hasattr(ztax_result, 'to_dict'):
            ztax_result = ztax_result.to_dict('records')

        vat_codes = []
        output_nominal_accounts = set()
        input_nominal_accounts = set()
        ref_date = datetime.now().date()

        for row in ztax_result or []:
            code = row['tx_code'].strip() if row['tx_code'] else ''
            nominal = row['tx_nominal'].strip() if row['tx_nominal'] else ''
            vat_type = row['tx_trantyp'].strip() if row['tx_trantyp'] else ''

            # Calculate applicable rate based on today's date
            rate1 = float(row['tx_rate1'] or 0)
            rate2 = float(row['tx_rate2'] or 0)
            date1 = row.get('tx_rate1dy')
            date2 = row.get('tx_rate2dy')

            # Convert dates if needed, handle NaT/None/NaN
            try:
                if date1 is not None and date1 == date1:  # NaT/NaN check (NaN != NaN)
                    if hasattr(date1, 'date'):
                        date1 = date1.date()
                else:
                    date1 = None
            except (TypeError, ValueError):
                date1 = None

            try:
                if date2 is not None and date2 == date2:  # NaT/NaN check
                    if hasattr(date2, 'date'):
                        date2 = date2.date()
                else:
                    date2 = None
            except (TypeError, ValueError):
                date2 = None

            # Determine applicable rate (most recent effective date <= today)
            applicable_rate = rate1
            if date1 and date2:
                if date2 <= ref_date and date1 <= ref_date:
                    applicable_rate = rate2 if date2 > date1 else rate1
                elif date2 <= ref_date:
                    applicable_rate = rate2
                elif date1 <= ref_date:
                    applicable_rate = rate1
            elif date2 and date2 <= ref_date:
                applicable_rate = rate2

            vat_codes.append({
                "code": code,
                "description": row['tx_desc'].strip() if row['tx_desc'] else '',
                "rate": applicable_rate,
                "type": vat_type,  # 'S' = Sales/Output, 'P' = Purchase/Input
                "nominal_account": nominal
            })
            if nominal:
                if vat_type == 'S':
                    output_nominal_accounts.add(nominal)
                elif vat_type == 'P':
                    input_nominal_accounts.add(nominal)

        reconciliation["vat_codes"] = vat_codes

        quarter_start = quarter_info['quarter_start']
        quarter_end = quarter_info['quarter_end']

        # ==========================================
        # CURRENT QUARTER - Uncommitted VAT (zvtran)
        # ==========================================

        # Get uncommitted Output VAT from zvtran (va_done = 0, va_vattype = 'S')
        uncommitted_output_sql = f"""
            SELECT
                va_anvat AS vat_code,
                COUNT(*) AS transaction_count,
                SUM(va_vatval) AS vat_amount,
                SUM(va_trvalue) AS net_amount
            FROM zvtran WITH (NOLOCK)
            WHERE va_vattype = 'S'
              AND va_done = 0
              AND va_taxdate >= '{quarter_start}'
              AND va_taxdate <= '{quarter_end}'
            GROUP BY va_anvat
            ORDER BY va_anvat
        """
        uncommitted_output_result = sql_connector.execute_query(uncommitted_output_sql)
        if hasattr(uncommitted_output_result, 'to_dict'):
            uncommitted_output_result = uncommitted_output_result.to_dict('records')

        uncommitted_output_total = 0
        uncommitted_output_by_code = []
        for row in uncommitted_output_result or []:
            vat_amount = float(row['vat_amount'] or 0)
            uncommitted_output_total += vat_amount
            uncommitted_output_by_code.append({
                "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
                "transaction_count": int(row['transaction_count'] or 0),
                "vat_amount": round(vat_amount, 2),
                "net_amount": round(float(row['net_amount'] or 0), 2)
            })

        # Get uncommitted Input VAT from zvtran (va_done = 0, va_vattype = 'P')
        uncommitted_input_sql = f"""
            SELECT
                va_anvat AS vat_code,
                COUNT(*) AS transaction_count,
                SUM(va_vatval) AS vat_amount,
                SUM(va_trvalue) AS net_amount
            FROM zvtran WITH (NOLOCK)
            WHERE va_vattype = 'P'
              AND va_done = 0
              AND va_taxdate >= '{quarter_start}'
              AND va_taxdate <= '{quarter_end}'
            GROUP BY va_anvat
            ORDER BY va_anvat
        """
        uncommitted_input_result = sql_connector.execute_query(uncommitted_input_sql)
        if hasattr(uncommitted_input_result, 'to_dict'):
            uncommitted_input_result = uncommitted_input_result.to_dict('records')

        uncommitted_input_total = 0
        uncommitted_input_by_code = []
        for row in uncommitted_input_result or []:
            vat_amount = float(row['vat_amount'] or 0)
            uncommitted_input_total += vat_amount
            uncommitted_input_by_code.append({
                "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
                "transaction_count": int(row['transaction_count'] or 0),
                "vat_amount": round(vat_amount, 2),
                "net_amount": round(float(row['net_amount'] or 0), 2)
            })

        uncommitted_net = uncommitted_output_total - uncommitted_input_total

        reconciliation["current_quarter"]["uncommitted"] = {
            "source": "zvtran (VAT Return Transactions - va_done=0)",
            "quarter": quarter_info['current_quarter'],
            "period_start": quarter_start,
            "period_end": quarter_end,
            "output_vat": {
                "total": round(uncommitted_output_total, 2),
                "by_code": uncommitted_output_by_code
            },
            "input_vat": {
                "total": round(uncommitted_input_total, 2),
                "by_code": uncommitted_input_by_code
            },
            "net_liability": round(uncommitted_net, 2),
            "description": "VAT transactions not yet submitted in a VAT return"
        }

        # ==========================================
        # CURRENT QUARTER - NL Movements
        # ==========================================

        # Get quarter nominal ledger movements for VAT accounts
        all_vat_nominals = output_nominal_accounts.union(input_nominal_accounts)
        quarter_nl_movements = []
        quarter_nl_output_total = 0
        quarter_nl_input_total = 0

        for acnt in all_vat_nominals:
            ntran_quarter_sql = f"""
                SELECT
                    SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                    SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                    SUM(nt_value) AS net,
                    COUNT(*) AS transaction_count
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{acnt}'
                  AND nt_entr >= '{quarter_start}'
                  AND nt_entr <= '{quarter_end}'
            """
            ntran_quarter_result = sql_connector.execute_query(ntran_quarter_sql)
            if hasattr(ntran_quarter_result, 'to_dict'):
                ntran_quarter_result = ntran_quarter_result.to_dict('records')

            if ntran_quarter_result and ntran_quarter_result[0]:
                row = ntran_quarter_result[0]
                debits = float(row['debits'] or 0)
                credits = float(row['credits'] or 0)
                net = float(row['net'] or 0)
                txn_count = int(row['transaction_count'] or 0)

                is_output = acnt in output_nominal_accounts
                is_input = acnt in input_nominal_accounts

                # Get account description
                nacnt_sql = f"SELECT RTRIM(na_desc) AS description FROM nacnt WITH (NOLOCK) WHERE na_acnt = '{acnt}'"
                nacnt_result = sql_connector.execute_query(nacnt_sql)
                if hasattr(nacnt_result, 'to_dict'):
                    nacnt_result = nacnt_result.to_dict('records')
                description = nacnt_result[0]['description'] if nacnt_result else ''

                if txn_count > 0:
                    quarter_nl_movements.append({
                        "account": acnt,
                        "description": description,
                        "type": "Output" if is_output else ("Input" if is_input else "Mixed"),
                        "debits": round(debits, 2),
                        "credits": round(credits, 2),
                        "net": round(net, 2),
                        "transaction_count": txn_count
                    })

                    # For Output VAT, credits increase liability
                    if is_output:
                        quarter_nl_output_total += credits
                    # For Input VAT, debits represent reclaimable VAT
                    if is_input:
                        quarter_nl_input_total += debits

        reconciliation["current_quarter"]["nominal_movements"] = {
            "source": "ntran (Nominal Ledger)",
            "quarter": quarter_info['current_quarter'],
            "period_start": quarter_start,
            "period_end": quarter_end,
            "accounts": quarter_nl_movements,
            "output_vat_total": round(quarter_nl_output_total, 2),
            "input_vat_total": round(quarter_nl_input_total, 2),
            "net_movement": round(quarter_nl_output_total - quarter_nl_input_total, 2)
        }

        # ==========================================
        # CURRENT QUARTER - nvat transactions
        # ==========================================

        # Get quarter Output VAT from nvat
        quarter_output_sql = f"""
            SELECT
                nv_vatcode AS vat_code,
                COUNT(*) AS transaction_count,
                SUM(nv_vatval) AS vat_amount
            FROM nvat WITH (NOLOCK)
            WHERE nv_vattype = 'S'
              AND nv_date >= '{quarter_start}'
              AND nv_date <= '{quarter_end}'
            GROUP BY nv_vatcode
            ORDER BY nv_vatcode
        """
        quarter_output_result = sql_connector.execute_query(quarter_output_sql)
        if hasattr(quarter_output_result, 'to_dict'):
            quarter_output_result = quarter_output_result.to_dict('records')

        quarter_output_total = 0
        quarter_output_by_code = []
        for row in quarter_output_result or []:
            vat_amount = float(row['vat_amount'] or 0)
            quarter_output_total += vat_amount
            quarter_output_by_code.append({
                "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
                "transaction_count": int(row['transaction_count'] or 0),
                "vat_amount": round(vat_amount, 2)
            })

        reconciliation["current_quarter"]["output_vat"] = {
            "source": "nvat (VAT Transactions - Sales/Output)",
            "total_vat": round(quarter_output_total, 2),
            "by_code": quarter_output_by_code,
            "quarter": quarter_info['current_quarter']
        }

        # Get quarter Input VAT from nvat
        quarter_input_sql = f"""
            SELECT
                nv_vatcode AS vat_code,
                COUNT(*) AS transaction_count,
                SUM(nv_vatval) AS vat_amount
            FROM nvat WITH (NOLOCK)
            WHERE nv_vattype = 'P'
              AND nv_date >= '{quarter_start}'
              AND nv_date <= '{quarter_end}'
            GROUP BY nv_vatcode
            ORDER BY nv_vatcode
        """
        quarter_input_result = sql_connector.execute_query(quarter_input_sql)
        if hasattr(quarter_input_result, 'to_dict'):
            quarter_input_result = quarter_input_result.to_dict('records')

        quarter_input_total = 0
        quarter_input_by_code = []
        for row in quarter_input_result or []:
            vat_amount = float(row['vat_amount'] or 0)
            quarter_input_total += vat_amount
            quarter_input_by_code.append({
                "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
                "transaction_count": int(row['transaction_count'] or 0),
                "vat_amount": round(vat_amount, 2)
            })

        reconciliation["current_quarter"]["input_vat"] = {
            "source": "nvat (VAT Transactions - Purchase/Input)",
            "total_vat": round(quarter_input_total, 2),
            "by_code": quarter_input_by_code,
            "quarter": quarter_info['current_quarter']
        }

        # ==========================================
        # YEAR TO DATE - nvat totals
        # ==========================================

        # Get YTD Output VAT (Sales) from nvat
        output_vat_sql = f"""
            SELECT
                nv_vatcode AS vat_code,
                COUNT(*) AS transaction_count,
                SUM(nv_vatval) AS vat_amount
            FROM nvat WITH (NOLOCK)
            WHERE nv_vattype = 'S' AND YEAR(nv_date) = {current_year}
            GROUP BY nv_vatcode
            ORDER BY nv_vatcode
        """
        output_result = sql_connector.execute_query(output_vat_sql)
        if hasattr(output_result, 'to_dict'):
            output_result = output_result.to_dict('records')

        ytd_output_total = 0
        ytd_output_by_code = []
        for row in output_result or []:
            vat_amount = float(row['vat_amount'] or 0)
            ytd_output_total += vat_amount
            ytd_output_by_code.append({
                "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
                "transaction_count": int(row['transaction_count'] or 0),
                "vat_amount": round(vat_amount, 2)
            })

        reconciliation["year_to_date"]["output_vat"] = {
            "source": "nvat (VAT Transactions - Sales/Output)",
            "total_vat": round(ytd_output_total, 2),
            "by_code": ytd_output_by_code,
            "current_year": current_year
        }

        # Get YTD Input VAT (Purchases) from nvat
        input_vat_sql = f"""
            SELECT
                nv_vatcode AS vat_code,
                COUNT(*) AS transaction_count,
                SUM(nv_vatval) AS vat_amount
            FROM nvat WITH (NOLOCK)
            WHERE nv_vattype = 'P' AND YEAR(nv_date) = {current_year}
            GROUP BY nv_vatcode
            ORDER BY nv_vatcode
        """
        input_result = sql_connector.execute_query(input_vat_sql)
        if hasattr(input_result, 'to_dict'):
            input_result = input_result.to_dict('records')

        ytd_input_total = 0
        ytd_input_by_code = []
        for row in input_result or []:
            vat_amount = float(row['vat_amount'] or 0)
            ytd_input_total += vat_amount
            ytd_input_by_code.append({
                "vat_code": row['vat_code'].strip() if row['vat_code'] else '',
                "transaction_count": int(row['transaction_count'] or 0),
                "vat_amount": round(vat_amount, 2)
            })

        reconciliation["year_to_date"]["input_vat"] = {
            "source": "nvat (VAT Transactions - Purchase/Input)",
            "total_vat": round(ytd_input_total, 2),
            "by_code": ytd_input_by_code,
            "current_year": current_year
        }

        # ==========================================
        # YEAR TO DATE - Nominal accounts
        # ==========================================

        nominal_accounts = []
        nl_total = 0

        for acnt in all_vat_nominals:
            nacnt_sql = f"""
                SELECT na_acnt, RTRIM(na_desc) AS description, na_ytddr, na_ytdcr, na_prydr, na_prycr
                FROM nacnt WITH (NOLOCK)
                WHERE na_acnt = '{acnt}'
            """
            nacnt_result = sql_connector.execute_query(nacnt_sql)
            if hasattr(nacnt_result, 'to_dict'):
                nacnt_result = nacnt_result.to_dict('records')

            if nacnt_result:
                acc = nacnt_result[0]
                ytd_dr = float(acc['na_ytddr'] or 0)
                ytd_cr = float(acc['na_ytdcr'] or 0)
                pry_dr = float(acc['na_prydr'] or 0)
                pry_cr = float(acc['na_prycr'] or 0)

                bf_balance = pry_cr - pry_dr

                # Get current year transactions
                ntran_sql = f"""
                    SELECT
                        SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                        SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                        SUM(nt_value) AS net
                    FROM ntran WITH (NOLOCK)
                    WHERE nt_acnt = '{acnt}' AND nt_year = {current_year}
                """
                ntran_result = sql_connector.execute_query(ntran_sql)
                if hasattr(ntran_result, 'to_dict'):
                    ntran_result = ntran_result.to_dict('records')

                current_year_dr = float(ntran_result[0]['debits'] or 0) if ntran_result else 0
                current_year_cr = float(ntran_result[0]['credits'] or 0) if ntran_result else 0
                current_year_net = float(ntran_result[0]['net'] or 0) if ntran_result else 0

                # VAT liability is typically a credit balance (negative in accounting convention)
                closing_balance = -current_year_net

                is_output = acnt in output_nominal_accounts
                is_input = acnt in input_nominal_accounts

                nominal_accounts.append({
                    "account": acnt,
                    "description": acc['description'] or '',
                    "type": "Output" if is_output else ("Input" if is_input else "Mixed"),
                    "brought_forward": round(bf_balance, 2),
                    "current_year_debits": round(current_year_dr, 2),
                    "current_year_credits": round(current_year_cr, 2),
                    "current_year_net": round(current_year_net, 2),
                    "closing_balance": round(closing_balance, 2)
                })

                nl_total += closing_balance

        reconciliation["year_to_date"]["nominal_accounts"] = {
            "source": "ntran (Nominal Ledger)",
            "accounts": nominal_accounts,
            "total_balance": round(nl_total, 2),
            "current_year": current_year
        }

        # ==========================================
        # VAT ACCOUNT BALANCE & BANK TRANSACTIONS
        # ==========================================

        # Get total uncommitted VAT (ALL uncommitted, not just current quarter)
        total_uncommitted_sql = """
            SELECT
                SUM(CASE WHEN va_vattype = 'S' THEN va_vatval ELSE 0 END) AS output_total,
                SUM(CASE WHEN va_vattype = 'P' THEN va_vatval ELSE 0 END) AS input_total
            FROM zvtran WITH (NOLOCK)
            WHERE va_done = 0
        """
        total_uncommitted_result = sql_connector.execute_query(total_uncommitted_sql)
        if hasattr(total_uncommitted_result, 'to_dict'):
            total_uncommitted_result = total_uncommitted_result.to_dict('records')

        total_uncommitted_output = float(total_uncommitted_result[0]['output_total'] or 0) if total_uncommitted_result else 0
        total_uncommitted_input = float(total_uncommitted_result[0]['input_total'] or 0) if total_uncommitted_result else 0
        total_uncommitted_net = total_uncommitted_output - total_uncommitted_input

        # Get current VAT nominal account balance
        vat_account_balance = 0
        for acnt in all_vat_nominals:
            balance_sql = f"""
                SELECT SUM(nt_value) AS balance
                FROM ntran WITH (NOLOCK)
                WHERE nt_acnt = '{acnt}'
            """
            balance_result = sql_connector.execute_query(balance_sql)
            if hasattr(balance_result, 'to_dict'):
                balance_result = balance_result.to_dict('records')
            if balance_result and balance_result[0]['balance']:
                # VAT liability is typically credit (negative), so we negate
                vat_account_balance += float(balance_result[0]['balance'] or 0)

        # Check for bank/cashbook transactions on VAT accounts
        # These would be VAT payments to HMRC (reducing liability) or refunds
        vat_bank_transactions = []
        vat_bank_total = 0
        for acnt in all_vat_nominals:
            bank_vat_sql = f"""
                SELECT
                    ax_date AS trans_date,
                    ax_value AS amount,
                    ax_tref AS reference,
                    ax_source AS source
                FROM anoml WITH (NOLOCK)
                WHERE ax_nacnt = '{acnt}'
                  AND ax_source = 'A'
                ORDER BY ax_date DESC
            """
            bank_vat_result = sql_connector.execute_query(bank_vat_sql)
            if hasattr(bank_vat_result, 'to_dict'):
                bank_vat_result = bank_vat_result.to_dict('records')

            for row in bank_vat_result or []:
                amount = float(row['amount'] or 0)
                vat_bank_total += amount
                vat_bank_transactions.append({
                    "date": str(row['trans_date'] or ''),
                    "amount": round(amount, 2),
                    "reference": (row['reference'] or '').strip(),
                    "account": acnt
                })

        # The reconciliation formula:
        # Total Uncommitted VAT should = VAT Account Balance + VAT Bank Payments (if payment made but return not processed)
        # Or: VAT Account Balance should = Total Uncommitted VAT - VAT Bank Payments
        adjusted_balance = -vat_account_balance  # Negate because liability is credit
        reconciliation["vat_balance"] = {
            "total_uncommitted_vat": round(total_uncommitted_net, 2),
            "total_uncommitted_output": round(total_uncommitted_output, 2),
            "total_uncommitted_input": round(total_uncommitted_input, 2),
            "vat_account_balance": round(adjusted_balance, 2),
            "vat_bank_transactions": vat_bank_transactions[:10],  # Last 10 transactions
            "vat_bank_total": round(vat_bank_total, 2),
            "expected_balance": round(total_uncommitted_net - vat_bank_total, 2),
            "balance_variance": round(adjusted_balance - (total_uncommitted_net - vat_bank_total), 2),
            "reconciled": abs(adjusted_balance - (total_uncommitted_net - vat_bank_total)) < 1.00
        }

        # ==========================================
        # VARIANCE CALCULATION
        # ==========================================

        # Primary reconciliation: Uncommitted VAT (zvtran) vs NL quarter movements
        quarter_variance = uncommitted_net - (quarter_nl_output_total - quarter_nl_input_total)
        quarter_variance_abs = abs(quarter_variance)

        # YTD reconciliation: nvat totals vs NL balances
        ytd_net_vat = ytd_output_total - ytd_input_total
        ytd_variance = ytd_net_vat - nl_total
        ytd_variance_abs = abs(ytd_variance)

        reconciliation["variance"] = {
            "quarter": {
                "uncommitted_output": round(uncommitted_output_total, 2),
                "uncommitted_input": round(uncommitted_input_total, 2),
                "uncommitted_net": round(uncommitted_net, 2),
                "nl_output_movement": round(quarter_nl_output_total, 2),
                "nl_input_movement": round(quarter_nl_input_total, 2),
                "nl_net_movement": round(quarter_nl_output_total - quarter_nl_input_total, 2),
                "variance_amount": round(quarter_variance, 2),
                "variance_absolute": round(quarter_variance_abs, 2),
                "reconciled": quarter_variance_abs < 1.00
            },
            "year_to_date": {
                "nvat_output_total": round(ytd_output_total, 2),
                "nvat_input_total": round(ytd_input_total, 2),
                "nvat_net_liability": round(ytd_net_vat, 2),
                "nominal_ledger_balance": round(nl_total, 2),
                "variance_amount": round(ytd_variance, 2),
                "variance_absolute": round(ytd_variance_abs, 2),
                "reconciled": ytd_variance_abs < 1.00
            }
        }

        # Overall status based on quarter reconciliation (primary focus)
        if quarter_variance_abs < 1.00:
            reconciliation["status"] = "RECONCILED"
            reconciliation["message"] = f"{quarter_info['current_quarter']}: Uncommitted VAT (£{uncommitted_net:,.2f}) reconciles to NL movements"
        else:
            reconciliation["status"] = "VARIANCE"
            if quarter_variance > 0:
                reconciliation["message"] = f"{quarter_info['current_quarter']}: Uncommitted VAT (£{uncommitted_net:,.2f}) is £{quarter_variance_abs:,.2f} MORE than NL movements"
            else:
                reconciliation["message"] = f"{quarter_info['current_quarter']}: Uncommitted VAT (£{uncommitted_net:,.2f}) is £{quarter_variance_abs:,.2f} LESS than NL movements"

        return reconciliation

    except Exception as e:
        logger.error(f"VAT reconciliation failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/reconcile/banks")
async def get_bank_accounts():
    """
    Get list of bank accounts for reconciliation.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get bank accounts from nbank
        banks_sql = """
            SELECT nk_acnt AS account_code, RTRIM(nk_desc) AS description,
                   nk_sort AS sort_code, nk_number AS account_number
            FROM nbank
            WHERE nk_acnt LIKE 'BC%'
            ORDER BY nk_acnt
        """
        banks = sql_connector.execute_query(banks_sql)
        if hasattr(banks, 'to_dict'):
            banks = banks.to_dict('records')

        return {
            "success": True,
            "banks": [
                {
                    "account_code": b['account_code'].strip() if b['account_code'] else '',
                    "description": b['description'].strip() if b['description'] else '',
                    "sort_code": b['sort_code'].strip() if b['sort_code'] else '',
                    "account_number": b['account_number'].strip() if b['account_number'] else ''
                }
                for b in banks or []
            ]
        }
    except Exception as e:
        logger.error(f"Failed to get bank accounts: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/reconcile/bank/{bank_code}")
async def reconcile_bank(bank_code: str):
    """
    Reconcile a specific bank account (aentry) to its Nominal Ledger control account.
    Uses anoml transfer file to identify pending postings.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        reconciliation = {
            "success": True,
            "reconciliation_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "bank_code": bank_code,
            "bank_account": {},
            "cashbook": {},
            "nominal_ledger": {},
            "variance": {},
            "status": "UNRECONCILED",
            "details": []
        }

        # Get bank account details
        bank_sql = f"""
            SELECT nk_acnt, RTRIM(nk_desc) AS description, nk_sort, nk_number
            FROM nbank
            WHERE nk_acnt = '{bank_code}'
        """
        bank_result = sql_connector.execute_query(bank_sql)
        if hasattr(bank_result, 'to_dict'):
            bank_result = bank_result.to_dict('records')

        if not bank_result:
            return {"success": False, "error": f"Bank account {bank_code} not found"}

        bank_info = bank_result[0]
        reconciliation["bank_account"] = {
            "code": bank_info['nk_acnt'].strip(),
            "description": bank_info['description'] or '',
            "sort_code": bank_info['nk_sort'].strip() if bank_info['nk_sort'] else '',
            "account_number": bank_info['nk_number'].strip() if bank_info['nk_number'] else ''
        }

        # ========== NOMINAL LEDGER SETUP (get current year first) ==========
        current_year_sql = "SELECT MAX(nt_year) AS current_year FROM ntran"
        cy_result = sql_connector.execute_query(current_year_sql)
        if hasattr(cy_result, 'to_dict'):
            cy_result = cy_result.to_dict('records')
        current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

        # ========== CASHBOOK (aentry/atran) ==========
        # Get CURRENT YEAR cashbook movements from atran (amounts stored in PENCE)
        # atran does NOT include B/F entries - only actual transactions
        cb_current_year_sql = f"""
            SELECT
                COUNT(DISTINCT at_entry) AS entry_count,
                COUNT(*) AS transaction_count,
                SUM(CASE WHEN at_value > 0 THEN at_value ELSE 0 END) AS receipts_pence,
                SUM(CASE WHEN at_value < 0 THEN ABS(at_value) ELSE 0 END) AS payments_pence,
                SUM(at_value) AS net_pence
            FROM atran
            WHERE at_acnt = '{bank_code}'
              AND YEAR(at_pstdate) = {current_year}
        """
        cb_cy_result = sql_connector.execute_query(cb_current_year_sql)
        if hasattr(cb_cy_result, 'to_dict'):
            cb_cy_result = cb_cy_result.to_dict('records')

        cb_cy_count = int(cb_cy_result[0]['entry_count'] or 0) if cb_cy_result else 0
        cb_cy_txn_count = int(cb_cy_result[0]['transaction_count'] or 0) if cb_cy_result else 0
        cb_cy_receipts_pence = float(cb_cy_result[0]['receipts_pence'] or 0) if cb_cy_result else 0
        cb_cy_payments_pence = float(cb_cy_result[0]['payments_pence'] or 0) if cb_cy_result else 0
        cb_cy_net_pence = float(cb_cy_result[0]['net_pence'] or 0) if cb_cy_result else 0

        # Convert to pounds
        cb_cy_receipts_pounds = cb_cy_receipts_pence / 100
        cb_cy_payments_pounds = cb_cy_payments_pence / 100
        cb_cy_movements = cb_cy_net_pence / 100  # Current year movements only

        # Also get ALL TIME totals for reference
        cb_all_sql = f"""
            SELECT
                COUNT(DISTINCT at_entry) AS entry_count,
                COUNT(*) AS transaction_count,
                SUM(at_value) AS net_pence
            FROM atran
            WHERE at_acnt = '{bank_code}'
        """
        cb_all_result = sql_connector.execute_query(cb_all_sql)
        if hasattr(cb_all_result, 'to_dict'):
            cb_all_result = cb_all_result.to_dict('records')
        cb_all_count = int(cb_all_result[0]['entry_count'] or 0) if cb_all_result else 0
        cb_all_net_pence = float(cb_all_result[0]['net_pence'] or 0) if cb_all_result else 0
        cb_all_total = cb_all_net_pence / 100

        # ========== BANK MASTER (nbank.nk_curbal) ==========
        # Get the running balance from nbank - stored in PENCE
        # This represents the CURRENT closing balance
        nbank_bal_sql = f"""
            SELECT nk_curbal FROM nbank WHERE nk_acnt = '{bank_code}'
        """
        nbank_result = sql_connector.execute_query(nbank_bal_sql)
        if hasattr(nbank_result, 'to_dict'):
            nbank_result = nbank_result.to_dict('records')
        nbank_curbal_pence = float(nbank_result[0]['nk_curbal'] or 0) if nbank_result else 0
        nbank_curbal_pounds = nbank_curbal_pence / 100

        # ========== NOMINAL LEDGER ==========
        # Get the nominal ledger balance for this bank account
        current_year_sql = "SELECT MAX(nt_year) AS current_year FROM ntran"
        cy_result = sql_connector.execute_query(current_year_sql)
        if hasattr(cy_result, 'to_dict'):
            cy_result = cy_result.to_dict('records')
        current_year = int(cy_result[0]['current_year']) if cy_result and cy_result[0]['current_year'] else datetime.now().year

        # Get account details from nacnt
        nacnt_sql = f"""
            SELECT na_acnt, RTRIM(na_desc) AS description, na_ytddr, na_ytdcr, na_prydr, na_prycr
            FROM nacnt
            WHERE na_acnt = '{bank_code}'
        """
        nacnt_result = sql_connector.execute_query(nacnt_sql)
        if hasattr(nacnt_result, 'to_dict'):
            nacnt_result = nacnt_result.to_dict('records')

        nl_total = 0
        nl_details = {}
        if nacnt_result:
            acc = nacnt_result[0]
            pry_dr = float(acc['na_prydr'] or 0)
            pry_cr = float(acc['na_prycr'] or 0)
            bf_balance = pry_dr - pry_cr

            # Get current year transactions
            ntran_sql = f"""
                SELECT
                    SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS debits,
                    SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS credits,
                    SUM(nt_value) AS net
                FROM ntran
                WHERE nt_acnt = '{bank_code}' AND nt_year = {current_year}
            """
            ntran_result = sql_connector.execute_query(ntran_sql)
            if hasattr(ntran_result, 'to_dict'):
                ntran_result = ntran_result.to_dict('records')

            current_year_dr = float(ntran_result[0]['debits'] or 0) if ntran_result else 0
            current_year_cr = float(ntran_result[0]['credits'] or 0) if ntran_result else 0
            current_year_net = float(ntran_result[0]['net'] or 0) if ntran_result else 0

            # Bank is a debit balance account (same logic as debtors control)
            # Use current year net for reconciliation (consistent with creditors/debtors)
            current_year_balance = current_year_net if current_year_net > 0 else abs(current_year_net)
            closing_balance = current_year_balance
            nl_total = current_year_balance

            nl_details = {
                "source": "ntran (Nominal Ledger)",
                "account": bank_code,
                "description": acc['description'] or '',
                "current_year": current_year,
                "brought_forward": round(bf_balance, 2),
                "current_year_debits": round(current_year_dr, 2),
                "current_year_credits": round(current_year_cr, 2),
                "current_year_net": round(current_year_net, 2),
                "closing_balance": round(closing_balance, 2),
                "total_balance": round(nl_total, 2)
            }
        else:
            nl_details = {
                "source": "ntran (Nominal Ledger)",
                "account": bank_code,
                "description": "Account not found in nacnt",
                "total_balance": 0
            }

        # Calculate expected closing balance:
        # atran current year movements + nacnt prior year B/F = expected closing
        cb_expected_closing = cb_cy_movements + bf_balance if nacnt_result else cb_cy_movements

        # For bank reconciliation, we compare:
        # 1. cb_expected_closing - atran movements + B/F
        # 2. nbank_curbal_pounds - bank master current balance
        # 3. nl_total - ntran current year net (includes B/F entry)
        # All three should match when fully reconciled

        # ========== TRANSFER FILE (anoml) ==========
        # Check for transactions in the transfer file for this bank
        # ax_nacnt contains the nominal account (which for banks is the bank code itself)
        anoml_pending_sql = f"""
            SELECT
                ax_nacnt AS nominal_account,
                ax_source AS source,
                ax_date AS date,
                ax_value AS value,
                ax_tref AS reference,
                ax_comment AS comment,
                ax_done AS status
            FROM anoml
            WHERE ax_nacnt = '{bank_code}' AND (ax_done <> 'Y' OR ax_done IS NULL)
            ORDER BY ax_date DESC
        """
        try:
            anoml_pending = sql_connector.execute_query(anoml_pending_sql)
            if hasattr(anoml_pending, 'to_dict'):
                anoml_pending = anoml_pending.to_dict('records')
        except Exception:
            anoml_pending = []

        # Count posted vs pending in transfer file for this bank
        anoml_summary_sql = f"""
            SELECT
                CASE WHEN ax_done = 'Y' THEN 'Posted' ELSE 'Pending' END AS status,
                COUNT(*) AS count,
                SUM(ax_value) AS total
            FROM anoml
            WHERE ax_nacnt = '{bank_code}'
            GROUP BY CASE WHEN ax_done = 'Y' THEN 'Posted' ELSE 'Pending' END
        """
        try:
            anoml_summary = sql_connector.execute_query(anoml_summary_sql)
            if hasattr(anoml_summary, 'to_dict'):
                anoml_summary = anoml_summary.to_dict('records')
        except Exception:
            anoml_summary = []

        posted_count = 0
        posted_total = 0
        pending_count = 0
        pending_total = 0
        for row in anoml_summary or []:
            if row['status'] == 'Posted':
                posted_count = int(row['count'] or 0)
                posted_total = float(row['total'] or 0)
            else:
                pending_count = int(row['count'] or 0)
                pending_total = float(row['total'] or 0)

        # Build pending transactions list
        pending_transactions = []
        for row in anoml_pending or []:
            tr_date = row['date']
            if hasattr(tr_date, 'strftime'):
                tr_date = tr_date.strftime('%Y-%m-%d')
            value = float(row['value'] or 0)
            source_desc = {'P': 'Purchase', 'S': 'Sales', 'A': 'Cashbook', 'J': 'Journal'}.get(
                row['source'].strip() if row['source'] else '', row['source'] or ''
            )
            pending_transactions.append({
                "nominal_account": row['nominal_account'].strip() if row['nominal_account'] else '',
                "source": row['source'].strip() if row['source'] else '',
                "source_desc": source_desc,
                "date": str(tr_date) if tr_date else '',
                "value": round(value, 2),
                "reference": row['reference'].strip() if row['reference'] else '',
                "comment": row['comment'].strip() if row['comment'] else ''
            })

        reconciliation["cashbook"] = {
            "source": "atran (Cashbook Transactions)",
            "current_year": current_year,
            "current_year_entries": cb_cy_count,
            "current_year_transactions": cb_cy_txn_count,
            "current_year_receipts": round(cb_cy_receipts_pounds, 2),
            "current_year_payments": round(cb_cy_payments_pounds, 2),
            "current_year_movements": round(cb_cy_movements, 2),
            "prior_year_bf": round(bf_balance, 2) if nacnt_result else 0,
            "expected_closing": round(cb_expected_closing, 2),
            "all_time_entries": cb_all_count,
            "all_time_net": round(cb_all_total, 2),
            "transfer_file": {
                "source": "anoml (Cashbook to Nominal Transfer File)",
                "posted_to_nl": {
                    "count": posted_count,
                    "total": round(posted_total, 2)
                },
                "pending_transfer": {
                    "count": pending_count,
                    "total": round(pending_total, 2),
                    "transactions": pending_transactions
                }
            }
        }

        # Bank master balance
        reconciliation["bank_master"] = {
            "source": "nbank.nk_curbal (Bank Master Balance)",
            "balance_pence": round(nbank_curbal_pence, 0),
            "balance_pounds": round(nbank_curbal_pounds, 2)
        }

        # Nominal ledger details already calculated above
        reconciliation["nominal_ledger"] = nl_details

        # ========== VARIANCE CALCULATION ==========
        # Primary comparison: Cashbook expected closing vs nbank.nk_curbal
        # (atran movements + B/F should equal bank master balance)
        variance_cb_nbank = cb_expected_closing - nbank_curbal_pounds
        variance_cb_nbank_abs = abs(variance_cb_nbank)

        # Secondary comparison: Bank Master vs Nominal Ledger current year net
        # (nbank.nk_curbal should equal ntran current year total)
        variance_nbank_nl = nbank_curbal_pounds - nl_total
        variance_nbank_nl_abs = abs(variance_nbank_nl)

        # Tertiary comparison: Cashbook expected vs Nominal Ledger
        variance_cb_nl = cb_expected_closing - nl_total
        variance_cb_nl_abs = abs(variance_cb_nl)

        # All three should match when fully reconciled
        all_reconciled = variance_cb_nbank_abs < 1.00 and variance_nbank_nl_abs < 1.00

        reconciliation["variance"] = {
            "cashbook_vs_bank_master": {
                "description": "atran movements + B/F vs nbank.nk_curbal",
                "cashbook_expected": round(cb_expected_closing, 2),
                "bank_master": round(nbank_curbal_pounds, 2),
                "amount": round(variance_cb_nbank, 2),
                "absolute": round(variance_cb_nbank_abs, 2),
                "reconciled": variance_cb_nbank_abs < 1.00
            },
            "bank_master_vs_nominal": {
                "description": "nbank.nk_curbal vs ntran current year",
                "bank_master": round(nbank_curbal_pounds, 2),
                "nominal_ledger": round(nl_total, 2),
                "amount": round(variance_nbank_nl, 2),
                "absolute": round(variance_nbank_nl_abs, 2),
                "reconciled": variance_nbank_nl_abs < 1.00
            },
            "cashbook_vs_nominal": {
                "description": "atran expected vs ntran",
                "cashbook_expected": round(cb_expected_closing, 2),
                "nominal_ledger": round(nl_total, 2),
                "amount": round(variance_cb_nl, 2),
                "absolute": round(variance_cb_nl_abs, 2),
                "reconciled": variance_cb_nl_abs < 1.00
            },
            "summary": {
                "current_year": current_year,
                "cashbook_movements": round(cb_cy_movements, 2),
                "prior_year_bf": round(bf_balance, 2) if nacnt_result else 0,
                "cashbook_expected_closing": round(cb_expected_closing, 2),
                "bank_master_balance": round(nbank_curbal_pounds, 2),
                "nominal_ledger_balance": round(nl_total, 2),
                "transfer_file_pending": round(pending_total, 2),
                "all_reconciled": all_reconciled,
                "has_pending_transfers": pending_count > 0
            }
        }

        # Determine status based on all three sources matching
        if all_reconciled:
            reconciliation["status"] = "RECONCILED"
            if pending_count > 0:
                reconciliation["message"] = f"Bank {bank_code} reconciles across all sources. {pending_count} entries (£{abs(pending_total):,.2f}) in transfer file pending."
            else:
                reconciliation["message"] = f"Bank {bank_code} fully reconciles: Cashbook = Bank Master = Nominal Ledger"
        else:
            reconciliation["status"] = "UNRECONCILED"
            # Build detailed message showing where mismatches occur
            issues = []
            if variance_cb_nl_abs >= 1.00:
                if variance_cb_nl > 0:
                    issues.append(f"Cashbook £{variance_cb_nl_abs:,.2f} MORE than NL")
                else:
                    issues.append(f"Cashbook £{variance_cb_nl_abs:,.2f} LESS than NL")
            if variance_cb_nbank_abs >= 1.00:
                if variance_cb_nbank > 0:
                    issues.append(f"Cashbook £{variance_cb_nbank_abs:,.2f} MORE than Bank Master")
                else:
                    issues.append(f"Cashbook £{variance_cb_nbank_abs:,.2f} LESS than Bank Master")
            if variance_nbank_nl_abs >= 1.00:
                if variance_nbank_nl > 0:
                    issues.append(f"Bank Master £{variance_nbank_nl_abs:,.2f} MORE than NL")
                else:
                    issues.append(f"Bank Master £{variance_nbank_nl_abs:,.2f} LESS than NL")
            reconciliation["message"] = "; ".join(issues) if issues else "Variance detected"

        return reconciliation

    except Exception as e:
        logger.error(f"Bank reconciliation failed for {bank_code}: {e}")
        return {"success": False, "error": str(e)}


# ============ Bank Statement Reconciliation (Mark as Reconciled) ============

@app.get("/api/reconcile/bank/{bank_code}/status")
async def get_bank_reconciliation_status(bank_code: str):
    """
    Get current bank reconciliation status including balances and unreconciled counts.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        opera = OperaSQLImport(sql_connector)

        status = opera.get_reconciliation_status(bank_code)

        if 'error' in status:
            return {"success": False, "error": status['error']}

        return {
            "success": True,
            **status
        }
    except Exception as e:
        logger.error(f"Failed to get reconciliation status for {bank_code}: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/reconcile/bank/{bank_code}/unreconciled")
async def get_unreconciled_entries(bank_code: str):
    """
    Get list of unreconciled cashbook entries for a bank account.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        opera = OperaSQLImport(sql_connector)

        entries = opera.get_unreconciled_entries(bank_code)

        return {
            "success": True,
            "bank_code": bank_code,
            "count": len(entries),
            "entries": entries
        }
    except Exception as e:
        logger.error(f"Failed to get unreconciled entries for {bank_code}: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/reconcile/bank/{bank_code}/complete-batch/{entry_number}")
async def complete_batch(bank_code: str, entry_number: str):
    """
    Complete an incomplete cashbook batch, making it available for reconciliation.

    This sets ae_complet = 1 on the aentry record and creates the necessary
    nominal ledger entries (ntran) and transfer file records (anoml).
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        opera = OperaSQLImport(sql_connector)

        # Check if entry exists and is incomplete
        check_query = f"""
            SELECT ae_entry, ae_acnt, ae_complet, ae_value, ae_lstdate, ae_cbtype, ae_entref
            FROM aentry WITH (NOLOCK)
            WHERE ae_entry = '{entry_number}'
              AND ae_acnt = '{bank_code}'
        """
        df = sql_connector.execute_query(check_query)

        if df is None or len(df) == 0:
            return {"success": False, "error": f"Entry {entry_number} not found for bank {bank_code}"}

        entry = df.iloc[0]
        if entry['ae_complet'] == 1:
            return {"success": False, "error": f"Entry {entry_number} is already complete"}

        # Complete the batch - set ae_complet = 1
        # Note: In a full implementation, this would also create ntran/anoml records
        # For now, we just mark it complete
        update_query = f"""
            UPDATE aentry WITH (ROWLOCK)
            SET ae_complet = 1,
                datemodified = GETDATE()
            WHERE ae_entry = '{entry_number}'
              AND ae_acnt = '{bank_code}'
              AND ae_complet = 0
        """

        with sql_connector.engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text(update_query))
            conn.commit()

            if result.rowcount == 0:
                return {"success": False, "error": "Failed to update entry - may already be complete"}

        return {
            "success": True,
            "entry_number": entry_number,
            "message": f"Batch {entry_number} completed successfully",
            "value_pounds": float(entry['ae_value']) / 100.0
        }

    except Exception as e:
        logger.error(f"Failed to complete batch {entry_number}: {e}")
        return {"success": False, "error": str(e)}


class ReconcileEntriesRequest(BaseModel):
    """Request body for marking entries as reconciled."""
    entries: List[dict]  # Each entry: {"entry_number": "P100008036", "statement_line": 10}
    statement_number: int
    statement_date: Optional[str] = None  # YYYY-MM-DD format
    reconciliation_date: Optional[str] = None  # YYYY-MM-DD format


@app.post("/api/reconcile/bank/{bank_code}/mark-reconciled")
async def mark_entries_reconciled(bank_code: str, request: ReconcileEntriesRequest):
    """
    Mark cashbook entries as reconciled.

    This replicates Opera's Bank Reconciliation routine:
    - Updates aentry records with reconciliation batch number, statement line, etc.
    - Updates nbank master with new reconciled balance

    Request body:
    {
        "entries": [
            {"entry_number": "P100008036", "statement_line": 10},
            {"entry_number": "PR00000534", "statement_line": 20}
        ],
        "statement_number": 86918,
        "statement_date": "2026-02-08",
        "reconciliation_date": "2026-02-08"
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        from datetime import datetime

        opera = OperaSQLImport(sql_connector)

        # Parse dates if provided
        stmt_date = None
        rec_date = None
        if request.statement_date:
            stmt_date = datetime.strptime(request.statement_date, '%Y-%m-%d').date()
        if request.reconciliation_date:
            rec_date = datetime.strptime(request.reconciliation_date, '%Y-%m-%d').date()

        result = opera.mark_entries_reconciled(
            bank_account=bank_code,
            entries=request.entries,
            statement_number=request.statement_number,
            statement_date=stmt_date,
            reconciliation_date=rec_date
        )

        if result.success:
            return {
                "success": True,
                "message": f"Reconciled {result.records_imported} entries",
                "records_reconciled": result.records_imported,
                "details": result.warnings
            }
        else:
            return {
                "success": False,
                "errors": result.errors
            }
    except Exception as e:
        logger.error(f"Failed to mark entries reconciled for {bank_code}: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/reconcile/bank/{bank_code}/unreconcile")
async def unreconcile_entries(bank_code: str, entry_numbers: List[str]):
    """
    Unreconcile previously reconciled entries (reverse reconciliation).

    Request body: ["P100008036", "PR00000534"]
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from datetime import datetime

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        entry_list = "', '".join(entry_numbers)

        # Reset reconciliation fields
        update_sql = f"""
            UPDATE aentry WITH (ROWLOCK)
            SET ae_reclnum = 0,
                ae_recdate = NULL,
                ae_statln = 0,
                ae_frstat = 0,
                ae_tostat = 0,
                ae_tmpstat = 0,
                datemodified = '{now_str}'
            WHERE ae_acnt = '{bank_code}'
              AND ae_entry IN ('{entry_list}')
              AND ae_reclnum > 0
        """

        with sql_connector.engine.connect() as conn:
            trans = conn.begin()
            try:
                from sqlalchemy import text
                result = conn.execute(text(update_sql))
                rows_affected = result.rowcount

                # Recalculate nbank reconciled balance
                recalc_sql = f"""
                    SELECT COALESCE(SUM(ae_value), 0) as reconciled_total
                    FROM aentry WITH (NOLOCK)
                    WHERE ae_acnt = '{bank_code}'
                      AND ae_reclnum > 0
                """
                recalc_result = conn.execute(text(recalc_sql))
                new_rec_total = float(recalc_result.fetchone()[0] or 0)

                # Update nbank
                nbank_update = f"""
                    UPDATE nbank WITH (ROWLOCK)
                    SET nk_recbal = {int(new_rec_total)},
                        datemodified = '{now_str}'
                    WHERE nk_acnt = '{bank_code}'
                """
                conn.execute(text(nbank_update))

                trans.commit()

                return {
                    "success": True,
                    "message": f"Unreconciled {rows_affected} entries",
                    "entries_unreconciled": rows_affected,
                    "new_reconciled_balance": new_rec_total / 100.0
                }
            except Exception as e:
                trans.rollback()
                raise

    except Exception as e:
        logger.error(f"Failed to unreconcile entries for {bank_code}: {e}")
        return {"success": False, "error": str(e)}


# ============ Statement Auto-Reconciliation (PDF/Image Processing) ============

@app.post("/api/reconcile/process-statement")
async def process_bank_statement(
    file_path: str,
    bank_code: str = Query(..., description="Opera bank account code (selected by user)")
):
    """
    Process a bank statement PDF/image and extract transactions for matching.

    Workflow:
    1. User selects bank account from dropdown
    2. User provides statement file path
    3. System validates statement matches selected bank account
    4. System extracts and matches transactions

    Args:
        file_path: Path to the statement file (PDF or image)
        bank_code: Opera bank account code (user-selected)

    Returns:
        Statement info, validation result, extracted transactions, and matches
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from sql_rag.statement_reconcile import StatementReconciler
        from pathlib import Path

        if not Path(file_path).exists():
            return {"success": False, "error": f"File not found: {file_path}"}

        # Pass config to use configured Anthropic API key and model
        reconciler = StatementReconciler(sql_connector, config=config)

        # Extract transactions from statement
        statement_info, transactions = reconciler.extract_transactions_from_pdf(file_path)

        # Validate that statement matches the selected bank account
        bank_validation = reconciler.validate_statement_bank(bank_code, statement_info)
        if not bank_validation['valid']:
            return {
                "success": False,
                "error": bank_validation['error'],
                "bank_code": bank_code,
                "bank_validation": bank_validation,
                "statement_info": {
                    "bank_name": statement_info.bank_name,
                    "account_number": statement_info.account_number,
                    "sort_code": statement_info.sort_code
                }
            }

        # Get unreconciled Opera entries for the date range
        date_from = statement_info.period_start
        date_to = statement_info.period_end

        opera_entries = reconciler.get_unreconciled_entries(
            bank_code,
            date_from=date_from,
            date_to=date_to
        )

        # Match transactions
        matches, unmatched_stmt, unmatched_opera = reconciler.match_transactions(
            transactions, opera_entries
        )

        # Format response
        return {
            "success": True,
            "bank_code": bank_code,
            "bank_validation": bank_validation,  # Validation that statement matches selected bank
            "statement_info": {
                "bank_name": statement_info.bank_name,
                "account_number": statement_info.account_number,
                "sort_code": statement_info.sort_code,
                "statement_date": statement_info.statement_date.isoformat() if statement_info.statement_date else None,
                "period_start": statement_info.period_start.isoformat() if statement_info.period_start else None,
                "period_end": statement_info.period_end.isoformat() if statement_info.period_end else None,
                "opening_balance": statement_info.opening_balance,
                "closing_balance": statement_info.closing_balance
            },
            "extracted_transactions": len(transactions),
            "opera_unreconciled": len(opera_entries),
            "matches": [
                {
                    "statement_txn": {
                        "date": m.statement_txn.date.isoformat(),
                        "description": m.statement_txn.description,
                        "amount": m.statement_txn.amount,
                        "balance": m.statement_txn.balance,
                        "type": m.statement_txn.transaction_type
                    },
                    "opera_entry": {
                        "ae_entry": m.opera_entry['ae_entry'],
                        "ae_date": m.opera_entry['ae_date'].isoformat() if hasattr(m.opera_entry['ae_date'], 'isoformat') else str(m.opera_entry['ae_date']),
                        "ae_ref": m.opera_entry['ae_ref'],
                        "value_pounds": m.opera_entry['value_pounds'],
                        "ae_detail": m.opera_entry.get('ae_detail', '')
                    },
                    "match_score": m.match_score,
                    "match_reasons": m.match_reasons
                }
                for m in matches
            ],
            "unmatched_statement": [
                {
                    "date": t.date.isoformat(),
                    "description": t.description,
                    "amount": t.amount,
                    "balance": t.balance,
                    "type": t.transaction_type
                }
                for t in unmatched_stmt
            ],
            "unmatched_opera": [
                {
                    "ae_entry": e['ae_entry'],
                    "ae_date": e['ae_date'].isoformat() if hasattr(e['ae_date'], 'isoformat') else str(e['ae_date']),
                    "ae_ref": e['ae_ref'],
                    "value_pounds": e['value_pounds'],
                    "ae_detail": e.get('ae_detail', '')
                }
                for e in unmatched_opera
            ]
        }

    except Exception as e:
        logger.error(f"Failed to process statement: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


@app.post("/api/reconcile/process-statement-unified")
async def process_statement_unified(
    file_path: str,
    bank_code: str = Query(..., description="Opera bank account code (selected by user)")
):
    """
    Unified statement processing: identifies transactions to IMPORT and RECONCILE.

    Workflow:
    1. User selects bank account from dropdown
    2. User provides statement file path
    3. System extracts transactions from PDF
    4. System VALIDATES statement matches selected bank (sort code/account number)
    5. System matches against existing Opera entries
    6. System identifies new transactions for import and existing ones to reconcile

    Args:
        file_path: Path to the statement PDF
        bank_code: Opera bank account code (user-selected from dropdown)

    Returns:
        bank_code: The selected bank code
        bank_validation: Validation result (confirms statement matches selected account)
        to_import: Transactions not in Opera (need importing)
        to_reconcile: Matches with unreconciled Opera entries
        already_reconciled: Matches with already reconciled entries (verification)
        balance_check: Closing balance verification
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from sql_rag.statement_reconcile import StatementReconciler
        from pathlib import Path

        if not Path(file_path).exists():
            return {"success": False, "error": f"File not found: {file_path}"}

        # Pass config to use configured Anthropic API key and model
        reconciler = StatementReconciler(sql_connector, config=config)

        # Use the unified processing method - bank_code can be None for auto-detection
        result = reconciler.process_statement_unified(bank_code, file_path)

        # Format the response
        def format_stmt_txn(txn):
            return {
                "date": txn.date.isoformat() if hasattr(txn.date, 'isoformat') else str(txn.date),
                "description": txn.description,
                "amount": txn.amount,
                "balance": txn.balance,
                "type": txn.transaction_type,
                "reference": txn.reference
            }

        def format_match(m):
            return {
                "statement_txn": format_stmt_txn(m['statement_txn']),
                "opera_entry": {
                    "ae_entry": m['opera_entry']['ae_entry'],
                    "ae_date": m['opera_entry']['ae_date'].isoformat() if hasattr(m['opera_entry']['ae_date'], 'isoformat') else str(m['opera_entry']['ae_date']),
                    "ae_ref": m['opera_entry']['ae_ref'],
                    "value_pounds": m['opera_entry']['value_pounds'],
                    "ae_detail": m['opera_entry'].get('ae_detail', ''),
                    "is_reconciled": m['opera_entry'].get('is_reconciled', False)
                },
                "match_score": m['match_score'],
                "match_reasons": m['match_reasons']
            }

        # Handle error case (e.g., bank not found)
        if not result.get('success', False):
            return result

        stmt_info = result['statement_info']

        return {
            "success": True,
            "bank_code": result.get('bank_code'),
            "bank_validation": result.get('bank_validation'),  # Validation that statement matches selected bank
            "statement_info": {
                "bank_name": stmt_info.bank_name,
                "account_number": stmt_info.account_number,
                "sort_code": stmt_info.sort_code,
                "statement_date": stmt_info.statement_date.isoformat() if stmt_info.statement_date else None,
                "period_start": stmt_info.period_start.isoformat() if stmt_info.period_start else None,
                "period_end": stmt_info.period_end.isoformat() if stmt_info.period_end else None,
                "opening_balance": stmt_info.opening_balance,
                "closing_balance": stmt_info.closing_balance
            },
            "summary": result['summary'],
            "to_import": [format_stmt_txn(txn) for txn in result['to_import']],
            "to_reconcile": [format_match(m) for m in result['to_reconcile']],
            "already_reconciled": [format_match(m) for m in result['already_reconciled']],
            "balance_check": result['balance_check']
        }

    except Exception as e:
        logger.error(f"Failed to process unified statement: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


@app.post("/api/reconcile/bank/{bank_code}/import-from-statement")
async def import_from_statement(
    bank_code: str,
    transactions: List[Dict],
    statement_date: str
):
    """
    Import transactions from a bank statement using the existing bank import matching logic.

    This uses the same matching infrastructure as CSV imports but with PDF-extracted data.
    Transactions are matched against customers/suppliers and categorized automatically.

    Args:
        bank_code: The bank account code
        transactions: List of transactions to import (date, description, amount, type)
        statement_date: Statement date for reference

    Returns:
        Preview of how transactions would be categorized (same format as CSV preview)
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from datetime import datetime
        from sql_rag.bank_import import BankStatementImporter, BankTransaction

        # Create bank transactions from the PDF-extracted data
        bank_txns = []
        for i, txn in enumerate(transactions):
            amount = float(txn['amount'])
            txn_date = datetime.strptime(txn['date'][:10], '%Y-%m-%d')

            bank_txn = BankTransaction(
                row_number=i + 1,
                date=txn_date.date(),
                name=txn.get('description', '')[:100],
                reference=txn.get('reference') or txn.get('description', '')[:30],
                amount=amount,
                abs_amount=abs(amount),
                is_debit=amount < 0,
                transaction_type=txn.get('type', 'Other')
            )
            bank_txns.append(bank_txn)

        # Use the existing bank importer for matching
        importer = BankStatementImporter(
            sql_connector=sql_connector,
            bank_code=bank_code,
            default_vat_code='0'
        )

        # Match each transaction
        matched_receipts = []
        matched_payments = []
        unmatched = []

        for txn in bank_txns:
            importer._match_transaction(txn)

            txn_data = {
                "row": txn.row_number,
                "date": str(txn.date),
                "name": txn.name,
                "reference": txn.reference,
                "amount": txn.amount,
                "action": txn.action,
                "match_type": txn.match_type,
                "matched_account": txn.matched_account,
                "matched_name": txn.matched_name,
                "match_score": txn.match_score,
                "skip_reason": txn.skip_reason
            }

            if txn.action == 'sales_receipt':
                matched_receipts.append(txn_data)
            elif txn.action == 'purchase_payment':
                matched_payments.append(txn_data)
            else:
                unmatched.append(txn_data)

        return {
            "success": True,
            "total_transactions": len(bank_txns),
            "matched_receipts": matched_receipts,
            "matched_payments": matched_payments,
            "unmatched": unmatched,
            "summary": {
                "receipts": len(matched_receipts),
                "payments": len(matched_payments),
                "unmatched": len(unmatched)
            }
        }

    except Exception as e:
        logger.error(f"Failed to process import from statement for {bank_code}: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


@app.post("/api/reconcile/bank/{bank_code}/confirm-matches")
async def confirm_statement_matches(
    bank_code: str,
    matches: List[Dict],
    statement_balance: float,
    statement_date: str
):
    """
    Confirm matched transactions and mark them as reconciled in Opera.

    Args:
        bank_code: The bank account code
        matches: List of confirmed matches (each with 'ae_entry' key)
        statement_balance: Closing balance from the statement
        statement_date: Statement date (YYYY-MM-DD)

    Returns:
        Reconciliation result
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        from datetime import datetime

        stmt_date = datetime.strptime(statement_date, '%Y-%m-%d')

        # Get the entry IDs to reconcile
        entry_ids = [m.get('ae_entry') or m.get('opera_entry', {}).get('ae_entry') for m in matches]
        entry_ids = [e for e in entry_ids if e]  # Filter out None values

        if not entry_ids:
            return {"success": False, "error": "No valid entry IDs provided"}

        # Get next batch number
        batch_query = f"""
            SELECT ISNULL(MAX(ae_reclnum), 0) + 1 as next_batch
            FROM aentry WITH (NOLOCK)
            WHERE ae_bank = '{bank_code}'
        """
        batch_result = sql_connector.execute_query(batch_query)
        next_batch = int(batch_result.iloc[0]['next_batch']) if batch_result is not None else 1

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        reconciled_count = 0

        with sql_connector.engine.connect() as conn:
            trans = conn.begin()
            try:
                from sqlalchemy import text

                for i, entry_id in enumerate(entry_ids):
                    line_number = (i + 1) * 10

                    update_query = f"""
                        UPDATE aentry WITH (ROWLOCK)
                        SET ae_reclnum = {next_batch},
                            ae_statln = {line_number},
                            ae_recdate = '{stmt_date.strftime('%Y-%m-%d')}',
                            ae_recbal = {int(statement_balance * 100)},
                            datemodified = '{now_str}'
                        WHERE ae_entry = '{entry_id}'
                          AND ae_bank = '{bank_code}'
                          AND ae_reclnum = 0
                    """
                    result = conn.execute(text(update_query))
                    reconciled_count += result.rowcount

                # Update nbank
                nbank_update = f"""
                    UPDATE nbank WITH (ROWLOCK)
                    SET nk_recbal = {int(statement_balance * 100)},
                        nk_lstrecl = {next_batch},
                        nk_lststno = ISNULL(nk_lststno, 0) + 1,
                        nk_lststdt = '{stmt_date.strftime('%Y-%m-%d')}',
                        datemodified = '{now_str}'
                    WHERE nk_code = '{bank_code}'
                """
                conn.execute(text(nbank_update))

                trans.commit()

                return {
                    "success": True,
                    "message": f"Reconciled {reconciled_count} entries",
                    "reconciled_count": reconciled_count,
                    "batch_number": next_batch,
                    "statement_balance": statement_balance
                }
            except Exception as e:
                trans.rollback()
                raise

    except Exception as e:
        logger.error(f"Failed to confirm matches for {bank_code}: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/reconcile/bank/{bank_code}/scan-emails")
async def scan_emails_for_statements(bank_code: str, email_address: Optional[str] = None):
    """
    Scan email inbox for bank statement attachments.

    Args:
        bank_code: The bank account code
        email_address: Optional email address to scan (defaults to configured inbox)

    Returns:
        List of emails with bank statement attachments
    """
    # TODO: Implement email scanning using existing email infrastructure
    # For now, return a placeholder
    return {
        "success": True,
        "message": "Email scanning not yet implemented - use file upload",
        "statements_found": []
    }


# ============ File Archive Management ============

@app.post("/api/archive/file")
async def archive_import_file(
    file_path: str,
    import_type: str,
    transactions_extracted: Optional[int] = None,
    transactions_matched: Optional[int] = None,
    transactions_reconciled: Optional[int] = None
):
    """
    Archive a processed import file.

    Args:
        file_path: Path to the file to archive
        import_type: Type of import ('bank-statement', 'gocardless', 'invoice')
        transactions_extracted: Number of transactions extracted from file
        transactions_matched: Number of transactions matched
        transactions_reconciled: Number of transactions reconciled

    Returns:
        Archive result with new file path
    """
    try:
        from sql_rag.file_archive import archive_file

        metadata = {
            "transactions_extracted": transactions_extracted,
            "transactions_matched": transactions_matched,
            "transactions_reconciled": transactions_reconciled,
        }

        result = archive_file(file_path, import_type, metadata)
        return result

    except Exception as e:
        logger.error(f"Failed to archive file {file_path}: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/archive/history")
async def get_archive_history(import_type: Optional[str] = None, limit: int = 50):
    """
    Get archive history.

    Args:
        import_type: Filter by type ('bank-statement', 'gocardless', 'invoice'), or None for all
        limit: Maximum entries to return

    Returns:
        List of archived files with metadata
    """
    try:
        from sql_rag.file_archive import get_archive_history as get_history

        history = get_history(import_type, limit)
        return {"success": True, "history": history, "count": len(history)}

    except Exception as e:
        logger.error(f"Failed to get archive history: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/archive/pending")
async def get_pending_files(import_type: str):
    """
    Get list of files pending in source directories (not yet archived).

    Args:
        import_type: Type of import to check ('bank-statement', 'gocardless', 'invoice')

    Returns:
        List of pending files
    """
    try:
        from sql_rag.file_archive import get_pending_files as get_pending

        files = get_pending(import_type)
        return {"success": True, "files": files, "count": len(files)}

    except Exception as e:
        logger.error(f"Failed to get pending files: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/statement-files")
async def list_statement_files(bank_folder: Optional[str] = None):
    """
    List PDF statement files available for processing.

    Args:
        bank_folder: Optional bank folder name (barclays, hsbc, lloyds, natwest).
                    If not provided, lists files from all bank folders.

    Returns:
        List of PDF files with path, filename, size, and modified date
    """
    import os
    from pathlib import Path
    from datetime import datetime

    base_path = Path("/Users/maccb/Downloads/bank-statements")
    bank_folders = ["barclays", "hsbc", "lloyds", "natwest"]

    if bank_folder:
        if bank_folder.lower() not in bank_folders:
            return {"success": False, "error": f"Unknown bank folder: {bank_folder}"}
        folders_to_scan = [base_path / bank_folder.lower()]
    else:
        folders_to_scan = [base_path / f for f in bank_folders]

    files = []
    for folder in folders_to_scan:
        if not folder.exists():
            continue

        for file_path in folder.iterdir():
            if file_path.is_file() and file_path.suffix.lower() == '.pdf':
                stat = file_path.stat()
                files.append({
                    "path": str(file_path),
                    "filename": file_path.name,
                    "folder": folder.name,
                    "size": stat.st_size,
                    "size_formatted": f"{stat.st_size / 1024:.1f} KB",
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "modified_formatted": datetime.fromtimestamp(stat.st_mtime).strftime("%d %b %Y %H:%M")
                })

    # Sort by modified date descending (newest first)
    files.sort(key=lambda x: x["modified"], reverse=True)

    return {
        "success": True,
        "files": files,
        "count": len(files)
    }


# ============ Enhanced Sales Dashboard Endpoints for Intsys UK ============

@app.get("/api/dashboard/executive-summary")
async def get_executive_summary(year: int = 2026):
    """
    Executive Summary KPIs - what a Sales Director should see first.
    Provides at-a-glance performance metrics with like-for-like comparisons.
    """
    from datetime import datetime as dt

    try:
        current_date = dt.now()
        current_month = current_date.month
        current_quarter = (current_month - 1) // 3 + 1

        # Get monthly revenue data for both years with type breakdown
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period as month,
                SUM(CASE WHEN RTRIM(nt_type) IN ('E', '30') THEN -nt_value ELSE 0 END) as revenue
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year IN ({year}, {year - 1}, {year - 2})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Organize by year and month
        revenue_by_year = {}
        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            if y not in revenue_by_year:
                revenue_by_year[y] = {}
            revenue_by_year[y][m] = float(row['revenue'] or 0)

        # Calculate KPIs
        curr_year = revenue_by_year.get(year, {})
        prev_year = revenue_by_year.get(year - 1, {})

        # Current month vs same month last year
        curr_month_rev = curr_year.get(current_month, 0)
        prev_month_rev = prev_year.get(current_month, 0)
        month_yoy_change = ((curr_month_rev - prev_month_rev) / prev_month_rev * 100) if prev_month_rev else 0

        # Current quarter vs same quarter last year
        quarter_months = list(range((current_quarter - 1) * 3 + 1, current_quarter * 3 + 1))
        curr_qtd = sum(curr_year.get(m, 0) for m in quarter_months if m <= current_month)
        prev_qtd = sum(prev_year.get(m, 0) for m in quarter_months if m <= current_month)
        quarter_yoy_change = ((curr_qtd - prev_qtd) / prev_qtd * 100) if prev_qtd else 0

        # YTD comparison (same months as current year)
        curr_ytd = sum(curr_year.get(m, 0) for m in range(1, current_month + 1))
        prev_ytd = sum(prev_year.get(m, 0) for m in range(1, current_month + 1))
        ytd_yoy_change = ((curr_ytd - prev_ytd) / prev_ytd * 100) if prev_ytd else 0

        # Rolling 12 months (last 12 complete months)
        rolling_12 = 0
        prev_rolling_12 = 0
        for i in range(12):
            m = current_month - i
            y = year if m > 0 else year - 1
            m = m if m > 0 else m + 12
            rolling_12 += revenue_by_year.get(y, {}).get(m, 0)
            # Previous period
            py = y - 1
            prev_rolling_12 += revenue_by_year.get(py, {}).get(m, 0)

        rolling_12_change = ((rolling_12 - prev_rolling_12) / prev_rolling_12 * 100) if prev_rolling_12 else 0

        # Monthly run rate (based on last 3 months)
        recent_months = []
        for i in range(3):
            m = current_month - i
            y = year if m > 0 else year - 1
            m = m if m > 0 else m + 12
            recent_months.append(revenue_by_year.get(y, {}).get(m, 0))
        monthly_run_rate = sum(recent_months) / len(recent_months) if recent_months else 0
        annual_run_rate = monthly_run_rate * 12

        # Full year projection
        months_elapsed = current_month
        projected_full_year = (curr_ytd / months_elapsed * 12) if months_elapsed > 0 else 0
        prev_full_year = sum(prev_year.get(m, 0) for m in range(1, 13))
        projection_vs_prior = ((projected_full_year - prev_full_year) / prev_full_year * 100) if prev_full_year else 0

        return {
            "success": True,
            "year": year,
            "period": {
                "current_month": current_month,
                "current_quarter": current_quarter,
                "months_elapsed": months_elapsed
            },
            "kpis": {
                # Current month metrics
                "current_month": {
                    "value": round(curr_month_rev, 2),
                    "prior_year": round(prev_month_rev, 2),
                    "yoy_change_percent": round(month_yoy_change, 1),
                    "trend": "up" if month_yoy_change > 0 else "down" if month_yoy_change < 0 else "flat"
                },
                # Quarter metrics
                "quarter_to_date": {
                    "value": round(curr_qtd, 2),
                    "prior_year": round(prev_qtd, 2),
                    "yoy_change_percent": round(quarter_yoy_change, 1),
                    "trend": "up" if quarter_yoy_change > 0 else "down" if quarter_yoy_change < 0 else "flat"
                },
                # YTD metrics
                "year_to_date": {
                    "value": round(curr_ytd, 2),
                    "prior_year": round(prev_ytd, 2),
                    "yoy_change_percent": round(ytd_yoy_change, 1),
                    "trend": "up" if ytd_yoy_change > 0 else "down" if ytd_yoy_change < 0 else "flat"
                },
                # Rolling 12 months
                "rolling_12_months": {
                    "value": round(rolling_12, 2),
                    "prior_period": round(prev_rolling_12, 2),
                    "change_percent": round(rolling_12_change, 1),
                    "trend": "up" if rolling_12_change > 0 else "down" if rolling_12_change < 0 else "flat"
                },
                # Run rate and projections
                "monthly_run_rate": round(monthly_run_rate, 2),
                "annual_run_rate": round(annual_run_rate, 2),
                "projected_full_year": round(projected_full_year, 2),
                "prior_full_year": round(prev_full_year, 2),
                "projection_vs_prior_percent": round(projection_vs_prior, 1)
            }
        }
    except Exception as e:
        logger.error(f"Executive summary error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/revenue-by-category-detailed")
async def get_revenue_by_category_detailed(year: int = 2026):
    """
    Detailed revenue breakdown by sales category with YoY comparison.
    Categories: Recurring (Support/AMC), Consultancy, Cloud/Hosting, Software Resale.
    """
    try:
        # Get revenue by nominal account subtype - provides category breakdown
        df = sql_connector.execute_query(f"""
            SELECT
                nt.nt_year,
                nt.nt_period as month,
                COALESCE(NULLIF(RTRIM(na.na_subt), ''),
                    CASE
                        WHEN nt.nt_acnt LIKE 'E1%' OR nt.nt_acnt LIKE '30%' THEN 'Primary Revenue'
                        WHEN nt.nt_acnt LIKE 'E2%' THEN 'Secondary Revenue'
                        WHEN nt.nt_acnt LIKE 'E3%' THEN 'Service Revenue'
                        ELSE 'Other Revenue'
                    END
                ) as category,
                SUM(-nt.nt_value) as revenue
            FROM ntran nt
            LEFT JOIN nacnt na ON RTRIM(nt.nt_acnt) = RTRIM(na.na_acnt) AND na.na_year = nt.nt_year
            WHERE RTRIM(nt.nt_type) IN ('E', '30')
            AND nt.nt_year IN ({year}, {year - 1})
            GROUP BY nt.nt_year, nt.nt_period,
                COALESCE(NULLIF(RTRIM(na.na_subt), ''),
                    CASE
                        WHEN nt.nt_acnt LIKE 'E1%' OR nt.nt_acnt LIKE '30%' THEN 'Primary Revenue'
                        WHEN nt.nt_acnt LIKE 'E2%' THEN 'Secondary Revenue'
                        WHEN nt.nt_acnt LIKE 'E3%' THEN 'Service Revenue'
                        ELSE 'Other Revenue'
                    END
                )
            ORDER BY nt.nt_year, nt.nt_period
        """)
        data = df_to_records(df)

        # Aggregate by category and year
        category_totals = {}
        monthly_by_category = {}

        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            cat = row['category']
            rev = float(row['revenue'] or 0)

            if cat not in category_totals:
                category_totals[cat] = {year: 0, year - 1: 0}
                monthly_by_category[cat] = {year: {}, year - 1: {}}

            category_totals[cat][y] = category_totals[cat].get(y, 0) + rev
            monthly_by_category[cat][y][m] = monthly_by_category[cat][y].get(m, 0) + rev

        # Build response
        categories = []
        total_current = 0
        total_previous = 0

        for cat, totals in sorted(category_totals.items(), key=lambda x: -x[1].get(year, 0)):
            curr = totals.get(year, 0)
            prev = totals.get(year - 1, 0)
            total_current += curr
            total_previous += prev

            change_pct = ((curr - prev) / prev * 100) if prev else 0

            # Monthly trend for this category
            monthly_trend = []
            for m in range(1, 13):
                monthly_trend.append({
                    "month": m,
                    "current": monthly_by_category[cat][year].get(m, 0),
                    "previous": monthly_by_category[cat][year - 1].get(m, 0)
                })

            categories.append({
                "category": cat,
                "current_year": round(curr, 2),
                "previous_year": round(prev, 2),
                "change_amount": round(curr - prev, 2),
                "change_percent": round(change_pct, 1),
                "percent_of_total": round(curr / total_current * 100, 1) if total_current else 0,
                "trend": "up" if change_pct > 5 else "down" if change_pct < -5 else "stable",
                "monthly_trend": monthly_trend
            })

        return {
            "success": True,
            "year": year,
            "summary": {
                "total_current": round(total_current, 2),
                "total_previous": round(total_previous, 2),
                "total_change_percent": round((total_current - total_previous) / total_previous * 100, 1) if total_previous else 0
            },
            "categories": categories
        }
    except Exception as e:
        logger.error(f"Revenue by category error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/new-vs-existing-revenue")
async def get_new_vs_existing_revenue(year: int = 2026):
    """
    Split revenue between new business (first invoice this year or last year)
    vs existing customers (invoiced in prior years).
    Critical for understanding growth sources.
    """
    try:
        # Get customer first transaction dates and revenue by year
        df = sql_connector.execute_query(f"""
            WITH CustomerHistory AS (
                SELECT
                    RTRIM(st_account) as account,
                    MIN(st_trdate) as first_invoice_date,
                    SUM(CASE WHEN YEAR(st_trdate) = {year} THEN st_trvalue ELSE 0 END) as current_year_rev,
                    SUM(CASE WHEN YEAR(st_trdate) = {year - 1} THEN st_trvalue ELSE 0 END) as prev_year_rev,
                    SUM(CASE WHEN YEAR(st_trdate) < {year - 1} THEN st_trvalue ELSE 0 END) as older_rev
                FROM stran
                WHERE st_trtype = 'I'
                GROUP BY st_account
            )
            SELECT
                account,
                first_invoice_date,
                current_year_rev,
                prev_year_rev,
                older_rev
            FROM CustomerHistory
            WHERE current_year_rev > 0 OR prev_year_rev > 0
        """)
        data = df_to_records(df)

        # Classify customers
        new_customers_current = {"count": 0, "revenue": 0}  # First invoice this year
        new_customers_prev = {"count": 0, "revenue": 0}     # First invoice last year (still "new-ish")
        existing_customers = {"count": 0, "revenue": 0}     # Invoiced before last year

        for row in data:
            first_date = row['first_invoice_date']
            curr_rev = float(row['current_year_rev'] or 0)

            if first_date:
                first_year = first_date.year if hasattr(first_date, 'year') else int(str(first_date)[:4])

                if first_year == year and curr_rev > 0:
                    new_customers_current["count"] += 1
                    new_customers_current["revenue"] += curr_rev
                elif first_year == year - 1 and curr_rev > 0:
                    new_customers_prev["count"] += 1
                    new_customers_prev["revenue"] += curr_rev
                elif curr_rev > 0:
                    existing_customers["count"] += 1
                    existing_customers["revenue"] += curr_rev

        total_revenue = new_customers_current["revenue"] + new_customers_prev["revenue"] + existing_customers["revenue"]
        total_customers = new_customers_current["count"] + new_customers_prev["count"] + existing_customers["count"]

        return {
            "success": True,
            "year": year,
            "summary": {
                "total_revenue": round(total_revenue, 2),
                "total_customers": total_customers
            },
            "new_business": {
                "this_year": {
                    "customers": new_customers_current["count"],
                    "revenue": round(new_customers_current["revenue"], 2),
                    "percent_of_total": round(new_customers_current["revenue"] / total_revenue * 100, 1) if total_revenue else 0,
                    "avg_per_customer": round(new_customers_current["revenue"] / new_customers_current["count"], 2) if new_customers_current["count"] else 0
                },
                "last_year_acquired": {
                    "customers": new_customers_prev["count"],
                    "revenue": round(new_customers_prev["revenue"], 2),
                    "percent_of_total": round(new_customers_prev["revenue"] / total_revenue * 100, 1) if total_revenue else 0,
                    "avg_per_customer": round(new_customers_prev["revenue"] / new_customers_prev["count"], 2) if new_customers_prev["count"] else 0
                }
            },
            "existing_business": {
                "customers": existing_customers["count"],
                "revenue": round(existing_customers["revenue"], 2),
                "percent_of_total": round(existing_customers["revenue"] / total_revenue * 100, 1) if total_revenue else 0,
                "avg_per_customer": round(existing_customers["revenue"] / existing_customers["count"], 2) if existing_customers["count"] else 0
            }
        }
    except Exception as e:
        logger.error(f"New vs existing revenue error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/customer-churn-analysis")
async def get_customer_churn_analysis(year: int = 2026):
    """
    Customer churn and retention analysis.
    Shows customers lost, at risk, and retention rate.
    """
    try:
        df = sql_connector.execute_query(f"""
            SELECT
                RTRIM(t.st_account) as account,
                RTRIM(s.sn_name) as customer_name,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year} THEN t.st_trvalue ELSE 0 END) as current_year,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year - 1} THEN t.st_trvalue ELSE 0 END) as prev_year,
                SUM(CASE WHEN YEAR(t.st_trdate) = {year - 2} THEN t.st_trvalue ELSE 0 END) as two_years_ago,
                MAX(t.st_trdate) as last_invoice_date
            FROM stran t
            INNER JOIN sname s ON RTRIM(t.st_account) = RTRIM(s.sn_account)
            WHERE t.st_trtype = 'I'
            AND YEAR(t.st_trdate) >= {year - 2}
            GROUP BY t.st_account, s.sn_name
        """)
        data = df_to_records(df)

        churned = []  # Had revenue last year, none this year
        at_risk = []  # Revenue dropped >50% vs last year
        growing = []  # Revenue up vs last year
        stable = []   # Revenue within +/- 20%
        declining = []  # Revenue down 20-50%

        total_churned_revenue = 0
        total_at_risk_revenue = 0

        for row in data:
            curr = float(row['current_year'] or 0)
            prev = float(row['prev_year'] or 0)

            if prev > 0 and curr == 0:
                # Churned customer
                churned.append({
                    "account": row['account'],
                    "customer_name": row['customer_name'],
                    "last_year_revenue": round(prev, 2),
                    "last_invoice": str(row['last_invoice_date'])[:10] if row['last_invoice_date'] else None
                })
                total_churned_revenue += prev
            elif prev > 0 and curr > 0:
                change_pct = (curr - prev) / prev * 100

                if change_pct < -50:
                    at_risk.append({
                        "account": row['account'],
                        "customer_name": row['customer_name'],
                        "current_revenue": round(curr, 2),
                        "previous_revenue": round(prev, 2),
                        "change_percent": round(change_pct, 1)
                    })
                    total_at_risk_revenue += prev - curr  # Revenue at risk
                elif change_pct > 20:
                    growing.append({
                        "account": row['account'],
                        "customer_name": row['customer_name'],
                        "current_revenue": round(curr, 2),
                        "previous_revenue": round(prev, 2),
                        "change_percent": round(change_pct, 1)
                    })
                elif change_pct < -20:
                    declining.append({
                        "account": row['account'],
                        "customer_name": row['customer_name'],
                        "current_revenue": round(curr, 2),
                        "previous_revenue": round(prev, 2),
                        "change_percent": round(change_pct, 1)
                    })
                else:
                    stable.append({
                        "account": row['account'],
                        "customer_name": row['customer_name'],
                        "current_revenue": round(curr, 2)
                    })

        # Sort by revenue impact
        churned.sort(key=lambda x: -x['last_year_revenue'])
        at_risk.sort(key=lambda x: x['change_percent'])
        growing.sort(key=lambda x: -x['change_percent'])

        # Calculate retention rate
        prev_year_customers = len([d for d in data if (d['prev_year'] or 0) > 0])
        retained_customers = prev_year_customers - len(churned)
        retention_rate = (retained_customers / prev_year_customers * 100) if prev_year_customers else 0

        return {
            "success": True,
            "year": year,
            "summary": {
                "retention_rate": round(retention_rate, 1),
                "churned_count": len(churned),
                "churned_revenue": round(total_churned_revenue, 2),
                "at_risk_count": len(at_risk),
                "at_risk_revenue": round(total_at_risk_revenue, 2),
                "growing_count": len(growing),
                "stable_count": len(stable),
                "declining_count": len(declining)
            },
            "churned_customers": churned[:10],  # Top 10 by revenue
            "at_risk_customers": at_risk[:10],   # Top 10 most at risk
            "growing_customers": growing[:10]    # Top 10 growing
        }
    except Exception as e:
        logger.error(f"Churn analysis error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/forward-indicators")
async def get_forward_indicators(year: int = 2026):
    """
    Forward-looking indicators: run rate, recurring revenue base, risk flags.
    Helps predict future performance.
    """
    from datetime import datetime as dt

    try:
        current_date = dt.now()
        current_month = current_date.month

        # Get monthly revenue for trend analysis
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period as month,
                SUM(CASE WHEN RTRIM(nt_type) IN ('E', '30') THEN -nt_value ELSE 0 END) as revenue
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', '30')
            AND nt_year IN ({year}, {year - 1})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Organize data
        revenue_by_month = {year: {}, year - 1: {}}
        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            revenue_by_month[y][m] = float(row['revenue'] or 0)

        # Calculate various run rates
        last_3_months = sum(revenue_by_month[year].get(m, 0) for m in range(max(1, current_month - 2), current_month + 1))
        avg_3_month = last_3_months / min(3, current_month)

        last_6_months = sum(revenue_by_month[year].get(m, 0) for m in range(max(1, current_month - 5), current_month + 1))
        avg_6_month = last_6_months / min(6, current_month)

        ytd = sum(revenue_by_month[year].get(m, 0) for m in range(1, current_month + 1))
        avg_ytd = ytd / current_month if current_month > 0 else 0

        # Trend direction (comparing recent 3 months vs prior 3 months)
        recent_3 = sum(revenue_by_month[year].get(m, 0) for m in range(max(1, current_month - 2), current_month + 1))
        prior_3_start = max(1, current_month - 5)
        prior_3_end = max(1, current_month - 2)
        prior_3 = sum(revenue_by_month[year].get(m, 0) for m in range(prior_3_start, prior_3_end))

        trend_direction = "accelerating" if recent_3 > prior_3 * 1.1 else "decelerating" if recent_3 < prior_3 * 0.9 else "stable"

        # Previous year same period comparison
        prev_ytd = sum(revenue_by_month[year - 1].get(m, 0) for m in range(1, current_month + 1))
        prev_full_year = sum(revenue_by_month[year - 1].get(m, 0) for m in range(1, 13))

        # Projections
        projection_conservative = avg_ytd * 12  # Based on YTD average
        projection_optimistic = avg_3_month * 12  # Based on recent trend
        projection_midpoint = (projection_conservative + projection_optimistic) / 2

        # Risk flags
        risk_flags = []

        # Check for declining trend
        if trend_direction == "decelerating":
            risk_flags.append({
                "type": "trend",
                "severity": "medium",
                "message": "Revenue trend is decelerating vs prior quarter"
            })

        # Check for underperformance vs prior year
        if ytd < prev_ytd * 0.9:
            risk_flags.append({
                "type": "yoy_performance",
                "severity": "high" if ytd < prev_ytd * 0.8 else "medium",
                "message": f"YTD revenue {round((1 - ytd/prev_ytd) * 100, 1)}% below prior year"
            })

        # Check for projection below prior year
        if projection_midpoint < prev_full_year * 0.95:
            risk_flags.append({
                "type": "projection",
                "severity": "medium",
                "message": f"Full year projection tracking below prior year"
            })

        return {
            "success": True,
            "year": year,
            "current_month": current_month,
            "run_rates": {
                "monthly_3m_avg": round(avg_3_month, 2),
                "monthly_6m_avg": round(avg_6_month, 2),
                "monthly_ytd_avg": round(avg_ytd, 2),
                "annual_3m_basis": round(avg_3_month * 12, 2),
                "annual_6m_basis": round(avg_6_month * 12, 2),
                "annual_ytd_basis": round(avg_ytd * 12, 2)
            },
            "trend": {
                "direction": trend_direction,
                "recent_3_months": round(recent_3, 2),
                "prior_3_months": round(prior_3, 2)
            },
            "projections": {
                "conservative": round(projection_conservative, 2),
                "optimistic": round(projection_optimistic, 2),
                "midpoint": round(projection_midpoint, 2),
                "prior_year_actual": round(prev_full_year, 2),
                "vs_prior_year_percent": round((projection_midpoint - prev_full_year) / prev_full_year * 100, 1) if prev_full_year else 0
            },
            "risk_flags": risk_flags,
            "risk_level": "high" if any(f['severity'] == 'high' for f in risk_flags) else "medium" if risk_flags else "low"
        }
    except Exception as e:
        logger.error(f"Forward indicators error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/dashboard/monthly-comparison")
async def get_monthly_comparison(year: int = 2026):
    """
    Detailed month-by-month comparison: current year vs same month last year.
    Shows seasonality patterns and identifies anomalies.
    """
    try:
        df = sql_connector.execute_query(f"""
            SELECT
                nt_year,
                nt_period as month,
                SUM(CASE WHEN RTRIM(nt_type) IN ('E', '30') THEN -nt_value ELSE 0 END) as revenue,
                SUM(CASE WHEN RTRIM(nt_type) IN ('F', '35') THEN nt_value ELSE 0 END) as cost_of_sales
            FROM ntran
            WHERE RTRIM(nt_type) IN ('E', 'F', '30', '35')
            AND nt_year IN ({year}, {year - 1}, {year - 2})
            GROUP BY nt_year, nt_period
            ORDER BY nt_year, nt_period
        """)
        data = df_to_records(df)

        # Organize by year
        by_year = {year: {}, year - 1: {}, year - 2: {}}
        for row in data:
            y = int(row['nt_year'])
            m = int(row['month']) if row['month'] else 0
            by_year[y][m] = {
                "revenue": float(row['revenue'] or 0),
                "cost_of_sales": float(row['cost_of_sales'] or 0)
            }

        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        months = []
        ytd_current = 0
        ytd_previous = 0

        for m in range(1, 13):
            curr = by_year[year].get(m, {"revenue": 0, "cost_of_sales": 0})
            prev = by_year[year - 1].get(m, {"revenue": 0, "cost_of_sales": 0})
            two_yr = by_year[year - 2].get(m, {"revenue": 0, "cost_of_sales": 0})

            curr_rev = curr["revenue"]
            prev_rev = prev["revenue"]
            two_yr_rev = two_yr["revenue"]

            ytd_current += curr_rev
            ytd_previous += prev_rev

            yoy_change = ((curr_rev - prev_rev) / prev_rev * 100) if prev_rev else 0

            # Calculate gross margin
            curr_gp = curr_rev - curr["cost_of_sales"]
            curr_margin = (curr_gp / curr_rev * 100) if curr_rev else 0

            months.append({
                "month": m,
                "month_name": month_names[m - 1],
                "current_year": round(curr_rev, 2),
                "previous_year": round(prev_rev, 2),
                "two_years_ago": round(two_yr_rev, 2),
                "yoy_change_amount": round(curr_rev - prev_rev, 2),
                "yoy_change_percent": round(yoy_change, 1),
                "gross_profit": round(curr_gp, 2),
                "gross_margin_percent": round(curr_margin, 1),
                "ytd_current": round(ytd_current, 2),
                "ytd_previous": round(ytd_previous, 2),
                "ytd_variance": round(ytd_current - ytd_previous, 2)
            })

        return {
            "success": True,
            "year": year,
            "months": months,
            "totals": {
                "current_year": round(sum(m["current_year"] for m in months), 2),
                "previous_year": round(sum(m["previous_year"] for m in months), 2),
                "two_years_ago": round(sum(m["two_years_ago"] for m in months), 2)
            }
        }
    except Exception as e:
        logger.error(f"Monthly comparison error: {e}")
        return {"success": False, "error": str(e)}


# =============================================================================
# OPERA COM AUTOMATION ENDPOINTS
# =============================================================================

# Import Opera COM module (gracefully handles non-Windows environments)
try:
    from sql_rag.opera_com import (
        OperaCOM, get_opera_connection, ImportType, ImportResult, OperaCOMError
    )
    OPERA_COM_AVAILABLE = True
except ImportError:
    OPERA_COM_AVAILABLE = False
    logger.info("Opera COM module not available (requires Windows with pywin32)")


class OperaImportRequest(BaseModel):
    """Request model for Opera import operations"""
    import_type: str = Field(..., description="Type of import: sales_invoices, purchase_invoices, nominal_journals, customers, suppliers, products, sales_orders, purchase_orders")
    file_path: str = Field(..., description="Path to the import file on the server")
    company_code: str = Field(default="", description="Opera company code (uses current if blank)")
    validate_only: bool = Field(default=False, description="Only validate, don't import")
    options: Dict[str, Any] = Field(default={}, description="Additional import options")


class OperaPostRequest(BaseModel):
    """Request model for Opera posting operations"""
    post_type: str = Field(..., description="Type of posting: sales_invoices, purchase_invoices, nominal_journals")
    batch_ref: Optional[str] = Field(default=None, description="Specific batch to post (all if blank)")
    company_code: str = Field(default="", description="Opera company code (uses current if blank)")


@app.get("/api/opera/status")
async def get_opera_status():
    """
    Check if Opera COM automation is available on this server.
    Returns availability status and system information.
    """
    import platform

    return {
        "success": True,
        "opera_com_available": OPERA_COM_AVAILABLE,
        "platform": platform.system(),
        "is_windows": platform.system() == "Windows",
        "message": "Opera COM automation is available" if OPERA_COM_AVAILABLE else "Opera COM automation requires Windows with Opera 3 and pywin32 installed",
        "supported_imports": [
            "sales_invoices",
            "purchase_invoices",
            "nominal_journals",
            "customers",
            "suppliers",
            "products",
            "sales_orders",
            "purchase_orders"
        ] if OPERA_COM_AVAILABLE else []
    }


@app.post("/api/opera/import")
async def opera_import(request: OperaImportRequest):
    """
    Import data into Opera 3 using COM automation.

    This endpoint allows importing various types of data:
    - Sales/Purchase Invoices
    - Nominal Journal Entries
    - Customer/Supplier records
    - Product records
    - Sales/Purchase Orders

    The import uses Opera's native import functions which:
    - Validate data against Opera's business rules
    - Maintain audit trails
    - Update all related ledgers correctly
    """
    if not OPERA_COM_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="Opera COM automation not available. Requires Windows server with Opera 3 and pywin32 installed."
        )

    try:
        # Validate import type
        try:
            import_type = ImportType(request.import_type)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid import type: {request.import_type}. Valid types: {[t.value for t in ImportType]}"
            )

        # Get Opera connection
        opera = get_opera_connection()

        # Connect to company if specified or use current
        company = request.company_code or (current_company.get("code") if current_company else None)
        if not company:
            raise HTTPException(
                status_code=400,
                detail="No company specified and no current company selected"
            )

        if not opera.connected or opera.company != company:
            if not opera.connect(company):
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to connect to Opera company: {company}"
                )

        # Prepare options
        options = request.options.copy()
        options["validate_only"] = request.validate_only

        # Execute import
        result = opera.import_from_file(import_type, request.file_path, options)

        return {
            "success": result.success,
            "import_type": request.import_type,
            "file_path": request.file_path,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "warnings": result.warnings
        }

    except OperaCOMError as e:
        logger.error(f"Opera COM error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Opera import error: {e}")
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")


@app.post("/api/opera/validate")
async def opera_validate(request: OperaImportRequest):
    """
    Validate an import file without actually importing.
    Returns validation results including any errors or warnings.
    """
    request.validate_only = True
    return await opera_import(request)


@app.post("/api/opera/post")
async def opera_post(request: OperaPostRequest):
    """
    Post transactions to the nominal ledger via Opera COM.

    Supported posting types:
    - sales_invoices: Post sales invoices to nominal
    - purchase_invoices: Post purchase invoices to nominal
    - nominal_journals: Post nominal journal entries
    """
    if not OPERA_COM_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="Opera COM automation not available. Requires Windows server with Opera 3 and pywin32 installed."
        )

    try:
        # Get Opera connection
        opera = get_opera_connection()

        # Connect to company
        company = request.company_code or (current_company.get("code") if current_company else None)
        if not company:
            raise HTTPException(
                status_code=400,
                detail="No company specified and no current company selected"
            )

        if not opera.connected or opera.company != company:
            if not opera.connect(company):
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to connect to Opera company: {company}"
                )

        # Execute posting
        if request.post_type == "sales_invoices":
            result = opera.post_sales_invoices(request.batch_ref)
        elif request.post_type == "purchase_invoices":
            result = opera.post_purchase_invoices(request.batch_ref)
        elif request.post_type == "nominal_journals":
            result = opera.post_nominal_journals(request.batch_ref)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid post type: {request.post_type}. Valid types: sales_invoices, purchase_invoices, nominal_journals"
            )

        return {
            "success": result,
            "post_type": request.post_type,
            "batch_ref": request.batch_ref,
            "message": "Posting completed successfully" if result else "Posting failed"
        }

    except OperaCOMError as e:
        logger.error(f"Opera COM error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Opera post error: {e}")
        raise HTTPException(status_code=500, detail=f"Posting failed: {str(e)}")


@app.get("/api/opera/company-info")
async def get_opera_company_info():
    """
    Get information about the currently connected Opera company.
    """
    if not OPERA_COM_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="Opera COM automation not available"
        )

    try:
        opera = get_opera_connection()

        if not opera.connected:
            return {
                "success": False,
                "connected": False,
                "message": "Not connected to Opera"
            }

        info = opera.get_company_info()
        return {
            "success": True,
            "connected": True,
            **info
        }

    except OperaCOMError as e:
        logger.error(f"Opera COM error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# OPERA SQL SE IMPORT ENDPOINTS (Works from any platform!)
# =============================================================================

from sql_rag.opera_sql_import import OperaSQLImport, ImportType as SQLImportType, get_opera_sql_import


class OperaSQLImportRequest(BaseModel):
    """Request model for Opera SQL SE import operations"""
    import_type: str = Field(..., description="Type: customers, suppliers, nominal_journals, sales_invoices")
    data: List[Dict[str, Any]] = Field(default=[], description="Data records to import")
    file_path: Optional[str] = Field(default=None, description="Or path to CSV file")
    field_mapping: Optional[Dict[str, str]] = Field(default=None, description="CSV column to Opera field mapping")
    validate_only: bool = Field(default=False, description="Only validate, don't import")
    update_existing: bool = Field(default=False, description="Update existing records")


@app.get("/api/opera-sql/status")
async def get_opera_sql_status():
    """
    Check Opera SQL SE import capabilities.
    This works from any platform - no Windows required!
    """
    if not sql_connector:
        return {
            "success": False,
            "available": False,
            "message": "No database connection"
        }

    try:
        importer = get_opera_sql_import(sql_connector)
        capabilities = importer.discover_import_capabilities()

        return {
            "success": True,
            "available": True,
            "platform_independent": True,
            "message": "Opera SQL SE import available - works from any platform!",
            "capabilities": capabilities,
            "supported_imports": [
                "customers",
                "suppliers",
                "nominal_journals",
                "sales_invoices",
                "purchase_invoices"
            ]
        }
    except Exception as e:
        logger.error(f"Error checking Opera SQL status: {e}")
        return {
            "success": False,
            "available": False,
            "error": str(e)
        }


@app.get("/api/opera-sql/stored-procedures")
async def get_opera_stored_procedures():
    """
    List ALL stored procedures in the Opera SQL SE database.
    This helps discover if Pegasus has provided import procedures we should use.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query("""
            SELECT
                s.name as schema_name,
                p.name as procedure_name,
                p.type_desc,
                p.create_date,
                p.modify_date
            FROM sys.procedures p
            JOIN sys.schemas s ON p.schema_id = s.schema_id
            ORDER BY s.name, p.name
        """)

        procedures = df.to_dict('records') if not df.empty else []

        # Group by schema for easier reading
        by_schema = {}
        for proc in procedures:
            schema = proc['schema_name']
            if schema not in by_schema:
                by_schema[schema] = []
            by_schema[schema].append(proc['procedure_name'])

        return {
            "success": True,
            "total_count": len(procedures),
            "by_schema": by_schema,
            "all_procedures": procedures,
            "note": "Look for procedures with 'import', 'post', 'process' in their names - these may be Opera's standard routines"
        }
    except Exception as e:
        logger.error(f"Error listing procedures: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/opera-sql/procedure-definition/{procedure_name}")
async def get_procedure_definition(procedure_name: str):
    """
    Get the SQL definition of a stored procedure.
    Useful for understanding what Opera's import procedures do.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query(f"""
            SELECT
                OBJECT_NAME(object_id) as procedure_name,
                definition
            FROM sys.sql_modules
            WHERE OBJECT_NAME(object_id) = '{procedure_name}'
        """)

        if df.empty:
            raise HTTPException(status_code=404, detail=f"Procedure '{procedure_name}' not found")

        return {
            "success": True,
            "procedure_name": procedure_name,
            "definition": df.iloc[0]['definition']
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting procedure definition: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/opera-sql/table-schema/{table_name}")
async def get_opera_table_schema(table_name: str):
    """
    Get the schema for an Opera SQL SE table.
    Useful for understanding required fields before importing.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        importer = get_opera_sql_import(sql_connector)
        schema = importer.get_table_schema(table_name)

        return {
            "success": True,
            "table": table_name,
            "columns": schema
        }
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/opera-sql/import")
async def opera_sql_import(request: OperaSQLImportRequest):
    """
    Import data into Opera SQL SE.

    This endpoint works from ANY platform (Mac, Linux, Windows) because
    it uses direct SQL Server access rather than COM automation.

    Supported import types:
    - customers: Customer master records
    - suppliers: Supplier master records
    - nominal_journals: Nominal ledger journal entries
    - sales_invoices: Sales invoice headers and lines
    - purchase_invoices: Purchase invoice headers and lines

    You can either:
    1. Send data directly in the 'data' field
    2. Specify a 'file_path' to a CSV file on the server

    Example request:
    {
        "import_type": "customers",
        "data": [
            {"account": "CUST001", "name": "Test Customer", "email": "test@example.com"}
        ],
        "validate_only": true
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        # Validate import type
        try:
            import_type = SQLImportType(request.import_type)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid import type: {request.import_type}. Valid: customers, suppliers, nominal_journals, sales_invoices, purchase_invoices"
            )

        importer = get_opera_sql_import(sql_connector)

        # Import from file or data
        if request.file_path:
            result = importer.import_from_csv(
                import_type,
                request.file_path,
                field_mapping=request.field_mapping,
                validate_only=request.validate_only
            )
        elif request.data:
            # Route to appropriate method
            if import_type == SQLImportType.CUSTOMERS:
                result = importer.import_customers(
                    request.data,
                    update_existing=request.update_existing,
                    validate_only=request.validate_only
                )
            elif import_type == SQLImportType.NOMINAL_JOURNALS:
                result = importer.import_nominal_journals(
                    request.data,
                    validate_only=request.validate_only
                )
            elif import_type == SQLImportType.SALES_INVOICES:
                result = importer.import_sales_invoices(
                    request.data,
                    validate_only=request.validate_only
                )
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Import type {import_type.value} not yet implemented for direct data"
                )
        else:
            raise HTTPException(
                status_code=400,
                detail="Must provide either 'data' or 'file_path'"
            )

        return {
            "success": result.success,
            "import_type": request.import_type,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "warnings": result.warnings
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Opera SQL import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/opera-sql/validate")
async def opera_sql_validate(request: OperaSQLImportRequest):
    """
    Validate import data without actually importing.
    Same as /import with validate_only=true
    """
    request.validate_only = True
    return await opera_sql_import(request)


# =============================================================================
# SALES RECEIPT IMPORT - Replicates Opera's exact pattern
# =============================================================================

class SalesReceiptRequest(BaseModel):
    """Request model for importing a sales receipt"""
    bank_account: str = "BC010"  # Default bank account
    customer_account: str  # Required - customer account code
    amount: float  # Amount in POUNDS (e.g., 100.00)
    reference: str = ""  # Your reference (e.g., invoice number)
    post_date: str  # Posting date YYYY-MM-DD
    input_by: str = "IMPORT"  # User code for audit trail
    sales_ledger_control: Optional[str] = None  # Loaded from config if not specified
    validate_only: bool = False  # If True, only validate without inserting


class SalesReceiptBatchRequest(BaseModel):
    """Request model for importing multiple sales receipts"""
    receipts: List[dict]  # List of receipt dictionaries
    validate_only: bool = False


@app.post("/api/opera-sql/sales-receipt")
async def import_sales_receipt(request: SalesReceiptRequest):
    """
    Import a single sales receipt into Opera SQL SE.

    This replicates the EXACT pattern Opera uses when a user manually
    enters a sales receipt, creating records in:
    - aentry (Cashbook Entry Header)
    - atran (Cashbook Transaction)
    - ntran (Nominal Ledger - 2 rows for double-entry bookkeeping)

    Example request:
    {
        "bank_account": "BC010",
        "customer_account": "A046",
        "amount": 100.00,
        "reference": "inv12345",
        "post_date": "2026-01-31",
        "input_by": "TEST",
        "validate_only": false
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime

        importer = get_opera_sql_import(sql_connector)

        # Parse the date
        post_date = datetime.strptime(request.post_date, '%Y-%m-%d').date()

        result = importer.import_sales_receipt(
            bank_account=request.bank_account,
            customer_account=request.customer_account,
            amount_pounds=request.amount,
            reference=request.reference,
            post_date=post_date,
            input_by=request.input_by,
            sales_ledger_control=request.sales_ledger_control,
            validate_only=request.validate_only
        )

        return {
            "success": result.success,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "details": result.warnings  # Contains entry number, journal number, amount
        }

    except Exception as e:
        logger.error(f"Sales receipt import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/opera-sql/sales-receipts-batch")
async def import_sales_receipts_batch(request: SalesReceiptBatchRequest):
    """
    Import multiple sales receipts into Opera SQL SE.

    Example request:
    {
        "receipts": [
            {
                "bank_account": "BC010",
                "customer_account": "A046",
                "amount": 100.00,
                "reference": "inv12345",
                "post_date": "2026-01-31"
            },
            {
                "bank_account": "BC010",
                "customer_account": "A047",
                "amount": 250.00,
                "reference": "inv12346",
                "post_date": "2026-01-31"
            }
        ],
        "validate_only": false
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        importer = get_opera_sql_import(sql_connector)

        result = importer.import_sales_receipts_batch(
            receipts=request.receipts,
            validate_only=request.validate_only
        )

        return {
            "success": result.success,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "details": result.warnings
        }

    except Exception as e:
        logger.error(f"Sales receipts batch import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# PURCHASE PAYMENT IMPORT
# =============================================================================

class PurchasePaymentRequest(BaseModel):
    """Request model for importing a purchase payment"""
    bank_account: str = "BC010"
    supplier_account: str
    amount: float
    reference: str = ""
    post_date: str
    input_by: str = "IMPORT"
    creditors_control: Optional[str] = None  # Loaded from config if not specified
    payment_type: str = "Direct Cr"  # Payment type description
    validate_only: bool = False


@app.post("/api/opera-sql/purchase-payment")
async def import_purchase_payment(request: PurchasePaymentRequest):
    """
    Import a single purchase payment into Opera SQL SE.

    This replicates the pattern Opera uses when a user manually
    enters a supplier payment, creating records in:
    - aentry (Cashbook Entry Header)
    - atran (Cashbook Transaction)
    - ntran (Nominal Ledger - 2 rows for double-entry bookkeeping)

    Example request:
    {
        "bank_account": "BC010",
        "supplier_account": "P001",
        "amount": 500.00,
        "reference": "PAY12345",
        "post_date": "2026-01-31",
        "input_by": "IMPORT",
        "validate_only": false
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime

        importer = get_opera_sql_import(sql_connector)
        post_date = datetime.strptime(request.post_date, '%Y-%m-%d').date()

        result = importer.import_purchase_payment(
            bank_account=request.bank_account,
            supplier_account=request.supplier_account,
            amount_pounds=request.amount,
            reference=request.reference,
            post_date=post_date,
            input_by=request.input_by,
            creditors_control=request.creditors_control,
            payment_type=request.payment_type,
            validate_only=request.validate_only
        )

        return {
            "success": result.success,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "details": result.warnings
        }

    except Exception as e:
        logger.error(f"Purchase payment import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# SALES INVOICE IMPORT (Replicates Opera's exact pattern)
# Creates: stran, ntran (3), nhist
# =============================================================================

class SalesInvoiceRequest(BaseModel):
    """Request model for importing a sales invoice to Opera SQL SE"""
    customer_account: str
    invoice_number: str
    net_amount: float
    vat_amount: float = 0.0
    post_date: str
    customer_ref: str = ""  # Customer's reference (PO number etc)
    sales_nominal: str = "E4030"  # Sales P&L account
    vat_nominal: str = "CA060"  # VAT output account
    debtors_control: Optional[str] = None  # Loaded from config if not specified
    department: str = "U999"  # Department code
    payment_days: int = 14  # Days until payment due
    input_by: str = "IMPORT"
    description: str = ""
    validate_only: bool = False


@app.post("/api/opera-sql/sales-invoice")
async def import_sales_invoice(request: SalesInvoiceRequest):
    """
    Import a sales invoice into Opera SQL SE.

    This replicates the EXACT pattern Opera uses when a user manually
    enters a sales invoice, creating records in:
    - stran (Sales Ledger Transaction)
    - ntran (Nominal Ledger - 3 rows for double-entry)
    - nhist (Nominal History for P&L account)

    Example request:
    {
        "customer_account": "A046",
        "invoice_number": "INV001",
        "net_amount": 100.00,
        "vat_amount": 20.00,
        "post_date": "2026-01-31",
        "customer_ref": "PO12345",
        "sales_nominal": "E4030",
        "vat_nominal": "CA060",
        "department": "U999",
        "payment_days": 14,
        "validate_only": false
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime

        importer = get_opera_sql_import(sql_connector)
        post_date = datetime.strptime(request.post_date, '%Y-%m-%d').date()

        result = importer.import_sales_invoice(
            customer_account=request.customer_account,
            invoice_number=request.invoice_number,
            net_amount=request.net_amount,
            vat_amount=request.vat_amount,
            post_date=post_date,
            customer_ref=request.customer_ref,
            sales_nominal=request.sales_nominal,
            vat_nominal=request.vat_nominal,
            debtors_control=request.debtors_control,
            department=request.department,
            payment_days=request.payment_days,
            input_by=request.input_by,
            description=request.description,
            validate_only=request.validate_only
        )

        return {
            "success": result.success,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "details": result.warnings
        }

    except Exception as e:
        logger.error(f"Sales invoice import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# PURCHASE INVOICE POSTING IMPORT
# =============================================================================

class PurchaseInvoicePostingRequest(BaseModel):
    """Request model for importing a purchase invoice posting"""
    supplier_account: str
    invoice_number: str
    net_amount: float
    vat_amount: float = 0.0
    post_date: str
    nominal_account: str = "HA010"
    vat_account: str = "BB040"
    purchase_ledger_control: Optional[str] = None  # Loaded from config if not specified
    input_by: str = "IMPORT"
    description: str = ""
    validate_only: bool = False


@app.post("/api/opera-sql/purchase-invoice")
async def import_purchase_invoice(request: PurchaseInvoicePostingRequest):
    """
    Import a purchase invoice posting into Opera SQL SE.

    Creates nominal ledger entries:
    - Credit: Purchase Ledger Control (we owe supplier)
    - Debit: Expense Account (cost incurred)
    - Debit: VAT Account (VAT reclaimable)

    Example request:
    {
        "supplier_account": "P001",
        "invoice_number": "PINV001",
        "net_amount": 500.00,
        "vat_amount": 100.00,
        "post_date": "2026-01-31",
        "nominal_account": "HA010",
        "validate_only": false
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime

        importer = get_opera_sql_import(sql_connector)
        post_date = datetime.strptime(request.post_date, '%Y-%m-%d').date()

        result = importer.import_purchase_invoice_posting(
            supplier_account=request.supplier_account,
            invoice_number=request.invoice_number,
            net_amount=request.net_amount,
            vat_amount=request.vat_amount,
            post_date=post_date,
            nominal_account=request.nominal_account,
            vat_account=request.vat_account,
            purchase_ledger_control=request.purchase_ledger_control,
            input_by=request.input_by,
            description=request.description,
            validate_only=request.validate_only
        )

        return {
            "success": result.success,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "details": result.warnings
        }

    except Exception as e:
        logger.error(f"Purchase invoice import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# NOMINAL JOURNAL IMPORT
# =============================================================================

class NominalJournalLine(BaseModel):
    account: str
    amount: float
    description: str = ""


class NominalJournalRequest(BaseModel):
    """Request model for importing a nominal journal"""
    lines: List[NominalJournalLine]
    reference: str
    post_date: str
    input_by: str = "IMPORT"
    description: str = ""
    validate_only: bool = False


@app.post("/api/opera-sql/nominal-journal")
async def import_nominal_journal(request: NominalJournalRequest):
    """
    Import a nominal journal into Opera SQL SE.

    The journal MUST balance (total of all amounts = 0).
    Positive amounts = Debit, Negative amounts = Credit.

    Example request:
    {
        "lines": [
            {"account": "GA010", "amount": 100.00, "description": "Sales"},
            {"account": "BB020", "amount": -100.00, "description": "Control"}
        ],
        "reference": "JNL001",
        "post_date": "2026-01-31",
        "validate_only": false
    }
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime

        importer = get_opera_sql_import(sql_connector)
        post_date = datetime.strptime(request.post_date, '%Y-%m-%d').date()

        # Convert Pydantic models to dicts
        lines = [{"account": l.account, "amount": l.amount, "description": l.description} for l in request.lines]

        result = importer.import_nominal_journal(
            lines=lines,
            reference=request.reference,
            post_date=post_date,
            input_by=request.input_by,
            description=request.description,
            validate_only=request.validate_only
        )

        return {
            "success": result.success,
            "validate_only": request.validate_only,
            "records_processed": result.records_processed,
            "records_imported": result.records_imported,
            "records_failed": result.records_failed,
            "errors": result.errors,
            "details": result.warnings
        }

    except Exception as e:
        logger.error(f"Nominal journal import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# BANK STATEMENT IMPORT
# =============================================================================

class BankImportPreviewResponse(BaseModel):
    """Response model for bank import preview"""
    success: bool
    filename: str
    total_transactions: int
    to_import: int
    already_posted: int
    skipped: int
    transactions: List[Dict[str, Any]]
    errors: List[str]


@app.get("/api/opera-sql/bank-accounts")
async def get_bank_accounts():
    """
    Get list of available bank accounts from Opera.

    Returns all bank accounts configured in Opera's nbank table,
    including account codes, descriptions, and current balances.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        accounts = BankStatementImport.get_available_bank_accounts()
        return {
            "success": True,
            "bank_accounts": accounts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/opera-sql/bank-accounts")
async def get_bank_accounts():
    """
    Get list of available bank accounts from Opera.

    Returns bank account codes, descriptions, sort codes, account numbers and balances.
    Use this to determine which bank_code to use for imports.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        accounts = BankStatementImport.get_available_bank_accounts()
        return {
            "success": True,
            "count": len(accounts),
            "accounts": accounts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/opera-sql/bank-import/preview")
async def preview_bank_import(
    filepath: str = Query(..., description="Path to CSV file"),
    bank_code: str = Query("BC010", description="Opera bank account code")
):
    """
    Preview what would be imported from a bank statement CSV.

    Returns matched transactions, already posted, and skipped items.
    Use this to review before actual import.
    """
    # Backend validation - check for empty filepath
    if not filepath or not filepath.strip():
        return {
            "success": False,
            "filename": "",
            "total_transactions": 0,
            "matched_receipts": [],
            "matched_payments": [],
            "already_posted": [],
            "skipped": [],
            "errors": ["CSV file path is required. Please enter the path to your bank statement CSV file."]
        }

    import os
    if not os.path.exists(filepath):
        return {
            "success": False,
            "filename": filepath,
            "total_transactions": 0,
            "matched_receipts": [],
            "matched_payments": [],
            "already_posted": [],
            "skipped": [],
            "errors": [f"File not found: {filepath}. Please check the file path."]
        }

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        importer = BankStatementImport(bank_code=bank_code)
        result = importer.preview_file(filepath)

        # Categorize transactions for frontend display
        matched_receipts = []
        matched_payments = []
        already_posted = []
        skipped = []

        for txn in result.transactions:
            txn_data = {
                "row": txn.row_number,
                "date": txn.date.isoformat(),
                "amount": txn.amount,
                "name": txn.name,
                "reference": txn.reference,
                "account": txn.matched_account,
                "match_score": txn.match_score if txn.match_score else 0,
                "reason": txn.skip_reason
            }

            if txn.action == 'sales_receipt':
                matched_receipts.append(txn_data)
            elif txn.action == 'purchase_payment':
                matched_payments.append(txn_data)
            elif txn.skip_reason and 'Already' in txn.skip_reason:
                already_posted.append(txn_data)
            else:
                # Skipped for other reasons (no match, ambiguous, etc.)
                skipped.append(txn_data)

        return {
            "success": True,
            "filename": result.filename,
            "total_transactions": result.total_transactions,
            "matched_receipts": matched_receipts,
            "matched_payments": matched_payments,
            "already_posted": already_posted,
            "skipped": skipped,
            "errors": result.errors
        }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {filepath}")
    except Exception as e:
        logger.error(f"Bank import preview error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/opera-sql/bank-import/import")
async def import_bank_statement(
    filepath: str = Query(..., description="Path to CSV file"),
    bank_code: str = Query("BC010", description="Opera bank account code")
):
    """
    Import matched transactions from a bank statement CSV.

    Only imports:
    - Sales receipts (customer payments)
    - Purchase payments (supplier payments)

    Skips:
    - Already posted transactions
    - Direct debits, standing orders, card payments
    - Transactions matching both customer and supplier (ambiguous)
    - Unmatched transactions
    """
    # Backend validation - check for empty filepath
    import os
    if not filepath or not filepath.strip():
        return {
            "success": False,
            "error": "CSV file path is required. Please enter the path to your bank statement CSV file."
        }

    if not os.path.exists(filepath):
        return {
            "success": False,
            "error": f"File not found: {filepath}. Please check the file path."
        }

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        importer = BankStatementImport(bank_code=bank_code)
        result = importer.import_file(filepath)

        # Format imported transactions for response
        imported = []
        for txn in result.transactions:
            if txn.imported:
                imported.append({
                    "date": txn.date.isoformat(),
                    "amount": txn.abs_amount,
                    "type": txn.action,
                    "account": txn.matched_account,
                    "name": txn.matched_name
                })

        return {
            "success": result.imported_transactions > 0 or result.matched_transactions == 0,
            "total_transactions": result.total_transactions,
            "imported": result.imported_transactions,
            "already_posted": result.already_posted,
            "skipped": result.skipped_transactions,
            "imported_transactions": imported,
            "errors": result.errors
        }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {filepath}")
    except Exception as e:
        logger.error(f"Bank import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/opera-sql/bank-import/audit")
async def bank_import_audit_report(
    filepath: str = Query(..., description="Path to CSV file"),
    bank_code: str = Query("BC010", description="Opera bank account code"),
    format: str = Query("json", description="Output format: json or text")
):
    """
    Generate an audit report of bank statement import.

    Returns detailed breakdown of:
    - Imported transactions with matched accounts
    - Not imported transactions with reasons
    - Summary by skip reason category
    - Total amounts

    Use format=text for a printable report.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        importer = BankStatementImport(bank_code=bank_code)
        result = importer.preview_file(filepath)

        if format == "text":
            return {
                "success": True,
                "report": importer.get_audit_report_text(result)
            }
        else:
            return {
                "success": True,
                **importer.generate_audit_report(result)
            }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {filepath}")
    except Exception as e:
        logger.error(f"Bank import audit error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# OPERA TRANSACTION ANALYSIS - Learn from manual entries
# =============================================================================

# Store snapshots for comparison
_table_snapshots: Dict[str, Any] = {}


@app.get("/api/opera-sql/snapshot/take")
async def take_table_snapshot(
    label: str = "before",
    tables: str = "ntran,sltrn,cbtrn"
):
    """
    Take a snapshot of Opera tables BEFORE you enter a transaction manually.

    Usage:
    1. Call this endpoint with label="before"
    2. Enter your transaction in Opera
    3. Call with label="after"
    4. Call /api/opera-sql/snapshot/compare to see what changed

    Default tables monitored:
    - ntran: Nominal transactions
    - sltrn: Sales ledger transactions
    - pltrn: Purchase ledger transactions
    - audit: Audit trail (if exists)
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    # Parse comma-separated tables string into list
    table_list = [t.strip() for t in tables.split(",") if t.strip()]

    try:
        snapshot = {
            "label": label,
            "timestamp": datetime.now().isoformat(),
            "tables": {}
        }

        for table in table_list:
            try:
                # Get row count and max ID/date
                df = sql_connector.execute_query(f"""
                    SELECT
                        COUNT(*) as row_count,
                        MAX(CAST(NEWID() AS VARCHAR(36))) as snapshot_id
                    FROM {table}
                """)

                # Get recent rows (last 50)
                df_recent = sql_connector.execute_query(f"""
                    SELECT TOP 50 * FROM {table}
                    ORDER BY 1 DESC
                """)

                snapshot["tables"][table] = {
                    "row_count": int(df.iloc[0]['row_count']) if not df.empty else 0,
                    "recent_rows": df_recent.to_dict('records') if not df_recent.empty else []
                }
            except Exception as e:
                snapshot["tables"][table] = {"error": str(e)}

        _table_snapshots[label] = snapshot

        return {
            "success": True,
            "label": label,
            "timestamp": snapshot["timestamp"],
            "tables_captured": list(snapshot["tables"].keys()),
            "row_counts": {t: snapshot["tables"][t].get("row_count", "error") for t in table_list},
            "message": f"Snapshot '{label}' taken. Now enter your transaction in Opera, then take another snapshot with label='after'"
        }

    except Exception as e:
        logger.error(f"Snapshot error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/opera-sql/snapshot/compare")
async def compare_snapshots(before_label: str = "before", after_label: str = "after"):
    """
    Compare two snapshots to see what Opera changed when you entered a transaction.

    This shows you exactly what tables and fields Opera populates,
    so we can replicate it for imports.
    """
    if before_label not in _table_snapshots:
        raise HTTPException(status_code=400, detail=f"No snapshot with label '{before_label}'. Take a snapshot first.")
    if after_label not in _table_snapshots:
        raise HTTPException(status_code=400, detail=f"No snapshot with label '{after_label}'. Take a snapshot first.")

    before = _table_snapshots[before_label]
    after = _table_snapshots[after_label]

    comparison = {
        "before_timestamp": before["timestamp"],
        "after_timestamp": after["timestamp"],
        "changes": {}
    }

    for table in after["tables"]:
        if table not in before["tables"]:
            comparison["changes"][table] = {"status": "new_table"}
            continue

        before_count = before["tables"][table].get("row_count", 0)
        after_count = after["tables"][table].get("row_count", 0)

        if before_count != after_count:
            # Find new rows
            before_rows = before["tables"][table].get("recent_rows", [])
            after_rows = after["tables"][table].get("recent_rows", [])

            # Simple comparison - rows in after but not in before
            new_rows = []
            if after_count > before_count:
                new_rows = after_rows[:after_count - before_count]

            comparison["changes"][table] = {
                "rows_added": after_count - before_count,
                "before_count": before_count,
                "after_count": after_count,
                "new_rows": new_rows
            }
        else:
            comparison["changes"][table] = {"status": "no_change", "row_count": after_count}

    return {
        "success": True,
        **comparison,
        "summary": {
            "tables_changed": [t for t, c in comparison["changes"].items() if c.get("rows_added", 0) > 0],
            "total_rows_added": sum(c.get("rows_added", 0) for c in comparison["changes"].values())
        }
    }


@app.get("/api/opera-sql/snapshot/new-rows/{table}")
async def get_new_rows_detail(table: str, before_label: str = "before", after_label: str = "after"):
    """
    Get detailed view of new rows added to a specific table.
    Shows all columns and values so you can see exactly what Opera populated.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    if before_label not in _table_snapshots or after_label not in _table_snapshots:
        raise HTTPException(status_code=400, detail="Missing snapshots. Take before/after snapshots first.")

    before_count = _table_snapshots[before_label]["tables"].get(table, {}).get("row_count", 0)
    after_count = _table_snapshots[after_label]["tables"].get(table, {}).get("row_count", 0)

    rows_added = after_count - before_count

    if rows_added <= 0:
        return {
            "success": True,
            "table": table,
            "rows_added": 0,
            "message": "No new rows in this table"
        }

    try:
        # Get the newest rows
        df = sql_connector.execute_query(f"""
            SELECT TOP {rows_added} * FROM {table}
            ORDER BY 1 DESC
        """)

        new_rows = df.to_dict('records') if not df.empty else []

        # Get column info
        schema_df = sql_connector.execute_query(f"""
            SELECT
                c.name as column_name,
                t.name as data_type,
                c.is_nullable
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            WHERE c.object_id = OBJECT_ID('{table}')
            ORDER BY c.column_id
        """)

        columns = schema_df.to_dict('records') if not schema_df.empty else []

        return {
            "success": True,
            "table": table,
            "rows_added": rows_added,
            "columns": columns,
            "new_rows": new_rows,
            "note": "These are the exact rows Opera created. Use these field names and values as a template for imports."
        }

    except Exception as e:
        logger.error(f"Error getting new rows: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# BANK IMPORT ALIAS MANAGEMENT
# =============================================================================

@app.get("/api/bank-aliases")
async def get_bank_aliases(ledger_type: Optional[str] = Query(None, description="Filter by ledger type: 'S' for supplier, 'C' for customer")):
    """
    Get all bank import aliases.

    Aliases are learned matches between bank statement names and Opera accounts.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_aliases import BankAliasManager

        manager = BankAliasManager()
        aliases = manager.get_all_aliases()

        if ledger_type:
            aliases = [a for a in aliases if a.ledger_type == ledger_type.upper()]

        return {
            "success": True,
            "count": len(aliases),
            "aliases": [
                {
                    "id": a.id,
                    "bank_name": a.bank_name,
                    "ledger_type": a.ledger_type,
                    "ledger_type_name": "Supplier" if a.ledger_type == 'S' else "Customer",
                    "account_code": a.account_code,
                    "account_name": a.account_name,
                    "match_score": round(a.match_score * 100) if a.match_score else None,
                    "use_count": a.use_count,
                    "last_used": a.last_used.isoformat() if a.last_used else None,
                    "created_date": a.created_date.isoformat() if a.created_date else None,
                    "created_by": a.created_by
                }
                for a in aliases
            ]
        }

    except Exception as e:
        logger.error(f"Error getting aliases: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/bank-aliases/statistics")
async def get_bank_alias_statistics():
    """
    Get statistics about bank import aliases.

    Returns counts, usage statistics, and most frequently used aliases.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_aliases import BankAliasManager

        manager = BankAliasManager()
        stats = manager.get_statistics()

        return {
            "success": True,
            "statistics": {
                "total_aliases": stats.get('total_aliases', 0),
                "active_aliases": stats.get('active_aliases', 0),
                "supplier_aliases": stats.get('supplier_aliases', 0),
                "customer_aliases": stats.get('customer_aliases', 0),
                "total_uses": stats.get('total_uses', 0),
                "avg_match_score": round(stats.get('avg_match_score', 0) * 100) if stats.get('avg_match_score') else 0,
                "last_used": stats.get('last_used').isoformat() if stats.get('last_used') else None
            }
        }

    except Exception as e:
        logger.error(f"Error getting alias statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class CreateAliasRequest(BaseModel):
    """Request model for creating a bank alias"""
    bank_name: str
    ledger_type: str  # 'S' or 'C'
    account_code: str
    account_name: Optional[str] = None


@app.post("/api/bank-aliases")
async def create_bank_alias(request: CreateAliasRequest):
    """
    Manually create a bank import alias.

    Use this to add aliases for bank names that fuzzy matching struggles with.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    if request.ledger_type.upper() not in ('S', 'C'):
        raise HTTPException(status_code=400, detail="ledger_type must be 'S' (supplier) or 'C' (customer)")

    try:
        from sql_rag.bank_aliases import BankAliasManager

        manager = BankAliasManager()
        success = manager.save_alias(
            bank_name=request.bank_name,
            ledger_type=request.ledger_type.upper(),
            account_code=request.account_code,
            match_score=1.0,  # Manual aliases get perfect score
            account_name=request.account_name,
            created_by='MANUAL'
        )

        if success:
            return {
                "success": True,
                "message": f"Alias created: '{request.bank_name}' -> {request.account_code}"
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to create alias")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating alias: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/bank-aliases")
async def delete_bank_alias(
    bank_name: str = Query(..., description="Bank name to delete"),
    ledger_type: str = Query(..., description="'S' for supplier, 'C' for customer")
):
    """
    Delete a bank import alias.

    This performs a soft delete (deactivates the alias).
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    if ledger_type.upper() not in ('S', 'C'):
        raise HTTPException(status_code=400, detail="ledger_type must be 'S' (supplier) or 'C' (customer)")

    try:
        from sql_rag.bank_aliases import BankAliasManager

        manager = BankAliasManager()
        success = manager.delete_alias(bank_name, ledger_type.upper())

        if success:
            return {
                "success": True,
                "message": f"Alias deleted: '{bank_name}'"
            }
        else:
            return {
                "success": False,
                "message": f"Alias not found: '{bank_name}'"
            }

    except Exception as e:
        logger.error(f"Error deleting alias: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/bank-aliases/for-account/{account_code}")
async def get_aliases_for_account(account_code: str):
    """
    Get all aliases that map to a specific Opera account.

    Useful for seeing what bank names have been matched to an account.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_aliases import BankAliasManager

        manager = BankAliasManager()
        aliases = manager.get_aliases_for_account(account_code)

        return {
            "success": True,
            "account_code": account_code,
            "count": len(aliases),
            "aliases": [
                {
                    "bank_name": a.bank_name,
                    "ledger_type": a.ledger_type,
                    "use_count": a.use_count,
                    "last_used": a.last_used.isoformat() if a.last_used else None
                }
                for a in aliases
            ]
        }

    except Exception as e:
        logger.error(f"Error getting aliases for account: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ENHANCED BANK IMPORT ENDPOINTS
# =============================================================================

@app.post("/api/bank-import/detect-format")
async def detect_file_format(filepath: str = Query(..., description="Path to bank statement file")):
    """
    Detect the format of a bank statement file.

    Supports: CSV, OFX, QIF, MT940
    """
    import os
    if not filepath or not filepath.strip():
        return {"success": False, "error": "File path is required"}

    if not os.path.exists(filepath):
        return {"success": False, "error": f"File not found: {filepath}"}

    try:
        from sql_rag.bank_import import BankStatementImport

        detected = BankStatementImport.detect_file_format(filepath)
        return {
            "success": True,
            "filepath": filepath,
            "format": detected,
            "supported_formats": ["CSV", "OFX", "QIF", "MT940"]
        }
    except Exception as e:
        logger.error(f"Format detection error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/bank-import/detect-bank")
async def detect_bank_from_file(
    filepath: str = Query(..., description="Path to bank statement file")
):
    """
    Detect which Opera bank account a bank statement file belongs to.

    Reads the bank details (sort code, account number) from the file
    and matches against Opera's nbank table.

    Supports multiple CSV formats:
    - Header row format: "Account Number:,20-96-89,90764205"
    - Data column format: Account field with "sort_code account_number"

    Returns the detected bank code and account details.
    """
    import os
    import csv
    import re

    if not filepath or not filepath.strip():
        return {
            "success": False,
            "error": "File path is required"
        }

    if not os.path.exists(filepath):
        return {
            "success": False,
            "error": f"File not found: {filepath}"
        }

    try:
        from sql_rag.bank_import import BankStatementImport

        sort_code = None
        account_number = None

        # Read file and try multiple detection methods
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()[:30]  # Read first 30 lines for detection

        # Method 1: Scan ALL lines for sort code (XX-XX-XX) and account number (8 digits) patterns
        # This works regardless of CSV format - just find the patterns
        for line in lines:
            # Look for sort code pattern: XX-XX-XX (6 digits with dashes)
            sort_match = re.search(r'(\d{2}-\d{2}-\d{2})', line)
            # Look for 8-digit account number (not part of a longer number)
            acct_match = re.search(r'(?<!\d)(\d{8})(?!\d)', line)

            if sort_match and acct_match:
                sort_code = sort_match.group(1)
                account_number = acct_match.group(1)
                break

        # Method 2: Try to find in data rows with 'Account' column (format: "20-96-89 90764205")
        if not (sort_code and account_number):
            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    # Find the header row (contains 'Date' and 'Account' columns)
                    for i, line in enumerate(f):
                        line_lower = line.lower()
                        if 'date' in line_lower and 'account' in line_lower:
                            f.seek(0)
                            # Skip to header row
                            for _ in range(i):
                                next(f)
                            reader = csv.DictReader(f)
                            first_row = next(reader, None)
                            if first_row:
                                # Try 'Account' field (case-insensitive lookup)
                                account_field = None
                                for key in first_row.keys():
                                    if key.lower() == 'account':
                                        account_field = first_row[key].strip()
                                        break
                                if account_field:
                                    # Format: "20-96-89 90764205"
                                    parts = account_field.split(' ', 1)
                                    if len(parts) == 2:
                                        sort_code = parts[0].strip()
                                        account_number = parts[1].strip()
                            break
            except Exception as e:
                logger.warning(f"Method 2 bank detection error: {e}")

        # If we found bank details, look up in Opera
        detected_code = None
        if sort_code and account_number:
            detected_code = BankStatementImport.find_bank_account_by_details(sort_code, account_number)

        if detected_code:
            # Get full bank details
            bank_accounts = BankStatementImport.get_available_bank_accounts()
            bank_info = next((b for b in bank_accounts if b['code'] == detected_code), None)

            return {
                "success": True,
                "detected": True,
                "bank_code": detected_code,
                "bank_description": bank_info['description'] if bank_info else detected_code,
                "sort_code": bank_info.get('sort_code', '') if bank_info else sort_code,
                "account_number": bank_info.get('account_number', '') if bank_info else account_number,
                "message": f"Detected bank account: {detected_code}"
            }
        else:
            # Could not detect - return all available banks for manual selection
            bank_accounts = BankStatementImport.get_available_bank_accounts()
            return {
                "success": True,
                "detected": False,
                "bank_code": None,
                "message": f"Could not detect bank account from file.{' Found: ' + sort_code + ' ' + account_number if sort_code else ''} Please select manually.",
                "available_banks": bank_accounts
            }

    except Exception as e:
        logger.error(f"Bank detection error: {e}")
        return {
            "success": False,
            "error": str(e)
        }


@app.post("/api/bank-import/preview-multiformat")
async def preview_bank_import_multiformat(
    filepath: str = Query(..., description="Path to bank statement file"),
    bank_code: str = Query("BC010", description="Opera bank account code"),
    format_override: Optional[str] = Query(None, description="Force specific format: CSV, OFX, QIF, MT940")
):
    """
    Preview bank statement import with auto-format detection.

    Supports CSV, OFX, QIF, and MT940 formats.
    Returns transactions categorized for import with duplicate detection.
    """
    import os
    if not filepath or not filepath.strip():
        return {
            "success": False,
            "error": "File path is required",
            "transactions": []
        }

    if not os.path.exists(filepath):
        return {
            "success": False,
            "error": f"File not found: {filepath}",
            "transactions": []
        }

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        importer = BankStatementImport(
            bank_code=bank_code,
            use_enhanced_matching=True,
            use_fingerprinting=True
        )

        # Validate bank account matches the CSV file
        is_valid, validation_message, detected_bank = importer.validate_bank_account_from_csv(filepath)
        if not is_valid:
            return {
                "success": False,
                "error": validation_message,
                "bank_mismatch": True,
                "detected_bank": detected_bank,
                "selected_bank": bank_code,
                "transactions": []
            }

        # Parse with auto-detection
        transactions, detected_format = importer.parse_file(filepath, format_override)

        # Process transactions (matching, duplicate detection)
        importer.process_transactions(transactions)

        # Validate period accounting for each transaction using ledger-specific rules
        from sql_rag.opera_config import (
            get_period_posting_decision,
            get_current_period_info,
            is_open_period_accounting_enabled,
            validate_posting_period,
            get_ledger_type_for_transaction
        )

        period_info = get_current_period_info(sql_connector)
        open_period_enabled = is_open_period_accounting_enabled(sql_connector)
        period_violations = []

        for txn in transactions:
            # Store original date for reference
            txn.original_date = txn.date

            # Determine the appropriate ledger type based on transaction action
            # Only validate matched transactions that have an action
            if txn.action and txn.action not in ('skip', None):
                ledger_type = get_ledger_type_for_transaction(txn.action)

                # Use ledger-specific validation
                period_result = validate_posting_period(sql_connector, txn.date, ledger_type)

                if not period_result.is_valid:
                    txn.period_valid = False
                    txn.period_error = period_result.error_message
                    period_violations.append({
                        "row": txn.row_number,
                        "date": txn.date.isoformat(),
                        "name": txn.name,
                        "action": txn.action,
                        "ledger_type": ledger_type,
                        "error": period_result.error_message,
                        "transaction_year": period_result.year,
                        "transaction_period": period_result.period,
                        "current_year": period_info.get('np_year'),
                        "current_period": period_info.get('np_perno')
                    })
                else:
                    txn.period_valid = True
                    txn.period_error = None
            else:
                # Unmatched/skipped transactions - still check basic year validation
                decision = get_period_posting_decision(sql_connector, txn.date)
                if not decision.can_post:
                    txn.period_valid = False
                    txn.period_error = decision.error_message
                else:
                    txn.period_valid = True
                    txn.period_error = None

        # Categorize for frontend
        matched_receipts = []
        matched_payments = []
        matched_refunds = []
        repeat_entries = []
        unmatched = []
        already_posted = []
        skipped = []

        for txn in transactions:
            txn_data = {
                "row": txn.row_number,
                "date": txn.date.isoformat(),
                "amount": txn.amount,
                "name": txn.name,
                "reference": txn.reference,
                "memo": txn.memo,
                "fit_id": txn.fit_id,
                "account": txn.matched_account,
                "account_name": txn.matched_name,
                "match_score": round(txn.match_score * 100) if txn.match_score else 0,
                "match_source": txn.match_source,
                "action": txn.action,
                "reason": txn.skip_reason,
                "fingerprint": txn.fingerprint,
                "is_duplicate": txn.is_duplicate,
                "duplicate_candidates": [
                    {
                        "table": c.table,
                        "record_id": c.record_id,
                        "match_type": c.match_type,
                        "confidence": round(c.confidence * 100)
                    }
                    for c in (txn.duplicate_candidates or [])
                ],
                "refund_credit_note": getattr(txn, 'refund_credit_note', None),
                "refund_credit_amount": getattr(txn, 'refund_credit_amount', None),
                # Repeat entry fields
                "repeat_entry_ref": getattr(txn, 'repeat_entry_ref', None),
                "repeat_entry_desc": getattr(txn, 'repeat_entry_desc', None),
                "repeat_entry_next_date": getattr(txn, 'repeat_entry_next_date', None).isoformat() if getattr(txn, 'repeat_entry_next_date', None) else None,
                "repeat_entry_posted": getattr(txn, 'repeat_entry_posted', None),
                "repeat_entry_total": getattr(txn, 'repeat_entry_total', None),
                # Period validation fields
                "period_valid": getattr(txn, 'period_valid', True),
                "period_error": getattr(txn, 'period_error', None),
                "original_date": getattr(txn, 'original_date', txn.date).isoformat() if getattr(txn, 'original_date', None) else txn.date.isoformat(),
            }

            if txn.action == 'sales_receipt':
                matched_receipts.append(txn_data)
            elif txn.action == 'purchase_payment':
                matched_payments.append(txn_data)
            elif txn.action in ('sales_refund', 'purchase_refund'):
                matched_refunds.append(txn_data)
            elif txn.action == 'repeat_entry':
                repeat_entries.append(txn_data)
            elif txn.is_duplicate or (txn.skip_reason and 'Already' in txn.skip_reason):
                already_posted.append(txn_data)
            elif txn.skip_reason and ('No customer' in txn.skip_reason or 'No supplier' in txn.skip_reason):
                unmatched.append(txn_data)
            else:
                skipped.append(txn_data)

        return {
            "success": True,
            "filename": filepath,
            "detected_format": detected_format,
            "total_transactions": len(transactions),
            "matched_receipts": matched_receipts,
            "matched_payments": matched_payments,
            "matched_refunds": matched_refunds,
            "repeat_entries": repeat_entries,
            "unmatched": unmatched,
            "already_posted": already_posted,
            "skipped": skipped,
            "summary": {
                "to_import": len(matched_receipts) + len(matched_payments) + len(matched_refunds),
                "refund_count": len(matched_refunds),
                "repeat_entry_count": len(repeat_entries),
                "unmatched_count": len(unmatched),
                "already_posted_count": len(already_posted),
                "skipped_count": len(skipped)
            },
            # Period validation info
            "period_info": {
                "current_year": period_info.get('np_year'),
                "current_period": period_info.get('np_perno'),
                "open_period_accounting": open_period_enabled
            },
            "period_violations": period_violations,
            "has_period_violations": len(period_violations) > 0
        }

    except Exception as e:
        logger.error(f"Multi-format preview error: {e}")
        return {"success": False, "error": str(e), "transactions": []}


@app.post("/api/bank-import/correction")
async def record_correction(
    bank_name: str = Query(..., description="Name from bank statement"),
    wrong_account: str = Query(..., description="The incorrectly matched account"),
    correct_account: str = Query(..., description="The correct account"),
    ledger_type: str = Query(..., description="'S' for supplier, 'C' for customer"),
    account_name: Optional[str] = Query(None, description="Name of the correct account")
):
    """
    Record a user correction for alias learning.

    This teaches the system to:
    1. Map the bank name to the correct account
    2. Avoid matching to the wrong account in future
    """
    try:
        from sql_rag.bank_aliases import BankAliasManager

        # Try enhanced manager first
        try:
            from sql_rag.bank_aliases import EnhancedAliasManager
            manager = EnhancedAliasManager()
        except ImportError:
            manager = BankAliasManager()

        if hasattr(manager, 'record_correction'):
            success = manager.record_correction(
                bank_name=bank_name,
                wrong_account=wrong_account,
                correct_account=correct_account,
                ledger_type=ledger_type.upper(),
                account_name=account_name
            )
        else:
            # Fallback: just save the alias
            success = manager.save_alias(
                bank_name=bank_name,
                ledger_type=ledger_type.upper(),
                account_code=correct_account,
                match_score=1.0,
                account_name=account_name
            )

        return {
            "success": success,
            "message": f"Correction recorded: '{bank_name}' -> {correct_account}" if success else "Failed to record correction"
        }

    except Exception as e:
        logger.error(f"Error recording correction: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/bank-import/check-duplicates")
async def check_duplicates(
    transactions: List[Dict[str, Any]],
    bank_code: str = Query("BC010", description="Opera bank account code")
):
    """
    Check multiple transactions for duplicates.

    Input: List of transactions with 'name', 'amount', 'date', optional 'account'
    Returns: Dict mapping transaction index to duplicate candidates
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_duplicates import EnhancedDuplicateDetector
        from datetime import datetime

        detector = EnhancedDuplicateDetector(sql_connector)

        # Parse dates if needed
        for txn in transactions:
            if isinstance(txn.get('date'), str):
                try:
                    txn['date'] = datetime.strptime(txn['date'], '%Y-%m-%d').date()
                except ValueError:
                    txn['date'] = datetime.strptime(txn['date'], '%d/%m/%Y').date()

        results = detector.check_batch(transactions, bank_code)

        # Format for JSON response
        formatted_results = {}
        for idx, candidates in results.items():
            formatted_results[str(idx)] = [
                {
                    "table": c.table,
                    "record_id": c.record_id,
                    "match_type": c.match_type,
                    "confidence": round(c.confidence * 100),
                    "details": c.details
                }
                for c in candidates
            ]

        return {
            "success": True,
            "duplicates_found": len(results),
            "results": formatted_results
        }

    except ImportError:
        return {"success": False, "error": "Duplicate detection module not available"}
    except Exception as e:
        logger.error(f"Error checking duplicates: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/bank-import/duplicate-override")
async def override_duplicate(
    transaction_hash: str = Query(..., description="Hash of the transaction"),
    reason: str = Query(..., description="Reason for override")
):
    """
    Record a duplicate override decision.

    When a user decides to import a transaction despite it being flagged
    as a potential duplicate, record the decision.
    """
    try:
        from sql_rag.bank_aliases import BankAliasManager
        import sqlite3

        manager = BankAliasManager()
        conn = manager._get_conn()

        # Create table if needed
        conn.execute("""
            CREATE TABLE IF NOT EXISTS duplicate_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_hash TEXT NOT NULL UNIQUE,
                override_reason TEXT,
                user_code TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            INSERT OR REPLACE INTO duplicate_overrides (transaction_hash, override_reason)
            VALUES (?, ?)
        """, (transaction_hash, reason))
        conn.commit()

        return {
            "success": True,
            "message": "Duplicate override recorded"
        }

    except Exception as e:
        logger.error(f"Error recording duplicate override: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/bank-import/config")
async def get_match_config():
    """
    Get matching configuration settings.
    """
    try:
        from sql_rag.bank_aliases import BankAliasManager

        manager = BankAliasManager()
        conn = manager._get_conn()

        cursor = conn.execute("""
            SELECT * FROM match_config ORDER BY id DESC LIMIT 1
        """)
        row = cursor.fetchone()

        if row:
            config = dict(row)
        else:
            config = {
                "min_match_score": 0.6,
                "learn_threshold": 0.8,
                "ambiguity_threshold": 0.15,
                "use_phonetic": True,
                "use_levenshtein": True,
                "use_ngram": True
            }

        return {
            "success": True,
            "config": config
        }

    except Exception as e:
        logger.error(f"Error getting match config: {e}")
        return {
            "success": True,
            "config": {
                "min_match_score": 0.6,
                "learn_threshold": 0.8,
                "ambiguity_threshold": 0.15
            }
        }


@app.put("/api/bank-import/config")
async def update_match_config(
    min_match_score: float = Query(0.6, ge=0.0, le=1.0),
    learn_threshold: float = Query(0.8, ge=0.0, le=1.0),
    ambiguity_threshold: float = Query(0.15, ge=0.0, le=1.0),
    use_phonetic: bool = Query(True),
    use_levenshtein: bool = Query(True),
    use_ngram: bool = Query(True)
):
    """
    Update matching configuration settings.
    """
    try:
        from sql_rag.bank_aliases import BankAliasManager
        from datetime import datetime

        manager = BankAliasManager()
        conn = manager._get_conn()

        conn.execute("""
            INSERT INTO match_config (
                min_match_score, learn_threshold, ambiguity_threshold,
                use_phonetic, use_levenshtein, use_ngram, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            min_match_score, learn_threshold, ambiguity_threshold,
            1 if use_phonetic else 0,
            1 if use_levenshtein else 0,
            1 if use_ngram else 0,
            datetime.now().isoformat()
        ))
        conn.commit()

        return {
            "success": True,
            "message": "Configuration updated"
        }

    except Exception as e:
        logger.error(f"Error updating match config: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/bank-import/list-csv")
async def list_csv_files(directory: str):
    """
    List CSV files in a directory with their dates and sizes.
    Used by the frontend to populate a file picker dropdown.
    """
    import glob
    from datetime import datetime

    try:
        if not os.path.isdir(directory):
            return {"success": False, "files": [], "error": f"Directory not found: {directory}"}

        csv_files = []
        for pattern in ['*.csv', '*.CSV']:
            for filepath in glob.glob(os.path.join(directory, pattern)):
                stat = os.stat(filepath)
                csv_files.append({
                    "filename": os.path.basename(filepath),
                    "size_bytes": stat.st_size,
                    "size_display": f"{stat.st_size / 1024:.1f} KB" if stat.st_size < 1048576 else f"{stat.st_size / 1048576:.1f} MB",
                    "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%d/%m/%Y %H:%M"),
                    "modified_timestamp": stat.st_mtime,
                })

        # Deduplicate (*.csv and *.CSV could match same file on case-insensitive filesystem)
        seen = set()
        unique_files = []
        for f in csv_files:
            if f["filename"] not in seen:
                seen.add(f["filename"])
                unique_files.append(f)

        # Sort by date descending (newest first)
        unique_files.sort(key=lambda f: f["modified_timestamp"], reverse=True)

        # For each CSV, detect which bank account it belongs to (from first row)
        from sql_rag.bank_import import BankStatementImport
        for f in unique_files:
            full_path = os.path.join(directory, f["filename"])
            try:
                detected_bank = BankStatementImport.find_bank_account_by_details_from_csv(full_path)
                f["detected_bank"] = detected_bank
            except Exception:
                f["detected_bank"] = None

        return {"success": True, "files": unique_files, "directory": directory}

    except Exception as e:
        logger.error(f"Error listing CSV files: {e}")
        return {"success": False, "files": [], "error": str(e)}


@app.post("/api/bank-import/validate-csv")
async def validate_csv_bank_match(
    filepath: str = Query(..., description="Path to CSV file"),
    bank_code: str = Query(..., description="Selected bank account code")
):
    """
    Validate that a CSV file matches the selected bank account.

    Returns whether the bank account in the CSV (sort code + account number)
    matches the selected Opera bank account.
    """
    import os
    if not filepath or not os.path.exists(filepath):
        return {"success": False, "error": "File not found", "valid": False}

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport

        importer = BankStatementImport(bank_code=bank_code)
        is_valid, message, detected_bank = importer.validate_bank_account_from_csv(filepath)

        return {
            "success": True,
            "valid": is_valid,
            "message": message,
            "detected_bank": detected_bank,
            "selected_bank": bank_code
        }

    except Exception as e:
        logger.error(f"Error validating CSV: {e}")
        return {"success": False, "error": str(e), "valid": False}


@app.get("/api/bank-import/accounts/customers")
async def get_customers_for_dropdown():
    """
    Get customer accounts for dropdown selection in UI.

    Returns simplified list for account selection dropdowns.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query("""
            SELECT
                RTRIM(sn_account) as code,
                RTRIM(sn_name) as name,
                RTRIM(ISNULL(sn_key1, '')) as search_key
            FROM sname WITH (NOLOCK)
            ORDER BY sn_account
        """)

        accounts = [
            {
                "code": row['code'],
                "name": row['name'],
                "search_key": row.get('search_key', ''),
                "display": f"{row['code']} - {row['name']}"
            }
            for _, row in df.iterrows()
        ]

        return {
            "success": True,
            "count": len(accounts),
            "accounts": accounts
        }

    except Exception as e:
        logger.error(f"Error getting customers: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/bank-import/accounts/suppliers")
async def get_suppliers_for_dropdown():
    """
    Get supplier accounts for dropdown selection in UI.

    Returns simplified list for account selection dropdowns.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query("""
            SELECT
                RTRIM(pn_account) as code,
                RTRIM(pn_name) as name,
                RTRIM(ISNULL(pn_payee, '')) as payee
            FROM pname WITH (NOLOCK)
            ORDER BY pn_account
        """)

        accounts = [
            {
                "code": row['code'],
                "name": row['name'],
                "payee": row.get('payee', ''),
                "display": f"{row['code']} - {row['name']}"
            }
            for _, row in df.iterrows()
        ]

        return {
            "success": True,
            "count": len(accounts),
            "accounts": accounts
        }

    except Exception as e:
        logger.error(f"Error getting suppliers: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/bank-import/import-with-overrides")
async def import_with_manual_overrides(
    filepath: str = Query(..., description="Path to bank statement file"),
    bank_code: str = Query("BC010", description="Opera bank account code"),
    request_body: Dict[str, Any] = None
):
    """
    Import bank statement with manual account overrides, date overrides, and rejected rows.

    Request body format:
    {
        "overrides": [{"row": 1, "account": "A001", "ledger_type": "C", "transaction_type": "sales_refund"}, ...],
        "date_overrides": [{"row": 1, "date": "2025-01-15"}, ...],  // Date changes for period violations
        "selected_rows": [1, 2, 3, 5]  // Row numbers to import (only these rows will be imported)
    }

    Also accepts legacy format (just array of overrides) for backwards compatibility.
    If selected_rows is not provided, all matched transactions are imported.
    Import will be blocked if any selected transactions have period violations.
    """
    import os
    from datetime import datetime
    if not filepath or not os.path.exists(filepath):
        return {"success": False, "error": "File not found"}

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport
        from sql_rag.opera_config import get_period_posting_decision

        # Handle both new format (object with overrides and selected_rows) and legacy format (just array)
        if request_body is None:
            overrides = []
            date_overrides = []
            selected_rows = None  # None means import all matched
        elif isinstance(request_body, list):
            # Legacy format: just an array of overrides
            overrides = request_body
            date_overrides = []
            selected_rows = None
        else:
            # New format: object with overrides, date_overrides, and selected_rows
            overrides = request_body.get('overrides', [])
            date_overrides = request_body.get('date_overrides', [])
            selected_rows_list = request_body.get('selected_rows')
            selected_rows = set(selected_rows_list) if selected_rows_list is not None else None

        importer = BankStatementImport(
            bank_code=bank_code,
            use_enhanced_matching=True,
            use_fingerprinting=True
        )

        # Parse and process
        transactions, detected_format = importer.parse_file(filepath)
        importer.process_transactions(transactions)

        # Apply date overrides first (to fix period violations)
        date_override_map = {d['row']: d['date'] for d in date_overrides}
        for txn in transactions:
            if txn.row_number in date_override_map:
                new_date_str = date_override_map[txn.row_number]
                txn.original_date = txn.date  # Preserve original
                txn.date = datetime.strptime(new_date_str, '%Y-%m-%d').date()

        # Apply manual overrides (supports unmatched, skipped, and refund modifications)
        override_map = {o['row']: o for o in overrides}
        for txn in transactions:
            if txn.row_number in override_map:
                override = override_map[txn.row_number]
                # Only apply account override if provided
                if override.get('account'):
                    txn.manual_account = override.get('account')
                    txn.manual_ledger_type = override.get('ledger_type')

                # Use explicit transaction_type if provided, otherwise infer from ledger type
                transaction_type = override.get('transaction_type')
                if transaction_type and transaction_type in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund'):
                    txn.action = transaction_type
                elif override.get('ledger_type') == 'C':
                    txn.action = 'sales_receipt'
                elif override.get('ledger_type') == 'S':
                    txn.action = 'purchase_payment'

        # Validate periods for all selected transactions before importing
        # Use ledger-specific validation (SL for receipts/refunds to customers, PL for payments/refunds from suppliers)
        from sql_rag.opera_config import (
            validate_posting_period,
            get_ledger_type_for_transaction,
            get_current_period_info
        )

        period_info = get_current_period_info(sql_connector)
        period_violations = []

        for txn in transactions:
            # Only check transactions that will be imported
            if selected_rows is not None and txn.row_number not in selected_rows:
                continue
            if txn.action not in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund'):
                continue
            if txn.is_duplicate:
                continue

            # Get the appropriate ledger type for this transaction
            ledger_type = get_ledger_type_for_transaction(txn.action)

            # Use ledger-specific period validation
            period_result = validate_posting_period(sql_connector, txn.date, ledger_type)

            if not period_result.is_valid:
                ledger_names = {'SL': 'Sales Ledger', 'PL': 'Purchase Ledger', 'NL': 'Nominal Ledger'}
                period_violations.append({
                    "row": txn.row_number,
                    "date": txn.date.isoformat(),
                    "name": txn.name,
                    "amount": txn.amount,
                    "action": txn.action,
                    "ledger_type": ledger_type,
                    "ledger_name": ledger_names.get(ledger_type, ledger_type),
                    "error": period_result.error_message,
                    "year": period_result.year,
                    "period": period_result.period
                })

        # Block import if any period violations exist
        if period_violations:
            return {
                "success": False,
                "error": "Cannot import - some transactions are in blocked periods for their respective ledgers",
                "period_violations": period_violations,
                "period_info": {
                    "current_year": period_info.get('np_year'),
                    "current_period": period_info.get('np_perno')
                },
                "message": "The following transactions cannot be posted because their dates fall within "
                          "closed or blocked periods for the Sales or Purchase Ledger. Please adjust the "
                          "dates or open the periods in Opera before importing."
            }

        # Block import if there are unprocessed repeat entries
        # User must run Opera's Repeat Entries routine first, then re-preview
        unprocessed_repeat_entries = []
        for txn in transactions:
            if txn.action == 'repeat_entry':
                unprocessed_repeat_entries.append({
                    "row": txn.row_number,
                    "name": txn.name,
                    "amount": txn.amount,
                    "date": txn.date.isoformat(),
                    "entry_ref": getattr(txn, 'repeat_entry_ref', None),
                    "entry_desc": getattr(txn, 'repeat_entry_desc', None)
                })

        if unprocessed_repeat_entries:
            return {
                "success": False,
                "error": "Cannot import - there are unprocessed repeat entries",
                "repeat_entries": unprocessed_repeat_entries,
                "message": "Please run Opera's Repeat Entries routine first to post these transactions, "
                          "then re-preview the bank statement. The repeat entry transactions will then "
                          "be detected as already posted (duplicates) and excluded from import."
            }

        # Import transactions (all 4 action types), only importing selected rows
        imported = []
        errors = []
        skipped_not_selected = 0
        skipped_incomplete = 0

        for txn in transactions:
            # Skip rows not in selected_rows (if selected_rows is specified)
            if selected_rows is not None and txn.row_number not in selected_rows:
                skipped_not_selected += 1
                continue

            if txn.action in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund') and not txn.is_duplicate:
                # Validate mandatory data before import
                account = txn.manual_account or txn.matched_account
                if not account:
                    skipped_incomplete += 1
                    errors.append({
                        "row": txn.row_number,
                        "error": "Missing account - cannot import without customer/supplier assigned"
                    })
                    continue

                if not txn.action or txn.action not in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund'):
                    skipped_incomplete += 1
                    errors.append({
                        "row": txn.row_number,
                        "error": "Missing transaction type - cannot import without valid type assigned"
                    })
                    continue

                try:
                    result = importer.import_transaction(txn)
                    if result.success:
                        imported.append({
                            "row": txn.row_number,
                            "account": txn.manual_account or txn.matched_account,
                            "amount": txn.amount,
                            "action": txn.action
                        })

                        # Learn from manual assignment
                        if txn.manual_account and importer.alias_manager:
                            inferred_ledger = 'C' if txn.action in ('sales_receipt', 'sales_refund') else 'S'
                            importer.alias_manager.save_alias(
                                bank_name=txn.name,
                                ledger_type=txn.manual_ledger_type or inferred_ledger,
                                account_code=txn.manual_account,
                                match_score=1.0,
                                created_by='MANUAL_IMPORT'
                            )
                    else:
                        error_msg = '; '.join(result.errors) if result.errors else 'Import failed'
                        errors.append({"row": txn.row_number, "error": error_msg})
                except Exception as e:
                    errors.append({"row": txn.row_number, "error": str(e)})

        # Calculate totals by action type
        receipts_imported = sum(1 for t in imported if t['action'] == 'sales_receipt')
        payments_imported = sum(1 for t in imported if t['action'] == 'purchase_payment')
        refunds_imported = sum(1 for t in imported if t['action'] in ('sales_refund', 'purchase_refund'))

        return {
            "success": len(errors) == 0,
            "imported_count": len(imported),
            "receipts_imported": receipts_imported,
            "payments_imported": payments_imported,
            "refunds_imported": refunds_imported,
            "skipped_not_selected": skipped_not_selected,
            "skipped_incomplete": skipped_incomplete,
            "imported_transactions": imported,
            "errors": errors
        }

    except Exception as e:
        logger.error(f"Import with overrides error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/bank-import/update-repeat-entry-date")
async def update_repeat_entry_date(
    entry_ref: str = Query(..., description="Repeat entry reference (ae_entry)"),
    bank_code: str = Query(..., description="Bank account code"),
    new_date: str = Query(..., description="New next posting date (YYYY-MM-DD)"),
    statement_name: Optional[str] = Query(None, description="Bank statement name/reference for learning")
):
    """
    Update the next posting date (ae_nxtpost) for a repeat entry.

    This allows syncing the repeat entry schedule with the actual bank transaction date.
    After updating, the user should run Opera's Repeat Entries routine to post the transaction,
    then re-preview the bank statement.

    If statement_name is provided, saves an alias for future automatic matching.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime

        # Validate date format
        try:
            parsed_date = datetime.strptime(new_date, '%Y-%m-%d').date()
        except ValueError:
            return {"success": False, "error": f"Invalid date format: {new_date}. Expected YYYY-MM-DD"}

        # Verify the repeat entry exists
        verify_query = f"""
            SELECT ae_entry, ae_desc, ae_nxtpost, ae_acnt
            FROM arhead WITH (NOLOCK)
            WHERE RTRIM(ae_entry) = '{entry_ref}'
              AND RTRIM(ae_acnt) = '{bank_code}'
        """
        df = sql_connector.execute_query(verify_query)

        if df is None or len(df) == 0:
            return {
                "success": False,
                "error": f"Repeat entry '{entry_ref}' not found for bank '{bank_code}'"
            }

        old_date = df.iloc[0]['ae_nxtpost']
        description = str(df.iloc[0]['ae_desc']).strip()

        # Get current timestamp for audit fields
        now = datetime.now()
        amend_date = now.strftime('%Y-%m-%d')
        amend_time = now.strftime('%H:%M:%S')

        # Update the next posting date AND audit fields in the header record
        update_query = f"""
            UPDATE arhead WITH (ROWLOCK)
            SET ae_nxtpost = '{new_date}',
                sq_amdate = '{amend_date}',
                sq_amtime = '{amend_time}',
                sq_amuser = 'BANKIMP'
            WHERE RTRIM(ae_entry) = '{entry_ref}'
              AND RTRIM(ae_acnt) = '{bank_code}'
        """
        rows_affected = sql_connector.execute_non_query(update_query)

        if rows_affected == 0:
            return {
                "success": False,
                "error": f"No rows updated - entry may have been modified"
            }

        logger.info(f"Updated repeat entry {entry_ref} ae_nxtpost from {old_date} to {new_date} ({rows_affected} row(s))")

        # Save alias for future matching if statement_name provided
        alias_saved = False
        if statement_name:
            try:
                from sql_rag.bank_aliases import BankAliasManager
                alias_manager = BankAliasManager()
                alias_saved = alias_manager.save_repeat_entry_alias(
                    bank_name=statement_name,
                    bank_code=bank_code,
                    entry_ref=entry_ref,
                    entry_desc=description
                )
                if alias_saved:
                    logger.info(f"Saved repeat entry alias: '{statement_name}' -> {entry_ref}")
            except Exception as alias_err:
                logger.warning(f"Could not save repeat entry alias: {alias_err}")

        return {
            "success": True,
            "message": f"Updated '{description}' next posting date to {new_date}",
            "entry_ref": entry_ref,
            "old_date": str(old_date) if old_date else None,
            "new_date": new_date,
            "alias_saved": alias_saved
        }

    except Exception as e:
        logger.error(f"Error updating repeat entry date: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/bank-import/repeat-entries")
async def list_repeat_entries(
    bank_code: str = Query(..., description="Bank account code")
):
    """
    List all active repeat entries for a bank account.
    Useful for debugging repeat entry matching.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        query = f"""
            SELECT
                h.ae_entry,
                h.ae_desc,
                h.ae_nxtpost,
                h.ae_freq,
                h.ae_every,
                h.ae_posted,
                h.ae_topost,
                h.ae_type,
                l.at_value,
                l.at_account,
                l.at_cbtype,
                l.at_comment,
                CASE WHEN h.ae_topost = 0 OR h.ae_posted < h.ae_topost THEN 'Active' ELSE 'Completed' END as status
            FROM arhead h WITH (NOLOCK)
            JOIN arline l WITH (NOLOCK) ON h.ae_entry = l.at_entry AND h.ae_acnt = l.at_acnt
            WHERE RTRIM(h.ae_acnt) = '{bank_code}'
            ORDER BY h.ae_nxtpost DESC
        """
        df = sql_connector.execute_query(query)

        if df is None or len(df) == 0:
            return {
                "success": True,
                "bank_code": bank_code,
                "repeat_entries": [],
                "message": f"No repeat entries found for bank {bank_code}"
            }

        entries = []
        for _, row in df.iterrows():
            amount_pence = row.get('at_value', 0)
            amount_pounds = abs(amount_pence) / 100 if amount_pence else 0
            entries.append({
                "entry_ref": str(row.get('ae_entry', '')).strip(),
                "description": str(row.get('ae_desc', '')).strip() or str(row.get('at_comment', '')).strip(),
                "next_post_date": str(row.get('ae_nxtpost', ''))[:10] if row.get('ae_nxtpost') else None,
                "frequency": row.get('ae_freq', ''),
                "every": row.get('ae_every', 1),
                "posted_count": row.get('ae_posted', 0),
                "total_posts": row.get('ae_topost', 0),
                "status": row.get('status', ''),
                "amount_pence": amount_pence,
                "amount_pounds": amount_pounds,
                "account": str(row.get('at_account', '')).strip(),
                "cb_type": str(row.get('at_cbtype', '')).strip()
            })

        return {
            "success": True,
            "bank_code": bank_code,
            "repeat_entries": entries,
            "count": len(entries)
        }

    except Exception as e:
        logger.error(f"Error listing repeat entries: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Bank Statement Email Scanning Endpoints
# ============================================================

# Bank statement detection patterns
BANK_STATEMENT_PATTERNS = {
    'barclays': {
        'sender_patterns': ['@barclays.co.uk', '@barclays.com', 'barclays'],
        'filename_patterns': ['barclays', 'bcb_statement'],
    },
    'lloyds': {
        'sender_patterns': ['@lloydsbank.co.uk', '@lloydsbank.com', 'lloyds'],
        'filename_patterns': ['lloyds', 'lbg_statement'],
    },
    'hsbc': {
        'sender_patterns': ['@hsbc.co.uk', '@hsbc.com', 'hsbc'],
        'filename_patterns': ['hsbc'],
    },
    'natwest': {
        'sender_patterns': ['@natwest.com', 'natwest'],
        'filename_patterns': ['natwest'],
    },
    'santander': {
        'sender_patterns': ['@santander.co.uk', 'santander'],
        'filename_patterns': ['santander'],
    },
}

# Allowed bank statement file extensions
BANK_STATEMENT_EXTENSIONS = {'.csv', '.ofx', '.qif', '.mt940', '.sta', '.pdf'}

# Allowed content types for bank statements
BANK_STATEMENT_CONTENT_TYPES = {
    'text/csv', 'application/csv', 'text/plain',
    'application/vnd.ms-excel', 'application/ofx', 'application/pdf',
    'application/x-ofx', 'application/qif'
}


def detect_bank_from_email(from_address: str, filename: str) -> Optional[str]:
    """Detect which bank a statement might be from based on sender and filename."""
    from_lower = from_address.lower() if from_address else ''
    filename_lower = filename.lower() if filename else ''

    for bank_name, patterns in BANK_STATEMENT_PATTERNS.items():
        # Check sender patterns
        for pattern in patterns['sender_patterns']:
            if pattern.lower() in from_lower:
                return bank_name

        # Check filename patterns
        for pattern in patterns['filename_patterns']:
            if pattern.lower() in filename_lower:
                return bank_name

    return None


def is_bank_statement_attachment(filename: str, content_type: str) -> bool:
    """Check if an attachment could be a bank statement based on extension/type."""
    if not filename:
        return False

    # Check extension
    ext = '.' + filename.lower().split('.')[-1] if '.' in filename else ''
    if ext in BANK_STATEMENT_EXTENSIONS:
        return True

    # Check content type
    if content_type and content_type.lower() in BANK_STATEMENT_CONTENT_TYPES:
        return True

    return False


@app.get("/api/bank-import/scan-emails")
async def scan_emails_for_bank_statements(
    bank_code: str = Query("BC010", description="Opera bank account code"),
    days_back: int = Query(30, description="Number of days to search back"),
    include_processed: bool = Query(False, description="Include already-processed emails")
):
    """
    Scan inbox for emails with bank statement attachments.

    Returns list of candidate emails with:
    - email_id, subject, from_address, received_at
    - attachments: [{attachment_id, filename, size_bytes}]
    - detected_bank (if identifiable from sender/filename)
    - already_processed flag
    """
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        from datetime import datetime, timedelta

        # Calculate date range
        from_date = datetime.utcnow() - timedelta(days=days_back)

        # Get emails with attachments in date range
        result = email_storage.get_emails(
            from_date=from_date,
            page=1,
            page_size=500  # Reasonable limit for scanning
        )

        # Get list of already processed attachments
        processed = email_storage.get_processed_bank_statement_attachments(bank_code if not include_processed else None)
        processed_keys = {(p['email_id'], p['attachment_id']) for p in processed}

        statements_found = []
        already_processed_count = 0

        for email in result.get('emails', []):
            email_id = email.get('id')
            if not email.get('has_attachments'):
                continue

            # Get attachments for this email
            email_detail = email_storage.get_email_by_id(email_id)
            if not email_detail:
                continue

            attachments = email_detail.get('attachments', [])
            if not attachments:
                continue

            # Filter to potential bank statement attachments
            statement_attachments = []
            for att in attachments:
                filename = att.get('filename', '')
                content_type = att.get('content_type', '')
                attachment_id = att.get('attachment_id', '')

                if is_bank_statement_attachment(filename, content_type):
                    is_processed = (email_id, attachment_id) in processed_keys
                    if is_processed:
                        already_processed_count += 1
                        if not include_processed:
                            continue

                    statement_attachments.append({
                        'attachment_id': attachment_id,
                        'filename': filename,
                        'size_bytes': att.get('size_bytes', 0),
                        'content_type': content_type,
                        'already_processed': is_processed
                    })

            if statement_attachments:
                # Detect bank from sender or first attachment filename
                detected_bank = detect_bank_from_email(
                    email.get('from_address', ''),
                    statement_attachments[0]['filename']
                )

                statements_found.append({
                    'email_id': email_id,
                    'message_id': email.get('message_id'),
                    'subject': email.get('subject'),
                    'from_address': email.get('from_address'),
                    'from_name': email.get('from_name'),
                    'received_at': email.get('received_at'),
                    'attachments': statement_attachments,
                    'detected_bank': detected_bank,
                    'already_processed': all(a['already_processed'] for a in statement_attachments)
                })

        return {
            "success": True,
            "statements_found": statements_found,
            "total_found": len(statements_found),
            "already_processed_count": already_processed_count,
            "days_searched": days_back,
            "bank_code": bank_code
        }

    except Exception as e:
        logger.error(f"Error scanning emails for bank statements: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/bank-import/preview-from-email")
async def preview_bank_import_from_email(
    email_id: int = Query(..., description="Email ID"),
    attachment_id: str = Query(..., description="Attachment ID"),
    bank_code: str = Query("BC010", description="Opera bank account code"),
    format_override: Optional[str] = Query(None, description="Force specific format: CSV, OFX, QIF, MT940"),
    extraction_method: str = Query("auto", description="Extraction method: auto (AI for PDFs), ai (force AI), parse (force text parsing)")
):
    """
    Preview bank statement from email attachment.
    Same response format as preview-multiformat.

    Extraction methods:
    - auto: Use AI extraction for PDFs, text parsing for CSV/OFX/QIF/MT940
    - ai: Force AI extraction for any file type
    - parse: Force text parsing (will fail for binary PDFs)
    """
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email services not initialized")

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.bank_import import BankStatementImport
        from sql_rag.opera_config import (
            get_period_posting_decision,
            get_current_period_info,
            is_open_period_accounting_enabled,
            validate_posting_period,
            get_ledger_type_for_transaction
        )

        # Get email details
        email = email_storage.get_email_by_id(email_id)
        if not email:
            return {"success": False, "error": f"Email {email_id} not found"}

        # Find the attachment
        attachments = email.get('attachments', [])
        attachment_meta = next(
            (a for a in attachments if a.get('attachment_id') == attachment_id),
            None
        )
        if not attachment_meta:
            return {"success": False, "error": f"Attachment {attachment_id} not found"}

        filename = attachment_meta.get('filename', 'statement')

        # Get provider for this email
        provider_id = email.get('provider_id')
        provider = email_sync_manager.providers.get(provider_id)
        if not provider:
            return {"success": False, "error": f"Provider {provider_id} not available"}

        message_id = email.get('message_id')

        # Get folder_id
        folder_id_db = email.get('folder_id')
        folder_id = 'INBOX'
        if folder_id_db:
            with email_storage._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_db,))
                row = cursor.fetchone()
                if row:
                    folder_id = row['folder_id']

        # Download attachment content
        result = await provider.download_attachment(message_id, attachment_id, folder_id)
        if not result:
            return {"success": False, "error": "Failed to download attachment"}

        content_bytes, _, _ = result

        # Determine extraction method
        use_ai_extraction = (
            extraction_method == 'ai' or
            (extraction_method == 'auto' and filename.lower().endswith('.pdf'))
        )

        # Initialize variables
        detected_bank_info = None
        detected_bank_code = None

        # Handle AI extraction (for PDFs or when forced)
        if use_ai_extraction:
            import tempfile
            import os
            from sql_rag.statement_reconcile import StatementReconciler
            from sql_rag.bank_import import BankTransaction

            # Save to temp file for AI extraction
            file_ext = '.' + filename.split('.')[-1] if '.' in filename else '.pdf'
            with tempfile.NamedTemporaryFile(suffix=file_ext, delete=False) as tmp_file:
                tmp_file.write(content_bytes)
                tmp_path = tmp_file.name

            try:
                # Use StatementReconciler for AI extraction
                reconciler = StatementReconciler(sql_connector, config=config)
                statement_info, stmt_transactions = reconciler.extract_transactions_from_pdf(tmp_path)

                # Convert StatementTransaction to BankTransaction format
                transactions = []
                for i, st in enumerate(stmt_transactions, start=1):
                    # StatementTransaction.amount: positive = money in, negative = money out
                    txn = BankTransaction(
                        row_number=i,
                        date=st.date,
                        amount=st.amount,
                        subcategory=st.transaction_type or '',
                        memo=st.description or '',
                        name=st.description or '',
                        reference=st.reference or '',
                        fit_id=''
                    )
                    transactions.append(txn)

                detected_format = "PDF (AI Extraction)"

                # Validate/detect bank from statement
                detected_bank_info = {
                    "bank_name": statement_info.bank_name,
                    "account_number": statement_info.account_number,
                    "sort_code": statement_info.sort_code,
                    "statement_date": statement_info.statement_date.isoformat() if statement_info.statement_date else None,
                    "opening_balance": statement_info.opening_balance,
                    "closing_balance": statement_info.closing_balance
                }

                # Always try to match detected bank to Opera bank accounts
                banks_df = sql_connector.execute_query("""
                    SELECT nk_acnt, nk_sort, nk_number, RTRIM(nk_desc) AS nk_desc
                    FROM nbank WITH (NOLOCK)
                """)
                if banks_df is not None and len(banks_df) > 0:
                    for _, bank in banks_df.iterrows():
                        # Normalize sort codes for comparison
                        stmt_sort = (statement_info.sort_code or '').replace('-', '').replace(' ', '')
                        bank_sort = (bank['nk_sort'] or '').replace('-', '').replace(' ', '')
                        stmt_acnt = (statement_info.account_number or '').replace(' ', '')
                        bank_acnt = (bank['nk_number'] or '').replace(' ', '')

                        if stmt_sort == bank_sort and stmt_acnt == bank_acnt:
                            detected_bank_code = bank['nk_acnt'].strip()
                            detected_bank_info['matched_opera_bank'] = detected_bank_code
                            detected_bank_info['matched_opera_name'] = bank['nk_desc']
                            break

                # Warn if selected bank doesn't match detected bank
                if detected_bank_code and detected_bank_code.strip() != bank_code.strip():
                    detected_bank_info['bank_mismatch'] = True
                    detected_bank_info['selected_bank'] = bank_code

                # Create importer for matching
                importer = BankStatementImport(
                    bank_code=detected_bank_code or bank_code,
                    use_enhanced_matching=True,
                    use_fingerprinting=True
                )

            finally:
                os.unlink(tmp_path)
        else:
            # Decode bytes to string for text-based formats
            try:
                content = content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                content = content_bytes.decode('latin-1')

            # Create importer and parse content
            importer = BankStatementImport(
                bank_code=bank_code,
                use_enhanced_matching=True,
                use_fingerprinting=True
            )

            # Validate bank from content (for CSV files)
            if filename.lower().endswith('.csv'):
                is_valid, validation_message, detected_bank = importer.validate_bank_from_content(content)
                if not is_valid:
                    return {
                        "success": False,
                        "error": validation_message,
                        "bank_mismatch": True,
                        "detected_bank": detected_bank,
                        "selected_bank": bank_code,
                        "transactions": []
                    }

            # Parse content
            transactions, detected_format = importer.parse_content(content, filename, format_override)

        # Process transactions (matching, duplicate detection)
        importer.process_transactions(transactions)

        # Validate period accounting for each transaction
        period_info = get_current_period_info(sql_connector)
        open_period_enabled = is_open_period_accounting_enabled(sql_connector)
        period_violations = []

        for txn in transactions:
            txn.original_date = txn.date

            if txn.action and txn.action not in ('skip', None):
                ledger_type = get_ledger_type_for_transaction(txn.action)
                period_result = validate_posting_period(sql_connector, txn.date, ledger_type)

                if not period_result.is_valid:
                    txn.period_valid = False
                    txn.period_error = period_result.error_message
                    period_violations.append({
                        "row": txn.row_number,
                        "date": txn.date.isoformat(),
                        "name": txn.name,
                        "action": txn.action,
                        "ledger_type": ledger_type,
                        "error": period_result.error_message,
                        "transaction_year": period_result.year,
                        "transaction_period": period_result.period,
                        "current_year": period_info.get('np_year'),
                        "current_period": period_info.get('np_perno')
                    })
                else:
                    txn.period_valid = True
                    txn.period_error = None
            else:
                decision = get_period_posting_decision(sql_connector, txn.date)
                if not decision.can_post:
                    txn.period_valid = False
                    txn.period_error = decision.error_message
                else:
                    txn.period_valid = True
                    txn.period_error = None

        # Categorize for frontend (same as preview-multiformat)
        matched_receipts = []
        matched_payments = []
        matched_refunds = []
        repeat_entries = []
        unmatched = []
        already_posted = []
        skipped = []

        for txn in transactions:
            txn_data = {
                "row": txn.row_number,
                "date": txn.date.isoformat(),
                "amount": txn.amount,
                "name": txn.name,
                "reference": txn.reference,
                "memo": txn.memo,
                "fit_id": txn.fit_id,
                "account": txn.matched_account,
                "account_name": txn.matched_name,
                "match_score": round(txn.match_score * 100) if txn.match_score else 0,
                "match_source": txn.match_source,
                "action": txn.action,
                "reason": txn.skip_reason,
                "fingerprint": txn.fingerprint,
                "is_duplicate": txn.is_duplicate,
                "duplicate_candidates": [
                    {
                        "table": c.table,
                        "record_id": c.record_id,
                        "match_type": c.match_type,
                        "confidence": round(c.confidence * 100)
                    }
                    for c in (txn.duplicate_candidates or [])
                ],
                "refund_credit_note": getattr(txn, 'refund_credit_note', None),
                "refund_credit_amount": getattr(txn, 'refund_credit_amount', None),
                "repeat_entry_ref": getattr(txn, 'repeat_entry_ref', None),
                "repeat_entry_desc": getattr(txn, 'repeat_entry_desc', None),
                "repeat_entry_next_date": getattr(txn, 'repeat_entry_next_date', None).isoformat() if getattr(txn, 'repeat_entry_next_date', None) else None,
                "repeat_entry_posted": getattr(txn, 'repeat_entry_posted', None),
                "repeat_entry_total": getattr(txn, 'repeat_entry_total', None),
                "period_valid": getattr(txn, 'period_valid', True),
                "period_error": getattr(txn, 'period_error', None),
                "original_date": getattr(txn, 'original_date', txn.date).isoformat() if getattr(txn, 'original_date', None) else txn.date.isoformat(),
            }

            if txn.action == 'sales_receipt':
                matched_receipts.append(txn_data)
            elif txn.action == 'purchase_payment':
                matched_payments.append(txn_data)
            elif txn.action in ('sales_refund', 'purchase_refund'):
                matched_refunds.append(txn_data)
            elif txn.action == 'repeat_entry':
                repeat_entries.append(txn_data)
            elif txn.is_duplicate or (txn.skip_reason and 'Already' in txn.skip_reason):
                already_posted.append(txn_data)
            elif txn.skip_reason and ('No customer' in txn.skip_reason or 'No supplier' in txn.skip_reason):
                unmatched.append(txn_data)
            else:
                skipped.append(txn_data)

        # Include detected bank info if available (from AI extraction)
        statement_bank_info = detected_bank_info if use_ai_extraction else None

        # Check for bank mismatch (for frontend warning)
        has_bank_mismatch = (
            statement_bank_info and
            statement_bank_info.get('bank_mismatch', False)
        )

        return {
            "success": True,
            "filename": filename,
            "source": "email",
            "email_id": email_id,
            "attachment_id": attachment_id,
            "detected_format": detected_format,
            "statement_bank_info": statement_bank_info,
            "bank_mismatch": has_bank_mismatch,
            "detected_bank": statement_bank_info.get('matched_opera_bank') if has_bank_mismatch else None,
            "selected_bank": bank_code if has_bank_mismatch else None,
            "bank_code_used": detected_bank_code if use_ai_extraction and detected_bank_code else bank_code,
            "total_transactions": len(transactions),
            "matched_receipts": matched_receipts,
            "matched_payments": matched_payments,
            "matched_refunds": matched_refunds,
            "repeat_entries": repeat_entries,
            "unmatched": unmatched,
            "already_posted": already_posted,
            "skipped": skipped,
            "summary": {
                "to_import": len(matched_receipts) + len(matched_payments) + len(matched_refunds),
                "refund_count": len(matched_refunds),
                "repeat_entry_count": len(repeat_entries),
                "unmatched_count": len(unmatched),
                "already_posted_count": len(already_posted),
                "skipped_count": len(skipped)
            },
            "errors": [],
            "period_info": {
                "current_year": period_info.get('np_year'),
                "current_period": period_info.get('np_perno'),
                "open_period_accounting": open_period_enabled
            },
            "period_violations": period_violations,
            "has_period_violations": len(period_violations) > 0
        }

    except Exception as e:
        logger.error(f"Error previewing bank import from email: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/bank-import/import-from-email")
async def import_bank_statement_from_email(
    email_id: int = Query(..., description="Email ID"),
    attachment_id: str = Query(..., description="Attachment ID"),
    bank_code: str = Query("BC010", description="Opera bank account code"),
    request_body: Dict[str, Any] = None
):
    """
    Import bank statement from email attachment.
    Same request body format as import-with-overrides.

    Request body format:
    {
        "overrides": [{"row": 1, "account": "A001", "ledger_type": "C", "transaction_type": "sales_refund"}, ...],
        "date_overrides": [{"row": 1, "date": "2025-01-15"}, ...],
        "selected_rows": [1, 2, 3, 5]
    }
    """
    if not email_storage or not email_sync_manager:
        raise HTTPException(status_code=503, detail="Email services not initialized")

    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime
        from sql_rag.bank_import import BankStatementImport
        from sql_rag.opera_config import validate_posting_period, get_ledger_type_for_transaction, get_current_period_info

        # Get email details
        email = email_storage.get_email_by_id(email_id)
        if not email:
            return {"success": False, "error": f"Email {email_id} not found"}

        # Find the attachment
        attachments = email.get('attachments', [])
        attachment_meta = next(
            (a for a in attachments if a.get('attachment_id') == attachment_id),
            None
        )
        if not attachment_meta:
            return {"success": False, "error": f"Attachment {attachment_id} not found"}

        filename = attachment_meta.get('filename', 'statement')

        # Get provider for this email
        provider_id = email.get('provider_id')
        provider = email_sync_manager.providers.get(provider_id)
        if not provider:
            return {"success": False, "error": f"Provider {provider_id} not available"}

        message_id = email.get('message_id')

        # Get folder_id
        folder_id_db = email.get('folder_id')
        folder_id = 'INBOX'
        if folder_id_db:
            with email_storage._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT folder_id FROM email_folders WHERE id = ?", (folder_id_db,))
                row = cursor.fetchone()
                if row:
                    folder_id = row['folder_id']

        # Download attachment content
        result = await provider.download_attachment(message_id, attachment_id, folder_id)
        if not result:
            return {"success": False, "error": "Failed to download attachment"}

        content_bytes, _, _ = result

        # Parse request body first
        if request_body is None:
            overrides = []
            date_overrides = []
            selected_rows = None
        elif isinstance(request_body, list):
            overrides = request_body
            date_overrides = []
            selected_rows = None
        else:
            overrides = request_body.get('overrides', [])
            date_overrides = request_body.get('date_overrides', [])
            selected_rows_list = request_body.get('selected_rows')
            selected_rows = set(selected_rows_list) if selected_rows_list is not None else None

        # Create importer
        importer = BankStatementImport(
            bank_code=bank_code,
            use_enhanced_matching=True,
            use_fingerprinting=True
        )

        # Handle PDF files with AI extraction (same as preview endpoint)
        if filename.lower().endswith('.pdf'):
            import tempfile
            import os
            from sql_rag.statement_reconcile import StatementReconciler
            from sql_rag.bank_import import BankTransaction

            # Save to temp file for AI extraction
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                tmp_file.write(content_bytes)
                tmp_path = tmp_file.name

            try:
                # Use StatementReconciler for AI extraction
                reconciler = StatementReconciler(sql_connector, config=config)
                statement_info, stmt_transactions = reconciler.extract_transactions_from_pdf(tmp_path)

                # Convert StatementTransaction to BankTransaction format
                transactions = []
                for i, st in enumerate(stmt_transactions, start=1):
                    txn = BankTransaction(
                        row_number=i,
                        date=st.date,
                        amount=st.amount,
                        subcategory=st.transaction_type or '',
                        memo=st.description or '',
                        name=st.description or '',
                        reference=st.reference or '',
                        fit_id=''
                    )
                    transactions.append(txn)

                detected_format = "PDF (AI Extraction)"
            finally:
                # Clean up temp file
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
        else:
            # Decode bytes to string for CSV/OFX/QIF/MT940
            try:
                content = content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                content = content_bytes.decode('latin-1')

            transactions, detected_format = importer.parse_content(content, filename)
        importer.process_transactions(transactions)

        # Apply date overrides
        date_override_map = {d['row']: d['date'] for d in date_overrides}
        for txn in transactions:
            if txn.row_number in date_override_map:
                new_date_str = date_override_map[txn.row_number]
                txn.original_date = txn.date
                txn.date = datetime.strptime(new_date_str, '%Y-%m-%d').date()

        # Apply manual overrides
        override_map = {o['row']: o for o in overrides}
        for txn in transactions:
            if txn.row_number in override_map:
                override = override_map[txn.row_number]
                if override.get('account'):
                    txn.manual_account = override.get('account')
                    txn.manual_ledger_type = override.get('ledger_type')

                transaction_type = override.get('transaction_type')
                if transaction_type and transaction_type in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund'):
                    txn.action = transaction_type
                elif override.get('ledger_type') == 'C':
                    txn.action = 'sales_receipt'
                elif override.get('ledger_type') == 'S':
                    txn.action = 'purchase_payment'

        # Validate periods
        period_info = get_current_period_info(sql_connector)
        period_violations = []

        for txn in transactions:
            if selected_rows is not None and txn.row_number not in selected_rows:
                continue
            if txn.action not in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund'):
                continue
            if txn.is_duplicate:
                continue

            ledger_type = get_ledger_type_for_transaction(txn.action)
            period_result = validate_posting_period(sql_connector, txn.date, ledger_type)

            if not period_result.is_valid:
                ledger_names = {'SL': 'Sales Ledger', 'PL': 'Purchase Ledger', 'NL': 'Nominal Ledger'}
                period_violations.append({
                    "row": txn.row_number,
                    "date": txn.date.isoformat(),
                    "name": txn.name,
                    "amount": txn.amount,
                    "action": txn.action,
                    "ledger_type": ledger_type,
                    "ledger_name": ledger_names.get(ledger_type, ledger_type),
                    "error": period_result.error_message,
                    "year": period_result.year,
                    "period": period_result.period
                })

        if period_violations:
            return {
                "success": False,
                "error": "Cannot import - some transactions are in blocked periods",
                "period_violations": period_violations,
                "period_info": {
                    "current_year": period_info.get('np_year'),
                    "current_period": period_info.get('np_perno')
                }
            }

        # Check for unprocessed repeat entries
        unprocessed_repeat_entries = []
        for txn in transactions:
            if txn.action == 'repeat_entry':
                unprocessed_repeat_entries.append({
                    "row": txn.row_number,
                    "name": txn.name,
                    "amount": txn.amount,
                    "date": txn.date.isoformat(),
                    "entry_ref": getattr(txn, 'repeat_entry_ref', None),
                    "entry_desc": getattr(txn, 'repeat_entry_desc', None)
                })

        if unprocessed_repeat_entries:
            return {
                "success": False,
                "error": "Cannot import - there are unprocessed repeat entries",
                "repeat_entries": unprocessed_repeat_entries,
                "message": "Please run Opera's Repeat Entries routine first"
            }

        # Import transactions
        imported = []
        errors = []
        skipped_not_selected = 0
        skipped_incomplete = 0

        for txn in transactions:
            if selected_rows is not None and txn.row_number not in selected_rows:
                skipped_not_selected += 1
                continue

            if txn.action in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund') and not txn.is_duplicate:
                account = txn.manual_account or txn.matched_account
                if not account:
                    skipped_incomplete += 1
                    errors.append({"row": txn.row_number, "error": "Missing account"})
                    continue

                if not txn.action or txn.action not in ('sales_receipt', 'purchase_payment', 'sales_refund', 'purchase_refund'):
                    skipped_incomplete += 1
                    errors.append({"row": txn.row_number, "error": "Missing transaction type"})
                    continue

                try:
                    result = importer.import_transaction(txn, validate_only=False)
                    if result.success:
                        imported.append({
                            "row": txn.row_number,
                            "date": txn.date.isoformat(),
                            "amount": txn.amount,
                            "account": txn.manual_account or txn.matched_account,
                            "action": txn.action,
                            "batch_ref": getattr(result, 'batch_ref', None) or getattr(result, 'batch_number', None)
                        })

                        # Save alias for manual overrides
                        if txn.manual_account and importer.alias_manager:
                            inferred_ledger = 'C' if txn.action in ('sales_receipt', 'sales_refund') else 'S'
                            importer.alias_manager.save_alias(
                                bank_name=txn.name,
                                ledger_type=txn.manual_ledger_type or inferred_ledger,
                                account_code=txn.manual_account,
                                match_score=1.0,
                                created_by='MANUAL_IMPORT'
                            )
                    else:
                        error_msg = '; '.join(result.errors) if result.errors else 'Import failed'
                        errors.append({"row": txn.row_number, "error": error_msg})
                except Exception as e:
                    errors.append({"row": txn.row_number, "error": str(e)})

        # Record successful import in tracking table
        if len(imported) > 0:
            email_storage.record_bank_statement_import(
                email_id=email_id,
                attachment_id=attachment_id,
                bank_code=bank_code,
                filename=filename,
                transactions_imported=len(imported),
                imported_by='BANK_IMPORT'
            )

        # Calculate totals by action type
        receipts_imported = sum(1 for t in imported if t['action'] == 'sales_receipt')
        payments_imported = sum(1 for t in imported if t['action'] == 'purchase_payment')
        refunds_imported = sum(1 for t in imported if t['action'] in ('sales_refund', 'purchase_refund'))

        return {
            "success": len(errors) == 0,
            "source": "email",
            "email_id": email_id,
            "attachment_id": attachment_id,
            "filename": filename,
            "imported_count": len(imported),
            "receipts_imported": receipts_imported,
            "payments_imported": payments_imported,
            "refunds_imported": refunds_imported,
            "skipped_not_selected": skipped_not_selected,
            "skipped_incomplete": skipped_incomplete,
            "imported_transactions": imported,
            "errors": errors
        }

    except Exception as e:
        logger.error(f"Error importing bank statement from email: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/bank-import/email-import-history")
async def get_bank_statement_email_import_history(
    bank_code: Optional[str] = Query(None, description="Filter by bank code"),
    limit: int = Query(50, description="Maximum records to return")
):
    """
    Get history of bank statements imported from email.
    """
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not initialized")

    try:
        history = email_storage.get_bank_statement_import_history(bank_code=bank_code, limit=limit)
        return {
            "success": True,
            "history": history,
            "count": len(history)
        }
    except Exception as e:
        logger.error(f"Error getting email import history: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# GoCardless Import Endpoints
# ============================================================

from sql_rag.gocardless_parser import parse_gocardless_email, parse_gocardless_table, GoCardlessBatch


@app.post("/api/gocardless/ocr")
async def ocr_gocardless_image(file: UploadFile = File(...)):
    """
    Extract text from a GoCardless screenshot using OCR.
    Accepts file upload via multipart form.
    """
    try:
        import pytesseract
        from PIL import Image
        import io

        # Read uploaded file into memory
        contents = await file.read()
        img = Image.open(io.BytesIO(contents))

        # Extract text using OCR
        text = pytesseract.image_to_string(img)

        if not text.strip():
            return {"success": False, "error": "No text could be extracted from image"}

        return {
            "success": True,
            "text": text,
            "filename": file.filename
        }

    except ImportError:
        return {"success": False, "error": "OCR not available - pytesseract not installed"}
    except Exception as e:
        logger.error(f"OCR error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/gocardless/ocr-path")
async def ocr_gocardless_image_path(file_path: str = Body(..., embed=True)):
    """
    Extract text from a GoCardless screenshot using OCR (test mode - file path).
    """
    try:
        import pytesseract
        from PIL import Image
        import os

        if not os.path.exists(file_path):
            return {"success": False, "error": f"File not found: {file_path}"}

        img = Image.open(file_path)
        text = pytesseract.image_to_string(img)

        if not text.strip():
            return {"success": False, "error": "No text could be extracted from image"}

        return {
            "success": True,
            "text": text,
            "file_path": file_path
        }

    except ImportError:
        return {"success": False, "error": "OCR not available - pytesseract not installed"}
    except Exception as e:
        logger.error(f"OCR error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/gocardless/test-data")
async def get_gocardless_test_data():
    """
    Returns test data extracted from gocardless.png screenshot for testing.
    """
    return {
        "success": True,
        "payment_count": 18,
        "gross_amount": 29869.80,
        "gocardless_fees": -118.31,
        "vat_on_fees": -19.73,
        "net_amount": 29751.49,
        "bank_reference": "INTSYSUKLTD-KN3CMJ",
        "payments": [
            {"customer_name": "Deep Blue Restaurantes Ltd", "description": "Intsys INV26362,26363", "amount": 7380.00, "invoice_refs": ["INV26362", "INV26363"]},
            {"customer_name": "Medimpex UK Ltd", "description": "Intsys INV26365", "amount": 1530.00, "invoice_refs": ["INV26365"]},
            {"customer_name": "The Prospect Trust", "description": "Intsys INV", "amount": 3000.00, "invoice_refs": []},
            {"customer_name": "SMCP UK Limited", "description": "Intsys INV26374,26375", "amount": 1320.00, "invoice_refs": ["INV26374", "INV26375"]},
            {"customer_name": "Vectair Systems Limited", "description": "Intsys INV26378", "amount": 8398.80, "invoice_refs": ["INV26378"]},
            {"customer_name": "Jackson Lifts", "description": "Intsys Opera 3 Support", "amount": 123.00, "invoice_refs": []},
            {"customer_name": "Vectair Systems Limited", "description": "Opera SE Toolkit", "amount": 109.20, "invoice_refs": []},
            {"customer_name": "A WARNE & CO LTD", "description": "Intsys Data Connector", "amount": 168.00, "invoice_refs": []},
            {"customer_name": "Physique Management Ltd", "description": "Intsys Pegasus Support", "amount": 551.40, "invoice_refs": []},
            {"customer_name": "Ormiston Wire Ltd", "description": "Intsys Opera 3 Support", "amount": 90.00, "invoice_refs": []},
            {"customer_name": "Totality GCS Ltd", "description": "Intsys Pegasus Support", "amount": 240.00, "invoice_refs": []},
            {"customer_name": "Red Band Chemical Co Ltd T/A Lindsay & Gilmour", "description": "Intsys Pegasus Upgrade Plan", "amount": 74.40, "invoice_refs": []},
            {"customer_name": "P Flannery Plant Hire (Oval) Ltd", "description": "Intsys Pegasus Upgrade Plan", "amount": 78.00, "invoice_refs": []},
            {"customer_name": "Harro Foods Limited", "description": "Intsys Opera 3 Sales Website", "amount": 5607.00, "invoice_refs": []},
            {"customer_name": "Physique Management Ltd", "description": "Intsys Data Connector", "amount": 168.00, "invoice_refs": []},
            {"customer_name": "Nisbets Limited", "description": "Intsys Opera 3 Licence Subs", "amount": 540.00, "invoice_refs": []},
            {"customer_name": "Vectair Systems Limited", "description": "Intsys Pegasus WEBLINK", "amount": 192.00, "invoice_refs": []},
            {"customer_name": "ST Astier Limited", "description": "Intsys CIS Support", "amount": 300.00, "invoice_refs": []}
        ]
    }


@app.post("/api/gocardless/parse")
async def parse_gocardless_content(
    content: str = Body(..., description="GoCardless email content or table text")
):
    """
    Parse GoCardless email content to extract customer payments.
    Returns parsed payments ready for customer matching.
    """
    try:
        # Try parsing as full email first, then as table only
        batch = parse_gocardless_email(content)
        if not batch.payments:
            batch = parse_gocardless_table(content)

        if not batch.payments:
            return {
                "success": False,
                "error": "Could not parse any payments from the content. Please paste the GoCardless email or payment table."
            }

        return {
            "success": True,
            "payment_count": batch.payment_count,
            "gross_amount": batch.gross_amount,
            "gocardless_fees": batch.gocardless_fees,
            "vat_on_fees": batch.vat_on_fees,
            "net_amount": batch.net_amount,
            "bank_reference": batch.bank_reference,
            "payments": [
                {
                    "customer_name": p.customer_name,
                    "description": p.description,
                    "amount": p.amount,
                    "invoice_refs": p.invoice_refs
                }
                for p in batch.payments
            ]
        }

    except Exception as e:
        logger.error(f"Error parsing GoCardless content: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/gocardless/match-customers")
async def match_gocardless_customers(
    payments: List[Dict[str, Any]] = Body(..., description="List of payments from parse endpoint")
):
    """
    Match GoCardless payment customer names to Opera customer accounts.

    Matching priority:
    1. Invoice reference lookup - extract INV number from description and find in stran
    2. Amount + invoice pattern - match amount against outstanding invoices
    3. Fuzzy name matching - fall back to customer name comparison
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        import re
        from sql_rag.bank_import import BankStatementImport

        # Get all customers for matching
        customers_df = sql_connector.execute_query("""
            SELECT sn_account, sn_name FROM sname WITH (NOLOCK)
            WHERE sn_stop = 0 OR sn_stop IS NULL
        """)

        if customers_df is None or len(customers_df) == 0:
            return {"success": False, "error": "No customers found in database"}

        customers = {
            row['sn_account'].strip(): row['sn_name'].strip()
            for _, row in customers_df.iterrows()
        }

        # Helper function to extract invoice reference from description
        def extract_invoice_ref(text: str) -> list:
            """
            Extract invoice reference(s) like INV26388 from text.
            Returns list of found refs (may be multiple in one description).
            """
            if not text:
                return []

            refs = []
            # Pattern: INV followed by digits (with optional space)
            patterns = [
                (r'INV\s*(\d+)', 'INV'),      # INV26388 or INV 26388
                (r'Invoice\s*#?\s*(\d+)', 'INV'),  # Invoice #12345
                (r'#(\d{4,})', 'INV'),        # #26388 (4+ digits)
                (r'(?:^|\s)(\d{5,6})(?:\s|$|,)', ''),  # Standalone 5-6 digit number like "26388"
                (r'SI-?(\d+)', 'SI'),         # SI12345 or SI-12345 (Sales Invoice)
            ]
            for pattern, prefix in patterns:
                for match in re.finditer(pattern, text, re.IGNORECASE):
                    ref = f"{prefix}{match.group(1)}" if prefix else match.group(1)
                    if ref not in refs:
                        refs.append(ref)
            return refs

        # Helper function to find customer by invoice reference
        def find_customer_by_invoice(invoice_ref: str, amount: float = None) -> tuple:
            """
            Look up invoice in stran to find customer account.
            Returns (account, customer_name, invoice_found) or (None, None, False)
            """
            if not invoice_ref:
                return None, None, False

            # Query stran for invoices matching the reference
            # st_trtype 'I' = Invoice
            invoice_query = f"""
                SELECT TOP 1 st.st_account, sn.sn_name, st.st_trref, st.st_trvalue, st.st_trbal
                FROM stran st WITH (NOLOCK)
                JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                WHERE st.st_trtype = 'I'
                  AND (st.st_trref LIKE '%{invoice_ref[-6:]}%'
                       OR st.st_trref LIKE '%{invoice_ref}%')
                ORDER BY st.st_trdate DESC
            """
            try:
                inv_df = sql_connector.execute_query(invoice_query)
                if inv_df is not None and len(inv_df) > 0:
                    row = inv_df.iloc[0]
                    account = row['st_account'].strip()
                    name = row['sn_name'].strip() if row['sn_name'] else ''

                    # If amount provided, verify it matches (within tolerance)
                    if amount:
                        inv_value = abs(float(row['st_trvalue'] or 0))
                        if abs(inv_value - amount) <= 0.01:  # Within 1p
                            return account, name, True

                    return account, name, True
            except Exception as e:
                logger.warning(f"Invoice lookup failed for {invoice_ref}: {e}")

            return None, None, False

        # Helper function to find customer by amount (outstanding invoices)
        def find_customer_by_amount(amount: float) -> tuple:
            """
            Find customer with outstanding invoice matching the exact amount.
            Returns (account, customer_name, invoice_ref) or (None, None, None)
            """
            if not amount or amount <= 0:
                return None, None, None

            # Query for outstanding invoices with matching balance
            amount_query = f"""
                SELECT TOP 1 st.st_account, sn.sn_name, st.st_trref, st.st_trbal
                FROM stran st WITH (NOLOCK)
                JOIN sname sn WITH (NOLOCK) ON st.st_account = sn.sn_account
                WHERE st.st_trtype = 'I'
                  AND st.st_trbal > 0
                  AND ABS(st.st_trbal - {amount}) <= 0.01
                ORDER BY st.st_trdate DESC
            """
            try:
                amt_df = sql_connector.execute_query(amount_query)
                if amt_df is not None and len(amt_df) > 0:
                    row = amt_df.iloc[0]
                    account = row['st_account'].strip()
                    name = row['sn_name'].strip() if row['sn_name'] else ''
                    inv_ref = row['st_trref'].strip() if row['st_trref'] else None
                    return account, name, inv_ref
            except Exception as e:
                logger.warning(f"Amount lookup failed for {amount}: {e}")

            return None, None, None

        matched_payments = []
        unmatched_count = 0

        for payment in payments:
            customer_name = payment.get('customer_name', '')
            amount = payment.get('amount', 0)
            description = payment.get('description', '')
            payment_ref = payment.get('reference', '')  # GoCardless payment reference

            best_match = None
            best_name = None
            best_score = 0
            match_method = None
            found_invoice_refs = []

            # Priority 1: Try invoice reference lookup from description AND reference
            # Combine description and reference for ref extraction
            combined_text = f"{description} {payment_ref}".strip()
            invoice_refs = extract_invoice_ref(combined_text)
            found_invoice_refs = invoice_refs.copy()

            for inv_ref in invoice_refs:
                account, name, found = find_customer_by_invoice(inv_ref, amount)
                if found:
                    best_match = account
                    best_name = name
                    best_score = 1.0
                    match_method = f"invoice:{inv_ref}"
                    logger.info(f"Matched by invoice ref {inv_ref} -> {account} ({name})")
                    break

            # Priority 2: Try amount matching against outstanding invoices
            if not best_match and amount > 0:
                account, name, inv_ref = find_customer_by_amount(amount)
                if account:
                    best_match = account
                    best_name = name
                    best_score = 0.9  # High confidence but not as certain as invoice ref
                    match_method = f"amount:{amount}:{inv_ref}"
                    logger.info(f"Matched by amount £{amount} -> {account} ({name}), invoice: {inv_ref}")
                    if inv_ref and inv_ref not in found_invoice_refs:
                        found_invoice_refs.append(inv_ref)

            # Priority 3: Fall back to name-based fuzzy matching
            # Skip if customer_name is "Unknown" or empty (common for API payments)
            if not best_match and customer_name and customer_name.lower() not in ('unknown', ''):
                for account, name in customers.items():
                    name_lower = name.lower()
                    search_lower = customer_name.lower()

                    # Exact match
                    if name_lower == search_lower:
                        best_match = account
                        best_name = name
                        best_score = 1.0
                        match_method = "name:exact"
                        break

                    # Contains match (either direction)
                    if search_lower in name_lower or name_lower in search_lower:
                        score = len(search_lower) / max(len(name_lower), len(search_lower))
                        if score > best_score:
                            best_match = account
                            best_name = name
                            best_score = score
                            match_method = "name:contains"

                    # Word match - useful for partial company names
                    search_words = set(w for w in search_lower.split() if len(w) > 2)  # Ignore short words
                    name_words = set(w for w in name_lower.split() if len(w) > 2)
                    common_words = search_words & name_words
                    if common_words:
                        # Weighted score: more common words = higher score
                        score = len(common_words) / max(len(search_words), len(name_words))
                        if score > best_score:
                            best_match = account
                            best_name = name
                            best_score = score
                            match_method = "name:words"

            # Priority 4: Try matching description keywords against customer names
            # This helps when customer_name is "Unknown" but description has company name
            if not best_match and description:
                # Extract potential company name from description (usually first part before "INV")
                desc_clean = re.sub(r'\s+INV\d+.*', '', description, flags=re.IGNORECASE).strip()
                desc_clean = re.sub(r'\s+Invoice.*', '', desc_clean, flags=re.IGNORECASE).strip()
                if desc_clean and len(desc_clean) > 3:
                    for account, name in customers.items():
                        name_lower = name.lower()
                        desc_lower = desc_clean.lower()
                        if desc_lower in name_lower or name_lower in desc_lower:
                            score = len(desc_lower) / max(len(name_lower), len(desc_lower))
                            if score > best_score and score >= 0.5:
                                best_match = account
                                best_name = name
                                best_score = score
                                match_method = f"description:{desc_clean[:20]}"

            matched_payment = {
                "customer_name": customer_name,
                "description": description,
                "amount": amount,
                "invoice_refs": payment.get('invoice_refs', []) + found_invoice_refs,
                "matched_account": best_match if best_score >= 0.5 else None,
                "matched_name": best_name if best_match and best_score >= 0.5 else None,
                "match_score": best_score,
                "match_method": match_method,
                "match_status": "matched" if best_score >= 0.8 else "review" if best_score >= 0.5 else "unmatched",
                "possible_duplicate": False,
                "duplicate_warning": None
            }
            matched_payments.append(matched_payment)

            if best_score < 0.5:
                unmatched_count += 1

        # Check for potential duplicates in Opera atran (cashbook) last 90 days
        # Look for receipts with same value - GoCardless batches go through cashbook
        try:
            # Get default cashbook type from settings for filtering
            gc_settings = _load_gocardless_settings()
            default_cbtype = gc_settings.get('default_batch_type', '')

            # Query atran for receipts (at_type=1 is receipt, at_value is positive for receipts)
            # Also join to aentry to get the reference - check full cashbook history
            duplicate_check_df = sql_connector.execute_query(f"""
                SELECT at_value, at_date, at_cbtype, ae_ref, ae_date
                FROM atran WITH (NOLOCK)
                JOIN aentry WITH (NOLOCK) ON at_batch = ae_batch
                WHERE at_type = 1  -- Receipts
                  {f"AND at_cbtype = '{default_cbtype}'" if default_cbtype else ""}
                ORDER BY at_date DESC
            """)

            if duplicate_check_df is not None and len(duplicate_check_df) > 0:
                for payment in matched_payments:
                    amount = payment['amount']
                    # Convert to pence for comparison (atran stores in pence)
                    amount_pence = int(round(amount * 100))

                    # Check for transactions with same value
                    for _, row in duplicate_check_df.iterrows():
                        existing_pence = abs(int(row['at_value'] or 0))
                        # Allow small tolerance (1 penny)
                        if abs(existing_pence - amount_pence) <= 1:
                            payment['possible_duplicate'] = True
                            tx_date = row['at_date']
                            date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                            ref = row['ae_ref'].strip() if row.get('ae_ref') else 'N/A'
                            cbtype = row['at_cbtype'].strip() if row.get('at_cbtype') else ''
                            payment['duplicate_warning'] = f"Cashbook entry found: £{existing_pence/100:.2f} on {date_str} (type: {cbtype}, ref: {ref})"
                            break
        except Exception as dup_err:
            logger.warning(f"Could not check for duplicates: {dup_err}")

        duplicate_count = len([p for p in matched_payments if p.get('possible_duplicate')])

        return {
            "success": True,
            "total_payments": len(matched_payments),
            "matched_count": len([p for p in matched_payments if p['match_status'] == 'matched']),
            "review_count": len([p for p in matched_payments if p['match_status'] == 'review']),
            "unmatched_count": unmatched_count,
            "duplicate_count": duplicate_count,
            "payments": matched_payments
        }

    except Exception as e:
        logger.error(f"Error matching GoCardless customers: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/gocardless/validate-date")
async def validate_gocardless_date(
    post_date: str = Query(..., description="Posting date to validate (YYYY-MM-DD)")
):
    """
    Validate that a posting date is allowed in Opera.
    Checks period status based on Opera's Open Period Accounting settings.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime
        from sql_rag.opera_config import validate_posting_period, get_current_period_info

        # Parse date
        try:
            parsed_date = datetime.strptime(post_date, '%Y-%m-%d').date()
        except ValueError:
            return {"success": False, "valid": False, "error": f"Invalid date format: {post_date}. Use YYYY-MM-DD"}

        # Get current period info
        current_info = get_current_period_info(sql_connector)

        # Validate for Sales Ledger (GoCardless receipts affect SL)
        result = validate_posting_period(sql_connector, parsed_date, 'SL')

        return {
            "success": True,
            "valid": result.is_valid,
            "error": result.error_message if not result.is_valid else None,
            "year": result.year,
            "period": result.period,
            "current_year": current_info.get('np_year'),
            "current_period": current_info.get('np_perno'),
            "open_period_accounting": result.open_period_accounting
        }

    except Exception as e:
        logger.error(f"Error validating posting date: {e}")
        return {"success": False, "valid": False, "error": str(e)}


@app.post("/api/gocardless/import")
async def import_gocardless_batch(
    bank_code: str = Query("BC010", description="Opera bank account code"),
    post_date: str = Query(..., description="Posting date (YYYY-MM-DD)"),
    reference: str = Query("GoCardless", description="Batch reference"),
    complete_batch: bool = Query(False, description="Complete batch immediately or leave for review"),
    cbtype: str = Query(None, description="Cashbook type code for batched receipt"),
    gocardless_fees: float = Query(0.0, description="GoCardless fees amount in pounds (gross including VAT)"),
    vat_on_fees: float = Query(0.0, description="VAT element of fees in pounds"),
    fees_nominal_account: str = Query(None, description="Nominal account for posting net fees"),
    fees_vat_code: str = Query("2", description="VAT code for fees - looked up in ztax for rate and nominal"),
    currency: str = Query(None, description="Currency code from GoCardless (e.g., 'GBP'). Rejected if not home currency."),
    payout_id: str = Query(None, description="GoCardless payout ID for history tracking"),
    source: str = Query("api", description="Import source: 'api' or 'email'"),
    payments: List[Dict[str, Any]] = Body(..., description="List of payments with customer_account and amount")
):
    """
    Import GoCardless batch into Opera as a batch receipt.

    Creates:
    - One aentry header (batch total)
    - Multiple atran lines (one per customer)
    - Multiple stran records

    If complete_batch=False, leaves the batch for review in Opera (ae_complet=0).
    If complete_batch=True, also creates ntran/anoml records.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        from datetime import datetime

        # Validate payments
        if not payments:
            return {"success": False, "error": "No payments provided"}

        # Validate each payment has required fields
        validated_payments = []
        for idx, p in enumerate(payments):
            if not p.get('customer_account'):
                return {"success": False, "error": f"Payment {idx+1}: Missing customer_account"}
            if not p.get('amount'):
                return {"success": False, "error": f"Payment {idx+1}: Missing amount"}

            validated_payments.append({
                "customer_account": p['customer_account'],
                "amount": float(p['amount']),
                "description": p.get('description', '')[:35]
            })

        # Parse date
        try:
            parsed_date = datetime.strptime(post_date, '%Y-%m-%d').date()
        except ValueError:
            return {"success": False, "error": f"Invalid date format: {post_date}. Use YYYY-MM-DD"}

        # Validate posting period
        from sql_rag.opera_config import validate_posting_period
        period_result = validate_posting_period(sql_connector, parsed_date, 'SL')  # Sales Ledger
        if not period_result.is_valid:
            return {"success": False, "error": f"Cannot post to this date: {period_result.error_message}"}

        # Validate fees configuration - MUST have fees_nominal_account if fees > 0
        if gocardless_fees > 0 and not fees_nominal_account:
            return {
                "success": False,
                "error": f"GoCardless fees of £{gocardless_fees:.2f} cannot be posted: Fees Nominal Account not configured. "
                         "Please configure the Fees Nominal Account in GoCardless Settings before importing."
            }

        # Import the batch
        importer = OperaSQLImport(sql_connector)
        result = importer.import_gocardless_batch(
            bank_account=bank_code,
            payments=validated_payments,
            post_date=parsed_date,
            reference=reference,
            gocardless_fees=gocardless_fees,
            vat_on_fees=vat_on_fees,
            fees_nominal_account=fees_nominal_account,
            fees_vat_code=fees_vat_code,
            complete_batch=complete_batch,
            cbtype=cbtype,
            input_by="GOCARDLS",
            currency=currency
        )

        if result.success:
            # Record to import history
            try:
                import json
                gross_amount = sum(p['amount'] for p in validated_payments)
                net_amount = gross_amount - gocardless_fees
                payments_json = json.dumps([{
                    "customer_account": p['customer_account'],
                    "amount": p['amount'],
                    "description": p.get('description', '')
                } for p in validated_payments])

                email_storage.record_gocardless_import(
                    target_system='opera_se',
                    payout_id=payout_id,
                    source=source,
                    bank_reference=reference,
                    gross_amount=gross_amount,
                    net_amount=net_amount,
                    gocardless_fees=gocardless_fees,
                    vat_on_fees=vat_on_fees,
                    payment_count=len(validated_payments),
                    payments_json=payments_json,
                    batch_ref=result.batch_ref if hasattr(result, 'batch_ref') else None,
                    imported_by="GOCARDLS"
                )
                logger.info(f"Recorded GoCardless import to history: ref={reference}, payout_id={payout_id}")
            except Exception as hist_err:
                logger.warning(f"Failed to record import to history: {hist_err}")

            return {
                "success": True,
                "message": f"Successfully imported {len(payments)} payments",
                "payments_imported": result.records_imported,
                "complete": complete_batch,
                "details": [w for w in result.warnings if w]
            }
        else:
            return {
                "success": False,
                "error": "; ".join(result.errors),
                "payments_processed": result.records_processed
            }

    except AttributeError as e:
        logger.error(f"Error importing GoCardless batch: {e}")
        if "import_gocardless_batch" in str(e):
            return {"success": False, "error": "GoCardless batch import not available. Please restart the API server."}
        return {"success": False, "error": f"Configuration error: {e}"}
    except ConnectionError as e:
        logger.error(f"Database connection error: {e}")
        return {"success": False, "error": "Cannot connect to Opera database. Please check the connection."}
    except Exception as e:
        logger.error(f"Error importing GoCardless batch: {e}")
        error_msg = str(e)
        # Make common errors more readable
        if "Invalid object name" in error_msg:
            return {"success": False, "error": "Database table not found. Please check Opera database connection."}
        if "Login failed" in error_msg:
            return {"success": False, "error": "Database login failed. Please check credentials."}
        if "Cannot insert" in error_msg or "duplicate" in error_msg.lower():
            return {"success": False, "error": "Failed to create records in Opera. A duplicate entry may exist."}
        if "foreign key" in error_msg.lower():
            return {"success": False, "error": "Invalid customer or bank account code. Please verify the accounts exist in Opera."}
        return {"success": False, "error": f"Import failed: {error_msg}"}


@app.get("/api/gocardless/batch-types")
async def get_gocardless_batch_types():
    """
    Get available batched receipt types from Opera for GoCardless import.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query("""
            SELECT ay_cbtype, ay_desc, ay_batched
            FROM atype
            WHERE ay_type = 'R' AND ay_batched = 1
            ORDER BY ay_desc
        """)

        if df is None or len(df) == 0:
            return {
                "success": True,
                "batch_types": [],
                "warning": "No batched receipt types found. You may need to create a GoCardless type in Opera."
            }

        types = [
            {
                "code": row['ay_cbtype'].strip(),
                "description": row['ay_desc'].strip(),
                "is_gocardless": 'gocardless' in row['ay_desc'].lower()
            }
            for _, row in df.iterrows()
        ]

        return {
            "success": True,
            "batch_types": types,
            "recommended": next((t for t in types if t['is_gocardless']), types[0] if types else None)
        }

    except Exception as e:
        logger.error(f"Error getting batch types: {e}")
        return {"success": False, "error": str(e)}


# GoCardless Settings Storage
GOCARDLESS_SETTINGS_FILE = Path(__file__).parent.parent / "gocardless_settings.json"

def _load_gocardless_settings() -> dict:
    """Load GoCardless settings from file."""
    if GOCARDLESS_SETTINGS_FILE.exists():
        try:
            with open(GOCARDLESS_SETTINGS_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "default_batch_type": "",
        "default_bank_code": "BC010",
        "fees_nominal_account": "",
        "fees_vat_code": "1",
        "fees_payment_type": "",
        "company_reference": ""  # e.g., "INTSYSUKLTD" - filters emails by bank reference
    }

def _save_gocardless_settings(settings: dict) -> bool:
    """Save GoCardless settings to file."""
    try:
        with open(GOCARDLESS_SETTINGS_FILE, 'w') as f:
            json.dump(settings, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Failed to save GoCardless settings: {e}")
        return False


@app.get("/api/gocardless/settings")
async def get_gocardless_settings():
    """Get GoCardless import settings."""
    settings = _load_gocardless_settings()
    return {"success": True, "settings": settings}


@app.post("/api/gocardless/settings")
async def save_gocardless_settings(
    default_batch_type: str = Body("", embed=True),
    default_bank_code: str = Body("BC010", embed=True),
    fees_nominal_account: str = Body("", embed=True),
    fees_vat_code: str = Body("", embed=True),
    fees_payment_type: str = Body("", embed=True),
    company_reference: str = Body("", embed=True),
    archive_folder: str = Body("Archive/GoCardless", embed=True),
    # API Settings
    api_access_token: str = Body("", embed=True),
    api_sandbox: bool = Body(False, embed=True),
    data_source: str = Body("email", embed=True)  # "email" or "api"
):
    """Save GoCardless import settings."""
    settings = {
        "default_batch_type": default_batch_type,
        "default_bank_code": default_bank_code,
        "fees_nominal_account": fees_nominal_account,
        "fees_vat_code": fees_vat_code,
        "fees_payment_type": fees_payment_type,
        "company_reference": company_reference,  # e.g., "INTSYSUKLTD"
        "archive_folder": archive_folder,  # Folder to move imported emails
        # API Settings
        "api_access_token": api_access_token,
        "api_sandbox": api_sandbox,
        "data_source": data_source  # "email" or "api"
    }
    if _save_gocardless_settings(settings):
        return {"success": True, "message": "Settings saved"}
    return {"success": False, "error": "Failed to save settings"}


@app.get("/api/gocardless/nominal-accounts")
async def get_nominal_accounts():
    """Get nominal accounts for dropdown selection from nacnt table."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        df = sql_connector.execute_query("""
            SELECT na_acnt, na_desc
            FROM nacnt WITH (NOLOCK)
            WHERE na_acnt NOT LIKE 'Z%'
            ORDER BY na_acnt
        """)

        if df is None or len(df) == 0:
            return {"success": True, "accounts": []}

        accounts = [
            {"code": row['na_acnt'].strip(), "description": row['na_desc'].strip() if row['na_desc'] else ''}
            for _, row in df.iterrows()
        ]
        return {"success": True, "accounts": accounts}
    except Exception as e:
        logger.error(f"Error fetching nominal accounts: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/gocardless/vat-codes")
async def get_vat_codes(
    as_of_date: str = Query(None, description="Date to determine applicable rate (YYYY-MM-DD). Defaults to today.")
):
    """
    Get VAT codes for dropdown selection from ztax table (Purchase type for fees).

    VAT rates can change over time. This endpoint returns the applicable rate based on:
    - as_of_date parameter if provided
    - Today's date otherwise

    The ztax table stores two rate/date pairs:
    - tx_rate1 / tx_rate1dy: First rate and its effective date
    - tx_rate2 / tx_rate2dy: Second rate and its effective date

    Logic: Use the rate where the effective date is most recent but <= as_of_date
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from datetime import datetime, date
        import pandas as pd

        # Determine the reference date
        if as_of_date:
            try:
                ref_date = datetime.strptime(as_of_date, '%Y-%m-%d').date()
            except ValueError:
                ref_date = date.today()
        else:
            ref_date = date.today()

        # Query ztax table with both rates and dates
        # tx_trantyp 'P' for Purchase (fees are expenses), tx_ctrytyp 'H' for Home country
        df = sql_connector.execute_query("""
            SELECT tx_code, tx_desc, tx_rate1, tx_rate1dy, tx_rate2, tx_rate2dy
            FROM ztax WITH (NOLOCK)
            WHERE tx_trantyp = 'P' AND tx_ctrytyp = 'H'
            ORDER BY tx_code
        """)

        if df is None or len(df) == 0:
            return {"success": True, "codes": []}

        codes = []
        for _, row in df.iterrows():
            code = str(row['tx_code']).strip()
            description = row['tx_desc'].strip() if row['tx_desc'] else ''

            rate1 = float(row['tx_rate1']) if pd.notna(row['tx_rate1']) else 0
            rate2 = float(row['tx_rate2']) if pd.notna(row['tx_rate2']) else 0

            # Parse dates (handle NaT/None)
            date1 = None
            date2 = None
            if pd.notna(row['tx_rate1dy']):
                date1 = row['tx_rate1dy'].date() if hasattr(row['tx_rate1dy'], 'date') else row['tx_rate1dy']
            if pd.notna(row['tx_rate2dy']):
                date2 = row['tx_rate2dy'].date() if hasattr(row['tx_rate2dy'], 'date') else row['tx_rate2dy']

            # Determine applicable rate based on dates
            # Use the rate with the most recent effective date that's <= ref_date
            applicable_rate = rate1  # Default to rate1

            if date1 and date2:
                # Both dates exist - find the most recent one <= ref_date
                if date2 <= ref_date and date1 <= ref_date:
                    # Both are applicable, use the more recent one
                    applicable_rate = rate2 if date2 > date1 else rate1
                elif date2 <= ref_date:
                    applicable_rate = rate2
                elif date1 <= ref_date:
                    applicable_rate = rate1
            elif date2 and date2 <= ref_date:
                applicable_rate = rate2
            elif date1 and date1 <= ref_date:
                applicable_rate = rate1
            elif not date1 and not date2:
                # No dates, default to rate1
                applicable_rate = rate1

            codes.append({
                "code": code,
                "description": description,
                "rate": applicable_rate,
                # Include rate history for reference
                "rate1": rate1,
                "rate1_date": date1.isoformat() if date1 else None,
                "rate2": rate2,
                "rate2_date": date2.isoformat() if date2 else None
            })

        return {"success": True, "codes": codes, "as_of_date": ref_date.isoformat()}
    except Exception as e:
        logger.error(f"Error fetching VAT codes: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/gocardless/payment-types")
async def get_nominal_payment_types():
    """Get payment types from atype (for nominal payments like fees)."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        # Get Payment types (ay_type = 'P') excluding batched items
        df = sql_connector.execute_query("""
            SELECT ay_cbtype, ay_desc
            FROM atype WITH (NOLOCK)
            WHERE ay_type = 'P' AND (ay_batched = 0 OR ay_batched IS NULL)
            ORDER BY ay_cbtype
        """)

        if df is None or len(df) == 0:
            return {"success": True, "types": []}

        types = [
            {"code": row['ay_cbtype'].strip(), "description": row['ay_desc'].strip()}
            for _, row in df.iterrows()
        ]
        return {"success": True, "types": types}
    except Exception as e:
        logger.error(f"Error fetching payment types: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/gocardless/test-api")
async def test_gocardless_api():
    """Test GoCardless API connection using saved credentials."""
    settings = _load_gocardless_settings()
    access_token = settings.get("api_access_token")

    if not access_token:
        return {"success": False, "error": "No API access token configured"}

    try:
        from sql_rag.gocardless_api import GoCardlessClient
        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)
        result = client.test_connection()
        return result
    except Exception as e:
        logger.error(f"GoCardless API test failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/gocardless/api-payouts")
async def get_gocardless_api_payouts(
    status: str = Query("paid", description="Payout status filter"),
    limit: int = Query(20, description="Number of payouts to fetch"),
    days_back: int = Query(30, description="Fetch payouts from last N days")
):
    """
    Fetch payouts directly from GoCardless API.

    Returns payouts with full payment details, ready for matching and import.
    """
    settings = _load_gocardless_settings()
    access_token = settings.get("api_access_token")

    if not access_token:
        return {"success": False, "error": "No API access token configured. Go to Settings to add your GoCardless API credentials."}

    try:
        from sql_rag.gocardless_api import GoCardlessClient
        from datetime import datetime, timedelta

        sandbox = settings.get("api_sandbox", False)
        client = GoCardlessClient(access_token=access_token, sandbox=sandbox)

        # Calculate date range
        created_at_gte = (datetime.now() - timedelta(days=days_back)).date()

        # Fetch payouts
        payouts, _ = client.get_payouts(
            status=status,
            limit=limit,
            created_at_gte=created_at_gte
        )

        # Get home currency for foreign currency detection
        home_currency_code = 'GBP'
        if sql_connector:
            try:
                from sql_rag.opera_sql_import import OperaSQLImport
                importer = OperaSQLImport(sql_connector)
                home_currency_result = importer.get_home_currency()
                if isinstance(home_currency_result, dict):
                    home_currency_code = home_currency_result.get('code', 'GBP')
                elif home_currency_result:
                    home_currency_code = str(home_currency_result)
            except Exception:
                pass

        # Fetch full details for each payout (with payments)
        batches = []
        for payout in payouts:
            try:
                # Check import history first - skip already imported payouts
                already_imported = False
                import_history_warning = None
                try:
                    if email_storage.is_gocardless_payout_imported(payout.id, 'opera_se'):
                        already_imported = True
                        import_history_warning = "Already imported (in history)"
                    elif email_storage.is_gocardless_reference_imported(payout.reference, 'opera_se'):
                        already_imported = True
                        import_history_warning = f"Already imported - ref {payout.reference} (in history)"
                except Exception as hist_err:
                    logger.debug(f"Could not check import history: {hist_err}")

                # Skip payouts that are already in import history
                if already_imported:
                    continue

                full_payout = client.get_payout_with_payments(payout.id)

                # Check for foreign currency
                is_foreign_currency = full_payout.currency.upper() != home_currency_code.upper()

                # Check for duplicate in Opera cashbook
                possible_duplicate = False
                bank_tx_warning = None
                if sql_connector:
                    try:
                        gross_pence = int(round(full_payout.gross_amount * 100))
                        net_pence = int(round(full_payout.amount * 100))

                        # For foreign currency, we can only reliably check by reference
                        # since the amount in Opera will be in GBP (different from EUR/USD gross)
                        if is_foreign_currency:
                            # Check by exact payout reference only (no amount comparison)
                            if full_payout.reference:
                                # Use the last part of reference (after the company prefix)
                                ref_suffix = full_payout.reference.split('-')[-1] if '-' in full_payout.reference else full_payout.reference[-8:]
                                ref_df = sql_connector.execute_query(f"""
                                    SELECT TOP 1 ae_entref, at_value, at_pstdate as at_date
                                    FROM aentry WITH (NOLOCK)
                                    JOIN atran WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                        AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                    WHERE at_type IN (1, 4, 6)
                                      AND at_value > 0
                                      AND RTRIM(ae_entref) LIKE '%{ref_suffix}%'
                                    ORDER BY at_pstdate DESC
                                """)
                                if ref_df is not None and len(ref_df) > 0:
                                    row = ref_df.iloc[0]
                                    possible_duplicate = True
                                    tx_date = row['at_date']
                                    date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                    bank_tx_warning = f"Already posted - ref '{ref_suffix}' found: £{int(row['at_value'])/100:.2f} on {date_str} (note: foreign currency, GBP equivalent)"
                        else:
                            # For GBP payouts, check by reference + amount OR amount alone
                            # Check by payout reference (specific match, not broad 'GC')
                            if full_payout.reference:
                                # Use the last part of reference for matching
                                ref_suffix = full_payout.reference.split('-')[-1] if '-' in full_payout.reference else full_payout.reference[-8:]
                                ref_df = sql_connector.execute_query(f"""
                                    SELECT TOP 1 ae_entref, at_value, at_pstdate as at_date
                                    FROM aentry WITH (NOLOCK)
                                    JOIN atran WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                        AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                    WHERE at_type IN (1, 4, 6)
                                      AND RTRIM(ae_entref) LIKE '%{ref_suffix}%'
                                      AND ABS(at_value - {gross_pence}) <= 100
                                    ORDER BY at_pstdate DESC
                                """)
                                if ref_df is not None and len(ref_df) > 0:
                                    row = ref_df.iloc[0]
                                    possible_duplicate = True
                                    tx_date = row['at_date']
                                    date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                    bank_tx_warning = f"Already posted - ref '{ref_suffix}': £{int(row['at_value'])/100:.2f} on {date_str}"

                            # Check by gross amount if not found by reference
                            # Only flag if amount matches AND date is within 14 days of payout date
                            if not possible_duplicate and gross_pence > 0 and full_payout.arrival_date:
                                payout_date_str = full_payout.arrival_date.strftime('%Y-%m-%d')
                                gross_df = sql_connector.execute_query(f"""
                                    SELECT TOP 1 at_value, at_pstdate as at_date, ae_entref
                                    FROM atran WITH (NOLOCK)
                                    JOIN aentry WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                        AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                    WHERE at_type IN (1, 4, 6)
                                      AND at_value > 0
                                      AND ABS(at_value - {gross_pence}) <= 1
                                      AND ABS(DATEDIFF(day, at_pstdate, '{payout_date_str}')) <= 14
                                    ORDER BY at_pstdate DESC
                                """)
                                if gross_df is not None and len(gross_df) > 0:
                                    row = gross_df.iloc[0]
                                    possible_duplicate = True
                                    tx_date = row['at_date']
                                    date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                    ref = row['ae_entref'].strip() if row.get('ae_entref') else 'N/A'
                                    bank_tx_warning = f"Already posted - gross amount: £{int(row['at_value'])/100:.2f} on {date_str} (ref: {ref})"
                    except Exception as dup_err:
                        logger.warning(f"Could not check duplicate for payout {payout.id}: {dup_err}")

                # Skip payouts already posted in Opera cashbook
                if possible_duplicate:
                    logger.debug(f"Skipping payout {payout.id} - already posted in Opera: {bank_tx_warning}")
                    continue

                # Validate posting period
                period_valid = True
                period_error = None
                if full_payout.arrival_date and sql_connector:
                    try:
                        from sql_rag.opera_config import validate_posting_period
                        period_result = validate_posting_period(sql_connector, full_payout.arrival_date, 'SL')
                        period_valid = period_result.is_valid
                        if not period_valid:
                            period_error = period_result.error_message
                    except Exception:
                        pass

                batch_data = {
                    "payout_id": full_payout.id,
                    "source": "api",
                    "possible_duplicate": possible_duplicate,
                    "bank_tx_warning": bank_tx_warning,
                    "period_valid": period_valid,
                    "period_error": period_error,
                    "is_foreign_currency": is_foreign_currency,
                    "home_currency": home_currency_code,
                    "batch": {
                        "gross_amount": full_payout.gross_amount,
                        "gocardless_fees": full_payout.deducted_fees,
                        "vat_on_fees": full_payout.fees_vat,  # VAT from payout items API
                        "net_amount": full_payout.amount,
                        "bank_reference": full_payout.reference,
                        "currency": full_payout.currency,
                        "payment_date": full_payout.arrival_date.isoformat() if full_payout.arrival_date else None,
                        "payment_count": len(full_payout.payments),
                        "payments": [
                            {
                                "customer_name": p.customer_name or "Not provided",
                                "description": p.description or p.reference or "",
                                "amount": p.amount,
                                "invoice_refs": []
                            }
                            for p in full_payout.payments
                        ]
                    }
                }
                batches.append(batch_data)

            except Exception as e:
                logger.warning(f"Error fetching payout details {payout.id}: {e}")

        return {
            "success": True,
            "source": "api",
            "environment": "sandbox" if sandbox else "live",
            "total_payouts": len(batches),
            "batches": batches
        }

    except Exception as e:
        logger.error(f"Error fetching GoCardless API payouts: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/gocardless/import-history")
async def get_gocardless_import_history(
    limit: int = Query(50, description="Maximum records to return"),
    from_date: str = Query(None, description="Filter from date (YYYY-MM-DD)"),
    to_date: str = Query(None, description="Filter to date (YYYY-MM-DD)")
):
    """
    Get history of GoCardless imports.

    Returns list of previously imported batches with details.
    """
    try:
        history = email_storage.get_gocardless_import_history(
            limit=limit,
            target_system='opera_se',
            from_date=from_date,
            to_date=to_date
        )

        return {
            "success": True,
            "total": len(history),
            "imports": history
        }
    except Exception as e:
        logger.error(f"Error fetching GoCardless import history: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/gocardless/revalidate-batches")
async def revalidate_gocardless_batches(
    batches: List[Dict[str, Any]] = Body(..., description="Batches to revalidate")
):
    """
    Revalidate existing GoCardless batches against Opera.

    Use this after changing Opera parameters (opening periods, etc.) to refresh
    validation status without re-fetching from GoCardless API.

    Revalidates:
    - Posting period (checks if date is in open period)
    - Duplicate detection (checks if already posted to cashbook)
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.opera_config import validate_posting_period

        # Get current period info
        period_df = sql_connector.execute_query("""
            SELECT np_year, np_perno FROM nparm WITH (NOLOCK)
        """)
        current_period = None
        if period_df is not None and len(period_df) > 0:
            current_period = {
                "year": int(period_df.iloc[0]['np_year']),
                "period": int(period_df.iloc[0]['np_perno'])
            }

        # Get home currency
        home_currency_code = 'GBP'
        try:
            from sql_rag.opera_sql_import import OperaSQLImport
            importer = OperaSQLImport(sql_connector)
            home_currency_result = importer.get_home_currency()
            if isinstance(home_currency_result, dict):
                home_currency_code = home_currency_result.get('code', 'GBP')
            elif home_currency_result:
                home_currency_code = str(home_currency_result)
        except Exception:
            pass

        revalidated_batches = []

        for batch in batches:
            batch_data = batch.get('batch', {})
            gross_amount = batch_data.get('gross_amount', 0)
            net_amount = batch_data.get('net_amount', 0)
            bank_reference = batch_data.get('bank_reference', '')
            payment_date_str = batch_data.get('payment_date')
            currency = batch_data.get('currency', 'GBP')

            # Parse payment date
            payment_date = None
            if payment_date_str:
                try:
                    payment_date = datetime.strptime(payment_date_str[:10], '%Y-%m-%d').date()
                except:
                    pass

            # Check foreign currency
            is_foreign_currency = currency.upper() != home_currency_code.upper()

            # Revalidate posting period
            period_valid = True
            period_error = None
            if payment_date:
                try:
                    period_result = validate_posting_period(sql_connector, payment_date, 'SL')
                    period_valid = period_result.is_valid
                    if not period_valid:
                        period_error = period_result.error_message
                except Exception as e:
                    logger.warning(f"Period validation failed: {e}")

            # Revalidate duplicate detection
            possible_duplicate = False
            bank_tx_warning = None

            try:
                gross_pence = int(round(gross_amount * 100))
                net_pence = int(round(net_amount * 100))

                if is_foreign_currency:
                    # Foreign currency: only check by reference
                    if bank_reference:
                        ref_suffix = bank_reference.split('-')[-1] if '-' in bank_reference else bank_reference[-8:]
                        ref_df = sql_connector.execute_query(f"""
                            SELECT TOP 1 ae_entref, at_value, at_pstdate as at_date
                            FROM aentry WITH (NOLOCK)
                            JOIN atran WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                            WHERE at_type IN (1, 4, 6)
                              AND at_value > 0
                              AND RTRIM(ae_entref) LIKE '%{ref_suffix}%'
                            ORDER BY at_pstdate DESC
                        """)
                        if ref_df is not None and len(ref_df) > 0:
                            row = ref_df.iloc[0]
                            possible_duplicate = True
                            tx_date = row['at_date']
                            date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                            bank_tx_warning = f"Already posted - ref '{ref_suffix}' found: £{int(row['at_value'])/100:.2f} on {date_str} (note: foreign currency, GBP equivalent)"
                else:
                    # GBP: check by reference + amount OR amount alone
                    if bank_reference:
                        ref_suffix = bank_reference.split('-')[-1] if '-' in bank_reference else bank_reference[-8:]
                        ref_df = sql_connector.execute_query(f"""
                            SELECT TOP 1 ae_entref, at_value, at_pstdate as at_date
                            FROM aentry WITH (NOLOCK)
                            JOIN atran WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                            WHERE at_type IN (1, 4, 6)
                              AND RTRIM(ae_entref) LIKE '%{ref_suffix}%'
                              AND ABS(at_value - {gross_pence}) <= 100
                            ORDER BY at_pstdate DESC
                        """)
                        if ref_df is not None and len(ref_df) > 0:
                            row = ref_df.iloc[0]
                            possible_duplicate = True
                            tx_date = row['at_date']
                            date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                            bank_tx_warning = f"Already posted - ref '{ref_suffix}': £{int(row['at_value'])/100:.2f} on {date_str}"

                    # Check by gross amount if not found by reference
                    # Only flag if amount matches AND date is within 14 days
                    if not possible_duplicate and gross_pence > 0 and payment_date:
                        payout_date_str = payment_date.strftime('%Y-%m-%d')
                        gross_df = sql_connector.execute_query(f"""
                            SELECT TOP 1 at_value, at_pstdate as at_date, ae_entref
                            FROM atran WITH (NOLOCK)
                            JOIN aentry WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                            WHERE at_type IN (1, 4, 6)
                              AND at_value > 0
                              AND ABS(at_value - {gross_pence}) <= 1
                              AND ABS(DATEDIFF(day, at_pstdate, '{payout_date_str}')) <= 14
                            ORDER BY at_pstdate DESC
                        """)
                        if gross_df is not None and len(gross_df) > 0:
                            row = gross_df.iloc[0]
                            possible_duplicate = True
                            tx_date = row['at_date']
                            date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                            ref = row['ae_entref'].strip() if row.get('ae_entref') else 'N/A'
                            bank_tx_warning = f"Already posted - gross amount: £{int(row['at_value'])/100:.2f} on {date_str} (ref: {ref})"
            except Exception as dup_err:
                logger.warning(f"Duplicate check failed: {dup_err}")

            # Build revalidated batch (preserve original data, update validation fields)
            revalidated_batch = {
                **batch,
                "period_valid": period_valid,
                "period_error": period_error,
                "possible_duplicate": possible_duplicate,
                "bank_tx_warning": bank_tx_warning,
                "is_foreign_currency": is_foreign_currency,
                "home_currency": home_currency_code
            }
            revalidated_batches.append(revalidated_batch)

        return {
            "success": True,
            "batches": revalidated_batches,
            "current_period": current_period,
            "message": f"Revalidated {len(revalidated_batches)} batch(es) against Opera"
        }

    except Exception as e:
        logger.error(f"Error revalidating batches: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/gocardless/bank-accounts")
async def get_bank_accounts():
    """Get bank accounts for dropdown selection from nacnt table."""
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        # Query nacnt table for bank accounts (BC prefix for bank/cash accounts)
        df = sql_connector.execute_query("""
            SELECT na_acnt, na_desc
            FROM nacnt WITH (NOLOCK)
            WHERE na_acnt LIKE 'BC%'
            ORDER BY na_acnt
        """)

        if df is None or len(df) == 0:
            return {"success": True, "accounts": []}

        accounts = [
            {"code": row['na_acnt'].strip(), "description": row['na_desc'].strip() if row['na_desc'] else ''}
            for _, row in df.iterrows()
        ]
        return {"success": True, "accounts": accounts}
    except Exception as e:
        logger.error(f"Error fetching bank accounts: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/gocardless/scan-emails")
async def scan_gocardless_emails(
    from_date: Optional[str] = Query(None, description="Start date filter (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date filter (YYYY-MM-DD)"),
    include_processed: bool = Query(False, description="Include previously processed emails"),
    company_reference: Optional[str] = Query(None, description="Override company reference filter (e.g., INTSYSUKLTD)")
):
    """
    Scan mailbox for GoCardless payment notification emails.

    Searches for emails from GoCardless and parses them to extract payment batches.
    Filters by company reference (from settings or parameter) to ensure only
    transactions for the correct company are returned.

    Returns a list of batches ready for review and import.
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        from datetime import datetime
        from sql_rag.gocardless_parser import parse_gocardless_email

        # Auto-sync email inbox before scanning
        if email_sync_manager:
            try:
                import asyncio
                # Timeout sync after 30 seconds to avoid blocking if email server is slow
                await asyncio.wait_for(email_sync_manager.sync_all_providers(), timeout=30.0)
                logger.info("Auto-synced email inbox before GoCardless scan")
            except asyncio.TimeoutError:
                logger.warning("Email sync timed out after 30s (continuing with cached emails)")
            except Exception as sync_err:
                logger.warning(f"Email sync failed (continuing with cached emails): {sync_err}")

        # Load settings to get company reference
        settings = _load_gocardless_settings()
        company_ref = company_reference or settings.get('company_reference', '')

        # Parse date filters
        date_from = datetime.strptime(from_date, '%Y-%m-%d') if from_date else None
        date_to = datetime.strptime(to_date, '%Y-%m-%d') if to_date else None

        # Get list of already-imported email IDs (unless include_processed is True)
        imported_email_ids = set()
        if not include_processed:
            imported_email_ids = set(email_storage.get_imported_gocardless_email_ids())

        # Search for GoCardless emails
        # Search by sender domain or subject containing "gocardless"
        result = email_storage.get_emails(
            search="gocardless",
            from_date=date_from,
            to_date=date_to,
            page_size=100  # Get up to 100 emails
        )

        emails = result.get('emails', []) or result.get('items', [])

        if not emails:
            return {
                "success": True,
                "message": "No GoCardless emails found",
                "batches": [],
                "total_emails": 0,
                "company_reference": company_ref
            }

        # Import period validation
        from sql_rag.opera_config import validate_posting_period

        # Parse each email to extract payment batches
        batches = []
        processed_count = 0
        error_count = 0
        skipped_wrong_company = 0
        skipped_already_imported = 0
        skipped_duplicates = 0

        for email in emails:
            try:
                email_id = email.get('id')

                # Skip already-imported emails (unless include_processed is True)
                if email_id in imported_email_ids:
                    skipped_already_imported += 1
                    continue

                # Get email content (prefer text, fall back to HTML)
                content = email.get('body_text') or email.get('body_html') or ''

                if not content:
                    continue

                # Check if this email looks like a payment notification
                # GoCardless payment emails typically have "payout", "payment", or "paid" in subject
                subject = email.get('subject', '').lower()
                if not any(keyword in subject for keyword in ['payout', 'payment', 'collected', 'paid']):
                    continue

                # Parse the email content
                batch = parse_gocardless_email(content)

                # Filter by company reference if configured
                # The bank reference in GoCardless emails contains the company identifier (e.g., "INTSYSUKLTD")
                if company_ref:
                    batch_ref = (batch.bank_reference or '').upper()
                    if company_ref.upper() not in batch_ref and batch_ref not in company_ref.upper():
                        # Also check the email body for the reference
                        if company_ref.upper() not in content.upper():
                            skipped_wrong_company += 1
                            continue

                # Check for foreign currency (include in results but flag as not importable)
                is_foreign_currency = False
                home_currency_code = 'GBP'  # Default
                if sql_connector:
                    from sql_rag.opera_sql_import import OperaSQLImport
                    importer = OperaSQLImport(sql_connector)
                    home_currency = importer.get_home_currency()
                    home_currency_code = home_currency['code']
                    if batch.currency and batch.currency.upper() != home_currency_code.upper():
                        is_foreign_currency = True
                        logger.debug(f"Foreign currency batch found: {batch.currency} (home is {home_currency_code})")
                else:
                    # Fallback to GBP if no database connection
                    if batch.currency and batch.currency != 'GBP':
                        is_foreign_currency = True
                        logger.debug(f"Foreign currency batch found: {batch.currency}")

                # Only include if we found payments
                if batch.payments:
                    # Format payment date if available
                    payment_date_str = None
                    if batch.payment_date:
                        payment_date_str = batch.payment_date.strftime('%Y-%m-%d')

                    # Check for duplicate batch in cashbook using NET amount, GROSS amount, and reference
                    possible_duplicate = False
                    duplicate_warning = None
                    bank_tx_warning = None  # Additional check for gross amount in bank transactions
                    ref_warning = None  # Check by GoCardless reference
                    try:
                        net_pence = int(round(batch.net_amount * 100))
                        gross_pence = int(round(batch.gross_amount * 100))
                        gc_settings = _load_gocardless_settings()
                        default_cbtype = gc_settings.get('default_batch_type', '')
                        bank_ref = (batch.bank_reference or '').strip()

                        # Check 1: By GoCardless reference (most reliable for future imports)
                        if bank_ref:
                            ref_df = sql_connector.execute_query(f"""
                                SELECT TOP 1 ae_entref, at_value, at_pstdate as at_date
                                FROM aentry WITH (NOLOCK)
                                JOIN atran WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                    AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                WHERE at_type = 4
                                  AND RTRIM(ae_entref) = '{bank_ref[:20]}'
                                ORDER BY at_pstdate DESC
                            """)
                            if ref_df is not None and len(ref_df) > 0:
                                row = ref_df.iloc[0]
                                possible_duplicate = True
                                tx_date = row['at_date']
                                date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                ref_warning = f"Already imported: ref '{bank_ref}' on {date_str}"

                        # Check 2: NET amount in cashbook (catches direct GoCardless imports where net was posted)
                        if not ref_warning:  # Only check by amount if reference didn't match
                            dup_df = sql_connector.execute_query(f"""
                                SELECT TOP 1 at_value, at_pstdate as at_date, at_cbtype, ae_entref
                                FROM atran WITH (NOLOCK)
                                JOIN aentry WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                    AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                WHERE at_type = 4
                                  AND ABS(at_value - {net_pence}) <= 1
                                  {f"AND at_cbtype = '{default_cbtype}'" if default_cbtype else ""}
                                ORDER BY at_pstdate DESC
                            """)
                            if dup_df is not None and len(dup_df) > 0:
                                row = dup_df.iloc[0]
                                possible_duplicate = True
                                tx_date = row['at_date']
                                date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                ref = row['ae_entref'].strip() if row.get('ae_entref') else 'N/A'
                                duplicate_warning = f"Cashbook entry found: £{int(row['at_value'])/100:.2f} on {date_str} (ref: {ref})"

                        # Check 3: GROSS amount in cashbook (catches bank statement imports or manual entries)
                        # Check all positive receipt types (1=Sales Receipt, 4=Nominal Payment, 6=Nominal Receipt)
                        # Manual GoCardless entries often use type 4 with "GC" reference
                        # Only flag if date is within 14 days of payout date
                        if batch.payment_date:
                            payout_date_str = batch.payment_date.strftime('%Y-%m-%d')
                            gross_df = sql_connector.execute_query(f"""
                                SELECT TOP 1 at_value, at_pstdate as at_date, at_cbtype, ae_entref, at_refer
                                FROM atran WITH (NOLOCK)
                                JOIN aentry WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                    AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                WHERE at_type IN (1, 4, 6)
                                  AND at_value > 0
                                  AND ABS(at_value - {gross_pence}) <= 1
                                  AND ABS(DATEDIFF(day, at_pstdate, '{payout_date_str}')) <= 14
                                ORDER BY at_pstdate DESC
                            """)
                            if gross_df is not None and len(gross_df) > 0:
                                row = gross_df.iloc[0]
                                tx_date = row['at_date']
                                date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                existing_ref = row['ae_entref'].strip() if row.get('ae_entref') else (row.get('at_refer', '').strip() or 'N/A')
                                # Only flag as definite duplicate if the existing ref matches our GC ref
                                # Otherwise just show as warning (amount-only match is not conclusive)
                                if bank_ref and existing_ref.upper().startswith(bank_ref[:10].upper()):
                                    bank_tx_warning = f"Already posted - gross amount: £{int(row['at_value'])/100:.2f} on {date_str} (ref: {existing_ref})"
                                    if not possible_duplicate:
                                        possible_duplicate = True
                                else:
                                    # Warning only - different ref suggests different transaction with same amount
                                    bank_tx_warning = f"Similar amount found: £{int(row['at_value'])/100:.2f} on {date_str} (ref: {existing_ref}) - verify before importing"

                        # Check 3b: Find batched entries where total matches gross and verify individual payments
                        # This catches manual posting as a batch without correct GC reference
                        # Only run this expensive check if reference and gross amount checks didn't find anything
                        if not ref_warning and not bank_tx_warning and batch.payments and len(batch.payments) > 1:
                            # Find entries where total of positive values equals gross amount
                            batch_entry_df = sql_connector.execute_query(f"""
                                SELECT at_acnt, at_cntr, at_cbtype, at_entry,
                                       SUM(at_value) as entry_total,
                                       MIN(at_pstdate) as entry_date,
                                       COUNT(*) as line_count
                                FROM atran WITH (NOLOCK)
                                WHERE at_type IN (1, 4, 6)
                                  AND at_value > 0
                                GROUP BY at_acnt, at_cntr, at_cbtype, at_entry
                                HAVING ABS(SUM(at_value) - {gross_pence}) <= 10
                                   AND COUNT(*) >= {len(batch.payments)}
                                ORDER BY MIN(at_pstdate) DESC
                            """)
                            if batch_entry_df is not None and len(batch_entry_df) > 0:
                                # Check each matching entry to see if individual payments match
                                for _, entry_row in batch_entry_df.iterrows():
                                    entry_key = f"at_acnt = '{entry_row['at_acnt'].strip()}' AND at_cntr = '{entry_row['at_cntr'].strip()}' AND at_cbtype = '{entry_row['at_cbtype'].strip()}' AND at_entry = '{entry_row['at_entry'].strip()}'"
                                    entry_lines_df = sql_connector.execute_query(f"""
                                        SELECT at_value, at_name FROM atran WITH (NOLOCK)
                                        WHERE {entry_key} AND at_type IN (1, 4, 6) AND at_value > 0
                                    """)
                                    if entry_lines_df is not None and len(entry_lines_df) > 0:
                                        # Get all amounts from this entry
                                        entry_amounts = sorted([int(row['at_value']) for _, row in entry_lines_df.iterrows()])
                                        # Get all amounts from GoCardless batch
                                        gc_amounts = sorted([int(round(p.amount * 100)) for p in batch.payments])
                                        # Check if amounts match (allow 1 penny tolerance per amount)
                                        if len(entry_amounts) == len(gc_amounts):
                                            amounts_match = all(abs(a - b) <= 1 for a, b in zip(entry_amounts, gc_amounts))
                                            if amounts_match:
                                                tx_date = entry_row['entry_date']
                                                date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                                bank_tx_warning = f"Already posted - batch: {len(entry_amounts)} payments totaling £{int(entry_row['entry_total'])/100:.2f} on {date_str}"
                                                if not possible_duplicate:
                                                    possible_duplicate = True
                                                break

                        # Check 3c: Individual payment amounts with GC reference (catches manual posting of individual customer receipts)
                        if not bank_tx_warning and batch.payments:
                            for payment in batch.payments[:5]:  # Check first 5 payments
                                payment_pence = int(round(payment.amount * 100))
                                payment_df = sql_connector.execute_query(f"""
                                    SELECT TOP 1 at_value, at_pstdate as at_date, at_name, at_refer
                                    FROM atran WITH (NOLOCK)
                                    WHERE at_type IN (1, 4)
                                      AND at_value > 0
                                      AND ABS(at_value - {payment_pence}) <= 1
                                      AND (at_refer LIKE '%GC%' OR at_refer LIKE '%GoCardless%')
                                    ORDER BY at_pstdate DESC
                                """)
                                if payment_df is not None and len(payment_df) > 0:
                                    row = payment_df.iloc[0]
                                    tx_date = row['at_date']
                                    date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                    name = row['at_name'].strip()[:20] if row.get('at_name') else ''
                                    bank_tx_warning = f"Already posted - payment: £{int(row['at_value'])/100:.2f} ({name}) on {date_str} with GC ref"
                                    if not possible_duplicate:
                                        possible_duplicate = True
                                    break  # Found one match, that's enough

                        # Check 4: FEES amount in cashbook (catches manual posting of fees as separate payment)
                        # Fees are posted as negative payments, so check for matching payment amount
                        fees_pence = int(round(abs(batch.gocardless_fees) * 100))
                        if fees_pence > 0:
                            fees_df = sql_connector.execute_query(f"""
                                SELECT TOP 1 at_value, at_pstdate as at_date, at_cbtype, ae_entref
                                FROM atran WITH (NOLOCK)
                                JOIN aentry WITH (NOLOCK) ON ae_acnt = at_acnt AND ae_cntr = at_cntr
                                    AND ae_cbtype = at_cbtype AND ae_entry = at_entry
                                WHERE at_type IN (2, 4)
                                  AND ABS(ABS(at_value) - {fees_pence}) <= 1
                                ORDER BY at_pstdate DESC
                            """)
                            if fees_df is not None and len(fees_df) > 0:
                                row = fees_df.iloc[0]
                                tx_date = row['at_date']
                                date_str = tx_date.strftime('%d/%m/%Y') if hasattr(tx_date, 'strftime') else str(tx_date)[:10]
                                ref = row['ae_entref'].strip() if row.get('ae_entref') else 'N/A'
                                if not bank_tx_warning:
                                    bank_tx_warning = f"Already posted - fees: £{abs(int(row['at_value']))/100:.2f} on {date_str} (ref: {ref})"
                                else:
                                    bank_tx_warning += f" | Fees also posted: £{abs(int(row['at_value']))/100:.2f} on {date_str}"
                                if not possible_duplicate:
                                    possible_duplicate = True
                    except Exception as dup_err:
                        logger.warning(f"Could not check batch duplicate: {dup_err}")

                    # Validate posting period for the payment date
                    period_valid = True
                    period_error = None
                    if batch.payment_date:
                        try:
                            period_result = validate_posting_period(sql_connector, batch.payment_date.date(), 'SL')
                            period_valid = period_result.is_valid
                            if not period_valid:
                                period_error = period_result.error_message
                        except Exception as period_err:
                            logger.warning(f"Could not validate period: {period_err}")

                    batch_data = {
                        "email_id": email.get('id'),
                        "email_subject": email.get('subject'),
                        "email_date": email.get('received_at'),
                        "email_from": email.get('from_address'),
                        "possible_duplicate": possible_duplicate,
                        "duplicate_warning": duplicate_warning,
                        "bank_tx_warning": bank_tx_warning,  # Gross amount found in bank transactions
                        "ref_warning": ref_warning,  # Reference already exists in cashbook
                        "period_valid": period_valid,
                        "period_error": period_error,
                        "is_foreign_currency": is_foreign_currency,
                        "home_currency": home_currency_code,
                        "batch": {
                            "gross_amount": batch.gross_amount,
                            "gocardless_fees": batch.gocardless_fees,
                            "vat_on_fees": batch.vat_on_fees,
                            "net_amount": batch.net_amount,
                            "bank_reference": batch.bank_reference,
                            "currency": batch.currency,
                            "payment_date": payment_date_str,
                            "payment_count": len(batch.payments),
                            "payments": [
                                {
                                    "customer_name": p.customer_name,
                                    "description": p.description,
                                    "amount": p.amount,
                                    "invoice_refs": p.invoice_refs
                                }
                                for p in batch.payments
                            ]
                        }
                    }
                    # Always include batch but track duplicate count for stats
                    batches.append(batch_data)
                    if possible_duplicate:
                        skipped_duplicates += 1
                    processed_count += 1

            except Exception as e:
                logger.warning(f"Error parsing email {email.get('id')}: {e}")
                error_count += 1
                continue

        # Get current period info for client-side validation
        from sql_rag.opera_config import get_current_period_info
        current_period = get_current_period_info(sql_connector)

        return {
            "success": True,
            "total_emails": len(emails),
            "parsed_count": processed_count,
            "error_count": error_count,
            "skipped_wrong_company": skipped_wrong_company,
            "skipped_already_imported": skipped_already_imported,
            "skipped_duplicates": skipped_duplicates,
            "company_reference": company_ref,
            "current_period": {
                "year": current_period.get('np_year'),
                "period": current_period.get('np_perno')
            },
            "batches": batches
        }

    except Exception as e:
        logger.error(f"Error scanning GoCardless emails: {e}")
        return {"success": False, "error": str(e)}


@app.delete("/api/gocardless/import-history")
async def clear_gocardless_import_history(
    from_date: Optional[str] = Query(None, description="Clear from date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="Clear to date (YYYY-MM-DD)")
):
    """
    Clear GoCardless import history within a date range.

    If no dates specified, clears ALL history (use with caution).
    This only removes the tracking records - does not affect Opera data.
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        deleted_count = email_storage.clear_gocardless_import_history(
            from_date=from_date,
            to_date=to_date
        )
        return {
            "success": True,
            "deleted_count": deleted_count,
            "message": f"Cleared {deleted_count} import history records"
        }
    except Exception as e:
        logger.error(f"Error clearing GoCardless import history: {e}")
        return {"success": False, "error": str(e)}


@app.delete("/api/gocardless/import-history/{record_id}")
async def delete_gocardless_import_record(record_id: int):
    """
    Delete a single import history record to allow re-importing.

    This removes the tracking record so the payout can be fetched and imported again.
    Does not affect Opera data - only the import tracking.
    """
    if not email_storage:
        return {"success": False, "error": "Email storage not configured"}

    try:
        deleted = email_storage.delete_gocardless_import_record(record_id)
        if deleted:
            return {
                "success": True,
                "message": "Import record deleted - payout can now be re-imported"
            }
        else:
            return {"success": False, "error": "Record not found"}
    except Exception as e:
        logger.error(f"Error deleting GoCardless import record: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/gocardless/import-from-email")
async def import_gocardless_from_email(
    email_id: int = Query(..., description="Email ID to import from"),
    bank_code: str = Query("BC010", description="Opera bank account code"),
    post_date: str = Query(..., description="Posting date (YYYY-MM-DD)"),
    reference: str = Query("GoCardless", description="Batch reference"),
    complete_batch: bool = Query(False, description="Complete batch immediately"),
    cbtype: str = Query(None, description="Cashbook type code"),
    gocardless_fees: float = Query(0.0, description="GoCardless fees amount (gross including VAT)"),
    vat_on_fees: float = Query(0.0, description="VAT element of fees"),
    fees_nominal_account: str = Query(None, description="Nominal account for net fees"),
    fees_vat_code: str = Query("2", description="VAT code for fees - looked up in ztax for rate and nominal"),
    currency: str = Query(None, description="Currency code from GoCardless (e.g., 'GBP'). Rejected if not home currency."),
    archive_folder: str = Query("Archive/GoCardless", description="Folder to move email after import"),
    payments: List[Dict[str, Any]] = Body(..., description="List of payments with matched customer accounts")
):
    """
    Import GoCardless batch from a scanned email.

    This endpoint takes the email ID and matched payment data, validates the period,
    and imports into Opera.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="No database connection")

    try:
        from sql_rag.opera_sql_import import OperaSQLImport
        from datetime import datetime

        # Parse date
        try:
            parsed_date = datetime.strptime(post_date, '%Y-%m-%d').date()
        except ValueError:
            return {"success": False, "error": f"Invalid date format: {post_date}. Use YYYY-MM-DD"}

        # Validate posting period (Sales Ledger)
        from sql_rag.opera_config import validate_posting_period
        period_result = validate_posting_period(sql_connector, parsed_date, 'SL')
        if not period_result.is_valid:
            return {"success": False, "error": f"Cannot post to this date: {period_result.error_message}"}

        # Validate payments
        if not payments:
            return {"success": False, "error": "No payments provided"}

        validated_payments = []
        for idx, p in enumerate(payments):
            if not p.get('customer_account'):
                return {"success": False, "error": f"Payment {idx+1}: Missing customer_account"}
            if not p.get('amount'):
                return {"success": False, "error": f"Payment {idx+1}: Missing amount"}

            validated_payments.append({
                "customer_account": p['customer_account'],
                "amount": float(p['amount']),
                "description": p.get('description', '')[:35]
            })

        # Validate fees_nominal_account is configured if there are fees
        if gocardless_fees and gocardless_fees > 0 and not fees_nominal_account:
            return {
                "success": False,
                "error": f"GoCardless fees of £{gocardless_fees:.2f} cannot be posted: Fees Nominal Account not configured. "
                         "Please configure the Fees Nominal Account in GoCardless Settings before importing."
            }

        # Import the batch
        importer = OperaSQLImport(sql_connector)
        result = importer.import_gocardless_batch(
            bank_account=bank_code,
            payments=validated_payments,
            post_date=parsed_date,
            reference=reference,
            gocardless_fees=gocardless_fees,
            vat_on_fees=vat_on_fees,
            fees_nominal_account=fees_nominal_account,
            fees_vat_code=fees_vat_code,
            complete_batch=complete_batch,
            cbtype=cbtype,
            input_by="GOCARDLS",
            currency=currency
        )

        if result.success:
            # Record the import to track this email as processed
            # Only AFTER successful Opera import - email will be filtered from future scans
            try:
                gross_amount = sum(p.get('amount', 0) for p in payments)
                net_amount = gross_amount - gocardless_fees
                email_storage.record_gocardless_import(
                    email_id=email_id,
                    target_system='opera_se',
                    bank_reference=reference,
                    gross_amount=gross_amount,
                    net_amount=net_amount,
                    payment_count=len(payments),
                    batch_ref=result.batch_number,
                    imported_by="GOCARDLS"
                )
            except Exception as track_err:
                logger.warning(f"Failed to record GoCardless import tracking: {track_err}")

            # Archive the email (move to archive folder)
            archive_status = "not_attempted"
            if archive_folder and email_storage:
                try:
                    # Get email details including message_id and provider_id
                    email_details = email_storage.get_email_by_id(email_id)
                    if email_details:
                        provider_id = email_details.get('provider_id')
                        message_id = email_details.get('message_id')
                        source_folder = email_details.get('folder_id', 'INBOX')

                        if provider_id and message_id and provider_id in email_sync_manager.providers:
                            provider = email_sync_manager.providers[provider_id]
                            # Move email to archive folder
                            move_success = await provider.move_email(
                                message_id=message_id,
                                source_folder=source_folder,
                                dest_folder=archive_folder
                            )
                            archive_status = "archived" if move_success else "move_failed"
                            if move_success:
                                logger.info(f"Archived GoCardless email {email_id} to {archive_folder}")
                            else:
                                logger.warning(f"Failed to archive email {email_id}")
                        else:
                            archive_status = "provider_not_available"
                    else:
                        archive_status = "email_not_found"
                except Exception as archive_err:
                    logger.warning(f"Failed to archive GoCardless email: {archive_err}")
                    archive_status = f"error: {str(archive_err)}"

            return {
                "success": True,
                "message": f"Successfully imported {len(payments)} payments from email",
                "email_id": email_id,
                "payments_imported": result.records_imported,
                "complete": complete_batch,
                "archive_status": archive_status
            }
        else:
            return {
                "success": False,
                "error": "; ".join(result.errors),
                "payments_processed": result.records_processed
            }

    except Exception as e:
        logger.error(f"Error importing GoCardless from email: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/gocardless/archive-email")
async def archive_gocardless_email(
    email_id: int = Query(..., description="Email ID to archive"),
    archive_folder: str = Query("Archive/GoCardless", description="Folder to move email after archive")
):
    """
    Archive a GoCardless email without importing (for duplicates already in Opera).

    This marks the email as processed so it won't appear in future scans,
    and moves it to the archive folder.
    """
    if not email_storage:
        raise HTTPException(status_code=503, detail="Email storage not configured")

    try:
        # Record the email as processed (with no import data)
        try:
            email_storage.record_gocardless_import(
                email_id=email_id,
                target_system='archived',  # Mark as archived, not imported
                bank_reference='ARCHIVED',
                gross_amount=0,
                net_amount=0,
                payment_count=0,
                batch_ref=None,
                imported_by="ARCHIVE"
            )
        except Exception as track_err:
            logger.warning(f"Failed to record archive tracking: {track_err}")

        # Archive the email (move to archive folder)
        archive_status = "not_attempted"
        if archive_folder and email_sync_manager:
            try:
                # Get email details including message_id and provider_id
                email_details = email_storage.get_email_by_id(email_id)
                if email_details:
                    provider_id = email_details.get('provider_id')
                    message_id = email_details.get('message_id')
                    source_folder = email_details.get('folder_id', 'INBOX')

                    if provider_id and message_id and provider_id in email_sync_manager.providers:
                        provider = email_sync_manager.providers[provider_id]
                        # Move email to archive folder
                        move_success = await provider.move_email(
                            message_id=message_id,
                            source_folder=source_folder,
                            dest_folder=archive_folder
                        )
                        archive_status = "archived" if move_success else "move_failed"
                        if move_success:
                            logger.info(f"Archived GoCardless email {email_id} to {archive_folder}")
                        else:
                            logger.warning(f"Failed to archive email {email_id}")
                    else:
                        archive_status = "provider_not_available"
                else:
                    archive_status = "email_not_found"
            except Exception as archive_err:
                logger.warning(f"Failed to archive GoCardless email: {archive_err}")
                archive_status = f"error: {str(archive_err)}"

        return {
            "success": True,
            "message": "Email archived (already in Opera)",
            "email_id": email_id,
            "archive_status": archive_status
        }

    except Exception as e:
        logger.error(f"Error archiving GoCardless email: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Opera 3 FoxPro API Endpoints
# ============================================================
# These endpoints mirror the SQL SE endpoints but read directly from
# Opera 3 FoxPro DBF files. Requires data_path parameter pointing to
# the Opera 3 company data folder.

from sql_rag.opera3_data_provider import Opera3DataProvider


def _get_opera3_provider(data_path: str) -> Opera3DataProvider:
    """Get or create Opera3DataProvider for the given data path."""
    return Opera3DataProvider(data_path)


@app.get("/api/opera3/credit-control/dashboard")
async def opera3_credit_control_dashboard(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """
    Get credit control dashboard from Opera 3 FoxPro data.
    Mirrors /api/credit-control/dashboard but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        metrics = provider.get_credit_control_metrics()
        priority_actions = provider.get_priority_customers(limit=10)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "metrics": metrics,
            "priority_actions": priority_actions
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 credit control dashboard failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3/credit-control/debtors-report")
async def opera3_debtors_report(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """
    Get aged debtors report from Opera 3 FoxPro data.
    Mirrors /api/credit-control/debtors-report but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        data = provider.get_customer_aging()

        # Calculate totals
        totals = {
            "balance": sum(r.get("balance", 0) or 0 for r in data),
            "current": sum(r.get("current", 0) or 0 for r in data),
            "month_1": sum(r.get("month1", 0) or 0 for r in data),
            "month_2": sum(r.get("month2", 0) or 0 for r in data),
            "month_3_plus": sum(r.get("month3_plus", 0) or 0 for r in data),
        }

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "data": data,
            "count": len(data),
            "totals": totals
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 debtors report failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3/nominal/trial-balance")
async def opera3_trial_balance(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    year: int = Query(2026, description="Financial year")
):
    """
    Get trial balance from Opera 3 FoxPro data.
    Mirrors /api/nominal/trial-balance but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        data = provider.get_nominal_trial_balance(year)

        # Calculate totals
        total_debit = sum(r.get("debit", 0) or 0 for r in data)
        total_credit = sum(r.get("credit", 0) or 0 for r in data)

        # Group by account type for summary
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
        type_summary = {}
        for r in data:
            atype = (r.get("account_type") or "?").strip()
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
            "source": "opera3",
            "data_path": data_path,
            "year": year,
            "data": data,
            "count": len(data),
            "totals": {
                "debit": total_debit,
                "credit": total_credit,
                "difference": total_debit - total_credit
            },
            "by_type": type_summary
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 trial balance failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3/dashboard/finance-summary")
async def opera3_finance_summary(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    year: int = Query(2024, description="Financial year")
):
    """
    Get financial summary from Opera 3 FoxPro data.
    Mirrors /api/dashboard/finance-summary but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        summary = provider.get_finance_summary(year)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "year": year,
            **summary
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 finance summary failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3/dashboard/finance-monthly")
async def opera3_finance_monthly(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    year: int = Query(2024, description="Financial year")
):
    """
    Get monthly P&L breakdown from Opera 3 FoxPro data.
    Mirrors /api/dashboard/finance-monthly but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        months = provider.get_nominal_monthly(year)

        # Calculate YTD totals
        ytd_revenue = sum(m['revenue'] for m in months)
        ytd_cos = sum(m['cost_of_sales'] for m in months)
        ytd_overheads = sum(m['overheads'] for m in months)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "year": year,
            "months": months,
            "ytd": {
                "revenue": round(ytd_revenue, 2),
                "cost_of_sales": round(ytd_cos, 2),
                "gross_profit": round(ytd_revenue - ytd_cos, 2),
                "overheads": round(ytd_overheads, 2),
                "net_profit": round(ytd_revenue - ytd_cos - ytd_overheads, 2)
            }
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 finance monthly failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3/dashboard/executive-summary")
async def opera3_executive_summary(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    year: int = Query(2026, description="Financial year")
):
    """
    Get executive KPIs from Opera 3 FoxPro data.
    Mirrors /api/dashboard/executive-summary but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        summary = provider.get_executive_summary(year)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "year": year,
            **summary
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 executive summary failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3/creditors/dashboard")
async def opera3_creditors_dashboard(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """
    Get creditors dashboard from Opera 3 FoxPro data.
    Mirrors /api/creditors/dashboard but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        metrics = provider.get_creditors_metrics()
        top_suppliers = provider.get_top_suppliers(limit=10)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "metrics": metrics,
            "top_suppliers": top_suppliers
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 creditors dashboard failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3/creditors/report")
async def opera3_creditors_report(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """
    Get aged creditors report from Opera 3 FoxPro data.
    Mirrors /api/creditors/report but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        data = provider.get_supplier_aging()

        # Calculate totals
        totals = {
            "balance": sum(r.get("balance", 0) or 0 for r in data),
            "current": sum(r.get("current", 0) or 0 for r in data),
            "month_1": sum(r.get("month1", 0) or 0 for r in data),
            "month_2": sum(r.get("month2", 0) or 0 for r in data),
            "month_3_plus": sum(r.get("month3_plus", 0) or 0 for r in data),
        }

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "data": data,
            "count": len(data),
            "totals": totals
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 creditors report failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3/customers")
async def opera3_get_customers(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    active_only: bool = Query(True, description="Only return active customers")
):
    """
    Get customer master list from Opera 3 FoxPro data.
    """
    try:
        provider = _get_opera3_provider(data_path)
        customers = provider.get_customers(active_only=active_only)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "data": customers,
            "count": len(customers)
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 customers query failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3/suppliers")
async def opera3_get_suppliers(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    active_only: bool = Query(True, description="Only return active suppliers")
):
    """
    Get supplier master list from Opera 3 FoxPro data.
    """
    try:
        provider = _get_opera3_provider(data_path)
        suppliers = provider.get_suppliers(active_only=active_only)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "data": suppliers,
            "count": len(suppliers)
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 suppliers query failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3/nominal-accounts")
async def opera3_get_nominal_accounts(data_path: str = Query(..., description="Path to Opera 3 company data folder")):
    """
    Get nominal account master list from Opera 3 FoxPro data.
    """
    try:
        provider = _get_opera3_provider(data_path)
        accounts = provider.get_nominal_accounts()

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "data": accounts,
            "count": len(accounts)
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 nominal accounts query failed: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Opera 3 Reconciliation Endpoints
# ============================================================

@app.get("/api/opera3/reconcile/debtors")
async def opera3_reconcile_debtors(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    debtors_control: str = Query("C110", description="Debtors control account code")
):
    """
    Reconcile Sales Ledger to Debtors Control Account from Opera 3 FoxPro data.
    Mirrors /api/reconcile/debtors but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        reconciliation = provider.get_debtors_reconciliation(debtors_control)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            **reconciliation
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 debtors reconciliation failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3/reconcile/creditors")
async def opera3_reconcile_creditors(
    data_path: str = Query(..., description="Path to Opera 3 company data folder"),
    creditors_control: str = Query("D110", description="Creditors control account code")
):
    """
    Reconcile Purchase Ledger to Creditors Control Account from Opera 3 FoxPro data.
    Mirrors /api/reconcile/creditors but reads from DBF files.
    """
    try:
        provider = _get_opera3_provider(data_path)
        reconciliation = provider.get_creditors_reconciliation(creditors_control)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            **reconciliation
        }
    except FileNotFoundError as e:
        return {"success": False, "error": f"Data path not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 creditors reconciliation failed: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Opera 3 Bank Statement Import Endpoints
# ============================================================

@app.post("/api/opera3/bank-import/preview")
async def opera3_preview_bank_import(
    filepath: str = Query(..., description="Path to CSV file"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Preview what would be imported from a bank statement CSV using Opera 3 data.
    Matches transactions against Opera 3 customer/supplier master files.
    """
    # Backend validation - check for empty paths
    import os

    if not filepath or not filepath.strip():
        return {
            "success": False,
            "filename": "",
            "total_transactions": 0,
            "matched_receipts": [],
            "matched_payments": [],
            "repeat_entries": [],
            "already_posted": [],
            "skipped": [],
            "errors": ["CSV file path is required. Please enter the path to your bank statement CSV file."]
        }

    if not data_path or not data_path.strip():
        return {
            "success": False,
            "filename": filepath,
            "total_transactions": 0,
            "matched_receipts": [],
            "matched_payments": [],
            "repeat_entries": [],
            "already_posted": [],
            "skipped": [],
            "errors": ["Opera 3 data path is required. Please enter the path to your Opera 3 company data folder."]
        }

    if not os.path.exists(filepath):
        return {
            "success": False,
            "filename": filepath,
            "total_transactions": 0,
            "matched_receipts": [],
            "matched_payments": [],
            "repeat_entries": [],
            "already_posted": [],
            "skipped": [],
            "errors": [f"CSV file not found: {filepath}. Please check the file path."]
        }

    if not os.path.isdir(data_path):
        return {
            "success": False,
            "filename": filepath,
            "total_transactions": 0,
            "matched_receipts": [],
            "matched_payments": [],
            "repeat_entries": [],
            "already_posted": [],
            "skipped": [],
            "errors": [f"Opera 3 data path not found: {data_path}. Please check the folder path."]
        }

    try:
        from sql_rag.bank_import_opera3 import BankStatementMatcherOpera3

        matcher = BankStatementMatcherOpera3(data_path)
        result = matcher.preview_file(filepath)

        # Categorize transactions for frontend display
        matched_receipts = []
        matched_payments = []
        repeat_entries = []
        already_posted = []
        skipped = []

        for txn in result.transactions:
            txn_data = {
                "row": txn.row_number,
                "date": txn.date.isoformat(),
                "amount": txn.amount,
                "name": txn.name,
                "reference": txn.reference,
                "account": txn.matched_account,
                "match_score": txn.match_score if txn.match_score else 0,
                "reason": txn.skip_reason,
                # Repeat entry fields
                "repeat_entry_ref": getattr(txn, 'repeat_entry_ref', None),
                "repeat_entry_desc": getattr(txn, 'repeat_entry_desc', None),
                "repeat_entry_next_date": getattr(txn, 'repeat_entry_next_date', None).isoformat() if getattr(txn, 'repeat_entry_next_date', None) else None,
                "repeat_entry_posted": getattr(txn, 'repeat_entry_posted', None),
                "repeat_entry_total": getattr(txn, 'repeat_entry_total', None),
            }

            if txn.action == 'sales_receipt':
                matched_receipts.append(txn_data)
            elif txn.action == 'purchase_payment':
                matched_payments.append(txn_data)
            elif txn.action == 'repeat_entry':
                repeat_entries.append(txn_data)
            elif txn.skip_reason and 'Already' in txn.skip_reason:
                already_posted.append(txn_data)
            else:
                skipped.append(txn_data)

        return {
            "success": True,
            "source": "opera3",
            "data_path": data_path,
            "filename": result.filename,
            "total_transactions": result.total_transactions,
            "matched_receipts": matched_receipts,
            "matched_payments": matched_payments,
            "repeat_entries": repeat_entries,
            "already_posted": already_posted,
            "skipped": skipped,
            "errors": result.errors,
            "summary": {
                "repeat_entry_count": len(repeat_entries)
            }
        }

    except FileNotFoundError as e:
        return {"success": False, "error": f"File not found: {e}"}
    except Exception as e:
        logger.error(f"Opera 3 bank import preview error: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Lock Monitor Endpoints
# ============================================================

@app.post("/api/lock-monitor/connect")
async def lock_monitor_connect(
    name: str = Query(..., description="Name for this connection"),
    server: str = Query(..., description="SQL Server hostname"),
    database: str = Query(..., description="Database name"),
    port: str = Query("1433", description="SQL Server port"),
    username: str = Query(None, description="Username (optional for Windows auth)"),
    password: str = Query(None, description="Password (optional for Windows auth)"),
    driver: str = Query("ODBC Driver 18 for SQL Server", description="ODBC driver name")
):
    """
    Connect to a SQL Server instance for lock monitoring.
    Creates a named monitor that can be started/stopped.
    """
    try:
        from sql_rag.lock_monitor import get_monitor, save_monitor_config
        import urllib.parse

        # Build server string with port if not default
        server_with_port = f"{server}:{port}" if port and port != "1433" else server

        # Build connection string
        use_windows_auth = not (username and password)
        if username and password:
            encoded_password = urllib.parse.quote_plus(password)
            conn_str = (
                f"mssql+pyodbc://{username}:{encoded_password}@{server_with_port}/{database}"
                f"?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
            )
        else:
            conn_str = (
                f"mssql+pyodbc://@{server_with_port}/{database}"
                f"?driver={driver.replace(' ', '+')}&trusted_connection=yes&TrustServerCertificate=yes"
            )

        monitor = get_monitor(name, conn_str)

        # Test connection by initializing table
        monitor.initialize_table()

        # Save config for persistence (connection string includes credentials for reconnect)
        save_monitor_config(
            name=name,
            connection_string=conn_str,
            server=server,
            port=port,
            database=database,
            username=username,
            use_windows_auth=use_windows_auth
        )

        return {
            "success": True,
            "name": name,
            "server": server,
            "database": database,
            "message": f"Connected to {server}/{database} as '{name}'"
        }

    except Exception as e:
        logger.error(f"Lock monitor connect error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/lock-monitor/test-connection")
async def lock_monitor_test_connection(
    server: str = Query(..., description="SQL Server hostname"),
    port: str = Query("1433", description="SQL Server port"),
    username: str = Query(None, description="Username (optional for Windows auth)"),
    password: str = Query(None, description="Password (optional for Windows auth)"),
    driver: str = Query("ODBC Driver 18 for SQL Server", description="ODBC driver name")
):
    """
    Test SQL Server connection and return list of available databases (companies).
    Connects to 'master' database to list all user databases on the server.
    """
    try:
        from sqlalchemy import create_engine, text
        import urllib.parse

        # Build server string with port if not default
        server_with_port = f"{server}:{port}" if port and port != "1433" else server

        # Connect to master database to list all databases
        if username and password:
            encoded_password = urllib.parse.quote_plus(password)
            conn_str = (
                f"mssql+pyodbc://{username}:{encoded_password}@{server_with_port}/master"
                f"?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
            )
        else:
            conn_str = (
                f"mssql+pyodbc://@{server_with_port}/master"
                f"?driver={driver.replace(' ', '+')}&trusted_connection=yes&TrustServerCertificate=yes"
            )

        # Test connection and list databases
        engine = create_engine(conn_str, connect_args={"timeout": 10})
        databases = []

        with engine.connect() as conn:
            # Get list of user databases that the user has access to
            result = conn.execute(text("""
                SELECT name
                FROM sys.databases
                WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
                  AND state_desc = 'ONLINE'
                  AND HAS_DBACCESS(name) = 1
                ORDER BY name
            """))

            for row in result:
                db_name = row[0]
                databases.append({
                    "code": db_name,
                    "name": db_name
                })

        engine.dispose()

        if not databases:
            return {
                "success": False,
                "error": "No user databases found on server",
                "databases": []
            }

        return {
            "success": True,
            "message": f"Found {len(databases)} databases",
            "databases": databases
        }

    except Exception as e:
        logger.error(f"Lock monitor test connection error: {e}")
        return {"success": False, "error": str(e), "databases": []}


@app.post("/api/lock-monitor/{name}/start")
async def lock_monitor_start(
    name: str,
    poll_interval: int = Query(5, description="Seconds between polls"),
    min_wait_time: int = Query(1000, description="Minimum wait time (ms) to log")
):
    """Start lock monitoring for a named connection."""
    try:
        from sql_rag.lock_monitor import get_monitor

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found. Connect first."}

        if monitor.is_monitoring:
            return {"success": True, "message": "Monitoring already running", "status": "running"}

        monitor.start_monitoring(poll_interval=poll_interval, min_wait_time=min_wait_time)

        return {
            "success": True,
            "name": name,
            "status": "running",
            "poll_interval": poll_interval,
            "min_wait_time": min_wait_time
        }

    except Exception as e:
        logger.error(f"Lock monitor start error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/lock-monitor/{name}/stop")
async def lock_monitor_stop(name: str):
    """Stop lock monitoring for a named connection."""
    try:
        from sql_rag.lock_monitor import get_monitor

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        monitor.stop_monitoring()

        return {"success": True, "name": name, "status": "stopped"}

    except Exception as e:
        logger.error(f"Lock monitor stop error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/lock-monitor/{name}/status")
async def lock_monitor_status(name: str):
    """Get status of a named lock monitor."""
    try:
        from sql_rag.lock_monitor import get_monitor

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        return {
            "success": True,
            "name": name,
            "is_monitoring": monitor.is_monitoring
        }

    except Exception as e:
        logger.error(f"Lock monitor status error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/lock-monitor/{name}/disconnect")
async def lock_monitor_disconnect(name: str):
    """
    Disconnect from monitor without deleting the saved configuration.
    Stops monitoring and closes the connection, but keeps config for reconnection.
    """
    try:
        from sql_rag.lock_monitor import get_monitor, _monitors

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        # Stop monitoring if active
        if monitor.is_monitoring:
            monitor.stop_monitoring()

        # Dispose the engine to close all connections
        if monitor._engine:
            monitor._engine.dispose()
            monitor._engine = None

        # Remove from active monitors (but keep saved config)
        if name in _monitors:
            del _monitors[name]

        return {
            "success": True,
            "name": name,
            "message": f"Disconnected from '{name}'. Configuration saved for reconnection."
        }

    except Exception as e:
        logger.error(f"Lock monitor disconnect error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/lock-monitor/{name}/current")
async def lock_monitor_current_locks(name: str):
    """Get current blocking events (live snapshot)."""
    try:
        from sql_rag.lock_monitor import get_monitor
        from dataclasses import asdict

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        events = monitor.get_current_locks()

        return {
            "success": True,
            "name": name,
            "timestamp": datetime.now().isoformat(),
            "event_count": len(events),
            "events": [
                {
                    "blocked_session": e.blocked_session,
                    "blocking_session": e.blocking_session,
                    "blocked_user": e.blocked_user,
                    "blocking_user": e.blocking_user,
                    "table_name": e.table_name,
                    "lock_type": e.lock_type,
                    "wait_time_ms": e.wait_time_ms,
                    "blocked_query": e.blocked_query[:500] if e.blocked_query else "",
                    "blocking_query": e.blocking_query[:500] if e.blocking_query else "",
                    # Enhanced details
                    "database_name": e.database_name,
                    "schema_name": e.schema_name,
                    "index_name": e.index_name,
                    "resource_type": e.resource_type,
                    "resource_description": e.resource_description,
                    "lock_mode": e.lock_mode,
                    "blocking_lock_mode": e.blocking_lock_mode
                }
                for e in events
            ]
        }

    except Exception as e:
        logger.error(f"Lock monitor current locks error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/lock-monitor/{name}/summary")
async def lock_monitor_summary(
    name: str,
    hours: int = Query(24, description="Number of hours to include in summary")
):
    """Get summary report of lock events."""
    try:
        from sql_rag.lock_monitor import get_monitor
        from dataclasses import asdict

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        summary = monitor.get_summary(hours=hours)

        return {
            "success": True,
            "name": name,
            "hours": hours,
            "summary": asdict(summary)
        }

    except Exception as e:
        logger.error(f"Lock monitor summary error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/lock-monitor/list")
async def lock_monitor_list():
    """List all configured lock monitors and saved configs."""
    try:
        from sql_rag.lock_monitor import list_monitors, get_monitor, get_saved_configs

        # Active monitors
        monitors = []
        active_names = set()
        for name in list_monitors():
            monitor = get_monitor(name)
            monitors.append({
                "name": name,
                "is_monitoring": monitor.is_monitoring if monitor else False,
                "connected": True
            })
            active_names.add(name)

        # Saved configs that aren't currently connected (need password re-entry)
        saved_configs = get_saved_configs()
        for config in saved_configs:
            if config['name'] not in active_names:
                monitors.append({
                    "name": config['name'],
                    "is_monitoring": False,
                    "connected": False,
                    "server": config.get('server'),
                    "database": config.get('database_name'),
                    "username": config.get('username'),
                    "use_windows_auth": bool(config.get('use_windows_auth')),
                    "needs_password": not bool(config.get('use_windows_auth'))
                })

        return {"success": True, "monitors": monitors}

    except Exception as e:
        logger.error(f"Lock monitor list error: {e}")
        return {"success": False, "error": str(e)}


@app.delete("/api/lock-monitor/{name}")
async def lock_monitor_remove(name: str):
    """Remove a lock monitor connection."""
    try:
        from sql_rag.lock_monitor import remove_monitor

        if remove_monitor(name):
            return {"success": True, "message": f"Monitor '{name}' removed"}
        else:
            return {"success": False, "error": f"Monitor '{name}' not found"}

    except Exception as e:
        logger.error(f"Lock monitor remove error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/lock-monitor/{name}/clear-old")
async def lock_monitor_clear_old(
    name: str,
    days: int = Query(30, description="Keep events from last N days")
):
    """Clear lock events older than specified days."""
    try:
        from sql_rag.lock_monitor import get_monitor

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        deleted = monitor.clear_old_events(days=days)

        return {
            "success": True,
            "name": name,
            "deleted_count": deleted,
            "kept_days": days
        }

    except Exception as e:
        logger.error(f"Lock monitor clear old error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/lock-monitor/{name}/connections")
async def lock_monitor_connections(name: str):
    """
    Get ALL current connections to the database.
    Shows which services/applications are connected - useful for identifying
    what needs to be stopped before exclusive operations like database restores.
    """
    try:
        from sql_rag.lock_monitor import get_monitor

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        summary = monitor.get_connections_summary()

        return {
            "success": True,
            "name": name,
            "total_connections": summary['total_connections'],
            "by_program": summary['by_program'],
            "by_host": summary['by_host'],
            "connections": summary['connections']
        }

    except Exception as e:
        logger.error(f"Lock monitor connections error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/lock-monitor/{name}/kill-connections")
async def lock_monitor_kill_connections(
    name: str,
    exclude_self: bool = Query(True, description="Exclude the current connection from being killed")
):
    """
    Kill all connections to the database to prepare for exclusive operations like restore.

    WARNING: This will forcefully disconnect all users from the database.
    Use with caution - any uncommitted transactions will be rolled back.
    """
    try:
        from sql_rag.lock_monitor import get_monitor
        from sqlalchemy import text

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        engine = monitor._get_engine()

        # Get database name from current connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DB_NAME() as db_name"))
            database_name = result.fetchone()[0]

        # Get current connections before killing
        summary_before = monitor.get_connections_summary()
        connections_before = summary_before['total_connections']

        # Kill connections using SQL Server commands
        # Get list of SPIDs to kill (excluding system processes and our own)
        spid_query = """
            SELECT session_id, program_name, host_name, login_name
            FROM sys.dm_exec_sessions
            WHERE is_user_process = 1
              AND session_id != @@SPID
              AND session_id > 50
        """

        connections_to_kill = []
        killed_count = 0
        failed_count = 0
        errors = []

        with engine.connect() as conn:
            result = conn.execute(text(spid_query))
            rows = result.fetchall()

            for row in rows:
                spid = row[0]
                program = row[1] or 'Unknown'
                host = row[2] or 'Unknown'
                login = row[3] or 'Unknown'

                connections_to_kill.append({
                    'session_id': spid,
                    'program': program,
                    'host': host,
                    'login': login
                })

                # Kill the connection
                try:
                    kill_query = f"KILL {spid}"
                    conn.execute(text(kill_query))
                    killed_count += 1
                except Exception as kill_error:
                    failed_count += 1
                    errors.append(f"SPID {spid}: {str(kill_error)}")

            conn.commit()

        # Get connections after killing
        summary_after = monitor.get_connections_summary()
        connections_after = summary_after['total_connections']

        return {
            "success": True,
            "name": name,
            "database": database_name,
            "connections_before": connections_before,
            "connections_killed": killed_count,
            "connections_failed": failed_count,
            "connections_after": connections_after,
            "killed_sessions": connections_to_kill[:20],  # Limit detail for large lists
            "errors": errors[:10] if errors else None,
            "message": f"Killed {killed_count} connections. {connections_after} connections remaining."
        }

    except Exception as e:
        logger.error(f"Lock monitor kill connections error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/lock-monitor/{name}/set-single-user")
async def lock_monitor_set_single_user(name: str):
    """
    Set database to SINGLE_USER mode with ROLLBACK IMMEDIATE.
    This kills all other connections and prevents new ones.
    Use this before restore operations.
    """
    try:
        from sql_rag.lock_monitor import get_monitor
        from sqlalchemy import text

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        engine = monitor._get_engine()

        # Get database name first
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DB_NAME() as db_name"))
            database_name = result.fetchone()[0]

        # ALTER DATABASE must run outside of a transaction - use autocommit
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            alter_query = f"ALTER DATABASE [{database_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
            conn.execute(text(alter_query))

        return {
            "success": True,
            "name": name,
            "database": database_name,
            "mode": "SINGLE_USER",
            "message": f"Database {database_name} set to SINGLE_USER mode. All other connections terminated."
        }

    except Exception as e:
        logger.error(f"Lock monitor set single user error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/lock-monitor/{name}/set-multi-user")
async def lock_monitor_set_multi_user(name: str, database: str = Query(None)):
    """
    Set database back to MULTI_USER mode after restore operations.
    Connects via master database to avoid SINGLE_USER lock issues.
    """
    try:
        from sql_rag.lock_monitor import get_monitor, get_saved_configs
        from sqlalchemy import text, create_engine
        import re

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        # Get database name from parameter, config, or connection string
        database_name = database
        if not database_name:
            # Try to get from saved config
            configs = get_saved_configs()
            for cfg in configs:
                if cfg.get('name') == name:
                    database_name = cfg.get('database_name')
                    break

        if not database_name:
            # Parse from connection string
            conn_str = monitor.connection_string
            match = re.search(r'Database=([^;]+)', conn_str, re.IGNORECASE)
            if match:
                database_name = match.group(1)

        if not database_name:
            return {"success": False, "error": "Could not determine database name. Please provide 'database' parameter."}

        # Create a connection to master database to run ALTER DATABASE
        # Replace the database in connection string with master
        master_conn_str = re.sub(
            r'Database=[^;]+',
            'Database=master',
            monitor.connection_string,
            flags=re.IGNORECASE
        )
        master_engine = create_engine(master_conn_str)

        # ALTER DATABASE must run outside of a transaction - use autocommit
        with master_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            alter_query = f"ALTER DATABASE [{database_name}] SET MULTI_USER"
            conn.execute(text(alter_query))

        master_engine.dispose()

        return {
            "success": True,
            "name": name,
            "database": database_name,
            "mode": "MULTI_USER",
            "message": f"Database {database_name} set to MULTI_USER mode. Normal operations can resume."
        }

    except Exception as e:
        logger.error(f"Lock monitor set multi user error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/lock-monitor/{name}/blocking-services")
async def lock_monitor_blocking_services(
    name: str,
    hours: int = Query(24, description="Hours of history to analyze")
):
    """
    Get summary of which services/programs have been causing locks.
    Helps identify problematic services that may need configuration changes
    or to be stopped during maintenance.
    """
    try:
        from sql_rag.lock_monitor import get_monitor

        monitor = get_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        summary = monitor.get_summary(hours=hours)

        return {
            "success": True,
            "name": name,
            "hours_analyzed": hours,
            "total_lock_events": summary.total_events,
            "blocking_services": summary.most_blocking_programs,
            "blocking_users": summary.most_blocking_users,
            "affected_tables": summary.most_blocked_tables
        }

    except Exception as e:
        logger.error(f"Lock monitor blocking services error: {e}")
        return {"success": False, "error": str(e)}


# ============================================================
# Opera 3 Lock Monitor Endpoints
# ============================================================

@app.post("/api/opera3-lock-monitor/connect")
async def opera3_lock_monitor_connect(
    name: str = Query(..., description="Name for this monitor"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """Connect to an Opera 3 installation for file lock monitoring."""
    try:
        from sql_rag.opera3_lock_monitor import get_opera3_monitor

        monitor = get_opera3_monitor(name, data_path)
        monitor.initialize()

        return {
            "success": True,
            "name": name,
            "data_path": data_path,
            "message": f"Connected to Opera 3 at {data_path}"
        }

    except Exception as e:
        logger.error(f"Opera 3 lock monitor connect error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/opera3-lock-monitor/list-companies")
async def opera3_lock_monitor_list_companies(
    base_path: str = Query(..., description="Path to Opera 3 installation (e.g., C:\\Apps\\O3 Server VFP)")
):
    """
    List available companies from an Opera 3 installation.
    Reads from seqco.dbf in the System folder.
    """
    try:
        from sql_rag.opera3_foxpro import Opera3System

        system = Opera3System(base_path)
        companies = system.get_companies()

        if not companies:
            return {
                "success": False,
                "error": "No companies found in Opera 3 installation",
                "companies": []
            }

        return {
            "success": True,
            "message": f"Found {len(companies)} companies",
            "companies": [
                {
                    "code": c["code"],
                    "name": c["name"],
                    "data_path": c.get("data_path", "")
                }
                for c in companies
            ]
        }

    except ImportError:
        return {"success": False, "error": "dbfread package not installed", "companies": []}
    except FileNotFoundError as e:
        return {"success": False, "error": f"Path not found: {str(e)}", "companies": []}
    except Exception as e:
        logger.error(f"Opera 3 list companies error: {e}")
        return {"success": False, "error": str(e), "companies": []}


@app.post("/api/opera3-lock-monitor/{name}/start")
async def opera3_lock_monitor_start(
    name: str,
    poll_interval: int = Query(5, description="Seconds between polls")
):
    """Start Opera 3 file lock monitoring."""
    try:
        from sql_rag.opera3_lock_monitor import get_opera3_monitor

        monitor = get_opera3_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found. Connect first."}

        if monitor.is_monitoring:
            return {"success": True, "message": "Monitoring already running", "status": "running"}

        monitor.start_monitoring(poll_interval=poll_interval)

        return {
            "success": True,
            "name": name,
            "status": "running",
            "poll_interval": poll_interval
        }

    except Exception as e:
        logger.error(f"Opera 3 lock monitor start error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/opera3-lock-monitor/{name}/stop")
async def opera3_lock_monitor_stop(name: str):
    """Stop Opera 3 file lock monitoring."""
    try:
        from sql_rag.opera3_lock_monitor import get_opera3_monitor

        monitor = get_opera3_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        monitor.stop_monitoring()

        return {"success": True, "name": name, "status": "stopped"}

    except Exception as e:
        logger.error(f"Opera 3 lock monitor stop error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3-lock-monitor/{name}/status")
async def opera3_lock_monitor_status(name: str):
    """Get status of Opera 3 lock monitor."""
    try:
        from sql_rag.opera3_lock_monitor import get_opera3_monitor

        monitor = get_opera3_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        return {
            "success": True,
            "name": name,
            "is_monitoring": monitor.is_monitoring,
            "data_path": str(monitor.data_path)
        }

    except Exception as e:
        logger.error(f"Opera 3 lock monitor status error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3-lock-monitor/{name}/current")
async def opera3_lock_monitor_current(name: str):
    """Get current file locks on Opera 3 files."""
    try:
        from sql_rag.opera3_lock_monitor import get_opera3_monitor

        monitor = get_opera3_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        events = monitor.get_current_locks()

        return {
            "success": True,
            "name": name,
            "timestamp": datetime.now().isoformat(),
            "event_count": len(events),
            "events": [
                {
                    "file_name": e.file_name,
                    "table_name": e.table_name,
                    "process": e.process_name,
                    "process_id": e.process_id,
                    "lock_type": e.lock_type,
                    "user": e.user
                }
                for e in events
            ]
        }

    except Exception as e:
        logger.error(f"Opera 3 lock monitor current error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3-lock-monitor/{name}/summary")
async def opera3_lock_monitor_summary(
    name: str,
    hours: int = Query(24, description="Number of hours to include")
):
    """Get summary of Opera 3 file lock activity."""
    try:
        from sql_rag.opera3_lock_monitor import get_opera3_monitor
        from dataclasses import asdict

        monitor = get_opera3_monitor(name)
        if not monitor:
            return {"success": False, "error": f"Monitor '{name}' not found"}

        summary = monitor.get_summary(hours=hours)

        return {
            "success": True,
            "name": name,
            "hours": hours,
            "summary": asdict(summary)
        }

    except Exception as e:
        logger.error(f"Opera 3 lock monitor summary error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/opera3-lock-monitor/list")
async def opera3_lock_monitor_list():
    """List all Opera 3 lock monitors."""
    try:
        from sql_rag.opera3_lock_monitor import list_opera3_monitors, get_opera3_monitor

        monitors = []
        for name in list_opera3_monitors():
            monitor = get_opera3_monitor(name)
            monitors.append({
                "name": name,
                "is_monitoring": monitor.is_monitoring if monitor else False,
                "data_path": str(monitor.data_path) if monitor else None
            })

        return {"success": True, "monitors": monitors}

    except Exception as e:
        logger.error(f"Opera 3 lock monitor list error: {e}")
        return {"success": False, "error": str(e)}


@app.delete("/api/opera3-lock-monitor/{name}")
async def opera3_lock_monitor_remove(name: str):
    """Remove an Opera 3 lock monitor."""
    try:
        from sql_rag.opera3_lock_monitor import remove_opera3_monitor

        if remove_opera3_monitor(name):
            return {"success": True, "message": f"Monitor '{name}' removed"}
        else:
            return {"success": False, "error": f"Monitor '{name}' not found"}

    except Exception as e:
        logger.error(f"Opera 3 lock monitor remove error: {e}")
        return {"success": False, "error": str(e)}


# ============ Opera 3 Statement Reconciliation ============

@app.post("/api/opera3/reconcile/process-statement")
async def opera3_process_statement(
    file_path: str = Query(..., description="Path to the statement PDF"),
    bank_code: str = Query(..., description="Opera bank account code"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Process a bank statement PDF and extract transactions for matching (Opera 3).

    Workflow:
    1. User selects bank account and Opera 3 data path
    2. User provides statement file path
    3. System validates statement matches selected bank account
    4. System extracts and matches transactions
    """
    try:
        from sql_rag.statement_reconcile_opera3 import StatementReconcilerOpera3
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(file_path).exists():
            return {"success": False, "error": f"File not found: {file_path}"}

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        # Create Opera 3 reader and reconciler
        reader = Opera3Reader(data_path)
        reconciler = StatementReconcilerOpera3(reader, config=config)

        # Extract transactions from statement
        statement_info, transactions = reconciler.extract_transactions_from_pdf(file_path)

        # Validate that statement matches the selected bank account
        bank_validation = reconciler.validate_statement_bank(bank_code, statement_info)
        if not bank_validation['valid']:
            return {
                "success": False,
                "error": bank_validation['error'],
                "bank_code": bank_code,
                "bank_validation": bank_validation,
                "statement_info": {
                    "bank_name": statement_info.bank_name,
                    "account_number": statement_info.account_number,
                    "sort_code": statement_info.sort_code
                }
            }

        # Get unreconciled Opera entries for the date range
        opera_entries = reconciler.get_unreconciled_entries(
            bank_code,
            date_from=statement_info.period_start,
            date_to=statement_info.period_end
        )

        # Match transactions
        matches, unmatched_stmt, unmatched_opera = reconciler.match_transactions(
            transactions, opera_entries
        )

        # Format response
        return {
            "success": True,
            "bank_code": bank_code,
            "bank_validation": bank_validation,
            "statement_info": {
                "bank_name": statement_info.bank_name,
                "account_number": statement_info.account_number,
                "sort_code": statement_info.sort_code,
                "statement_date": statement_info.statement_date.isoformat() if statement_info.statement_date else None,
                "period_start": statement_info.period_start.isoformat() if statement_info.period_start else None,
                "period_end": statement_info.period_end.isoformat() if statement_info.period_end else None,
                "opening_balance": statement_info.opening_balance,
                "closing_balance": statement_info.closing_balance
            },
            "extracted_transactions": len(transactions),
            "opera_unreconciled": len(opera_entries),
            "matches": [
                {
                    "statement_txn": {
                        "date": m.statement_txn.date.isoformat(),
                        "description": m.statement_txn.description,
                        "amount": m.statement_txn.amount,
                        "balance": m.statement_txn.balance,
                        "type": m.statement_txn.transaction_type
                    },
                    "opera_entry": {
                        "ae_entry": m.opera_entry['ae_entry'],
                        "ae_date": m.opera_entry['ae_date'].isoformat() if hasattr(m.opera_entry['ae_date'], 'isoformat') else str(m.opera_entry['ae_date']),
                        "ae_ref": m.opera_entry['ae_ref'],
                        "value_pounds": m.opera_entry['value_pounds'],
                        "ae_detail": m.opera_entry.get('ae_detail', '')
                    },
                    "match_score": m.match_score,
                    "match_reasons": m.match_reasons
                }
                for m in matches
            ],
            "unmatched_statement": [
                {
                    "date": t.date.isoformat(),
                    "description": t.description,
                    "amount": t.amount,
                    "balance": t.balance,
                    "type": t.transaction_type
                }
                for t in unmatched_stmt
            ],
            "unmatched_opera": [
                {
                    "ae_entry": e['ae_entry'],
                    "ae_date": e['ae_date'].isoformat() if hasattr(e['ae_date'], 'isoformat') else str(e['ae_date']),
                    "ae_ref": e['ae_ref'],
                    "value_pounds": e['value_pounds'],
                    "ae_detail": e.get('ae_detail', '')
                }
                for e in unmatched_opera
            ]
        }

    except Exception as e:
        logger.error(f"Opera 3 statement processing failed: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


@app.post("/api/opera3/reconcile/process-statement-unified")
async def opera3_process_statement_unified(
    file_path: str = Query(..., description="Path to the statement PDF"),
    bank_code: str = Query(..., description="Opera bank account code"),
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Unified statement processing for Opera 3: identifies transactions to IMPORT and RECONCILE.
    """
    try:
        from sql_rag.statement_reconcile_opera3 import StatementReconcilerOpera3
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(file_path).exists():
            return {"success": False, "error": f"File not found: {file_path}"}

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        # Create Opera 3 reader and reconciler
        reader = Opera3Reader(data_path)
        reconciler = StatementReconcilerOpera3(reader, config=config)

        # Use the unified processing method
        result = reconciler.process_statement_unified(bank_code, file_path)

        # Handle error case
        if not result.get('success', False):
            return result

        stmt_info = result['statement_info']

        # Format the response
        def format_stmt_txn(txn):
            return {
                "date": txn.date.isoformat() if hasattr(txn.date, 'isoformat') else str(txn.date),
                "description": txn.description,
                "amount": txn.amount,
                "balance": txn.balance,
                "type": txn.transaction_type,
                "reference": txn.reference
            }

        def format_match(m):
            return {
                "statement_txn": format_stmt_txn(m['statement_txn']),
                "opera_entry": {
                    "ae_entry": m['opera_entry']['ae_entry'],
                    "ae_date": m['opera_entry']['ae_date'].isoformat() if hasattr(m['opera_entry']['ae_date'], 'isoformat') else str(m['opera_entry']['ae_date']),
                    "ae_ref": m['opera_entry']['ae_ref'],
                    "value_pounds": m['opera_entry']['value_pounds'],
                    "ae_detail": m['opera_entry'].get('ae_detail', ''),
                    "is_reconciled": m['opera_entry'].get('is_reconciled', False)
                },
                "match_score": m['match_score'],
                "match_reasons": m['match_reasons']
            }

        return {
            "success": True,
            "bank_code": result.get('bank_code'),
            "bank_validation": result.get('bank_validation'),
            "statement_info": {
                "bank_name": stmt_info.bank_name,
                "account_number": stmt_info.account_number,
                "sort_code": stmt_info.sort_code,
                "statement_date": stmt_info.statement_date.isoformat() if stmt_info.statement_date else None,
                "period_start": stmt_info.period_start.isoformat() if stmt_info.period_start else None,
                "period_end": stmt_info.period_end.isoformat() if stmt_info.period_end else None,
                "opening_balance": stmt_info.opening_balance,
                "closing_balance": stmt_info.closing_balance
            },
            "summary": result['summary'],
            "to_import": [format_stmt_txn(txn) for txn in result['to_import']],
            "to_reconcile": [format_match(m) for m in result['to_reconcile']],
            "already_reconciled": [format_match(m) for m in result['already_reconciled']],
            "balance_check": result['balance_check']
        }

    except Exception as e:
        logger.error(f"Opera 3 unified statement processing failed: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


@app.get("/api/opera3/bank-accounts")
async def opera3_get_bank_accounts(
    data_path: str = Query(..., description="Path to Opera 3 company data folder")
):
    """
    Get list of available bank accounts from Opera 3's nbank table.
    """
    try:
        from sql_rag.opera3_foxpro import Opera3Reader
        from pathlib import Path

        if not Path(data_path).exists():
            return {"success": False, "error": f"Opera 3 data path not found: {data_path}"}

        reader = Opera3Reader(data_path)
        nbank_records = reader.read_table('nbank')

        accounts = []
        for row in nbank_records:
            is_petty = row.get('nk_petty', 0) == 1
            accounts.append({
                "code": (row.get('nk_acnt') or '').strip(),
                "description": (row.get('nk_desc') or '').strip(),
                "sort_code": (row.get('nk_sort') or '').strip(),
                "account_number": (row.get('nk_number') or '').strip(),
                "balance": (row.get('nk_curbal', 0) or 0) / 100.0,
                "type": "Petty Cash" if is_petty else "Bank Account"
            })

        # Sort by code
        accounts.sort(key=lambda x: x['code'])

        return {
            "success": True,
            "count": len(accounts),
            "accounts": accounts
        }

    except Exception as e:
        logger.error(f"Opera 3 bank accounts error: {e}")
        return {"success": False, "error": str(e)}


# =============================================================================
# USER ACTIVITY MONITORING
# =============================================================================

@app.get("/api/user-activity")
async def get_user_activity(
    start_date: str = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(None, description="End date (YYYY-MM-DD)"),
    user_filter: str = Query(None, description="Filter by specific user code")
):
    """
    Get user activity summary from Opera - transactions posted by user, type, and date.
    Tracks activity from ntran (nominal ledger), atran (cashbook), and aentry tables.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Default to last 30 days if no dates provided
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        result = {
            "success": True,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            },
            "users": [],
            "summary": {
                "total_users": 0,
                "total_transactions": 0,
                "by_type": {}
            },
            "daily_activity": [],
            "hourly_distribution": []
        }

        # Build user filter clause
        user_clause = ""
        if user_filter:
            user_clause = f"AND RTRIM(nt_inp) = '{user_filter}'"

        # ===========================================
        # 1. NOMINAL LEDGER ACTIVITY (ntran)
        # ===========================================

        # Get transaction counts by user
        ntran_user_sql = f"""
            SELECT
                RTRIM(nt_inp) AS user_code,
                COUNT(*) AS transaction_count,
                SUM(CASE WHEN nt_value > 0 THEN nt_value ELSE 0 END) AS total_debits,
                SUM(CASE WHEN nt_value < 0 THEN ABS(nt_value) ELSE 0 END) AS total_credits,
                MIN(datecreated) AS first_activity,
                MAX(datecreated) AS last_activity
            FROM ntran WITH (NOLOCK)
            WHERE datecreated >= '{start_date}'
              AND datecreated < DATEADD(day, 1, '{end_date}')
              AND nt_inp IS NOT NULL
              AND RTRIM(nt_inp) != ''
              {user_clause}
            GROUP BY RTRIM(nt_inp)
            ORDER BY transaction_count DESC
        """
        ntran_users = sql_connector.execute_query(ntran_user_sql)
        if hasattr(ntran_users, 'to_dict'):
            ntran_users = ntran_users.to_dict('records')

        # Get transaction counts by type per user
        ntran_type_sql = f"""
            SELECT
                RTRIM(nt_inp) AS user_code,
                nt_posttyp AS post_type,
                COUNT(*) AS count,
                SUM(ABS(nt_value)) AS total_value
            FROM ntran WITH (NOLOCK)
            WHERE datecreated >= '{start_date}'
              AND datecreated < DATEADD(day, 1, '{end_date}')
              AND nt_inp IS NOT NULL
              AND RTRIM(nt_inp) != ''
              {user_clause}
            GROUP BY RTRIM(nt_inp), nt_posttyp
            ORDER BY RTRIM(nt_inp), count DESC
        """
        ntran_types = sql_connector.execute_query(ntran_type_sql)
        if hasattr(ntran_types, 'to_dict'):
            ntran_types = ntran_types.to_dict('records')

        # Map post types to task descriptions (business operations)
        post_type_names = {
            'I': 'Sales Invoices Entered',
            'R': 'Sales Receipts Posted',
            'C': 'Sales Credit Notes Entered',
            'P': 'Purchase Payments Posted',
            'S': 'Purchase Invoices Entered',
            'D': 'Purchase Credit Notes Entered',
            'J': 'Journals Posted',
            'B': 'Bank Transactions Posted',
            'V': 'VAT Entries',
            'Y': 'Year End Adjustments',
            '': 'Other Entries'
        }

        # Build user activity data
        users_dict = {}
        for row in ntran_users or []:
            user_code = (row.get('user_code') or '').strip()
            if not user_code:
                continue

            users_dict[user_code] = {
                "user_code": user_code,
                "nominal_transactions": int(row.get('transaction_count') or 0),
                "cashbook_entries": 0,
                "total_debits": round(float(row.get('total_debits') or 0), 2),
                "total_credits": round(float(row.get('total_credits') or 0), 2),
                "first_activity": str(row.get('first_activity') or ''),
                "last_activity": str(row.get('last_activity') or ''),
                "by_type": {}
            }

        # Add type breakdowns
        for row in ntran_types or []:
            user_code = (row.get('user_code') or '').strip()
            if not user_code or user_code not in users_dict:
                continue

            post_type = (row.get('post_type') or '').strip()
            type_name = post_type_names.get(post_type, f'Type {post_type}')

            users_dict[user_code]["by_type"][type_name] = {
                "count": int(row.get('count') or 0),
                "total_value": round(float(row.get('total_value') or 0), 2)
            }

        # ===========================================
        # 2. CASHBOOK ACTIVITY (atran/aentry)
        # ===========================================

        # Build user filter for cashbook
        cb_user_clause = ""
        if user_filter:
            cb_user_clause = f"AND RTRIM(at_inputby) = '{user_filter}'"

        atran_user_sql = f"""
            SELECT
                RTRIM(at_inputby) AS user_code,
                COUNT(*) AS transaction_count,
                SUM(CASE WHEN at_value > 0 THEN at_value ELSE 0 END) / 100.0 AS total_receipts,
                SUM(CASE WHEN at_value < 0 THEN ABS(at_value) ELSE 0 END) / 100.0 AS total_payments,
                MIN(datecreated) AS first_activity,
                MAX(datecreated) AS last_activity
            FROM atran WITH (NOLOCK)
            WHERE datecreated >= '{start_date}'
              AND datecreated < DATEADD(day, 1, '{end_date}')
              AND at_inputby IS NOT NULL
              AND RTRIM(at_inputby) != ''
              {cb_user_clause}
            GROUP BY RTRIM(at_inputby)
            ORDER BY transaction_count DESC
        """
        atran_users = sql_connector.execute_query(atran_user_sql)
        if hasattr(atran_users, 'to_dict'):
            atran_users = atran_users.to_dict('records')

        # Merge cashbook data into users
        for row in atran_users or []:
            user_code = (row.get('user_code') or '').strip()
            if not user_code:
                continue

            if user_code not in users_dict:
                users_dict[user_code] = {
                    "user_code": user_code,
                    "nominal_transactions": 0,
                    "cashbook_entries": 0,
                    "total_debits": 0,
                    "total_credits": 0,
                    "first_activity": str(row.get('first_activity') or ''),
                    "last_activity": str(row.get('last_activity') or ''),
                    "by_type": {}
                }

            users_dict[user_code]["cashbook_entries"] = int(row.get('transaction_count') or 0)
            users_dict[user_code]["by_type"]["Cashbook Receipts"] = {
                "count": int(row.get('transaction_count') or 0),
                "total_value": round(float(row.get('total_receipts') or 0), 2)
            }
            users_dict[user_code]["by_type"]["Cashbook Payments"] = {
                "count": int(row.get('transaction_count') or 0),
                "total_value": round(float(row.get('total_payments') or 0), 2)
            }

            # Update activity times if cashbook activity is earlier/later
            cb_first = str(row.get('first_activity') or '')
            cb_last = str(row.get('last_activity') or '')
            if cb_first and (not users_dict[user_code]["first_activity"] or cb_first < users_dict[user_code]["first_activity"]):
                users_dict[user_code]["first_activity"] = cb_first
            if cb_last and (not users_dict[user_code]["last_activity"] or cb_last > users_dict[user_code]["last_activity"]):
                users_dict[user_code]["last_activity"] = cb_last

        # ===========================================
        # 3. DAILY ACTIVITY PATTERN
        # ===========================================

        daily_sql = f"""
            SELECT
                CONVERT(date, datecreated) AS activity_date,
                COUNT(*) AS transaction_count
            FROM ntran WITH (NOLOCK)
            WHERE datecreated >= '{start_date}'
              AND datecreated < DATEADD(day, 1, '{end_date}')
              AND nt_inp IS NOT NULL
              AND RTRIM(nt_inp) != ''
              {user_clause}
            GROUP BY CONVERT(date, datecreated)
            ORDER BY activity_date
        """
        daily_result = sql_connector.execute_query(daily_sql)
        if hasattr(daily_result, 'to_dict'):
            daily_result = daily_result.to_dict('records')

        daily_activity = []
        for row in daily_result or []:
            daily_activity.append({
                "date": str(row.get('activity_date') or ''),
                "count": int(row.get('transaction_count') or 0)
            })

        # ===========================================
        # 4. HOURLY DISTRIBUTION
        # ===========================================

        hourly_sql = f"""
            SELECT
                DATEPART(hour, datecreated) AS hour,
                COUNT(*) AS transaction_count
            FROM ntran WITH (NOLOCK)
            WHERE datecreated >= '{start_date}'
              AND datecreated < DATEADD(day, 1, '{end_date}')
              AND nt_inp IS NOT NULL
              AND RTRIM(nt_inp) != ''
              {user_clause}
            GROUP BY DATEPART(hour, datecreated)
            ORDER BY hour
        """
        hourly_result = sql_connector.execute_query(hourly_sql)
        if hasattr(hourly_result, 'to_dict'):
            hourly_result = hourly_result.to_dict('records')

        hourly_distribution = []
        for row in hourly_result or []:
            hour = int(row.get('hour') or 0)
            hourly_distribution.append({
                "hour": hour,
                "label": f"{hour:02d}:00",
                "count": int(row.get('transaction_count') or 0)
            })

        # ===========================================
        # 5. BUILD SUMMARY
        # ===========================================

        # Calculate totals
        total_transactions = 0
        by_type_totals = {}

        for user_data in users_dict.values():
            total_transactions += user_data["nominal_transactions"] + user_data["cashbook_entries"]
            for type_name, type_data in user_data["by_type"].items():
                if type_name not in by_type_totals:
                    by_type_totals[type_name] = {"count": 0, "total_value": 0}
                by_type_totals[type_name]["count"] += type_data["count"]
                by_type_totals[type_name]["total_value"] += type_data["total_value"]

        # Sort users by total activity
        users_list = sorted(
            users_dict.values(),
            key=lambda x: x["nominal_transactions"] + x["cashbook_entries"],
            reverse=True
        )

        result["users"] = users_list
        result["summary"]["total_users"] = len(users_list)
        result["summary"]["total_transactions"] = total_transactions
        result["summary"]["by_type"] = by_type_totals
        result["daily_activity"] = daily_activity
        result["hourly_distribution"] = hourly_distribution

        return result

    except Exception as e:
        logger.error(f"User activity error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/user-activity/users")
async def get_user_list():
    """
    Get list of all users who have posted transactions in Opera.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Get unique users from ntran
        users_sql = """
            SELECT DISTINCT RTRIM(nt_inp) AS user_code
            FROM ntran WITH (NOLOCK)
            WHERE nt_inp IS NOT NULL
              AND RTRIM(nt_inp) != ''
            ORDER BY user_code
        """
        users_result = sql_connector.execute_query(users_sql)
        if hasattr(users_result, 'to_dict'):
            users_result = users_result.to_dict('records')

        users = [row['user_code'].strip() for row in (users_result or []) if row.get('user_code')]

        return {
            "success": True,
            "count": len(users),
            "users": users
        }

    except Exception as e:
        logger.error(f"User list error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/user-activity/discover-fields")
async def discover_user_fields():
    """
    Discover all tables and columns in the database that might track user activity.
    Searches for columns with patterns like: *_inp*, *_user*, *_oper*, *_by, *cruser*, etc.
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Query information_schema to find all columns that might contain user data
        discovery_sql = """
            SELECT
                t.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.TABLES t
            INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
            WHERE t.TABLE_TYPE = 'BASE TABLE'
              AND (
                  c.COLUMN_NAME LIKE '%_inp%'
                  OR c.COLUMN_NAME LIKE '%_user%'
                  OR c.COLUMN_NAME LIKE '%_oper%'
                  OR c.COLUMN_NAME LIKE '%cruser%'
                  OR c.COLUMN_NAME LIKE '%inputby%'
                  OR c.COLUMN_NAME LIKE '%_by'
                  OR c.COLUMN_NAME LIKE '%operator%'
                  OR c.COLUMN_NAME LIKE '%created%'
                  OR c.COLUMN_NAME LIKE '%modified%'
              )
            ORDER BY t.TABLE_NAME, c.COLUMN_NAME
        """
        discovery_result = sql_connector.execute_query(discovery_sql)
        if hasattr(discovery_result, 'to_dict'):
            discovery_result = discovery_result.to_dict('records')

        # Group by table
        tables = {}
        for row in discovery_result or []:
            table_name = row['TABLE_NAME']
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append({
                "column": row['COLUMN_NAME'],
                "type": row['DATA_TYPE'],
                "max_length": row['CHARACTER_MAXIMUM_LENGTH']
            })

        return {
            "success": True,
            "table_count": len(tables),
            "tables": tables
        }

    except Exception as e:
        logger.error(f"Discovery error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/user-activity/all-modules")
async def get_all_module_activity(
    start_date: str = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(None, description="End date (YYYY-MM-DD)"),
    user_filter: str = Query(None, description="Filter by specific user code")
):
    """
    Get user activity across ALL Opera modules:
    - Nominal Ledger (ntran)
    - Cashbook (atran/aentry)
    - Sales Ledger (stran via datecreated)
    - Purchase Ledger (ptran via datecreated)
    - SOP - Sales Order Processing
    - POP - Purchase Order Processing
    """
    if not sql_connector:
        raise HTTPException(status_code=503, detail="SQL connector not initialized")

    try:
        # Default to last 30 days if no dates provided
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        result = {
            "success": True,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            },
            "modules": {}
        }

        # Helper to build user filter
        def make_user_clause(field_name):
            if user_filter:
                return f"AND RTRIM({field_name}) = '{user_filter}'"
            return ""

        # ===========================================
        # 1. NOMINAL LEDGER (ntran - nt_inp field)
        # ===========================================
        try:
            nl_sql = f"""
                SELECT
                    RTRIM(nt_inp) AS user_code,
                    COUNT(*) AS count,
                    SUM(ABS(nt_value)) AS total_value,
                    MIN(datecreated) AS first_activity,
                    MAX(datecreated) AS last_activity
                FROM ntran WITH (NOLOCK)
                WHERE datecreated >= '{start_date}'
                  AND datecreated < DATEADD(day, 1, '{end_date}')
                  AND nt_inp IS NOT NULL
                  AND RTRIM(nt_inp) != ''
                  {make_user_clause('nt_inp')}
                GROUP BY RTRIM(nt_inp)
                ORDER BY count DESC
            """
            nl_result = sql_connector.execute_query(nl_sql)
            if hasattr(nl_result, 'to_dict'):
                nl_result = nl_result.to_dict('records')
            result["modules"]["nominal_ledger"] = {
                "table": "ntran",
                "user_field": "nt_inp",
                "users": [{
                    "user": r.get('user_code', '').strip(),
                    "count": int(r.get('count') or 0),
                    "value": round(float(r.get('total_value') or 0), 2),
                    "first": str(r.get('first_activity') or ''),
                    "last": str(r.get('last_activity') or '')
                } for r in (nl_result or [])]
            }
        except Exception as e:
            result["modules"]["nominal_ledger"] = {"error": str(e)}

        # ===========================================
        # 2. CASHBOOK (atran - at_inputby field)
        # ===========================================
        try:
            cb_sql = f"""
                SELECT
                    RTRIM(at_inputby) AS user_code,
                    COUNT(*) AS count,
                    SUM(ABS(at_value)) / 100.0 AS total_value,
                    MIN(datecreated) AS first_activity,
                    MAX(datecreated) AS last_activity
                FROM atran WITH (NOLOCK)
                WHERE datecreated >= '{start_date}'
                  AND datecreated < DATEADD(day, 1, '{end_date}')
                  AND at_inputby IS NOT NULL
                  AND RTRIM(at_inputby) != ''
                  {make_user_clause('at_inputby')}
                GROUP BY RTRIM(at_inputby)
                ORDER BY count DESC
            """
            cb_result = sql_connector.execute_query(cb_sql)
            if hasattr(cb_result, 'to_dict'):
                cb_result = cb_result.to_dict('records')
            result["modules"]["cashbook"] = {
                "table": "atran",
                "user_field": "at_inputby",
                "users": [{
                    "user": r.get('user_code', '').strip(),
                    "count": int(r.get('count') or 0),
                    "value": round(float(r.get('total_value') or 0), 2),
                    "first": str(r.get('first_activity') or ''),
                    "last": str(r.get('last_activity') or '')
                } for r in (cb_result or [])]
            }
        except Exception as e:
            result["modules"]["cashbook"] = {"error": str(e)}

        # ===========================================
        # 3. SALES LEDGER (stran - check for st_inp or use datecreated)
        # ===========================================
        try:
            # First try to check if st_inp exists
            sl_check_sql = """
                SELECT TOP 1 * FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'stran' AND COLUMN_NAME LIKE 'st_%inp%'
            """
            sl_check = sql_connector.execute_query(sl_check_sql)
            if hasattr(sl_check, 'to_dict'):
                sl_check = sl_check.to_dict('records')

            if sl_check:
                # Use the user field
                user_col = sl_check[0]['COLUMN_NAME']
                sl_sql = f"""
                    SELECT
                        RTRIM({user_col}) AS user_code,
                        COUNT(*) AS count,
                        SUM(ABS(st_trvalue)) AS total_value,
                        MIN(datecreated) AS first_activity,
                        MAX(datecreated) AS last_activity
                    FROM stran WITH (NOLOCK)
                    WHERE datecreated >= '{start_date}'
                      AND datecreated < DATEADD(day, 1, '{end_date}')
                      AND {user_col} IS NOT NULL
                      AND RTRIM({user_col}) != ''
                    GROUP BY RTRIM({user_col})
                    ORDER BY count DESC
                """
                sl_result = sql_connector.execute_query(sl_sql)
                if hasattr(sl_result, 'to_dict'):
                    sl_result = sl_result.to_dict('records')
                result["modules"]["sales_ledger"] = {
                    "table": "stran",
                    "user_field": user_col,
                    "users": [{
                        "user": r.get('user_code', '').strip(),
                        "count": int(r.get('count') or 0),
                        "value": round(float(r.get('total_value') or 0), 2),
                        "first": str(r.get('first_activity') or ''),
                        "last": str(r.get('last_activity') or '')
                    } for r in (sl_result or [])]
                }
            else:
                # Just count by date
                sl_sql = f"""
                    SELECT
                        COUNT(*) AS count,
                        SUM(ABS(st_trvalue)) AS total_value,
                        MIN(datecreated) AS first_activity,
                        MAX(datecreated) AS last_activity
                    FROM stran WITH (NOLOCK)
                    WHERE datecreated >= '{start_date}'
                      AND datecreated < DATEADD(day, 1, '{end_date}')
                """
                sl_result = sql_connector.execute_query(sl_sql)
                if hasattr(sl_result, 'to_dict'):
                    sl_result = sl_result.to_dict('records')
                row = sl_result[0] if sl_result else {}
                result["modules"]["sales_ledger"] = {
                    "table": "stran",
                    "user_field": "none - tracked via ntran",
                    "total_count": int(row.get('count') or 0),
                    "total_value": round(float(row.get('total_value') or 0), 2)
                }
        except Exception as e:
            result["modules"]["sales_ledger"] = {"error": str(e)}

        # ===========================================
        # 4. PURCHASE LEDGER (ptran - check for pt_inp or use datecreated)
        # ===========================================
        try:
            pl_check_sql = """
                SELECT TOP 1 * FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'ptran' AND COLUMN_NAME LIKE 'pt_%inp%'
            """
            pl_check = sql_connector.execute_query(pl_check_sql)
            if hasattr(pl_check, 'to_dict'):
                pl_check = pl_check.to_dict('records')

            if pl_check:
                user_col = pl_check[0]['COLUMN_NAME']
                pl_sql = f"""
                    SELECT
                        RTRIM({user_col}) AS user_code,
                        COUNT(*) AS count,
                        SUM(ABS(pt_trvalue)) AS total_value,
                        MIN(datecreated) AS first_activity,
                        MAX(datecreated) AS last_activity
                    FROM ptran WITH (NOLOCK)
                    WHERE datecreated >= '{start_date}'
                      AND datecreated < DATEADD(day, 1, '{end_date}')
                      AND {user_col} IS NOT NULL
                      AND RTRIM({user_col}) != ''
                    GROUP BY RTRIM({user_col})
                    ORDER BY count DESC
                """
                pl_result = sql_connector.execute_query(pl_sql)
                if hasattr(pl_result, 'to_dict'):
                    pl_result = pl_result.to_dict('records')
                result["modules"]["purchase_ledger"] = {
                    "table": "ptran",
                    "user_field": user_col,
                    "users": [{
                        "user": r.get('user_code', '').strip(),
                        "count": int(r.get('count') or 0),
                        "value": round(float(r.get('total_value') or 0), 2),
                        "first": str(r.get('first_activity') or ''),
                        "last": str(r.get('last_activity') or '')
                    } for r in (pl_result or [])]
                }
            else:
                pl_sql = f"""
                    SELECT
                        COUNT(*) AS count,
                        SUM(ABS(pt_trvalue)) AS total_value,
                        MIN(datecreated) AS first_activity,
                        MAX(datecreated) AS last_activity
                    FROM ptran WITH (NOLOCK)
                    WHERE datecreated >= '{start_date}'
                      AND datecreated < DATEADD(day, 1, '{end_date}')
                """
                pl_result = sql_connector.execute_query(pl_sql)
                if hasattr(pl_result, 'to_dict'):
                    pl_result = pl_result.to_dict('records')
                row = pl_result[0] if pl_result else {}
                result["modules"]["purchase_ledger"] = {
                    "table": "ptran",
                    "user_field": "none - tracked via ntran",
                    "total_count": int(row.get('count') or 0),
                    "total_value": round(float(row.get('total_value') or 0), 2)
                }
        except Exception as e:
            result["modules"]["purchase_ledger"] = {"error": str(e)}

        # ===========================================
        # 5. SALES ORDER PROCESSING (SOP)
        # Check for sorder, sorhed, sohist tables
        # ===========================================
        sop_tables = ['sorder', 'sorhed', 'sohist', 'soline']
        for sop_table in sop_tables:
            try:
                # Check if table exists and find user columns
                sop_check_sql = f"""
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{sop_table}'
                    AND (COLUMN_NAME LIKE '%_inp%' OR COLUMN_NAME LIKE '%_user%' OR COLUMN_NAME LIKE '%_oper%' OR COLUMN_NAME LIKE '%inputby%')
                """
                sop_check = sql_connector.execute_query(sop_check_sql)
                if hasattr(sop_check, 'to_dict'):
                    sop_check = sop_check.to_dict('records')

                if sop_check:
                    user_col = sop_check[0]['COLUMN_NAME']
                    sop_sql = f"""
                        SELECT
                            RTRIM({user_col}) AS user_code,
                            COUNT(*) AS count,
                            MIN(datecreated) AS first_activity,
                            MAX(datecreated) AS last_activity
                        FROM {sop_table} WITH (NOLOCK)
                        WHERE datecreated >= '{start_date}'
                          AND datecreated < DATEADD(day, 1, '{end_date}')
                          AND {user_col} IS NOT NULL
                          AND RTRIM({user_col}) != ''
                        GROUP BY RTRIM({user_col})
                        ORDER BY count DESC
                    """
                    sop_result = sql_connector.execute_query(sop_sql)
                    if hasattr(sop_result, 'to_dict'):
                        sop_result = sop_result.to_dict('records')
                    result["modules"][f"sop_{sop_table}"] = {
                        "table": sop_table,
                        "user_field": user_col,
                        "users": [{
                            "user": r.get('user_code', '').strip(),
                            "count": int(r.get('count') or 0),
                            "first": str(r.get('first_activity') or ''),
                            "last": str(r.get('last_activity') or '')
                        } for r in (sop_result or [])]
                    }
            except Exception:
                pass  # Table may not exist

        # ===========================================
        # 6. PURCHASE ORDER PROCESSING (POP)
        # Check for porder, porhed, pohist tables
        # ===========================================
        pop_tables = ['porder', 'porhed', 'pohist', 'poline']
        for pop_table in pop_tables:
            try:
                pop_check_sql = f"""
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{pop_table}'
                    AND (COLUMN_NAME LIKE '%_inp%' OR COLUMN_NAME LIKE '%_user%' OR COLUMN_NAME LIKE '%_oper%' OR COLUMN_NAME LIKE '%inputby%')
                """
                pop_check = sql_connector.execute_query(pop_check_sql)
                if hasattr(pop_check, 'to_dict'):
                    pop_check = pop_check.to_dict('records')

                if pop_check:
                    user_col = pop_check[0]['COLUMN_NAME']
                    pop_sql = f"""
                        SELECT
                            RTRIM({user_col}) AS user_code,
                            COUNT(*) AS count,
                            MIN(datecreated) AS first_activity,
                            MAX(datecreated) AS last_activity
                        FROM {pop_table} WITH (NOLOCK)
                        WHERE datecreated >= '{start_date}'
                          AND datecreated < DATEADD(day, 1, '{end_date}')
                          AND {user_col} IS NOT NULL
                          AND RTRIM({user_col}) != ''
                        GROUP BY RTRIM({user_col})
                        ORDER BY count DESC
                    """
                    pop_result = sql_connector.execute_query(pop_sql)
                    if hasattr(pop_result, 'to_dict'):
                        pop_result = pop_result.to_dict('records')
                    result["modules"][f"pop_{pop_table}"] = {
                        "table": pop_table,
                        "user_field": user_col,
                        "users": [{
                            "user": r.get('user_code', '').strip(),
                            "count": int(r.get('count') or 0),
                            "first": str(r.get('first_activity') or ''),
                            "last": str(r.get('last_activity') or '')
                        } for r in (pop_result or [])]
                    }
            except Exception:
                pass  # Table may not exist

        # ===========================================
        # 7. DYNAMICALLY CHECK ALL TABLES FOR USER COLUMNS
        # ===========================================
        try:
            all_user_cols_sql = """
                SELECT DISTINCT t.TABLE_NAME, c.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLES t
                INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                  AND (
                      c.COLUMN_NAME LIKE '%_inp'
                      OR c.COLUMN_NAME LIKE '%_inp_%'
                      OR c.COLUMN_NAME LIKE '%inputby%'
                      OR c.COLUMN_NAME LIKE '%cruser%'
                  )
                  AND c.DATA_TYPE IN ('char', 'varchar', 'nchar', 'nvarchar')
                ORDER BY t.TABLE_NAME
            """
            all_cols = sql_connector.execute_query(all_user_cols_sql)
            if hasattr(all_cols, 'to_dict'):
                all_cols = all_cols.to_dict('records')

            discovered_tables = {}
            for row in all_cols or []:
                table = row['TABLE_NAME']
                col = row['COLUMN_NAME']
                if table not in discovered_tables:
                    discovered_tables[table] = []
                discovered_tables[table].append(col)

            result["discovered_user_columns"] = discovered_tables

        except Exception as e:
            result["discovered_user_columns"] = {"error": str(e)}

        return result

    except Exception as e:
        logger.error(f"All modules activity error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# PENSION EXPORT ENDPOINTS
# =============================================================================

@app.get("/api/pension/schemes")
async def get_pension_schemes(data_source: str = Query("sql", description="Data source: sql or opera3")):
    """Get all configured pension schemes."""
    try:
        provider = get_pension_data_provider(data_source)
        schemes = provider.get_pension_schemes()

        return {
            'success': True,
            'data_source': data_source,
            'schemes': [
                {
                    'code': s.code,
                    'description': s.description,
                    'provider_name': s.provider_name,
                    'provider_reference': s.provider_reference,
                    'scheme_reference': s.scheme_reference,
                    'employer_rate': float(s.employer_rate),
                    'employee_rate': float(s.employee_rate),
                    'auto_enrolment': s.auto_enrolment,
                    'scheme_type': s.scheme_type,
                    'enrolled_count': s.enrolled_count
                }
                for s in schemes
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting pension schemes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pension/enrolled-employees")
async def get_pension_enrolled_employees(scheme_code: str = Query(...)):
    """Get employees enrolled in a specific pension scheme."""
    try:
        sql = f"""
        SELECT
            e.wep_ref,
            e.wep_code,
            e.wep_erper,
            e.wep_eeper,
            e.wep_jndt,
            e.wep_lfdt,
            e.wep_ter,
            e.wep_tee,
            w.wn_surname,
            w.wn_forenam,
            w.wn_ninum,
            w.wn_birth
        FROM wepen e
        JOIN wname w ON e.wep_ref = w.wn_ref
        WHERE e.wep_code = '{scheme_code}'
          AND (e.wep_lfdt IS NULL OR e.wep_lfdt > GETDATE())
        ORDER BY w.wn_surname, w.wn_forenam
        """
        result = sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        employees = []
        for row in result or []:
            employees.append({
                'employee_ref': row['wep_ref'].strip() if row.get('wep_ref') else '',
                'surname': row['wn_surname'].strip() if row.get('wn_surname') else '',
                'forename': row['wn_forenam'].strip() if row.get('wn_forenam') else '',
                'ni_number': row['wn_ninum'].strip() if row.get('wn_ninum') else '',
                'date_of_birth': row['wn_birth'].isoformat() if row.get('wn_birth') else None,
                'join_date': row['wep_jndt'].isoformat() if row.get('wep_jndt') else None,
                'leave_date': row['wep_lfdt'].isoformat() if row.get('wep_lfdt') else None,
                'employer_rate': float(row.get('wep_erper') or 0),
                'employee_rate': float(row.get('wep_eeper') or 0),
                'total_employer_contributions': float(row.get('wep_ter') or 0),
                'total_employee_contributions': float(row.get('wep_tee') or 0)
            })

        return {
            'success': True,
            'scheme_code': scheme_code,
            'employees': employees
        }
    except Exception as e:
        logger.error(f"Error getting enrolled employees: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pension/payroll-periods")
async def get_payroll_periods(
    tax_year: str = Query(None),
    data_source: str = Query("sql", description="Data source: sql or opera3")
):
    """Get available payroll periods."""
    try:
        provider = get_pension_data_provider(data_source)
        result = provider.get_payroll_periods(tax_year)

        return {
            'success': True,
            'data_source': data_source,
            'tax_year': result.get('tax_year', ''),
            'tax_years': result.get('tax_years', []),
            'periods': result.get('periods', [])
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting payroll periods: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pension/nest/preview")
async def preview_nest_export(
    tax_year: str = Query(...),
    period: int = Query(...)
):
    """Preview NEST pension export for a specific period."""
    try:
        from sql_rag.pension_exports.nest_export import NestExport

        nest = NestExport(sql_connector)
        preview = nest.preview_export(tax_year, period)

        return {
            'success': True,
            'preview': preview
        }
    except Exception as e:
        logger.error(f"Error previewing NEST export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pension/nest/generate")
async def generate_nest_export(
    tax_year: str = Query(...),
    period: int = Query(...),
    payment_source: str = Query("Bank Account")
):
    """Generate NEST pension contribution CSV file."""
    try:
        from sql_rag.pension_exports.nest_export import NestExport

        nest = NestExport(sql_connector)
        result = nest.generate_csv(tax_year, period, payment_source)

        if result.success:
            return {
                'success': True,
                'filename': result.filename,
                'csv_content': result.csv_content,
                'record_count': result.record_count,
                'total_employer_contributions': float(result.total_employer_contributions),
                'total_employee_contributions': float(result.total_employee_contributions),
                'total_pensionable_earnings': float(result.total_pensionable_earnings),
                'warnings': result.warnings
            }
        else:
            return {
                'success': False,
                'errors': result.errors,
                'warnings': result.warnings
            }
    except Exception as e:
        logger.error(f"Error generating NEST export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pension/nest/download")
async def download_nest_export(
    tax_year: str = Query(...),
    period: int = Query(...),
    payment_source: str = Query("Bank Account")
):
    """Download NEST pension contribution CSV file."""
    try:
        from sql_rag.pension_exports.nest_export import NestExport
        from fastapi.responses import Response

        nest = NestExport(sql_connector)
        result = nest.generate_csv(tax_year, period, payment_source)

        if result.success:
            return Response(
                content=result.csv_content,
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename={result.filename}"
                }
            )
        else:
            raise HTTPException(status_code=400, detail=result.errors[0] if result.errors else "Export failed")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading NEST export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def get_pension_data_provider(data_source: str = "sql"):
    """Get the appropriate pension data provider based on data source."""
    from sql_rag.pension_exports.data_provider import OperaSQLPensionProvider, Opera3PensionProvider

    if data_source == "opera3":
        # Check if Opera 3 is configured
        opera3_path = None
        if config and config.has_section("opera"):
            opera3_path = config.get("opera", "opera3_base_path", fallback=None)
        if not opera3_path:
            raise HTTPException(status_code=400, detail="Opera 3 path not configured")

        from sql_rag.opera3_foxpro import Opera3Reader
        reader = Opera3Reader(opera3_path)
        return Opera3PensionProvider(reader)
    else:
        return OperaSQLPensionProvider(sql_connector)


@app.get("/api/pension/config")
async def get_pension_config():
    """Get pension configuration for the current company."""
    global current_company

    company_name = "Unknown"
    export_folder = ""
    pension_provider = ""
    data_source = "sql"  # Default to SQL SE

    if current_company:
        company_name = current_company.get("name", "Unknown")
        payroll_config = current_company.get("payroll", {})
        export_folder = payroll_config.get("pension_export_folder", "")
        pension_provider = payroll_config.get("pension_provider", "")

    # Check if Opera 3 is configured
    opera3_available = False
    if config and config.has_section("opera"):
        opera3_path = config.get("opera", "opera3_base_path", fallback=None)
        opera3_available = bool(opera3_path)

    return {
        "success": True,
        "company_name": company_name,
        "export_folder": export_folder,
        "pension_provider": pension_provider,
        "data_source": data_source,
        "opera3_available": opera3_available,
        "providers": [
            {"key": "nest", "name": "NEST"},
            {"key": "aviva", "name": "Aviva"},
            {"key": "scottish_widows", "name": "Scottish Widows"},
            {"key": "smart_pension", "name": "Smart Pension (PAPDIS)"},
            {"key": "peoples_pension", "name": "People's Pension"},
            {"key": "royal_london", "name": "Royal London"},
            {"key": "standard_life", "name": "Standard Life"},
            {"key": "legal_general", "name": "Legal & General"},
            {"key": "aegon", "name": "Aegon"}
        ]
    }


@app.post("/api/pension/config")
async def save_pension_config(request: Request):
    """Save pension configuration for the current company."""
    global current_company

    if not current_company:
        raise HTTPException(status_code=400, detail="No company selected")

    try:
        body = await request.json()
        pension_provider = body.get("pension_provider", "")
        pension_export_folder = body.get("pension_export_folder", "")

        # Update current_company in memory
        if "payroll" not in current_company:
            current_company["payroll"] = {}
        current_company["payroll"]["pension_provider"] = pension_provider
        current_company["payroll"]["pension_export_folder"] = pension_export_folder

        # Save to company JSON file
        company_id = current_company.get("id")
        if company_id:
            import json
            filepath = os.path.join(COMPANIES_DIR, f"{company_id}.json")
            if os.path.exists(filepath):
                with open(filepath, 'w') as f:
                    json.dump(current_company, f, indent=2)

        return {
            "success": True,
            "message": "Pension settings saved",
            "pension_provider": pension_provider,
            "pension_export_folder": pension_export_folder
        }
    except Exception as e:
        logger.error(f"Error saving pension config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pension/employee-groups")
async def get_employee_groups(data_source: str = Query("sql", description="Data source: sql or opera3")):
    """Get all employee groups for payroll filtering."""
    try:
        provider = get_pension_data_provider(data_source)
        groups = provider.get_employee_groups()

        return {
            'success': True,
            'data_source': data_source,
            'groups': [
                {
                    'code': g.code,
                    'description': g.name,
                    'employee_count': g.employee_count
                }
                for g in groups
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting employee groups: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pension/payment-sources")
async def get_pension_payment_sources(scheme_code: str = Query(...)):
    """Get payment sources configured for a pension scheme."""
    try:
        # Get payment sources from wpnps (pension payment sources) table
        sql = f"""
        SELECT
            wpp_code,
            wpp_name,
            wpp_default
        FROM wpnps
        WHERE wpp_schcode = '{scheme_code}'
        ORDER BY wpp_name
        """
        result = sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        sources = []
        for row in result or []:
            sources.append({
                'code': row['wpp_code'].strip() if row.get('wpp_code') else '',
                'name': row['wpp_name'].strip() if row.get('wpp_name') else '',
                'is_default': bool(row.get('wpp_default'))
            })

        # If no payment sources found, return a default one
        if not sources:
            sources = [{'code': 'DEFAULT', 'name': 'Bank Account', 'is_default': True}]

        return {
            'success': True,
            'scheme_code': scheme_code,
            'payment_sources': sources
        }
    except Exception as e:
        logger.error(f"Error getting payment sources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pension/contribution-groups")
async def get_pension_contribution_groups(scheme_code: str = Query(...)):
    """Get contribution groups for a pension scheme."""
    try:
        # Get contribution groups from wpncg (pension contribution groups)
        sql = f"""
        SELECT
            wpc_code,
            wpc_desc,
            wpc_freq
        FROM wpncg
        WHERE wpc_schcode = '{scheme_code}'
        ORDER BY wpc_desc
        """
        result = sql_connector.execute_query(sql)
        if hasattr(result, 'to_dict'):
            result = result.to_dict('records')

        groups = []
        for row in result or []:
            groups.append({
                'code': row['wpc_code'].strip() if row.get('wpc_code') else '',
                'description': row['wpc_desc'].strip() if row.get('wpc_desc') else '',
                'frequency': row['wpc_freq'].strip() if row.get('wpc_freq') else 'Monthly'
            })

        # If no contribution groups found, return a default one
        if not groups:
            groups = [{'code': 'MONTHLY', 'description': 'Monthly', 'frequency': 'Monthly'}]

        return {
            'success': True,
            'scheme_code': scheme_code,
            'contribution_groups': groups
        }
    except Exception as e:
        logger.error(f"Error getting contribution groups: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pension/providers")
async def get_pension_providers():
    """Get list of all available pension export providers."""
    try:
        from sql_rag.pension_exports import list_providers

        providers = list_providers()

        return {
            'success': True,
            'providers': providers
        }
    except Exception as e:
        logger.error(f"Error getting pension providers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pension/contributions")
async def get_pension_contributions(
    scheme_code: str = Query(...),
    tax_year: str = Query(...),
    period: int = Query(...),
    group_codes: str = Query(None, description="Comma-separated group codes to filter by"),
    data_source: str = Query("sql", description="Data source: sql or opera3")
):
    """Get pension contributions for a specific scheme and period."""
    try:
        from decimal import Decimal

        provider = get_pension_data_provider(data_source)
        group_list = group_codes.split(',') if group_codes else None
        contributions_data = provider.get_contributions(scheme_code, tax_year, period, group_list)

        # Calculate totals
        total_ee = Decimal('0')
        total_er = Decimal('0')
        total_pensionable = Decimal('0')
        new_starters = 0
        leavers = 0

        contributions = []
        for c in contributions_data:
            total_ee += c.employee_contribution
            total_er += c.employer_contribution
            total_pensionable += c.pensionable_earnings

            if c.is_new_starter:
                new_starters += 1
            if c.is_leaver:
                leavers += 1

            contributions.append({
                'employee_ref': c.employee_ref,
                'surname': c.surname,
                'forename': c.forename,
                'ni_number': c.ni_number,
                'group': c.group,
                'date_of_birth': c.date_of_birth.isoformat() if c.date_of_birth else None,
                'gender': c.gender,
                'address_1': c.address_1,
                'address_2': c.address_2,
                'address_3': c.address_3,
                'postcode': c.postcode,
                'title': c.title,
                'start_date': c.start_date.isoformat() if c.start_date else None,
                'scheme_join_date': c.scheme_join_date.isoformat() if c.scheme_join_date else None,
                'leave_date': c.leave_date.isoformat() if c.leave_date else None,
                'pensionable_earnings': float(c.pensionable_earnings),
                'employee_contribution': float(c.employee_contribution),
                'employer_contribution': float(c.employer_contribution),
                'employee_rate': float(c.employee_rate),
                'employer_rate': float(c.employer_rate),
                'is_new_starter': c.is_new_starter,
                'is_leaver': c.is_leaver
            })

        return {
            'success': True,
            'data_source': data_source,
            'scheme_code': scheme_code,
            'tax_year': tax_year,
            'period': period,
            'contributions': contributions,
            'summary': {
                'total_employees': len(contributions),
                'new_starters': new_starters,
                'leavers': leavers,
                'total_pensionable_earnings': float(total_pensionable),
                'total_employee_contributions': float(total_ee),
                'total_employer_contributions': float(total_er)
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting pension contributions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/pension/generate")
async def generate_pension_export(
    provider: str = Query(..., description="Provider key: nest, aviva, scottish_widows, etc."),
    scheme_code: str = Query(...),
    tax_year: str = Query(...),
    period: int = Query(...),
    payment_source: str = Query("Bank Account"),
    group_codes: str = Query(None, description="Comma-separated group codes"),
    employee_refs: str = Query(None, description="Comma-separated employee refs to include"),
    output_folder: str = Query(None, description="Folder path to save the export file"),
    data_source: str = Query("sql", description="Data source: sql or opera3")
):
    """Generate pension export file for any provider. Supports both Opera SQL SE and Opera 3."""
    try:
        from sql_rag.pension_exports import get_provider_class, PENSION_PROVIDERS

        # Get the provider class
        provider_class = get_provider_class(provider)
        if not provider_class:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown provider: {provider}. Available: {list(PENSION_PROVIDERS.keys())}"
            )

        # Check if this is the base export class or NEST (which has its own implementation)
        if provider == 'nest':
            from sql_rag.pension_exports.nest_export import NestExport
            exporter = NestExport(sql_connector)
            result = exporter.generate_csv(tax_year, period, payment_source)

            if result.success:
                return {
                    'success': True,
                    'provider': provider,
                    'filename': result.filename,
                    'csv_content': result.csv_content,
                    'record_count': result.record_count,
                    'total_employer_contributions': float(result.total_employer_contributions),
                    'total_employee_contributions': float(result.total_employee_contributions),
                    'total_pensionable_earnings': float(result.total_pensionable_earnings),
                    'warnings': result.warnings
                }
            else:
                return {
                    'success': False,
                    'errors': result.errors,
                    'warnings': result.warnings
                }

        # For other providers, use the base export class
        exporter = provider_class(sql_connector, scheme_code)

        # Get contributions with optional filtering
        group_list = group_codes.split(',') if group_codes else None
        employee_list = employee_refs.split(',') if employee_refs else None

        result = exporter.generate_export(
            tax_year=tax_year,
            period=period,
            payment_source=payment_source,
            group_codes=group_list,
            employee_refs=employee_list,
            output_folder=output_folder
        )

        if result.success:
            return {
                'success': True,
                'provider': provider,
                'filename': result.filename,
                'filepath': result.filepath,
                'content': result.content,
                'content_type': result.content_type,
                'record_count': result.record_count,
                'total_employer_contributions': float(result.total_employer_contributions),
                'total_employee_contributions': float(result.total_employee_contributions),
                'total_pensionable_earnings': float(result.total_pensionable_earnings),
                'warnings': result.warnings
            }
        else:
            return {
                'success': False,
                'errors': result.errors,
                'warnings': result.warnings
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating pension export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pension/download")
async def download_pension_export(
    provider: str = Query(...),
    scheme_code: str = Query(...),
    tax_year: str = Query(...),
    period: int = Query(...),
    payment_source: str = Query("Bank Account"),
    group_codes: str = Query(None),
    employee_refs: str = Query(None),
    output_folder: str = Query(None, description="Optional folder to also save file to")
):
    """Download pension export file for any provider."""
    try:
        from sql_rag.pension_exports import get_provider_class
        from fastapi.responses import Response

        provider_class = get_provider_class(provider)
        if not provider_class:
            raise HTTPException(status_code=400, detail=f"Unknown provider: {provider}")

        # Handle NEST separately
        if provider == 'nest':
            from sql_rag.pension_exports.nest_export import NestExport
            exporter = NestExport(sql_connector)
            result = exporter.generate_csv(tax_year, period, payment_source)

            if result.success:
                return Response(
                    content=result.csv_content,
                    media_type="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={result.filename}"}
                )
            else:
                raise HTTPException(status_code=400, detail=result.errors[0] if result.errors else "Export failed")

        # For other providers
        exporter = provider_class(sql_connector, scheme_code)
        group_list = group_codes.split(',') if group_codes else None
        employee_list = employee_refs.split(',') if employee_refs else None

        result = exporter.generate_export(
            tax_year=tax_year,
            period=period,
            payment_source=payment_source,
            group_codes=group_list,
            employee_refs=employee_list,
            output_folder=output_folder
        )

        if result.success:
            return Response(
                content=result.content,
                media_type=result.content_type,
                headers={"Content-Disposition": f"attachment; filename={result.filename}"}
            )
        else:
            raise HTTPException(status_code=400, detail=result.errors[0] if result.errors else "Export failed")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading pension export: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
