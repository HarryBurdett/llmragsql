"""
SQL Connector module for database operations.

This module provides functionality for connecting to various SQL databases,
executing queries, and returning results as pandas DataFrames. It includes
connection pooling, multi-database support, and robust error handling.
"""

import logging
import configparser
import os
import time
import urllib.parse
from typing import Optional, List, Dict, Any, Union, Tuple, Generator

import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_CONNECTION_TIMEOUT = 30
DEFAULT_COMMAND_TIMEOUT = 60
DEFAULT_POOL_SIZE = 5
DEFAULT_MAX_OVERFLOW = 10
DEFAULT_POOL_TIMEOUT = 30
DEFAULT_POOL_RECYCLE = 3600  # 1 hour
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 1.0  # seconds

class DatabaseType:
    """Enum-like class for supported database types"""
    MSSQL = "mssql"
    MYSQL = "mysql"
    POSTGRES = "postgresql"
    SQLITE = "sqlite"
    ORACLE = "oracle"

    @classmethod
    def all(cls) -> List[str]:
        """Return all supported database types"""
        return [cls.MSSQL, cls.MYSQL, cls.POSTGRES, cls.SQLITE, cls.ORACLE]

class DatabaseError(Exception):
    """Base class for all database-related exceptions"""
    pass

class ConnectionError(DatabaseError):
    """Exception raised for connection issues"""
    pass

class QueryError(DatabaseError):
    """Exception raised for query execution issues"""
    pass

class SQLConnector:
    """Class for connecting to SQL databases and executing queries with connection pooling."""
    
    def __init__(self, config_path: str = "config.ini", connection_args: Dict[str, Any] = None):
        """
        Initialize the SQL connector with configuration.
        
        Args:
            config_path: Path to the configuration file containing database settings
            connection_args: Optional override for connection arguments
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.connection_args = connection_args or {}
        self.db_type = self._get_database_type()
        self.engine = self._create_engine()
        self.metadata = MetaData()
        
    def _load_config(self) -> configparser.ConfigParser:
        """
        Load configuration from the config file.
        
        Returns:
            ConfigParser object with loaded configuration
            
        Raises:
            FileNotFoundError: If the config file doesn't exist
            configparser.Error: If there's an error parsing the config file
        """
        if not os.path.exists(self.config_path):
            logger.warning(f"Config file not found: {self.config_path}, using default configuration")
            config = configparser.ConfigParser()
            config['database'] = {
                'type': DatabaseType.MSSQL,
                'server': 'localhost',
                'database': '',
                'use_windows_auth': 'false',
                'username': '',
                'password': '',
                'pool_size': str(DEFAULT_POOL_SIZE),
                'max_overflow': str(DEFAULT_MAX_OVERFLOW),
                'pool_timeout': str(DEFAULT_POOL_TIMEOUT),
                'pool_recycle': str(DEFAULT_POOL_RECYCLE),
                'connection_timeout': str(DEFAULT_CONNECTION_TIMEOUT),
                'command_timeout': str(DEFAULT_COMMAND_TIMEOUT)
            }
            return config
        
        config = configparser.ConfigParser()
        try:
            config.read(self.config_path)
            return config
        except configparser.Error as e:
            logger.error(f"Error parsing config file: {e}")
            raise
    
    def _get_database_type(self) -> str:
        """
        Get the database type from configuration.
        
        Returns:
            Database type as a string
        """
        db_config = self.config['database']
        db_type = db_config.get('type', DatabaseType.MSSQL)
        
        # Convert to string and lowercase if it's a complex type
        if not isinstance(db_type, str):
            db_type = str(db_type)
        
        db_type = db_type.lower()
        
        if db_type not in DatabaseType.all():
            logger.warning(f"Unsupported database type: {db_type}, defaulting to {DatabaseType.MSSQL}")
            return DatabaseType.MSSQL
        
        return db_type
    
    def _get_connection_parameters(self) -> Dict[str, Any]:
        """
        Get connection parameters from configuration.
        
        Returns:
            Dictionary of connection parameters
        """
        db_config = self.config['database']
        
        params = {
            'server': db_config.get('server', 'localhost'),
            'port': db_config.get('port', ''),
            'database': db_config.get('database', ''),
            'username': db_config.get('username', ''),
            'password': db_config.get('password', ''),
            'use_windows_auth': db_config.getboolean('use_windows_auth', False),
            'pool_size': db_config.getint('pool_size', DEFAULT_POOL_SIZE),
            'max_overflow': db_config.getint('max_overflow', DEFAULT_MAX_OVERFLOW),
            'pool_timeout': db_config.getint('pool_timeout', DEFAULT_POOL_TIMEOUT),
            'pool_recycle': db_config.getint('pool_recycle', DEFAULT_POOL_RECYCLE),
            'connection_timeout': db_config.getint('connection_timeout', DEFAULT_CONNECTION_TIMEOUT),
            'command_timeout': db_config.getint('command_timeout', DEFAULT_COMMAND_TIMEOUT),
            'ssl': db_config.getboolean('ssl', False),
            'ssl_ca': db_config.get('ssl_ca', ''),
            'ssl_cert': db_config.get('ssl_cert', ''),
            'ssl_key': db_config.get('ssl_key', ''),
            'trust_server_certificate': db_config.getboolean('trust_server_certificate', True)
        }
        
        # Override with connection_args if provided
        params.update(self.connection_args)
        
        return params
    
    def _build_connection_string(self) -> str:
        """
        Build a connection string based on the database type and parameters.
        
        Returns:
            Connection string for SQLAlchemy
        """
        params = self._get_connection_parameters()
        
        if self.db_type == DatabaseType.MSSQL:
            # Default driver name
            driver = "ODBC Driver 17 for SQL Server"
            
            # For macOS/Linux: try to detect available drivers
            if os.name == 'posix':
                try:
                    # Get available drivers
                    drivers = pyodbc.drivers()
                    logger.info(f"Available ODBC drivers: {drivers}")
                    
                    # Try to find a SQL Server driver
                    if drivers:
                        for d in drivers:
                            if 'SQL Server' in d:
                                driver = d
                                logger.info(f"Using detected SQL Server driver: {driver}")
                                break
                    
                    # If no SQL Server driver found, try to use SQLite instead
                    if not drivers or not any('SQL Server' in d for d in drivers):
                        logger.warning("No SQL Server ODBC driver found. Using SQLite instead.")
                        # Change database type to SQLite
                        self.db_type = DatabaseType.SQLITE
                        # Create an in-memory SQLite database
                        return "sqlite:///:memory:"
                except Exception as e:
                    logger.error(f"Error detecting ODBC drivers: {e}")
                    logger.warning("Falling back to SQLite.")
                    # Change database type to SQLite
                    self.db_type = DatabaseType.SQLITE
                    # Create an in-memory SQLite database
                    return "sqlite:///:memory:"
            
            conn_str = f"mssql+pyodbc://"

            # Build server string with port if specified
            server_str = params['server']
            if params.get('port'):
                server_str = f"{params['server']},{params['port']}"

            # Authentication
            if params['use_windows_auth']:
                conn_str += f"/?odbc_connect="
                odbc_params = [
                    f"DRIVER={{{driver}}}",
                    f"SERVER={server_str}",
                ]
                if params['database']:
                    odbc_params.append(f"DATABASE={params['database']}")
                odbc_params.append("Trusted_Connection=yes")
                if params['connection_timeout']:
                    odbc_params.append(f"Connection Timeout={params['connection_timeout']}")
                # ODBC Driver 18 requires TrustServerCertificate for non-verified certs
                if params.get('trust_server_certificate', True):
                    odbc_params.append("TrustServerCertificate=yes")
                conn_str += urllib.parse.quote_plus(";".join(odbc_params))
            else:
                if params['username'] and params['password']:
                    conn_str += f"{urllib.parse.quote_plus(params['username'])}:{urllib.parse.quote_plus(params['password'])}@"
                conn_str += f"{server_str}"
                if params['database']:
                    conn_str += f"/{params['database']}"
                conn_str += f"?driver={urllib.parse.quote_plus(driver)}"
                if params['connection_timeout']:
                    conn_str += f"&timeout={params['connection_timeout']}"
                # ODBC Driver 18 requires TrustServerCertificate for non-verified certs
                if params.get('trust_server_certificate', True):
                    conn_str += "&TrustServerCertificate=yes"

            return conn_str
        
        elif self.db_type == DatabaseType.MYSQL:
            conn_str = f"mysql+pymysql://"
            
            if params['username'] and params['password']:
                conn_str += f"{urllib.parse.quote_plus(params['username'])}:{urllib.parse.quote_plus(params['password'])}@"
            
            conn_str += f"{params['server']}"
            
            if params['database']:
                conn_str += f"/{params['database']}"
            
            query_params = []
            if params['connection_timeout']:
                query_params.append(f"connect_timeout={params['connection_timeout']}")
            
            if params['ssl']:
                query_params.append("ssl=true")
                if params['ssl_ca']:
                    query_params.append(f"ssl_ca={params['ssl_ca']}")
                if params['ssl_cert']:
                    query_params.append(f"ssl_cert={params['ssl_cert']}")
                if params['ssl_key']:
                    query_params.append(f"ssl_key={params['ssl_key']}")
            
            if query_params:
                conn_str += f"?{'&'.join(query_params)}"
            
            return conn_str
        
        elif self.db_type == DatabaseType.POSTGRES:
            conn_str = f"postgresql://"
            
            if params['username'] and params['password']:
                conn_str += f"{urllib.parse.quote_plus(params['username'])}:{urllib.parse.quote_plus(params['password'])}@"
            
            conn_str += f"{params['server']}"
            
            if params['database']:
                conn_str += f"/{params['database']}"
            
            query_params = []
            if params['connection_timeout']:
                query_params.append(f"connect_timeout={params['connection_timeout']}")
            
            if params['ssl']:
                query_params.append("sslmode=require")
                if params['ssl_ca']:
                    query_params.append(f"sslrootcert={params['ssl_ca']}")
                if params['ssl_cert']:
                    query_params.append(f"sslcert={params['ssl_cert']}")
                if params['ssl_key']:
                    query_params.append(f"sslkey={params['ssl_key']}")
            
            if query_params:
                conn_str += f"?{'&'.join(query_params)}"
            
            return conn_str
        
        elif self.db_type == DatabaseType.SQLITE:
            database_path = params['database'] or ':memory:'
            return f"sqlite:///{database_path}"
        
        elif self.db_type == DatabaseType.ORACLE:
            conn_str = f"oracle+cx_oracle://"
            
            if params['username'] and params['password']:
                conn_str += f"{urllib.parse.quote_plus(params['username'])}:{urllib.parse.quote_plus(params['password'])}@"
            
            # Oracle connection string: host:port/service_name
            conn_str += f"{params['server']}"
            
            if params['database']:
                conn_str += f"/{params['database']}"
            
            return conn_str
        
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def _create_engine(self) -> sqlalchemy.engine.Engine:
        """
        Create a SQLAlchemy engine with connection pooling.
        
        Returns:
            SQLAlchemy engine
        
        Raises:
            ConnectionError: If engine creation fails
        """
        params = self._get_connection_parameters()
        
        try:
            connection_string = self._build_connection_string()
            
            # Create engine with connection pool settings
            engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=params['pool_size'],
                max_overflow=params['max_overflow'],
                pool_timeout=params['pool_timeout'],
                pool_recycle=params['pool_recycle'],
                pool_pre_ping=True,  # Check connection validity before using from pool
                connect_args={
                    'timeout': params['connection_timeout'],
                    'command_timeout': params['command_timeout']
                } if self.db_type == DatabaseType.MSSQL else {}
            )
            
            logger.info(f"Created database engine for {self.db_type} with connection pooling")
            return engine
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to create database engine: {e}")
            raise ConnectionError(f"Database engine creation failed: {e}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections from the pool.

        For MSSQL databases, sets READ UNCOMMITTED isolation level to prevent
        locking issues with live ERP data. This allows "dirty reads" which is
        acceptable for reporting/dashboard queries where absolute consistency
        is not required.

        Yields:
            An active database connection from the pool

        Raises:
            ConnectionError: If connection acquisition fails
        """
        conn = None
        try:
            conn = self.engine.connect()
            # Set READ UNCOMMITTED isolation level for MSSQL to prevent locking
            # This is critical for dashboard queries against live Opera 3 data
            if self.db_type == DatabaseType.MSSQL:
                conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"))
                logger.debug("Set MSSQL connection to READ UNCOMMITTED isolation level")
            yield conn
        except SQLAlchemyError as e:
            logger.error(f"Database connection error: {e}")
            raise ConnectionError(f"Database connection failed: {e}")
        finally:
            if conn:
                conn.close()
    
    def _execute_with_retry(self, operation, *args, **kwargs):
        """
        Execute an operation with retry logic for transient errors.
        
        Args:
            operation: Callable to execute
            *args: Positional arguments for the operation
            **kwargs: Keyword arguments for the operation
            
        Returns:
            Result of the operation
            
        Raises:
            DatabaseError: If all retry attempts fail
        """
        last_error = None
        
        for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
            try:
                return operation(*args, **kwargs)
            except (SQLAlchemyError, pyodbc.Error) as e:
                last_error = e
                
                # Check if error is transient and retryable
                error_str = str(e).lower()
                is_retryable = (
                    'timeout' in error_str or
                    'deadlock' in error_str or
                    'connection reset' in error_str or
                    'connection has been closed' in error_str or
                    'server has gone away' in error_str
                )
                
                if not is_retryable or attempt == MAX_RETRY_ATTEMPTS:
                    break
                
                # Exponential backoff
                sleep_time = RETRY_DELAY * (2 ** (attempt - 1))
                logger.warning(f"Transient error occurred (attempt {attempt}/{MAX_RETRY_ATTEMPTS}), "
                               f"retrying in {sleep_time:.2f}s: {e}")
                time.sleep(sleep_time)
        
        # If we got here, all retry attempts failed
        logger.error(f"All retry attempts failed: {last_error}")
        if isinstance(last_error, SQLAlchemyError):
            raise QueryError(f"Query execution failed after {MAX_RETRY_ATTEMPTS} attempts: {last_error}")
        else:
            raise last_error
    
    def execute_query(self, query: str, params: Optional[Union[List, Tuple, Dict]] = None) -> pd.DataFrame:
        """
        Execute a SELECT query and return results as a pandas DataFrame.
        
        Args:
            query: SQL query string
            params: Parameters to substitute into the query
            
        Returns:
            pandas DataFrame containing the query results
            
        Raises:
            QueryError: If the query execution fails
            ValueError: If the query is empty or None
        """
        if not query:
            raise ValueError("Query cannot be empty")
            
        logger.info(f"Executing query: {query}")
        
        def _execute():
            with self.get_connection() as conn:
                if params:
                    df = pd.read_sql(text(query), conn, params=params)
                else:
                    df = pd.read_sql(text(query), conn)
                
                logger.info(f"Query returned {len(df)} rows")
                return df
        
        try:
            return self._execute_with_retry(_execute)
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            if isinstance(e, QueryError):
                raise
            else:
                raise QueryError(f"Query execution failed: {e}")
    
    def execute_query_iter(self, query: str, params: Optional[Union[List, Tuple, Dict]] = None,
                          batch_size: int = 1000) -> Generator[pd.DataFrame, None, None]:
        """
        Execute a SELECT query and yield results in batches.
        Useful for processing large result sets that don't fit in memory.
        
        Args:
            query: SQL query string
            params: Parameters to substitute into the query
            batch_size: Number of rows to fetch per batch
            
        Yields:
            pandas DataFrame containing a batch of query results
            
        Raises:
            QueryError: If the query execution fails
            ValueError: If the query is empty or None
        """
        if not query:
            raise ValueError("Query cannot be empty")
            
        logger.info(f"Executing query with iterator (batch size: {batch_size}): {query}")
        
        try:
            with self.get_connection() as conn:
                if params:
                    result = pd.read_sql(text(query), conn, params=params, chunksize=batch_size)
                else:
                    result = pd.read_sql(text(query), conn, chunksize=batch_size)
                
                total_rows = 0
                for chunk in result:
                    total_rows += len(chunk)
                    yield chunk
                
                logger.info(f"Query iterator returned {total_rows} rows in total")
        except SQLAlchemyError as e:
            logger.error(f"Error executing query iterator: {e}")
            raise QueryError(f"Query iterator execution failed: {e}")
    
    def execute_non_query(self, query: str, params: Optional[Union[List, Tuple, Dict]] = None) -> int:
        """
        Execute a non-SELECT query (INSERT, UPDATE, DELETE) and return affected rows.
        
        Args:
            query: SQL query string for modification operations
            params: Parameters to substitute into the query
            
        Returns:
            Number of rows affected by the query
            
        Raises:
            QueryError: If the query execution fails
            ValueError: If the query is empty or None
        """
        if not query:
            raise ValueError("Query cannot be empty")
            
        logger.info(f"Executing non-query: {query}")
        
        def _execute():
            with self.get_connection() as conn:
                # Execute the UPDATE/INSERT/DELETE and commit
                # Note: get_connection() already starts an implicit transaction
                result = conn.execute(text(query), params if params else {})
                affected_rows = result.rowcount
                conn.commit()  # Commit the changes
                logger.info(f"Query affected {affected_rows} rows")
                return affected_rows
        
        try:
            return self._execute_with_retry(_execute)
        except Exception as e:
            logger.error(f"Error executing non-query: {e}")
            if isinstance(e, QueryError):
                raise
            else:
                raise QueryError(f"Non-query execution failed: {e}")
    
    def execute_many(self, query: str, params_list: List[Union[List, Tuple, Dict]]) -> int:
        """
        Execute a query multiple times with different parameter sets.
        
        Args:
            query: SQL query string
            params_list: List of parameter sets to substitute into the query
            
        Returns:
            Total number of rows affected
            
        Raises:
            QueryError: If the query execution fails
            ValueError: If the query is empty or None or params_list is empty
        """
        if not query:
            raise ValueError("Query cannot be empty")
            
        if not params_list:
            raise ValueError("Parameters list cannot be empty")
            
        logger.info(f"Executing multiple queries (batch size: {len(params_list)}): {query}")
        
        def _execute():
            with self.get_connection() as conn:
                with conn.begin():  # Start a transaction
                    result = conn.execute(text(query), params_list)
                    affected_rows = result.rowcount
                    logger.info(f"Batch query affected {affected_rows} rows")
                    return affected_rows
        
        try:
            return self._execute_with_retry(_execute)
        except Exception as e:
            logger.error(f"Error executing batch query: {e}")
            if isinstance(e, QueryError):
                raise
            else:
                raise QueryError(f"Batch query execution failed: {e}")
    
    @contextmanager
    def transaction(self):
        """
        Context manager for explicit transaction control.
        
        Yields:
            A connection with an active transaction
            
        Raises:
            ConnectionError: If connection acquisition fails
        """
        with self.get_connection() as conn:
            with conn.begin() as trans:
                try:
                    yield conn
                except:
                    trans.rollback()
                    raise
    
    def execute_script(self, sql_script: str) -> bool:
        """
        Execute a SQL script containing multiple statements.
        
        Args:
            sql_script: String containing multiple SQL statements separated by semicolons
            
        Returns:
            True if execution was successful
            
        Raises:
            QueryError: If the script execution fails
            ValueError: If the script is empty or None
        """
        if not sql_script:
            raise ValueError("SQL script cannot be empty")
            
        logger.info("Executing SQL script")
        
        # Split the script into individual statements
        # This basic split won't handle all cases (like nested semicolons in functions)
        # For more complex scripts, consider using a proper SQL parser
        statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
        
        try:
            with self.transaction() as conn:
                for statement in statements:
                    conn.execute(text(statement))
                
                logger.info(f"SQL script with {len(statements)} statements executed successfully")
                return True
        except Exception as e:
            logger.error(f"Error executing SQL script: {e}")
            raise QueryError(f"SQL script execution failed: {e}")
    
    def test_connection(self) -> bool:
        """
        Test the database connection by executing a simple query.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                # Use a database-specific test query
                if self.db_type == DatabaseType.MSSQL:
                    result = conn.execute(text("SELECT 1")).scalar()
                elif self.db_type == DatabaseType.MYSQL:
                    result = conn.execute(text("SELECT 1")).scalar()
                elif self.db_type == DatabaseType.POSTGRES:
                    result = conn.execute(text("SELECT 1")).scalar()
                elif self.db_type == DatabaseType.SQLITE:
                    result = conn.execute(text("SELECT 1")).scalar()
                elif self.db_type == DatabaseType.ORACLE:
                    result = conn.execute(text("SELECT 1 FROM DUAL")).scalar()
                else:
                    result = 0
                
                return result == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def get_tables(self) -> pd.DataFrame:
        """
        Get a list of all tables in the database.
        
        Returns:
            DataFrame containing table information
            
        Raises:
            QueryError: If the query execution fails
        """
        try:
            # Use database-specific query based on the type
            if self.db_type == DatabaseType.MSSQL:
                query = """
                SELECT 
                    TABLE_SCHEMA as schema_name,
                    TABLE_NAME as table_name,
                    TABLE_TYPE as table_type
                FROM 
                    INFORMATION_SCHEMA.TABLES
                ORDER BY 
                    TABLE_SCHEMA, TABLE_NAME
                """
            elif self.db_type == DatabaseType.MYSQL:
                query = """
                SELECT 
                    TABLE_SCHEMA as schema_name,
                    TABLE_NAME as table_name,
                    TABLE_TYPE as table_type
                FROM 
                    information_schema.TABLES
                WHERE
                    TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
                ORDER BY 
                    TABLE_SCHEMA, TABLE_NAME
                """
            elif self.db_type == DatabaseType.POSTGRES:
                query = """
                SELECT 
                    table_schema as schema_name,
                    table_name as table_name,
                    CASE 
                        WHEN table_type = 'BASE TABLE' THEN 'TABLE'
                        ELSE table_type
                    END as table_type
                FROM 
                    information_schema.tables
                WHERE
                    table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY 
                    table_schema, table_name
                """
            elif self.db_type == DatabaseType.SQLITE:
                query = """
                SELECT 
                    '' as schema_name,
                    name as table_name,
                    'TABLE' as table_type
                FROM 
                    sqlite_master
                WHERE 
                    type='table' AND
                    name NOT LIKE 'sqlite_%'
                ORDER BY 
                    name
                """
            elif self.db_type == DatabaseType.ORACLE:
                query = """
                SELECT 
                    OWNER as schema_name,
                    TABLE_NAME as table_name,
                    'TABLE' as table_type
                FROM 
                    ALL_TABLES
                WHERE 
                    OWNER NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'WMSYS')
                ORDER BY 
                    OWNER, TABLE_NAME
                """
            else:
                raise QueryError(f"Unsupported database type for get_tables: {self.db_type}")
            
            return self.execute_query(query)
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            if isinstance(e, QueryError):
                raise
            else:
                raise QueryError(f"Failed to get tables: {e}")
    
    def get_columns(self, table_name: str, schema: str = 'dbo') -> pd.DataFrame:
        """
        Get column information for a specific table.
        
        Args:
            table_name: Name of the table
            schema: Database schema (default is database-dependent)
            
        Returns:
            DataFrame containing column information
            
        Raises:
            QueryError: If the query execution fails
            ValueError: If table_name is empty or None
        """
        if not table_name:
            raise ValueError("Table name cannot be empty")
        
        try:
            # Use database-specific query based on the type
            if self.db_type == DatabaseType.MSSQL:
                query = """
                SELECT 
                    COLUMN_NAME as column_name,
                    DATA_TYPE as data_type,
                    CHARACTER_MAXIMUM_LENGTH as max_length,
                    IS_NULLABLE as is_nullable,
                    ORDINAL_POSITION as ordinal_position
                FROM 
                    INFORMATION_SCHEMA.COLUMNS
                WHERE 
                    TABLE_NAME = :table_name
                    AND TABLE_SCHEMA = :schema
                ORDER BY 
                    ORDINAL_POSITION
                """
                params = {'table_name': table_name, 'schema': schema}
            elif self.db_type == DatabaseType.MYSQL:
                query = """
                SELECT 
                    COLUMN_NAME as column_name,
                    DATA_TYPE as data_type,
                    CHARACTER_MAXIMUM_LENGTH as max_length,
                    IS_NULLABLE as is_nullable,
                    ORDINAL_POSITION as ordinal_position
                FROM 
                    information_schema.COLUMNS
                WHERE 
                    TABLE_NAME = :table_name
                    AND TABLE_SCHEMA = :schema
                ORDER BY 
                    ORDINAL_POSITION
                """
                params = {'table_name': table_name, 'schema': schema}
            elif self.db_type == DatabaseType.POSTGRES:
                query = """
                SELECT 
                    column_name as column_name,
                    data_type as data_type,
                    character_maximum_length as max_length,
                    is_nullable as is_nullable,
                    ordinal_position as ordinal_position
                FROM 
                    information_schema.columns
                WHERE 
                    table_name = :table_name
                    AND table_schema = :schema
                ORDER BY 
                    ordinal_position
                """
                params = {'table_name': table_name, 'schema': schema}
            elif self.db_type == DatabaseType.SQLITE:
                # For SQLite, we need to format the query directly since SQLite PRAGMA doesn't
                # support parameterized queries correctly
                query = f"""
                PRAGMA table_info({table_name})
                """
                # SQLite doesn't support schemas, so we ignore the schema parameter
                params = {}
                
                # For SQLite, we need to transform the results to match our standard format
                result = self.execute_query(query, params)
                if not result.empty:
                    return pd.DataFrame({
                        'column_name': result['name'],
                        'data_type': result['type'],
                        'max_length': None,
                        'is_nullable': result['notnull'].apply(lambda x: 'NO' if x == 1 else 'YES'),
                        'ordinal_position': result['cid']
                    })
                return pd.DataFrame(columns=['column_name', 'data_type', 'max_length', 'is_nullable', 'ordinal_position'])
            elif self.db_type == DatabaseType.ORACLE:
                query = """
                SELECT 
                    COLUMN_NAME as column_name,
                    DATA_TYPE as data_type,
                    DATA_LENGTH as max_length,
                    NULLABLE as is_nullable,
                    COLUMN_ID as ordinal_position
                FROM 
                    ALL_TAB_COLUMNS
                WHERE 
                    TABLE_NAME = :table_name
                    AND OWNER = :schema
                ORDER BY 
                    COLUMN_ID
                """
                params = {'table_name': table_name, 'schema': schema}
            else:
                raise QueryError(f"Unsupported database type for get_columns: {self.db_type}")
            
            if self.db_type != DatabaseType.SQLITE:
                return self.execute_query(query, params)
        except Exception as e:
            logger.error(f"Error getting columns for table {table_name}: {e}")
            if isinstance(e, QueryError):
                raise
            else:
                raise QueryError(f"Failed to get columns for table {table_name}: {e}")
    
    def get_primary_keys(self, table_name: str, schema: str = 'dbo') -> pd.DataFrame:
        """
        Get primary key information for a specific table.
        
        Args:
            table_name: Name of the table
            schema: Database schema (default is database-dependent)
            
        Returns:
            DataFrame containing primary key information
            
        Raises:
            QueryError: If the query execution fails
            ValueError: If table_name is empty or None
        """
        if not table_name:
            raise ValueError("Table name cannot be empty")
        
        try:
            # Use database-specific query based on the type
            if self.db_type == DatabaseType.MSSQL:
                query = """
                SELECT 
                    tc.CONSTRAINT_NAME as constraint_name,
                    kcu.COLUMN_NAME as column_name,
                    kcu.ORDINAL_POSITION as position
                FROM 
                    INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN 
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                ON 
                    tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                WHERE 
                    tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.TABLE_NAME = :table_name
                    AND tc.TABLE_SCHEMA = :schema
                ORDER BY 
                    kcu.ORDINAL_POSITION
                """
                params = {'table_name': table_name, 'schema': schema}
            elif self.db_type == DatabaseType.MYSQL:
                query = """
                SELECT 
                    tc.CONSTRAINT_NAME as constraint_name,
                    kcu.COLUMN_NAME as column_name,
                    kcu.ORDINAL_POSITION as position
                FROM 
                    information_schema.TABLE_CONSTRAINTS tc
                JOIN 
                    information_schema.KEY_COLUMN_USAGE kcu 
                ON 
                    tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                WHERE 
                    tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.TABLE_NAME = :table_name
                    AND tc.TABLE_SCHEMA = :schema
                ORDER BY 
                    kcu.ORDINAL_POSITION
                """
                params = {'table_name': table_name, 'schema': schema}
            elif self.db_type == DatabaseType.POSTGRES:
                query = """
                SELECT 
                    tc.constraint_name as constraint_name,
                    kcu.column_name as column_name,
                    kcu.ordinal_position as position
                FROM 
                    information_schema.table_constraints tc
                JOIN 
                    information_schema.key_column_usage kcu 
                ON 
                    tc.constraint_name = kcu.constraint_name
                WHERE 
                    tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_name = :table_name
                    AND tc.table_schema = :schema
                ORDER BY 
                    kcu.ordinal_position
                """
                params = {'table_name': table_name, 'schema': schema}
            elif self.db_type == DatabaseType.SQLITE:
                # SQLite PRAGMA doesn't support parameter binding
                query = f"""
                PRAGMA table_info('{table_name}')
                """
                # SQLite doesn't support schemas, so we ignore the schema parameter
                params = {}
                
                # For SQLite, we need to transform the results to match our standard format
                result = self.execute_query(query, params)
                if not result.empty:
                    # Filter rows where pk > 0 (primary key columns)
                    pk_result = result[result['pk'] > 0]
                    if not pk_result.empty:
                        return pd.DataFrame({
                            'constraint_name': f'PK_{table_name}',
                            'column_name': pk_result['name'],
                            'position': pk_result['pk']
                        })
                return pd.DataFrame(columns=['constraint_name', 'column_name', 'position'])
            elif self.db_type == DatabaseType.ORACLE:
                query = """
                SELECT 
                    cons.CONSTRAINT_NAME as constraint_name,
                    cols.COLUMN_NAME as column_name,
                    cols.POSITION as position
                FROM 
                    ALL_CONSTRAINTS cons
                JOIN 
                    ALL_CONS_COLUMNS cols 
                ON 
                    cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
                WHERE 
                    cons.CONSTRAINT_TYPE = 'P'
                    AND cons.TABLE_NAME = :table_name
                    AND cons.OWNER = :schema
                ORDER BY 
                    cols.POSITION
                """
                params = {'table_name': table_name, 'schema': schema}
            else:
                raise QueryError(f"Unsupported database type for get_primary_keys: {self.db_type}")
            
            if self.db_type != DatabaseType.SQLITE:
                return self.execute_query(query, params)
        except Exception as e:
            logger.error(f"Error getting primary keys for table {table_name}: {e}")
            if isinstance(e, QueryError):
                raise
            else:
                raise QueryError(f"Failed to get primary keys for table {table_name}: {e}")
    
    def get_foreign_keys(self, table_name: str, schema: str = 'dbo') -> pd.DataFrame:
        """
        Get foreign key information for a specific table.
        
        Args:
            table_name: Name of the table
            schema: Database schema (default is database-dependent)
            
        Returns:
            DataFrame containing foreign key information
            
        Raises:
            QueryError: If the query execution fails
            ValueError: If table_name is empty or None
        """
        if not table_name:
            raise ValueError("Table name cannot be empty")
        
        try:
            # Use database-specific query based on the type
            if self.db_type == DatabaseType.MSSQL:
                query = """
                SELECT 
                    fk.name as constraint_name,
                    OBJECT_NAME(fk.parent_object_id) as table_name,
                    c1.name as column_name,
                    OBJECT_NAME(fk.referenced_object_id) as referenced_table_name,
                    c2.name as referenced_column_name
                FROM 
                    sys.foreign_keys fk
                INNER JOIN 
                    sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                INNER JOIN 
                    sys.columns c1 ON fkc.parent_column_id = c1.column_id AND fkc.parent_object_id = c1.object_id
                INNER JOIN 
                    sys.columns c2 ON fkc.referenced_column_id = c2.column_id AND fkc.referenced_object_id = c2.object_id
                WHERE 
                    OBJECT_NAME(fk.parent_object_id) = :table_name
                    AND OBJECT_SCHEMA_NAME(fk.parent_object_id) = :schema
                """
                params = {'table_name': table_name, 'schema': schema}
            elif self.db_type == DatabaseType.MYSQL:
                query = """
                SELECT 
                    rc.CONSTRAINT_NAME as constraint_name,
                    kcu.TABLE_NAME as table_name,
                    kcu.COLUMN_NAME as column_name,
                    kcu.REFERENCED_TABLE_NAME as referenced_table_name,
                    kcu.REFERENCED_COLUMN_NAME as referenced_column_name
                FROM 
                    information_schema.REFERENTIAL_CONSTRAINTS rc
                JOIN 
                    information_schema.KEY_COLUMN_USAGE kcu 
                ON 
                    rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                WHERE 
                    kcu.TABLE_NAME = :table_name
                    AND kcu.TABLE_SCHEMA = :schema
                """
                params = {'table_name': table_name, 'schema': schema}
            elif self.db_type == DatabaseType.POSTGRES:
                query = """
                SELECT
                    tc.constraint_name as constraint_name,
                    tc.table_name as table_name,
                    kcu.column_name as column_name,
                    ccu.table_name AS referenced_table_name,
                    ccu.column_name AS referenced_column_name
                FROM 
                    information_schema.table_constraints AS tc 
                JOIN 
                    information_schema.key_column_usage AS kcu
                ON 
                    tc.constraint_name = kcu.constraint_name
                JOIN 
                    information_schema.constraint_column_usage AS ccu 
                ON 
                    ccu.constraint_name = tc.constraint_name
                WHERE 
                    tc.constraint_type = 'FOREIGN KEY' 
                    AND tc.table_name = :table_name
                    AND tc.table_schema = :schema
                """
                params = {'table_name': table_name, 'schema': schema}
            elif self.db_type == DatabaseType.SQLITE:
                # SQLite PRAGMA doesn't support parameter binding
                query = f"""
                PRAGMA foreign_key_list('{table_name}')
                """
                # SQLite doesn't support schemas, so we ignore the schema parameter
                params = {}
                
                # For SQLite, we need to transform the results to match our standard format
                result = self.execute_query(query, params)
                if not result.empty:
                    return pd.DataFrame({
                        'constraint_name': f'FK_{table_name}_{result["id"]}',
                        'table_name': table_name,
                        'column_name': result['from'],
                        'referenced_table_name': result['table'],
                        'referenced_column_name': result['to']
                    })
                return pd.DataFrame(columns=['constraint_name', 'table_name', 'column_name', 
                                           'referenced_table_name', 'referenced_column_name'])
            elif self.db_type == DatabaseType.ORACLE:
                query = """
                SELECT 
                    a.constraint_name as constraint_name,
                    a.table_name as table_name,
                    a.column_name as column_name,
                    c_pk.table_name as referenced_table_name,
                    c_pk.column_name as referenced_column_name
                FROM 
                    all_cons_columns a
                JOIN 
                    all_constraints c ON a.owner = c.owner AND a.constraint_name = c.constraint_name
                JOIN 
                    all_constraints c_pk ON c.r_owner = c_pk.owner AND c.r_constraint_name = c_pk.constraint_name
                JOIN 
                    all_cons_columns c_pk_col ON c_pk.owner = c_pk_col.owner 
                    AND c_pk.constraint_name = c_pk_col.constraint_name 
                    AND c_pk_col.position = a.position
                WHERE 
                    c.constraint_type = 'R'
                    AND a.table_name = :table_name
                    AND a.owner = :schema
                """
                params = {'table_name': table_name, 'schema': schema}
            else:
                raise QueryError(f"Unsupported database type for get_foreign_keys: {self.db_type}")
            
            if self.db_type != DatabaseType.SQLITE:
                return self.execute_query(query, params)
        except Exception as e:
            logger.error(f"Error getting foreign keys for table {table_name}: {e}")
            if isinstance(e, QueryError):
                raise
            else:
                raise QueryError(f"Failed to get foreign keys for table {table_name}: {e}")
    
    def get_indexes(self, table_name: str, schema: str = 'dbo') -> pd.DataFrame:
        """
        Get index information for a specific table.
        
        Args:
            table_name: Name of the table
            schema: Database schema (default is database-dependent)
            
        Returns:
            DataFrame containing index information
            
        Raises:
            QueryError: If the query execution fails
            ValueError: If table_name is empty or None
        """
        if not table_name:
            raise ValueError("Table name cannot be empty")
        
        try:
            # Use database-specific query based on the type
            if self.db_type == DatabaseType.MSSQL:
                query = """
                SELECT 
                    i.name as index_name,
                    c.name as column_name,
                    i.is_unique as is_unique,
                    i.is_primary_key as is_primary_key,
                    ic.key_ordinal as ordinal_position
                FROM 
                    sys.indexes i
                INNER JOIN 
                    sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN 
                    sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE 
                    OBJECT_NAME(i.object_id) = :table_name
                    AND OBJECT_SCHEMA_NAME(i.object_id) = :schema
                ORDER BY 
                    i.name, ic.key_ordinal
                """
                params = {'table_name': table_name, 'schema': schema}
            elif self.db_type == DatabaseType.MYSQL:
                query = """
                SELECT 
                    INDEX_NAME as index_name,
                    COLUMN_NAME as column_name,
                    NOT NON_UNIQUE as is_unique,
                    INDEX_NAME = 'PRIMARY' as is_primary_key,
                    SEQ_IN_INDEX as ordinal_position
                FROM 
                    information_schema.STATISTICS
                WHERE 
                    TABLE_NAME = :table_name
                    AND TABLE_SCHEMA = :schema
                ORDER BY 
                    INDEX_NAME, SEQ_IN_INDEX
                """
                params = {'table_name': table_name, 'schema': schema}
            elif self.db_type == DatabaseType.POSTGRES:
                query = """
                SELECT 
                    i.relname as index_name,
                    a.attname as column_name,
                    ix.indisunique as is_unique,
                    ix.indisprimary as is_primary_key,
                    array_position(ix.indkey, a.attnum) as ordinal_position
                FROM 
                    pg_class t,
                    pg_class i,
                    pg_index ix,
                    pg_attribute a,
                    pg_namespace n
                WHERE 
                    t.oid = ix.indrelid
                    AND i.oid = ix.indexrelid
                    AND a.attrelid = t.oid
                    AND a.attnum = ANY(ix.indkey)
                    AND t.relnamespace = n.oid
                    AND t.relname = :table_name
                    AND n.nspname = :schema
                ORDER BY 
                    i.relname, array_position(ix.indkey, a.attnum)
                """
                params = {'table_name': table_name, 'schema': schema}
            elif self.db_type == DatabaseType.SQLITE:
                # SQLite PRAGMA doesn't support parameter binding
                query = f"""
                PRAGMA index_list('{table_name}')
                """
                # SQLite doesn't support schemas, so we ignore the schema parameter
                params = {}
                
                # For SQLite, we need to get index details for each index
                indexes = self.execute_query(query, params)
                if indexes.empty:
                    return pd.DataFrame(columns=['index_name', 'column_name', 'is_unique', 'is_primary_key', 'ordinal_position'])
                
                result_data = []
                for _, idx in indexes.iterrows():
                    index_name = idx['name']
                    is_unique = idx['unique'] == 1
                    
                    # Get columns in this index
                    index_info = self.execute_query(f"PRAGMA index_info('{index_name}')")
                    for i, col in index_info.iterrows():
                        column_name = col['name']
                        # Check if this is a primary key index
                        is_primary = False
                        if index_name.startswith('sqlite_autoindex_'):
                            # These might be automatically created for PRIMARY KEYs
                            pk_info = self.execute_query(f"PRAGMA table_info('{table_name}')")
                            pk_columns = pk_info[pk_info['pk'] > 0]['name'].tolist()
                            is_primary = column_name in pk_columns
                        
                        result_data.append({
                            'index_name': index_name,
                            'column_name': column_name,
                            'is_unique': is_unique,
                            'is_primary_key': is_primary,
                            'ordinal_position': i + 1
                        })
                
                if result_data:
                    return pd.DataFrame(result_data)
                return pd.DataFrame(columns=['index_name', 'column_name', 'is_unique', 'is_primary_key', 'ordinal_position'])
            elif self.db_type == DatabaseType.ORACLE:
                query = """
                SELECT 
                    i.index_name as index_name,
                    c.column_name as column_name,
                    i.uniqueness = 'UNIQUE' as is_unique,
                    CASE WHEN c.constraint_type = 'P' THEN 1 ELSE 0 END as is_primary_key,
                    ic.column_position as ordinal_position
                FROM 
                    all_indexes i
                JOIN 
                    all_ind_columns ic ON i.index_name = ic.index_name AND i.owner = ic.index_owner
                LEFT JOIN (
                    SELECT 
                        cons.constraint_name, 
                        cons.constraint_type, 
                        cols.column_name
                    FROM 
                        all_constraints cons
                    JOIN 
                        all_cons_columns cols ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
                    WHERE 
                        cons.constraint_type = 'P'
                ) c ON i.index_name = c.constraint_name
                WHERE 
                    i.table_name = :table_name
                    AND i.owner = :schema
                ORDER BY 
                    i.index_name, ic.column_position
                """
                params = {'table_name': table_name, 'schema': schema}
            else:
                raise QueryError(f"Unsupported database type for get_indexes: {self.db_type}")
            
            if self.db_type != DatabaseType.SQLITE:
                return self.execute_query(query, params)
        except Exception as e:
            logger.error(f"Error getting indexes for table {table_name}: {e}")
            if isinstance(e, QueryError):
                raise
            else:
                raise QueryError(f"Failed to get indexes for table {table_name}: {e}")
    
    def get_schema_info(self) -> Dict[str, Any]:
        """
        Get comprehensive schema information for all tables in the database.
        
        Returns:
            Dictionary with schema information for all tables
            
        Raises:
            QueryError: If schema queries fail
        """
        try:
            tables = self.get_tables()
            if tables.empty:
                return {}
            
            schema_info = {}
            
            for _, row in tables.iterrows():
                schema_name = row['schema_name']
                table_name = row['table_name']
                table_type = row['table_type']
                
                # Skip system tables and views if requested
                if table_type not in ('TABLE', 'BASE TABLE'):
                    continue
                
                # Get table information
                try:
                    columns = self.get_columns(table_name, schema_name)
                    primary_keys = self.get_primary_keys(table_name, schema_name)
                    foreign_keys = self.get_foreign_keys(table_name, schema_name)
                    indexes = self.get_indexes(table_name, schema_name)
                    
                    full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
                    
                    # Convert DataFrames to dictionaries for easier JSON serialization
                    schema_info[full_table_name] = {
                        'schema': schema_name,
                        'name': table_name,
                        'type': table_type,
                        'columns': columns.to_dict('records') if not columns.empty else [],
                        'primary_keys': primary_keys.to_dict('records') if not primary_keys.empty else [],
                        'foreign_keys': foreign_keys.to_dict('records') if not foreign_keys.empty else [],
                        'indexes': indexes.to_dict('records') if not indexes.empty else []
                    }
                except Exception as e:
                    logger.warning(f"Error getting detailed information for table {schema_name}.{table_name}: {e}")
                    # Continue with other tables even if one fails
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Error getting schema information: {e}")
            if isinstance(e, QueryError):
                raise
            else:
                raise QueryError(f"Failed to get schema information: {e}")
    
    def get_pool_status(self) -> Dict[str, Any]:
        """
        Get status information about the connection pool.
        
        Returns:
            Dictionary with pool status information
        """
        try:
            # SQLAlchemy doesn't provide direct access to pool statistics
            # This is an approximation using internal inspection
            pool = self.engine.pool
            return {
                'pool_size': pool.size(),
                'checked_out_connections': pool.checkedout(),
                'overflow': pool._overflow,
                'checkedin': pool.checkedin()
            }
        except Exception as e:
            logger.error(f"Error getting pool status: {e}")
            return {
                'error': str(e)
            }
    
    def close_all_connections(self):
        """
        Close all connections in the pool.
        """
        try:
            self.engine.dispose()
            logger.info("All database connections closed")
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")