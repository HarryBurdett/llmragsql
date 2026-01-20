#!/bin/bash

echo
echo "==================================="
echo " SQL-RAG Local Installation Script"
echo "==================================="
echo

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Python not found! Please install Python 3.9 or newer."
    echo "Visit https://www.python.org/downloads/ for installation instructions."
    echo
    echo "After installing Python, run this script again."
    echo
    exit 1
fi

# Create virtual environment
echo "Creating Python virtual environment..."
python3 -m venv venv
if [ $? -ne 0 ]; then
    echo "Failed to create virtual environment."
    echo "Please make sure you have the venv module available."
    exit 1
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate
if [ $? -ne 0 ]; then
    echo "Failed to activate virtual environment."
    exit 1
fi

# Install dependencies
echo "Installing dependencies (this may take a few minutes)..."
pip install -r requirements.txt
if [ $? -ne 0 ]; then
    echo "Failed to install dependencies."
    exit 1
fi

# Create output directories
echo "Creating directories..."
mkdir -p output

# Download the embedding model
echo "Downloading embedding model..."
python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"
if [ $? -ne 0 ]; then
    echo "Failed to download embedding model."
    echo "Will try again when the application runs."
fi

# Check for GPU availability
echo "Checking for GPU..."
python -c "import torch; print(f'GPU available: {torch.cuda.is_available()}')"

# Check if Ollama is running
echo "Checking for Ollama..."
OLLAMA_RUNNING=false
python -c "
import requests
try:
    response = requests.get('http://localhost:11434/api/tags', timeout=5)
    print('Ollama is running' if response.status_code == 200 else 'Ollama is not running')
    exit(0 if response.status_code == 200 else 1)
except:
    print('Ollama is not running')
    exit(1)
"
if [ $? -eq 0 ]; then
    OLLAMA_RUNNING=true
else
    echo "WARNING: Ollama is not running. You'll need to start it before using this application."
    echo "Run 'ollama serve' in a separate terminal window."
fi

# Check if Qdrant is running
echo "Checking for Qdrant..."
QDRANT_RUNNING=false
python -c "
import requests
try:
    # Try the health endpoint first (works for both API and Docker)
    response = requests.get('http://localhost:6333/healthz', timeout=5)
    if response.status_code == 200:
        print('Qdrant is running')
        exit(0)
    
    # Try dashboard as fallback
    response = requests.get('http://localhost:6333/dashboard/', timeout=5)
    print('Qdrant is running' if response.status_code == 200 else 'Qdrant is not running')
    exit(0 if response.status_code == 200 else 1)
except:
    print('Qdrant is not running')
    exit(1)
"
if [ $? -eq 0 ]; then
    QDRANT_RUNNING=true
else
    echo "WARNING: Qdrant is not running. You'll need to start it before using this application."
    echo "To run Qdrant in Docker, use:"
    echo "  docker run -p 6333:6333 -p 6334:6334 -v $(pwd)/qdrant_data:/qdrant/storage qdrant/qdrant"
fi

# Configure SQL connection
echo
echo "==================================="
echo "SQL Database Configuration"
echo "==================================="
read -p "SQL Server (default: localhost): " SQL_SERVER
SQL_SERVER=${SQL_SERVER:-localhost}

read -p "Database Name: " SQL_DATABASE

read -p "Use Windows Authentication? (y/n, default: n): " USE_WINDOWS_AUTH
USE_WINDOWS_AUTH=${USE_WINDOWS_AUTH:-n}

if [[ $USE_WINDOWS_AUTH =~ ^[Nn]$ ]]; then
    read -p "Username: " SQL_USERNAME
    read -s -p "Password: " SQL_PASSWORD
    echo
fi

# Update config.ini with SQL details
echo "Updating configuration file..."
CONFIG_FILE="config.ini"

# Backup existing config
if [ -f "$CONFIG_FILE" ]; then
    cp "$CONFIG_FILE" "${CONFIG_FILE}.bak"
fi

# Create or update database section in config.ini
if [ -f "$CONFIG_FILE" ]; then
    # Update existing file
    python -c "
import configparser
config = configparser.ConfigParser()
config.read('$CONFIG_FILE')

if not 'database' in config:
    config['database'] = {}

config['database']['server'] = '$SQL_SERVER'
config['database']['database'] = '$SQL_DATABASE'
config['database']['use_windows_auth'] = 'true' if '$USE_WINDOWS_AUTH'.lower() in ['y', 'yes'] else 'false'
config['database']['username'] = '$SQL_USERNAME'
config['database']['password'] = '$SQL_PASSWORD'

with open('$CONFIG_FILE', 'w') as f:
    config.write(f)
"
else
    # Create new file with all required sections
    python -c "
import configparser
config = configparser.ConfigParser()

config['database'] = {
    'server': '$SQL_SERVER',
    'database': '$SQL_DATABASE',
    'use_windows_auth': 'true' if '$USE_WINDOWS_AUTH'.lower() in ['y', 'yes'] else 'false',
    'username': '$SQL_USERNAME',
    'password': '$SQL_PASSWORD'
}

config['models'] = {
    'embedding_model': 'all-MiniLM-L6-v2',
    'llm_api_url': 'http://localhost:11434/api',
    'llm_model': 'mistral:7b-instruct-v0.2'
}

config['system'] = {
    'vector_db_url': 'http://localhost:6333',
    'vector_db_collection': 'sql_data',
    'output_path': './output',
    'log_level': 'INFO',
    'max_token_limit': '1000',
    'temperature': '0.2'
}

config['ui'] = {
    'web_port': '8501',
    'theme': 'light'
}

with open('$CONFIG_FILE', 'w') as f:
    config.write(f)
"
fi

# Test SQL connection
echo "Testing SQL connection..."
if python -c "
from sql_rag.sql_connector import SQLConnector
try:
    connector = SQLConnector()
    result = connector.test_connection()
    print(f'SQL connection test: {\"Success\" if result else \"Failed\"}')
    exit(0 if result else 1)
except Exception as e:
    print(f'SQL connection test failed: {e}')
    exit(1)
"; then
    echo "SQL connection successful!"
else
    echo "WARNING: SQL connection failed. Please check your connection details and try again later."
fi

# Add code to update run.sh to interrogate SQL and store in RAG
echo "Creating SQL interrogation script..."
cat > interrogate_sql.py << 'EOL'
#!/usr/bin/env python3
"""
Script to interrogate SQL database and store results in vector database.
"""
import argparse
import logging
import sys
from sql_rag.main import SQLRagApplication

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('interrogate_sql')

def main():
    parser = argparse.ArgumentParser(description="Interrogate SQL database and store in vector DB")
    parser.add_argument("--tables", action="store_true", help="Query all tables structure")
    parser.add_argument("--sample", action="store_true", help="Query sample data from each table")
    parser.add_argument("--query", type=str, help="Custom SQL query to execute")
    parser.add_argument("--config", type=str, default="config.ini", help="Config file path")
    
    args = parser.parse_args()
    
    app = SQLRagApplication(config_path=args.config)
    
    if not (args.tables or args.sample or args.query):
        logger.error("Please specify at least one option: --tables, --sample, or --query")
        parser.print_help()
        sys.exit(1)
    
    # Store table metadata
    if args.tables:
        logger.info("Getting database schema information...")
        try:
            # Get all tables
            tables_df = app.sql_connector.get_tables()
            if tables_df.empty:
                logger.warning("No tables found in database")
            else:
                # Store table list
                app.store_sql_data_in_vector_db(
                    "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES",
                    {"content_type": "table_list"}
                )
                
                # For each table, get its columns
                for _, row in tables_df.iterrows():
                    schema = row['schema_name']
                    table = row['table_name']
                    logger.info(f"Getting columns for {schema}.{table}")
                    
                    # Store column information
                    app.store_sql_data_in_vector_db(
                        f"""
                        SELECT 
                            TABLE_SCHEMA, 
                            TABLE_NAME, 
                            COLUMN_NAME, 
                            DATA_TYPE, 
                            CHARACTER_MAXIMUM_LENGTH, 
                            IS_NULLABLE
                        FROM 
                            INFORMATION_SCHEMA.COLUMNS
                        WHERE 
                            TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
                        """,
                        {"content_type": "table_schema", "table": f"{schema}.{table}"}
                    )
                
                logger.info("Successfully stored database schema information")
        except Exception as e:
            logger.error(f"Error getting database schema: {e}")
    
    # Sample data from each table
    if args.sample:
        logger.info("Getting sample data from tables...")
        try:
            tables_df = app.sql_connector.get_tables()
            if tables_df.empty:
                logger.warning("No tables found in database")
            else:
                for _, row in tables_df.iterrows():
                    schema = row['schema_name']
                    table = row['table_name']
                    
                    if row['table_type'] != 'BASE TABLE':
                        logger.info(f"Skipping {schema}.{table} as it's not a base table")
                        continue
                    
                    logger.info(f"Getting sample data from {schema}.{table}")
                    try:
                        # Get top 10 rows as sample
                        app.store_sql_data_in_vector_db(
                            f"SELECT TOP 10 * FROM [{schema}].[{table}]",
                            {"content_type": "table_sample", "table": f"{schema}.{table}"}
                        )
                    except Exception as e:
                        logger.warning(f"Could not get sample from {schema}.{table}: {e}")
                
                logger.info("Successfully stored sample data from tables")
        except Exception as e:
            logger.error(f"Error getting sample data: {e}")
    
    # Custom query
    if args.query:
        logger.info(f"Executing custom query: {args.query}")
        try:
            app.store_sql_data_in_vector_db(
                args.query,
                {"content_type": "custom_query", "query": args.query}
            )
            logger.info("Successfully stored custom query results")
        except Exception as e:
            logger.error(f"Error executing custom query: {e}")

if __name__ == "__main__":
    main()
EOL

chmod +x interrogate_sql.py

# Setup complete
echo
echo "======================================="
echo " Installation completed successfully!"
echo "======================================="
if [ "$OLLAMA_RUNNING" = false ] || [ "$QDRANT_RUNNING" = false ]; then
    echo
    echo "WARNING: Required services are not running:"
    [ "$OLLAMA_RUNNING" = false ] && echo "- Ollama is not running. Start with 'ollama serve'"
    [ "$QDRANT_RUNNING" = false ] && echo "- Qdrant is not running. See docs for setup instructions"
    echo
    echo "Please start these services before running the application."
fi
echo
echo "To populate the vector database with SQL data, run:"
echo
echo "  ./interrogate_sql.py --tables --sample"
echo
echo "To start the application, run:"
echo
echo "  ./run.sh"
echo
echo "======================================="
echo
