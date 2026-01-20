import argparse
import configparser
import logging
import os
import sys
from typing import List, Dict, Any, Optional

import pandas as pd
import streamlit as st
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.http import models

from sql_rag.sql_connector import SQLConnector
from sql_rag.vector_db import VectorDB
from sql_rag.llm import create_llm_instance, LLMInterface

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SQLRagApplication:
    """Main application class for SQL RAG functionality."""
    
    def __init__(self, config_path: str = "config.ini"):
        """Initialize the application with configuration."""
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self.setup_logging()
        
        # Initialize components
        self.sql_connector = self._initialize_sql_connector()
        self.vector_db = self._initialize_vector_db()
        self.llm = self._initialize_llm()
        
        logger.info("SQL RAG Application initialized")

    def _load_config(self, config_path: str) -> configparser.ConfigParser:
        """Load configuration from the specified file."""
        if not os.path.exists(config_path):
            logger.error(f"Configuration file {config_path} not found")
            sys.exit(1)
            
        config = configparser.ConfigParser()
        config.read(config_path)
        return config
    
    def setup_logging(self):
        """Configure logging based on settings in config."""
        log_level = self.config.get("system", "log_level", fallback="INFO")
        numeric_level = getattr(logging, log_level.upper(), logging.INFO)
        logging.getLogger().setLevel(numeric_level)
    
    def _initialize_sql_connector(self) -> SQLConnector:
        """Initialize the SQL connector with database configuration."""
        # Pass the same config path used to initialize this application
        return SQLConnector(config_path=self.config_path)
    
    def _initialize_embedding_model(self) -> SentenceTransformer:
        """Initialize the sentence transformer model for embeddings."""
        model_name = self.config.get("models", "embedding_model", fallback="all-MiniLM-L6-v2")
        logger.info(f"Loading embedding model: {model_name}")
        return SentenceTransformer(model_name)
    
    def _initialize_vector_db(self) -> VectorDB:
        """Initialize vector database interface."""
        logger.info("Initializing vector database")
        return VectorDB(self.config)
    
    def _initialize_llm(self) -> LLMInterface:
        """Initialize LLM interface."""
        logger.info("Initializing LLM")
        return create_llm_instance(self.config)
    
    def ensure_collection_exists(self):
        """Ensure that the vector collection exists, create if not."""
        self.vector_db.ensure_collection_exists()
    
    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for given text."""
        return self.vector_db.generate_embedding(text)
    
    def store_sql_data_in_vector_db(self, query: str, metadata: Dict[str, Any] = None):
        """Execute SQL query and store results in vector database."""
        try:
            # Execute the SQL query
            df = self.sql_connector.execute_query(query)
            if df.empty:
                logger.warning("SQL query returned no results")
                return
                
            logger.info(f"Retrieved {len(df)} rows from SQL database")
            
            # Process rows for storage
            texts = []
            meta_list = []
            
            for idx, row in df.iterrows():
                # Convert row to string for embedding
                row_text = " ".join([f"{col}: {val}" for col, val in row.items()])
                
                # Prepare metadata for this row
                row_metadata = {
                    "source": "sql_query",
                    "query": query,
                    "row_index": idx
                }
                
                if metadata:
                    row_metadata.update(metadata)
                
                texts.append(row_text)
                meta_list.append(row_metadata)
            
            # Store all vectors at once for better performance
            result = self.vector_db.store_vectors(texts, meta_list)
            
            if result:
                logger.info(f"Stored {len(df)} records in vector database")
            else:
                logger.error("Failed to store vectors in database")
            
        except Exception as e:
            logger.error(f"Error storing SQL data in vector DB: {e}")
            raise
    
    def query_similar_data(self, query_text: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Query vector database for data similar to the input text."""
        # Use the vector_db interface to search for similar data
        return self.vector_db.search_similar(query_text, limit)
    
    def run_rag_query(self, user_query: str) -> Dict[str, Any]:
        """Execute a RAG query combining retrieval and generation."""
        logger.info(f"Processing RAG query: {user_query}")
        
        # 1. Retrieve relevant information from the vector database
        similar_data = self.query_similar_data(user_query)
        
        # 2 & 3. Format the retrieved information into a prompt and send to LLM
        if not similar_data:
            response = "I couldn't find any relevant information to answer your question."
        else:
            response = self.llm.process_rag_query(user_query, similar_data)
        
        # 4. Return the response with retrieved data
        return {
            "query": user_query,
            "retrieved_data": similar_data,
            "response": response
        }
        
    def get_sql_for_question(self, user_query: str) -> str:
        """Generate SQL query based on natural language question."""
        logger.info(f"Generating SQL for question: {user_query}")
        
        # Get database schema information to help with query generation
        try:
            tables_df = self.sql_connector.get_tables()
            schema_info = {}
            
            if not tables_df.empty:
                for _, row in tables_df.iterrows():
                    schema_name = row['schema_name']
                    table_name = row['table_name']
                    
                    # Get column information for this table
                    columns_df = self.sql_connector.get_columns(table_name, schema_name)
                    
                    # Format table and column information
                    full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
                    column_info = []
                    
                    for _, col_row in columns_df.iterrows():
                        column_info.append({
                            "name": col_row['column_name'],
                            "type": col_row['data_type'],
                            "nullable": col_row['is_nullable']
                        })
                    
                    schema_info[full_table_name] = {
                        "columns": column_info
                    }
        except Exception as e:
            logger.warning(f"Error getting schema for SQL generation: {e}")
            schema_info = {}
            
        # Create a prompt with database schema context
        schema_context = ""
        if schema_info:
            schema_context = "Database Schema:\n"
            for table_name, table_info in schema_info.items():
                schema_context += f"Table: {table_name}\n"
                schema_context += "Columns:\n"
                for col in table_info["columns"]:
                    schema_context += f"  - {col['name']} ({col['type']})\n"
                schema_context += "\n"
        
        prompt = f"""You are a SQL query generator. 
Given the following database schema and a natural language question, 
generate a valid SQL query that would answer the question.

{schema_context}

Question: {user_query}

Generate only the SQL query without any explanations or comments. 
The SQL query should be valid and compatible with SQL Server/T-SQL syntax."""
        
        # Get completion from LLM
        sql_query = self.llm.get_completion(prompt)
        
        # Clean up the response to extract just the SQL query
        sql_query = sql_query.strip()
        
        # Remove markdown code block formatting if present
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.startswith("```"):
            sql_query = sql_query[3:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
            
        return sql_query.strip()

    def run_cli_mode(self):
        """Run the application in interactive CLI mode."""
        print("\n===== SQL RAG CLI Mode =====")
        print("Type 'exit' or 'quit' to exit the application")
        print("Type 'help' for available commands")
        
        while True:
            try:
                user_input = input("\nSQL RAG> ").strip()
                
                if user_input.lower() in ('exit', 'quit'):
                    print("Exiting SQL RAG CLI...")
                    break
                
                elif user_input.lower() == 'help':
                    print("\nAvailable commands:")
                    print("  sql <query>  - Execute SQL query and store results in vector database")
                    print("  ask <query>  - Ask a natural language question using RAG")
                    print("  help         - Show this help message")
                    print("  exit/quit    - Exit the application")
                
                elif user_input.lower().startswith('sql '):
                    sql_query = user_input[4:].strip()
                    if not sql_query:
                        print("Error: SQL query is empty")
                        continue
                    
                    print(f"Executing SQL query: {sql_query}")
                    try:
                        self.store_sql_data_in_vector_db(sql_query)
                        print("SQL query results stored in vector database")
                    except Exception as e:
                        print(f"Error executing SQL query: {e}")
                
                elif user_input.lower().startswith('ask '):
                    rag_query = user_input[4:].strip()
                    if not rag_query:
                        print("Error: Question is empty")
                        continue
                    
                    print(f"Processing question: {rag_query}")
                    try:
                        result = self.run_rag_query(rag_query)
                        print("\nRetrieved Data:")
                        for item in result["retrieved_data"]:
                            print(f"- Score: {item['score']:.4f}")
                            print(f"  Text: {item['payload']['text'][:100]}...")
                        print(f"\nResponse: {result['response']}")
                    except Exception as e:
                        print(f"Error processing question: {e}")
                
                else:
                    print("Unknown command. Type 'help' for available commands.")
            
            except KeyboardInterrupt:
                print("\nInterrupted. Exiting SQL RAG CLI...")
                break
            except Exception as e:
                print(f"Error: {e}")

    def run_web_mode(self, port: int = None):
        """Run the application in web mode using Streamlit."""
        # This function doesn't need to do anything as Streamlit has its own entry point
        # The actual Streamlit app is defined outside this class
        if port is None:
            port = int(self.config.get("ui", "web_port", fallback="8501"))
        
        print(f"To start the web interface, run: streamlit run {__file__} --server.port={port}")
        sys.exit(0)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="SQL RAG Application")
    parser.add_argument("--config", type=str, default="config.ini",
                        help="Path to configuration file")
    parser.add_argument("--mode", type=str, choices=["cli", "web"], default=None,
                        help="Run mode: 'cli' for interactive command line, 'web' for Streamlit interface")
    parser.add_argument("--query", type=str,
                        help="SQL query to execute and store in vector database")
    parser.add_argument("--rag-query", type=str,
                        help="Natural language query for RAG processing")
    parser.add_argument("--port", type=int, default=None,
                        help="Port for web interface (only used with --mode=web)")
    return parser.parse_args()


def main():
    """Main entry point for the application."""
    args = parse_arguments()
    
    try:
        # Initialize application
        app = SQLRagApplication(config_path=args.config)
        
        # Ensure vector collection exists
        app.ensure_collection_exists()
        
        # Check if a specific mode was requested
        if args.mode == "cli":
            app.run_cli_mode()
            return
        
        elif args.mode == "web":
            app.run_web_mode(port=args.port)
            return  # The run_web_mode function will exit
        
        # If no mode specified, process command line arguments
        if args.query:
            app.store_sql_data_in_vector_db(args.query)
            print(f"SQL query results stored in vector database")
            
        if args.rag_query:
            result = app.run_rag_query(args.rag_query)
            print("\nRAG Query Results:")
            print(f"Query: {result['query']}")
            print("\nRetrieved Data:")
            for item in result["retrieved_data"]:
                print(f"- Score: {item['score']:.4f}")
                print(f"  Text: {item['payload']['text'][:100]}...")
            print(f"\nResponse: {result['response']}")
        
        if not args.query and not args.rag_query and not args.mode:
            print("No operation specified. Use --mode, --query, or --rag-query arguments.")
            print("Run with --help for more information.")
    
    except Exception as e:
        logger.error(f"Error in main: {e}")
        sys.exit(1)


# Streamlit web interface
def streamlit_app():
    """Define the Streamlit web interface."""
    st.set_page_config(
        page_title="SQL RAG Application",
        page_icon="🔍",
        layout="wide",
    )
    
    st.title("SQL RAG Application")
    st.subheader("Query SQL databases with natural language")
    
    # Initialize the application
    @st.cache_resource
    def get_app():
        config_path = st.session_state.get("config_path", "config.ini")
        app = SQLRagApplication(config_path=config_path)
        app.ensure_collection_exists()
        return app
    
    app = get_app()
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        config_path = st.text_input("Config Path", value="config.ini")
        if st.button("Reload Config"):
            st.session_state["config_path"] = config_path
            st.rerun()
        
        st.markdown("---")
        st.subheader("About")
        st.write("SQL RAG Application allows you to query SQL databases using natural language.")
    
    # Create tabs for different functionalities
    tab1, tab2 = st.tabs(["Execute SQL Query", "Ask Questions"])
    
    # Tab 1: Execute SQL Query
    with tab1:
        st.header("Store SQL Query Results")
        sql_query = st.text_area("Enter SQL Query", height=150)
        
        if st.button("Execute SQL Query"):
            if sql_query:
                with st.spinner("Executing SQL query and storing results..."):
                    try:
                        app.store_sql_data_in_vector_db(sql_query)
                        st.success("SQL query results stored in vector database")
                    except Exception as e:
                        st.error(f"Error executing SQL query: {e}")
            else:
                st.warning("Please enter a SQL query")
    
    # Tab 2: Ask Questions
    with tab2:
        st.header("Ask Questions About Your Data")
        question = st.text_input("Enter your question")
        
        if st.button("Ask Question"):
            if question:
                with st.spinner("Processing your question..."):
                    try:
                        result = app.run_rag_query(question)
                        
                        st.subheader("Query Results:")
                        st.write(f"**Question:** {result['query']}")
                        
                        st.subheader("Retrieved Data:")
                        for i, item in enumerate(result["retrieved_data"]):
                            with st.expander(f"Result {i+1} (Score: {item['score']:.4f})", expanded=i==0):
                                st.text(item['payload']['text'])
                        
                        st.subheader("Response:")
                        st.write(result['response'])
                    except Exception as e:
                        st.error(f"Error processing question: {e}")
            else:
                st.warning("Please enter a question")


# Check if this script is being run directly
if __name__ == "__main__":
    # If script is run with streamlit, use the streamlit app
    if any(arg.startswith('--server.') for arg in sys.argv[1:]) or 'streamlit' in sys.argv[0]:
        streamlit_app()
    else:
        # Otherwise, run the CLI version
        main()
