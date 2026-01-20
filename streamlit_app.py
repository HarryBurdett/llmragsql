#!/usr/bin/env python3
"""
Streamlit app entry point for SQL-RAG web interface.
"""

import sys
import os
import configparser
import streamlit as st

# Add the project root to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Import the web interface
from sql_rag.ui.web import start_web

def main():
    """Main function to start the Streamlit app."""
    # Load configuration
    config = configparser.ConfigParser()
    
    config_path = "config.ini"
    if os.path.exists(config_path):
        config.read(config_path)
    else:
        # Create basic config
        config["database"] = {
            "server": "localhost",
            "database": "",
            "use_windows_auth": "true",
            "username": "",
            "password": ""
        }
        config["models"] = {
            "embedding_model": "all-MiniLM-L6-v2",
            "llm_api_url": "http://localhost:11434/api",
            "llm_model": "mistral:7b-instruct-v0.2"
        }
        config["system"] = {
            "vector_db_url": "http://localhost:6333",
            "vector_db_collection": "sql_data",
            "output_path": "./output",
            "log_level": "INFO",
            "max_token_limit": "1000",
            "temperature": "0.2"
        }
        config["ui"] = {
            "web_port": "8501",
            "theme": "light"
        }
    
    # Start web interface
    start_web(config)

if __name__ == "__main__":
    main()