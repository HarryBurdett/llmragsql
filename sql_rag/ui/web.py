"""
Web Interface for SQL RAG application using Streamlit.
"""

import logging
import sys
import os
from typing import Dict, Any

import streamlit as st
import pandas as pd
import plotly.express as px
from plotly.graph_objects import Figure

from sql_rag.main import SQLRagApplication

# Configure logging
logger = logging.getLogger(__name__)

def setup_page():
    """Set up the Streamlit page configuration."""
    st.set_page_config(
        page_title="SQL RAG Web Interface",
        page_icon="🔍",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #4257B2;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #5C5C5C;
        margin-bottom: 1rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #F0F2F6;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #E0E5FF;
    }
    </style>
    """, unsafe_allow_html=True)

def setup_sidebar():
    """Set up the sidebar with configuration options."""
    st.sidebar.title("SQL RAG")
    st.sidebar.markdown("---")
    
    st.sidebar.subheader("Configuration")
    
    # Service Status Check
    with st.sidebar.expander("Service Status", expanded=True):
        # Check Ollama status
        try:
            import requests
            ollama_status = False
            try:
                response = requests.get('http://localhost:11434/api/tags', timeout=3)
                ollama_status = response.status_code == 200
            except:
                ollama_status = False
            
            # Check Qdrant status
            qdrant_status = False
            try:
                # Try health endpoint first (works for both standalone and Docker)
                response = requests.get('http://localhost:6333/healthz', timeout=3)
                qdrant_status = response.status_code == 200
            except:
                try:
                    # Try dashboard as fallback
                    response = requests.get('http://localhost:6333/dashboard/', timeout=3)
                    qdrant_status = response.status_code == 200
                except:
                    qdrant_status = False
            
            # Display status with colored indicators
            col1, col2 = st.columns(2)
            with col1:
                if ollama_status:
                    st.success("✅ Ollama: Running")
                else:
                    st.error("❌ Ollama: Not Running")
                    st.markdown("Run: `ollama serve`")
            
            with col2:
                if qdrant_status:
                    st.success("✅ Qdrant: Running")
                else:
                    st.error("❌ Qdrant: Not Running")
                    st.markdown("Start Docker container")
            
            if st.button("Refresh Status"):
                st.rerun()
                
        except Exception as e:
            st.error(f"Error checking services: {e}")
    
    # Database connection
    with st.sidebar.expander("Database Connection", expanded=True):
        config = st.session_state.get("config")
        
        if config and "database" in config:
            db_config = config["database"]
            server = st.text_input("Server", value=db_config.get("server", "localhost"), key="db_server")
            database = st.text_input("Database", value=db_config.get("database", ""), key="db_database")
            use_windows_auth = st.checkbox("Use Windows Authentication", 
                                        value=db_config.getboolean("use_windows_auth", True), 
                                        key="db_use_windows_auth")
            
            if not use_windows_auth:
                username = st.text_input("Username", value=db_config.get("username", ""), key="db_username")
                password = st.text_input("Password", type="password", value=db_config.get("password", ""), key="db_password")
            else:
                username = ""
                password = ""
        else:
            server = st.text_input("Server", value="localhost", key="db_server")
            database = st.text_input("Database", value="", key="db_database")
            use_windows_auth = st.checkbox("Use Windows Authentication", value=True, key="db_use_windows_auth")
            
            if not use_windows_auth:
                username = st.text_input("Username", value="", key="db_username")
                password = st.text_input("Password", type="password", value="", key="db_password")
            else:
                username = ""
                password = ""
        
        if st.button("Test Connection"):
            if not database:
                st.error("Please enter a database name")
            else:
                # Create a temporary connector to test
                try:
                    import configparser
                    from sql_rag.sql_connector import SQLConnector
                    
                    temp_config = configparser.ConfigParser()
                    if not temp_config.has_section("database"):
                        temp_config.add_section("database")
                    
                    temp_config["database"]["server"] = server
                    temp_config["database"]["database"] = database
                    temp_config["database"]["use_windows_auth"] = str(use_windows_auth)
                    temp_config["database"]["username"] = username
                    temp_config["database"]["password"] = password
                    
                    # Save temporary config
                    temp_config_path = "temp_config.ini"
                    with open(temp_config_path, "w") as f:
                        temp_config.write(f)
                    
                    # Test connection
                    with st.spinner("Testing connection..."):
                        conn = SQLConnector(config_path=temp_config_path)
                        result = conn.test_connection()
                    
                    # Remove temp file
                    if os.path.exists(temp_config_path):
                        os.remove(temp_config_path)
                    
                    if result:
                        st.success("Connection successful!")
                    else:
                        st.error("Connection failed.")
                except Exception as e:
                    st.error(f"Connection error: {e}")
        
        if st.button("Save Database Config"):
            # Update config
            config = st.session_state.get("config")
            if config:
                if not config.has_section("database"):
                    config.add_section("database")
                
                config["database"]["server"] = server
                config["database"]["database"] = database
                config["database"]["use_windows_auth"] = str(use_windows_auth)
                config["database"]["username"] = username
                config["database"]["password"] = password
                
                # Save to file
                with open("config.ini", "w") as f:
                    config.write(f)
                
                st.success("Database configuration saved")
                st.session_state["reload_app"] = True
                st.rerun()
    
    # Model settings
    with st.sidebar.expander("Model Settings", expanded=False):
        config = st.session_state.get("config")
        
        if config and "models" in config:
            models_config = config["models"]
            system_config = config["system"]
            
            # LLM Provider selection
            provider_options = ["local", "openai", "anthropic", "gemini", "groq"]
            provider = st.selectbox(
                "LLM Provider",
                options=provider_options,
                index=provider_options.index(models_config.get("provider", "local")) if models_config.get("provider", "local") in provider_options else 0,
                key="provider",
                help="Select your LLM provider: local (Ollama), OpenAI, Anthropic Claude, Google Gemini, or Groq"
            )
            
            # Provider-specific settings
            if provider == "local":
                # Initialize with default value from config
                model_name = models_config.get("llm_model", "mistral:7b-instruct-v0.2")
                
                # List of preset local models
                preset_models = [
                    "llama2", "llama2:7b", "llama2:13b", "llama2:70b",
                    "llama3", "llama3:8b", "llama3:70b",
                    "mistral", "mistral:7b", "mistral:7b-instruct-v0.2",
                    "codellama", "codellama:7b", "codellama:13b", "codellama:34b",
                    "phi", "phi:2.7b", "phi:3",
                    "gemma", "gemma:2b", "gemma:7b"
                ]
                
                # Fetch available models if Ollama is running
                available_models = []
                if ollama_status:
                    try:
                        from sql_rag.llm import LocalLLM
                        if config:
                            llm = LocalLLM(config)
                            available_models = llm.list_available_models()
                            if available_models:
                                st.success(f"✅ Found {len(available_models)} installed local models")
                    except Exception as e:
                        st.error(f"Error loading installed models: {e}")
                
                # Create tabs for choosing models
                local_model_tabs = st.tabs(["Installed Models", "Preset Models", "Custom Model"])
                
                # Installed Models Tab
                with local_model_tabs[0]:
                    if available_models:
                        model_idx = 0
                        if model_name in available_models:
                            model_idx = available_models.index(model_name)
                        model_name = st.selectbox(
                            "Select Installed Model", 
                            options=available_models,
                            index=model_idx,
                            key="installed_model"
                        )
                    else:
                        st.warning("No installed models found. Start Ollama or select a model from the Preset Models tab.")
                
                # Preset Models Tab
                with local_model_tabs[1]:
                    model_idx = 0
                    if model_name in preset_models:
                        model_idx = preset_models.index(model_name)
                    preset_model = st.selectbox(
                        "Select Preset Model",
                        options=preset_models,
                        index=model_idx,
                        key="preset_model"
                    )
                    
                    if st.button("Use This Preset"):
                        model_name = preset_model
                        st.success(f"Selected: {model_name}")
                        if not ollama_status:
                            st.info(f"To install: ollama pull {model_name}")
                
                # Custom Model Tab
                with local_model_tabs[2]:
                    custom_model = st.text_input("Custom Model Name", value=model_name, key="custom_model")
                    if st.button("Use Custom Model"):
                        model_name = custom_model
                        st.success(f"Using custom model: {model_name}")
                
            elif provider == "openai":
                # OpenAI settings
                if config.has_section("openai"):
                    openai_config = config["openai"]
                    api_key = st.text_input("API Key", value=openai_config.get("api_key", ""), type="password", key="openai_api_key")
                    
                    # Pre-populate available models
                    openai_models = [
                        "gpt-3.5-turbo", "gpt-3.5-turbo-16k",
                        "gpt-4", "gpt-4-turbo", "gpt-4-32k", "gpt-4o"
                    ]
                    
                    # Try to fetch models if API key is provided
                    dynamic_models = []
                    if api_key:
                        try:
                            # Initialize a temporary OpenAI LLM to get models
                            from sql_rag.llm import OpenAILLM
                            temp_config = configparser.ConfigParser()
                            temp_config["openai"] = {"api_key": api_key}
                            temp_config["system"] = {"temperature": "0.2", "max_token_limit": "1000"}
                            openai_llm = OpenAILLM(temp_config)
                            dynamic_models = openai_llm.list_available_models()
                            if dynamic_models:
                                openai_models = dynamic_models
                                st.success(f"✅ Found {len(openai_models)} OpenAI models")
                        except Exception as e:
                            st.warning(f"Could not fetch models: {e}")
                    
                    # Model selection
                    model_name = openai_config.get("model", "gpt-3.5-turbo")
                    model_idx = 0
                    if model_name in openai_models:
                        model_idx = openai_models.index(model_name)
                        
                    model_name = st.selectbox(
                        "Model", 
                        options=openai_models,
                        index=model_idx,
                        key="openai_model"
                    )
                else:
                    api_key = st.text_input("API Key", value="", type="password", key="openai_api_key")
                    model_name = st.selectbox(
                        "Model", 
                        options=["gpt-3.5-turbo", "gpt-3.5-turbo-16k", "gpt-4", "gpt-4-turbo", "gpt-4-32k", "gpt-4o"],
                        key="openai_model"
                    )
                
            elif provider == "anthropic":
                # Anthropic settings
                if config.has_section("anthropic"):
                    anthropic_config = config["anthropic"]
                    api_key = st.text_input("API Key", value=anthropic_config.get("api_key", ""), type="password", key="anthropic_api_key")
                    
                    # Define available models
                    anthropic_models = [
                        "claude-3-opus-20240229", 
                        "claude-3-sonnet-20240229", 
                        "claude-3-haiku-20240307",
                        "claude-3.5-sonnet",
                        "claude-2.1", 
                        "claude-2.0",
                        "claude-instant-1.2"
                    ]
                    
                    # Try to get real-time list if API key is provided
                    if api_key:
                        try:
                            # Initialize a temporary Anthropic LLM to get models
                            from sql_rag.llm import AnthropicLLM
                            temp_config = configparser.ConfigParser()
                            temp_config["anthropic"] = {"api_key": api_key}
                            temp_config["system"] = {"temperature": "0.2", "max_token_limit": "1000"}
                            anthropic_llm = AnthropicLLM(temp_config)
                            available_models = anthropic_llm.list_available_models()
                            if available_models:
                                st.success(f"✅ API key is valid")
                        except Exception as e:
                            st.warning(f"Could not validate API key: {e}")
                    
                    # Model selection
                    model_name = anthropic_config.get("model", "claude-3-sonnet-20240229")
                    model_idx = 1  # Default to sonnet
                    if model_name in anthropic_models:
                        model_idx = anthropic_models.index(model_name)
                    
                    model_name = st.selectbox(
                        "Model", 
                        options=anthropic_models,
                        index=model_idx,
                        key="anthropic_model"
                    )
                else:
                    api_key = st.text_input("API Key", value="", type="password", key="anthropic_api_key")
                    model_name = st.selectbox(
                        "Model",
                        options=[
                            "claude-3-5-sonnet-20241022",
                            "claude-3-5-haiku-20241022",
                            "claude-3-opus-20240229",
                            "claude-3-sonnet-20240229",
                            "claude-3-haiku-20240307",
                        ],
                        index=0,
                        key="anthropic_model"
                    )

            elif provider == "gemini":
                # Gemini settings
                st.markdown("**Google Gemini** - Get API key from [Google AI Studio](https://aistudio.google.com/apikey)")
                if config.has_section("gemini"):
                    gemini_config = config["gemini"]
                    api_key = st.text_input("API Key", value=gemini_config.get("api_key", ""), type="password", key="gemini_api_key")

                    # Define available models
                    gemini_models = [
                        "gemini-1.5-pro",
                        "gemini-1.5-flash",
                        "gemini-1.5-flash-8b",
                        "gemini-1.0-pro",
                    ]

                    # Try to get real-time list if API key is provided
                    if api_key:
                        try:
                            from sql_rag.llm import GeminiLLM
                            temp_config = configparser.ConfigParser()
                            temp_config["gemini"] = {"api_key": api_key}
                            temp_config["system"] = {"temperature": "0.2", "max_token_limit": "1000"}
                            gemini_llm = GeminiLLM(temp_config)
                            available_models = gemini_llm.list_available_models()
                            if available_models:
                                gemini_models = available_models
                                st.success(f"✅ Found {len(gemini_models)} Gemini models")
                        except Exception as e:
                            st.warning(f"Could not fetch models: {e}")

                    # Model selection
                    model_name = gemini_config.get("model", "gemini-1.5-flash")
                    model_idx = 0
                    if model_name in gemini_models:
                        model_idx = gemini_models.index(model_name)

                    model_name = st.selectbox(
                        "Model",
                        options=gemini_models,
                        index=model_idx,
                        key="gemini_model"
                    )
                else:
                    api_key = st.text_input("API Key", value="", type="password", key="gemini_api_key")
                    model_name = st.selectbox(
                        "Model",
                        options=[
                            "gemini-1.5-pro",
                            "gemini-1.5-flash",
                            "gemini-1.5-flash-8b",
                            "gemini-1.0-pro",
                        ],
                        index=1,
                        key="gemini_model"
                    )

            elif provider == "groq":
                # Groq settings
                st.markdown("**Groq** - Fast inference for open models. Get API key from [Groq Console](https://console.groq.com/keys)")
                if config.has_section("groq"):
                    groq_config = config["groq"]
                    api_key = st.text_input("API Key", value=groq_config.get("api_key", ""), type="password", key="groq_api_key")

                    # Define available models
                    groq_models = [
                        "llama-3.3-70b-versatile",
                        "llama-3.1-70b-versatile",
                        "llama-3.1-8b-instant",
                        "mixtral-8x7b-32768",
                        "gemma2-9b-it",
                    ]

                    # Try to get real-time list if API key is provided
                    if api_key:
                        try:
                            from sql_rag.llm import GroqLLM
                            temp_config = configparser.ConfigParser()
                            temp_config["groq"] = {"api_key": api_key}
                            temp_config["system"] = {"temperature": "0.2", "max_token_limit": "1000"}
                            groq_llm = GroqLLM(temp_config)
                            available_models = groq_llm.list_available_models()
                            if available_models:
                                groq_models = available_models
                                st.success(f"✅ Found {len(groq_models)} Groq models")
                        except Exception as e:
                            st.warning(f"Could not fetch models: {e}")

                    # Model selection
                    model_name = groq_config.get("model", "llama-3.1-70b-versatile")
                    model_idx = 0
                    if model_name in groq_models:
                        model_idx = groq_models.index(model_name)

                    model_name = st.selectbox(
                        "Model",
                        options=groq_models,
                        index=model_idx,
                        key="groq_model"
                    )
                else:
                    api_key = st.text_input("API Key", value="", type="password", key="groq_api_key")
                    model_name = st.selectbox(
                        "Model",
                        options=[
                            "llama-3.3-70b-versatile",
                            "llama-3.1-70b-versatile",
                            "llama-3.1-8b-instant",
                            "mixtral-8x7b-32768",
                            "gemma2-9b-it",
                        ],
                        index=0,
                        key="groq_model"
                    )

            # Common settings for all providers
            temperature = st.slider("Temperature", min_value=0.0, max_value=1.0, 
                                    value=float(system_config.get("temperature", "0.2")), 
                                    step=0.1, key="temperature")
            max_tokens = st.number_input("Max Tokens", min_value=100, max_value=8000, 
                                        value=int(system_config.get("max_token_limit", "1000")), 
                                        step=100, key="max_tokens")
        else:
            provider = st.selectbox("LLM Provider", options=["local", "openai", "anthropic", "gemini", "groq"], key="provider")

            if provider == "local":
                # Preset local models for selection
                preset_models = [
                    "llama2", "llama2:7b", "llama2:13b", "llama2:70b",
                    "llama3", "llama3:8b", "llama3:70b",
                    "mistral", "mistral:7b", "mistral:7b-instruct-v0.2",
                    "codellama", "codellama:7b", "codellama:13b", "codellama:34b",
                    "phi", "phi:2.7b", "phi:3",
                    "gemma", "gemma:2b", "gemma:7b"
                ]
                model_name = st.selectbox(
                    "Model",
                    options=preset_models,
                    index=preset_models.index("mistral:7b-instruct-v0.2") if "mistral:7b-instruct-v0.2" in preset_models else 0,
                    key="local_model"
                )
            elif provider == "openai":
                api_key = st.text_input("API Key", value="", type="password", key="openai_api_key")
                model_name = st.selectbox(
                    "Model",
                    options=["gpt-4o", "gpt-4-turbo", "gpt-4", "gpt-3.5-turbo"],
                    key="openai_model"
                )
            elif provider == "anthropic":
                api_key = st.text_input("API Key", value="", type="password", key="anthropic_api_key")
                model_name = st.selectbox(
                    "Model",
                    options=[
                        "claude-3-5-sonnet-20241022",
                        "claude-3-5-haiku-20241022",
                        "claude-3-opus-20240229",
                        "claude-3-sonnet-20240229",
                        "claude-3-haiku-20240307",
                    ],
                    index=0,
                    key="anthropic_model"
                )
            elif provider == "gemini":
                st.markdown("Get API key from [Google AI Studio](https://aistudio.google.com/apikey)")
                api_key = st.text_input("API Key", value="", type="password", key="gemini_api_key")
                model_name = st.selectbox(
                    "Model",
                    options=[
                        "gemini-1.5-pro",
                        "gemini-1.5-flash",
                        "gemini-1.5-flash-8b",
                        "gemini-1.0-pro",
                    ],
                    index=1,
                    key="gemini_model"
                )
            elif provider == "groq":
                st.markdown("Get API key from [Groq Console](https://console.groq.com/keys)")
                api_key = st.text_input("API Key", value="", type="password", key="groq_api_key")
                model_name = st.selectbox(
                    "Model",
                    options=[
                        "llama-3.3-70b-versatile",
                        "llama-3.1-70b-versatile",
                        "llama-3.1-8b-instant",
                        "mixtral-8x7b-32768",
                        "gemma2-9b-it",
                    ],
                    index=0,
                    key="groq_model"
                )

            temperature = st.slider("Temperature", min_value=0.0, max_value=1.0, value=0.2, step=0.1, key="temperature")
            max_tokens = st.number_input("Max Tokens", min_value=100, max_value=8000, value=1000, step=100, key="max_tokens")
        
        if st.button("Save Model Config"):
            # Update config
            config = st.session_state.get("config")
            if config:
                if not config.has_section("models"):
                    config.add_section("models")
                if not config.has_section("system"):
                    config.add_section("system")
                
                # Save provider selection
                config["models"]["provider"] = provider
                
                # Save provider-specific settings
                if provider == "local":
                    config["models"]["llm_model"] = model_name
                elif provider == "openai":
                    if not config.has_section("openai"):
                        config.add_section("openai")
                    config["openai"]["api_key"] = api_key
                    config["openai"]["model"] = model_name
                elif provider == "anthropic":
                    if not config.has_section("anthropic"):
                        config.add_section("anthropic")
                    config["anthropic"]["api_key"] = api_key
                    config["anthropic"]["model"] = model_name
                elif provider == "gemini":
                    if not config.has_section("gemini"):
                        config.add_section("gemini")
                    config["gemini"]["api_key"] = api_key
                    config["gemini"]["model"] = model_name
                elif provider == "groq":
                    if not config.has_section("groq"):
                        config.add_section("groq")
                    config["groq"]["api_key"] = api_key
                    config["groq"]["model"] = model_name
                
                # Save common settings
                config["system"]["temperature"] = str(temperature)
                config["system"]["max_token_limit"] = str(max_tokens)
                
                # Save to file
                with open("config.ini", "w") as f:
                    config.write(f)
                
                st.success("Model configuration saved")
                st.session_state["reload_app"] = True
                st.rerun()
    
    # Vector database settings
    with st.sidebar.expander("Vector Database", expanded=False):
        config = st.session_state.get("config")
        
        if config and "system" in config:
            system_config = config["system"]
            vector_db_url = st.text_input("Vector DB URL", 
                                        value=system_config.get("vector_db_url", "http://localhost:6333"), 
                                        key="vector_db_url")
            collection_name = st.text_input("Collection Name", 
                                          value=system_config.get("vector_db_collection", "sql_data"), 
                                          key="collection_name")
        else:
            vector_db_url = st.text_input("Vector DB URL", value="http://localhost:6333", key="vector_db_url")
            collection_name = st.text_input("Collection Name", value="sql_data", key="collection_name")
        
        if st.button("Save Vector DB Config"):
            # Update config
            config = st.session_state.get("config")
            if config:
                if not config.has_section("system"):
                    config.add_section("system")
                
                config["system"]["vector_db_url"] = vector_db_url
                config["system"]["vector_db_collection"] = collection_name
                
                # Save to file
                with open("config.ini", "w") as f:
                    config.write(f)
                
                st.success("Vector DB configuration saved")
                st.session_state["reload_app"] = True
                st.rerun()

        # Add button to populate vector database from SQL
        if st.button("Populate Vector DB from SQL"):
            if not qdrant_status:
                st.error("Qdrant is not running. Please start it first.")
            else:
                # Create tab with interrogate_sql.py functionality
                st.session_state["active_tab"] = "Data Ingestion"
                st.rerun()
    
    # About section
    st.sidebar.markdown("---")
    st.sidebar.subheader("About")
    st.sidebar.markdown("""
    **SQL RAG Application** is a tool that allows you to:
    
    - Connect to SQL databases
    - Store query results in a vector database
    - Ask natural language questions about your data
    
    The application uses:
    - Qdrant for vector storage
    - Ollama for local LLM inferencing
    - Sentence Transformers for embeddings
    """)

def display_database_tab(app):
    """Display database connection and query tab."""
    st.markdown("<h2 class='sub-header'>Database Operations</h2>", unsafe_allow_html=True)
    
    if app is None:
        st.error("Application not initialized. Please check your configuration in the Settings tab.")
        return
    
    # Display connection status
    try:
        connection_status = app.sql_connector.test_connection()
        if connection_status:
            st.success("✅ Connected to database")
        else:
            st.error("❌ Database connection failed")
    except Exception as e:
        st.error(f"❌ Database connection error: {e}")
    
    # Query section
    st.subheader("Execute SQL Query")
    
    query = st.text_area("Enter SQL Query", height=150,
                         placeholder="SELECT * FROM Customers LIMIT 10")
    
    col1, col2 = st.columns([1, 4])
    with col1:
        execute_button = st.button("Execute Query", type="primary")
    with col2:
        store_in_vector_db = st.checkbox("Store results in vector database", value=True)
    
    if execute_button and query:
        try:
            with st.spinner("Executing query..."):
                df = app.sql_connector.execute_query(query)
                
                if store_in_vector_db:
                    # Store results in vector database
                    with st.spinner("Storing results in vector database..."):
                        app.store_sql_data_in_vector_db(query)
                    st.success("Results stored in vector database")
                
                # Display results
                st.subheader("Query Results")
                st.dataframe(df, use_container_width=True)
                
                # Show basic statistics
                st.subheader("Statistics")
                col1, col2, col3 = st.columns(3)
                col1.metric("Rows", len(df))
                col2.metric("Columns", len(df.columns))
                
                # Store in session state for other tabs
                st.session_state["last_query_results"] = df
                
                # Show visualization options if applicable
                if len(df) > 0 and len(df.columns) > 1:
                    st.subheader("Visualization")
                    
                    # Only show for numeric/date columns that might make sense to visualize
                    numeric_cols = df.select_dtypes(include=['int', 'float']).columns.tolist()
                    if len(numeric_cols) >= 2:
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            x_axis = st.selectbox("X-Axis", df.columns.tolist())
                        with col2:
                            y_options = [col for col in numeric_cols if col != x_axis] if x_axis not in numeric_cols else numeric_cols
                            y_axis = st.selectbox("Y-Axis", y_options) if y_options else None
                        
                        if y_axis:
                            chart_type = st.radio("Chart Type", ["Bar", "Line", "Scatter"], horizontal=True)
                            
                            if chart_type == "Bar":
                                fig = px.bar(df, x=x_axis, y=y_axis, title=f"{y_axis} by {x_axis}")
                            elif chart_type == "Line":
                                fig = px.line(df, x=x_axis, y=y_axis, title=f"{y_axis} over {x_axis}")
                            else:  # Scatter
                                fig = px.scatter(df, x=x_axis, y=y_axis, title=f"{y_axis} vs {x_axis}")
                            
                            st.plotly_chart(fig, use_container_width=True)
        
        except Exception as e:
            st.error(f"Error executing query: {e}")
    
    # Database schema explorer
    st.markdown("---")
    st.subheader("Database Schema")
    
    try:
        with st.spinner("Loading database schema..."):
            tables_df = app.sql_connector.get_tables()
        
        if not tables_df.empty:
            # Create a dropdown of tables
            table_options = [f"{row['schema_name']}.{row['table_name']}" for _, row in tables_df.iterrows()]
            selected_table = st.selectbox("Select a table to view its schema", options=[""] + table_options)
            
            if selected_table:
                schema, table = selected_table.split('.')
                
                with st.spinner(f"Loading schema for {selected_table}..."):
                    columns_df = app.sql_connector.get_columns(table, schema)
                
                if not columns_df.empty:
                    st.dataframe(columns_df, use_container_width=True)
                    
                    # Add a button to generate a sample query for this table
                    if st.button(f"Generate Sample Query for {selected_table}"):
                        column_list = ", ".join(columns_df['column_name'].tolist())
                        sample_query = f"SELECT {column_list} FROM {selected_table} LIMIT 10"
                        st.code(sample_query, language="sql")
                        st.session_state["sample_query"] = sample_query
        else:
            st.info("No tables found in the database or unable to retrieve schema information.")
    
    except Exception as e:
        st.error(f"Error loading schema: {e}")

def display_rag_tab(app):
    """Display RAG query tab."""
    st.markdown("<h2 class='sub-header'>Question Answering</h2>", unsafe_allow_html=True)
    
    if app is None:
        st.error("Application not initialized. Please check your configuration in the Settings tab.")
        return
    
    # Check if we have data in the vector database
    db_has_data = False
    try:
        # Use the get_collection_info method which is more robust
        collection_info = app.vector_db.get_collection_info()
        
        if not collection_info.get("exists", False) or collection_info.get("points_count", 0) == 0:
            st.warning("Your vector database is empty. Please execute some SQL queries first and store the results.")
            st.info("Go to the 'Data Ingestion' tab to load data from your SQL database into the vector database.")
            
            # Add a quick shortcut button with a unique key
            data_nav_button = st.button("Go to Data Ingestion", key="nav_to_ingestion_button")
            if data_nav_button:
                st.session_state["active_tab"] = "Data Ingestion"
                st.rerun()
        else:
            st.success(f"Vector database contains {collection_info.get('points_count', '?')} data points")
            db_has_data = True
    except Exception as e:
        st.warning(f"Unable to check vector database status: {str(e)}")
        
    # Always show the question input form, even if the vector DB is empty
    
    # RAG query section
    question = st.text_input("Ask a question about your data", 
                           placeholder="What is the total revenue from customers in New York?")
    
    ask_button = st.button("Ask Question", type="primary", key="ask_question_button")
    if ask_button:
        if not question:
            st.error("Please enter a question first.")
        elif not db_has_data:
            st.error("The vector database is empty. Please add some data first before asking questions.")
            # Add a quick shortcut button with a unique key
            db_nav_button = st.button("Go to Database Tab", key="nav_to_db_button")
            if db_nav_button:
                st.session_state["active_tab"] = "Database"
                st.rerun()
        else:
            with st.spinner("Searching for information and generating answer..."):
                try:
                    # Execute RAG query
                    result = app.run_rag_query(question)
                    
                    # Display retrieved data
                    st.subheader("Retrieved Information")
                    
                    for i, item in enumerate(result["retrieved_data"]):
                        with st.expander(f"Result {i+1} (Score: {item['score']:.4f})", expanded=i==0):
                            st.text(item['payload']['text'])
                            if 'query' in item['payload']:
                                st.code(item['payload']['query'], language="sql")
                    
                    # Display response
                    st.markdown("---")
                    st.subheader("Answer")
                    st.markdown(f"**Q: {question}**")
                    st.markdown(f"**A: {result['response']}**")
                    
                except Exception as e:
                    st.error(f"Error processing question: {e}")

def display_ai_query_tab(app):
    """Display AI-Generated Query tab for creating SQL queries with AI assistance."""
    st.markdown("<h2 class='sub-header'>AI-Generated SQL Queries</h2>", unsafe_allow_html=True)
    
    if app is None:
        st.error("Application not initialized. Please check your configuration in the Settings tab.")
        return
    
    # Display connection status
    try:
        connection_status = app.sql_connector.test_connection()
        if connection_status:
            st.success("✅ Connected to database")
        else:
            st.error("❌ Database connection failed")
            return
    except Exception as e:
        st.error(f"❌ Database connection error: {e}")
        return
    
    # Check if there's a working LLM
    try:
        # Simple probe to check if LLM is available
        test_result = app.llm.get_completion("Hello", max_tokens=10)
        if "Error" in test_result:
            st.warning("⚠️ LLM connection issue. Consider switching to a different model in the sidebar.")
            st.info(test_result)
    except Exception as e:
        st.warning(f"⚠️ LLM connection issue: {e}")
    
    # Natural language query section
    st.subheader("Convert Natural Language to SQL")
    
    # Get schema information to help the LLM
    db_tables = None
    try:
        with st.spinner("Getting database schema..."):
            tables_df = app.sql_connector.get_tables()
            if not tables_df.empty:
                db_tables = tables_df
                st.success(f"Found {len(tables_df)} tables in the database")
                
                # Display compact table list
                table_list = ", ".join([f"{row['schema_name']}.{row['table_name']}" 
                                      if row['schema_name'] else row['table_name'] 
                                      for _, row in tables_df.iterrows()])
                st.info(f"Available tables: {table_list if table_list else 'None'}")
            else:
                st.info("No tables found in the database. Create sample data below.")
    except Exception as e:
        st.error(f"Error loading database schema: {e}")
    
    # Sample data creator
    with st.expander("No tables? Create sample data", expanded=db_tables is None or len(db_tables) == 0):
        st.write("You can create sample data for experimentation:")
        
        if st.button("Create Sample SQLite Tables", key="create_sample_button"):
            try:
                # Create sample SQLite tables
                sample_schema = """
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id INTEGER PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    email TEXT,
                    phone TEXT,
                    address TEXT,
                    city TEXT,
                    state TEXT,
                    zipcode TEXT,
                    registration_date DATE
                );

                CREATE TABLE IF NOT EXISTS products (
                    product_id INTEGER PRIMARY KEY,
                    product_name TEXT,
                    category TEXT,
                    price REAL,
                    stock INTEGER,
                    description TEXT
                );

                CREATE TABLE IF NOT EXISTS orders (
                    order_id INTEGER PRIMARY KEY,
                    customer_id INTEGER,
                    order_date DATE,
                    total_amount REAL,
                    status TEXT,
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                );

                CREATE TABLE IF NOT EXISTS order_items (
                    item_id INTEGER PRIMARY KEY,
                    order_id INTEGER,
                    product_id INTEGER,
                    quantity INTEGER,
                    price_per_unit REAL,
                    FOREIGN KEY (order_id) REFERENCES orders(order_id),
                    FOREIGN KEY (product_id) REFERENCES products(product_id)
                );
                """
                
                app.sql_connector.execute_script(sample_schema)
                
                # Insert sample data for customers
                customer_data = """
                INSERT INTO customers VALUES 
                (1, 'John', 'Smith', 'john.smith@example.com', '555-123-4567', '123 Main St', 'New York', 'NY', '10001', '2023-01-15'),
                (2, 'Jane', 'Doe', 'jane.doe@example.com', '555-987-6543', '456 Oak Ave', 'Los Angeles', 'CA', '90001', '2023-02-20'),
                (3, 'Bob', 'Johnson', 'bob.johnson@example.com', '555-567-8901', '789 Pine Rd', 'Chicago', 'IL', '60007', '2023-03-10'),
                (4, 'Sarah', 'Williams', 'sarah.williams@example.com', '555-345-6789', '321 Cedar Ln', 'Houston', 'TX', '77001', '2023-04-05'),
                (5, 'Michael', 'Brown', 'michael.brown@example.com', '555-678-1234', '654 Birch Blvd', 'Phoenix', 'AZ', '85001', '2023-05-12');
                """
                app.sql_connector.execute_script(customer_data)
                
                # Insert sample data for products
                product_data = """
                INSERT INTO products VALUES 
                (1, 'Laptop', 'Electronics', 999.99, 50, 'High-performance laptop with 16GB RAM'),
                (2, 'Smartphone', 'Electronics', 699.99, 100, 'Latest model with 128GB storage'),
                (3, 'Coffee Maker', 'Kitchen', 79.99, 30, 'Programmable coffee maker with timer'),
                (4, 'Running Shoes', 'Sports', 129.99, 75, 'Lightweight running shoes for all terrains'),
                (5, 'Desk Chair', 'Furniture', 249.99, 25, 'Ergonomic office chair with lumbar support');
                """
                app.sql_connector.execute_script(product_data)
                
                # Insert sample data for orders
                order_data = """
                INSERT INTO orders VALUES 
                (1, 1, '2023-05-01', 1699.98, 'Delivered'),
                (2, 2, '2023-05-15', 699.99, 'Shipped'),
                (3, 3, '2023-06-01', 329.97, 'Processing'),
                (4, 4, '2023-06-10', 129.99, 'Delivered'),
                (5, 5, '2023-06-20', 1249.95, 'Shipped'),
                (6, 1, '2023-07-05', 79.99, 'Delivered');
                """
                app.sql_connector.execute_script(order_data)
                
                # Insert sample data for order items
                order_items_data = """
                INSERT INTO order_items VALUES 
                (1, 1, 1, 1, 999.99),
                (2, 1, 2, 1, 699.99),
                (3, 2, 2, 1, 699.99),
                (4, 3, 3, 1, 79.99),
                (5, 3, 4, 2, 129.99),
                (6, 4, 4, 1, 129.99),
                (7, 5, 5, 5, 249.99),
                (8, 6, 3, 1, 79.99);
                """
                app.sql_connector.execute_script(order_items_data)
                
                st.success("Sample data created successfully! E-commerce database with customers, products, orders, and order items.")
                
                # Refresh the page to show the new tables
                st.rerun()
                
            except Exception as e:
                st.error(f"Error creating sample data: {e}")
    
    # Natural language query input
    nl_question = st.text_area(
        "Describe what information you want in plain English",
        height=100,
        placeholder="Find all orders placed by customers in New York state"
    )
    
    if st.button("Generate SQL Query", type="primary", key="generate_sql_btn"):
        if not nl_question:
            st.error("Please enter a question or description first.")
            return
        
        with st.spinner("Generating SQL query..."):
            try:
                # Get SQL query from LLM
                generated_sql = app.get_sql_for_question(nl_question)
                
                # Display the generated SQL
                st.subheader("Generated SQL Query")
                st.code(generated_sql, language="sql")
                
                # Save to session state for easy execution
                st.session_state["generated_sql"] = generated_sql
                
                # Add execution option
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button("Execute Query", key="execute_generated_sql"):
                        with st.spinner("Executing query..."):
                            try:
                                # Execute the query
                                df = app.sql_connector.execute_query(generated_sql)
                                
                                # Store results in vector database
                                app.store_sql_data_in_vector_db(
                                    generated_sql,
                                    {"content_type": "ai_generated_query", "question": nl_question}
                                )
                                st.success("Results stored in vector database")
                                
                                # Display results
                                st.subheader("Query Results")
                                st.dataframe(df, use_container_width=True)
                                
                                # Show basic statistics
                                st.subheader("Statistics")
                                col1, col2 = st.columns(2)
                                col1.metric("Rows", len(df))
                                col2.metric("Columns", len(df.columns))
                                
                            except Exception as e:
                                st.error(f"Error executing query: {e}")
                                st.info("You may need to modify the SQL query to match your database schema.")
                
                with col2:
                    if st.button("Edit Query", key="edit_generated_sql"):
                        st.session_state["editing_sql"] = True
                
                # Provide an editor if the user wants to edit the query
                if st.session_state.get("editing_sql", False):
                    edited_sql = st.text_area(
                        "Edit SQL Query", 
                        value=st.session_state.get("generated_sql", ""),
                        height=150,
                        key="edited_sql_area"
                    )
                    
                    if st.button("Execute Edited Query", key="execute_edited_sql"):
                        with st.spinner("Executing edited query..."):
                            try:
                                # Execute the edited query
                                df = app.sql_connector.execute_query(edited_sql)
                                
                                # Store results in vector database
                                app.store_sql_data_in_vector_db(
                                    edited_sql,
                                    {"content_type": "ai_generated_query", "question": nl_question}
                                )
                                st.success("Results stored in vector database")
                                
                                # Display results
                                st.subheader("Query Results")
                                st.dataframe(df, use_container_width=True)
                                
                                # Show basic statistics
                                st.subheader("Statistics")
                                col1, col2 = st.columns(2)
                                col1.metric("Rows", len(df))
                                col2.metric("Columns", len(df.columns))
                                
                            except Exception as e:
                                st.error(f"Error executing query: {e}")
                
            except Exception as e:
                st.error(f"Error generating SQL query: {e}")
                st.info("The AI might not have enough context about your database schema. Try a more specific question or provide more details about your database tables.")
    
    # Examples section
    with st.expander("Example Questions"):
        examples = [
            "Show me the total number of orders per customer",
            "Find the top 3 most expensive products",
            "Which customers spent the most money?",
            "How many orders were placed in June 2023?",
            "What is the average order value?",
            "List all products with less than 30 items in stock",
            "Find customers who have placed more than one order"
        ]
        
        for i, example in enumerate(examples):
            if st.button(example, key=f"example_{i}"):
                # Set the example as the question and trigger generation
                st.session_state["nl_question"] = example
                st.rerun()

def display_settings_tab(app):
    """Display settings and configuration tab."""
    st.markdown("<h2 class='sub-header'>Settings & Configuration</h2>", unsafe_allow_html=True)
    
    # Configuration editor
    st.subheader("Configuration File")
    
    config_path = "config.ini"
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config_content = f.read()
        
        new_config = st.text_area("Edit configuration", value=config_content, height=300)
        
        if st.button("Save Configuration"):
            try:
                # Write to file
                with open(config_path, "w") as f:
                    f.write(new_config)
                st.success("Configuration saved successfully")
                st.session_state["reload_app"] = True
                
                # Prompt to restart the application
                st.warning("Please restart the application for changes to take effect")
                if st.button("Restart Now"):
                    st.rerun()
            except Exception as e:
                st.error(f"Error saving configuration: {e}")
    else:
        st.warning(f"Configuration file not found. Creating a default configuration file.")
        # Create basic config
        config = configparser.ConfigParser()
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
        
        try:
            with open(config_path, "w") as f:
                config.write(f)
            st.success("Default configuration created. Please reload the page.")
            
            # Display the content
            with open(config_path, "r") as f:
                config_content = f.read()
            st.text_area("Configuration", value=config_content, height=300)
            
            if st.button("Reload Page"):
                st.rerun()
        except Exception as e:
            st.error(f"Error creating default configuration: {e}")
    
    # System information
    st.markdown("---")
    st.subheader("System Information")
    
    # Check Ollama status
    try:
        import requests
        ollama_status = "Unknown"
        ollama_models = []
        
        try:
            response = requests.get('http://localhost:11434/api/tags', timeout=3)
            if response.status_code == 200:
                ollama_models = [m['name'] for m in response.json().get('models', [])]
                ollama_status = "Running"
            else:
                ollama_status = "Not responding"
        except:
            ollama_status = "Not running"
        
        # Check Qdrant status
        qdrant_status = "Unknown"
        try:
            # Try health endpoint first
            response = requests.get('http://localhost:6333/healthz', timeout=3)
            if response.status_code == 200:
                qdrant_status = "Running"
            else:
                # Try dashboard as fallback
                response = requests.get('http://localhost:6333/dashboard/', timeout=3)
                if response.status_code == 200:
                    qdrant_status = "Running"
                else:
                    qdrant_status = "Not responding"
        except:
            qdrant_status = "Not running"
        
        # Display status
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Ollama Status", ollama_status)
            if ollama_models:
                st.write("Available models:")
                for model in ollama_models:
                    st.code(model)
            
            if ollama_status != "Running":
                st.warning("Ollama is not running. To start it, run:")
                st.code("ollama serve")
        
        with col2:
            st.metric("Qdrant Status", qdrant_status)
            
            # Show connection info if app is available
            if app is not None:
                try:
                    st.write("Vector DB URL:")
                    st.code(app.config.get("system", "vector_db_url"))
                    st.write("Collection name:")
                    st.code(app.config.get("system", "vector_db_collection"))
                except:
                    st.write("Vector DB URL: http://localhost:6333")
                    st.write("Collection name: sql_data")
            else:
                st.write("Vector DB URL: http://localhost:6333")
                st.write("Collection name: sql_data")
            
            if qdrant_status != "Running":
                st.warning("Qdrant is not running. To start it with Docker, run:")
                st.code("docker run -p 6333:6333 -p 6334:6334 -v $(pwd)/qdrant_data:/qdrant/storage qdrant/qdrant")
        
        # System information
        st.markdown("---")
        import platform
        import torch
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**System:**")
            st.code(f"OS: {platform.system()} {platform.version()}")
            st.code(f"Python: {sys.version.split()[0]}")
        
        with col2:
            st.write("**GPU Support:**")
            has_cuda = torch.cuda.is_available()
            if has_cuda:
                st.code(f"CUDA: Available ({torch.cuda.get_device_name(0)})")
            else:
                st.code("CUDA: Not available")
    
    except Exception as e:
        st.error(f"Error retrieving system information: {e}")

def display_ingestion_tab(app):
    """Display data ingestion tab for populating the vector database."""
    st.markdown("<h2 class='sub-header'>SQL Data Ingestion</h2>", unsafe_allow_html=True)
    
    st.write("This tab helps you load SQL database information into the vector database for RAG queries.")
    
    if app is None:
        st.error("Application not initialized. Please check your configuration in the Settings tab.")
        return
    
    # Check if services are running
    try:
        import requests
        ollama_status = False
        try:
            response = requests.get('http://localhost:11434/api/tags', timeout=3)
            ollama_status = response.status_code == 200
        except:
            ollama_status = False
        
        qdrant_status = False
        try:
            response = requests.get('http://localhost:6333/healthz', timeout=3)
            qdrant_status = response.status_code == 200
        except:
            try:
                response = requests.get('http://localhost:6333/dashboard/', timeout=3)
                qdrant_status = response.status_code == 200
            except:
                qdrant_status = False
    except:
        ollama_status = False
        qdrant_status = False
    
    if not ollama_status or not qdrant_status:
        st.warning("Some required services are not running:")
        if not ollama_status:
            st.warning("• Ollama is not running. Please start it before continuing.")
        if not qdrant_status:
            st.warning("• Qdrant is not running. Please start it before continuing.")
            
        return
    
    # Check SQL connection
    try:
        sql_ok = app.sql_connector.test_connection()
        if not sql_ok:
            st.error("SQL connection failed. Please check your database settings in the Configuration panel.")
            return
        else:
            st.success("✅ Connected to SQL Server")
    except Exception as e:
        st.error(f"Error connecting to SQL database: {e}")
        return
    
    # Data ingestion options
    st.markdown("---")
    st.subheader("Select data to ingest")
    
    col1, col2 = st.columns(2)
    with col1:
        ingest_tables = st.checkbox("Database Schema (Tables & Columns)", value=True)
    with col2:
        ingest_samples = st.checkbox("Sample Data from Tables (10 rows each)", value=True)
    
    # Custom SQL query option
    st.markdown("---")
    st.subheader("Custom SQL Query")
    custom_query = st.text_area("Enter a custom SQL query to execute and store in the vector database:", 
                               height=100,
                               placeholder="SELECT * FROM Customers WHERE Region = 'North'")
    
    # Progress info
    progress_placeholder = st.empty()
    results_placeholder = st.empty()
    
    # Run ingestion
    if st.button("Start Data Ingestion", type="primary"):
        if not (ingest_tables or ingest_samples or custom_query):
            st.error("Please select at least one data source to ingest.")
            return
        
        progress_bar = progress_placeholder.progress(0)
        status_text = results_placeholder.empty()
        
        try:
            if ingest_tables:
                status_text.text("Getting database schema information...")
                progress_bar.progress(10)
                
                # Get all tables
                tables_df = app.sql_connector.get_tables()
                if tables_df.empty:
                    status_text.warning("No tables found in database")
                else:
                    progress_bar.progress(20)
                    # Store table list
                    status_text.text("Storing table list...")
                    app.store_sql_data_in_vector_db(
                        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES",
                        {"content_type": "table_list"}
                    )
                    
                    progress_bar.progress(30)
                    # For each table, get its columns
                    total_tables = len(tables_df)
                    for i, (_, row) in enumerate(tables_df.iterrows()):
                        schema = row['schema_name']
                        table = row['table_name']
                        table_pct = 30 + (i / total_tables * 20)
                        progress_bar.progress(int(table_pct))
                        status_text.text(f"Getting columns for {schema}.{table} ({i+1}/{total_tables})")
                        
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
                    
                    status_text.text("Successfully stored database schema information")
            
            if ingest_samples:
                progress_bar.progress(50)
                status_text.text("Getting sample data from tables...")
                
                tables_df = app.sql_connector.get_tables()
                if tables_df.empty:
                    status_text.warning("No tables found in database")
                else:
                    base_tables = tables_df[tables_df['table_type'] == 'BASE TABLE']
                    total_tables = len(base_tables)
                    
                    for i, (_, row) in enumerate(base_tables.iterrows()):
                        schema = row['schema_name']
                        table = row['table_name']
                        sample_pct = 50 + (i / total_tables * 30)
                        progress_bar.progress(int(sample_pct))
                        status_text.text(f"Getting sample data from {schema}.{table} ({i+1}/{total_tables})")
                        
                        try:
                            # Get top 10 rows as sample
                            app.store_sql_data_in_vector_db(
                                f"SELECT TOP 10 * FROM [{schema}].[{table}]",
                                {"content_type": "table_sample", "table": f"{schema}.{table}"}
                            )
                        except Exception as e:
                            status_text.warning(f"Could not get sample from {schema}.{table}: {str(e)}")
                    
                    status_text.text("Successfully stored sample data from tables")
            
            if custom_query:
                progress_bar.progress(80)
                status_text.text(f"Executing custom query: {custom_query}")
                
                try:
                    app.store_sql_data_in_vector_db(
                        custom_query,
                        {"content_type": "custom_query", "query": custom_query}
                    )
                    status_text.text("Successfully stored custom query results")
                except Exception as e:
                    status_text.error(f"Error executing custom query: {str(e)}")
            
            # Get vector DB stats
            progress_bar.progress(90)
            status_text.text("Getting vector database statistics...")
            
            try:
                info = app.vector_db.get_collection_info()
                progress_bar.progress(100)
                status_text.success(f"Data ingestion complete! Vector database now contains {info.get('points_count', '?')} data points.")
            except Exception as e:
                progress_bar.progress(100)
                status_text.warning(f"Data ingestion complete, but could not get vector database stats: {str(e)}")
                
        except Exception as e:
            progress_bar.progress(100)
            status_text.error(f"Error during data ingestion: {str(e)}")

def start_web(config):
    """Start the Streamlit web interface."""
    # Store the config in session state
    if "config" not in st.session_state:
        st.session_state["config"] = config
    
    # Check if we need to reload the app
    if st.session_state.get("reload_app", False):
        st.session_state["reload_app"] = False
        st.session_state["config"] = config
    
    # Setup page
    setup_page()
    
    # Main header
    st.markdown("<h1 class='main-header'>SQL RAG Web Interface</h1>", unsafe_allow_html=True)
    st.markdown(
        "Query SQL databases and ask natural language questions about your data using "
        "retrieval-augmented generation."
    )
    
    # Setup sidebar
    setup_sidebar()
    
    # Check if we should activate a specific tab
    active_tab = st.session_state.get("active_tab", "Database")
    
    # Initialize application
    try:
        @st.cache_resource(ttl=3600)
        def get_app_instance():
            try:
                app = SQLRagApplication(config_path="config.ini")
                app.ensure_collection_exists()
                return app
            except Exception as e:
                st.error(f"Error initializing application: {e}")
                st.info("Please check your configuration and make sure all dependencies are installed.")
                return None
        
        app = get_app_instance()
        
        # If app initialization failed, only show settings tab
        if app is None:
            st.error("Application failed to initialize. Please configure your settings.")
            display_settings_tab(None)
            return
        
        # Create tabs and display all content
        # This approach displays all tabs properly and lets Streamlit handle the tab switching
        tab_db, tab_ingestion, tab_rag, tab_ai_query, tab_settings = st.tabs([
            "Database", "Data Ingestion", "Ask Questions", "AI-Generated Query", "Settings"
        ])
        
        # Display content for all tabs
        with tab_db:
            display_database_tab(app)
            
        with tab_ingestion:
            display_ingestion_tab(app)
            
        with tab_rag:
            display_rag_tab(app)
            
        with tab_ai_query:
            display_ai_query_tab(app)
            
        with tab_settings:
            display_settings_tab(app)
            
        # Set the active tab based on session state
        active_tab = st.session_state.get("active_tab", "Database")
        tab_index = {"Database": 0, "Data Ingestion": 1, "Ask Questions": 2, "AI-Generated Query": 3, "Settings": 4}
        if active_tab in tab_index:
            # Note: This is a JavaScript hack to switch tabs
            # It doesn't actually switch tabs programmatically, but provides a visual indicator
            st.markdown(f"""
            <script>
                document.querySelectorAll('.stTabs button[role="tab"]')[{tab_index[active_tab]}].click();
            </script>
            """, unsafe_allow_html=True)
    
    except Exception as e:
        st.error(f"Error initializing application: {e}")
        st.info("Please check your configuration and make sure all dependencies are installed.")
        
        if "database connection" in str(e).lower():
            st.warning("Database connection failed. Please check your connection settings in the sidebar.")