#!/bin/bash

echo
echo "==================================="
echo " Starting SQL-RAG Local System"
echo "==================================="
echo

# Activate virtual environment
source venv/bin/activate
if [ $? -ne 0 ]; then
    echo "Failed to activate virtual environment."
    echo "Please run setup.sh first."
    exit 1
fi

# Check if Ollama is running
python -c "
import requests
try:
    requests.get('http://localhost:11434/api/tags', timeout=2)
    exit(0)
except:
    exit(1)
"

if [ $? -ne 0 ]; then
    echo -e "\033[33mWARNING: Ollama does not appear to be running.\033[0m"
    echo
    echo "Please make sure Ollama is installed and running:"
    echo "1. Install from https://ollama.ai/"
    echo "2. Run 'ollama serve' in a separate terminal"
    echo "3. Run 'ollama pull mistral:7b-instruct-v0.2' to download the model"
    echo
    echo "The application will continue, but some features will be limited."
    echo
    read -p "Press Enter to continue..."
fi

# Check if Qdrant is running
python -c "
import requests
try:
    requests.get('http://localhost:6333/dashboard/', timeout=2)
    exit(0)
except:
    exit(1)
"

if [ $? -ne 0 ]; then
    echo -e "\033[33mWARNING: Qdrant does not appear to be running.\033[0m"
    echo
    echo "The system will attempt to use Qdrant in memory mode."
    echo "This means your vector database will be lost when the application closes."
    echo
    echo "For persistent storage, please install Qdrant:"
    echo "1. Download from https://qdrant.tech/documentation/install/"
    echo "2. Or use Docker: docker run -p 6333:6333 qdrant/qdrant"
    echo
    echo "The application will continue but data will not persist between sessions."
    echo
    read -p "Press Enter to continue..."
fi

# Parse command line arguments
WEB_MODE=true
PORT=8501

# Check for mode parameter
if [ "$1" == "cli" ]; then
    echo "Starting command-line interface..."
    python -m sql_rag.main --mode cli
    exit 0
elif [ "$1" == "web" ]; then
    WEB_MODE=true
    shift
elif [ "$1" == "--port" ] && [ -n "$2" ]; then
    PORT="$2"
    shift 2
fi

# No specific mode requested or web mode explicitly requested
if [ "$WEB_MODE" = true ]; then
    echo "Starting web interface on port $PORT..."
    # Make sure streamlit_app.py is executable
    chmod +x streamlit_app.py
    streamlit run streamlit_app.py --server.port=$PORT
else
    # This shouldn't happen with the current logic, but kept for safety
    echo "Starting web interface (default)..."
    chmod +x streamlit_app.py
    streamlit run streamlit_app.py --server.port=8501
fi

echo
echo "Application closed."
