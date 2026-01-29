"""
LLM interface for various model providers.

This module provides interfaces for interacting with different LLM providers,
including local models (Ollama) and cloud providers (OpenAI, etc).
"""

import logging
import os
import json
import requests
import time
import configparser
from typing import Dict, Any, Optional, List, Union, Literal
from abc import ABC, abstractmethod

logger = logging.getLogger('sql_rag.llm')

class LLMInterface(ABC):
    """Abstract base class for LLM providers"""
    
    @abstractmethod
    def check_api_available(self) -> bool:
        """Check if the API is available"""
        pass
    
    @abstractmethod
    def list_available_models(self) -> List[str]:
        """Get list of available models"""
        pass
    
    @abstractmethod
    def get_completion(self, prompt: str, temperature: Optional[float] = None,
                      max_tokens: Optional[int] = None) -> str:
        """Get a completion from the model"""
        pass
    
    def process_rag_query(self, query: str, context: List[Dict[str, Any]]) -> str:
        """Process a RAG query with context from vector search"""
        # Format context into prompt
        context_str = "\n".join([
            f"[{i+1}] {item['payload']['text']}" 
            for i, item in enumerate(context)
        ])
        
        # Create a prompt that instructs the model to use the retrieved context
        prompt = f"""You are a helpful AI assistant that answers questions about SQL data. 
Use ONLY the following retrieved information to answer the question. 
If you don't know the answer based on the provided information, say "I don't have enough information to answer that question."

Retrieved information:
{context_str}

Question: {query}

Answer:"""
        
        # Get completion from model
        return self.get_completion(prompt)

class LocalLLM(LLMInterface):
    """Interface to Ollama LLM server"""
    
    def __init__(self, config):
        """Initialize with configuration"""
        self.config = config
        self.base_url = config["models"].get("llm_api_url", "http://localhost:11434/api")
        self.model_name = config["models"].get("llm_model", "mistral:7b-instruct-v0.2")
        self.temperature = float(config["system"].get("temperature", "0.2"))
        self.max_tokens = int(config["system"].get("max_token_limit", "1000"))
    
    def check_api_available(self) -> bool:
        """Check if Ollama is available"""
        try:
            # Ollama provides a /api/tags endpoint to list available models
            # Handle both http://host:port/api and http://host:port formats
            base = self.base_url.rstrip('/')
            if base.endswith('/api'):
                tags_url = f"{base}/tags"
            else:
                tags_url = f"{base}/api/tags"
            response = requests.get(tags_url, timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Ollama API not available: {e}")
            return False
    
    def list_available_models(self) -> List[str]:
        """Get list of available models from Ollama"""
        try:
            base = self.base_url.rstrip('/')
            if base.endswith('/api'):
                tags_url = f"{base}/tags"
            else:
                tags_url = f"{base}/api/tags"
            response = requests.get(tags_url, timeout=5)
            if response.status_code == 200:
                models = response.json().get("models", [])
                return [model["name"] for model in models]
            return []
        except Exception as e:
            logger.error(f"Error getting available models: {e}")
            return []
    
    def get_completion(self, prompt: str, temperature: Optional[float] = None, 
                       max_tokens: Optional[int] = None) -> str:
        """Get a completion from Ollama"""
        if not self.check_api_available():
            return "Error: Ollama is not available. Please make sure Ollama is running with the command 'ollama serve'."
        
        # Use provided values or defaults from config
        temperature = temperature if temperature is not None else self.temperature
        max_tokens = max_tokens if max_tokens is not None else self.max_tokens
        
        try:
            # Ollama uses /api/generate endpoint for completions
            headers = {"Content-Type": "application/json"}
            payload = {
                "model": self.model_name,
                "prompt": prompt,
                "temperature": temperature,
                "num_predict": max_tokens,
                "stream": False
            }
            
            response = requests.post(
                f"{self.base_url}/generate",
                headers=headers,
                json=payload,
                timeout=60
            )
            
            if response.status_code != 200:
                logger.error(f"Error from Ollama API: {response.status_code} - {response.text}")
                return f"Error: Ollama API returned status code {response.status_code}"
            
            result = response.json()
            return result.get("response", "")
            
        except Exception as e:
            logger.error(f"Error calling Ollama API: {e}")
            return f"Error: {str(e)}"
    
    def provide_llm_options(self) -> str:
        """Provide information about setting up Ollama"""
        options = """
To use this system with Ollama, follow these steps:

1. Install Ollama
   - Download from https://ollama.ai/
   - Follow the installation instructions for your OS

2. Run Ollama
   - Start Ollama with the command: ollama serve

3. Download a model
   - Open a new terminal/command prompt
   - Run: ollama pull mistral:7b-instruct-v0.2
   
4. Verify installation
   - Run: ollama list
   - You should see 'mistral:7b-instruct-v0.2' in the list

Once Ollama is running with a model installed, restart this application.
"""
        return options

class OpenAILLM(LLMInterface):
    """Interface to OpenAI API"""
    
    def __init__(self, config):
        """Initialize with configuration"""
        self.config = config
        self.api_key = config["openai"].get("api_key", os.environ.get("OPENAI_API_KEY", ""))
        self.model_name = config["openai"].get("model", "gpt-3.5-turbo")
        self.temperature = float(config["system"].get("temperature", "0.2"))
        self.max_tokens = int(config["system"].get("max_token_limit", "1000"))
        
        # Rate limiting
        self.request_delay = float(config["openai"].get("request_delay", "0.5"))
        self.last_request_time = 0
    
    def check_api_available(self) -> bool:
        """Check if OpenAI API is available"""
        if not self.api_key:
            logger.warning("OpenAI API key not set")
            return False
        
        try:
            import openai
            openai.api_key = self.api_key
            response = openai.models.list()
            return True
        except Exception as e:
            logger.warning(f"OpenAI API not available: {e}")
            return False
    
    def list_available_models(self) -> List[str]:
        """Get list of available models from OpenAI"""
        try:
            import openai
            openai.api_key = self.api_key
            models = openai.models.list()
            
            # Handle potential missing attributes
            if not hasattr(models, 'data'):
                logger.warning("No model data received from OpenAI API")
                return ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo"]
                
            # Safely filter models
            result = []
            for model in models.data:
                if hasattr(model, 'id') and isinstance(model.id, str) and "gpt" in model.id.lower():
                    result.append(model.id)
            
            return result if result else ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo"]
        except Exception as e:
            logger.error(f"Error getting available models: {e}")
            return ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo"]  # Return default options
    
    def get_completion(self, prompt: str, temperature: Optional[float] = None, 
                       max_tokens: Optional[int] = None) -> str:
        """Get a completion from OpenAI"""
        if not self.api_key:
            return "Error: OpenAI API key not set. Please configure your API key in the settings."
        
        # Apply rate limiting
        elapsed = time.time() - self.last_request_time
        if elapsed < self.request_delay:
            time.sleep(self.request_delay - elapsed)
        
        # Use provided values or defaults from config
        temperature = temperature if temperature is not None else self.temperature
        max_tokens = max_tokens if max_tokens is not None else self.max_tokens
        
        try:
            import openai
            openai.api_key = self.api_key
            
            response = openai.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=max_tokens
            )
            
            self.last_request_time = time.time()
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error calling OpenAI API: {e}")
            return f"Error: {str(e)}"

class AnthropicLLM(LLMInterface):
    """Interface to Anthropic Claude API"""

    def __init__(self, config):
        """Initialize with configuration"""
        self.config = config
        self.api_key = config["anthropic"].get("api_key", os.environ.get("ANTHROPIC_API_KEY", ""))
        self.model_name = config["anthropic"].get("model", "claude-3-sonnet-20240229")
        self.temperature = float(config["system"].get("temperature", "0.2"))
        self.max_tokens = int(config["system"].get("max_token_limit", "1000"))

        # Rate limiting
        self.request_delay = float(config["anthropic"].get("request_delay", "0.5"))
        self.last_request_time = 0

    def check_api_available(self) -> bool:
        """Check if Anthropic API is available"""
        if not self.api_key:
            logger.warning("Anthropic API key not set")
            return False

        try:
            import anthropic
            client = anthropic.Anthropic(api_key=self.api_key)
            # This is a lightweight call to check API validity
            client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=1,
                messages=[{"role": "user", "content": "Hi"}],
                temperature=0
            )
            return True
        except Exception as e:
            logger.warning(f"Anthropic API not available: {e}")
            return False

    def list_available_models(self) -> List[str]:
        """Get list of available models from Anthropic"""
        # Anthropic doesn't have a model listing endpoint, so we'll return the known models
        return [
            "claude-3-5-sonnet-20241022",
            "claude-3-5-haiku-20241022",
            "claude-3-opus-20240229",
            "claude-3-sonnet-20240229",
            "claude-3-haiku-20240307",
        ]

    def get_completion(self, prompt: str, temperature: Optional[float] = None,
                       max_tokens: Optional[int] = None) -> str:
        """Get a completion from Anthropic"""
        if not self.api_key:
            return "Error: Anthropic API key not set. Please configure your API key in the settings."

        # Apply rate limiting
        elapsed = time.time() - self.last_request_time
        if elapsed < self.request_delay:
            time.sleep(self.request_delay - elapsed)

        # Use provided values or defaults from config
        temperature = temperature if temperature is not None else self.temperature
        max_tokens = max_tokens if max_tokens is not None else self.max_tokens

        try:
            import anthropic
            client = anthropic.Anthropic(api_key=self.api_key)

            response = client.messages.create(
                model=self.model_name,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature
            )

            self.last_request_time = time.time()
            return response.content[0].text

        except Exception as e:
            logger.error(f"Error calling Anthropic API: {e}")
            return f"Error: {str(e)}"


class GeminiLLM(LLMInterface):
    """Interface to Google Gemini API"""

    def __init__(self, config):
        """Initialize with configuration"""
        self.config = config
        self.api_key = config["gemini"].get("api_key", os.environ.get("GOOGLE_API_KEY", ""))
        self.model_name = config["gemini"].get("model", "gemini-1.5-flash")
        self.temperature = float(config["system"].get("temperature", "0.2"))
        self.max_tokens = int(config["system"].get("max_token_limit", "1000"))

        # Rate limiting
        self.request_delay = float(config["gemini"].get("request_delay", "0.5"))
        self.last_request_time = 0

    def check_api_available(self) -> bool:
        """Check if Gemini API is available"""
        if not self.api_key:
            logger.warning("Gemini API key not set")
            return False

        try:
            import google.generativeai as genai
            genai.configure(api_key=self.api_key)
            # List models to verify API key works
            models = genai.list_models()
            return True
        except Exception as e:
            logger.warning(f"Gemini API not available: {e}")
            return False

    def list_available_models(self) -> List[str]:
        """Get list of available models from Gemini"""
        try:
            import google.generativeai as genai
            genai.configure(api_key=self.api_key)
            models = genai.list_models()
            # Filter for generative models
            return [m.name.replace("models/", "") for m in models
                    if "generateContent" in [method.name for method in m.supported_generation_methods]]
        except Exception as e:
            logger.error(f"Error getting available Gemini models: {e}")
            # Return default options
            return [
                "gemini-1.5-pro",
                "gemini-1.5-flash",
                "gemini-1.5-flash-8b",
                "gemini-1.0-pro",
            ]

    def get_completion(self, prompt: str, temperature: Optional[float] = None,
                       max_tokens: Optional[int] = None) -> str:
        """Get a completion from Gemini"""
        if not self.api_key:
            return "Error: Gemini API key not set. Please configure your API key in the settings."

        # Apply rate limiting
        elapsed = time.time() - self.last_request_time
        if elapsed < self.request_delay:
            time.sleep(self.request_delay - elapsed)

        # Use provided values or defaults from config
        temperature = temperature if temperature is not None else self.temperature
        max_tokens = max_tokens if max_tokens is not None else self.max_tokens

        try:
            import google.generativeai as genai
            genai.configure(api_key=self.api_key)

            # Configure generation settings
            generation_config = genai.GenerationConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
            )

            model = genai.GenerativeModel(self.model_name, generation_config=generation_config)
            response = model.generate_content(prompt)

            self.last_request_time = time.time()
            return response.text

        except Exception as e:
            logger.error(f"Error calling Gemini API: {e}")
            return f"Error: {str(e)}"


class GroqLLM(LLMInterface):
    """Interface to Groq API (fast inference for open models)"""

    def __init__(self, config):
        """Initialize with configuration"""
        self.config = config
        self.api_key = config["groq"].get("api_key", os.environ.get("GROQ_API_KEY", ""))
        self.model_name = config["groq"].get("model", "llama-3.1-70b-versatile")
        self.temperature = float(config["system"].get("temperature", "0.2"))
        self.max_tokens = int(config["system"].get("max_token_limit", "1000"))

        # Rate limiting
        self.request_delay = float(config["groq"].get("request_delay", "0.1"))
        self.last_request_time = 0

    def check_api_available(self) -> bool:
        """Check if Groq API is available"""
        if not self.api_key:
            logger.warning("Groq API key not set")
            return False

        try:
            from groq import Groq
            client = Groq(api_key=self.api_key)
            # List models to verify API key works
            client.models.list()
            return True
        except Exception as e:
            logger.warning(f"Groq API not available: {e}")
            return False

    def list_available_models(self) -> List[str]:
        """Get list of available models from Groq"""
        try:
            from groq import Groq
            client = Groq(api_key=self.api_key)
            models = client.models.list()
            return [m.id for m in models.data]
        except Exception as e:
            logger.error(f"Error getting available Groq models: {e}")
            # Return default options
            return [
                "llama-3.3-70b-versatile",
                "llama-3.1-70b-versatile",
                "llama-3.1-8b-instant",
                "mixtral-8x7b-32768",
                "gemma2-9b-it",
            ]

    def get_completion(self, prompt: str, temperature: Optional[float] = None,
                       max_tokens: Optional[int] = None) -> str:
        """Get a completion from Groq"""
        if not self.api_key:
            return "Error: Groq API key not set. Please configure your API key in the settings."

        # Apply rate limiting
        elapsed = time.time() - self.last_request_time
        if elapsed < self.request_delay:
            time.sleep(self.request_delay - elapsed)

        # Use provided values or defaults from config
        temperature = temperature if temperature is not None else self.temperature
        max_tokens = max_tokens if max_tokens is not None else self.max_tokens

        try:
            from groq import Groq
            client = Groq(api_key=self.api_key)

            response = client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=max_tokens,
            )

            self.last_request_time = time.time()
            return response.choices[0].message.content

        except Exception as e:
            logger.error(f"Error calling Groq API: {e}")
            return f"Error: {str(e)}"


def create_llm_instance(config) -> LLMInterface:
    """Factory function to create the appropriate LLM instance"""
    # Default to local if config is invalid or missing
    if not config or not isinstance(config, configparser.ConfigParser):
        logger.warning("Invalid config provided to LLM factory. Using local LLM.")
        config = configparser.ConfigParser()
        config["models"] = {"provider": "local", "llm_model": "mistral:7b-instruct-v0.2"}
        config["system"] = {"temperature": "0.2", "max_token_limit": "1000"}
        return LocalLLM(config)
    
    # Get provider - safely extract with fallback to local
    try:
        provider = config.get("models", "provider", fallback="local")
    except Exception as e:
        logger.warning(f"Error getting LLM provider from config: {e}. Using local LLM.")
        provider = "local"
    
    # Create appropriate LLM instance based on provider
    if provider == "openai":
        # Verify OpenAI is installed
        try:
            import openai
            return OpenAILLM(config)
        except ImportError:
            logger.error("OpenAI package not installed. Please install it with: pip install openai")
            logger.info("Falling back to local LLM")
            return LocalLLM(config)
        except Exception as e:
            logger.error(f"Error creating OpenAI LLM: {e}")
            logger.info("Falling back to local LLM")
            return LocalLLM(config)
    elif provider == "anthropic":
        # Verify Anthropic is installed
        try:
            import anthropic
            return AnthropicLLM(config)
        except ImportError:
            logger.error("Anthropic package not installed. Please install it with: pip install anthropic")
            logger.info("Falling back to local LLM")
            return LocalLLM(config)
        except Exception as e:
            logger.error(f"Error creating Anthropic LLM: {e}")
            logger.info("Falling back to local LLM")
            return LocalLLM(config)
    elif provider == "gemini":
        # Verify Google Generative AI is installed
        try:
            import google.generativeai
            return GeminiLLM(config)
        except ImportError:
            logger.error("Google Generative AI package not installed. Please install it with: pip install google-generativeai")
            logger.info("Falling back to local LLM")
            return LocalLLM(config)
        except Exception as e:
            logger.error(f"Error creating Gemini LLM: {e}")
            logger.info("Falling back to local LLM")
            return LocalLLM(config)
    elif provider == "groq":
        # Verify Groq is installed
        try:
            import groq
            return GroqLLM(config)
        except ImportError:
            logger.error("Groq package not installed. Please install it with: pip install groq")
            logger.info("Falling back to local LLM")
            return LocalLLM(config)
        except Exception as e:
            logger.error(f"Error creating Groq LLM: {e}")
            logger.info("Falling back to local LLM")
            return LocalLLM(config)
    else:
        # Default to local (Ollama)
        try:
            return LocalLLM(config)
        except Exception as e:
            logger.error(f"Error creating Local LLM: {e}")
            # Create a minimal working config as last resort
            emergency_config = configparser.ConfigParser()
            emergency_config["models"] = {"llm_model": "mistral:7b-instruct-v0.2"}
            emergency_config["system"] = {"temperature": "0.2", "max_token_limit": "1000"}
            return LocalLLM(emergency_config)