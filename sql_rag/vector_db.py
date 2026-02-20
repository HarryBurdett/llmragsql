"""
Vector database interface for ChromaDB.

This module provides functionality for storing and retrieving vectors
in a ChromaDB vector database, which is used for similarity search operations.
"""

import logging
import os
from typing import List, Dict, Any, Optional
import uuid

import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer

logger = logging.getLogger('sql_rag.vector_db')


class VectorDB:
    """Interface to ChromaDB vector database"""

    def __init__(self, config, persist_dir: str = None):
        """Initialize with configuration.

        Args:
            config: ConfigParser with system/model settings
            persist_dir: Override for ChromaDB persist directory (per-company).
                         If None, uses config or per-company auto-detection.
        """
        self.config = config
        self.embedding_model_name = config["models"].get("embedding_model", "all-MiniLM-L6-v2")
        self.collection_name = config["system"].get("vector_db_collection", "sql_data")

        # ChromaDB persistence path (per-company > explicit param > config > default)
        if persist_dir:
            self.persist_directory = persist_dir
        else:
            self.persist_directory = self._resolve_persist_dir(config)

        # Initialize embedding model
        self._init_embedding_model()

        # Initialize ChromaDB client
        self._init_chroma_client()

    @staticmethod
    def _resolve_persist_dir(config) -> str:
        """Resolve ChromaDB persist directory, preferring per-company path."""
        try:
            from sql_rag.company_data import get_current_company_id, get_company_chroma_dir
            company_id = get_current_company_id()
            if company_id is not None:
                return str(get_company_chroma_dir(company_id))
        except ImportError:
            pass
        return config["system"].get("chroma_persist_dir", "./chroma_db")

    def _init_embedding_model(self):
        """Initialize the sentence transformer model"""
        try:
            logger.info(f"Loading embedding model: {self.embedding_model_name}")
            self.embedding_model = SentenceTransformer(self.embedding_model_name)
            self.embedding_dimension = self.embedding_model.get_sentence_embedding_dimension()
            logger.info(f"Embedding model loaded, dimension: {self.embedding_dimension}")
        except Exception as e:
            logger.error(f"Error loading embedding model: {e}")
            raise

    def _init_chroma_client(self):
        """Initialize the ChromaDB client"""
        try:
            # Create persist directory if it doesn't exist
            os.makedirs(self.persist_directory, exist_ok=True)

            logger.info(f"Initializing ChromaDB with persistence at {self.persist_directory}")

            # Initialize ChromaDB with persistent storage
            self.client = chromadb.PersistentClient(path=self.persist_directory)

            # Get or create the collection
            self.collection = self.client.get_or_create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"}  # Use cosine similarity
            )

            logger.info(f"ChromaDB initialized with collection: {self.collection_name}")
        except Exception as e:
            logger.error(f"Error initializing ChromaDB client: {e}")
            raise

    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for given text"""
        embedding = self.embedding_model.encode(text)
        return embedding.tolist()

    def store_vectors(self, texts: List[str], metadata: List[Dict[str, Any]] = None) -> bool:
        """
        Store multiple vectors in the database

        Args:
            texts: List of texts to embed and store
            metadata: List of metadata dictionaries to store with the vectors

        Returns:
            True if successful
        """
        if not texts:
            logger.warning("No texts provided to store_vectors")
            return False

        if metadata is None:
            metadata = [{} for _ in texts]

        # Ensure metadata is a list of the same length as texts
        if len(metadata) != len(texts):
            logger.warning(f"Metadata length ({len(metadata)}) does not match texts length ({len(texts)})")
            metadata = metadata[:len(texts)] if len(metadata) > len(texts) else metadata + [{} for _ in range(len(texts) - len(metadata))]

        try:
            # Generate embeddings for all texts
            embeddings = [self.generate_embedding(text) for text in texts]

            # Generate unique IDs for each document
            ids = [str(uuid.uuid4()) for _ in texts]

            # Clean metadata - ChromaDB only accepts str, int, float, bool values
            cleaned_metadata = []
            for meta in metadata:
                cleaned = {}
                for key, value in meta.items():
                    if isinstance(value, (str, int, float, bool)):
                        cleaned[key] = value
                    elif value is None:
                        cleaned[key] = ""
                    else:
                        cleaned[key] = str(value)
                cleaned_metadata.append(cleaned)

            # Add to ChromaDB collection
            self.collection.add(
                embeddings=embeddings,
                documents=texts,
                metadatas=cleaned_metadata,
                ids=ids
            )

            logger.info(f"Stored {len(texts)} vectors in the database")
            return True

        except Exception as e:
            logger.error(f"Error storing vectors: {e}")
            return False

    def search_similar(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Search for similar vectors to the given query

        Args:
            query: Query text to search for
            limit: Maximum number of results to return

        Returns:
            List of search results with scores and payloads
        """
        try:
            # Generate embedding for the query
            query_embedding = self.generate_embedding(query)

            # Search the ChromaDB collection
            search_results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=limit,
                include=["documents", "metadatas", "distances"]
            )

            # Format results to match the expected structure
            results = []
            if search_results and search_results['ids'] and len(search_results['ids']) > 0:
                for i, doc_id in enumerate(search_results['ids'][0]):
                    # ChromaDB returns distances, convert to similarity score
                    # For cosine distance: similarity = 1 - distance
                    distance = search_results['distances'][0][i] if search_results['distances'] else 0
                    score = 1 - distance  # Convert distance to similarity

                    payload = search_results['metadatas'][0][i] if search_results['metadatas'] else {}
                    payload['text'] = search_results['documents'][0][i] if search_results['documents'] else ""

                    results.append({
                        "score": score,
                        "payload": payload
                    })

            logger.info(f"Found {len(results)} similar vectors for query")
            return results

        except Exception as e:
            logger.error(f"Error searching similar vectors: {e}")
            return []

    def delete_all(self) -> bool:
        """Delete all vectors in the collection"""
        try:
            # Get all IDs in the collection
            all_ids = self.collection.get()['ids']

            if all_ids:
                self.collection.delete(ids=all_ids)
                logger.info(f"Deleted {len(all_ids)} vectors from collection {self.collection_name}")
            else:
                logger.info(f"Collection {self.collection_name} is already empty")

            return True
        except Exception as e:
            logger.error(f"Error deleting vectors: {e}")
            return False

    def get_collection_info(self) -> Dict[str, Any]:
        """Get information about the collection"""
        try:
            count = self.collection.count()
            return {
                "name": self.collection_name,
                "vectors_count": count,
                "points_count": count,
                "status": "green",
                "exists": True,
                "persist_directory": self.persist_directory
            }
        except Exception as e:
            logger.error(f"Error getting collection info: {e}")
            return {
                "name": self.collection_name,
                "vectors_count": 0,
                "points_count": 0,
                "status": "error",
                "error": str(e),
                "exists": False
            }

    def check_health(self) -> bool:
        """Check if the vector database is healthy"""
        try:
            # Try to get collection count as a health check
            self.collection.count()
            return True
        except Exception as e:
            logger.error(f"Vector database health check failed: {e}")
            return False
