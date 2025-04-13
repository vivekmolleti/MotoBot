import os
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosHttpResponseError
import numpy as np
from typing import List, Tuple, Optional, Dict, Any
import logging
from config import Config

logger = logging.getLogger(__name__)

class CosmosManager:
    """Handles Azure Cosmos DB operations."""
    
    def __init__(self):
        """Initialize Cosmos DB client and database."""
        try:
            config = Config.get_cosmos_config()
            logger.info(f"Connecting to Cosmos DB at {config['endpoint']}")
            
            self.client = CosmosClient(
                url=config['endpoint'],
                credential=config['key']
            )
            self.database = self.client.get_database_client(config['database'])
            logger.info(f"Successfully connected to Cosmos DB database: {config['database']}")
        except Exception as e:
            logger.error(f"Failed to initialize Cosmos DB client: {e}")
            raise
    
    def create_container_if_not_exists(self, id: str, partition_key: str, vector_embedding_policy: dict = None):
        """Create a container if it doesn't exist."""
        try:
            container = self.database.create_container(
                id=id,
                partition_key=PartitionKey(path=partition_key),
                vector_embedding_policy=vector_embedding_policy
            )
            logger.info(f"Created container: {id}")
            return container
        except Exception as e:
            if "Conflict" in str(e):
                logger.info(f"Container {id} already exists")
                return self.database.get_container_client(id)
            else:
                logger.error(f"Error creating container {id}: {e}")
                raise
    
    def store_chunk(self, chunk_id: str, document_id: str, chunk_text: str, embedding: np.ndarray) -> None:
        """Store a document chunk with its embedding."""
        try:
            item = {
                "id": chunk_id,
                "document_id": document_id,
                "chunk_text": chunk_text,
                "embedding": embedding.tolist(),
                "embedding_model": Config.EMBEDDING_MODEL
            }
            self.container.upsert_item(item)
            logger.debug(f"Stored chunk {chunk_id}")
        except Exception as e:
            logger.error(f"Error storing chunk {chunk_id}: {e}")
            raise
    
    def store_chunks_batch(self, chunks: List[Dict[str, Any]]) -> None:
        """Store multiple chunks in a batch."""
        try:
            for chunk in chunks:
                self.container.upsert_item(chunk)
            logger.info(f"Stored {len(chunks)} chunks")
        except Exception as e:
            logger.error(f"Error storing batch of chunks: {e}")
            raise
    
    def search_similar_chunks(self, query_embedding: np.ndarray, document_id: Optional[str] = None, limit: int = 5) -> List[Dict[str, Any]]:
        """Search for similar chunks using vector similarity."""
        try:
            query = {
                "query": "SELECT TOP @limit c.id, c.chunk_text, c.document_id, VectorDistance(c.embedding, @embedding) as similarity FROM c",
                "parameters": [
                    {"name": "@limit", "value": limit},
                    {"name": "@embedding", "value": query_embedding.tolist()}
                ]
            }
            
            if document_id:
                query["query"] += " WHERE c.document_id = @document_id"
                query["parameters"].append({"name": "@document_id", "value": document_id})
            
            query["query"] += " ORDER BY VectorDistance(c.embedding, @embedding)"
            
            results = list(self.container.query_items(query=query, enable_cross_partition_query=True))
            logger.debug(f"Found {len(results)} similar chunks")
            return results
        except Exception as e:
            logger.error(f"Error searching similar chunks: {e}")
            raise
    
    def get_chunks_without_embeddings(self, limit: Optional[int] = None) -> List[Tuple[str, str]]:
        """Get chunks that don't have embeddings yet."""
        try:
            query = {
                "query": "SELECT c.id, c.chunk_text FROM c WHERE IS_DEFINED(c.embedding) = false"
            }
            if limit:
                query["query"] += f" OFFSET 0 LIMIT {limit}"
            
            results = list(self.container.query_items(query=query, enable_cross_partition_query=True))
            return [(item["id"], item["chunk_text"]) for item in results]
        except Exception as e:
            logger.error(f"Error fetching chunks without embeddings: {e}")
            raise
    
    def update_embeddings(self, chunk_embeddings: List[Tuple[str, np.ndarray]]) -> None:
        """Update chunks with their embeddings."""
        try:
            for chunk_id, embedding in chunk_embeddings:
                item = self.container.read_item(item=chunk_id, partition_key=chunk_id)
                item["embedding"] = embedding.tolist()
                item["embedding_model"] = Config.EMBEDDING_MODEL
                self.container.upsert_item(item)
            logger.info(f"Updated {len(chunk_embeddings)} embeddings")
        except Exception as e:
            logger.error(f"Error updating embeddings: {e}")
            raise 