import os
import sys
from pathlib import Path
import psycopg2
import numpy as np
from psycopg2.extras import execute_batch
from sentence_transformers import SentenceTransformer
import time
from tqdm import tqdm
import logging
from logging.handlers import RotatingFileHandler
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config import Config

# Set up logging
log_dir = Path(Config.LOG_DIR)
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / "embedding.log"

logging.basicConfig(
    level=Config.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            log_file,
            maxBytes=Config.LOG_MAX_BYTES,
            backupCount=Config.LOG_BACKUP_COUNT
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
logger.info(f"Logging initialized. Log file: {log_file}")

@dataclass
class EmbeddingConfig:
    """Configuration for embedding generation."""
    model_name: str = Config.EMBEDDING_MODEL
    batch_size: int = Config.EMBEDDING_BATCH_SIZE
    embedding_dimensions: int = Config.EMBEDDING_DIMENSIONS
    max_retries: int = Config.EMBEDDING_MAX_RETRIES
    retry_delay: int = Config.EMBEDDING_RETRY_DELAY
    # verify_sample_size: int = Config.EMBEDDING_VERIFY_SAMPLE_SIZE
    # similarity_threshold: float = Config.EMBEDDING_SIMILARITY_THRESHOLD

class DatabaseManager:
    """Handles database connections and operations."""
    
    def __init__(self, config: EmbeddingConfig):
        self.config = config
        self.conn = None
        self.db_config = Config.get_db_config()
    
    def connect(self):
        """Create a connection to the PostgreSQL database with retry logic."""
        for attempt in range(self.config.max_retries):
            try:
                self.conn = psycopg2.connect(**self.db_config)
                self.conn.autocommit = False
                return True
            except Exception as e:
                logger.error(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_delay)
                else:
                    raise
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def get_chunks_without_embeddings(self, limit: Optional[int] = None) -> List[Tuple[str, str]]:
        """Get document chunks that don't have embeddings yet."""
        try:
            with self.conn.cursor() as cursor:
                query = """
                SELECT chunk_id, chunk_text
                FROM DocumentChunks
                WHERE embedding IS NULL
                """
                
                if limit:
                    query += f" LIMIT {limit}"
                    
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching chunks: {e}")
            raise
    
    def update_embeddings(self, chunk_embeddings: List[Tuple[str, np.ndarray]]) -> None:
        """Update the database with the generated embeddings."""
        try:
            with self.conn.cursor() as cursor:
                query = """
                UPDATE DocumentChunks
                SET 
                    embedding = %s::vector,
                    embedding_model = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE chunk_id = %s
                """
                
                batch_data = [
                    (
                        embedding.tolist(),
                        self.config.model_name,
                        chunk_id
                    )
                    for chunk_id, embedding in chunk_embeddings
                ]
                
                execute_batch(cursor, query, batch_data)
                self.conn.commit()
        except Exception as e:
            logger.error(f"Error updating embeddings: {e}")
            self.conn.rollback()
            raise

class EmbeddingProcessor:
    """Handles the embedding generation process."""
    
    def __init__(self, config: Optional[EmbeddingConfig] = None):
        self.config = config or EmbeddingConfig()
        self.db = DatabaseManager(self.config)
        self.model = None
    
    def initialize(self):
        """Initialize the processor."""
        self.db.connect()
        logger.info(f"Loading model: {self.config.model_name}")
        self.model = SentenceTransformer(self.config.model_name)
    
    def cleanup(self):
        """Clean up resources."""
        self.db.close()
    
    def enrich_text(self, chunk_id: str, chunk_text: str) -> str:
        """Enrich the text with section information."""
        return f"[Section: {chunk_id}]\n{chunk_text}"
    
    def process_embeddings(self):
        """Main process to generate and store embeddings."""
        try:
            self.initialize()
            
            # Get total count for progress tracking
            with self.db.conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM DocumentChunks WHERE embedding IS NULL")
                total_remaining = cursor.fetchone()[0]
            
            logger.info(f"Found {total_remaining} chunks without embeddings")
            
            # Process in batches
            processed_count = 0
            start_time = time.time()
            
            with tqdm(total=total_remaining, desc="Processing chunks") as pbar:
                while processed_count < total_remaining:
                    # Get a batch of chunks
                    chunks = self.db.get_chunks_without_embeddings(self.config.batch_size)
                    
                    if not chunks:
                        logger.info("No more chunks to process")
                        break
                    
                    chunk_ids = [chunk[0] for chunk in chunks]
                    chunk_texts = [chunk[1] for chunk in chunks]
                    
                    # Enrich texts with section information
                    enriched_texts = [
                        self.enrich_text(chunk_id, text)
                        for chunk_id, text in zip(chunk_ids, chunk_texts)
                    ]
                    
                    # Generate embeddings
                    logger.info(f"Generating embeddings for batch of {len(enriched_texts)} chunks")
                    embeddings = self.model.encode(enriched_texts, show_progress_bar=True)
                    
                    # Prepare chunk_id and embedding pairs
                    chunk_embeddings = list(zip(chunk_ids, embeddings))
                    
                    # Update database
                    self.db.update_embeddings(chunk_embeddings)
                    
                    # Update progress
                    batch_size = len(chunks)
                    processed_count += batch_size
                    pbar.update(batch_size)
                    
                    # Log progress
                    elapsed = time.time() - start_time
                    chunks_per_second = processed_count / elapsed
                    estimated_remaining = (total_remaining - processed_count) / chunks_per_second if chunks_per_second > 0 else 0
                    
                    logger.info(
                        f"Progress: {processed_count}/{total_remaining} chunks processed "
                        f"({chunks_per_second:.2f} chunks/second, ~{estimated_remaining/60:.2f} minutes remaining)"
                    )
            
            logger.info(f"Embedding generation complete! Processed {processed_count} chunks")
            
        except Exception as e:
            logger.error(f"Error in embedding process: {e}", exc_info=True)
            raise
        finally:
            self.cleanup()
    
    # def verify_embeddings(self, sample_size: Optional[int] = None):
    #     """Verify a sample of embeddings to ensure they were stored correctly."""
    #     try:
    #         self.initialize()
            
    #         sample_size = sample_size or self.config.verify_sample_size
            
    #         with self.db.conn.cursor() as cursor:
    #             cursor.execute("""
    #                 SELECT chunk_id, chunk_text, embedding 
    #                 FROM DocumentChunks 
    #                 WHERE embedding IS NOT NULL 
    #                 ORDER BY RANDOM() 
    #                 LIMIT %s
    #             """, (sample_size,))
                
    #             samples = cursor.fetchall()
                
    #             if not samples:
    #                 logger.warning("No embedded chunks found to verify")
    #                 return
                
    #             for chunk_id, text, stored_embedding in samples:
    #                 # Enrich text for verification
    #                 enriched_text = self.enrich_text(chunk_id, text)
                    
    #                 # Generate embedding for verification
    #                 verification_embedding = self.model.encode(enriched_text)
                    
    #                 # Convert stored embedding from database format to numpy array
    #                 stored_embedding_array = np.array(stored_embedding)
                    
    #                 # Calculate cosine similarity
    #                 similarity = np.dot(verification_embedding, stored_embedding_array) / (
    #                     np.linalg.norm(verification_embedding) * np.linalg.norm(stored_embedding_array)
    #                 )
                    
    #                 logger.info(f"Chunk {chunk_id}: Verification similarity = {similarity:.4f}")
                    
    #                 if similarity < self.config.similarity_threshold:
    #                     logger.warning(
    #                         f"Embedding verification warning: Chunk {chunk_id} "
    #                         f"similarity is only {similarity:.4f}"
    #                     )
        
    #     except Exception as e:
    #         logger.error(f"Error in embedding verification: {e}", exc_info=True)
    #         raise
    #     finally:
    #         self.cleanup()

def main():
    """Main entry point for the embedding process."""
    try:
        # Initialize configuration
        config = EmbeddingConfig()
        
        # Create processor
        processor = EmbeddingProcessor(config)
        
        # Process embeddings
        logger.info("Starting embedding generation process")
        processor.process_embeddings()
        
        # Verify embeddings
        # logger.info("Verifying sample embeddings")
        # processor.verify_embeddings()
        
        logger.info("Process complete")
    
    except Exception as e:
        logger.error(f"Process failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()