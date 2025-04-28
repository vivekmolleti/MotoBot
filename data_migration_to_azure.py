import psycopg2
import logging
from typing import Dict, List, Any
from config import Config
from database_schema.cosmos_manager import CosmosManager
from pathlib import Path
import json
from azure.storage.blob import BlobServiceClient
import os
import fitz  # PyMuPDF
from PIL import Image
import io
from sentence_transformers import SentenceTransformer
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Set up logging
log_dir = Path(Config.LOG_DIR)
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / "migration.log"

logging.basicConfig(
    level=Config.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class DataMigrator:
    def __init__(self):
        self.cosmos = CosmosManager()
        #self.pg_conn = psycopg2.connect(**Config.get_db_config())
        #self.pg_conn.autocommit = True
        #self.pg_cursor = self.pg_conn.cursor()
        #self.pdf_dir = Path(Config.PDF_DIR)
        self.blob_service_client = BlobServiceClient.from_connection_string(Config.AZURE_BLOB_CONNECTION_STRING)
        self.container_client = self.blob_service_client.get_container_client(Config.AZURE_BLOB_CONTAINER)
        #self.images_dir = Path(Config.IMAGE_DIR)
        #self.images_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize the embedding model with a smaller, faster model
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.batch_size = 32  # Smaller batch size for CPU
        self.max_workers = 2  # Fewer workers to prevent CPU overload
        
        # Checkpoint file
        self.checkpoint_file = Path("embedding_checkpoint.json")
        self.processed_chunks = self.load_checkpoint()
    
    def load_checkpoint(self) -> set:
        """Load processed chunk IDs from checkpoint file."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    return set(json.load(f))
            except Exception as e:
                logger.error(f"Error loading checkpoint: {str(e)}")
        return set()
    
    def save_checkpoint(self):
        """Save processed chunk IDs to checkpoint file."""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(list(self.processed_chunks), f)
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")
    
    def create_cosmos_containers(self):
        """Create all necessary containers in Cosmos DB."""
        containers = {
            'companies': {
                'partition_key': '/company_id',
                'vector_embedding_policy': None
            },
            'pdf_families': {
                'partition_key': '/family_id',
                'vector_embedding_policy': None
            },
            'documents': {
                'partition_key': '/document_id',
                'vector_embedding_policy': None
            },
            'document_chunks': {
                'partition_key': '/document_id',
                'vector_embedding_policy': {
                    "vectorEmbeddings": [{
                        "path": "/embedding",
                        "dataType": "float32",
                        "dimensions": Config.EMBEDDING_DIMENSIONS,

                        "distanceFunction": "cosine"
                    }]
                }
            },
            'document_images': {
                'partition_key': '/document_id',
                'vector_embedding_policy': None
            },
            'rag_queries': {
                'partition_key': '/query_id',
                'vector_embedding_policy': None
            }
        }
        
        for container_name, container_config in containers.items():
            try:
                self.cosmos.create_container_if_not_exists(
                    id=container_name,
                    partition_key=container_config['partition_key'],
                    vector_embedding_policy=container_config['vector_embedding_policy']
                )
                logger.info(f"Created container {container_name}")
            except Exception as e:
                if "Conflict" in str(e):
                    logger.info(f"Container {container_name} already exists")
                else:
                    raise
    
    def migrate_companies(self):
        """Migrate Companies table."""
        logger.info("Migrating Companies table")
        self.pg_cursor.execute("""
            SELECT company_id, company_name, company_description, created_at, updated_at
            FROM Companies
        """)
        
        companies = self.pg_cursor.fetchall()
        container = self.cosmos.database.get_container_client('companies')
        
        for company in companies:
            item = {
                'id': str(company[0]),
                'company_id': str(company[0]),
                'company_name': company[1],
                'company_description': company[2],
                'created_at': company[3].isoformat() if company[3] else None,
                'updated_at': company[4].isoformat() if company[4] else None
            }
            container.upsert_item(item)
        
        logger.info(f"Migrated {len(companies)} companies")
    
    def migrate_pdf_families(self):
        """Migrate PDFFamilies table."""
        logger.info("Migrating PDFFamilies table")
        self.pg_cursor.execute("""
            SELECT family_id, company_id, family_name, family_description, created_at, updated_at
            FROM PDFFamilies
        """)
        
        families = self.pg_cursor.fetchall()
        container = self.cosmos.database.get_container_client('pdf_families')
        
        for family in families:
            item = {
                'id': str(family[0]),
                'family_id': str(family[0]),
                'company_id': str(family[1]),
                'family_name': family[2],
                'family_description': family[3],
                'created_at': family[4].isoformat() if family[4] else None,
                'updated_at': family[5].isoformat() if family[5] else None
            }
            container.upsert_item(item)
        
        logger.info(f"Migrated {len(families)} PDF families")

    def migrate_documents(self):
        """Migrate Documents table."""
        logger.info("Migrating Documents table")
        self.pg_cursor.execute("""
            SELECT document_id, family_id, document_name, original_filename, 
                   file_size, file_type, upload_date, last_accessed, 
                   metadata, created_at, updated_at
            FROM Documents
        """)
        
        documents = self.pg_cursor.fetchall()
        container = self.cosmos.database.get_container_client('documents')
        
        for doc in documents:
            item = {
                'id': str(doc[0]),
                'document_id': str(doc[0]),
                'family_id': str(doc[1]),
                'document_name': doc[2],
                'original_filename': doc[3],
                'file_size': doc[4],
                'file_type': doc[5],
                'upload_date': doc[6].isoformat() if doc[6] else None,
                'last_accessed': doc[7].isoformat() if doc[7] else None,
                'metadata': doc[8] if doc[8] else None,
                'created_at': doc[9].isoformat() if doc[9] else None,
                'updated_at': doc[10].isoformat() if doc[10] else None
            }
            
            try:
                container.upsert_item(item)
                logger.info(f"Upserted document {doc[0]} successfully")
            except Exception as e:
                logger.error(f"Failed to upsert document {doc[0]}: {str(e)}")
                continue
        
        logger.info(f"Migrated {len(documents)} documents")
    
    def migrate_document_chunks(self):
        """Migrate DocumentChunks table."""
        logger.info("Migrating DocumentChunks table")
        self.pg_cursor.execute("""
            SELECT chunk_id, document_id, chunk_text, chunk_index, 
                               position_data,  created_at,  updated_at
            FROM DocumentChunks
        """)
        
        chunks = self.pg_cursor.fetchall()
        container = self.cosmos.database.get_container_client('document_chunks')
        
        for chunk in chunks:
            item = {
                'id': str(chunk[0]),
                'chunk_id': str(chunk[0]),
                'document_id': str(chunk[1]),
                'chunk_text': chunk[2],
                'chunk_index': chunk[3],
                'position_data': chunk[4] if chunk[4] else None,
                'created_at': chunk[5].isoformat() if chunk[5] else None,
                'updated_at': chunk[6].isoformat() if chunk[6] else None,
            }
            container.upsert_item(item)
        
        logger.info(f"Migrated {len(chunks)} document chunks")
    
    def migrate_document_images(self):
        """Migrate DocumentImages table."""
        logger.info("Migrating DocumentImages table")
        self.pg_cursor.execute("""
            SELECT image_id, document_id, page_number, image_path,
                   position_data, created_at
            FROM DocumentImages
        """)
        
        images = self.pg_cursor.fetchall()
        container = self.cosmos.database.get_container_client('document_images')
        
        for image in images:
            item = {
                'id': str(image[0]),
                'image_id': str(image[0]),
                'document_id': str(image[1]),
                'page_number': image[2],
                'image_path': image[3],
                'image_data': image[4] if image[4] else None,
                'created_at': image[5].isoformat() if image[5] else None
            }
            container.upsert_item(item)
        
        logger.info(f"Migrated {len(images)} document images")
    
    def migrate_rag_queries(self):
        """Migrate RAGQueries table."""
        logger.info("Migrating RAGQueries table")
        self.pg_cursor.execute("""
            SELECT query_id, user_query, processed_query, response,
                   chunks_used, images_used, feedback_score, query_time
            FROM RAGQueries
        """)
        
        queries = self.pg_cursor.fetchall()
        container = self.cosmos.database.get_container_client('rag_queries')
        
        for query in queries:
            item = {
                'id': str(query[0]),
                'query_id': str(query[0]),
                'user_query': query[1],
                'processed_query': query[2],
                'response': query[3],
                'chunks_used': query[4] if query[4] else None,  
                'images_used': query[5] if query[5] else None,
                'feedback_score': query[6],
                'query_time': query[7].isoformat() if query[7] else None,
            }
            container.upsert_item(item)
        
        logger.info(f"Migrated {len(queries)} RAG queries")
    
    def get_local_pdfs(self) -> Dict[str, Path]:
        """Get all PDF files from the local directory."""
        pdf_files = {}
        for pdf_file in self.pdf_dir.glob("**/*.pdf"):
            pdf_files[pdf_file.name] = pdf_file
        return pdf_files
    
    def update_document_urls(self):
        """Update document URLs in Cosmos DB based on local PDFs."""
        logger.info("Updating document URLs in Cosmos DB")
        
        # Get all local PDFs
        local_pdfs = self.get_local_pdfs()
        logger.info(f"Found {len(local_pdfs)} local PDF files")
        
        # Get documents container
        container = self.cosmos.database.get_container_client('documents')
        
        # Query all documents
        query = "SELECT * FROM c"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        updated_count = 0
        for item in items:
            original_filename = item.get('original_filename')
            if not original_filename:
                continue
                
            # Check if we have this PDF locally
            if original_filename in local_pdfs:
                try:
                    # Get blob URL for the PDF
                    blob_name = f"documents/{original_filename}"
                    blob_client = self.container_client.get_blob_client(blob_name)
                    
                    if blob_client.exists():
                        # Update document with blob URL
                        item['blob_url'] = blob_client.url
                        container.upsert_item(item)
                        updated_count += 1
                        logger.info(f"Updated URL for document {item['document_id']}: {blob_client.url}")
                    else:
                        logger.warning(f"Blob not found for {original_filename}")
                except Exception as e:
                    logger.error(f"Error updating document {item['document_id']}: {str(e)}")
                    continue
        
        logger.info(f"Updated {updated_count} document URLs")
    
    def extract_image_from_pdf(self, pdf_path: Path, page_number: int, image_data: dict) -> Image.Image:
        """Extract image from PDF at specified coordinates."""
        try:
            return 
        except Exception as e:
            logger.error(f"Error extracting image from {pdf_path} page {page_number}: {str(e)}")
            return None
    
    def upload_image_to_blob(self, image: Image.Image, blob_name: str) -> str:
        """Upload image to blob storage and return the URL."""
        try:
            # Convert image to bytes
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='PNG')
            img_byte_arr = img_byte_arr.getvalue()
            
            # Upload to blob storage
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.upload_blob(img_byte_arr, overwrite=True)
            
            return blob_client.url
        except Exception as e:
            logger.error(f"Error uploading image {blob_name}: {str(e)}")
            return None
    
    def update_document_images(self):
        """Update document images in Cosmos DB with blob URLs."""
        logger.info("Updating document images in Cosmos DB")
        
        # Get all local PDFs
        local_pdfs = self.get_local_pdfs()
        logger.info(f"Found {len(local_pdfs)} local PDF files")
        
        # Get images container
        container = self.cosmos.database.get_container_client('document_images')
        
        # Query all images
        query = "SELECT * FROM c"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        updated_count = 0
        for item in items:
            try:
                # Get the document to find the PDF
                doc_container = self.cosmos.database.get_container_client('documents')
                doc = doc_container.read_item(item['document_id'], partition_key=item['document_id'])
                
                if not doc or 'original_filename' not in doc:
                    logger.warning(f"Document not found for image {item['image_id']}")
                    continue
                
                pdf_name = doc['original_filename']
                if pdf_name not in local_pdfs:
                    logger.warning(f"PDF not found locally for image {item['image_id']}: {pdf_name}")
                    continue
                
                # Extract image from PDF
                image = self.extract_image_from_pdf(
                    local_pdfs[pdf_name],
                    item['page_number'],
                    item['image_data']
                )
                
                if not image:
                    continue
                
                # Generate blob name
                blob_name = f"images/{item['image_id']}.png"
                
                # Upload to blob storage
                blob_url = self.upload_image_to_blob(image, blob_name)
                
                if blob_url:
                    # Update image record
                    item['image_path'] = blob_url
                    container.upsert_item(item)
                    updated_count += 1
                    logger.info(f"Updated image {item['image_id']} with URL: {blob_url}")
                
            except Exception as e:
                logger.error(f"Error processing image {item['image_id']}: {str(e)}")
                continue
        
        logger.info(f"Updated {updated_count} document images")
    
    def update_chunk_embeddings(self):
        """Update document chunks with vector embeddings using optimized batch processing."""
        logger.info("Generating and storing vector embeddings for document chunks")
        
        # Get chunks container
        container = self.cosmos.database.get_container_client('document_chunks')
        
        # Query chunks without embeddings and not yet processed
        query = "SELECT * FROM document_chunks as c WHERE NOT IS_DEFINED(c.embedding)"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        
        # Filter out already processed chunks
        items = [item for item in items if item['chunk_id'] not in self.processed_chunks]
        
        logger.info(f"Found {len(items)} chunks to process")
        
        # Split items into smaller batches
        batches = [items[i:i + self.batch_size] for i in range(0, len(items), self.batch_size)]
        total_updated = 0
        
        try:
            # Process batches sequentially to prevent CPU overload
            for i, batch in enumerate(batches):
                try:
                    # Extract texts from the batch
                    texts = [item['chunk_text'] for item in batch]
                    
                    # Generate embeddings for the batch
                    embeddings = self.embedding_model.encode(
                        texts,
                        batch_size=self.batch_size,
                        show_progress_bar=True,
                        convert_to_numpy=True
                    )
                    
                    if embeddings is not None:
                        # Update each chunk with its embeddings
                        for item, embedding in zip(batch, embeddings):
                            item['embedding'] = embedding.tolist()
                            container.upsert_item(item)
                            self.processed_chunks.add(item['chunk_id'])
                            total_updated += 1
                        
                        # Save checkpoint after each successful batch
                        self.save_checkpoint()
                        
                        logger.info(f"Processed batch {i+1}/{len(batches)} - Updated {len(batch)} chunks")
                        logger.info(f"Total updated: {total_updated}")
                    
                except Exception as e:
                    logger.error(f"Error processing batch {i+1}: {str(e)}")
                    continue
                
                # Add a small delay to prevent CPU overheating
                time.sleep(1)
        
        except Exception as e:
            logger.error(f"Error in embedding generation: {str(e)}")
        finally:
            self.save_checkpoint()
            logger.info(f"Completed processing. Total chunks updated: {total_updated}")
    
    def migrate_all(self):
        """Migrate all tables."""
        try:
            logger.info("Starting migration to Cosmos DB")
            
            # Migrate in order to maintain referential integrity
            # self.migrate_companies()
            # self.migrate_pdf_families()
            # self.migrate_documents()
            # self.migrate_document_chunks()
            # self.migrate_document_images()
            # self.migrate_rag_queries()
            
            # # Update document URLs after migration
            # self.update_document_urls()
            
            # # Update document images
            # self.update_document_images()
            
            # Generate and store embeddings
            self.update_chunk_embeddings()
            
            logger.info("Migration completed successfully")
        except Exception as e:
            logger.error(f"Migration failed: {e}", exc_info=True)
            raise
        finally:
            self.pg_cursor.close()
            self.pg_conn.close()

if __name__ == "__main__":
    migrator = DataMigrator()
    migrator.migrate_all()
