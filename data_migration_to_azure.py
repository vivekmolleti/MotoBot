import psycopg2
import logging
from typing import Dict, List, Any
from config import Config
from database_schema.cosmos_manager import CosmosManager
from pathlib import Path
import json
from azure.storage.blob import BlobServiceClient

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
        self.pg_conn = psycopg2.connect(**Config.get_db_config())
        self.pg_conn.autocommit = True
        self.pg_cursor = self.pg_conn.cursor()
        
    
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
                   image_data, created_at
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
                'image_data': image[4].hex() if image[4] else None,
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
    
    def migrate_all(self):
        """Migrate all tables."""
        try:
            logger.info("Starting migration to Cosmos DB")
            # self.create_cosmos_containers()
            
            # Migrate in order to maintain referential integrity
            # self.migrate_companies()
            # self.migrate_pdf_families()
            self.migrate_documents()
            self.migrate_document_chunks()
            self.migrate_document_images()
            #self.migrate_rag_queries()
            
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
