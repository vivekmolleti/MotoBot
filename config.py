from pathlib import Path
from typing import Dict, Any, List
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

def get_env_int(key: str, default: int) -> int:
    """Safely get integer from environment variable."""
    value = os.getenv(key, str(default))
    # Remove any comments or whitespace
    value = value.split('#')[0].strip()
    try:
        return int(value)
    except ValueError:
        logging.warning(f"Invalid integer value for {key}: {value}, using default: {default}")
        return default

def get_env_str(key: str, default: str) -> str:
    """Safely get string from environment variable."""
    value = os.getenv(key, default)
    # Remove any comments or whitespace
    return value.split('#')[0].strip()

def get_env_float(key: str, default: float) -> float:
    """Safely get float from environment variable."""
    value = os.getenv(key, str(default))
    # Remove any comments or whitespace
    value = value.split('#')[0].strip()
    try:
        return float(value)
    except ValueError:
        logging.warning(f"Invalid float value for {key}: {value}, using default: {default}")
        return default

class Config:
    """Configuration class for the application."""
    
    # Base paths
    BASE_DIR = Path(__file__).parent
    LIB_PATH = get_env_str('LIB_PATH', '/path/to/your/library')
    
    # Logging configuration
    LOG_DIR = BASE_DIR / "logs"
    LOG_LEVEL = get_env_str('LOG_LEVEL', 'INFO')
    LOG_MAX_BYTES = get_env_int('LOG_MAX_BYTES', 10485760)
    LOG_BACKUP_COUNT = get_env_int('LOG_BACKUP_COUNT', 5)
    
    # Processing settings
    BATCH_SIZE = get_env_int('BATCH_SIZE', 32)
    MAX_WORKERS = get_env_int('MAX_WORKERS', 4)
    
    # Output settings
    OUTPUT_DIR = get_env_str('OUTPUT_DIR', 'output')
    OUTPUT_FILE = get_env_str('OUTPUT_FILE', 'final_grouped_family.json')
    
    # Cache settings
    CACHE_ENABLED = get_env_str('CACHE_ENABLED', 'true').lower() == 'true'
    CACHE_DIR = get_env_str('CACHE_DIR', 'cache')
    
    # PDF Processing settings
    PDF_DIR = get_env_str('PDF_DIR', 'pdfs')
    PDF_EXTENSIONS = get_env_str('PDF_EXTENSIONS', '.pdf').split(',')
    MAX_PDF_SIZE = get_env_int('MAX_PDF_SIZE', 104857600)
    PDF_PROCESSING_TIMEOUT = get_env_int('PDF_PROCESSING_TIMEOUT', 300)
    
    # Image Processing settings
    IMAGE_DIR = get_env_str('IMAGE_DIR', 'images')
    IMAGE_EXTENSIONS = get_env_str('IMAGE_EXTENSIONS', '.png,.jpg,.jpeg').split(',')
    MAX_IMAGE_SIZE = get_env_int('MAX_IMAGE_SIZE', 10485760)
    IMAGE_QUALITY = get_env_int('IMAGE_QUALITY', 85)
    
    # Statistics and Monitoring
    STATS_DIR = get_env_str('STATS_DIR', 'stats')
    CLEANING_STATS_DIR = get_env_str('CLEANING_STATS_DIR', 'stats/cleaning')
    CHUNKING_STATS_DIR = get_env_str('CHUNKING_STATS_DIR', 'stats/chunking')
    PDF_STATS_DIR = get_env_str('PDF_STATS_DIR', 'stats/pdf')
    
    # Ducati-specific settings
    DUCATI_FAMILIES = get_env_str('DUCATI_FAMILIES', 'DesertX,Diavel,Hypermotard,Monster,Multistrada,Off-Road,Panigale,Scrambler,Streetfighter,Supersport,Superbike,XDiavel').split(',')
    NAME_CLEANUP_PATTERNS = get_env_str('NAME_CLEANUP_PATTERNS', 'OM,_,.pdf,-,EN,ED00,ED01,ED02,ED03,Rev01,Rev02').split(',')
    
    # Error handling
    MAX_RETRIES = get_env_int('MAX_RETRIES', 3)
    RETRY_DELAY = get_env_int('RETRY_DELAY', 5)
    
    # Database settings
    DB_HOST = get_env_str('DB_HOST', 'localhost')
    DB_PORT = get_env_int('DB_PORT', 5432)
    DB_NAME = get_env_str('DB_NAME', 'motobot')
    DB_USER = get_env_str('DB_USER', 'postgres')
    DB_PASSWORD = get_env_str('DB_PASSWORD', 'Ironman#99')
    
    # Embedding settings
    EMBEDDING_MODEL = get_env_str('EMBEDDING_MODEL', 'all-mpnet-base-v2')
    EMBEDDING_BATCH_SIZE = get_env_int('EMBEDDING_BATCH_SIZE', 500)
    EMBEDDING_DIMENSIONS = get_env_int('EMBEDDING_DIMENSIONS', 768)
    EMBEDDING_MAX_RETRIES = get_env_int('EMBEDDING_MAX_RETRIES', 3)
    EMBEDDING_RETRY_DELAY = get_env_int('EMBEDDING_RETRY_DELAY', 5)
    EMBEDDING_VERIFY_SAMPLE_SIZE = get_env_int('EMBEDDING_VERIFY_SAMPLE_SIZE', 5)
    EMBEDDING_SIMILARITY_THRESHOLD = get_env_float('EMBEDDING_SIMILARITY_THRESHOLD', 0.9999)
    
    # Cosmos DB settings
    COSMOS_ENDPOINT = get_env_str('COSMOS_ENDPOINT', 'https://motobot-db.documents.azure.com:443/')
    COSMOS_KEY = get_env_str('COSMOS_KEY', '')
    COSMOS_DATABASE = get_env_str('COSMOS_DATABASE', 'motobot-db')
    COSMOS_CONTAINER = get_env_str('COSMOS_CONTAINER', 'motobot-container')
    AZURE_STORAGE_CONNECTION_STRING = get_env_str('AZURE_STORAGE_CONNECTION_STRING', '')
    
    # Blob container settings
    AZURE_BLOB_ACCOUNT_NAME = get_env_str('AZURE_BLOB_ACCOUNT_NAME', '')
    AZURE_BLOB_KEY = get_env_str('AZURE_BLOB_KEY', '')
    AZURE_BLOB_CONTAINER = get_env_str('AZURE_BLOB_CONTAINER', 'motobotdatastore')
    AZURE_BLOB_CONNECTION_STRING = get_env_str('AZURE_BLOB_CONNECTION_STRING', '')
    
    @classmethod
    def create_directories(cls) -> None:
        """Create necessary directories if they don't exist."""
        directories = [
            cls.BASE_DIR / "logs",
            Path(cls.OUTPUT_DIR),
            Path(cls.CACHE_DIR),
            Path(cls.PDF_DIR),
            Path(cls.IMAGE_DIR),
            Path(cls.STATS_DIR),
            Path(cls.CLEANING_STATS_DIR),
            Path(cls.CHUNKING_STATS_DIR),
            Path(cls.PDF_STATS_DIR)
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_settings(cls) -> Dict[str, Any]:
        """Get all settings as a dictionary."""
        return {
            "base_dir": str(cls.BASE_DIR),
            "lib_path": str(cls.LIB_PATH),
            "log_file": str(cls.LOG_FILE),
            "log_max_bytes": cls.LOG_MAX_BYTES,
            "log_backup_count": cls.LOG_BACKUP_COUNT,
            "log_level": cls.LOG_LEVEL,
            "batch_size": cls.BATCH_SIZE,
            "max_workers": cls.MAX_WORKERS,
            "output_dir": str(cls.OUTPUT_DIR),
            "output_file": str(cls.OUTPUT_FILE),
            "cache_dir": str(cls.CACHE_DIR),
            "cache_enabled": cls.CACHE_ENABLED,
            "pdf_dir": str(cls.PDF_DIR),
            "pdf_extensions": cls.PDF_EXTENSIONS,
            "max_pdf_size": cls.MAX_PDF_SIZE,
            "pdf_processing_timeout": cls.PDF_PROCESSING_TIMEOUT,
            "image_dir": str(cls.IMAGE_DIR),
            "image_extensions": cls.IMAGE_EXTENSIONS,
            "max_image_size": cls.MAX_IMAGE_SIZE,
            "image_quality": cls.IMAGE_QUALITY,
            "stats_dir": str(cls.STATS_DIR),
            "cleaning_stats_dir": str(cls.CLEANING_STATS_DIR),
            "chunking_stats_dir": str(cls.CHUNKING_STATS_DIR),
            "pdf_stats_dir": str(cls.PDF_STATS_DIR),
            "ducati_families": cls.DUCATI_FAMILIES,
            "name_cleanup_patterns": cls.NAME_CLEANUP_PATTERNS,
            "max_retries": cls.MAX_RETRIES,
            "retry_delay": cls.RETRY_DELAY,
            "db_host": cls.DB_HOST,
            "db_port": cls.DB_PORT,
            "db_name": cls.DB_NAME,
            "db_user": cls.DB_USER,
            "db_password": cls.DB_PASSWORD,
            "embedding_model": cls.EMBEDDING_MODEL,
            "embedding_batch_size": cls.EMBEDDING_BATCH_SIZE,
            "embedding_dimensions": cls.EMBEDDING_DIMENSIONS,
            "embedding_max_retries": cls.EMBEDDING_MAX_RETRIES,
            "embedding_retry_delay": cls.EMBEDDING_RETRY_DELAY,
            "embedding_verify_sample_size": cls.EMBEDDING_VERIFY_SAMPLE_SIZE,
            "embedding_similarity_threshold": cls.EMBEDDING_SIMILARITY_THRESHOLD,
            "cosmos_endpoint": cls.COSMOS_ENDPOINT,
            "cosmos_key": cls.COSMOS_KEY,
            "cosmos_database": cls.COSMOS_DATABASE,
            "cosmos_container": cls.COSMOS_CONTAINER,
            "azure_storage_connection_string": cls.AZURE_STORAGE_CONNECTION_STRING,
            "azure_blob_account_name": cls.AZURE_BLOB_ACCOUNT_NAME,
            "azure_blob_key": cls.AZURE_BLOB_KEY,
            "azure_blob_container": cls.AZURE_BLOB_CONTAINER,
            "azure_blob_connection_string": cls.AZURE_BLOB_CONNECTION_STRING
        }

    @classmethod
    def handle_error(cls, file: Path, e: Exception) -> None:
        """Handle an error by logging it."""
        logging.error(f"Error processing {file.name}: {str(e)}", exc_info=True)

    @classmethod
    def get_embedding_config(cls) -> Dict[str, Any]:
        """Get embedding configuration as a dictionary."""
        return {
            'model_name': cls.EMBEDDING_MODEL,
            'batch_size': cls.EMBEDDING_BATCH_SIZE,
            'embedding_dimensions': cls.EMBEDDING_DIMENSIONS,
            'max_retries': cls.EMBEDDING_MAX_RETRIES,
            'retry_delay': cls.EMBEDDING_RETRY_DELAY,
            'verify_sample_size': cls.EMBEDDING_VERIFY_SAMPLE_SIZE,
            'similarity_threshold': cls.EMBEDDING_SIMILARITY_THRESHOLD
        }
    
    @classmethod
    def get_db_config(cls) -> Dict[str, Any]:
        """Get database configuration as a dictionary."""
        return {
            'dbname': cls.DB_NAME,
            'user': cls.DB_USER,
            'password': cls.DB_PASSWORD,
            'host': cls.DB_HOST,
            'port': cls.DB_PORT
        }
    
    @classmethod
    def get_processing_config(cls) -> Dict[str, Any]:
        """Get processing configuration as a dictionary."""
        return {
            'batch_size': cls.BATCH_SIZE,
            'max_workers': cls.MAX_WORKERS,
            'max_retries': cls.MAX_RETRIES,
            'retry_delay': cls.RETRY_DELAY
        }
    
    @classmethod
    def get_path_config(cls) -> Dict[str, Path]:
        """Get path configuration as a dictionary of Path objects."""
        return {
            'lib_path': Path(cls.LIB_PATH),
            'output_dir': Path(cls.OUTPUT_DIR),
            'cache_dir': Path(cls.CACHE_DIR),
            'pdf_dir': Path(cls.PDF_DIR),
            'image_dir': Path(cls.IMAGE_DIR),
            'stats_dir': Path(cls.STATS_DIR),
            'cleaning_stats_dir': Path(cls.CLEANING_STATS_DIR),
            'chunking_stats_dir': Path(cls.CHUNKING_STATS_DIR),
            'pdf_stats_dir': Path(cls.PDF_STATS_DIR)
        }
    
    @classmethod
    def setup_logging(cls):
        """Setup logging configuration."""
        logging.basicConfig(
            level=cls.LOG_LEVEL,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(
                    "application.log",
                    maxBytes=cls.LOG_MAX_BYTES,
                    backupCount=cls.LOG_BACKUP_COUNT
                ),
                logging.StreamHandler()
            ]
        )
    
    @classmethod
    def validate_config(cls) -> List[str]:
        """Validate configuration and return list of issues."""
        issues = []
        
        # Check required directories
        for name, path in cls.get_path_config().items():
            if not path.exists():
                issues.append(f"Directory does not exist: {name} ({path})")
        
        # Check database connection
        try:
            import psycopg2
            conn = psycopg2.connect(**cls.get_db_config())
            conn.close()
        except Exception as e:
            issues.append(f"Database connection failed: {str(e)}")
        
        # Check embedding model
        try:
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer(cls.EMBEDDING_MODEL)
            del model
        except Exception as e:
            issues.append(f"Embedding model loading failed: {str(e)}")
        
        return issues 

    @classmethod
    def get_cosmos_config(cls) -> Dict[str, str]:
        """Get Cosmos DB configuration as a dictionary."""
        return {
            'endpoint': cls.COSMOS_ENDPOINT,
            'key': cls.COSMOS_KEY,
            'database': cls.COSMOS_DATABASE,
            'container': cls.COSMOS_CONTAINER
        } 
    
    # @classmethod
    # def get_blob_config(cls) -> Dict[str, str]:
    #     """Get Azure Blob Storage configuration as a dictionary."""
    #     if not cls.AZURE_BLOB_CONNECTION_STRING:
    #         raise ValueError("Azure Blob Storage connection string must be set in environment variables")
            
    #     return {
    #         'connection_string': cls.AZURE_BLOB_CONNECTION_STRING,
    #         'container': cls.AZURE_BLOB_CONTAINER
    #     }