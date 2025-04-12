from pathlib import Path
from typing import Dict, Any, List
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

class Config:
    # Base paths
    BASE_DIR = Path(__file__).parent
    LIB_PATH = Path(os.getenv("LIB_PATH"))
    
    # Logging configuration
    LOG_FILE = BASE_DIR / "logs" / "moto_bot.log"
    LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
    LOG_BACKUP_COUNT = 5
    LOG_LEVEL = os.getenv("LOG_LEVEL")
    
    # Processing settings
    BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
    MAX_WORKERS = int(os.getenv("MAX_WORKERS"))
    
    # Output settings
    OUTPUT_DIR = BASE_DIR / "output"
    OUTPUT_FILE = OUTPUT_DIR / "final_grouped_family.json"
    
    # Cache settings
    CACHE_DIR = BASE_DIR / "cache"
    CACHE_ENABLED = os.getenv("CACHE_ENABLED").lower() == "true"
    
    # PDF Processing settings
    PDF_DIR = BASE_DIR / "pdfs"
    PDF_EXTENSIONS = [".pdf"]
    MAX_PDF_SIZE = 100 * 1024 * 1024  # 100MB
    PDF_PROCESSING_TIMEOUT = 300  # 5 minutes
    
    # Image Processing settings
    IMAGE_DIR = BASE_DIR / "images"
    IMAGE_EXTENSIONS = [".png", ".jpg", ".jpeg"]
    MAX_IMAGE_SIZE = 10 * 1024 * 1024  # 10MB
    IMAGE_QUALITY = 85  # JPEG quality
    
    # Statistics and Monitoring
    STATS_DIR = BASE_DIR / "stats"
    CLEANING_STATS_DIR = STATS_DIR / "cleaning"
    CHUNKING_STATS_DIR = STATS_DIR / "chunking"
    PDF_STATS_DIR = STATS_DIR / "pdf"
    
    # Ducati-specific settings
    DUCATI_FAMILIES = [
        'DesertX', 'Diavel', 'Hypermotard', 'Monster', 'Multistrada',
        'Off-Road', 'Panigale', 'Scrambler', 'Streetfighter',
        'Supersport', 'Superbike', 'XDiavel'
    ]
    
    NAME_CLEANUP_PATTERNS = [
        'OM', '_', '.pdf', '-', 'EN', 'ED00', 'ED01', 'ED02', 'ED03',
        'Rev01', 'Rev02'
    ]
    
    # Error handling
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # seconds
    
    @classmethod
    def create_directories(cls) -> None:
        """Create necessary directories if they don't exist."""
        directories = [
            cls.BASE_DIR / "logs",
            cls.OUTPUT_DIR,
            cls.CACHE_DIR,
            cls.PDF_DIR,
            cls.IMAGE_DIR,
            cls.STATS_DIR,
            cls.CLEANING_STATS_DIR,
            cls.CHUNKING_STATS_DIR,
            cls.PDF_STATS_DIR
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
            "retry_delay": cls.RETRY_DELAY
        }

    @classmethod
    def handle_error(cls, file: Path, e: Exception) -> None:
        """Handle an error by logging it."""
        logging.error(f"Error processing {file.name}: {str(e)}", exc_info=True) 