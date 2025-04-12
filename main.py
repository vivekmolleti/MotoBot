from typing import Dict, List, Tuple, Optional, Any
import os
import json
import logging
from pathlib import Path
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
from concurrent.futures import ProcessPoolExecutor, as_completed
import pickle
import hashlib
import time
from datetime import datetime

import fitz
import pypdf
import pypdfium2 as pdfium
from PIL import Image
import numpy as np

from Image_extraction.images_extract import extract_diagram_from_pdf
from pdf_loading.pdf_loader import PDFLoader
from nlp.cleaning import TextCleaner
from nlp.chunking import TextChunker
from config import Config


@dataclass
class ProcessingStats:
    """Statistics for PDF processing."""
    total_files: int = 0
    processed_files: int = 0
    failed_files: int = 0
    total_pages: int = 0
    total_drawings: int = 0
    processing_time: float = 0.0
    errors: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class PDFDocument:
    """Represents a processed PDF document."""
    family: str
    year: str
    model: str
    content: Dict
    drawings: List
    file_path: Path
    processing_time: float = 0.0
    error: Optional[str] = None


class MotoBot:
    def __init__(self):
        # Initialize components
        self.pdf_loader = PDFLoader()
        self.text_cleaner = TextCleaner()
        self.text_chunker = TextChunker()
        
        # Use config paths
        self.lib_path = Config.LIB_PATH
        self.output_file = Config.OUTPUT_FILE
        
        # Use processing settings
        self.batch_size = Config.BATCH_SIZE
        self.max_workers = Config.MAX_WORKERS
        
        # Initialize statistics
        self.stats = ProcessingStats()
        self.grouped_family: Dict = {}
        
        # Setup logging and directories
        self.setup_logging()
        Config.create_directories()
    
    def setup_logging(self) -> None:
        """Configure rotating file logger."""
        log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        
        handler = RotatingFileHandler(
            Config.LOG_FILE,
            maxBytes=Config.LOG_MAX_BYTES,
            backupCount=Config.LOG_BACKUP_COUNT
        )
        handler.setFormatter(log_formatter)
        
        logger = logging.getLogger()
        logger.setLevel(Config.LOG_LEVEL)
        logger.addHandler(handler)
    
    def _get_cache_path(self, file_path: Path) -> Path:
        """Get the cache path for a file."""
        file_hash = hashlib.md5(str(file_path).encode()).hexdigest()
        return Config.CACHE_DIR / f"{file_hash}.cache"
    
    def _load_from_cache(self, file_path: Path) -> Optional[PDFDocument]:
        """Load processed PDF from cache if available."""
        if not Config.CACHE_ENABLED:
            return None
            
        cache_path = self._get_cache_path(file_path)
        if cache_path.exists():
            try:
                with open(cache_path, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logging.warning(f"Failed to load cache for {file_path}: {e}")
        return None
    
    def _save_to_cache(self, doc: PDFDocument) -> None:
        """Save processed PDF to cache."""
        if not Config.CACHE_ENABLED:
            return
            
        cache_path = self._get_cache_path(doc.file_path)
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(doc, f)
        except Exception as e:
            logging.warning(f"Failed to save cache for {doc.file_path}: {e}")
    
    def _save_processing_stats(self) -> None:
        """Save processing statistics to file."""
        try:
            stats_file = Config.PDF_STATS_DIR / f"processing_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            stats_data = {
                "total_files": self.stats.total_files,
                "processed_files": self.stats.processed_files,
                "failed_files": self.stats.failed_files,
                "total_pages": self.stats.total_pages,
                "total_drawings": self.stats.total_drawings,
                "processing_time": self.stats.processing_time,
                "errors": self.stats.errors,
                "timestamp": datetime.now().isoformat()
            }
            
            with open(stats_file, 'w') as f:
                json.dump(stats_data, f, indent=2)
            
            logging.info(f"Processing statistics saved to {stats_file}")
            
        except Exception as e:
            logging.error(f"Error saving processing statistics: {str(e)}")
    
    def process_pdf(self, file_path: Path) -> PDFDocument:
        """Process a single PDF file."""
        start_time = time.time()
        logging.info(f"Processing file: {file_path.name}")
        
        # Try to load from cache
        cached_doc = self._load_from_cache(file_path)
        if cached_doc:
            logging.info(f"Loaded {file_path.name} from cache")
            return cached_doc
        
        try:
            # Extract metadata
            metadata = self.pdf_loader.extract_metadata(file_path.name)
            logging.info(f"Extracted metadata: {metadata}")
            
            # PDF processing with proper resource management
            with (
                fitz.open(str(file_path)) as pdf_file,
                open(file_path, 'rb') as pdf_stream
            ):
                table_of_contents = self.pdf_loader.extract_toc(pdf_file)
                pdf_reader = pypdf.PdfReader(pdf_stream)
                content = self.pdf_loader.extract_page_content(pdf_reader)
                
                # Update statistics
                self.stats.total_pages += len(pdf_file)
            
            # Clean and organize content
            cleaned_content = self.text_cleaner.clean_content(content, str(file_path))
            organized_content = self.pdf_loader.organize_by_section(
                table_of_contents, cleaned_content,
                metadata.year, metadata.model_name, str(file_path)
            )
            chunked_content = self.text_chunker.chunk(organized_content)
            
            # Extract and map drawings
            doc = pdfium.PdfDocument(str(file_path))
            drawings = extract_diagram_from_pdf(doc, metadata.model_name, metadata.year, str(file_path))
            mapped_drawings = self.pdf_loader.map_drawings_to_sections(chunked_content, drawings)
            
            # Update statistics
            self.stats.total_drawings += len(drawings)
            
            result = PDFDocument(
                family=metadata.family,
                year=metadata.year,
                model=metadata.model_name,
                content=chunked_content,
                drawings=mapped_drawings,
                file_path=file_path,
                processing_time=time.time() - start_time
            )
            
            # Save to cache
            self._save_to_cache(result)
            
            return result
            
        except Exception as e:
            error_msg = f"Error processing {file_path.name}: {str(e)}"
            logging.error(error_msg, exc_info=True)
            self.stats.errors.append({
                "file": str(file_path),
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
            return PDFDocument(
                family="",
                year="",
                model="",
                content={},
                drawings=[],
                file_path=file_path,
                processing_time=time.time() - start_time,
                error=error_msg
            )
    
    def process_batch(self, files: List[Path]) -> None:
        """Process a batch of PDF files."""
        for file in files:
            try:
                self.stats.total_files += 1
                doc = self.process_pdf(file)
                
                if doc.error:
                    self.stats.failed_files += 1
                    continue
                
                self.stats.processed_files += 1
                if doc.family not in self.grouped_family:
                    self.grouped_family[doc.family] = []
                self.grouped_family[doc.family].extend(doc.drawings)
                logging.info(f"Successfully processed: {file.name}")
                
            except Exception as e:
                self.stats.failed_files += 1
                error_msg = f"Error processing {file.name}: {str(e)}"
                logging.error(error_msg, exc_info=True)
                self.stats.errors.append({
                    "file": str(file),
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
    
    def process_all_pdfs(self) -> None:
        """Process all PDF files in the library path using multiprocessing."""
        start_time = time.time()
        
        pdf_files = list(self.lib_path.glob("*.pdf"))
        if not pdf_files:
            logging.warning(f"No PDF files found in {self.lib_path}")
            return
            
        # Split files into batches
        batches = [
            pdf_files[i:i + self.batch_size]
            for i in range(0, len(pdf_files), self.batch_size)
        ]
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self.process_batch, batch)
                for batch in batches
            ]
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error in batch processing: {str(e)}", exc_info=True)
        
        # Update total processing time
        self.stats.processing_time = time.time() - start_time
        
        # Save statistics
        self._save_processing_stats()
    
    def save_results(self) -> None:
        """Save processed results to JSON file."""
        try:
            with open(self.output_file, "w", encoding="utf-8") as json_file:
                json.dump(self.grouped_family, json_file, indent=4, ensure_ascii=False)
            logging.info(f"Results saved to {self.output_file}")
        except Exception as e:
            logging.error(f"Error saving results: {str(e)}", exc_info=True)


def main():
    try:
        bot = MotoBot()
        bot.process_all_pdfs()
        bot.save_results()
        logging.info("MotoBot processing completed successfully")
    except Exception as e:
        logging.error(f"Fatal error in main process: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()