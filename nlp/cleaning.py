from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
import logging
import re
import json
from pathlib import Path
from datetime import datetime
from config import Config

from cleaners.text_cleaners import (
    clean_non_ascii_chars,
    clean_bullets,
    clean_ordered_bullets,
    clean_ligatures,
    group_bullet_paragraph,
    group_broken_paragraphs,
    clean_extra_whitespace,
    clean_trailing_punctuation,
    clean_dashes,
    replace_unicode_quotes,
    remove_punctuation,
)

@dataclass
class CleaningStats:
    original_length: int
    cleaned_length: int
    removed_chars: int
    removed_lines: int
    processing_time: float
    errors: List[str]

class TextCleaner:
    def __init__(self):
        self.cleaning_functions = [
            clean_non_ascii_chars,
            clean_bullets,
            clean_ordered_bullets,
            clean_ligatures,
            group_bullet_paragraph,
            group_broken_paragraphs,
            clean_extra_whitespace,
            clean_trailing_punctuation,
            clean_dashes,
            replace_unicode_quotes,
            remove_punctuation,
        ]
        self._setup_logging()
        self._setup_stats_storage()
    
    def _setup_logging(self) -> None:
        """Setup logging for the cleaner."""
        self.logger = logging.getLogger(__name__)
    
    def _setup_stats_storage(self) -> None:
        """Setup directory for storing cleaning statistics."""
        self.stats_dir = Config.STATS_DIR / "cleaning"
        self.stats_dir.mkdir(parents=True, exist_ok=True)
    
    def _save_cleaning_stats(self, stats: Dict[str, CleaningStats], source_file: str) -> None:
        """Save cleaning statistics to a JSON file."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            stats_file = self.stats_dir / f"cleaning_stats_{timestamp}.json"
            
            # Convert CleaningStats objects to dictionaries
            stats_dict = {
                page: {
                    "original_length": s.original_length,
                    "cleaned_length": s.cleaned_length,
                    "removed_chars": s.removed_chars,
                    "removed_lines": s.removed_lines,
                    "processing_time": s.processing_time,
                    "errors": s.errors
                }
                for page, s in stats.items()
            }
            
            # Add metadata
            stats_data = {
                "source_file": source_file,
                "timestamp": timestamp,
                "total_pages": len(stats),
                "total_removed_chars": sum(s.removed_chars for s in stats.values()),
                "total_removed_lines": sum(s.removed_lines for s in stats.values()),
                "page_stats": stats_dict
            }
            
            # Save to file
            with open(stats_file, 'w') as f:
                json.dump(stats_data, f, indent=2)
            
            self.logger.info(f"Cleaning statistics saved to {stats_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving cleaning statistics: {str(e)}")
    
    def _clean_text_with_stats(self, text: str) -> tuple[str, CleaningStats]:
        """Clean text and collect statistics about the cleaning process."""
        import time
        from datetime import datetime
        
        start_time = time.time()
        original_text = text
        errors = []
        
        try:
            # Initial cleanup
            text = text.replace("\x00", "").strip()
            
            # Skip empty text
            if not text:
                return "", CleaningStats(
                    original_length=0,
                    cleaned_length=0,
                    removed_chars=0,
                    removed_lines=0,
                    processing_time=time.time() - start_time,
                    errors=errors
                )
            
            # Apply each cleaning function
            for clean_func in self.cleaning_functions:
                try:
                    text = clean_func(text)
                except Exception as e:
                    error_msg = f"Error in {clean_func.__name__}: {str(e)}"
                    self.logger.error(error_msg)
                    errors.append(error_msg)
            
            # Calculate statistics
            original_length = len(original_text)
            cleaned_length = len(text)
            removed_chars = original_length - cleaned_length
            removed_lines = len(original_text.splitlines()) - len(text.splitlines())
            
            return text, CleaningStats(
                original_length=original_length,
                cleaned_length=cleaned_length,
                removed_chars=removed_chars,
                removed_lines=removed_lines,
                processing_time=time.time() - start_time,
                errors=errors
            )
            
        except Exception as e:
            self.logger.error(f"Error in text cleaning: {str(e)}")
            errors.append(str(e))
            return original_text, CleaningStats(
                original_length=len(original_text),
                cleaned_length=len(original_text),
                removed_chars=0,
                removed_lines=0,
                processing_time=time.time() - start_time,
                errors=errors
            )
    
    def clean_text(self, text: str) -> str:
        """Clean a single text string."""
        cleaned_text, _ = self._clean_text_with_stats(text)
        return cleaned_text
    
    def clean_content(self, content: Dict[str, Union[str, List[str]]], source_file: Optional[str] = None) -> Dict[str, str]:
        """Clean content dictionary with detailed statistics."""
        cleaned_content = {}
        cleaning_stats = {}
        
        for page, page_content in content.items():
            try:
                # Handle different content types
                if not page_content or not isinstance(page_content, (str, list)):
                    cleaned_content[page] = ""
                    cleaning_stats[page] = CleaningStats(0, 0, 0, 0, 0, ["Invalid content type"])
                    continue
                
                # Convert list to string if necessary
                if isinstance(page_content, list):
                    page_content = " ".join(page_content)
                
                # Clean the text and get statistics
                cleaned_text, stats = self._clean_text_with_stats(page_content)
                cleaned_content[page] = cleaned_text
                cleaning_stats[page] = stats
                
                # Log significant changes
                if stats.removed_chars > 1000 or stats.removed_lines > 50:
                    self.logger.warning(
                        f"Significant cleaning on page {page}: "
                        f"removed {stats.removed_chars} chars, "
                        f"{stats.removed_lines} lines"
                    )
                
            except Exception as e:
                self.logger.error(f"Error cleaning page {page}: {str(e)}")
                cleaned_content[page] = str(page_content)
                cleaning_stats[page] = CleaningStats(
                    len(str(page_content)),
                    len(str(page_content)),
                    0,
                    0,
                    0,
                    [str(e)]
                )
        
        # Save statistics if source file is provided
        if source_file:
            self._save_cleaning_stats(cleaning_stats, source_file)
        
        # Log overall statistics
        total_removed_chars = sum(stats.removed_chars for stats in cleaning_stats.values())
        total_removed_lines = sum(stats.removed_lines for stats in cleaning_stats.values())
        self.logger.info(
            f"Cleaning completed: removed {total_removed_chars} chars, "
            f"{total_removed_lines} lines across {len(content)} pages"
        )
        
        return cleaned_content

def clean_text(text: str) -> str:
    """Clean a single text string (maintains backward compatibility)."""
    cleaner = TextCleaner()
    return cleaner.clean_text(text)

def cleaning_text(content: Dict[str, Union[str, List[str]]]) -> Dict[str, str]:
    """Clean content dictionary (maintains backward compatibility)."""
    cleaner = TextCleaner()
    return cleaner.clean_content(content)
   