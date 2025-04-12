import nltk
from nltk.tokenize import word_tokenize, sent_tokenize
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import logging
from pathlib import Path
from config import Config

@dataclass
class TextChunk:
    chunk_text: str
    heading: str
    start_page: int
    end_page: int
    model: str
    year: str
    source_pdf: str
    word_count: int
    sentence_count: int
    chunk_index: int
    total_chunks: int

class TextChunker:
    def __init__(self):
        self.max_words = Config.CHUNK_MAX_WORDS
        self.overlap_words = Config.CHUNK_OVERLAP_WORDS
        self.min_chunk_size = Config.CHUNK_MIN_SIZE
        self._setup_nltk()
    
    def _setup_nltk(self) -> None:
        """Setup NLTK resources."""
        try:
            nltk.data.path.append(str(Config.NLTK_DATA_DIR))
            nltk.download('punkt', download_dir=str(Config.NLTK_DATA_DIR))
        except Exception as e:
            logging.error(f"Error setting up NLTK: {str(e)}")
            raise
    
    def _count_words(self, text: str) -> int:
        """Count words in text."""
        try:
            return len(word_tokenize(text))
        except Exception as e:
            logging.error(f"Error counting words: {str(e)}")
            return 0
    
    def _count_sentences(self, text: str) -> int:
        """Count sentences in text."""
        try:
            return len(sent_tokenize(text))
        except Exception as e:
            logging.error(f"Error counting sentences: {str(e)}")
            return 0
    
    def _create_chunk(
        self,
        chunk_text: str,
        section_data: Dict[str, Any],
        chunk_index: int,
        total_chunks: int
    ) -> TextChunk:
        """Create a TextChunk object from chunk text and metadata."""
        try:
            return TextChunk(
                chunk_text=chunk_text,
                heading=section_data["heading"],
                start_page=section_data["start_page"],
                end_page=section_data["end_page"],
                model=section_data["model"],
                year=section_data["year"],
                source_pdf=section_data["source_pdf"],
                word_count=self._count_words(chunk_text),
                sentence_count=self._count_sentences(chunk_text),
                chunk_index=chunk_index,
                total_chunks=total_chunks
            )
        except Exception as e:
            logging.error(f"Error creating chunk: {str(e)}")
            raise
    
    def _chunk_section(
        self,
        section_data: Dict[str, Any],
        max_words: Optional[int] = None,
        overlap_words: Optional[int] = None
    ) -> List[TextChunk]:
        """Split a section into chunks with overlap."""
        try:
            max_words = max_words or self.max_words
            overlap_words = overlap_words or self.overlap_words
            
            text = section_data["text"]
            if not text or len(text.strip()) == 0:
                logging.warning(f"Empty text in section: {section_data.get('heading', 'Unknown')}")
                return []
            
            words = word_tokenize(text)
            if len(words) < self.min_chunk_size:
                logging.info(f"Section too small to chunk: {section_data.get('heading', 'Unknown')}")
                return [self._create_chunk(text, section_data, 0, 1)]
            
            chunks = []
            start_idx = 0
            chunk_index = 0
            
            while start_idx < len(words):
                end_idx = min(start_idx + max_words, len(words))
                chunk_words = words[start_idx:end_idx]
                chunk_text = " ".join(chunk_words)
                
                # Ensure we don't split in the middle of a sentence
                if end_idx < len(words):
                    last_sentence = sent_tokenize(chunk_text)[-1]
                    if not last_sentence.endswith(('.', '!', '?')):
                        # Find the last sentence boundary
                        sentences = sent_tokenize(" ".join(words[start_idx:]))
                        current_sentences = sent_tokenize(chunk_text)
                        if len(current_sentences) > 1:
                            # Remove the incomplete last sentence
                            chunk_text = " ".join(current_sentences[:-1])
                            # Update end_idx to match the new chunk
                            end_idx = start_idx + len(word_tokenize(chunk_text))
                
                chunk = self._create_chunk(
                    chunk_text,
                    section_data,
                    chunk_index,
                    (len(words) + max_words - 1) // max_words
                )
                chunks.append(chunk)
                
                start_idx += (max_words - overlap_words)
                chunk_index += 1
            
            return chunks
        except Exception as e:
            logging.error(f"Error chunking section: {str(e)}")
            raise
    
    def chunk(self, content: List[Dict[str, Any]]) -> List[TextChunk]:
        """Split content into chunks."""
        all_chunks = []
        
        try:
            for section in content:
                try:
                    section_chunks = self._chunk_section(section)
                    all_chunks.extend(section_chunks)
                except Exception as e:
                    logging.error(f"Error processing section: {str(e)}")
                    continue
            
            return all_chunks
        except Exception as e:
            logging.error(f"Error in chunking process: {str(e)}")
            raise

def chunk(content: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Main function to chunk content (maintains backward compatibility)."""
    chunker = TextChunker()
    chunks = chunker.chunk(content)
    
    # Convert TextChunk objects to dictionaries for backward compatibility
    return [
        {
            "chunk_text": c.chunk_text,
            "heading": c.heading,
            "start_page": c.start_page,
            "end_page": c.end_page,
            "model": c.model,
            "year": c.year,
            "source_pdf": c.source_pdf,
            "word_count": c.word_count,
            "sentence_count": c.sentence_count,
            "chunk_index": c.chunk_index,
            "total_chunks": c.total_chunks
        }
        for c in chunks
    ]






