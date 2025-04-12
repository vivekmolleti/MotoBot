from collections import defaultdict
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union
import logging
from pathlib import Path
from datetime import datetime
from config import Config

@dataclass
class PDFSection:
    """Represents a section of a PDF document."""
    heading: str
    start_page: int
    end_page: int
    text: str
    model: str
    year: str
    source_pdf: str
    drawings: List[Dict[str, Union[str, int, float]]] = None

    def __post_init__(self):
        if self.drawings is None:
            self.drawings = []

@dataclass
class PDFMetadata:
    """Represents metadata extracted from a PDF filename."""
    family: str
    year: Optional[str]
    model_name: str
    original_name: str

class PDFLoader:
    """Handles loading and processing of PDF documents."""
    
    def __init__(self):
        self._setup_logging()
        self.ducati_families = [
            'DesertX', 'Diavel', 'Hypermotard', 'Monster', 'Multistrada',
            'Off-Road', 'Panigale', 'Scrambler', 'Streetfighter',
            'Supersport', 'Superbike', 'XDiavel'
        ]
        self._name_cleanup_patterns = [
            'OM', '_', '.pdf', '-', 'EN', 'ED00', 'ED01', 'ED02', 'ED03',
            'Rev01', 'Rev02'
        ]
    
    def _setup_logging(self) -> None:
        """Setup logging for the PDF loader."""
        self.logger = logging.getLogger(__name__)
    
    def extract_toc(self, pypdf_file) -> Dict[str, int]:
        """
        Extract table of contents from PDF.
        
        Args:
            pypdf_file: PyPDF2 or PyMuPDF file object
            
        Returns:
            Dictionary mapping section titles to page numbers
        """
        try:
            toc = {}
            pdf_toc = pypdf_file.get_toc()
            
            if pdf_toc:
                for entry in pdf_toc:
                    if len(entry) >= 3:  # Level, title, page
                        title = entry[1]
                        page = entry[2]
                        toc[title] = page
                self.logger.info(f"Extracted TOC with {len(toc)} entries")
            else:
                self.logger.warning("No TOC found in PDF")
            
            return toc
            
        except Exception as e:
            self.logger.error(f"Error extracting TOC: {str(e)}")
            return {}

    def extract_page_content(self, pypdf_file) -> Dict[int, str]:
        """
        Extract text content from each page of the PDF.
        
        Args:
            pypdf_file: PyPDF2 or PyMuPDF file object
            
        Returns:
            Dictionary mapping page numbers to their content
        """
        try:
            content = {}
            total_pages = len(pypdf_file.pages)
            
            for i in range(total_pages):
                try:
                    content[i+1] = pypdf_file.pages[i].extract_text()
                except Exception as e:
                    self.logger.error(f"Error extracting content from page {i+1}: {str(e)}")
                    content[i+1] = ""
            
            self.logger.info(f"Extracted content from {total_pages} pages")
            return content
            
        except Exception as e:
            self.logger.error(f"Error extracting page content: {str(e)}")
            return {}

    def organize_by_section(
        self,
        toc_map: Dict[str, int],
        cleaned_content: Dict[int, str],
        year: str,
        model: str,
        pdf_name: str
    ) -> List[PDFSection]:
        """
        Organize PDF content into sections based on table of contents.
        
        Args:
            toc_map: Dictionary mapping section titles to page numbers
            cleaned_content: Dictionary mapping page numbers to cleaned content
            year: Year of the document
            model: Model name
            pdf_name: Original PDF filename
            
        Returns:
            List of PDFSection objects
        """
        try:
            if not toc_map:
                self.logger.warning("No TOC available for section organization")
                return []
            
            sorted_toc = sorted(toc_map.items(), key=lambda x: x[1])
            section_chunks = []
            
            for i in range(len(sorted_toc)):
                heading, start_page = sorted_toc[i]
                end_page = sorted_toc[i + 1][1] - 1 if i + 1 < len(sorted_toc) else len(cleaned_content)
                
                # Validate page numbers
                if start_page < 1 or end_page > len(cleaned_content):
                    self.logger.warning(
                        f"Invalid page range for section '{heading}': "
                        f"start={start_page}, end={end_page}"
                    )
                    continue
                
                # Collect text between start and end page
                section_text = ""
                for p in range(start_page, end_page + 1):
                    section_text += cleaned_content.get(p, "") + "\n"
                
                section = PDFSection(
                    heading=heading,
                    start_page=start_page,
                    end_page=end_page,
                    text=section_text.strip(),
                    model=model,
                    year=year,
                    source_pdf=pdf_name
                )
                section_chunks.append(section)
            
            self.logger.info(f"Organized content into {len(section_chunks)} sections")
            return section_chunks
            
        except Exception as e:
            self.logger.error(f"Error organizing content by section: {str(e)}")
            return []

    def extract_metadata(self, pdf_name: str) -> PDFMetadata:
        """
        Extract metadata from PDF filename.
        
        Args:
            pdf_name: Name of the PDF file
            
        Returns:
            PDFMetadata object containing extracted information
        """
        try:
            # Clean the filename
            cleaned_name = pdf_name
            for pattern in self._name_cleanup_patterns:
                cleaned_name = cleaned_name.replace(pattern, '')
            cleaned_name = cleaned_name.strip()
            
            # Find family
            family = None
            for f in self.ducati_families:
                if f in cleaned_name:
                    family = f
                    break
            
            if not family:
                self.logger.warning(f"Could not identify family from filename: {pdf_name}")
                return PDFMetadata(None, None, cleaned_name, pdf_name)
            
            # Extract year
            year = None
            year_match = re.search(r'MY([0-9][0-9])', cleaned_name)
            if year_match:
                year_str = year_match.group(1)
                year = f"19{year_str}" if int(year_str) > 50 else f"20{year_str}"
                cleaned_name = cleaned_name.replace(year_match.group(0), '').strip()
            
            return PDFMetadata(
                family=family,
                year=year,
                model_name=cleaned_name,
                original_name=pdf_name
            )
            
        except Exception as e:
            self.logger.error(f"Error extracting metadata from filename: {str(e)}")
            return PDFMetadata(None, None, pdf_name, pdf_name)

    def map_drawings_to_sections(
        self,
        section_chunks: List[PDFSection],
        drawings: List[Dict[str, Union[str, int, float]]]
    ) -> List[PDFSection]:
        """
        Map drawings to their corresponding sections.
        
        Args:
            section_chunks: List of PDFSection objects
            drawings: List of drawing dictionaries
            
        Returns:
            Updated list of PDFSection objects with drawings mapped
        """
        try:
            for section in section_chunks:
                section.drawings = []
                
                for drawing in drawings:
                    if (
                        drawing["model"] == section.model and
                        drawing["year"] == section.year and
                        drawing["source_pdf"] == section.source_pdf and
                        section.start_page <= drawing["page_number"] <= section.end_page
                    ):
                        section.drawings.append({
                            "drawing_id": drawing["drawing_id"],
                            "page_number": drawing["page_number"],
                            "image_path": drawing["image_path"],
                            "x": drawing["x"],
                            "y": drawing["y"],
                            "w": drawing["w"],
                            "h": drawing["h"]
                        })
            
            self.logger.info(f"Mapped {len(drawings)} drawings to sections")
            return section_chunks
            
        except Exception as e:
            self.logger.error(f"Error mapping drawings to sections: {str(e)}")
            return section_chunks

# Backward compatibility functions
def extract_toc(pypdf_file):
    """Backward compatibility wrapper for extract_toc."""
    loader = PDFLoader()
    return loader.extract_toc(pypdf_file)

def extract_page_content(pypdf_file):
    """Backward compatibility wrapper for extract_page_content."""
    loader = PDFLoader()
    return loader.extract_page_content(pypdf_file)

def organize_by_section(toc_map, cleaned_content, year, model, pdf_name):
    """Backward compatibility wrapper for organize_by_section."""
    loader = PDFLoader()
    return loader.organize_by_section(toc_map, cleaned_content, year, model, pdf_name)

def extracting_name_family_year(pdf_name):
    """Backward compatibility wrapper for extract_metadata."""
    loader = PDFLoader()
    metadata = loader.extract_metadata(pdf_name)
    return metadata.family, metadata.year, metadata.model_name

def map_drawings_to_sections(section_chunks, drawings):
    """Backward compatibility wrapper for map_drawings_to_sections."""
    loader = PDFLoader()
    return loader.map_drawings_to_sections(section_chunks, drawings)
