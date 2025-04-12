import cv2
import io
import numpy as np
from PIL import Image
import os
from pathlib import Path
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from config import Config

@dataclass
class DiagramInfo:
    drawing_id: str
    x: int
    y: int
    width: int
    height: int
    page_number: int
    image_path: Path
    model: str
    year: str
    source_pdf: str

class ImageExtractor:
    def __init__(self):
        self.images_dir = Config.IMAGES_DIR
        self.min_area = 30000  # Minimum area for diagram detection
        self.aspect_ratio_range = (0.5, 2.0)  # Acceptable aspect ratio range
        self.padding = 15  # Padding around detected diagrams
        
    def _create_model_directory(self, model: str) -> Path:
        """Create directory for model-specific images if it doesn't exist."""
        model_path = self.images_dir / model
        model_path.mkdir(parents=True, exist_ok=True)
        return model_path
    
    def _preprocess_image(self, cv_img: np.ndarray) -> np.ndarray:
        """Preprocess image for diagram detection."""
        try:
            # Convert to grayscale
            gray = cv2.cvtColor(cv_img, cv2.COLOR_RGB2GRAY)
            
            # Apply adaptive thresholding
            thresh = cv2.adaptiveThreshold(
                gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY_INV, 11, 2
            )
            
            # Apply morphological operations
            kernel = np.ones((3, 3), np.uint8)
            dilated = cv2.dilate(thresh, kernel, iterations=2)
            
            return dilated
        except Exception as e:
            logging.error(f"Error in image preprocessing: {str(e)}")
            raise
    
    def _find_diagrams(self, preprocessed_img: np.ndarray, cv_img: np.ndarray) -> List[Dict[str, Any]]:
        """Find potential diagrams in the preprocessed image."""
        try:
            contours, _ = cv2.findContours(
                preprocessed_img, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE
            )
            
            potential_diagrams = []
            for cnt in contours:
                area = cv2.contourArea(cnt)
                if area > self.min_area:
                    x, y, w, h = cv2.boundingRect(cnt)
                    aspect_ratio = float(w) / h
                    
                    if self.aspect_ratio_range[0] < aspect_ratio < self.aspect_ratio_range[1]:
                        potential_diagrams.append({
                            'x': x, 'y': y, 'w': w, 'h': h,
                            'area': area, 'contour': cnt
                        })
            
            # Sort by area (largest first)
            return sorted(potential_diagrams, key=lambda x: x['area'], reverse=True)
        except Exception as e:
            logging.error(f"Error in diagram detection: {str(e)}")
            raise
    
    def _extract_diagram(self, cv_img: np.ndarray, diagram_info: Dict[str, Any], 
                        model: str, year: str, page_num: int, count: int) -> DiagramInfo:
        """Extract and save a single diagram."""
        try:
            x, y, w, h = diagram_info['x'], diagram_info['y'], diagram_info['w'], diagram_info['h']
            
            # Apply padding with boundary checks
            x_pad = max(0, x - self.padding)
            y_pad = max(0, y - self.padding)
            w_pad = min(cv_img.shape[1] - x_pad, w + 2 * self.padding)
            h_pad = min(cv_img.shape[0] - y_pad, h + 2 * self.padding)
            
            # Extract diagram
            diagram = cv_img[y_pad:y_pad+h_pad, x_pad:x_pad+w_pad]
            
            # Convert to PIL Image and save
            pil_image = Image.fromarray(diagram)
            model_path = self._create_model_directory(model)
            image_path = model_path / f"{model}_{year}_pg{page_num}_{count}.png"
            pil_image.save(image_path, "PNG")
            
            return DiagramInfo(
                drawing_id=f"{model}_{year}_diagram_pg{page_num}_{count}",
                x=x_pad,
                y=y_pad,
                width=w_pad,
                height=h_pad,
                page_number=page_num,
                image_path=image_path,
                model=model,
                year=year,
                source_pdf=str(image_path)
            )
        except Exception as e:
            logging.error(f"Error extracting diagram: {str(e)}")
            raise
    
    def extract_diagrams_from_pdf(self, doc, model: str, year: str, pdf_name: str) -> List[DiagramInfo]:
        """Extract diagrams from a PDF document."""
        drawings = []
        
        try:
            for page_num, page in enumerate(doc):
                # Render page to numpy array
                cv_img = page.render(scale=2.0).to_numpy()
                
                # Preprocess image
                preprocessed = self._preprocess_image(cv_img)
                
                # Find potential diagrams
                potential_diagrams = self._find_diagrams(preprocessed, cv_img)
                
                # Process each found diagram
                for count, diagram_info in enumerate(potential_diagrams, 1):
                    try:
                        diagram = self._extract_diagram(
                            cv_img, diagram_info, model, year, page_num, count
                        )
                        drawings.append(diagram)
                    except Exception as e:
                        logging.error(f"Error processing diagram {count} on page {page_num}: {str(e)}")
                        continue
                        
        except Exception as e:
            logging.error(f"Error processing PDF {pdf_name}: {str(e)}")
            raise
            
        return drawings

def extract_diagram_from_pdf(doc, model: str, year: str, pdf_name: str) -> List[Dict[str, Any]]:
    """Main function to extract diagrams from PDF (maintains backward compatibility)."""
    extractor = ImageExtractor()
    diagrams = extractor.extract_diagrams_from_pdf(doc, model, year, pdf_name)
    
    # Convert DiagramInfo objects to dictionaries for backward compatibility
    return [
        {
            "drawing_id": d.drawing_id,
            "x": d.x,
            "y": d.y,
            "w": d.width,
            "h": d.height,
            "page_number": d.page_number,
            "image_path": str(d.image_path),
            "model": d.model,
            "year": d.year,
            "source_pdf": d.source_pdf,
        }
        for d in diagrams
    ]
    




