import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from main import MotoBot, PDFDocument
from config import Config
import json

@pytest.fixture
def mock_pdf_document():
    return PDFDocument(
        family="test_family",
        year="2023",
        model="test_model",
        content={"page1": "test content"},
        drawings=["drawing1", "drawing2"],
        file_path=Path("test.pdf")
    )

@pytest.fixture
def motobot():
    return MotoBot(str(Config.LIB_PATH))

def test_motobot_initialization(motobot):
    assert motobot.lib_path == Config.LIB_PATH
    assert isinstance(motobot.grouped_family, dict)
    assert len(motobot.grouped_family) == 0

@patch('main.extracting_name_family_year')
@patch('main.extract_toc')
@patch('main.extract_page_content')
@patch('main.cleaning_text')
@patch('main.organize_by_section')
@patch('main.chunk')
@patch('main.extract_diagram_from_pdf')
@patch('main.map_drawings_to_sections')
def test_process_pdf(mock_map_drawings, mock_extract_diagram, mock_chunk,
                    mock_organize, mock_clean, mock_extract_content,
                    mock_extract_toc, mock_extract_name, motobot, mock_pdf_document):
    # Setup mocks
    mock_extract_name.return_value = ("test_family", "2023", "test_model")
    mock_extract_toc.return_value = {"toc": "test"}
    mock_extract_content.return_value = {"content": "test"}
    mock_clean.return_value = {"cleaned": "test"}
    mock_organize.return_value = {"organized": "test"}
    mock_chunk.return_value = {"chunked": "test"}
    mock_extract_diagram.return_value = ["drawing1", "drawing2"]
    mock_map_drawings.return_value = ["mapped1", "mapped2"]

    # Test process_pdf
    result = motobot.process_pdf(Path("test.pdf"))
    
    assert isinstance(result, PDFDocument)
    assert result.family == "test_family"
    assert result.year == "2023"
    assert result.model == "test_model"

def test_save_results(motobot, tmp_path):
    # Setup test data
    motobot.grouped_family = {"test": ["data1", "data2"]}
    output_file = tmp_path / "test_output.json"
    
    # Test save_results
    with patch('config.Config.OUTPUT_FILE', output_file):
        motobot.save_results()
        
    assert output_file.exists()
    with open(output_file) as f:
        data = json.load(f)
        assert data == {"test": ["data1", "data2"]} 