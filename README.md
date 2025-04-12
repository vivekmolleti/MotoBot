# MotoBot

A Python application for processing motorcycle manuals and extracting structured information. MotoBot specializes in processing Ducati motorcycle manuals, extracting diagrams, organizing content, and providing detailed statistics about the processing.

## Features

- **PDF Processing**
  - Table of contents extraction
  - Content organization by sections
  - Metadata extraction (family, year, model)
  - Page-by-page content extraction

- **Image Processing**
  - Diagram extraction from PDFs
  - Image quality optimization
  - Automatic image mapping to sections

- **Text Processing**
  - Advanced text cleaning
  - Content chunking and organization
  - Section-based content mapping
  - Unicode and special character handling

- **Performance & Monitoring**
  - Parallel processing support
  - Intelligent caching system
  - Comprehensive statistics tracking
  - Detailed error logging and reporting

- **Data Management**
  - Structured data organization
  - JSON output format
  - Statistics storage and analysis
  - Cache management

## Installation

1. Clone the repository:
```bash
git clone https://github.com/vivekmolleti/motobot.git
cd motobot
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project root with the following variables:

```env
# Base paths
LIB_PATH=C:\path\to\your\pdf\directory

# Logging
LOG_LEVEL=INFO
LOG_MAX_BYTES=10485760  # 10MB
LOG_BACKUP_COUNT=5

# Processing
BATCH_SIZE=5
MAX_WORKERS=4
CACHE_ENABLED=true

# PDF Processing
MAX_PDF_SIZE=104857600  # 100MB
PDF_PROCESSING_TIMEOUT=300  # 5 minutes

# Image Processing
MAX_IMAGE_SIZE=10485760  # 10MB
IMAGE_QUALITY=85

# Error Handling
MAX_RETRIES=3
RETRY_DELAY=5  # seconds
```

## Usage

### Basic Usage

Run the application:
```bash
python main.py
```

### Output

The application generates:
- Processed content in `output/final_grouped_family.json`
- Statistics in `stats/` directory
- Logs in `logs/` directory
- Cache files in `cache/` directory

### Statistics

Processing statistics are saved in JSON format with:
- Total files processed
- Success/failure counts
- Page and drawing counts
- Processing times
- Error details

## Project Structure

```
motobot/
├── config.py           # Configuration settings
├── main.py            # Main application entry point
├── requirements.txt   # Project dependencies
├── setup.py          # Package installation configuration
├── logs/             # Log files
├── output/           # Output files
├── cache/            # Cache files
├── stats/            # Processing statistics
│   ├── cleaning/     # Text cleaning statistics
│   ├── chunking/     # Content chunking statistics
│   └── pdf/          # PDF processing statistics
├── Image_extraction/ # Image processing modules
├── pdf_loading/      # PDF processing modules
├── nlp/              # NLP processing modules
└── tests/            # Test files
```

## Development

### Running Tests

```bash
python -m pytest tests/
```

### Code Style

This project uses:
- Black for code formatting
- Flake8 for linting
- isort for import sorting
- mypy for type checking

Run code style checks:
```bash
black .
flake8
isort .
mypy .
```

### Adding New Features

1. Create a new branch:
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes and commit:
```bash
git add .
git commit -m "Description of your changes"
```

3. Push and create a pull request:
```bash
git push origin feature/your-feature-name
```

## Error Handling

The application includes comprehensive error handling:
- Automatic retries for failed operations
- Detailed error logging
- Graceful degradation
- Error statistics tracking

## Performance Optimization

- Parallel processing with configurable batch sizes
- Intelligent caching system
- Memory-efficient processing
- Configurable timeouts and limits

## License

MIT License - see LICENSE file for details

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Support

For support, please open an issue in the GitHub repository. 

