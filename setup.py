from setuptools import setup, find_packages
import pathlib

# Read the README file
here = pathlib.Path(__file__).parent
readme = (here / "README.md").read_text(encoding="utf-8")

setup(
    name="motobot",
    version="1.0.0",
    description="A bot for processing motorcycle manuals and extracting information",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/vivekmolleti/motobot",
    packages=find_packages(),
    package_data={
        "motobot": ["*.json", "*.txt"],
    },
    install_requires=[
        # Core dependencies
        "Pillow>=9.5.0",
        "numpy>=1.24.0",
        "PyMuPDF>=1.22.5",
        "PyPDF2>=3.0.0",
        "pypdfium2>=4.16.0",
        "python-dotenv>=1.0.0",
        
        # Logging and monitoring
        "loguru>=0.7.0",
        
        # Type checking
        "typing-extensions>=4.5.0",
        
        # Data processing
        "pandas>=2.0.0",
        "tqdm>=4.65.0",
    ],
    extras_require={
        "dev": [
            # Testing
            "pytest>=7.3.1",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.10.0",
            
            # Code quality
            "black>=23.3.0",
            "flake8>=6.0.0",
            "isort>=5.12.0",
            "mypy>=1.3.0",
            
            # Documentation
            "mkdocs>=1.4.3",
            "mkdocs-material>=9.1.8",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Text Processing :: General",
        "Topic :: Utilities",
    ],
    entry_points={
        "console_scripts": [
            "motobot=main:main",
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/vivekmolleti/motobot/issues",
        "Source": "https://github.com/vivekmolleti/motobot",
    },
) 