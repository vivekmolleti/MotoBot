import psycopg2
import logging
from typing import Dict, List, Any
from config import Config
from database_schema.cosmos_manager import CosmosManager
from pathlib import Path
import json
import pypdfium2 as pdfium
from azure.storage.blob import BlobServiceClient


def blob_client() -> BlobServiceClient:
    """
    Create a BlobServiceClient instance to interact with Azure Blob Storage.
    """
    blob_service_client = BlobServiceClient.from_connection_string(Config.AZURE_STORAGE_CONNECTION_STRING)
    return blob_service_client

def get_blob_pdf_list(container_name: str, blob_name: str) -> bytes:
    """
    Retrieve the content of a blob from Azure Blob Storage.
    """
    try:
        blob_service_client = blob_client()
        conatiner_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_pdf_names = [blob.name for blob in conatiner_client.list_blobs()]
        return blob_pdf_names
    except Exception as e:
        logging.error(f"Error retrieving blob data: {e}")
        raise e

def get_blob_pdf_data(container_name: str, blob_name: str) -> bytes:
    """
    Retrieve the content of a blob from Azure Blob Storage.

    Args:
        container_name (str): The name of the container.
        blob_name (str): The name of the blob.

    Returns:
        bytes: The content of the blob as bytes.
    """
    try:
        blob_service_client = blob_client()
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_data = blob_client.download_blob().readall()
        return blob_data
    except Exception as e:
        logging.error(f"Error retrieving blob data: {e}")
        raise e


