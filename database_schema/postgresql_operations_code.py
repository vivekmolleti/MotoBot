import psycopg2
from psycopg2.extras import RealDictCursor, register_uuid
from datetime import datetime
import logging

# Register UUID type for PostgreSQL

    
def connect():
    """Connect to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(host="localhost",port="5432", database="motobot", user="postgres", password="Ironman#99")
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error connecting to PostgreSQL: {error}")
        return False
    
        
def close(self):
    """Close the database connection"""
    if self.conn is not None:
        self.conn.close()
        
# Company operations
def extract_company_id(conn):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            get_sql = """
                    SELECT company_id
                    FROM Companies
                    WHERE company_name = 'Ducati';
                """
            cur.execute(get_sql)
            company_id = cur.fetchone()['company_id']
            logging.info(f"Company ID for 'Ducati': {company_id}")
    except Exception as e:
        logging.error(f"Error fetching company ID: {e}")
        return None
    return company_id


        
# def update_company(self, company_id, company_name=None, description=None):
#     """Update an existing company"""
#     try:
#         cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
#         # Get current values if new ones not provided
#         if company_name is None or description is None:
#             get_sql = """
#                 SELECT company_name, company_description
#                 FROM Companies
#                 WHERE company_id = %s;
#             """
#             cursor.execute(get_sql, (company_id,))
#             current = cursor.fetchone()
            
#             if current is None:
#                 print(f"Company with ID {company_id} not found")
#                 cursor.close()
#                 return None
                
#             company_name = company_name if company_name is not None else current['company_name']
#             description = description if description is not None else current['company_description']
        
#         sql = """
#             UPDATE Companies
#             SET company_name = %s, company_description = %s
#             WHERE company_id = %s
#             RETURNING company_id, company_name, company_description, created_at, updated_at;
#         """
#         cursor.execute(sql, (company_name, description, company_id))
#         company = cursor.fetchone()
#         self.conn.commit()
#         cursor.close()
#         return company
#     except (Exception, psycopg2.DatabaseError) as error:
#         self.conn.rollback()
#         print(f"Error updating company: {error}")
#         return None

# # PDF Family operations
# def create_pdf_family(self, company_id, family_name, description=None):
#     """Create a new PDF family"""
#     try:
#         cursor = self.conn.cursor(cursor_factory=RealDictCursor)
#         sql = """
#             INSERT INTO PDFFamilies (company_id, family_name)
#             VALUES (%s, %s)
#             RETURNING family_id, company_id, family_name, family_description, created_at, updated_at;
#         """
#         cursor.execute(sql, (company_id, family_name, description))
#         family = cursor.fetchone()
#         self.conn.commit()
#         cursor.close()
#         return family
#     except (Exception, psycopg2.DatabaseError) as error:
#         self.conn.rollback()
#         print(f"Error creating PDF family: {error}")
#         return None
        
# def update_pdf_family(self, family_id, family_name=None, description=None):
#     """Update an existing PDF family"""
#     try:
#         cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
#         # Get current values if new ones not provided
#         if family_name is None or description is None:
#             get_sql = """
#                 SELECT family_name, family_description
#                 FROM PDFFamilies
#                 WHERE family_id = %s;
#             """
#             cursor.execute(get_sql, (family_id,))
#             current = cursor.fetchone()
            
#             if current is None:
#                 print(f"PDF Family with ID {family_id} not found")
#                 cursor.close()
#                 return None
                
#             family_name = family_name if family_name is not None else current['family_name']
#             description = description if description is not None else current['family_description']
        
#         sql = """
#             UPDATE PDFFamilies
#             SET family_name = %s, family_description = %s
#             WHERE family_id = %s
#             RETURNING family_id, company_id, family_name, family_description, created_at, updated_at;
#         """
#         cursor.execute(sql, (family_name, description, family_id))
#         family = cursor.fetchone()
#         self.conn.commit()
#         cursor.close()
#         return family
#     except (Exception, psycopg2.DatabaseError) as error:
#         self.conn.rollback()
#         print(f"Error updating PDF family: {error}")
#         return None

# # Subcategory operations
# def create_subcategory(self, family_id, subcategory_name, description=None):
#     """Create a new subcategory"""
#     try:
#         cursor = self.conn.cursor(cursor_factory=RealDictCursor)
#         sql = """
#             INSERT INTO Subcategories (family_id, subcategory_name, subcategory_description)
#             VALUES (%s, %s, %s)
#             RETURNING subcategory_id, family_id, subcategory_name, subcategory_description, created_at, updated_at;
#         """
#         cursor.execute(sql, (family_id, subcategory_name, description))
#         subcategory = cursor.fetchone()
#         self.conn.commit()
#         cursor.close()
#         return subcategory
#     except (Exception, psycopg2.DatabaseError) as error:
#         self.conn.rollback()
#         print(f"Error creating subcategory: {error}")
#         return None
        
# def update_subcategory(self, subcategory_id, subcategory_name=None, description=None):
#     """Update an existing subcategory"""
#     try:
#         cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
#         # Get current values if new ones not provided
#         if subcategory_name is None or description is None:
#             get_sql = """
#                 SELECT subcategory_name, subcategory_description
#                 FROM Subcategories
#                 WHERE subcategory_id = %s;
#             """
#             cursor.execute(get_sql, (subcategory_id,))
#             current = cursor.fetchone()
            
#             if current is None:
#                 print(f"Subcategory with ID {subcategory_id} not found")
#                 cursor.close()
#                 return None
                
#             subcategory_name = subcategory_name if subcategory_name is not None else current['subcategory_name']
#             description = description if description is not None else current['subcategory_description']
        
#         sql = """
#             UPDATE Subcategories
#             SET subcategory_name = %s, subcategory_description = %s
#             WHERE subcategory_id = %s
#             RETURNING subcategory_id, family_id, subcategory_name, subcategory_description, created_at, updated_at;
#         """
#         cursor.execute(sql, (subcategory_name, description, subcategory_id))
#         subcategory = cursor.fetchone()
#         self.conn.commit()
#         cursor.close()
#         return subcategory
#     except (Exception, psycopg2.DatabaseError) as error:
#         self.conn.rollback()
#         print(f"Error updating subcategory: {error}")
#         return None

# # Document operations
# def create_document(self, subcategory_id, document_name, original_filename, 
#                 blob_url, blob_container, blob_path, file_size=None, 
#                 file_type=None, content_hash=None, metadata=None):
#     """Create a new document"""
#     try:
#         cursor = self.conn.cursor(cursor_factory=RealDictCursor)
#         # Convert metadata to JSON if provided
#         metadata_json = json.dumps(metadata) if metadata else None
        
#         sql = """
#             INSERT INTO Documents (
#                 subcategory_id, document_name, original_filename, blob_url, 
#                 blob_container, blob_path, file_size, file_type, 
#                 content_hash, metadata
#             )
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#             RETURNING document_id, subcategory_id, document_name, original_filename, 
#                 blob_url, blob_container, blob_path, file_size, file_type, 
#                 upload_date, last_accessed, content_hash, metadata, created_at, updated_at;
#         """
#         cursor.execute(sql, (
#             subcategory_id, document_name, original_filename, blob_url, 
#             blob_container, blob_path, file_size, file_type, 
#             content_hash, metadata_json
#         ))
#         document = cursor.fetchone()
#         self.conn.commit()
#         cursor.close()
#         return document
#     except (Exception, psycopg2.DatabaseError) as error:
#         self.conn.rollback()
#         print(f"Error creating document: {error}")
#         return None
        
# def update_document(self, document_id, document_name=None, blob_url=None, 
#                   file_size=None, last_accessed=None, metadata=None):
#     """Update an existing document"""
#     try:
#         cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
#         # Get current values if new ones not provided
#         get_sql = """
#             SELECT document_name, blob_url, file_size, metadata
#             FROM Documents
#             WHERE document_id = %s;
#         """
#         cursor.execute(get_sql, (document_id,))
#         current = cursor.fetchone()
        
#         if current is None:
#             print(f"Document with ID {document_id} not found")
#             cursor.close()
#             return None
            
#         document_name = document_name if document_name is not None else current['document_name']
#         blob_url = blob_url if blob_url is not None else current['blob_url']
#         file_size = file_size if file_size is not None else current['file_size']
        
#         # Handle metadata merge if needed
#         if metadata is not None:
#             current_metadata = current['metadata'] or {}
#             if isinstance(current_metadata, str):
#                 current_metadata = json.loads(current_metadata)
#             merged_metadata = {**current_metadata, **metadata}
#             metadata_json = json.dumps(merged_metadata)
#         else:
#             metadata_json = current['metadata']
        
#         # Update last_accessed if specified
#         last_accessed_value = "NOW()" if last_accessed is True else "last_accessed"
        
#         sql = f"""
#             UPDATE Documents
#             SET document_name = %s, 
#                 blob_url = %s, 
#                 file_size = %s, 
#                 last_accessed = {last_accessed_value}, 
#                 metadata = %s
#             WHERE document_id = %s
#             RETURNING document_id, subcategory_id, document_name, original_filename, 
#                 blob_url, blob_container, blob_path, file_size, file_type, 
#                 upload_date, last_accessed, content_hash, metadata, created_at, updated_at;
#         """
        
#         # Adjust parameters based on whether last_accessed is being updated
#         if last_accessed is True:
#             cursor.execute(sql, (document_name, blob_url, file_size, metadata_json, document_id))
#         else:
#             cursor.execute(sql, (document_name, blob_url, file_size, metadata_json, document_id))
            
#         document = cursor.fetchone()
#         self.conn.commit()
#         cursor.close()
#         return document
#     except (Exception, psycopg2.DatabaseError) as error:
#         self.conn.rollback()
#         print(f"Error updating document: {error}")
#         return None

# # Document tag operations
# def add_document_tag(self, document_id, tag_name):
#     """Add a tag to a document"""
#     try:
#         cursor = self.conn.cursor(cursor_factory=RealDictCursor)
#         sql = """
#             INSERT INTO DocumentTags (document_id, tag_name)
#             VALUES (%s, %s)
#             RETURNING tag_id, document_id, tag_name, created_at;
#         """
#         cursor.execute(sql, (document_id, tag_name))
#         tag = cursor.fetchone()
#         self.conn.commit()
#         cursor.close()
#         return tag
#     except (Exception, psycopg2.DatabaseError) as error:
#         self.conn.rollback()
#         print(f"Error adding document tag: {error}")
#         return None

# # Document version operations
# def add_document_version(self, document_id, version_number, blob_url, blob_path, uploaded_by=None):
#     """Add a new version to a document"""
#     try:
#         cursor = self.conn.cursor(cursor_factory=RealDictCursor)
#         sql = """
#             INSERT INTO DocumentVersions (document_id, version_number, blob_url, blob_path, uploaded_by)
#             VALUES (%s, %s, %s, %s, %s)
#             RETURNING version_id, document_id, version_number, blob_url, blob_path, upload_date, uploaded_by, created_at;
#         """
#         cursor.execute(sql, (document_id, version_number, blob_url, blob_path, uploaded_by))
#         version = cursor.fetchone()
#         self.conn.commit()
#         cursor.close()
#         return version
#     except (Exception, psycopg2.DatabaseError) as error:
#         self.conn.rollback()
#         print(f"Error adding document version: {error}")
#         return None

