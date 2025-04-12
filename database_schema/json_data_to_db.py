import json
from datetime import datetime
import logging

from psycopg2.extras import RealDictCursor

def json_to_csv(json_path, conn, company_id):
    """Load JSON data into the database"""
    # Get current timestamp
    now = datetime.now().isoformat()
    
    with open(json_path, 'r', encoding="utf-8") as f:
        pdf_data = json.load(f)
    
        for family, chunks in pdf_data.items():
            chunk_count = len(chunks)
            for chunk in chunks:

                try:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        get_sql = """
                                INSERT INTO PDFfamilies (company_id, family_name, family_description)
                                VALUES (%s, %s, %s)
                                RETURNING family_id, company_id, family_name, family_description,  created_at, updated_at
                            """
                        cur.execute(get_sql, (company_id, family, None))
                        conn.commit()

                except Exception as e:
                    logging.error(f"Error inserting PDFfamily data: {e}")
                    conn.rollback()


                #reading family_id from PDFfamilies table to insert into documents table
                try:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        get_sql = """
                                SELECT family_id
                                FROM PDFfamilies
                                WHERE family_name = %s AND company_id = %s;
                            """
                        cur.execute(get_sql, (family, company_id))
                        family_id = cur.fetchone()['family_id']
                        logging.info(f"Family ID for '{family}': {family_id}")

                except Exception as e:
                    logging.error(f"Error inserting family data: {e}")
                    conn.rollback()

                #writing documents data documents table
                chunk_metadata = json.dumps(
                            {
                                "family": family,
                                "year": chunk['year'],
                                "chunk_count": chunk_count,
                                }
                        )
                #Inserting data into documents table
                try:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        get_sql = """
                                INSERT INTO documents (family_id, document_name, original_filename, blob_url, blob_container, blob_path, file_size, file_type, last_accessed, content_hash, metadata)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                RETURNING document_id, family_id, document_name, original_filename, blob_url, blob_container, blob_path, file_size, file_type, upload_date, last_accessed, content_hash, metadata, created_at, updated_at
                                """
                        cur.execute(get_sql, (family_id, chunk["model"], chunk['source_pdf'], None, None, None, None, "document/pdf", now, None, chunk_metadata))
                        conn.commit()
                        logging.info(f"Inserted data into documents table for family '{family}'")
                except Exception as e:
                    logging.error(f"Error inserting data into documents table: {e}")
                    conn.rollback()


                #Fetch the document_id for the inserted document
                try:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        get_sql = """
                                SELECT document_id
                                FROM Documents
                                WHERE family_id = %s AND document_name = %s;
                            """
                        cur.execute(get_sql, (family_id, chunk["model"]))
                        document_id = cur.fetchone()['document_id']
                        logging.info(f"Document ID for '{chunk['model']}': {document_id}")
                except Exception as e:
                    logging.error(f"Error fetching document ID: {e}")
                    conn.rollback()            
                
                #writing chunks data into DocumentChunks table
                position_data = json.dumps({
                        'start_page': chunk['start_page'],
                        'end_page': chunk['end_page'],
                        'heading': chunk['heading']
                        })
                
                try:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur: 
                        get_sql = """
                                INSERT INTO DocumentChunks (document_id, chunk_index, chunk_text, page_number, position_data, embedding_id, embedding_model)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                RETURNING chunk_id, document_id, chunk_index, chunk_text, page_number, position_data, embedding_id, embedding_model, created_at
                                """
                        cur.execute(get_sql, (document_id, chunk['heading'], chunk['chunk_text'], None, position_data, None, None))
                        conn.commit()
                        logging.info(f"Inserted data into DocumentChunks table for family '{family}'")
                except Exception as e:
                    logging.error(f"Error inserting data into DocumentChunks table: {e}")
                    conn.rollback()
                

                #Inserting image data into DocumentImages table
                try:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        get_sql = """
                                INSERT INTO DocumentImages (document_id, page_number, image_path, image_url, image_caption, width, height, position_data, embedding_id, embedding_model)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                RETURNING image_id, document_id, page_number, image_path, image_url, image_caption, width, height, position_data, embedding_id, embedding_model, created_at
                                """
                        for i, drawing in enumerate(chunk['drawings']):
                            position_data = json.dumps({
                            'x': drawing['x'],
                            'y': drawing['y'],
                            'width': drawing['w'],
                            'height': drawing['h'],
                            })
                            cur.execute(get_sql,(document_id,drawing['page_number'],drawing['image_path'],None,None,None,None,position_data,None,None))
                        conn.commit()
                        logging.info(f"Inserted data into DocumentImages table for family '{family}'")
                except Exception as e:
                    logging.error(f"Error inserting data into DocumentImages table: {e}")   
                    conn.rollback()


