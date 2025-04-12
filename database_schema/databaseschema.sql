-- Create Companies Table
CREATE TABLE Companies (
    company_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name VARCHAR(100) NOT NULL,
    company_description VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create PDF Families Table
CREATE TABLE PDFFamilies (
    family_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID NOT NULL,
    family_name VARCHAR(100) NOT NULL,
    family_description VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (company_id) REFERENCES Companies(company_id) ON DELETE CASCADE
);

-- Documents Table
CREATE TABLE Documents (
    document_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    family_id UUID NOT NULL,
    document_name VARCHAR(255) NOT NULL,
    original_filename VARCHAR(255) NOT NULL,
    blob_url VARCHAR(500) NULL,
    blob_container VARCHAR(100) NULL,
    blob_path VARCHAR(500)  NULL,
    file_size BIGINT,
    file_type VARCHAR(50),
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_accessed TIMESTAMP,
    content_hash VARCHAR(128),
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (family_id) REFERENCES PDFFamilies(family_id) ON DELETE CASCADE
);

-- Document Text Chunks Table (for RAG)
CREATE TABLE DocumentChunks (
    chunk_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL,
    chunk_index INTEGER NOT NULL,
    chunk_text TEXT NOT NULL,
    page_number INTEGER NOT NULL,
    position_data JSONB, -- Contains location info like {x, y, width, height}
    embedding_id VARCHAR(100), -- Reference to vector DB
    embedding_model VARCHAR(100), -- Model used for embedding
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES Documents(document_id) ON DELETE CASCADE
);

-- Document Images Table (for Multimodal RAG)
CREATE TABLE DocumentImages (
    image_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL,
    page_number INTEGER NOT NULL,
    image_path VARCHAR(500) NOT NULL, -- Path to stored image
    image_url VARCHAR(500), -- URL to access image
    image_caption TEXT, -- Generated caption for the image
    width INTEGER,
    height INTEGER,
    position_data JSONB, -- Contains location info like {x, y, width, height}
    embedding_id VARCHAR(100), -- Reference to vector DB for image embeddings
    embedding_model VARCHAR(100), -- Model used for embedding
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES Documents(document_id) ON DELETE CASCADE
);

-- Document Summaries Table
CREATE TABLE DocumentSummaries (
    summary_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL,
    summary_text TEXT NOT NULL,
    summary_type VARCHAR(50) NOT NULL, -- 'executive', 'detailed', 'page_level', etc.
    page_number INTEGER, -- NULL for document-level summaries
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES Documents(document_id) ON DELETE CASCADE
);

-- RAG Queries Table (for tracking and improvement)
CREATE TABLE RAGQueries (
    query_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_query TEXT NOT NULL,
    processed_query TEXT,
    response TEXT,
    chunks_used JSONB, -- Array of chunk_ids used in the response
    images_used JSONB, -- Array of image_ids used in the response
    feedback_score INTEGER, -- Optional user feedback
    query_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_pdffamilies_company_id ON PDFFamilies(company_id);
CREATE INDEX idx_documents_family_id ON Documents(family_id);
CREATE INDEX idx_documentchunks_document_id ON DocumentChunks(document_id);
CREATE INDEX idx_documentimages_document_id ON DocumentImages(document_id);
CREATE INDEX idx_documentsummaries_document_id ON DocumentSummaries(document_id);
CREATE INDEX idx_documentchunks_embedding_id ON DocumentChunks(embedding_id);
CREATE INDEX idx_documentimages_embedding_id ON DocumentImages(embedding_id);

-- Add GIN index for JSONB fields to support efficient queries
CREATE INDEX idx_document_metadata ON Documents USING GIN (metadata);
CREATE INDEX idx_documentchunks_position ON DocumentChunks USING GIN (position_data);
CREATE INDEX idx_documentimages_position ON DocumentImages USING GIN (position_data);

-- Create triggers for automatically updating timestamps
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_companies_modified
BEFORE UPDATE ON Companies
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_pdffamilies_modified
BEFORE UPDATE ON PDFFamilies
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_documents_modified
BEFORE UPDATE ON Documents
FOR EACH ROW EXECUTE FUNCTION update_modified_column();