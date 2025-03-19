-- Enable extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- For text similarity search
CREATE EXTENSION IF NOT EXISTS unaccent; -- For handling accented characters
CREATE EXTENSION IF NOT EXISTS "uuid-ossp"; -- For UUID generation

-----------------------------------------------------------
-- User Publications Schema (from CSV)
-----------------------------------------------------------

CREATE TABLE publications (
    id VARCHAR(255) PRIMARY KEY,
    title TEXT NOT NULL,
    title_normalized TEXT,  -- Normalized for matching
    person_id VARCHAR(255),
    author TEXT,
    author_normalized TEXT, -- Normalized for matching
    publication_date DATE,
    year INT,
    doc_type_id INT,
    doc_type VARCHAR(100),
    volume VARCHAR(50),
    number VARCHAR(50),
    pages VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create trigram index for fuzzy title matching
CREATE INDEX idx_publications_title_trgm ON publications USING gin (title_normalized gin_trgm_ops);
CREATE INDEX idx_publications_author_trgm ON publications USING gin (author_normalized gin_trgm_ops);
CREATE INDEX idx_publications_year ON publications (year);
CREATE INDEX idx_publications_volume ON publications (volume);

-----------------------------------------------------------
-- PubMed Records Schema (from XML)
-----------------------------------------------------------

CREATE TABLE pubmed_records (
    pmid VARCHAR(20) PRIMARY KEY,
    title TEXT NOT NULL,
    title_normalized TEXT,  -- Normalized for matching
    journal VARCHAR(255),
    issn VARCHAR(20),
    volume VARCHAR(50),
    issue VARCHAR(50),
    year INT,
    abstract TEXT,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file VARCHAR(255)
);

CREATE TABLE authors (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    name_normalized VARCHAR(255),
    UNIQUE(name)
);

CREATE TABLE affiliations (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    UNIQUE(name)
);

CREATE TABLE mesh_terms (
    id SERIAL PRIMARY KEY,
    term VARCHAR(255) NOT NULL,
    UNIQUE(term)
);

CREATE TABLE keywords (
    id SERIAL PRIMARY KEY,
    keyword VARCHAR(255) NOT NULL,
    UNIQUE(keyword)
);

CREATE TABLE publication_types (
    id SERIAL PRIMARY KEY,
    type VARCHAR(100) NOT NULL,
    UNIQUE(type)
);

-- Junction tables for many-to-many relationships
CREATE TABLE pubmed_authors (
    pubmed_id VARCHAR(20) REFERENCES pubmed_records(pmid) ON DELETE CASCADE,
    author_id INT REFERENCES authors(id) ON DELETE CASCADE,
    position INT,
    PRIMARY KEY (pubmed_id, author_id)
);

CREATE TABLE author_affiliations (
    pubmed_id VARCHAR(20) REFERENCES pubmed_records(pmid) ON DELETE CASCADE,
    author_id INT REFERENCES authors(id) ON DELETE CASCADE,
    affiliation_id INT REFERENCES affiliations(id) ON DELETE CASCADE,
    PRIMARY KEY (pubmed_id, author_id, affiliation_id)
);

CREATE TABLE pubmed_mesh_terms (
    pubmed_id VARCHAR(20) REFERENCES pubmed_records(pmid) ON DELETE CASCADE,
    mesh_id INT REFERENCES mesh_terms(id) ON DELETE CASCADE,
    PRIMARY KEY (pubmed_id, mesh_id)
);

CREATE TABLE pubmed_keywords (
    pubmed_id VARCHAR(20) REFERENCES pubmed_records(pmid) ON DELETE CASCADE,
    keyword_id INT REFERENCES keywords(id) ON DELETE CASCADE,
    PRIMARY KEY (pubmed_id, keyword_id)
);

CREATE TABLE pubmed_publication_types (
    pubmed_id VARCHAR(20) REFERENCES pubmed_records(pmid) ON DELETE CASCADE,
    type_id INT REFERENCES publication_types(id) ON DELETE CASCADE,
    PRIMARY KEY (pubmed_id, type_id)
);

-- Create indices for PubMed records
CREATE INDEX idx_pubmed_records_title_trgm ON pubmed_records USING gin (title_normalized gin_trgm_ops);
CREATE INDEX idx_pubmed_records_year ON pubmed_records (year);
CREATE INDEX idx_pubmed_records_volume ON pubmed_records (volume);
CREATE INDEX idx_pubmed_records_journal ON pubmed_records (journal);

-----------------------------------------------------------
-- Matching and Enrichment Schema
-----------------------------------------------------------

CREATE TYPE match_type AS ENUM ('exact_title', 'title_author', 'title_year', 'fuzzy_title', 'manual');

CREATE TABLE publication_pubmed_matches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    publication_id VARCHAR(255) REFERENCES publications(id) ON DELETE CASCADE,
    pmid VARCHAR(20) REFERENCES pubmed_records(pmid) ON DELETE CASCADE,
    match_quality NUMERIC(5,2),
    match_type match_type NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(publication_id, pmid)
);

CREATE INDEX idx_matches_publication_id ON publication_pubmed_matches (publication_id);
CREATE INDEX idx_matches_pmid ON publication_pubmed_matches (pmid);
CREATE INDEX idx_matches_quality ON publication_pubmed_matches (match_quality);

-----------------------------------------------------------
-- Views for Convenience
-----------------------------------------------------------

-- View for enriched publications
CREATE VIEW enriched_publications AS
SELECT 
    p.*,
    pm.pmid,
    pm.title AS pubmed_title,
    pm.journal AS pubmed_journal,
    pm.abstract AS pubmed_abstract,
    STRING_AGG(DISTINCT a.name, '; ') AS pubmed_authors,
    STRING_AGG(DISTINCT m.term, '; ') AS pubmed_mesh_terms,
    STRING_AGG(DISTINCT k.keyword, '; ') AS pubmed_keywords,
    m2.match_quality,
    m2.match_type::TEXT,
    m2.created_at AS match_date
FROM 
    publications p
LEFT JOIN 
    publication_pubmed_matches m2 ON p.id = m2.publication_id
LEFT JOIN 
    pubmed_records pm ON m2.pmid = pm.pmid
LEFT JOIN 
    pubmed_authors pa ON pm.pmid = pa.pubmed_id
LEFT JOIN 
    authors a ON pa.author_id = a.id
LEFT JOIN 
    pubmed_mesh_terms pmt ON pm.pmid = pmt.pubmed_id
LEFT JOIN 
    mesh_terms m ON pmt.mesh_id = m.id
LEFT JOIN 
    pubmed_keywords pk ON pm.pmid = pk.pubmed_id
LEFT JOIN 
    keywords k ON pk.keyword_id = k.id
GROUP BY 
    p.id, pm.pmid, m2.match_quality, m2.match_type, m2.created_at;

-- View for PubMed records with their associated data
CREATE VIEW complete_pubmed_records AS
SELECT 
    pr.pmid,
    pr.title,
    pr.journal,
    pr.issn,
    pr.volume,
    pr.issue,
    pr.year,
    pr.abstract,
    STRING_AGG(DISTINCT a.name, '; ') AS authors,
    STRING_AGG(DISTINCT aff.name, '; ') AS affiliations,
    STRING_AGG(DISTINCT mt.term, '; ') AS mesh_terms,
    STRING_AGG(DISTINCT k.keyword, '; ') AS keywords,
    STRING_AGG(DISTINCT pt.type, '; ') AS publication_types,
    pr.source_file
FROM 
    pubmed_records pr
LEFT JOIN 
    pubmed_authors pa ON pr.pmid = pa.pubmed_id
LEFT JOIN 
    authors a ON pa.author_id = a.id
LEFT JOIN 
    author_affiliations aa ON pr.pmid = aa.pubmed_id AND a.id = aa.author_id
LEFT JOIN 
    affiliations aff ON aa.affiliation_id = aff.id
LEFT JOIN 
    pubmed_mesh_terms pmt ON pr.pmid = pmt.pubmed_id
LEFT JOIN 
    mesh_terms mt ON pmt.mesh_id = mt.id
LEFT JOIN 
    pubmed_keywords pk ON pr.pmid = pk.pubmed_id
LEFT JOIN 
    keywords k ON pk.keyword_id = k.id
LEFT JOIN 
    pubmed_publication_types ppt ON pr.pmid = ppt.pubmed_id
LEFT JOIN 
    publication_types pt ON ppt.type_id = pt.id
GROUP BY 
    pr.pmid;

-----------------------------------------------------------
-- Functions for Text Normalization and Matching
-----------------------------------------------------------

-- Function to normalize text for matching
CREATE OR REPLACE FUNCTION normalize_text(input_text TEXT) 
RETURNS TEXT AS $$
BEGIN
    RETURN LOWER(REGEXP_REPLACE(UNACCENT(input_text), '[^\w\s]', '', 'g'));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to update normalized fields
CREATE OR REPLACE FUNCTION update_normalized_fields() 
RETURNS TRIGGER AS $$
BEGIN
    NEW.title_normalized = normalize_text(NEW.title);
    
    IF NEW.author IS NOT NULL THEN
        NEW.author_normalized = normalize_text(NEW.author);
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update normalized fields in publications
CREATE TRIGGER update_publication_normalized_fields
BEFORE INSERT OR UPDATE ON publications
FOR EACH ROW EXECUTE FUNCTION update_normalized_fields();

-- Function to update normalized fields in pubmed_records
CREATE OR REPLACE FUNCTION update_pubmed_normalized_fields() 
RETURNS TRIGGER AS $$
BEGIN
    NEW.title_normalized = normalize_text(NEW.title);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update normalized fields in pubmed_records
CREATE TRIGGER update_pubmed_normalized_fields
BEFORE INSERT OR UPDATE ON pubmed_records
FOR EACH ROW EXECUTE FUNCTION update_pubmed_normalized_fields();

-- Function to update normalized fields in authors
CREATE OR REPLACE FUNCTION update_author_normalized_fields() 
RETURNS TRIGGER AS $$
BEGIN
    NEW.name_normalized = normalize_text(NEW.name);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update normalized fields in authors
CREATE TRIGGER update_author_normalized_fields
BEFORE INSERT OR UPDATE ON authors
FOR EACH ROW EXECUTE FUNCTION update_author_normalized_fields();


-----------------------------------------------------------
-- Persistent temporary tables for bulk processing
-----------------------------------------------------------

-- These tables are used by the Python processor for staging data
-- They're structured as regular tables instead of TEMP tables for persistence

CREATE TABLE IF NOT EXISTS pubmed_records_temp (
    pmid VARCHAR(20),
    title TEXT NOT NULL,
    title_normalized TEXT,
    journal VARCHAR(255),
    volume VARCHAR(50),
    issue VARCHAR(50),
    year INT,
    abstract TEXT,
    source_file VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS pubmed_authors_temp (
    pmid VARCHAR(20),
    author_name TEXT NOT NULL,
    position INT
);

CREATE TABLE IF NOT EXISTS pubmed_mesh_temp (
    pmid VARCHAR(20),
    mesh_term TEXT NOT NULL
);

-- Create indexes to improve bulk loading performance
CREATE INDEX IF NOT EXISTS idx_pubmed_records_temp_pmid ON pubmed_records_temp(pmid);
CREATE INDEX IF NOT EXISTS idx_pubmed_authors_temp_pmid ON pubmed_authors_temp(pmid);
CREATE INDEX IF NOT EXISTS idx_pubmed_mesh_temp_pmid ON pubmed_mesh_temp(pmid);