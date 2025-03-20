-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-----------------------------------------------------------
-- User Publications Schema (from CSV)
-----------------------------------------------------------
CREATE TABLE publications (
    id VARCHAR(255) PRIMARY KEY,  -- cfrespublid from CSV
    title TEXT NOT NULL,
    title_normalized TEXT,
    person_id VARCHAR(255),       -- cfpersid_id from CSV
    author TEXT,
    author_normalized TEXT,
    publication_date DATE,        -- cfrespubldate from CSV
    year INT,
    doc_type_id INT,              -- cfdctype_id from CSV
    doc_type VARCHAR(100),        -- cfdctype from CSV
    volume VARCHAR(50),
    number VARCHAR(50),           -- number from CSV
    pages VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create optimized indices for publications
CREATE INDEX idx_publications_title_trgm ON publications USING gin (title_normalized gin_trgm_ops);
CREATE INDEX idx_publications_author_trgm ON publications USING gin (author_normalized gin_trgm_ops);
CREATE INDEX idx_publications_year ON publications (year);
CREATE INDEX idx_publications_person_id ON publications (person_id);

-----------------------------------------------------------
-- PubMed Records Schema (from XML)
-----------------------------------------------------------
-- Using table partitioning for better performance with large datasets
CREATE TABLE pubmed_records (
    pmid VARCHAR(20) PRIMARY KEY,
    title TEXT NOT NULL,
    title_normalized TEXT,
    journal VARCHAR(255),
    journal_iso VARCHAR(100),
    issn VARCHAR(20),
    issn_linking VARCHAR(20),
    issn_type VARCHAR(20),
    volume VARCHAR(50),
    issue VARCHAR(50),
    pagination VARCHAR(50),
    doi VARCHAR(100),
    year INT,
    pub_date DATE,
    pub_status VARCHAR(50),
    language VARCHAR(50),
    vernacular_title TEXT,
    medline_ta VARCHAR(255),
    nlm_unique_id VARCHAR(100),
    country VARCHAR(100),
    abstract TEXT,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_file VARCHAR(255)
);


-- Optimized indices for PubMed records
CREATE INDEX idx_pubmed_records_title_trgm ON pubmed_records USING gin (title_normalized gin_trgm_ops);
CREATE INDEX idx_pubmed_records_journal ON pubmed_records (journal);
CREATE INDEX idx_pubmed_records_year_vol_issue ON pubmed_records (year, volume, issue);
CREATE INDEX idx_pubmed_records_doi ON pubmed_records (doi) WHERE doi IS NOT NULL;

-- Reference tables with optimized structure
CREATE TABLE authors (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    name_normalized VARCHAR(255),
    UNIQUE(name)
);

CREATE TABLE affiliations (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    country VARCHAR(100),
    UNIQUE(name)
);

CREATE TABLE mesh_terms (
    id SERIAL PRIMARY KEY,
    term VARCHAR(255) NOT NULL,
    UNIQUE(term)
);

CREATE TABLE chemicals (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    UNIQUE(name)
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

-- Junction tables with improved constraints
CREATE TABLE pubmed_authors (
    pubmed_id VARCHAR(20) REFERENCES pubmed_records(pmid) ON DELETE CASCADE,
    author_id INT REFERENCES authors(id) ON DELETE CASCADE,
    position INT NOT NULL,
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
    is_major_topic BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (pubmed_id, mesh_id)
);

CREATE TABLE pubmed_chemicals (
    pubmed_id VARCHAR(20) REFERENCES pubmed_records(pmid) ON DELETE CASCADE,
    chemical_id INT REFERENCES chemicals(id) ON DELETE CASCADE,
    PRIMARY KEY (pubmed_id, chemical_id)
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

-----------------------------------------------------------
-- Matching and Enrichment Schema
-----------------------------------------------------------
CREATE TYPE match_type AS ENUM ('exact_title', 'title_author', 'title_year', 'fuzzy_title', 'doi_match', 'manual');

CREATE TABLE publication_pubmed_matches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    publication_id VARCHAR(255) REFERENCES publications(id) ON DELETE CASCADE,
    pmid VARCHAR(20) REFERENCES pubmed_records(pmid) ON DELETE CASCADE,
    match_quality NUMERIC(5,2),
    match_type match_type NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(publication_id, pmid)
);

-- Optimized indices for matches
CREATE INDEX idx_matches_publication_id ON publication_pubmed_matches (publication_id);
CREATE INDEX idx_matches_pmid ON publication_pubmed_matches (pmid);
CREATE INDEX idx_matches_quality_type ON publication_pubmed_matches (match_quality, match_type);

-----------------------------------------------------------
-- Text Normalization and Matching
-----------------------------------------------------------
-- normalization function
CREATE OR REPLACE FUNCTION normalize_text(input_text TEXT) 
RETURNS TEXT AS $$
BEGIN

    RETURN TRIM(REGEXP_REPLACE(
              REGEXP_REPLACE(
                UNACCENT(LOWER(input_text)), 
                '[^\w\s]', ' ', 'g'),
              '\s+', ' ', 'g'));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to update normalized fields
CREATE OR REPLACE FUNCTION update_normalized_fields() 
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.title IS NOT NULL THEN
        NEW.title_normalized = normalize_text(NEW.title);
    END IF;
    
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
    IF NEW.title IS NOT NULL THEN
        NEW.title_normalized = normalize_text(NEW.title);
    END IF;
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
-- Optimized Matching View
-----------------------------------------------------------
CREATE MATERIALIZED VIEW enriched_publications AS
SELECT 
    p.*,
    pm.pmid,
    pm.title AS pubmed_title,
    pm.journal AS pubmed_journal,
    pm.doi AS pubmed_doi,
    pm.pub_status AS pubmed_status,
    pm.abstract AS pubmed_abstract,
    STRING_AGG(DISTINCT a.name, '; ') AS pubmed_authors,
    STRING_AGG(DISTINCT m.term, '; ') AS pubmed_mesh_terms,
    STRING_AGG(DISTINCT k.keyword, '; ') AS pubmed_keywords,
    STRING_AGG(DISTINCT c.name, '; ') AS pubmed_chemicals,
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
LEFT JOIN 
    pubmed_chemicals pc ON pm.pmid = pc.pubmed_id
LEFT JOIN 
    chemicals c ON pc.chemical_id = c.id
GROUP BY 
    p.id, pm.pmid, m2.match_quality, m2.match_type, m2.created_at;

CREATE UNIQUE INDEX idx_enriched_publications_id ON enriched_publications (id);

-- Function to refresh the materialized view
CREATE OR REPLACE FUNCTION refresh_enriched_publications()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY enriched_publications;
END;
$$ LANGUAGE plpgsql;