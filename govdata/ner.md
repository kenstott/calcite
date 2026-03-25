# Named Entity Recognition & Cluster Analysis for Vectorized Chunks

## Problem

The `vectorized_chunks` table contains ~millions of semantic text chunks from SEC 10-K/10-Q filings (MD&A, risk factors, earnings Q&A). These chunks have embeddings but no structured entity links. When a filing says "we expanded operations in Los Angeles County" or "competition from Berkshire Hathaway intensified," there's no way to query by entity without keyword search.

Meanwhile, 130+ govdata tables contain ~500K+ unique entity values (company names, people, geographies, tickers, agencies) that could serve as a custom NER dictionary.

## Goals

1. **Entity linking** — tag each chunk with references to known entities from the govdata catalog
2. **Entity discovery** — find entities mentioned in text that aren't in any structured table
3. **Cluster analysis** — group chunks by semantic similarity to discover topics and themes
4. **Client-side execution** — run on the local DuckDB VSS cache, not server-side

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   SERVER SIDE                        │
│                                                      │
│  ETL Workers ──► Iceberg Tables ──► S3              │
│       │                                              │
│       ▼                                              │
│  GPU Server ──► Embeddings ──► S3                   │
│                                                      │
└──────────────────────┬──────────────────────────────┘
                       │ iceberg_scan / VSS cache sync
                       ▼
┌─────────────────────────────────────────────────────┐
│                   CLIENT SIDE                        │
│                                                      │
│  DuckDB (local VSS cache)                           │
│       │                                              │
│       ├── NER: gazetteer matching against chunks    │
│       ├── NER: model-based extraction (optional)    │
│       ├── Cluster: HNSW similarity grouping         │
│       └── Output: entity_mentions, chunk_clusters   │
│                                                      │
└─────────────────────────────────────────────────────┘
```

### Why Client-Side

- The DuckDB VSS cache already has all chunks + embeddings locally
- Gazetteer matching is CPU-bound string ops — fast in DuckDB
- Cluster analysis uses the HNSW index already built locally
- No additional S3 round-trips or GPU needed
- Results can be persisted locally or pushed back to Iceberg

## Component 1: Named Entity Recognition

### 1.1 Gazetteer Construction

Build a dictionary from existing govdata entity columns at startup:

```sql
-- Companies (from SEC filing metadata + GLEIF)
CREATE TABLE ner_gazetteer_companies AS
SELECT DISTINCT company_name AS term, cik AS entity_key, 'company' AS entity_type
FROM filing_metadata WHERE company_name IS NOT NULL
UNION ALL
SELECT DISTINCT legal_name, lei, 'company'
FROM gleif_entities WHERE legal_name IS NOT NULL
UNION ALL
SELECT DISTINCT issuer_name, cusip, 'company'
FROM institutional_holdings WHERE issuer_name IS NOT NULL;

-- People (from insider transactions, earnings, FEC)
CREATE TABLE ner_gazetteer_people AS
SELECT DISTINCT reporting_person_name AS term,
       reporting_person_cik AS entity_key, 'person' AS entity_type
FROM insider_transactions WHERE reporting_person_name IS NOT NULL
UNION ALL
SELECT DISTINCT speaker_name, NULL, 'person'
FROM earnings_transcripts WHERE speaker_name IS NOT NULL
UNION ALL
SELECT DISTINCT candidate_name, candidate_id, 'person'
FROM candidates WHERE candidate_name IS NOT NULL;

-- Geographies (from geo schema)
CREATE TABLE ner_gazetteer_geo AS
SELECT DISTINCT state_name AS term, state_fips AS entity_key, 'state' AS entity_type
FROM states
UNION ALL
SELECT DISTINCT county_name, county_code, 'county'
FROM counties WHERE county_name IS NOT NULL
UNION ALL
SELECT DISTINCT place_name, place_fips, 'place'
FROM places WHERE place_name IS NOT NULL
UNION ALL
SELECT DISTINCT cbsa_name, cbsa_fips, 'cbsa'
FROM cbsa WHERE cbsa_name IS NOT NULL;

-- Tickers / Instruments
CREATE TABLE ner_gazetteer_instruments AS
SELECT DISTINCT ticker AS term, cik AS entity_key, 'ticker' AS entity_type
FROM filing_metadata WHERE ticker IS NOT NULL
UNION ALL
SELECT DISTINCT name, figi, 'instrument'
FROM figi_instruments WHERE name IS NOT NULL;

-- Combined gazetteer
CREATE TABLE ner_gazetteer AS
SELECT * FROM ner_gazetteer_companies
UNION ALL SELECT * FROM ner_gazetteer_people
UNION ALL SELECT * FROM ner_gazetteer_geo
UNION ALL SELECT * FROM ner_gazetteer_instruments;
```

Estimated size: ~500K-1M unique terms. Fits in memory.

### 1.2 Dictionary-Based NER (Phase 1)

Fast, deterministic matching using DuckDB string functions:

```sql
-- Match gazetteer terms against chunk text
CREATE TABLE entity_mentions AS
SELECT
    vc.cik,
    vc.accession_number,
    vc.chunk_id,
    g.entity_type,
    g.term AS entity_value,
    g.entity_key,
    1.0 AS confidence,
    'gazetteer' AS method
FROM vectorized_chunks vc
JOIN ner_gazetteer g
  ON vc.enriched_text ILIKE '%' || g.term || '%'
WHERE length(g.term) >= 4  -- avoid false positives on short terms
  AND g.term NOT IN ('The', 'Inc', 'Corp');  -- exclusion list
```

**Optimization**: For large gazetteers, use Aho-Corasick multi-pattern matching via a DuckDB extension or pre-filter by first few characters:

```sql
-- Pre-filter: only match terms whose first word appears in text
-- This reduces the O(chunks × terms) join to manageable size
WITH term_prefixes AS (
    SELECT *, split_part(term, ' ', 1) AS first_word
    FROM ner_gazetteer
    WHERE length(term) >= 4
)
SELECT vc.chunk_id, tp.entity_type, tp.term, tp.entity_key
FROM vectorized_chunks vc
JOIN term_prefixes tp
  ON vc.enriched_text ILIKE '%' || tp.first_word || '%'
  AND vc.enriched_text ILIKE '%' || tp.term || '%';
```

### 1.3 Model-Based NER (Phase 2, Optional)

For discovering entities NOT in the gazetteer, use a lightweight NER model:

**Option A: DuckDB + quackformers** (if token classification is supported)
```sql
-- Hypothetical — depends on quackformers NER support
SELECT chunk_id, ner_extract(enriched_text, 'dslim/bert-base-NER') AS entities
FROM vectorized_chunks;
```

**Option B: Python post-process script** (like the GPU embeddings pattern)
```python
# ner-runner.py — runs client-side
import spacy
nlp = spacy.load("en_core_web_sm")

for chunk in chunks_without_ner:
    doc = nlp(chunk.enriched_text)
    for ent in doc.ents:
        yield {
            'chunk_id': chunk.chunk_id,
            'entity_type': ent.label_,  # ORG, PERSON, GPE, etc.
            'entity_value': ent.text,
            'span_start': ent.start_char,
            'span_end': ent.end_char,
            'confidence': 0.0,  # spaCy doesn't provide confidence
            'method': 'spacy'
        }
```

**Option C: LLM-based extraction** (highest quality, highest cost)
- Send chunks to Claude API with a structured extraction prompt
- Best for high-value chunks (e.g., risk factors mentioning competitors)
- Use sparingly — cost scales with chunk count

### 1.4 Entity Mentions Schema

```sql
CREATE TABLE entity_mentions (
    -- Chunk reference
    cik VARCHAR NOT NULL,
    accession_number VARCHAR NOT NULL,
    chunk_id VARCHAR NOT NULL,
    year INT NOT NULL,

    -- Entity identification
    entity_type VARCHAR NOT NULL,     -- company, person, state, county, place,
                                      -- ticker, agency, cbsa, instrument,
                                      -- ORG, PERSON, GPE (spaCy types)
    entity_value VARCHAR NOT NULL,    -- matched text
    entity_key VARCHAR,               -- FK to source table (cik, fips, etc.)
    entity_source_table VARCHAR,      -- e.g., 'filing_metadata', 'geo.counties'

    -- Match metadata
    confidence DOUBLE,                -- 1.0 for gazetteer, model score for NER
    method VARCHAR NOT NULL,          -- 'gazetteer', 'spacy', 'llm'
    span_start INT,                   -- character offset in enriched_text
    span_end INT                      -- character offset end
);
```

### 1.5 Self-Reference Filtering

Chunks from company X's filing will always mention company X. Filter self-references:

```sql
-- Exclude self-mentions (the filing company mentioning itself)
DELETE FROM entity_mentions em
WHERE em.entity_type = 'company'
  AND em.entity_key = em.cik;
```

This leaves only cross-references — competitor mentions, supplier/customer names, geographic references.

## Component 2: Cluster Analysis

### 2.1 Semantic Clustering

Group chunks by embedding similarity to discover topics:

```sql
-- Step 1: Build HNSW index (already exists in VSS cache)
-- CREATE INDEX vec_hnsw_idx ON vectorized_chunks USING HNSW (embedding);

-- Step 2: K-means clustering via DuckDB
-- DuckDB doesn't have native k-means, but we can approximate:

-- Option A: Use DuckDB's built-in KMEANS (if available in extension)
CREATE TABLE chunk_clusters AS
SELECT chunk_id, cik, section, source_type, enriched_text,
       kmeans(embedding, 50) OVER () AS cluster_id
FROM vectorized_chunks
WHERE embedding IS NOT NULL;

-- Option B: Hierarchical clustering via pairwise similarity
-- For each chunk, find its nearest neighbors and group
CREATE TABLE chunk_neighborhoods AS
SELECT
    a.chunk_id,
    a.cik,
    b.chunk_id AS neighbor_chunk_id,
    b.cik AS neighbor_cik,
    array_cosine_similarity(a.embedding, b.embedding) AS similarity
FROM vectorized_chunks a, vectorized_chunks b
WHERE a.chunk_id != b.chunk_id
  AND array_cosine_similarity(a.embedding, b.embedding) > 0.85
ORDER BY similarity DESC;
```

### 2.2 Topic Labeling

After clustering, label each cluster by its dominant terms:

```sql
-- Extract top terms per cluster using TF-IDF-like scoring
CREATE TABLE cluster_topics AS
WITH words AS (
    SELECT cluster_id, unnest(string_split(lower(enriched_text), ' ')) AS word
    FROM chunk_clusters
),
word_counts AS (
    SELECT cluster_id, word, COUNT(*) AS tf,
           COUNT(*) OVER (PARTITION BY word) AS df
    FROM words
    WHERE length(word) > 3
    GROUP BY cluster_id, word
),
scored AS (
    SELECT cluster_id, word, tf * log(50.0 / df) AS tfidf
    FROM word_counts
    WHERE df < 25  -- exclude overly common words
)
SELECT cluster_id,
       string_agg(word, ', ' ORDER BY tfidf DESC) AS top_terms
FROM (
    SELECT cluster_id, word, tfidf,
           ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY tfidf DESC) AS rn
    FROM scored
) WHERE rn <= 10
GROUP BY cluster_id;
```

### 2.3 Cross-Company Topic Analysis

Find companies discussing similar topics:

```sql
-- Which companies talk about the same things?
SELECT
    cc.cluster_id,
    ct.top_terms,
    COUNT(DISTINCT cc.cik) AS num_companies,
    string_agg(DISTINCT fm.company_name, ', ' ORDER BY fm.company_name) AS companies
FROM chunk_clusters cc
JOIN cluster_topics ct ON cc.cluster_id = ct.cluster_id
JOIN filing_metadata fm ON cc.cik = fm.cik
GROUP BY cc.cluster_id, ct.top_terms
HAVING COUNT(DISTINCT cc.cik) > 3
ORDER BY num_companies DESC;
```

### 2.4 Temporal Cluster Drift

Track how topics evolve over time:

```sql
-- Topic prevalence by year
SELECT
    cc.cluster_id,
    ct.top_terms,
    vc.year,
    COUNT(*) AS chunk_count,
    COUNT(DISTINCT cc.cik) AS company_count
FROM chunk_clusters cc
JOIN cluster_topics ct ON cc.cluster_id = ct.cluster_id
JOIN vectorized_chunks vc ON cc.chunk_id = vc.chunk_id
GROUP BY cc.cluster_id, ct.top_terms, vc.year
ORDER BY cc.cluster_id, vc.year;
```

## Component 3: Combined Entity-Cluster Analysis

### 3.1 Entity Co-occurrence

Which entities are mentioned together?

```sql
-- Entity co-occurrence matrix
SELECT
    a.entity_value AS entity_a,
    a.entity_type AS type_a,
    b.entity_value AS entity_b,
    b.entity_type AS type_b,
    COUNT(DISTINCT a.chunk_id) AS co_occurrences
FROM entity_mentions a
JOIN entity_mentions b
  ON a.chunk_id = b.chunk_id
  AND a.entity_value < b.entity_value  -- avoid duplicates
WHERE a.entity_type IN ('company', 'person')
  AND b.entity_type IN ('company', 'person', 'state')
GROUP BY a.entity_value, a.entity_type, b.entity_value, b.entity_type
HAVING COUNT(DISTINCT a.chunk_id) >= 3
ORDER BY co_occurrences DESC;
```

### 3.2 Entity Sentiment by Cluster

Which clusters mention which entities, and in what context?

```sql
-- Entities enriched with cluster context
SELECT
    em.entity_value,
    em.entity_type,
    cc.cluster_id,
    ct.top_terms AS cluster_topic,
    COUNT(*) AS mentions,
    COUNT(DISTINCT em.cik) AS mentioning_companies
FROM entity_mentions em
JOIN chunk_clusters cc ON em.chunk_id = cc.chunk_id
JOIN cluster_topics ct ON cc.cluster_id = ct.cluster_id
GROUP BY em.entity_value, em.entity_type, cc.cluster_id, ct.top_terms
ORDER BY mentions DESC;
```

## Implementation Plan

### Phase 1: Gazetteer NER (1-2 days)
- Build gazetteer construction SQL from entity columns
- Implement dictionary matching in DuckDB
- Create `entity_mentions` table
- Add self-reference filtering
- Package as a SQL script runnable against local DuckDB VSS cache

### Phase 2: Cluster Analysis (1-2 days)
- Implement k-means or DBSCAN clustering on embeddings
- Build topic labeling pipeline
- Create cross-company and temporal analysis views
- Package as SQL script

### Phase 3: Model-Based NER (3-5 days)
- Evaluate spaCy vs quackformers for client-side NER
- Implement Python runner script (following vss-bulk-gpu.py pattern)
- Merge model-based entities with gazetteer entities
- Add confidence scoring and deduplication

### Phase 4: Integration (2-3 days)
- Create Calcite views that expose entity_mentions and chunk_clusters
- Add to the schema YAML so they're queryable via SQL
- Build example queries for common use cases
- Add to DuckDB VSS cache sync pipeline

## Example Use Cases

### "Which companies mention Berkshire Hathaway as a competitor?"
```sql
SELECT DISTINCT fm.company_name, fm.ticker, em.cik
FROM entity_mentions em
JOIN filing_metadata fm ON em.cik = fm.cik
WHERE em.entity_value = 'Berkshire Hathaway'
  AND em.entity_type = 'company'
  AND em.cik != '0001067983'  -- exclude Berkshire's own filings
ORDER BY fm.company_name;
```

### "What geographic regions are most discussed in risk factors?"
```sql
SELECT em.entity_value, em.entity_type, COUNT(*) AS mentions
FROM entity_mentions em
JOIN vectorized_chunks vc ON em.chunk_id = vc.chunk_id
WHERE vc.source_type = 'risk_factor'
  AND em.entity_type IN ('state', 'county', 'place', 'cbsa')
GROUP BY em.entity_value, em.entity_type
ORDER BY mentions DESC
LIMIT 20;
```

### "Find emerging risk topics across the S&P 500"
```sql
SELECT ct.top_terms, vc.year,
       COUNT(*) AS chunk_count,
       COUNT(DISTINCT cc.cik) AS company_count
FROM chunk_clusters cc
JOIN cluster_topics ct ON cc.cluster_id = ct.cluster_id
JOIN vectorized_chunks vc ON cc.chunk_id = vc.chunk_id
WHERE vc.source_type = 'risk_factor'
GROUP BY ct.top_terms, vc.year
HAVING COUNT(DISTINCT cc.cik) > 10
ORDER BY vc.year DESC, company_count DESC;
```

### "Which insiders joined companies that mention AI/ML risks?"
```sql
SELECT it.reporting_person_name, it.officer_title, fm.company_name,
       it.filing_date, vc.enriched_text
FROM insider_transactions it
JOIN filing_metadata fm ON it.cik = fm.cik
JOIN entity_mentions em ON em.cik = it.cik
JOIN vectorized_chunks vc ON em.chunk_id = vc.chunk_id
WHERE it.transaction_code = 'I'  -- initial appointment
  AND vc.source_type = 'risk_factor'
  AND vc.enriched_text ILIKE '%artificial intelligence%'
ORDER BY it.filing_date DESC;
```

## Dependencies

- DuckDB with VSS extension (HNSW index) — already deployed
- DuckDB with Iceberg extension — already deployed
- Local VSS cache with vectorized_chunks — already synced
- Entity source tables accessible via iceberg_scan — already available
- Optional: spaCy (`pip install spacy && python -m spacy download en_core_web_sm`)
- Optional: Claude API for LLM-based extraction

## Open Questions

1. **Gazetteer size vs precision** — 500K+ terms will produce false positives. Need minimum term length and exclusion lists. Should we require multi-word matches only for common words?
2. **Clustering algorithm** — K-means requires pre-specifying K. DBSCAN auto-discovers clusters but needs epsilon tuning. HDBSCAN may be better for variable-density text data.
3. **Incremental updates** — when new chunks arrive, do we re-cluster everything or assign new chunks to existing clusters?
4. **Storage** — persist entity_mentions and chunk_clusters in local DuckDB only, or push back to Iceberg for cross-client consistency?
5. **Performance** — gazetteer matching on millions of chunks × 500K terms needs optimization. Aho-Corasick, bloom filters, or pre-computed term hashes?
