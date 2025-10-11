# SEC Schema Documentation

## Overview

The SEC schema provides access to financial filings from the U.S. Securities and Exchange Commission's EDGAR system, including XBRL data, company metadata, and stock prices.

## Architecture Note: FileSchema Delegation

The SEC schema operates as a **declarative data pipeline** that:
1. **Downloads** SEC EDGAR filings based on your configuration
2. **Transforms** XBRL/XML data to optimized Parquet format
3. **Enriches** tables with metadata, comments, and foreign key constraints
4. **Delegates** actual query execution to the FileSchema adapter

This means you get the benefits of:
- **Automatic data acquisition** - No manual downloading or parsing
- **Rich metadata** - Table and column comments for easy discovery
- **Optimized execution** - Leverages DuckDB, Parquet, or Arrow engines via FileSchema
- **Cross-domain joins** - Foreign keys link to GEO and ECON schemas

## Tables

### Core Filing Tables

#### `filing_metadata` (Central Reference Table)
Primary key: `(cik, filing_type, year, accession_number)`

| Column | Type | Description |
|--------|------|-------------|
| cik | VARCHAR | Central Index Key (10-digit with leading zeros) |
| filing_type | VARCHAR | Type of filing (10-K, 10-Q, 8-K, etc.) |
| year | INTEGER | Filing year |
| accession_number | VARCHAR | Unique EDGAR accession number |
| company_name | VARCHAR | Company legal name |
| sic_code | VARCHAR | Standard Industrial Classification code |
| state_of_incorporation | VARCHAR | 2-letter state code (FK → geo.tiger_states.state_code) |
| fiscal_year_end | VARCHAR | Fiscal year end date (MMDD format) |
| business_address | VARCHAR | Principal business address |
| business_phone | VARCHAR | Business contact phone |
| filing_date | DATE | Date filing was submitted |

#### `financial_line_items`
Contains XBRL financial statement data extracted from filings.

Primary key: `(cik, filing_type, year, accession_number, concept, period, context_ref)`

| Column | Type | Description |
|--------|------|-------------|
| cik | VARCHAR | Central Index Key (FK → filing_metadata) |
| filing_type | VARCHAR | Type of filing (FK → filing_metadata) |
| year | INTEGER | Filing year (FK → filing_metadata) |
| accession_number | VARCHAR | EDGAR accession (FK → filing_metadata) |
| concept | VARCHAR | XBRL concept name (e.g., "NetIncomeLoss") |
| period | VARCHAR | Reporting period |
| context_ref | VARCHAR | Context reference ID |
| value | DECIMAL | Numeric value |
| unit | VARCHAR | Unit of measure (USD, shares, etc.) |
| segment | VARCHAR | Business segment if applicable |
| filing_date | DATE | Date of filing |

#### `filing_contexts`
XBRL context definitions for dimensional reporting.

Primary key: `(cik, filing_type, year, accession_number, context_id)`

| Column | Type | Description |
|--------|------|-------------|
| cik | VARCHAR | Central Index Key (FK → filing_metadata) |
| filing_type | VARCHAR | Type of filing |
| year | INTEGER | Filing year |
| accession_number | VARCHAR | EDGAR accession |
| context_id | VARCHAR | Context identifier |
| start_date | DATE | Period start date |
| end_date | DATE | Period end date |
| instant | BOOLEAN | True if instant (point in time) |
| segment | VARCHAR | Segment information |

### Text and Document Tables

#### `mda_sections`
Management Discussion & Analysis sections extracted from filings.

Primary key: `(cik, filing_type, year, accession_number, section_id)`

| Column | Type | Description |
|--------|------|-------------|
| cik | VARCHAR | Central Index Key (FK → filing_metadata) |
| filing_type | VARCHAR | Type of filing |
| year | INTEGER | Filing year |
| accession_number | VARCHAR | EDGAR accession |
| section_id | VARCHAR | Section identifier |
| section_text | TEXT | Extracted text content |
| section_order | INTEGER | Order in document |

#### `footnotes`
Financial statement footnotes with detailed explanations.

Primary key: `(cik, filing_type, year, accession_number, footnote_id)`

| Column | Type | Description |
|--------|------|-------------|
| cik | VARCHAR | Central Index Key (FK → filing_metadata) |
| filing_type | VARCHAR | Type of filing |
| year | INTEGER | Filing year |
| accession_number | VARCHAR | EDGAR accession |
| footnote_id | VARCHAR | Footnote identifier |
| footnote_text | TEXT | Footnote content |
| referenced_concept | VARCHAR | XBRL concept being explained |
| footnote_number | INTEGER | Footnote number |

#### `earnings_transcripts`
Earnings call transcripts from 8-K filings.

Primary key: `(cik, filing_type, year, accession_number, sequence)`

| Column | Type | Description |
|--------|------|-------------|
| cik | VARCHAR | Central Index Key (FK → filing_metadata) |
| filing_type | VARCHAR | Type of filing (typically 8-K) |
| year | INTEGER | Filing year |
| accession_number | VARCHAR | EDGAR accession |
| speaker | VARCHAR | Speaker name |
| text | TEXT | Transcript text |
| sequence | INTEGER | Order in transcript |

### Trading and Ownership Tables

#### `insider_transactions`
Forms 3, 4, and 5 insider trading data.

Primary key: `(cik, filing_type, year, accession_number, transaction_id)`

| Column | Type | Description |
|--------|------|-------------|
| cik | VARCHAR | Company CIK (FK → filing_metadata) |
| filing_type | VARCHAR | Form type (3, 4, or 5) |
| year | INTEGER | Filing year |
| accession_number | VARCHAR | EDGAR accession |
| transaction_id | VARCHAR | Transaction identifier |
| insider_cik | VARCHAR | Insider's CIK |
| insider_name | VARCHAR | Insider name |
| transaction_date | DATE | Date of transaction |
| transaction_type | VARCHAR | Buy/Sell/Grant/Exercise |
| shares | DECIMAL | Number of shares |
| price_per_share | DECIMAL | Transaction price |

#### `stock_prices`
Daily stock price data from Yahoo Finance.

Primary key: `(ticker, date)`

| Column | Type | Description |
|--------|------|-------------|
| ticker | VARCHAR | Stock ticker symbol |
| date | DATE | Trading date |
| cik | VARCHAR | Company CIK |
| open | DECIMAL | Opening price |
| high | DECIMAL | Daily high |
| low | DECIMAL | Daily low |
| close | DECIMAL | Closing price |
| volume | BIGINT | Trading volume |
| adjusted_close | DECIMAL | Adjusted closing price |

### XBRL Metadata Tables

#### `xbrl_relationships`
XBRL calculation and presentation relationships.

Primary key: `(cik, filing_type, year, accession_number, relationship_id)`

| Column | Type | Description |
|--------|------|-------------|
| cik | VARCHAR | Central Index Key (FK → filing_metadata) |
| filing_type | VARCHAR | Type of filing |
| year | INTEGER | Filing year |
| accession_number | VARCHAR | EDGAR accession |
| relationship_id | VARCHAR | Relationship identifier |
| from_concept | VARCHAR | Source concept |
| to_concept | VARCHAR | Target concept |
| arc_role | VARCHAR | Relationship type |

### Search and Analytics Tables

#### `vectorized_blobs`
Text embeddings for semantic search across filing content.

Primary key: `(cik, filing_type, year, accession_number, blob_id)`

| Column | Type | Description |
|--------|------|-------------|
| cik | VARCHAR | Central Index Key (FK → filing_metadata) |
| filing_type | VARCHAR | Type of filing |
| year | INTEGER | Filing year |
| accession_number | VARCHAR | EDGAR accession |
| blob_id | VARCHAR | Blob identifier |
| blob_type | VARCHAR | Type of content (mda, footnote, earnings) |
| source_table | VARCHAR | Source table name |
| source_id | VARCHAR | Source record ID |
| content | TEXT | Text content |
| embedding | ARRAY[FLOAT] | Vector embedding |
| start_offset | INTEGER | Start position in source |
| end_offset | INTEGER | End position in source |

## Foreign Key Relationships

### Internal Relationships
All SEC tables (except `stock_prices`) have foreign key relationships to `filing_metadata` using the composite key `(cik, filing_type, year, accession_number)`.

### Cross-Domain Relationships
- `filing_metadata.state_of_incorporation` → `geo.tiger_states.state_code`

### Complete Reference
For a comprehensive view of all relationships including the complete ERD diagram, cross-schema query examples, and detailed FK implementation status, see the **[Schema Relationships Guide](relationships.md)**.

## Partitioning Strategy

Tables are partitioned by:
1. **cik** - Company identifier
2. **filing_type** - Type of filing (10-K, 10-Q, 8-K, etc.)
3. **year** - Filing year

This enables efficient queries filtering by company and time period.

## Common Query Patterns

### Financial Analysis
```sql
-- Revenue and income by company
SELECT
    f.company_name,
    l.year,
    MAX(CASE WHEN l.concept = 'Revenues' THEN l.value END) as revenue,
    MAX(CASE WHEN l.concept = 'NetIncomeLoss' THEN l.value END) as net_income
FROM financial_line_items l
JOIN filing_metadata f USING (cik, filing_type, year, accession_number)
WHERE l.filing_type = '10-K'
GROUP BY f.company_name, l.year
ORDER BY f.company_name, l.year;
```

### Geographic Analysis
```sql
-- Companies by state of incorporation
SELECT
    s.state_name,
    COUNT(DISTINCT f.cik) as company_count,
    AVG(l.value) as avg_revenue
FROM filing_metadata f
JOIN geo.tiger_states s ON f.state_of_incorporation = s.state_code
JOIN financial_line_items l USING (cik, filing_type, year, accession_number)
WHERE l.concept = 'Revenues' AND l.filing_type = '10-K'
GROUP BY s.state_name
ORDER BY company_count DESC;
```

### Insider Trading Analysis
```sql
-- Recent insider transactions by company
SELECT
    f.company_name,
    i.insider_name,
    i.transaction_date,
    i.transaction_type,
    i.shares,
    i.price_per_share,
    i.shares * i.price_per_share as total_value
FROM insider_transactions i
JOIN filing_metadata f USING (cik, filing_type, year, accession_number)
WHERE i.transaction_date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY i.transaction_date DESC;
```

## Configuration Options

### Connection Properties
```java
Properties props = new Properties();
props.setProperty("ciks", "AAPL,MSFT,GOOGL");  // Companies to download
props.setProperty("startYear", "2020");         // Start year
props.setProperty("endYear", "2024");           // End year
props.setProperty("directory", "/path/to/cache"); // Cache directory
props.setProperty("downloadMissing", "true");   // Auto-download missing data
```

### Model Configuration
```json
{
  "name": "sec",
  "type": "custom",
  "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
  "operand": {
    "dataSource": "sec",
    "ciks": ["0000320193", "0000789019"],  // Apple, Microsoft
    "startYear": 2020,
    "endYear": 2024,
    "filingTypes": ["10-K", "10-Q", "8-K"],
    "downloadMissing": true,
    "cacheExpiry": "30d",
    "enableVectorization": true
  }
}
```

## Data Sources

- **SEC EDGAR**: Primary source for all filing data
- **Yahoo Finance**: Stock price data
- **XBRL US**: XBRL taxonomy definitions

## Update Frequency

- Filing data: Real-time via SEC RSS feeds
- Stock prices: Daily updates after market close
- XBRL taxonomies: Quarterly updates
