# SEC Schema Documentation

## Overview

The SEC schema provides access to financial filings from the U.S. Securities and Exchange Commission's EDGAR system, including XBRL data, company metadata, narrative text, and stock prices.

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
Primary key: `(cik, accession_number)`

SEC filing metadata for all public company submissions. Forms include 10-K (annual report), 10-Q (quarterly report), 8-K (current report), DEF 14A (proxy statement), and Form 3/4/5 (insider trading).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| cik | VARCHAR | No | Central Index Key (10-digit with leading zeros) |
| accession_number | VARCHAR | No | Unique EDGAR accession number (format 0000000000-00-000000) |
| filing_type | VARCHAR | No | Type of filing (10-K, 10-Q, 8-K, etc.) |
| filing_date | VARCHAR | No | Date filing was submitted (ISO 8601 format) |
| primary_document | VARCHAR | Yes | Primary document filename in the filing |
| company_name | VARCHAR | No | Legal name of the registrant company |
| period_of_report | VARCHAR | Yes | Reporting period end date (ISO 8601 format) |
| acceptance_datetime | VARCHAR | Yes | Date and time the filing was accepted by SEC |
| file_size | BIGINT | Yes | Total size of filing in bytes |
| fiscal_year | INTEGER | Yes | Fiscal year of the reporting period |
| state_of_incorporation | VARCHAR | Yes | State or jurisdiction of incorporation (FK → geo.states.state_abbr) |
| fiscal_year_end | VARCHAR | Yes | Fiscal year end date (MMDD format) |
| business_address | VARCHAR | Yes | Physical business address of the company |
| mailing_address | VARCHAR | Yes | Mailing address for correspondence |
| phone | VARCHAR | Yes | Company phone number |
| sic_code | VARCHAR | Yes | Standard Industrial Classification code |
| irs_number | VARCHAR | Yes | IRS Employer Identification Number (EIN) |
| ticker | VARCHAR | Yes | Stock ticker symbol (if available) |

#### `financial_line_items`
Primary key: `(cik, accession_number, element_id)`

XBRL financial facts extracted from 10-K (annual) and 10-Q (quarterly) filings. Contains structured data for balance sheet, income statement, and cash flow items using standardized US-GAAP taxonomy.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| cik | VARCHAR | No | Central Index Key (FK → filing_metadata) |
| accession_number | VARCHAR | No | EDGAR accession (FK → filing_metadata) |
| filing_date | VARCHAR | No | Date of filing (ISO 8601 format) |
| concept | VARCHAR | No | XBRL concept name (e.g., 'us-gaap:Assets', 'dei:EntityRegistrantName') |
| context_ref | VARCHAR | No | Reference to context element defining the reporting period and entity |
| unit_ref | VARCHAR | Yes | Reference to unit element defining measurement units (e.g., 'USD', 'shares') |
| value | VARCHAR | Yes | Text value of the fact element |
| full_text | VARCHAR | Yes | Full text content for TextBlock elements (narrative disclosures) |
| numeric_value | DOUBLE | Yes | Numeric value for monetary and numeric facts |
| period_start | VARCHAR | Yes | Start date of the reporting period (ISO 8601 format) |
| period_end | VARCHAR | Yes | End date of the reporting period (ISO 8601 format) |
| is_instant | BOOLEAN | No | Whether this is an instant-in-time fact (true) or duration fact (false) |
| footnote_refs | VARCHAR | Yes | Comma-separated list of footnote references |
| element_id | VARCHAR | No | Unique element identifier for linking to other elements |
| decimals | INTEGER | Yes | Decimal precision of the numeric value |
| scale | INTEGER | Yes | Scale factor applied to the value |

#### `filing_contexts`
Primary key: `(cik, accession_number, context_id)`

XBRL context definitions from 10-K and 10-Q filings that specify the reporting period and entity for each financial fact.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| cik | VARCHAR | No | Central Index Key (FK → filing_metadata) |
| accession_number | VARCHAR | No | EDGAR accession (FK → filing_metadata) |
| filing_date | VARCHAR | No | Date of filing (ISO 8601 format) |
| context_id | VARCHAR | No | Unique identifier for this context element |
| entity_identifier | VARCHAR | No | Entity identifier (typically CIK number) |
| entity_scheme | VARCHAR | Yes | Entity identifier scheme (e.g., 'http://www.sec.gov/CIK') |
| period_start | VARCHAR | Yes | Start date of duration period (ISO 8601 format) |
| period_end | VARCHAR | Yes | End date of duration period (ISO 8601 format) |
| period_instant | VARCHAR | Yes | Instant date for point-in-time facts (ISO 8601 format) |
| segment | VARCHAR | Yes | Segment dimension information (XML fragment) |
| scenario | VARCHAR | Yes | Scenario dimension information (XML fragment) |

### Text and Document Tables

#### `mda_sections`
Primary key: `(cik, accession_number, section, paragraph_number)`

Management Discussion & Analysis (MD&A) text from 10-K (Item 7) and 10-Q (Item 2) filings. MD&A is a required narrative section where management explains the company's financial condition, results of operations, and known risks/uncertainties.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| cik | VARCHAR | No | Central Index Key (FK → filing_metadata) |
| accession_number | VARCHAR | No | EDGAR accession (FK → filing_metadata) |
| filing_date | VARCHAR | No | Date of filing (ISO 8601 format) |
| section | VARCHAR | No | Item section identifier (e.g., 'Item 7', 'Item 7A') |
| subsection | VARCHAR | Yes | Subsection name (e.g., 'Overview', 'Results of Operations') |
| paragraph_number | INTEGER | No | Sequential paragraph number within the subsection |
| paragraph_text | VARCHAR | No | Full text content of the paragraph |
| footnote_refs | VARCHAR | Yes | Comma-separated list of footnote references |

#### `earnings_transcripts`
Primary key: `(cik, accession_number, section_type, paragraph_number)`

Earnings-related content from 8-K filings (Item 2.02 - Results of Operations). 8-K forms are "current reports" filed within 4 days of material events.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| cik | VARCHAR | No | Central Index Key (FK → filing_metadata) |
| accession_number | VARCHAR | No | EDGAR accession (FK → filing_metadata) |
| filing_date | VARCHAR | No | Date of filing (ISO 8601 format) |
| filing_type | VARCHAR | No | Type of filing (typically 8-K) |
| exhibit_number | VARCHAR | Yes | Exhibit number within the filing |
| section_type | VARCHAR | No | Section type (prepared_remarks, qa_session) |
| paragraph_number | INTEGER | No | Sequential paragraph number |
| paragraph_text | VARCHAR | No | Full text content of the paragraph |
| speaker_name | VARCHAR | Yes | Name of the speaker (for Q&A sections) |
| speaker_role | VARCHAR | Yes | Role/title of the speaker |

### Trading and Ownership Tables

#### `insider_transactions`
Primary key: `(cik, accession_number, reporting_person_cik, security_title, transaction_code)`

Insider trading transactions from Form 3 (initial ownership), Form 4 (changes in ownership within 2 days), and Form 5 (annual summary). Required filings by company officers, directors, and 10%+ shareholders.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| cik | VARCHAR | No | Company CIK (FK → filing_metadata) |
| accession_number | VARCHAR | No | EDGAR accession (FK → filing_metadata) |
| filing_date | VARCHAR | No | Date of filing (ISO 8601 format) |
| filing_type | VARCHAR | No | Form type (3, 4, or 5) |
| reporting_person_cik | VARCHAR | No | CIK of the reporting insider |
| reporting_person_name | VARCHAR | No | Name of the reporting insider |
| is_director | BOOLEAN | No | Whether the insider is a director |
| is_officer | BOOLEAN | No | Whether the insider is an officer |
| is_ten_percent_owner | BOOLEAN | No | Whether the insider owns 10% or more |
| officer_title | VARCHAR | Yes | Title of the officer (if applicable) |
| transaction_date | VARCHAR | Yes | Date of the transaction (ISO 8601) - null for holdings |
| transaction_code | VARCHAR | Yes | Transaction code (P=purchase, S=sale, A=award, H=holding) |
| security_title | VARCHAR | Yes | Title of the security transacted |
| shares_transacted | DOUBLE | Yes | Number of shares bought or sold (null for holdings) |
| price_per_share | DOUBLE | Yes | Price per share in the transaction |
| shares_owned_after | DOUBLE | Yes | Shares beneficially owned after the transaction |
| acquired_disposed_code | VARCHAR | Yes | Whether shares were acquired (A) or disposed (D) |
| ownership_type | VARCHAR | Yes | Type of ownership (direct or indirect) |
| footnotes | VARCHAR | Yes | Additional footnotes and explanations |

#### `stock_prices`
Primary key: `(ticker, date)`

Daily stock price data (OHLCV) for SEC-reporting companies. Not from SEC filings but from market data providers (Yahoo Finance, AlphaVantage).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| cik | VARCHAR | No | Company CIK (FK → filing_metadata) |
| ticker | VARCHAR | No | Stock ticker symbol (e.g., 'AAPL', 'MSFT') |
| date | VARCHAR | No | Trading date (ISO 8601 format) |
| open | DOUBLE | No | Opening price for the trading day |
| high | DOUBLE | No | Highest price during the trading day |
| low | DOUBLE | No | Lowest price during the trading day |
| close | DOUBLE | No | Closing price for the trading day |
| volume | BIGINT | No | Number of shares traded during the day |
| adjusted_close | DOUBLE | Yes | Closing price adjusted for splits and dividends |
| split_coefficient | DOUBLE | Yes | Stock split multiplier (1.0 if no split) |
| dividend | DOUBLE | Yes | Dividend amount paid on this date (if any) |

### XBRL Metadata Tables

#### `xbrl_relationships`
Primary key: `(cik, accession_number, linkbase_type, from_concept, to_concept)`

XBRL linkbase relationships from 10-K and 10-Q filings showing how financial concepts connect (e.g., Assets = CurrentAssets + NoncurrentAssets).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| cik | VARCHAR | No | Central Index Key (FK → filing_metadata) |
| accession_number | VARCHAR | No | EDGAR accession (FK → filing_metadata) |
| filing_date | VARCHAR | No | Date of filing (ISO 8601 format) |
| linkbase_type | VARCHAR | No | Type of linkbase (presentation, calculation, definition) |
| arc_role | VARCHAR | No | Arc role defining relationship type (e.g., parent-child, summation-item) |
| from_concept | VARCHAR | No | Source concept in the relationship |
| to_concept | VARCHAR | No | Target concept in the relationship |
| weight | DOUBLE | Yes | Calculation weight (+1 for addition, -1 for subtraction) |
| order | DOUBLE | Yes | Presentation order for display sequencing |
| preferred_label | VARCHAR | Yes | Preferred label role for presentation |

### Search and Analytics Tables

#### `vectorized_chunks`
Primary key: `(cik, accession_number, chunk_id)`

Semantic text chunks from 10-K and 10-Q filings with embeddings for similarity search. Text is chunked, enriched with context tags and cross-references, then normalized for temporal/monetary consistency.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| cik | VARCHAR | No | Central Index Key (FK → filing_metadata) |
| accession_number | VARCHAR | No | EDGAR accession (FK → filing_metadata) |
| year | INTEGER | No | Filing year for Iceberg partitioning |
| chunk_id | VARCHAR | No | Unique identifier for this chunk |
| source_type | VARCHAR | No | Origin type (mda_paragraph, footnote, risk_factor, earnings) |
| section | VARCHAR | Yes | Parent section (e.g., 'Item 7', 'Note 1') |
| sequence | INTEGER | No | Order within parent section |
| filing_date | VARCHAR | No | Date of filing (ISO 8601 format) |
| chunk_text | VARCHAR | No | Original text chunk before enrichment |
| enriched_text | VARCHAR | No | Normalized text with context tags, cross-references, and standardized temporal/monetary expressions |
| embedding | ARRAY\<DOUBLE\> | Yes | 384-dim all-MiniLM-L6-v2 embedding of enriched_text via DuckDB quackformers |
| content_type | VARCHAR | Yes | Content classification (paragraph, table, list, heading, mixed) |
| financial_concepts | VARCHAR | Yes | Comma-separated list of referenced financial concepts |

**Text Enrichment Pipeline:**
1. **SemanticTextChunker** - Splits text into semantic units
2. **SecTextVectorizer** - Adds context tags and cross-references
3. **TextNormalizer** - Standardizes temporal/monetary expressions:
   - `"Q1 2024"` → `"2024-Q1"`
   - `"$5.2 million"` → `"$5,200,000"`
   - `"prior year"` → `"FY2023"`

The original text is preserved in `chunk_text`; enriched version in `enriched_text`.

## Views

### `latest_filings`
Most recent filing of each type per company.

```sql
SELECT fm.* FROM filing_metadata fm
INNER JOIN (
  SELECT cik, filing_type, MAX(filing_date) as max_date
  FROM filing_metadata
  GROUP BY cik, filing_type
) latest ON fm.cik = latest.cik
  AND fm.filing_type = latest.filing_type
  AND fm.filing_date = latest.max_date
```

### `revenue_trends`
Revenue line items across quarters for trend analysis.

```sql
SELECT
  fm.company_name,
  fm.ticker,
  fli.period_end,
  fli.numeric_value as revenue
FROM financial_line_items fli
JOIN filing_metadata fm ON fli.cik = fm.cik AND fli.accession_number = fm.accession_number
WHERE fli.concept LIKE '%Revenue%'
  AND fli.numeric_value IS NOT NULL
ORDER BY fm.cik, fli.period_end
```

### `insider_activity_summary`
Aggregated insider trading activity by company.

```sql
SELECT
  fm.company_name,
  fm.ticker,
  it.transaction_code,
  COUNT(*) as transaction_count,
  SUM(it.shares_transacted) as total_shares,
  AVG(it.price_per_share) as avg_price
FROM insider_transactions it
JOIN filing_metadata fm ON it.cik = fm.cik
WHERE it.transaction_code IN ('P', 'S')
GROUP BY fm.company_name, fm.ticker, it.transaction_code
```

## Foreign Key Relationships

### Internal Relationships
All SEC tables have foreign key relationships to `filing_metadata` using:
- `(cik, accession_number)` for all filing-based tables
- `(cik)` only for `stock_prices` (spans multiple filings)

Additionally:
- `financial_line_items.(cik, accession_number, context_ref)` → `filing_contexts.(cik, accession_number, context_id)` - context IDs are only unique within a filing

### Cross-Domain Relationships
- `filing_metadata.state_of_incorporation` → `geo.states.state_abbr`

### Complete Reference
For a comprehensive view of all relationships including the complete ERD diagram, cross-schema query examples, and detailed FK implementation status, see the **[Schema Relationships Guide](relationships.md)**.

## Partitioning Strategy

Tables are partitioned by **year only** using Hive-style partitioning:
- Pattern: `year=*/*.parquet`
- Target file size: 128MB for optimal query performance
- CIK filtering uses Parquet/Iceberg statistics on sorted data

This enables efficient time-based queries while maintaining reasonable file sizes.

## Common Query Patterns

### Financial Analysis
```sql
-- Revenue and income by company
SELECT
  f.company_name,
  YEAR(l.period_end) as year,
  MAX(CASE WHEN l.concept LIKE '%Revenue%' THEN l.numeric_value END) as revenue,
  MAX(CASE WHEN l.concept = 'us-gaap:NetIncomeLoss' THEN l.numeric_value END) as net_income
FROM financial_line_items l
JOIN filing_metadata f USING (cik, accession_number)
WHERE f.filing_type = '10-K'
GROUP BY f.company_name, YEAR(l.period_end)
ORDER BY f.company_name, year;
```

### Geographic Analysis
```sql
-- Companies by state of incorporation
SELECT
  s.state_name,
  COUNT(DISTINCT f.cik) as company_count
FROM filing_metadata f
JOIN geo.states s ON f.state_of_incorporation = s.state_abbr
GROUP BY s.state_name
ORDER BY company_count DESC;
```

### Insider Trading Analysis
```sql
-- Recent insider transactions by company
SELECT
  f.company_name,
  i.reporting_person_name,
  i.transaction_date,
  i.transaction_code,
  i.shares_transacted,
  i.price_per_share,
  i.shares_transacted * i.price_per_share as total_value
FROM insider_transactions i
JOIN filing_metadata f USING (cik)
WHERE i.transaction_date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY i.transaction_date DESC;
```

### Semantic Search
```sql
-- Find chunks similar to a query (using DuckDB vector similarity)
SELECT
  cik,
  accession_number,
  chunk_text,
  array_cosine_similarity(embedding, embed('revenue growth guidance')) as similarity
FROM vectorized_chunks
WHERE embedding IS NOT NULL
ORDER BY similarity DESC
LIMIT 10;
```

## Configuration Options

### Connection Properties
```java
Properties props = new Properties();
props.setProperty("ciks", "0000320193,0000789019");  // CIK numbers (not tickers)
props.setProperty("startYear", "2020");              // Start year
props.setProperty("endYear", "2024");                // End year
props.setProperty("directory", "/path/to/cache");   // Cache directory
props.setProperty("downloadMissing", "true");       // Auto-download missing data
```

### Model Configuration
```json
{
  "name": "sec",
  "type": "custom",
  "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
  "operand": {
    "dataSource": "sec",
    "ciks": ["0000320193", "0000789019"],
    "startYear": 2020,
    "endYear": 2024,
    "filingTypes": ["10-K", "10-Q", "8-K"],
    "downloadMissing": true,
    "cacheExpiry": "30d",
    "enableVectorization": true
  }
}
```

### CIK Resolution Options
The `cikSource` property controls how company CIKs are resolved:
- `config` - Use CIKs specified in configuration (default)
- `sp500` - All S&P 500 companies
- `russell2000` - All Russell 2000 companies
- `nasdaq100` - All NASDAQ 100 companies

## Data Sources

- **SEC EDGAR**: Primary source for all filing data
- **Yahoo Finance**: Stock price data (primary)
- **AlphaVantage**: Stock price data (fallback)

## Update Frequency

- Filing data: Downloaded on-demand from SEC EDGAR
- Stock prices: Daily updates after market close
- Vectorized chunks: Generated during ETL processing

## Reconstruction Capability

The schema retains sufficient information to reconstruct filing content:

| Category | Retention | Reconstructible |
|----------|-----------|-----------------|
| Filing metadata | 100% | ✓ Via accession_number |
| Financial facts (XBRL) | 100% | ✓ Full fidelity |
| Narrative text | 100% (plain text) | ✓ Content preserved |
| Document formatting | 0% | ✗ By design |
| Cross-references | 100% | ✓ Via element_id, context_ref |

Original filings can always be retrieved from SEC EDGAR using:
```
https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{primary_document}
```