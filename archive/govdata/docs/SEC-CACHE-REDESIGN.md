# SEC Filing Cache Design

## Schema

```sql
CREATE TABLE sec_filing_cache (
  cik VARCHAR(10) NOT NULL,
  accession VARCHAR(25) NOT NULL,
  form_type VARCHAR(20) NOT NULL,
  filing_date VARCHAR(10) NOT NULL,
  fiscal_year INTEGER NOT NULL,

  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  -- 'pending', 'complete', 'partial', 'no_xbrl', 'failed'

  vectorization_enabled BOOLEAN NOT NULL DEFAULT FALSE,

  -- Output inventory
  has_metadata BOOLEAN NOT NULL DEFAULT FALSE,
  has_facts BOOLEAN NOT NULL DEFAULT FALSE,
  has_contexts BOOLEAN NOT NULL DEFAULT FALSE,
  has_relationships BOOLEAN NOT NULL DEFAULT FALSE,
  has_mda BOOLEAN NOT NULL DEFAULT FALSE,
  has_insider BOOLEAN NOT NULL DEFAULT FALSE,
  has_earnings BOOLEAN NOT NULL DEFAULT FALSE,
  has_chunks BOOLEAN NOT NULL DEFAULT FALSE,

  processed_at BIGINT,
  error_message VARCHAR,
  error_count INTEGER NOT NULL DEFAULT 0,

  PRIMARY KEY (cik, accession)
);
```

## Expected Outputs by Form Type

| Form | metadata | facts | contexts | relationships | mda | insider | earnings | chunks* |
|------|----------|-------|----------|---------------|-----|---------|----------|---------|
| 10-K | ✓ | ✓ | ✓ | ✓ | ✓ | | | ✓ |
| 10-Q | ✓ | ✓ | ✓ | ✓ | ✓ | | | ✓ |
| 8-K | ✓ | ✓ | ✓ | ✓ | | | ✓ | ✓ |
| 3/4/5 | ✓ | | | | | ✓ | | |

*when vectorization enabled

## Processing Logic

```java
ProcessingDecision checkFiling(cik, accession, formType, filingDate, vectorizationEnabled) {

  entry = cache.get(cik, accession);

  if (entry == null) {
    inventory = checkS3Files(cik, accession);
    if (inventory.isComplete(formType, vectorizationEnabled)) {
      cache.insert(cik, accession, formType, filingDate, "complete", inventory);
      return SKIP;
    }
    return PROCESS;
  }

  switch (entry.status) {
    case "complete":
      if (vectorizationEnabled && !entry.hasChunks) {
        return PROCESS_CHUNKS_ONLY;
      }
      return SKIP;

    case "partial":
      return PROCESS;

    case "no_xbrl":
      return SKIP;

    case "failed":
      return entry.errorCount < 3 ? PROCESS : SKIP;
  }
}
```

## Self-Healing

On cache miss, check S3 for all expected output files before processing:

```java
FileInventory checkS3Files(cik, accession) {
  pattern = "source=sec/year=*/{cik}_{accession}_*.parquet";
  files = s3.glob(pattern);

  return FileInventory.builder()
    .hasMetadata(files.contains("_metadata.parquet"))
    .hasFacts(files.contains("_facts.parquet"))
    .hasContexts(files.contains("_contexts.parquet"))
    .hasRelationships(files.contains("_relationships.parquet"))
    .hasMda(files.contains("_mda.parquet"))
    .hasInsider(files.contains("_insider.parquet"))
    .hasEarnings(files.contains("_earnings.parquet"))
    .hasChunks(files.contains("_chunks.parquet"))
    .build();
}

boolean isComplete(formType, vectorizationEnabled) {
  expected = FormType.expectedOutputs(formType, vectorizationEnabled);
  return inventory.hasAll(expected);
}
```

## Queries

```sql
-- Filings needing vectorization
SELECT * FROM sec_filing_cache
WHERE status = 'complete' AND has_chunks = FALSE;

-- Incomplete filings
SELECT * FROM sec_filing_cache WHERE status = 'partial';

-- Failed filings under retry limit
SELECT * FROM sec_filing_cache WHERE status = 'failed' AND error_count < 3;
```
