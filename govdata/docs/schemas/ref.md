# Reference Schema Documentation

## Overview

The `ref` schema provides cross-domain entity reference data for linking legal entities,
financial instruments, and securities across other schemas. It contains the Global LEI Foundation
(GLEIF) entity registry, a CIK-to-LEI bridge, and OpenFIGI instrument identifiers.

The schema is served by `RefSchemaFactory` via `GovDataSchemaFactory` and is driven by
`ref-schema.yaml`. It is a **key cross-schema join layer** — most financial entity lookups
across `sec`, `econ`, `health` (drug manufacturers), and `patents` (assignees) resolve through
`ref`.

---

## Tables

| Table | Description | Primary source | Cadence |
|---|---|---|---|
| `gleif_entities` | Global LEI Foundation entity records: LEI, legal name, jurisdiction, entity legal form, registration authority, headquarters and registration country/city, registration/update/renewal dates | GLEIF full CSV bulk download | Annual |
| `gleif_cik_mapping` | Bridge table linking SEC CIK numbers to LEI codes (entities registered with RA000602 = SEC) | GLEIF full CSV bulk download | Annual |
| `figi_instruments` | OpenFIGI financial instrument identifiers: FIGI, name, exchange code, market sector, security type, composite FIGI, share class FIGI | OpenFIGI API v3 `/mapping` | Annual |

---

## Views

| View | Description | Depends on |
|---|---|---|
| `ticker_instrument_map` | Maps trading tickers to FIGI instruments with exchange and security type; links to GLEIF entities registered with SEC | `figi_instruments`, `gleif_entities`, `gleif_cik_mapping` |

---

## Environment Variables

### Required

| Variable | Description |
|---|---|
| `GOVDATA_PARQUET_DIR` | Root Parquet directory |
| `GLEIF_CSV_URL` | URL to GLEIF full bulk CSV download (rotates; obtain from gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy) |

---

## Cross-Schema Join Patterns

### SEC filings → entity metadata
```sql
-- Enrich SEC filing with GLEIF entity metadata via CIK
SELECT f.company_name, f.fiscal_year, e.jurisdiction, e.entity_legal_form
FROM sec.filing_metadata f
JOIN ref.gleif_cik_mapping m ON CAST(f.cik AS VARCHAR) = CAST(m.cik AS VARCHAR)
JOIN ref.gleif_entities e ON m.lei = e.lei
WHERE f.form_type = '10-K';
```

### Drug manufacturer identity
```sql
-- Link FDA drug approval sponsor to GLEIF entity
SELECT a.sponsor_name, a.application_number, e.lei, e.jurisdiction
FROM health.fda_drug_approvals a
JOIN ref.gleif_entities e ON LOWER(a.sponsor_name) = LOWER(e.legal_name);
```

### Patent assignee to public company
```sql
-- Link patent assignees to traded instruments
SELECT pa.assignee_organization, f.figi, f.exchange_code
FROM patents.patent_assignees pa
JOIN ref.figi_instruments f ON LOWER(pa.assignee_organization) = LOWER(f.name);
```

---

## Sample Queries

```sql
-- All US-registered entities in GLEIF
SELECT jurisdiction, COUNT(*) AS entity_count
FROM ref.gleif_entities
WHERE registered_country = 'US'
GROUP BY jurisdiction
ORDER BY entity_count DESC
LIMIT 20;

-- Find LEI for a known company name
SELECT lei, legal_name, jurisdiction, entity_status
FROM ref.gleif_entities
WHERE LOWER(legal_name) LIKE '%apple%'
  AND registered_country = 'US';

-- SEC registrants with their LEI
SELECT m.cik, e.lei, e.legal_name, e.jurisdiction
FROM ref.gleif_cik_mapping m
JOIN ref.gleif_entities e ON m.lei = e.lei
LIMIT 20;
```
