# Federal Register Schema Documentation

## Overview

The `fedregister` schema provides access to U.S. Federal Register documents and agency metadata
via the federalregister.gov public API. It covers rules, proposed rules, notices, and
presidential documents published since 2010, along with the full roster of issuing agencies.

The schema is served by `FedRegisterSchemaFactory` via `GovDataSchemaFactory` and is driven by
`fedregister-schema.yaml`.

---

## Tables

| Table | Description | Primary source | Cadence |
|---|---|---|---|
| `fr_documents` | Federal Register documents: title, type, abstract, publication/effective dates, agency names, CFR references, RIN, OIRA significance flag, docket IDs, and links to full text | federalregister.gov API v1 `/documents.json` | Annual (by doc_type × year) |
| `fr_agencies` | Complete Federal Register agency roster: ID, name, abbreviation, slug, URL, parent agency hierarchy | federalregister.gov API v1 `/agencies.json` | Static |

---

## Views

No views are defined in this schema.

---

## Environment Variables

### Required

| Variable | Description |
|---|---|
| `GOVDATA_PARQUET_DIR` | Root Parquet directory |

---

## Key Fields

### fr_documents

- `document_number` — unique identifier (e.g., `2026-07234`)
- `doc_type` — `RULE`, `PRORULE`, `NOTICE`, or `PRESDOC`
- `cfr_references` — JSON array of `{title, part}` objects mapping to regulated industry
- `rin` — Regulatory Identifier Number; links proposed rules to final rules across lifecycle
- `significant` — OIRA economically significant flag (estimated economic impact ≥ $100M/year)
- `docket_ids` — JSON array linking to regulations.gov comment dockets
- `agency_slugs` — comma-separated agency slugs; join to `fr_agencies.slug`

---

## Sample Queries

```sql
-- Count of economically significant rules by year
SELECT YEAR(publication_date) AS pub_year,
       COUNT(*) AS significant_rules
FROM fedregister.fr_documents
WHERE doc_type = 'RULE' AND significant = true
GROUP BY pub_year
ORDER BY pub_year DESC;

-- Most active rulemaking agencies (last 3 years)
SELECT agency_names, COUNT(*) AS rules
FROM fedregister.fr_documents
WHERE doc_type = 'RULE'
  AND publication_date >= '2023-01-01'
GROUP BY agency_names
ORDER BY rules DESC
LIMIT 20;

-- Proposed rules that became final (matched by RIN)
SELECT p.rin, p.title AS proposed_title,
       p.publication_date AS proposed_date,
       f.publication_date AS final_date,
       f.effective_on
FROM fedregister.fr_documents p
JOIN fedregister.fr_documents f
  ON p.rin = f.rin AND p.rin IS NOT NULL
WHERE p.doc_type = 'PRORULE'
  AND f.doc_type = 'RULE'
ORDER BY f.effective_on DESC
LIMIT 20;

-- Agency lookup by slug
SELECT id, name, short_name, slug, parent_id
FROM fedregister.fr_agencies
WHERE slug LIKE 'environ%';
```
