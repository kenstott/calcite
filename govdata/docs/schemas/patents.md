# Patents Schema Documentation

## Overview

The `patents` schema provides access to U.S. patent and trademark data from the USPTO and
PatentsView. It covers granted patents with full metadata, disambiguated assignees and inventors,
CPC classification codes, claim text, pre-aggregated annual summaries, and USPTO trademark
applications.

The schema is served by `PatentsSchemaFactory` via `GovDataSchemaFactory` and is driven by
`patents-schema.yaml`. All data is sourced from PatentsView bulk TSV downloads hosted on S3.

---

## Tables

| Table | Description | Primary source | Cadence |
|---|---|---|---|
| `patent_grants` | Granted patents: grant/filing dates, type (utility/design/plant), title, abstract, claims count, figures, forward citation counts, WIPO kind code, withdrawn flag | PatentsView `g_patent.tsv.zip` + `g_application.tsv` + `g_patent_abstract.tsv` + `g_figures.tsv` | Annual |
| `patent_assignees` | Disambiguated assignees per patent: sequence, assignee ID, organization name or individual name, assignee type (US company / foreign company / US government / individual) | PatentsView `g_assignee_disambiguated.tsv.zip` | Annual |
| `patent_inventors` | Disambiguated inventors per patent: sequence, inventor ID, first/last name, inferred gender code | PatentsView `g_inventor_disambiguated.tsv.zip` | Annual |
| `patent_cpc_classes` | CPC (Cooperative Patent Classification) codes per patent: section, class, subclass, group, code type (inventive/additional) | PatentsView `g_cpc_current.tsv.zip` | Annual |
| `patent_claims` | Full claim text per patent: claim sequence, claim number, claim text | PatentsView claims bulk | Annual |
| `patent_summaries` | Pre-aggregated annual patent statistics: grants per year, by technology and geography | PatentsView + aggregation | Annual |
| `trademark_applications` | USPTO trademark applications: application number, mark, goods/services description, filing date, status, owner | USPTO trademark bulk | Annual |

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

- `patent_id` — USPTO patent number (e.g., `10123456`); PK component
- `grant_year` — calendar year of patent grant; primary partition key
- `state_fips` — 2-digit state FIPS code; FK to `geo.states`
- `county_fips` — 5-digit county FIPS code; FK to `geo.counties`
- `assignee_type` — `US company`, `foreign company`, `US government`, `individual`
- `gender_code` — `M`/`F` inferred by PatentsView disambiguation; useful for diversity analysis
- `cpc_type` — `i` (inventive) or `a` (additional)
- `num_us_citations` — forward US citation count (innovation signal)

---

## Cross-Schema Join Patterns

```sql
-- Patent assignees linked to SEC public companies via ref
SELECT pa.assignee_organization, COUNT(DISTINCT pg.patent_id) AS patent_count,
       e.jurisdiction, e.lei
FROM patents.patent_assignees pa
JOIN patents.patent_grants pg ON pa.patent_id = pg.patent_id
JOIN ref.gleif_entities e ON LOWER(pa.assignee_organization) = LOWER(e.legal_name)
WHERE pa.assignee_type = 'US company'
  AND pg.grant_year >= 2020
GROUP BY pa.assignee_organization, e.jurisdiction, e.lei
ORDER BY patent_count DESC
LIMIT 20;
```

---

## Sample Queries

```sql
-- Top patent-granting states (latest 5 years)
SELECT state_fips, COUNT(*) AS grants
FROM patents.patent_grants
WHERE grant_year >= 2020 AND withdrawn = false
GROUP BY state_fips
ORDER BY grants DESC
LIMIT 20;

-- Most cited utility patents (innovation leaders)
SELECT patent_id, patent_title, grant_year,
       num_us_citations + num_foreign_citations AS total_citations
FROM patents.patent_grants
WHERE patent_type = 'utility' AND withdrawn = false
ORDER BY total_citations DESC
LIMIT 20;

-- Top CPC technology sections by grant volume (latest year)
SELECT cpc_section, COUNT(DISTINCT patent_id) AS patents
FROM patents.patent_cpc_classes
WHERE cpc_type = 'i'
GROUP BY cpc_section
ORDER BY patents DESC;

-- Inventor gender distribution by technology section
SELECT c.cpc_section,
       SUM(CASE WHEN i.gender_code = 'M' THEN 1 ELSE 0 END) AS male_inventors,
       SUM(CASE WHEN i.gender_code = 'F' THEN 1 ELSE 0 END) AS female_inventors,
       ROUND(100.0 * SUM(CASE WHEN i.gender_code = 'F' THEN 1 ELSE 0 END)
             / NULLIF(COUNT(*), 0), 1) AS pct_female
FROM patents.patent_inventors i
JOIN patents.patent_cpc_classes c ON i.patent_id = c.patent_id
WHERE c.cpc_type = 'i'
GROUP BY c.cpc_section
ORDER BY pct_female DESC;
```
