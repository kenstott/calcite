# Trade Schema Data Plan

## Implementation Status (2026-07-08)

There is no dedicated `trade` schema — trade data lives in `census` and `econ`.

- **`bea_fdi_by_industry` — DELIVERED** as `econ.fdi_direct_investment` +
  `econ.fdi_activities` (BEA `MNE` dataset, Country × Industry, inward/outward), surfaced
  by the `econ.fdi_by_country` view. Shape is tall (vs. this plan's wide) and it lives in
  `econ`, not a `trade` schema.
- **`IIP` (secondary dataset) — DELIVERED** as `econ.iip_positions` (BEA `IIP` dataset,
  `TypeOfInvestment=All` × `Component=All`, annual). Tall layout: one row per
  type_of_investment × component × period, covering U.S. external assets/liabilities
  (position) and the change-in-position decomposition (transactions, price, exchange-rate,
  other). New `IipDataTransformer`; annual only (BEA also offers QNSA/QSA), for parity with
  `ita_data`. Endpoint field set verified live 2026-07-08. Verification gate: a keyed
  ETL/DQ run (`BEA_API_KEY`) — not yet completed.
- **`usa_trade_monthly` — DELIVERED** (substance) as `econ.trade_exports` /
  `econ.trade_imports`. Originally a thin country-level annual aggregate with a **hardcoded
  3-year `time` list** (frozen at 2023) living in the `census` schema. Reworked 2026-07-07 to
  the `intltrade/{exports,imports}/hs` endpoints: HS-6 × country × **month**, with value,
  quantity, and (imports) CIF charges; dynamic `year` yearRange (never hardcoded) ×
  `month` list, partitioned `[type, direction, year, month]` with `overwritePartitions`. The
  `/hs` series requires discrete `YEAR=`/`MONTH=` predicates (a `time=YYYY-MM` predicate
  returns HTTP 204). **Moved from `census` to `econ` (2026-07-08)** to consolidate with the
  BEA trade/FDI tables — all trade data now lives in one schema. Verification gate: a keyed
  ETL/DQ run (`CENSUS_API_KEY`) — not yet completed.
- **`usa_trade_by_state` — DELIVERED** as `econ.trade_by_state` (Census
  `intltrade/exports/statehs`, HS-6 × origin state × destination country × month, with
  total/vessel/air/container export values). Exports-only (statehs has no import origin);
  partitioned `[type, year, month]` with `overwritePartitions`. Like `/hs`, statehs requires
  discrete `YEAR=`/`MONTH=` predicates. One (year, month) fetch returns ~2.1M state-detail
  rows (~100 MB); `state='-'`/`country_code='-'` rows are U.S./all-countries roll-ups.
  Verified live 2026-07-08. Verification gate: a keyed ETL/DQ run (`CENSUS_API_KEY`) — not
  yet completed.

## Existing Coverage in ECON Schema

The `econ` schema already includes the following trade-related tables:

| Table | Content |
|---|---|
| `ita_data` | BEA International Transactions Accounts — balance of payments (quarterly/annual) |
| `trade_statistics` | View over BEA NIPA 4.2.5B — exports/imports of goods and services |
| `trade_balance_summary` | Aggregated view: total exports, imports, net balance by category |

These cover **macro-level BEA balance-of-payments data**. The gaps this plan addressed
(all now delivered — see the Implementation Status above):

1. **HS-commodity granularity** — `trade_exports` / `trade_imports` add Census HS-6 × country × month
2. **State-level exports** — `trade_by_state` adds sub-national HS-6 export exposure
3. **FDI by industry** — `fdi_direct_investment` / `fdi_activities` add multinational enterprise investment flows (BEA MNE dataset)
4. **External balance sheet** — `iip_positions` adds the U.S. International Investment Position (BEA IIP dataset)

The `bea_international_transactions` table proposed below overlaps with `econ.ita_data`
and should **not** be implemented if the econ ITA coverage is sufficient. Evaluate after
comparing column sets.

## Strategic Context (Extensions)

This plan extends the existing macro trade coverage in `econ` with commodity-level and
sub-national detail. The primary additional cross-schema value is **supply chain and
company-level trade analysis**: HS-6 import flows by country join to SEC 10-K filings
for companies disclosing material import/export exposure; state exports join to BLS
employment for trade-affected community research.

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| U.S. trade by commodity (HS) | Census Bureau | REST API | Free, no key | Monthly |
| U.S. trade by state | Census Bureau | REST API | Free, no key | Annual |
| BEA Foreign Direct Investment | BEA | REST API | Free, API key required | Annual |
| BEA International Transactions | BEA | REST API | Free, API key required | Quarterly |

Census trade API: `https://api.census.gov/data/timeseries/intltrade/`
BEA API: `https://apps.bea.gov/api/` (key: free at apps.bea.gov/API/signup)

---

## Proposed Tables

### `usa_trade_monthly`

Monthly U.S. import and export values by HS-6 commodity code and trading partner country.
The primary macro-level trade signal — joins to `econ.fred_indicators` for trade balance
series cross-check; joins to `sec.filing_metadata` via NAICS-to-HS crosswalk for
company-level import/export exposure analysis.

**Source:** Census Bureau USA Trade API — `https://api.census.gov/data/timeseries/intltrade/imports`
and `/exports`
**Partition:** `trade_year`
**Auth:** None (Census API key recommended for stability)
**Cadence:** Monthly (prior month published ~5 weeks after month end)
**Release window:** Months 1–12

| Column | Type | Description |
|---|---|---|
| trade_year | INTEGER | Partition key |
| trade_month | INTEGER | Month (1–12) |
| flow | VARCHAR | Import / Export |
| hs2 | VARCHAR | HS 2-digit chapter code |
| hs6 | VARCHAR | HS 6-digit commodity code |
| hs_description | VARCHAR | Commodity description |
| country_code | VARCHAR | ISO 3-letter trading partner country code |
| country_name | VARCHAR | Trading partner name |
| value_usd | DOUBLE | Trade value (USD) |
| cif_charges | DOUBLE | Cost, insurance, freight charges (imports only, USD) |
| quantity | DOUBLE | Quantity (units vary by commodity) |
| quantity_unit | VARCHAR | KG / BBL / DOZ / SQM / etc. |

---

### `usa_trade_by_state`

Annual U.S. merchandise exports by state of origin and destination country. Provides
sub-national export exposure; joins to `econ.bls_employment` for trade-affected
manufacturing communities; joins to `fec.contributions` for trade-sensitive donor geography.

**Source:** Census Bureau USA Trade — State Exports by HS Commodity
**Partition:** `export_year`
**Auth:** None
**Cadence:** Annual (prior year published ~April)
**Release window:** Months 4–6

| Column | Type | Description |
|---|---|---|
| export_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states (state of production/origin) |
| hs2 | VARCHAR | HS 2-digit chapter |
| hs_description | VARCHAR | Commodity description |
| country_code | VARCHAR | ISO 3-letter destination country code |
| country_name | VARCHAR | Destination country name |
| value_usd | DOUBLE | Export value (USD) |
| yoy_change_pct | DOUBLE | Year-over-year change (computed during ETL) |

---

### `bea_fdi_by_industry`

BEA Foreign Direct Investment statistics — inward FDI (foreign investment into the U.S.)
and outward FDI (U.S. investment abroad) by industry and partner country. Annual.
Joins to `sec.filing_metadata` for M&A context; joins to `ref.lei_entities` for
cross-border entity identification.

**Source:** BEA International Investment Position — `https://apps.bea.gov/api/`
datasets: `MNE` (Multinational Enterprises) and `IIP` (International Investment Position)
**Partition:** `fdi_year`
**Auth:** BEA API key (free)
**Cadence:** Annual (prior year published ~July)
**Release window:** Months 7–9

| Column | Type | Description |
|---|---|---|
| fdi_year | INTEGER | Partition key |
| direction | VARCHAR | Inward / Outward |
| partner_country | VARCHAR | Country of foreign parent (inward) or affiliate (outward) |
| industry | VARCHAR | BEA industry classification |
| naics_code | VARCHAR | NAICS equivalent code |
| position_usd_million | DOUBLE | FDI position (USD millions) |
| income_usd_million | DOUBLE | Investment income (USD millions) |
| employment_thousands | DOUBLE | Affiliate employment (thousands) |
| sales_usd_million | DOUBLE | Affiliate sales (USD millions) |
| assets_usd_million | DOUBLE | Total assets of affiliates (USD millions) |

---

## Join Architecture

```
[macro-level — use econ.ita_data / econ.trade_statistics for balance-of-payments]
econ.trade_balance_summary (existing)
    └── usa_trade_monthly (flow + year/month → granular HS-level commodity detail)
            └── sec.filing_metadata (HS chapter → NAICS → company import/export exposure)

geo.states (state_fips)
    └── usa_trade_by_state (state_fips, export_year)
            ├── econ.bls_employment (state_fips → manufacturing employment)
            └── fec.contributions (state_fips → trade-sensitive donor geography)

ref.lei_entities (lei)
    └── bea_fdi_by_industry (industry + country → cross-border M&A context)
```

---

## Worker Assignment

| Worker | Mode | Tables | Heap | Schedule |
|---|---|---|---|---|
| 94 | initial | All 3 tables, full history (2000+) | 4 GB / 6 GB | Once |
| 95 | recurring | `usa_trade_monthly` (monthly); `usa_trade_by_state`, `bea_fdi_by_industry` (annual) | 2 GB / 3 GB | Monthly |

---

## Release Windows

| Table | Window | Rationale |
|---|---|---|
| `usa_trade_monthly` | Months 1–12 | Monthly Census trade release |
| `usa_trade_by_state` | Months 4–6 | Annual state export data |
| `bea_fdi_by_industry` | Months 7–9 | BEA annual FDI release |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `TRADE_PARQUET_DIR` | Recommended | Output path (falls back to `${GOVDATA_PARQUET_DIR}/source=trade`) |
| `TRADE_CACHE_DIR` | Recommended | Raw download cache |
| `TRADE_BEA_API_KEY` | Required | BEA API key (free at apps.bea.gov) |
| `TRADE_SINCE_YEAR` | Optional | Start year for monthly trade data (default: 2000) |

---

## Implementation Notes

- **HS code vintage.** The Harmonized System is revised every 5 years (HS-2002, HS-2007,
  HS-2012, HS-2017, HS-2022). Census trade data uses the current HS vintage for each year.
  Long time series across revisions require HS concordance tables (UN Statistics Division
  publishes these). Store `hs_revision_year` for future concordance mapping.
- **Monthly trade volume.** At HS-6 × country × month, the dataset is ~2M rows/year.
  Filter to `hs2 IN (select industries of interest)` for targeted analysis; store full
  detail for initial load.
- **BEA API rate limits.** BEA enforces 100 requests/minute with an API key. The initial
  load for ITA quarterly data going back to 1960 requires ~200 API calls; implement
  exponential backoff.
