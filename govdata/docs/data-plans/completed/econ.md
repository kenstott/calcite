# Econ Schema Data Plan

## Status: DELIVERED (2026-07-16)

The trade / international / science-input tables proposed here are **implemented and merged to main**. They ship in [econ-schema.yaml](../../../src/main/resources/econ/econ-schema.yaml) with transformers under [org.apache.calcite.adapter.govdata.econ](../../../src/main/java/org/apache/calcite/adapter/govdata/econ/).

| Planned table | Delivered as | Java transformer |
|---|---|---|
| `usa_trade_monthly` | `trade_exports` + `trade_imports` (split by flow) | `CensusTradeStreamingTransformer` |
| `usa_trade_by_state` | `trade_by_state` | `CensusTradeStreamingTransformer` |
| `bea_fdi_by_industry` | `fdi_direct_investment` + `fdi_activities` (+ views `fdi_by_country`, `fdi_data`) | `FdiDataTransformer` |
| `IIP` | `iip_positions` | `IipDataTransformer` |
| `usitc_tariffs` | `usitc_tariffs` | `UsitcTariffsTransformer` |
| `usitc_tariff_schedule` | `usitc_tariff_schedule` | `UsitcHtsScheduleTransformer` |
| `comtrade_flows` (Follow-on 2) | `comtrade_flows` (+ views `comtrade_bilateral_trade`, `comtrade_partner_balances`) | `ComtradeTransformer` |
| `ilostat_indicators` (Follow-on 3) | `ilostat_indicators` (+ view `ilostat_labor_snapshot`) | `IlostatTransformer` |
| `world_indicators` (Follow-on 1) | pre-existing `world_indicators` | catalog-driven (`worldbank-indicators.json`) |

**Delivery deviations:**
- `bea_international_transactions` was **not** built — the plan itself instructed skipping it when the existing `ita_data` table (`ItaDataTransformer`) already covers the balance-of-payments series.
- The USITC tariff pair was the last remaining gap; merged 2026-07-16 (`a74c04c0b`). `usitc_tariffs` is per-year DataWeb realized-duty microdata; `usitc_tariff_schedule` is the statutory HTS rate lines.

The proposal below is preserved for historical context.

The `econ` schema is the project's home for **cross-country economics and U.S. macro/trade
data**. It already carries the BLS/BEA/FRED macro series, World Bank benchmarking
(`world_indicators`), and the full trade/FDI stack. Because trade tables *are* econ tables,
their plan lives here rather than in a separate `trade` doc — there is no standalone `trade`
schema.

This document covers the econ schema's **trade/FDI/tariff** tables and its **international /
cross-country** tables (World Bank, UN Comtrade, ILOSTAT). One remaining econ source family is
still documented separately:

- [research.md](research.md) — NSF/NCSES R&D tables (`nsf_national_rd`,
  `nsf_herd_by_institution`, `nsf_federal_rd_obligations`)

Two UN feeds are routed to the schema that owns their U.S. analog rather than to `econ`:
WHO GHO → [health.md](health.md), FAOSTAT → [ag.md](ag.md).

---

## Trade / FDI / Tariffs

### Implementation Status (2026-07-08)

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

### Existing Coverage in ECON Schema

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

### Strategic Context (Extensions)

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
| Tariffs, duties & customs value (HTS) | USITC DataWeb | REST API + bulk | Free, token required | Annual/Monthly |

Census trade API: `https://api.census.gov/data/timeseries/intltrade/`
BEA API: `https://apps.bea.gov/api/` (key: free at apps.bea.gov/API/signup)
USITC DataWeb API: `https://datawebws.usitc.gov/dataweb` (token: free at dataweb.usitc.gov)

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

## USITC — Tariffs & Duties (the schedule dimension)

The Census tables above (`trade_exports` / `trade_imports` / `trade_by_state` in `econ`) carry
trade **flows** (values and quantities). They do not carry the **tariff schedule** — the duty
rates and duties actually collected. The **U.S. International Trade Commission (USITC)** publishes
that missing dimension through its **DataWeb** system: HTS-8 customs value, dutiable value,
calculated duties, and ad-valorem-equivalent duty rates by commodity × partner country × year,
broken out by trade program (General, GSP, USMCA/other FTA). Joining USITC duty rates onto the
Census import flows yields effective-tariff-rate analysis and company tariff-exposure research
that neither source supports alone.

USITC is the last of the notable missing U.S. trade agencies (Census flows and BEA
balance-of-payments/FDI are already covered). DataWeb is free but requires a **token** (one-time
free registration at `dataweb.usitc.gov`), passed via a schema-YAML env var per rule #7.

**Placement — these land in `econ`, not a `trade` schema.** As the Implementation Status at the
top of this doc records, there is no standalone `trade` schema; all trade tables were delivered
into `econ` (`econ.trade_imports`, `econ.trade_by_state`, `econ.fdi_*`, `econ.iip_positions`).
The USITC tables follow the same rule — they are `econ.usitc_tariffs` and
`econ.usitc_tariff_schedule`, sitting directly beside the Census flows they join to. The
unqualified names below are the `econ`-schema table names.

### `usitc_tariffs`

USITC DataWeb import statistics with the duty/tariff breakout — one row per HTS-8 × partner
country × year × trade program. The tariff-schedule counterpart to `econ.trade_imports`; the
`hs6` prefix column is the join key back to the Census HS-6 flows.

**Source:** USITC DataWeb API — `https://datawebws.usitc.gov/dataweb` (import + duty query)
**Partition:** `[type, year]` — `type` fixed to `imports`; fan-out on `year`
**Auth:** USITC DataWeb API token (`TRADE_USITC_API_TOKEN`)
**Cadence:** Annual (HTS revised annually; DataWeb also offers monthly — annual for parity with `trade_by_state`)
**Release window:** Months 3–6 (prior-year final trade + duty data)

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Data year — partition key |
| hts8 | VARCHAR | HTS 8-digit commodity code (U.S. tariff-line detail) |
| hs6 | VARCHAR | HS-6 prefix — join key to `econ.trade_imports.hs6` |
| hts_description | VARCHAR | Commodity description |
| country_code | VARCHAR | ISO partner country code (FK to ref.countries) |
| country_name | VARCHAR | Partner country name |
| trade_program | VARCHAR | General / GSP / USMCA / other FTA (special-rate program) |
| customs_value_usd | DOUBLE | Customs (import) value |
| dutiable_value_usd | DOUBLE | Dutiable value (portion subject to duty) |
| calculated_duties_usd | DOUBLE | Calculated duties collected |
| ave_duty_rate | DOUBLE | Ad-valorem-equivalent effective duty rate (`calculated_duties / dutiable_value`) |
| first_unit_quantity | DOUBLE | Quantity (first HTS unit of measure) |
| first_unit_of_measure | VARCHAR | First unit of measure (KG, NO, DOZ, …) |

### `usitc_tariff_schedule`

The statutory **Harmonized Tariff Schedule** rate lines (as opposed to duties actually
collected) — one row per HTS-8 × rate-column × effective period. The reference table that lets
analysts see the *published* general/special/column-2 rates independent of realized trade.
Slowly-changing; keyed on HTS-8.

**Source:** USITC HTS data — `https://hts.usitc.gov/` (HTS export JSON/CSV)
**Partition:** `[year]` (HTS revision year)
**Auth:** None (HTS export is open; only DataWeb statistics need the token)
**Cadence:** Annual (HTS reissued each January, with intra-year revisions)
**Release window:** Months 1–3

| Column | Type | Description |
|---|---|---|
| year | INTEGER | HTS revision year — partition key |
| hts8 | VARCHAR | HTS 8-digit code |
| hs6 | VARCHAR | HS-6 prefix (join key) |
| description | VARCHAR | Tariff-line description |
| unit_of_quantity | VARCHAR | Statutory unit(s) of quantity |
| general_rate | VARCHAR | Column-1 General (MFN/normal trade relations) rate text |
| general_ave | DOUBLE | Ad-valorem equivalent of the general rate (null if non-ad-valorem/complex) |
| special_rate | VARCHAR | Column-1 Special (FTA/preference) rate text |
| column2_rate | VARCHAR | Column-2 (non-NTR) rate text |
| effective_date | DATE | Effective date of this rate line |

**Materialize (both USITC tables):** `format: iceberg`, `overwritePartitions: true` — USITC
restates/reissues per year (same pattern as the Census `trade_*` tables). New
`UsitcTariffsTransformer` (DataWeb query, paginated by year, token from the model) and
`UsitcHtsScheduleTransformer` (HTS export). Map the DataWeb country field to `ref.countries` and
**fail loudly** (rule #6) on an unmapped real partner; never default to a placeholder country.

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

econ.trade_imports (hs6, country_code, year)          [Census import FLOWS — value/quantity]
    └── usitc_tariffs (hs6, country_code, year)        [USITC duty SCHEDULE — rate/duties]
            ├── effective-tariff-rate = calculated_duties / customs_value by hs6 × country
            ├── usitc_tariff_schedule (hts8 → statutory general/special/column-2 rates)
            └── sec.filing_metadata (NAICS↔HS crosswalk → company tariff exposure)
ref.countries (iso code)
    └── usitc_tariffs (country_code → partner-country duty profile)
```

---

## Worker Assignment

| Worker | Mode | Tables | Heap | Schedule |
|---|---|---|---|---|
| 94 | initial | All 3 Census/BEA tables + `usitc_tariffs`, `usitc_tariff_schedule`, full history (2000+) | 4 GB / 6 GB | Once |
| 95 | recurring | `usa_trade_monthly` (monthly); `usa_trade_by_state`, `bea_fdi_by_industry`, `usitc_tariffs` (annual, months 3–6); `usitc_tariff_schedule` (annual, months 1–3) | 2 GB / 3 GB | Monthly |

---

## Release Windows

| Table | Window | Rationale |
|---|---|---|
| `usa_trade_monthly` | Months 1–12 | Monthly Census trade release |
| `usa_trade_by_state` | Months 4–6 | Annual state export data |
| `bea_fdi_by_industry` | Months 7–9 | BEA annual FDI release |
| `usitc_tariffs` | Months 3–6 | USITC DataWeb prior-year final duty data |
| `usitc_tariff_schedule` | Months 1–3 | USITC HTS reissued each January |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `TRADE_PARQUET_DIR` | Recommended | Output path (falls back to `${GOVDATA_PARQUET_DIR}/source=trade`) |
| `TRADE_CACHE_DIR` | Recommended | Raw download cache |
| `TRADE_BEA_API_KEY` | Required | BEA API key (free at apps.bea.gov) |
| `TRADE_USITC_API_TOKEN` | Required (USITC tables) | USITC DataWeb API token (free at dataweb.usitc.gov); read from the model, never `System.getenv` |
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
- **USITC DataWeb token + query shape.** DataWeb requires a free bearer token
  (`TRADE_USITC_API_TOKEN`, declared in the schema YAML, read from the model). Queries are POSTed
  as JSON specs (measure = customs value / dutiable value / calculated duty; classification =
  HTS-8; partner = country). Page by year; one year of HTS-8 × country import+duty detail is
  ~1–2M rows. The `hts8` → `hs6` prefix is a substring, not a lookup — derive it during ETL so
  the join to `econ.trade_imports.hs6` is exact.
- **HTS vs. duties.** `usitc_tariff_schedule` is the *statutory* rate (what the law says);
  `usitc_tariffs` is the *realized* duty (what was collected). Effective rates differ from
  statutory rates because of FTA/GSP program eligibility — keep both so analysts can compare
  scheduled vs. effective tariffs. Non-ad-valorem rates (e.g. cents/kg) leave `general_ave` null;
  do not fabricate an AVE — leave null and let downstream views handle it (rule #6).
- **HS/HTS concordance reuse.** The same HS-vintage concordance issue noted above applies to
  HTS-8; store `year` so the UN/USITC concordance can be applied later without re-ingest.

---

## International / Cross-Country

International economic data lands in the `econ` schema (not a separate `intl` schema),
alongside the trade tables above. This keeps World Bank benchmarking, U.S. trade, and
bilateral world trade queryable in one place.

### Implementation Status (2026-07-15)

- **World Bank WDI — DELIVERED** as `econ.world_indicators`. Bulk fetch (all countries, one
  API call per indicator, response-partitioned on write, `overwritePartitions`), fanned out
  over a **30-indicator curated catalog** in `worldbank/worldbank-indicators.json` (GDP,
  GDP/capita, GNI, CPI inflation, unemployment, labor force, population, life expectancy,
  current-account balance, FDI, gov debt, reserves, real interest rate, exchange rate,
  school enrollment, health/education spend, Gini, energy use, CO₂/capita). Partitioned
  `[type, frequency, year]`; FK `countryiso3code → ref.countries.iso_alpha3`. **No new table
  needed** — the only follow-on is *expanding the indicator catalog* (below).
- **U.S. trade — DELIVERED** as `econ.trade_exports` / `trade_imports`, `trade_by_state`,
  `ita_data`, and the BEA FDI/IIP tables — see the **Trade / FDI / Tariffs** section above.
- **UN Comtrade — NOT STARTED.** The one genuine gap: no existing econ table carries the
  **partner side** of a trade flow (reporter × partner × commodity). Specified below as the
  single net-new econ table.

### Strategic Context

Every other govdata schema is U.S.-domestic; `econ` is where cross-country economics belongs.
World Bank WDI already provides the macro benchmarking spine (U.S. vs. peer countries), and
the Census/BEA tables cover the U.S. side of trade. The missing capability is **bilateral
world trade** — which country trades what with which partner — the international complement
that turns the one-sided U.S. Census flows into a two-sided, reconcilable picture and enables
supply-chain analysis that never touches a U.S. port.

### Follow-on 1 (no new table): expand `world_indicators` catalog

`world_indicators` is catalog-driven — adding indicators is a **data-only edit** to
`govdata/src/main/resources/worldbank/worldbank-indicators.json`, no schema or Java change.
The current 30 codes are macro-heavy; candidate additions that concord to existing govdata
schemas (keep the list curated — `log()` the count, never ingest the full ~1,400-code WDI
catalog):

| Concords to | Candidate WDI codes |
|---|---|
| `health` | `SH.STA.MMRT` (maternal mortality), `SH.DYN.MORT` (under-5 mortality), `SH.MED.BEDS.ZS` (hospital beds), `SH.IMM.MEAS` (measles immunization) |
| `edu` | `SE.TER.ENRR` (tertiary enrollment), `SE.ADT.LITR.ZS` (adult literacy), `SE.PRM.CMPT.ZS` (primary completion) |
| `environment` | `EN.ATM.PM25.MC.M3` (PM2.5 exposure), `AG.LND.FRST.ZS` (forest area), `ER.H2O.FWTL.ZS` (freshwater withdrawal) |
| `ag` | `AG.PRD.FOOD.XD` (food production index), `AG.LND.AGRI.ZS` (agricultural land %) |
| `bls` / labor | `SL.EMP.TOTL.SP.ZS` (employment-to-population), `SL.TLF.CACT.FE.ZS` (female labor force participation) |

### Follow-on 2 (net-new table): `comtrade_flows`

UN Comtrade bilateral merchandise trade — reported value (and quantity) per reporter country
× partner country × HS commodity × trade flow × year. Tall (one ETL table = one source, per
project rule). Joins to `ref.countries` twice (reporter, partner); reconciles against
`econ.trade_exports` / `trade_imports` on HS-6 + country (two-sided check); joins to
`sec.filing_metadata` via HS→NAICS concordance for company import/export exposure.

**Source:** UN Comtrade v1 —
`https://comtradeapi.un.org/data/v1/get/C/A/HS?reporterCode={reporter}&period={effective_year}&flowCode={flow}`
(`C`=commodities, `A`=annual, `HS` classification)
**Partition:** `[type, year]` — fan-out dimension is `reporter_code` (per-country call keeps each response bounded)
**Auth:** `${COMTRADE_API_KEY}` — free tier, rate-limited (declared in YAML, read from the model; never `System.getenv`)
**Cadence:** Annual (prior-year data accrues as reporters submit); monthly `M` frequency is a possible follow-on table
**Release window:** Months 3–9

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Trade year — partition column |
| reporter_code | VARCHAR | Reporting country — ISO alpha-3 (FK to ref.countries.iso_alpha3) |
| partner_code | VARCHAR | Partner country — ISO alpha-3 (FK to ref.countries.iso_alpha3; `WLD` = world total) |
| flow | VARCHAR | Import / Export / Re-import / Re-export |
| hs2 | VARCHAR | HS 2-digit chapter |
| hs6 | VARCHAR | HS 6-digit commodity code |
| hs_description | VARCHAR | Commodity description |
| hs_revision | VARCHAR | HS vintage of the reported code (H0…H6) — for cross-revision concordance |
| trade_value_usd | DOUBLE | Trade value (USD) |
| net_weight_kg | DOUBLE | Net weight (kg) when reported |
| quantity | DOUBLE | Quantity in quantity_unit when reported |
| quantity_unit | VARCHAR | Supplementary quantity unit |
| is_aggregate | BOOLEAN | True for `partner_code='WLD'` / estimated roll-up rows (exclude from partner-level joins) |

**Materialize:** `format: iceberg`, partition `[type, year]`, `overwritePartitions: true`
(reporters revise prior years, so each year partition is replaced wholesale — same pattern as
`world_indicators` and `trade_by_state`, and the fix that kept `ag.ers_farm_income` from
duplicating to billions of rows).

**Constraints:**

```
comtrade_flows:
  primaryKey: [type, year, reporter_code, partner_code, flow, hs6, hs_revision]
  foreignKeys:
    - columns: [reporter_code]  → ref.countries.iso_alpha3
    - columns: [partner_code]   → ref.countries.iso_alpha3   # excludes WLD / aggregate rows
```

**Transformer:** new `ComtradeTransformer` (Java) to (1) map Comtrade's **M49 numeric**
reporter/partner codes to ISO alpha-3 via `ref.countries.iso_numeric → iso_alpha3` (ISO
3166-1 numeric == M49 for all assigned countries, so **no new crosswalk column is needed** —
the mapping already exists); (2) split the reported HS code into `hs2`/`hs6`; (3) flag
Comtrade's non-country pseudo-codes — `0`/`WLD` (World), and "nes"/bunkers/free-zone
partners (490 Other Asia nes, 837 bunkers, 899 Areas nes, …) — as `is_aggregate` and exclude
them from the partner FK. A **real** M49 country code that fails to resolve must **fail
loudly**, not silently drop (rule #6); the pseudo-codes above are the known, enumerated
exceptions, not failures.

### Follow-on 3 (net-new table): `ilostat_indicators`

ILOSTAT (UN ILO) is the international analog of the U.S. **labor** data — which lives in
`econ` (`employment_statistics`, `state`/`metro`/`county_wages`, `jolts_*`, via
`BlsResponseTransformer`), **not** a standalone `bls` schema. So ILOSTAT belongs here in
`econ`, alongside `world_indicators` and `comtrade_flows`.

Labor market indicators (employment, unemployment, wages, hours, informality) per country ×
indicator × classification × year. Tall. Joins to `ref.countries` on `country_code`;
reconciles semantically against the econ BLS tables for U.S.-vs-world labor comparison.

**Source:** ILOSTAT SDMX — `https://sdmx.ilo.org/rest/data/ILO,{dataflow}/...?format=jsondata`
(bulk CSV fallback at `ilo.org/ilostat-files/WEB_bulk_download/`)
**Partition:** `[type, year]` — fan-out dimension is `indicator_code` (curated catalog)
**Auth:** None
**Cadence:** Annual (some indicators quarterly upstream; annual ingested for parity)
**Release window:** Months 1–12

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Reference year — partition column |
| indicator_code | VARCHAR | ILOSTAT indicator code (e.g. `UNE_DEAP_SEX_AGE_RT`) |
| indicator_name | VARCHAR | Indicator label |
| country_code | VARCHAR | ISO alpha-3 (FK to ref.countries.iso_alpha3; mapped from ILO `ref_area`) |
| sex | VARCHAR | Sex classification (Total / Male / Female) |
| classif1 | VARCHAR | Primary classification value (age band, sector, …); "Total" when none |
| classif2 | VARCHAR | Secondary classification value when present |
| value | DOUBLE | Indicator value (unit implied by indicator; null when unreported) |
| unit | VARCHAR | Unit / measure (rate %, thousands, local currency, …) |
| obs_status | VARCHAR | ILO observation status flag |

**Materialize:** iceberg, partition `[type, year]`, `overwritePartitions: true` (ILO
republishes full history). **Transformer:** new `IlostatTransformer` mapping ILO `ref_area`
(ISO alpha-3) → `ref.countries.iso_alpha3`, fail-loud on an unmapped real country, flag ILO
regional/income aggregates as `is_aggregate`. Curated `indicator_code` catalog in an
`ilo/ilostat-indicators.json` resource (mirror the World Bank catalog pattern); `log()` the count.

### Routed to analog schemas (planned)

The remaining UN feeds are owned by the schema that holds their U.S. analog:

| Source | Schema | Why | Plan |
|---|---|---|---|
| WHO GHO | `health` | Analog of the U.S. openFDA/clinical `health` tables | [health.md](health.md) |
| FAOSTAT (FAO) | `ag` | Analog of USDA NASS/ERS production, land, food balance | [ag.md](ag.md) |

> ILOSTAT is **not** in this table — U.S. labor lives in `econ` (there is no `bls` schema), so
> ILOSTAT is an econ feed (Follow-on 3 above). The health/education/environment/ag WDI codes
> in *Follow-on 1* also stay in `econ.world_indicators` — that is one catalog-driven World
> Bank feed, not split by topic.

### International Environment Variables

`econ` uses the **global** `GOVDATA_*` knobs (no per-schema parquet/cache/year prefix) plus
per-source API keys. Comtrade adds exactly one new key.

| Variable | Required | Description |
|---|---|---|
| `COMTRADE_API_KEY` | Required (Comtrade only) | UN Comtrade API key (free at comtradedeveloper.un.org) |

### International Implementation Notes

- **Country-code normalization.** `world_indicators` already resolves cleanly (World Bank
  returns ISO alpha-3). Comtrade uses **M49 numeric**, which equals ISO 3166-1 numeric for
  all assigned countries — so it joins directly through the **existing** `ref.countries`
  columns (`iso_numeric → iso_alpha3`); **no new crosswalk column is needed**. The transformer
  fails loudly on a real unmapped country code (no fallback) but treats Comtrade's enumerated
  non-country pseudo-codes (`0`/`WLD`, bunkers/free-zone/"nes" partners) as `is_aggregate`.
- **Aggregates vs. countries.** World Bank ("World", "OECD", income groups) and Comtrade
  (`partner_code='WLD'`) interleave non-country aggregates. Keep them (useful denominators)
  but flag them (`is_aggregate`) so partner-level joins to `ref.countries` exclude them
  without dropping data — the existing `world_indicators` FK comment already notes aggregates
  do not resolve.
- **HS revision vintage** (Comtrade). Reports use the HS vintage in force for each year
  (H0=1992 … H6=2022). Store `hs_revision` and reuse the UN Statistics Division HS
  concordance tables (the same ones the Trade / FDI / Tariffs section calls out) for long
  cross-revision series.
- **Comtrade rate limits.** Free tier is request- and volume-capped. Fan out by reporter ×
  year with `requestsPerSecond` throttling and exponential backoff on 429; enable
  `rawCache` so re-runs don't re-hit the API.
- **Overwrite semantics.** Comtrade reporters revise prior years, so materialize must
  **replace** each year partition (`overwritePartitions: true`), matching `world_indicators`.
- **Verification gate.** Net-new work (`comtrade_flows` + its transformer) is not "done" until
  a keyed ETL/DQ run (`COMTRADE_API_KEY`) plus `verify-tables` passes end-to-end through the
  Calcite→DuckDB read path.
</content>
</invoke>
