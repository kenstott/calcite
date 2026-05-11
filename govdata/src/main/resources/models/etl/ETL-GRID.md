# ETL Model Grid

## Data Source References

| Source | Documentation | Data Range |
|--------|---------------|------------|
| SEC EDGAR | [SEC XBRL](https://www.sec.gov/structureddata) | 2010+ (XBRL mandate) |
| Stock Prices | [Stooq.com](https://stooq.com) | 1980s+ (varies by ticker) |
| BLS | [BLS API](https://www.bls.gov/bls/api_features.htm) | ~2004+ (10-20yr rolling) |
| BEA | [BEA NIPA](https://apps.bea.gov/iTable/index_nipa.cfm) | 1929+ (annual) |
| FRED | [FRED API](https://fred.stlouisfed.org) | Varies (1940s+) |
| Treasury | [Treasury Rates](https://home.treasury.gov/policy-issues/financing-the-government/interest-rate-statistics) | 1990+ (daily yields) |
| World Bank | [World Bank API](https://datahelpdesk.worldbank.org/knowledgebase/articles/889386) | 1960s+ |
| Census ACS | [ACS Data](https://www.census.gov/data/developers/data-sets/acs-1year.html) | 2005+ |
| TIGER | [TIGER/Line](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html) | 2007+ (shapefiles) |
| HUD | [HUD Crosswalk](https://www.huduser.gov/portal/datasets/usps_crosswalk.html) | 2010+ |
| USFS ArcGIS (FACTS) | [USFS EDW](https://apps.fs.usda.gov/arcx/rest/services/EDW/) | 2004+ (harvest activities) |
| NPS IRMA | [NPS Stats](https://irmaservices.nps.gov/Stats/v1/) | 1904+ (most parks 1979+) |
| NPS ArcGIS | [NPS Boundaries](https://services1.arcgis.com/fBc8EJBxQRMcHlei/) | Current snapshot |
| BLM ArcGIS | [BLM Admin Units](https://gis.blm.gov/arcgis/rest/services/admin_boundaries/) | Current snapshot |
| ONRR Revenue Data | [ONRR Downloads](https://revenuedata.onrr.gov/downloads/) | FY 2004+ (bulk CSV) |
| FIA DataMart | [FIA API](https://apps.fs.usda.gov/fiadb-api/) | Annual estimates (1990+) |

---

## Supported Model Config Parameters

### SEC Schema
| Parameter | Type | Values | Description |
|-----------|------|--------|-------------|
| `ciks` | string/array | Tickers (AAPL), Groups (FAANG, SP500), CIKs (0000320193) | Company filter |
| `filingTypes` | array | 10-K, 10-Q, 8-K, 3, 4, 5, DEF 14A, S-3, S-4, S-8 | Form types |
| `startYear` | int | 2010-2026 | Start year (XBRL introduced 2010) |
| `endYear` | int | 2010-2026 | End year |
| `fetchStockPrices` | boolean | true/false | Enable stock price download (default: true) |
| `stockPriceSource` | string | stooq, alphavantage | Price data provider (default: stooq) |
| `stockPriceTtlHours` | int | 1-168 | Cache TTL in hours (default: 24) |

**Stock Price Notes:**
- Stooq: Permissive rate limit (~1 req/sec), ~100 min for 6,000 tickers, full history per request
- Alpha Vantage: Severe limits (25/day free tier), requires API key, impractical for bulk
- Stock prices are downloaded for the same CIKs/tickers as filings
- Historical data availability varies by ticker (AAPL back to 1984, newer tickers less history)

### ECON Schema
| Parameter | Type | Values | Description |
|-----------|------|--------|-------------|
| `enabledSources` | array | bls, bea, fred, treasury, worldbank | Data source filter |
| `blsConfig.includeTables` | array | employment_statistics, inflation_metrics, etc. | BLS table filter |
| `startYear` | int | varies by source (see grid) | Start year |
| `endYear` | int | 2026 | End year |

### CENSUS Schema
| Parameter | Type | Values | Description |
|-----------|------|--------|-------------|
| `enabledSources` | array | acs | Data source filter |
| `startYear` | int | 2005-2026 | Start year (ACS launched 2005) |
| `endYear` | int | 2005-2026 | End year |

### GEO Schema
| Parameter | Type | Values | Description |
|-----------|------|--------|-------------|
| `enabledSources` | array | tiger, hud | Data source filter |
| `tigerYear` | int | 2007-2025 (shapefiles) | TIGER boundary year |

---

## Data Availability Grid (2000-2026)

### SEC - By Filing Type (Priority Order)

| Priority | Filing Type | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 22 | 23 | 24 | 25 | 26 | Description |
|----------|-------------|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|-------------|
| 1 | 10-K | x | x | x | x | x | x | x | x | x | x | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | Annual report (most comprehensive) |
| 2 | 10-Q | x | x | x | x | x | x | x | x | x | x | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | Quarterly report |
| 3 | 8-K | x | x | x | x | x | x | x | x | x | x | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | Material events |
| 4 | 4 | x | x | x | x | x | x | x | x | x | x | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | Insider transactions |
| 5 | 3/5 | x | x | x | x | x | x | x | x | x | x | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | Initial/annual insider ownership |
| 5 | DEF 14A | x | x | x | x | x | x | x | x | x | x | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | Proxy statements |
| 5 | S-1/S-3/S-4 | x | x | x | x | x | x | x | x | x | x | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | Registration statements |

*XBRL mandate began 2009-2010. Pre-2010 filings lack structured data.*
*Priority: 1=most useful for financial analysis, 5=supplementary*

### Stock Prices - By Source

| Source | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 22 | 23 | 24 | 25 | 26 | Notes |
|--------|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|
| stooq | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ~1 req/sec, 100min/6000 tickers |
| alphavantage | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | 25/day free tier - impractical |

*Stock prices available earlier than XBRL (varies by ticker). Stooq recommended. Requires STOOQ_USERNAME/STOOQ_PASSWORD for premium rate limits.*

### SEC - CIK Groups (Download Priority Order)

**Priority:** DOW30 → SP500 → Russell → All (smallest to largest for phased testing)

| Priority | Group | Companies | Description |
|----------|-------|-----------|-------------|
| P1 | `_DJIA_CONSTITUENTS` | 30 | Dow Jones Industrial Average (fastest testing) |
| P2 | `_SP500_CONSTITUENTS` | ~500 | S&P 500 |
| P3 | `_RUSSELL1000_CONSTITUENTS` | ~1000 | Russell 1000 (large cap) |
| P3 | `_RUSSELL3000_CONSTITUENTS` | ~3000 | Russell 3000 (broad market) |
| P4 | `_ALL_EDGAR_FILERS` | ~7800+ | **All SEC EDGAR filers** (complete coverage) |

**Other Groups:**
| Group | Companies | Description |
|-------|-----------|-------------|
| `_RUSSELL2000_CONSTITUENTS` | ~2000 | Russell 2000 (small cap) |
| `_NYSE_LISTED` | varies | NYSE listed companies |
| `_NASDAQ_LISTED` | varies | NASDAQ listed companies |
| `FAANG` | 5 | Facebook, Apple, Amazon, Netflix, Google |
| `MAGNIFICENT7` | 7 | Apple, Microsoft, Google, Amazon, Nvidia, Meta, Tesla |
| `FINANCIALS` | ~50 | Major financial sector |
| `HEALTHCARE` | ~40 | Major healthcare sector |
| `ENERGY` | ~30 | Major energy sector |

*Note: Groups starting with `_` are dynamically fetched from SEC/external sources.*

### ECON - By Source

| Source | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 22 | 23 | 24 | 25 | 26 | Notes |
|--------|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|
| bls | x | x | x | x | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | API 10-20yr rolling window |
| bea | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | NIPA from 1929 |
| fred | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | 800k+ series |
| treasury | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | Daily yields from 1990 |
| worldbank | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | From 1960s |

### CENSUS - By Source

| Source | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 22 | 23 | 24 | 25 | 26 | Notes |
|--------|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|
| acs | x | x | x | x | x | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ACS launched 2005 |

### GEO - By Source

| Source | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 22 | 23 | 24 | 25 | 26 | Notes |
|--------|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|
| tiger | ● | x | x | x | x | x | x | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | x | 2000 legacy; 2007+ shapefiles |
| hud | x | x | x | x | x | x | x | x | x | x | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | x | From 2010 Q1 |

**Legend:**
- ○ = Annual data available
- ● = Point-in-time snapshot (boundaries/crosswalks)
- x = Not available

---

## Proposed Model Files

### Naming Convention

| Pattern | Description | Example |
|---------|-------------|---------|
| `{schema}-{year}.json` | Single year, all sources | `sec-2026.json` |
| `{schema}-{source}-{year}.json` | Single year, single source | `econ-bls-2026.json` |
| `{schema}-{start}-{end}.json` | Year range | `sec-2020-2026.json` |
| `sec-{formtype}-{year}.json` | SEC by form type | `sec-10k-2026.json` |
| `sec-{cikgroup}-{year}.json` | SEC by CIK group | `sec-sp500-2026.json` |

### SEC Models (XBRL 2010+)

**Download Priority:** 10-K → 10-Q → 8-K → Form 4 → Other

*When `ciks` is omitted, defaults to `_ALL_EDGAR_FILERS` (~7800+ companies)*

| Model File | ciks | filingTypes | Years | Priority |
|------------|------|-------------|-------|----------|
| `sec-10k-2026.json` | _ALL_EDGAR_FILERS | 10-K | 2026 | P1 |
| `sec-10k-2010-2026.json` | _ALL_EDGAR_FILERS | 10-K | 2010-2026 | P1 (full) |
| `sec-10q-2026.json` | _ALL_EDGAR_FILERS | 10-Q | 2026 | P2 |
| `sec-10q-2010-2026.json` | _ALL_EDGAR_FILERS | 10-Q | 2010-2026 | P2 (full) |
| `sec-8k-2026.json` | _ALL_EDGAR_FILERS | 8-K | 2026 | P3 |
| `sec-8k-2010-2026.json` | _ALL_EDGAR_FILERS | 8-K | 2010-2026 | P3 (full) |
| `sec-form4-2026.json` | _ALL_EDGAR_FILERS | 4 | 2026 | P4 |
| `sec-form4-2010-2026.json` | _ALL_EDGAR_FILERS | 4 | 2010-2026 | P4 (full) |
| `sec-insider-2026.json` | _ALL_EDGAR_FILERS | 3,4,5 | 2026 | P4-5 |
| `sec-other-2026.json` | _ALL_EDGAR_FILERS | DEF 14A,S-1,S-3,S-4 | 2026 | P5 |
| `sec-dow30-2026.json` | _DJIA_CONSTITUENTS | 10-K,10-Q,8-K | 2026 | CIK-P1 |
| `sec-dow30-2010-2026.json` | _DJIA_CONSTITUENTS | 10-K,10-Q,8-K | 2010-2026 | CIK-P1 (full) |
| `sec-sp500-2026.json` | _SP500_CONSTITUENTS | 10-K,10-Q,8-K | 2026 | CIK-P2 |
| `sec-sp500-2010-2026.json` | _SP500_CONSTITUENTS | 10-K,10-Q,8-K | 2010-2026 | CIK-P2 (full) |
| `sec-russell1000-2026.json` | _RUSSELL1000_CONSTITUENTS | 10-K,10-Q,8-K | 2026 | CIK-P3 |
| `sec-russell1000-2010-2026.json` | _RUSSELL1000_CONSTITUENTS | 10-K,10-Q,8-K | 2010-2026 | CIK-P3 (full) |
| `sec-2026.json` | _ALL_EDGAR_FILERS | 10-K,10-Q,8-K | 2026 | CIK-P4 |
| `sec-2010-2026.json` | _ALL_EDGAR_FILERS | 10-K,10-Q,8-K | 2010-2026 | CIK-P4 (full) |
| `sec-mag7-2026.json` | MAGNIFICENT7 | 10-K,10-Q,8-K | 2026 | Test |

### Stock Price Models (within SEC schema)

| Model File | ciks | fetchStockPrices | filingTypes | Years | Notes |
|------------|------|------------------|-------------|-------|-------|
| `sec-prices-only-2026.json` | _ALL_EDGAR_FILERS | true | (none) | 2026 | Stock prices only, no filings |
| `sec-prices-only-2020-2026.json` | _ALL_EDGAR_FILERS | true | (none) | 2020-2026 | Stock prices only |
| `sec-prices-only-2000-2019.json` | _ALL_EDGAR_FILERS | true | (none) | 2000-2019 | Historical prices (pre-XBRL) |
| `sec-prices-only-sp500-2026.json` | _SP500_CONSTITUENTS | true | (none) | 2026 | S&P 500 prices only |
| `sec-noprices-2026.json` | _ALL_EDGAR_FILERS | false | 10-K,10-Q,8-K | 2026 | Filings only, skip prices |
| `sec-noprices-sp500-2026.json` | _SP500_CONSTITUENTS | false | 10-K,10-Q,8-K | 2026 | SP500 filings only |

*Note: Stock prices use same `ciks` filter as filings but have wider year range availability.*
*`_ALL_EDGAR_FILERS` = ~7800+ companies, `_SP500_CONSTITUENTS` = ~500 companies*

### ECON Models

| Model File | enabledSources | Years | Notes |
|------------|----------------|-------|-------|
| `econ-2026.json` | (all) | 2026 | |
| `econ-2025.json` | (all) | 2025 | |
| `econ-2020-2026.json` | (all) | 2020-2026 | |
| `econ-2010-2019.json` | (all) | 2010-2019 | |
| `econ-2004-2009.json` | (all) | 2004-2009 | BLS earliest |
| `econ-2000-2003.json` | bea,fred,treasury,worldbank | 2000-2003 | No BLS |
| `econ-2000-2026.json` | bea,fred,treasury,worldbank | 2000-2026 | Full (no BLS) |
| `econ-bls-2026.json` | bls | 2026 | BLS only |
| `econ-bea-2026.json` | bea | 2026 | |
| `econ-fred-2026.json` | fred | 2026 | |
| `econ-treasury-2026.json` | treasury | 2026 | |
| `econ-nobls-2026.json` | bea,fred,treasury,worldbank | 2026 | Skip BLS rate limits |

### CENSUS Models

| Model File | enabledSources | Years | Notes |
|------------|----------------|-------|-------|
| `census-2026.json` | acs | 2026 | |
| `census-2025.json` | acs | 2025 | |
| `census-2020-2026.json` | acs | 2020-2026 | |
| `census-2010-2019.json` | acs | 2010-2019 | |
| `census-2005-2009.json` | acs | 2005-2009 | ACS earliest |
| `census-2005-2026.json` | acs | 2005-2026 | Full ACS |

### GEO Models

| Model File | enabledSources | tigerYear | Notes |
|------------|----------------|-----------|-------|
| `geo-2024.json` | tiger,hud | 2024 | Current |
| `geo-2020.json` | tiger,hud | 2020 | Decennial |
| `geo-2010.json` | tiger,hud | 2010 | First HUD year |
| `geo-2000.json` | tiger | 2000 | Decennial (legacy format) |
| `geo-tiger-2024.json` | tiger | 2024 | TIGER only |
| `geo-hud-2024.json` | hud | 2024 | HUD only |

### Combined Models

| Model File | Schemas | Years | Notes |
|------------|---------|-------|-------|
| `all-2026.json` | sec,econ,census,geo | 2026 | Current year |
| `all-2020-2026.json` | sec,econ,census,geo | 2020-2026 | Recent |
| `all-2010-2019.json` | sec,econ,census,geo | 2010-2019 | Decade |
| `all-2010-2026.json` | sec,econ,census,geo | 2010-2026 | Full XBRL era |

---

## Execution Strategy

### Phase 1: Validate Single Sources (current year, smallest CIK group first)
```bash
# Start with DOW30 (~30 companies) - fastest validation
./scripts/etl-runner.sh --model etl/sec-dow30-2026.json
./scripts/etl-verify.sh --model etl/sec-dow30-2026.json

# Then SP500 (~500 companies)
./scripts/etl-runner.sh --model etl/sec-sp500-2026.json
./scripts/etl-verify.sh --model etl/sec-sp500-2026.json

# Test stock prices separately (can run pre-2010 unlike filings)
./scripts/etl-runner.sh --model etl/sec-prices-only-sp500-2026.json
./scripts/etl-verify.sh --model etl/sec-prices-only-sp500-2026.json

./scripts/etl-runner.sh --model etl/econ-fred-2026.json
./scripts/etl-verify.sh --model etl/econ-fred-2026.json

./scripts/etl-runner.sh --model etl/census-2026.json
./scripts/etl-verify.sh --model etl/census-2026.json

./scripts/etl-runner.sh --model etl/geo-2024.json
./scripts/etl-verify.sh --model etl/geo-2024.json
```

### Phase 2: Expand CIK Coverage (current year)
```bash
# CIK Priority: DOW30 → SP500 → Russell → All
./scripts/etl-runner.sh --model etl/sec-dow30-2026.json      # P1: 30 companies
./scripts/etl-runner.sh --model etl/sec-sp500-2026.json      # P2: ~500 companies
./scripts/etl-runner.sh --model etl/sec-russell1000-2026.json # P3: ~1000 companies
./scripts/etl-runner.sh --model etl/sec-2026.json            # P4: ~7800+ companies

# Other data sources (parallel)
./scripts/etl-runner.sh --model etl/econ-2026.json
./scripts/etl-runner.sh --model etl/census-2026.json
./scripts/etl-runner.sh --model etl/geo-2024.json
```

### Phase 3: Historical Backfill (Two-Dimensional Priority)

**Strategy:** Filing Type Priority × CIK Group Priority
- Filing: 10-K → 10-Q → 8-K → Form 4 → Other
- CIK: DOW30 → SP500 → Russell → All

```bash
# Most valuable combination first: 10-K × DOW30
./scripts/etl-runner.sh --model etl/sec-dow30-2010-2026.json   # Core filings, smallest group

# Expand to 10-K × SP500
./scripts/etl-runner.sh --model etl/sec-sp500-2010-2026.json

# Expand to 10-K × Russell1000
./scripts/etl-runner.sh --model etl/sec-russell1000-2010-2026.json

# Then all companies by filing type priority
./scripts/etl-runner.sh --model etl/sec-10k-2010-2026.json     # P1: 10-K (all)
./scripts/etl-runner.sh --model etl/sec-10q-2010-2026.json     # P2: 10-Q (all)
./scripts/etl-runner.sh --model etl/sec-8k-2010-2026.json      # P3: 8-K (all)
./scripts/etl-runner.sh --model etl/sec-form4-2010-2026.json   # P4: Form 4 (all)
./scripts/etl-runner.sh --model etl/sec-other-2010-2026.json   # P5: Other (all)

# STOCK PRICES: 2000-2026 (wider availability than XBRL)
# Note: ~100 minutes per 6,000 tickers with Stooq (~1 req/sec)
./scripts/etl-runner.sh --model etl/sec-prices-only-2020-2026.json
./scripts/etl-runner.sh --model etl/sec-prices-only-2000-2019.json  # Pre-XBRL prices!

# ECON: 2000-2026 (sources vary)
./scripts/etl-runner.sh --model etl/econ-2020-2026.json
./scripts/etl-runner.sh --model etl/econ-2010-2019.json
./scripts/etl-runner.sh --model etl/econ-2004-2009.json
./scripts/etl-runner.sh --model etl/econ-2000-2003.json  # no BLS

# CENSUS: 2005-2026 (ACS era)
./scripts/etl-runner.sh --model etl/census-2020-2026.json
./scripts/etl-runner.sh --model etl/census-2010-2019.json
./scripts/etl-runner.sh --model etl/census-2005-2009.json

# GEO: snapshots
./scripts/etl-runner.sh --model etl/geo-2020.json
./scripts/etl-runner.sh --model etl/geo-2010.json
./scripts/etl-runner.sh --model etl/geo-2000.json
```

### Phase 4: Full Population
```bash
./scripts/etl-runner.sh --model etl/all-2010-2026.json
```
