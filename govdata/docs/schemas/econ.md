# ECON Schema Documentation

## Overview

The ECON schema provides unified access to economic indicators from multiple U.S. government sources including the Bureau of Labor Statistics (BLS), Federal Reserve Economic Data (FRED), Treasury Direct, Bureau of Economic Analysis (BEA), and World Bank.

## Architecture Note: FileSchema Delegation

The ECON schema operates as a **declarative data pipeline** that:
1. **Downloads** economic data from multiple government APIs
2. **Transforms** time series data to optimized Parquet format
3. **Enriches** tables with metadata, comments, and foreign key constraints
4. **Delegates** actual query execution to the FileSchema adapter

This means you get the benefits of:
- **Unified access** - Multiple APIs through single SQL interface
- **Automatic updates** - Data refreshed based on configuration
- **API key management** - Handles authentication transparently
- **Cross-domain joins** - Foreign keys link to GEO schema for regional analysis

## Tables

### Employment and Labor Tables

#### `employment_statistics`
National employment and unemployment data from BLS.

Primary key: `(date, series_id)`

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Observation date |
| series_id | VARCHAR | BLS/FRED series identifier |
| series_name | VARCHAR | Human-readable series name |
| value | DECIMAL | Statistical value |
| unit | VARCHAR | Unit of measure |
| seasonally_adjusted | BOOLEAN | Whether seasonally adjusted |
| percent_change_month | DECIMAL | Month-over-month change |
| percent_change_year | DECIMAL | Year-over-year change |
| category | VARCHAR | Data category |
| subcategory | VARCHAR | Data subcategory |

#### `regional_employment`
State and metropolitan area employment statistics.

Primary key: `(date, area_code)`

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Observation date |
| area_code | VARCHAR | Geographic area code |
| area_name | VARCHAR | Area name |
| area_type | VARCHAR | Type (state, MSA, county) |
| state_code | VARCHAR | 2-letter state code (FK → geo.tiger_states.state_code) |
| unemployment_rate | DECIMAL | Unemployment percentage |
| employment_level | BIGINT | Number employed |
| labor_force | BIGINT | Total labor force |
| participation_rate | DECIMAL | Labor force participation rate |
| employment_population_ratio | DECIMAL | Employment to population ratio |

#### `wage_growth`
Earnings by industry and occupation from BLS.

Primary key: `(date, series_id, industry_code, occupation_code)`

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Observation date |
| series_id | VARCHAR | BLS series identifier |
| industry_code | VARCHAR | NAICS industry code |
| occupation_code | VARCHAR | SOC occupation code |
| industry_name | VARCHAR | Industry description |
| occupation_name | VARCHAR | Occupation description |
| average_hourly_earnings | DECIMAL | Average hourly wage |
| average_weekly_earnings | DECIMAL | Average weekly earnings |
| employment_cost_index | DECIMAL | Employment cost index |
| percent_change_year | DECIMAL | Year-over-year change |

### Price and Inflation Tables

#### `inflation_metrics`
Consumer and producer price indices from BLS.

Primary key: `(date, index_type, item_code, area_code)`

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Observation date |
| index_type | VARCHAR | CPI-U, CPI-W, PPI, etc. |
| item_code | VARCHAR | Item/category code |
| area_code | VARCHAR | Geographic area |
| item_name | VARCHAR | Item description |
| index_value | DECIMAL | Index value |
| percent_change_month | DECIMAL | Monthly change |
| percent_change_year | DECIMAL | Annual change |
| area_name | VARCHAR | Area description |
| seasonally_adjusted | BOOLEAN | Whether seasonally adjusted |

### Treasury and Federal Debt Tables

#### `treasury_yields`
Treasury yield curves and auction data.

Primary key: `(date, maturity_months)`

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Observation date |
| maturity_months | INTEGER | Maturity in months |
| yield_rate | DECIMAL | Yield percentage |
| treasury_type | VARCHAR | Bill, Note, Bond |
| bid_to_cover_ratio | DECIMAL | Auction bid-to-cover |
| high_yield | DECIMAL | Auction high yield |
| low_yield | DECIMAL | Auction low yield |
| accepted_amount | BIGINT | Amount accepted ($) |
| tendered_amount | BIGINT | Amount tendered ($) |

#### `federal_debt`
Federal debt statistics from Treasury.

Primary key: `(date, debt_type)`

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Observation date |
| debt_type | VARCHAR | Type of debt |
| debt_amount | DECIMAL | Amount in billions |
| interest_rate | DECIMAL | Average interest rate |
| debt_category | VARCHAR | Category (public, intragovernmental) |
| percent_of_total | DECIMAL | Percentage of total debt |
| year_over_year_change | DECIMAL | YoY change percentage |

### Federal Reserve (FRED) Tables

#### `fred_indicators`
800,000+ economic time series from FRED.

Primary key: `(series_id, date)`

| Column | Type | Description |
|--------|------|-------------|
| series_id | VARCHAR | FRED series identifier |
| date | DATE | Observation date |
| series_title | VARCHAR | Series description |
| value | DECIMAL | Observation value |
| unit | VARCHAR | Unit of measure |
| frequency | VARCHAR | Data frequency (D, W, M, Q, A) |
| seasonally_adjusted | BOOLEAN | Whether seasonally adjusted |
| last_updated | DATE | Last update date |

Common FRED Series:
- **GDP**: Gross Domestic Product
- **UNRATE**: Unemployment Rate
- **CPIAUCSL**: Consumer Price Index
- **DFF**: Federal Funds Rate
- **DEXUSEU**: USD/EUR Exchange Rate
- **HOUST**: Housing Starts
- **UMCSENT**: Consumer Sentiment
- Plus 12 newly added indicators for banking, real estate, and consumer sentiment

### Bureau of Economic Analysis (BEA) Tables

#### `gdp_components`
GDP components and detailed economic accounts.

Primary key: `(table_id, line_number, year)`

| Column | Type | Description |
|--------|------|-------------|
| table_id | VARCHAR | BEA table identifier |
| line_number | INTEGER | Line in table |
| year | INTEGER | Observation year |
| component_name | VARCHAR | Component description |
| value | DECIMAL | Value in billions |
| unit | VARCHAR | Unit of measure |
| percent_change | DECIMAL | Percentage change |
| contribution_to_growth | DECIMAL | Contribution to GDP growth |
| data_source | VARCHAR | Source dataset |

#### `regional_income`
State and regional personal income statistics.

Primary key: `(geo_fips, metric, year)`

| Column | Type | Description |
|--------|------|-------------|
| geo_fips | VARCHAR | FIPS code (FK → geo.tiger_states.state_fips - partial) |
| metric | VARCHAR | Income metric type |
| year | INTEGER | Observation year |
| geo_name | VARCHAR | Geographic name |
| value | DECIMAL | Value |
| unit | VARCHAR | Unit of measure |
| geo_type | VARCHAR | Geographic type |
| per_capita_value | DECIMAL | Per capita value |
| percent_change_year | DECIMAL | YoY change |

#### `trade_statistics`
International trade data (imports/exports).

Primary key: `(year, line_number)`

| Column | Type | Description |
|--------|------|-------------|
| year | INTEGER | Trade year |
| line_number | INTEGER | Line identifier |
| line_description | VARCHAR | Category description |
| trade_type | VARCHAR | EXPORT or IMPORT |
| category | VARCHAR | Trade category |
| value | DECIMAL | Value in billions |

#### `industry_gdp`
GDP by industry (NAICS classification).

Primary key: `(industry_code, year, frequency)`

| Column | Type | Description |
|--------|------|-------------|
| industry_code | VARCHAR | NAICS code |
| industry_name | VARCHAR | Industry description |
| year | INTEGER | Observation year |
| frequency | VARCHAR | A (Annual) or Q (Quarterly) |
| value | DECIMAL | Value added in billions |
| table_id | VARCHAR | Source table |

### World Bank Tables

#### `world_indicators`
International economic indicators.

Primary key: `(country_code, indicator_code, year)`

| Column | Type | Description |
|--------|------|-------------|
| country_code | VARCHAR | ISO country code |
| indicator_code | VARCHAR | World Bank indicator |
| year | INTEGER | Observation year |
| country_name | VARCHAR | Country name |
| indicator_name | VARCHAR | Indicator description |
| value | DECIMAL | Indicator value |
| unit | VARCHAR | Unit of measure |
| region | VARCHAR | World region |
| income_group | VARCHAR | Income classification |

## Foreign Key Relationships

### Internal Relationships
- `employment_statistics.series_id` → `fred_indicators.series_id` (partial overlap)
- `inflation_metrics.area_code` → `regional_employment.area_code` (geographic overlap)
- `gdp_components.table_id` → `fred_indicators.series_id` (GDP series)

### Cross-Domain Relationships
- `regional_employment.state_code` → `geo.tiger_states.state_code`
- `regional_income.geo_fips` → `geo.tiger_states.state_fips` (state-level only)

### Complete Reference
For a comprehensive view of all relationships including the complete ERD diagram, cross-schema query examples, and detailed FK implementation status, see the **[Schema Relationships Guide](relationships.md)**.

## API Configuration

### Required API Keys
```bash
export BLS_API_KEY=your_bls_api_key      # Bureau of Labor Statistics
export FRED_API_KEY=your_fred_api_key    # Federal Reserve Economic Data
export BEA_API_KEY=your_bea_api_key      # Bureau of Economic Analysis
```

### Model Configuration
```json
{
  "name": "econ",
  "type": "custom",
  "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
  "operand": {
    "dataSource": "econ",
    "blsApiKey": "${BLS_API_KEY}",
    "fredApiKey": "${FRED_API_KEY}",
    "beaApiKey": "${BEA_API_KEY}",
    "enabledSources": ["bls", "fred", "treasury", "bea"],
    "updateFrequency": "daily",
    "cacheExpiry": "24h"
  }
}
```

## Common Query Patterns

### Economic Overview
```sql
-- Current economic indicators
SELECT
    'GDP' as indicator,
    value as current_value,
    percent_change_year as yoy_change
FROM gdp_components
WHERE component_name = 'Gross domestic product'
  AND year = YEAR(CURRENT_DATE) - 1
UNION ALL
SELECT
    'Unemployment',
    value,
    percent_change_year
FROM employment_statistics
WHERE series_id = 'UNRATE'
  AND date = (SELECT MAX(date) FROM employment_statistics WHERE series_id = 'UNRATE')
UNION ALL
SELECT
    'Inflation (CPI)',
    index_value,
    percent_change_year
FROM inflation_metrics
WHERE index_type = 'CPI-U'
  AND item_code = 'All Items'
  AND date = (SELECT MAX(date) FROM inflation_metrics);
```

### Regional Analysis
```sql
-- State unemployment rates with context
SELECT
    r.state_code,
    s.state_name,
    r.unemployment_rate,
    r.employment_level,
    r.labor_force,
    r.participation_rate
FROM regional_employment r
JOIN geo.tiger_states s ON r.state_code = s.state_code
WHERE r.area_type = 'state'
  AND r.date = (SELECT MAX(date) FROM regional_employment WHERE area_type = 'state')
ORDER BY r.unemployment_rate;
```

### Time Series Analysis
```sql
-- Recession indicators over time
SELECT
    f1.date,
    f1.value as unemployment_rate,
    f2.value as gdp_growth,
    f3.value as yield_spread_10y_2y
FROM fred_indicators f1
JOIN fred_indicators f2 ON f1.date = f2.date
JOIN fred_indicators f3 ON f1.date = f3.date
WHERE f1.series_id = 'UNRATE'
  AND f2.series_id = 'A191RL1Q225SBEA'  -- Real GDP Growth
  AND f3.series_id = 'T10Y2Y'           -- 10Y-2Y Treasury Spread
  AND f1.date >= CURRENT_DATE - INTERVAL '5' YEAR
ORDER BY f1.date;
```

## Data Update Schedule

| Source | Update Frequency | Typical Release Time |
|--------|-----------------|---------------------|
| BLS Employment | Monthly | First Friday, 8:30 AM ET |
| BLS CPI | Monthly | Mid-month, 8:30 AM ET |
| FRED | Varies by series | Real-time to annual |
| Treasury | Daily | 4:00 PM ET |
| BEA GDP | Quarterly | End of month following quarter |
| World Bank | Annual | April (previous year data) |

## Performance Tips

1. **Use Date Filters**: Always filter by date range to reduce data scanned
2. **Leverage Partitioning**: Data is partitioned by year for efficient queries
3. **Cache Frequent Queries**: Economic data changes slowly, cache results
4. **Join on Indexed Columns**: Use series_id and date columns for joins
5. **Aggregate Wisely**: Pre-aggregate in subqueries before joining
