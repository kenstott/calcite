# ECON Schema Documentation

## Overview

The ECON schema provides unified access to economic indicators from multiple U.S. government sources including the Bureau of Labor Statistics (BLS), Federal Reserve Economic Data (FRED), Treasury Direct, Bureau of Economic Analysis (BEA), and World Bank.

The schema includes comprehensive regional economic data covering 1,845 BLS data series across state, metro, and Census region geographic levels:
- Regional CPI (4 regions), Metro CPI (27 metros)
- State Industry Employment (1,122 series), State Wages (51 series)
- Metro Industry Employment (594 series), Metro Wages (27 series)
- JOLTS Regional (20 series)

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
State-level employment statistics from BLS LAUS (Local Area Unemployment Statistics) for all 51 U.S. jurisdictions (50 states + DC).

Primary key: `(date, area_code)`

Foreign keys:
- `state_code` → `geo.states.state_abbr` (links to geographic/demographic data)

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Observation date (monthly) |
| area_code | VARCHAR | Geographic area code (state FIPS code) |
| area_name | VARCHAR | State name |
| area_type | VARCHAR | Type (state) |
| state_code | VARCHAR | 2-letter state code (FK) |
| unemployment_rate | DECIMAL | Unemployment rate as percentage |
| employment_level | BIGINT | Number of employed persons |
| labor_force | BIGINT | Total labor force size |
| participation_rate | DECIMAL | Labor force participation rate |
| employment_population_ratio | DECIMAL | Employment to population ratio |

**Coverage**: 204 series (51 jurisdictions × 4 metrics: unemployment rate, unemployment level, employment level, labor force)

**Note**: County-level LAUS data (~3,142 counties) not yet implemented due to API rate limit constraints. See source code for implementation notes.

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

#### `jolts_regional`
Job Openings and Labor Turnover Survey (JOLTS) data for 4 U.S. Census regions.

Primary key: `(date, region_code, metric)`

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Observation date |
| region_code | VARCHAR | Census region code (0100-0400) |
| region_name | VARCHAR | Region name |
| metric | VARCHAR | JOLTS metric type |
| series_id | VARCHAR | BLS series identifier |
| value | DECIMAL | Metric value |
| rate | DECIMAL | Rate (if applicable) |
| seasonally_adjusted | BOOLEAN | Whether seasonally adjusted |

**JOLTS Metrics**: Job openings rate, hires rate, total separations rate, quits rate, layoffs/discharges rate

### Regional Employment and Wages Tables

#### `state_industry`
Employment by industry sector for all 51 U.S. jurisdictions (50 states + DC) across 22 NAICS supersector codes.

Primary key: `(date, state_fips, supersector_code)`

Foreign keys:
- `state_fips` → `geo.states.state_fips` (links to geographic/demographic data)

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Observation date |
| state_fips | VARCHAR | 2-digit state FIPS code (FK) |
| state_name | VARCHAR | State name |
| supersector_code | VARCHAR | 8-digit NAICS supersector code |
| supersector_name | VARCHAR | Industry sector name |
| series_id | VARCHAR | BLS series identifier |
| employment | INTEGER | Number of employees (thousands) |
| seasonally_adjusted | BOOLEAN | Whether seasonally adjusted |

**Coverage**: 1,122 series (51 jurisdictions × 22 sectors) including total nonfarm, manufacturing, construction, retail, healthcare, government, etc.

#### `state_wages`
Average weekly wages for all 51 U.S. jurisdictions from BLS QCEW (Quarterly Census of Employment and Wages).

Primary key: `(date, state_fips)`

Foreign keys:
- `state_fips` → `geo.states.state_fips` (links to geographic/demographic data)

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Observation date |
| state_fips | VARCHAR | 2-digit state FIPS code (FK) |
| state_name | VARCHAR | State name |
| series_id | VARCHAR | BLS series identifier |
| average_weekly_wage | DECIMAL | Average weekly wage ($) |
| total_employment | INTEGER | Total employment count |
| percent_change_year | DECIMAL | Year-over-year wage change |

#### `metro_industry`
Employment by industry sector for 27 major U.S. metropolitan areas across 22 NAICS supersector codes.

Primary key: `(date, metro_area_code, supersector_code)`

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Observation date |
| metro_area_code | VARCHAR | BLS metro area code (e.g., A100) |
| metro_area_name | VARCHAR | Metro area name |
| supersector_code | VARCHAR | 8-digit NAICS supersector code |
| supersector_name | VARCHAR | Industry sector name |
| series_id | VARCHAR | BLS series identifier |
| employment | INTEGER | Number of employees (thousands) |
| seasonally_adjusted | BOOLEAN | Whether seasonally adjusted |

**Coverage**: 594 series (27 metros × 22 sectors)

#### `metro_wages`
Average weekly wages and annual pay for 27 major U.S. metropolitan areas from BLS QCEW bulk CSV files.

Primary key: `(year, qtr, metro_area_code)`

| Column | Type | Description |
|--------|------|-------------|
| metro_area_code | VARCHAR | BLS metro publication code (e.g., "A419" for Atlanta) |
| metro_area_name | VARCHAR | Metro area name |
| year | INTEGER | Calendar year |
| qtr | VARCHAR | Quarter: "A" = annual average, "1"-"4" = quarterly data |
| average_weekly_wage | DECIMAL | Average weekly wage ($) |
| average_annual_pay | DECIMAL | Average annual pay ($) |

**Data Availability**: 1990-present, both annual and quarterly granularities

**Data Source**: BLS QCEW bulk CSV files (~80MB annual, ~323MB quarterly per year)

**Example Queries**:

Get annual average wages for all metros:
```sql
SELECT metro_area_name, year, average_weekly_wage, average_annual_pay
FROM metro_wages
WHERE qtr = 'A'
  AND year >= 2020
ORDER BY year DESC, metro_area_name;
```

Compare quarterly wage trends for a specific metro:
```sql
SELECT year, qtr, average_weekly_wage,
    LAG(average_weekly_wage) OVER (ORDER BY year, qtr) as prev_qtr_wage,
    ROUND((average_weekly_wage - LAG(average_weekly_wage) OVER (ORDER BY year, qtr)) /
          LAG(average_weekly_wage) OVER (ORDER BY year, qtr) * 100, 2) as pct_change
FROM metro_wages
WHERE metro_area_code = 'A419'  -- Atlanta
  AND qtr IN ('1', '2', '3', '4')
  AND year >= 2022
ORDER BY year, qtr;
```

Find metros with highest annual wage growth:
```sql
WITH annual_data AS (
    SELECT metro_area_name, year, average_annual_pay
    FROM metro_wages
    WHERE qtr = 'A' AND year IN (2022, 2023)
)
SELECT
    cur.metro_area_name,
    prev.average_annual_pay as pay_2022,
    cur.average_annual_pay as pay_2023,
    ROUND((cur.average_annual_pay - prev.average_annual_pay) / prev.average_annual_pay * 100, 2) as growth_pct
FROM annual_data cur
JOIN annual_data prev ON cur.metro_area_name = prev.metro_area_name
WHERE cur.year = 2023 AND prev.year = 2022
ORDER BY growth_pct DESC
LIMIT 10;
```

#### `county_qcew`
County-level employment and wages from BLS QCEW (Quarterly Census of Employment and Wages) for all ~3,142 U.S. counties.

Primary key: `(area_fips, own_code, industry_code, agglvl_code)`

Foreign keys:
- `area_fips` → `geo.counties.county_fips` (links to county geographic data)

| Column | Type | Description |
|--------|------|-------------|
| area_fips | VARCHAR | 5-digit county FIPS code (FK) |
| own_code | VARCHAR | Ownership code: 0=Total, 1=Private, 2=Federal govt, 3=State govt, 4=Local govt, 5=International/other |
| industry_code | VARCHAR | 6-character NAICS industry code |
| agglvl_code | VARCHAR | Aggregation level: 70=Total, 71=By ownership, 72=Goods/services, 73=SuperSector, 74=Sector, 75=3-digit NAICS, 76=4-digit NAICS, 77=5-digit NAICS, 78=6-digit NAICS |
| annual_avg_estabs | INTEGER | Annual average establishment count |
| annual_avg_emplvl | INTEGER | Annual average employment level |
| total_annual_wages | BIGINT | Total annual wages ($) |
| annual_avg_wkly_wage | INTEGER | Average weekly wage ($) |

**Coverage**: Comprehensive county-level data with industry and ownership breakdowns

**Data Source**: BLS QCEW flat files (CSV format, ~500MB per year) downloaded and converted to Parquet

**Note**: Provides detailed industry breakdown by NAICS codes and ownership types (private, federal, state, local government) for all U.S. counties. Essential for county-level economic analysis, industry concentration studies, and regional development research.

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

#### `regional_cpi`
Consumer Price Index for 4 U.S. Census regions (Northeast, Midwest, South, West).

Primary key: `(date, region_code)`

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Observation date |
| region_code | VARCHAR | Census region code (0100-0400) |
| region_name | VARCHAR | Region name |
| series_id | VARCHAR | BLS series identifier |
| cpi_value | DECIMAL | CPI index value (1982-84=100) |
| percent_change_month | DECIMAL | Month-over-month change |
| percent_change_year | DECIMAL | Year-over-year change |

#### `metro_cpi`
Consumer Price Index for 27 major U.S. metropolitan areas.

Primary key: `(date, metro_area_code)`

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Observation date |
| metro_area_code | VARCHAR | BLS metro area code (e.g., A100) |
| metro_area_name | VARCHAR | Metro area name |
| series_id | VARCHAR | BLS series identifier |
| cpi_value | DECIMAL | CPI index value (1982-84=100) |
| percent_change_month | DECIMAL | Month-over-month change |
| percent_change_year | DECIMAL | Year-over-year change |

**Metro Areas Covered**: NYC, LA, Chicago, Houston, Phoenix, Philadelphia, San Antonio, San Diego, Dallas, San Jose, Austin, Jacksonville, Boston, Seattle, Denver, Washington DC, Detroit, Cleveland, Minneapolis, Miami, Atlanta, Portland, Riverside, St. Louis, Baltimore, Tampa, Anchorage

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
- Plus 12 additional indicators for banking, real estate, and consumer sentiment

#### `reference_fred_series`
Complete FRED economic data series catalog with 841,000+ series for data discovery and programmatic access.

Primary key: `(series)`

Partitions: `(type, category, frequency, source, status)` - Status partition separates 'active' (currently updated) from 'discontinued' (no longer updated) series

Indexes: `title`, `units`, `frequency`, `seasonal_adjustment`

| Column | Type | Description |
|--------|------|-------------|
| series | VARCHAR | FRED series identifier (e.g., 'GDP', 'UNRATE') |
| title | VARCHAR | Human-readable series title |
| observation_start | VARCHAR | First available observation date |
| observation_end | VARCHAR | Most recent observation date |
| frequency | VARCHAR | Data frequency (Daily, Weekly, Monthly, Quarterly, Annual) |
| frequency_short | VARCHAR | Frequency code (D, W, M, Q, A) |
| units | VARCHAR | Unit of measurement |
| units_short | VARCHAR | Units code |
| seasonal_adjustment | VARCHAR | Seasonal adjustment status |
| seasonal_adjustment_short | VARCHAR | Adjustment code |
| popularity | INTEGER | Series popularity ranking |
| group_popularity | INTEGER | Category popularity |
| notes | VARCHAR | Series description and methodology |

**Usage**: Query this table to discover available economic data series before downloading observations. JOIN with `fred_indicators` to get actual observation values.

**Example - Find all GDP-related series**:
```sql
SELECT series, title, frequency, observation_start, observation_end
FROM reference_fred_series
WHERE title ILIKE '%GDP%'
  AND frequency = 'Quarterly'
ORDER BY popularity DESC
LIMIT 20;
```

**Example - Discover housing market indicators**:
```sql
SELECT series, title, units, observation_end
FROM reference_fred_series
WHERE title ILIKE '%housing%'
  AND frequency IN ('Monthly', 'Quarterly')
ORDER BY popularity DESC;
```

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

#### `state_gdp`
State-level GDP statistics from BEA Regional Economic Accounts (SAGDP2N table).

Primary key: `(geo_fips, line_code, year)`

Foreign keys:
- `geo_fips` → `geo.states.state_fips`

| Column | Type | Description |
|--------|------|-------------|
| geo_fips | VARCHAR | State FIPS code (FK) |
| geo_name | VARCHAR | State name |
| line_code | INTEGER | BEA line code |
| line_description | VARCHAR | GDP metric description |
| year | INTEGER | Calendar year |
| value | DECIMAL | GDP value (millions $) |
| unit | VARCHAR | Unit of measurement |

**Coverage**: Provides both total GDP and per capita real GDP by state across all NAICS industry sectors.

---

#### `industry_gdp`
GDP by Industry data from BEA showing value added by NAICS industry sectors.

Primary key: `(industry_code, year, quarter)`

| Column | Type | Description |
|--------|------|-------------|
| industry_code | VARCHAR | NAICS industry code |
| industry_title | VARCHAR | Industry description |
| year | INTEGER | Calendar year |
| quarter | INTEGER | Quarter (1-4 for quarterly, null for annual) |
| value | DECIMAL | Value added (billions $) |
| unit | VARCHAR | Unit of measurement |
| frequency | VARCHAR | Annual or Quarterly |

**Coverage**: Comprehensive breakdown of economic output by industry including agriculture, mining, manufacturing, services, and government sectors. Available at both annual and quarterly frequencies for detailed sectoral analysis.

---

#### `ita_data`
International Transactions Accounts (ITA) from BEA providing balance of payments statistics.

Primary key: `(year, quarter, account_type)`

| Column | Type | Description |
|--------|------|-------------|
| year | INTEGER | Calendar year |
| quarter | VARCHAR | Quarter (Q1-Q4) |
| account_type | VARCHAR | Type of account |
| account_description | VARCHAR | Account description |
| value | DECIMAL | Value (millions $) |
| unit | VARCHAR | Unit of measurement |

**Coverage**: Includes trade balance, current account, capital account, and income balances.

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
- `regional_employment.state_code` → `geo.states.state_abbr`
- `regional_income.geo_fips` → `geo.states.state_fips` (state-level only)
- `state_industry.state_fips` → `geo.states.state_fips` (enables state-level industry analysis with geographic/demographic data)
- `state_wages.state_fips` → `geo.states.state_fips` (enables regional wage comparisons with cost-of-living data)
- `county_qcew.area_fips` → `geo.counties.county_fips` (links to county geographic data)
- `county_wages.county_fips` → `geo.counties.county_fips` (links to county geographic data)

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

### BLS Table Filtering

Control which BLS tables are downloaded and visible in the schema using `blsConfig`:

#### Include Tables (Whitelist)
```json
{
  "operand": {
    "blsConfig": {
      "includeTables": ["regional_cpi", "metro_cpi", "state_wages"]
    }
  }
}
```

#### Exclude Tables (Blacklist)
```json
{
  "operand": {
    "blsConfig": {
      "excludeTables": ["state_industry", "metro_industry"]
    }
  }
}
```

**Important Notes**:
- Cannot specify both `includeTables` and `excludeTables` (mutually exclusive)
- Omitting `blsConfig` downloads all tables (backward compatible)
- Excluded tables will not appear in the SQL schema
- Useful for managing BLS API quotas (500 daily requests without key, 500/day with key)

**Available BLS Tables**:
- `employment_statistics` (~3 series)
- `inflation_metrics` (~3 series)
- `regional_cpi` (4 series)
- `metro_cpi` (27 series)
- `state_industry` (1,122 series) ⚠️ API intensive
- `state_wages` (51 series)
- `metro_industry` (594 series) ⚠️ API intensive
- `metro_wages` (27 series)
- `jolts_regional` (20 series)
- `wage_growth` (~2 series)
- `regional_employment` (~3 series)

**Example: Minimize API Calls**
```json
{
  "blsConfig": {
    "excludeTables": ["state_industry", "metro_industry"]
  }
}
```
This excludes 1,716 series (saves ~35 API calls per year) while retaining all other regional data.

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

#### Cost of Living by Region
```sql
-- Compare regional CPI trends
SELECT
    region_name,
    date,
    cpi_value,
    percent_change_year,
    LAG(cpi_value, 12) OVER (PARTITION BY region_code ORDER BY date) as cpi_year_ago
FROM regional_cpi
WHERE date >= CURRENT_DATE - INTERVAL '2' YEAR
ORDER BY region_name, date;
```

#### Metro Area Inflation Comparison
```sql
-- Top 10 metros with highest inflation
SELECT
    metro_area_name,
    cpi_value,
    percent_change_year,
    percent_change_month
FROM metro_cpi
WHERE date = (SELECT MAX(date) FROM metro_cpi)
ORDER BY percent_change_year DESC
LIMIT 10;
```

#### State Industry Employment Analysis
```sql
-- Manufacturing employment by state with geographic data
SELECT
    si.state_name,
    si.employment,
    si.employment * 1000 / s.population as employment_per_capita,
    s.region,
    s.division
FROM state_industry si
JOIN geo.states s ON si.state_fips = s.state_fips
WHERE si.supersector_name = 'Manufacturing'
  AND si.date = (SELECT MAX(date) FROM state_industry)
ORDER BY si.employment DESC;
```

#### State Wage Comparison
```sql
-- States with highest wage growth
SELECT
    sw.state_name,
    sw.average_weekly_wage,
    sw.percent_change_year,
    sw.total_employment,
    s.population
FROM state_wages sw
JOIN geo.states s ON sw.state_fips = s.state_fips
WHERE sw.date = (SELECT MAX(date) FROM state_wages)
ORDER BY sw.percent_change_year DESC
LIMIT 10;
```

#### Metro Industry Mix Analysis
```sql
-- Tech sector employment in major metros
SELECT
    metro_area_name,
    supersector_name,
    employment,
    RANK() OVER (PARTITION BY metro_area_name ORDER BY employment DESC) as sector_rank
FROM metro_industry
WHERE date = (SELECT MAX(date) FROM metro_industry)
  AND supersector_name IN ('Information', 'Professional and Business Services')
ORDER BY metro_area_name, employment DESC;
```

#### Regional Labor Market Dynamics
```sql
-- JOLTS regional comparison
SELECT
    region_name,
    metric,
    value,
    rate
FROM jolts_regional
WHERE date = (SELECT MAX(date) FROM jolts_regional)
  AND metric IN ('Job Openings Rate', 'Quits Rate', 'Layoffs Rate')
ORDER BY region_name, metric;
```

#### Cross-Schema Regional Economic Dashboard
```sql
-- Comprehensive state economic profile
SELECT
    s.state_name,
    s.region,
    si.employment as total_employment,
    sw.average_weekly_wage,
    ri.per_capita_value as per_capita_income,
    s.population,
    s.land_area_sq_miles
FROM geo.states s
LEFT JOIN state_industry si
    ON s.state_fips = si.state_fips
    AND si.supersector_name = 'Total Nonfarm'
    AND si.date = (SELECT MAX(date) FROM state_industry)
LEFT JOIN state_wages sw
    ON s.state_fips = sw.state_fips
    AND sw.date = (SELECT MAX(date) FROM state_wages)
LEFT JOIN regional_income ri
    ON s.state_fips = ri.geo_fips
    AND ri.metric = 'Per capita personal income'
    AND ri.year = (SELECT MAX(year) FROM regional_income)
ORDER BY s.state_name;
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
