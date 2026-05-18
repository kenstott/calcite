# GovData JDBC Driver

Query 15 US government datasets — SEC filings, census data, economic indicators, crime, weather, and more — using standard SQL from any JDBC client.

## Download

Build the shadow JAR from this repo:

```bash
./gradlew :govdata:shadowJar
# Output: govdata/build/libs/sih-govdata-*.jar
```

Or use the pre-built AskAmerica engine JAR (includes this driver):

```bash
./gradlew :askamerica-engine:shadowJar
```

## Connect

**JDBC URL format:**
```
jdbc:govdata:source=<schema>[,<schema2>,...]
```

**Driver class:** `org.apache.calcite.adapter.govdata.GovDataDriver`

### Quick examples

```
jdbc:govdata:source=geo
jdbc:govdata:source=sec,geo,econ
jdbc:govdata:source=fec,crime,weather
```

## Available schemas

| Schema | What's in it |
|--------|-------------|
| `sec` | SEC EDGAR filings, financial statements, insider trades |
| `geo` | US states, counties, ZIP codes, FIPS codes |
| `econ` | BLS/BEA economic indicators, CPI, unemployment, GDP |
| `census` | ACS 5-year estimates, decennial census |
| `crime` | FBI UCR crime statistics by state and agency |
| `weather` | NOAA GHCND daily observations (temperature, precipitation) |
| `fec` | FEC campaign finance contributions and expenditures |
| `fedregister` | Federal Register rules and notices |
| `cyber_vuln` | NIST NVD CVE vulnerability database |
| `cyber_threat` | CISA known exploited vulnerabilities |
| `energy` | EIA energy production and consumption |
| `health` | CDC, CMS, and clinical trial data |
| `edu` | NCES education statistics (IPEDS, CCD) |
| `ref` | Shared reference tables (NAICS, SIC, state codes) |

## DBeaver setup

1. **New Connection → JDBC**
2. **JDBC URL:** `jdbc:govdata:source=geo,sec`
3. **Driver JAR:** add `sih-govdata-*.jar`
4. **Driver class:** `org.apache.calcite.adapter.govdata.GovDataDriver`
5. No username or password required for most schemas

## Sample queries

```sql
-- Top revenue companies from SEC filings
SELECT company_name, value_dollars
FROM sec.financial_facts
WHERE canonical_name = 'Revenue'
ORDER BY value_dollars DESC
FETCH FIRST 10 ROWS ONLY;

-- State-level unemployment
SELECT area_name, year, period, value AS unemployment_rate
FROM econ.bls_series
WHERE series_id LIKE 'LAUS%' AND measure_code = '03'
ORDER BY year DESC, period DESC;

-- Recent federal cybersecurity vulnerabilities
SELECT cve_id, vendor_project, product, date_added
FROM cyber_threat.kev_catalog
ORDER BY date_added DESC
FETCH FIRST 20 ROWS ONLY;

-- Campaign contributions by state
SELECT contributor_state, SUM(transaction_amt) AS total
FROM fec.contributions
WHERE election_yr = 2024
GROUP BY contributor_state
ORDER BY total DESC;
```

## Environment variables (optional)

Some schemas require API keys for higher rate limits:

| Variable | Schema | Purpose |
|----------|--------|---------|
| `BLS_API_KEY` | `econ` | BLS higher rate limit |
| `BEA_API_KEY` | `econ` | BEA higher rate limit |
| `CENSUS_API_KEY` | `census` | Census API key |
| `EIA_API_KEY` | `energy` | EIA API key |
| `NVD_API_KEY` | `cyber_vuln` | NVD higher rate limit |

## Data directory

By default, downloaded data is cached to `$GOVDATA_PARQUET_DIR`. Set this to a persistent directory for faster subsequent queries:

```bash
export GOVDATA_PARQUET_DIR=/data/govdata
```
