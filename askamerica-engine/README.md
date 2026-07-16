# AskAmerica JDBC Driver

Query 15 US government datasets — SEC filings, census, economic indicators, crime, weather, and more — using standard SQL from any JDBC client.

## Download

Grab the latest JAR from [GitHub Releases](https://github.com/kenstott/calcite/releases):

```bash
# Python (recommended)
pip install 'askamerica[engine]'
askamerica login

# The engine JAR is downloaded automatically on first use.
# To pre-download explicitly (CI, Docker, offline environments):
askamerica install-engine

# Or download the JAR directly (for JDBC clients)
curl -L https://github.com/kenstott/calcite/releases/latest/download/askamerica-engine.jar -o askamerica-engine.jar
```

## Connect

**JDBC URL format:**
```
jdbc:askamerica:source=<schema>[,<schema2>,...]
```

**Driver class:** `org.apache.calcite.adapter.askamerica.AskAmericaDriver`

### Quick examples

```
jdbc:askamerica:source=geo
jdbc:askamerica:source=sec,geo,econ
jdbc:askamerica:source=fec,crime,weather
```

## Available schemas

| Schema | What's in it |
|--------|-------------|
| `sec` | SEC EDGAR filings, financial statements, insider trades |
| `geo` | US states, counties, ZIP codes, FIPS codes |
| `econ` | BLS/BEA economic indicators, CPI, unemployment, GDP |
| `econ_reference` | BLS area, industry, and occupation classification tables |
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
| `lands` | USDA/NPS/BLM public lands boundaries, forest inventory, mineral royalties |
| `patents` | USPTO patent grants, inventors, assignees, trademarks |
| `disasters` | FEMA disaster declarations, NFIP claims/policies, NOAA storm events, WFIGS wildfire perimeters |
| `housing` | FHFA house price indexes, Census new-residential building permits, HUD Fair Market Rents and income limits |
| `cftc` | CFTC swaps and derivatives data |
| `ag` | USDA agriculture (NASS/RMA/FSA) |
| `transport` | NHTSA recalls/complaints/FARS crashes, BTS airline on-time & T-100, FAA airports, FTA transit ridership |
| `environment` | EPA air quality, TRI, GHG, drinking water, Superfund/RCRA facilities + USGS water |
| `research` | NSF NCSES R&D: National Patterns, Federal Funds for R&D, Higher Education R&D (HERD) |
| `ref` | Shared reference tables (NAICS, SIC, state codes) |

## DBeaver setup

1. **New Connection → JDBC**
2. **JDBC URL:** `jdbc:askamerica:source=geo,sec`
3. **Driver JAR:** add `askamerica-engine.jar`
4. **Driver class:** `org.apache.calcite.adapter.askamerica.AskAmericaDriver`
5. No username or password required for most schemas

## Python

Install the package, download the JAR, and query:

```bash
pip install 'askamerica[engine]'
askamerica login            # get your free API key
# Engine JAR downloads automatically on first use (~80 MB, cached to ~/.askamerica/engine/)
# To pre-download (CI/Docker): askamerica install-engine
```

**One-liner — returns a pandas DataFrame:**

```python
import askamerica as aa

df = aa.query("SELECT company_name, filing_type, filing_date FROM sec.filing_metadata FETCH FIRST 5 ROWS ONLY")
print(df)
```

**Raw JDBC connection — for metadata, prepared statements, or custom URLs:**

```python
import askamerica as aa

conn = aa.connect()   # JVM and JAR managed internally — no JPype import needed
stmt = conn.createStatement()
rs   = stmt.executeQuery(
    "SELECT company_name, filing_type FROM sec.filing_metadata "
    "ORDER BY filing_date DESC FETCH FIRST 5 ROWS ONLY"
)
while rs.next():
    print(rs.getString("company_name"), rs.getString("filing_type"))
conn.close()
```

## Sample queries

```sql
-- Top 10 revenue companies from SEC filings
SELECT company_name, value_dollars
FROM sec.financial_facts
WHERE canonical_name = 'Revenue'
ORDER BY value_dollars DESC
FETCH FIRST 10 ROWS ONLY;

-- State population from census
SELECT state_name, estimate AS population
FROM census.acs_b
JOIN geo.states USING (state_fips)
WHERE variable = 'B01001_001E' AND year = 2022;

-- Recent federal cybersecurity vulnerabilities
SELECT cve_id, vendor_project, product, date_added
FROM cyber_threat.kev_catalog
ORDER BY date_added DESC
FETCH FIRST 20 ROWS ONLY;
```

## Performance

Remote parquet reads from R2/S3 are cached to a local disk cache that survives process restarts. On the first query after a cold start, data is fetched over the network; subsequent queries against the same data are served from the local cache.

**Cache location:** `~/.askamerica/.duckdb_httpfs_cache/` (override with the `duckdb.cache_httpfs.directory` JVM system property)

**Platform support:** macOS and Linux. Windows users can get the same caching by running the Linux version under WSL — install WSL, then use the `.deb` installer or the JAR directly via `java -jar askamerica-engine.jar --mcp`.

## Environment variables (optional)

Some schemas require API keys for higher rate limits:

| Variable | Schema | Purpose |
|----------|--------|---------|
| `BLS_API_KEY` | `econ` | BLS higher rate limit |
| `BEA_API_KEY` | `econ` | BEA higher rate limit |
| `CENSUS_API_KEY` | `census` | Census API key |
| `EIA_API_KEY` | `energy` | EIA API key |
| `NVD_API_KEY` | `cyber_vuln` | NVD higher rate limit |
| `HUD_TOKEN` | `housing` | HUD USER API (Fair Market Rents, income limits) |
