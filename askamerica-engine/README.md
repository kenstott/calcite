# AskAmerica JDBC Driver

Query 15 US government datasets — SEC filings, census, economic indicators, crime, weather, and more — using standard SQL from any JDBC client.

## Download

Grab the latest JAR from [GitHub Releases](https://github.com/kenstott/calcite/releases):

```bash
# Python (recommended)
pip install 'askamerica[engine]'
askamerica install-engine

# Or download directly
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
2. **JDBC URL:** `jdbc:askamerica:source=geo,sec`
3. **Driver JAR:** add `askamerica-engine.jar`
4. **Driver class:** `org.apache.calcite.adapter.askamerica.AskAmericaDriver`
5. No username or password required for most schemas

## Python (JPype)

Install the Python package, download the JAR, and query in three steps:

```bash
pip install 'askamerica[engine]'
askamerica install-engine   # downloads JAR to ~/.askamerica/engine/
askamerica login            # get your free API key
```

```python
import askamerica as aa

df = aa.query("SELECT company_name, filing_type, filing_date FROM sec.filing_metadata FETCH FIRST 5 ROWS ONLY")
print(df)
```

Or use JPype directly, letting the `askamerica` package resolve the JAR path:

```python
import jpype
from askamerica.engine import get_engine_jar  # resolves ~/.askamerica/engine/ or $ASKAMERICA_ENGINE_JAR

jpype.startJVM(classpath=[get_engine_jar()])

Driver = jpype.JClass("org.apache.calcite.adapter.askamerica.AskAmericaDriver")
conn   = Driver().connect("jdbc:askamerica:source=sec", jpype.JClass("java.util.Properties")())
stmt   = conn.createStatement()
rs     = stmt.executeQuery(
    "SELECT company_name, filing_type FROM sec.filing_metadata "
    "ORDER BY filing_date DESC FETCH FIRST 5 ROWS ONLY"
)
while rs.next():
    print(rs.getString("company_name"), rs.getString("filing_type"))
conn.close()
```

`get_engine_jar()` checks `$ASKAMERICA_ENGINE_JAR` first, then falls back to `~/.askamerica/engine/askamerica-engine.jar` (the default `install-engine` location), and raises a clear error if neither exists.

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

## Environment variables (optional)

Some schemas require API keys for higher rate limits:

| Variable | Schema | Purpose |
|----------|--------|---------|
| `BLS_API_KEY` | `econ` | BLS higher rate limit |
| `BEA_API_KEY` | `econ` | BEA higher rate limit |
| `CENSUS_API_KEY` | `census` | Census API key |
| `EIA_API_KEY` | `energy` | EIA API key |
| `NVD_API_KEY` | `cyber_vuln` | NVD higher rate limit |
