# GovData Adapter

[![Build Status](https://github.com/apache/calcite/actions/workflows/gradle.yml/badge.svg)](https://github.com/apache/calcite/actions/workflows/gradle.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

The GovData Adapter provides unified SQL access to various U.S. government data sources through Apache Calcite, enabling cross-domain analysis of financial, economic, and geographic data.

## 🎯 Overview

GovData transforms disparate government APIs and data formats into queryable SQL tables, allowing you to:

- **Analyze SEC filings** alongside economic indicators
- **Join financial data** with geographic boundaries
- **Correlate company performance** with regional employment statistics
- **Build composite views** across multiple government data sources

## ✨ How It Works: The Magic of Declarative Data Pipelines

GovData is a **declarative data pipeline orchestrator**. You describe what data you want in familiar business terms, and GovData automatically:

1. **Downloads** the data from government APIs
2. **Transforms** it to optimized Parquet format
3. **Enriches** it with metadata, comments, and foreign key relationships
4. **Delegates** query execution to high-performance engines (DuckDB, Arrow, etc.)

### Zero-Effort Data Lake Creation

As one user described it: "Explain the data of interest in its native language - get a fully-formed data lake with all metadata including comments and constraints - for zero effort."

```json
// You declare what data you need:
{
  "dataSource": "sec",           // Which schema (sec/econ/geo)
  "ciks": ["AAPL", "MSFT"],     // Company tickers or CIK numbers
  "startYear": 2020,             // Date range start
  "endYear": 2024,               // Date range end
  "filingTypes": ["10-K", "10-Q"] // SEC filing types
}

// GovData automatically creates SQL tables with:
// ✓ Downloaded and transformed data
// ✓ Table comments explaining each column
// ✓ Foreign key constraints for joins
// ✓ Optimized storage and partitioning
```

### Collaboration Made Simple

- **Personal Projects**: Use local storage for independent work
- **Team Collaboration**: Point to a shared S3 bucket - everyone gets the same data
- **Hybrid Approach**: Mix local and cloud storage as needed

The adapter handles all the complexity - you just write SQL queries against well-documented, properly-linked tables.

## 🚀 Quick Start

```java
// Connect to multiple government data sources
String modelPath = "path/to/govdata-model.json";
Connection conn = DriverManager.getConnection("jdbc:calcite:model=" + modelPath);

// Query across domains - e.g., companies by state with economic context
ResultSet rs = conn.createStatement().executeQuery(
    "SELECT s.state_name, COUNT(DISTINCT f.cik) as companies, " +
    "       AVG(fo.value) as avg_metric " +
    "FROM sec.filing_metadata f " +
    "JOIN geo.states s ON f.state_of_incorporation = s.state_abbr " +
    "JOIN econ.fred_indicators fo ON fo.series = 'GDP' " +
    "GROUP BY s.state_name"
);
```

## 🔍 Pre-Built Analytical Views

The ECON schema includes analytical views for common economic analysis patterns:

- **interest_rate_spreads** - Treasury yield curve analytics with term spreads for recession forecasting
- **housing_indicators** - Real estate market dashboard (starts, permits, prices, mortgage rates)
- **monetary_aggregates** - M1/M2 money supply and velocity analysis
- **business_indicators** - Business cycle dashboard (industrial production, capacity utilization, credit)
- **trade_balance_summary** - International trade flows aggregated by category

Views combine data from multiple tables and provide ready-to-query metrics for macroeconomic analysis.

## 📊 Supported Data Sources

### Currently Available

| Schema | Domain | Key Tables | Data Sources |
|--------|--------|------------|--------------|
| **SEC** | Financial Data | `facts`, `insider`, `text_blocks`, `stock_prices` | SEC EDGAR, Yahoo Finance |
| **ECON** | Economic Data | `reference_fred_series` (841K+ series), `fred_indicators`, employment/wages tables | BLS, FRED, Treasury, BEA, World Bank |
| **GEO** | Geographic Boundaries | `states`, `counties`, `places`, `zctas`, `census_tracts`, `cbsa`, `congressional_districts` | Census TIGER/Line, HUD |
| **CENSUS** | Demographics & Population | `acs_population`, `acs_demographics`, `acs_income`, `acs_education`, `acs_housing` | Census Bureau ACS, Decennial Census |
| **EDU** | Education Institutions | `ccd_districts`, `ccd_schools`, `naep_scores`, `crdc_schools`, `ipeds_institutions`, `ipeds_completions`, `ipeds_financials`, `ipeds_tuition`, `college_scorecard`, `college_scorecard_programs` | NCES (CCD, IPEDS, NAEP), Dept of Ed (CRDC), College Scorecard API |
| **CYBER** | Cybersecurity Intelligence | `vulnerabilities`, `cwe_catalog`, `kev_catalog`, `attack_techniques`, `nist_controls`, `cis_controls`, `owasp_top10`, `ioc_urls`, `ioc_hashes`, `threat_pulses` | NVD, MITRE ATT&CK, CISA KEV, OTX, ThreatFox |
| **HEALTH** | Public Health | `clinical_trials`, `fda_drug_approvals`, `fda_adverse_events`, `cdc_mortality`, `cdc_brfss`, `cms_hospital_quality`, `medicaid_drug_utilization`, `rxnorm_drugs` | FDA, ClinicalTrials.gov, CDC, CMS, NIH RxNorm |
| **FEC** | Campaign Finance | `individual_contributions`, `pac_contributions`, `candidate_summaries`, `committee_summaries` | FEC bulk data |
| **REF** | Reference Identifiers | `lei_entities`, `lei_relationships` | GLEIF golden copy |
| **CRIME** | Crime Statistics | `cde_agencies`, `cde_offenses`, `cde_hate_crimes`, `cde_police_employment`, `cde_use_of_force` | FBI Crime Data Explorer |

### Future Planned

Plans with links have full data plans in `docs/data-plans/`. Workers 74–95 are reserved.

Ordered by cross-schema leverage against existing data (highest first):

| Priority | Schema | Domain | Key Tables | Workers | Plan |
|---|---|---|---|---|---|
| 1 | **ENERGY** | Energy Markets | `eia_electricity_generation`, `eia_fossil_fuel_production`, `eia_power_plants` | 82–83 | [energy.md](docs/data-plans/energy.md) |
| 2 | **DISASTERS** | Emergency Events | `disaster_declarations`, `wildfire_perimeters`, `storm_events`, `nfip_claims` | 74–76 | [disasters.md](docs/data-plans/disasters.md) |
| 3 | **HOUSING** | Housing & Mortgages | `hmda_loans`, `hud_fair_market_rents`, `census_building_permits` | 92–93 | [housing.md](docs/data-plans/housing.md) |
| 4 | **ENV** | Environment & EPA | `epa_tri_releases`, `epa_ghg_emissions`, `epa_air_quality`, `epa_superfund_sites` | 86–87 | [environment.md](docs/data-plans/environment.md) |
| 5 | **AGR** | Agriculture | `nass_crop_production`, `rma_crop_insurance`, `fsa_commodity_payments` | 84–85 | [agriculture.md](docs/data-plans/agriculture.md) |
| 6 | **TRADE** | International Trade | `usa_trade_monthly`, `usa_trade_by_state`, `bea_fdi_by_industry` | 94–95 | [trade.md](docs/data-plans/trade.md) |
| 7 | **PATENTS** | IP & Trademarks | `patent_grants`, `patent_assignees`, `patent_inventors`, `patent_cpc_classes` | 80–81 | [patents.md](docs/data-plans/patents.md) |
| 8 | **TRANSPORT** | Transportation | `bts_airline_performance`, `fmcsa_carriers`, `ntsb_aviation_accidents` | 88–89 | [transport.md](docs/data-plans/transport.md) |
| 9 | **LANDS** | Federal Public Lands | `national_forests`, `nps_units`, `nps_visitation`, `onrr_revenues` | 77–79 | [lands.md](docs/data-plans/lands.md) |
| 10 | **CRIME+** | Crime (extensions) | `nibrs_offenses` (county-level), `bjs_state_corrections` | 90–91 | [crime.md](docs/data-plans/crime.md) |
| — | **LEG** | Legislation | Congressional bills, votes, committees | TBD | — |
| — | **POL** | Political Offices | Presidents, Governors, SCOTUS, executive orders | TBD | — |
| — | **SAFETY** | Transportation Safety | NHTSA crash data | TBD | — |

## 🔧 Installation

### Maven
```xml
<dependency>
    <groupId>org.apache.calcite</groupId>
    <artifactId>calcite-govdata</artifactId>
    <version>${calcite.version}</version>
</dependency>
```

### Gradle
```gradle
implementation 'org.apache.calcite:calcite-govdata:${calcite.version}'
```

## 🔑 Configuration

### Basic Model Configuration

Create a `govdata-model.json` file:

```json
{
  "version": "1.0",
  "defaultSchema": "sec",
  "schemas": [
    {
      "name": "sec",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
      "operand": {
        "dataSource": "sec",
        "ciks": ["AAPL", "MSFT"],
        "startYear": 2020,
        "endYear": 2024
      }
    },
    {
      "name": "econ",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
      "operand": {
        "dataSource": "econ",
        "blsApiKey": "${BLS_API_KEY}"
      }
    },
    {
      "name": "geo",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
      "operand": {
        "dataSource": "geo",
        "autoDownload": true
      }
    }
  ]
}
```

### Environment Variables

```bash
# Required for economic data sources
export BLS_API_KEY=your_bls_api_key
export FRED_API_KEY=your_fred_api_key
export BEA_API_KEY=your_bea_api_key
export ALPHA_VANTAGE_KEY=your_alpha_vantage_key

# Data directories (defaults to system temp)
export GOVDATA_CACHE_DIR=/path/to/cache
export GOVDATA_PARQUET_DIR=/path/to/parquet

# Date ranges (defaults to last 5 years)
export GOVDATA_START_YEAR=2020
export GOVDATA_END_YEAR=2024

# Additional configuration
export SEC_API_KEY=your_sec_api_key  # Optional for rate limit increases
export CENSUS_API_KEY=your_census_api_key  # Optional for enhanced access
```

## 💡 Key Features

### Cross-Domain Relationships
- **Automatic Foreign Key Detection** - Cross-schema constraints when multiple schemas are configured
- **Geographic Integration** - Companies linked to states via `state_of_incorporation` → `state_abbr`
- **Temporal Analysis** - Join financial filings with contemporary economic indicators by date ranges

### Performance Optimization
- **Intelligent Caching** - Downloaded data cached as Parquet files
- **Partition Pruning** - Year/CIK/Filing-type partitioning for fast queries
- **Parallel Processing** - Concurrent API downloads and conversions
- **Storage Flexibility** - Support for local, S3, and HDFS storage
- **Engine Delegation** - Configurable execution engines (DuckDB, Parquet, LINQ4J, Arrow)

### Data Quality
- **Automatic Updates** - Configurable refresh intervals
- **Data Validation** - Schema enforcement and type checking
- **Error Recovery** - Retry logic with exponential backoff
- **Metadata Tracking** - Data lineage and download timestamps

## 📖 Documentation

### Schema Documentation
- [SEC Schema Guide](docs/schemas/sec.md) - Financial filings and XBRL data
- [ECON Schema Guide](docs/schemas/econ.md) - Economic indicators and statistics
- [GEO Schema Guide](docs/schemas/geo.md) - Geographic boundaries and mappings
- [CENSUS Schema Guide](docs/schemas/census.md) - Demographics and population data
- [**Schema Relationships**](docs/schemas/relationships.md) - Complete ERD, foreign keys, and cross-domain query examples

### Configuration & Architecture
- [Configuration Reference](docs/configuration/) - Model configuration options
- [Architecture Overview](docs/architecture/overview.md) - System design and components

## 🔮 Roadmap

### Near Term
- ✅ Cross-domain foreign key constraints
- ✅ S3/HDFS storage support
- ✅ Expanded FRED/BEA indicators
- 🚧 Enhanced partitioning strategy
- 🚧 Additional virtual tables

### Future
- Public Safety data integration (FBI, NHTSA, FEMA)
- Public Research data (NIH, NASA, NSF)
- Real-time streaming updates
- Machine learning integration
- GraphQL API support

## 🤝 Contributing

We welcome contributions!

### Key Areas for Contribution
- Additional government data sources
- Performance optimizations
- Documentation improvements
- Bug fixes and testing

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Calcite community for the extensible SQL framework
- U.S. government agencies for providing public data APIs
- Contributors and users of the GovData adapter

## 📞 Support

- **Documentation**: [Full Documentation](docs/)
- **Issues**: [GitHub Issues](https://github.com/apache/calcite/issues)
- **Discussions**: [Apache Calcite Mailing Lists](https://calcite.apache.org/community/)

---

*Part of the [Apache Calcite](https://calcite.apache.org) project*
