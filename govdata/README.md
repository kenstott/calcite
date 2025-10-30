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
    "SELECT s.name as state_name, COUNT(DISTINCT f.cik) as companies, " +
    "       AVG(fo.value) as avg_metric " +
    "FROM sec.facts f " +
    "JOIN geo.census_states s ON f.state_of_incorporation = s.statefp " +
    "JOIN econ.fred_indicators fo ON fo.series_id = 'GDP' " +
    "GROUP BY s.name"
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
| **ECON** | Economic Data | `fred_data_series_catalog` (841K+ series), `fred_indicators`, employment/wages tables, analytical views | BLS, FRED, Treasury, BEA, World Bank |
| **GEO** | Geographic Boundaries | `states`, `counties`, `places`, `zctas`, `census_tracts`, `cbsa`, `congressional_districts` | Census TIGER/Line, HUD |
| **CENSUS** | Demographics & Population | `acs_population`, `acs_demographics`, `acs_income`, `acs_education`, `acs_housing` | Census Bureau ACS, Decennial Census |

### Future Planned
- **LEG**: Congressional bills, votes, committees, nominations, hearings - legislative process data (planning stage - see [LEG/POL Implementation Plan](LEG_POL_SCHEMAS_PLAN.md))
- **POL**: Political offices (Presidents, Governors, Cabinet, SCOTUS), campaigns, executive orders (planning stage - see [LEG/POL Implementation Plan](LEG_POL_SCHEMAS_PLAN.md))
- **SAFETY**: FBI crime, NHTSA crashes, FEMA disasters (not yet implemented)
- **PUB**: NIH grants, NASA projects, NSF research, PTO patents (not yet implemented)
- **HEALTH**: FDA approvals/recalls, CDC health statistics (not yet implemented)
- **WEATHER**: NOAA weather data, climate indicators (not yet implemented)

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
- **Geographic Integration** - Companies linked to states via `state_of_incorporation` → `statefp`
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

### Getting Started
- [Installation Guide](docs/installation.md)
- [Configuration Reference](docs/configuration/)
- [Quick Start Tutorial](docs/tutorials/quick-start.md)

### Schema Documentation
- [SEC Schema Guide](docs/schemas/sec.md) - Financial filings and XBRL data
- [ECON Schema Guide](docs/schemas/econ.md) - Economic indicators and statistics
- [GEO Schema Guide](docs/schemas/geo.md) - Geographic boundaries and mappings
- [**Schema Relationships**](docs/schemas/relationships.md) - Complete ERD, foreign keys, and cross-domain query examples

### Advanced Topics
- [Cross-Domain Queries](docs/tutorials/cross-domain-queries.md)
- [Performance Tuning](docs/architecture/performance.md)
- [Storage Providers](docs/configuration/storage-providers.md)
- [API Integration](docs/api-integration/)

### Development
- [Architecture Overview](docs/architecture/overview.md)
- [Contributing Guide](docs/development/contributing.md)
- [Testing Guide](docs/development/testing.md)
- [API Documentation](docs/api/)

## 🔮 Roadmap

See our [Implementation Plan](implementation_plan_phase_2.md) for detailed roadmap.

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

We welcome contributions! Please see our [Contributing Guide](docs/development/contributing.md) for details.

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
