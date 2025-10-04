# DJIA Production Data Loading System

A complete production data loading solution for Dow Jones Industrial Average (DJIA) companies using Apache Calcite's government data adapters. This system loads SEC filings, economic indicators, and geographic data from 2010 to present and persists everything to AWS S3.

## Architecture Overview

This production system leverages the existing Apache Calcite government data adapters without requiring any code changes. It's a pure configuration-based solution that:

1. **Downloads and processes SEC data** for all 30 DJIA companies (2010-present)
2. **Excludes 424B forms** (prospectus supplements with minimal analytical value)
3. **Loads economic indicators** from FRED, BLS, Treasury, and BEA
4. **Includes geographic data** from Census Bureau
5. **Persists all data to AWS S3** with Hive-partitioned structure
6. **Provides SQL access** via Apache Calcite JDBC

## Quick Start

### 1. Prerequisites

- Java 8+
- Apache Calcite with government data adapters
- AWS S3 bucket
- API keys for government data sources

### 2. Configuration

```bash
# 1. Source the environment configuration
source djia-production-env.sh

# 2. Update API keys and AWS credentials in djia-production-env.sh
# Edit the file and set:
#   - AWS_ACCESS_KEY_ID
#   - AWS_SECRET_ACCESS_KEY
#   - GOVDATA_S3_BUCKET
#   - FRED_API_KEY
#   - BLS_API_KEY
#   - BEA_API_KEY
#   - CENSUS_API_KEY

# 3. Start data loading via SQL connection
java -cp 'calcite-*:file-*:govdata-*' sqlline.SqlLine
```

### 3. Connect and Load Data

```sql
-- Connect to the production model
!connect $JDBC_URL "" "" org.apache.calcite.jdbc.Driver

-- Data loading begins automatically when you connect
-- Use the verification queries to check progress
```

### 4. Verify Data Completeness

```bash
# Run the verification queries
sqlline> !run djia-production-verification.sql
```

## Files Overview

### Configuration Files

- **`djia-production-model.json`** - Calcite model configuration with S3 persistence
- **`djia-production-env.sh`** - Environment variables and setup script
- **`djia-production-verification.sql`** - SQL queries to verify data completeness

### Key Configuration Sections

#### SEC Data Configuration
```json
"sec": {
  "companies": "DJIA",
  "enabledForms": ["10-K", "10-Q", "8-K", "DEF 14A", "3", "4", "5"],
  "excludeForms": ["424B"],
  "downloadTimeout": 2147483647,
  "maxConcurrentDownloads": 5,
  "rateLimitDelayMs": 100
}
```

#### Economic Data Configuration
```json
"econ": {
  "enabledSources": ["fred", "bls", "treasury", "bea"],
  "customFredSeries": ["DGS10", "DGS2", "DGS30", "UNRATE", "PAYEMS", "CPIAUCSL", "GDPC1"],
  "fredSeriesGroups": {
    "treasury_rates": {
      "tableName": "fred_treasury_rates",
      "series": ["DGS1MO", "DGS3MO", "DGS6MO", "DGS1", "DGS2", "DGS3", "DGS5", "DGS7", "DGS10", "DGS20", "DGS30"],
      "partitionStrategy": "YEAR_MATURITY",
      "partitionFields": ["year", "maturity"]
    }
  }
}
```

#### AWS S3 Configuration
```json
"s3Config": {
  "bucket": "${GOVDATA_S3_BUCKET}",
  "region": "${AWS_REGION:us-east-1}",
  "accessKeyId": "${AWS_ACCESS_KEY_ID}",
  "secretAccessKey": "${AWS_SECRET_ACCESS_KEY}",
  "prefix": "govdata-production/"
}
```

## Data Sources and Coverage

### SEC Filings (2010-Present)
- **Companies**: All 30 DJIA companies
- **Forms**: 10-K, 10-Q, 8-K, DEF 14A, insider forms (3, 4, 5)
- **Excluded**: 424B forms (prospectus supplements)
- **Data**: Filing metadata, XBRL facts, insider transactions, vectorized content

### Economic Indicators (2010-Present)
- **FRED**: Interest rates, employment, inflation, GDP
- **BLS**: Employment statistics, wage growth, regional data
- **Treasury**: Yield curves, federal debt
- **BEA**: GDP components, trade statistics, regional income

### Geographic Data
- **Census**: Demographics, boundaries (TIGER/Line)
- **Coverage**: States, counties, ZIP codes

## AWS S3 Data Structure

Data is stored in S3 with Hive-style partitioning:

```
s3://your-bucket/govdata-production/
├── source=sec/
│   ├── cik=0000320193/          # Apple
│   │   ├── filing_type=10-K/
│   │   │   └── year=2024/
│   │   │       ├── *.parquet    # Filing data
│   │   │       └── *.json       # Raw data
│   │   └── filing_type=4/       # Insider transactions
│   └── cik=0000012927/          # Boeing
├── source=econ/
│   ├── type=indicators/
│   │   └── year=2024/
│   │       ├── employment_statistics.parquet
│   │       ├── gdp_components.parquet
│   │       └── fred_indicators.parquet
│   └── type=timeseries/
└── source=geo/
    ├── type=boundary/
    └── type=demographic/
```

## API Keys Required

### Free Government Data APIs

1. **FRED API Key**
   - Register: https://fred.stlouisfed.org/docs/api/api_key.html
   - Used for: Federal Reserve economic data

2. **BLS API Key**
   - Register: https://www.bls.gov/developers/api_signature_v2.html
   - Used for: Employment and labor statistics

3. **BEA API Key**
   - Register: https://apps.bea.gov/API/signup/
   - Used for: GDP, trade, and economic accounts

4. **Census API Key**
   - Register: https://api.census.gov/data/key_signup.html
   - Used for: Demographics and geographic boundaries

### AWS Configuration

Set these environment variables:
- `AWS_ACCESS_KEY_ID` - Your AWS access key
- `AWS_SECRET_ACCESS_KEY` - Your AWS secret key
- `AWS_REGION` - S3 bucket region (default: us-east-1)
- `GOVDATA_S3_BUCKET` - Your S3 bucket name

## Sample Queries

### Check DJIA Companies Data
```sql
SELECT
  c.company_name,
  COUNT(DISTINCT s.filing_type) as filing_types,
  COUNT(*) as total_filings,
  MIN(s.filing_date) as earliest_filing,
  MAX(s.filing_date) as latest_filing
FROM cik_registry c
JOIN sec_filings s ON c.cik = s.cik
WHERE c.group_name = 'DJIA'
GROUP BY c.company_name
ORDER BY c.company_name;
```

### Economic Data Integration
```sql
SELECT
  t.date,
  t.value as ten_year_treasury,
  u.value as unemployment_rate,
  g.value as gdp_growth
FROM fred_treasury_rates t
JOIN fred_employment_indicators u ON t.date = u.date AND u.series_id = 'UNRATE'
JOIN gdp_statistics g ON EXTRACT(YEAR FROM t.date) = EXTRACT(YEAR FROM g.period_date)
WHERE t.series_id = 'DGS10'
  AND t.date >= '2020-01-01'
ORDER BY t.date DESC
LIMIT 20;
```

## Performance and Monitoring

### Expected Data Volumes
- **SEC Filings**: ~500,000 filings across 30 companies (2010-present)
- **Insider Transactions**: ~50,000 transactions
- **XBRL Facts**: ~10 million facts
- **Economic Indicators**: ~1 million data points
- **Total Storage**: ~50-100 GB in S3

### Monitoring Data Loading

```bash
# Monitor progress via log files
tail -f /tmp/govdata-production-cache/*.log

# Check S3 uploads
aws s3 ls s3://your-bucket/govdata-production/ --recursive

# Verify table row counts
sqlline> SELECT COUNT(*) FROM sec_filings;
sqlline> SELECT COUNT(*) FROM fred_treasury_rates;
```

## Troubleshooting

### Common Issues

1. **API Rate Limits**
   - FRED: 120 calls/minute
   - BLS: 500 calls/day (with key)
   - Solution: Rate limiting is built-in

2. **S3 Permissions**
   - Ensure AWS credentials have s3:PutObject, s3:GetObject permissions
   - Check bucket policy allows access

3. **Memory Issues**
   - Increase JVM heap: `export JAVA_OPTS="-Xmx16g"`
   - Use DuckDB execution engine for large queries

### Debug Mode

```bash
# Enable debug logging
export CALCITE_DEBUG=true
export GOVDATA_DEBUG=true

# Check specific logs
tail -f /tmp/govdata-production-cache/sec/*.log
tail -f /tmp/govdata-production-cache/econ/*.log
```

## Security Considerations

1. **API Keys**: Store in environment variables, not in code
2. **AWS Credentials**: Use IAM roles in production
3. **S3 Bucket**: Enable versioning and encryption
4. **Access Control**: Restrict S3 bucket access to authorized users

## Production Deployment

### Recommended Infrastructure

- **EC2 Instance**: m5.2xlarge or larger
- **Storage**: 100GB+ EBS for local cache
- **Network**: VPC with internet access for API calls
- **Monitoring**: CloudWatch for S3 and EC2 metrics

### Automation

```bash
# Cron job for daily updates (after initial load)
0 2 * * * cd /path/to/calcite && source djia-production-env.sh && sqlline --run="SELECT 'Data refresh completed' FROM sec_filings LIMIT 1"
```

## Support and Maintenance

- **Schema Updates**: Handled automatically by adapters
- **New DJIA Companies**: Update CIK registry and restart
- **API Changes**: Monitor government data source notifications
- **Cost Management**: Monitor S3 storage costs and implement lifecycle policies

For technical support, refer to the Apache Calcite documentation and government data adapter implementations.
