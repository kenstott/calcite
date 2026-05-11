# Tabular Mode

In tabular mode the source defines the schema. Point at data — a file, a directory, an S3 prefix, an HTTP endpoint — and the adapter discovers the structure and makes it queryable as SQL tables. No schema definition, no transformer code.

---

## Supported Formats

### Native Tabular Formats

Schema comes directly from the file structure:

| Format | Notes |
|--------|-------|
| CSV / TSV | Automatic type inference. See [CSV Type Inference](csv-type-inference.md). |
| JSON | Nested objects flattened; JSONPath for multi-table extraction |
| YAML | Hierarchical data, same flattening as JSON |
| Parquet | Native columnar; fastest read performance |
| Apache Arrow | In-memory columnar; best for hot data |
| Apache Iceberg | Full time-travel and schema evolution support |

### Embedded Tabular Formats

The adapter extracts tabular structure automatically — no custom code:

| Format | What gets extracted |
|--------|-------------------|
| Excel (.xlsx, .xls) | Each sheet → separate table |
| Word (.docx) | Each table in the document → separate table |
| PowerPoint (.pptx) | Tables on each slide → separate tables |
| HTML (.html, .htm) | Each `<table>` element → separate table |
| Markdown (.md) | Each GFM table → separate table |
| XML (.xml) | XPath-defined element sets → tables |

**Naming convention for multi-table files:**
- `report.xlsx` (sheets: Summary, Detail) → `report__summary`, `report__detail`
- `document.docx` (2 tables) → `document__table_1`, `document__table_2`
- `slides.pptx` (titled slide) → `slides__slide_title__table_name`
- `page.html` (3 tables) → `page__table_1`, `page__table_2`, `page__table_3`

---

## Transports

The format and the transport are independent. The same Excel or CSV file can be read from any of these:

| Transport | Configuration |
|-----------|--------------|
| Local filesystem | `"directory": "/data/reports"` |
| S3 / R2 / MinIO | `"storageType": "s3"` |
| HTTP / HTTPS | `"storageType": "http"` or `https://` table URL |
| FTP / SFTP | `"storageType": "ftp"` |
| SharePoint | `"storageType": "sharepoint"` |

Transport is configured at the schema level (all tables use the same transport) or per-table (each table URL determines its transport).

---

## Crawlers

Point at a location; the adapter finds all files.

**Directory crawl:**
```json
{
  "operand": {
    "directory": "/data/feeds",
    "recursive": true
  }
}
```

Every file in `/data/feeds/` and its subdirectories becomes a table. CSV, JSON, Parquet, Excel, and other supported formats are all discovered. Files in subdirectories get names prefixed with the subdirectory path using `__` as separator.

**S3 prefix crawl:**
```json
{
  "operand": {
    "directory": "s3://my-bucket/feeds/",
    "storageType": "s3",
    "recursive": true
  }
}
```

**Filtered crawl with glob pattern:**
```json
{
  "operand": {
    "directory": "/data",
    "directoryPattern": "2024/**/*.csv"
  }
}
```

Only files matching the glob are discovered. `directoryPattern` takes precedence over `recursive`.

**Explicit glob table:**
```json
{
  "tables": [{
    "name": "all_sales",
    "url": "sales/*.csv"
  }]
}
```

Combines all matching files into one table. Schema is unified across all matched files.

---

## HTTP APIs

Any HTTP endpoint that returns structured data (JSON, CSV, XML) is a tabular source. The adapter fetches, parses, and makes it queryable as a table — no transformer code required as long as the response shape maps naturally to rows.

### Simple REST Endpoint

```json
{
  "tables": [{
    "name": "weather_stations",
    "url": "https://api.weather.gov/stations",
    "storageConfig": {
      "headers": {
        "Accept": "application/json"
      }
    }
  }]
}
```

### Authenticated Endpoint

Five auth types are supported via the `storageConfig` block:

**Bearer token (static)**
```json
{
  "storageConfig": {
    "auth": "bearer",
    "token": "${API_TOKEN}"
  }
}
```

**API key**
```json
{
  "storageConfig": {
    "auth": "api_key",
    "name": "X-Api-Key",
    "value": "${API_KEY}"
  }
}
```

**HTTP Basic**
```json
{
  "storageConfig": {
    "auth": "basic",
    "username": "${API_USER}",
    "password": "${API_PASS}"
  }
}
```

**OAuth2**
```json
{
  "storageConfig": {
    "auth": "oauth2",
    "tokenEndpoint": "https://auth.example.com/token",
    "clientId": "${CLIENT_ID}",
    "clientSecret": "${CLIENT_SECRET}"
  }
}
```

**Dynamic token — fetched at runtime**

When the token changes between runs (rotated credentials, short-lived tokens), the adapter can fetch it dynamically before each request batch:

```json
{
  "storageConfig": {
    "tokenCommand": "aws secretsmanager get-secret-value --secret-id my-api-key --query SecretString --output text",
    "tokenEnv": "API_TOKEN",
    "tokenFile": "/run/secrets/api_token",
    "cacheEnabled": true,
    "cacheTtl": 300000
  }
}
```

Only one of `tokenCommand`, `tokenEnv`, or `tokenFile` is needed. Token is cached for `cacheTtl` milliseconds (default 300,000 ms = 5 minutes).

### Paginated Endpoint

Four pagination strategies are supported:

**Offset/limit** — most common REST pattern:
```json
{
  "storageConfig": {
    "pagination": {
      "type": "offset",
      "pageSize": 1000,
      "offsetParam": "offset",
      "limitParam": "limit"
    }
  }
}
```

**Cursor** — API returns a cursor token in the response body pointing to the next page:
```json
{
  "storageConfig": {
    "pagination": {
      "type": "cursor",
      "pageSize": 500,
      "cursorParam": "after",
      "cursorPath": "$.meta.next_cursor"
    }
  }
}
```

`cursorPath` is a JSONPath expression applied to the response body to extract the next cursor value. Pagination stops when the path returns null or an empty string.

**Page number** — API uses a `page=N` parameter:
```json
{
  "storageConfig": {
    "pagination": {
      "type": "page",
      "pageSize": 100,
      "pageParam": "page"
    }
  }
}
```

**CSV stream** — for endpoints that stream a single large CSV response (no separate pages):
```json
{
  "storageConfig": {
    "pagination": {
      "type": "csv_stream"
    }
  }
}
```

All pagination strategies fetch all pages and present the concatenated result as a single table.

---

## API to Lake — Multi-Endpoint APIs

This is where tabular mode becomes a full API-to-lake pipeline. A complex API with multiple query endpoints, each returning different data, wired entirely through config. Each endpoint becomes a table in the schema. No code.

### Example: Multi-Endpoint Government API

```json
{
  "name": "census",
  "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
  "operand": {
    "directory": "s3://lake/source=census/",
    "storageType": "s3",
    "executionEngine": "DUCKDB",
    "tables": [
      {
        "name": "population",
        "url": "https://api.census.gov/data/2023/acs/acs5",
        "storageConfig": {
          "params": {
            "get": "B01003_001E,NAME",
            "for": "state:*",
            "key": "${CENSUS_API_KEY}"
          }
        }
      },
      {
        "name": "income",
        "url": "https://api.census.gov/data/2023/acs/acs5",
        "storageConfig": {
          "params": {
            "get": "B19013_001E,NAME",
            "for": "county:*",
            "key": "${CENSUS_API_KEY}"
          }
        }
      },
      {
        "name": "housing",
        "url": "https://api.census.gov/data/2023/acs/acs5",
        "storageConfig": {
          "params": {
            "get": "B25001_001E,NAME",
            "for": "tract:*",
            "key": "${CENSUS_API_KEY}"
          }
        }
      }
    ]
  }
}
```

Three API endpoints, three tables, one schema, one JDBC connection. No code. Query them individually or join across them:

```sql
SELECT p.NAME, p.population, i.median_income
FROM census.population p
JOIN census.income i ON p.state = i.state;
```

### Dimension Expansion

For APIs where you must call once per dimension value (year, geography, category), the adapter expands a dimension list into multiple calls automatically and concatenates the results into a single table:

```json
{
  "name": "annual_data",
  "url": "https://api.example.com/data",
  "dimensions": {
    "year": ["2020", "2021", "2022", "2023", "2024"]
  },
  "storageConfig": {
    "params": {
      "year": "${year}"
    }
  }
}
```

Five API calls, one table. The `year` dimension is injected as a query parameter on each call. Results are concatenated with the dimension value added as a column.

---

## Refresh

Tabular tables can refresh when source data changes:

```json
{
  "tables": [{
    "name": "live_feed",
    "url": "data.csv",
    "refreshInterval": "5 minutes"
  }]
}
```

Refresh is lazy — checked on query access, not on a background timer. File modification time (local) or ETag/Last-Modified (HTTP) determines whether a refresh is needed.

**Refresh support by format:**

| Format | Refresh support |
|--------|----------------|
| CSV, JSON, YAML | Data refresh on interval |
| Parquet | Data refresh on interval |
| Excel, Word, PowerPoint, HTML, XML, Markdown | Converted once at startup; restart required for changes |

**Note**: enabling `refreshInterval` disables Parquet conversion caching. You cannot have both automatic refresh and Parquet-cached performance simultaneously.

---

## Performance

For best query performance on large tabular datasets:

1. **Use Parquet or Iceberg as source format** where possible — native columnar, fastest reads
2. **Enable materialization** — CSV/JSON sources are converted to Parquet cache on first access
3. **Use DuckDB engine** — best default for single-node analytics with full SQL support
4. **Declare partition columns** — enables partition pruning before the query engine runs

See [Performance Tuning](performance-tuning.md) and [JDBC Engines](jdbc-engines.md) for details.
