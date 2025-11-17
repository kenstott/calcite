# DuckDB Conversion Extensions Guide

## Overview

DuckDB conversion extensions are automatically loaded during data transformation to enable advanced operations like embedding generation, geospatial processing, and file format support. These extensions are loaded in **ephemeral DuckDB connections** used for converting source data (JSON, Excel, shapefiles) to Parquet.

## Extension Loading

Extensions are automatically loaded in `AbstractGovDataDownloader.getDuckDBConnection()`:

```java
private Connection getDuckDBConnection() throws SQLException {
  Connection conn = DriverManager.getConnection("jdbc:duckdb:");
  loadConversionExtensions(conn);
  return conn;
}

private void loadConversionExtensions(Connection conn) {
  String[][] extensions = {
      {"quackformers", "FROM community"},  // Embedding generation
      {"spatial", ""},                      // GIS operations
      {"h3", "FROM community"},             // Geospatial hex indexing
      {"excel", ""},                        // Excel file support
      {"fts", ""}                           // Full-text indexing
  };

  for (String[] ext : extensions) {
    try {
      conn.createStatement().execute("INSTALL " + ext[0] + " " + ext[1]);
      conn.createStatement().execute("LOAD " + ext[0]);
    } catch (SQLException e) {
      LOGGER.warn("Failed to load extension '{}' (continuing): {}", ext[0], e.getMessage());
    }
  }
}
```

**Graceful Degradation**: If an extension fails to load, a warning is logged and processing continues. This ensures the system works even when extensions are unavailable.

## Conversion vs Query Extensions

**Clear Separation:**

| Concern | Location | Extensions | Lifecycle |
|---------|----------|------------|-----------|
| **Conversion** | govdata downloader | quackformers, spatial, h3, fts, excel | Ephemeral connection |
| **Query** | DuckDB JDBC schema | vss, fts | Persistent catalog |

**Why separate?**
- Conversion: Transform raw data → structured Parquet (one-time per file)
- Query: Fast retrieval with indexes (many times per file)
- Different connection lifecycles (ephemeral vs persistent)

## Extension Reference

### 1. Quackformers - Embedding Generation

**Purpose**: Generate semantic embeddings for text

**Installation**: `INSTALL quackformers FROM community`

**Function**: `embed(text) → FLOAT[384]`

**Model**: sentence-transformers/all-MiniLM-L6-v2

**Example Schema**:
```json
{
  "name": "description_embedding",
  "type": "array<double>",
  "expression": "embed(description)::FLOAT[384]",
  "comment": "Semantic embedding for similarity search"
}
```

**Use Cases**:
- Semantic search on descriptions, risk factors, business narratives
- Finding similar documents, companies, or concepts
- Clustering and topic modeling

**Performance**: ~50-100ms per embedding (row-by-row processing)

**See**: `duckdb_upgrades.md` Phase 3 for full quackformers integration

---

### 2. Spatial - GIS Operations

**Purpose**: Read shapefiles, perform spatial calculations

**Installation**: `INSTALL spatial` (core extension)

**Key Functions**:
- `ST_Read('file.shp')` - Read shapefile directly
- `ST_Point(lon, lat)` - Create point geometry
- `ST_Area(geometry)` - Calculate area
- `ST_Contains(poly, point)` - Point-in-polygon test
- `ST_Distance(geom1, geom2)` - Distance calculation
- `ST_AsGeoJSON(geometry)` - Export to GeoJSON

**Example Expression**:
```json
{
  "name": "area_sq_km",
  "type": "double",
  "expression": "ST_Area(geometry) / 1000000",
  "comment": "Area in square kilometers"
}
```

**Shapefile Reading**:
```sql
-- Read Census TIGER shapefile directly
COPY (
  SELECT
    GEOID,
    NAME,
    ST_Area(geom) as area,
    ST_AsText(geom) as wkt_geometry
  FROM ST_Read('tl_2020_us_county.shp')
) TO 'counties.parquet' (FORMAT PARQUET);
```

**Use Cases**:
- Reading Census TIGER shapefiles (counties, tracts, blocks)
- Calculating geographic areas and distances
- Point-in-polygon lookups (ZIP to county mapping)
- Geospatial joins

**Benefits vs Java Shapefile Parser**:
- Native DuckDB processing (no Java marshaling)
- Simpler code (SQL vs 1,500+ lines Java)
- Better performance for large shapefiles
- Direct Parquet output

**See**: "Spatial Extension" section below for detailed examples

---

### 3. H3 - Geospatial Hex Indexing

**Purpose**: Hierarchical hexagonal spatial indexing

**Installation**: `INSTALL h3 FROM community`

**Key Functions**:
- `h3_latlng_to_cell(lat, lon, resolution)` - Lat/lon to H3 cell
- `h3_cell_to_lat(h3_index)` - H3 cell to latitude
- `h3_cell_to_lng(h3_index)` - H3 cell to longitude
- `h3_get_resolution(h3_index)` - Get resolution level
- `h3_cell_to_parent(h3_index, resolution)` - Get parent cell
- `h3_cell_to_children(h3_index, resolution)` - Get child cells

**Resolution Levels**:
| Level | Avg Hex Edge | Avg Hex Area | Use Case |
|-------|--------------|--------------|----------|
| 0 | 1107.71 km | ~4,250,546 km² | Continent |
| 3 | 59.81 km | ~12,393 km² | Large metro area |
| 5 | 8.54 km | ~252 km² | City |
| 7 | 1.22 km | ~5.16 km² | Neighborhood |
| 9 | 174.38 m | ~0.105 km² | Block |
| 11 | 24.91 m | ~0.002 km² | Building |

**Example Expression**:
```json
{
  "name": "h3_index",
  "type": "varchar",
  "expression": "h3_latlng_to_cell(latitude, longitude, 7)",
  "comment": "H3 hex index for neighborhood-level aggregation"
}
```

**Spatial Aggregation Example**:
```sql
-- Aggregate population by H3 cells (neighborhoods)
COPY (
  SELECT
    h3_latlng_to_cell(lat, lon, 7) as h3_neighborhood,
    COUNT(*) as location_count,
    SUM(population) as total_population,
    AVG(median_income) as avg_income
  FROM census_blocks
  GROUP BY h3_neighborhood
) TO 'neighborhood_stats.parquet' (FORMAT PARQUET);
```

**Use Cases**:
- Spatial aggregation at consistent scales
- Geospatial clustering and visualization
- Grid-based analysis (uber H3 style)
- Multi-resolution spatial indexing

**See**: "H3 Extension" section below for detailed examples

---

### 4. FTS - Full-Text Search

**Purpose**: Full-text indexing and BM25 ranking

**Installation**: `INSTALL fts` (core extension)

**Functions**:
- `PRAGMA create_fts_index(table, id_col, text_col)` - Create FTS index
- `fts_main_<table>.match_bm25(id, query)` - BM25 ranking function

**Example Usage**:
```sql
-- Create table and FTS index
CREATE TABLE documents (id INTEGER, text VARCHAR);
INSERT INTO documents VALUES
  (1, 'Revenue increased due to strong product demand'),
  (2, 'Operating expenses decreased through cost optimization');

-- Create FTS index
PRAGMA create_fts_index('documents', 'id', 'text');

-- Search with BM25 ranking
SELECT id, text, score
FROM (
  SELECT *, fts_main_documents.match_bm25(id, 'revenue product') AS score
  FROM documents
) WHERE score IS NOT NULL
ORDER BY score DESC;
```

**Use in Conversion**:
While FTS is primarily a query-time feature, indexes can be created during conversion for immediate search availability.

**Use Cases**:
- Keyword search in financial disclosures
- Risk factor analysis
- MD&A narrative search
- Document classification

---

### 5. Excel - Excel File Reading

**Purpose**: Read .xlsx and .xls files directly

**Installation**: Part of spatial extension

**Function**: `st_read('file.xlsx')` or DuckDB's native excel functions

**Example**:
```sql
-- Read Excel file directly
COPY (
  SELECT *
  FROM st_read('economic_indicators.xlsx', layer='Sheet1')
) TO 'indicators.parquet' (FORMAT PARQUET);
```

**Alternative** (DuckDB native):
```sql
-- Using DuckDB's built-in Excel support
COPY (
  SELECT * FROM read_excel('data.xlsx', sheet='Sheet1')
) TO 'output.parquet' (FORMAT PARQUET);
```

**Use Cases**:
- Reading BLS Excel downloads
- Converting Census Excel files
- Processing IRS data releases
- FRED spreadsheet imports

---

## Complete Conversion Example

### Scenario: Census Tract Data with Geospatial Features

**1. Source Data** (JSON):
```json
{
  "geoid": "36061000100",
  "name": "Census Tract 1",
  "lat": 40.7128,
  "lon": -74.0060,
  "population": 3500,
  "description": "Urban residential area with mixed commercial use"
}
```

**2. Schema Definition** (econ-schema.json):
```json
{
  "partitionedTables": [{
    "name": "census_tracts",
    "columns": [
      {"name": "geoid", "type": "varchar"},
      {"name": "name", "type": "varchar"},
      {"name": "lat", "type": "double"},
      {"name": "lon", "type": "double"},
      {"name": "population", "type": "integer"},
      {"name": "description", "type": "varchar"},
      {
        "name": "state_fips",
        "type": "varchar",
        "expression": "SUBSTR(geoid, 1, 2)",
        "comment": "State FIPS code"
      },
      {
        "name": "h3_neighborhood",
        "type": "varchar",
        "expression": "h3_latlng_to_cell(lat, lon, 7)",
        "comment": "H3 hex for neighborhood aggregation"
      },
      {
        "name": "point_geometry",
        "type": "varchar",
        "expression": "ST_AsText(ST_Point(lon, lat))",
        "comment": "WKT point geometry"
      },
      {
        "name": "description_embedding",
        "type": "array<double>",
        "expression": "CASE WHEN length(description) > 50 THEN embed(description)::FLOAT[384] ELSE NULL END",
        "comment": "Semantic embedding for similarity search"
      }
    ]
  }]
}
```

**3. Generated SQL** (AbstractGovDataDownloader.buildConversionSql()):
```sql
COPY (
  SELECT
    CAST(geoid AS VARCHAR) AS geoid,
    CAST(name AS VARCHAR) AS name,
    CAST(lat AS DOUBLE) AS lat,
    CAST(lon AS DOUBLE) AS lon,
    CAST(population AS INTEGER) AS population,
    CAST(description AS VARCHAR) AS description,
    (SUBSTR(geoid, 1, 2)) AS state_fips,
    (h3_latlng_to_cell(lat, lon, 7)) AS h3_neighborhood,
    (ST_AsText(ST_Point(lon, lat))) AS point_geometry,
    (CASE WHEN length(description) > 50 THEN embed(description)::FLOAT[384] ELSE NULL END) AS description_embedding
  FROM read_json_auto('/cache/census_tracts.json')
) TO '/parquet/census_tracts.parquet' (FORMAT PARQUET);
```

**4. Extensions Used**:
- **spatial**: `ST_Point()`, `ST_AsText()` for geometry
- **h3**: `h3_latlng_to_cell()` for hex indexing
- **quackformers**: `embed()` for semantic embeddings

**5. Result**: Single Parquet file with all computed columns, ready for queries

---

## Extension Availability Matrix

| Extension | Platform | Installation | Availability |
|-----------|----------|--------------|--------------|
| quackformers | linux_amd64, osx_arm64 | `FROM community` | ⚠️ Platform-specific |
| spatial | All platforms | Core | ✅ Always available |
| h3 | Most platforms | `FROM community` | ✅ Usually available |
| fts | All platforms | Core | ✅ Always available |
| excel | All platforms | Via spatial | ✅ Usually available |

**Note**: quackformers may not be available on all platforms (e.g., osx_arm64 for DuckDB v1.1.3). Graceful degradation ensures system continues to work.

---

## Best Practices

### 1. Use Expressions for Computed Columns

✅ **Good** - Let DuckDB compute:
```json
{
  "name": "h3_index",
  "expression": "h3_latlng_to_cell(lat, lon, 7)"
}
```

❌ **Bad** - Don't compute in Java:
```java
for (JsonNode record : records) {
  String h3 = H3Core.newInstance().latLngToCellAddress(lat, lon, 7);
  record.put("h3_index", h3);  // Expensive marshaling
}
```

### 2. Check Extension Availability

Conversion continues even if extensions fail:
```
WARN: Failed to load extension 'quackformers' (continuing): HTTP 404
```

Design schemas to work without optional extensions.

### 3. Combine Extensions

Use multiple extensions in one expression:
```json
{
  "name": "spatial_summary",
  "expression": "concat('H3:', h3_latlng_to_cell(lat, lon, 7), ' Point:', ST_AsText(ST_Point(lon, lat)))"
}
```

### 4. Performance Considerations

- **quackformers**: ~50-100ms per row (use for important text only)
- **spatial**: Fast for simple operations, slower for complex geometries
- **h3**: Very fast (<1ms per row)
- **fts**: Index creation adds time, but enables fast searches

### 5. When to Use Each Extension

| Extension | Use When |
|-----------|----------|
| quackformers | Long text (>100 chars), semantic search needed |
| spatial | Working with shapefiles, need GIS operations |
| h3 | Need consistent spatial grid, multi-resolution analysis |
| fts | Keyword search requirements |
| excel | Source data in Excel format |

---

## Troubleshooting

### Extension Fails to Load

**Symptom**: Warning in logs about failed extension

**Causes**:
- Extension not available for platform
- Network issue downloading community extension
- DuckDB version incompatibility

**Solution**: Design schemas to work without extension, or use conditional expressions:
```json
{
  "expression": "CASE WHEN h3_latlng_to_cell IS NOT NULL THEN h3_latlng_to_cell(lat, lon, 7) ELSE NULL END"
}
```

### Slow Conversion

**Symptom**: Long conversion times

**Diagnosis**:
- Check if using quackformers on large text fields
- Profile extension usage with DuckDB EXPLAIN

**Solution**:
- Limit embedding generation to important fields
- Use conditional expressions to skip short text
- Consider batch processing for large datasets

### Memory Issues

**Symptom**: OOM during conversion

**Solution**:
- Process data in chunks
- Reduce concurrent conversions
- Limit embedding generation

---

## Version History

- **Phase 2 (2025-11-10)**: Added automatic extension loading in getDuckDBConnection()
- **Phase 6 (2025-11-10)**: Comprehensive documentation and integration tests
- Extensions: quackformers, spatial, h3, fts, excel
