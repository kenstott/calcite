# DuckDB-Native Architecture Upgrade Plan

**Created**: 2025-11-10
**Status**: Planning Phase

## Executive Summary

Migrate to a DuckDB-native architecture that:
1. Implements **generic expression columns** in the file adapter
2. Uses **DuckDB for ALL Parquet transformations** (eliminate Java row processing)
3. Removes **Java embedding/caching infrastructure** (~2,000 lines)
4. Adopts **quackformers extension** for native embedding generation
5. Integrates **high-value DuckDB extensions** (spatial, h3, fts, excel, vss)
6. **Experimental**: Tests webbed extension for XBRL parsing

**Key Insight**: Keep computation where data lives - eliminate expensive data transfer between DuckDB and Java.

---

## Part 1: Generic Expression Columns (File Adapter)

### 1.1 Core Design

**Principle**: Any column with `expression` attribute = computed column

**Location**: `file/src/main/java/org/apache/calcite/adapter/file/partition/PartitionedTableConfig.java`

**Why generic?**
- Not tied to govdata domain
- Reusable by any adapter (splunk, sharepoint, etc.)
- Any SQL expression, not just embeddings

### 1.2 TableColumn Schema

```java
public static class TableColumn {
  private final String name;
  private final String type;
  private final boolean nullable;
  private final String comment;
  private final String expression;  // SQL expression for computed columns

  public boolean hasExpression() {
    return expression != null && !expression.trim().isEmpty();
  }

  public boolean isComputed() {
    return hasExpression();
  }
}
```

**Remove**:
- `computed` boolean field (redundant with expression)
- `embeddingConfig` Map field (obsolete)
- All embedding-specific methods

### 1.3 JSON Schema Format

```json
{
  "columns": [
    {"name": "id", "type": "varchar"},
    {"name": "date", "type": "date"},
    {
      "name": "year",
      "type": "integer",
      "expression": "EXTRACT(YEAR FROM date)"
    },
    {
      "name": "embedding",
      "type": "array<double>",
      "expression": "embed(description)::FLOAT[384]"
    }
  ]
}
```

### 1.4 Expression Examples

**Embeddings**:
```json
{"expression": "embed(text)::FLOAT[384]"}
{"expression": "embed(concat_ws(' | ', title, description))::FLOAT[384]"}
{"expression": "CASE WHEN length(text) > 100 THEN embed(text)::FLOAT[384] ELSE NULL END"}
```

**Date/Time**:
```json
{"expression": "EXTRACT(YEAR FROM date)"}
{"expression": "CAST(SUBSTR(time_period, 2, 1) AS INTEGER)"}
{"expression": "strptime(date_string, '%Y-%m-%d')"}
```

**String Operations**:
```json
{"expression": "first_name || ' ' || last_name"}
{"expression": "SUBSTR(fips, 1, 2)"}
{"expression": "UPPER(category)"}
```

**Numeric Calculations**:
```json
{"expression": "price * quantity"}
{"expression": "(value - lag(value, 12) OVER (ORDER BY date)) / lag(value, 12)"}
```

**Geospatial**:
```json
{"expression": "h3_latlng_to_cell(lat, lon, 7)"}
{"expression": "ST_Area(geometry)"}
```

---

## Part 2: Single DuckDB Conversion Path

### 2.1 Problem with Dual Path

**Current** (AbstractGovDataDownloader.java:1835-1958):
```java
if (transformer == null) {
    convertCachedJsonToParquetViaDuckDB(...);  // Fast path
} else {
    // Slow path: Java row processing
    for (record : records) {
        record = transformer.transform(record);
        generateEmbeddingColumns(record, columns, provider);
    }
    storageProvider.writeAvroParquet(...);  // Avro conversion overhead
}
```

**Issues**:
- Dual code paths = complexity
- Data transfer overhead: DuckDB → Java → DuckDB
- Marshaling cost: 10-30 seconds per 100k records
- Memory pressure: Java object allocation

### 2.2 New Single Path

```java
protected void convertCachedJsonToParquet(...) {
    // ALWAYS use DuckDB
    String sql = buildConversionSql(columns, missingValueIndicator,
                                    fullJsonPath, fullParquetPath);

    try (Connection conn = getDuckDBConnection();
         Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
    }
}
```

### 2.3 SQL Generation with Expressions

**File**: `govdata/src/main/java/org/apache/calcite/adapter/govdata/AbstractGovDataDownloader.java:1641-1695`

```java
protected String buildConversionSql(
    List<TableColumn> columns,
    String missingValueIndicator,
    String jsonPath,
    String parquetPath) {

  StringBuilder sql = new StringBuilder("COPY (\n  SELECT\n");
  List<String> columnExpressions = new ArrayList<>();

  for (TableColumn column : columns) {
    if (column.hasExpression()) {
      // Expression column: use SQL directly
      columnExpressions.add(
          "    (" + column.getExpression() + ") AS " +
          quoteIdentifier(column.getName())
      );
    } else {
      // Regular column: CAST from JSON with type conversion
      String sqlType = javaToDuckDbType(column.getType());
      String castExpr = buildCastExpression(
          column.getName(), sqlType, missingValueIndicator);
      columnExpressions.add(
          "    " + castExpr + " AS " + quoteIdentifier(column.getName())
      );
    }
  }

  sql.append(String.join(",\n", columnExpressions));
  sql.append("\n  FROM read_json_auto(")
     .append(quoteLiteral(jsonPath)).append(")\n");
  sql.append(") TO ").append(quoteLiteral(parquetPath))
     .append(" (FORMAT PARQUET);");

  return sql.toString();
}
```

**Example Output**:
```sql
COPY (
  SELECT
    CAST(series_id AS VARCHAR) AS series_id,
    CAST(date AS DATE) AS date,
    (EXTRACT(YEAR FROM date)) AS year,
    (embed(description)::FLOAT[384]) AS description_embedding
  FROM read_json_auto('/cache/data.json')
) TO '/parquet/output.parquet' (FORMAT PARQUET);
```

### 2.4 Performance Analysis

**Data Transfer Overhead Eliminated**:

**Old 3-step approach** (rejected):
```
Step 1: JSON → DuckDB → base.parquet              (5 sec)
Step 2: base.parquet → Java → embeddings.parquet  (30 sec transfer + 500 sec compute)
Step 3: JOIN → final.parquet                       (10 sec)
TOTAL: 545 seconds
```

**New single-pass approach**:
```
JSON → DuckDB [embed() in SELECT] → parquet       (5 sec + 500 sec compute)
TOTAL: 505 seconds
```

**Speedup**: 8% from eliminating 40 seconds of data transfer

**For larger datasets or faster embeddings, overhead dominates more**:
- 1M records: 5-10 minutes saved in marshaling
- If embedding = 2ms/record: 200s compute + 28s transfer → **12% speedup**
- If embedding = 1ms/record: 100s compute + 28s transfer → **22% speedup**

---

## Part 3: Remove Java Embedding/Cache Infrastructure

### 3.1 Files to Delete (~1,750 lines)

**Embedding providers**:
- `file/src/main/java/org/apache/calcite/adapter/file/similarity/ONNXEmbeddingProvider.java` (~300 lines)
- `file/src/main/java/org/apache/calcite/adapter/file/similarity/OpenAIEmbeddingProvider.java` (~200 lines)
- `file/src/main/java/org/apache/calcite/adapter/file/similarity/LocalEmbeddingProvider.java` (~150 lines)
- `file/src/main/java/org/apache/calcite/adapter/file/similarity/TextEmbeddingProvider.java` (~100 lines)
- `file/src/main/java/org/apache/calcite/adapter/file/similarity/EmbeddingProviderFactory.java` (~150 lines)

**Cache infrastructure**:
- `file/src/main/java/org/apache/calcite/adapter/file/similarity/PersistentEmbeddingCache.java` (~400 lines)
- `file/src/main/java/org/apache/calcite/adapter/file/similarity/CachedEmbeddingProvider.java` (~150 lines)

**Govdata methods** (AbstractGovDataDownloader.java):
- `generateEmbeddingColumns()` (lines 1998-2042, ~45 lines)
- `concatenateFieldsNatural()` (lines 2048-2083, ~35 lines)
- `humanizeColumnName()` (lines 2089-2093, ~5 lines)
- `cleanseValue()` (lines 2098-2126, ~30 lines)
- `getEmbeddingProvider()` (lines 267-310, ~44 lines)

**Total**: ~1,750 lines

### 3.2 Keep These Files

- `SimilarityFunctions.java` - Calcite-side UDFs (may be useful for non-DuckDB queries)

### 3.3 Remove Dependencies

**File**: `build.gradle`

```gradle
// REMOVE these dependencies
implementation 'com.microsoft.onnxruntime:onnxruntime:1.16.3'      // ~80MB
implementation 'ai.djl:api:0.25.0'                                   // ~15MB
implementation 'ai.djl.huggingface:tokenizers:0.25.0'              // ~5MB
```

**JAR size reduction**: ~100MB

### 3.4 Remove from Schemas

Delete `embeddingCache` configuration from all schema JSON files:
```json
{
  "embeddingCache": {  // DELETE THIS ENTIRE SECTION
    "enabled": true,
    "path": "{cacheDir}/embeddings_cache",
    "provider": "onnx",
    "model": "all-MiniLM-L6-v2",
    "dimension": 384
  }
}
```

### 3.5 Rationale for Cache Removal

**Why caching doesn't help**:
- Embeddings should be used for **long, variable text** (SEC filings, descriptions)
- Long text = high variability = low cache hit rate
- Short, repeated text (categories, IDs) → use regular indexes instead
- Cache complexity not justified for primary use case

**Best practices**:
- ✅ **Use embeddings for**: 100+ char unique text, semantic search needs
- ❌ **Don't use embeddings for**: Short repeated strings, exact match queries

---

## Part 4: Conversion-Time DuckDB Extensions

### 4.1 Extension Loading

**File**: `govdata/src/main/java/org/apache/calcite/adapter/govdata/AbstractGovDataDownloader.java`

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
      LOGGER.debug("Loading conversion extension: {}", ext[0]);
      conn.createStatement().execute("INSTALL " + ext[0] + " " + ext[1]);
      conn.createStatement().execute("LOAD " + ext[0]);
    } catch (SQLException e) {
      LOGGER.warn("Failed to load {} (continuing): {}", ext[0], e.getMessage());
    }
  }
}
```

### 4.2 Extension Purposes

| Extension | Purpose | Use Case in Expressions |
|-----------|---------|------------------------|
| **quackformers** | Generate embeddings | `embed(text)::FLOAT[384]` |
| **spatial** | GIS operations | `ST_Read('file.shp')`, `ST_Area(geometry)` |
| **h3** | Geospatial hex indexing | `h3_latlng_to_cell(lat, lon, 7)` |
| **excel** | Read Excel files | `read_excel('data.xlsx')` |
| **fts** | Full-text indexing | Create FTS indexes during conversion |

### 4.3 Quackformers Details

**Repository**: https://github.com/martin-conur/quackformers
**Status**: Active (v0.1.1, May 2025)
**License**: MIT
**Implementation**: Rust (83.5%)

**Functions**:
- `embed(text)` → `FLOAT[384]` (sentence-transformers/all-MiniLM-L6-v2)
- `embed_jina(text)` → `FLOAT[768]` (Jina BERT)

**Perfect match**: Uses same model as current Java ONNX implementation

**Installation**:
```sql
INSTALL quackformers FROM community;
LOAD quackformers;
```

**Limitations**:
- Row-by-row processing (acknowledged in roadmap)
- No built-in caching (intentional - not needed for long text)

**Advantages over Java ONNX**:
- Zero data transfer (stays in DuckDB)
- No JNI boundary crossings
- Model bundled in extension
- Simpler deployment

### 4.4 Spatial Extension Benefits

**Current**: Custom Java shapefile parser (1,549+ lines)
- `ShapefileToParquetConverter.java`
- `TigerShapefileParser.java`

**With spatial extension**:
```sql
-- Read shapefile directly in DuckDB
CREATE VIEW tiger_counties AS
SELECT * FROM ST_Read('tiger/counties.shp');

-- Spatial operations
SELECT
  county_name,
  ST_Area(geometry) as area_sq_km,
  ST_Contains(geometry, ST_Point(lon, lat)) as contains_point
FROM tiger_counties;
```

**Potential**: Eliminate 1,549+ lines of Java code

---

## Part 5: Query-Time DuckDB Extensions

### 5.1 VSS Extension = Query Optimization

**Key insight**: VSS is NOT a conversion feature, it's a **query-time optimization**

**Load in persistent DuckDB catalog** (not in-memory conversion connection)

**File**: `file/src/main/java/org/apache/calcite/adapter/file/duckdb/DuckDBJdbcSchemaFactory.java:334`

```java
private static void loadQueryExtensions(Connection conn) {
  String[] extensions = {"vss", "fts"};  // Query optimization only

  for (String ext : extensions) {
    try {
      LOGGER.info("Loading query extension: {}", ext);
      conn.createStatement().execute("INSTALL " + ext);
      conn.createStatement().execute("LOAD " + ext);
      LOGGER.info("✓ Loaded: {}", ext);
    } catch (SQLException e) {
      LOGGER.warn("✗ Failed to load {}: {}", ext, e.getMessage());
    }
  }
}
```

### 5.2 VSS Usage Pattern

**Create HNSW index** (after data loaded):
```sql
CREATE INDEX embedding_hnsw ON table_name
USING HNSW(embedding_column) WITH (metric='cosine');
```

**Query with index** (10-100x faster):
```sql
SELECT * FROM table_name
ORDER BY array_distance(embedding_column, embed('query text'))
LIMIT 10;
-- Uses HNSW index for approximate nearest neighbor search
```

**Without index** (brute force):
```sql
SELECT * FROM table_name
ORDER BY array_cosine_similarity(embedding_column, embed('query text')) DESC
LIMIT 10;
-- Scans all rows: O(n)
```

### 5.3 Future Enhancement: Query Rewriting

**Potential**: Rewrite Calcite's `COSINE_SIMILARITY()` to use HNSW automatically

**User writes** (Calcite SQL):
```sql
SELECT * FROM facts
WHERE COSINE_SIMILARITY(risk_embedding, embed('supply chain')) > 0.8
ORDER BY COSINE_SIMILARITY(risk_embedding, embed('supply chain')) DESC
LIMIT 10;
```

**DuckDB adapter could rewrite to**:
```sql
SELECT * FROM facts
WHERE array_cosine_similarity(risk_embedding, embed('supply chain')) > 0.8
ORDER BY array_distance(risk_embedding, embed('supply chain'))
LIMIT 10;
-- Automatically uses HNSW index if available
```

**Implementation complexity**: Medium (requires Calcite query rewriting logic)

---

## Part 6: Architecture Summary

### 6.1 Conversion Time (In-Memory DuckDB)

```
Source Data (JSON/Excel/Shapefile)
    ↓
DuckDB Connection (ephemeral)
  Extensions: quackformers, spatial, h3, fts, excel
    ↓
SQL: COPY (SELECT ... [with expressions] ... FROM read_json(...)) TO parquet
    ↓
Parquet Output (with computed columns)
```

**Key**: All transformations via SQL expressions, data never leaves DuckDB

### 6.2 Query Time (Persistent DuckDB Catalog)

```
SQL Query
    ↓
DuckDB JDBC Connection (persistent)
  Extensions: vss, fts
  HNSW Indexes on embedding columns
    ↓
Optimized Execution (approximate nearest neighbor)
    ↓
Results
```

**Key**: HNSW indexes accelerate similarity search by 10-100x

### 6.3 Clear Separation

| Concern | Location | Extensions |
|---------|----------|------------|
| **Data Conversion** | govdata downloader | quackformers, spatial, h3, fts, excel |
| **Query Optimization** | DuckDB JDBC schema | vss, fts |

**Why separate?**:
- Conversion: Transform raw data → structured Parquet
- Query: Fast retrieval with indexes
- Different lifecycles (ephemeral vs persistent connections)

---

## Part 7: Example Configurations

### 7.1 FRED Economic Data

```json
{
  "partitionedTables": [{
    "name": "fred_series",
    "columns": [
      {"name": "series_id", "type": "varchar"},
      {"name": "title", "type": "varchar"},
      {"name": "notes", "type": "varchar"},
      {"name": "observation_date", "type": "date"},
      {"name": "value", "type": "double"},
      {
        "name": "year",
        "type": "integer",
        "expression": "EXTRACT(YEAR FROM observation_date)",
        "comment": "Computed year for partitioning"
      },
      {
        "name": "notes_embedding",
        "type": "array<double>",
        "expression": "CASE WHEN length(notes) > 100 THEN embed(notes)::FLOAT[384] ELSE NULL END",
        "comment": "Semantic search (only for substantial descriptions)"
      },
      {
        "name": "yoy_change",
        "type": "double",
        "expression": "(value - lag(value, 12) OVER (PARTITION BY series_id ORDER BY observation_date)) / lag(value, 12)",
        "comment": "Year-over-year percentage change"
      }
    ]
  }]
}
```

### 7.2 Census Geographic Data

```json
{
  "partitionedTables": [{
    "name": "census_tracts",
    "columns": [
      {"name": "geoid", "type": "varchar"},
      {"name": "name", "type": "varchar"},
      {"name": "population", "type": "integer"},
      {"name": "lat", "type": "double"},
      {"name": "lon", "type": "double"},
      {
        "name": "state_fips",
        "type": "varchar",
        "expression": "SUBSTR(geoid, 1, 2)",
        "comment": "State FIPS code from GEOID"
      },
      {
        "name": "county_fips",
        "type": "varchar",
        "expression": "SUBSTR(geoid, 1, 5)",
        "comment": "County FIPS code from GEOID"
      },
      {
        "name": "h3_index",
        "type": "varchar",
        "expression": "h3_latlng_to_cell(lat, lon, 7)",
        "comment": "H3 hexagonal cell for spatial aggregation"
      }
    ]
  }]
}
```

### 7.3 SEC Financial Data

```json
{
  "partitionedTables": [{
    "name": "company_facts",
    "columns": [
      {"name": "cik", "type": "varchar"},
      {"name": "fiscal_year", "type": "integer"},
      {"name": "business_description", "type": "varchar"},
      {"name": "risk_factors", "type": "varchar"},
      {
        "name": "full_profile",
        "type": "varchar",
        "expression": "concat_ws(' | ', business_description, risk_factors)",
        "comment": "Combined text for full-text search"
      },
      {
        "name": "risk_embedding",
        "type": "array<double>",
        "expression": "CASE WHEN length(risk_factors) > 100 THEN embed(risk_factors)::FLOAT[384] ELSE NULL END",
        "comment": "Semantic search on risk disclosures (only if substantial)"
      }
    ]
  }]
}
```

---

## Part 8: Experimental - XBRL with webbed Extension

### 8.1 Current XBRL Parsing (Java)

**Files**:
- `govdata/src/main/java/org/apache/calcite/adapter/govdata/sec/SecXbrlParser.java` (~500+ lines)
- `govdata/src/main/java/org/apache/calcite/adapter/govdata/sec/SecTextVectorizer.java` (uses XBRL parsing)

**Process**:
1. Download XBRL XML files from SEC EDGAR
2. Parse in Java using DOM/SAX
3. Extract financial concepts via XPath in Java
4. Handle namespaces, contexts, periods
5. Convert to structured records
6. Write to Parquet

**Complexity**:
- XBRL has complex nested structure
- Multiple namespaces (us-gaap, dei, etc.)
- Contexts link facts to periods/entities
- Inline XBRL (iXBRL) embedded in HTML

### 8.2 webbed Extension Overview

**Repository**: DuckDB community extension
**Purpose**: Web scraping and HTML/XML parsing in DuckDB
**Capabilities**:
- XPath queries in SQL
- CSS selectors
- Native XML/HTML handling
- Web scraping functions

### 8.3 Potential Usage

**Hypothetical expression-based extraction**:
```json
{
  "partitionedTables": [{
    "name": "xbrl_facts",
    "columns": [
      {"name": "cik", "type": "varchar"},
      {"name": "filing_date", "type": "date"},
      {"name": "xbrl_content", "type": "varchar"},
      {
        "name": "revenue",
        "type": "double",
        "expression": "CAST(xpath(xbrl_content, '//us-gaap:Revenue/text()') AS DOUBLE)",
        "comment": "Extracted via XPath"
      },
      {
        "name": "net_income",
        "type": "double",
        "expression": "CAST(xpath(xbrl_content, '//us-gaap:NetIncomeLoss/text()') AS DOUBLE)"
      },
      {
        "name": "company_name",
        "type": "varchar",
        "expression": "xpath(xbrl_content, '//dei:EntityRegistrantName/text()')"
      }
    ]
  }]
}
```

**Or direct XML reading**:
```sql
SELECT
  xpath(content, '//us-gaap:Revenue/text()') as revenue,
  xpath(content, '//us-gaap:NetIncomeLoss/text()') as net_income
FROM read_xml('10-k.xbrl');
```

### 8.4 Experimental Phase Plan

**Phase 7a: Research** (1-2 days)
- Install webbed extension
- Test with sample SEC XBRL files
- Verify XPath support for financial namespaces
- Test handling of contexts and periods
- Check inline XBRL (iXBRL) support

**Phase 7b: Prototype** (3-5 days)
- Create expression-based XBRL schema
- Implement key financial concept extraction
- Test with 10-K, 10-Q filings
- Compare output with existing SecXbrlParser
- Identify gaps and limitations

**Phase 7c: Evaluate** (1 day)
- **Completeness**: Can webbed handle XBRL complexity?
- **Accuracy**: Do extracted values match Java parser?
- **Performance**: DuckDB native vs Java parsing
- **Maintainability**: SQL expressions vs Java code
- **Decision**: Adopt, hybrid approach, or keep Java

### 8.5 Known Challenges

**XBRL Complexity**:
- Nested contexts link facts to time periods
- Dimensions for additional categorization
- Footnote references
- Unit conversions (shares, USD, etc.)
- Inline XBRL mixed with HTML narrative

**Potential Solutions**:
1. **Simple extraction**: Use webbed for basic facts, Java for complex logic
2. **Hybrid approach**: webbed for extraction, Java for validation/enrichment
3. **Full migration**: If webbed handles all complexity
4. **Keep Java**: If webbed insufficient

### 8.6 Expected Benefits (if successful)

- Eliminate ~500+ lines of XBRL parsing Java code
- Native DuckDB XML processing (potentially faster)
- Declarative extraction via SQL expressions
- Consistent with overall DuckDB-native architecture
- Simpler maintenance (SQL vs Java)

### 8.7 Risk Assessment

**Risk Level**: Medium-High
**Why experimental**: XBRL is complex, webbed capabilities unknown for this use case

**Mitigation**: Prototype first, compare thoroughly, keep Java as fallback

---

## Part 9: Implementation Phases

### Phase 1: Generic Expression Columns (Week 1)
**Module**: `file` adapter
**Changes**:
- Add `expression` field to TableColumn class
- Remove `computed` boolean, `embeddingConfig` map
- Update JSON parsing in parseColumns()
- Add unit tests for expression parsing
- Test backward compatibility

**Deliverable**: Expression column infrastructure working

### Phase 2: DuckDB Single Path (Week 1)
**Module**: `govdata` adapter
**Changes**:
- Simplify convertCachedJsonToParquet() - remove dual path logic
- Modify buildConversionSql() to handle expression columns
- Add getDuckDBConnection() with extension loading
- Test with simple expressions (EXTRACT, concatenation)
- Performance benchmark vs old approach

**Deliverable**: Single DuckDB conversion path working

### Phase 3: Quackformers Integration (Week 2)
**Module**: `govdata` adapter
**Changes**:
- Add quackformers to conversion extension loader
- Test embed() function availability and output
- Add embedding expression columns to one schema (econ)
- Create integration tests for embedding generation
- Performance benchmarking: embedding speed, total conversion time
- Compare output with old ONNX approach (verify dimensions match)

**Deliverable**: Native embeddings via quackformers working

### Phase 4: Remove Java Embedding Code (Week 2)
**Module**: `file` + `govdata` adapters
**Changes**:
- Delete all embedding provider classes
- Delete cache infrastructure classes
- Remove ONNX dependencies from build.gradle
- Delete embedding generation methods from AbstractGovDataDownloader
- Update all schemas to remove embeddingCache config
- Run full test suite to ensure nothing broken

**Deliverable**: Java embedding code eliminated, JAR 100MB smaller

### Phase 5: VSS Query Optimization (Week 3)
**Module**: `file` adapter (DuckDB JDBC)
**Changes**:
- Load vss extension in DuckDBJdbcSchemaFactory
- Document HNSW index creation patterns
- Create example queries with/without indexes
- Test query performance improvement (should be 10-100x)
- Consider query rewrite mechanism (future enhancement)

**Deliverable**: VSS available for fast similarity queries

### Phase 6: Other Conversion Extensions (Week 3)
**Module**: `govdata` adapter
**Changes**:
- Add spatial, h3, fts, excel to conversion extension loader
- Test each extension individually
- Create documentation with usage examples
- Consider replacing ShapefileToParquetConverter with spatial extension
- Update relevant schemas to use h3, spatial expressions

**Deliverable**: Full extension suite available

### Phase 7: XBRL Experiment (Week 4) - EXPERIMENTAL
**Module**: `govdata` adapter (SEC schema)
**Changes**:
- Research webbed extension capabilities and limitations
- Install and test with sample XBRL files
- Prototype extraction of key financial concepts
- Compare accuracy and performance with SecXbrlParser
- Document findings and recommendation
- Decision: adopt, hybrid, or keep Java

**Deliverable**: XBRL feasibility assessment, potential path forward

---

## Part 10: Testing Strategy

### 10.1 Unit Tests

**File adapter** (`file/src/test/java/org/apache/calcite/adapter/file/partition/TableColumnTest.java`):
```java
@Test
void testExpressionColumn() {
  TableColumn col = new TableColumn(
    "year", "integer", true, null, "EXTRACT(YEAR FROM date)");
  assertTrue(col.hasExpression());
  assertTrue(col.isComputed());
  assertEquals("EXTRACT(YEAR FROM date)", col.getExpression());
}

@Test
void testRegularColumn() {
  TableColumn col = new TableColumn("name", "varchar", true, null, null);
  assertFalse(col.hasExpression());
  assertFalse(col.isComputed());
}
```

**SQL generation** (`govdata/src/test/java/org/apache/calcite/adapter/govdata/SqlGenerationTest.java`):
```java
@Test
void testBuildSqlWithExpressions() {
  List<TableColumn> columns = List.of(
    new TableColumn("id", "varchar", true, null, null),
    new TableColumn("year", "integer", true, null, "EXTRACT(YEAR FROM date)"),
    new TableColumn("emb", "array<double>", true, null, "embed(text)::FLOAT[384]")
  );

  String sql = buildConversionSql(columns, null, "in.json", "out.parquet");

  assertThat(sql).contains("CAST(id AS VARCHAR) AS id");
  assertThat(sql).contains("(EXTRACT(YEAR FROM date)) AS year");
  assertThat(sql).contains("(embed(text)::FLOAT[384]) AS emb");
}
```

### 10.2 Integration Tests

**Extension availability** (`govdata/src/test/java/org/apache/calcite/adapter/govdata/ExtensionTest.java`):
```java
@Test
@Tag("integration")
void testQuackformersAvailable() {
  try (Connection conn = getDuckDBConnection()) {
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT embed('test sentence')::FLOAT[384] as emb"
    );
    rs.next();
    Object emb = rs.getObject("emb");
    assertNotNull(emb);
    // Verify 384 dimensions
  }
}

@Test
@Tag("integration")
void testSpatialExtensionAvailable() {
  try (Connection conn = getDuckDBConnection()) {
    conn.createStatement().execute("SELECT ST_Point(0, 0) as pt");
  }
}
```

**End-to-end conversion**:
```java
@Test
@Tag("integration")
void testEndToEndWithExpressions() {
  // Create test JSON with sample data
  // Define schema with expression columns
  // Run conversion via buildConversionSql()
  // Verify parquet has all columns including computed ones
  // Read parquet and verify computed values are correct
}
```

### 10.3 Performance Tests

**Benchmark old vs new**:
```java
@Test
@Tag("performance")
void benchmarkConversionApproaches() {
  // Test with 100k records
  // Measure: old dual path vs new single DuckDB path
  // Expect: 8-50% speedup (eliminate marshaling overhead)
}
```

**VSS query performance**:
```java
@Test
@Tag("performance")
void benchmarkVssHnswIndex() {
  // Load data with embeddings
  // Query without index (brute force)
  // Create HNSW index
  // Query with index
  // Expect: 10-100x speedup
}
```

### 10.4 XBRL Experiment Tests

```java
@Test
@Tag("experimental")
void testWebbedXbrlExtraction() {
  // Load sample 10-K XBRL
  // Extract facts using xpath() expressions
  // Compare with SecXbrlParser output
  // Verify accuracy
}
```

---

## Part 11: Success Criteria

### Core Functionality
- ✅ Expression columns work for any SQL expression
- ✅ Single DuckDB conversion path (dual path eliminated)
- ✅ Embeddings generated via quackformers
- ✅ Extensions loaded successfully (quackformers, spatial, h3, fts, excel, vss)
- ✅ VSS HNSW indexes accelerate similarity queries

### Code Quality
- ✅ Remove ~2,000 lines (embedding + cache infrastructure)
- ✅ Simplify TableColumn class (~200 lines simpler)
- ✅ Eliminate dual conversion logic
- ✅ JAR size reduced by ~100MB (no ONNX Runtime)
- ⚠️ Potentially remove ~500+ lines (if webbed works for XBRL)

### Performance
- ✅ 8-50% faster conversion (eliminate data transfer overhead)
- ✅ 10-100x faster similarity queries (VSS HNSW indexes)
- ✅ Lower memory usage (no Java object allocation for rows)
- ✅ Simpler error handling (fewer code paths)

### Architecture
- ✅ Expression columns = generic file adapter feature (not govdata-specific)
- ✅ Zero Java data processing during conversion
- ✅ All transformations via SQL expressions
- ✅ Clear separation: conversion extensions vs query extensions
- ✅ Data stays in DuckDB (columnar format throughout)

### Documentation
- ✅ Expression column examples for common patterns
- ✅ Extension usage documentation
- ✅ Query optimization patterns (VSS indexes)
- ✅ Best practices: when to use embeddings vs indexes
- ⚠️ XBRL findings documented (if experiment conducted)

---

## Part 12: Benefits Summary

### Architectural Benefits
- **Generic expression columns**: Reusable by any adapter (not just govdata)
- **Single code path**: Eliminate dual-path complexity and maintenance burden
- **DuckDB-native**: Computation where data lives (no expensive transfers)
- **Declarative**: SQL expressions in schema (easier to understand/modify)
- **Separation of concerns**: Conversion vs query extensions clearly separated

### Performance Benefits
- **Conversion speed**: 8-50% faster (eliminate marshaling overhead)
- **Query speed**: 10-100x faster similarity search (HNSW indexes)
- **Memory efficiency**: No Java heap pressure from row objects
- **Scalability**: DuckDB's vectorized execution handles large datasets

### Code Quality Benefits
- **Lines removed**: ~2,000 (embeddings/cache) + potentially ~500 (XBRL)
- **Complexity reduced**: Single conversion path vs dual path
- **Dependencies removed**: No ONNX Runtime, DJL (~100MB JAR reduction)
- **Maintainability**: SQL expressions easier than Java row processing

### Capability Benefits
- **Flexible expressions**: Any SQL, not limited to embeddings
- **Rich functions**: Date/time, string, numeric, window functions, geospatial
- **Extension ecosystem**: Leverage DuckDB community extensions
- **Future-proof**: As DuckDB adds features, automatically available

### Operational Benefits
- **Simpler deployment**: Fewer dependencies, smaller JAR
- **Easier debugging**: SQL expressions vs Java stack traces
- **Better error messages**: DuckDB SQL errors vs Java exceptions
- **Graceful degradation**: Extensions load with warnings, not failures

---

## Part 13: Risk Assessment & Mitigation

### Phase 1-2: Expression Columns & Single Path
**Risk**: Low
**Rationale**: Additive changes, backward compatible
**Mitigation**: Phased rollout, keep old code initially

### Phase 3-4: Quackformers & Remove Java Code
**Risk**: Medium
**Rationale**: Quackformers performance unproven, no rollback after deletion
**Mitigation**:
- Benchmark thoroughly in Phase 3
- Only delete Java code after validation
- Keep git history for emergency rollback

### Phase 5: VSS Query Optimization
**Risk**: Low
**Rationale**: Additive feature, doesn't break existing queries
**Mitigation**: Load with warnings, graceful degradation

### Phase 6: Other Extensions
**Risk**: Low
**Rationale**: Optional features, not critical path
**Mitigation**: Individual testing, fallback to Java if needed

### Phase 7: XBRL Experiment
**Risk**: High (experimental)
**Rationale**: XBRL complexity unknown, webbed capabilities unknown
**Mitigation**:
- Clearly marked as experimental
- Keep Java parser as fallback
- Thorough comparison before any adoption
- Hybrid approach if full migration not feasible

---

## Part 14: Future Enhancements

### Query Rewriting
- Detect Calcite `COSINE_SIMILARITY()` calls
- Rewrite to DuckDB `array_distance()` with HNSW indexes
- Automatic optimization without user SQL changes

### Automatic Index Creation
- Detect embedding columns in schemas
- Auto-create HNSW indexes on table registration
- Configure index parameters via schema JSON

### Batch Embedding Optimization
- If quackformers adds batch support, leverage it
- Current row-by-row is acceptable but not optimal

### More DuckDB Extensions
- **delta**: ACID transactions, time travel for data lakes
- **postgres_scanner**: Query PostgreSQL databases directly
- **mysql_scanner**: Query MySQL databases directly
- **stochastic**: Statistical functions for econometric analysis

### Cross-Adapter Expression Columns
- Apply to splunk adapter
- Apply to sharepoint adapter
- Any adapter using PartitionedTableConfig

---

## Part 15: Documentation Locations

### Code Documentation
- `CLAUDE.md` - Add section on expression columns and extensions
- `CLAUDE-ADAPTERS.md` - Update govdata adapter patterns
- Inline javadoc for TableColumn.expression field

### User Documentation
- Create `govdata/EXPRESSION_COLUMNS.md` - Examples and patterns
- Create `govdata/EMBEDDING_GUIDELINES.md` - When to use embeddings
- Create `govdata/DUCKDB_EXTENSIONS.md` - Extension reference

### Architecture Documentation
- Update `ARCHITECTURE_SUMMARY.txt` - Reflect DuckDB-native approach
- Create diagram showing conversion vs query separation
- Document extension loading patterns

---

## Appendix A: Key File Locations

### File Adapter (Generic)
- `file/src/main/java/org/apache/calcite/adapter/file/partition/PartitionedTableConfig.java`
  - Lines 177-279: TableColumn class (ADD expression field)
  - Line 409: parseColumns() (UPDATE to parse expression)

- `file/src/main/java/org/apache/calcite/adapter/file/duckdb/DuckDBJdbcSchemaFactory.java`
  - Line 334: createInternal() (ADD loadQueryExtensions())
  - NEW: loadQueryExtensions() method (vss, fts)

### GovData Adapter (Conversion)
- `govdata/src/main/java/org/apache/calcite/adapter/govdata/AbstractGovDataDownloader.java`
  - Lines 1641-1695: buildConversionSql() (MODIFY for expressions)
  - Lines 1835-1958: convertCachedJsonToParquet() (SIMPLIFY to single path)
  - Lines 1998-2126: DELETE embedding methods
  - Lines 267-310: DELETE getEmbeddingProvider()
  - NEW: getDuckDBConnection() method
  - NEW: loadConversionExtensions() method

### Files to Delete
- `file/src/main/java/org/apache/calcite/adapter/file/similarity/`
  - ONNXEmbeddingProvider.java
  - OpenAIEmbeddingProvider.java
  - LocalEmbeddingProvider.java
  - TextEmbeddingProvider.java
  - EmbeddingProviderFactory.java
  - PersistentEmbeddingCache.java
  - CachedEmbeddingProvider.java

### Build Configuration
- `build.gradle` - Remove ONNX Runtime dependencies

---

## Appendix B: Extension Reference

### Conversion Extensions (Loaded in ephemeral DuckDB connection)

| Extension | Installation | Purpose |
|-----------|--------------|---------|
| quackformers | `FROM community` | Generate embeddings: `embed(text)::FLOAT[384]` |
| spatial | `(core)` | GIS: `ST_Read()`, `ST_Area()`, etc. |
| h3 | `FROM community` | Geospatial hex: `h3_latlng_to_cell()` |
| excel | `(core)` | Read Excel: `read_excel('file.xlsx')` |
| fts | `(core)` | Full-text search indexing |

### Query Extensions (Loaded in persistent DuckDB catalog)

| Extension | Installation | Purpose |
|-----------|--------------|---------|
| vss | `(core)` | Vector similarity: HNSW indexes, `array_distance()` |
| fts | `(core)` | Full-text search: keyword queries, BM25 ranking |

### Experimental Extensions

| Extension | Installation | Purpose |
|-----------|--------------|---------|
| webbed | `FROM community` | Web/XML parsing: `xpath()`, CSS selectors |

---

## Appendix C: Migration Checklist

### Pre-Implementation
- [ ] Review plan with team
- [ ] Identify test datasets for validation
- [ ] Set up performance benchmarking framework
- [ ] Create rollback strategy

### Phase 1: Expression Columns
- [ ] Modify TableColumn class
- [ ] Update JSON parsing
- [ ] Add unit tests
- [ ] Update documentation

### Phase 2: Single DuckDB Path
- [ ] Simplify conversion logic
- [ ] Modify SQL generation
- [ ] Add extension loading
- [ ] Test with existing schemas

### Phase 3: Quackformers
- [ ] Test quackformers installation
- [ ] Validate embed() output (384 dimensions)
- [ ] Performance benchmark
- [ ] Update one schema (econ)

### Phase 4: Cleanup
- [ ] Delete embedding provider classes
- [ ] Delete cache infrastructure
- [ ] Remove dependencies from build.gradle
- [ ] Run full test suite

### Phase 5: VSS
- [ ] Load vss in DuckDBJdbcSchemaFactory
- [ ] Create HNSW indexes
- [ ] Benchmark query performance
- [ ] Document usage patterns

### Phase 6: Other Extensions
- [ ] Test spatial, h3, fts, excel
- [ ] Update documentation
- [ ] Consider ShapefileToParquetConverter replacement

### Phase 7: XBRL Experiment
- [ ] Research webbed capabilities
- [ ] Prototype extraction
- [ ] Compare with Java parser
- [ ] Document findings
- [ ] Make decision

---

**End of Plan**

*This is a living document. Update as implementation progresses.*
