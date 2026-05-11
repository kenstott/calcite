# Document ETL Mode

Document ETL mode is for sources where a single canonical schema should be populated from many documents or API responses. You define the target schema in advance; a `ResponseTransformer` normalizes each source into it. Results materialize into Iceberg.

This is the right mode when:
- Many heterogeneous sources must produce one unified table
- The source response shape doesn't directly map to the table you want
- You need custom normalization, unit conversion, field derivation, or filtering logic

For sources where the data structure maps naturally to rows without custom logic, use [Tabular Mode](tabular-mode.md) instead.

---

## The Core Pattern

```
HTTP endpoint(s) or files
        ↓
  ResponseTransformer        ← your code: parse response → JSON array
        ↓
  Canonical schema           ← defined in model.json
        ↓
  Iceberg table on S3
```

The transformer receives the raw response and a `RequestContext` (URL, headers, dimension values). It returns a JSON array of objects where each object's keys match the target table's column names.

---

## Defining a Canonical Schema

Columns are declared explicitly in the table config:

```json
{
  "name": "financial_facts",
  "url": "https://data.sec.gov/api/xbrl/companyfacts/CIK${cik}.json",
  "dimensions": {
    "cik": ["0000320193", "0000789019", "0001018724"]
  },
  "columns": [
    {"name": "cik",          "type": "VARCHAR"},
    {"name": "entity_name",  "type": "VARCHAR"},
    {"name": "concept",      "type": "VARCHAR"},
    {"name": "label",        "type": "VARCHAR"},
    {"name": "value",        "type": "DOUBLE"},
    {"name": "unit",         "type": "VARCHAR"},
    {"name": "period_start", "type": "DATE"},
    {"name": "period_end",   "type": "DATE"},
    {"name": "form",         "type": "VARCHAR"},
    {"name": "filed",        "type": "DATE"}
  ],
  "responseTransformer": "org.example.FinancialFactsTransformer",
  "primaryKey": ["cik", "concept", "period_end", "form"]
}
```

The `dimensions` block drives multiple API calls — one per CIK value. Each call's response is passed through the transformer and appended to the same table.

---

## Writing a ResponseTransformer

Implement `org.apache.calcite.adapter.file.etl.ResponseTransformer`:

```java
public interface ResponseTransformer {
  String transform(String response, RequestContext context);
}
```

The transformer receives:
- `response` — raw HTTP response body (or file contents) as a string
- `context` — URL, headers, dimension values for this specific call

It must return a JSON array string (`"[{...}, {...}]"`) where each object's keys match the canonical schema columns. Return `"[]"` for empty or error responses.

### Example: SEC Company Facts

```java
public class FinancialFactsTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      return "[]";
    }
    try {
      JsonNode root = MAPPER.readTree(response);
      String cik = context.getDimensionValues().get("cik");
      String entityName = root.path("entityName").asText();
      ArrayNode out = MAPPER.createArrayNode();

      JsonNode facts = root.path("facts");
      facts.fields().forEachRemaining(taxonomy -> {
        taxonomy.getValue().fields().forEachRemaining(concept -> {
          String conceptName = concept.getKey();
          JsonNode units = concept.getValue().path("units");
          units.fields().forEachRemaining(unit -> {
            for (JsonNode entry : unit.getValue()) {
              ObjectNode row = MAPPER.createObjectNode();
              row.put("cik", cik);
              row.put("entity_name", entityName);
              row.put("concept", conceptName);
              row.put("label", concept.getValue().path("label").asText());
              row.put("value", entry.path("val").asDouble());
              row.put("unit", unit.getKey());
              row.put("period_end", entry.path("end").asText());
              row.put("period_start", entry.path("start").asText(""));
              row.put("form", entry.path("form").asText());
              row.put("filed", entry.path("filed").asText());
              out.add(row);
            }
          });
        });
      });

      return out.toString();
    } catch (Exception e) {
      return "[]";
    }
  }
}
```

### Using RequestContext

```java
// Dimension values injected by the adapter (from the "dimensions" config)
String year = context.getDimensionValues().get("year");
String region = context.getDimensionValues().get("region");

// The URL that was fetched
String url = context.getUrl();

// Request headers
Map<String, String> headers = context.getHeaders();
```

Dimension values let you distinguish which call produced the response — useful when the response itself doesn't include the dimension (e.g., the year isn't in the response body, only in the URL).

---

## Multi-Dimension Expansion

Declare any number of dimensions and the adapter generates the full cartesian product — one API call (or file fetch) per combination. There is no limit on the number of dimensions.

```json
{
  "name": "naep_scores",
  "url": "https://www.nationsreportcard.gov/api/data/GetAdhocData",
  "dimensions": {
    "subscale": ["MRPCM", "RRPCM"],
    "grade":    ["4", "8"],
    "year":     ["2019", "2022", "2024"]
  },
  "storageConfig": {
    "params": {
      "type":      "State",
      "subject":   "${subscale}",
      "grade":     "${grade}",
      "year":      "${year}",
      "stattype":  "MN:MN"
    }
  },
  "responseTransformer": "org.example.NaepScoresTransformer"
}
```

This generates 2 × 2 × 3 = 12 API calls. Each response is transformed and appended to `naep_scores`. The transformer receives the dimension values in `context.getDimensionValues()` to disambiguate.

---

## Dimension Types

Each dimension can use one of six types. The shorthand list syntax above is `list` — use the structured form for anything else.

### list — Explicit values

```json
"dimensions": {
  "region": { "type": "list", "values": ["NORTH", "SOUTH", "EAST", "WEST"] }
}
```

Shorthand: a bare JSON array is equivalent to `type: list`.

### range — Numeric sequence

```json
"dimensions": {
  "quarter": { "type": "range", "start": 1, "end": 4 },
  "year":    { "type": "range", "start": 2010, "end": 2024, "step": 2 }
}
```

Generates all integers from `start` to `end` inclusive, advancing by `step` (default 1).

### yearRange — Year sequence with publication lag

Designed for datasets where the most recent year is not yet published:

```json
"dimensions": {
  "year": {
    "type":          "yearRange",
    "start":         2010,
    "end":           "current",
    "dataLag":       1,
    "releaseMonth":  9,
    "excludeYears":  [2011],
    "minYear":       2005,
    "maxYear":       2030
  }
}
```

| Option | Description |
|--------|-------------|
| `end: current` | Resolves to the current calendar year at runtime |
| `dataLag` | Subtract this many years from `end` (data not yet available) |
| `releaseMonth` | If today is before this month, add one extra year of lag (data released mid-year) |
| `excludeYears` | Skip specific years entirely |
| `minYear` / `maxYear` | Hard bounds — batches outside this range are skipped |

Example with `dataLag: 1` and `releaseMonth: 9`: before September the end year is `current - 2`; from September onward it is `current - 1`.

### query — SQL-driven values

Fetch dimension values from any table already in the schema at runtime:

```json
"dimensions": {
  "station_id": {
    "type": "query",
    "sql":  "SELECT DISTINCT station_id FROM stations WHERE active = true"
  }
}
```

Useful when the set of valid values is itself stored in the lake rather than known statically.

### json_catalog — Values from a classpath JSON file

```json
"dimensions": {
  "country": {
    "type":   "json_catalog",
    "source": "/worldbank/worldbank-countries.json",
    "path":   "countryGroups.G20.countries"
  }
}
```

`path` supports dot notation and `[*]` for array iteration:
- `countryGroups.G20.countries` — direct path to an array
- `indicators[*].items[*].code` — flatten nested arrays

### custom — Programmatic resolution

For dimension values that cannot be expressed as a static list, range, SQL query, or catalog file — use a `DimensionResolver`. The resolver is called once per expansion step and **receives the current values of all already-resolved dimensions as context**. This means a dimension's valid values can depend on the values chosen for earlier dimensions.

```json
{
  "name": "bea_regional",
  "url":  "https://apps.bea.gov/api/data",
  "dimensions": {
    "tablename": ["SAINC1", "SAINC30", "SAINC5H"],
    "line_code":  { "type": "custom" }
  },
  "hooks": {
    "dimensionResolver": "org.example.BeaDimensionResolver"
  }
}
```

`line_code` is declared `custom`. For every value of `tablename` the adapter calls the resolver, passing the current `tablename` value in context, and gets back the set of valid line codes for that table. The resulting expansion covers every valid `(tablename, line_code)` pair — not the full cartesian product of all line codes against all tables.

```java
public class BeaDimensionResolver implements DimensionResolver {

  @Override
  public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {

    if ("line_code".equals(dimensionName)) {
      String tablename = context.get("tablename");  // already resolved
      return fetchValidLineCodesFor(tablename);      // only valid combinations
    }
    return Collections.emptyList();
  }
}
```

The `context` map contains every dimension that was resolved before this one in declaration order. Declare dimensions in dependency order — a dimension that depends on another must come after it.

Wire the resolver in the `hooks` block:

```json
"hooks": {
  "dimensionResolver": "com.example.BeaDimensionResolver"
}
```

The resolver class must be on the classpath. The same class handles all `custom` dimensions in the table — use `dimensionName` to dispatch to the right logic.

---

## Pipeline Tracking and Resumability

For large multi-dimension jobs (hundreds or thousands of API calls), the adapter tracks completion state per batch in S3. If a run is interrupted, the next run skips already-completed batches and retries failed ones.

Tracker files live under `${CALCITE_TRACKER_S3_BUCKET}/year=*/source_key=<table-name>/`. A batch is retried unless its state is `complete` — error-state batches automatically re-run on the next execution.

Configure the tracker backend:

```bash
CALCITE_TRACKER_BACKEND=s3
CALCITE_TRACKER_S3_BUCKET=s3://my-tracker-bucket
```

---

## Pagination

For APIs that paginate responses, configure the pagination strategy on the HTTP transport:

```json
{
  "storageConfig": {
    "pagination": {
      "type":        "cursor",
      "cursorPath":  "$.next_cursor",
      "cursorParam": "cursor",
      "pageSize":    500
    }
  }
}
```

Supported pagination types: `offset`, `cursor`, `page`, `csv_stream`. The adapter streams pages through the transformer one at a time — results are written to Iceberg incrementally, not buffered in memory. This prevents OOM on large paginated datasets.

---

## Hooks

The `hooks` block wires extension points into the ETL pipeline. All hooks are optional and compose — you can use any combination.

```json
{
  "hooks": {
    "responseTransformer":    "com.example.MyResponseTransformer",
    "rowTransformers":        [{ "type": "class", "class": "com.example.MyRowTransformer" }],
    "validators":             [{ "type": "expression", "condition": "value IS NOT NULL", "action": "drop" }],
    "dimensionResolver":      "com.example.MyDimensionResolver",
    "dataProvider":           "com.example.MyDataProvider",
    "variableNormalizer":     "com.example.MyVariableNormalizer",
    "tableLifecycleListener": "com.example.MyLifecycleListener",
    "enabled":                true
  }
}
```

### responseTransformer

Documented in [The Core Pattern](#the-core-pattern) above. Receives the raw HTTP response and returns a JSON array matching the canonical schema.

### rowTransformers

Called once per row after `responseTransformer`, before writing to Iceberg. Two forms:

**Class-based** — for lookups, stateful logic, or anything not expressible as SQL:

```json
"rowTransformers": [{ "type": "class", "class": "com.example.CensusGeoEnricher" }]
```

```java
public class CensusGeoEnricher implements RowTransformer {
  public Map<String, Object> transform(Map<String, Object> row, RowContext context) {
    String fips = (String) row.get("geo_fips");
    GeoMetadata meta = geoLookup.get(fips);
    if (meta != null) {
      row.put("census_region", meta.getRegion());
    }
    return row;  // return null to drop the row
  }
}
```

**Expression-based** — inline SQL expression applied to a single column, no code required:

```json
"rowTransformers": [
  { "type": "expression", "column": "data_value", "expression": "REPLACE(data_value, '(NA)', '')" }
]
```

Multiple transformers run in declaration order. A class-based transformer can drop a row by returning `null`.

### validators

Called after all transformers. Two forms, same as `rowTransformers`:

**Expression-based** — most common:

```json
"validators": [
  { "type": "expression", "condition": "geo_fips IS NOT NULL", "action": "drop" },
  { "type": "expression", "condition": "value >= 0",           "action": "warn" }
]
```

**Class-based** — for complex multi-field rules:

```java
public class DataValidator implements Validator {
  public ValidationResult validate(Map<String, Object> row) {
    if (row.get("geo_fips") == null) {
      return ValidationResult.drop("Missing geo_fips");
    }
    return ValidationResult.valid();
  }
}
```

Available actions: `drop` (silently exclude row), `warn` (log and include), `fail` (abort pipeline).

### dataProvider

Replaces the built-in HTTP fetcher entirely. Use when the source is not a simple HTTP endpoint — FTP, a database, a message queue, an API with batching requirements:

```java
public class BlsDataProvider implements DataProvider {
  public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    // BLS API accepts up to 50 series IDs per request — batch them here
    List<String> seriesIds = buildSeriesIds(variables);
    return blsClient.fetchBatch(seriesIds);
  }
}
```

```json
"hooks": { "dataProvider": "com.example.BlsDataProvider" }
```

When `dataProvider` returns non-null the built-in `HttpSource` is skipped entirely for that batch.

### variableNormalizer

Many APIs change field names across versions or years. `VariableNormalizer` maps API-specific names to stable conceptual names so column expressions in your schema don't need to change when the upstream API changes:

```java
public class CensusVariableNormalizer implements VariableNormalizer {
  public String normalize(String apiVariable, Map<String, String> context) {
    int year = Integer.parseInt(context.getOrDefault("year", "2020"));
    // B01001_001E (ACS) and P1_001N (Decennial 2020) both → total_population
    return ConceptualVariableMapper.getInstance()
        .findConceptualNameForVariable(apiVariable, year);
  }
}
```

```json
"hooks": { "variableNormalizer": "com.example.CensusVariableNormalizer" }
```

Geographic columns (`state`, `county`, `tract`, any field containing `fips` or `geoid`) are preserved as-is by default. Override `shouldPreserve()` to change this.

### tableLifecycleListener

Full visibility into the pipeline lifecycle with optional override of fetch and write:

```
beforeTable → resolveDimensions → [per batch: beforeSource → fetchData → afterSource
             → beforeMaterialize → writeData → afterMaterialize] → afterTable
```

```java
public class AuditListener implements TableLifecycleListener {

  public void beforeTable(TableContext context) {
    log.info("Starting {}", context.getTableName());
  }

  public void afterTable(TableContext context, EtlResult result) {
    metrics.record(context.getTableName(), result.getRowCount());
  }

  public boolean onTableError(TableContext context, Exception error) {
    alerting.send(error);
    return true; // continue with other tables
  }
}
```

Two hooks fully replace the built-in infrastructure when they return non-null/-1:

- `fetchData(context, variables)` — return an `Iterator<Map<String,Object>>` to skip `HttpSource`
- `writeData(context, data, variables)` — return row count to skip `IcebergWriter` (e.g., write to Kafka instead)

`resolveDimensions(context, staticDimensions)` — override or augment the dimensions declared in config at runtime.

```json
"hooks": { "tableLifecycleListener": "com.example.AuditListener" }
```

### enabled

Set `false` to disable a table without removing it from config. Disabled tables are skipped during ETL and excluded from schema metadata:

```json
"hooks": { "enabled": false }
```

### Error Handling

Configure what happens when a hook throws:

```json
"hooks": {
  "errorHandling": {
    "responseTransformer": "fail",
    "rowTransformer":      "skip_row",
    "validator":           "continue"
  }
}
```

| Value | Behaviour |
|-------|-----------|
| `fail` | Abort the pipeline (default for `responseTransformer`) |
| `skip_row` | Drop the offending row and continue (default for `rowTransformer`) |
| `continue` | Log and include the row (default for `validator`) |

---

## Schema-Level Hooks

The hooks described above are all **table-level** — configured per table in the `hooks` block. A separate **schema-level** `hooks` block sits above the table list and applies to the schema as a whole:

```yaml
schema:
  name: econ
  hooks:
    schemaLifecycleListener: "org.example.EconSchemaListener"
    tableLifecycleListener:  "org.example.EconTableListener"

  tables:
    - name: gdp
      hooks:
        responseTransformer: "org.example.BeaTransformer"
```

### schemaLifecycleListener

Implement `SchemaLifecycleListener` to run logic around the entire schema run — before any table starts, after all tables finish, and on failure. Also the extension point for custom bulk-download logic.

```java
public class EconSchemaListener implements SchemaLifecycleListener {

  @Override
  public void beforeSchema(SchemaContext ctx) throws Exception {
    // authenticate, open shared connections, validate config
  }

  @Override
  public void afterSchema(SchemaContext ctx, SchemaResult result) {
    // send summary notification, flush caches, close shared resources
  }

  @Override
  public void onSchemaError(SchemaContext ctx, Exception error) {
    // alert, rollback, cleanup
  }

  // Optional: override to supply custom download logic for non-HTTP sources.
  // Return null to fall through to the built-in HTTP downloader.
  @Override
  public String downloadBulkFile(SchemaContext ctx, BulkDownloadConfig cfg,
      Map<String, String> vars, String targetPath) throws Exception {
    if (cfg.resolveUrl(vars).startsWith("sharepoint://")) {
      return sharePointClient.downloadToPath(cfg.resolveUrl(vars), targetPath);
    }
    return null;
  }
}
```

Lifecycle order:

```
beforeSchema()
  [bulk download phase: downloadBulkFile() per declared bulk file]
  [for each table: TableLifecycleListener events]
afterSchema()  — OR —  onSchemaError()
```

### tableLifecycleListener (schema-level default)

Set `tableLifecycleListener` in the schema-level `hooks` block to apply one listener to every table in the schema without repeating it per table. A table's own `hooks.tableLifecycleListener` overrides this default for that table only.

```yaml
schema:
  hooks:
    tableLifecycleListener: "org.example.DefaultTableListener"
  tables:
    - name: gdp          # uses DefaultTableListener
    - name: employment   # uses DefaultTableListener
    - name: special
      hooks:
        tableLifecycleListener: "org.example.SpecialTableListener"  # overrides
```

---

## Registering Extension Classes

All hook classes must be on the classpath at runtime. In a Gradle project:

```groovy
dependencies {
  implementation project(':file')
}
```

Reference by fully qualified class name in the `hooks` block. The same classpath rule applies to `responseTransformer`, `dataProvider`, `dimensionResolver`, `variableNormalizer`, and `tableLifecycleListener`.

---

## Built-In Transformers

The govdata module ships transformers for all its public data schemas (SEC, NOAA, NCES, BLS, etc.) as reference implementations. Browse `govdata/src/main/java/org/apache/calcite/adapter/govdata/` for examples covering a wide range of API response shapes, pagination patterns, and normalization strategies.
