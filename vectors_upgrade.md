# Metadata-Driven Semantic Search Implementation Plan

## Overview
Add semantic search capability to govdata adapter using ONNX-based embeddings with persistent cache layer. Vector columns auto-generated during JSON-to-Parquet conversion based on schema metadata. **Cache ensures each unique string is embedded only once, shared across all datasets.**

## Architecture Design

### Cache-First Performance Strategy
- **Key Insight**: Each unique text string is embedded exactly once and cached permanently
- **First Run**: 100k unique strings = 8-33 minutes (one-time cost)
- **Subsequent Runs**: 100k strings = ~5 seconds (cache lookups only)
- **Cross-Dataset Reuse**: "Unemployment Rate" embedded once, reused everywhere
- **Storage**: Parquet-backed cache at `{cacheDir}/embeddings_cache/provider={model}/embeddings.parquet`

### Integration Points
- **Existing Infrastructure**: TextEmbeddingProvider, StorageProvider, DuckDB COSINE_SIMILARITY
- **Model**: sentence-transformers/all-MiniLM-L6-v2 via ONNX Runtime
- **Dimensions**: 384-dimensional embeddings, L2 normalized
- **Storage**: Embeddings stored as `array<double>` in parquet files

---

## Implementation Status

### ✅ Phase 1: Core Infrastructure (COMPLETED)

#### 1. PersistentEmbeddingCache
**File**: `file/src/main/java/org/apache/calcite/adapter/file/similarity/PersistentEmbeddingCache.java`

**Features**:
- SHA-256 hashing of text for cache keys
- In-memory hot cache (ConcurrentHashMap, default 100k entries)
- LRU eviction when memory cache exceeds limit
- Automatic flush after 1000 pending entries
- Thread-safe with ReadWriteLock
- Cache statistics tracking (hits, misses, hit rate)

**Parquet Schema**:
```
text_hash (VARCHAR)          - SHA-256 hash of text
text (VARCHAR)               - First 1000 chars (for debugging)
embedding (DOUBLE[])         - 384-dimensional vector
provider_model (VARCHAR)     - e.g., "onnx-sentence-transformers/all-MiniLM-L6-v2"
created_timestamp (BIGINT)   - Creation timestamp
```

**Key Methods**:
- `get(String text)` - Retrieve cached embedding
- `put(String text, double[] embedding)` - Store embedding
- `flushToStorage()` - Persist pending entries to parquet
- `getStats()` - Cache statistics

#### 2. CachedEmbeddingProvider
**File**: `file/src/main/java/org/apache/calcite/adapter/file/similarity/CachedEmbeddingProvider.java`

**Features**:
- Wraps any TextEmbeddingProvider with caching
- Check cache first, delegate on miss
- Batch processing optimization (separates hits/misses)
- Automatic cache flush on close()
- Exposes cache statistics

**Performance**:
- Cache hit: < 1ms (memory lookup)
- Cache miss: 5-20ms (ONNX inference + cache store)
- Batch processing: Generates only cache misses

#### 3. ONNXEmbeddingProvider
**File**: `file/src/main/java/org/apache/calcite/adapter/file/similarity/ONNXEmbeddingProvider.java`

**Features**:
- ONNX Runtime for model inference
- DJL HuggingFace tokenizers for text preprocessing
- Mean pooling over token embeddings
- L2 normalization for cosine similarity
- Configurable model/tokenizer paths
- Environment variable support (ONNX_MODEL_PATH, ONNX_TOKENIZER_PATH)

**Model Loading Priority**:
1. Explicit config (`modelPath`, `tokenizerPath`)
2. Environment variables (`ONNX_MODEL_PATH`, `ONNX_TOKENIZER_PATH`)
3. Classpath resources (`/models/all-MiniLM-L6-v2/model.onnx`)
4. Default filesystem path (`file/src/main/resources/models/all-MiniLM-L6-v2/`)

**Configuration**:
```java
Map<String, Object> config = Map.of(
    "dimensions", 384,
    "maxSequenceLength", 128,
    "maxInputLength", 8192,
    "modelPath", "/path/to/model.onnx",
    "tokenizerPath", "/path/to/tokenizer.json"
);
```

#### 4. StorageProvider Extensions
**File**: `file/src/main/java/org/apache/calcite/adapter/file/storage/StorageProvider.java`

**New Methods**:
- `readParquet(String path)` - Read parquet file to List<Map<String, Object>>
- `appendParquet(String path, List<Map>, List<schema>)` - Append/create parquet file

**Features**:
- Converts Avro GenericRecords to Maps
- Handles `double[]` <-> Avro array conversion
- Works with remote storage (downloads to temp file first)
- Supports `array<double>` type in schema

#### 5. EmbeddingProviderFactory Updates
**File**: `file/src/main/java/org/apache/calcite/adapter/file/similarity/EmbeddingProviderFactory.java`

**New Provider Type**: `"onnx"`

**Auto-Caching Logic**:
```java
// If cachePath and storageProvider are in config, wrap with cache
if (config.containsKey("cachePath") && config.containsKey("storageProvider")) {
    PersistentEmbeddingCache cache = new PersistentEmbeddingCache(...);
    return new CachedEmbeddingProvider(baseProvider, cache);
}
```

#### 6. Dependencies
**File**: `file/build.gradle.kts`

**Added**:
```kotlin
implementation("com.microsoft.onnxruntime:onnxruntime:1.16.3")
implementation("ai.djl:api:0.25.0")
implementation("ai.djl.huggingface:tokenizers:0.25.0")
```

---

## 🔄 Phase 2: ONNX Model Assets (PENDING)

### Download Model Files

**Model**: sentence-transformers/all-MiniLM-L6-v2

**Option A - Pre-converted ONNX (Recommended)**:
```bash
# Download from HuggingFace or Optimum
# Place at: file/src/main/resources/models/all-MiniLM-L6-v2/
#   - model.onnx (~90MB)
#   - tokenizer.json (~500KB)
```

**Option B - Convert from PyTorch**:
```bash
pip install optimum[onnxruntime]

optimum-cli export onnx \
  --model sentence-transformers/all-MiniLM-L6-v2 \
  --task feature-extraction \
  file/src/main/resources/models/all-MiniLM-L6-v2/
```

**Files to Create**:
- `file/src/main/resources/models/all-MiniLM-L6-v2/model.onnx`
- `file/src/main/resources/models/all-MiniLM-L6-v2/tokenizer.json`

**Git LFS** (if model > 100MB):
```bash
# .gitattributes
*.onnx filter=lfs diff=lfs merge=lfs -text
```

---

## 🔄 Phase 3: Schema JSON Extensions (PENDING)

### Schema-Level Configuration

**File**: `govdata/src/main/resources/econ-schema.json`

**Add to schema root**:
```json
{
  "schemaName": "econ",
  "comment": "U.S. economic indicators...",

  "embeddingCache": {
    "enabled": true,
    "path": "{cacheDir}/embeddings_cache",
    "provider": "onnx",
    "model": "all-MiniLM-L6-v2",
    "dimension": 384
  },

  "partitionedTables": [...]
}
```

### Table-Level Vector Columns

**Add to columns array**:
```json
{
  "name": "employment_statistics",
  "columns": [
    {
      "name": "series_name",
      "type": "string",
      "nullable": true,
      "comment": "Descriptive name of employment series"
    },
    {
      "name": "series_name_embedding",
      "type": "array<double>",
      "nullable": true,
      "computed": true,
      "embeddingConfig": {
        "sourceColumn": "series_name",
        "provider": "onnx"
      },
      "comment": "384-dim vector embedding for semantic search on series_name"
    }
  ]
}
```

**New Column Attributes**:
- `type: "array<double>"` - Vector column type
- `computed: true` - Indicates derived/generated column
- `embeddingConfig.sourceColumn` - Source text column
- `embeddingConfig.provider` - Provider override (optional, defaults to schema-level)

---

## 🔄 Phase 4: Govdata Adapter Integration (PENDING)

### 1. Extend TableColumn Class

**File**: Find/update TableColumn definition (likely in `govdata/src/main/java/org/apache/calcite/adapter/govdata/`)

**Add Fields**:
```java
public class TableColumn {
  private String name;
  private String type;
  private boolean nullable;
  private String comment;

  // NEW: Embedding support
  private boolean computed;
  private Map<String, Object> embeddingConfig;

  // NEW: Methods
  public boolean isComputed() {
    return computed;
  }

  public boolean hasEmbeddingConfig() {
    return embeddingConfig != null && !embeddingConfig.isEmpty();
  }

  public String getEmbeddingSourceColumn() {
    return embeddingConfig != null ?
        (String) embeddingConfig.get("sourceColumn") : null;
  }

  public String getEmbeddingProvider() {
    return embeddingConfig != null ?
        (String) embeddingConfig.getOrDefault("provider", "onnx") : "onnx";
  }

  public boolean isVectorType() {
    return type != null && type.startsWith("array<");
  }
}
```

### 2. Update AbstractGovDataDownloader

**File**: `govdata/src/main/java/org/apache/calcite/adapter/govdata/AbstractGovDataDownloader.java`

**Add Field**:
```java
private TextEmbeddingProvider embeddingProvider; // Lazy-initialized
```

**Add Initialization Method** (after existing fields):
```java
private TextEmbeddingProvider getEmbeddingProvider() throws EmbeddingException {
  if (embeddingProvider == null) {
    // Read embedding config from schema
    Map<String, Object> embeddingCacheConfig =
        (Map<String, Object>) schemaConfig.get("embeddingCache");

    if (embeddingCacheConfig == null ||
        !Boolean.TRUE.equals(embeddingCacheConfig.get("enabled"))) {
      return null; // Embeddings disabled
    }

    // Build provider config
    String provider = (String) embeddingCacheConfig.getOrDefault("provider", "onnx");
    String cachePath = resolveParquetPath(
        (String) embeddingCacheConfig.get("path"),
        new HashMap<>());

    Map<String, Object> config = new HashMap<>();
    config.put("cachePath", cachePath);
    config.put("storageProvider", this.storageProvider);
    config.put("dimensions", embeddingCacheConfig.getOrDefault("dimension", 384));
    config.put("model", embeddingCacheConfig.get("model"));

    embeddingProvider = EmbeddingProviderFactory.createProvider(provider, config);
    logger.info("Initialized embedding provider: {}", embeddingProvider.getProviderName());
  }

  return embeddingProvider;
}
```

**Modify convertJsonRecordToTypedMap()** (around line 1226):
```java
private Map<String, Object> convertJsonRecordToTypedMap(
    Map<String, Object> record, List<TableColumn> columns, String tableName) {

  Map<String, Object> typedRecord = new LinkedHashMap<>();
  Map<String, String> columnTypeMap = buildColumnTypeMap(columns);

  // Existing type conversion for non-computed columns
  for (Map.Entry<String, Object> entry : record.entrySet()) {
    String columnName = entry.getKey();
    if (isComputedColumn(columns, columnName)) {
      continue; // Skip computed columns in source data
    }

    String type = columnTypeMap.get(columnName);
    Object value = entry.getValue();
    Object convertedValue = convertValueByType(value, type);
    typedRecord.put(columnName, convertedValue);
  }

  // NEW: Generate computed columns (embeddings)
  try {
    TextEmbeddingProvider provider = getEmbeddingProvider();
    if (provider != null) {
      generateEmbeddingColumns(typedRecord, columns, provider);
    }
  } catch (EmbeddingException e) {
    logger.warn("Failed to initialize embedding provider for table {}: {}",
                tableName, e.getMessage());
  }

  return typedRecord;
}
```

**Add Helper Methods**:
```java
private void generateEmbeddingColumns(Map<String, Object> record,
    List<TableColumn> columns, TextEmbeddingProvider provider) {

  for (TableColumn column : columns) {
    if (!column.isComputed() || !column.hasEmbeddingConfig()) {
      continue;
    }

    String sourceColumn = column.getEmbeddingSourceColumn();
    Object sourceValue = record.get(sourceColumn);

    if (sourceValue instanceof String) {
      String text = (String) sourceValue;
      try {
        double[] embedding = provider.generateEmbedding(text);
        record.put(column.getName(), embedding);

        if (logger.isDebugEnabled()) {
          logger.debug("Generated embedding for column {} from {}: {} dimensions",
                      column.getName(), sourceColumn, embedding.length);
        }
      } catch (EmbeddingException e) {
        logger.debug("Failed to generate embedding for column {}: {}",
                    column.getName(), e.getMessage());
        record.put(column.getName(), null);
      }
    } else {
      // Source column is null or not a string
      record.put(column.getName(), null);
    }
  }
}

private boolean isComputedColumn(List<TableColumn> columns, String columnName) {
  return columns.stream()
      .anyMatch(c -> c.getName().equals(columnName) && c.isComputed());
}
```

**Add Cleanup**:
```java
public void close() {
  if (embeddingProvider != null) {
    embeddingProvider.close(); // Flushes cache
  }
  // ... existing cleanup
}
```

### 3. Update writeAvroParquet for array<double>

**File**: `file/src/main/java/org/apache/calcite/adapter/file/storage/StorageProvider.java`

**Verify array type handling** (should already work from Phase 1):
```java
// In writeAvroParquet() method around line 230
case "array<double>":
  if (column.isNullable()) {
    fields = fields.name(column.getName()).doc(column.getComment())
        .type().nullable().array().items().doubleType().noDefault();
  } else {
    fields = fields.name(column.getName()).doc(column.getComment())
        .type().array().items().doubleType().noDefault();
  }
  break;
```

---

## 🔄 Phase 5: Testing (PENDING)

### Unit Tests

#### 1. PersistentEmbeddingCacheTest
**File**: `file/src/test/java/org/apache/calcite/adapter/file/similarity/PersistentEmbeddingCacheTest.java`

```java
@Test
void testCachePersistence() throws Exception {
  String cachePath = tempDir + "/test_cache/embeddings.parquet";
  PersistentEmbeddingCache cache = new PersistentEmbeddingCache(
      storageProvider, cachePath, "test-model");

  double[] embedding = new double[384];
  Arrays.fill(embedding, 0.5);

  cache.put("test text", embedding);
  cache.flushToStorage();

  // Create new cache instance - should load from storage
  PersistentEmbeddingCache cache2 = new PersistentEmbeddingCache(
      storageProvider, cachePath, "test-model");

  double[] loaded = cache2.get("test text");
  assertNotNull(loaded);
  assertArrayEquals(embedding, loaded, 0.0001);
}

@Test
void testCacheHitRate() {
  // Test hits/misses tracking
  // Test auto-flush at 1000 entries
  // Test LRU eviction
}
```

#### 2. CachedEmbeddingProviderTest
**File**: `file/src/test/java/org/apache/calcite/adapter/file/similarity/CachedEmbeddingProviderTest.java`

```java
@Test
void testCacheIntegration() throws Exception {
  TextEmbeddingProvider mockProvider = Mockito.mock(ONNXEmbeddingProvider.class);
  when(mockProvider.generateEmbedding(anyString()))
      .thenReturn(new double[384]);

  PersistentEmbeddingCache cache = new PersistentEmbeddingCache(...);
  CachedEmbeddingProvider provider = new CachedEmbeddingProvider(mockProvider, cache);

  // First call - should hit mock provider
  provider.generateEmbedding("test");
  verify(mockProvider, times(1)).generateEmbedding("test");

  // Second call - should use cache
  provider.generateEmbedding("test");
  verify(mockProvider, times(1)).generateEmbedding("test"); // Still only 1 call
}

@Test
void testBatchProcessingOptimization() {
  // Test that batch separates hits/misses
  // Test that only misses are generated
}
```

#### 3. ONNXEmbeddingProviderTest
**File**: `file/src/test/java/org/apache/calcite/adapter/file/similarity/ONNXEmbeddingProviderTest.java`

**Note**: Requires model files to be present

```java
@Test
void testGenerateEmbedding() throws Exception {
  Map<String, Object> config = new HashMap<>();
  ONNXEmbeddingProvider provider = new ONNXEmbeddingProvider(config);

  double[] embedding = provider.generateEmbedding("unemployment rate");

  assertEquals(384, embedding.length);
  assertTrue(isNormalized(embedding));
}

@Test
void testBatchGeneration() throws Exception {
  ONNXEmbeddingProvider provider = new ONNXEmbeddingProvider(new HashMap<>());

  List<String> texts = Arrays.asList("inflation", "GDP", "employment");
  List<double[]> embeddings = provider.generateEmbeddings(texts);

  assertEquals(3, embeddings.size());
  for (double[] embedding : embeddings) {
    assertEquals(384, embedding.length);
  }
}

private boolean isNormalized(double[] vector) {
  double norm = 0.0;
  for (double v : vector) {
    norm += v * v;
  }
  return Math.abs(Math.sqrt(norm) - 1.0) < 0.0001;
}
```

### Integration Tests

#### EconEmbeddingIntegrationTest
**File**: `govdata/src/test/java/org/apache/calcite/adapter/govdata/econ/EconEmbeddingIntegrationTest.java`

```java
@Tag("integration")
@Test
void testEmploymentStatisticsWithEmbeddings() throws Exception {
  // Use test schema with embedding columns
  String modelJson = createTestModelWithEmbeddings();

  // Download data
  EconDataDownloader downloader = new EconDataDownloader(...);
  downloader.downloadAndConvert("employment_statistics",
      Map.of("frequency", "monthly", "year", "2023"));

  // Query with DuckDB
  String sql = "SELECT series_name, series_name_embedding " +
               "FROM econ.employment_statistics LIMIT 1";

  try (ResultSet rs = executeQuery(sql)) {
    assertTrue(rs.next());
    String seriesName = rs.getString("series_name");
    Array embeddingArray = rs.getArray("series_name_embedding");
    double[] embedding = (double[]) embeddingArray.getArray();

    assertEquals(384, embedding.length);
    assertNotNull(seriesName);
  }
}

@Tag("integration")
@Test
void testSemanticSimilarityQuery() throws Exception {
  downloadTestData();

  String sql =
      "WITH target AS (" +
      "  SELECT series_name_embedding AS target_emb " +
      "  FROM econ.employment_statistics " +
      "  WHERE series_name = 'Unemployment Rate' " +
      "  LIMIT 1" +
      ") " +
      "SELECT " +
      "  series_name, " +
      "  COSINE_SIMILARITY(series_name_embedding, target.target_emb) AS similarity " +
      "FROM econ.employment_statistics, target " +
      "ORDER BY similarity DESC " +
      "LIMIT 5";

  try (ResultSet rs = executeQuery(sql)) {
    int count = 0;
    while (rs.next()) {
      String name = rs.getString("series_name");
      double similarity = rs.getDouble("similarity");

      assertTrue(similarity >= 0.0 && similarity <= 1.0);
      count++;
    }
    assertEquals(5, count);
  }
}

@Tag("integration")
@Test
void testCacheReuse() throws Exception {
  // First download
  downloadTestData();

  // Get cache stats
  CacheStats stats1 = getEmbeddingCacheStats();
  int initialSize = stats1.getSize();

  // Download same data again
  downloadTestData();

  // Cache should have same size (100% hits)
  CacheStats stats2 = getEmbeddingCacheStats();
  assertEquals(initialSize, stats2.getSize());
  assertTrue(stats2.getHitRate() > 0.95); // At least 95% cache hits
}
```

---

## 🔄 Phase 6: Production Schema Updates (PENDING)

### Update econ-schema.json

**File**: `govdata/src/main/resources/econ-schema.json`

**Add to schema root**:
```json
{
  "schemaName": "econ",
  "embeddingCache": {
    "enabled": true,
    "path": "{cacheDir}/embeddings_cache",
    "provider": "onnx",
    "model": "all-MiniLM-L6-v2",
    "dimension": 384
  }
}
```

**Update key tables**:

**employment_statistics**:
```json
{
  "name": "series_name_embedding",
  "type": "array<double>",
  "nullable": true,
  "computed": true,
  "embeddingConfig": {
    "sourceColumn": "series_name"
  },
  "comment": "Vector embedding for semantic search on series names"
}
```

**fred_indicators**:
```json
{
  "name": "title_embedding",
  "type": "array<double>",
  "nullable": true,
  "computed": true,
  "embeddingConfig": {
    "sourceColumn": "title"
  },
  "comment": "Vector embedding for semantic search on indicator titles"
}
```

**regional_gdp**:
```json
{
  "name": "line_name_embedding",
  "type": "array<double>",
  "nullable": true,
  "computed": true,
  "embeddingConfig": {
    "sourceColumn": "line_name"
  },
  "comment": "Vector embedding for semantic search on GDP line items"
}
```

---

## Usage Examples

### Query Examples

**Find similar employment series**:
```sql
WITH target AS (
  SELECT series_name_embedding AS emb
  FROM econ.employment_statistics
  WHERE series_name = 'Unemployment Rate'
  LIMIT 1
)
SELECT
  series_name,
  COSINE_SIMILARITY(series_name_embedding, target.emb) AS similarity
FROM econ.employment_statistics, target
WHERE COSINE_SIMILARITY(series_name_embedding, target.emb) > 0.7
ORDER BY similarity DESC
LIMIT 20;
```

**Cross-dataset semantic search**:
```sql
-- Find BEA GDP series similar to a FRED indicator
WITH fred_target AS (
  SELECT title_embedding AS emb
  FROM econ.fred_indicators
  WHERE series = 'GDP'
  LIMIT 1
)
SELECT
  line_name,
  COSINE_SIMILARITY(line_name_embedding, fred_target.emb) AS similarity
FROM econ.regional_gdp, fred_target
ORDER BY similarity DESC
LIMIT 10;
```

**Semantic clustering**:
```sql
SELECT
  series_name,
  category,
  COSINE_SIMILARITY(series_name_embedding,
      (SELECT series_name_embedding FROM econ.employment_statistics
       WHERE series = 'LNS14000000' LIMIT 1)) AS cluster_1_similarity,
  COSINE_SIMILARITY(series_name_embedding,
      (SELECT series_name_embedding FROM econ.employment_statistics
       WHERE series = 'CEU0000000001' LIMIT 1)) AS cluster_2_similarity
FROM econ.employment_statistics
ORDER BY GREATEST(cluster_1_similarity, cluster_2_similarity) DESC;
```

### Environment Variables

```bash
# Override model path
export ONNX_MODEL_PATH=/path/to/custom/model.onnx
export ONNX_TOKENIZER_PATH=/path/to/tokenizer.json

# Performance tuning
export EMBEDDING_BATCH_SIZE=64
export EMBEDDING_CACHE_MAX_SIZE=200000

# Disable embeddings
export DISABLE_EMBEDDINGS=true
```

---

## Performance Characteristics

### Expected Timings

**ONNX Inference (CPU)**:
- Single embedding: 5-20ms
- Batch of 32: ~200-600ms
- Model loading: ~500ms (one-time)

**Cache Operations**:
- Memory cache hit: < 1ms
- Parquet read (100k entries): ~2-3 seconds
- Parquet write (1k entries): ~500ms

**End-to-End Performance**:

| Dataset Size | First Run (Cold Cache) | Subsequent Runs (Warm Cache) |
|--------------|------------------------|------------------------------|
| 1K records   | 5-20 seconds          | 1-2 seconds                 |
| 10K records  | 50-200 seconds        | 2-5 seconds                 |
| 100K records | 8-33 minutes          | 5-10 seconds                |
| 1M records   | 80-330 minutes        | 30-60 seconds               |

**Cache Hit Rate**:
- Same dataset re-downloaded: ~100%
- Related datasets (overlapping descriptions): 20-60%
- Unrelated datasets: 0-10%

### Storage Requirements

**Per 100k unique strings**:
- Embeddings: ~120MB (384 dims × 8 bytes × 100k)
- Parquet compressed: ~30-40MB
- Cache metadata: ~5MB

**Scaling**:
- 1M unique strings: ~300-400MB cache file
- 10M unique strings: ~3-4GB cache file

---

## Architecture Benefits

### ✅ Surgical Integration
- Minimal code changes (5 new files, 3 file updates)
- Leverages existing patterns (TextEmbeddingProvider, metadata-driven)
- No breaking changes to existing functionality

### ✅ Performance Optimization
- Persistent cache eliminates redundant embedding generation
- Cross-dataset cache reuse (global deduplication)
- In-memory hot cache for session-level performance
- Batch processing for efficient cache miss handling

### ✅ Flexibility
- Model-agnostic (supports ONNX, OpenAI, local TF-IDF)
- Configurable at schema and table levels
- Environment variable overrides
- Works with both local and S3 storage

### ✅ Production Ready
- Thread-safe cache operations
- Automatic cache persistence
- Error handling and fallback
- Cache statistics for monitoring

---

## Troubleshooting

### Common Issues

**1. Model files not found**:
```
ERROR: ONNX model file not found. Please set ONNX_MODEL_PATH...
```
**Solution**: Download model files or set environment variables

**2. Out of memory during embedding generation**:
```
java.lang.OutOfMemoryError: Java heap space
```
**Solution**: Increase heap size or reduce cache size
```bash
export EMBEDDING_CACHE_MAX_SIZE=50000
```

**3. Cache corruption**:
```
WARN: Failed to read existing parquet file, will overwrite
```
**Solution**: Delete cache file and regenerate
```bash
rm -rf {cacheDir}/embeddings_cache
```

**4. Slow performance on first run**:
- **Expected**: First run generates all embeddings
- **Solution**: Use smaller test datasets for development
- **Monitoring**: Check cache stats to verify caching is working

---

## Future Enhancements

### Performance
- GPU acceleration (ONNX CUDA provider)
- True batch processing in ONNXEmbeddingProvider
- Parallel embedding generation
- Cache pre-warming strategies

### Features
- Multiple embedding models per schema
- Embedding model versioning/migration
- Approximate nearest neighbor search (HNSW)
- Embedding fine-tuning on domain data

### Operations
- Cache management CLI
- Cache statistics dashboard
- Embedding quality metrics
- Model performance benchmarking

---

## Rollback Plan

If issues arise:

1. **Disable embeddings**: Set `embeddingCache.enabled: false` in schema
2. **Remove vector columns**: Delete from schema JSON
3. **Revert code**: Git revert to commit before this implementation
4. **Clean cache**: Delete `{cacheDir}/embeddings_cache` directory

No data loss - source data remains unchanged in original parquet files.

---

## References

- **ONNX Runtime**: https://onnxruntime.ai/
- **DJL Tokenizers**: https://docs.djl.ai/extensions/tokenizers/index.html
- **sentence-transformers**: https://www.sbert.net/
- **all-MiniLM-L6-v2**: https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2
- **DuckDB Vector Functions**: https://duckdb.org/docs/sql/functions/list.html#list_cosine_similarity

---

## Completion Checklist

### Phase 1: Core Infrastructure ✅
- [x] PersistentEmbeddingCache
- [x] CachedEmbeddingProvider
- [x] ONNXEmbeddingProvider
- [x] StorageProvider extensions
- [x] EmbeddingProviderFactory updates
- [x] Dependencies added
- [x] Compilation successful

### Phase 2: Model Assets ⏳
- [ ] Download all-MiniLM-L6-v2 ONNX model
- [ ] Download tokenizer.json
- [ ] Place in resources directory
- [ ] Test model loading

### Phase 3: Schema Extensions ⏳
- [ ] Add embeddingCache to schema root
- [ ] Add vector columns to tables
- [ ] Validate schema parsing

### Phase 4: Govdata Integration ⏳
- [ ] Extend TableColumn class
- [ ] Update AbstractGovDataDownloader
- [ ] Add embedding generation logic
- [ ] Test with sample data

### Phase 5: Testing ⏳
- [ ] Unit tests for cache
- [ ] Unit tests for providers
- [ ] Integration tests
- [ ] Performance benchmarks

### Phase 6: Production ⏳
- [ ] Update production schemas
- [ ] Document query patterns
- [ ] Performance monitoring
- [ ] User documentation
