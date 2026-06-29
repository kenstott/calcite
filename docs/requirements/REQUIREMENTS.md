# Requirements — generated view

> Generated from `docs/requirements/*.yaml` by `gen_requirements_md.py`. Do not hand-edit.
> Status: `[x]` complete `[~]` in-progress `[ ]` accepted `[.]` proposed `[-]` rejected.
**247 requirements across 4 adapters.**


## file  (173: 2 accepted, 61 complete, 9 in-progress, 98 proposed, 3 rejected)

| | ID | Pri | Type | Group / Category | Guarantee | Tests |
|---|---|---|---|---|---|---|
| [x] | FILE-001 | MUST | behavioral | csv / Type inference | CSV type inference maps each column to an exact SQL type (INTEGER, VARCHAR, DOUBLE, DATE, TIME, BOOLEAN) via … | file/CsvGoldenIngestTest#mixedTypes_inferredTypesAreExact |
| [x] | FILE-002 | MUST | behavioral | csv / Value fidelity | A CSV table round-trips to its exact cell values, SQL NULL distinct from the empty string in every column. | file/CsvGoldenIngestTest#mixedTypes_fullTableMatchesGolden |
| [x] | FILE-003 | MUST | behavioral | csv / Null handling | Textual null sentinels (NULL, NA, N/A, NONE, nil + case variants) in non-VARCHAR columns collapse to SQL NULL… | file/CsvGoldenIngestTest#nullsAndEmpty_sentinelsCollapseToN… |
| [x] | FILE-004 | MUST | behavioral | csv / Idempotence | Re-ingesting identical CSV bytes into a persistent cache yields an identical materialized table. | file/CsvGoldenIngestTest#mixedTypes_reingestionIsIdempotent |
| [ ] | FILE-005 | MUST | behavioral | csv / Type inference | With type inference disabled, every column is VARCHAR and values are preserved as raw text. | — |
| [x] | FILE-006 | MUST | behavioral | csv / Temporal inference | Local, UTC and RFC datetime formats are inferred as TIMESTAMP / TIMESTAMP WITH TIME ZONE and round-trip to ca… | file/CsvInferenceRequirementsTest#timestampInferenceIsExact |
| [ ] | FILE-007 | SHOULD | behavioral | csv / Null handling | blankStringsAsNull controls whether a blank VARCHAR cell becomes SQL NULL (true) or "" (false); whitespace-on… | — |
| [~] | FILE-008 | MUST | constraint | csv / Error handling | A non-null value violating an inferred column type must surface an error, not be silently coerced to NULL/def… | file/ResolvedBugTargetsTest#badNumericValueRaises, file/Res… |
| [x] | FILE-009 | MUST | behavioral | csv / Engine invariance | The csv golden (FILE-001..004) holds identically across every applicable engine — same source, same table — r… | file/EngineInvarianceTest#csvGoldenIsEngineInvariant |
| [.] | FILE-010 | MUST | behavioral | json / Value fidelity | A flat JSON source round-trips to exact cell values and inferred types — the basis every complex format decom… | — |
| [x] | FILE-011 | SHOULD | behavioral | json / Path extraction | Nested JSON is flattened/extracted per a configured path (JsonPathConverter) to an exact table, arrays and ne… | file/JsonPathParquetReaderTest |
| [x] | FILE-012 | MUST | behavioral | json / Engine invariance | The json golden holds identically across engines. | file/EngineInvarianceTest#jsonGoldenIsEngineInvariant |
| [x] | FILE-013 | MUST | behavioral | parquet / Value fidelity | A Parquet source round-trips to exact values with types taken from the file schema (self-describing, no infer… | file/JsonPathParquetReaderTest |
| [x] | FILE-014 | MUST | behavioral | xlsx / Extraction (decomposition) | MultiTableExcelToJsonConverter scans a workbook and emits one JSON file per discovered sheet/table; the golde… | file/ConverterDecompositionTest |
| [x] | FILE-015 | MUST | constraint | xlsx / Conversion idempotence | Re-converting an unchanged workbook produces byte-identical JSON and is skipped via the etag/mtime gate (Conv… | file/XlsxConversionIdempotenceTest#reConvertingUnchangedXls… |
| [x] | FILE-016 | MUST | behavioral | docx / Extraction (decomposition) | DocxTableScanner scans the document body for inline tables and emits one JSON per detected table; the catalog… | file/ConverterDecompositionTest |
| [x] | FILE-017 | MUST | behavioral | html / Extraction (decomposition) | HtmlTableScanner emits one JSON per detected HTML table; catalog golden pins the table-set. | file/TableExtractionRequirementsTest |
| [.] | FILE-018 | SHOULD | behavioral | html / Crawl discovery | The HTML crawler (CrawlerConfiguration maxDepth / linkPattern / followExternalLinks) discovers the correct pa… | — |
| [x] | FILE-019 | SHOULD | behavioral | pptx / Extraction (decomposition) | PptxTableScanner emits one JSON per detected slide table; catalog golden pins the set. | file/ConverterDecompositionTest |
| [.] | FILE-020 | SHOULD | behavioral | xml / Extraction (decomposition) | XmlTableScanner / XmlToJsonConverter emits JSON per detected table; catalog golden pins the set. | — |
| [x] | FILE-021 | MAY | behavioral | markdown / Extraction (decomposition) | MarkdownTableScanner emits JSON per detected table; catalog golden pins the set. | file/TableExtractionRequirementsTest |
| [.] | FILE-022 | MUST | behavioral | storage/local / Storage seam | LocalFileStorageProvider.listFiles returns the correct catalog (recursive enumeration) and openInputStream re… | — |
| [.] | FILE-023 | MUST | behavioral | storage/s3 / Storage seam | S3StorageProvider listFiles catalog + openInputStream bytes, verified at the seam. | — |
| [.] | FILE-024 | SHOULD | behavioral | storage/remote / Storage seam | Remote providers (http, ftp, sftp, hdfs, sharepoint) each deliver correct bytes and (where listable) the corr… | — |
| [x] | FILE-025 | MUST | behavioral | walking / Discovery | A directory/prefix walk discovers the correct file-set under recursion, glob and include/exclude rules; the d… | file/WalkingDiscoveryRequirementsTest |
| [x] | FILE-026 | MUST | structural | walking / Table-set assembly | The discovered catalog assembles into a schema: table names derived from paths, partition columns recognized,… | file/WalkingDiscoveryRequirementsTest |
| [x] | FILE-027 | MUST | behavioral | walking / Incremental refresh | Re-walking detects added / removed / changed files and updates the table-set correctly; an unchanged tree is … | file/WalkingRediscoveryTest#reWalkReflectsAddedAndRemovedFi… |
| [.] | FILE-028 | MUST | behavioral | refresh / Change detection | A refreshable table re-reads its source when the change signal fires (local mtime, remote ETag); a stale read… | — |
| [x] | FILE-029 | MUST | constraint | refresh / Idempotence | Refresh of an unchanged source performs no rewrite (no new parquet/cache artifact) — gated by ConversionMetad… | file/RefreshIdempotenceTest#reingestionDoesNotRewriteParque… |
| [.] | FILE-030 | MUST | behavioral | write/parquet / Materialization | ParquetMaterializationWriter writes a table whose read-back is value-identical to the source (round-trip gold… | — |
| [.] | FILE-031 | SHOULD | behavioral | write/iceberg / Materialization | IcebergTableWriter materializes a table that reads back value-identical; snapshots advance correctly. | — |
| [.] | FILE-032 | SHOULD | behavioral | partitioning / Partition pruning | Partition columns are derived from the path layout and a filtered query prunes to the correct partition set, … | — |
| [.] | FILE-033 | SHOULD | behavioral | partitioning / Incremental tracking | IncrementalTracker records processed partitions so a re-run only ingests new partitions (idempotent). | — |
| [~] | FILE-034 | SHOULD | behavioral | schema-evolution / Column evolution | Across files with added/removed columns, the unified table exposes the superset with nulls for absent columns… | file/NamingUnionTrackerRequirementsTest |
| [.] | FILE-035 | SHOULD | behavioral | compression / Transparent decompression | Compressed sources (gzip, etc.) decompress transparently; the table equals the uncompressed golden. | — |
| [.] | FILE-036 | MAY | behavioral | compaction / Compaction correctness | CompactionRunner merges small files/partitions without changing query results (value-identical before/after). | — |
| [.] | FILE-037 | MUST | behavioral | interaction / Engine-storage interaction | duckdb reads storage directly via httpfs (DuckDBFunctionMapping), bypassing StorageProvider — the duckdb × st… | — |
| [x] | FILE-038 | MUST | constraint | optimization / Transparency | Every exact optimization preserves results — a query with the rule enabled yields a table identical to the sa… | file/FileOptimizationTransparencyTest#exactOptimizationsPre… |
| [x] | FILE-039 | MUST | behavioral | optimization / HLL approx-distinct | APPROX_COUNT_DISTINCT returns an estimate within the documented relative error of exact COUNT(DISTINCT), and … | file/OptimizationRuleRequirementsTest |
| [x] | FILE-040 | MUST | behavioral | optimization / HLL sketch reuse | HLL sketches are computed once and cached (HLLSketchCache), and merge across partitions/files — the merged es… | file/StatisticsRequirementsTest (tagged FILE-040) |
| [x] | FILE-041 | MUST | behavioral | optimization / Count-star from statistics | COUNT(*) is answered from metadata (CountStarStatisticsRule) — result exact, no full scan — and the rule fire… | file/OptimizationRuleRequirementsTest |
| [x] | FILE-042 | SHOULD | behavioral | optimization / Pushdown / pruning | Filter pushdown, column pruning and partition selection (SimpleFileFilterPushdownRule, SimpleFileColumnPrunin… | file/OptimizationRuleRequirementsTest |
| [.] | FILE-043 | MUST | structural | optimization / Statistics accuracy | ColumnStatistics / ParquetStatisticsExtractor produce correct min/max/null-count/NDV — wrong stats silently c… | — |
| [.] | FILE-044 | SHOULD | behavioral | optimization / Engine-specific parity | Engine-specific optimization variants (DuckDBHLLCountDistinctRule, DuckDBIcebergCountStarRule, ClickHouseHLLC… | — |
| [.] | FILE-045 | MUST | behavioral | config / storageType precedence | Selection priority: (1) schema-level storageType forces ALL files through that provider and `directory` is ig… | — |
| [x] | FILE-046 | MUST | structural | config / storageType value set | Supported storageType values are local, s3, http, sharepoint, graph, ftp, sftp, hdfs (sharepoint/graph have n… | file/StorageProviderRequirementsTest (tagged FILE-046) |
| [.] | FILE-047 | MUST | structural | config / operand defaults | Operand defaults: recursive=true (README says false for the JDBC driver — reconcile), executionEngine=parquet… | — |
| [.] | FILE-048 | MUST | behavioral | config / name casing | tableNameCasing/columnNameCasing ∈ {SMART_CASING (→snake_case, default), UPPER, LOWER, UNCHANGED}; SMART_CASI… | — |
| [.] | FILE-049 | MUST | behavioral | config / env-var substitution | Operand values support ${VAR} (throws IllegalArgumentException if undefined) and ${VAR:default}, resolved env… | — |
| [.] | FILE-050 | MUST | constraint | config / schema name uniqueness | Schema names are case-sensitive and unique within a connection; a duplicate throws IllegalArgumentException (… | — |
| [~] | FILE-051 | MUST | behavioral | config / JDBC driver | AperioDriver (org.apache.calcite.adapter.file.AperioDriver) accepts jdbc:aperio:<path>[?param=...]; a bare pa… | file/AperioDriverOperandTest#operandDefaults |
| [.] | FILE-052 | SHOULD | behavioral | csv / inference config defaults | csvTypeInference defaults: enabled=false, samplingRate=0.1, maxSampleRows=1000, confidenceThreshold=0.95, mak… | — |
| [.] | FILE-053 | SHOULD | behavioral | csv / numeric promotion | Whole numbers in 32-bit range → INTEGER, outside → BIGINT, decimals/scientific → DOUBLE. | — |
| [x] | FILE-054 | SHOULD | behavioral | csv / boolean & temporal tokens | BOOLEAN matches {true,false,TRUE,FALSE,True,False,0,1}. Temporal detects DATE (yyyy-MM-dd, MM/dd/yyyy, dd/MM/… | file/CsvInferenceRequirementsTest#timeAndSlashDateFormatsIn… |
| [.] | FILE-055 | MUST | behavioral | csv / null token set | Recognized null tokens: empty, NULL/null/Null, NA, N/A, NONE/None, NIL/nil. With makeAllNullable=false a colu… | — |
| [x] | FILE-056 | SHOULD | behavioral | json / multi-table jsonSearchPaths | jsonSearchPaths (JSONPath, e.g. $.data.users) yields one table per configured path (named by the path's last … | file/TableExtractionRequirementsTest |
| [-] | FILE-057 | SHOULD | structural | xlsx / excelConfig | excelConfig keys evaluateFormulas, skipEmptyRows, headerRowIndex (e.g. 0), and a `sheets` allow-list; each sh… | — |
| [-] | FILE-058 | SHOULD | structural | html / htmlConfig / crawlConfig | htmlConfig tableSelector (default "table"), headerSelector ("th"), skipEmptyTables; crawlConfig generateTable… | — |
| [-] | FILE-059 | SHOULD | structural | xml / xmlConfig | xmlConfig rootPath/recordPath (XPath) define the repeating record set; attributePrefix prefixes attribute col… | — |
| [.] | FILE-060 | MUST | structural | table-naming / multi-table separator | Embedded multi-table names use `__` separator (report__summary, document__table_1, slides__slide__table); sub… | — |
| [x] | FILE-061 | MUST | structural | table-naming / filename normalization | Table names derive from filename (no extension), lowercased, hyphens→underscores (PRODUCTS.JSON→products, use… | file/NamingUnionTrackerRequirementsTest |
| [.] | FILE-062 | SHOULD | behavioral | yaml / yaml flattening | YAML nested keys flatten like JSON (profile.age→profile_age); list-of-records → rows. | — |
| [.] | FILE-063 | SHOULD | behavioral | compression / supported codecs | Auto-detect+decompress gzip(.gz), bzip2(.bz2), xz(.xz), zip(.zip) by extension; compressionConfig maxUncompre… | — |
| [.] | FILE-064 | SHOULD | behavioral | storage/hdfs / hdfs routing & auth | directory starting hdfs:// auto-routes to HDFS (recursive listing like local/S3); namenode from fs.defaultFS … | — |
| [.] | FILE-065 | SHOULD | behavioral | storage/s3 / s3-compatible endpoints | S3 supports MinIO/Wasabi/R2 via custom endpoint + options.usePathStyleAccess=true; region may be "auto" (R2);… | — |
| [.] | FILE-066 | SHOULD | behavioral | storage/http / http auth & error policy | HTTP auth types bearer/basic/apikey/oauth2; error handling is timeout (httpTimeout) + simple retry count (htt… | — |
| [.] | FILE-067 | SHOULD | behavioral | storage/sftp / sftp/ftp auth & host-key | SFTP (port 22) password or SSH key (privateKeyPath + passphrase); host-key via options.strictHostKeyChecking … | — |
| [.] | FILE-068 | MUST | behavioral | storage/sharepoint / sharepoint auth modes | storageType sharepoint supports oauth (clientId/secret/tenantId) and ntlm; client-secret auth requires legacy… | — |
| [.] | FILE-069 | SHOULD | behavioral | engine / default selection | JDBC default engine duckdb (CALCITE_FILE_ENGINE_TYPE / queryEngine.type override). In-process guidance: PARQU… | — |
| [.] | FILE-070 | MUST | constraint | engine / write path is duckdb | JDBC query engines (DuckDB/Trino/ClickHouse/Spark) are read-only; ALL writes (materialization, partition reor… | — |
| [.] | FILE-071 | MUST | behavioral | iceberg / time travel | No timeRange/snapshotId/asOfTimestamp → read latest snapshot (HEAD); snapshotId selects an exact snapshot; as… | — |
| [.] | FILE-072 | MUST | behavioral | iceberg / snapshot isolation | Each query reads a single consistent snapshot; concurrent readers see a consistent snapshot; each write is an… | — |
| [x] | FILE-073 | SHOULD | behavioral | schema-evolution / resolution strategy defaults | Schema resolution defaults: parquet LATEST_SCHEMA_WINS, csv RICHEST_FILE, json LATEST_FILE. LATEST_SCHEMA_WIN… | file/CsvSchemaRequirementsTest (tagged FILE-073) |
| [.] | FILE-074 | MUST | constraint | iceberg / read-only limitation | Iceberg READ integration is read-only (no INSERT/UPDATE/DELETE); position/equality deletes unsupported; only … | — |
| [.] | FILE-075 | SHOULD | behavioral | write/iceberg / inline compaction | materialize.iceberg.runCompaction (default false) compacts a partition when small-file count hits compactionM… | — |
| [.] | FILE-076 | MUST | behavioral | optimization / count-star from metadata | COUNT(*) answered from Iceberg/Parquet-footer row counts (no scan), equal to a full-scan count, whenever the … | — |
| [.] | FILE-077 | SHOULD | behavioral | optimization / HLL error bound & gate | APPROX_COUNT_DISTINCT (and intercepted COUNT(DISTINCT)) answered from HLL sketches built per ingestion (stati… | — |
| [.] | FILE-078 | SHOULD | behavioral | optimization / pushdown operators | Filter pushdown (enableFilterPushdown, default on) pushes =,!=,<,>,<=,>=,IN,LIKE to storage; pushed results e… | — |
| [.] | FILE-079 | SHOULD | behavioral | optimization / constraint-driven | Declared PK drives self-join/join elimination; FK drives join reordering and cardinality estimation; results … | — |
| [x] | FILE-080 | MUST | constraint | constraints / declaration model | constraints block declares primaryKey (composite allowed), foreignKeys (columns/targetTable/targetColumns, ma… | file/ConstraintRequirementsTest (tagged FILE-080) |
| [x] | FILE-081 | MUST | structural | constraints / jdbc/statistic exposure | Constraints exposed via DatabaseMetaData (getPrimaryKeys KEY_SEQ order, getImportedKeys, getIndexInfo NON_UNI… | file/ConstraintRequirementsTest (tagged FILE-081) |
| [.] | FILE-082 | MUST | behavioral | document-etl / ResponseTransformer contract | A ResponseTransformer(raw, RequestContext) MUST return a JSON array string whose object keys match the canoni… | — |
| [x] | FILE-083 | MUST | behavioral | document-etl / dimension types & expansion | Dimension types list/range(inclusive,step1)/yearRange(dataLag,releaseMonth,excludeYears,min/max)/query/json_c… | file/EtlTrackerRequirementsTest |
| [x] | FILE-084 | MUST | behavioral | document-etl / resumability tracker | Per-batch completion tracked in S3 (${CALCITE_TRACKER_S3_BUCKET}/year=*/source_key=<table>/); a re-run skips … | file/EtlTrackerRequirementsTest |
| [.] | FILE-085 | MUST | behavioral | document-etl / hook order & error policy | Per-row order responseTransformer → rowTransformers (declaration order; null drops row) → validators (drop|wa… | — |
| [~] | FILE-086 | MUST | constraint | refresh / refresh vs cache exclusivity | Setting refreshInterval disables Parquet conversion caching (mutually exclusive); refresh is lazy (on query a… | file/RefreshIdempotenceTest#refreshIntervalDisablesParquetC… |
| [.] | FILE-087 | SHOULD | behavioral | refresh / materialized-view rewrite | A model.json materialization computes once on first access (stored Parquet) and the optimizer transparently r… | — |
| [.] | FILE-088 | SHOULD | behavioral | refresh / freshness skip gate | freshness (opt-in; snapshot/computed_delta only) stores a tracker high-water token and probes→compares→skips.… | — |
| [.] | FILE-089 | SHOULD | behavioral | period-dimensions / calendar-correct providers | quarter/month/week/day/day_of_week providers emit calendar-correct values (leap days, 30/31, ISO 52/53), desc… | — |
| [.] | FILE-090 | SHOULD | behavioral | period-dimensions / dataset_type semantics | dataset_type ∈ {snapshot, delta(default), computed_delta}: snapshot overwrites only the open period (closed f… | — |
| [x] | FILE-091 | SHOULD | behavioral | raw-cache / bundle archive & classification | At session end BundleArchiver scans cache/raw and classifies by size (default 1MB): small files concatenated … | file/RawCacheRequirementsTest |
| [x] | FILE-092 | MUST | behavioral | raw-cache / read-tier resolution | CacheResolver.resolve tries tier-1 local first, then merged index byte-range GET from .bin or full GET from .… | file/RawCacheRequirementsTest |
| [x] | FILE-093 | MUST | behavioral | vector-search / similarity UDFs | Every schema auto-registers COSINE_SIMILARITY (−1..1), COSINE_DISTANCE (0..2), EUCLIDEAN_DISTANCE, DOT_PRODUC… | file/VectorFunctionRequirementsTest (tagged FILE-093) |
| [.] | FILE-094 | SHOULD | behavioral | vector-search / input encodings & pushdown | Vector UDFs accept comma-strings, '[..]', '(..)', native float[]/double[]/int[], List<Number>, Avro arrays — … | — |
| [.] | FILE-095 | SHOULD | constraint | error-handling / documented errors & SQLStates | Unknown schema/table → SqlValidatorException "Object 'X' not found"; FileReaderException(SQLException) report… | — |
| [~] | FILE-096 | MUST | behavioral | csv / type resolution order | CsvTypeInferrer.determineType forces VARCHAR if any sampled value is VARCHAR, else picks the most general typ… | file/CsvInferenceRequirementsTest#allNullColumnIsNullableVa… |
| [~] | FILE-097 | MUST | behavioral | csv / numeric promotion | Integer-pattern values fitting [Integer.MIN,MAX] → INTEGER, outside → BIGINT; INTEGER_PATTERN is tried before… | file/CsvInferenceRequirementsTest#numericPromotion, file/Cs… |
| [x] | FILE-098 | MUST | constraint | csv / null tokens (exact) | NullEquivalents.DEFAULT = {NULL, NA, N/A, NONE, NIL, ""} matched case-insensitively after trim; any empty/whi… | file/CsvInferenceRequirementsTest#defaultNullTokenSet, file… |
| [x] | FILE-099 | MUST | constraint | csv / sampling determinism | inferTypes samples via Math.random() against samplingRate (default 0.10), capped at maxSampleRows (default 10… | file/CsvInferenceRequirementsTest#inferenceIsDeterministicA… |
| [.] | FILE-100 | MUST | behavioral | csv / value coercion & storage | CsvTypeConverter stores DATE as int epoch-day, TIME as int millis-of-day, TIMESTAMP as long UTC wall-clock mi… | — |
| [~] | FILE-101 | MUST | constraint | csv / silent temporal fallback | On an unparseable DATE/TIME/TIMESTAMP, CsvTypeConverter logs a warning and returns null (and parses a date-on… | file/ResolvedBugTargetsTest#badTemporalValueRaises_target, … |
| [x] | FILE-102 | MUST | behavioral | xlsx / table detection | MultiTableExcelToJsonConverter splits a sheet on ≥ MIN_EMPTY_ROWS_BETWEEN_TABLES (2) blank rows, treats a sin… | file/TableExtractionRequirementsTest |
| [x] | FILE-103 | MUST | behavioral | html / table name & sanitization | HtmlTableScanner.getTableName priority: id attr > caption > preceding h1-h6 (≤3 sibs) > preceding title/heade… | file/TableScannerCatalogTest#htmlDuplicateNamesGetSuffixed,… |
| [x] | FILE-104 | MUST | behavioral | xml / detection & XXE hardening | XmlTableScanner detects a table as ≥ MIN_REPEATING_ELEMENTS (2) structurally-similar same-tag siblings (attr-… | file/TableScannerCatalogTest#xmlRepeatingElementsAreDetecte… |
| [x] | FILE-105 | MUST | behavioral | markdown / parsing & sparse rows | MarkdownTableScanner needs ≥3 lines (header, separator not at index 0, ≥1 data row); cells split on unescaped… | file/ConverterUtilRequirementsTest (tagged FILE-105) |
| [x] | FILE-106 | MUST | behavioral | docx / header rows & cell typing | DocxTableScanner assumes row 0 is a header, promotes row 1 to a 2nd header by cell-count/looksLikeHeader (≤3 … | file/ConverterDecompositionTest |
| [.] | FILE-107 | MUST | behavioral | json / flattener semantics | JsonFlattener defaults: separator "__", maxDepth 3, nullValue "". Nested objects join keys with __; empty obj… | — |
| [.] | FILE-108 | MUST | constraint | table-naming / identifier sanitization | ConverterUtils.sanitizeIdentifier maps non-[A-Za-z0-9_]→_, collapses 3+ underscores to EXACTLY "__" (delibera… | — |
| [x] | FILE-109 | SHOULD | behavioral | converters / converter system properties | ConverterUtils datetime ISO coercion stays LOCAL unless system property calcite.file.converter.timezone is se… | file/ConverterUtilRequirementsTest (tagged FILE-109) |
| [.] | FILE-110 | MUST | constraint | walking / recursive default (actual) | FileSchemaFactory reads recursive as operand.get("recursive") == Boolean.TRUE, so the default is FALSE and a … | — |
| [.] | FILE-111 | MUST | behavioral | walking / hidden-file skip & glob | FileSchema.listFilesRecursively skips any file/dir whose basename startsWith "." (dotfiles, .aperio). directo… | — |
| [.] | FILE-112 | MUST | behavioral | config / storage routing (createFromUrl) | StorageProviderFactory.createFromUrl: s3:// THROWS IllegalArgumentException (S3 needs explicit credentials — … | — |
| [x] | FILE-113 | MUST | behavioral | storage/local / listFiles contract | LocalFileStorageProvider.listFiles throws IOException("Directory does not exist") on a missing/non-dir path; … | file/StorageProviderRequirementsTest (tagged FILE-113) |
| [.] | FILE-114 | MUST | constraint | storage/http / timeouts & response codes | HttpStorageProvider hard-codes 30000ms connect+read timeouts, a fixed User-Agent, accepts only HTTP 200/201/3… | — |
| [x] | FILE-115 | SHOULD | behavioral | storage/ftp / ftp/sftp defaults | FtpStorageProvider: port 21, default anonymous creds, 30s connect/60s data, BINARY+passive; refusal/login thr… | file/StorageProviderRequirementsTest (tagged FILE-115) |
| [x] | FILE-116 | MUST | constraint | storage/s3 / client config & listing | S3StorageProvider with config requires accessKeyId+secretAccessKey or throws (NO env fallback); region defaul… | file/StorageProviderRequirementsTest (tagged FILE-116) |
| [.] | FILE-117 | MUST | behavioral | storage/hdfs / missing-path contract | HDFSStorageProvider defaults fs.defaultFS to hdfs://localhost:9000; listFiles on a MISSING path returns an EM… | — |
| [.] | FILE-118 | MUST | behavioral | storage / hasChanged comparison | StorageProvider.hasChanged: null cached → changed; compares size, then ETag (when both present), then lastMod… | — |
| [.] | FILE-119 | MUST | behavioral | engine / default engine (actual) | ExecutionEngineConfig.getDefaultEngine() unconditionally returns "parquet" (no DuckDB/classpath detection); n… | — |
| [x] | FILE-120 | MUST | behavioral | statistics / HLL precision & hashing | HyperLogLogSketch default precision 14 (16384 buckets), ctor rejects <4 or >16; add() hashes via MD5 first-8-… | file/StatisticsRequirementsTest (tagged FILE-120) |
| [x] | FILE-121 | MUST | constraint | statistics / stats-rule activation gates | The statistics optimization rules are each gated by a system property defaulting FALSE: calcite.file.statisti… | file/StatisticsRequirementsTest (tagged FILE-121) |
| [.] | FILE-122 | MUST | behavioral | optimization / count-star guards | CountStarStatisticsRule fires only for true COUNT(*) (empty groupSet, single non-distinct COUNT, empty argLis… | — |
| [.] | FILE-123 | MUST | behavioral | optimization / pushdown/prune/reorder thresholds | SimpleFileFilterPushdownRule (ParquetTranslatableTable only): folds always-true to scan / always-false to emp… | — |
| [.] | FILE-124 | SHOULD | behavioral | optimization / HLL rule instance bug | SimpleHLLCountDistinctRule.APPROX_ONLY_INSTANCE is configured approxOnly=true but shouldOptimize() ignores th… | — |
| [~] | FILE-125 | SHOULD | constraint | optimization / debug-artifact leak | DuckDBHLLCountDistinctRule writes hard-coded /tmp/duckdb_hll_rule_loaded.txt (static init) and appends /tmp/d… | file/ResolvedBugTargetsTest#duckdbHllRuleWritesNoTmpArtifac… |
| [x] | FILE-126 | MUST | behavioral | statistics / parquet stats & NDV fallbacks | ParquetStatisticsExtractor reads min/max/nullCount/rowCount from the footer (no NDV), summing across row grou… | file/StatisticsRequirementsTest (tagged FILE-126) |
| [x] | FILE-127 | MUST | behavioral | concurrency / parquet conversion lock | ConcurrentParquetCache.convertWithLocking: Redis lock if available, else per-(schema:absPath) ReentrantLock t… | file/ConcurrencyRequirementsTest |
| [x] | FILE-128 | MUST | behavioral | concurrency / source file locks | SourceFileLockManager.acquireReadLock takes a SHARED FileLock, acquireWriteLock EXCLUSIVE, 30s timeout, 100ms… | file/ConcurrencyRequirementsTest |
| [x] | FILE-129 | MUST | behavioral | concurrency / iceberg commit lock | Every table-mutating Iceberg commit runs under IcebergTableWriter.underCommitLock = a JVM monitor keyed by Ta… | file/ConcurrencyRequirementsTest |
| [.] | FILE-130 | MUST | behavioral | conversion / staleness decision | ParquetConversionUtil.needsConversion skips re-conversion only when the cache exists AND source.lastModified(… | — |
| [.] | FILE-131 | MUST | behavioral | conversion / conversion-record preservation | ConversionMetadata.recordTable preserves an existing non-DIRECT conversionType (e.g. SEC_XBRL_TO_PARQUET, ICE… | — |
| [.] | FILE-132 | MUST | behavioral | refresh / lazy trigger & anti-thrash | File-table refresh is LAZY (query-time: project/toRel/scan/getRowType per table type). needsRefresh() gates t… | — |
| [.] | FILE-133 | MUST | behavioral | refresh / eager conversion watcher | ConversionFileWatcher is the ONLY eager/timer refresher: a daemon scheduleWithFixedDelay (default 60000ms whe… | — |
| [.] | FILE-134 | SHOULD | constraint | refresh / interval parsing | RefreshInterval.parse accepts ISO-8601 (P/PT) and "<n> (second|minute|hour|day)s?"; null/empty/unmatched → nu… | — |
| [.] | FILE-135 | MUST | behavioral | refresh / partitioned staleness | RefreshablePartitionedParquetTable.filesChangedComparedToBaseline → true on null/empty baseline, file-count c… | — |
| [.] | FILE-136 | MUST | behavioral | refresh / parquet-cache rewrite | RefreshableParquetCacheTable.doRefresh, when the monitored source changed, deletes the old parquet + *.tmp.*.… | — |
| [.] | FILE-137 | MUST | behavioral | materialized-view / one-shot CAS invalidation | MaterializedViewTable.materialize is one-shot via materialized.compareAndSet(false,true) with NO staleness ch… | — |
| [.] | FILE-138 | MUST | behavioral | document-etl / freshness fail-safe | FreshnessCheck.changed(prev,cur) → true whenever prev or cur is null, else !prev.equals(cur) — an unknown/mis… | — |
| [x] | FILE-139 | MUST | constraint | document-etl / error fatality classes | IncompleteFetchException (unchecked) marks a fetch that could not complete (e.g. 429 exhausted) as a FAILURE … | file/EtlErrorClassesTest#incompleteFetchIsUnchecked, file/E… |
| [x] | FILE-140 | MUST | behavioral | document-etl / circuit breaker & rate limit | HttpSource keeps a process-wide consecutive-503 counter per base URI; at threshold 5 it fast-skips via Skippe… | file/EtlTrackerRequirementsTest |
| [.] | FILE-141 | MUST | structural | partitioning / incremental tracker keys | IncrementalTracker period key is year_quarter_month_week_day_day_of_week_<pipeline> with literal "NA" for abs… | — |
| [.] | FILE-142 | MUST | behavioral | partitioning / empty vs complete (self-heal) | IncrementalTracker.markProcessedEmpty (HTTP 200, zero rows) is distinct from terminal complete: an empty peri… | — |
| [.] | FILE-143 | MUST | behavioral | raw-cache / no-TTL & tier resolution | Raw cache is immutable with NO TTL — HttpSource.hasValidRawCache returns valid whenever the cache file exists… | — |
| [.] | FILE-144 | MUST | behavioral | iceberg / commit mode & version hint | IcebergTableWriter commits via AppendFiles (newAppend, append-only, relying on SQL dedup); replace-partitions… | — |
| [.] | FILE-145 | MUST | behavioral | iceberg / time-travel precedence & stats gap | IcebergEnumerator snapshot precedence: snapshotId > asOfTimestamp (Instant.parse; bad format throws) > curren… | — |
| [.] | FILE-146 | MUST | behavioral | jdbc / second JDBC driver | A SECOND driver FileJdbcDriver accepts jdbc:calcite: URLs containing schema=file|materialized_view with snake… | — |
| [~] | FILE-147 | MUST | behavioral | jdbc / AperioDriver defaults & var expansion | AperioDriver.buildOperand always emits lex=ORACLE, unquotedCasing=TO_LOWER, quotedCasing=UNCHANGED, table/col… | file/AperioDriverOperandTest#operandDefaults, file/AperioDr… |
| [.] | FILE-148 | MUST | behavioral | table-naming / SmartCasing rules | SmartCasing.toSnakeCase lowercases an all-uppercase non-acronym without splitting (DEPTS→depts), applies a di… | — |
| [x] | FILE-149 | MUST | structural | constraints / positional FK resolution | TableConstraints builds PK/unique bitsets from columnNames.indexOf, SILENTLY dropping unfound columns (and a … | file/ConstraintRequirementsTest (tagged FILE-149) |
| [x] | FILE-150 | MUST | behavioral | vector-search / UDF semantics & encodings | SimilarityFunctions registers exactly 10 UDFs (COSINE_SIMILARITY, SEMANTIC_SIMILARITY, EMBED, COSINE_DISTANCE… | file/VectorFunctionRequirementsTest (tagged FILE-150) |
| [x] | FILE-151 | MUST | structural | error-handling / FileReaderException has no SQLState | FileReaderException is a plain package-private checked Exception (extends Exception) with NO SQLState/vendor … | file/JsonPathParquetReaderTest |
| [.] | FILE-152 | MUST | behavioral | statistics / HLL measured error bounds | Tested HLL accuracy: precision 12 estimates 100 distinct within 20%, 10k within 5%, 100k within 5%; precision… | — |
| [.] | FILE-153 | MUST | behavioral | statistics / selectivity formulas | ColumnStatistics.getSelectivity pins: "=" → 1/distinct, "!=" → 1-1/distinct, IS NULL → nullCount/total, IS NO… | — |
| [.] | FILE-154 | MUST | behavioral | engine / engine equivalence (tested) | Parquet and DuckDB engines return identical COUNT(*) (both 1000) and identical row-level results for CSV/JSON… | — |
| [.] | FILE-155 | MUST | behavioral | csv / explicit typed header | A typed CSV header ("EMPNO:int,NAME:string,SLACKER:boolean,...") drives column SQL types directly, independen… | — |
| [x] | FILE-156 | SHOULD | behavioral | csv / decimal & config clamping | parseDecimal uses HALF_UP symmetric for negatives ("123.455"→123.46, "-123.455"→-123.46); precision overflow … | file/ConverterUtilRequirementsTest (tagged FILE-156) |
| [x] | FILE-157 | SHOULD | behavioral | csv / empty & header-only sources | An empty source and a disabled config both return an EMPTY type list; a header-only CSV returns one nullable … | file/CsvSchemaRequirementsTest (tagged FILE-157) |
| [.] | FILE-158 | SHOULD | behavioral | json / multi-table discovery defaults | jsonSearchPaths register one table per path (keyed by last segment); the wrapper/source table is NOT register… | — |
| [.] | FILE-159 | MUST | behavioral | walking / glob semantics matrix | PathMatcher contract: "**" matches root+sub+nested, "**/*" matches sub+nested but NOT root, "*" matches only … | — |
| [.] | FILE-160 | SHOULD | behavioral | storage / provider edge contracts | readRange beyond EOF returns a trimmed array (no padding/error). Local resolvePath normalizes ".." and lets a… | — |
| [.] | FILE-161 | MUST | behavioral | partitioning / partition detection | PartitionDetector: extractHivePartitions parses ordered key=value segments across levels (special chars prese… | — |
| [.] | FILE-162 | SHOULD | behavioral | storage/http / POST/GraphQL & mime override | HttpStorageProvider supports method=POST with a request body (REST search / GraphQL); a configured mimeType o… | — |
| [.] | FILE-163 | MUST | constraint | write/iceberg / writer preconditions & evolution | IcebergMaterializationWriter.initialize rejects null config (IAE), non-ICEBERG format (IAE), disabled materia… | — |
| [.] | FILE-164 | SHOULD | structural | iceberg / metadata tables shape | IcebergMetadataTables.createMetadataTables returns exactly 5 ScannableTables: history (4 cols), snapshots (6 … | — |
| [x] | FILE-165 | MUST | behavioral | partitioning / per-period completion isolation | IncrementalTracker keys completion per canonical period, so completing year 2025 never marks an unprocessed y… | file/NamingUnionTrackerRequirementsTest |
| [.] | FILE-166 | MUST | behavioral | conversion / format routing | FileConversionManager routes docx/pptx/md/html/xml to document converters (requiresConversion true, isDirectl… | — |
| [.] | FILE-167 | MUST | behavioral | concurrency / conversion at-least-once | Under 5 racing threads on one source, ConcurrentParquetCache serializes via the lock with the invariant conve… | — |
| [.] | FILE-168 | MUST | behavioral | jdbc / FileJdbcDriver parsing & precedence | FileJdbcDriver: schema=file → schema named "files", default engine "parquet", storage_path ${tmp}/calcite_fil… | — |
| [.] | FILE-169 | SHOULD | structural | constraints / declaration paths & iceberg | PKs may be declared via top-level constraints OR partitionedTables.primaryKey, both surfacing through getPrim… | — |
| [.] | FILE-170 | MUST | behavioral | vector-search / exact UDF results & null policy | COSINE_SIMILARITY identical=1.0/orthogonal=0.0/opposite=-1.0/zero-norm=0.0; COSINE_DISTANCE=1-sim; EUCLIDEAN_… | — |
| [.] | FILE-171 | MUST | behavioral | table-naming / multi-table suffix rule | A SINGLE discovered table gets NO _Tn suffix; only multiple tables sharing a base name get _T1/_T2 (and the u… | — |
| [.] | FILE-172 | SHOULD | behavioral | html / selector & header rules | HtmlToJsonConverter: CSS selector + index selects a specific table (out-of-bounds/no-match → empty list; null… | — |
| [.] | FILE-173 | SHOULD | behavioral | config / Oracle lexical + case sensitivity | With lex=ORACLE, unquotedCasing=TO_LOWER and column_name_casing=LOWER, quoted lowercase identifiers resolve a… | — |

## govdata  (74: 74 accepted)

| | ID | Pri | Type | Group / Category | Guarantee | Tests |
|---|---|---|---|---|---|---|
| [ ] | GOV-001 | MUST | structural | econ / Schema shape | The econ schema exposes BLS/BEA/FRED economic series as tables whose grain is one observation per (series ide… | — |
| [ ] | GOV-002 | MUST | behavioral | econ / Period semantics | In employment_statistics the period code encodes granularity — M01–M12 monthly, Q01–Q04 quarterly — and is_la… | — |
| [ ] | GOV-003 | MUST | behavioral | econ / Units & meaning | inflation_metrics values are base-period-indexed price indices (base = 100) for CPI/PPI series, not raw price… | — |
| [ ] | GOV-004 | MUST | constraint | econ / Freshness | econ data honors each source's publication lag — the queryable window excludes periods more recent than the k… | — |
| [ ] | GOV-005 | SHOULD | behavioral | econ / Schema shape | regional_cpi exposes CPI by US census region, each row carrying the census region code and name alongside a b… | — |
| [ ] | GOV-006 | MUST | structural | sec / Schema shape | The sec schema exposes SEC EDGAR filings as document-derived tables, all year-partitioned (Hive type=*/year=*… | — |
| [ ] | GOV-007 | MUST | behavioral | sec / Reference table | filing_metadata is the core reference table with one row per (cik, accession_number) carrying filing_type, fi… | — |
| [ ] | GOV-008 | MUST | behavioral | sec / Units & meaning | In financial_line_items, value is the raw XBRL text and value_numeric is a best-effort numeric representation… | — |
| [ ] | GOV-009 | MUST | behavioral | sec / Period semantics | filing_contexts defines each XBRL fact's reporting period and entity; a context is either an instant (period_… | — |
| [ ] | GOV-010 | SHOULD | behavioral | sec / Units & meaning | insider_transactions (Form 3/4/5) carries one row per reporting insider transaction with transaction_code (P=… | — |
| [ ] | GOV-011 | SHOULD | behavioral | sec / Grain & units | institutional_holdings (Form 13F-HR, filed quarterly by $100M+ managers) is one row per (cik, accession_numbe… | — |
| [ ] | GOV-012 | SHOULD | behavioral | sec / Grain & meaning | beneficial_ownership (Schedule 13D activist / 13G passive, >5% holders) carries percent_of_class, shares_bene… | — |
| [ ] | GOV-013 | SHOULD | behavioral | sec / Text extraction | mda_sections holds Management Discussion & Analysis prose chunked one row per (cik, accession_number, section… | — |
| [ ] | GOV-014 | SHOULD | behavioral | sec / Units & grain | stock_prices is daily OHLCV, one row per (ticker, date) with ticker in Stooq format (e.g. 'AAPL.US'); open/hi… | — |
| [ ] | GOV-015 | MUST | constraint | sec / Constraint | SEC documents non-enforced primary keys and foreign keys for query optimization: financial_line_items PK is (… | — |
| [ ] | GOV-016 | SHOULD | constraint | sec / Freshness | SEC ingestion has dataLagYears=0 (filings are available the day they are submitted, unlike the lagged econ/ge… | — |
| [ ] | GOV-017 | MAY | behavioral | sec / Derived view | The financial_facts view maps XBRL concept variants to canonical metric names (e.g. both us-gaap:Revenues and… | — |
| [ ] | GOV-018 | MUST | structural | geo / Schema shape | The geo schema exposes TIGER/Line boundaries, HUD ZIP crosswalks, USDA rural-urban classifications, Census Ga… | — |
| [ ] | GOV-019 | MUST | behavioral | geo / Units & meaning | Boundary tables (states, counties, places, tracts, block_groups, etc.) carry FIPS identifiers of a fixed digi… | — |
| [ ] | GOV-020 | MUST | constraint | geo / Freshness | TIGER boundary/gazetteer tables use dataLagYears=1 (TIGER vintage Y publishes ~Sep Y+1, so the current year i… | — |
| [ ] | GOV-021 | SHOULD | behavioral | geo / Vintage handling | Tables whose Census field names change across the 2010 vs 2020 census vintage (zctas, urban_areas, pumas, vot… | — |
| [ ] | GOV-022 | SHOULD | constraint | geo / Vintage availability | Years with no published shapefile are excluded rather than 404'd: zctas, urban_areas and pumas exclude 2011, … | — |
| [ ] | GOV-023 | MUST | behavioral | geo / Crosswalk semantics | HUD ZIP crosswalk tables (zip_county, zip_cbsa, zip_tract, zip_cd and their reverses) carry res_ratio, bus_ra… | — |
| [ ] | GOV-024 | SHOULD | behavioral | geo / Classification | rural_urban_continuum is one row per county with rucc_code 1-9 (metro 1-3 / nonmetro 4-9), and ruca_codes is … | — |
| [ ] | GOV-025 | SHOULD | behavioral | geo / Reference data | Gazetteer tables (gazetteer_counties, gazetteer_places, gazetteer_zctas) give authoritative names plus land_a… | — |
| [ ] | GOV-026 | SHOULD | behavioral | geo / Hierarchy | USGS watershed tables form a nested hierarchy by Hydrologic Unit Code length: watersheds_huc2 (22 regions) ->… | — |
| [ ] | GOV-027 | MUST | constraint | geo / Constraint | Every geo table's primary key is (geographic_id, year), with hierarchical foreign keys (counties.state_fips -… | — |
| [ ] | GOV-028 | SHOULD | constraint | geo / Coverage | State-partitioned boundary tables cover all 50 states + DC (FIPS 01-56 excluding 03,07,14,43,52), with custom… | — |
| [ ] | GOV-029 | MUST | structural | census / Schema shape | The census schema exposes ACS, Decennial, PEP and Census economic programs as tables partitioned by (type, ye… | — |
| [ ] | GOV-030 | MUST | behavioral | census / Units & meaning | ACS estimate columns are population/household counts (long) cast via TRY_CAST from API variable codes; income… | — |
| [ ] | GOV-031 | MUST | behavioral | census / Schema evolution | Census columns use conceptual canonical names normalized by CensusVariableNormalizer from year/program-specif… | — |
| [ ] | GOV-032 | MUST | constraint | census / Freshness | Each census product honors its own publication lag via the year dimension: ACS 5-year dataLag=2 with releaseM… | — |
| [ ] | GOV-033 | SHOULD | constraint | census / Minimum-year floors | Tables whose source feed begins after the global default start floor at a minYear that returns data: ACS 5-ye… | — |
| [ ] | GOV-034 | MUST | behavioral | census / Geography & grain | ACS/decennial tables are one row per (year, geography, state, county) - that is their primary key - with coun… | — |
| [ ] | GOV-035 | SHOULD | behavioral | census / Derived breakdowns | Aggregate demographic columns are summed from many fine-grained ACS variables in SQL expressions: acs_age.age… | — |
| [ ] | GOV-036 | SHOULD | behavioral | census / Economic programs | cbp_establishments is one row per (geography, year, NAICS2017 industry) with establishments, employees (mid-M… | — |
| [ ] | GOV-037 | SHOULD | behavioral | census / Small-area estimates | SAIPE (saipe_poverty) gives annual county poverty counts and rates plus median_household_income, and SAHIE (s… | — |
| [ ] | GOV-038 | SHOULD | behavioral | census / Time-series grain | qwi_employment is partitioned by (type, time) with time in YYYY-QN format and one fetch per state FIPS (the Q… | — |
| [ ] | GOV-039 | MAY | behavioral | census / Fixed-width parsing | building_permits parses the Census Building Permits Survey fixed-width state file (stYYMMy.txt, December cumu… | — |
| [ ] | GOV-040 | MUST | constraint | census / Constraint | Census tables foreign-key into the geo schema for geographic resolution: every ACS/decennial table's state ->… | — |
| [ ] | GOV-041 | MAY | behavioral | census / Derived view | State-level summary views (population_summary, income_summary, poverty_rate, education_attainment, unemployme… | — |
| [ ] | GOV-042 | MUST | structural | energy / Schema shape | The energy schema exposes EIA and MSHA datasets; all state-level tables carry state_abbr (2-letter, e.g. 'CA'… | — |
| [ ] | GOV-043 | MUST | structural | energy / Grain | eia_electricity_generation is one row per (state_abbr, energy_source_code, sector_code, generation_year, gene… | — |
| [ ] | GOV-044 | MUST | behavioral | energy / Units & meaning | eia_electricity_prices avg_price_cents_kwh is in cents per kWh, revenue_million_dollars in millions of dollar… | — |
| [ ] | GOV-045 | MUST | structural | energy / Grain & keys | eia_power_plants (EIA-860) is one row per generator keyed (plant_id, generator_id, report_year), with capacit… | — |
| [ ] | GOV-046 | MUST | constraint | energy / Freshness | Each EIA bulk source honors its archive publication lag via dataLag on the year range: EIA-861 utility (dataL… | — |
| [ ] | GOV-047 | SHOULD | behavioral | energy / Partition strategy | Sub-annual EIA series (eia_electricity_generation, eia_fossil_fuel_production, eia_refinery_operations) parti… | — |
| [ ] | GOV-048 | MUST | behavioral | energy / Units & meaning | eia_natural_gas_storage volumes are in billion cubic feet (volume_bcf), one row per (eia_region_code, storage… | — |
| [ ] | GOV-049 | SHOULD | behavioral | energy / Encoded codes | eia_state_energy_consumption (SEDS) is one row per (state_abbr, msn, consumption_year) where msn is a 5-char … | — |
| [ ] | GOV-050 | SHOULD | behavioral | energy / Grain & meaning | eia_fossil_fuel_production unions crude oil and natural gas into one table distinguished by fuel_type ('crude… | — |
| [ ] | GOV-051 | SHOULD | behavioral | energy / Grain & filtering | eia_coal_mines (MSHA MinesProdYearly joined to Mines on mine_id) is one row per (mine_id, subunit_code, repor… | — |
| [ ] | GOV-052 | MAY | behavioral | energy / Derived views | state_energy_mix aggregates eia_electricity_generation to one row per state+year, classifying energy_source_c… | — |
| [ ] | GOV-053 | MUST | structural | health / Schema shape | The health schema exposes openFDA, ClinicalTrials.gov, CDC, CMS, Medicaid, and RxNorm datasets as single-part… | — |
| [ ] | GOV-054 | MUST | structural | health / Grain & keys | fda_ndc_products is the master drug reference, one row per product_ndc (PK), with FKs application_number -> f… | — |
| [ ] | GOV-055 | MUST | constraint | health / Freshness | Each openFDA table (fda_ndc_products, fda_drug_approvals, fda_drug_recalls, fda_adverse_events, fda_device_re… | — |
| [ ] | GOV-056 | MUST | behavioral | health / Units & meaning | fda_drug_recalls and fda_device_recalls use the FDA classification scheme where Class I is most severe, Class… | — |
| [ ] | GOV-057 | MUST | structural | health / Grain & keys | clinical_trials is one row per nct_id (PK); clinical_trial_conditions (composite PK nct_id+ condition_name) a… | — |
| [ ] | GOV-058 | MUST | structural | health / Grain & keys | medicaid_drug_utilization has composite PK (state, ndc, year, quarter) with quarter in {Q1..Q4}; ndc is FK to… | — |
| [ ] | GOV-059 | SHOULD | constraint | health / Release window | cms_open_payments is partitioned by (payment_type in {general,research,ownership}, year) with dataLag 1 and m… | — |
| [ ] | GOV-060 | SHOULD | constraint | health / Release window | Source-specific release windows gate fetches: cms_hospital_quality July-October, cdc_brfss May-August (dataLa… | — |
| [ ] | GOV-061 | SHOULD | behavioral | health / Frozen data | cdc_covid_vaccinations is a frozen dataset (no updates after 2023-05-12) carried with no incremental block, s… | — |
| [ ] | GOV-062 | SHOULD | behavioral | health / Grain & union | cdc_mortality unions CDC annual (bi63-dtpu) and weekly provisional (muzy-jte6) feeds into one table, distingu… | — |
| [ ] | GOV-063 | SHOULD | structural | health / Grain & FK | rxnorm_drugs is one row per rxcui (PK) covering term types tty in {IN, BN, SCD}, gated by freshness type:hash… | — |
| [ ] | GOV-064 | MUST | structural | cyber / Grain & keys | vulnerabilities (NVD CVE 2.0) is one row per cve_id (PK [type, cve_id]) partitioned by type/year/quarter via … | — |
| [ ] | GOV-065 | MUST | constraint | cyber / Freshness | NVD ingest (vulnerabilities, vulnerability_cwes) gates on freshness type:count, probing services.nvd.nist.gov… | — |
| [ ] | GOV-066 | MUST | structural | cyber / Junction tables | vulnerability_cwes (PK type+cve_id+cwe_id) and kev_cwes (PK type+cve_id+cwe_id) are ingest-time materialized … | — |
| [ ] | GOV-067 | MUST | behavioral | cyber / Snapshot semantics | Catalog tables (cwe_catalog, kev_catalog, kev_cwes, osv_vulnerabilities) are dataset_type:snapshot with overw… | — |
| [ ] | GOV-068 | SHOULD | behavioral | cyber / Delimited fields | Multi-valued cyber columns are pipe-delimited strings, not arrays: vulnerabilities.cwe_ids, cwe_catalog.paren… | — |
| [ ] | GOV-069 | MUST | behavioral | cyber / Append-log & dedup | vuln_cross_refs is a GHSA-only incremental APPEND log (overwritePartitions:false) keyed (type, cve_id, extern… | — |
| [ ] | GOV-070 | SHOULD | constraint | cyber / Freshness & watermark | vuln_cross_refs gates on freshness type:graphql (max securityAdvisories.updatedAt); when unchanged the whole … | — |
| [ ] | GOV-071 | SHOULD | structural | cyber / Grain & partition | IOC threat tables (ioc_urls/URLhaus, ioc_hashes/MalwareBazaar, ioc_ips/Feodo, ioc_mixed/ThreatFox, threat_pul… | — |
| [ ] | GOV-072 | MUST | behavioral | cyber / Write-mode & idempotence | threat_pulses (OTX) gates on freshness type:version (max pulse modified) with watermark_var otxModifiedSince … | — |
| [ ] | GOV-073 | SHOULD | structural | cyber / Taxonomy tables | Standards/taxonomy tables are version-pinned snapshots keyed on their natural ID: attack_techniques (techniqu… | — |
| [ ] | GOV-074 | SHOULD | structural | cyber / Cross-domain mapping | attack_to_nist_mappings (PK type+technique_id+nist_control_id, mapping_type 'mitigates', status='complete', n… | — |

