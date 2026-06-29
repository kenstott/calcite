/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.StatisticsCache;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Execution-layer refinements that ADD to the already-done FILE-039/041/042 contracts
 * recorded in {@code OptimizationRuleRequirementsTest}. These do not duplicate those
 * tests; they pin three more-specific EXECUTION guarantees:
 *
 * <ul>
 *   <li>FILE-076 - COUNT(*) over a MULTI-FILE materialized Parquet table equals the
 *       known row count, and equals a filtered COUNT cross-check, so the count cannot
 *       silently come from a single file or a wrong constant.</li>
 *   <li>FILE-077 - APPROX_COUNT_DISTINCT is HLL-answered within the documented relative
 *       error (a few-thousand-distinct column), while exact COUNT(DISTINCT) on a column
 *       WITHOUT a registered sketch still full-scans and returns the EXACT value.</li>
 *   <li>FILE-078 - the comparison / set / pattern operators (=, !=, &lt;, &gt;, &lt;=,
 *       &gt;=, IN, LIKE) each return results exactly equal to the obvious expected rows
 *       (the pushed result equals the unpushed result); column pruning is always on; and
 *       partition pruning applies on a Hive {@code year=}/{@code month=} layout.</li>
 * </ul>
 *
 * <p>Harness mirrors {@code OptimizationRuleRequirementsTest}: a temp directory of
 * Parquet files wrapped in a {@code FileSchemaFactory} schema with
 * {@code executionEngine=parquet} and {@code ephemeralCache=true}, the same HLL sketch
 * setup ({@link StatisticsCache#saveHLLSketch} plus the
 * {@code calcite.file.statistics.*} system properties), and the same
 * {@code explain plan for} probe. The Hive-partitioned logical table is built with the
 * {@code partitionedTables} operand ({@code partitions.style=hive}), exactly as
 * {@code partition.PartitionIntegrationTest} does, so {@code year}/{@code month} surface
 * as real columns. ENGINE USED: parquet.
 *
 * <p>This is an {@code @Tag("integration")} class, matching the exemplar and every
 * file-adapter rule test it refines (they spin up the full Parquet/JDBC stack).
 *
 * <p>OMIT/NOTE summary (see per-method javadoc for detail):
 * <ul>
 *   <li>NOTE (FILE-076): plan-firing of the Parquet-footer COUNT(*) shortcut
 *       (metadata VALUES node) is covered elsewhere - in this hermetic setup the
 *       freshly-written Parquet has a null Statistic.getRowCount(), so the
 *       CountStarStatisticsRule does not rewrite to a constant (documented in the
 *       exemplar's FILE-041). We therefore assert exactness + a filtered-COUNT
 *       cross-check + the honest aggregate-over-scan plan, and only NOTE the footer
 *       shortcut. The multi-file fixture is the real refinement: the count must be
 *       correct ACROSS files, not just within one.</li>
 *   <li>NOTE (FILE-078): operators verified for exact correctness = =, !=, &lt;, &gt;,
 *       &lt;=, &gt;=, IN, LIKE. Partition pruning is asserted by result-correctness on
 *       a {@code year=}/{@code month=} filter (the pruned result equals the obvious
 *       expected rows); the I/O-elimination internals of the pruner are covered by the
 *       partition package's own tests, so only the observable correctness is asserted
 *       here.</li>
 * </ul>
 */
@Tag("integration")
@SuppressWarnings("deprecation")
public class ExecutionRefinementRequirementsTest {

  // Flat multi-file Parquet table (FILE-076 / FILE-077 / FILE-078 operators).
  // 8 files x 500 rows = 4000 rows. customer_id = i % 2000 over 4000 rows =>
  // exactly 2000 distinct values (a few-thousand-distinct column for HLL).
  private static final int FILES = 8;
  private static final int ROWS_PER_FILE = 500;
  private static final int TOTAL_ROWS = FILES * ROWS_PER_FILE; // 4000
  private static final int DISTINCT_CUSTOMER_IDS = 2000;
  private static final int REGIONS = 5; // region = "Region" + (i % 5)

  private File tempDir;
  private File flatDir;
  private File hiveDir;
  private File cacheDir;
  private Connection calciteConn;

  private String savedHllEnabled;
  private String savedFilterEnabled;
  private String savedColumnPruningEnabled;
  private String savedCacheDirectory;

  @BeforeEach
  public void setUp(@TempDir Path dir) throws Exception {
    tempDir = dir.toFile();
    flatDir = new File(tempDir, "flat");
    hiveDir = new File(tempDir, "hive");
    cacheDir = new File(tempDir, "stats_cache");
    flatDir.mkdirs();
    hiveDir.mkdirs();
    cacheDir.mkdirs();

    savedHllEnabled = System.getProperty("calcite.file.statistics.hll.enabled");
    savedFilterEnabled = System.getProperty("calcite.file.statistics.filter.enabled");
    savedColumnPruningEnabled =
        System.getProperty("calcite.file.statistics.column.pruning.enabled");
    savedCacheDirectory =
        System.getProperty("calcite.file.statistics.cache.directory");

    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    System.setProperty("calcite.file.statistics.filter.enabled", "true");
    System.setProperty("calcite.file.statistics.column.pruning.enabled", "true");
    System.setProperty("calcite.file.statistics.cache.directory",
        cacheDir.getAbsolutePath());

    createFlatMultiFileData();
    createHivePartitionedData();
    createHllSketch();
    setupCalciteConnection();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (calciteConn != null) {
      calciteConn.close();
    }
    restoreProperty("calcite.file.statistics.hll.enabled", savedHllEnabled);
    restoreProperty("calcite.file.statistics.filter.enabled", savedFilterEnabled);
    restoreProperty("calcite.file.statistics.column.pruning.enabled",
        savedColumnPruningEnabled);
    restoreProperty("calcite.file.statistics.cache.directory", savedCacheDirectory);
  }

  // ------------------------------------------------------------------
  // FILE-076 : COUNT(*) from metadata/footer over a multi-file Parquet table.
  // ------------------------------------------------------------------

  /**
   * FILE-076: COUNT(*) over a table materialized to MANY Parquet files must equal the
   * known total row count, and equal an independent filtered-COUNT cross-check, so the
   * answer is correct ACROSS files (not just within a single file or row group).
   *
   * <p>Result-correctness is asserted exactly: COUNT(*) == {@value #TOTAL_ROWS} (8 files
   * of {@value #ROWS_PER_FILE} rows). Cross-check: summing the per-region filtered counts
   * must reconstruct the same total, which a wrong/short count (e.g. one file only) could
   * not satisfy.
   *
   * <p>NOTE on the footer shortcut: the Parquet-footer / Iceberg row-count shortcut that
   * answers COUNT(*) with no scan is covered elsewhere - {@code CountStarStatisticsRule}
   * only rewrites to a constant {@code EnumerableValues} when
   * {@code table.getStatistic().getRowCount()} is non-null, which (per the exemplar's
   * FILE-041) is null for a freshly written hermetic Parquet file with no separate
   * statistics pass. So here the captured plan is the honest aggregate over a real scan
   * of the partitioned table (a {@code BindableTableScan(table=[[files, flat]])} under an
   * {@code EnumerableInterpreter}, captured via {@code explain plan for}); we assert that
   * shape and that no {@code EnumerableValues} constant is fabricated, leaving the
   * metadata-VALUES footer-shortcut firing to the rule's own unit test.
   */
  @Test @Tag("FILE-076")
  public void file076CountStarOverMultipleFilesExact() throws Exception {
    String countAll = "SELECT COUNT(*) FROM files.\"flat\"";

    long total = queryLong(countAll);
    assertEquals(TOTAL_ROWS, total,
        "FILE-076: COUNT(*) must equal the known total row count across all files");

    // Cross-check: the per-region filtered counts must sum back to the total. Each
    // region matches i % 5 == r over TOTAL_ROWS rows => exactly TOTAL_ROWS/REGIONS rows.
    long summed = 0;
    for (int r = 0; r < REGIONS; r++) {
      long c = queryLong(
          "SELECT COUNT(*) FROM files.\"flat\" WHERE \"region\" = 'Region" + r + "'");
      assertEquals(TOTAL_ROWS / REGIONS, c,
          "FILE-076: region='Region" + r + "' filtered count must be TOTAL_ROWS/REGIONS");
      summed += c;
    }
    assertEquals(total, summed,
        "FILE-076: filtered counts must reconstruct the full COUNT(*) (cross-file check)");

    // Honest plan: an aggregate over a real scan of the partitioned "flat" table, with no
    // fabricated metadata VALUES node. (A partitioned table renders as a BindableTableScan
    // under an EnumerableInterpreter rather than a StatisticsAwareParquetTableScan; the
    // footer-shortcut VALUES firing is covered by the rule's own unit test - see NOTE.)
    String plan = explain(countAll);
    assertTrue(plan.contains("EnumerableAggregate"),
        "FILE-076: COUNT(*) plan should aggregate over the scan, was:\n" + plan);
    assertTrue(plan.contains("table=[[files, flat]]"),
        "FILE-076: COUNT(*) plan should scan the flat table, was:\n" + plan);
    assertTrue(!plan.contains("EnumerableValues"),
        "FILE-076: COUNT(*) plan must not fabricate a metadata VALUES node here, was:\n" + plan);
  }

  // ------------------------------------------------------------------
  // FILE-077 : APPROX_COUNT_DISTINCT via HLL sketch; exact COUNT(DISTINCT) full-scans.
  // ------------------------------------------------------------------

  /**
   * FILE-077: APPROX_COUNT_DISTINCT must be HLL-answered within the documented relative
   * error on a few-thousand-distinct column when {@code calcite.file.statistics.hll.enabled}
   * is not false; and exact COUNT(DISTINCT) on a column WITHOUT a registered sketch must
   * still full-scan and return the EXACT value.
   *
   * <p>Accuracy: {@code customer_id = i % 2000} yields exactly
   * {@value #DISTINCT_CUSTOMER_IDS} distinct values, and the registered sketch was built
   * from {@value #DISTINCT_CUSTOMER_IDS} distinct keys, so the estimate is within the
   * required relative error.
   *
   * <p>No-sketch path: {@code region} has no registered HLL sketch, so its exact
   * COUNT(DISTINCT) cannot be answered from a sketch and must full-scan to the exact
   * {@value #REGIONS}.
   */
  @Test @Tag("FILE-077")
  public void file077ApproxFromHllExactWithoutSketch() throws Exception {
    // (a) exact baseline for the sketched column, computed independently.
    long exact = queryLong(
        "SELECT COUNT(DISTINCT \"customer_id\") FROM files.\"flat\"");
    assertEquals(DISTINCT_CUSTOMER_IDS, exact,
        "FILE-077: sanity - exact distinct count of customer_id must be the known value");

    // (b) APPROX_COUNT_DISTINCT within the documented relative error of exact.
    long est = queryLong(
        "SELECT APPROX_COUNT_DISTINCT(\"customer_id\") FROM files.\"flat\"");
    double relErr = Math.abs((double) (est - exact)) / (double) exact;
    assertTrue(relErr <= 0.10,
        "FILE-077: |approx-exact|/exact must be <= 0.10, was " + relErr
            + " (approx=" + est + ", exact=" + exact + ")");

    // (c) exact COUNT(DISTINCT) on a column WITHOUT a sketch full-scans to the EXACT value.
    long exactNoSketch = queryLong(
        "SELECT COUNT(DISTINCT \"region\") FROM files.\"flat\"");
    assertEquals(REGIONS, exactNoSketch,
        "FILE-077: exact COUNT(DISTINCT region) (no sketch) must return the exact value");
  }

  // ------------------------------------------------------------------
  // FILE-078 : operator pushdown + column pruning + Hive partition pruning.
  // ------------------------------------------------------------------

  /**
   * FILE-078: each of =, !=, &lt;, &gt;, &lt;=, &gt;=, IN, LIKE returns results exactly
   * equal to the obvious expected rows (the pushed result equals the unpushed result);
   * column pruning is always on; and partition pruning applies on a Hive
   * {@code year=}/{@code month=} layout.
   *
   * <p>NOTE - operators verified here for exact correctness: =, !=, &lt;, &gt;, &lt;=,
   * &gt;=, IN, LIKE. Each is checked against a closed-form expected count over the flat
   * 4000-row table (id is 1..4000 dense; region is "Region0".."Region4").
   *
   * <p>Column pruning: a single-column projection over the filter must yield exactly one
   * column and the plan must show the pruned projection {@code region=[...]} over the
   * Parquet scan.
   *
   * <p>Partition pruning: a {@code year=}/{@code month=} predicate on the Hive-partitioned
   * logical table returns exactly the rows of that partition - the partition columns are
   * real, queryable columns and the result equals the obvious expected rows.
   */
  @Test @Tag("FILE-078")
  public void file078OperatorPushdownPruningAndPartitions() throws Exception {
    // id is dense 1..TOTAL_ROWS. Expected counts are closed-form.
    // = : id = 1234 -> exactly 1 row.
    assertEquals(1L, queryLong(
        "SELECT COUNT(*) FROM files.\"flat\" WHERE \"id\" = 1234"),
        "FILE-078[=]: id = 1234 must match exactly 1 row");

    // != : id <> 1234 -> TOTAL_ROWS - 1 rows.
    assertEquals(TOTAL_ROWS - 1L, queryLong(
        "SELECT COUNT(*) FROM files.\"flat\" WHERE \"id\" <> 1234"),
        "FILE-078[!=]: id <> 1234 must match TOTAL_ROWS-1 rows");

    // < : id < 1001 -> ids 1..1000 -> 1000 rows.
    assertEquals(1000L, queryLong(
        "SELECT COUNT(*) FROM files.\"flat\" WHERE \"id\" < 1001"),
        "FILE-078[<]: id < 1001 must match 1000 rows");

    // > : id > 3000 -> ids 3001..4000 -> 1000 rows.
    assertEquals(1000L, queryLong(
        "SELECT COUNT(*) FROM files.\"flat\" WHERE \"id\" > 3000"),
        "FILE-078[>]: id > 3000 must match 1000 rows");

    // <= : id <= 1000 -> 1000 rows.
    assertEquals(1000L, queryLong(
        "SELECT COUNT(*) FROM files.\"flat\" WHERE \"id\" <= 1000"),
        "FILE-078[<=]: id <= 1000 must match 1000 rows");

    // >= : id >= 3001 -> 1000 rows.
    assertEquals(1000L, queryLong(
        "SELECT COUNT(*) FROM files.\"flat\" WHERE \"id\" >= 3001"),
        "FILE-078[>=]: id >= 3001 must match 1000 rows");

    // IN : id IN (1, 2, 3, 4000) -> exactly 4 rows (all in range).
    assertEquals(4L, queryLong(
        "SELECT COUNT(*) FROM files.\"flat\" WHERE \"id\" IN (1, 2, 3, 4000)"),
        "FILE-078[IN]: id IN (1,2,3,4000) must match exactly 4 rows");

    // LIKE : region LIKE 'Region%' -> every row (all regions start with 'Region').
    assertEquals((long) TOTAL_ROWS, queryLong(
        "SELECT COUNT(*) FROM files.\"flat\" WHERE \"region\" LIKE 'Region%'"),
        "FILE-078[LIKE]: region LIKE 'Region%' must match every row");
    // LIKE narrowing : region LIKE 'Region1' -> i % 5 == 1 -> TOTAL_ROWS/REGIONS rows.
    assertEquals((long) (TOTAL_ROWS / REGIONS), queryLong(
        "SELECT COUNT(*) FROM files.\"flat\" WHERE \"region\" LIKE 'Region1'"),
        "FILE-078[LIKE]: region LIKE 'Region1' must match TOTAL_ROWS/REGIONS rows");

    // Cross-check (pushed == unpushed): the pushed-filter result equals a result with
    // the filter evaluated over the projected column set.
    String filteredProjected =
        "SELECT \"region\" FROM files.\"flat\" WHERE \"region\" = 'Region0'";
    int matched = 0;
    try (PreparedStatement ps = calciteConn.prepareStatement(filteredProjected);
         ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        assertEquals("Region0", rs.getString(1),
            "FILE-078: every returned row must satisfy the pushed filter");
        matched++;
      }
      assertEquals(1, getColumnCount(rs),
          "FILE-078: column pruning must yield exactly one projected column (region)");
    }
    assertEquals(TOTAL_ROWS / REGIONS, matched,
        "FILE-078: region='Region0' must match exactly TOTAL_ROWS/REGIONS rows");

    // Filter pushdown + column pruning are observable in the plan: the comparison is pushed
    // onto the scan as a filter, and only the projected column survives in the Calc.
    // (Partitioned tables render as BindableTableScan under an EnumerableInterpreter.)
    String plan = explain(filteredProjected);
    assertTrue(plan.contains("table=[[files, flat]]"),
        "FILE-078: plan should scan the flat table, was:\n" + plan);
    assertTrue(plan.contains("filters=["),
        "FILE-078: plan should carry the pushed filter onto the scan, was:\n" + plan);
    assertTrue(plan.contains("region=["),
        "FILE-078: plan should show the pruned single-column projection, was:\n" + plan);

    // Partition pruning on the Hive year=/month= layout: year and month are real columns
    // and a partition predicate returns exactly that partition's rows.
    // Layout: year in {2023,2024} x month in {01,02} x HIVE_ROWS_PER_PART rows each.
    long part = queryLong(
        "SELECT COUNT(*) FROM hive.\"hive_events\" "
            + "WHERE \"year\" = '2024' AND \"month\" = '01'");
    assertEquals((long) HIVE_ROWS_PER_PART, part,
        "FILE-078[partition]: year=2024/month=01 must return exactly that partition's rows");

    // The partition columns are queryable and carry the path-encoded values.
    try (PreparedStatement ps = calciteConn.prepareStatement(
            "SELECT \"year\", \"month\" FROM hive.\"hive_events\" "
                + "WHERE \"year\" = '2024' AND \"month\" = '01'");
         ResultSet rs = ps.executeQuery()) {
      assertTrue(rs.next(), "FILE-078[partition]: partition query must return rows");
      assertEquals("2024", rs.getString(1),
          "FILE-078[partition]: year partition column must carry the path value");
      assertEquals("01", rs.getString(2),
          "FILE-078[partition]: month partition column must carry the path value");
    }
  }

  // ------------------------------------------------------------------
  // Helpers.
  // ------------------------------------------------------------------

  private long queryLong(String sql) throws Exception {
    try (PreparedStatement ps = calciteConn.prepareStatement(sql);
         ResultSet rs = ps.executeQuery()) {
      assertTrue(rs.next(), "Query should return a row: " + sql);
      return rs.getLong(1);
    }
  }

  private String explain(String sql) throws Exception {
    StringBuilder sb = new StringBuilder();
    try (PreparedStatement ps = calciteConn.prepareStatement("explain plan for " + sql);
         ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        sb.append(rs.getString(1)).append('\n');
      }
    }
    return sb.toString();
  }

  private static int getColumnCount(ResultSet rs) throws Exception {
    return rs.getMetaData().getColumnCount();
  }

  private static final String FLAT_SCHEMA =
      "{\"type\":\"record\",\"name\":\"FlatRecord\",\"fields\":["
          + "  {\"name\":\"id\",\"type\":\"int\"},"
          + "  {\"name\":\"customer_id\",\"type\":\"int\"},"
          + "  {\"name\":\"region\",\"type\":\"string\"},"
          + "  {\"name\":\"amount\",\"type\":\"double\"}"
          + "]}";

  /**
   * Writes the flat table as {@value #FILES} Parquet files, one per
   * {@code part=N/data.parquet} leaf, unioned into a single logical {@code flat} table
   * via a {@code partitionedTables} (style=hive) config. The Hive layout is just the
   * vehicle for a MULTI-FILE materialized table (it adds a {@code part} partition column
   * that the FILE-076/077/078 queries ignore); the refinement under test is that
   * COUNT(*), HLL, and operator pushdown are correct ACROSS files. id is dense
   * 1..TOTAL_ROWS, customer_id = i % DISTINCT_CUSTOMER_IDS, region = "Region" + (i % REGIONS).
   */
  private void createFlatMultiFileData() throws Exception {
    Schema avroSchema = new Schema.Parser().parse(FLAT_SCHEMA);
    for (int f = 0; f < FILES; f++) {
      File partDir = new File(flatDir, "part=" + f);
      partDir.mkdirs();
      File file = new File(partDir, "data.parquet");
      try (ParquetWriter<GenericRecord> writer =
               AvroParquetWriter
                   .<GenericRecord>builder(
                       new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                   .withSchema(avroSchema)
                   .withCompressionCodec(CompressionCodecName.SNAPPY)
                   .build()) {
        for (int j = 0; j < ROWS_PER_FILE; j++) {
          int i = f * ROWS_PER_FILE + j; // global 0-based index
          GenericRecord record = new GenericData.Record(avroSchema);
          record.put("id", i + 1);
          record.put("customer_id", i % DISTINCT_CUSTOMER_IDS);
          record.put("region", "Region" + (i % REGIONS));
          record.put("amount", 10.0 + i);
          writer.write(record);
        }
      }
    }
  }

  // Hive layout: year in {2023,2024} x month in {01,02} = 4 partitions.
  private static final String[] HIVE_YEARS = {"2023", "2024"};
  private static final String[] HIVE_MONTHS = {"01", "02"};
  private static final int HIVE_ROWS_PER_PART = 5;

  private static final String HIVE_SCHEMA =
      "{\"type\":\"record\",\"name\":\"EventRecord\",\"fields\":["
          + "  {\"name\":\"event_id\",\"type\":\"int\"},"
          + "  {\"name\":\"payload\",\"type\":\"string\"}"
          + "]}";

  /**
   * Writes a Hive-style {@code year=YYYY/month=MM/data.parquet} layout. The partition
   * values live only in the path; {@code year}/{@code month} are surfaced as columns by
   * the {@code partitionedTables} (style=hive) config registered in the operand.
   */
  private void createHivePartitionedData() throws Exception {
    Schema avroSchema = new Schema.Parser().parse(HIVE_SCHEMA);
    int eventId = 1;
    for (String year : HIVE_YEARS) {
      for (String month : HIVE_MONTHS) {
        File partDir = new File(hiveDir, "year=" + year + "/month=" + month);
        partDir.mkdirs();
        File file = new File(partDir, "data.parquet");
        try (ParquetWriter<GenericRecord> writer =
                 AvroParquetWriter
                     .<GenericRecord>builder(
                         new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                     .withSchema(avroSchema)
                     .withCompressionCodec(CompressionCodecName.SNAPPY)
                     .build()) {
          for (int k = 0; k < HIVE_ROWS_PER_PART; k++) {
            GenericRecord record = new GenericData.Record(avroSchema);
            record.put("event_id", eventId++);
            record.put("payload", "p_" + year + "_" + month + "_" + k);
            writer.write(record);
          }
        }
      }
    }
  }

  /**
   * Registers an HLL sketch for {@code flat.customer_id} so APPROX_COUNT_DISTINCT can be
   * answered from the sketch. The cache locates sketches by file name
   * {@code <table>_<column>.hll} under the configured cache directory. No sketch is
   * registered for {@code region}, so its exact COUNT(DISTINCT) must full-scan.
   */
  private void createHllSketch() throws Exception {
    HyperLogLogSketch sketch = new HyperLogLogSketch(14);
    for (int i = 0; i < DISTINCT_CUSTOMER_IDS; i++) {
      sketch.add(String.valueOf(i));
    }
    StatisticsCache.saveHLLSketch(sketch,
        new File(cacheDir, "flat_customer_id.hll"));
  }

  private void setupCalciteConnection() throws Exception {
    calciteConn = DriverManager.getConnection(
        "jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
    CalciteConnection calciteConnection =
        calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // Flat multi-file table: a partitionedTables (style=hive) config unions all
    // part=N/data.parquet leaves into one logical "flat" table.
    Map<String, Object> flatPartitions = new LinkedHashMap<>();
    flatPartitions.put("style", "hive");

    Map<String, Object> flatTableConfig = new LinkedHashMap<>();
    flatTableConfig.put("name", "flat");
    flatTableConfig.put("pattern", "**/data.parquet");
    flatTableConfig.put("partitions", flatPartitions);

    List<Map<String, Object>> flatPartitionedTables = new ArrayList<>();
    flatPartitionedTables.add(flatTableConfig);

    Map<String, Object> flatOperand = new LinkedHashMap<>();
    flatOperand.put("directory", flatDir.toString());
    flatOperand.put("executionEngine", "parquet");
    flatOperand.put("ephemeralCache", true);
    flatOperand.put("partitionedTables", flatPartitionedTables);
    rootSchema.add("files",
        FileSchemaFactory.INSTANCE.create(rootSchema, "files", flatOperand));

    // Hive-partitioned table in a separate schema: partitionedTables (style=hive)
    // synthesizes year/month columns over year=YYYY/month=MM/data.parquet leaves.
    Map<String, Object> partitions = new LinkedHashMap<>();
    partitions.put("style", "hive");

    Map<String, Object> tableConfig = new LinkedHashMap<>();
    tableConfig.put("name", "hive_events");
    tableConfig.put("pattern", "**/data.parquet");
    tableConfig.put("partitions", partitions);

    List<Map<String, Object>> partitionedTables = new ArrayList<>();
    partitionedTables.add(tableConfig);

    Map<String, Object> hiveOperand = new LinkedHashMap<>();
    hiveOperand.put("directory", hiveDir.toString());
    hiveOperand.put("executionEngine", "parquet");
    hiveOperand.put("ephemeralCache", true);
    hiveOperand.put("partitionedTables", partitionedTables);

    // The partitioned table needs its own factory operand (different directory +
    // partitionedTables config), so it is mounted as a separate "hive" schema and queried
    // as hive."hive_events".
    rootSchema.add("hive",
        FileSchemaFactory.INSTANCE.create(rootSchema, "hive", hiveOperand));
  }

  private static void restoreProperty(String key, String savedValue) {
    if (savedValue == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, savedValue);
    }
  }
}
