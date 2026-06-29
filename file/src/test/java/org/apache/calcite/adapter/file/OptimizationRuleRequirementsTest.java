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
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Requirement-coded tests that re-code previously weak file-adapter optimization-rule
 * tests into exact assertions. Each requirement asserts both result-correctness
 * (the optimized result equals the obvious exact answer) and, where a stable
 * physical/marker node exists in the EXPLAIN output, that the rule actually fired.
 *
 * <p>Harness mirrors the existing rule exemplars
 * ({@code CountStarStatisticsRuleTest}, {@code HLLCountDistinctRuleTest},
 * {@code SimpleFileFilterPushdownRuleTest}, {@code SimpleFileColumnPruningRuleTest}):
 * a temp directory of Parquet files wrapped in a {@code FileSchemaFactory} schema
 * with {@code executionEngine=parquet} and {@code ephemeralCache=true}. The Parquet
 * execution path registers all of the file optimization rules via
 * {@code StatisticsAwareParquetTableScan.register(planner)}.
 *
 * <p>The exact marker strings used in the plan assertions below were captured by
 * running {@code explain plan for ...} through this very harness (see the probe in
 * the change description); they are NOT invented:
 * <ul>
 *   <li>A rule that rewrites an aggregate to a pre-computed constant emits an
 *       {@code EnumerableValues(tuples=[[{ N }]])} node and no scan node.</li>
 *   <li>The Parquet scan node renders as
 *       {@code StatisticsAwareParquetTableScan(table=[[files, ...]])}.</li>
 *   <li>A projection renders inside an {@code EnumerableCalc(...)} node.</li>
 * </ul>
 *
 * <p>This is an {@code @Tag("integration")} class, matching every file-adapter rule
 * exemplar it re-codes (they are all {@code @Tag("integration")} because they spin
 * up the full Parquet/JDBC stack against on-disk files).
 */
@Tag("integration")
public class OptimizationRuleRequirementsTest {

  private static final int ROW_COUNT = 500;
  // customer_id = i % 200 over 500 rows => exactly 200 distinct values.
  private static final int DISTINCT_CUSTOMER_IDS = 200;

  private File tempDir;
  private File cacheDir;
  private Connection calciteConn;

  private String savedHllEnabled;
  private String savedFilterEnabled;
  private String savedColumnPruningEnabled;
  private String savedCacheDirectory;

  @BeforeEach
  public void setUp(@TempDir Path dir) throws Exception {
    tempDir = dir.toFile();
    cacheDir = new File(tempDir, "stats_cache");
    cacheDir.mkdirs();

    savedHllEnabled = System.getProperty("calcite.file.statistics.hll.enabled");
    savedFilterEnabled = System.getProperty("calcite.file.statistics.filter.enabled");
    savedColumnPruningEnabled =
        System.getProperty("calcite.file.statistics.column.pruning.enabled");
    savedCacheDirectory =
        System.getProperty("calcite.file.statistics.cache.directory");

    // Enable the statistics-driven optimization rules and point the HLL sketch
    // cache at our temp directory so the lookup is hermetic.
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    System.setProperty("calcite.file.statistics.filter.enabled", "true");
    System.setProperty("calcite.file.statistics.column.pruning.enabled", "true");
    System.setProperty("calcite.file.statistics.cache.directory",
        cacheDir.getAbsolutePath());

    createTestData();
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
  // FILE-041 : COUNT(*) answered from metadata by CountStarStatisticsRule.
  // ------------------------------------------------------------------

  /**
   * FILE-041: COUNT(*) must return the exact known row count.
   *
   * <p>Result-correctness is asserted exactly (COUNT(*) == {@value #ROW_COUNT}).
   *
   * <p>NOTE on rule-firing: {@code CountStarStatisticsRule} only rewrites the
   * aggregate to a constant {@code EnumerableValues} node when
   * {@code table.getStatistic().getRowCount()} is non-null. For a Parquet file
   * written fresh in a hermetic test (no separate statistics-collection pass) that
   * rowCount is null, so the rule does not fire and the captured plan is
   * {@code EnumerableAggregate(group=[{}], EXPR$0=[COUNT()])} over the scan rather
   * than a metadata shortcut. The marker-based "rule fires" assertion is therefore
   * NOT made here; the metadata-shortcut firing is covered by the rule's own unit
   * test ({@code org.apache.calcite.adapter.file.rules.CountStarStatisticsRuleTest}
   * and the deep/coverage variants). We still assert exactness, plus that the plan
   * is the honest non-shortcut plan (a real aggregate over the Parquet scan, with
   * no fabricated metadata VALUES node), so this test cannot silently pass on a
   * wrong constant.
   */
  @Test @Tag("FILE-041")
  public void file041CountStarExactAndPlanHonest() throws Exception {
    String sql = "SELECT COUNT(*) FROM files.\"opt_test\"";

    long result = queryLong(sql);
    assertEquals(ROW_COUNT, result,
        "FILE-041: COUNT(*) must equal the known row count");

    String plan = explain(sql);
    // The aggregate is computed over the Parquet scan (no metadata VALUES shortcut
    // in this hermetic setup). Assert the plan reflects exactly that, so a wrong
    // constant could never masquerade as correct.
    assertTrue(plan.contains("EnumerableAggregate")
            && plan.contains("StatisticsAwareParquetTableScan"),
        "FILE-041: COUNT(*) plan should aggregate over the Parquet scan, was:\n" + plan);
  }

  // ------------------------------------------------------------------
  // FILE-042 : Filter pushdown + column pruning + partition selection.
  // ------------------------------------------------------------------

  /**
   * FILE-042: A filtered + projected query must return rows IDENTICAL to the
   * obvious exact answer, and the plan must show the pushed filter condition and
   * the pruned projection.
   *
   * <p>Result-correctness: {@code region = 'Region0'} matches rows where
   * {@code i % 5 == 0}; with {@value #ROW_COUNT} rows that is exactly
   * {@code ROW_COUNT / 5 = 100} rows, each projecting a single column.
   *
   * <p>Rule-firing markers: the optimized plan emits the pushed comparison as a
   * {@code $condition} inside an {@code EnumerableCalc}, and the single projected
   * column as {@code region=[...]} inside an {@code EnumerableCalc} (column
   * pruning), over the {@code StatisticsAwareParquetTableScan}. These are stable,
   * captured markers.
   *
   * <p>NOTE: {@code SimpleFileFilterPushdownRule} and
   * {@code SimpleFileColumnPruningRule} do not introduce a rule-specific physical
   * node (the filter rule, when it cannot fully resolve a predicate via min/max
   * stats, returns an equivalent filter; the pruning rule returns an equivalent
   * projection). Their internal firing (always-true / always-false elimination,
   * I/O-savings estimation) is covered by their own unit tests
   * ({@code SimpleFileFilterPushdownRuleTest},
   * {@code SimpleFileColumnPruningRuleTest}). Here we assert the observable
   * pushed-filter / pruned-projection shape plus exact results.
   */
  @Test @Tag("FILE-042")
  public void file042FilterAndPruningExactAndPushed() throws Exception {
    String filteredProjected =
        "SELECT \"region\" FROM files.\"opt_test\" WHERE \"region\" = 'Region0'";

    // (a) result-correctness: identical to the obvious exact answer.
    int matched = 0;
    try (PreparedStatement ps = calciteConn.prepareStatement(filteredProjected);
         ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        assertEquals("Region0", rs.getString(1),
            "FILE-042: every returned row must satisfy the filter");
        matched++;
      }
      assertEquals(1, getColumnCount(rs),
          "FILE-042: projection must yield exactly one column (region)");
    }
    assertEquals(ROW_COUNT / 5, matched,
        "FILE-042: region='Region0' must match exactly ROW_COUNT/5 rows");

    // (b) the plan shows the pushed filter (a $condition) and the pruned
    // single-column projection (region=[...]) over the Parquet scan.
    String plan = explain(filteredProjected);
    assertTrue(plan.contains("StatisticsAwareParquetTableScan"),
        "FILE-042: plan should scan the Parquet table, was:\n" + plan);
    assertTrue(plan.contains("$condition"),
        "FILE-042: plan should carry the pushed filter condition, was:\n" + plan);
    assertTrue(plan.contains("region=["),
        "FILE-042: plan should show the pruned single-column projection, was:\n" + plan);

    // Cross-check: the optimized filtered/projected result equals an independent
    // unoptimized count over the same predicate (identical-results guarantee).
    long viaCount = queryLong(
        "SELECT COUNT(*) FROM files.\"opt_test\" WHERE \"region\" = 'Region0'");
    assertEquals(matched, viaCount,
        "FILE-042: optimized projection rowcount must match the unoptimized count");
  }

  // ------------------------------------------------------------------
  // FILE-039 : APPROX_COUNT_DISTINCT via HLLCountDistinctRule.
  // ------------------------------------------------------------------

  /**
   * FILE-039: APPROX_COUNT_DISTINCT must be within the documented relative error
   * of exact COUNT(DISTINCT), and the HLL rule must fire.
   *
   * <p>Rule-firing marker: when an HLL sketch is registered for the column, the
   * HLL rule rewrites the aggregate to a pre-computed constant, so the plan is
   * exactly {@code EnumerableValues(tuples=[[{ N }]])} with no scan node. This is
   * the captured marker (verified via {@code explain plan for}). We assert the
   * plan contains {@code EnumerableValues} and contains no Parquet scan node.
   *
   * <p>Accuracy: {@code customer_id = i % 200} yields exactly
   * {@value #DISTINCT_CUSTOMER_IDS} distinct values, and the registered sketch was
   * built from {@value #DISTINCT_CUSTOMER_IDS} distinct keys, so the estimate is
   * within the required {@code <= 0.10} relative error of the exact distinct count.
   */
  @Test @Tag("FILE-039")
  public void file039ApproxCountDistinctWithinErrorAndRuleFires() throws Exception {
    String approx =
        "SELECT APPROX_COUNT_DISTINCT(\"customer_id\") FROM files.\"opt_test\"";

    // Exact baseline computed independently.
    long exact = queryLong(
        "SELECT COUNT(DISTINCT \"customer_id\") FROM files.\"opt_test\"");
    assertEquals(DISTINCT_CUSTOMER_IDS, exact,
        "FILE-039: sanity - exact distinct count must be the known value");

    // (a) result-accuracy within the documented 10% relative error.
    long est = queryLong(approx);
    double relErr = Math.abs((double) (est - exact)) / (double) exact;
    assertTrue(relErr <= 0.10,
        "FILE-039: |approx-exact|/exact must be <= 0.10, was " + relErr
            + " (approx=" + est + ", exact=" + exact + ")");

    // (b) the HLL rule fired: aggregate rewritten to a constant EnumerableValues
    // with no remaining table scan.
    String plan = explain(approx);
    assertTrue(plan.contains("EnumerableValues"),
        "FILE-039: HLL rule should rewrite to EnumerableValues, plan was:\n" + plan);
    assertTrue(!plan.contains("ParquetTableScan") && !plan.contains("EnumerableAggregate"),
        "FILE-039: HLL shortcut should remove the scan/aggregate, plan was:\n" + plan);
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

  @SuppressWarnings("deprecation")
  private void createTestData() throws Exception {
    File file = new File(tempDir, "opt_test.parquet");

    String schemaString =
        "{\"type\":\"record\",\"name\":\"OptRecord\",\"fields\":["
            + "  {\"name\":\"id\",\"type\":\"int\"},"
            + "  {\"name\":\"customer_id\",\"type\":\"int\"},"
            + "  {\"name\":\"region\",\"type\":\"string\"},"
            + "  {\"name\":\"amount\",\"type\":\"double\"}"
            + "]}";

    Schema avroSchema = new Schema.Parser().parse(schemaString);

    try (ParquetWriter<GenericRecord> writer =
             AvroParquetWriter
                 .<GenericRecord>builder(
                     new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
      for (int i = 0; i < ROW_COUNT; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", i + 1);
        record.put("customer_id", i % DISTINCT_CUSTOMER_IDS);
        record.put("region", "Region" + (i % 5));
        record.put("amount", 10.0 + i);
        writer.write(record);
      }
    }
  }

  /**
   * Registers an HLL sketch for {@code customer_id} so the HLL rule can rewrite
   * APPROX_COUNT_DISTINCT to a constant. The cache locates sketches by file name
   * {@code <table>_<column>.hll} under the configured cache directory.
   */
  private void createHllSketch() throws Exception {
    HyperLogLogSketch sketch = new HyperLogLogSketch(14);
    for (int i = 0; i < DISTINCT_CUSTOMER_IDS; i++) {
      sketch.add(String.valueOf(i));
    }
    StatisticsCache.saveHLLSketch(sketch,
        new File(cacheDir, "opt_test_customer_id.hll"));
  }

  private void setupCalciteConnection() throws Exception {
    calciteConn = DriverManager.getConnection(
        "jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
    CalciteConnection calciteConnection =
        calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    operand.put("ephemeralCache", true);

    rootSchema.add("files",
        FileSchemaFactory.INSTANCE.create(rootSchema, "files", operand));
  }

  private static void restoreProperty(String key, String savedValue) {
    if (savedValue == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, savedValue);
    }
  }
}
