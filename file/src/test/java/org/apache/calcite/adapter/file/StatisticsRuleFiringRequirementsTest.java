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

import org.apache.calcite.adapter.file.statistics.ColumnStatistics;
import org.apache.calcite.adapter.file.statistics.ParquetStatisticsExtractor;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Requirement-pinning tests for the file-adapter STATISTICS goldens and the
 * OPTIMIZATION-RULE firing conditions. Each method is tagged with its
 * requirement id (FILE-043 / FILE-122 / FILE-123).
 *
 * <p>Harness is copied from {@code OptimizationRuleRequirementsTest} and the
 * file-adapter rule exemplars ({@code CountStarStatisticsRuleTest},
 * {@code SimpleFileFilterPushdownRuleTest}): a temp directory of Parquet files
 * written with {@code AvroParquetWriter}, wrapped in a {@code FileSchemaFactory}
 * schema with {@code executionEngine=parquet} and {@code ephemeralCache=true},
 * queried through a {@code lex=ORACLE;unquotedCasing=TO_LOWER} JDBC connection
 * with prepared statements and {@code explain plan for ...}.
 *
 * <p>EXPLAIN marker strings used below are the real ones rendered by this stack
 * (captured with a throwaway probe; not invented):
 * <ul>
 *   <li>The Parquet scan renders as
 *       {@code StatisticsAwareParquetTableScan(...)}.</li>
 *   <li>A rule that folds an aggregate / filter to a constant relation emits an
 *       {@code EnumerableValues(...)} node with no scan node.</li>
 *   <li>A grand-total aggregate renders as {@code EnumerableAggregate(group=[{}]...)}.</li>
 * </ul>
 *
 * <p>{@code @Tag("integration")} matches every exemplar this re-codes (the
 * parquet/EXPLAIN methods spin up the full Parquet/JDBC stack). FILE-043 reads
 * statistics directly from the extractor and needs no JDBC stack, but is left in
 * the same integration class for cohesion.
 */
@Tag("integration")
public class StatisticsRuleFiringRequirementsTest {

  // 12 rows; col_int values 10,20,...,100,null,null => min 10, max 100,
  // 2 nulls, 10 distinct non-null values. col_str "a".."j" + 2 nulls.
  private static final int ROW_COUNT = 12;
  private static final int NON_NULL = 10;
  private static final int NULL_COUNT = 2;
  private static final long MIN_INT = 10L;
  private static final long MAX_INT = 100L;

  private File tempDir;
  private File cacheDir;
  private Connection calciteConn;

  private String savedFilterEnabled;
  private String savedCacheDirectory;

  @BeforeEach
  public void setUp(@TempDir Path dir) throws Exception {
    tempDir = dir.toFile();
    cacheDir = new File(tempDir, "stats_cache");
    cacheDir.mkdirs();

    savedFilterEnabled = System.getProperty("calcite.file.statistics.filter.enabled");
    savedCacheDirectory = System.getProperty("calcite.file.statistics.cache.directory");

    System.setProperty("calcite.file.statistics.filter.enabled", "true");
    System.setProperty("calcite.file.statistics.cache.directory",
        cacheDir.getAbsolutePath());

    createTestData();
    setupCalciteConnection();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (calciteConn != null) {
      calciteConn.close();
    }
    restoreProperty("calcite.file.statistics.filter.enabled", savedFilterEnabled);
    restoreProperty("calcite.file.statistics.cache.directory", savedCacheDirectory);
  }

  // ------------------------------------------------------------------
  // FILE-043 : ParquetStatisticsExtractor / ColumnStatistics goldens.
  // ------------------------------------------------------------------

  /**
   * FILE-043: the extractor must read CORRECT min / max / null-count from a
   * Parquet file with known values (including nulls), and ColumnStatistics must
   * expose them. NDV is asserted as the documented small-N value.
   *
   * <p>Goldens for {@code col_int}: min={@value #MIN_INT}, max={@value #MAX_INT},
   * null-count={@value #NULL_COUNT}, non-null rows={@value #NON_NULL}, total
   * rows={@value #ROW_COUNT}.
   *
   * <p>NOTE on NDV: {@link ParquetStatisticsExtractor} never attaches an HLL
   * sketch (it only reads Parquet footer min/max/null statistics), so
   * {@link ColumnStatistics#getDistinctCount()} returns the documented no-sketch
   * fallback {@code Math.min(1000, totalCount)} rather than a true HLL NDV. For
   * small N that fallback equals the exact row count, which we assert here as an
   * exact bound. The HLL-backed NDV path (within HLL error) is covered by the
   * extractor-independent sketch tests ({@code StatisticsRequirementsTest},
   * {@code HyperLogLogSketchTest}).
   */
  @Test @Tag("FILE-043")
  public void file043ExtractorMinMaxNullCountGoldens() throws Exception {
    File parquet = new File(tempDir, "stats_test.parquet");

    Map<String, ParquetStatisticsExtractor.ColumnStatsBuilder> builders =
        ParquetStatisticsExtractor.extractStatistics(parquet);

    ParquetStatisticsExtractor.ColumnStatsBuilder intBuilder = builders.get("col_int");
    assertNotNull(intBuilder, "FILE-043: extractor must produce a builder for col_int");

    ColumnStatistics intStats = intBuilder.build();

    // Parquet stores INT32 min/max as Integer; compare numerically to be type-safe.
    assertNotNull(intStats.getMinValue(), "FILE-043: min must be present");
    assertNotNull(intStats.getMaxValue(), "FILE-043: max must be present");
    assertEquals(MIN_INT, ((Number) intStats.getMinValue()).longValue(),
        "FILE-043: col_int min must be exact");
    assertEquals(MAX_INT, ((Number) intStats.getMaxValue()).longValue(),
        "FILE-043: col_int max must be exact");
    assertEquals(NULL_COUNT, intStats.getNullCount(),
        "FILE-043: col_int null-count must be exact");
    assertEquals(ROW_COUNT, intStats.getTotalCount(),
        "FILE-043: col_int total-count must be exact");

    // NDV: no HLL sketch from the extractor => documented Math.min(1000,total).
    assertEquals(Math.min(1000L, (long) ROW_COUNT), intStats.getDistinctCount(),
        "FILE-043: NDV must equal the documented no-sketch small-N fallback");

    // String column min/max are decoded from Binary to UTF-8 String.
    ParquetStatisticsExtractor.ColumnStatsBuilder strBuilder = builders.get("col_str");
    assertNotNull(strBuilder, "FILE-043: extractor must produce a builder for col_str");
    ColumnStatistics strStats = strBuilder.build();
    assertEquals("a", strStats.getMinValue(), "FILE-043: col_str min must be 'a'");
    assertEquals("j", strStats.getMaxValue(), "FILE-043: col_str max must be 'j'");
    assertEquals(NULL_COUNT, strStats.getNullCount(),
        "FILE-043: col_str null-count must be exact");
  }

  // ------------------------------------------------------------------
  // FILE-122 : CountStarStatisticsRule firing conditions.
  // ------------------------------------------------------------------

  /**
   * FILE-122: CountStarStatisticsRule must rewrite a TRUE COUNT(*) (empty
   * groupSet, single non-distinct COUNT with empty argList) to a metadata
   * VALUES node ONLY when the table's {@code Statistic.getRowCount()} is
   * non-null; it must NOT rewrite COUNT(col), COUNT(DISTINCT col) or grouped
   * COUNT.
   *
   * <p>This stack does not surface a non-null {@code rowCount} for a freshly
   * written hermetic Parquet file with no separate statistics-collection pass
   * (documented in {@code OptimizationRuleRequirementsTest#file041...}), so the
   * positive CountStar metadata-VALUES rewrite is NOT asserted via EXPLAIN here
   * (the captured COUNT(*) plan is a real aggregate over the scan); that firing
   * is covered by the rule's own unit tests
   * ({@code CountStarStatisticsRuleTest}, {@code CountStarStatisticsRuleDeepTest}).
   * What we pin here are the NEGATIVE firing conditions and result-correctness:
   * COUNT(col) and grouped COUNT (the shapes CountStar explicitly rejects -
   * non-empty argList and non-empty groupSet) retain a real
   * {@code EnumerableAggregate} over the {@code StatisticsAwareParquetTableScan}.
   *
   * <p>NOTE on COUNT(DISTINCT col): the captured plan shows it folded to
   * {@code EnumerableValues(tuples=[[{ 10 }]])}. CountStar does NOT do this (it
   * early-returns on {@code isDistinct()}); the fold is performed by the separate
   * distinct-count rule. So this is asserted for result-correctness only, with
   * the firing of the distinct fold owned by that rule's own tests
   * ({@code HLLCountDistinctRuleTest} and the simple-distinct variants).
   */
  @Test @Tag("FILE-122")
  public void file122CountStarFiringConditions() throws Exception {
    // (a) TRUE COUNT(*) result-correctness (exact row count).
    String countStar = "SELECT COUNT(*) FROM files.\"stats_test\"";
    assertEquals(ROW_COUNT, queryLong(countStar),
        "FILE-122: COUNT(*) must equal the known row count");

    // (b) COUNT(col): a single non-distinct COUNT but WITH an argument => the
    // CountStar rule must NOT fire. col_int has 2 nulls, so COUNT(col_int)
    // = NON_NULL, and the plan keeps a real aggregate over the scan.
    String countCol = "SELECT COUNT(\"col_int\") FROM files.\"stats_test\"";
    assertEquals(NON_NULL, queryLong(countCol),
        "FILE-122: COUNT(col) must skip nulls (exact)");
    assertAggregateNotFoldedToValues(countCol, "COUNT(col)");

    // (c) COUNT(DISTINCT col): result-correctness only (10 distinct non-null
    // values). The distinct fold is owned by the distinct-count rule, not
    // CountStar (see method NOTE).
    String countDistinct =
        "SELECT COUNT(DISTINCT \"col_int\") FROM files.\"stats_test\"";
    assertEquals(NON_NULL, queryLong(countDistinct),
        "FILE-122: COUNT(DISTINCT col) must be the exact distinct count");

    // (d) grouped COUNT: non-empty groupSet => the rule must NOT fire. col_grp
    // has 3 distinct groups.
    String groupedCount =
        "SELECT \"col_grp\", COUNT(*) FROM files.\"stats_test\" GROUP BY \"col_grp\"";
    String groupedPlan = explain(groupedCount);
    assertTrue(groupedPlan.contains("EnumerableAggregate"),
        "FILE-122: grouped COUNT must keep an aggregate, plan was:\n" + groupedPlan);
    // CountStar early-returns on a non-empty groupSet: the aggregate must keep a
    // non-empty group (rendered group=[{<idx>}]), not the grand-total group=[{}].
    assertFalse(groupedPlan.contains("group=[{}]"),
        "FILE-122: grouped COUNT must keep its non-empty group set, plan was:\n"
            + groupedPlan);
    assertTrue(groupedPlan.contains("StatisticsAwareParquetTableScan"),
        "FILE-122: grouped COUNT must aggregate over the Parquet scan, plan was:\n"
            + groupedPlan);
  }

  /**
   * Asserts the COUNT plan keeps a real aggregate over the Parquet scan (the
   * CountStar metadata-VALUES rewrite did NOT collapse it to a bare values node).
   */
  private void assertAggregateNotFoldedToValues(String sql, String label)
      throws Exception {
    String plan = explain(sql);
    assertTrue(plan.contains("EnumerableAggregate"),
        "FILE-122: " + label + " must keep an aggregate, plan was:\n" + plan);
    assertTrue(plan.contains("StatisticsAwareParquetTableScan"),
        "FILE-122: " + label + " must aggregate over the Parquet scan, plan was:\n"
            + plan);
  }

  // ------------------------------------------------------------------
  // FILE-123 : SimpleFileFilterPushdownRule always-false / always-true.
  // ------------------------------------------------------------------

  /**
   * FILE-123: SimpleFileFilterPushdownRule (ParquetTranslatableTable only) folds
   * an always-FALSE predicate (value outside Parquet min/max) to empty VALUES,
   * and an always-TRUE predicate to a bare scan (EQUALS is always-true only when
   * min == max == value).
   *
   * <p>{@code col_const} is the constant 7 in every row, so {@code min == max == 7};
   * therefore {@code col_const = 7} is the always-TRUE case and {@code col_int < 0}
   * (min is {@value #MIN_INT}) is the always-FALSE case.
   *
   * <p>NOTE on firing: this rule's operand is {@code LogicalFilter} directly over
   * {@code TableScan.noInputs()} and it returns an equivalent relation (bare scan
   * or empty VALUES) rather than a rule-specific physical node. In this end-to-end
   * Parquet/Enumerable stack the captured EXPLAIN for every one of these
   * predicates (always-true, always-false, never-true) is the SAME shape - an
   * {@code EnumerableCalc(...$condition=[...])} over the
   * {@code StatisticsAwareParquetTableScan} - i.e. the min/max fold is not
   * observable from the optimized physical plan. So per the requirement's
   * fallback we assert RESULT-CORRECTNESS only here (always-false -> empty
   * result, always-true -> full result, plus a never-true cross-check), and the
   * always-true/always-false elimination via Parquet min/max is covered by the
   * rule's own unit tests ({@code SimpleFileFilterPushdownRuleTest},
   * {@code SimpleFileFilterPushdownRuleDeepTest}).
   */
  @Test @Tag("FILE-123")
  public void file123FilterAlwaysFalseAndAlwaysTrue() throws Exception {
    // always-FALSE: col_int < 0 (min is 10) => empty result.
    String alwaysFalse =
        "SELECT COUNT(*) FROM files.\"stats_test\" WHERE \"col_int\" < 0";
    assertEquals(0, queryLong(alwaysFalse),
        "FILE-123: always-false predicate must return 0 rows");

    // Independent cross-check: the always-false predicate yields no rows.
    int falseRows = 0;
    try (PreparedStatement ps = calciteConn.prepareStatement(
             "SELECT \"col_int\" FROM files.\"stats_test\" WHERE \"col_int\" < 0");
         ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        falseRows++;
      }
    }
    assertEquals(0, falseRows,
        "FILE-123: always-false predicate must produce zero rows");

    // always-TRUE: col_const = 7 (min == max == 7) => every row.
    String alwaysTrue =
        "SELECT COUNT(*) FROM files.\"stats_test\" WHERE \"col_const\" = 7";
    assertEquals(ROW_COUNT, queryLong(alwaysTrue),
        "FILE-123: always-true predicate must return every row");

    // Cross-check: always-true filtered count equals the unfiltered count, and
    // the plan still scans the Parquet table (result-equivalence guarantee).
    assertEquals(queryLong("SELECT COUNT(*) FROM files.\"stats_test\""),
        queryLong(alwaysTrue),
        "FILE-123: always-true filtered count must equal the unfiltered count");
    assertTrue(explain(alwaysTrue).contains("StatisticsAwareParquetTableScan"),
        "FILE-123: the always-true query must still scan the Parquet table");
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
    try (PreparedStatement ps =
             calciteConn.prepareStatement("explain plan for " + sql);
         ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        sb.append(rs.getString(1)).append('\n');
      }
    }
    return sb.toString();
  }

  @SuppressWarnings("deprecation")
  private void createTestData() throws Exception {
    File file = new File(tempDir, "stats_test.parquet");

    String schemaString =
        "{\"type\":\"record\",\"name\":\"StatsRecord\",\"fields\":["
            + "  {\"name\":\"col_int\",\"type\":[\"null\",\"int\"]},"
            + "  {\"name\":\"col_str\",\"type\":[\"null\",\"string\"]},"
            + "  {\"name\":\"col_const\",\"type\":\"int\"},"
            + "  {\"name\":\"col_grp\",\"type\":\"int\"}"
            + "]}";

    Schema avroSchema = new Schema.Parser().parse(schemaString);

    try (ParquetWriter<GenericRecord> writer =
             AvroParquetWriter
                 .<GenericRecord>builder(
                     new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
      // 10 non-null rows: col_int 10,20,...,100 ; col_str "a".."j".
      for (int i = 0; i < NON_NULL; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("col_int", (i + 1) * 10);
        record.put("col_str", String.valueOf((char) ('a' + i)));
        record.put("col_const", 7);
        record.put("col_grp", i % 3);
        writer.write(record);
      }
      // 2 null rows for col_int / col_str.
      for (int i = 0; i < NULL_COUNT; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("col_int", null);
        record.put("col_str", null);
        record.put("col_const", 7);
        record.put("col_grp", i % 3);
        writer.write(record);
      }
    }
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
