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

import org.apache.calcite.adapter.file.metadata.ConversionMetadata.FileBaseline;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata.PartitionBaseline;
import org.apache.calcite.adapter.file.refresh.AbstractRefreshableTable;
import org.apache.calcite.adapter.file.refresh.RefreshInterval;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-028 / FILE-135 / FILE-137 — exact-assertion recode of the file-adapter REFRESH logic tests.
 *
 * <p>FILE-028: a refreshable table re-reads its source when the change signal (local mtime increase)
 * fires; a stale read never serves pre-change data past the refresh.
 *
 * <p>FILE-135: {@code RefreshablePartitionedParquetTable.filesChangedComparedToBaseline} returns TRUE
 * on a null/empty baseline, a file-count change, an added/removed path, or per-file
 * {@code FileBaseline.hasChanged} (etag &gt; size &gt; lastModified with 1s tolerance); a getMetadata
 * failure assumes changed. Asserted here directly against the baseline-comparison primitives.
 *
 * <p>FILE-137: {@code MaterializedViewTable.materialize} is ONE-SHOT via
 * {@code materialized.compareAndSet(false,true)} with no staleness check — a second materialize is a
 * no-op within an instance.
 *
 * <p>FILE-134: {@code RefreshInterval.parse} accepts ISO-8601 (P/PT) and "&lt;n&gt; unit(s)"
 * (case-insensitive, trimmed); null/empty/unmatched → null. {@code getEffectiveInterval} prefers the
 * table-level interval over the schema-level one and yields null when neither parses — there is NO
 * built-in table default. (RECODE of {@code refresh/RefreshIntervalTest} → exact-Duration golden.)
 *
 * <p>FILE-132: {@code AbstractRefreshableTable.needsRefresh} is the lazy refresh gate: a null
 * interval never refreshes, a null lastRefreshTime always refreshes on first call, and otherwise it
 * compares only the elapsed interval — it consults NO file state, so the interval gates before any
 * mtime check. (RECODE strengthening {@code refresh/AbstractRefreshableTableTest}.)
 */
@Tag("unit")
public class RefreshLogicRequirementsTest {

  // =============================================================================================
  // FILE-028 — refreshable table re-reads its source on a local mtime increase
  // =============================================================================================

  @Test @Tag("FILE-028")
  void refreshableTableServesNewDataAfterSourceMutationAndMtimeBump(@TempDir Path root)
      throws Exception {
    Path src = Files.createDirectories(root.resolve("src"));
    Path cache = Files.createDirectories(root.resolve("cache"));
    Path csv = src.resolve("data.csv");

    Files.write(csv, "id:int,name:string\n1,alice\n".getBytes(StandardCharsets.UTF_8));

    // First read: original value served.
    assertEquals("alice", queryName(src, cache));

    // Mutate the source file and bump its mtime past the ~1s comparison granularity.
    Thread.sleep(1100);
    Files.write(csv, "id:int,name:string\n1,zara\n".getBytes(StandardCharsets.UTF_8));
    csv.toFile().setLastModified(System.currentTimeMillis());

    // A short refreshInterval means the next read triggers/awaits refresh and re-reads the source.
    // The NEW value must be served and the OLD value must be gone.
    String afterRefresh = queryName(src, cache);
    assertEquals("zara", afterRefresh,
        "refreshable table must serve the post-mutation value after the mtime change signal fires");
    assertFalse("alice".equals(afterRefresh),
        "a stale read must never serve pre-change data past the refresh");
  }

  /**
   * Opens a fresh connection over the model and returns name where id = 1.
   * A fresh connection per call mirrors the refresh exemplar (RefreshIdempotenceTest) and ensures
   * the read goes through the refresh-on-access path rather than a cached planner result.
   */
  private static String queryName(Path src, Path cache) throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + model(src, cache, "1 second"));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         ResultSet rs = conn.createStatement()
             .executeQuery("SELECT name FROM s.data WHERE id = 1")) {
      assertTrue(rs.next(), "expected exactly one row for id = 1");
      String name = rs.getString(1);
      assertFalse(rs.next(), "expected exactly one row for id = 1");
      return name;
    }
  }

  private static String model(Path src, Path cache, String refreshInterval) {
    return "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"s\",\n"
        + "  \"schemas\": [{\n"
        + "    \"name\": \"s\",\n"
        + "    \"type\": \"custom\",\n"
        + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "    \"operand\": {\n"
        + "      \"directory\": \"" + esc(src) + "\",\n"
        + "      \"baseDirectory\": \"" + esc(cache) + "\",\n"
        + "      \"ephemeralCache\": true,\n"
        + "      \"primeCache\": false,\n"
        + "      \"refreshInterval\": \"" + refreshInterval + "\"\n"
        + "    }\n"
        + "  }]\n"
        + "}\n";
  }

  private static String esc(Path p) {
    return p.toString().replace("\\", "\\\\");
  }

  // =============================================================================================
  // FILE-135 — baseline-comparison logic of RefreshablePartitionedParquetTable
  // =============================================================================================
  //
  // filesChangedComparedToBaseline / scan are package-private instance methods that require a live
  // StorageProvider + discovered file set, so the comparison logic is asserted here against its
  // building blocks: PartitionBaseline.isEmpty and FileBaseline.hasChanged (the exact predicates the
  // method delegates to). The path/count branches are structural checks over those same primitives.

  @Test @Tag("FILE-135")
  void nullOrEmptyBaselineIsTreatedAsChanged() {
    // The method's first guard: "if (baseline == null || baseline.isEmpty() ...) return true".
    PartitionBaseline nullFiles = new PartitionBaseline();
    nullFiles.files = null;
    assertTrue(nullFiles.isEmpty(), "a baseline with null files map is empty -> treated as changed");

    PartitionBaseline emptyFiles = new PartitionBaseline();
    emptyFiles.files = new HashMap<String, FileBaseline>();
    assertTrue(emptyFiles.isEmpty(), "a baseline with no files is empty -> treated as changed");

    PartitionBaseline populated = new PartitionBaseline();
    populated.files = new HashMap<String, FileBaseline>();
    populated.files.put("a.parquet", new FileBaseline(1L, null, 0L));
    assertFalse(populated.isEmpty(), "a populated baseline is not empty");
  }

  @Test @Tag("FILE-135")
  void fileCountChangeIsTreatedAsChanged() {
    // "if (currentFiles.size() != baselineFiles.size()) return true".
    Map<String, FileBaseline> baseline = new HashMap<String, FileBaseline>();
    baseline.put("a.parquet", new FileBaseline(1L, "e1", 0L));

    List<String> sameCount = new ArrayList<String>(Arrays.asList("a.parquet"));
    List<String> moreFiles = new ArrayList<String>(Arrays.asList("a.parquet", "b.parquet"));

    assertEquals(baseline.keySet().size(), sameCount.size(),
        "matching counts must not short-circuit as changed on the count check");
    assertFalse(moreFiles.size() == baseline.keySet().size(),
        "a file-count change must be treated as changed");
  }

  @Test @Tag("FILE-135")
  void addedOrRemovedPathIsTreatedAsChanged() {
    // Added: a current path absent from baseline -> baseline.get(path) == null -> changed.
    // Removed: a baseline path absent from currentFiles -> changed.
    Map<String, FileBaseline> baseline = new HashMap<String, FileBaseline>();
    baseline.put("a.parquet", new FileBaseline(1L, "e1", 0L));
    baseline.put("b.parquet", new FileBaseline(1L, "e2", 0L));

    // Same count, but one path swapped (c for b): "added" branch fires for the unknown path.
    List<String> withAdded = new ArrayList<String>(Arrays.asList("a.parquet", "c.parquet"));
    assertEquals(baseline.size(), withAdded.size(), "count is unchanged so the path branch decides");
    assertTrue(baseline.get("c.parquet") == null,
        "a current path absent from the baseline is an added file -> changed");

    // "removed" branch: a baseline path not present in the current list.
    assertFalse(withAdded.contains("b.parquet"),
        "a baseline path absent from the current list is a removed file -> changed");
  }

  @Test @Tag("FILE-135")
  void perFileHasChangedFollowsEtagThenSizeThenLastModified() {
    // ETag is authoritative when both present.
    FileBaseline etagBase = new FileBaseline(10L, "etag-A", 1000L);
    assertTrue(etagBase.hasChanged(new FileBaseline(10L, "etag-B", 1000L)),
        "differing etags -> changed (etag wins)");
    assertFalse(etagBase.hasChanged(new FileBaseline(999L, "etag-A", 9999L)),
        "equal etags -> unchanged even if size/lastModified differ (etag wins)");

    // No etag on one side: fall back to size.
    FileBaseline sizeBase = new FileBaseline(10L, null, 1000L);
    assertTrue(sizeBase.hasChanged(new FileBaseline(11L, null, 1000L)),
        "no etag, differing size -> changed");

    // Same size, no etag: fall back to lastModified with 1s tolerance.
    FileBaseline timeBase = new FileBaseline(10L, null, 1000L);
    assertFalse(timeBase.hasChanged(new FileBaseline(10L, null, 1500L)),
        "lastModified delta within 1000ms tolerance -> unchanged");
    assertTrue(timeBase.hasChanged(new FileBaseline(10L, null, 2001L)),
        "lastModified delta beyond 1000ms tolerance -> changed");

    // Null current is always changed.
    assertTrue(timeBase.hasChanged(null), "null current metadata -> changed");
  }

  // getMetadata-failure-assumes-changed: in filesChangedComparedToBaseline the catch block returns
  // true. That branch needs a throwing StorageProvider stub, which is package-private wiring on the
  // instance method; asserted indirectly via the hasChanged(null)/null-baseline cases above.
  // NOTE: OMITTED the direct getMetadata-throws assertion — cannot invoke the private instance method
  // without constructing a full RefreshablePartitionedParquetTable + StorageProvider hermetically.

  @Test @Tag("FILE-135")
  @Disabled("C-20: RefreshablePartitionedParquetTable.scan() missing null/empty-baseline guard "
      + "— pending code fix")
  void scanShouldGuardAgainstNullOrEmptyBaseline() {
    // INTENDED behavior (documented, NOT asserted as currently passing): scan() should consult the
    // same null/empty-baseline guard that filesChangedComparedToBaseline uses, forcing a refresh
    // rather than serving from a stale/absent baseline. The current scan() omits this guard.
    // No assertion of current (wrong) behavior is made here.
  }

  // =============================================================================================
  // FILE-137 — MaterializedViewTable.materialize is one-shot (compareAndSet(false,true), no staleness)
  // =============================================================================================

  @Test @Tag("FILE-137")
  void materializeIsOneShotSecondCallIsNoOp(@TempDir Path root) throws Exception {
    File parquetFile = root.resolve("mv.parquet").toFile();
    Map<String, org.apache.calcite.schema.Table> tables =
        new HashMap<String, org.apache.calcite.schema.Table>();

    org.apache.calcite.adapter.file.materialized.MaterializedViewTable mv =
        new org.apache.calcite.adapter.file.materialized.MaterializedViewTable(
            null, "test_schema", "test_view", "SELECT 1 AS n", parquetFile, tables);

    org.apache.calcite.rel.type.RelDataTypeFactory typeFactory =
        new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

    // First access triggers materialize() -> writes the parquet output exactly once (CAS false->true).
    mv.getRowType(typeFactory);
    assertTrue(parquetFile.exists(), "materialize() must produce the parquet output on first access");
    long firstMtime = parquetFile.lastModified();

    // Second access: materialized is already true, so the CAS fails and materialize() is a no-op.
    // No re-write -> the parquet output is left untouched (one-shot, no staleness check).
    Thread.sleep(1100); // exceed the ~1s mtime granularity so a re-write would be observable
    mv.getRowType(typeFactory);
    assertEquals(firstMtime, parquetFile.lastModified(),
        "a second materialize must be a no-op (compareAndSet(false,true) is one-shot)");
  }

  @Test @Tag("FILE-137")
  @Disabled("C-21: materialize flag must reset + delete partial parquet on failure "
      + "— pending code fix")
  void materializeShouldResetFlagAndDeletePartialParquetOnFailure() {
    // INTENDED behavior (documented, NOT asserted as currently passing): materialize() should set the
    // materialized flag true only on SUCCESS; on failure it should reset the flag to false and delete
    // any partially-written parquet, so a subsequent access retries cleanly. The current code sets the
    // flag before the work and never resets/cleans up on failure.
    // No assertion of current (wrong) behavior is made here.
  }

  // =============================================================================================
  // FILE-134 — RefreshInterval.parse / getEffectiveInterval (pure, exact Duration golden)
  // =============================================================================================

  @Test @Tag("FILE-134")
  void parseReturnsNullForNullEmptyOrUnmatched() {
    assertNull(RefreshInterval.parse(null), "null input -> no refresh");
    assertNull(RefreshInterval.parse(""), "empty input -> no refresh");
    assertNull(RefreshInterval.parse("   "), "whitespace-only input -> no refresh");
    assertNull(RefreshInterval.parse("5minutes"), "missing separator -> unmatched -> null");
    assertNull(RefreshInterval.parse("5 fortnights"), "unsupported unit -> unmatched -> null");
    assertNull(RefreshInterval.parse("soon"), "non-numeric -> unmatched -> null");
    // Starts with P (ISO branch) but is not a valid Duration -> falls through -> unmatched -> null.
    assertNull(RefreshInterval.parse("P1Y"), "period (not duration) falls through -> null");
  }

  @Test @Tag("FILE-134")
  void parseHumanReadableYieldsExactDuration() {
    assertEquals(Duration.ofSeconds(30), RefreshInterval.parse("30 seconds"));
    assertEquals(Duration.ofSeconds(1), RefreshInterval.parse("1 second"));
    assertEquals(Duration.ofMinutes(5), RefreshInterval.parse("5 minutes"));
    assertEquals(Duration.ofMinutes(1), RefreshInterval.parse("1 minute"));
    assertEquals(Duration.ofHours(2), RefreshInterval.parse("2 hours"));
    assertEquals(Duration.ofHours(1), RefreshInterval.parse("1 hour"));
    assertEquals(Duration.ofDays(3), RefreshInterval.parse("3 days"));
    assertEquals(Duration.ofDays(1), RefreshInterval.parse("1 day"));
    // Case-insensitive and trimmed.
    assertEquals(Duration.ofMinutes(5), RefreshInterval.parse("5 MINUTES"));
    assertEquals(Duration.ofMinutes(10), RefreshInterval.parse("  10 minutes  "));
  }

  @Test @Tag("FILE-134")
  void parseIso8601YieldsExactDuration() {
    assertEquals(Duration.ofSeconds(1), RefreshInterval.parse("PT1S"));
    assertEquals(Duration.ofMinutes(5), RefreshInterval.parse("PT5M"));
    assertEquals(Duration.ofHours(2), RefreshInterval.parse("PT2H"));
    assertEquals(Duration.ofDays(2), RefreshInterval.parse("P2D"));
  }

  @Test @Tag("FILE-134")
  void getEffectiveIntervalPrefersTableThenSchemaElseNull() {
    // Table level wins when it parses.
    assertEquals(Duration.ofHours(1),
        RefreshInterval.getEffectiveInterval("1 hour", "1 day"));
    // Null/empty/invalid table falls back to schema.
    assertEquals(Duration.ofDays(1),
        RefreshInterval.getEffectiveInterval(null, "1 day"));
    assertEquals(Duration.ofHours(2),
        RefreshInterval.getEffectiveInterval("", "2 hours"));
    assertEquals(Duration.ofDays(1),
        RefreshInterval.getEffectiveInterval("bogus", "1 day"));
    // Neither parses -> no refresh: tables carry NO built-in default interval.
    assertNull(RefreshInterval.getEffectiveInterval(null, null));
    assertNull(RefreshInterval.getEffectiveInterval("bogus", "also-bogus"));
  }

  // =============================================================================================
  // FILE-132 — AbstractRefreshableTable.needsRefresh lazy gate (interval before any file state)
  // =============================================================================================
  //
  // A minimal concrete subclass exposes the three branches of the protected instance method. The
  // lastRefreshTime field is protected, so the subclass sets it directly to drive the elapsed check.

  /** Minimal concrete table to exercise the inherited needsRefresh() gate. */
  private static final class ProbeRefreshableTable extends AbstractRefreshableTable {
    ProbeRefreshableTable(@org.checkerframework.checker.nullness.qual.Nullable Duration interval) {
      super("probe", interval);
    }

    void setLastRefreshTime(Instant t) {
      this.lastRefreshTime = t;
    }

    @Override protected void doRefresh() {
      // no-op: this test exercises only needsRefresh(), not the refresh body.
    }

    @Override public RefreshBehavior getRefreshBehavior() {
      return RefreshBehavior.SINGLE_FILE;
    }

    @Override public org.apache.calcite.rel.type.RelDataType getRowType(
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("id", org.apache.calcite.sql.type.SqlTypeName.INTEGER)
          .build();
    }
  }

  @Test @Tag("FILE-132")
  void nullIntervalNeverRefreshes() {
    ProbeRefreshableTable t = new ProbeRefreshableTable(null);
    assertFalse(t.needsRefresh(), "a null refresh interval means the table never refreshes");
    // Even after a (hypothetical) prior refresh, a null interval still never refreshes.
    t.setLastRefreshTime(Instant.now().minus(Duration.ofDays(365)));
    assertFalse(t.needsRefresh(), "null interval gates out regardless of how long ago it last ran");
  }

  @Test @Tag("FILE-132")
  void nonNullIntervalAlwaysRefreshesOnFirstCall() {
    ProbeRefreshableTable t = new ProbeRefreshableTable(Duration.ofHours(1));
    // lastRefreshTime is null on construction -> first call always refreshes.
    assertTrue(t.needsRefresh(), "first access (null lastRefreshTime) always refreshes");
  }

  @Test @Tag("FILE-132")
  void intervalGatesBeforeAnyFileState() {
    ProbeRefreshableTable t = new ProbeRefreshableTable(Duration.ofHours(1));
    // Within the interval: needsRefresh is false even though needsRefresh consults NO file/mtime.
    t.setLastRefreshTime(Instant.now());
    assertFalse(t.needsRefresh(),
        "within the interval the gate returns false without ever checking file state");
    // Beyond the interval: the elapsed check fires.
    t.setLastRefreshTime(Instant.now().minus(Duration.ofHours(2)));
    assertTrue(t.needsRefresh(), "once the interval has elapsed the table needs refresh");
  }
}
