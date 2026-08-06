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
package org.apache.calcite.adapter.file.statistics;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Primary-key uniqueness statistics: the duplication arithmetic the verify check depends on,
 * and the on-disk compatibility rules that let the field be added without touching data that
 * is already cached.
 */
@Tag("unit")
public class PrimaryKeyStatisticsTest {

  private static final List<String> PK = Arrays.asList("cik", "filing_date");

  private static PrimaryKeyStatistics stats(long keyed, long distinct, boolean exact) {
    return new PrimaryKeyStatistics(PK, keyed, distinct, exact, "snap-1", null);
  }

  @Test void uniqueKeyReportsFactorOne() {
    assertEquals(1.0, stats(1000, 1000, true).duplicationFactor(), 0.0001);
  }

  /**
   * The case the check exists for: a table whose files were appended twice reports every key
   * twice. A design that derived "distinct" from the row count would report 1.0x here and let
   * the duplication through.
   */
  @Test void doubledTableReportsFactorTwo() {
    assertEquals(2.0, stats(2000, 1000, true).duplicationFactor(), 0.0001);
  }

  @Test void nineteenTimesDuplicationIsVisible() {
    assertEquals(19.0, stats(19000, 1000, true).duplicationFactor(), 0.0001);
  }

  /** No keyed rows describes nothing; it must not read as a duplication factor. */
  @Test void emptyKeyedSetReportsZero() {
    assertEquals(0.0, stats(0, 0, true).duplicationFactor(), 0.0001);
  }

  @Test void statisticIsValidOnlyForTheSnapshotItMeasured() {
    PrimaryKeyStatistics s = stats(10, 10, true);
    assertTrue(s.isValidFor("snap-1"));
    assertFalse(s.isValidFor("snap-2"), "a new snapshot must invalidate the statistic");
    assertFalse(s.isValidFor(null));
  }

  @Test void keyColumnsAreNotMutableThroughTheAccessor() {
    PrimaryKeyStatistics s = stats(10, 10, true);
    assertThrows(UnsupportedOperationException.class, () -> s.getKeyColumns().add("extra"));
  }

  @Test void survivesCacheRoundTrip(@TempDir Path dir) throws IOException {
    File file = dir.resolve("round-trip.aperio_stats").toFile();
    TableStatistics original = new TableStatistics(2000, 4096, new HashMap<>(), "src-hash",
        new PrimaryKeyStatistics(PK, 2000, 1000, true, "snap-1", null));

    StatisticsCache.saveStatistics(original, file);
    PrimaryKeyStatistics loaded =
        StatisticsCache.loadStatistics(file).getPrimaryKeyStatistics();

    assertNotNull(loaded);
    assertEquals(PK, loaded.getKeyColumns());
    assertEquals(2000, loaded.getKeyedRowCount());
    assertEquals(1000, loaded.getDistinctKeyEstimate());
    assertTrue(loaded.isExact());
    assertEquals("snap-1", loaded.getSnapshotId());
    assertEquals(2.0, loaded.duplicationFactor(), 0.0001);
  }

  /**
   * A statistics file written before this field existed must still load, with the statistic
   * absent rather than the load failing or inventing a value. Absent means "not measured",
   * which makes the caller measure.
   */
  @Test void loadsStatisticsFileWrittenBeforeThisFieldExisted(@TempDir Path dir)
      throws IOException {
    File file = dir.resolve("legacy.aperio_stats").toFile();
    String legacy = "{\n"
        + "  \"version\" : \"1.0\",\n"
        + "  \"rowCount\" : 1234,\n"
        + "  \"dataSize\" : 5678,\n"
        + "  \"lastUpdated\" : 1750000000000,\n"
        + "  \"sourceHash\" : \"legacy-hash\",\n"
        + "  \"columns\" : { }\n"
        + "}\n";
    Files.write(file.toPath(), legacy.getBytes(StandardCharsets.UTF_8));

    TableStatistics loaded = StatisticsCache.loadStatistics(file);

    assertEquals(1234, loaded.getRowCount());
    assertEquals("legacy-hash", loaded.getSourceHash());
    assertNull(loaded.getPrimaryKeyStatistics(),
        "an unmeasured key must read as absent, never as unique");
  }

  /**
   * Statistics with no key measurement must not write the node at all, so a file stays
   * readable by a reader that predates the field.
   */
  @Test void omitsTheNodeWhenNoKeyWasMeasured(@TempDir Path dir) throws IOException {
    File file = dir.resolve("no-pk.aperio_stats").toFile();
    StatisticsCache.saveStatistics(
        new TableStatistics(10, 20, new HashMap<>(), "h"), file);

    String written = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);

    assertFalse(written.contains("pkUniqueness"));
    assertNull(StatisticsCache.loadStatistics(file).getPrimaryKeyStatistics());
  }

  /** The version stays 1.0 — bumping it would make every already-cached file fail to load. */
  @Test void keepsTheFileVersionUnchanged(@TempDir Path dir) throws IOException {
    File file = dir.resolve("version.aperio_stats").toFile();
    StatisticsCache.saveStatistics(
        new TableStatistics(1, 1, new HashMap<>(), "h", stats(2, 1, true)), file);

    String written = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);

    assertTrue(written.contains("\"version\" : \"1.0\""), written);
  }

  @Test void attachesAKeyStatisticToStatisticsCollectedWithoutOne() {
    Map<String, ColumnStatistics> columns = new HashMap<>();
    TableStatistics before = new TableStatistics(100, 200, columns, "h");
    assertNull(before.getPrimaryKeyStatistics());

    TableStatistics after = before.withPrimaryKeyStatistics(stats(100, 50, true));

    assertNotNull(after.getPrimaryKeyStatistics());
    assertEquals(2.0, after.getPrimaryKeyStatistics().duplicationFactor(), 0.0001);
    assertEquals(before.getRowCount(), after.getRowCount());
    assertEquals(before.getSourceHash(), after.getSourceHash());
    assertNull(before.getPrimaryKeyStatistics(), "the original must be unchanged");
  }
}
