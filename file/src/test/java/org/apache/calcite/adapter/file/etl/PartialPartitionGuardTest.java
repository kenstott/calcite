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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a pending fetch unit reopens the rest of its partition when replace-partitions
 * would otherwise drop them. The fixture is the {@code econ.fred_indicators} shape: a fetch unit of
 * (series, year, month) written to a partition of (type, year, month).
 */
@Tag("unit")
public class PartialPartitionGuardTest {

  private static final String[] SERIES = {"RSXFS", "RETAILIMSA", "UNRATE"};
  private static final String[] MONTHS = {"05", "06", "07"};

  private static DimensionConfig list(String name, String... values) {
    return DimensionConfig.builder()
        .name(name).type(DimensionType.LIST).values(Arrays.asList(values)).build();
  }

  private static EtlPipelineConfig config(MaterializeConfig.Format format,
      MaterializeConfig.IcebergConfig iceberg, List<String> partitionColumns) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("type", list("type", "fred_indicators"));
    dims.put("series", list("series", SERIES));
    dims.put("year", DimensionConfig.builder()
        .name("year").type(DimensionType.YEAR_RANGE).start(2013).build());
    dims.put("month", list("month", MONTHS));

    Map<String, String> valueSource = new LinkedHashMap<String, String>();
    valueSource.put("year", "effective_year");
    MaterializeConfig.Builder materialize = MaterializeConfig.builder()
        .format(format)
        .partition(MaterializePartitionConfig.builder()
            .columns(partitionColumns).valueSource(valueSource).build())
        .output(MaterializeOutputConfig.builder().pattern("out/").build());
    if (iceberg != null) {
      materialize.iceberg(iceberg);
    }
    return EtlPipelineConfig.builder()
        .name("fred_indicators")
        .source(HttpSourceConfig.builder().url("https://api.example.gov/{series}").build())
        .dimensions(dims)
        .materialize(materialize.build())
        .build();
  }

  private static EtlPipelineConfig fred(MaterializeConfig.IcebergConfig iceberg) {
    return config(MaterializeConfig.Format.ICEBERG, iceberg,
        Arrays.asList("type", "year", "month"));
  }

  private static MaterializeConfig.IcebergConfig replacing() {
    return MaterializeConfig.IcebergConfig.builder().overwritePartitions(true).build();
  }

  /** Every (series, month) for 2013, in dispatch order: series varies slowest. */
  private static List<Map<String, String>> combos() {
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    for (String series : SERIES) {
      for (String month : MONTHS) {
        Map<String, String> c = new LinkedHashMap<String, String>();
        c.put("type", "fred_indicators");
        c.put("series", series);
        c.put("year", "2013");
        c.put("effective_year", "2013");
        c.put("month", month);
        combos.add(c);
      }
    }
    return combos;
  }

  private static int indexOf(List<Map<String, String>> combos, String series, String month) {
    for (int i = 0; i < combos.size(); i++) {
      if (series.equals(combos.get(i).get("series")) && month.equals(combos.get(i).get("month"))) {
        return i;
      }
    }
    throw new AssertionError(series + " " + month);
  }

  private static final Predicate<Map<String, String>> ALWAYS = combo -> true;

  /** The live defect: RETAILIMSA 2013-06 alone is pending and must not replace 2013-06 by itself. */
  @Test void onePendingUnitReopensItsWholePartition() {
    List<Map<String, String>> combos = combos();
    Set<Integer> pending = new HashSet<Integer>();
    pending.add(indexOf(combos, "RETAILIMSA", "06"));

    int added = PartialPartitionGuard.reopenPartitions(fred(replacing()), combos, pending, ALWAYS);

    assertEquals(2, added);
    Set<Integer> expected = new HashSet<Integer>(Arrays.asList(
        indexOf(combos, "RSXFS", "06"), indexOf(combos, "RETAILIMSA", "06"),
        indexOf(combos, "UNRATE", "06")));
    assertEquals(expected, pending, "the other months' partitions are not touched");
  }

  /** Two pending units in different partitions reopen both partitions and no others. */
  @Test void eachPendingPartitionIsReopenedIndependently() {
    List<Map<String, String>> combos = combos();
    Set<Integer> pending = new HashSet<Integer>();
    pending.add(indexOf(combos, "RSXFS", "05"));
    pending.add(indexOf(combos, "UNRATE", "07"));

    PartialPartitionGuard.reopenPartitions(fred(replacing()), combos, pending, ALWAYS);

    assertEquals(6, pending.size());
    assertFalse(pending.contains(indexOf(combos, "RSXFS", "06")));
  }

  /** A sibling inside its unavailable retry window has no data to lose and is not re-requested. */
  @Test void unavailableSiblingIsNotReopened() {
    final List<Map<String, String>> combos = combos();
    Set<Integer> pending = new HashSet<Integer>();
    pending.add(indexOf(combos, "RETAILIMSA", "06"));

    PartialPartitionGuard.reopenPartitions(fred(replacing()), combos, pending,
        combo -> !"UNRATE".equals(combo.get("series")));

    assertTrue(pending.contains(indexOf(combos, "RSXFS", "06")));
    assertFalse(pending.contains(indexOf(combos, "UNRATE", "06")));
  }

  /** Nothing pending means nothing to reopen: a fully processed table stays skipped. */
  @Test void nothingPendingAddsNothing() {
    Set<Integer> pending = new HashSet<Integer>();

    assertEquals(0,
        PartialPartitionGuard.reopenPartitions(fred(replacing()), combos(), pending, ALWAYS));
    assertTrue(pending.isEmpty());
  }

  /** When the partition key carries the fetch dimension, a unit is its own partition. */
  @Test void partitionKeyDeterminingTheUnitNeedsNoReopen() {
    EtlPipelineConfig c = config(MaterializeConfig.Format.ICEBERG, replacing(),
        Arrays.asList("type", "series", "year", "month"));
    List<Map<String, String>> combos = combos();
    Set<Integer> pending = new HashSet<Integer>();
    pending.add(indexOf(combos, "RETAILIMSA", "06"));

    assertFalse(PartialPartitionGuard.applies(c));
    assertEquals(0, PartialPartitionGuard.reopenPartitions(c, combos, pending, ALWAYS));
    assertEquals(1, pending.size());
  }

  /** An appending table adds rows rather than replacing a partition; a reopen would duplicate. */
  @Test void appendingTableIsLeftAlone() {
    EtlPipelineConfig c = fred(
        MaterializeConfig.IcebergConfig.builder().overwritePartitions(false).build());
    List<Map<String, String>> combos = combos();
    Set<Integer> pending = new HashSet<Integer>();
    pending.add(indexOf(combos, "RETAILIMSA", "06"));

    assertFalse(PartialPartitionGuard.applies(c));
    assertEquals(0, PartialPartitionGuard.reopenPartitions(c, combos, pending, ALWAYS));
  }

  /** Only the Iceberg writer has replace-partitions semantics. */
  @Test void nonIcebergFormatIsLeftAlone() {
    EtlPipelineConfig c = config(MaterializeConfig.Format.PARQUET, null,
        Arrays.asList("type", "year", "month"));

    assertFalse(PartialPartitionGuard.applies(c));
  }

  @Test public void commitBlockingErrorsIgnoresUnavailableUnits() {
    List<String> errors = Arrays.asList(
        "Batch 3/9 failed: HTTP 404: not published",
        "Batch 5/9 failed: HTTP 429: {\"error_code\":429}");
    List<String> blocking = PartialPartitionGuard.commitBlockingErrors(errors);
    assertEquals(1, blocking.size());
    assertTrue(blocking.get(0).contains("HTTP 429"));
  }

  @Test public void commitBlockingErrorsEmptyWhenNoFailures() {
    assertTrue(PartialPartitionGuard.commitBlockingErrors(new ArrayList<String>()).isEmpty());
  }
}
