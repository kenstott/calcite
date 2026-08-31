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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies when per-unit freshness skipping is safe.
 *
 * <p>The cases below are the real table shapes the audit measured, because the predicate's value is
 * entirely in whether it classifies those correctly: too strict and it needlessly degrades healthy
 * tables, too loose and it leaves the data-loss path open.
 */
@Tag("unit")
public class PerUnitSkipSafetyTest {

  private static DimensionConfig list(String name, String... values) {
    return DimensionConfig.builder()
        .name(name).type(DimensionType.LIST).values(Arrays.asList(values)).build();
  }

  private static DimensionConfig yearRange() {
    return DimensionConfig.builder()
        .name("year").type(DimensionType.YEAR_RANGE).start(2010).build();
  }

  private static EtlPipelineConfig config(Map<String, DimensionConfig> dims, List<String> partition,
      Map<String, String> valueSource, FreshnessConfig freshness, String sourceUrl) {
    MaterializePartitionConfig.Builder p = MaterializePartitionConfig.builder().columns(partition);
    if (valueSource != null) {
      p.valueSource(valueSource);
    }
    return EtlPipelineConfig.builder()
        .name("t")
        .source(HttpSourceConfig.builder().url(sourceUrl).build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .partition(p.build())
            .output(MaterializeOutputConfig.builder().pattern("out/").build())
            .build())
        .freshness(freshness)
        .build();
  }

  private static Map<String, DimensionConfig> dims(Object... pairs) {
    Map<String, DimensionConfig> m = new LinkedHashMap<String, DimensionConfig>();
    for (int i = 0; i < pairs.length; i += 2) {
      m.put((String) pairs[i], (DimensionConfig) pairs[i + 1]);
    }
    return m;
  }

  private static FreshnessConfig etag() {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    m.put("type", "etag");
    return FreshnessConfig.fromMap(m);
  }

  private static FreshnessConfig versionWithConstantProbe() {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    m.put("type", "version");
    m.put("probe_url", "https://api.example.gov/download.json");
    m.put("version_field", "results.export_date");
    return FreshnessConfig.fromMap(m);
  }

  /** The FIA shape: 51 states collapsing into a single {@code [type]} partition. */
  @Test void multiValuedDimensionOutsideThePartitionIsUnsafe() {
    EtlPipelineConfig c = config(
        dims("type", list("type", "fia_plots"), "state", list("state", "AL", "AK", "AZ")),
        Arrays.asList("type"), null, etag(), "https://example.gov/{state}.zip");

    assertFalse(PerUnitSkipSafety.isSafe(c));
    assertEquals(java.util.Collections.singleton("state"), PerUnitSkipSafety.unsafeDimensions(c));
  }

  /** Adding the fetch dimension to the partition is what makes it safe again. */
  @Test void partitioningOnThatDimensionMakesItSafe() {
    EtlPipelineConfig c = config(
        dims("type", list("type", "fia_plots"), "state", list("state", "AL", "AK", "AZ")),
        Arrays.asList("type", "state"), null, etag(), "https://example.gov/{state}.zip");

    assertTrue(PerUnitSkipSafety.isSafe(c));
  }

  /**
   * The patents shape: the only unpartitioned dimension resolves to one value, so the table is a
   * single unit per run and a skip is all-or-nothing regardless.
   */
  @Test void singleValuedDimensionCannotDistinguishUnits() {
    EtlPipelineConfig c = config(
        dims("type", list("type", "patent_abstracts"), "quarter", list("quarter", "2026Q1")),
        Arrays.asList("type"), null, etag(), "https://example.gov/{quarter}.zip");

    assertTrue(PerUnitSkipSafety.isSafe(c), "one value cannot diverge from itself");
  }

  /**
   * The openFDA shape: a constant probe hands every unit the same endpoint-level token, so units
   * cannot diverge even though the fetch dimension is unpartitioned.
   */
  @Test void constantProbeExemptsAnUnpartitionedDimension() {
    EtlPipelineConfig c = config(
        dims("type", list("type", "fda_drug_recalls"),
            "partition_file", list("partition_file", "a.zip", "b.zip", "c.zip")),
        Arrays.asList("type"), null, versionWithConstantProbe(),
        "https://example.gov/{partition_file}");

    assertTrue(PerUnitSkipSafety.isSafe(c), "an invariant probe yields one token for every unit");
  }

  /** Resolving valueSource matters: partitioning on effective_year covers the year axis. */
  @Test void valueSourceIsResolvedBackToTheDimensionItCovers() {
    Map<String, String> vs = new LinkedHashMap<String, String>();
    vs.put("year", "effective_year");
    EtlPipelineConfig c = config(
        dims("type", list("type", "t"), "year", yearRange()),
        Arrays.asList("type", "year"), vs, etag(), "https://example.gov/{effective_year}.zip");

    assertTrue(PerUnitSkipSafety.isSafe(c),
        "ignoring valueSource would mark this table unsafe when it is not");
  }

  /** No freshness gate means nothing can be skipped, so nothing can be dropped. */
  @Test void withoutFreshnessThereIsNothingToSkip() {
    EtlPipelineConfig c = config(
        dims("type", list("type", "t"), "state", list("state", "AL", "AK")),
        Arrays.asList("type"), null, null, "https://example.gov/{state}.zip");

    assertTrue(PerUnitSkipSafety.isSafe(c));
  }

  /** A hash token is content-derived, so it varies per unit whatever the probe URL looks like. */
  @Test void hashVariesPerUnitEvenWithAConstantUrl() {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    m.put("type", "hash");
    EtlPipelineConfig c = config(
        dims("type", list("type", "t"), "series", list("series", "A", "B", "C")),
        Arrays.asList("type", "year"), null, FreshnessConfig.fromMap(m),
        "https://example.gov/constant");

    assertFalse(PerUnitSkipSafety.isSafe(c));
    assertTrue(PerUnitSkipSafety.unsafeDimensions(c).contains("series"));
  }
}
