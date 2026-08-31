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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Decides whether per-unit freshness skipping can safely be applied to a pipeline.
 *
 * <p>Skipping some fetch units and not others is only safe when the Iceberg partition key is a
 * function of the fetch unit — that is, when two distinct units can never land in the same
 * partition. Replace-partitions swaps a partition for exactly the files handed to it, so a run that
 * wrote only some of a partition's units would drop the rest. That invariant was implicit, which is
 * how it came to be violated when per-unit skipping was extended to non-period fetch dimensions.
 *
 * <p>When the invariant does not hold, per-unit skipping is disabled and the pipeline falls back to
 * all-or-nothing freshness. That is the behavior these tables had before per-unit skipping was
 * extended to them, so it removes an optimization rather than changing what is written.
 *
 * <h3>Exemptions</h3>
 * Three cases cannot produce a partial skip and are therefore safe despite a coarse partition:
 * <ul>
 *   <li><b>Single-valued dimensions</b> — a dimension with one value cannot distinguish two units.</li>
 *   <li><b>Derived companions</b> — {@code effective_year} and {@code month_end} are computed from
 *       another dimension rather than being independent axes.</li>
 *   <li><b>A unit-invariant freshness probe</b> — when {@code probe_url} is a constant, every unit
 *       receives an identical token, so they cannot diverge. This is what makes the openFDA tables
 *       safe: they probe one endpoint-level export date for every partition file.</li>
 * </ul>
 */
final class PerUnitSkipSafety {

  /** Companions injected from another dimension; not independent fetch axes. */
  private static final Set<String> DERIVED =
      Collections.unmodifiableSet(new LinkedHashSet<>(java.util.Arrays.asList(
          "effective_year", "month_end")));

  /** Matches a {@code {var}} placeholder, i.e. a probe that varies per unit. */
  private static final Pattern TEMPLATE = Pattern.compile("\\{[A-Za-z_][A-Za-z0-9_]*\\}");

  private PerUnitSkipSafety() {
  }

  /**
   * True when per-unit freshness skipping cannot drop another unit's data.
   *
   * @param config the pipeline config
   * @return whether per-unit skipping is safe for this pipeline
   */
  static boolean isSafe(EtlPipelineConfig config) {
    return unsafeDimensions(config).isEmpty();
  }

  /**
   * The multi-valued fetch dimensions that are absent from the partition key — empty when per-unit
   * skipping is safe. Returned rather than a bare boolean so the caller can name them in a log
   * line, which is what turns "this table is degraded" into "add these to its partition".
   *
   * @param config the pipeline config
   * @return offending dimension names, in declaration order
   */
  static Set<String> unsafeDimensions(EtlPipelineConfig config) {
    Set<String> none = Collections.emptySet();
    if (config == null) {
      return none;
    }
    FreshnessConfig freshness = config.getFreshness();
    if (freshness == null) {
      return none;
    }
    // A constant probe hands every unit the same token, so units cannot diverge.
    if (freshness.getType() != FreshnessConfig.Type.HASH && !probeVariesPerUnit(config)) {
      return none;
    }

    Map<String, DimensionConfig> dimensions = config.getDimensions();
    if (dimensions == null || dimensions.isEmpty()) {
      return none;
    }

    Set<String> partitionVars = partitionVariables(config);
    Set<String> offending = new LinkedHashSet<>();
    for (Map.Entry<String, DimensionConfig> entry : dimensions.entrySet()) {
      String name = entry.getKey();
      if (DERIVED.contains(name) || partitionVars.contains(name)) {
        continue;
      }
      if (isSingleValued(entry.getValue())) {
        continue;
      }
      offending.add(name);
    }
    return offending;
  }

  /**
   * Partition columns resolved back to the dimension each sources from, so a column declared as
   * {@code valueSource: {year: effective_year}} still counts as covering {@code year}. Ignoring the
   * mapping would mark safe tables unsafe.
   */
  private static Set<String> partitionVariables(EtlPipelineConfig config) {
    Set<String> vars = new LinkedHashSet<>();
    MaterializeConfig materialize = config.getMaterialize();
    MaterializePartitionConfig partition =
        materialize != null ? materialize.getPartition() : null;
    if (partition == null || partition.getColumns() == null) {
      return vars;
    }
    Map<String, String> valueSource = partition.getValueSource();
    for (String column : partition.getColumns()) {
      vars.add(column);
      String source = valueSource != null ? valueSource.get(column) : null;
      if (source != null) {
        vars.add(source);
        // effective_year is derived from year, so partitioning on it covers the year axis.
        if ("effective_year".equals(source)) {
          vars.add("year");
        }
      }
    }
    return vars;
  }

  /**
   * Whether the freshness probe can yield different tokens for different units. A templated probe
   * (or, absent an override, a templated source URL) varies; a constant one cannot.
   */
  private static boolean probeVariesPerUnit(EtlPipelineConfig config) {
    FreshnessConfig freshness = config.getFreshness();
    String probeUrl = freshness != null ? freshness.getProbeUrl() : null;
    if (probeUrl != null && !probeUrl.isEmpty()) {
      return TEMPLATE.matcher(probeUrl).find();
    }
    HttpSourceConfig source = config.getSource();
    String sourceUrl = source != null ? source.getUrl() : null;
    return sourceUrl != null && TEMPLATE.matcher(sourceUrl).find();
  }

  /** A dimension resolving to exactly one value cannot distinguish two units. */
  private static boolean isSingleValued(DimensionConfig dimension) {
    if (dimension == null) {
      return true;
    }
    List<String> values = dimension.getValues();
    return values != null && values.size() == 1;
  }
}
