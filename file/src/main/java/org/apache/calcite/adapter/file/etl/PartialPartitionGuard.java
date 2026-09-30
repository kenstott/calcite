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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Keeps a replace-partitions write from displacing a partition's other fetch units.
 *
 * <p>Replace-partitions swaps a partition for exactly the files a run hands it. When the partition
 * key is coarser than the fetch unit (a multi-valued fetch dimension such as {@code series} is not
 * in the key), several units feed one partition, and a run dispatching only the units the tracker
 * still lists as pending — one that failed last time, say — writes a partition holding just those
 * units and drops the rest. The tracker is the authority on what was fetched, not on what a
 * partition may safely be replaced with.
 *
 * <p>For such a table any pending unit therefore reopens every unit in its partition, the same
 * all-or-nothing behavior {@link PerUnitSkipSafety} falls back to for freshness skipping. It is
 * independent of any freshness gate and of how the pending set was derived.
 *
 * <p>The same holds at commit time: a unit whose fetch failed contributes no rows, so committing
 * the run would replace its partition without it. {@link #commitBlockingErrors} identifies the
 * failures that must stop the commit so the previously committed partition stays intact.
 *
 * <p>Units the tracker records as unavailable are not reopened: they have no data to lose, and
 * re-requesting them ahead of their retry window is what the unavailable skip exists to prevent.
 */
final class PartialPartitionGuard {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartialPartitionGuard.class);

  private PartialPartitionGuard() {
  }

  /**
   * True when the pipeline replaces partitions and its partition key does not determine the fetch
   * unit, so a partial dispatch would displace sibling units.
   *
   * @param config the pipeline config
   * @return whether pending units must reopen their whole partition
   */
  static boolean applies(EtlPipelineConfig config) {
    MaterializeConfig materialize = config != null ? config.getMaterialize() : null;
    if (materialize == null || !materialize.isEnabled()
        || materialize.getFormat() != MaterializeConfig.Format.ICEBERG) {
      return false;
    }
    MaterializeConfig.IcebergConfig iceberg = materialize.getIceberg();
    if (iceberg == null || !iceberg.isReplacingPartitionsThisRun()) {
      return false;
    }
    return !PerUnitSkipSafety.dimensionsOutsidePartition(config).isEmpty();
  }

  /**
   * Adds to {@code pending} every combination that shares a partition with a pending one.
   *
   * @param config     the pipeline config
   * @param combos     the combinations the indices refer to
   * @param pending    indices to dispatch; widened in place
   * @param reopenable whether a sibling combination may be dispatched again (false for one in its
   *                   unavailable retry window)
   * @return how many combinations were added
   */
  static int reopenPartitions(EtlPipelineConfig config, List<Map<String, String>> combos,
      Set<Integer> pending, Predicate<Map<String, String>> reopenable) {
    if (pending.isEmpty() || !applies(config)) {
      return 0;
    }
    Set<String> pendingKeys = new HashSet<String>();
    for (int idx : pending) {
      pendingKeys.add(partitionKey(config, combos.get(idx)));
    }
    List<Integer> added = new ArrayList<Integer>();
    for (int i = 0; i < combos.size(); i++) {
      if (pending.contains(i)) {
        continue;
      }
      Map<String, String> combo = combos.get(i);
      if (pendingKeys.contains(partitionKey(config, combo)) && reopenable.test(combo)) {
        added.add(i);
      }
    }
    pending.addAll(added);
    if (!added.isEmpty()) {
      LOGGER.info("Partition reopen for '{}': fetch dimension(s) {} are not in the partition key, "
          + "so {} sibling combination(s) rejoin the dispatch to keep replace-partitions from "
          + "dropping them", config.getName(),
          PerUnitSkipSafety.dimensionsOutsidePartition(config), added.size());
    }
    return added.size();
  }

  /**
   * The batch errors that leave a unit's rows missing from a replaced partition. A batch that
   * failed with HTTP 404 is recorded as unavailable — the source has no data for it, so there is
   * nothing to lose — and does not count.
   *
   * @param errors the batch error messages collected during the run
   * @return the errors that make a replace-partitions commit unsafe
   */
  static List<String> commitBlockingErrors(List<String> errors) {
    List<String> blocking = new ArrayList<String>();
    for (String error : errors) {
      if (!error.contains("HTTP 404")) {
        blocking.add(error);
      }
    }
    return blocking;
  }

  /**
   * The combination's partition key, from the same combo variables the writer partitions on
   * (honoring {@code valueSource}). A variable missing from every combination yields the same
   * component for all of them, which widens rather than narrows.
   */
  private static String partitionKey(EtlPipelineConfig config, Map<String, String> combo) {
    MaterializePartitionConfig partition = config.getMaterialize().getPartition();
    StringBuilder key = new StringBuilder();
    if (partition == null || partition.getColumns() == null) {
      return key.toString();
    }
    Map<String, String> valueSource = partition.getValueSource();
    for (String column : partition.getColumns()) {
      String source = valueSource != null && valueSource.containsKey(column)
          ? valueSource.get(column) : column;
      key.append(EtlPipeline.canonicalPartitionValue(combo.get(source))).append('\u0000');
    }
    return key.toString();
  }
}
