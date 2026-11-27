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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tracks, for a replace-partitions table whose partitions are each fed by several fetch units,
 * when a partition has received every unit dispatched for it and may be committed.
 *
 * <p>{@link PartialPartitionGuard} makes a run dispatch every unit of any partition it touches, so
 * a partition whose dispatched units have all been written is complete. A unit that fails or is
 * skipped never counts, which leaves its partition uncommitted.
 *
 * <p>Not thread-safe; callers serialize on the pipeline's write lock.
 */
final class PartitionCompletion {

  private final List<String> columns;
  private final Map<String, String> valueSource;
  /** Units still to be written, per partition group. */
  private final Map<Map<String, String>, Integer> remaining =
      new HashMap<Map<String, String>, Integer>();
  /** Partition keys the group's written units have produced. */
  private final Map<Map<String, String>, Set<String>> written =
      new HashMap<Map<String, String>, Set<String>>();

  PartitionCompletion(EtlPipelineConfig config, List<EtlPipeline.FetchUnit> units) {
    MaterializePartitionConfig partition = config.getMaterialize().getPartition();
    this.columns = partition.getColumns();
    this.valueSource = partition.getValueSource();
    for (EtlPipeline.FetchUnit unit : units) {
      Map<String, String> group = groupOf(unit.getFetchVariables());
      Integer count = remaining.get(group);
      remaining.put(group, count == null ? 1 : count + 1);
    }
  }

  /**
   * Whether the pipeline publishes partitions as they complete: it opts in with
   * {@code options.commitPerPartition}, its partitions are fed by several fetch units, it writes
   * through the Iceberg writer, and each unit is written in one pass.
   */
  static boolean applies(EtlPipelineConfig config, MaterializationWriter writer) {
    return writer instanceof IcebergMaterializationWriter
        && PartialPartitionGuard.applies(config)
        && config.getMaterialize().getOptions() != null
        && config.getMaterialize().getOptions().isCommitPerPartition()
        && !(config.getSource() != null && config.getSource().hasResponsePartitioning());
  }

  /** The partition a unit or combination feeds, as partition column to value. */
  Map<String, String> groupOf(Map<String, String> variables) {
    Map<String, String> group = new LinkedHashMap<String, String>();
    for (String column : columns) {
      String source = valueSource.containsKey(column) ? valueSource.get(column) : column;
      group.put(column, variables.get(source));
    }
    return group;
  }

  /**
   * Records one written unit of {@code group}.
   *
   * @param group the unit's partition group, from {@link #groupOf}
   * @param partitionKeys writer partition keys the unit produced
   * @return every key of the group when this was its last unit, otherwise null
   */
  Set<String> unitWritten(Map<String, String> group, Collection<String> partitionKeys) {
    Integer count = remaining.get(group);
    if (count == null) {
      throw new IllegalStateException("No fetch unit outstanding for partition " + group);
    }
    Set<String> keys = written.get(group);
    if (keys == null) {
      keys = new LinkedHashSet<String>();
      written.put(group, keys);
    }
    keys.addAll(partitionKeys);
    if (count > 1) {
      remaining.put(group, count - 1);
      return null;
    }
    remaining.remove(group);
    return written.remove(group);
  }
}
