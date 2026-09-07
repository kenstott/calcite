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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

/**
 * Answers {@code COUNT(*) ... WHERE <partition predicate>} from Iceberg manifest metadata.
 *
 * <p>Iceberg records a row count per data file in its manifests, along with that file's partition
 * tuple. When a predicate is decided entirely by the partition tuple, the count for the predicate
 * is the sum of the row counts of the files whose partitions satisfy it — no data file is opened.
 *
 * <p>The proof that a predicate is decided entirely by the partition tuple is not attempted here
 * by inspecting the partition spec. Iceberg already computes it: after partition pruning, every
 * surviving {@link FileScanTask} carries a <em>residual</em>, the part of the predicate still to be
 * evaluated row by row. A residual of {@code alwaysTrue()} means the partition value settled the
 * predicate for every row in that file. Requiring that of every task is a stronger and far more
 * direct check than reimplementing transform reasoning — it holds for identity partitioning and
 * correctly refuses when a column is bucketed, truncated, unpartitioned, or absent from the spec,
 * and it stays correct across partition-spec evolution because each task is judged under its own
 * spec.
 */
public final class IcebergPartitionRowCount {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergPartitionRowCount.class);

  private IcebergPartitionRowCount() {
  }

  /**
   * Builds an Iceberg predicate from a column-to-accepted-values map, or null when a value has no
   * Iceberg literal representation.
   *
   * <p>Each entry becomes {@code col = v} (one value) or {@code col IN (v, ...)} (several), and
   * the entries are ANDed. An empty map yields {@code alwaysTrue()}, i.e. the whole table.
   */
  public static Expression toPredicate(Map<String, List<Object>> acceptedValues) {
    Expression expression = Expressions.alwaysTrue();
    for (Map.Entry<String, List<Object>> entry : acceptedValues.entrySet()) {
      List<Object> values = entry.getValue();
      if (values.isEmpty()) {
        return null;
      }
      for (Object value : values) {
        if (!isIcebergLiteral(value)) {
          LOGGER.debug("Value {} on column {} has no Iceberg literal form",
              value, entry.getKey());
          return null;
        }
      }
      Expression term = values.size() == 1
          ? Expressions.equal(entry.getKey(), values.get(0))
          : Expressions.in(entry.getKey(), values);
      expression = Expressions.and(expression, term);
    }
    return expression;
  }

  /** The Java types {@code org.apache.iceberg.expressions.Literals#from} accepts. */
  private static boolean isIcebergLiteral(Object value) {
    return value instanceof CharSequence
        || value instanceof Boolean
        || value instanceof Integer
        || value instanceof Long
        || value instanceof Float
        || value instanceof Double
        || value instanceof java.math.BigDecimal
        || value instanceof java.util.UUID;
  }

  /**
   * Number of rows matching {@code predicate}, read from manifests, or null when the manifests
   * cannot prove an exact answer.
   *
   * <p>Null — decline, let the query run normally — is returned when:
   * <ul>
   *   <li>the predicate does not bind to the table schema (a column it names is not there, or a
   *       literal is not comparable with the column's type); or</li>
   *   <li>a surviving file has a residual other than {@code alwaysTrue()}, meaning the predicate
   *       is not settled by that file's partition tuple; or</li>
   *   <li>a surviving file has delete files attached (merge-on-read), so its manifest row count
   *       overstates the rows actually visible.</li>
   * </ul>
   *
   * <p>An I/O failure reading the manifests is not one of those cases and is not swallowed: it
   * propagates, because a metadata read that was supposed to work and did not is a real fault, not
   * a shape mismatch.
   *
   * @throws UncheckedIOException if the manifest list or a manifest file cannot be read
   */
  public static Long countMatching(Table table, Expression predicate) {
    if (table.currentSnapshot() == null) {
      // No snapshot: the table is empty, so every predicate matches nothing.
      return 0L;
    }
    org.apache.iceberg.TableScan scan;
    try {
      scan = table.newScan().caseSensitive(false).filter(predicate);
    } catch (ValidationException e) {
      // The predicate names a column the table does not have, or compares it against an
      // incompatible literal. That is a shape mismatch, not a failure.
      LOGGER.debug("Predicate {} does not bind to {}: {}", predicate, table.name(), e.getMessage());
      return null;
    }
    long total = 0L;
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        if (task.residual().op() != Expression.Operation.TRUE) {
          LOGGER.debug("Declining count for {}: residual {} is not settled by the partition tuple",
              table.name(), task.residual());
          return null;
        }
        if (!task.deletes().isEmpty()) {
          LOGGER.debug("Declining count for {}: {} has delete files, manifest count overstates it",
              table.name(), task.file().path());
          return null;
        }
        total += task.file().recordCount();
      }
    } catch (ValidationException e) {
      LOGGER.debug("Predicate {} does not bind to {}: {}", predicate, table.name(), e.getMessage());
      return null;
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to close the manifest scan for Iceberg table " + table.name(), e);
    }
    return total;
  }
}
