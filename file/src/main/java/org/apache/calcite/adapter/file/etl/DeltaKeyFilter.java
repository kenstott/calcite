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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * Streams only the fetched rows whose {@code incremental.keyFields} values the target table does
 * not hold yet (and that this run has not already emitted).
 *
 * <p>The table itself is the record of what has been appended, so a re-run, a lost or restored
 * tracker, and a rebuilt table all converge on one row per key. A row the source publishes late,
 * carrying an older modification time than rows already appended, is still written.
 */
final class DeltaKeyFilter implements java.util.Iterator<Map<String, Object>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeltaKeyFilter.class);

  private final String pipelineName;
  private final List<String> sourceKeys;
  private final DeltaKeyIndex index;
  private final java.util.Iterator<Map<String, Object>> upstream;
  private final String[] values;
  private Map<String, Object> nextRow;
  private long seen;
  private long emitted;

  DeltaKeyFilter(String pipelineName, EtlPipelineConfig config, DeltaKeyIndex index,
      java.util.Iterator<Map<String, Object>> upstream) {
    if (config.getModifiedField() != null) {
      throw new IllegalStateException("computed_delta pipeline '" + pipelineName
          + "' declares both incremental.keyFields and incremental.dateField; use one");
    }
    if (config.getColumns() == null || config.getColumns().isEmpty()) {
      throw new IllegalStateException("computed_delta pipeline '" + pipelineName
          + "': incremental.keyFields requires declared table columns");
    }
    this.pipelineName = pipelineName;
    this.sourceKeys = new ArrayList<String>();
    // Fetched rows are keyed by source field name; keyFields name table columns.
    for (String column : config.getDeltaKeyFields()) {
      String sourceKey = ColumnConfig.resolveSourceKey(config.getColumns(), column);
      if (sourceKey == null) {
        throw new IllegalStateException("computed_delta pipeline '" + pipelineName
            + "': key field '" + column + "' is not a fetched column. Known names: "
            + ColumnConfig.resolvableNames(config.getColumns()));
      }
      sourceKeys.add(sourceKey);
    }
    this.index = index;
    this.upstream = upstream;
    this.values = new String[sourceKeys.size()];
  }

  /** Reads the keys the writer's target table currently holds. */
  static DeltaKeyIndex load(final String pipelineName, final List<String> columns,
      MaterializationWriter writer) throws IOException {
    final DeltaKeyIndex index = new DeltaKeyIndex();
    final long[] rows = {0L};
    writer.scanExistingKeys(columns, new Consumer<String[]>() {
      @Override public void accept(String[] key) {
        for (int i = 0; i < key.length; i++) {
          requireValue(pipelineName, columns.get(i), key[i], "table");
        }
        rows[0]++;
        index.add(key);
      }
    });
    LOGGER.info("computed_delta: pipeline '{}' table holds {} rows, {} distinct keys on {}",
        pipelineName, rows[0], index.size(), columns);
    return index;
  }

  private static void requireValue(String pipelineName, String field, String value,
      String origin) {
    if (value == null || value.isEmpty()) {
      throw new IllegalStateException("computed_delta pipeline '" + pipelineName
          + "': key field '" + field + "' is empty in a " + origin + " row");
    }
  }

  @Override public boolean hasNext() {
    while (nextRow == null && upstream.hasNext()) {
      Map<String, Object> row = upstream.next();
      seen++;
      for (int i = 0; i < values.length; i++) {
        Object value = row.get(sourceKeys.get(i));
        values[i] = value == null ? null : String.valueOf(value);
        requireValue(pipelineName, sourceKeys.get(i), values[i], "source");
      }
      if (index.add(values)) {
        emitted++;
        nextRow = row;
      }
    }
    return nextRow != null;
  }

  @Override public Map<String, Object> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Map<String, Object> row = nextRow;
    nextRow = null;
    return row;
  }

  long seen() {
    return seen;
  }

  long emitted() {
    return emitted;
  }
}
