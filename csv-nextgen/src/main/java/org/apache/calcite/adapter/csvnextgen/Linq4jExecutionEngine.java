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
package org.apache.calcite.adapter.csvnextgen;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Source;

import java.util.Iterator;

/**
 * Traditional Linq4j execution engine that processes data row-by-row.
 * Uses the existing Calcite enumerable framework.
 */
public class Linq4jExecutionEngine {
  private final int batchSize;

  public Linq4jExecutionEngine(int batchSize) {
    this.batchSize = batchSize;
  }

  /**
   * Creates an enumerable that processes CSV data using traditional row-by-row approach.
   */
  public Enumerable<Object[]> scan(Source source, RelDataType rowType, boolean hasHeader) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new Linq4jEnumerator(source, rowType, batchSize, hasHeader);
      }
    };
  }

  /**
   * Applies filtering using row-by-row processing.
   */
  public Enumerable<Object[]> filter(Source source, RelDataType rowType, boolean hasHeader,
      FilterPredicate predicate) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new Linq4jFilteredEnumerator(source, rowType, batchSize, hasHeader, predicate);
      }
    };
  }

  /**
   * Traditional row-by-row enumerator.
   */
  static class Linq4jEnumerator implements Enumerator<Object[]> {
    private final CsvBatchReader batchReader;
    private final Iterator<DataBatch> batchIterator;
    private final RelDataType rowType;
    private Iterator<Object[]> currentRowIterator;
    private Object[] current;

    Linq4jEnumerator(Source source, RelDataType rowType, int batchSize, boolean hasHeader) {
      this.rowType = rowType;
      this.batchReader = new CsvBatchReader(source, rowType, batchSize, hasHeader);
      this.batchIterator = batchReader.getBatches();
    }

    @Override public Object[] current() {
      return current;
    }

    @Override public boolean moveNext() {
      // Check if we need a new batch
      if (currentRowIterator == null || !currentRowIterator.hasNext()) {
        if (!loadNextBatch()) {
          return false;
        }
      }

      // Get next row using traditional row-by-row approach
      current = currentRowIterator.next();
      return true;
    }

    private boolean loadNextBatch() {
      if (!batchIterator.hasNext()) {
        return false;
      }

      DataBatch dataBatch = batchIterator.next();
      if (dataBatch == null) {
        return false; // End of data
      }

      currentRowIterator = dataBatch.asRows();

      return currentRowIterator.hasNext();
    }

    @Override public void reset() {
      throw new UnsupportedOperationException();
    }

    @Override public void close() {
      try {
        batchReader.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Traditional filtered enumerator with row-by-row predicate evaluation.
   */
  static class Linq4jFilteredEnumerator implements Enumerator<Object[]> {
    private final Linq4jEnumerator baseEnumerator;
    private final FilterPredicate predicate;
    private Object[] current;

    Linq4jFilteredEnumerator(Source source, RelDataType rowType, int batchSize,
        boolean hasHeader, FilterPredicate predicate) {
      this.baseEnumerator = new Linq4jEnumerator(source, rowType, batchSize, hasHeader);
      this.predicate = predicate;
    }

    @Override public Object[] current() {
      return current;
    }

    @Override public boolean moveNext() {
      while (baseEnumerator.moveNext()) {
        Object[] row = baseEnumerator.current();

        // Apply row-by-row filtering
        if (predicate.test(row)) {
          current = row;
          return true;
        }
      }
      return false;
    }

    @Override public void reset() {
      baseEnumerator.reset();
    }

    @Override public void close() {
      baseEnumerator.close();
    }
  }

  /**
   * Simple filter predicate interface.
   */
  public interface FilterPredicate {
    boolean test(Object[] row);
  }

  /**
   * Row-by-row aggregation operations.
   */
  public static class Linq4jAggregator {

    /**
     * Row-by-row COUNT operation.
     */
    public static long count(Enumerable<Object[]> rows) {
      long count = 0;
      try (Enumerator<Object[]> enumerator = rows.enumerator()) {
        while (enumerator.moveNext()) {
          count++;
        }
      }
      return count;
    }

    /**
     * Row-by-row SUM operation.
     */
    public static double sum(Enumerable<Object[]> rows, int columnIndex) {
      double sum = 0.0;
      try (Enumerator<Object[]> enumerator = rows.enumerator()) {
        while (enumerator.moveNext()) {
          Object[] row = enumerator.current();
          Object value = row[columnIndex];
          if (value instanceof Number) {
            sum += ((Number) value).doubleValue();
          }
        }
      }
      return sum;
    }

    /**
     * Row-by-row MIN/MAX operations.
     */
    public static double min(Enumerable<Object[]> rows, int columnIndex) {
      double min = Double.MAX_VALUE;
      try (Enumerator<Object[]> enumerator = rows.enumerator()) {
        while (enumerator.moveNext()) {
          Object[] row = enumerator.current();
          Object value = row[columnIndex];
          if (value instanceof Number) {
            min = Math.min(min, ((Number) value).doubleValue());
          }
        }
      }
      return min == Double.MAX_VALUE ? 0.0 : min;
    }

    public static double max(Enumerable<Object[]> rows, int columnIndex) {
      double max = Double.MIN_VALUE;
      try (Enumerator<Object[]> enumerator = rows.enumerator()) {
        while (enumerator.moveNext()) {
          Object[] row = enumerator.current();
          Object value = row[columnIndex];
          if (value instanceof Number) {
            max = Math.max(max, ((Number) value).doubleValue());
          }
        }
      }
      return max == Double.MIN_VALUE ? 0.0 : max;
    }
  }
}
