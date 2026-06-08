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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Source;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * CSV reader that produces DataBatch objects for both Linq4j and Arrow processing.
 */
public class CsvBatchReader implements AutoCloseable {
  private final Source source;
  private final RelDataType rowType;
  private final int batchSize;
  private final boolean hasHeader;
  private CSVReader csvReader;

  public CsvBatchReader(Source source, RelDataType rowType, int batchSize, boolean hasHeader) {
    this.source = source;
    this.rowType = rowType;
    this.batchSize = batchSize;
    this.hasHeader = hasHeader;
  }

  public Iterator<DataBatch> getBatches() {
    return new BatchIterator();
  }

  /**
   * Iterator for reading CSV data in batches.
   */
  private class BatchIterator implements Iterator<DataBatch> {
    private boolean initialized = false;
    private boolean hasMore = true;

    @Override public boolean hasNext() {
      if (!initialized) {
        initialize();
      }
      return hasMore;
    }

    @Override public DataBatch next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      List<String[]> rows = new ArrayList<>(batchSize);
      String[] row;
      int count = 0;

      while (count < batchSize) {
        try {
          row = csvReader.readNext();
          if (row == null) {
            break;
          }
          rows.add(row);
          count++;
        } catch (Exception e) {
          throw new RuntimeException("Error reading CSV row", e);
        }
      }

      if (count == 0) {
        hasMore = false;
        return null; // Indicate end of data
      }

      // Check if we've reached end of file
      if (count < batchSize) {
        hasMore = false;
      }

      return new DataBatch(rows, rowType);
    }

    private void initialize() {
      try {
        csvReader = openCsv(source);
        if (hasHeader && csvReader != null) {
          try {
            csvReader.readNext(); // Skip header
          } catch (Exception e) {
            throw new RuntimeException("Error reading CSV header", e);
          }
        }
        initialized = true;
      } catch (Exception e) {
        throw new RuntimeException("Error opening CSV file", e);
      }
    }
  }

  /**
   * Opens a CSV reader, handling TSV files.
   */
  private static CSVReader openCsv(Source source) throws IOException {
    if (source.path().endsWith(".tsv")) {
      CSVParserBuilder parserBuilder = new CSVParserBuilder()
          .withSeparator('\t');
      return new CSVReaderBuilder(source.reader())
          .withCSVParser(parserBuilder.build())
          .build();
    }
    return new CSVReader(source.reader());
  }

  @Override public void close() throws Exception {
    if (csvReader != null) {
      csvReader.close();
    }
  }
}
