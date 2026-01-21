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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;

/**
 * TIGER States table.
 */
public class TigerStatesTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TigerStatesTable.class);

  private final TigerDataDownloader tigerDownloader;

  public TigerStatesTable(TigerDataDownloader tigerDownloader) {
    this.tigerDownloader = tigerDownloader;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("state_fips", SqlTypeName.VARCHAR)
        .add("state_code", SqlTypeName.VARCHAR)  // 2-letter state code for FK from SEC
        .add("state_name", SqlTypeName.VARCHAR)
        .add("state_abbr", SqlTypeName.VARCHAR)
        .add("land_area", SqlTypeName.DOUBLE)
        .add("water_area", SqlTypeName.DOUBLE)
        .build();
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        try {
          LOGGER.info("Fetching TIGER states data");

          // Download states shapefiles for all years
          tigerDownloader.downloadStates();

          // For now, return stub data - would need shapefile parsing
          // TODO: Iterate through year partitions and merge data
          LOGGER.warn("TIGER shapefile parsing not yet implemented, returning empty result");
          return new TigerStatesEnumerator();

        } catch (Exception e) {
          LOGGER.error("Error fetching TIGER states data", e);
          throw new RuntimeException("Failed to fetch TIGER states data", e);
        }
      }
    };
  }

  private static class TigerStatesEnumerator implements Enumerator<Object[]> {
    private final Iterator<Object[]> iterator;
    private Object[] current;

    TigerStatesEnumerator() {
      this.iterator = Collections.emptyIterator();
    }

    @Override public Object[] current() {
      return current;
    }

    @Override public boolean moveNext() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      }
      return false;
    }

    @Override public void reset() {
      throw new UnsupportedOperationException("Reset not supported");
    }

    @Override public void close() {
      // Nothing to close
    }
  }
}
