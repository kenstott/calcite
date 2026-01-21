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

import java.util.Iterator;
import java.util.List;

/**
 * HUD ZIP-County crosswalk table.
 *
 * <p>Provides ZIP code to county FIPS mappings with residential/business address ratios.
 */
public class HudZipCountyTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HudZipCountyTable.class);

  private final HudCrosswalkFetcher hudFetcher;

  public HudZipCountyTable(HudCrosswalkFetcher hudFetcher) {
    this.hudFetcher = hudFetcher;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("zip", SqlTypeName.VARCHAR)
        .add("county_fips", SqlTypeName.VARCHAR)
        .add("res_ratio", SqlTypeName.DOUBLE)
        .add("bus_ratio", SqlTypeName.DOUBLE)
        .add("oth_ratio", SqlTypeName.DOUBLE)
        .add("tot_ratio", SqlTypeName.DOUBLE)
        .add("city", SqlTypeName.VARCHAR)
        .add("state", SqlTypeName.VARCHAR)
        .build();
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        try {
          LOGGER.info("Fetching HUD ZIP-County crosswalk data for Q2 2024");

          // Download latest crosswalk data from HUD API
          java.io.File csvFile = hudFetcher.downloadZipToCounty("2", 2024);

          // Load the actual crosswalk records from the CSV file
          List<HudCrosswalkFetcher.CrosswalkRecord> records = hudFetcher.loadCrosswalkData(csvFile);
          return new HudZipCountyEnumerator(records);

        } catch (Exception e) {
          LOGGER.error("Error fetching HUD ZIP-County data", e);
          throw new RuntimeException("Failed to fetch HUD ZIP-County data", e);
        }
      }
    };
  }

  private static class HudZipCountyEnumerator implements Enumerator<Object[]> {
    private final Iterator<Object[]> iterator;
    private Object[] current;

    HudZipCountyEnumerator(List<HudCrosswalkFetcher.CrosswalkRecord> records) {
      // Convert HUD crosswalk records to Object arrays
      this.iterator = records.stream()
          .map(record -> new Object[] {
              record.zip,
              record.geoCode,
              record.resRatio,
              record.busRatio,
              record.othRatio,
              record.totRatio,
              record.city,
              record.state
          })
          .iterator();
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
