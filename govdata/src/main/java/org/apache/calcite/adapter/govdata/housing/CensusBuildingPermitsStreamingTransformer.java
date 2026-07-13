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
package org.apache.calcite.adapter.govdata.housing;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

/**
 * Maps a Census Building Permits Survey county-annual file ({@code co<year>a.txt}) into
 * {@code building_permits} rows. The file has two header rows and a blank separator line before
 * the data; columns are positional (comma-delimited, fixed layout):
 *
 * <pre>
 *   0 year | 1 state FIPS | 2 county FIPS | 3 region | 4 division | 5 county name |
 *   6-8   1-unit (bldgs, units, value) | 9-11  2-unit | 12-14 3-4 unit | 15-17 5-plus unit |
 *   18-29 the "reported" (non-imputed) duplicates — ignored, the imputed 6-17 totals are canonical
 * </pre>
 *
 * <p>The header/blank preamble is skipped by requiring a 4-digit year in column 0. The response
 * body is a modest per-year file (~0.4 MB), so it is transformed in one pass rather than streamed.
 */
public class CensusBuildingPermitsStreamingTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CensusBuildingPermitsStreamingTransformer.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      return "[]";
    }
    try {
      ArrayNode out = MAPPER.createArrayNode();
      int rows = 0;
      try (BufferedReader reader = new BufferedReader(new StringReader(response))) {
        String record;
        while ((record = CsvRecordReader.readRecord(reader)) != null) {
          List<String> cols = CsvRecordReader.splitFields(record, ',');
          if (cols.size() < 18) {
            continue;
          }
          Integer year = intg(get(cols, 0));
          if (year == null || get(cols, 0).trim().length() != 4) {
            // header rows / blank separator have no 4-digit year in column 0
            continue;
          }
          String stateFips = pad(get(cols, 1), 2);
          String countyPart = pad(get(cols, 2), 3);
          ObjectNode row = MAPPER.createObjectNode();
          row.put("state_fips", stateFips);
          row.put("county_fips",
              stateFips != null && countyPart != null ? stateFips + countyPart : null);
          putText(row, "county_name", get(cols, 5));
          putInt(row, "region_code", get(cols, 3));
          putInt(row, "division_code", get(cols, 4));
          putInt(row, "units_1unit_bldgs", get(cols, 6));
          putInt(row, "units_1unit_units", get(cols, 7));
          putLong(row, "units_1unit_value", get(cols, 8));
          putInt(row, "units_2unit_bldgs", get(cols, 9));
          putInt(row, "units_2unit_units", get(cols, 10));
          putLong(row, "units_2unit_value", get(cols, 11));
          putInt(row, "units_34unit_bldgs", get(cols, 12));
          putInt(row, "units_34unit_units", get(cols, 13));
          putLong(row, "units_34unit_value", get(cols, 14));
          putInt(row, "units_5plus_bldgs", get(cols, 15));
          putInt(row, "units_5plus_units", get(cols, 16));
          putLong(row, "units_5plus_value", get(cols, 17));
          out.add(row);
          rows++;
        }
      }
      LOGGER.debug("building_permits: transformed {} county rows", rows);
      return MAPPER.writeValueAsString(out);
    } catch (IOException e) {
      throw new RuntimeException("building_permits transform failed: " + e.getMessage(), e);
    }
  }

  private static String get(List<String> cols, int i) {
    return i < cols.size() ? cols.get(i) : null;
  }

  private static void putText(ObjectNode row, String col, String v) {
    if (v == null || v.trim().isEmpty()) {
      row.putNull(col);
    } else {
      row.put(col, v.trim());
    }
  }

  private static void putInt(ObjectNode row, String col, String v) {
    Integer n = intg(v);
    if (n == null) {
      row.putNull(col);
    } else {
      row.put(col, n);
    }
  }

  private static void putLong(ObjectNode row, String col, String v) {
    if (v == null) {
      row.putNull(col);
      return;
    }
    String s = v.trim();
    if (s.isEmpty()) {
      row.putNull(col);
      return;
    }
    try {
      row.put(col, (long) Double.parseDouble(s));
    } catch (NumberFormatException e) {
      row.putNull(col);
    }
  }

  private static Integer intg(String v) {
    if (v == null) {
      return null;
    }
    String s = v.trim();
    if (s.isEmpty()) {
      return null;
    }
    try {
      return (int) Double.parseDouble(s);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String pad(String v, int width) {
    if (v == null) {
      return null;
    }
    String s = v.trim();
    if (s.isEmpty()) {
      return null;
    }
    while (s.length() < width) {
      s = "0" + s;
    }
    return s.length() > width ? s.substring(s.length() - width) : s;
  }
}
