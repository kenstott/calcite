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
 * Maps a Census Building Permits Survey place-annual file
 * ({@code Place/<Region>/<prefix><year>a.txt}) into {@code building_permits_place} rows.
 *
 * <p>Same two-header-rows-plus-blank-separator shape as the county file
 * ({@link CensusBuildingPermitsStreamingTransformer}) but a wider, different layout — 41
 * comma-delimited positional fields carrying place identity that the county file has no room
 * for:
 *
 * <pre>
 *   0 year | 1 state FIPS | 2 6-digit id | 3 county FIPS | 4 census place | 5 FIPS place |
 *   6 FIPS MCD | 7 population | 8 CSA | 9 CBSA | 10 footnote | 11 central city | 12 ZIP |
 *   13 region | 14 division | 15 months reported | 16 place name |
 *   17-19 1-unit (bldgs, units, value) | 20-22 2-unit | 23-25 3-4 unit | 26-28 5-plus unit |
 *   29-40 the "reported" (non-imputed) duplicates — ignored, as on the county file, because the
 *         imputed 17-28 totals are Census's canonical series
 * </pre>
 *
 * <p>{@code months_reported} (field 15) is kept rather than dropped with the rest of the
 * reporting block: it is the only signal of how much of a place-year is imputed versus actually
 * reported, and at place grain — where a single small jurisdiction can report for part of a year
 * — that distinction changes how a row should be read.
 *
 * <p>The header/blank preamble is skipped by requiring a 4-digit year in field 0, matching the
 * county transformer. Each region-year file is a few hundred KB, so it is transformed in one
 * pass rather than streamed.
 */
public class CensusBuildingPermitsPlaceTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CensusBuildingPermitsPlaceTransformer.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Field count of a complete data record; anything shorter is preamble or truncated. */
  private static final int EXPECTED_FIELDS = 41;

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
          if (cols.size() < EXPECTED_FIELDS) {
            continue;
          }
          Integer year = intg(get(cols, 0));
          if (year == null || get(cols, 0).trim().length() != 4) {
            // header rows / blank separator have no 4-digit year in field 0
            continue;
          }
          String stateFips = pad(get(cols, 1), 2);
          String placeFips = pad(get(cols, 5), 5);
          String mcdFips = pad(get(cols, 6), 5);
          String countyPart = pad(get(cols, 3), 3);
          String countyFips =
              stateFips != null && countyPart != null ? stateFips + countyPart : null;

          // Two of Census's place codes are sentinels, not jurisdictions: 99990 is a county's
          // unincorporated remainder (it recurs once per county in a state) and 00000 means the
          // row is not identified by place at all — in strong-MCD states the permit-issuing
          // jurisdiction is a township/town carried in the MCD field instead, which is the
          // majority of rows nationally. Emitting "<state>00000" as a place GEOID for those
          // would let a join silently collapse every township in a county into one place that
          // does not exist, so a GEOID is emitted only where a real code backs it.
          boolean realPlace = placeFips != null && !"00000".equals(placeFips)
              && !"99990".equals(placeFips);
          boolean realMcd = mcdFips != null && !"00000".equals(mcdFips);

          ObjectNode row = MAPPER.createObjectNode();
          row.put("state_fips", stateFips);
          row.put("place_fips", placeFips);
          // 7-character state+place GEOID, the standard join key for a Census place
          row.put("place_geoid", realPlace ? stateFips + placeFips : null);
          putText(row, "place_name", get(cols, 16));
          row.put("county_fips", countyFips);
          row.put("is_unincorporated_area", "99990".equals(placeFips));
          // 10-character state+county+MCD GEOID, the join key for an MCD-reported jurisdiction
          row.put("mcd_geoid", realMcd && countyFips != null ? countyFips + mcdFips : null);
          row.put("jurisdiction_type", realPlace ? "place" : realMcd ? "mcd"
              : "99990".equals(placeFips) ? "unincorporated" : "county");
          putText(row, "census_place_code", get(cols, 4));
          row.put("mcd_fips", mcdFips);
          putInt(row, "population", get(cols, 7));
          putText(row, "csa_code", get(cols, 8));
          putText(row, "cbsa_code", get(cols, 9));
          putText(row, "footnote_code", get(cols, 10));
          putText(row, "central_city", get(cols, 11));
          putText(row, "zip_code", get(cols, 12));
          putInt(row, "region_code", get(cols, 13));
          putInt(row, "division_code", get(cols, 14));
          putInt(row, "months_reported", get(cols, 15));
          putInt(row, "units_1unit_bldgs", get(cols, 17));
          putInt(row, "units_1unit_units", get(cols, 18));
          putLong(row, "units_1unit_value", get(cols, 19));
          putInt(row, "units_2unit_bldgs", get(cols, 20));
          putInt(row, "units_2unit_units", get(cols, 21));
          putLong(row, "units_2unit_value", get(cols, 22));
          putInt(row, "units_34unit_bldgs", get(cols, 23));
          putInt(row, "units_34unit_units", get(cols, 24));
          putLong(row, "units_34unit_value", get(cols, 25));
          putInt(row, "units_5plus_bldgs", get(cols, 26));
          putInt(row, "units_5plus_units", get(cols, 27));
          putLong(row, "units_5plus_value", get(cols, 28));
          out.add(row);
          rows++;
        }
      }
      LOGGER.debug("building_permits_place: transformed {} place rows", rows);
      return MAPPER.writeValueAsString(out);
    } catch (IOException e) {
      throw new RuntimeException("building_permits_place transform failed: " + e.getMessage(), e);
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
    // fallback-guard: allow nullable-field parser; malformed text correctly yields null, not a fabricated integer
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
