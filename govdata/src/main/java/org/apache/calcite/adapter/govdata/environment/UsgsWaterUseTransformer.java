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
package org.apache.calcite.adapter.govdata.environment;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Pivots a USGS National Water-Use Science Project county-level compilation (the quinquennial
 * "Estimated Use of Water in the United States" census: 2000, 2005, 2010, 2015 — the last year
 * published at county grain before USGS moved to an annual, HUC12, model-based approach) from its
 * wide per-year layout into long-format rows of one (county, sector) withdrawal observation each.
 *
 * <p>2000/2005/2010 are fetched as the legacy tab-delimited {@code usco<year>.txt} files under
 * {@code water.usgs.gov/watuse/data/<year>/}; 2015 is fetched from its ScienceBase data-release
 * CSV (the legacy site never published a 2015 county file), which carries one non-data citation
 * line before the real header row. Column names drift release to release even though the sector
 * abbreviation convention (e.g. {@code IN-WGWFr} = Industrial groundwater withdrawal, fresh) is
 * stable from 2005 onward; 2000 uses {@code IT-} for the irrigation total instead of {@code IR-}.
 * The crosswalk in {@link #sectorsFor} was built from each year's live header row plus the 2015
 * release's FGDC metadata attribute definitions (confirmed 2026-09-21) — never guessed from
 * column-name pattern-matching, since sector codes are reused for different meanings across years
 * (e.g. {@code LI}/{@code AQ} vs. the older {@code LA}/{@code LS} livestock/aquaculture split).
 *
 * <p>Scoped to the sectors relevant to industrial water-availability screening — public supply,
 * domestic, industrial, irrigation, mining, thermoelectric, and the all-sector total — omitting
 * livestock and aquaculture, whose sector-code assignment is ambiguous in the pre-2005 files and
 * whose withdrawal volumes are immaterial to that use case.
 *
 * <p>FIPS codes are reconstructed from the per-year {@code STATEFIPS}/{@code COUNTYFIPS} fields
 * rather than the combined {@code FIPS} column: the 2000 file's {@code FIPS} column is a numeric
 * field that drops the state's leading zero (e.g. {@code 1001} instead of {@code 01001}), while
 * {@code STATEFIPS} and {@code COUNTYFIPS} are stored zero-padded as text in every year.
 */
public class UsgsWaterUseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsgsWaterUseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** One output sector row's source column names for a given year. */
  private static final class SectorCols {
    final String sector;
    final String gwFreshCol;
    final String swFreshCol;
    final String totalFreshCol;
    final String totalAllCol; // nullable — sector has no saline breakdown for this year

    SectorCols(String sector, String gwFreshCol, String swFreshCol, String totalFreshCol,
        String totalAllCol) {
      this.sector = sector;
      this.gwFreshCol = gwFreshCol;
      this.swFreshCol = swFreshCol;
      this.totalFreshCol = totalFreshCol;
      this.totalAllCol = totalAllCol;
    }
  }

  private static List<SectorCols> sectorsFor(int year) {
    // 2000 uses IT- (Irrigation Total) in place of IR- used from 2005 onward; both name the
    // same aggregate irrigation withdrawal, never broken out by saline (irrigation is fresh-only).
    String irrigationPrefix = year == 2000 ? "IT" : "IR";
    // 2000's PS-/DO- columns have no saline breakout at all (PS-Wtotl doesn't exist that year).
    String publicSupplyTotalAll = year == 2000 ? null : "PS-Wtotl";

    List<SectorCols> sectors = new ArrayList<>();
    sectors.add(new SectorCols("public_supply", "PS-WGWFr", "PS-WSWFr", "PS-WFrTo",
        publicSupplyTotalAll));
    sectors.add(new SectorCols("domestic", "DO-WGWFr", "DO-WSWFr", "DO-WFrTo", null));
    sectors.add(new SectorCols("industrial", "IN-WGWFr", "IN-WSWFr", "IN-WFrTo", "IN-Wtotl"));
    sectors.add(new SectorCols("irrigation", irrigationPrefix + "-WGWFr",
        irrigationPrefix + "-WSWFr", irrigationPrefix + "-WFrTo", null));
    sectors.add(new SectorCols("mining", "MI-WGWFr", "MI-WSWFr", "MI-WFrTo", "MI-Wtotl"));
    sectors.add(new SectorCols("thermoelectric", "PT-WGWFr", "PT-WSWFr", "PT-WFrTo",
        "PT-Wtotl"));
    sectors.add(new SectorCols("total", "TO-WGWFr", "TO-WSWFr", "TO-WFrTo", "TO-Wtotl"));
    return sectors;
  }

  @Override public String transform(String response, RequestContext context) {
    String yearStr = context.getDimensionValues().get("year");
    if (yearStr == null) {
      throw new IllegalStateException("USGS water use: no 'year' dimension value in context");
    }
    int year = Integer.parseInt(yearStr.trim());

    if (response == null || response.trim().isEmpty()) {
      LOGGER.warn("USGS water use {}: empty response from {}", year, context.getUrl());
      return "[]";
    }

    // The 2015 ScienceBase CSV carries one non-data citation line before the real header row;
    // the legacy 2000/2005/2010 tab-delimited files start with the header on line 0.
    char delimiter = year == 2015 ? ',' : '\t';
    int headerLineIndex = year == 2015 ? 1 : 0;

    List<String> lines;
    try {
      lines = splitLines(response);
    } catch (IOException e) {
      throw new RuntimeException("USGS water use " + year + ": failed to read response body", e);
    }
    if (lines.size() <= headerLineIndex) {
      throw new IllegalStateException(
          "USGS water use " + year + ": response has no header row (" + lines.size()
              + " lines)");
    }

    Map<String, Integer> colIndex = new HashMap<>();
    List<String> header = parseLine(lines.get(headerLineIndex), delimiter);
    for (int i = 0; i < header.size(); i++) {
      colIndex.put(header.get(i).trim().toUpperCase(Locale.ROOT), i);
    }
    Integer stateFipsCol = colIndex.get("STATEFIPS");
    Integer countyFipsCol = colIndex.get("COUNTYFIPS");
    if (stateFipsCol == null || countyFipsCol == null) {
      throw new IllegalStateException(
          "USGS water use " + year + ": expected STATEFIPS/COUNTYFIPS columns not found in "
              + "header — source layout may have changed");
    }

    List<SectorCols> sectors = sectorsFor(year);

    ArrayNode result = MAPPER.createArrayNode();
    for (int i = headerLineIndex + 1; i < lines.size(); i++) {
      String line = lines.get(i);
      if (line.trim().isEmpty()) {
        continue;
      }
      List<String> row = parseLine(line, delimiter);
      String stateFipsRaw = getRaw(row, stateFipsCol);
      String countyFipsRaw = getRaw(row, countyFipsCol);
      if (stateFipsRaw == null || stateFipsRaw.trim().isEmpty()
          || countyFipsRaw == null || countyFipsRaw.trim().isEmpty()) {
        continue;
      }
      String stateFips = padLeft(stateFipsRaw.trim(), 2);
      String countyFips = stateFips + padLeft(countyFipsRaw.trim(), 3);

      for (SectorCols sc : sectors) {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("state_fips", stateFips);
        out.put("county_fips", countyFips);
        out.put("year", year);
        out.put("sector", sc.sector);
        putDoubleOrNull(out, "groundwater_fresh_mgd",
            parseDouble(getCol(row, colIndex, sc.gwFreshCol, year)));
        putDoubleOrNull(out, "surfacewater_fresh_mgd",
            parseDouble(getCol(row, colIndex, sc.swFreshCol, year)));
        putDoubleOrNull(out, "total_fresh_mgd",
            parseDouble(getCol(row, colIndex, sc.totalFreshCol, year)));
        putDoubleOrNull(out, "total_withdrawal_mgd",
            sc.totalAllCol == null ? null
                : parseDouble(getCol(row, colIndex, sc.totalAllCol, year)));
        result.add(out);
      }
    }

    LOGGER.debug("USGS water use {}: parsed {} sector-county rows", year, result.size());
    return result.toString();
  }

  private static List<String> splitLines(String text) throws IOException {
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new StringReader(text))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    }
    return lines;
  }

  /** Splits one line on {@code delimiter}; comma mode is quote-aware (2015's CSV release). */
  private static List<String> parseLine(String line, char delimiter) {
    if (delimiter == '\t') {
      List<String> fields = new ArrayList<>();
      int start = 0;
      for (int i = 0; i <= line.length(); i++) {
        if (i == line.length() || line.charAt(i) == '\t') {
          fields.add(line.substring(start, i));
          start = i + 1;
        }
      }
      return fields;
    }
    List<String> fields = new ArrayList<>();
    StringBuilder cur = new StringBuilder();
    boolean inQuotes = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
            cur.append('"');
            i++;
          } else {
            inQuotes = false;
          }
        } else {
          cur.append(c);
        }
      } else if (c == '"') {
        inQuotes = true;
      } else if (c == delimiter) {
        fields.add(cur.toString());
        cur.setLength(0);
      } else {
        cur.append(c);
      }
    }
    fields.add(cur.toString());
    return fields;
  }

  private static String getRaw(List<String> row, int index) {
    return index < row.size() ? row.get(index) : null;
  }

  private static String getCol(List<String> row, Map<String, Integer> colIndex, String colName,
      int year) {
    Integer idx = colIndex.get(colName.toUpperCase(Locale.ROOT));
    if (idx == null) {
      throw new IllegalStateException(
          "USGS water use " + year + ": expected column '" + colName + "' not found in header "
              + "— crosswalk in UsgsWaterUseTransformer.sectorsFor is stale for this year");
    }
    return getRaw(row, idx);
  }

  private static Double parseDouble(String raw) {
    if (raw == null) {
      return null;
    }
    String trimmed = raw.trim();
    if (trimmed.isEmpty() || "--".equals(trimmed) || "N/A".equalsIgnoreCase(trimmed)) {
      return null;
    }
    try {
      return Double.parseDouble(trimmed);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static void putDoubleOrNull(ObjectNode out, String field, Double value) {
    if (value == null) {
      out.putNull(field);
    } else {
      out.put(field, value);
    }
  }

  private static String padLeft(String value, int width) {
    StringBuilder sb = new StringBuilder(value);
    while (sb.length() < width) {
      sb.insert(0, '0');
    }
    return sb.toString();
  }
}
