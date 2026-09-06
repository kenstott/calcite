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
package org.apache.calcite.adapter.govdata.disasters;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.govdata.energy.EiaBulkXlsxTransformer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Transforms the USFS Wildfire Risk to Communities (WRC) county-summary workbook's
 * "Counties" sheet into JSON rows.
 *
 * <p>The workbook's own {@code GEOID} column is stored as a numeric Excel value and
 * silently loses its leading zero for FIPS codes under 10000 (e.g. Alabama's 01001
 * becomes 1001); {@code GEOIDFQ} carries the same identifier as text
 * ({@code "0500000US01001"}) with the zero-padding intact, so county/state FIPS are
 * derived from its trailing 5 characters instead of trusting GEOID directly.
 */
public class WildfireRiskByCountyTransformer extends EiaBulkXlsxTransformer {

  private static final String SHEET_NAME = "Counties";

  @Override
  public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    XSSFWorkbook workbook = null;
    try {
      byte[] bytes = downloadBytes(url);
      // The WRC workbook embeds a high-resolution logo image alongside its data sheets,
      // which can trip POI's zip-bomb guard (min inflate ratio) on an otherwise trusted
      // federal publication.
      ZipSecureFile.setMinInflateRatio(0.0);
      workbook = new XSSFWorkbook(new ByteArrayInputStream(bytes));
      return parseCountiesSheet(workbook);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse USFS Wildfire Risk to Communities XLSX from "
          + url, e);
    } finally {
      if (workbook != null) {
        try {
          workbook.close();
        } catch (Exception e) {
          LOGGER.warn("Failed to close workbook for {}: {}", url, e.getMessage());
        }
      }
    }
  }

  private String parseCountiesSheet(XSSFWorkbook workbook) {
    Sheet sheet = workbook.getSheet(SHEET_NAME);
    if (sheet == null) {
      LOGGER.error("Wildfire risk by county: sheet '{}' not found", SHEET_NAME);
      return "[]";
    }

    Row headerRow = sheet.getRow(0);
    if (headerRow == null) {
      LOGGER.error("Wildfire risk by county: header row missing");
      return "[]";
    }

    Map<String, Integer> colIndex = new HashMap<String, Integer>();
    int lastCol = headerRow.getLastCellNum();
    for (int c = 0; c < lastCol; c++) {
      String h = cellString(headerRow.getCell(c));
      if (h != null) {
        colIndex.put(h.trim(), c);
      }
    }

    ArrayNode result = MAPPER.createArrayNode();
    for (int r = 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String geoidfq = cellString(row.getCell(colIndex.get("GEOIDFQ")));
      if (geoidfq == null || geoidfq.length() < 5) {
        continue;
      }
      String countyFips = geoidfq.substring(geoidfq.length() - 5);
      String stateFips = countyFips.substring(0, 2);

      ObjectNode out = MAPPER.createObjectNode();
      out.put("state_fips", stateFips);
      out.put("county_fips", countyFips);
      putString(out, "state_abbr", row, colIndex.get("STUSPS"));
      putString(out, "county_name", row, colIndex.get("NAME"));
      putLongOrNull(out, "total_buildings", row, colIndex.get("TOTAL_BUILDINGS"));
      putDoubleOrNull(out, "buildings_fraction_minimal_exposure", row,
          colIndex.get("BUILDINGS_FRACTION_ME"));
      putDoubleOrNull(out, "buildings_fraction_indirect_exposure", row,
          colIndex.get("BUILDINGS_FRACTION_IE"));
      putDoubleOrNull(out, "buildings_fraction_direct_exposure", row,
          colIndex.get("BUILDINGS_FRACTION_DE"));
      putDoubleOrNull(out, "burn_probability_state_rank", row, colIndex.get("BP_STATE_RANK"));
      putDoubleOrNull(out, "burn_probability_national_rank", row,
          colIndex.get("BP_NATIONAL_RANK"));
      putDoubleOrNull(out, "risk_state_rank", row, colIndex.get("RISK_STATE_RANK"));
      putDoubleOrNull(out, "risk_national_rank", row, colIndex.get("RISK_NATIONAL_RANK"));
      result.add(out);
    }
    LOGGER.debug("Wildfire risk by county: parsed {} rows", result.size());
    return result.toString();
  }

  private void putString(ObjectNode out, String field, Row row, Integer col) {
    String v = col == null ? null : cellString(row.getCell(col));
    if (v == null) {
      out.putNull(field);
    } else {
      out.put(field, v);
    }
  }

  private void putDoubleOrNull(ObjectNode out, String field, Row row, Integer col) {
    Double v = col == null ? null : cellDouble(row.getCell(col));
    if (v == null) {
      out.putNull(field);
    } else {
      out.put(field, v.doubleValue());
    }
  }

  private void putLongOrNull(ObjectNode out, String field, Row row, Integer col) {
    Double v = col == null ? null : cellDouble(row.getCell(col));
    if (v == null) {
      out.putNull(field);
    } else {
      out.put(field, v.longValue());
    }
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) { // NOSONAR - required by base class
    // Not used — transform() is overridden to select the "Counties" sheet by name.
    return "[]";
  }
}
