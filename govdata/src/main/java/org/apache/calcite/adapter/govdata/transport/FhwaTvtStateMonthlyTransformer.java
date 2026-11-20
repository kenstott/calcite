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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;

/**
 * Parses Table 5 ("Changes on ALL Estimated Roads by Region and State") of the FHWA
 * <b>Traffic Volume Trends</b> monthly workbook into {@code tvt_state_monthly} rows.
 *
 * <p>The monthly workbook lives at
 * {@code travel_monitoring/{yy}{mon}tvt/{yy}{mon}tvt.xlsx} (e.g. {@code 26augtvt}); the
 * two-digit year cannot be rendered by the URL templater, so this class builds the URL from
 * the {@code year} and {@code month} dimensions and ignores the HTTP body, as the MV-1
 * transformer does. Table 5 is located by its title cell (the sheet index shifts between
 * releases). Layout is positional within the sheet: A state, D stations, E current-year
 * vehicle-miles (millions, preliminary), F prior-year vehicle-miles, G percent change.
 */
public class FhwaTvtStateMonthlyTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FhwaTvtStateMonthlyTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String BASE =
      "https://www.fhwa.dot.gov/policyinformation/travel_monitoring/";
  private static final String[] MONTHS = {"jan", "feb", "mar", "apr", "may", "jun",
      "jul", "aug", "sep", "oct", "nov", "dec"};
  private static final String TITLE = "Table - 5.";

  @Override public String transform(String response, RequestContext context) {
    String year = context.getDimensionValues().get("effective_year");
    String monthValue = context.getDimensionValues().get("month");
    int month = Integer.parseInt(monthValue);
    String token = year.substring(year.length() - 2) + MONTHS[month - 1] + "tvt";
    String url = BASE + token + "/" + token + ".xlsx";
    try {
      byte[] bytes = downloadBytes(url);
      Workbook wb = WorkbookFactory.create(new ByteArrayInputStream(bytes));
      try {
        Sheet sheet = findTable5(wb);
        if (sheet == null) {
          throw new IOException("Table 5 sheet not found in " + url);
        }
        ArrayNode out = MAPPER.createArrayNode();
        for (int r = 0; r <= sheet.getLastRowNum(); r++) {
          Row row = sheet.getRow(r);
          if (row == null) {
            continue;
          }
          String name = cellString(row.getCell(0));
          if (name == null) {
            continue;
          }
          String fips =
              FhwaVehicleRegistrationsTransformer.STATE_FIPS.get(name.trim().toUpperCase());
          if (fips == null) {
            continue;
          }
          ObjectNode o = MAPPER.createObjectNode();
          o.put("state_fips", fips);
          o.put("state_name", name.trim());
          putNumber(o, "stations", row.getCell(3), true);
          putNumber(o, "vmt_millions", row.getCell(4), true);
          putNumber(o, "prior_year_vmt_millions", row.getCell(5), true);
          putNumber(o, "pct_change", row.getCell(6), false);
          out.add(o);
        }
        LOGGER.debug("tvt_state_monthly: {} state rows from {}", out.size(), url);
        return MAPPER.writeValueAsString(out);
      } finally {
        wb.close();
      }
    } catch (Exception e) {
      // The message carries the cause so the pipeline recognises "HTTP 404" (month not yet
      // posted) and marks the period unavailable instead of failing it.
      throw new RuntimeException(
          "tvt_state_monthly transform failed for " + url + ": " + e.getMessage(), e);
    }
  }

  private static Sheet findTable5(Workbook wb) {
    for (int i = 0; i < wb.getNumberOfSheets(); i++) {
      Sheet s = wb.getSheetAt(i);
      int limit = Math.min(s.getLastRowNum(), 5);
      for (int r = 0; r <= limit; r++) {
        Row row = s.getRow(r);
        if (row == null) {
          continue;
        }
        for (Cell c : row) {
          if (c.getCellType() == CellType.STRING
              && c.getStringCellValue().trim().startsWith(TITLE)) {
            return s;
          }
        }
      }
    }
    return null;
  }

  private static byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    InputStream is = conn.getInputStream();
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
      return baos.toByteArray();
    } finally {
      is.close();
    }
  }

  private static String cellString(Cell cell) {
    if (cell == null || cell.getCellType() != CellType.STRING) {
      return null;
    }
    String s = cell.getStringCellValue();
    return s.trim().isEmpty() ? null : s;
  }

  private static void putNumber(ObjectNode row, String col, Cell cell, boolean asLong) {
    if (cell == null || cell.getCellType() != CellType.NUMERIC) {
      row.putNull(col);
      return;
    }
    if (asLong) {
      row.put(col, Math.round(cell.getNumericCellValue()));
    } else {
      row.put(col, cell.getNumericCellValue());
    }
  }
}
