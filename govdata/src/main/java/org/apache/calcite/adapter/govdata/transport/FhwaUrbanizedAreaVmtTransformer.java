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
 * Parses FHWA Highway Statistics table <b>HM-71</b> ("Urbanized Areas: Miles and Daily
 * Vehicle-Miles Traveled") into {@code urbanized_area_vmt} rows.
 *
 * <p>The workbook is at {@code statistics/{year}/xls/hm71.xls} for 2022 and earlier and
 * {@code hm71.xlsx} from 2023 (verified 2026-11-19), so the URL is built from the
 * {@code effective_year} dimension and the HTTP body is ignored. Areas are spread over eight
 * data sheets named {@code A} to {@code H} (2012-2015 workbooks name the last one {@code I}); each row is: A area name, B census population,
 * C-J lane-miles (interstate, other freeways and expressways, other principal arterial, minor
 * arterial, major collector, minor collector, local, total), K-R daily VMT in thousands for
 * the same eight classes. Zero values are shown as a dash by the
 * workbook's number format but are stored as numeric zero, so every measure cell is numeric.
 */
public class FhwaUrbanizedAreaVmtTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FhwaUrbanizedAreaVmtTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String BASE = "https://www.fhwa.dot.gov/policyinformation/statistics/";
  private static final int LAST_XLS_YEAR = 2022;
  private static final String[] CLASSES = {"interstate", "freeways_expressways",
      "other_principal_arterial", "minor_arterial", "major_collector", "minor_collector",
      "local", "total"};
  private static final int POPULATION_COL = 1;
  private static final int MILES_COL = 2;
  private static final int VMT_COL = MILES_COL + CLASSES.length;

  @Override public String transform(String response, RequestContext context) {
    String yearValue = context.getDimensionValues().get("effective_year");
    int year = Integer.parseInt(yearValue);
    String url = BASE + yearValue + "/xls/hm71." + (year <= LAST_XLS_YEAR ? "xls" : "xlsx");
    try {
      byte[] bytes = downloadBytes(url);
      Workbook wb = WorkbookFactory.create(new ByteArrayInputStream(bytes));
      try {
        ArrayNode out = MAPPER.createArrayNode();
        int sheets = 0;
        for (int i = 0; i < wb.getNumberOfSheets(); i++) {
          Sheet sheet = wb.getSheetAt(i);
          String sn = sheet.getSheetName();
          if (sn.length() != 1 || sn.charAt(0) < 'A' || sn.charAt(0) > 'I') {
            continue;
          }
          sheets++;
          for (int r = 0; r <= sheet.getLastRowNum(); r++) {
            ObjectNode o = parseRow(sheet.getRow(r), url);
            if (o != null) {
              out.add(o);
            }
          }
        }
        if (sheets != 8 || out.size() == 0) {
          throw new IOException("expected 8 data sheets with rows, found " + sheets
              + " sheets and " + out.size() + " rows in " + url);
        }
        LOGGER.debug("urbanized_area_vmt: {} area rows from {}", out.size(), url);
        return MAPPER.writeValueAsString(out);
      } finally {
        wb.close();
      }
    } catch (Exception e) {
      // The message carries the cause so the pipeline recognises "HTTP 404" (year not yet
      // posted) and marks the period unavailable instead of failing it.
      throw new RuntimeException(
          "urbanized_area_vmt transform failed for " + url + ": " + e.getMessage(), e);
    }
  }

  private static ObjectNode parseRow(Row row, String url) throws IOException {
    if (row == null) {
      return null;
    }
    Cell nameCell = row.getCell(0);
    Cell popCell = row.getCell(POPULATION_COL);
    if (nameCell == null || nameCell.getCellType() != CellType.STRING
        || popCell == null || popCell.getCellType() != CellType.NUMERIC) {
      return null;
    }
    String name = nameCell.getStringCellValue().trim();
    if (name.isEmpty() || "TOTAL".equalsIgnoreCase(name)) {
      return null;
    }
    ObjectNode o = MAPPER.createObjectNode();
    int comma = name.lastIndexOf(", ");
    if (comma < 0) {
      throw new IOException("area name without state list '" + name + "' in " + url);
    }
    o.put("urbanized_area", name.substring(0, comma).trim());
    o.put("states", name.substring(comma + 2).trim());
    o.put("population", Math.round(popCell.getNumericCellValue()));
    for (int k = 0; k < CLASSES.length; k++) {
      putMeasure(o, "miles_" + CLASSES[k], row.getCell(MILES_COL + k), name, url);
      putMeasure(o, "dvmt_thousands_" + CLASSES[k], row.getCell(VMT_COL + k), name, url);
    }
    return o;
  }

  private static void putMeasure(ObjectNode o, String col, Cell cell, String area, String url)
      throws IOException {
    if (cell == null || cell.getCellType() != CellType.NUMERIC) {
      throw new IOException("non-numeric " + col + " cell for '" + area + "' in " + url);
    }
    o.put(col, cell.getNumericCellValue());
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
}
