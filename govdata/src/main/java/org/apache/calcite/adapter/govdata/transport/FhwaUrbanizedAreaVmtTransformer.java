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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses FHWA Highway Statistics table <b>HM-71</b> ("Urbanized Areas: Miles and Daily
 * Vehicle-Miles Traveled") into {@code urbanized_area_vmt} rows.
 *
 * <p>The workbook is located and downloaded by {@link FhwaHighwayStatistics}; the source URL of
 * the table is not read. It is {@code .xls} in the archive years (2022 and earlier) and
 * {@code .xlsx} from 2023. Areas are spread over eight
 * data sheets named {@code A} to {@code H} (2012-2015 workbooks name the last one {@code I});
 * each row is: A area name, B census population,
 * C-J lane-miles (interstate, other freeways and expressways, other principal arterial, minor
 * arterial, major collector, minor collector, local, total), K-R daily VMT in thousands for
 * the same eight classes. Zero values are shown as a dash by the
 * workbook's number format but are stored as numeric zero, so every measure cell is numeric.
 */
public class FhwaUrbanizedAreaVmtTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FhwaUrbanizedAreaVmtTransformer.class);
  private static final String[] CLASSES = {"interstate", "freeways_expressways",
      "other_principal_arterial", "minor_arterial", "major_collector", "minor_collector",
      "local", "total"};
  private static final int POPULATION_COL = 1;
  private static final int MILES_COL = 2;
  private static final int VMT_COL = MILES_COL + CLASSES.length;

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        "urbanized_area_vmt is a StreamingResponseTransformer; call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context) {
    int year = Integer.parseInt(context.getDimensionValues().get("effective_year"));
    FhwaHighwayStatistics.Workbook source =
        FhwaHighwayStatistics.fetch(context, "hm71", "xls", year);
    try {
      List<Map<String, Object>> rows = parseWorkbook(source.bytes, source.url);
      LOGGER.debug("urbanized_area_vmt: {} area rows from {}", rows.size(), source.url);
      return rows.iterator();
    } catch (IOException e) {
      throw new RuntimeException("urbanized_area_vmt: failed to parse HM-71 from " + source.url
          + ": " + e.getMessage(), e);
    }
  }

  private static List<Map<String, Object>> parseWorkbook(byte[] bytes, String url)
      throws IOException {
    Workbook wb = WorkbookFactory.create(new ByteArrayInputStream(bytes));
    try {
      List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
      int sheets = 0;
      for (int i = 0; i < wb.getNumberOfSheets(); i++) {
        Sheet sheet = wb.getSheetAt(i);
        String sn = sheet.getSheetName();
        if (sn.length() != 1 || sn.charAt(0) < 'A' || sn.charAt(0) > 'I') {
          continue;
        }
        sheets++;
        for (int r = 0; r <= sheet.getLastRowNum(); r++) {
          Map<String, Object> o = parseRow(sheet.getRow(r), url);
          if (o != null) {
            out.add(o);
          }
        }
      }
      if (sheets != 8 || out.isEmpty()) {
        throw new IOException("expected 8 data sheets with rows, found " + sheets
            + " sheets and " + out.size() + " rows in " + url);
      }
      return out;
    } finally {
      wb.close();
    }
  }

  private static Map<String, Object> parseRow(Row row, String url) throws IOException {
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
    Map<String, Object> o = new LinkedHashMap<String, Object>();
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

  private static void putMeasure(Map<String, Object> o, String col, Cell cell, String area, String url)
      throws IOException {
    if (cell == null || cell.getCellType() != CellType.NUMERIC) {
      throw new IOException("non-numeric " + col + " cell for '" + area + "' in " + url);
    }
    o.put(col, cell.getNumericCellValue());
  }
}
