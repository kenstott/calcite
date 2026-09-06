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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the FHWA Highway Statistics <b>HM-64</b> table (functional-system road mileage
 * by measured pavement roughness) into {@code pavement_roughness} rows.
 *
 * <p>FHWA publishes HM-64 as 4 sheets — "A" (Rural Interstate), "B" (Rural Other
 * classes), "C" (Urban Interstate), "D" (Urban Other classes) — each a merged
 * multi-header layout with one or more road-class blocks laid out side by side, every
 * block sharing an identical fixed 10-column shape (verified 2026-09-06, sheets named
 * literally "A"/"B"/"C"/"D"):
 * <pre>
 *   col+0  NOT REPORTED   col+1..+8  IRI buckets (&lt;60 ... &gt;220)   col+9  TOTAL REPORTED
 * </pre>
 * The area type (rural/urban) is read from the sheet's own title text rather than
 * hardcoded by sheet letter, and each block's road class is read from its own header
 * cell — both because a fixed row/sheet-letter assumption is exactly the kind of
 * silent-misattribution bug this table's federal-spreadsheet siblings (MV-1, OMB Table
 * 3.2) have already shown themselves prone to. Only the "STATE" anchor row and 10-column
 * block width are treated as truly fixed, since FHWA has kept those unchanged for years.
 *
 * <p>The HTTP body is UTF-8-decoded and useless for a binary XLSX, so — as
 * {@link FhwaVehicleRegistrationsTransformer} does — this class ignores {@code response}
 * and re-downloads the URL as raw bytes, then reads it with POI via
 * {@link WorkbookFactory} (content-sniffing, so it also handles any {@code .xls} year).
 * State FIPS is derived from the cleaned state name for the geo join; the {@code year}
 * partition comes from the {@code effective_year} dimension, so it is not emitted here.
 */
public class FhwaPavementRoughnessTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FhwaPavementRoughnessTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String[] SHEET_NAMES = {"A", "B", "C", "D"};
  private static final int BLOCK_WIDTH = 10;

  /** Strips a trailing footnote marker such as " (2)" or " (3)" from header/state text. */
  private static final Pattern FOOTNOTE = Pattern.compile("\\s*\\(\\d+\\)\\s*$");

  private static final Map<String, String> STATE_FIPS = buildStateFips();

  @Override public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    try {
      byte[] bytes = downloadBytes(url);
      Workbook wb = WorkbookFactory.create(new ByteArrayInputStream(bytes));
      try {
        ArrayNode out = MAPPER.createArrayNode();
        for (String sheetName : SHEET_NAMES) {
          Sheet sheet = wb.getSheet(sheetName);
          if (sheet == null) {
            LOGGER.warn("pavement_roughness: sheet '{}' not found in {}", sheetName, url);
            continue;
          }
          parseSheet(sheet, out);
        }
        LOGGER.debug("pavement_roughness: transformed {} rows from {}", out.size(), url);
        return MAPPER.writeValueAsString(out);
      } finally {
        wb.close();
      }
    } catch (Exception e) {
      LOGGER.error("pavement_roughness: failed to parse HM-64 from {}: {}", url, e.getMessage(), e);
      throw new RuntimeException("pavement_roughness transform failed for " + url, e);
    }
  }

  private void parseSheet(Sheet sheet, ArrayNode out) {
    String areaType = findAreaType(sheet);
    if (areaType == null) {
      LOGGER.warn("pavement_roughness: could not determine area type for sheet '{}'",
          sheet.getSheetName());
      return;
    }
    Row stateHeaderRow = findStateHeaderRow(sheet);
    if (stateHeaderRow == null) {
      LOGGER.warn("pavement_roughness: 'STATE' header row not found in sheet '{}'",
          sheet.getSheetName());
      return;
    }
    Row blockHeaderRow = sheet.getRow(stateHeaderRow.getRowNum() - 1);
    if (blockHeaderRow == null) {
      LOGGER.warn("pavement_roughness: block header row not found in sheet '{}'",
          sheet.getSheetName());
      return;
    }
    List<Block> blocks = findBlocks(blockHeaderRow);
    if (blocks.isEmpty()) {
      LOGGER.warn("pavement_roughness: no road-class blocks found in sheet '{}'",
          sheet.getSheetName());
      return;
    }

    int firstDataRow = stateHeaderRow.getRowNum() + 2; // skip the bucket-label sub-header row
    int lastRow = sheet.getLastRowNum();
    for (int r = firstDataRow; r <= lastRow; r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String rawState = cellString(row.getCell(0));
      if (rawState == null) {
        continue;
      }
      String stateName = FOOTNOTE.matcher(rawState.trim()).replaceAll("").trim();
      String fips = STATE_FIPS.get(stateName.toUpperCase(java.util.Locale.ROOT));
      if (fips == null) {
        // Skips "U.S. Total", "Grand Total", footnote text, and any other non-state row.
        continue;
      }
      for (Block block : blocks) {
        ObjectNode o = MAPPER.createObjectNode();
        o.put("state_fips", fips);
        o.put("state_name", stateName);
        o.put("area_type", areaType);
        o.put("road_class", block.roadClass);
        putDouble(o, "miles_not_reported", row.getCell(block.startCol));
        putDouble(o, "miles_iri_under_60", row.getCell(block.startCol + 1));
        putDouble(o, "miles_iri_60_94", row.getCell(block.startCol + 2));
        putDouble(o, "miles_iri_95_119", row.getCell(block.startCol + 3));
        putDouble(o, "miles_iri_120_144", row.getCell(block.startCol + 4));
        putDouble(o, "miles_iri_145_170", row.getCell(block.startCol + 5));
        putDouble(o, "miles_iri_171_194", row.getCell(block.startCol + 6));
        putDouble(o, "miles_iri_195_220", row.getCell(block.startCol + 7));
        putDouble(o, "miles_iri_over_220", row.getCell(block.startCol + 8));
        putDouble(o, "miles_total_reported", row.getCell(block.startCol + 9));
        out.add(o);
      }
    }
  }

  /** Reads the sheet's own title text (e.g. "MILES BY MEASURED PAVEMENT ROUGHNESS - RURAL")
   * rather than assuming a fixed sheet-letter-to-area mapping. */
  private String findAreaType(Sheet sheet) {
    for (int r = 0; r <= Math.min(10, sheet.getLastRowNum()); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String s = cellString(row.getCell(0));
      if (s == null) {
        continue;
      }
      String upper = s.toUpperCase(java.util.Locale.ROOT);
      if (upper.contains("RURAL")) {
        return "rural";
      }
      if (upper.contains("URBAN")) {
        return "urban";
      }
    }
    return null;
  }

  private Row findStateHeaderRow(Sheet sheet) {
    for (int r = 0; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String s = cellString(row.getCell(0));
      if (s != null && "STATE".equalsIgnoreCase(s.trim())) {
        return row;
      }
    }
    return null;
  }

  /** Each populated cell in the block header row marks the start column of a 10-column
   * road-class block; the cell's text (footnote-stripped) names the road class. */
  private List<Block> findBlocks(Row blockHeaderRow) {
    List<Block> blocks = new ArrayList<Block>();
    int lastCol = blockHeaderRow.getLastCellNum();
    for (int c = 1; c < lastCol; c++) {
      String h = cellString(blockHeaderRow.getCell(c));
      if (h == null) {
        continue;
      }
      String roadClass = normalizeRoadClass(h);
      if (roadClass != null) {
        blocks.add(new Block(c, roadClass));
      }
    }
    return blocks;
  }

  private String normalizeRoadClass(String header) {
    String cleaned = FOOTNOTE.matcher(header.trim()).replaceAll("").trim()
        .replaceAll("\\s+", " ").toUpperCase(java.util.Locale.ROOT);
    if ("INTERSTATE".equals(cleaned)) {
      return "interstate";
    }
    if (cleaned.startsWith("OTHER FREEWAYS")) {
      return "other_freeways_expressways";
    }
    if (cleaned.startsWith("OTHER PRINCIPAL ARTERIAL")) {
      return "other_principal_arterial";
    }
    if (cleaned.startsWith("MINOR ARTERIAL")) {
      return "minor_arterial";
    }
    return null;
  }

  private static byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
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
    if (cell == null) {
      return null;
    }
    if (cell.getCellType() == CellType.STRING) {
      String s = cell.getStringCellValue();
      return s == null || s.trim().isEmpty() ? null : s;
    }
    if (cell.getCellType() == CellType.NUMERIC) {
      return String.valueOf(cell.getNumericCellValue());
    }
    return null;
  }

  private static void putDouble(ObjectNode row, String col, Cell cell) {
    if (cell == null) {
      row.putNull(col);
      return;
    }
    try {
      if (cell.getCellType() == CellType.NUMERIC) {
        row.put(col, cell.getNumericCellValue());
        return;
      }
      if (cell.getCellType() == CellType.STRING) {
        String s = cell.getStringCellValue().replace(",", "").trim();
        if (s.isEmpty()) {
          row.putNull(col);
          return;
        }
        row.put(col, Double.parseDouble(s));
        return;
      }
    } catch (NumberFormatException e) {
      // fall through to null
    }
    row.putNull(col);
  }

  private static Map<String, String> buildStateFips() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("ALABAMA", "01");
    m.put("ALASKA", "02");
    m.put("ARIZONA", "04");
    m.put("ARKANSAS", "05");
    m.put("CALIFORNIA", "06");
    m.put("COLORADO", "08");
    m.put("CONNECTICUT", "09");
    m.put("DELAWARE", "10");
    m.put("DISTRICT OF COLUMBIA", "11");
    m.put("DIST. OF COL.", "11");
    m.put("FLORIDA", "12");
    m.put("GEORGIA", "13");
    m.put("HAWAII", "15");
    m.put("IDAHO", "16");
    m.put("ILLINOIS", "17");
    m.put("INDIANA", "18");
    m.put("IOWA", "19");
    m.put("KANSAS", "20");
    m.put("KENTUCKY", "21");
    m.put("LOUISIANA", "22");
    m.put("MAINE", "23");
    m.put("MARYLAND", "24");
    m.put("MASSACHUSETTS", "25");
    m.put("MICHIGAN", "26");
    m.put("MINNESOTA", "27");
    m.put("MISSISSIPPI", "28");
    m.put("MISSOURI", "29");
    m.put("MONTANA", "30");
    m.put("NEBRASKA", "31");
    m.put("NEVADA", "32");
    m.put("NEW HAMPSHIRE", "33");
    m.put("NEW JERSEY", "34");
    m.put("NEW MEXICO", "35");
    m.put("NEW YORK", "36");
    m.put("NORTH CAROLINA", "37");
    m.put("NORTH DAKOTA", "38");
    m.put("OHIO", "39");
    m.put("OKLAHOMA", "40");
    m.put("OREGON", "41");
    m.put("PENNSYLVANIA", "42");
    m.put("RHODE ISLAND", "44");
    m.put("SOUTH CAROLINA", "45");
    m.put("SOUTH DAKOTA", "46");
    m.put("TENNESSEE", "47");
    m.put("TEXAS", "48");
    m.put("UTAH", "49");
    m.put("VERMONT", "50");
    m.put("VIRGINIA", "51");
    m.put("WASHINGTON", "53");
    m.put("WEST VIRGINIA", "54");
    m.put("WISCONSIN", "55");
    m.put("WYOMING", "56");
    m.put("PUERTO RICO", "72");
    return Collections.unmodifiableMap(m);
  }

  private static final class Block {
    final int startCol;
    final String roadClass;

    Block(int startCol, String roadClass) {
      this.startCol = startCol;
      this.roadClass = roadClass;
    }
  }
}
