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
package org.apache.calcite.adapter.govdata.census;

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
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base for national time-series tables built from the Census Bureau's CPS ASEC historical
 * workbooks (poverty Table 2, income Table H-5).
 *
 * <p>The configured source URL is the Census page that links the workbook; this class reads the
 * link from the page and downloads the workbook itself, because the response body is
 * delivered as text. The "All Races" block of the first sheet is parsed: a row whose first cell
 * is a year, optionally followed by footnote markers such as {@code 2024 (2)}. A year the source
 * lists twice (a change in processing or income questions) yields two rows: the first listing
 * (Census's current basis) is {@code primary}, the second {@code alternate}.
 */
public abstract class CpsHistoricalXlsxTransformer implements ResponseTransformer {

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(CpsHistoricalXlsxTransformer.class);
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Pattern YEAR_CELL = Pattern.compile("^(\\d{4})(\\s*\\(\\d+\\))*$");

  /** Regex over the page HTML whose group 1 is the workbook URL (absolute or protocol-relative). */
  protected abstract Pattern workbookLink();

  /** Parses the workbook into a JSON array of rows. */
  protected abstract ArrayNode parseWorkbook(XSSFWorkbook workbook);

  @Override public String transform(String response, RequestContext context) {
    String pageUrl = context.getUrl();
    try {
      Matcher m = workbookLink().matcher(response);
      if (!m.find()) {
        throw new IOException("no workbook link matching " + workbookLink() + " on " + pageUrl);
      }
      String url = m.group(1);
      if (url.startsWith("//")) {
        url = "https:" + url;
      }
      LOGGER.info("CPS ASEC historical workbook: {}", url);
      XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(download(url)));
      try {
        ArrayNode rows = parseWorkbook(workbook);
        if (rows.size() == 0) {
          throw new IllegalStateException("no year rows parsed from " + url);
        }
        return rows.toString();
      } finally {
        workbook.close();
      }
    } catch (IOException e) {
      throw new RuntimeException("CPS ASEC historical: failed to read workbook via " + pageUrl, e);
    }
  }

  /**
   * Walks the rows of the "All Races" block of the first sheet, handing each year row to
   * {@code visitor} with its year and vintage ("primary" or "alternate").
   */
  protected static ArrayNode allRacesRows(Sheet sheet, RowVisitor visitor) {
    ArrayNode out = MAPPER.createArrayNode();
    Set<Integer> seen = new HashSet<>();
    boolean inBlock = false;
    for (int r = 0; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      String a = row == null ? null : text(row.getCell(0));
      if (a == null) {
        continue;
      }
      if (!inBlock) {
        inBlock = a.startsWith("All Races");
        continue;
      }
      Matcher m = YEAR_CELL.matcher(a);
      if (!m.matches()) {
        if (out.size() > 0 && !a.startsWith("Year") && !a.startsWith("Race")) {
          break;
        }
        continue;
      }
      int year = Integer.parseInt(m.group(1));
      ObjectNode node = MAPPER.createObjectNode();
      node.put("year", year);
      node.put("vintage", seen.add(year) ? "primary" : "alternate");
      visitor.visit(row, node);
      out.add(node);
    }
    return out;
  }

  /** Fills one year row. */
  protected interface RowVisitor {
    void visit(Row row, ObjectNode node);
  }

  private static byte[] download(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    InputStream in = conn.getInputStream();
    try {
      byte[] buf = new byte[65536];
      int len;
      while ((len = in.read(buf)) > 0) {
        out.write(buf, 0, len);
      }
    } finally {
      in.close();
    }
    return out.toByteArray();
  }

  protected static String text(Cell cell) {
    if (cell == null) {
      return null;
    }
    String s;
    if (cell.getCellType() == CellType.STRING) {
      s = cell.getStringCellValue().trim();
    } else if (cell.getCellType() == CellType.NUMERIC) {
      s = String.valueOf((long) cell.getNumericCellValue());
    } else {
      return null;
    }
    return s.isEmpty() ? null : s;
  }

  /**
   * Writes a numeric cell. The source prints "N" (not available) in place of a number for cells
   * it never collected; that is its own statement that no value exists, so it becomes SQL NULL.
   * Any other non-numeric content is a layout change and fails the run.
   */
  protected static void put(ObjectNode node, String field, Cell cell, int decimals) {
    if (cell == null || cell.getCellType() == CellType.BLANK) {
      node.putNull(field);
    } else if (cell.getCellType() == CellType.NUMERIC) {
      double v = cell.getNumericCellValue();
      if (decimals > 0) {
        double scale = Math.pow(10, decimals);
        v = Math.round(v * scale) / scale;
        node.put(field, v);
      } else {
        node.put(field, Math.round(v));
      }
    } else if (cell.getCellType() == CellType.STRING && "N".equals(cell.getStringCellValue().trim())) {
      node.putNull(field);
    } else {
      throw new IllegalStateException("unexpected cell " + cell.getAddress() + " for " + field);
    }
  }
}
