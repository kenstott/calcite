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
package org.apache.calcite.adapter.govdata.crime;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base for tables built from the DHS Office of Homeland Security Statistics (OHSS) Yearbook of
 * Immigration Statistics enforcement workbook.
 *
 * <p>The workbook's URL carries its publication date
 * ({@code /system/files/2026-08/2026_0806_ohss_yearbook_enforcement_fy2023.xlsx}), so it cannot be
 * written into the schema. The configured source URL is the Yearbook index page; this class finds
 * the newest yearbook year whose page links an {@code ..._yearbook_enforcement_fy....xlsx} file
 * (a year's page can exist before its enforcement workbook is published) and downloads it. Each
 * workbook holds the complete history, so the newest one supersedes the older ones.
 */
public abstract class OhssEnforcementXlsxTransformer implements ResponseTransformer {

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(OhssEnforcementXlsxTransformer.class);
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String HOST = "https://ohss.dhs.gov";
  private static final Pattern YEAR_LINK =
      Pattern.compile("href=\"/topics/immigration/yearbook/(\\d{4})\"");
  private static final Pattern ENFORCEMENT_LINK =
      Pattern.compile("href=\"(/[^\"]*yearbook_enforcement_fy\\d{4}[^\"]*\\.xlsx)\"");

  /** Parses the workbook's sheets into rows. */
  protected abstract String parseWorkbook(XSSFWorkbook workbook) throws Exception;

  @Override public String transform(String response, RequestContext context) {
    String indexUrl = context.getUrl();
    try {
      String workbookUrl = findEnforcementWorkbook(response);
      LOGGER.info("OHSS enforcement workbook: {}", workbookUrl);
      XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(download(workbookUrl)));
      try {
        return parseWorkbook(workbook);
      } finally {
        workbook.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("OHSS enforcement: failed to read workbook via " + indexUrl, e);
    }
  }

  private String findEnforcementWorkbook(String indexHtml) throws IOException {
    List<Integer> years = new ArrayList<>();
    Matcher m = YEAR_LINK.matcher(indexHtml);
    while (m.find()) {
      Integer y = Integer.valueOf(m.group(1));
      if (!years.contains(y)) {
        years.add(y);
      }
    }
    if (years.isEmpty()) {
      throw new IOException("no /topics/immigration/yearbook/<year> links on the Yearbook index");
    }
    Collections.sort(years, Collections.<Integer>reverseOrder());
    for (Integer year : years) {
      String page = new String(download(HOST + "/topics/immigration/yearbook/" + year),
          StandardCharsets.UTF_8);
      Matcher link = ENFORCEMENT_LINK.matcher(page);
      if (link.find()) {
        return HOST + link.group(1);
      }
    }
    throw new IOException("no yearbook page in " + years + " links an enforcement workbook");
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

  /** The text of a cell, or null when blank. */
  protected static String text(Cell cell) {
    if (cell == null) {
      return null;
    }
    CellType type = cell.getCellType();
    String s;
    if (type == CellType.STRING) {
      s = cell.getStringCellValue();
    } else if (type == CellType.NUMERIC) {
      s = String.valueOf((long) cell.getNumericCellValue());
    } else {
      return null;
    }
    s = s.trim();
    return s.isEmpty() ? null : s;
  }

  /**
   * A cell's whole-number value. The workbook uses the text markers "NA" (not available) and
   * "X" (not applicable) in place of a number; those are the source's own statement that no
   * value exists, so they become SQL NULL.
   */
  protected static Long number(Cell cell) {
    if (cell == null || cell.getCellType() != CellType.NUMERIC) {
      return null;
    }
    return Long.valueOf((long) cell.getNumericCellValue());
  }

  protected static void put(ObjectNode node, String field, Long value) {
    if (value == null) {
      node.putNull(field);
    } else {
      node.put(field, value.longValue());
    }
  }

  protected static Sheet sheet(XSSFWorkbook workbook, String name) {
    Sheet sheet = workbook.getSheet(name);
    if (sheet == null) {
      throw new IllegalStateException("workbook has no sheet '" + name + "'");
    }
    return sheet;
  }

  protected static Row row(Sheet sheet, int index) {
    return sheet.getRow(index);
  }
}
