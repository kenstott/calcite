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

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.binary.XSSFBSharedStringsTable;
import org.apache.poi.xssf.binary.XSSFBSheetHandler;
import org.apache.poi.xssf.binary.XSSFBStylesTable;
import org.apache.poi.xssf.eventusermodel.XSSFBReader;
import org.apache.poi.xssf.usermodel.XSSFComment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Shared HUD Point-in-Time (PIT) homeless-count workbook parsing for
 * {@link HudPitCoCCountsProvider} and {@link HudPitStateCountsProvider}. HUD publishes both the
 * CoC-grain and state-grain PIT counts as a single {@code .xlsb} workbook with one sheet per data
 * year (2007-2024+) plus several non-year metadata/template sheets that this skips by matching
 * the sheet name against a 4-digit year.
 *
 * <p>Each year's sheet is wide (HUD adds new subpopulation/demographic breakout columns most
 * years: 25 columns in 2007 growing to 1,300+ by 2024, always a superset of the prior year's
 * columns) rather than long, so this melts every non-identifier column into one output row per
 * (year, entity, metric) with a null/blank cell skipped rather than materialized — the column set
 * a subclass must not melt (CoC/state identifying fields) is supplied via {@link #idColumnNames()}
 * and matched by header text, not column position, because the identifier columns' own position
 * shifts across years (e.g. 'CoC Category' does not exist before 2024, so 'Count Types' shifts
 * from index 2 to index 3). Both files also carry a national 'Total' row (state file: State column
 * literally 'Total'; CoC file: CoC Number blank, CoC Name 'Total') and trailing footnote text rows
 * after the last real data row — the footnote rows have text only in one identifier column and no
 * metric values, so the null-cell skip already excludes them without any special-case detection.
 *
 * <p>HUD's WAF answers a non-browser User-Agent with an HTTP 202 challenge and no file (same
 * gate as {@link HudPictureCountyDataProvider}); a full browser UA bypasses it.
 */
abstract class AbstractHudPitCountsDataProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHudPitCountsDataProvider.class);

  private static final Pattern YEAR_SHEET = Pattern.compile("(19|20)\\d{2}");
  private static final String REFERER =
      "https://www.huduser.gov/portal/datasets/ahar/2024-ahar-part-1-pit-estimates-of-homelessness-in-the-us.html";
  private static final String USER_AGENT =
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          + "(KHTML, like Gecko) Chrome/128.0 Safari/537.36";
  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 180_000;
  private static final int MAX_RETRIES = 3;

  /** Direct download URL for this table's workbook. */
  protected abstract String workbookUrl();

  /** Header text of columns that identify the row (CoC/state) rather than a metric value. */
  protected abstract Set<String> idColumnNames();

  /** Builds one output row for a single (year, entity, metric) melted cell. */
  protected abstract Map<String, Object> buildRow(int year, Map<String, String> idValues,
      String metricName, double count);

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String url = workbookUrl();
    LOGGER.info("{}: fetching {}", getClass().getSimpleName(), url);
    byte[] body = download(url);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    try (OPCPackage pkg = OPCPackage.open(new ByteArrayInputStream(body))) {
      XSSFBReader reader = new XSSFBReader(pkg);
      XSSFBStylesTable styles = reader.getXSSFBStylesTable();
      XSSFBSharedStringsTable sst = new XSSFBSharedStringsTable(pkg);
      XSSFBReader.SheetIterator sheetIterator =
          (XSSFBReader.SheetIterator) reader.getSheetsData();
      while (sheetIterator.hasNext()) {
        try (InputStream sheetStream = sheetIterator.next()) {
          String sheetName = sheetIterator.getSheetName();
          if (sheetName == null || !YEAR_SHEET.matcher(sheetName.trim()).matches()) {
            continue;
          }
          int year = Integer.parseInt(sheetName.trim());
          parseSheet(sheetStream, styles, sst, year, rows);
        }
      }
    } catch (OpenXML4JException | SAXException e) {
      throw new IOException("Failed to parse HUD PIT counts workbook from " + url, e);
    }
    LOGGER.info("{}: {} long-format rows from {}", getClass().getSimpleName(), rows.size(), url);
    return rows.iterator();
  }

  private void parseSheet(InputStream sheetStream, XSSFBStylesTable styles,
      XSSFBSharedStringsTable sst, int year, List<Map<String, Object>> out) throws IOException {
    final Set<String> idColumns = idColumnNames();
    final List<String> headers = new ArrayList<String>();
    final List<String> currentRow = new ArrayList<String>();
    XSSFBSheetHandler.SheetContentsHandler handler = new XSSFBSheetHandler.SheetContentsHandler() {
      @Override public void startRow(int rowNum) {
        currentRow.clear();
      }

      @Override public void endRow(int rowNum) {
        if (rowNum == 0) {
          headers.clear();
          headers.addAll(currentRow);
          return;
        }
        if (headers.isEmpty()) {
          return;
        }
        Map<String, String> idValues = new LinkedHashMap<String, String>();
        for (int i = 0; i < headers.size(); i++) {
          String h = headers.get(i);
          if (h != null && idColumns.contains(h)) {
            idValues.put(h, i < currentRow.size() ? currentRow.get(i) : null);
          }
        }
        for (int i = 0; i < headers.size(); i++) {
          String h = headers.get(i);
          if (h == null || idColumns.contains(h)) {
            continue;
          }
          String raw = i < currentRow.size() ? currentRow.get(i) : null;
          Double value = parseCount(raw);
          if (value == null) {
            continue;
          }
          out.add(buildRow(year, idValues, h, value.doubleValue()));
        }
      }

      @Override public void cell(String cellReference, String formattedValue,
          XSSFComment comment) {
        int col = new CellReference(cellReference).getCol();
        while (currentRow.size() <= col) {
          currentRow.add(null);
        }
        currentRow.set(col, formattedValue);
      }

      @Override public void hyperlinkCell(String cellReference, String relId, String location,
          String toolTip, XSSFComment comment) {
        // Not used - PIT count cells carry no hyperlinks.
      }
    };
    new XSSFBSheetHandler(sheetStream, styles, null, sst, handler, new DataFormatter(), false)
        .parse();
  }

  private static Double parseCount(String raw) {
    if (raw == null) {
      return null;
    }
    String s = raw.trim().replace(",", "");
    if (s.isEmpty()) {
      return null;
    }
    try {
      return Double.valueOf(s);
    } catch (NumberFormatException e) {
      LOGGER.warn("Unparseable HUD PIT count value '{}', treating as null", raw);
      return null;
    }
  }

  /** Downloads bytes with a browser UA + Referer (HUD's WAF rejects a bot-like client). */
  private static byte[] download(String url) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", USER_AGENT);
      conn.setRequestProperty("Referer", REFERER);
      conn.setRequestProperty("Accept", "*/*");
      try {
        int status = conn.getResponseCode();
        if (status == HttpURLConnection.HTTP_OK) {
          try (InputStream in = conn.getInputStream()) {
            return readAll(in);
          }
        }
        if (status != 429 && status != HttpURLConnection.HTTP_ACCEPTED && status < 500) {
          throw new IOException("HTTP " + status + " from " + url);
        }
        last = new IOException("HTTP " + status + " from " + url);
      } finally {
        conn.disconnect();
      }
      sleepBackoff(attempt);
    }
    throw last != null ? last : new IOException("GET failed: " + url);
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream(1 << 20);
    byte[] buf = new byte[8192];
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }

  private static void sleepBackoff(int attempt) throws IOException {
    try {
      Thread.sleep(1000L * attempt);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted during HUD retry backoff", e);
    }
  }
}
