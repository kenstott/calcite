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
package org.apache.calcite.adapter.govdata.energy;
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local temp files (download staging / xlsx extraction), not object-store URIs.

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.govdata.ZipDownloadUtils;

import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.util.XMLHelper;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.parsers.ParserConfigurationException;

/**
 * DataProvider for LBNL "Queued Up" — the project-level interconnection-queue dataset
 * (one row per generation/storage request seeking transmission interconnection).
 *
 * <p>LBNL's own hosts (emp.lbl.gov, eta.lbl.gov) sit behind a Cloudflare managed challenge and
 * cannot be fetched by a non-browser client. The same workbook is redistributed under CC BY 4.0
 * inside a Zenodo replication deposit, whose files are immutable per record version, so
 * {@code source.url} points at that deposit's zip. The zip carries several editions and unrelated
 * files; this provider opens it with random access, picks the newest
 * {@code lbnl_ix_queue_data_file_thru<YYYY>[_vN].xlsx}, and streams the
 * "03. Complete Queue Data" sheet through POI's SAX event model (never a whole-sheet array).
 *
 * <p>The as-of year is read from the workbook's own "02. Data Sample by Region" title
 * ("as of the end of YYYY"), and the load fails if it cannot be found.
 */
public class LbnlInterconnectionQueueProvider implements CachingDataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(LbnlInterconnectionQueueProvider.class);

  private static final Pattern ENTRY_NAME = Pattern.compile(
      "(?:.*[\\\\/])?lbnl_ix_queue_data_file_thru(\\d{4})(?:_v(\\d+))?\\.xlsx",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern AS_OF = Pattern.compile("as of the end of (\\d{4})",
      Pattern.CASE_INSENSITIVE);

  private static final String SUMMARY_SHEET = "02. Data Sample by Region";
  private static final String QUEUE_SHEET = "03. Complete Queue Data";
  private static final String KEY_COLUMN = "q_id";

  private static final String[] STRING_COLUMNS = {
      "q_id", "entity", "q_status", "ia_phase_raw", "ia_phase_clean", "county", "state",
      "poi_name", "region", "project_name", "utility", "developer", "cluster", "service",
      "project_type", "type_1", "type_2", "type_3", "type_clean"};
  private static final String[] DATE_COLUMNS = {
      "q_date", "prop_date", "on_date", "wd_date", "ia_date"};
  private static final String[] DOUBLE_COLUMNS = {"mw_1", "mw_2", "mw_3"};
  private static final String[] INT_COLUMNS = {"q_year", "prop_year"};

  /** Unchecked wrapper so the SAX handler can surface a fatal parse error. */
  private static final class QueueParseException extends RuntimeException {
    QueueParseException(String message) {
      super(message);
    }
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String url = config.getSource() != null ? config.getSource().getUrl() : null;
    if (url == null || url.isEmpty()) {
      throw new IOException("LBNL Queued Up: source.url (Zenodo deposit zip) is required");
    }
    final String zipUrl = url;
    File xlsx = File.createTempFile("lbnl-ix-", ".xlsx");
    try {
      try (InputStream cached = rawCache.openStream(zipUrl, () -> extractWorkbook(zipUrl));
          FileOutputStream out = new FileOutputStream(xlsx)) {
        byte[] chunk = new byte[1 << 16];
        int n;
        while ((n = cached.read(chunk)) != -1) {
          out.write(chunk, 0, n);
        }
      }
      List<Map<String, Object>> rows = parseWorkbook(xlsx, zipUrl);
      LOGGER.info("LBNL Queued Up: {} queue rows read from {}", rows.size(), zipUrl);
      return rows.iterator();
    } finally {
      if (!xlsx.delete()) {
        xlsx.deleteOnExit();
      }
    }
  }

  /**
   * Downloads the deposit zip, then streams the newest queue workbook out of it into a temp file
   * that is deleted when the returned stream is closed.
   */
  private InputStream extractWorkbook(String zipUrl) throws IOException {
    File tempZip = File.createTempFile("lbnl-ix-zip-", ".zip");
    File tempXlsx = File.createTempFile("lbnl-ix-entry-", ".xlsx");
    try {
      ZipDownloadUtils.downloadToFile(zipUrl, null, tempZip);
      try (ZipFile zip = new ZipFile(tempZip)) {
        ZipEntry best = null;
        long bestRank = -1L;
        Enumeration<? extends ZipEntry> entries = zip.entries();
        while (entries.hasMoreElements()) {
          ZipEntry e = entries.nextElement();
          Matcher m = ENTRY_NAME.matcher(e.getName());
          if (!m.matches()) {
            continue;
          }
          long version = m.group(2) == null ? 0L : Long.parseLong(m.group(2));
          long rank = Long.parseLong(m.group(1)) * 1000L + version;
          if (rank > bestRank) {
            bestRank = rank;
            best = e;
          }
        }
        if (best == null) {
          throw new IOException("LBNL Queued Up: no lbnl_ix_queue_data_file_thru<YYYY>.xlsx in "
              + zipUrl);
        }
        LOGGER.info("LBNL Queued Up: using zip entry {}", best.getName());
        try (InputStream in = zip.getInputStream(best);
            FileOutputStream out = new FileOutputStream(tempXlsx)) {
          byte[] chunk = new byte[1 << 16];
          int n;
          while ((n = in.read(chunk)) != -1) {
            out.write(chunk, 0, n);
          }
        }
      }
      return new DeleteOnCloseFileInputStream(tempXlsx);
    } catch (IOException | RuntimeException e) {
      if (tempXlsx.exists() && !tempXlsx.delete()) {
        LOGGER.debug("LBNL Queued Up: could not delete staging file {}", tempXlsx);
      }
      throw e;
    } finally {
      if (!tempZip.delete()) {
        tempZip.deleteOnExit();
      }
    }
  }

  /** A stream over a staging file that deletes it once the cache has consumed it. */
  private static final class DeleteOnCloseFileInputStream extends FileInputStream {
    private final File backing;

    DeleteOnCloseFileInputStream(File backing) throws IOException {
      super(backing);
      this.backing = backing;
    }

    @Override public void close() throws IOException {
      try {
        super.close();
      } finally {
        if (backing.exists() && !backing.delete()) {
          LOGGER.debug("LBNL Queued Up: could not delete staging file {}", backing);
        }
      }
    }
  }

  List<Map<String, Object>> parseWorkbook(File xlsx, String zipUrl) throws IOException {
    // The queue sheet is a few MB of XML; POI's zip-bomb inflate-ratio guard is meant for
    // untrusted uploads and trips on highly repetitive sheet XML.
    ZipSecureFile.setMinInflateRatio(0.0);
    OPCPackage pkg = null;
    try {
      pkg = OPCPackage.open(xlsx, PackageAccess.READ);
      ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
      XSSFReader reader = new XSSFReader(pkg);
      StylesTable styles = reader.getStylesTable();
      QueueSheetHandler handler = new QueueSheetHandler();
      XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
      boolean sawQueue = false;
      while (sheets.hasNext()) {
        try (InputStream sheet = sheets.next()) {
          String name = sheets.getSheetName();
          boolean summary = SUMMARY_SHEET.equals(name);
          boolean queue = QUEUE_SHEET.equals(name);
          if (!summary && !queue) {
            continue;
          }
          handler.mode = summary ? Mode.SUMMARY : Mode.QUEUE;
          XMLReader parser = XMLHelper.newXMLReader();
          parser.setContentHandler(new XSSFSheetXMLHandler(styles, null, strings, handler,
              new IsoDateFormatter(), false));
          parser.parse(new InputSource(sheet));
          sawQueue |= queue;
        }
      }
      if (!sawQueue) {
        throw new IOException("LBNL Queued Up: sheet '" + QUEUE_SHEET + "' not found in "
            + zipUrl);
      }
      if (handler.asOfYear == null) {
        throw new IOException("LBNL Queued Up: 'as of the end of YYYY' not found on sheet '"
            + SUMMARY_SHEET + "' in " + zipUrl);
      }
      if (!handler.sawHeader) {
        throw new IOException("LBNL Queued Up: header row (" + KEY_COLUMN + ") not found on '"
            + QUEUE_SHEET + "' in " + zipUrl);
      }
      for (Map<String, Object> row : handler.rows) {
        row.put("as_of_year", handler.asOfYear);
      }
      return handler.rows;
    } catch (QueueParseException e) {
      throw new IOException(e.getMessage(), e);
    } catch (OpenXML4JException e) {
      throw new IOException("LBNL Queued Up: xlsx open failed for " + zipUrl, e);
    } catch (SAXException e) {
      throw new IOException("LBNL Queued Up: xlsx SAX parse failed for " + zipUrl, e);
    } catch (ParserConfigurationException e) {
      throw new IOException("LBNL Queued Up: XML parser init failed for " + zipUrl, e);
    } finally {
      if (pkg != null) {
        pkg.revert();   // read-only: release without writing back
      }
    }
  }

  private enum Mode { SUMMARY, QUEUE }

  /** SAX handler: scans the summary sheet for the as-of year, then reads queue rows. */
  private static final class QueueSheetHandler implements SheetContentsHandler {
    private final List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    private final Map<Integer, String> current = new HashMap<Integer, String>();
    private final Map<String, Integer> headerCols = new LinkedHashMap<String, Integer>();
    private Mode mode;
    private boolean sawHeader;
    private Integer asOfYear;

    @Override public void startRow(int rowNum) {
      current.clear();
    }

    @Override public void cell(String cellReference, String formattedValue, XSSFComment comment) {
      if (cellReference == null || formattedValue == null) {
        return;
      }
      int col = colIndex(cellReference);
      if (col >= 0) {
        current.put(Integer.valueOf(col), formattedValue);
      }
    }

    @Override public void endRow(int rowNum) {
      if (mode == Mode.SUMMARY) {
        if (asOfYear == null) {
          for (String text : current.values()) {
            Matcher m = AS_OF.matcher(text);
            if (m.find()) {
              asOfYear = Integer.valueOf(m.group(1));
              return;
            }
          }
        }
        return;
      }
      if (!sawHeader) {
        for (Map.Entry<Integer, String> e : current.entrySet()) {
          if (KEY_COLUMN.equals(trimToNull(e.getValue()))) {
            for (Map.Entry<Integer, String> h : current.entrySet()) {
              String name = trimToNull(h.getValue());
              if (name != null) {
                headerCols.put(name, h.getKey());
              }
            }
            requireColumns();
            sawHeader = true;
            return;
          }
        }
        return;
      }
      if (trimToNull(current.get(headerCols.get(KEY_COLUMN))) == null) {
        return;
      }
      rows.add(buildRow(rowNum + 1));
    }

    private void requireColumns() {
      List<String> needed = new ArrayList<String>();
      for (String c : STRING_COLUMNS) {
        needed.add(sourceName(c));
      }
      for (String c : DATE_COLUMNS) {
        needed.add(c);
      }
      for (String c : DOUBLE_COLUMNS) {
        needed.add(c);
      }
      for (String c : INT_COLUMNS) {
        needed.add(c);
      }
      needed.add("fips_code");
      for (String c : needed) {
        if (!headerCols.containsKey(c)) {
          throw new QueueParseException("LBNL Queued Up: required column '" + c
              + "' missing from '" + QUEUE_SHEET + "' — header=" + headerCols.keySet());
        }
      }
    }

    private Map<String, Object> buildRow(int sourceRow) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("source_row", Integer.valueOf(sourceRow));
      for (String c : STRING_COLUMNS) {
        row.put(c, trimToNull(cellFor(sourceName(c))));
      }
      for (String c : DATE_COLUMNS) {
        String v = trimToNull(cellFor(c));
        try {
          row.put(c, v == null ? null : LocalDate.parse(v).toString());
        } catch (DateTimeParseException e) {
          throw new QueueParseException("LBNL Queued Up: row " + sourceRow + " column " + c
              + " is not a date: '" + v + "'");
        }
      }
      for (String c : DOUBLE_COLUMNS) {
        String v = trimToNull(cellFor(c));
        try {
          row.put(c, v == null ? null : Double.valueOf(v));
        } catch (NumberFormatException e) {
          throw new QueueParseException("LBNL Queued Up: row " + sourceRow + " column " + c
              + " is not numeric: '" + v + "'");
        }
      }
      for (String c : INT_COLUMNS) {
        String v = trimToNull(cellFor(c));
        try {
          row.put(c, v == null ? null : Integer.valueOf(v));
        } catch (NumberFormatException e) {
          throw new QueueParseException("LBNL Queued Up: row " + sourceRow + " column " + c
              + " is not an integer: '" + v + "'");
        }
      }
      row.put("county_fips", firstCountyFips(trimToNull(cellFor("fips_code")), sourceRow));
      return row;
    }

    private String cellFor(String header) {
      return current.get(headerCols.get(header));
    }

    @Override public void headerFooter(String text, boolean isHeader, String tagName) {
      // no-op
    }
  }

  /** Workbook header for an output column; only the two IA phase headers differ in case. */
  private static String sourceName(String outputName) {
    if ("ia_phase_raw".equals(outputName)) {
      return "IA_phase_raw";
    }
    if ("ia_phase_clean".equals(outputName)) {
      return "IA_phase_clean";
    }
    return outputName;
  }

  /**
   * The workbook stores FIPS as a number, so a leading zero is lost (4005 for Arizona's 04005),
   * and a request spanning two counties carries both codes concatenated. Returns the first
   * listed 5-digit code, matching the source codebook's "only the first listed county" rule.
   */
  static String firstCountyFips(String raw, int sourceRow) {
    if (raw == null) {
      return null;
    }
    if (!raw.matches("\\d+")) {
      throw new QueueParseException("LBNL Queued Up: row " + sourceRow
          + " fips_code is not numeric: '" + raw + "'");
    }
    int padded = ((raw.length() + 4) / 5) * 5;
    StringBuilder sb = new StringBuilder();
    for (int i = raw.length(); i < padded; i++) {
      sb.append('0');
    }
    sb.append(raw);
    return sb.substring(0, 5);
  }

  /**
   * Renders numeric cells raw (integers unadorned, no scientific notation) and date-formatted
   * cells as ISO yyyy-MM-dd, regardless of the workbook's display formats.
   */
  private static final class IsoDateFormatter extends DataFormatter {
    @Override public String formatRawCellContents(double value, int formatIndex,
        String formatString) {
      return render(value, formatIndex, formatString, false);
    }

    @Override public String formatRawCellContents(double value, int formatIndex,
        String formatString, boolean use1904Windowing) {
      return render(value, formatIndex, formatString, use1904Windowing);
    }

    private static String render(double value, int formatIndex, String formatString,
        boolean use1904) {
      if (DateUtil.isADateFormat(formatIndex, formatString)) {
        return DateUtil.getLocalDateTime(value, use1904).toLocalDate().toString();
      }
      if (value == Math.floor(value) && !Double.isInfinite(value)) {
        return String.valueOf((long) value);
      }
      return String.valueOf(value);
    }
  }

  /** Column letters of an A1 cell reference (e.g. {@code "AB12"}) -> 0-based index; -1 if none. */
  private static int colIndex(String cellReference) {
    int col = 0;
    int i = 0;
    int len = cellReference.length();
    while (i < len) {
      char c = cellReference.charAt(i);
      if (c >= 'A' && c <= 'Z') {
        col = col * 26 + (c - 'A' + 1);
      } else if (c >= 'a' && c <= 'z') {
        col = col * 26 + (c - 'a' + 1);
      } else {
        break;
      }
      i++;
    }
    return i == 0 ? -1 : col - 1;
  }

  private static String trimToNull(String s) {
    if (s == null) {
      return null;
    }
    String t = s.trim();
    return t.isEmpty() ? null : t;
  }
}
