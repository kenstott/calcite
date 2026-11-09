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

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.XMLHelper;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * ERCOT wholesale electricity prices — daily statistics per settlement point (the 8 trading
 * hubs and 8 load zones) from ERCOT's public, keyless MIS archive:
 *
 * <ul>
 *   <li>Day-Ahead Market (report 13060, "Historical DAM Load Zone and Hub Prices"): hourly.
 *   <li>Real-Time Market (report 13061, "Historical RTM Load Zone and Hub Prices"): 15-minute.
 * </ul>
 *
 * <p>Each report publishes one workbook per calendar year (zipped). The {@code type} dimension
 * selects the market ({@code ercot_dam} / {@code ercot_rtm}); {@code effective_year} the year.
 * The workbook is streamed (the RTM sheet is ~500k rows) and reduced to one row per delivery
 * date, price series and settlement point.
 *
 * <p>The RTM report carries two load-zone price series under the same point name: type
 * {@code LZ} (load-weighted) and {@code LZEW} (energy-weighted). The LZEW rows are emitted as
 * price_series {@code RTM_LZEW} so the (date, series, point) key stays unique.
 */
public class ErcotWholesalePricesTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ErcotWholesalePricesTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String DOC_LIST_URL =
      "https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=";
  private static final String DOC_DOWNLOAD_URL =
      "https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId=";

  private static final DateTimeFormatter ERCOT_DATE =
      DateTimeFormatter.ofPattern("MM/dd/yyyy", Locale.ROOT);

  private static final DateTimeFormatter ERCOT_DATE_TEXT =
      DateTimeFormatter.ofPattern("MMM d, yyyy", Locale.ENGLISH);

  private static final String TYPE_DAM = "ercot_dam";
  private static final String TYPE_RTM = "ercot_rtm";

  @Override
  public String transform(String response, RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    Objects.requireNonNull(dims, "dimension values missing — contract violation");
    String type = Objects.requireNonNull(dims.get("type"),
        "type dimension missing — contract violation");
    int year = Integer.parseInt(Objects.requireNonNull(dims.get("effective_year"),
        "effective_year dimension missing — contract violation"));

    boolean dam;
    String reportTypeId;
    String friendlyPrefix;
    if (TYPE_DAM.equals(type)) {
      dam = true;
      reportTypeId = "13060";
      friendlyPrefix = "DAMLZHBSPP_";
    } else if (TYPE_RTM.equals(type)) {
      dam = false;
      reportTypeId = "13061";
      friendlyPrefix = "RTMLZHBSPP_";
    } else {
      throw new IllegalArgumentException("ERCOT wholesale prices: unknown type '" + type + "'");
    }

    try {
      String docId = findDocId(reportTypeId, friendlyPrefix + year);
      if (docId == null) {
        // ERCOT publishes the current year's workbook only once it has a first delivery day
        // and a year that is not yet published has nothing to load — not an error.
        LOGGER.warn("ERCOT {}: no archive named {}{} yet; treating as no data", type,
            friendlyPrefix, year);
        return "[]";
      }
      // justification: local temp artifact; the POI SAX reader needs a seekable File
      Path tmpDir = Files.createTempDirectory("ercot-prices-");
      Path xlsx = tmpDir.resolve("prices.xlsx");
      try {
        downloadWorkbook(DOC_DOWNLOAD_URL + docId, xlsx);
        return aggregate(xlsx.toFile(), dam, year).toString();
      } finally {
        Files.deleteIfExists(xlsx);
        Files.deleteIfExists(tmpDir);
      }
    } catch (IOException e) {
      throw new RuntimeException("ERCOT wholesale prices (" + type + " " + year + "): "
          + e.getMessage(), e);
    }
  }

  private String findDocId(String reportTypeId, String friendlyName) throws IOException {
    JsonNode root;
    HttpURLConnection conn = open(DOC_LIST_URL + reportTypeId);
    try (InputStream is = conn.getInputStream()) {
      root = MAPPER.readTree(is);
    }
    JsonNode docs = root.path("ListDocsByRptTypeRes").path("DocumentList");
    if (!docs.isArray()) {
      throw new IOException("ERCOT document list for report " + reportTypeId
          + " has no DocumentList array");
    }
    for (JsonNode d : docs) {
      JsonNode doc = d.path("Document");
      if (friendlyName.equals(doc.path("FriendlyName").asText())) {
        return doc.path("DocID").asText();
      }
    }
    return null;
  }

  /** Downloads the zip and extracts its single .xlsx entry to {@code target}. */
  private void downloadWorkbook(String url, Path target) throws IOException {
    HttpURLConnection conn = open(url);
    try (ZipInputStream zis = new ZipInputStream(conn.getInputStream())) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        if (entry.getName().toLowerCase(Locale.ROOT).endsWith(".xlsx")) {
          // justification: local temp artifact (see transform)
          Files.copy(zis, target, StandardCopyOption.REPLACE_EXISTING);
          return;
        }
      }
    }
    throw new IOException("no .xlsx entry in archive from " + url);
  }

  private HttpURLConnection open(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    return conn;
  }

  // ---------------------------------------------------------------------------------------
  // Aggregation
  // ---------------------------------------------------------------------------------------

  /** Running price statistics for one (date, series, point). */
  private static final class Stats {
    double sum;
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    int count;

    void add(double price) {
      sum += price;
      min = Math.min(min, price);
      max = Math.max(max, price);
      count++;
    }
  }

  static ArrayNode aggregate(File xlsx, boolean dam, int year) throws IOException {
    // The sheet XML compresses far below POI's default zip-bomb inflate ratio; the source is
    // a trusted ERCOT publication.
    ZipSecureFile.setMinInflateRatio(0.0);
    // Sorted so output order is deterministic: key = date|series|point
    Map<String, Stats> stats = new TreeMap<String, Stats>();
    try (OPCPackage pkg = OPCPackage.open(xlsx, PackageAccess.READ)) {
      XSSFReader reader = new XSSFReader(pkg);
      ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
      StylesTable styles = reader.getStylesTable();
      XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
      int sheetCount = 0;
      while (sheets.hasNext()) {
        sheetCount++;
        try (InputStream sheet = sheets.next()) {
          XMLReader parser = XMLHelper.newXMLReader();
          parser.setContentHandler(new XSSFSheetXMLHandler(styles, null, strings,
              new RowHandler(dam, stats), new DataFormatter(), false));
          parser.parse(new InputSource(sheet));
        }
      }
      if (sheetCount == 0) {
        throw new IOException("workbook has no sheets");
      }
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("failed to read workbook: " + e.getMessage(), e);
    }

    ArrayNode out = MAPPER.createArrayNode();
    for (Map.Entry<String, Stats> e : stats.entrySet()) {
      String[] key = e.getKey().split("\\|", -1);
      Stats s = e.getValue();
      ObjectNode row = MAPPER.createObjectNode();
      row.put("delivery_date", key[0]);
      row.put("price_series", key[1]);
      row.put("settlement_point", key[2]);
      row.put("avg_price_usd_mwh", s.sum / s.count);
      row.put("min_price_usd_mwh", s.min);
      row.put("max_price_usd_mwh", s.max);
      row.put("interval_count", s.count);
      out.add(row);
    }
    if (out.size() == 0) {
      throw new IOException("workbook parsed to zero price rows for year " + year);
    }
    return out;
  }

  /** Collects rows, locating columns by header text so the sheet layout is verified. */
  private static final class RowHandler implements XSSFSheetXMLHandler.SheetContentsHandler {
    private final boolean dam;
    private final Map<String, Stats> stats;
    private List<String> current;
    private int dateCol = -1;
    private int pointCol = -1;
    private int typeCol = -1;
    private int priceCol = -1;

    RowHandler(boolean dam, Map<String, Stats> stats) {
      this.dam = dam;
      this.stats = stats;
    }

    @Override public void startRow(int rowNum) {
      current = new ArrayList<String>();
    }

    @Override public void cell(String cellReference, String formattedValue,
        XSSFComment comment) {
      int col = new CellReference(cellReference).getCol();
      while (current.size() <= col) {
        current.add(null);
      }
      current.set(col, formattedValue);
    }

    @Override public void endRow(int rowNum) {
      if (current.isEmpty()) {
        return;
      }
      if ("Delivery Date".equals(current.get(0))) {
        bindHeader(current);
        return;
      }
      if (dateCol < 0) {
        throw new IllegalStateException("ERCOT workbook row " + rowNum
            + " precedes the 'Delivery Date' header row");
      }
      if (isBlank(current, pointCol) && isBlank(current, priceCol)) {
        // 2011-2015 sheets end with a publication-stamp row (date, row count, time of day)
        // carrying no settlement point or price; it is not a price observation.
        return;
      }
      String rawDate = trimmed(current, dateCol);
      // Two layouts occur in the archive: "01/01/2024" and (RTM 2011-2015) "Jan 1, 2014".
      String date = LocalDate.parse(rawDate,
          rawDate.indexOf('/') >= 0 ? ERCOT_DATE : ERCOT_DATE_TEXT).toString();
      String point = trimmed(current, pointCol);
      // The SAX handler yields display-formatted text; large prices carry a thousands separator
      // ("1,175.47"). ERCOT publishes prices to 2 decimals, which the display format keeps.
      double price = Double.parseDouble(trimmed(current, priceCol).replace(",", ""));
      String series;
      if (dam) {
        series = "DAM";
      } else {
        series = "LZEW".equals(trimmed(current, typeCol)) ? "RTM_LZEW" : "RTM";
      }
      stats.computeIfAbsent(date + "|" + series + "|" + point, k -> new Stats()).add(price);
    }

    private void bindHeader(List<String> header) {
      dateCol = indexOf(header, "Delivery Date");
      pointCol = indexOf(header, dam ? "Settlement Point" : "Settlement Point Name");
      priceCol = indexOf(header, "Settlement Point Price");
      typeCol = dam ? -1 : indexOf(header, "Settlement Point Type");
    }

    private static int indexOf(List<String> header, String name) {
      for (int i = 0; i < header.size(); i++) {
        if (name.equals(header.get(i) == null ? null : header.get(i).trim())) {
          return i;
        }
      }
      throw new IllegalStateException("ERCOT workbook header lacks column '" + name
          + "' (found " + header + ")");
    }

    private static boolean isBlank(List<String> row, int col) {
      return col >= row.size() || row.get(col) == null || row.get(col).trim().isEmpty();
    }

    private static String trimmed(List<String> row, int col) {
      String v = col < row.size() ? row.get(col) : null;
      if (v == null || v.trim().isEmpty()) {
        throw new IllegalStateException("ERCOT workbook row has an empty value in column "
            + col + ": " + row);
      }
      return v.trim();
    }
  }
}
