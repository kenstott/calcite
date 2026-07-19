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
package org.apache.calcite.adapter.govdata.ag;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.govdata.ZipDownloadUtils;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.ss.usermodel.DataFormatter;
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
import org.xml.sax.XMLReader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataProvider for USDA FSA program/conservation payments (EFOIA "Payment Files
 * Information", Name-and-Address xlsx).
 *
 * <p>Per disbursement-year "vintage" FSA publishes ~6-10 state-range xlsx files
 * grouped under a {@code <h2>}/{@code <h3>} year heading on the payment-files
 * landing page. Discovery keys off that year section (not a filename token): FSA
 * names some vintages without a {@code pmt{yy}} token (e.g. the 2021 files are
 * {@code state_al_id.xlsx}), so a token match silently dropped whole years. Both
 * link forms observed live are resolved: direct {@code ....xlsx} and Drupal node
 * pages {@code /documents/...} that wrap the xlsx.
 *
 * <p>The raw files are per-payee transactional; this aggregates to one row per
 * (program_year, county_fips, program_code): SUM(disbursement) and a distinct
 * payee count (a proxy for farms — FSA files carry no operation/farm id and no
 * commodity dimension). Both the disbursement year (the vintage) and the program
 * year are kept; they differ (a 2023 file holds program years up to 2023).
 *
 * <p>xlsx read uses POI's streaming (SAX) {@link XSSFReader} /
 * {@link XSSFSheetXMLHandler} event model, one file at a time. These per-payee
 * files reach hundreds of MB uncompressed; the usermodel {@code XSSFWorkbook}
 * inflates a sheet into one contiguous array and trips POI's array-length guard
 * (165M-561M byte allocations), so it can never load them. Streaming reads the
 * sheet incrementally into the bounded aggregation map instead. Numeric cells are
 * rendered raw (see {@link RawNumberFormatter}) so values are identical to the
 * former usermodel path regardless of cell number-format.
 */
public class FsaCommodityPaymentsProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(FsaCommodityPaymentsProvider.class);

  private static final String HOST = "https://www.fsa.usda.gov";
  private static final String DEFAULT_UA = "Mozilla/5.0 (compatible; govdata-etl/1.0)";
  private static final Pattern HREF = Pattern.compile("href=\"([^\"]+)\"", Pattern.CASE_INSENSITIVE);
  private static final Pattern XLSX_IN_NODE =
      Pattern.compile("href=\"([^\"]*\\.xlsx)\"", Pattern.CASE_INSENSITIVE);
  private static final Pattern NEXT_YEAR_HEADING =
      Pattern.compile("<h[23][^>]*>", Pattern.CASE_INSENSITIVE);

  // Required xlsx header -> aggregation role.
  private static final String H_STATE = "State FSA Code";
  private static final String H_COUNTY = "County FSA Code";
  private static final String H_PAYEE = "Formatted Payee Name";
  private static final String H_AMOUNT = "Disbursement Amount";
  private static final String H_PROG_CODE = "Accounting Program Code";
  private static final String H_PROG_DESC = "Accounting Program Description";
  private static final String H_PROG_YEAR = "Accounting Program Year";

  private static final class Agg {
    String stateFips;
    String programDescription;
    double payments;
    final Set<String> payees = new HashSet<String>();
  }

  /** Unchecked wrapper so the SAX {@link SheetContentsHandler} can surface a fatal parse error. */
  private static final class FsaParseException extends RuntimeException {
    FsaParseException(String message) {
      super(message);
    }
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String landingUrl = config.getSource() != null ? config.getSource().getUrl() : null;
    if (landingUrl == null || landingUrl.isEmpty()) {
      throw new IOException("FSA: source.url (landing page) is required");
    }
    String userAgent = headerOrDefault(config, "User-Agent", DEFAULT_UA);
    int disbursementYear = resolveYear(variables);

    List<String> xlsxUrls = resolveVintageFiles(landingUrl, userAgent, disbursementYear);
    if (xlsxUrls.isEmpty()) {
      throw new IOException("FSA: no payment files found for disbursement year " + disbursementYear
          + " on " + landingUrl);
    }
    LOGGER.info("FSA: disbursement year {} -> {} file(s)", disbursementYear, xlsxUrls.size());

    Map<String, Agg> agg = new HashMap<String, Agg>();
    for (String xlsxUrl : xlsxUrls) {
      aggregateFile(xlsxUrl, userAgent, agg);
    }

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>(agg.size());
    for (Map.Entry<String, Agg> e : agg.entrySet()) {
      String[] key = e.getKey().split("\\|", -1);  // programYear | countyFips | programCode
      Agg a = e.getValue();
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("year", Integer.valueOf(disbursementYear));   // partition = disbursement (vintage) year
      row.put("program_year", key[0].isEmpty() ? null : key[0]);
      row.put("state_fips", a.stateFips);
      row.put("county_fips", key[1].isEmpty() ? null : key[1]);
      row.put("program_code", key[2].isEmpty() ? null : key[2]);
      row.put("program_description", a.programDescription);
      row.put("payments", Double.valueOf(a.payments));
      row.put("farms", Integer.valueOf(a.payees.size()));
      rows.add(row);
    }
    LOGGER.info("FSA: disbursement year {} aggregated to {} county/program/year rows",
        disbursementYear, rows.size());
    return rows.iterator();
  }

  private int resolveYear(Map<String, String> variables) throws IOException {
    String y = variables.get("effective_year");
    if (y == null || y.isEmpty()) {
      y = variables.get("year");
    }
    if (y == null || y.isEmpty()) {
      throw new IOException("FSA: no effective_year/year dimension value supplied");
    }
    return Integer.parseInt(y.trim());
  }

  /**
   * Scrapes the landing page and resolves every xlsx under the disbursement-year heading. FSA
   * groups each vintage's files under an {@code <h2>}/{@code <h3>} whose text is the year; the
   * section runs to the next such heading. Files inside are matched regardless of filename token.
   */
  private List<String> resolveVintageFiles(String landingUrl, String userAgent, int disbursementYear)
      throws IOException {
    String html = getText(landingUrl, userAgent);
    String section = sectionForYear(html, disbursementYear);
    if (section == null) {
      throw new IOException("FSA: no <h2>/<h3> section for disbursement year " + disbursementYear
          + " on " + landingUrl);
    }
    Set<String> resolved = new LinkedHashSet<String>();
    Matcher m = HREF.matcher(section);
    while (m.find()) {
      String href = m.group(1);
      String lower = href.toLowerCase();
      if (lower.endsWith(".xlsx")) {
        resolved.add(abs(href));
      } else if (lower.contains("/documents/")) {
        String inner = resolveNodeToXlsx(abs(href), userAgent);
        if (inner != null) {
          resolved.add(inner);
        }
      }
    }
    return new ArrayList<String>(resolved);
  }

  /** Substring from the year's {@code <h2>/<h3>} heading to the next year heading (or end). */
  private static String sectionForYear(String html, int year) {
    Matcher hdr = Pattern.compile("<h[23][^>]*>\\s*" + year + "\\s*</h[23]>",
        Pattern.CASE_INSENSITIVE).matcher(html);
    if (!hdr.find()) {
      return null;
    }
    int start = hdr.end();
    int end = html.length();
    Matcher next = NEXT_YEAR_HEADING.matcher(html);
    if (next.find(start)) {
      end = next.start();
    }
    return html.substring(start, end);
  }

  /** Fetches a Drupal node page and returns the first .xlsx link inside it. */
  private String resolveNodeToXlsx(String nodeUrl, String userAgent) throws IOException {
    String html = getText(nodeUrl, userAgent);
    Matcher m = XLSX_IN_NODE.matcher(html);
    if (m.find()) {
      return abs(m.group(1));
    }
    LOGGER.warn("FSA: node page had no .xlsx link: {}", nodeUrl);
    return null;
  }

  /**
   * Streams the first sheet of an xlsx via POI's SAX event model, folding each row into {@code agg}.
   * Downloads to a temp file first so the OPC package reads zip entries lazily (never the whole
   * sheet into one array).
   */
  private void aggregateFile(String xlsxUrl, String userAgent, Map<String, Agg> agg)
      throws IOException {
    // FSA sheet XML compresses very highly; disable POI's zip-bomb inflate-ratio guard (precedent:
    // NsfFederalRdTransformer, NsfNationalRdTransformer). Streaming avoids the byte[] size cap.
    ZipSecureFile.setMinInflateRatio(0.0);
    File tmp = File.createTempFile("fsa-", ".xlsx");
    OPCPackage pkg = null;
    try {
      Map<String, String> dlHeaders = new HashMap<String, String>();
      dlHeaders.put("User-Agent", userAgent);
      // Robust download: retry/backoff, HTML+404 detection, and a completeness check. A truncated
      // file makes POI fall back from random-access ZipFile to stream mode, which materializes the
      // whole sheet entry into one array and trips POI's array-length cap — the exact failure this
      // avoids (a partial download otherwise reaches OPCPackage.open and blows up there).
      ZipDownloadUtils.downloadToFile(xlsxUrl, dlHeaders, tmp);
      pkg = OPCPackage.open(tmp, PackageAccess.READ);
      ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
      XSSFReader reader = new XSSFReader(pkg);
      StylesTable styles = reader.getStylesTable();
      XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
      if (!sheets.hasNext()) {
        throw new IOException("FSA: no sheet in " + xlsxUrl);
      }
      FsaSheetHandler sheetHandler = new FsaSheetHandler(agg, xlsxUrl);
      XMLReader parser = XMLHelper.newXMLReader();
      parser.setContentHandler(
          new XSSFSheetXMLHandler(styles, null, strings, sheetHandler, new RawNumberFormatter(), false));
      try (InputStream sheet = sheets.next()) {   // first sheet only (matches former getSheetAt(0))
        parser.parse(new InputSource(sheet));
      }
      if (!sheetHandler.sawHeader()) {
        throw new IOException("FSA: empty sheet in " + xlsxUrl);
      }
    } catch (FsaParseException e) {
      throw new IOException(e.getMessage(), e);
    } catch (org.apache.poi.openxml4j.exceptions.OpenXML4JException e) {
      throw new IOException("FSA: xlsx open failed for " + xlsxUrl, e);
    } catch (org.xml.sax.SAXException e) {
      throw new IOException("FSA: xlsx SAX parse failed for " + xlsxUrl, e);
    } catch (javax.xml.parsers.ParserConfigurationException e) {
      throw new IOException("FSA: XML parser init failed for " + xlsxUrl, e);
    } finally {
      if (pkg != null) {
        pkg.revert();   // read-only: release without writing back
      }
      if (!tmp.delete()) {
        tmp.deleteOnExit();
      }
    }
  }

  /** SAX handler: builds the header column map from row 0, then aggregates each data row. */
  private static final class FsaSheetHandler implements SheetContentsHandler {
    private final Map<String, Agg> agg;
    private final String xlsxUrl;
    private final Map<Integer, String> current = new HashMap<Integer, String>();
    private boolean headerResolved;
    private int cState;
    private int cCounty;
    private int cPayee;
    private int cAmount;
    private int cCode;
    private int cDesc;
    private int cYear;

    FsaSheetHandler(Map<String, Agg> agg, String xlsxUrl) {
      this.agg = agg;
      this.xlsxUrl = xlsxUrl;
    }

    boolean sawHeader() {
      return headerResolved;
    }

    @Override public void startRow(int rowNum) {
      current.clear();
    }

    @Override public void cell(String cellReference, String formattedValue, XSSFComment comment) {
      if (cellReference == null) {
        return;
      }
      int col = colIndex(cellReference);
      if (col >= 0) {
        current.put(Integer.valueOf(col), formattedValue);
      }
    }

    @Override public void endRow(int rowNum) {
      if (!headerResolved) {
        resolveHeader();
        headerResolved = true;
        return;
      }
      String stateRaw = current.get(Integer.valueOf(cState));
      String countyRaw = current.get(Integer.valueOf(cCounty));
      String countyFips = countyFips(stateRaw, countyRaw);
      String programCode = trimToNull(current.get(Integer.valueOf(cCode)));
      String programYear = trimToNull(current.get(Integer.valueOf(cYear)));
      String key = nz(programYear) + "|" + nz(countyFips) + "|" + nz(programCode);
      Agg a = agg.get(key);
      if (a == null) {
        a = new Agg();
        a.stateFips = pad(stateRaw, 2);
        a.programDescription = trimToNull(current.get(Integer.valueOf(cDesc)));
        agg.put(key, a);
      }
      Double amount = parseAmount(current.get(Integer.valueOf(cAmount)));
      if (amount != null) {
        a.payments += amount.doubleValue();
      }
      String payee = trimToNull(current.get(Integer.valueOf(cPayee)));
      if (payee != null) {
        a.payees.add(payee);
      }
    }

    private void resolveHeader() {
      Map<String, Integer> col = new HashMap<String, Integer>();
      for (Map.Entry<Integer, String> e : current.entrySet()) {
        String name = trimToNull(e.getValue());
        if (name != null) {
          col.put(name, e.getKey());
        }
      }
      cState = require(col, H_STATE);
      cCounty = require(col, H_COUNTY);
      cPayee = require(col, H_PAYEE);
      cAmount = require(col, H_AMOUNT);
      cCode = require(col, H_PROG_CODE);
      cDesc = require(col, H_PROG_DESC);
      cYear = require(col, H_PROG_YEAR);
    }

    private int require(Map<String, Integer> col, String name) {
      Integer idx = col.get(name);
      if (idx == null) {
        throw new FsaParseException("FSA: required column '" + name + "' missing in " + xlsxUrl
            + " — header=" + col.keySet());
      }
      return idx.intValue();
    }

    @Override public void headerFooter(String text, boolean isHeader, String tagName) {
      // no-op
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
    return col - 1;
  }

  /**
   * Renders numeric cells raw (integers unadorned, no scientific notation) regardless of the cell's
   * number-format, so streamed values match the former usermodel path exactly. The FSA columns read
   * here carry no dates, so date-serial rendering is irrelevant.
   */
  private static final class RawNumberFormatter extends DataFormatter {
    @Override public String formatRawCellContents(double value, int formatIndex, String formatString) {
      return raw(value);
    }

    @Override public String formatRawCellContents(double value, int formatIndex, String formatString,
        boolean use1904Windowing) {
      return raw(value);
    }

    private static String raw(double value) {
      if (value == Math.floor(value) && !Double.isInfinite(value)) {
        return String.valueOf((long) value);
      }
      return String.valueOf(value);
    }
  }

  private static String countyFips(String stateFsa, String countyFsa) {
    String st = pad(stateFsa, 2);
    String co = pad(countyFsa, 3);
    if (st == null || co == null) {
      return null;
    }
    return st + co;
  }

  private static String pad(String v, int width) {
    if (v == null) {
      return null;
    }
    String t = v.trim();
    if (t.isEmpty()) {
      return null;
    }
    while (t.length() < width) {
      t = "0" + t;
    }
    return t;
  }

  private static Double parseAmount(String raw) {
    if (raw == null) {
      return null;
    }
    String t = raw.trim().replace(",", "").replace("$", "");
    if (t.isEmpty()) {
      return null;
    }
    try {
      return Double.valueOf(t);
    } catch (NumberFormatException e) {
      throw new RuntimeException("FSA: disbursement amount not numeric: '" + raw + "'", e);
    }
  }

  private static String nz(String s) {
    return s == null ? "" : s;
  }

  private static String trimToNull(String s) {
    if (s == null) {
      return null;
    }
    String t = s.trim();
    return t.isEmpty() ? null : t;
  }

  private String headerOrDefault(EtlPipelineConfig config, String name, String dflt) {
    Map<String, String> headers = config.getSource() != null ? config.getSource().getHeaders() : null;
    if (headers != null) {
      String v = headers.get(name);
      if (v != null && !v.isEmpty()) {
        return v;
      }
    }
    return dflt;
  }

  private String abs(String href) {
    return href.startsWith("http") ? href : HOST + href;
  }

  private String getText(String url, String userAgent) throws IOException {
    return new String(getBytes(url, userAgent), StandardCharsets.UTF_8);
  }

  /** Reads a (small) HTML page fully into memory. xlsx files use {@link #downloadToTemp} instead. */
  private byte[] getBytes(String url, String userAgent) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestProperty("User-Agent", userAgent);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setInstanceFollowRedirects(true);
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("FSA: HTTP " + code + " for " + url);
    }
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (InputStream in = conn.getInputStream()) {
      byte[] chunk = new byte[65536];
      int n;
      while ((n = in.read(chunk)) != -1) {
        buf.write(chunk, 0, n);
      }
    }
    return buf.toByteArray();
  }
}
