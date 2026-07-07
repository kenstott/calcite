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
 * <p>Per disbursement-year "vintage" (PMT{yy}) FSA publishes ~6 state-range xlsx
 * files. The provider scrapes the payment-files landing page (source.url) for the
 * requested vintage, resolving both link forms observed live: direct
 * {@code /sites/default/files/documents/....PMT{yy}....xlsx} and Drupal node pages
 * {@code /documents/state-...pmt{yy}...} that wrap the xlsx. The vintage token
 * {@code pmt{yy}} is present in both href forms, so matching is by that token.
 *
 * <p>The raw files are per-payee transactional; this aggregates to one row per
 * (program_year, county_fips, program_code): SUM(disbursement) and a distinct
 * payee count (a proxy for farms — FSA files carry no operation/farm id and no
 * commodity dimension). Both the disbursement year (the vintage) and the program
 * year are kept; they differ (a 2023 file holds program years up to 2023).
 *
 * <p>xlsx read uses POI's in-memory {@link XSSFWorkbook}, one file at a time, so
 * peak heap is one workbook plus the bounded aggregation map.
 */
public class FsaCommodityPaymentsProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(FsaCommodityPaymentsProvider.class);

  private static final String HOST = "https://www.fsa.usda.gov";
  private static final String DEFAULT_UA = "Mozilla/5.0 (compatible; govdata-etl/1.0)";
  private static final Pattern HREF = Pattern.compile("href=\"([^\"]+)\"", Pattern.CASE_INSENSITIVE);
  private static final Pattern XLSX_IN_NODE =
      Pattern.compile("href=\"([^\"]*\\.xlsx)\"", Pattern.CASE_INSENSITIVE);

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

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String landingUrl = config.getSource() != null ? config.getSource().getUrl() : null;
    if (landingUrl == null || landingUrl.isEmpty()) {
      throw new IOException("FSA: source.url (landing page) is required");
    }
    String userAgent = headerOrDefault(config, "User-Agent", DEFAULT_UA);
    int disbursementYear = resolveYear(variables);
    String token = "pmt" + String.format("%02d", disbursementYear % 100);

    List<String> xlsxUrls = resolveVintageFiles(landingUrl, userAgent, token);
    if (xlsxUrls.isEmpty()) {
      throw new IOException("FSA: no payment files found for vintage " + token
          + " (disbursement year " + disbursementYear + ") on " + landingUrl);
    }
    LOGGER.info("FSA: vintage {} -> {} file(s)", token, xlsxUrls.size());

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
    LOGGER.info("FSA: vintage {} aggregated to {} county/program/year rows", token, rows.size());
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

  /** Scrapes the landing page and resolves every file for the vintage token to an xlsx URL. */
  private List<String> resolveVintageFiles(String landingUrl, String userAgent, String token)
      throws IOException {
    String html = getText(landingUrl, userAgent);
    Set<String> resolved = new LinkedHashSet<String>();
    Matcher m = HREF.matcher(html);
    while (m.find()) {
      String href = m.group(1);
      String lower = href.toLowerCase();
      if (!lower.contains(token)) {
        continue;
      }
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

  private void aggregateFile(String xlsxUrl, String userAgent, Map<String, Agg> agg)
      throws IOException {
    byte[] bytes = getBytes(xlsxUrl, userAgent);
    XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(bytes));
    try {
      Sheet sheet = workbook.getSheetAt(0);
      Iterator<Row> it = sheet.iterator();
      if (!it.hasNext()) {
        throw new IOException("FSA: empty sheet in " + xlsxUrl);
      }
      Map<String, Integer> col = headerIndex(it.next(), xlsxUrl);
      int cState = col.get(H_STATE);
      int cCounty = col.get(H_COUNTY);
      int cPayee = col.get(H_PAYEE);
      int cAmount = col.get(H_AMOUNT);
      int cCode = col.get(H_PROG_CODE);
      int cDesc = col.get(H_PROG_DESC);
      int cYear = col.get(H_PROG_YEAR);
      while (it.hasNext()) {
        Row r = it.next();
        String stateRaw = cellString(r.getCell(cState));
        String countyRaw = cellString(r.getCell(cCounty));
        String countyFips = countyFips(stateRaw, countyRaw);
        String programCode = trimToNull(cellString(r.getCell(cCode)));
        String programYear = trimToNull(cellString(r.getCell(cYear)));
        String key = nz(programYear) + "|" + nz(countyFips) + "|" + nz(programCode);
        Agg a = agg.get(key);
        if (a == null) {
          a = new Agg();
          a.stateFips = pad(stateRaw, 2);
          a.programDescription = trimToNull(cellString(r.getCell(cDesc)));
          agg.put(key, a);
        }
        Double amount = parseAmount(cellString(r.getCell(cAmount)));
        if (amount != null) {
          a.payments += amount.doubleValue();
        }
        String payee = trimToNull(cellString(r.getCell(cPayee)));
        if (payee != null) {
          a.payees.add(payee);
        }
      }
    } finally {
      workbook.close();
    }
  }

  private Map<String, Integer> headerIndex(Row header, String xlsxUrl) throws IOException {
    Map<String, Integer> col = new HashMap<String, Integer>();
    for (int i = 0; i < header.getLastCellNum(); i++) {
      String name = trimToNull(cellString(header.getCell(i)));
      if (name != null) {
        col.put(name, Integer.valueOf(i));
      }
    }
    for (String required : new String[] {H_STATE, H_COUNTY, H_PAYEE, H_AMOUNT,
        H_PROG_CODE, H_PROG_DESC, H_PROG_YEAR}) {
      if (!col.containsKey(required)) {
        throw new IOException("FSA: required column '" + required + "' missing in " + xlsxUrl
            + " — header=" + col.keySet());
      }
    }
    return col;
  }

  private String countyFips(String stateFsa, String countyFsa) {
    String st = pad(stateFsa, 2);
    String co = pad(countyFsa, 3);
    if (st == null || co == null) {
      return null;
    }
    return st + co;
  }

  private String pad(String v, int width) {
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

  private Double parseAmount(String raw) {
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

  /** POI cell -> string (numbers rendered without scientific notation; longs unadorned). */
  private String cellString(Cell cell) {
    if (cell == null) {
      return null;
    }
    CellType type = cell.getCellType();
    if (type == CellType.STRING) {
      return cell.getStringCellValue();
    }
    if (type == CellType.NUMERIC) {
      double d = cell.getNumericCellValue();
      if (d == Math.floor(d) && !Double.isInfinite(d)) {
        return String.valueOf((long) d);
      }
      return String.valueOf(d);
    }
    if (type == CellType.BOOLEAN) {
      return String.valueOf(cell.getBooleanCellValue());
    }
    return null;
  }
}
