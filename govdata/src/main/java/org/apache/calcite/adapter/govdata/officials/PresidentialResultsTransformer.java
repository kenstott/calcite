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
package org.apache.calcite.adapter.govdata.officials;

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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Discovers and parses FEC's official "{year} Presidential General Election Results" XLSX.
 *
 * <p>Two-step process, both driven by this single transformer since the download URL has no
 * predictable year-based pattern (an opaque numeric document ID, e.g.
 * {@code /documents/5645/2024presgeresults.xlsx}): (1) the framework fetches FEC's static
 * results-and-voting-information listing page and hands its HTML to {@link #transform}; this
 * method scans it for an {@code href} ending in {@code {year}presgeresults.xlsx}; (2) if found,
 * downloads and parses that XLSX directly via POI.
 *
 * <p>Verified live for 2024 (2026-08-02): header row is {@code STATE, ELECTORAL VOTES,
 * "ELECTORAL VOTE: <winner> (<party>)", "ELECTORAL VOTE: <opponent> (<party>)", then one column
 * per candidate surname (alphabetical) carrying POPULAR vote counts, then TOTAL VOTES}. The two
 * "ELECTORAL VOTE: ..." columns are skipped here — that data already lives in
 * electoral_college_votes; every other non-STATE/ELECTORAL VOTES/TOTAL VOTES column is treated
 * as a popular-vote-by-candidate column. Only 2024 was confirmed to have this XLSX format at
 * build time; older cycles on FEC's site were PDF-only, so this method returns {@code "[]"} for
 * years with no matching link — logged at WARN with "no results link found", not thrown as an
 * error, since that is an expected (not exceptional) outcome for now.
 */
public class PresidentialResultsTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PresidentialResultsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String FEC_BASE = "https://www.fec.gov";

  private static final Pattern RESULTS_LINK =
      Pattern.compile("href=\"([^\"]*?(\\d{4})presgeresults\\.xlsx)\"");

  private static final Set<String> SKIP_HEADERS = new HashSet<>(java.util.Arrays.asList(
      "STATE", "ELECTORAL VOTES", "TOTAL VOTES"));

  private static final Map<String, String> STATE_NAMES = buildStateNameMap();

  @Override public String transform(String response, RequestContext context) {
    String yearStr = context.getDimensionValues().get("year");
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Presidential Results: Empty listing page response for year={}", yearStr);
      return "[]";
    }

    try {
      String xlsxUrl = findResultsUrl(response, yearStr);
      if (xlsxUrl == null) {
        LOGGER.warn("Presidential Results: no results link found for year={} on {}",
            yearStr, context.getUrl());
        return "[]";
      }

      byte[] xlsxBytes = downloadBytes(xlsxUrl);
      try (XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(xlsxBytes))) {
        return parseWorkbook(workbook, yearStr);
      }

    } catch (Exception e) {
      throw new RuntimeException("Presidential Results: Failed to parse response for "
          + context.getUrl() + " (year=" + yearStr + ")", e);
    }
  }

  private String findResultsUrl(String listingHtml, String yearStr) {
    Document doc = Jsoup.parse(listingHtml);
    Elements links = doc.select("a[href$=" + yearStr + "presgeresults.xlsx]");
    if (links.isEmpty()) {
      return null;
    }
    String href = links.first().attr("href");
    return href.startsWith("http") ? href : FEC_BASE + href;
  }

  private String parseWorkbook(XSSFWorkbook workbook, String yearStr) {
    Sheet sheet = workbook.getSheetAt(0);
    if (sheet == null || sheet.getLastRowNum() < 1) {
      LOGGER.warn("Presidential Results: workbook has no data rows for year={}", yearStr);
      return "[]";
    }

    Row headerRow = sheet.getRow(0);
    int stateCol = -1;
    int evCol = -1;
    int totalCol = -1;
    Map<Integer, String> candidateCols = new HashMap<>();
    for (int c = 0; c < headerRow.getLastCellNum(); c++) {
      String header = cellString(headerRow.getCell(c));
      if (header == null) {
        continue;
      }
      String h = header.trim().toUpperCase(Locale.ROOT);
      if ("STATE".equals(h)) {
        stateCol = c;
      } else if ("ELECTORAL VOTES".equals(h)) {
        evCol = c;
      } else if ("TOTAL VOTES".equals(h)) {
        totalCol = c;
      } else if (h.startsWith("ELECTORAL VOTE")) {
        continue; // per-candidate electoral vote column — already in electoral_college_votes
      } else if (!SKIP_HEADERS.contains(h)) {
        candidateCols.put(c, header.trim());
      }
    }
    if (stateCol < 0) {
      LOGGER.warn("Presidential Results: no STATE column found for year={}", yearStr);
      return "[]";
    }

    Integer year = parseIntOrNull(yearStr);
    ArrayNode result = MAPPER.createArrayNode();

    for (int r = 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      String stateCode = cellString(row.getCell(stateCol));
      if (stateCode == null || stateCode.trim().isEmpty()) {
        continue;
      }
      String stateName = STATE_NAMES.get(stateCode.trim().toUpperCase(Locale.ROOT));
      if (stateName == null) {
        continue; // territory/non-EV code not in the 50-states+DC map, or a totals row
      }
      Long stateEv = cellLong(row.getCell(evCol));
      Long stateTotal = totalCol >= 0 ? cellLong(row.getCell(totalCol)) : null;

      for (Map.Entry<Integer, String> entry : candidateCols.entrySet()) {
        Long votes = cellLong(row.getCell(entry.getKey()));
        if (votes == null) {
          continue;
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.put("year", year);
        out.put("state_name", stateName);
        if (stateEv != null) {
          out.put("state_electoral_votes", stateEv.intValue());
        } else {
          out.putNull("state_electoral_votes");
        }
        out.put("candidate_name", entry.getValue());
        out.put("popular_votes", votes);
        if (stateTotal != null) {
          out.put("state_total_votes", stateTotal);
        } else {
          out.putNull("state_total_votes");
        }
        result.add(out);
      }
    }

    LOGGER.debug("Presidential Results: Parsed {} rows for year={}", result.size(), yearStr);
    return result.toString();
  }

  private byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = conn.getInputStream();
         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
      return baos.toByteArray();
    }
  }

  private String cellString(Cell cell) {
    if (cell == null) {
      return null;
    }
    if (cell.getCellType() == CellType.STRING) {
      return cell.getStringCellValue();
    }
    if (cell.getCellType() == CellType.NUMERIC) {
      return String.valueOf((long) cell.getNumericCellValue());
    }
    return null;
  }

  private Long cellLong(Cell cell) {
    if (cell == null) {
      return null;
    }
    if (cell.getCellType() == CellType.NUMERIC) {
      return (long) cell.getNumericCellValue();
    }
    if (cell.getCellType() == CellType.STRING) {
      try {
        return Long.parseLong(cell.getStringCellValue().trim().replace(",", ""));
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private Integer parseIntOrNull(String s) {
    if (s == null) {
      return null;
    }
    try {
      return Integer.parseInt(s.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Map<String, String> buildStateNameMap() {
    Map<String, String> m = new HashMap<>();
    m.put("AL", "Alabama"); m.put("AK", "Alaska"); m.put("AZ", "Arizona");
    m.put("AR", "Arkansas"); m.put("CA", "California"); m.put("CO", "Colorado");
    m.put("CT", "Connecticut"); m.put("DE", "Delaware"); m.put("DC", "District of Columbia");
    m.put("FL", "Florida"); m.put("GA", "Georgia"); m.put("HI", "Hawaii");
    m.put("ID", "Idaho"); m.put("IL", "Illinois"); m.put("IN", "Indiana");
    m.put("IA", "Iowa"); m.put("KS", "Kansas"); m.put("KY", "Kentucky");
    m.put("LA", "Louisiana"); m.put("ME", "Maine"); m.put("MD", "Maryland");
    m.put("MA", "Massachusetts"); m.put("MI", "Michigan"); m.put("MN", "Minnesota");
    m.put("MS", "Mississippi"); m.put("MO", "Missouri"); m.put("MT", "Montana");
    m.put("NE", "Nebraska"); m.put("NV", "Nevada"); m.put("NH", "New Hampshire");
    m.put("NJ", "New Jersey"); m.put("NM", "New Mexico"); m.put("NY", "New York");
    m.put("NC", "North Carolina"); m.put("ND", "North Dakota"); m.put("OH", "Ohio");
    m.put("OK", "Oklahoma"); m.put("OR", "Oregon"); m.put("PA", "Pennsylvania");
    m.put("RI", "Rhode Island"); m.put("SC", "South Carolina"); m.put("SD", "South Dakota");
    m.put("TN", "Tennessee"); m.put("TX", "Texas"); m.put("UT", "Utah");
    m.put("VT", "Vermont"); m.put("VA", "Virginia"); m.put("WA", "Washington");
    m.put("WV", "West Virginia"); m.put("WI", "Wisconsin"); m.put("WY", "Wyoming");
    return m;
  }
}
