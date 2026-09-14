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

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the "Appendix: Facility Staffing Targets" table out of the FAA's Air Traffic
 * Controller Workforce Plan PDF (published annually to Congress at a stable URL that FAA
 * overwrites in place; no per-year URL and no CSV/XLSX distribution exists) into one row per
 * En Route (ARTCC) or Terminal (Tower/TRACON) facility: its CRWG staffing target, current
 * CPC/CPC-IT/developmental/total on-board counts, and the plan's 3-year CPC attrition
 * projection.
 *
 * <p>PDFBox's text stripper linearizes the appendix's two-column page layout (En Route left
 * column, Terminal right column) top-to-bottom-then-column-to-column, so the "En Route"/
 * "Terminal" bare section-header words appear only when a column's content starts a new
 * section — NOT on every page a section continues onto (verified against the live PDF: the
 * En Route table alone spans two page-columns with only one "En Route" header, and the
 * Terminal table's continuation onto the next page-column after an interleaved En Route
 * column carries no header at all). Section is therefore derived per-row from the facility
 * name instead (every En Route facility name ends in "ARTCC"; no Terminal facility name
 * does), not from tracking the bare header words.
 *
 * <p>A facility name that doesn't fit the name column wraps onto its own line, split from its
 * data row (e.g. {@code "LAF Lafayette Purdue"} / {@code "University Tower 12 8 0 5 13 1 0
 * 1"}). Each such fragment is buffered until the next line supplies the remaining name words
 * plus the row's 8 numeric columns.
 *
 * <p>The appendix itself prints "En Route Total"/"Terminal Total" subtotal rows (comma
 * -formatted, e.g. "5,667") for CRWG target/CPC/CPC-IT/developmental/total-on-board — this
 * parser re-derives the same five sums from the facility rows it extracted and throws if
 * either subtotal doesn't match, so a future layout change FAA makes to this PDF fails loudly
 * instead of silently emitting a partial or misparsed table.
 */
public class AtcFacilityStaffingTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String APPENDIX_START = "Appendix\nFACILITY STAFFING TARGETS";
  private static final String APPENDIX_END = "FAA Totals";

  // "ZAB Albuquerque ARTCC 234 159 0 57 216 8 9 11" — id, name (lazy), 8 numeric columns.
  private static final Pattern ROW = Pattern.compile(
      "^(\\S{2,4}) (.+?) (\\d+) (\\d+) (\\d+) (\\d+) (\\d+) (\\d+) (\\d+) (\\d+)$");
  // A line that starts a row but has no trailing 8-number run yet — a wrapped facility name.
  private static final Pattern PENDING_START = Pattern.compile("^([A-Z0-9]{2,4}) (\\S.*)$");
  private static final Pattern TRAILING_EIGHT_NUMBERS =
      Pattern.compile("\\d+ \\d+ \\d+ \\d+ \\d+ \\d+ \\d+ \\d+$");
  // A bare facility id with nothing else on the line (the id itself wrapped onto its own line).
  private static final Pattern ID_ONLY = Pattern.compile("^([A-Z0-9]{2,4})$");
  // The remainder of a wrapped name plus its 8 numeric columns, no leading facility id.
  private static final Pattern CONTINUATION = Pattern.compile(
      "^(.+?) (\\d+) (\\d+) (\\d+) (\\d+) (\\d+) (\\d+) (\\d+) (\\d+)$");
  // Just the 8 numeric columns, with no name text at all on the line (the name's last
  // fragment, e.g. "Tower", wrapped onto its own preceding line with no numbers attached).
  private static final Pattern NUMBERS_ONLY = Pattern.compile(
      "^(\\d+) (\\d+) (\\d+) (\\d+) (\\d+) (\\d+) (\\d+) (\\d+)$");

  private static final Pattern[] NON_DATA_LINES = {
      Pattern.compile("^ID\\s+FACILITY NAME"),
      Pattern.compile("^Actual on board"),
      Pattern.compile("^CPC Attrition Forecast"),
      Pattern.compile("^\\d{4}\\*\\s+\\d{4}\\*\\s+\\d{4}\\*$"),
      Pattern.compile("^(En Route|Terminal|Facility|Grand) Total\\b"),
      Pattern.compile("^\u2022?\\s*\\d+\\s*\u2022?$"),
      Pattern.compile("^Air Traffic Controller Workforce Plan$"),
      Pattern.compile("^\\d{4}\\s*-\\s*\\d{4}$"),
      Pattern.compile("^FACILITY STAFFING TARGETS$"),
      Pattern.compile("^(Note:|\\*)"),
  };

  private static final Pattern PLAN_PERIOD = Pattern.compile("(\\d{4})\\s*-\\s*(\\d{4})");
  private static final Pattern AS_OF_DATE =
      Pattern.compile("Actual on board as of (\\d{2})/(\\d{2})/(\\d{2})");
  private static final Pattern SECTION_TOTAL_PREFIX =
      Pattern.compile("^(En Route|Terminal) Total ([\\d,]+) ([\\d,]+) ([\\d,]+) ([\\d,]+) ([\\d,]+)");

  @Override
  public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    try {
      byte[] pdfBytes = downloadBytes(url);
      String fullText;
      try (PDDocument document = PDDocument.load(new ByteArrayInputStream(pdfBytes))) {
        fullText = new PDFTextStripper().getText(document);
      }
      // PDFBox's stripper leaves a trailing space or two before every line break (verified
      // against the live PDF); strip it so line-anchored markers/regexes don't need to guess.
      fullText = fullText.replaceAll("[ \\t]+\n", "\n");

      String planPeriod = extractPlanPeriod(fullText);
      LocalDate asOfDate = extractAsOfDate(fullText);
      String appendixText = extractAppendix(fullText);

      ArrayNode rows = MAPPER.createArrayNode();
      long[] enRouteSums = new long[5];
      long[] terminalSums = new long[5];
      parseRows(appendixText, rows, planPeriod, asOfDate, enRouteSums, terminalSums);
      verifySectionTotals(appendixText, "En Route", enRouteSums);
      verifySectionTotals(appendixText, "Terminal", terminalSums);

      return MAPPER.writeValueAsString(rows);
    } catch (Exception e) {
      throw new RuntimeException("FAA Controller Workforce Plan: failed to parse from " + url, e);
    }
  }

  private String extractPlanPeriod(String fullText) {
    Matcher m = PLAN_PERIOD.matcher(fullText);
    if (!m.find()) {
      throw new IllegalStateException("Plan period (e.g. \"2025 - 2028\") not found in workforce plan PDF");
    }
    return m.group(1) + "-" + m.group(2);
  }

  private LocalDate extractAsOfDate(String fullText) {
    Matcher m = AS_OF_DATE.matcher(fullText);
    if (!m.find()) {
      throw new IllegalStateException("\"Actual on board as of MM/DD/YY\" date not found in workforce plan PDF");
    }
    String mmddyy = m.group(1) + "/" + m.group(2) + "/" + m.group(3);
    return LocalDate.parse(mmddyy, DateTimeFormatter.ofPattern("MM/dd/yy"));
  }

  private String extractAppendix(String fullText) {
    int start = fullText.indexOf(APPENDIX_START);
    if (start < 0) {
      throw new IllegalStateException("Appendix start marker not found in workforce plan PDF");
    }
    int end = fullText.indexOf(APPENDIX_END, start);
    if (end < 0) {
      throw new IllegalStateException("Appendix end marker (\"FAA Totals\") not found in workforce plan PDF");
    }
    return fullText.substring(start, end);
  }

  private void parseRows(String appendixText, ArrayNode rows, String planPeriod, LocalDate asOfDate,
      long[] enRouteSums, long[] terminalSums) {
    String pendingFacilityId = null;
    StringBuilder pendingName = null;
    // The intro paragraph above the table contains stray ALL-CAPS 2-4 char tokens (e.g.
    // "FAA") that would otherwise look like a wrapped facility-name start to PENDING_START.
    // The literal "En Route"/"Terminal" section-header word is always the first thing the
    // real table prints (verified against the live PDF), so gate row parsing on having seen
    // one at least once rather than guessing which ALL-CAPS tokens are real facility ids.
    boolean inTable = false;
    for (String rawLine : appendixText.split("\n")) {
      String line = rawLine.trim();
      if (line.equals("En Route") || line.equals("Terminal")) {
        inTable = true;
        continue;
      }
      if (line.isEmpty() || isNonDataLine(line)) {
        continue;
      }
      if (!inTable) {
        continue;
      }

      if (pendingFacilityId != null) {
        // A name that doesn't fit the column wraps across an unpredictable number of bare
        // lines (verified against the live PDF: PDFBox splits some wraps into a bare id line,
        // one-or-more bare name-fragment lines, and either a "name-tail + 8 numbers" line or
        // an all-numbers line with no text at all) — keep accumulating fragments until a line
        // supplies the 8 numeric columns, however much or little name text comes with it.
        Matcher numbersOnly = NUMBERS_ONLY.matcher(line);
        if (numbersOnly.matches()) {
          addRow(rows, pendingFacilityId, pendingName.toString(), numbersOnly, 1, planPeriod, asOfDate,
              enRouteSums, terminalSums);
          pendingFacilityId = null;
          pendingName = null;
          continue;
        }
        Matcher cont = CONTINUATION.matcher(line);
        if (cont.matches()) {
          pendingName.append(' ').append(cont.group(1));
          addRow(rows, pendingFacilityId, pendingName.toString(), cont, 2, planPeriod, asOfDate,
              enRouteSums, terminalSums);
          pendingFacilityId = null;
          pendingName = null;
          continue;
        }
        // Pure text, no numeric columns yet — another name fragment.
        pendingName.append(' ').append(line);
        continue;
      }

      Matcher row = ROW.matcher(line);
      if (row.matches()) {
        addRow(rows, row.group(1), row.group(2), row, 3, planPeriod, asOfDate, enRouteSums, terminalSums);
        continue;
      }
      Matcher idOnly = ID_ONLY.matcher(line);
      if (idOnly.matches()) {
        pendingFacilityId = idOnly.group(1);
        pendingName = new StringBuilder();
        continue;
      }
      Matcher pendingStart = PENDING_START.matcher(line);
      if (pendingStart.matches() && !TRAILING_EIGHT_NUMBERS.matcher(line).find()) {
        pendingFacilityId = pendingStart.group(1);
        pendingName = new StringBuilder(pendingStart.group(2));
      }
    }
    if (pendingFacilityId != null) {
      throw new IllegalStateException(
          "Unresolved wrapped facility name in workforce plan PDF: " + pendingFacilityId + " " + pendingName);
    }
  }

  private boolean isNonDataLine(String line) {
    for (Pattern p : NON_DATA_LINES) {
      if (p.matcher(line).find()) {
        return true;
      }
    }
    return false;
  }

  private void addRow(ArrayNode rows, String facilityId, String facilityName, Matcher numbers,
      int firstGroup, String planPeriod, LocalDate asOfDate, long[] enRouteSums, long[] terminalSums) {
    int crwgTarget = Integer.parseInt(numbers.group(firstGroup));
    int cpc = Integer.parseInt(numbers.group(firstGroup + 1));
    int cpcIt = Integer.parseInt(numbers.group(firstGroup + 2));
    int developmental = Integer.parseInt(numbers.group(firstGroup + 3));
    int totalOnboard = Integer.parseInt(numbers.group(firstGroup + 4));
    int lossYr1 = Integer.parseInt(numbers.group(firstGroup + 5));
    int lossYr2 = Integer.parseInt(numbers.group(firstGroup + 6));
    int lossYr3 = Integer.parseInt(numbers.group(firstGroup + 7));

    boolean enRoute = facilityName.endsWith("ARTCC");
    long[] sums = enRoute ? enRouteSums : terminalSums;
    sums[0] += crwgTarget;
    sums[1] += cpc;
    sums[2] += cpcIt;
    sums[3] += developmental;
    sums[4] += totalOnboard;

    ObjectNode obj = MAPPER.createObjectNode();
    obj.put("facility_id", facilityId);
    obj.put("facility_name", facilityName);
    obj.put("facility_type", enRoute ? "En Route" : "Terminal");
    obj.put("crwg_target", crwgTarget);
    obj.put("cpc", cpc);
    obj.put("cpc_it", cpcIt);
    obj.put("developmental", developmental);
    obj.put("total_onboard", totalOnboard);
    obj.put("projected_attrition_yr1", lossYr1);
    obj.put("projected_attrition_yr2", lossYr2);
    obj.put("projected_attrition_yr3", lossYr3);
    obj.put("as_of_date", asOfDate.toString());
    obj.put("plan_period", planPeriod);
    obj.put("type", "atc_facility_staffing");
    rows.add(obj);
  }

  private void verifySectionTotals(String appendixText, String section, long[] computedSums) {
    for (String rawLine : appendixText.split("\n")) {
      Matcher m = SECTION_TOTAL_PREFIX.matcher(rawLine.trim());
      if (m.find() && m.group(1).equals(section)) {
        long[] printed = new long[5];
        for (int i = 0; i < 5; i++) {
          printed[i] = Long.parseLong(m.group(i + 2).replace(",", ""));
        }
        if (!java.util.Arrays.equals(printed, computedSums)) {
          throw new IllegalStateException(section + " subtotal mismatch: PDF prints "
              + java.util.Arrays.toString(printed) + " but parsed rows sum to "
              + java.util.Arrays.toString(computedSums));
        }
        return;
      }
    }
    throw new IllegalStateException(section + " Total line not found in workforce plan PDF appendix");
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
    InputStream is = conn.getInputStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
    } finally {
      is.close();
    }
    return baos.toByteArray();
  }
}
