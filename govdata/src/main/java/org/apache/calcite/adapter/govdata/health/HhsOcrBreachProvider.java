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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.CookieManager;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataProvider for {@code health.hhs_ocr_breaches} — the HHS Office for Civil Rights HIPAA
 * Breach Portal ("wall of shame"): one row per reported breach of unsecured protected health
 * information affecting 500+ individuals, from 2009 to the present.
 *
 * <p>The portal is a stateful JSF (PrimeFaces) application with no static download URL: the CSV
 * export is a form post that must carry the session cookie and the page's {@code ViewState}. This
 * provider drives that flow — front page, "View HIPAA Breach Reports" link, CSV export of the
 * default "Under Investigation" tab, then an ajax {@code tabChange} to the "Archive" tab and a
 * second CSV export (the archive holds every closed case back to 2009). JSF component ids
 * ({@code j_idtNN}) are generated and shift when the page changes, so they are located by their
 * visible link text and icon title, never hard-coded; a page that no longer matches throws.
 *
 * <p>Both exports carry the same nine columns. The first and eighth headers are the unrendered
 * JSF panel object's {@code toString()} (the portal's own export bug): by position, they are the
 * entity name and the business-associate-present flag, which this provider verifies by header
 * shape and the {@code Yes}/{@code No} domain rather than trusting silently.
 */
public class HhsOcrBreachProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(HhsOcrBreachProvider.class);

  private static final String PANEL_HEADER_PREFIX = "javax.faces.component.UIPanel@";
  private static final String[] EXPECTED_HEADERS = {
      null, "State", "Covered Entity Type", "Individuals Affected", "Breach Submission Date",
      "Type of Breach", "Location of Breached Information", null, "Web Description"};
  private static final DateTimeFormatter US_DATE = DateTimeFormatter.ofPattern("MM/dd/yyyy");

  private static final Pattern VIEW_STATE =
      Pattern.compile("name=\"javax\\.faces\\.ViewState\"[^>]*value=\"([^\"]+)\"");
  private static final Pattern VIEW_STATE_AJAX =
      Pattern.compile("<update id=\"[^\"]*ViewState[^\"]*\"><!\\[CDATA\\[([^\\]]+)\\]\\]>");
  private static final Pattern TAB_WIDGET =
      Pattern.compile("PrimeFaces\\.cw\\(\"TabView\",\"[^\"]+\",\\{id:\"([^\"]+)\"");
  private static final Pattern ARCHIVE_TAB_ID =
      Pattern.compile("id=\"([^\"]+:archiveTab)\"");

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String frontUrl = config.getSource() != null ? config.getSource().getUrl() : null;
    if (frontUrl == null || frontUrl.isEmpty()) {
      throw new IOException("HHS OCR breaches: source.url (portal front page) is required");
    }
    String userAgent = config.getSource().getHeaders() == null ? null
        : config.getSource().getHeaders().get("User-Agent");
    if (userAgent == null || userAgent.isEmpty()) {
      throw new IOException("HHS OCR breaches: source.headers.User-Agent is required "
          + "(the portal rejects non-browser agents)");
    }

    Session s = new Session(userAgent);
    URI front = URI.create(frontUrl);

    String frontHtml = s.get(front);
    String linkId = componentIdForLink(frontHtml, "View HIPAA Breach Reports");
    URI reportUri = s.postForRedirect(front, formFields(frontHtml, linkId));
    String reportHtml = s.get(reportUri);

    String csvId = componentIdForImgTitle(reportHtml, "Export as CSV");
    String viewState = viewState(reportHtml);

    List<Map<String, Object>> rows = new ArrayList<>();

    String underInvestigation = s.postText(reportUri, exportFields(csvId, viewState, null));
    int ui = parseCsv(underInvestigation, "Under Investigation", rows);

    Matcher tab = TAB_WIDGET.matcher(reportHtml);
    Matcher archive = ARCHIVE_TAB_ID.matcher(reportHtml);
    if (!tab.find() || !archive.find()) {
      throw new IOException("HHS OCR breaches: Archive tab not found on the report page");
    }
    String tabId = tab.group(1);
    String ajax = s.postText(reportUri, tabChangeFields(tabId, archive.group(1), viewState));
    Matcher vs = VIEW_STATE_AJAX.matcher(ajax);
    String archiveViewState = vs.find() ? vs.group(1) : viewState;
    String archiveCsv = s.postText(reportUri, exportFields(csvId, archiveViewState, tabId));
    int ar = parseCsv(archiveCsv, "Archive", rows);

    LOGGER.info("HHS OCR breaches: {} Under Investigation + {} Archive rows", ui, ar);
    if (ui == 0 || ar == 0) {
      throw new IOException("HHS OCR breaches: expected both tabs to export rows, got "
          + ui + " under investigation and " + ar + " archive");
    }
    return rows.iterator();
  }

  // ---------------------------------------------------------------------
  // JSF page parsing
  // ---------------------------------------------------------------------

  /** The generated component id of the anchor whose visible text is {@code linkText}. */
  static String componentIdForLink(String html, String linkText) throws IOException {
    Pattern p = Pattern.compile("\\{'([^']+)':'[^']+'\\},'[^']*'\\);return false\">"
        + Pattern.quote(linkText) + "<");
    Matcher m = p.matcher(html);
    if (!m.find()) {
      throw new IOException("HHS OCR breaches: link \"" + linkText + "\" not found on page");
    }
    return m.group(1);
  }

  /** The generated component id of the anchor wrapping the image titled {@code title}. */
  static String componentIdForImgTitle(String html, String title) throws IOException {
    Pattern p = Pattern.compile("\\{'([^']+)':'[^']+'\\},'[^']*'\\);return false\"><img [^>]*title=\""
        + Pattern.quote(title) + "\"");
    Matcher m = p.matcher(html);
    if (!m.find()) {
      throw new IOException("HHS OCR breaches: control \"" + title + "\" not found on page");
    }
    return m.group(1);
  }

  private static String viewState(String html) throws IOException {
    Matcher m = VIEW_STATE.matcher(html);
    if (!m.find()) {
      throw new IOException("HHS OCR breaches: javax.faces.ViewState not found on page");
    }
    return m.group(1);
  }

  private static Map<String, String> formFields(String html, String commandId)
      throws IOException {
    Map<String, String> f = new LinkedHashMap<>();
    f.put("ocrForm", "ocrForm");
    f.put(commandId, commandId);
    f.put("javax.faces.ViewState", viewState(html));
    return f;
  }

  private static Map<String, String> exportFields(String csvId, String viewState,
      String tabId) {
    Map<String, String> f = new LinkedHashMap<>();
    f.put("ocrForm", "ocrForm");
    if (tabId != null) {
      f.put(tabId + "_activeIndex", "1");
    }
    f.put(csvId, csvId);
    f.put("javax.faces.ViewState", viewState);
    return f;
  }

  private static Map<String, String> tabChangeFields(String tabId, String archiveTabId,
      String viewState) {
    Map<String, String> f = new LinkedHashMap<>();
    f.put("javax.faces.partial.ajax", "true");
    f.put("javax.faces.source", tabId);
    f.put("javax.faces.partial.execute", tabId);
    f.put("javax.faces.partial.render", "ocrForm:breachReports ocrForm:results");
    f.put("javax.faces.behavior.event", "tabChange");
    f.put("javax.faces.partial.event", "tabChange");
    f.put(tabId + "_newTab", archiveTabId);
    f.put(tabId + "_tabindex", "1");
    f.put(tabId + "_activeIndex", "1");
    f.put("ocrForm", "ocrForm");
    f.put("javax.faces.ViewState", viewState);
    return f;
  }

  // ---------------------------------------------------------------------
  // CSV -> rows
  // ---------------------------------------------------------------------

  /** Parses one export, appending rows; returns the number appended. */
  static int parseCsv(String csv, String status, List<Map<String, Object>> out)
      throws IOException {
    BufferedReader reader = new BufferedReader(new StringReader(csv));
    String headerRecord = CsvRecordReader.readRecord(reader);
    if (headerRecord == null) {
      throw new IOException("HHS OCR breaches: " + status + " export is empty");
    }
    List<String> header = CsvRecordReader.splitFields(headerRecord, ',');
    verifyHeader(header, status);

    int count = 0;
    String record;
    while ((record = CsvRecordReader.readRecord(reader)) != null) {
      if (record.trim().isEmpty()) {
        continue;
      }
      List<String> f = CsvRecordReader.splitFields(record, ',');
      if (f.size() != EXPECTED_HEADERS.length) {
        throw new IOException("HHS OCR breaches: " + status + " record has " + f.size()
            + " fields, expected " + EXPECTED_HEADERS.length + ": " + record);
      }
      out.add(toRow(f, status));
      count++;
    }
    return count;
  }

  private static void verifyHeader(List<String> header, String status) throws IOException {
    if (header.size() != EXPECTED_HEADERS.length) {
      throw new IOException("HHS OCR breaches: " + status + " header has " + header.size()
          + " columns, expected " + EXPECTED_HEADERS.length + ": " + header);
    }
    for (int i = 0; i < EXPECTED_HEADERS.length; i++) {
      String want = EXPECTED_HEADERS[i];
      String got = header.get(i);
      boolean ok = want == null ? got.startsWith(PANEL_HEADER_PREFIX) : want.equals(got);
      if (!ok) {
        throw new IOException("HHS OCR breaches: " + status + " header column " + i
            + " is \"" + got + "\", expected " + (want == null ? PANEL_HEADER_PREFIX + "*" : want));
      }
    }
  }

  private static Map<String, Object> toRow(List<String> f, String status) throws IOException {
    String submitted = f.get(4);
    LocalDate date = LocalDate.parse(submitted, US_DATE);
    String ba = f.get(7);
    Boolean baPresent;
    if ("Yes".equals(ba)) {
      baPresent = Boolean.TRUE;
    } else if ("No".equals(ba)) {
      baPresent = Boolean.FALSE;
    } else {
      throw new IOException("HHS OCR breaches: business associate flag is \"" + ba
          + "\", expected Yes or No (column positions may have shifted)");
    }
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("entity_name", blankToNull(f.get(0)));
    row.put("state", blankToNull(f.get(1)));
    row.put("covered_entity_type", blankToNull(f.get(2)));
    row.put("individuals_affected", blankToNull(f.get(3)) == null ? null
        : Long.valueOf(Long.parseLong(f.get(3).trim())));
    row.put("breach_submission_date", date.toString());
    row.put("breach_year", Integer.valueOf(date.getYear()));
    row.put("breach_type", blankToNull(f.get(5)));
    row.put("breach_location", blankToNull(f.get(6)));
    row.put("business_associate_present", baPresent);
    row.put("web_description", blankToNull(f.get(8)));
    row.put("report_status", status);
    row.put("type", "hhs_ocr_breaches");
    return row;
  }

  private static String blankToNull(String v) {
    return v == null || v.trim().isEmpty() ? null : v.trim();
  }

  // ---------------------------------------------------------------------
  // HTTP session (cookies + manual redirect handling)
  // ---------------------------------------------------------------------

  private static final class Session {
    private final HttpClient client;
    private final String userAgent;

    Session(String userAgent) {
      this.userAgent = userAgent;
      this.client = HttpClient.newBuilder()
          .cookieHandler(new CookieManager())
          .followRedirects(HttpClient.Redirect.NEVER)
          .connectTimeout(Duration.ofSeconds(30))
          .build();
    }

    String get(URI uri) throws IOException {
      HttpResponse<String> r = send(base(uri).GET().build());
      if (r.statusCode() == 302) {
        throw new IOException("HHS OCR breaches: unexpected redirect from " + uri + " to "
            + r.headers().firstValue("Location").orElse("?"));
      }
      return ok(r, uri);
    }

    URI postForRedirect(URI uri, Map<String, String> fields) throws IOException {
      HttpResponse<String> r = send(post(uri, fields));
      if (r.statusCode() != 302) {
        throw new IOException("HHS OCR breaches: expected 302 from " + uri + ", got "
            + r.statusCode());
      }
      String loc = r.headers().firstValue("Location").orElseThrow(
          () -> new IOException("HHS OCR breaches: 302 without Location from " + uri));
      return uri.resolve(loc);
    }

    String postText(URI uri, Map<String, String> fields) throws IOException {
      return ok(send(post(uri, fields)), uri);
    }

    private HttpRequest.Builder base(URI uri) {
      return HttpRequest.newBuilder(uri).timeout(Duration.ofMinutes(5))
          .header("User-Agent", userAgent);
    }

    private HttpRequest post(URI uri, Map<String, String> fields) {
      StringBuilder body = new StringBuilder();
      for (Map.Entry<String, String> e : fields.entrySet()) {
        if (body.length() > 0) {
          body.append('&');
        }
        body.append(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8)).append('=')
            .append(URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8));
      }
      HttpRequest.Builder b = base(uri)
          .header("Content-Type", "application/x-www-form-urlencoded")
          .POST(HttpRequest.BodyPublishers.ofString(body.toString()));
      if (fields.containsKey("javax.faces.partial.ajax")) {
        b.header("Faces-Request", "partial/ajax");
      }
      return b.build();
    }

    private HttpResponse<String> send(HttpRequest req) throws IOException {
      try {
        return client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("HHS OCR breaches: interrupted calling " + req.uri(), e);
      }
    }

    private static String ok(HttpResponse<String> r, URI uri) throws IOException {
      if (r.statusCode() < 200 || r.statusCode() >= 300) {
        throw new IOException("HHS OCR breaches: HTTP " + r.statusCode() + " for " + uri);
      }
      return r.body();
    }
  }
}
