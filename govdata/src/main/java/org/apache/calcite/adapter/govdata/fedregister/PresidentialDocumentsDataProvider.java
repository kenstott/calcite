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
package org.apache.calcite.adapter.govdata.fedregister;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.CrossProcessRateLimiter;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.file.etl.VariableResolver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DataProvider for {@code presidential_documents}: the official text of the President's
 * remarks, news conferences, interviews, statements, messages and signing statements from GPO's
 * Daily Compilation of Presidential Documents (govinfo collection {@code CPD}, one package
 * {@code DCPD-YYYYNNNNN} per document).
 *
 * <p>Per (year, month) it lists the packages published in that month through
 * {@code api.govinfo.gov/published/{from}/{to}?collection=CPD} (offsetMark pagination), then for
 * each package reads the metadata JSON from {@code /packages/{id}/summary} and the full text from
 * {@code www.govinfo.gov/content/pkg/{id}/html/{id}.htm}. The listing and summary calls need the
 * {@code API_DATA_GOV} key (1,000 requests per hour per key) and are paced to stay under it; the
 * summary is the only per-document API call, the text comes from the keyless content host.
 */
public class PresidentialDocumentsDataProvider implements CachingDataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PresidentialDocumentsDataProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String API = "https://api.govinfo.gov";
  private static final String CONTENT = "https://www.govinfo.gov/content/pkg/";
  private static final String PACKAGE_PREFIX = "DCPD-";
  private static final int LIST_PAGE_SIZE = 100;
  private static final int MAX_ATTEMPTS = 8;
  private static final long INITIAL_BACKOFF_MS = 2000;
  private static final long MAX_BACKOFF_MS = 120000;
  // api.data.gov allows 1,000 requests per hour per key: one call every 3.6 seconds.
  private static final long API_INTERVAL_MS = 3600L;
  private static final long CONTENT_INTERVAL_MS = 500L;

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    int year = Integer.parseInt(variables.get("year"));
    int month = Integer.parseInt(variables.get("month"));

    String apiKey = VariableResolver.resolveEnvVars("${API_DATA_GOV:}");
    if (apiKey == null || apiKey.trim().isEmpty()) {
      throw new IOException("presidential_documents: API_DATA_GOV is unset; the govinfo API "
          + "requires it.");
    }

    LocalDate from = LocalDate.of(year, month, 1);
    LocalDate to = from.plusMonths(1).minusDays(1);

    List<String> packageIds = listPackages(from, to, apiKey);
    List<String> confirm = listPackages(from, to, apiKey);
    if (!new LinkedHashSet<String>(packageIds).equals(new LinkedHashSet<String>(confirm))) {
      throw new IOException("presidential_documents: two listings of " + from + " to " + to
          + " disagree (" + packageIds.size() + " vs " + confirm.size() + " packages); "
          + "refusing to build the month from an unstable listing");
    }
    LOGGER.info("presidential_documents: {} packages issued {} to {}", packageIds.size(), from, to);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>(packageIds.size());
    for (String id : packageIds) {
      rows.add(buildRow(id, apiKey, rawCache));
    }
    return rows.iterator();
  }

  /** Lists the DCPD package ids issued in the window, following offsetMark pagination. */
  private List<String> listPackages(LocalDate from, LocalDate to, String apiKey)
      throws IOException {
    List<String> ids = new ArrayList<String>();
    String offsetMark = "*";
    while (offsetMark != null) {
      String url = API + "/published/" + from + "/" + to + "?collection=CPD&pageSize="
          + LIST_PAGE_SIZE + "&offsetMark=" + URLEncoder.encode(offsetMark, "UTF-8");
      JsonNode root;
      try (InputStream in = get(url, apiKey, API_INTERVAL_MS)) {
        root = MAPPER.readTree(in);
      }
      for (JsonNode pkg : root.path("packages")) {
        String id = text(pkg, "packageId");
        if (id == null || !id.startsWith(PACKAGE_PREFIX)) {
          throw new IOException("presidential_documents: unexpected package in CPD listing: " + id);
        }
        ids.add(id);
      }
      JsonNode count = root.get("count");
      if (count == null || !count.canConvertToInt()) {
        throw new IOException("presidential_documents: CPD listing has no count: " + from + " to "
            + to);
      }
      int pageSize = root.path("packages").size();
      String next = text(root, "nextPage");
      if (next == null && ids.size() != count.asInt()) {
        throw new IOException("presidential_documents: CPD listing " + from + " to " + to
            + " reports count=" + count.asInt() + " but returned " + ids.size() + " packages");
      }
      if (pageSize == 0 && next != null) {
        throw new IOException("presidential_documents: empty CPD page with a nextPage for "
            + from + " to " + to);
      }
      offsetMark = next == null ? null : queryParam(next, "offsetMark");
    }
    return ids;
  }

  private Map<String, Object> buildRow(String id, String apiKey, RawCache rawCache)
      throws IOException {
    String summaryUrl = API + "/packages/" + id + "/summary";
    JsonNode s;
    try (InputStream in = rawCache.openStream(summaryUrl,
        () -> get(summaryUrl, apiKey, API_INTERVAL_MS))) {
      s = MAPPER.readTree(in);
    }
    String htmlUrl = CONTENT + id + "/html/" + id + ".htm";
    String html;
    try (InputStream in = rawCache.openStream(htmlUrl, () -> get(htmlUrl, null, CONTENT_INTERVAL_MS))) {
      html = new String(readAll(in), StandardCharsets.UTF_8);
    }

    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("package_id", id);
    row.put("date_issued", text(s, "dateIssued"));
    row.put("title", text(s, "title"));
    JsonNode cat = s.path("dcpdCategory").path(0);
    row.put("category", text(cat, "level1"));
    row.put("subcategory", text(cat, "level2"));
    row.put("location", text(s, "location"));
    row.put("president", presidentName(s.path("president").path("names")));
    row.put("pages", s.hasNonNull("pages") ? Integer.valueOf(s.get("pages").asInt()) : null);
    row.put("notes", text(s, "notes"));
    row.put("subjects", subjects(s.path("subject")));
    row.put("names", joinArray(s.path("name"), "; "));
    row.put("details_link", text(s, "detailsLink"));
    row.put("body_text", bodyText(html, id));
    return row;
  }

  /** The first {@code authority-fnf} (first-name-first) name in the president's name list. */
  private static String presidentName(JsonNode names) {
    for (JsonNode n : names) {
      String v = text(n, "authority-fnf");
      if (v != null) {
        return v;
      }
    }
    return null;
  }

  /** Subjects as "level1 / level2 / level3" strings joined with "; ". */
  private static String subjects(JsonNode subjects) {
    Set<String> out = new LinkedHashSet<String>();
    for (JsonNode sub : subjects) {
      StringBuilder sb = new StringBuilder();
      for (String level : new String[] {"level1", "level2", "level3"}) {
        String v = text(sub, level);
        if (v != null) {
          if (sb.length() > 0) {
            sb.append(" / ");
          }
          sb.append(v);
        }
      }
      if (sb.length() > 0) {
        out.add(sb.toString());
      }
    }
    return out.isEmpty() ? null : String.join("; ", out);
  }

  private static String joinArray(JsonNode arr, String sep) {
    List<String> parts = new ArrayList<String>();
    for (JsonNode n : arr) {
      String v = n.asText();
      if (v != null && !v.trim().isEmpty()) {
        parts.add(v.trim());
      }
    }
    return parts.isEmpty() ? null : String.join(sep, parts);
  }

  /**
   * The document text: every paragraph of the GPO HTML after the leading "Administration of ..."
   * line, separated by blank lines. The title and date line stay in, as they do in the printed
   * compilation.
   */
  private static String bodyText(String html, String id) throws IOException {
    Document doc = Jsoup.parse(html);
    List<String> paragraphs = new ArrayList<String>();
    boolean first = true;
    for (Element p : doc.select("body h1, body p")) {
      // GPO double-encodes some entities (a literal "&ndash;" in the HTML text); decode once more.
      String t = Parser.unescapeEntities(p.text(), false).trim();
      if (first) {
        first = false;
        continue;
      }
      if (!t.isEmpty()) {
        paragraphs.add(t);
      }
    }
    if (paragraphs.isEmpty()) {
      throw new IOException("presidential_documents: no text paragraphs in " + id);
    }
    return String.join("\n\n", paragraphs);
  }

  /** Paced GET with exponential backoff on the transient statuses; the key never reaches a log. */
  private InputStream get(String url, String apiKey, long intervalMs) throws IOException {
    String full = apiKey == null ? url
        : url + (url.contains("?") ? "&" : "?") + "api_key=" + apiKey;
    int[] retryable = {429, 500, 502, 503, 504};
    IOException lastError = null;
    long backoffMs = INITIAL_BACKOFF_MS;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      CrossProcessRateLimiter.acquire(URI.create(url).getHost(), intervalMs);
      HttpURLConnection conn = (HttpURLConnection) URI.create(full).toURL().openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(60000);
      int status;
      try {
        NetworkOutageWait.connect(conn);
        status = conn.getResponseCode();
      } catch (IOException e) {
        lastError = e;
        status = -1;
      }
      if (status >= 200 && status < 300) {
        return conn.getInputStream();
      }
      boolean retry = status == -1;
      for (int code : retryable) {
        retry |= status == code;
      }
      if (!retry) {
        throw new IOException("govinfo HTTP " + status + " from " + url);
      }
      if (status != -1) {
        lastError = new IOException("govinfo HTTP " + status + " from " + url);
      }
      if (attempt < MAX_ATTEMPTS) {
        LOGGER.info("presidential_documents: retrying in {}ms (attempt {}/{}): HTTP {} {}",
            backoffMs, attempt + 1, MAX_ATTEMPTS, status, url);
        try {
          Thread.sleep(backoffMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted during retry wait for " + url, lastError);
        }
        backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
      }
    }
    throw lastError;
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while ((n = in.read(buf)) > 0) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }

  private static String queryParam(String url, String name) throws IOException {
    String query = URI.create(url).getRawQuery();
    for (String kv : query.split("&")) {
      if (kv.startsWith(name + "=")) {
        return URLDecoder.decode(kv.substring(name.length() + 1), "UTF-8");
      }
    }
    throw new IOException("presidential_documents: no " + name + " in " + url);
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    String s = v.asText();
    return s.trim().isEmpty() ? null : s;
  }
}
