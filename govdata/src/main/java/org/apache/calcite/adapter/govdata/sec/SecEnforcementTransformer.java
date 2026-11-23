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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the SEC Division of Enforcement listing pages: litigation releases (civil actions in
 * federal court) and administrative proceedings (Commission orders).
 *
 * <p>The source URL is the listing's first page ({@code ?page=0}); the response carries that
 * page and the pager, whose last link gives the page count. The remaining pages are downloaded
 * here through {@link EdgarRateLimiter}, the host-wide sec.gov limiter (shared pacing with the
 * filing workers, the 403 cool-down, 429 retry and SEC's fair-access User-Agent).
 */
public class SecEnforcementTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String SITE = "https://www.sec.gov";
  private static final Pattern PAGE_PARAM = Pattern.compile("[?&]page=(\\d+)");

  @Override public String transform(String response, RequestContext context) {
    String listing = context.getDimensionValues().get("listing");
    if (listing == null) {
      throw new IllegalStateException("sec enforcement_actions: no 'listing' dimension in request");
    }
    String actionType;
    if ("litigation-releases".equals(listing)) {
      actionType = "civil";
    } else if ("administrative-proceedings".equals(listing)) {
      actionType = "administrative";
    } else {
      throw new IllegalStateException("sec enforcement_actions: unknown listing '" + listing + "'");
    }
    String url = context.getUrl();
    if (!url.contains("page=0")) {
      throw new IllegalStateException("sec enforcement_actions: url must request page=0: " + url);
    }
    ArrayNode out = MAPPER.createArrayNode();
    try {
      int last = lastPage(response);
      parse(response, listing, actionType, out);
      for (int page = 1; page <= last; page++) {
        parse(download(url.replace("page=0", "page=" + page)), listing, actionType, out);
      }
    } catch (IOException e) {
      throw new RuntimeException("sec enforcement_actions: failed for " + listing, e);
    }
    if (out.size() == 0) {
      throw new IllegalStateException("sec enforcement_actions: no rows for " + listing);
    }
    return out.toString();
  }

  static int lastPage(String html) {
    int last = -1;
    for (Element a : Jsoup.parse(html).select("a[href*=page=]")) {
      Matcher m = PAGE_PARAM.matcher(a.attr("href"));
      if (m.find()) {
        last = Math.max(last, Integer.parseInt(m.group(1)));
      }
    }
    if (last < 0) {
      throw new IllegalStateException("sec enforcement_actions: no pager on listing page");
    }
    return last;
  }

  static void parse(String html, String listing, String actionType, ArrayNode out) {
    Document doc = Jsoup.parse(html, SITE);
    for (Element tr : doc.select("tr.pr-list-page-row")) {
      Element time = tr.selectFirst("time[datetime]");
      Element respondents = tr.selectFirst(".release-view__respondents");
      Element number = tr.selectFirst(".view-table_subfield_release_number .view-table_subfield_value");
      if (time == null || respondents == null || number == null) {
        throw new IllegalStateException("sec enforcement_actions: unrecognised row: " + tr.text());
      }
      String filed = OffsetDateTime.parse(time.attr("datetime")).withOffsetSameInstant(ZoneOffset.UTC)
          .toLocalDate().toString();
      int year = Integer.parseInt(filed.substring(0, 4));
      int month = Integer.parseInt(filed.substring(5, 7));
      Element link = respondents.selectFirst("a[href]");
      Element file = tr.selectFirst(".view-table_subfield_file_number .view-table_subfield_value");
      Element complaint = tr.selectFirst(".view-table_subfield_see_also a[href]");
      ObjectNode row = MAPPER.createObjectNode();
      row.put("listing", listing);
      row.put("action_type", actionType);
      row.put("release_number", number.text().trim());
      row.put("file_number", file == null ? null : file.text().trim());
      row.put("filed_date", filed);
      row.put("fiscal_year", month >= 10 ? year + 1 : year);
      row.put("respondents", respondents.text().trim());
      row.put("release_url", link == null ? null : link.absUrl("href"));
      row.put("complaint_url", complaint == null ? null : complaint.absUrl("href"));
      out.add(row);
    }
  }

  /** One page through the host-wide sec.gov limiter: shared pacing, cool-down, 429 retry, UA. */
  private static String download(String url) throws IOException {
    HttpURLConnection conn = EdgarRateLimiter.connect(url, "GET", 30000, 120000);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = conn.getInputStream()) {
      byte[] buf = new byte[8192];
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      int n;
      while ((n = is.read(buf)) > 0) {
        bos.write(buf, 0, n);
      }
      return new String(bos.toByteArray(), StandardCharsets.UTF_8);
    }
  }
}
