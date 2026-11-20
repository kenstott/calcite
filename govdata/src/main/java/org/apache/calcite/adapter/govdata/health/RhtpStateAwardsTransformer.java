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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns the state award table of a CMS Rural Health Transformation Program announcement into
 * {@code health.rhtp_state_awards} rows: one row per state per fiscal year.
 *
 * <p>The fiscal_year dimension selects the CMS press release that carries that year's table.
 * A fiscal year with no entry in {@link #RELEASES} fails the batch. A year is added here when
 * CMS announces it. The page must carry exactly 50 states and every amount must be a dollar
 * figure; anything else fails the batch.
 */
public class RhtpStateAwardsTransformer implements StreamingResponseTransformer {

  private static final String BASE = "https://www.cms.gov/newsroom/press-releases/";

  /** Fiscal year to {press release URL, announcement date}. */
  private static final Map<String, String[]> RELEASES = new LinkedHashMap<String, String[]>();

  static {
    RELEASES.put("2026", new String[] {
        BASE + "cms-announces-50-billion-awards-strengthen-rural-health-all-50-states",
        "2025-12-29"});
    RELEASES.put("2027", new String[] {
        BASE + "trump-administration-announces-10-billion-second-year-rural-health-"
            + "transformation-awards-all-50",
        "2026-11-09"});
  }

  private static final HttpClient CLIENT = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(30))
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build();

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String fy = context.getDimensionValues().get("fiscal_year");
    if (fy == null) {
      throw new IllegalStateException("RHTP: fiscal_year dimension is required");
    }
    String[] rel = RELEASES.get(fy.trim());
    if (rel == null) {
      throw new IllegalStateException("RHTP: no CMS release registered for fiscal_year " + fy);
    }
    return parse(download(rel[0]), Integer.parseInt(fy.trim()), rel[0], rel[1]).iterator();
  }

  private static String download(String url) throws IOException {
    HttpRequest req = HttpRequest.newBuilder(URI.create(url))
        .timeout(Duration.ofSeconds(60))
        .header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            + "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
        .GET()
        .build();
    try {
      HttpResponse<String> resp = CLIENT.send(req,
          HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      if (resp.statusCode() != 200) {
        throw new IOException("RHTP: HTTP " + resp.statusCode() + " for " + url);
      }
      return resp.body();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("RHTP: interrupted fetching " + url, e);
    }
  }

  /** Parses a release page; package-private for tests. */
  static List<Map<String, Object>> parse(String html, int fy, String url, String announced) {
    Document doc = Jsoup.parse(html);
    Element table = null;
    for (Element t : doc.select("table")) {
      Element first = t.selectFirst("tr");
      String head = first == null ? "" : first.text();
      if (head.startsWith("State") && head.contains("Award")) {
        table = t;
        break;
      }
    }
    if (table == null) {
      throw new IllegalStateException("RHTP: no State/Award table in " + url);
    }
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    boolean header = true;
    for (Element tr : table.select("tr")) {
      if (header) {
        header = false;
        continue;
      }
      List<String> cells = new ArrayList<String>();
      for (Element c : tr.select("th, td")) {
        cells.add(c.text().replace(' ', ' ').trim());
      }
      if (cells.size() != 2) {
        throw new IllegalStateException("RHTP: row with " + cells.size() + " cells in " + url);
      }
      String abbr = HealthStateCodes.abbr(cells.get(0));
      if (abbr == null) {
        throw new IllegalStateException("RHTP: unknown state '" + cells.get(0) + "' in " + url);
      }
      String amt = cells.get(1).replace("$", "").replace(",", "").trim();
      long award;
      try {
        award = Long.parseLong(amt);
      } catch (NumberFormatException e) {
        throw new IllegalStateException("RHTP: non-numeric award '" + cells.get(1)
            + "' for " + cells.get(0) + " in " + url, e);
      }
      Map<String, Object> r = new LinkedHashMap<String, Object>();
      r.put("fiscal_year", fy);
      r.put("state_name", cells.get(0));
      r.put("state_abbr", abbr);
      r.put("award_amount", award);
      r.put("announced_date", announced);
      r.put("source_url", url);
      out.add(r);
    }
    if (out.size() != 50) {
      throw new IllegalStateException("RHTP: expected 50 states, found " + out.size()
          + " in " + url);
    }
    return out;
  }
}
