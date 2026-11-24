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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.etl.RetryableHttp;
import org.apache.calcite.adapter.file.etl.SharedHttpClient;
import org.apache.calcite.adapter.file.etl.VariableResolver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Continuing-appropriations acts enacted after the newest CRS R46595 edition, read from the
 * Congress.gov API. CRS revises R46595 only occasionally, so acts enacted since the last
 * edition would otherwise be missing. An act qualifies when it is a public law of the Congress
 * in which the CRS table ends (or a later one) whose title contains "Continuing
 * Appropriations", enacted after the last act in the CRS table.
 *
 * <p>Congress.gov does not publish CRS's duration figure or the Statutes at Large citation, so
 * those columns are null for these rows. The expiration date is read from the act's
 * Congress.gov summary ("... through [the earlier of] January 30, 2026 ..."); when the summary
 * states none, expiration_date is null and source_cell_note says so.
 */
final class ContinuingResolutionsSupplement {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String API = "https://api.congress.gov/v3";
  private static final Pattern TITLE_YEAR = Pattern.compile("(\\d{4})\\.?\\s*$");
  private static final Pattern PL = Pattern.compile("(\\d+)-(\\d+)");
  private static final Pattern THROUGH =
      Pattern.compile("continuing[^.]*?\\bthrough (?:the earlier of )?"
          + "([A-Z][a-z]+ \\d{1,2}, \\d{4})", Pattern.CASE_INSENSITIVE);
  private static final DateTimeFormatter LONG_DATE =
      DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.US);

  private ContinuingResolutionsSupplement() {
  }

  /**
   * Returns one row per qualifying act, continuing the CRS numbering.
   *
   * @param lastPublicLaw public law of the last CRS row, e.g. "P.L. 119-4"
   * @param lastEnactment enactment date of the last CRS row
   * @param lastCumulative act_seq_cumulative of the last CRS row
   * @param actsPerFy acts already in the table per fiscal year
   */
  static List<Map<String, Object>> rows(HttpSourceConfig source, String lastPublicLaw,
      LocalDate lastEnactment, int lastCumulative, Map<Integer, Integer> actsPerFy,
      Map<String, String> variables) throws IOException {
    Map<String, String> headers = new LinkedHashMap<String, String>();
    headers.put("Accept", "application/json");
    if (source.getHeaders() != null) {
      for (Map.Entry<String, String> e : source.getHeaders().entrySet()) {
        String v = VariableResolver.substitute(e.getValue(), variables);
        if (v.contains("${")) {
          throw new IOException("continuing_resolutions: header " + e.getKey()
              + " has an unresolved variable (" + v + "); set the environment variable it names");
        }
        headers.put(e.getKey(), v);
      }
    }
    Matcher last = PL.matcher(lastPublicLaw);
    if (!last.find()) {
      throw new IOException("continuing_resolutions: cannot read congress from " + lastPublicLaw);
    }
    int firstCongress = Integer.parseInt(last.group(1));
    int lastLawNumber = Integer.parseInt(last.group(2));

    List<JsonNode> acts = new ArrayList<JsonNode>();
    int congress = firstCongress;
    while (true) {
      int found = 0;
      int offset = 0;
      while (true) {
        JsonNode page = get(API + "/law/" + congress + "/pub?limit=250&offset=" + offset
            + "&format=json", headers, source);
        JsonNode bills = page.get("bills");
        if (bills == null || bills.size() == 0) {
          break;
        }
        for (JsonNode b : bills) {
          found++;
          String title = b.path("title").asText("");
          if (!title.toLowerCase(Locale.ROOT).contains("continuing appropriations")) {
            continue;
          }
          Matcher pl = PL.matcher(b.path("laws").path(0).path("number").asText(""));
          if (!pl.find()) {
            throw new IOException("continuing_resolutions: no law number on " + b);
          }
          boolean after = Integer.parseInt(pl.group(1)) > firstCongress
              || Integer.parseInt(pl.group(2)) > lastLawNumber;
          if (after) {
            acts.add(b);
          }
        }
        offset += bills.size();
      }
      if (found == 0) {
        break;
      }
      congress++;
    }
    acts.sort((a, b) -> LocalDate.parse(a.path("latestAction").path("actionDate").asText())
        .compareTo(LocalDate.parse(b.path("latestAction").path("actionDate").asText())));

    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    int cumulative = lastCumulative;
    for (JsonNode b : acts) {
      LocalDate enacted = LocalDate.parse(b.path("latestAction").path("actionDate").asText());
      if (!enacted.isAfter(lastEnactment)) {
        continue;
      }
      String title = b.path("title").asText();
      Matcher ty = TITLE_YEAR.matcher(title);
      if (!ty.find()) {
        throw new IOException("continuing_resolutions: no fiscal year at end of title: " + title);
      }
      int fy = Integer.parseInt(ty.group(1));
      int seq = actsPerFy.merge(fy, 1, Integer::sum);
      cumulative++;
      String billPath = b.path("congress").asText() + "/" + b.path("type").asText().toLowerCase(
          Locale.ROOT) + "/" + b.path("number").asText();
      String law = b.path("laws").path(0).path("number").asText();
      LocalDate expiration = null;
      String note = null;
      String summary = publicLawSummary(billPath, headers, source);
      Matcher m = THROUGH.matcher(summary);
      if (m.find()) {
        expiration = LocalDate.parse(m.group(1), LONG_DATE);
      } else {
        note = "expiration not stated in the Congress.gov summary";
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("fiscal_year", fy);
      row.put("act_seq_in_fiscal_year", seq);
      row.put("act_seq_cumulative", cumulative);
      row.put("public_law", "P.L. " + law);
      row.put("statutes_at_large", null);
      row.put("enactment_date", enacted.toString());
      row.put("expiration_date", expiration == null ? null : expiration.toString());
      row.put("duration_days", null);
      row.put("expiration_date_second", null);
      row.put("duration_days_second", null);
      row.put("source_cell_note", (note == null ? "" : note + "; ")
          + "act enacted after the newest CRS R46595 edition; duration and Statutes at Large "
          + "citation not published by Congress.gov");
      row.put("crs_report_version", "congress.gov:v3/bill/" + billPath);
      out.add(row);
    }
    return out;
  }

  private static String publicLawSummary(String billPath, Map<String, String> headers,
      HttpSourceConfig source) throws IOException {
    JsonNode d = get(API + "/bill/" + billPath + "/summaries?format=json", headers, source);
    String text = null;
    for (JsonNode s : d.path("summaries")) {
      if ("Public Law".equals(s.path("actionDesc").asText())) {
        text = s.path("text").asText();
      }
    }
    if (text == null) {
      throw new IOException("continuing_resolutions: no Public Law summary for " + billPath);
    }
    return text.replaceAll("<[^>]+>", " ").replace("&nbsp;", " ").replaceAll("\\s+", " ");
  }

  private static JsonNode get(String url, Map<String, String> headers, HttpSourceConfig source)
      throws IOException {
    SharedHttpClient.Response conn =
        RetryableHttp.openWithRetry(url, headers, source.getRateLimit(), true);
    try (InputStream in = conn.getInputStream()) {
      return MAPPER.readTree(in);
    } finally {
      conn.disconnect();
    }
  }
}
