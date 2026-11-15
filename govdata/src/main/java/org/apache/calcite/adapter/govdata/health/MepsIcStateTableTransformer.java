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
import org.jsoup.select.Elements;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns one AHRQ MEPS Insurance Component state table page (one survey year, one table id such
 * as {@code tiia2}) into {@code health.meps_ic_employer_coverage} rows: one row per geography
 * (United States or a state) and firm-size column.
 *
 * <p>Each page carries two {@code dataTable} blocks: the estimates and, below them, the
 * standard errors of the same cells. They are joined on geography and column. A cell AHRQ
 * prints as {@code --} (suppressed for high standard error) loads as NULL; a trailing
 * {@code *} (does not meet the standard of reliability or precision) loads as the value with
 * {@code low_reliability} true. Any other non-numeric cell fails the batch.
 *
 * <p>Only the layout used by the 2008-2020 pages is parsed (an estimate table followed by a
 * standard-error table, each a header row plus one row per geography). The 1996-2006 pages use
 * other layouts and 2007 has no state tables; none of those years is requested.
 */
public class MepsIcStateTableTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MepsIcStateTableTransformer.class);

  private static final String SE_MARKER = "Standard error";
  private static final String US = "United States";

  /** Table id to {measure, unit}. */
  private static final Map<String, String[]> TABLES = new LinkedHashMap<String, String[]>();

  static {
    TABLES.put("tiia1", new String[] {"establishments", "count"});
    TABLES.put("tiia2", new String[] {"establishments_offering_pct", "percent"});
    TABLES.put("tiib1", new String[] {"employees", "count"});
    TABLES.put("tiib2", new String[] {"employees_in_offering_establishments_pct", "percent"});
    TABLES.put("tiib3", new String[] {"full_time_employees", "count"});
    TABLES.put("tiic1", new String[] {"single_premium", "dollars"});
    TABLES.put("tiic2", new String[] {"single_employee_contribution", "dollars"});
    TABLES.put("tiic3", new String[] {"single_employee_share_of_premium_pct", "percent"});
    TABLES.put("tiid1", new String[] {"family_premium", "dollars"});
    TABLES.put("tiid2", new String[] {"family_employee_contribution", "dollars"});
    TABLES.put("tiid3", new String[] {"family_employee_share_of_premium_pct", "percent"});
    TABLES.put("tiie1", new String[] {"employee_plus_one_premium", "dollars"});
    TABLES.put("tiie2", new String[] {"employee_plus_one_employee_contribution", "dollars"});
    TABLES.put("tiie3", new String[] {"employee_plus_one_employee_share_of_premium_pct",
        "percent"});
    TABLES.put("tiif1", new String[] {"enrolled_with_deductible_pct", "percent"});
    TABLES.put("tiif2", new String[] {"single_deductible", "dollars"});
  }

  private static final Map<String, String> STATE_ABBR = buildStateAbbr();

  private static final HttpClient CLIENT = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(30))
      .followRedirects(HttpClient.Redirect.NORMAL)
      .build();

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String yearText = context.getDimensionValues().get("year");
    String table = context.getDimensionValues().get("table_id");
    if (yearText == null || table == null) {
      throw new IllegalStateException("MEPS-IC: year and table_id dimensions are required");
    }
    int year = Integer.parseInt(yearText.trim());
    String[] meta = TABLES.get(table);
    if (meta == null) {
      throw new IllegalStateException("MEPS-IC: unknown table_id '" + table + "'");
    }
    String url = context.getUrl();
    String html = download(url);
    List<Map<String, Object>> rows = parse(html, year, table, meta[0], meta[1], url);
    LOGGER.debug("MEPS-IC {}: {} rows", url, rows.size());
    return rows.iterator();
  }

  private static String download(String url) throws IOException {
    HttpRequest req = HttpRequest.newBuilder(URI.create(url))
        .timeout(Duration.ofSeconds(60))
        .GET()
        .build();
    try {
      HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString(
          java.nio.charset.StandardCharsets.ISO_8859_1));
      if (resp.statusCode() != 200) {
        throw new IOException("MEPS-IC: HTTP " + resp.statusCode() + " for " + url);
      }
      return resp.body();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("MEPS-IC: interrupted fetching " + url, e);
    }
  }

  /** One parsed cell: value text interpreted. */
  static final class Cell {
    final Double value;
    final boolean lowReliability;

    Cell(Double value, boolean lowReliability) {
      this.value = value;
      this.lowReliability = lowReliability;
    }
  }

  /** Parses a page; package-private for tests. */
  static List<Map<String, Object>> parse(String html, int year, String table, String measure,
      String unit, String url) {
    Document doc = Jsoup.parse(html);
    Elements tables = doc.select("table");
    if (tables.size() != 2) {
      throw new IllegalStateException(
          "MEPS-IC: expected 2 data tables (estimates, standard errors) but found "
              + tables.size() + " in " + url);
    }
    Element est = null;
    Element se = null;
    for (Element t : tables) {
      String caption = title(t);
      if (caption.contains(SE_MARKER)) {
        se = t;
      } else {
        est = t;
      }
    }
    if (est == null || se == null) {
      throw new IllegalStateException("MEPS-IC: no estimate/standard-error pair in " + url);
    }
    List<String> sizes = headers(est, url);
    if (!sizes.equals(headers(se, url))) {
      throw new IllegalStateException("MEPS-IC: estimate and standard-error columns differ in "
          + url);
    }
    Map<String, Cell[]> seCells = new HashMap<String, Cell[]>();
    Map<String, String> seDivision = new HashMap<String, String>();
    readBody(se, sizes.size(), seCells, seDivision, url);
    Map<String, Cell[]> estCells = new LinkedHashMap<String, Cell[]>();
    Map<String, String> division = new HashMap<String, String>();
    readBody(est, sizes.size(), estCells, division, url);
    if (!estCells.containsKey(US)) {
      throw new IllegalStateException("MEPS-IC: no United States row in " + url);
    }
    if (!estCells.keySet().equals(seCells.keySet())) {
      throw new IllegalStateException("MEPS-IC: estimate and standard-error rows differ in "
          + url);
    }
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    for (Map.Entry<String, Cell[]> e : estCells.entrySet()) {
      String geo = e.getKey();
      boolean national = US.equals(geo);
      String abbr = null;
      if (!national) {
        abbr = STATE_ABBR.get(geo);
        if (abbr == null) {
          throw new IllegalStateException("MEPS-IC: unknown state '" + geo + "' in " + url);
        }
      }
      Cell[] se1 = seCells.get(geo);
      for (int i = 0; i < sizes.size(); i++) {
        Cell c = e.getValue()[i];
        Map<String, Object> r = new LinkedHashMap<String, Object>();
        r.put("survey_year", year);
        r.put("source_table", table);
        r.put("measure", measure);
        r.put("unit", unit);
        r.put("geo_level", national ? "national" : "state");
        r.put("state_name", national ? null : geo);
        r.put("state_abbr", abbr);
        r.put("census_division", national ? null : division.get(geo));
        r.put("firm_size", sizes.get(i));
        r.put("estimate", c.value);
        r.put("std_error", se1[i].value);
        r.put("low_reliability", c.lowReliability);
        out.add(r);
      }
    }
    return out;
  }

  /** Table caption (1996-98 and 2008+ pages) or the first header row (1999-2006 pages). */
  private static String title(Element t) {
    Element cap = t.selectFirst("caption");
    if (cap != null) {
      return cap.text();
    }
    Element first = t.selectFirst("tr");
    return first == null ? "" : first.text();
  }

  /** Non-blank cell texts of a row; the 1999-2006 pages interleave empty spacer cells. */
  private static List<String> texts(Element tr) {
    List<String> out = new ArrayList<String>();
    for (Element c : tr.select("th, td")) {
      String v = c.text().replace('\u00a0', ' ').trim();
      if (!v.isEmpty()) {
        out.add(v);
      }
    }
    return out;
  }

  private static List<String> headers(Element t, String url) {
    List<String> out = null;
    for (Element tr : t.select("thead tr")) {
      List<String> v = texts(tr);
      if (!v.isEmpty() && v.get(0).startsWith("Division and State")) {
        out = new ArrayList<String>(v.subList(1, v.size()));
      }
    }
    if (out == null || out.size() != 8) {
      throw new IllegalStateException("MEPS-IC: expected 8 firm-size columns, found " + out
          + " in " + url);
    }
    return out;
  }

  private static void readBody(Element t, int width, Map<String, Cell[]> cells,
      Map<String, String> division, String url) {
    String currentDivision = null;
    for (Element tr : t.select("tbody tr")) {
      List<String> v = texts(tr);
      if (v.isEmpty()) {
        continue;
      }
      String label = v.get(0);
      if (v.size() == 1) {
        if (!label.endsWith(":")) {
          throw new IllegalStateException("MEPS-IC: group row '" + label + "' in " + url);
        }
        currentDivision = label.substring(0, label.length() - 1).trim();
        continue;
      }
      if (v.size() - 1 != width) {
        throw new IllegalStateException("MEPS-IC: row '" + label + "' has " + (v.size() - 1)
            + " cells, expected " + width + " in " + url);
      }
      Cell[] row = new Cell[width];
      for (int i = 0; i < width; i++) {
        row[i] = cell(v.get(i + 1), label, url);
      }
      if (cells.put(label, row) != null) {
        throw new IllegalStateException("MEPS-IC: duplicate row '" + label + "' in " + url);
      }
      division.put(label, US.equals(label) ? null : currentDivision);
    }
  }

  /** Parses "51.1%", "$5,234", "1,234", "--" and a trailing "*" low-reliability flag. */
  static Cell cell(String raw, String row, String url) {
    String s = raw.replace('\u00a0', ' ').trim();
    if (s.equals("--")) {
      return new Cell(null, false);
    }
    boolean low = s.endsWith("*");
    if (low) {
      s = s.substring(0, s.length() - 1).trim();
    }
    s = s.replace("%", "").replace("$", "").replace(",", "").trim();
    try {
      return new Cell(Double.valueOf(s), low);
    } catch (NumberFormatException e) {
      throw new IllegalStateException("MEPS-IC: non-numeric cell '" + raw + "' in row '" + row
          + "' of " + url, e);
    }
  }

  private static Map<String, String> buildStateAbbr() {
    String[] p = {
        "Alabama", "AL", "Alaska", "AK", "Arizona", "AZ", "Arkansas", "AR", "California", "CA",
        "Colorado", "CO", "Connecticut", "CT", "Delaware", "DE", "District of Columbia", "DC",
        "Florida", "FL", "Georgia", "GA", "Hawaii", "HI", "Idaho", "ID", "Illinois", "IL",
        "Indiana", "IN", "Iowa", "IA", "Kansas", "KS", "Kentucky", "KY", "Louisiana", "LA",
        "Maine", "ME", "Maryland", "MD", "Massachusetts", "MA", "Michigan", "MI",
        "Minnesota", "MN", "Mississippi", "MS", "Missouri", "MO", "Montana", "MT",
        "Nebraska", "NE", "Nevada", "NV", "New Hampshire", "NH", "New Jersey", "NJ",
        "New Mexico", "NM", "New York", "NY", "North Carolina", "NC", "North Dakota", "ND",
        "Ohio", "OH", "Oklahoma", "OK", "Oregon", "OR", "Pennsylvania", "PA",
        "Rhode Island", "RI", "South Carolina", "SC", "South Dakota", "SD", "Tennessee", "TN",
        "Texas", "TX", "Utah", "UT", "Vermont", "VT", "Virginia", "VA", "Washington", "WA",
        "West Virginia", "WV", "Wisconsin", "WI", "Wyoming", "WY"};
    Map<String, String> m = new HashMap<String, String>();
    for (int i = 0; i < p.length; i += 2) {
      m.put(p[i], p[i + 1]);
    }
    return m;
  }
}
