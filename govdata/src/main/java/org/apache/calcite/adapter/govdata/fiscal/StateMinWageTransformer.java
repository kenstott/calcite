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

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataProvider for {@code state_minimum_wage_history} — DOL Wage and Hour Division's
 * published "Changes in Basic Minimum Wages in Non-Farm Employment Under State Law"
 * table (selected years, 1968 to present).
 *
 * <p>Live URL confirmed via the Wayback Machine's CDX index (crawled through at least
 * January 2025, most recently at capture timestamp 20250114103314): {@code
 * https://www.dol.gov/agencies/whd/state/minimum-wage/history}. dol.gov 403s every
 * direct fetch attempted from this build environment — even the bare domain root, which
 * points to a domain-wide Akamai edge block rather than a per-path bot rule — the same
 * failure mode already solved in this schema for {@code ssa_benefits_by_geography}
 * ({@link SsaBenefitsProvider}). This provider follows that precedent: it never
 * contacts dol.gov directly, fetching the page instead via
 * {@link FiscalHttp#fetchViaWayback}.
 *
 * <p>Page structure (verified against the 2025-01-14 Wayback capture): six {@code
 * <table class="minwage">} elements, one per "selected years" span (1968-1981,
 * 1988-1998, 2000-2006, 2007-2013, 2014-2019, 2020-2024 as of this capture — DOL
 * appends a new span/columns roughly annually, so the exact set of tables/years grows
 * over time). Each table has an identical 55-row body, same order in every table:
 * "Federal (FLSA)" first, then the 50 states alphabetically, then District of Columbia,
 * Guam, Puerto Rico, and U.S. Virgin Islands. A cell wrapped in {@code <strong>}
 * indicates (per the page's own key) an increase over the rate shown in the previous
 * published year column.
 *
 * <p>Cell text is inconsistent in ways that are DOL's own source data, not a scraping
 * artifact: {@code "..."} (no separate jurisdiction law that year), {@code "N.A."} /
 * {@code "NA"} (not available), a plain or {@code "$"}-prefixed number, a number with a
 * trailing footnote code in {@code []} or {@code ()}, a "low - high" range (hyphen or en
 * dash), a {@code "rate1 & rate2"} or {@code "rate1/rate2"} dual-track value (the
 * pre-1978 multi-track FLSA system, or a state's two-tier law), a non-hourly unit suffix
 * ({@code "/day"}, {@code "/wk"}), and at least one literal source typo ({@code
 * "4..65(g,,j)"}). {@link #classify} preserves every cell verbatim in {@code
 * raw_wage_text} and populates the numeric {@code wage_amount} only for the unambiguous
 * single-value case — never a guessed or truncated number for anything else.
 */
public class StateMinWageTransformer implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(StateMinWageTransformer.class);

  private static final String DOL_URL =
      "https://www.dol.gov/agencies/whd/state/minimum-wage/history";

  private static final Pattern YEAR_HEADER = Pattern.compile("(\\d{4})");

  // Single unambiguous value: optional "$", a number, an optional trailing footnote
  // code in [] or () (letters/digits/commas/spaces), nothing else.
  private static final Pattern SINGLE_VALUE =
      Pattern.compile("^\\$?(\\d+\\.?\\d*|\\.\\d+)\\s*(?:[\\[(]([^\\])]*)[\\])])?$");

  // Range: two numbers separated by a hyphen or en dash (–), with anything after
  // (a footnote code, a "/wk" unit suffix, etc.) accepted but not further parsed.
  private static final Pattern RANGE_VALUE =
      Pattern.compile("^\\$?(?:\\d+\\.?\\d*|\\.\\d+)\\s*[-\\u2013]\\s*\\$?(?:\\d+\\.?\\d*|\\.\\d+).*$");

  // Dual-track: two plain numbers separated by a slash and nothing else (no unit word
  // like "day"/"wk" on either side, which would indicate a unit suffix, not a second rate).
  private static final Pattern SLASH_DUAL_VALUE =
      Pattern.compile("^\\$?(?:\\d+\\.?\\d*|\\.\\d+)\\s*/\\s*\\$?(?:\\d+\\.?\\d*|\\.\\d+)$");

  private static final Map<String, String> STATE_NAME_TO_FIPS = buildStateFipsMap();

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    byte[] bytes = FiscalHttp.fetchViaWayback(DOL_URL);
    String html = new String(bytes, StandardCharsets.UTF_8);

    Document doc = Jsoup.parse(html);
    Elements tables = doc.select("table.minwage");
    if (tables.isEmpty()) {
      throw new IOException("state_minimum_wage_history: no table.minwage found in "
          + "Wayback capture of " + DOL_URL);
    }

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (Element table : tables) {
      parseTable(table, rows);
    }
    LOGGER.info("state_minimum_wage_history: parsed {} rows from {} table(s)",
        rows.size(), tables.size());
    return rows.iterator();
  }

  private void parseTable(Element table, List<Map<String, Object>> rows) {
    Element thead = table.selectFirst("thead");
    Element tbody = table.selectFirst("tbody");
    if (thead == null || tbody == null) {
      LOGGER.warn("state_minimum_wage_history: table missing thead/tbody, skipping");
      return;
    }
    Element headerRow = thead.select("tr").first();
    Elements headerCells = headerRow != null ? headerRow.select("th") : new Elements();
    if (headerCells.isEmpty()) {
      LOGGER.warn("state_minimum_wage_history: table has no header cells, skipping");
      return;
    }
    // headerCells[0] is the "State or other jurisdiction" label; the rest are years.
    List<Integer> years = new ArrayList<Integer>();
    for (int i = 1; i < headerCells.size(); i++) {
      Matcher m = YEAR_HEADER.matcher(headerCells.get(i).text());
      years.add(m.find() ? Integer.valueOf(m.group(1)) : null);
    }

    for (Element tr : tbody.select("tr")) {
      Elements tds = tr.select("td");
      if (tds.isEmpty()) {
        continue;
      }
      String jurisdictionName = tds.get(0).text().trim();
      if (jurisdictionName.isEmpty()) {
        continue;
      }
      String jurisdictionType = classifyJurisdiction(jurisdictionName);
      String stateFips = STATE_NAME_TO_FIPS.get(jurisdictionName);

      for (int i = 1; i < tds.size() && i - 1 < years.size(); i++) {
        Integer year = years.get(i - 1);
        if (year == null) {
          continue;
        }
        Element cell = tds.get(i);
        String rawText = cell.text().trim();
        if (rawText.isEmpty()) {
          continue;
        }
        boolean increased = !cell.select("strong").isEmpty();

        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("year", year);
        row.put("jurisdiction_name", jurisdictionName);
        row.put("jurisdiction_type", jurisdictionType);
        row.put("state_fips", stateFips);
        row.put("raw_wage_text", rawText);
        row.put("increased_from_prior_column", Boolean.valueOf(increased));
        classify(rawText, row);
        rows.add(row);
      }
    }
  }

  /** Classifies {@code rawText} and adds value_type / wage_amount / footnote_codes to row. */
  private void classify(String rawText, Map<String, Object> row) {
    String valueType;
    Double wageAmount = null;
    String footnoteCodes = null;

    if ("...".equals(rawText)) {
      valueType = "NOT_APPLICABLE";
    } else if ("N.A.".equalsIgnoreCase(rawText) || "NA".equalsIgnoreCase(rawText)) {
      valueType = "NOT_AVAILABLE";
    } else if (rawText.indexOf('&') >= 0) {
      valueType = "DUAL_TRACK";
    } else if (RANGE_VALUE.matcher(rawText).matches()) {
      valueType = "RANGE";
    } else if (SLASH_DUAL_VALUE.matcher(rawText).matches()) {
      valueType = "DUAL_TRACK";
    } else {
      Matcher m = SINGLE_VALUE.matcher(rawText);
      if (m.matches()) {
        valueType = "SINGLE";
        wageAmount = Double.valueOf(Double.parseDouble(m.group(1)));
        String fn = m.group(2);
        if (fn != null) {
          fn = fn.trim();
          footnoteCodes = fn.isEmpty() ? null : fn;
        }
      } else {
        valueType = "OTHER";
      }
    }

    row.put("value_type", valueType);
    row.put("wage_amount", wageAmount);
    row.put("footnote_codes", footnoteCodes);
  }

  private String classifyJurisdiction(String name) {
    if ("Federal (FLSA)".equals(name)) {
      return "FEDERAL";
    }
    if ("District of Columbia".equals(name)) {
      return "DISTRICT";
    }
    if ("Guam".equals(name) || "Puerto Rico".equals(name) || "U.S. Virgin Islands".equals(name)) {
      return "TERRITORY";
    }
    return "STATE";
  }

  private static Map<String, String> buildStateFipsMap() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("Alabama", "01"); m.put("Alaska", "02"); m.put("Arizona", "04");
    m.put("Arkansas", "05"); m.put("California", "06"); m.put("Colorado", "08");
    m.put("Connecticut", "09"); m.put("Delaware", "10"); m.put("District of Columbia", "11");
    m.put("Florida", "12"); m.put("Georgia", "13"); m.put("Hawaii", "15");
    m.put("Idaho", "16"); m.put("Illinois", "17"); m.put("Indiana", "18");
    m.put("Iowa", "19"); m.put("Kansas", "20"); m.put("Kentucky", "21");
    m.put("Louisiana", "22"); m.put("Maine", "23"); m.put("Maryland", "24");
    m.put("Massachusetts", "25"); m.put("Michigan", "26"); m.put("Minnesota", "27");
    m.put("Mississippi", "28"); m.put("Missouri", "29"); m.put("Montana", "30");
    m.put("Nebraska", "31"); m.put("Nevada", "32"); m.put("New Hampshire", "33");
    m.put("New Jersey", "34"); m.put("New Mexico", "35"); m.put("New York", "36");
    m.put("North Carolina", "37"); m.put("North Dakota", "38"); m.put("Ohio", "39");
    m.put("Oklahoma", "40"); m.put("Oregon", "41"); m.put("Pennsylvania", "42");
    m.put("Rhode Island", "44"); m.put("South Carolina", "45"); m.put("South Dakota", "46");
    m.put("Tennessee", "47"); m.put("Texas", "48"); m.put("Utah", "49");
    m.put("Vermont", "50"); m.put("Virginia", "51"); m.put("Washington", "53");
    m.put("West Virginia", "54"); m.put("Wisconsin", "55"); m.put("Wyoming", "56");
    return m;
  }
}
