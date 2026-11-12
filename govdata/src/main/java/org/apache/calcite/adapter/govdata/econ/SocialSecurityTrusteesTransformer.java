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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads Table II.D1 ("Projected Maximum Trust Fund Ratios During the Long-Range Period and Trust
 * Fund Reserve Depletion Dates") and the 75-year unfunded obligation sentence from section II.D of
 * each Social Security Trustees Report, {@code https://www.ssa.gov/oact/tr/<year>/II_D_project.html},
 * through a headed browser (ssa.gov refuses plain HTTP).
 *
 * <p>Reports 2016 to the latest are read; the table layout is the same in all of them (earlier
 * reports use a different layout and are not covered). The report for the current calendar year is
 * skipped when SSA has not published it yet (404); a 404 for any earlier year, or a page whose
 * table or unfunded-obligation sentence cannot be found, fails the run.
 */
public class SocialSecurityTrusteesTransformer implements StreamingResponseTransformer {

  static final int FIRST_REPORT_YEAR = 2016;
  static final String PAGE_URL = "https://www.ssa.gov/oact/tr/%d/II_D_project.html";

  private static final String[] FUNDS = {"OASI", "DI", "OASDI"};
  private static final String VALUE = "(\\d{4}|[a-z])";
  private static final Pattern MAX_RATIO = Pattern.compile(
      "Maximum projected trust fund ratio \\(percent\\)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s");
  private static final Pattern YEAR_ATTAINED = Pattern.compile(
      "Year attained\\s+(\\d{4})\\s+(\\d{4})\\s+(\\d{4})\\s");
  private static final Pattern DEPLETION = Pattern.compile(
      "Projected year of trust fund reserve depletion\\s+" + VALUE + "\\s+" + VALUE + "\\s+"
          + VALUE + "\\s");
  private static final Pattern UNFUNDED = Pattern.compile(
      "unfunded obligation of \\$([0-9]+(?:\\.[0-9]+)?)\\s+trillion");

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        "social_security_trustees_projections is a StreamingResponseTransformer; "
            + "call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context) {
    SsaBrowserSession session = SsaBrowserSession.get(context);
    int current = LocalDate.now(ZoneId.of("America/New_York")).getYear();
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int year = FIRST_REPORT_YEAR; year <= current; year++) {
      String url = String.format(PAGE_URL, year);
      String html;
      try {
        html = session.fetchText(url);
      } catch (IOException e) {
        throw new RuntimeException(
            "social_security_trustees_projections: cannot read " + url, e);
      }
      if (html == null) {
        if (year == current) {
          continue;
        }
        throw new IllegalStateException(
            "social_security_trustees_projections: " + url + " answered 404");
      }
      rows.addAll(parse(year, html));
    }
    if (rows.isEmpty()) {
      throw new IllegalStateException("social_security_trustees_projections: no report found");
    }
    return rows.iterator();
  }

  static List<Map<String, Object>> parse(int reportYear, String html) {
    String text = html.replaceAll("<[^>]+>", " ").replace("&nbsp;", " ").replace(' ', ' ')
        .replaceAll("\\s+", " ") + " ";
    String where = "social_security_trustees_projections " + reportYear + ": ";
    Matcher ratio = require(MAX_RATIO, text, where + "Table II.D1 maximum trust fund ratio row");
    Matcher attained = require(YEAR_ATTAINED, text, where + "Table II.D1 year attained row");
    Matcher depletion = require(DEPLETION, text, where + "Table II.D1 depletion year row");
    Matcher unfunded = require(UNFUNDED, text, where + "75-year unfunded obligation sentence");
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < FUNDS.length; i++) {
      String dep = depletion.group(i + 1);
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("report_year", reportYear);
      row.put("fund", FUNDS[i]);
      // A letter in the depletion row is SSA's footnote marker: reserves stay positive through the
      // end of the 75-year period, so there is no depletion year.
      row.put("depletion_year", Character.isDigit(dep.charAt(0)) ? Integer.valueOf(dep) : null);
      row.put("max_trust_fund_ratio_pct", Integer.valueOf(ratio.group(i + 1)));
      row.put("max_ratio_year", Integer.valueOf(attained.group(i + 1)));
      row.put("unfunded_obligation_trillions",
          i == FUNDS.length - 1 ? Double.valueOf(unfunded.group(1)) : null);
      rows.add(row);
    }
    return rows;
  }

  private static Matcher require(Pattern p, String text, String what) {
    Matcher m = p.matcher(text);
    if (!m.find()) {
      throw new IllegalStateException(what + " not found; the page layout changed");
    }
    return m;
  }
}
