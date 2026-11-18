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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads the Social Security Office of the Chief Actuary trust fund operations tables 4A1 (OASI),
 * 4A2 (DI) and 4A3 (OASDI combined), {@code https://www.ssa.gov/oact/STATS/table4a<n>.html},
 * through a headed browser (ssa.gov refuses plain HTTP).
 *
 * <p>Each page carries three tables: the summary (total income, total cost, net change, reserves
 * at end of year), the income components and the cost components, each with one row per calendar
 * year in millions of dollars. They are joined on the year into one row per fund and year. SSA
 * prints {@code &hellip;} where a component does not exist yet and {@code (c)} for "less than
 * $500,000"; both become null. A page whose tables, row counts or cells do not match the expected
 * layout fails the run.
 */
public class SocialSecurityTrustFundOperationsTransformer implements StreamingResponseTransformer {

  static final String PAGE_URL = "https://www.ssa.gov/oact/STATS/table4a%d.html";
  private static final String[] FUNDS = {"OASI", "DI", "OASDI"};
  private static final int[] FIRST_YEAR = {1937, 1957, 1957};

  private static final Pattern TABLE = Pattern.compile("<table.*?</table>", Pattern.DOTALL);
  private static final Pattern ROW = Pattern.compile("<tr[^>]*>(.*?)</tr>", Pattern.DOTALL);
  private static final Pattern CELL = Pattern.compile("<td[^>]*>(.*?)</td>", Pattern.DOTALL);
  private static final Pattern NUMBER = Pattern.compile("\\$?(-?)\\$?([0-9][0-9,]*)");

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        "social_security_trust_fund_operations is a StreamingResponseTransformer; "
            + "call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context) {
    SsaBrowserSession session = SsaBrowserSession.get(context);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < FUNDS.length; i++) {
      String url = String.format(PAGE_URL, i + 1);
      String html;
      try {
        html = session.fetchText(url);
      } catch (IOException e) {
        throw new RuntimeException(
            "social_security_trust_fund_operations: cannot read " + url, e);
      }
      if (html == null) {
        throw new IllegalStateException(
            "social_security_trust_fund_operations: " + url + " answered 404");
      }
      rows.addAll(parse(FUNDS[i], FIRST_YEAR[i], html));
    }
    return rows.iterator();
  }

  static List<Map<String, Object>> parse(String fund, int firstYear, String html) {
    String where = "social_security_trust_fund_operations " + fund + ": ";
    List<String> tables = new ArrayList<String>();
    Matcher t = TABLE.matcher(html);
    while (t.find()) {
      tables.add(t.group());
    }
    if (tables.size() != 3) {
      throw new IllegalStateException(where + "expected 3 data tables (summary, income, cost) but "
          + "found " + tables.size() + "; the page layout changed");
    }
    // Summary: income, cost, net change, reserves. Income: total, payroll, benefit taxation,
    // general fund, interest. Cost: total, benefits, administrative, railroad transfer.
    Map<Integer, Long[]> summary = readTable(where + "summary", tables.get(0), 4, firstYear);
    Map<Integer, Long[]> income = readTable(where + "income", tables.get(1), 5, firstYear);
    Map<Integer, Long[]> cost = readTable(where + "cost", tables.get(2), 4, firstYear);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (Map.Entry<Integer, Long[]> e : summary.entrySet()) {
      int year = e.getKey();
      Long[] s = e.getValue();
      Long[] inc = income.get(year);
      Long[] cst = cost.get(year);
      if (inc == null || cst == null) {
        throw new IllegalStateException(where + "year " + year
            + " is in the summary table but not in the income and cost tables");
      }
      if (!eq(s[0], inc[0]) || !eq(s[1], cst[0])) {
        throw new IllegalStateException(where + "year " + year + " total income/cost differ "
            + "between the summary table and the income/cost tables");
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("fund", fund);
      row.put("calendar_year", year);
      row.put("total_income", s[0]);
      row.put("payroll_tax_income", inc[1]);
      row.put("benefit_taxation_income", inc[2]);
      row.put("general_fund_transfers", inc[3]);
      row.put("interest_income", inc[4]);
      row.put("total_cost", s[1]);
      row.put("benefit_payments", cst[1]);
      row.put("admin_expenses", cst[2]);
      row.put("railroad_transfers", cst[3]);
      row.put("net_change", s[2]);
      row.put("assets_end", s[3]);
      rows.add(row);
    }
    return rows;
  }

  private static boolean eq(Long a, Long b) {
    return a == null ? b == null : a.equals(b);
  }

  /** Returns year to the cells after the year, in column order; null where SSA prints no figure. */
  private static Map<Integer, Long[]> readTable(String what, String table, int columns,
      int firstYear) {
    Map<Integer, Long[]> out = new LinkedHashMap<Integer, Long[]>();
    Matcher r = ROW.matcher(table);
    while (r.find()) {
      List<String> cells = new ArrayList<String>();
      Matcher c = CELL.matcher(r.group(1));
      while (c.find()) {
        cells.add(c.group(1).replaceAll("<[^>]+>", " ").replaceAll("\\s+", " ").trim());
      }
      if (cells.isEmpty() || !cells.get(0).matches("\\d{4}")) {
        continue;
      }
      if (cells.size() != columns + 1) {
        throw new IllegalStateException(what + ": row " + cells.get(0) + " has " + cells.size()
            + " cells, expected " + (columns + 1));
      }
      Long[] values = new Long[columns];
      for (int i = 0; i < columns; i++) {
        values[i] = parseCell(what, cells.get(0), cells.get(i + 1));
      }
      if (out.put(Integer.valueOf(cells.get(0)), values) != null) {
        throw new IllegalStateException(what + ": year " + cells.get(0) + " appears twice");
      }
    }
    if (out.isEmpty() || out.keySet().iterator().next() != firstYear) {
      throw new IllegalStateException(what + ": expected rows starting at " + firstYear
          + " but found " + (out.isEmpty() ? "none" : "first year "
          + out.keySet().iterator().next()));
    }
    return out;
  }

  private static Long parseCell(String what, String year, String cell) {
    if (cell.equals("&hellip;") || cell.equals("(c)") || cell.equals("c")) {
      return null;
    }
    Matcher m = NUMBER.matcher(cell);
    if (!m.matches()) {
      throw new IllegalStateException(what + ": year " + year + " has unreadable cell '" + cell
          + "'");
    }
    long v = Long.parseLong(m.group(2).replace(",", ""));
    return m.group(1).isEmpty() ? v : -v;
  }
}
