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
 * Parses the Social Security Administration cost-of-living adjustment table
 * ({@code https://www.ssa.gov/oact/cola/colaseries.html}) into one row per COLA year, 1975 to the
 * latest determination. The page is fetched through a headed browser (ssa.gov refuses plain HTTP).
 *
 * <p>SSA's {@code Year} is the year the increase takes effect: the 1975-1982 COLAs were effective
 * with benefits payable for June, later ones for December. A year with a footnote marker (1999)
 * carries the footnote text in {@code footnote}. A page that yields no rows, or a row whose
 * values do not parse, fails the run.
 */
public class SocialSecurityColaTransformer implements StreamingResponseTransformer {

  static final String PAGE_URL = "https://www.ssa.gov/oact/cola/colaseries.html";

  private static final Pattern ROW = Pattern.compile(
      "<tr>\\s*<td align=\"left\">\\s*(\\d{4})(.*?)</td>\\s*<td align=\"right\">\\s*([-0-9.]+)\\s*</td>\\s*</tr>",
      Pattern.DOTALL);
  private static final Pattern FOOTNOTE_A = Pattern.compile(
      "The COLA for December 1999 was.*?now 2\\.5 percent\\.", Pattern.DOTALL);

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        "social_security_cola is a StreamingResponseTransformer; call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context) {
    String html;
    try {
      html = SsaBrowserSession.get(context).fetchText(PAGE_URL);
    } catch (IOException e) {
      throw new RuntimeException("social_security_cola: cannot read " + PAGE_URL, e);
    }
    if (html == null) {
      throw new IllegalStateException("social_security_cola: " + PAGE_URL + " answered 404");
    }
    return parse(html).iterator();
  }

  static List<Map<String, Object>> parse(String html) {
    String footnote = null;
    Matcher fn = FOOTNOTE_A.matcher(html);
    if (fn.find()) {
      footnote = fn.group().replaceAll("<[^>]+>", " ").replaceAll("\\s+", " ").trim();
    }
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Matcher m = ROW.matcher(html);
    while (m.find()) {
      int year = Integer.parseInt(m.group(1));
      boolean marked = m.group(2).contains("sup");
      if (marked && footnote == null) {
        throw new IllegalStateException("social_security_cola: year " + year
            + " carries a footnote marker but its footnote text was not found");
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("year", year);
      row.put("cola_pct", Double.valueOf(m.group(3)));
      row.put("effective_month", year <= 1982 ? 6 : 12);
      row.put("footnote", marked ? footnote : null);
      rows.add(row);
    }
    if (rows.isEmpty()) {
      throw new IllegalStateException("social_security_cola: no COLA rows found in " + PAGE_URL
          + "; the page layout changed");
    }
    return rows;
  }
}
