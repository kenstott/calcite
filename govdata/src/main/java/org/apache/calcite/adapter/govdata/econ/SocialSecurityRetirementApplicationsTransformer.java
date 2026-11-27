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
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses the SSA open-data file "Social Security Retirement Insurance Applications Filed via the
 * Internet, FY 2012 onward" ({@code fy12-onward-rib-filed-via-internet.csv}) into one row per
 * month: total retirement insurance applications and the share filed on the internet. The file is
 * fetched through a headed browser (ssa.gov refuses plain HTTP).
 *
 * <p>SSA's fiscal year runs October to September and is labelled by the year it ends, so
 * {@code 2025,Oct} is October 2024. Any malformed row or an empty file fails the run.
 */
public class SocialSecurityRetirementApplicationsTransformer implements StreamingResponseTransformer {

  static final String FILE_URL = "https://www.ssa.gov/data/fy12-onward-rib-filed-via-internet.csv";

  private static final List<String> MONTHS = Arrays.asList(
      "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec");

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException("social_security_retirement_applications is a "
        + "StreamingResponseTransformer; call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context) {
    String csv;
    try {
      csv = SsaBrowserSession.get(context).fetchText(FILE_URL);
    } catch (IOException e) {
      throw new RuntimeException("social_security_retirement_applications: cannot read "
          + FILE_URL, e);
    }
    if (csv == null) {
      throw new IllegalStateException(
          "social_security_retirement_applications: " + FILE_URL + " answered 404");
    }
    return parse(csv).iterator();
  }

  static List<Map<String, Object>> parse(String csv) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    String[] lines = csv.split("\\r?\\n");
    for (int i = 1; i < lines.length; i++) {
      String line = lines[i].trim();
      if (line.isEmpty()) {
        continue;
      }
      List<String> f = fields(line);
      if (f.size() < 5) {
        throw new IllegalStateException("social_security_retirement_applications: line " + (i + 1)
            + " has " + f.size() + " fields, expected at least 5: " + line);
      }
      int fiscalYear = Integer.parseInt(f.get(0).trim());
      int month = MONTHS.indexOf(f.get(1).trim()) + 1;
      if (month == 0) {
        throw new IllegalStateException("social_security_retirement_applications: line "
            + (i + 1) + " has unknown month '" + f.get(1) + "'");
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("year", month >= 10 ? fiscalYear - 1 : fiscalYear);
      row.put("month", month);
      row.put("fiscal_year", fiscalYear);
      row.put("total_applications", Long.parseLong(f.get(2).trim().replace(",", "")));
      row.put("internet_applications", Long.parseLong(f.get(3).trim().replace(",", "")));
      row.put("pct_internet", Double.valueOf(f.get(4).trim()));
      rows.add(row);
    }
    if (rows.isEmpty()) {
      throw new IllegalStateException("social_security_retirement_applications: no data rows in "
          + FILE_URL + "; the file layout changed");
    }
    return rows;
  }

  /** Splits one CSV line, honouring double-quoted fields that contain commas. */
  private static List<String> fields(String line) {
    List<String> out = new ArrayList<String>();
    StringBuilder cur = new StringBuilder();
    boolean quoted = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (c == '"') {
        quoted = !quoted;
      } else if (c == ',' && !quoted) {
        out.add(cur.toString());
        cur.setLength(0);
      } else {
        cur.append(c);
      }
    }
    out.add(cur.toString());
    return out;
  }
}
