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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataProvider for {@code federal_funding_gaps}: CRS report RS20348 "Federal Funding Gaps: A
 * Brief Overview", Table 1 "Appropriations Funding Gaps Since FY1977" (one row per gap).
 *
 * <p>The fiscal-year cell is blank for the second and later gaps of a year and is carried
 * forward. Dates carry a two-digit year; the century is the one that puts the date in the
 * fiscal year or the calendar year before it. Each row's published full-day count is checked
 * against the dates ({@code terminated - final_budget_authority - 1}); a mismatch is a parse
 * error and aborts rather than loading a wrong number.
 */
public class FundingGapsTransformer implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(FundingGapsTransformer.class);
  private static final Pattern DATE = Pattern.compile("(\\d{1,2})/(\\d{1,2})/(\\d{2})\\s*$");

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    CrsReportTables report = CrsReportTables.load("RS20348");
    List<List<String>> body = report.table("Fiscal Year", "Final Date of Budget Authority",
        "Full Day(s) of Gap", "Date Gap Terminated");
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    int fy = 0;
    int seq = 0;
    for (List<String> r : body) {
      if (r.size() < 4) {
        throw new IOException("federal_funding_gaps: short row " + r);
      }
      if (!r.get(0).isEmpty()) {
        fy = Integer.parseInt(r.get(0));
        seq = 0;
      } else if (fy == 0) {
        throw new IOException("federal_funding_gaps: first row has no fiscal year: " + r);
      }
      seq++;
      LocalDate lastFunded = date(r.get(1), fy);
      LocalDate ended = date(r.get(3), fy);
      int days = Integer.parseInt(CrsReportTables.stripFootnote(r.get(2)));
      long computed = ChronoUnit.DAYS.between(lastFunded, ended) - 1;
      if (computed != days) {
        throw new IOException("federal_funding_gaps: FY" + fy + " gap " + seq + " published "
            + days + " full days but dates give " + computed + " (" + r + ")");
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("fiscal_year", fy);
      row.put("gap_seq_in_fiscal_year", seq);
      row.put("last_funded_date", lastFunded.toString());
      row.put("gap_start_date", lastFunded.plusDays(1).toString());
      row.put("gap_end_date", ended.toString());
      row.put("full_days", days);
      row.put("crs_report_version", report.version());
      rows.add(row);
    }
    if (rows.isEmpty()) {
      throw new IOException("federal_funding_gaps: RS20348 table had no rows");
    }
    LOGGER.info("federal_funding_gaps: {} gaps from {}", rows.size(), report.version());
    return rows.iterator();
  }

  private static LocalDate date(String cell, int fy) throws IOException {
    Matcher m = DATE.matcher(cell);
    if (!m.find()) {
      throw new IOException("federal_funding_gaps: no date in '" + cell + "'");
    }
    int yy = Integer.parseInt(m.group(3));
    int year = -1;
    for (int cand : new int[] {1900 + yy, 2000 + yy}) {
      if (cand == fy || cand == fy - 1) {
        year = cand;
      }
    }
    if (year < 0) {
      throw new IOException("federal_funding_gaps: date '" + cell + "' not near FY" + fy);
    }
    return LocalDate.of(year, Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
  }
}
