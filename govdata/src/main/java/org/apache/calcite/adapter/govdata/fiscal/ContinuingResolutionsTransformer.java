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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataProvider for {@code continuing_resolutions}: CRS report R46595 "Continuing Resolutions:
 * Overview of Components and Practices", Table A-1 (every continuing-appropriations act since
 * FY1977, one row per act).
 *
 * <p>The fiscal-year cell is blank after the first act of a year and is carried forward; a
 * year with no CR is printed as a single "[none]" row and produces no row here. The published
 * duration is kept as printed (a CR's duration is counted by CRS from the start of the period
 * it covers, which can precede the enactment date).
 */
public class ContinuingResolutionsTransformer implements DataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ContinuingResolutionsTransformer.class);
  private static final Pattern DATE_TOKEN = Pattern.compile("\\d{1,2}-\\d{1,2}-\\d{4}");
  private static final Pattern INT_TOKEN = Pattern.compile("\\d+");

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    CrsReportTables report = CrsReportTables.load("R46595");
    List<List<String>> body = report.table("Fiscal Year", "Number of Acts by Fiscal Year",
        "Number of Acts Cumulatively", "Public Law Number");
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    int fy = 0;
    int expectedCumulative = 0;
    for (List<String> r : body) {
      if (r.size() < 8) {
        throw new IOException("continuing_resolutions: short row " + r);
      }
      if (!r.get(0).isEmpty()) {
        fy = Integer.parseInt(r.get(0));
      } else if (fy == 0) {
        throw new IOException("continuing_resolutions: first row has no fiscal year: " + r);
      }
      if ("[none]".equals(r.get(1))) {
        continue;
      }
      int cumulative = Integer.parseInt(r.get(2));
      expectedCumulative++;
      if (cumulative != expectedCumulative) {
        throw new IOException("continuing_resolutions: cumulative count " + cumulative
            + " where " + expectedCumulative + " was expected in " + r);
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("fiscal_year", fy);
      row.put("act_seq_in_fiscal_year", Integer.parseInt(r.get(1)));
      row.put("act_seq_cumulative", cumulative);
      row.put("public_law", CrsReportTables.stripFootnote(r.get(3)));
      row.put("statutes_at_large", r.get(4));
      row.put("enactment_date", CrsReportTables.parseDate(r.get(5), "enactment").toString());
      putExpiration(row, r.get(6), r.get(7), r);
      row.put("crs_report_version", report.version());
      rows.add(row);
    }
    if (rows.isEmpty()) {
      throw new IOException("continuing_resolutions: R46595 Table A-1 had no rows");
    }
    int crsActs = rows.size();
    Map<String, Object> lastRow = rows.get(rows.size() - 1);
    Map<Integer, Integer> actsPerFy = new HashMap<Integer, Integer>();
    for (Map<String, Object> r : rows) {
      actsPerFy.merge((Integer) r.get("fiscal_year"), 1, Integer::sum);
    }
    rows.addAll(ContinuingResolutionsSupplement.rows(config.getSource(),
        (String) lastRow.get("public_law"),
        LocalDate.parse((String) lastRow.get("enactment_date")),
        (Integer) lastRow.get("act_seq_cumulative"), actsPerFy, variables));
    LOGGER.info("continuing_resolutions: {} acts after {} from Congress.gov",
        rows.size() - crsActs, report.version());
    LOGGER.info("continuing_resolutions: {} acts from {}", rows.size(), report.version());
    return rows.iterator();
  }

  /**
   * Fills expiration_date / duration_days (and the second pair for a "ladder" CR whose
   * activities expire on two dates), plus source_cell_note for anything CRS prints other than
   * a plain date and day count. CRS prints "[n/a]" where an act has no single expiration
   * (a revised full-year CR, or a CR enacted during a gap), "(N)" for durations it flags as
   * not comparable, and two dates and two counts for a ladder CR.
   */
  private static void putExpiration(Map<String, Object> row, String expCell, String durCell,
      List<String> rowCells) throws IOException {
    List<String> dates = all(DATE_TOKEN, expCell);
    List<String> durs = all(INT_TOKEN, durCell);
    boolean na = expCell.startsWith("[n/a]");
    if (dates.isEmpty() && !na) {
      throw new IOException("continuing_resolutions: unparseable expiration cell in " + rowCells);
    }
    if (dates.size() > 2 || durs.size() > 2 || (!na && durs.size() != dates.size())) {
      throw new IOException("continuing_resolutions: unexpected expiration/duration shape in "
          + rowCells);
    }
    row.put("expiration_date", dates.isEmpty() ? null
        : CrsReportTables.parseDate(dates.get(0), "expiration").toString());
    row.put("duration_days", durs.isEmpty() ? null : Integer.valueOf(durs.get(0)));
    row.put("expiration_date_second", dates.size() < 2 ? null
        : CrsReportTables.parseDate(dates.get(1), "second expiration").toString());
    row.put("duration_days_second", durs.size() < 2 ? null : Integer.valueOf(durs.get(1)));
    boolean clean = DATE_TOKEN.matcher(CrsReportTables.stripFootnote(expCell)).matches()
        && INT_TOKEN.matcher(CrsReportTables.stripFootnote(durCell)).matches();
    row.put("source_cell_note", clean ? null : "expiration=" + expCell + "; duration=" + durCell);
  }

  private static List<String> all(Pattern p, String s) {
    List<String> out = new ArrayList<String>();
    Matcher m = p.matcher(s);
    while (m.find()) {
      out.add(m.group());
    }
    return out;
  }
}
