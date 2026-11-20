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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * DataProvider for {@code appropriations_timeliness_by_fy}: one row per fiscal year from CRS
 * report R46595, joining its Table 1 (regular appropriations bills expected, bills enacted on
 * or before October 1, CRs enacted; FY1977 on) with its Table 2 (number of interim CRs, total
 * duration in days, final expiration date; FY1998 on, so null before).
 */
public class AppropriationsTimelinessTransformer implements DataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AppropriationsTimelinessTransformer.class);
  private static final Pattern YEAR = Pattern.compile("^\\d{4}$");

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    CrsReportTables report = CrsReportTables.load("R46595");
    List<List<String>> t1 = report.table("Fiscal Year", "Number of Regular Appropriations Bills",
        "Regular Appropriations Bills Enacted", "CRs Enacted");
    List<List<String>> t2 = report.table("Fiscal Year", "Number of Interim CRs",
        "Total Duration in Days", "Final Expiration Date");

    Map<Integer, List<String>> interim = new HashMap<Integer, List<String>>();
    for (List<String> r : t2) {
      if (!r.isEmpty() && YEAR.matcher(r.get(0)).matches()) {
        interim.put(Integer.valueOf(r.get(0)), r);
      }
    }

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (List<String> r : t1) {
      if (r.isEmpty() || !YEAR.matcher(r.get(0)).matches()) {
        continue;
      }
      if (r.size() < 4) {
        throw new IOException("appropriations_timeliness_by_fy: short row " + r);
      }
      int fy = Integer.parseInt(r.get(0));
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("fiscal_year", fy);
      row.put("regular_bills_count", Integer.parseInt(CrsReportTables.stripFootnote(r.get(1))));
      row.put("regular_bills_enacted_by_oct1",
          Integer.parseInt(CrsReportTables.stripFootnote(r.get(2))));
      row.put("continuing_resolutions_enacted", Integer.parseInt(CrsReportTables.stripFootnote(r.get(3))));
      List<String> i = interim.remove(Integer.valueOf(fy));
      if (i == null) {
        row.put("interim_crs", null);
        row.put("interim_cr_total_days", null);
        row.put("final_interim_expiration_date", null);
      } else {
        row.put("interim_crs", Integer.parseInt(CrsReportTables.stripFootnote(i.get(1))));
        row.put("interim_cr_total_days", Integer.parseInt(CrsReportTables.stripFootnote(i.get(2))));
        row.put("final_interim_expiration_date",
            CrsReportTables.parseDate(CrsReportTables.stripFootnote(i.get(3)),
                "final expiration").toString());
      }
      row.put("crs_report_version", report.version());
      rows.add(row);
    }
    if (!interim.isEmpty()) {
      throw new IOException("appropriations_timeliness_by_fy: interim-CR years absent from "
          + "Table 1: " + interim.keySet());
    }
    if (rows.isEmpty()) {
      throw new IOException("appropriations_timeliness_by_fy: R46595 Table 1 had no rows");
    }
    LOGGER.info("appropriations_timeliness_by_fy: {} fiscal years from {}", rows.size(),
        report.version());
    return rows.iterator();
  }
}
