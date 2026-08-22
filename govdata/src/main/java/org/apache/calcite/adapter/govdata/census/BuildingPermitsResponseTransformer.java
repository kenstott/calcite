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
package org.apache.calcite.adapter.govdata.census;

import org.apache.calcite.adapter.file.etl.PerRecordResponseTransformer;
import org.apache.calcite.adapter.file.etl.RequestContext;

import java.util.Map;

/**
 * Derives {@code survey_year} / {@code survey_month} from the Building Permits Survey's
 * {@code survey_date} column (YYYYMM format).
 *
 * <p>The table's {@code period_code} partition dimension carries the raw YYMM URL code (e.g.
 * {@code 2012} = December 2020), which collides with genuine 4-digit years and must never be
 * used as a time filter. {@code survey_date} — the row's own first positional field, always
 * 6 digits after the source's {@code rowFilter} — is the reliable source for the real calendar
 * year/month, so this transformer parses it once at ingest time rather than leaving every
 * consumer to re-parse survey_date.
 *
 * <p>Implements {@link PerRecordResponseTransformer} so {@code HttpSource} streams the CSV one
 * row at a time; {@link #transform(String, RequestContext)} is never reached on that path (raw
 * cache is enabled for this table) and throws rather than silently returning wrong data.
 */
public class BuildingPermitsResponseTransformer implements PerRecordResponseTransformer {

  @Override
  public void transformRecord(Map<String, Object> row, RequestContext context) {
    Object surveyDateValue = row.get("survey_date");
    String surveyDate = surveyDateValue == null ? null : String.valueOf(surveyDateValue).trim();
    Integer surveyYear = null;
    Integer surveyMonth = null;
    if (surveyDate != null && surveyDate.length() == 6) {
      try {
        int yyyymm = Integer.parseInt(surveyDate);
        surveyYear = yyyymm / 100;
        surveyMonth = yyyymm % 100;
      } catch (NumberFormatException e) {
        // Leaves surveyYear/surveyMonth null below — the row's own rowFilter guarantees
        // survey_date is 6 digits, so this only guards against a genuinely malformed value.
      }
    }
    row.put("survey_year", surveyYear);
    row.put("survey_month", surveyMonth);
  }

  @Override
  public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        "BuildingPermitsResponseTransformer requires the per-record streaming path "
            + "(rawCache must be enabled for building_permits)");
  }
}
