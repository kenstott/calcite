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

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * DataProvider for {@code health.msha_mnm_health_samples}: MSHA Personal Health Samples, the
 * metal/nonmetal mine inspector health sampling file (respirable dust, quartz/silica, metals,
 * fumes, noise-related contaminants) since 2000-01-01, one row per sample.
 */
public class MshaMnmHealthSamplesProvider extends AbstractMshaSampleProvider {

  private long skippedFuture;

  @Override String label() {
    return "MSHA personal health samples";
  }

  @Override String[] requiredColumns() {
    return new String[] {"MINE_ID", "SAMPLE_NO", "SAMPLE_DATE", "CONTAMINANT_CD", "CONCENTRATION"};
  }

  @Override void finished() {
    logSkipped(label(), skippedFuture);
  }

  @Override Map<String, Object> toRow(Map<String, String> r) {
    LocalDate d = date(r, "SAMPLE_DATE");
    if (d == null) {
      throw new IllegalStateException("MSHA health sample without a date: " + r.get("SAMPLE_NO"));
    }
    if (inFuture(d)) {
      skippedFuture++;
      return null;
    }
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("sample_no", str(r, "SAMPLE_NO"));
    row.put("mine_id", str(r, "MINE_ID"));
    row.put("event_no", str(r, "EVENT_NO"));
    row.put("sample_date", d.toString());
    row.put("contaminant_cd", str(r, "CONTAMINANT_CD"));
    row.put("contaminant_desc", str(r, "CONTAMINANT_DESC"));
    row.put("concentration", dbl(r, "CONCENTRATION"));
    row.put("exposure_limit", dbl(r, "EXPOSURE_LIMIT"));
    row.put("concentration_over_limit", dbl(r, "C_OVER_E"));
    row.put("protection_used", str(r, "PROTECTION_USED"));
    row.put("sampling_instrument_cd", str(r, "SAMPLING_INSTRUMENT_CD"));
    row.put("sampling_instrument_desc", str(r, "SAMPLING_INSTRUMENT_DESC"));
    row.put("short_term_exposure", str(r, "SHORT_TERM_EXPOSURE"));
    row.put("action_taken_cd", str(r, "ACTION_TAKEN"));
    row.put("action_taken_desc", str(r, "ACTION_TAKEN_DESC"));
    row.put("location_cd", str(r, "LOCATION_CD"));
    row.put("location_desc", str(r, "LOCATION_CD_DESC"));
    row.put("job_cd", str(r, "JOB_CD"));
    row.put("job_desc", str(r, "JOB_CD_DESC"));
    row.put("contractor_id", str(r, "CONTRACTOR_ID"));
    row.put("type", "msha_mnm_health_samples");
    row.put("year", Integer.valueOf(d.getYear()));
    return row;
  }
}
