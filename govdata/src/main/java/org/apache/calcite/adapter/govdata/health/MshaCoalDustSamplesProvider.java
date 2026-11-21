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

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * DataProvider for {@code health.msha_coal_dust_samples}: MSHA's Coal Dust Samples (operator and
 * inspector respirable dust, gravimetric and CPDM, since 2000-01-01) left-joined to Quartz
 * Samples (silica analysis of the same cassette). The quartz zip URL is read from
 * {@code source.quartzUrl} in the schema YAML.
 */
public class MshaCoalDustSamplesProvider extends AbstractMshaSampleProvider {

  private Map<String, String[]> quartz;
  private long skippedFuture;

  @Override String label() {
    return "MSHA coal dust samples";
  }

  @Override String[] requiredColumns() {
    return new String[] {"CASS_NUM", "SMP_TAKEN_DT", "MINE_ID", "CONC"};
  }

  @Override void prepare(EtlPipelineConfig config, RawCache rawCache) throws IOException {
    Object q = config.getRawSourceConfig() == null ? null
        : config.getRawSourceConfig().get("quartzUrl");
    if (!(q instanceof String) || ((String) q).isEmpty()) {
      throw new IllegalStateException("msha_coal_dust_samples: source.quartzUrl is required");
    }
    quartz = new HashMap<>();
    BufferedReader br = openFirstTxt(rawCache, (String) q);
    RecordIterator it = new RecordIterator(br, "MSHA quartz samples",
        new String[] {"CASSETTE_NO", "QUARTZ_PCT"});
    while (it.hasNext()) {
      Map<String, String> r = it.next();
      String cass = r.get("CASSETTE_NO");
      if (cass != null && !quartz.containsKey(cass)) {
        // A cassette analysed twice appears twice (13 of 234k): the first record is kept.
        quartz.put(cass, new String[] {r.get("QUARTZ_PCT"), r.get("QUARTZ_MICROGRAMS"),
            r.get("VOID_CODE")});
      }
    }
  }

  @Override void finished() {
    logSkipped(label(), skippedFuture);
  }

  @Override Map<String, Object> toRow(Map<String, String> r) {
    LocalDate d = date(r, "SMP_TAKEN_DT");
    if (d == null) {
      throw new IllegalStateException("MSHA coal dust: sample without a date, cassette "
          + r.get("CASS_NUM"));
    }
    if (inFuture(d)) {
      skippedFuture++;
      return null;
    }
    Map<String, Object> row = new LinkedHashMap<>();
    String cass = r.get("CASS_NUM");
    row.put("cassette_num", cass);
    row.put("cpdm_file_name", str(r, "SMP_CPDM_FILE_NAME"));
    row.put("mine_id", str(r, "MINE_ID"));
    row.put("sample_date", d.toString());
    row.put("device_type", str(r, "DEVICE_TYPE_CD"));
    row.put("inspector_sample", str(r, "INSP_IND"));
    row.put("concentration_mg_m3", dbl(r, "CONC"));
    row.put("initial_weight_mg", dbl(r, "INIT_WT"));
    row.put("final_weight_mg", dbl(r, "FINAL_WT"));
    row.put("sample_minutes", intg(r, "SMP_TIME"));
    row.put("sample_type_cd", str(r, "SMP_TYPE_CD"));
    row.put("sample_type", str(r, "SMP_TYPE_CD_DESC"));
    row.put("use_cd", str(r, "SMP_USE_CD"));
    row.put("use_desc", str(r, "SMP_USE_CD_DESC"));
    row.put("void_cd", str(r, "VOID_CD"));
    row.put("void_desc", str(r, "VOID_CD_DESC"));
    row.put("entity_num", str(r, "ENTITY_NUM"));
    row.put("entity_type_cd", str(r, "TYPE_ENTITY"));
    row.put("entity_type_desc", str(r, "TYPE_ENTITY_CD_DESC"));
    row.put("occupation_cd", str(r, "OCC_CD"));
    row.put("occupation_desc", str(r, "OCC_CD_DESC"));
    row.put("mining_method_cd", str(r, "INSP_METH_MINING"));
    row.put("mining_method_desc", str(r, "METH_MINE_TYPE_DESC"));
    row.put("single_sample_ecv", dbl(r, "SINGLE_SAMPLE_ECV"));
    row.put("average_5_ecv", dbl(r, "AVERAGE_5_ECV"));
    row.put("average_15_ecv", dbl(r, "AVERAGE_15_ECV"));
    String[] qz = cass == null ? null : quartz.get(cass);
    row.put("quartz_pct", qz == null ? null : parse(qz[0]));
    row.put("quartz_micrograms", qz == null ? null : parse(qz[1]));
    row.put("quartz_void_cd", qz == null ? null : qz[2]);
    row.put("type", "msha_coal_dust_samples");
    row.put("year", Integer.valueOf(d.getYear()));
    return row;
  }

  private static Double parse(String v) {
    return v == null ? null : Double.valueOf(v.trim());
  }
}
