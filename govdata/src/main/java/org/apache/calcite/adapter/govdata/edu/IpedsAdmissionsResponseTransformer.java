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
package org.apache.calcite.adapter.govdata.edu;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * IPEDS Admissions (ADM{year}): applicants, admitted and enrolled counts and test-score
 * percentiles, one row per unitid. Adds {@code admit_rate} (admitted / applicants) and
 * {@code yield_rate} (enrolled / admitted), left null when the denominator is absent or zero.
 * Columns introduced in later survey years (gender-unknown "AN"/"UN" splits, ACT/SAT 50th
 * percentiles, ADMCON10-12) are null for earlier years.
 */
public class IpedsAdmissionsResponseTransformer extends IpedsBulkCsvTransformer {

  private static final Map<String, String> COLUMN_MAP;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("UNITID", "unitid");
    m.put("ADMCON1", "admcon1");
    m.put("ADMCON2", "admcon2");
    m.put("ADMCON3", "admcon3");
    m.put("ADMCON4", "admcon4");
    m.put("ADMCON5", "admcon5");
    m.put("ADMCON6", "admcon6");
    m.put("ADMCON7", "admcon7");
    m.put("ADMCON8", "admcon8");
    m.put("ADMCON9", "admcon9");
    m.put("ADMCON10", "admcon10");
    m.put("ADMCON11", "admcon11");
    m.put("ADMCON12", "admcon12");
    m.put("APPLCN", "applcn");
    m.put("APPLCNM", "applcnm");
    m.put("APPLCNW", "applcnw");
    m.put("APPLCNAN", "applcnan");
    m.put("APPLCNUN", "applcnun");
    m.put("ADMSSN", "admssn");
    m.put("ADMSSNM", "admssnm");
    m.put("ADMSSNW", "admssnw");
    m.put("ADMSSNAN", "admssnan");
    m.put("ADMSSNUN", "admssnun");
    m.put("ENRLT", "enrlt");
    m.put("ENRLM", "enrlm");
    m.put("ENRLW", "enrlw");
    m.put("ENRLAN", "enrlan");
    m.put("ENRLUN", "enrlun");
    m.put("ENRLFT", "enrlft");
    m.put("ENRLFTM", "enrlftm");
    m.put("ENRLFTW", "enrlftw");
    m.put("ENRLFTAN", "enrlftan");
    m.put("ENRLFTUN", "enrlftun");
    m.put("ENRLPT", "enrlpt");
    m.put("ENRLPTM", "enrlptm");
    m.put("ENRLPTW", "enrlptw");
    m.put("ENRLPTAN", "enrlptan");
    m.put("ENRLPTUN", "enrlptun");
    m.put("SATNUM", "satnum");
    m.put("SATPCT", "satpct");
    m.put("ACTNUM", "actnum");
    m.put("ACTPCT", "actpct");
    m.put("SATVR25", "satvr25");
    m.put("SATVR50", "satvr50");
    m.put("SATVR75", "satvr75");
    m.put("SATMT25", "satmt25");
    m.put("SATMT50", "satmt50");
    m.put("SATMT75", "satmt75");
    m.put("SATWR25", "satwr25");
    m.put("SATWR75", "satwr75");
    m.put("ACTCM25", "actcm25");
    m.put("ACTCM50", "actcm50");
    m.put("ACTCM75", "actcm75");
    m.put("ACTEN25", "acten25");
    m.put("ACTEN50", "acten50");
    m.put("ACTEN75", "acten75");
    m.put("ACTMT25", "actmt25");
    m.put("ACTMT50", "actmt50");
    m.put("ACTMT75", "actmt75");
    m.put("ACTWR25", "actwr25");
    m.put("ACTWR75", "actwr75");
    COLUMN_MAP = Collections.unmodifiableMap(m);
  }

  @Override String csvBaseName(String effectiveYear) {
    return "adm" + effectiveYear;
  }

  @Override Map<String, String> columnMap() {
    return COLUMN_MAP;
  }

  @Override void derive(Map<String, Object> row) {
    Integer applied = (Integer) row.get("applcn");
    Integer admitted = (Integer) row.get("admssn");
    Integer enrolled = (Integer) row.get("enrlt");
    row.put("admit_rate", ratio(admitted, applied));
    row.put("yield_rate", ratio(enrolled, admitted));
  }

  private static Double ratio(Integer num, Integer den) {
    if (num == null || den == null || den == 0) {
      return null;
    }
    return Double.valueOf((double) num / den);
  }
}
