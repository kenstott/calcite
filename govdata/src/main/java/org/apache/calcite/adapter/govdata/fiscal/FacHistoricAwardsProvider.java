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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * DataProvider for {@code fac_federal_awards_historic} — one row per federal program audited,
 * audit years 1998-2015, from the archived Census files ({@code census-<year>.zip}, entry
 * {@code ELECAUDITS.csv}). Joins to {@code fac_audits_historic} on {@code (year, dbkey)}; the
 * EIN is repeated on the award row. See {@link FacHistoricAuditsProvider}.
 */
public class FacHistoricAwardsProvider extends AbstractBulkCsvProvider {

  private static final String[][] COLUMNS = {
      {"dbkey", "DBKEY", S},
      {"cfda_seq_num", "CFDASEQNUM", I},
      {"cfda", "CFDA", S},
      {"cfda_prefix", "CFDA_PREFIX", S},
      {"cfda_ext", "CFDA_EXT", S},
      {"ein", "EIN", S},
      {"federal_program_name", "FEDERALPROGRAMNAME", S},
      {"amount", "AMOUNT", D},
      {"program_total", "PROGRAMTOTAL", D},
      {"cluster_total", "CLUSTERTOTAL", D},
      {"major_program", "MAJORPROGRAM", S},
      {"direct", "DIRECT", S},
      {"loans", "LOANS", S},
      {"loan_balance", "LOANBALANCE", D},
      {"arra", "ARRA", S},
      {"passthrough_award", "PASSTHROUGHAWARD", S},
      {"passthrough_amount", "PASSTHROUGHAMOUNT", D},
      {"cluster_name", "CLUSTERNAME", S},
      {"state_cluster_name", "STATECLUSTERNAME", S},
      {"award_identification", "AWARDIDENTIFICATION", S},
      {"findings_count", "FINDINGSCOUNT", I},
      {"type_requirement", "TYPEREQUIREMENT", S},
      {"questioned_costs", "QCOSTS2", S},
  };

  /** URL of the per-audit-year Census archive for this batch's year. */
  static String yearUrl(Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      throw new IOException("fac historic: no effective_year in " + variables);
    }
    return "https://app.fac.gov/dissemination/public-data/census/csv/census-" + year.trim()
        + ".zip";
  }

  @Override String tableName() {
    return "fac_federal_awards_historic";
  }

  @Override String resolveUrl(Map<String, String> variables) throws IOException {
    return yearUrl(variables);
  }

  @Override String entrySuffix() {
    return "ELECAUDITS.csv";
  }

  @Override Charset charset() {
    return StandardCharsets.ISO_8859_1;
  }

  @Override String[][] columns() {
    return COLUMNS;
  }
}
