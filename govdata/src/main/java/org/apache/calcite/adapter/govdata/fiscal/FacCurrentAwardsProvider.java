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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * DataProvider for {@code fac_federal_awards} — one row per federal program (assistance listing)
 * audited in a Single Audit, audit years 2016 to date, from the current-data bulk file
 * {@code federal_awards.csv}. Joins to {@code fac_audits} on {@code report_id}; the EIN lives only
 * on the audit row. See {@link FacCurrentGeneralProvider} for the refresh and placeholder-UEI
 * notes.
 */
public class FacCurrentAwardsProvider extends AbstractBulkCsvProvider {

  static final String URL =
      "https://app.fac.gov/dissemination/public-data/gsa/full/federal_awards.csv";

  private static final String[][] COLUMNS = {
      {"report_id", "report_id", S},
      {"auditee_uei", "auditee_uei", S},
      {"audit_year", "audit_year", I},
      {"fac_accepted_date", "fac_accepted_date", S},
      {"award_reference", "award_reference", S},
      {"federal_agency_prefix", "federal_agency_prefix", S},
      {"federal_award_extension", "federal_award_extension", S},
      {"additional_award_identification", "additional_award_identification", S},
      {"federal_program_name", "federal_program_name", S},
      {"amount_expended", "amount_expended", D},
      {"cluster_name", "cluster_name", S},
      {"other_cluster_name", "other_cluster_name", S},
      {"state_cluster_name", "state_cluster_name", S},
      {"cluster_total", "cluster_total", D},
      {"federal_program_total", "federal_program_total", D},
      {"is_major", "is_major", S},
      {"is_loan", "is_loan", S},
      {"loan_balance", "loan_balance", S},
      {"is_direct", "is_direct", S},
      {"audit_report_type", "audit_report_type", S},
      {"findings_count", "findings_count", I},
      {"is_passthrough_award", "is_passthrough_award", S},
      {"passthrough_amount", "passthrough_amount", D},
  };

  @Override String tableName() {
    return "fac_federal_awards";
  }

  @Override String resolveUrl(Map<String, String> variables) {
    return URL;
  }

  @Override String cacheKey(String url, Map<String, String> variables) {
    return url + "?refresh_month=" + variables.get("refresh_month");
  }

  @Override String entrySuffix() {
    return null;
  }

  @Override Charset charset() {
    return StandardCharsets.UTF_8;
  }

  @Override String[][] columns() {
    return COLUMNS;
  }

  @Override String nullToken() {
    return "NULL";
  }
}
