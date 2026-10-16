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
 * DataProvider for {@code fac_audits} — one row per Single Audit filed with the Federal Audit
 * Clearinghouse for audit years 2016 to date, from the current-data bulk file {@code general.csv}.
 * Contact-person names, e-mail addresses and phone numbers are deliberately not loaded.
 *
 * <p>The file is a single full extract regenerated daily, so its URL never changes; the
 * {@code refresh_month} dimension is part of the cache key so each month re-reads it. The source
 * writes the literal {@code NULL} for a missing value. Audit years 2016-2021 carry the placeholder
 * UEI {@code GSA_MIGRATION} (the filings were migrated from the Census system before UEIs
 * existed); the EIN is always present.
 */
public class FacCurrentGeneralProvider extends AbstractBulkCsvProvider {

  static final String URL = "https://app.fac.gov/dissemination/public-data/gsa/full/general.csv";

  private static final String[][] COLUMNS = {
      {"report_id", "report_id", S},
      {"auditee_uei", "auditee_uei", S},
      {"audit_year", "audit_year", I},
      {"auditee_name", "auditee_name", S},
      {"auditee_ein", "auditee_ein", S},
      {"auditee_address_line_1", "auditee_address_line_1", S},
      {"auditee_city", "auditee_city", S},
      {"auditee_state", "auditee_state", S},
      {"auditee_zip", "auditee_zip", S},
      {"auditor_firm_name", "auditor_firm_name", S},
      {"auditor_ein", "auditor_ein", S},
      {"auditor_city", "auditor_city", S},
      {"auditor_state", "auditor_state", S},
      {"auditor_zip", "auditor_zip", S},
      {"cognizant_agency", "cognizant_agency", S},
      {"oversight_agency", "oversight_agency", S},
      {"date_created", "date_created", S},
      {"submitted_date", "submitted_date", S},
      {"fac_accepted_date", "fac_accepted_date", S},
      {"fy_start_date", "fy_start_date", S},
      {"fy_end_date", "fy_end_date", S},
      {"audit_type", "audit_type", S},
      {"audit_period_covered", "audit_period_covered", S},
      {"number_months", "number_months", S},
      {"gaap_results", "gaap_results", S},
      {"is_going_concern_included", "is_going_concern_included", S},
      {"is_internal_control_deficiency_disclosed", "is_internal_control_deficiency_disclosed", S},
      {"is_internal_control_material_weakness_disclosed",
          "is_internal_control_material_weakness_disclosed", S},
      {"is_material_noncompliance_disclosed", "is_material_noncompliance_disclosed", S},
      {"dollar_threshold", "dollar_threshold", D},
      {"is_low_risk_auditee", "is_low_risk_auditee", S},
      {"agencies_with_prior_findings", "agencies_with_prior_findings", S},
      {"entity_type", "entity_type", S},
      {"total_amount_expended", "total_amount_expended", D},
      {"type_audit_code", "type_audit_code", S},
      {"is_public", "is_public", S},
      {"data_source", "data_source", S},
      {"resubmission_version", "resubmission_version", S},
      {"resubmission_status", "resubmission_status", S},
      {"is_multiple_eins", "is_multiple_eins", S},
      {"is_additional_ueis", "is_additional_ueis", S},
      {"is_secondary_auditors", "is_secondary_auditors", S},
  };

  @Override String tableName() {
    return "fac_audits";
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
