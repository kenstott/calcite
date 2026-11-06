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
 * DataProvider for {@code fac_audits_historic} — one row per Single Audit for audit years
 * 1998-2015 from the Federal Audit Clearinghouse's archived Census files ({@code census-<year>.zip},
 * entry {@code ELECAUDITHEADER.csv}). The Census system pre-dates UEIs, so the keys are EIN and
 * DUNS. The files are frozen, so the year dimension alone keys the cache. Contact names, e-mail
 * addresses and phone numbers are deliberately not loaded.
 */
public class FacHistoricAuditsProvider extends AbstractBulkCsvProvider {

  private static final String[][] COLUMNS = {
      {"dbkey", "DBKEY", S},
      {"census_id", "CENSUSID", S},
      {"ein", "EIN", S},
      {"ein_subcode", "EINSUBCODE", S},
      {"duns", "DUNS", S},
      {"auditee_name", "AUDITEENAME", S},
      {"street", "STREET1", S},
      {"city", "CITY", S},
      {"state", "STATE", S},
      {"zip_code", "ZIPCODE", S},
      {"cpa_firm_name", "CPAFIRMNAME", S},
      {"cpa_city", "CPACITY", S},
      {"cpa_state", "CPASTATE", S},
      {"cpa_zip_code", "CPAZIPCODE", S},
      {"auditor_ein", "AUDITOR_EIN", S},
      {"cognizant_agency", "COGAGENCY", S},
      {"oversight_agency", "OVERSIGHTAGENCY", S},
      {"fy_start_date", "FYSTARTDATE", S},
      {"fy_end_date", "FYENDDATE", S},
      {"audit_type", "AUDITTYPE", S},
      {"period_covered", "PERIODCOVERED", S},
      {"number_months", "NUMBERMONTHS", S},
      {"report_type_fs", "TYPEREPORT_FS", S},
      {"material_weakness", "MATERIALWEAKNESS", S},
      {"material_noncompliance", "MATERIALNONCOMPLIANCE", S},
      {"going_concern", "GOINGCONCERN", S},
      {"dollar_threshold", "DOLLARTHRESHOLD", D},
      {"low_risk", "LOWRISK", S},
      {"total_federal_expenditure", "TOTFEDEXPEND", D},
      {"entity_type", "TYPEOFENTITY", S},
      {"fac_accepted_date", "FACACCEPTEDDATE", S},
      {"uei", "UEI", S},
  };

  @Override String tableName() {
    return "fac_audits_historic";
  }

  @Override String resolveUrl(Map<String, String> variables) throws IOException {
    return FacHistoricAwardsProvider.yearUrl(variables);
  }

  @Override String entrySuffix() {
    return "ELECAUDITHEADER.csv";
  }

  @Override Charset charset() {
    return StandardCharsets.ISO_8859_1;
  }

  @Override String[][] columns() {
    return COLUMNS;
  }
}
