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
package org.apache.calcite.adapter.govdata.econ;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Streams DOL OSHA_inspection.zip into {@code osha_inspections} rows: one row per OSHA inspection (activity), federal and state-plan, 1970s onward.
 */
public class OshaInspectionsTransformer extends OshaEnforcementTransformer {

  private static final List<Col> COLUMNS = Collections.unmodifiableList(Arrays.asList(
          str("activity_nr", "ACTIVITY_NR"),
          str("reporting_id", "REPORTING_ID"),
          str("state_flag", "STATE_FLAG"),
          str("estab_name", "ESTAB_NAME"),
          str("site_address", "SITE_ADDRESS"),
          str("site_city", "SITE_CITY"),
          str("site_state", "SITE_STATE"),
          str("site_zip", "SITE_ZIP"),
          str("owner_type", "OWNER_TYPE"),
          str("owner_code", "OWNER_CODE"),
          str("adv_notice", "ADV_NOTICE"),
          str("safety_hlth", "SAFETY_HLTH"),
          str("sic_code", "SIC_CODE"),
          str("naics_code", "NAICS_CODE"),
          str("insp_type", "INSP_TYPE"),
          str("insp_scope", "INSP_SCOPE"),
          str("why_no_insp", "WHY_NO_INSP"),
          str("union_status", "UNION_STATUS"),
          str("safety_manuf", "SAFETY_MANUF"),
          str("safety_const", "SAFETY_CONST"),
          str("safety_marit", "SAFETY_MARIT"),
          str("health_manuf", "HEALTH_MANUF"),
          str("health_const", "HEALTH_CONST"),
          str("health_marit", "HEALTH_MARIT"),
          str("migrant", "MIGRANT"),
          str("mail_street", "MAIL_STREET"),
          str("mail_city", "MAIL_CITY"),
          str("mail_state", "MAIL_STATE"),
          str("mail_zip", "MAIL_ZIP"),
          str("host_est_key", "HOST_EST_KEY"),
          intCol("nr_in_estab", "NR_IN_ESTAB"),
          date("open_date", "OPEN_DATE"),
          year("open_year", "OPEN_DATE"),
          date("case_mod_date", "CASE_MOD_DATE"),
          date("close_conf_date", "CLOSE_CONF_DATE"),
          date("close_case_date", "CLOSE_CASE_DATE")));

  @Override protected String datasetName() {
    return "inspection";
  }

  @Override protected List<Col> columns() {
    return COLUMNS;
  }
}
