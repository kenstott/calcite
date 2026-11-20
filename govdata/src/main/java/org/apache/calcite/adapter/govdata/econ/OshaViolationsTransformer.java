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
 * Streams DOL OSHA_violation.zip into {@code osha_violations} rows: one row per citation (activity_nr + citation_id).
 */
public class OshaViolationsTransformer extends OshaEnforcementTransformer {

  private static final List<Col> COLUMNS = Collections.unmodifiableList(Arrays.asList(
          str("activity_nr", "ACTIVITY_NR"),
          str("citation_id", "CITATION_ID"),
          str("delete_flag", "DELETE_FLAG"),
          str("standard", "STANDARD"),
          str("viol_type", "VIOL_TYPE"),
          date("issuance_date", "ISSUANCE_DATE"),
          year("issuance_year", "ISSUANCE_DATE"),
          date("abate_date", "ABATE_DATE"),
          str("abate_complete", "ABATE_COMPLETE"),
          dbl("current_penalty", "CURRENT_PENALTY"),
          dbl("initial_penalty", "INITIAL_PENALTY"),
          date("contest_date", "CONTEST_DATE"),
          date("final_order_date", "FINAL_ORDER_DATE"),
          intCol("nr_instances", "NR_INSTANCES"),
          intCol("nr_exposed", "NR_EXPOSED"),
          str("rec", "REC"),
          str("gravity", "GRAVITY"),
          str("emphasis", "EMPHASIS"),
          str("hazcat", "HAZCAT"),
          str("fta_insp_nr", "FTA_INSP_NR"),
          date("fta_issuance_date", "FTA_ISSUANCE_DATE"),
          dbl("fta_penalty", "FTA_PENALTY"),
          date("fta_contest_date", "FTA_CONTEST_DATE"),
          date("fta_final_order_date", "FTA_FINAL_ORDER_DATE"),
          str("hazsub1", "HAZSUB1"),
          str("hazsub2", "HAZSUB2"),
          str("hazsub3", "HAZSUB3"),
          str("hazsub4", "HAZSUB4"),
          str("hazsub5", "HAZSUB5")));

  @Override protected String datasetName() {
    return "violation";
  }

  @Override protected List<Col> columns() {
    return COLUMNS;
  }
}
