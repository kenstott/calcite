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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Transforms CMS Provider Data Catalog "Health Deficiencies" data (dataset r5ix-sfxw,
 * same datastore-query API family as {@link CmsNursingHomeResponseTransformer}).
 *
 * <p>Response structure: {@code { "results": [ { ... }, ... ] } } — one row per citation,
 * carrying the facility CCN, survey date, deficiency tag number, scope-severity code
 * (CMS's G–L actual-harm/immediate-jeopardy scale), correction status, and inspection
 * cycle. Multi-year citation-level history, complementing the single-snapshot
 * cms_nursing_home table.
 */
public class CmsNursingHomeDeficienciesResponseTransformer
    extends AbstractOpenFdaResponseTransformer {

  @Override
  protected void flattenRecord(JsonNode record, ObjectNode row) {
    put(row, "ccn", text(record, "cms_certification_number_ccn"));
    put(row, "provider_name", text(record, "provider_name"));
    put(row, "address", text(record, "provider_address"));
    put(row, "city", text(record, "citytown"));
    put(row, "state", text(record, "state"));
    put(row, "zip_code", text(record, "zip_code"));
    put(row, "survey_date", text(record, "survey_date"));
    put(row, "survey_type", text(record, "survey_type"));
    put(row, "deficiency_prefix", text(record, "deficiency_prefix"));
    put(row, "deficiency_category", text(record, "deficiency_category"));
    put(row, "deficiency_tag_number", text(record, "deficiency_tag_number"));
    put(row, "deficiency_description", text(record, "deficiency_description"));
    put(row, "scope_severity_code", text(record, "scope_severity_code"));
    put(row, "deficiency_corrected", text(record, "deficiency_corrected"));
    put(row, "correction_date", text(record, "correction_date"));
    put(row, "inspection_cycle", text(record, "inspection_cycle"));
    put(row, "standard_deficiency", text(record, "standard_deficiency"));
    put(row, "complaint_deficiency", text(record, "complaint_deficiency"));
    put(row, "infection_control_inspection_deficiency",
        text(record, "infection_control_inspection_deficiency"));
    put(row, "citation_under_idr", text(record, "citation_under_idr"));
    put(row, "citation_under_iidr", text(record, "citation_under_iidr"));
    put(row, "type", "cms_nursing_home_deficiencies");
  }
}
