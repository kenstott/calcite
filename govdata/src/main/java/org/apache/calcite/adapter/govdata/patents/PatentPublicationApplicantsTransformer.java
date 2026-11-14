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
package org.apache.calcite.adapter.govdata.patents;

import org.apache.calcite.adapter.file.etl.RequestContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Faithful recreation of PatentsView pg_applicant_not_disambiguated.tsv as
 * patent_publication_applicants rows — one row per applicant on a publication, as it appears in
 * the source XML, keyed by pgpub_id. Full snapshot, no year.
 */
public class PatentPublicationApplicantsTransformer extends AbstractPgpubTransformer {

  @Override protected String sourceName() {
    return "pg_applicant_not_disambiguated";
  }

  @Override protected Map<String, Object> toRow(String[] parts, Map<String, Integer> hdr,
      RequestContext context) {
    String pgpubId = getField(parts, hdr, "pgpub_id");
    if (pgpubId == null || pgpubId.isEmpty()) {
      return null;
    }
    Map<String, Object> row = new HashMap<>();
    row.put("pgpub_id", strVal(pgpubId));
    row.put("applicant_sequence", intVal(getField(parts, hdr, "applicant_sequence")));
    row.put("applicant_name_first", strVal(getField(parts, hdr, "raw_applicant_name_first")));
    row.put("applicant_name_last", strVal(getField(parts, hdr, "raw_applicant_name_last")));
    row.put("applicant_organization", strVal(getField(parts, hdr, "raw_applicant_organization")));
    row.put("applicant_type", strVal(getField(parts, hdr, "applicant_type")));
    row.put("applicant_designation", strVal(getField(parts, hdr, "applicant_designation")));
    row.put("applicant_authority", strVal(getField(parts, hdr, "applicant_authority")));
    row.put("rawlocation_id", strVal(getField(parts, hdr, "rawlocation_id")));
    return row;
  }
}
