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
 * Faithful recreation of PatentsView pg_cpc_current.tsv as patent_publication_cpc rows — one row
 * per publication-CPC code combination, keyed by pgpub_id. Full snapshot, no year.
 */
public class PatentPublicationCpcTransformer extends AbstractPgpubTransformer {

  @Override protected String sourceName() {
    return "pg_cpc_current";
  }

  @Override protected Map<String, Object> toRow(String[] parts, Map<String, Integer> hdr,
      RequestContext context) {
    String pgpubId = getField(parts, hdr, "pgpub_id");
    if (pgpubId == null || pgpubId.isEmpty()) {
      return null;
    }
    Map<String, Object> row = new HashMap<>();
    row.put("pgpub_id", strVal(pgpubId));
    row.put("cpc_sequence", intVal(getField(parts, hdr, "cpc_sequence")));
    row.put("cpc_section", strVal(getField(parts, hdr, "cpc_section")));
    row.put("cpc_class", strVal(getField(parts, hdr, "cpc_class")));
    row.put("cpc_subclass", strVal(getField(parts, hdr, "cpc_subclass")));
    row.put("cpc_group", strVal(getField(parts, hdr, "cpc_group")));
    row.put("cpc_type", strVal(getField(parts, hdr, "cpc_type")));
    row.put("cpc_version_indicator", strVal(getField(parts, hdr, "cpc_version_indicator")));
    return row;
  }
}
