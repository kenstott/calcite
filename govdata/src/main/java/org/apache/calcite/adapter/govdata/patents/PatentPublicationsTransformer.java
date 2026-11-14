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
 * Faithful recreation of PatentsView pg_published_application.tsv as patent_publications rows —
 * one row per USPTO pre-grant publication, self-partitioned by the year of published_date.
 */
public class PatentPublicationsTransformer extends AbstractPgpubTransformer {

  @Override protected String sourceName() {
    return "pg_published_application";
  }

  @Override protected Map<String, Object> toRow(String[] parts, Map<String, Integer> hdr,
      RequestContext context) {
    final String yearStr = getEffectiveYear(context);
    if (yearStr == null || yearStr.isEmpty()) {
      throw new IllegalStateException("PatentPublications: missing year dimension");
    }
    String publishedDate = getField(parts, hdr, "published_date");
    if (publishedDate == null || !publishedDate.startsWith(yearStr)) {
      return null;
    }
    String pgpubId = getField(parts, hdr, "pgpub_id");
    if (pgpubId == null || pgpubId.isEmpty()) {
      return null;
    }
    Map<String, Object> row = new HashMap<>();
    row.put("pgpub_id", strVal(pgpubId));
    row.put("publication_year", intVal(yearStr));
    row.put("application_id", strVal(getField(parts, hdr, "application_id")));
    row.put("filing_date", strVal(getField(parts, hdr, "filing_date")));
    row.put("published_date", strVal(publishedDate));
    row.put("patent_type", strVal(getField(parts, hdr, "patent_type")));
    row.put("filing_type", strVal(getField(parts, hdr, "filing_type")));
    row.put("wipo_kind", strVal(getField(parts, hdr, "wipo_kind")));
    row.put("series_code", strVal(getField(parts, hdr, "series_code")));
    row.put("application_title", strVal(getField(parts, hdr, "application_title")));
    row.put("rule_47_flag", boolVal(getField(parts, hdr, "rule_47_flag")));
    return row;
  }
}
