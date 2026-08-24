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
package org.apache.calcite.adapter.govdata.officials;

import org.apache.calcite.adapter.file.etl.RowContext;
import org.apache.calcite.adapter.file.etl.RowTransformer;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Drops duplicate (congress, citation) rows from the Congress.gov {@code /v3/nomination}
 * OFFSET-paginated feed.
 *
 * <p>Verified live (2026-08-24): many nominations in the same congress share an identical
 * {@code updateDate} (batch confirmations processed in one Senate executive session land
 * on the same timestamp), and the API gives no documented secondary sort key. Two separate
 * requests for the same offset window are not guaranteed to return rows in the same
 * relative order among tied-updateDate records, so a record can land in two adjacent pages'
 * windows across a run — observed as ~13 exact-duplicate (congress, citation) pairs across
 * ~3 congresses, always count=2, rows otherwise byte-for-byte identical. This is a
 * pagination-stability property of the upstream API, not something a source-side config
 * change can fix; deduplicating here (one fresh instance per congress, matching the
 * dimension fan-out — see {@code EtlPipeline.loadRowTransformers}) is cheap and exact
 * since a true duplicate is a full-row repeat, not a legitimately-updated re-fetch.
 */
public class NominationDedupTransformer implements RowTransformer {

  private final Set<String> seen = new HashSet<String>();

  @Override public List<Map<String, Object>> transform(Map<String, Object> row, RowContext context) {
    Object citation = row.get("citation");
    String congress = context.getDimensionValues().get("congress");
    String key = congress + "|" + citation;
    if (!seen.add(key)) {
      return Collections.emptyList();
    }
    return Collections.singletonList(row);
  }
}
