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
package org.apache.calcite.adapter.file.etl;

import java.util.List;
import java.util.Map;

/**
 * Transforms individual source rows into zero or more output rows during materialization.
 *
 * <p>RowTransformer is called for each row as the source is streamed into the writer.
 * It is a one-to-many transform: an input row may be dropped (return an empty list),
 * passed through or modified (return a single row), or expanded into several rows
 * (return many). This enables structural reshapes that cannot be expressed as SQL
 * column expressions, such as:
 * <ul>
 *   <li>Unpivoting a wide fixed-width record into one row per repeated group
 *       (e.g. a 12-month-wide agency record into one row per month)</li>
 *   <li>External lookups (enrichment from another data source)</li>
 *   <li>Stateful transformations (deduplication with memory)</li>
 *   <li>Complex parsing (nested JSON within a field)</li>
 *   <li>Non-SQL logic (custom algorithms, API-specific quirks)</li>
 * </ul>
 *
 * <p>Transformers are applied as a streaming flat-map: rows are not buffered beyond the
 * fan-out of a single input row, so memory stays bounded regardless of dataset size.
 * When several transformers are configured they are chained — each input row flows
 * through them in order, and every transformer sees the rows produced by the previous one.
 *
 * <p><b>Note:</b> Most computed columns should use schema-driven expressions
 * instead of RowTransformer:
 * <pre>{@code
 * columns:
 *   - name: quarter
 *     type: VARCHAR
 *     expression: "SUBSTR(period, 1, 2)"
 * }</pre>
 *
 * <h3>Usage Example (one-to-many unpivot)</h3>
 * <pre>{@code
 * public class MonthUnpivotTransformer implements RowTransformer {
 *     public List<Map<String, Object>> transform(Map<String, Object> row, RowContext context) {
 *         List<Map<String, Object>> out = new ArrayList<>();
 *         for (String mon : MONTHS) {
 *             Map<String, Object> r = new LinkedHashMap<>();
 *             r.put("ori", row.get("ori"));
 *             r.put("month", mon);
 *             r.put("actual_murder", row.get(mon + "_actual_murder"));
 *             out.add(r);
 *         }
 *         return out;
 *     }
 * }
 * }</pre>
 *
 * <h3>Schema Configuration</h3>
 * <pre>{@code
 * hooks:
 *   rowTransformers:
 *     - type: class
 *       class: "org.apache.calcite.adapter.govdata.crime.MonthUnpivotTransformer"
 * }</pre>
 *
 * @see RowContext
 * @see HooksConfig
 */
public interface RowTransformer {

  /**
   * Transforms a single source row into zero or more output rows.
   *
   * <p>The implementation may mutate and return the input map, or build new maps.
   * Return an empty list (or {@code null}) to drop the row, a single-element list to
   * keep/modify it, or a multi-element list to expand it.
   *
   * @param row Map of column name to value (mutable)
   * @param context Row context including dimension values and table config
   * @return Zero or more output rows; empty or {@code null} drops the input row
   */
  List<Map<String, Object>> transform(Map<String, Object> row, RowContext context);
}
