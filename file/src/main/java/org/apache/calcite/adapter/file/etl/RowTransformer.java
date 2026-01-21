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

import java.util.Map;

/**
 * Transforms individual rows during materialization.
 *
 * <p>RowTransformer is called for each row during the materialization process.
 * Use this interface for transformations that cannot be expressed as SQL column
 * expressions, such as:
 * <ul>
 *   <li>External lookups (enrichment from another data source)</li>
 *   <li>Stateful transformations (deduplication with memory)</li>
 *   <li>Complex parsing (nested JSON within a field)</li>
 *   <li>Non-SQL logic (custom algorithms, API-specific quirks)</li>
 * </ul>
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
 * <h3>Usage Example</h3>
 * <pre>{@code
 * public class CensusGeoEnricher implements RowTransformer {
 *     private final Map<String, GeoMetadata> geoLookup;
 *
 *     public Map<String, Object> transform(Map<String, Object> row, RowContext context) {
 *         String geoFips = (String) row.get("geo_fips");
 *         GeoMetadata meta = geoLookup.get(geoFips);
 *         if (meta != null) {
 *             row.put("land_area_sq_mi", meta.getLandArea());
 *             row.put("census_region", meta.getRegion());
 *         }
 *         return row;
 *     }
 * }
 * }</pre>
 *
 * <h3>Schema Configuration</h3>
 * <pre>{@code
 * hooks:
 *   rowTransformers:
 *     - type: class
 *       class: "org.apache.calcite.adapter.govdata.CensusGeoEnricher"
 * }</pre>
 *
 * @see RowContext
 * @see HooksConfig
 */
public interface RowTransformer {

  /**
   * Transforms a single row.
   *
   * <p>The implementation may modify the input map in place or create a new map.
   * Returning null will cause the row to be dropped from the output.
   *
   * @param row Map of column name to value (mutable)
   * @param context Row context including dimension values and table config
   * @return Transformed row, or null to drop the row
   */
  Map<String, Object> transform(Map<String, Object> row, RowContext context);
}
