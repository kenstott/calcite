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
 * Normalizes API-specific variable names to conceptual names for schema evolution.
 *
 * <p>VariableNormalizer enables the same column expressions to work across different
 * API versions, years, or data sources by mapping source-specific field names to
 * consistent conceptual names.
 *
 * <h3>Schema Evolution Use Case</h3>
 * <p>Many APIs change their field names over time:
 * <ul>
 *   <li>Census Bureau: {@code B01001_001E} (ACS) vs {@code P1_001N} (Decennial 2020)</li>
 *   <li>BEA: Field naming changes between API versions</li>
 *   <li>BLS: Series ID format evolution</li>
 * </ul>
 *
 * <p>The normalizer maps these to consistent names (e.g., {@code total_population})
 * so column expressions don't need to change when the underlying API changes.
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * public class CensusVariableNormalizer implements VariableNormalizer {
 *     public String normalize(String apiVariable, Map<String, String> context) {
 *         int year = Integer.parseInt(context.getOrDefault("year", "2020"));
 *         String type = context.getOrDefault("type", "acs");
 *         return ConceptualVariableMapper.getInstance()
 *             .findConceptualNameForVariable(apiVariable, year, type);
 *     }
 * }
 * }</pre>
 *
 * <h3>Schema Configuration</h3>
 * <pre>{@code
 * hooks:
 *   variableNormalizer: "org.apache.calcite.adapter.govdata.census.CensusVariableNormalizer"
 * }</pre>
 *
 * <p>When configured, the ETL pipeline will:
 * <ol>
 *   <li>Parse the API response into records with original field names</li>
 *   <li>Call {@link #normalize} for each field name</li>
 *   <li>Replace original names with normalized names in the records</li>
 *   <li>Evaluate column expressions using the normalized names</li>
 * </ol>
 *
 * @see HooksConfig
 * @see ResponseTransformer
 */
public interface VariableNormalizer {

  /**
   * Normalizes an API-specific variable name to a conceptual name.
   *
   * <p>This method is called for each field name in the parsed API response.
   * The implementation should return the conceptual name if a mapping exists,
   * or the original name if no mapping is found.
   *
   * @param apiVariable The API-specific variable name (e.g., "B01001_001E")
   * @param context Dimension values providing context for resolution (year, type, etc.)
   * @return The normalized conceptual name (e.g., "total_population"),
   *         or the original name if no mapping exists
   */
  String normalize(String apiVariable, Map<String, String> context);

  /**
   * Checks if a field name should be preserved as-is without normalization.
   *
   * <p>Override this method to skip normalization for geographic columns,
   * metadata fields, or other columns that should retain their original names.
   *
   * <p>Default implementation preserves common geographic column names.
   *
   * @param fieldName The field name to check
   * @return true if the field should be preserved without normalization
   */
  default boolean shouldPreserve(String fieldName) {
    if (fieldName == null) {
      return true;
    }
    String lower = fieldName.toLowerCase();
    return "name".equals(lower)
        || "state".equals(lower)
        || "county".equals(lower)
        || "tract".equals(lower)
        || "place".equals(lower)
        || lower.contains("fips")
        || lower.contains("geoid");
  }
}
