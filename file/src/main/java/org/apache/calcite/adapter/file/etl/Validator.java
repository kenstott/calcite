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
 * Validates rows before writing to output.
 *
 * <p>Validator is called for each row after all transformations but before
 * writing to Parquet. This allows for data quality checks and enforcing
 * business rules.
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * public class DataValidator implements Validator {
 *     public ValidationResult validate(Map<String, Object> row) {
 *         Object geoFips = row.get("geo_fips");
 *         if (geoFips == null) {
 *             return ValidationResult.drop("Missing required field: geo_fips");
 *         }
 *         Object value = row.get("value");
 *         if (value instanceof Number && ((Number) value).doubleValue() < 0) {
 *             return ValidationResult.warn("Negative value: " + value);
 *         }
 *         return ValidationResult.valid();
 *     }
 * }
 * }</pre>
 *
 * <h3>Schema Configuration</h3>
 * <pre>{@code
 * hooks:
 *   validators:
 *     - type: class
 *       class: "org.apache.calcite.adapter.govdata.DataValidator"
 *     - type: expression
 *       condition: "geo_fips IS NOT NULL"
 *       action: "drop"
 * }</pre>
 *
 * <h3>Validation Actions</h3>
 * <ul>
 *   <li>{@link ValidationResult.Action#VALID} - Include row in output</li>
 *   <li>{@link ValidationResult.Action#DROP} - Silently exclude row</li>
 *   <li>{@link ValidationResult.Action#WARN} - Log warning but include row</li>
 *   <li>{@link ValidationResult.Action#FAIL} - Stop processing with error</li>
 * </ul>
 *
 * @see ValidationResult
 * @see HooksConfig
 */
public interface Validator {

  /**
   * Validates a row.
   *
   * @param row Map of column name to value (should not be modified)
   * @return ValidationResult indicating whether the row is valid and what action to take
   */
  ValidationResult validate(Map<String, Object> row);
}
