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

import org.apache.calcite.adapter.file.etl.RowContext;
import org.apache.calcite.adapter.file.etl.RowTransformer;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Lifts the nested {@code country: {id, value}} object of a World Bank indicator row into the
 * {@code country_id} and {@code country_name} fields.
 *
 * <p>{@code country.id} is the only area identifier present on every row: the API leaves
 * {@code countryiso3code} empty for its income-group aggregates.
 */
public class WorldBankCountryTransformer implements RowTransformer {

  @Override public List<Map<String, Object>> transform(Map<String, Object> row, RowContext context) {
    Object country = row.get("country");
    if (!(country instanceof Map)) {
      throw new IllegalStateException("World Bank row has no country object: " + row);
    }
    row.put("country_id", required((Map<?, ?>) country, "id", row));
    row.put("country_name", required((Map<?, ?>) country, "value", row));
    return Collections.singletonList(row);
  }

  private static String required(Map<?, ?> country, String key, Map<String, Object> row) {
    Object value = country.get(key);
    if (value == null || value.toString().isEmpty()) {
      throw new IllegalStateException("World Bank row has no country." + key + ": " + row);
    }
    return value.toString();
  }
}
