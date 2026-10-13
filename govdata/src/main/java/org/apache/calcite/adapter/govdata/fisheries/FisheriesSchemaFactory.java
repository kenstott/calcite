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
package org.apache.calcite.adapter.govdata.fisheries;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Factory for the fisheries schema.
 *
 * <p>Commercial fisheries landings from NOAA Fisheries' FOSS (Fisheries One Stop Shop)
 * landings API, by state, species and year, covering every commercial species and
 * including all salmon species. Every source is free and unauthenticated, so no table is
 * key-gated.
 *
 * <p>Tables:
 * <ul>
 *   <li>{@code noaa_commercial_landings} — pounds and dollars landed by state, species and
 *       year, 1950 onward</li>
 * </ul>
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "name": "fisheries",
 *   "type": "custom",
 *   "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *   "operand": {
 *     "dataSource": "fisheries",
 *     "directory": "${GOVDATA_PARQUET_DIR}",
 *     "cacheDirectory": "${GOVDATA_CACHE_DIR}"
 *   }
 * }
 * </pre>
 */
public class FisheriesSchemaFactory implements GovDataSubSchemaFactory {

  @Override public String getSchemaResourceName() {
    return "/fisheries/fisheries-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureSchemaHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    // No source filtering — every table is keyless.
  }
}
