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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Sub-schema factory for the {@code health} data domain (Phase 1: openFDA).
 *
 * <p>Loads {@code /health/health-schema.yaml}. No special hooks are required
 * beyond what is configured in the YAML (pagination, transformers, iceberg
 * materialization). The optional {@code HEALTH_FDA_API_KEY} environment variable
 * raises the openFDA rate limit from 1k to 120k requests/day.
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "name": "health",
 *   "type": "custom",
 *   "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *   "operand": {
 *     "dataSource": "health",
 *     "directory": "${HEALTH_PARQUET_DIR}"
 *   }
 * }
 * </pre>
 */
public class HealthSchemaFactory implements GovDataSubSchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(HealthSchemaFactory.class);

  @Override
  public String getSchemaResourceName() {
    return "/health/health-schema.yaml";
  }

  @Override
  public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override
  public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    String fdaApiKey = System.getenv("HEALTH_FDA_API_KEY");
    if (fdaApiKey == null || fdaApiKey.isEmpty()) {
      LOGGER.info("HEALTH_FDA_API_KEY not set — using openFDA anonymous rate limit (1k req/day)");
    } else {
      LOGGER.info("HEALTH_FDA_API_KEY set — using elevated openFDA rate limit (120k req/day)");
    }
  }
}
