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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Factory for the REF (reference) schema containing cross-referencing tables
 * that link financial identifiers across systems.
 *
 * <p>This schema provides:
 * <ul>
 *   <li>GLEIF entity records — LEI to legal entity mapping</li>
 *   <li>GLEIF CIK mapping — direct LEI↔CIK bridge for SEC cross-referencing</li>
 *   <li>OpenFIGI instruments — ticker to FIGI mapping (optional, requires API key)</li>
 * </ul>
 *
 * <p>This is a standalone reference schema with no dependencies, similar to
 * {@code econ_reference}. Table enablement is controlled by the {@code enabled}
 * field in ref-schema.yaml using variable substitution for API key checks.
 */
public class RefSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(RefSchemaFactory.class);

  @Override public String getSchemaResourceName() {
    return "/ref/ref-schema.yaml";
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for REF schema");
    // Reference tables have no special hooks - enablement is controlled via YAML
    // The 'enabled' field in the schema uses variable substitution for API key checks
  }
}
