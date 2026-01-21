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
package org.apache.calcite.adapter.govdata.safety;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.schema.CommentableSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Public safety data schema implementation.
 *
 * <p>Provides SQL access to U.S. public safety data including crime statistics,
 * traffic safety data, emergency services, natural disasters, and law enforcement
 * information from federal, state, and local government sources.
 */
public class SafetySchema extends FileSchema implements CommentableSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(SafetySchema.class);

  @SuppressWarnings("unchecked")
  public SafetySchema(Schema parentSchema, String name, Map<String, Object> operand) {
    // Cast parentSchema to SchemaPlus which FileSchema expects
    super((SchemaPlus) parentSchema, name,
          getSourceDirectory(operand),
          (List<Map<String, Object>>) operand.get("tables"));
    LOGGER.debug("SafetySchema created with name: {}", name);
  }

  private static File getSourceDirectory(Map<String, Object> operand) {
    String directory = (String) operand.get("directory");
    if (directory != null) {
      return new File(directory);
    }

    // Check for explicit cache directory
    String cacheDirectory = (String) operand.get("cacheDirectory");
    if (cacheDirectory != null) {
      return new File(cacheDirectory);
    }

    // Use default public safety data cache directory
    String cacheHome = System.getenv("SAFETY_CACHE_HOME");
    if (cacheHome == null) {
      cacheHome = System.getProperty("user.home") + "/.calcite/safety-cache";
    }
    return new File(cacheHome);
  }

  @Override public @Nullable String getComment() {
    return "U.S. public safety data including crime statistics from FBI NIBRS/UCR systems, "
        + "traffic safety data from NHTSA FARS, emergency services response metrics, "
        + "natural disaster declarations from FEMA, law enforcement agency information, "
        + "and local government incident data. "
        + "Enables comprehensive risk assessment, emergency planning, site selection analysis, "
        + "and evidence-based public safety policy research. "
        + "Supports spatial analysis, trend detection, and cross-domain correlation "
        + "with economic, demographic, and geographic data sources.";
  }
}
