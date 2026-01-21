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

import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Schema factory for U.S. public safety data sources.
 *
 * <p>Provides access to public safety data from:
 * <ul>
 *   <li>FBI Crime Data Explorer - NIBRS incidents, UCR statistics, hate crimes</li>
 *   <li>NHTSA FARS - Traffic fatalities and crash analysis</li>
 *   <li>FEMA OpenFEMA - Disaster declarations and emergency management</li>
 *   <li>ATF - Firearms incidents and arson statistics</li>
 *   <li>CDC WISQARS - Injury and violence surveillance data</li>
 *   <li>Local Data Portals - City crime incidents and emergency response</li>
 * </ul>
 *
 * <p>Example configuration:
 * <pre>
 * {
 *   "schemas": [{
 *     "name": "SAFETY",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.safety.SafetySchemaFactory",
 *     "operand": {
 *       "fbiApiKey": "${FBI_API_KEY}",
 *       "femaApiKey": "${FEMA_API_KEY}",
 *       "nhtsaApiKey": "${NHTSA_API_KEY}",
 *       "updateFrequency": "monthly",
 *       "historicalDepth": "5 years",
 *       "enabledSources": ["fbi", "nhtsa", "fema", "atf"],
 *       "localDataPortals": {
 *         "chicago": {"endpoint": "data.cityofchicago.org"},
 *         "nyc": {"endpoint": "data.cityofnewyork.us"}
 *       },
 *       "spatialAnalysis": {"enabled": true, "radiusAnalysis": ["1mi", "5mi"]},
 *       "cacheDirectory": "${SAFETY_CACHE_DIR:/tmp/safety-cache}"
 *     }
 *   }]
 * }
 * </pre>
 */
public class SafetySchemaFactory implements ConstraintCapableSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SafetySchemaFactory.class);

  private Map<String, Map<String, Object>> tableConstraints;
  private List<JsonTable> tableDefinitions;

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    LOGGER.info("Creating public safety data schema: {}", name);

    // Extract configuration
    String fbiApiKey = (String) operand.get("fbiApiKey");
    String femaApiKey = (String) operand.get("femaApiKey");
    String nhtsaApiKey = (String) operand.get("nhtsaApiKey");
    String updateFrequency = (String) operand.getOrDefault("updateFrequency", "M");
    String historicalDepth = (String) operand.getOrDefault("historicalDepth", "5 years");
    String cacheDirectory = (String) operand.get("cacheDirectory");

    @SuppressWarnings("unchecked")
    List<String> enabledSources = (List<String>) operand.get("enabledSources");
    if (enabledSources == null) {
      // Default to core federal sources
      enabledSources = java.util.Arrays.asList("fbi", "nhtsa", "fema");
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> localDataPortals = (Map<String, Object>) operand.get("localDataPortals");

    @SuppressWarnings("unchecked")
    Map<String, Object> spatialAnalysis = (Map<String, Object>) operand.get("spatialAnalysis");

    LOGGER.debug("Public safety data sources enabled: {}", enabledSources);
    LOGGER.debug("Update frequency: {}, Historical depth: {}", updateFrequency, historicalDepth);

    if (localDataPortals != null) {
      LOGGER.debug("Local data portals configured: {}", localDataPortals.keySet());
    }

    if (spatialAnalysis != null && Boolean.TRUE.equals(spatialAnalysis.get("enabled"))) {
      LOGGER.debug("Spatial analysis enabled with configuration: {}", spatialAnalysis);
    }

    // Create the schema with configured sources
    return new SafetySchema(parentSchema, name, operand);
  }

  @Override public boolean supportsConstraints() {
    return true;
  }

  @Override public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    this.tableDefinitions = tableDefinitions;
    LOGGER.debug("Received constraint metadata for {} tables",
        tableConstraints != null ? tableConstraints.size() : 0);
  }
}
