/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Resolves CIK (Central Index Key) dimension values for SEC ETL.
 *
 * <p>CIKs can be provided through multiple sources:
 * <ul>
 *   <li>Direct configuration via dimension properties "values" or "ciks" array</li>
 *   <li>Ticker symbols that are resolved to CIKs</li>
 *   <li>Predefined groups (SP500, RUSSELL2000, etc.)</li>
 * </ul>
 *
 * <p>Configuration in schema YAML:
 * <pre>{@code
 * dimensions:
 *   cik:
 *     type: custom
 *     resolver: "org.apache.calcite.adapter.govdata.sec.CikResolver"
 *     properties:
 *       source: "${CIK_SOURCE:config}"
 *       values:
 *         - "0000320193"  # Apple
 *         - "0000070858"  # Bank of America
 * }</pre>
 */
public class CikResolver implements DimensionResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(CikResolver.class);

  @Override
  public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {
    LOGGER.debug("Resolving dimension '{}' with config: {}", dimensionName, config);

    // Get properties from config
    Map<String, String> properties = config.getProperties();

    // Get source type
    String source = "config";
    if (properties != null && properties.containsKey("source")) {
      source = properties.get("source");
    }

    List<String> ciks = new ArrayList<String>();

    switch (source.toLowerCase()) {
    case "config":
      ciks = resolveCiksFromConfig(config);
      break;
    case "sp500":
      ciks = resolveSp500Ciks();
      break;
    case "russell2000":
      ciks = resolveRussell2000Ciks();
      break;
    case "nasdaq100":
      ciks = resolveNasdaq100Ciks();
      break;
    default:
      LOGGER.warn("Unknown CIK source '{}', falling back to config", source);
      ciks = resolveCiksFromConfig(config);
    }

    // Filter out nulls (from failed normalizations)
    List<String> validCiks = new ArrayList<String>();
    for (String cik : ciks) {
      if (cik != null && !cik.isEmpty()) {
        validCiks.add(cik);
      }
    }

    LOGGER.info("Resolved {} CIKs from source '{}': {}", validCiks.size(), source, validCiks);
    return validCiks;
  }

  /**
   * Resolves CIKs from the dimension configuration.
   */
  private List<String> resolveCiksFromConfig(DimensionConfig config) {
    List<String> ciks = new ArrayList<String>();

    // First, check if dimension has direct values
    List<String> values = config.getValues();
    if (values != null && !values.isEmpty()) {
      for (String cik : values) {
        String normalized = normalizeCik(cik);
        if (normalized != null) {
          ciks.add(normalized);
        }
      }
      return ciks;
    }

    // Check properties for "values" (comma-separated string)
    Map<String, String> properties = config.getProperties();
    if (properties != null) {
      // Check for "values" or "ciks" property as comma-separated string
      String valuesStr = properties.get("values");
      if (valuesStr == null) {
        valuesStr = properties.get("ciks");
      }

      if (valuesStr != null && !valuesStr.isEmpty()) {
        // Comma-separated CIKs
        for (String cik : valuesStr.split(",")) {
          String trimmed = cik.trim();
          if (!trimmed.isEmpty()) {
            String normalized = normalizeCik(trimmed);
            if (normalized != null) {
              ciks.add(normalized);
            }
          }
        }
      }
    }

    if (ciks.isEmpty()) {
      LOGGER.warn("No CIKs found in dimension config - check 'values' or properties");
    }

    return ciks;
  }

  /**
   * Normalizes a CIK to 10-digit zero-padded format.
   *
   * @param cik Raw CIK string (may be ticker or numeric CIK)
   * @return Normalized 10-digit CIK, or null if not valid
   */
  private String normalizeCik(String cik) {
    if (cik == null || cik.isEmpty()) {
      return null;
    }

    // If it looks like a numeric CIK, pad to 10 digits
    if (cik.matches("\\d+")) {
      String padded = cik;
      while (padded.length() < 10) {
        padded = "0" + padded;
      }
      return padded;
    }

    // Could be a ticker - would need SEC mapping to resolve
    // For now, treat as CIK if numeric, skip otherwise
    LOGGER.warn("CIK '{}' is not numeric, skipping (ticker resolution not yet implemented)", cik);
    return null;
  }

  /**
   * Resolves S&amp;P 500 company CIKs.
   * TODO: Implement actual S&amp;P 500 CIK lookup
   */
  private List<String> resolveSp500Ciks() {
    LOGGER.warn("S&P 500 CIK resolution not yet implemented");
    return Collections.emptyList();
  }

  /**
   * Resolves Russell 2000 company CIKs.
   * TODO: Implement actual Russell 2000 CIK lookup
   */
  private List<String> resolveRussell2000Ciks() {
    LOGGER.warn("Russell 2000 CIK resolution not yet implemented");
    return Collections.emptyList();
  }

  /**
   * Resolves NASDAQ 100 company CIKs.
   * TODO: Implement actual NASDAQ 100 CIK lookup
   */
  private List<String> resolveNasdaq100Ciks() {
    LOGGER.warn("NASDAQ 100 CIK resolution not yet implemented");
    return Collections.emptyList();
  }
}
