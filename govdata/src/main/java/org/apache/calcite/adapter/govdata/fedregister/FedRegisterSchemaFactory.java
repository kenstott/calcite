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
package org.apache.calcite.adapter.govdata.fedregister;

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory for the Federal Register schema.
 *
 * <p>Provides access to U.S. Federal Register documents (final rules, proposed rules,
 * notices, presidential documents) via the federalregister.gov JSON API. No API key
 * is required.
 *
 * <p>Tables:
 * <ul>
 *   <li>{@code fr_documents} — all FR documents, partitioned by doc_type and year.
 *       Key fields: rin (rulemaking lifecycle), cfr_references (regulated industry),
 *       significant (OIRA ≥$100M/year flag), docket_ids (regulations.gov links).</li>
 *   <li>{@code fr_agencies} — static agency registry with slug join key.</li>
 * </ul>
 *
 * <p>Cross-reference value:
 * <ul>
 *   <li>SEC: correlate effective_on dates with 8-K/10-K filings for regulated industries
 *       (CFR title → industry → company)</li>
 *   <li>ECON: measure economic series shifts after significant rule effective dates</li>
 *   <li>GEO: EPA/USDA rules carry explicit geographic scope in full-text XML</li>
 * </ul>
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "name": "FEDREGISTER",
 *   "type": "custom",
 *   "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *   "operand": {
 *     "dataSource": "fedregister",
 *     "directory": "${GOVDATA_PARQUET_DIR}",
 *     "cacheDirectory": "${GOVDATA_CACHE_DIR}"
 *   }
 * }
 * </pre>
 */
public class FedRegisterSchemaFactory implements GovDataSubSchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(FedRegisterSchemaFactory.class);

  private static final Set<String> DOCUMENT_TABLES =
      new HashSet<>(Collections.singletonList("fr_documents"));

  private static final Set<String> AGENCY_TABLES =
      new HashSet<>(Collections.singletonList("fr_agencies"));

  @Override public String getSchemaResourceName() {
    return "/fedregister/fedregister-schema.yaml";
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for FEDREGISTER schema");

    Set<String> enabledSources = parseEnabledSources(operand);

    for (String tableName : DOCUMENT_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "fr", enabledSources));
    }

    for (String tableName : AGENCY_TABLES) {
      builder.isEnabled(tableName, ctx ->
          isTableEnabled(tableName, "fr", enabledSources));
    }

    LOGGER.debug("Configured hooks for FEDREGISTER schema: enabledSources={}",
        enabledSources != null ? enabledSources : "all");
  }

  private boolean isTableEnabled(String tableName, String source, Set<String> enabledSources) {
    if (enabledSources != null && !enabledSources.contains(source.toLowerCase())) {
      LOGGER.debug("Table '{}' disabled: source '{}' not in enabledSources", tableName, source);
      return false;
    }
    return true;
  }

  private Set<String> parseEnabledSources(Map<String, Object> operand) {
    Object sourcesObj = operand.get("enabledSources");
    if (sourcesObj == null) {
      return null;
    }

    Set<String> sources = new HashSet<String>();
    if (sourcesObj instanceof List) {
      for (Object source : (List<?>) sourcesObj) {
        if (source instanceof String) {
          sources.add(((String) source).toLowerCase());
        }
      }
    } else if (sourcesObj instanceof String[]) {
      for (String source : (String[]) sourcesObj) {
        sources.add(source.toLowerCase());
      }
    }

    LOGGER.info("FedRegister enabled sources: {}", sources);
    return sources;
  }
}
