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
package org.apache.calcite.adapter.govdata.cyber;

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
 * Shared sub-schema factory for both {@code cyber_vuln} and {@code cyber_threat}.
 *
 * <p>The {@code dataSource} operand in the model JSON controls which YAML is loaded
 * and which cache manifest is activated:
 * <ul>
 *   <li>{@code "dataSource": "cyber_vuln"} — loads cyber-vuln-schema.yaml</li>
 *   <li>{@code "dataSource": "cyber_threat"} — loads cyber-threat-schema.yaml</li>
 * </ul>
 *
 * <p>Example model configuration:
 * <pre>
 * {
 *   "name": "cyber_vuln",
 *   "type": "custom",
 *   "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *   "operand": {
 *     "dataSource": "cyber_vuln",
 *     "directory": "${GOVDATA_PARQUET_DIR}",
 *     "cacheDirectory": "${GOVDATA_CACHE_DIR}",
 *     "autoDownload": true
 *   }
 * }
 * </pre>
 */
public class CyberSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CyberSchemaFactory.class);

  private static final List<String> ALL_VULN_TABLES = Collections.unmodifiableList(Arrays.asList(
      "cwe_catalog", "vulnerabilities", "vulnerability_cwes",
      "kev_catalog", "kev_cwes", "osv_vulnerabilities",
      "vuln_cross_refs", "advisories"));

  private static final List<String> ALL_THREAT_TABLES = Collections.unmodifiableList(Arrays.asList(
      "attack_techniques", "ioc_urls", "ioc_hashes", "ioc_ips", "ioc_mixed",
      "nist_controls", "nist_csf_functions", "cis_controls", "owasp_top10",
      "attack_to_nist_mappings", "threat_pulses", "active_threat_intel"));

  private final String dataSource;

  public CyberSchemaFactory(String dataSource) {
    String normalized = dataSource == null ? "" : dataSource.toLowerCase().replace("_", "").replace("-", "");
    if ("cyberthreat".equals(normalized)) {
      this.dataSource = "cyber_threat";
    } else if ("cybervulnsmoke".equals(normalized) || "cybervuln_smoke".equals(normalized)) {
      this.dataSource = "cyber_vuln_smoke";
    } else {
      this.dataSource = "cyber_vuln";
    }
  }

  @Override public String getSchemaResourceName() {
    switch (dataSource) {
      case "cyber_threat":
        return "/cyber/cyber-threat-schema.yaml";
      case "cyber_vuln_smoke":
        return "/cyber/cyber-vuln-iceberg-smoke.yaml";
      case "cyber_vuln":
      default:
        return "/cyber/cyber-vuln-schema.yaml";
    }
  }

  @Override public List<String> getDependencies() {
    return Collections.emptyList();
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for {} schema", dataSource);

    // All cyber tables flow through the generic ETL pipeline (HTTP source + response transformer
    // + Iceberg, defined in the schema YAML). Tables are gated ONLY by enabledTables — credentials
    // are never read here to silently disable a table. A table whose required credential is absent
    // fails hard at fetch time (its transformer throws), per the no-silent-fallback rule.
    List<String> tables = "cyber_threat".equals(dataSource) ? ALL_THREAT_TABLES : ALL_VULN_TABLES;
    configureEnabledHooks(builder, operand, tables);
  }

  private void configureEnabledHooks(FileSchemaBuilder builder, Map<String, Object> operand,
      List<String> tables) {
    @SuppressWarnings("unchecked")
    List<String> enabledList = (List<String>) operand.get("enabledTables");
    final Set<String> enabled = (enabledList == null || enabledList.isEmpty())
        ? Collections.emptySet() : new HashSet<>(enabledList);

    for (final String table : tables) {
      builder.isEnabled(table, ctx -> enabled.isEmpty() || enabled.contains(table));
    }
  }

}
