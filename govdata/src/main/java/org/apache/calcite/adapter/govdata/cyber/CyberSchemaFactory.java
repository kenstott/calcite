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
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;
import org.apache.calcite.adapter.govdata.cyber.threat.CyberThreatCacheManifest;
import org.apache.calcite.adapter.govdata.cyber.vuln.CisaKevDownloader;
import org.apache.calcite.adapter.govdata.cyber.vuln.CweDownloader;
import org.apache.calcite.adapter.govdata.cyber.vuln.CyberVulnCacheManifest;

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
 *     "directory": "${CYBER_PARQUET_DIR}",
 *     "cacheDirectory": "${CYBER_CACHE_DIR}",
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

    String nvdApiKey = System.getenv("CYBER_NVD_API_KEY");
    String githubToken = System.getenv("CYBER_GITHUB_TOKEN");
    String threatfoxKey = System.getenv("CYBER_THREATFOX_API_KEY");
    String otxKey = System.getenv("CYBER_OTX_API_KEY");

    Boolean autoDownload = (Boolean) operand.get("autoDownload");
    if (Boolean.TRUE.equals(autoDownload)) {
      StorageProvider sp = (StorageProvider) operand.get("_storageProvider");
      if (sp != null) {
        if ("cyber_threat".equals(dataSource)) {
          configureThreatHooks(builder, operand, threatfoxKey, otxKey);
        } else {
          triggerVulnDownloads(sp, operand, nvdApiKey);
          configureVulnHooks(builder, operand, nvdApiKey, githubToken);
        }
        return;
      }
      LOGGER.warn("autoDownload=true but _storageProvider not set — skipping downloads");
    }

    if ("cyber_threat".equals(dataSource)) {
      configureThreatHooks(builder, operand, threatfoxKey, otxKey);
    } else {
      configureVulnHooks(builder, operand, nvdApiKey, githubToken);
    }
  }

  private void triggerVulnDownloads(StorageProvider sp, Map<String, Object> operand,
      String nvdApiKey) {
    String directory = (String) operand.get("directory");
    if (directory == null || directory.isEmpty()) {
      LOGGER.warn("autoDownload=true but no 'directory' operand — skipping downloads");
      return;
    }
    String vulnDir = sp.resolvePath(directory, "vuln");

    @SuppressWarnings("unchecked")
    List<String> enabledList = (List<String>) operand.get("enabledTables");
    final Set<String> enabled = (enabledList == null || enabledList.isEmpty())
        ? Collections.emptySet() : new HashSet<>(enabledList);

    try {
      if (enabled.isEmpty() || enabled.contains("cwe_catalog")) {
        LOGGER.info("Cyber vuln autoDownload: starting CWE catalog download");
        new CweDownloader(sp, vulnDir).download();
      } else {
        LOGGER.info("Cyber vuln autoDownload: cwe_catalog not in enabledTables — skipping");
      }

      if (enabled.isEmpty() || enabled.contains("kev_catalog")) {
        LOGGER.info("Cyber vuln autoDownload: starting CISA KEV download");
        new CisaKevDownloader(sp, vulnDir).download();
      } else {
        LOGGER.info("Cyber vuln autoDownload: kev_catalog not in enabledTables — skipping");
      }

      // NVD (vulnerabilities + vulnerability_cwes) is fetched by the ETL pipeline via
      // the file adapter's built-in OFFSET pagination — no autoDownload needed here.

      LOGGER.info("Cyber vuln autoDownload: all downloads complete");
    } catch (Exception e) {
      LOGGER.error("Cyber vuln autoDownload failed: {}", e.getMessage(), e);
      throw new RuntimeException("Cyber vuln autoDownload failed", e);
    }
  }

  private void configureVulnHooks(FileSchemaBuilder builder, Map<String, Object> operand,
      String nvdApiKey, String githubToken) {
    @SuppressWarnings("unchecked")
    List<String> enabledList = (List<String>) operand.get("enabledTables");
    final Set<String> enabled = (enabledList == null || enabledList.isEmpty())
        ? Collections.emptySet() : new HashSet<>(enabledList);

    for (final String table : ALL_VULN_TABLES) {
      if ("osv_vulnerabilities".equals(table) || "advisories".equals(table)) {
        continue; // handled separately below with compound checks
      }
      builder.isEnabled(table, ctx -> enabled.isEmpty() || enabled.contains(table));
    }

    builder.isEnabled("osv_vulnerabilities", ctx ->
        enabled.isEmpty() || enabled.contains("osv_vulnerabilities"));

    builder.isEnabled("advisories", ctx -> {
      if (!enabled.isEmpty() && !enabled.contains("advisories")) return false;
      return githubToken != null && !githubToken.isEmpty();
    });
  }

  private void configureThreatHooks(FileSchemaBuilder builder, Map<String, Object> operand,
      String threatfoxKey, String otxKey) {
    @SuppressWarnings("unchecked")
    List<String> enabledList = (List<String>) operand.get("enabledTables");
    final Set<String> enabled = (enabledList == null || enabledList.isEmpty())
        ? Collections.emptySet() : new HashSet<>(enabledList);

    for (final String table : ALL_THREAT_TABLES) {
      if ("ioc_mixed".equals(table) || "threat_pulses".equals(table)) {
        continue; // handled separately below with compound checks
      }
      builder.isEnabled(table, ctx -> enabled.isEmpty() || enabled.contains(table));
    }

    builder.isEnabled("ioc_mixed", ctx -> {
      if (!enabled.isEmpty() && !enabled.contains("ioc_mixed")) return false;
      return threatfoxKey != null && !threatfoxKey.isEmpty();
    });

    builder.isEnabled("threat_pulses", ctx -> {
      if (!enabled.isEmpty() && !enabled.contains("threat_pulses")) return false;
      return otxKey != null && !otxKey.isEmpty();
    });
  }

  /**
   * Returns the cache manifest for the configured data source.
   */
  public AbstractCyberCacheManifest createCacheManifest() {
    if ("cyber_threat".equals(dataSource)) {
      return new CyberThreatCacheManifest();
    }
    return new CyberVulnCacheManifest();
  }
}
