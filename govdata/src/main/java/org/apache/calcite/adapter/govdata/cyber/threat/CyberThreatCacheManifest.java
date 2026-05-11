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
package org.apache.calcite.adapter.govdata.cyber.threat;

import org.apache.calcite.adapter.govdata.cyber.AbstractCyberCacheManifest;

/**
 * Cache manifest for the {@code cyber_threat} schema.
 *
 * <p>Reads {@code ttl_days} from {@code cyber-threat-schema.yaml}. IOC feed
 * tables ({@code ioc_urls}, {@code ioc_hashes}, {@code ioc_ips}, {@code ioc_mixed})
 * configure short TTLs (e.g., 90 days) to evict stale indicators from the hot
 * Parquet partition. Static taxonomy tables (ATT&CK, NIST controls) have no TTL.
 *
 * <p>The default TTL for IOC tables falls back to the {@code CYBER_IOC_TTL_DAYS}
 * environment variable (default 90) when no per-table {@code ttl_days} is set.
 */
public class CyberThreatCacheManifest extends AbstractCyberCacheManifest {

  /** Fallback IOC TTL when per-table config is absent. */
  public static final int DEFAULT_IOC_TTL_DAYS = 90;

  @Override protected String getSchemaResourceName() {
    return "/cyber/cyber-threat-schema.yaml";
  }

  /**
   * Returns the effective IOC TTL, honouring per-table config then env var then default.
   */
  public int getIocTtlDays(String tableName) {
    if (hasTtl(tableName)) {
      return getTtlDays(tableName);
    }
    String envVal = System.getenv("CYBER_IOC_TTL_DAYS");
    if (envVal != null && !envVal.isEmpty()) {
      try {
        return Integer.parseInt(envVal.trim());
      } catch (NumberFormatException ignored) {
        // fall through to default
      }
    }
    return DEFAULT_IOC_TTL_DAYS;
  }
}
