/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without electric written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.govdata.cyber.vuln;

import org.apache.calcite.adapter.govdata.cyber.AbstractCyberCacheManifest;

/**
 * Cache manifest for the {@code cyber_vuln} schema.
 *
 * <p>Reads {@code ttl_days} from {@code cyber-vuln-schema.yaml} for per-table
 * TTL-based eviction. Vulnerability tables are generally long-lived (no TTL),
 * but active catalog tables like {@code kev_catalog} benefit from short TTLs
 * to force re-ingestion of updated entries.
 */
public class CyberVulnCacheManifest extends AbstractCyberCacheManifest {

  @Override protected String getSchemaResourceName() {
    return "/cyber/cyber-vuln-schema.yaml";
  }
}
