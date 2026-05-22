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
 */
public class CyberThreatCacheManifest extends AbstractCyberCacheManifest {

  @Override protected String getSchemaResourceName() {
    return "/cyber/cyber-threat-schema.yaml";
  }
}
