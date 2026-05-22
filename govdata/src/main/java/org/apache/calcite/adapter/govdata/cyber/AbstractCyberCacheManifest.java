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

/**
 * Base class for cyber schema cache manifests.
 *
 * <p>Subclasses specify the schema YAML resource via {@link #getSchemaResourceName()}.
 */
public abstract class AbstractCyberCacheManifest {

  /**
   * Returns the schema YAML resource path (e.g., "/cyber/cyber-vuln-schema.yaml").
   */
  protected abstract String getSchemaResourceName();
}
