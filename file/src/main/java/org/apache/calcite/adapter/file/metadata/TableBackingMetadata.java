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
package org.apache.calcite.adapter.file.metadata;

import java.io.File;

/**
 * Comprehensive metadata for tracking table backing files.
 * Each table has:
 * - originalSource: The original source file (e.g., HTML, Excel, CSV)
 * - generatedSource: Intermediate generated file (e.g., JSON from HTML conversion)
 * - cached: Optional cached file (e.g., Parquet for DuckDB/PARQUET engines)
 */
public class TableBackingMetadata {
  private final String tableName;
  private File originalSource;
  private File generatedSource;
  private File cached;

  public TableBackingMetadata(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public File getOriginalSource() {
    return originalSource;
  }

  public void setOriginalSource(File originalSource) {
    this.originalSource = originalSource;
  }

  public File getGeneratedSource() {
    return generatedSource;
  }

  public void setGeneratedSource(File generatedSource) {
    this.generatedSource = generatedSource;
  }

  public File getCached() {
    return cached;
  }

  public void setCached(File cached) {
    this.cached = cached;
  }

  /**
   * Get the appropriate backing file based on engine requirements.
   * For DuckDB/PARQUET engines, returns cached file if available.
   * Otherwise returns generatedSource or originalSource.
   */
  public File getBackingFile(boolean requiresCached) {
    if (requiresCached && cached != null) {
      return cached;
    }
    if (generatedSource != null) {
      return generatedSource;
    }
    return originalSource;
  }

  @Override public String toString() {
    return "TableBackingMetadata{" +
        "tableName='" + tableName + '\'' +
        ", originalSource=" + originalSource +
        ", generatedSource=" + generatedSource +
        ", cached=" + cached +
        '}';
  }
}
