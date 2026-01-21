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
package org.apache.calcite.adapter.file.materialized;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;

import java.util.Locale;

/**
 * Utility methods for materialized view handling.
 */
public class MaterializedViewUtil {

  private MaterializedViewUtil() {
    // Prevent instantiation
  }

  /**
   * Gets the appropriate file extension for a materialized view based on the execution engine.
   *
   * @param engineType The execution engine type
   * @return The file extension to use (without the dot)
   */
  public static String getFileExtension(String engineType) {
    if (engineType == null) {
      return "csv";
    }

    switch (engineType.toUpperCase(Locale.ROOT)) {
    case "PARQUET":
      return "parquet";
    case "ARROW":
      return "arrow";
    case "VECTORIZED":
      // Vectorized engine can use either CSV or Arrow format
      // Default to CSV for simplicity
      return "csv";
    case "LINQ4J":
    default:
      return "csv";
    }
  }

  /**
   * Gets the filename for a materialized view with the appropriate extension.
   *
   * @param tableName The name of the materialized view
   * @param engineConfig The execution engine configuration
   * @return The filename with extension
   */
  public static String getMaterializedViewFilename(String tableName,
      ExecutionEngineConfig engineConfig) {
    String extension = getFileExtension(engineConfig.getEngineType().name());
    return tableName + "." + extension;
  }

  /**
   * Checks if a file is a materialized view based on its extension and engine type.
   *
   * @param filename The filename to check
   * @param engineType The execution engine type
   * @return true if the file could be a materialized view for this engine
   */
  public static boolean isMaterializedViewFile(String filename, String engineType) {
    String expectedExtension = getFileExtension(engineType);
    return filename.endsWith("." + expectedExtension);
  }
}
