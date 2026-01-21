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
package org.apache.calcite.adapter.file.etl;

import java.util.Collections;
import java.util.List;

/**
 * Result of processing a model (multiple schemas).
 */
public class ModelResult {

  private final String modelName;
  private final List<SchemaResult> schemaResults;
  private final int successfulSchemas;
  private final int failedSchemas;
  private final long elapsedMs;

  public ModelResult(String modelName, List<SchemaResult> schemaResults,
      int successfulSchemas, int failedSchemas, long elapsedMs) {
    this.modelName = modelName;
    this.schemaResults = Collections.unmodifiableList(schemaResults);
    this.successfulSchemas = successfulSchemas;
    this.failedSchemas = failedSchemas;
    this.elapsedMs = elapsedMs;
  }

  public String getModelName() {
    return modelName;
  }

  public List<SchemaResult> getSchemaResults() {
    return schemaResults;
  }

  public int getTotalSchemas() {
    return schemaResults.size();
  }

  public int getSuccessfulSchemas() {
    return successfulSchemas;
  }

  public int getFailedSchemas() {
    return failedSchemas;
  }

  public long getElapsedMs() {
    return elapsedMs;
  }

  public long getTotalRows() {
    return schemaResults.stream().mapToLong(SchemaResult::getTotalRows).sum();
  }

  public int getTotalTables() {
    return schemaResults.stream().mapToInt(SchemaResult::getTotalTables).sum();
  }

  @Override
  public String toString() {
    return "ModelResult{name='" + modelName + "', schemas=" + getTotalSchemas()
        + " (" + successfulSchemas + " ok, " + failedSchemas + " failed)"
        + ", tables=" + getTotalTables() + ", rows=" + getTotalRows()
        + ", elapsed=" + elapsedMs + "ms}";
  }
}
