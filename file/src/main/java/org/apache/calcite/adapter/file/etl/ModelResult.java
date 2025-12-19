/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
