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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregated result from processing all tables in a schema.
 *
 * <p>Contains individual table results and summary statistics.
 *
 * @see SchemaLifecycleProcessor
 * @see EtlResult
 */
public class SchemaResult {

  private final String schemaName;
  private final Map<String, EtlResult> tableResults;
  private final long totalRows;
  private final int successfulTables;
  private final int failedTables;
  private final int skippedTables;
  private final long elapsedMs;
  private final List<String> errors;

  private SchemaResult(Builder builder) {
    this.schemaName = builder.schemaName;
    this.tableResults = Collections.unmodifiableMap(
        new LinkedHashMap<String, EtlResult>(builder.tableResults));
    this.totalRows = builder.totalRows;
    this.successfulTables = builder.successfulTables;
    this.failedTables = builder.failedTables;
    this.skippedTables = builder.skippedTables;
    this.elapsedMs = builder.elapsedMs;
    this.errors = Collections.unmodifiableList(new ArrayList<String>(builder.errors));
  }

  /**
   * Convenience constructor for error cases.
   */
  public SchemaResult(String schemaName, int successfulTables, int failedTables,
      int skippedTables, long totalRows, long elapsedMs, String error) {
    this.schemaName = schemaName;
    this.tableResults = Collections.emptyMap();
    this.totalRows = totalRows;
    this.successfulTables = successfulTables;
    this.failedTables = failedTables;
    this.skippedTables = skippedTables;
    this.elapsedMs = elapsedMs;
    this.errors = error != null ? Collections.singletonList(error) : Collections.emptyList();
  }

  public String getSchemaName() {
    return schemaName;
  }

  /**
   * Returns results for each table, keyed by table name.
   */
  public Map<String, EtlResult> getTableResults() {
    return tableResults;
  }

  /**
   * Returns the result for a specific table.
   */
  public EtlResult getTableResult(String tableName) {
    return tableResults.get(tableName);
  }

  public long getTotalRows() {
    return totalRows;
  }

  public int getSuccessfulTables() {
    return successfulTables;
  }

  public int getFailedTables() {
    return failedTables;
  }

  public int getSkippedTables() {
    return skippedTables;
  }

  public int getTotalTables() {
    return successfulTables + failedTables + skippedTables;
  }

  public long getElapsedMs() {
    return elapsedMs;
  }

  public List<String> getErrors() {
    return errors;
  }

  public boolean hasErrors() {
    return !errors.isEmpty() || failedTables > 0;
  }

  @Override public String toString() {
    return String.format(
        "SchemaResult{schema='%s', tables=%d (%d ok, %d failed, %d skipped), rows=%d, elapsed=%dms}",
        schemaName, getTotalTables(), successfulTables, failedTables, skippedTables,
        totalRows, elapsedMs);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for SchemaResult.
   */
  public static class Builder {
    private String schemaName;
    private Map<String, EtlResult> tableResults = new LinkedHashMap<String, EtlResult>();
    private long totalRows;
    private int successfulTables;
    private int failedTables;
    private int skippedTables;
    private long elapsedMs;
    private List<String> errors = new ArrayList<String>();

    public Builder schemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    public Builder addTableResult(String tableName, EtlResult result) {
      this.tableResults.put(tableName, result);
      this.totalRows += result.getTotalRows();
      if (result.isFailed()) {
        this.failedTables++;
      } else if (result.isSkipped()) {
        this.skippedTables++;
      } else {
        this.successfulTables++;
      }
      if (result.getErrors() != null) {
        this.errors.addAll(result.getErrors());
      }
      return this;
    }

    public Builder elapsedMs(long elapsedMs) {
      this.elapsedMs = elapsedMs;
      return this;
    }

    public Builder addError(String error) {
      this.errors.add(error);
      return this;
    }

    public SchemaResult build() {
      return new SchemaResult(this);
    }
  }
}
