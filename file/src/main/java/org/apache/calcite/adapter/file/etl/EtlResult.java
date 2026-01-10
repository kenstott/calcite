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
import java.util.List;

/**
 * Result of an ETL pipeline execution.
 *
 * <p>EtlResult contains statistics and status information about a completed
 * or failed pipeline execution.
 *
 * <h3>Usage Example</h3>
 * <pre>{@code
 * EtlResult result = pipeline.execute();
 * if (result.isSuccessful()) {
 *   System.out.println("Processed " + result.getTotalRows() + " rows");
 *   System.out.println("Time: " + result.getElapsedMs() + "ms");
 * } else {
 *   System.err.println("Pipeline failed: " + result.getFailureMessage());
 *   for (String error : result.getErrors()) {
 *     System.err.println("  - " + error);
 *   }
 * }
 * }</pre>
 *
 * @see EtlPipeline
 */
public class EtlResult {

  private final String pipelineName;
  private final long totalRows;
  private final int successfulBatches;
  private final int failedBatches;
  private final int skippedBatches;
  private final long elapsedMs;
  private final List<String> errors;
  private final boolean failed;
  private final String failureMessage;
  private final boolean skippedEntirePipeline;
  private final String tableLocation;
  private final MaterializeConfig.Format materializeFormat;

  private EtlResult(Builder builder) {
    this.pipelineName = builder.pipelineName;
    this.totalRows = builder.totalRows;
    this.successfulBatches = builder.successfulBatches;
    this.failedBatches = builder.failedBatches;
    this.skippedBatches = builder.skippedBatches;
    this.elapsedMs = builder.elapsedMs;
    this.errors = builder.errors != null
        ? Collections.unmodifiableList(new ArrayList<String>(builder.errors))
        : Collections.<String>emptyList();
    this.failed = builder.failed;
    this.failureMessage = builder.failureMessage;
    this.skippedEntirePipeline = builder.skippedEntirePipeline;
    this.tableLocation = builder.tableLocation;
    this.materializeFormat = builder.materializeFormat;
  }

  /**
   * Returns the pipeline name.
   */
  public String getPipelineName() {
    return pipelineName;
  }

  /**
   * Returns the total number of rows processed.
   */
  public long getTotalRows() {
    return totalRows;
  }

  /**
   * Returns the number of successful batches.
   */
  public int getSuccessfulBatches() {
    return successfulBatches;
  }

  /**
   * Returns the number of failed batches.
   */
  public int getFailedBatches() {
    return failedBatches;
  }

  /**
   * Returns the number of skipped batches.
   */
  public int getSkippedBatches() {
    return skippedBatches;
  }

  /**
   * Returns the total batches processed (successful + failed + skipped).
   */
  public int getTotalBatches() {
    return successfulBatches + failedBatches + skippedBatches;
  }

  /**
   * Returns the elapsed time in milliseconds.
   */
  public long getElapsedMs() {
    return elapsedMs;
  }

  /**
   * Returns the list of error messages.
   */
  public List<String> getErrors() {
    return errors;
  }

  /**
   * Returns whether the pipeline execution was successful.
   * A pipeline is successful if it didn't fail catastrophically.
   */
  public boolean isSuccessful() {
    return !failed;
  }

  /**
   * Returns whether the pipeline completely succeeded with no errors.
   */
  public boolean isCompleteSuccess() {
    return !failed && failedBatches == 0 && errors.isEmpty();
  }

  /**
   * Returns the failure message if the pipeline failed.
   */
  public String getFailureMessage() {
    return failureMessage;
  }

  /**
   * Returns whether the entire pipeline was skipped due to table completion.
   */
  public boolean isSkippedEntirePipeline() {
    return skippedEntirePipeline;
  }

  /**
   * Returns whether the entire pipeline was skipped.
   * Alias for isSkippedEntirePipeline().
   */
  public boolean isSkipped() {
    return skippedEntirePipeline;
  }

  /**
   * Returns whether the pipeline failed.
   */
  public boolean isFailed() {
    return failed;
  }

  /**
   * Returns the materialized table location.
   *
   * <p>For Iceberg format, this is the table location containing the metadata folder.
   * For Parquet format, this is the output directory pattern.
   *
   * @return The table location, or null if not materialized
   */
  public String getTableLocation() {
    return tableLocation;
  }

  /**
   * Returns the materialization format used.
   *
   * @return The format (ICEBERG or PARQUET), or null if not materialized
   */
  public MaterializeConfig.Format getMaterializeFormat() {
    return materializeFormat;
  }

  /**
   * Returns the throughput in rows per second.
   */
  public double getRowsPerSecond() {
    if (elapsedMs == 0) {
      return 0;
    }
    return (totalRows * 1000.0) / elapsedMs;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("EtlResult{pipeline='").append(pipelineName).append("'");
    if (skippedEntirePipeline) {
      sb.append(", SKIPPED (table complete)");
      sb.append(", elapsed=").append(elapsedMs).append("ms");
    } else if (failed) {
      sb.append(", FAILED: ").append(failureMessage);
    } else {
      sb.append(", rows=").append(totalRows);
      sb.append(", batches=").append(successfulBatches).append("/").append(getTotalBatches());
      if (failedBatches > 0) {
        sb.append(" (").append(failedBatches).append(" failed)");
      }
      if (skippedBatches > 0) {
        sb.append(" (").append(skippedBatches).append(" skipped)");
      }
      sb.append(", elapsed=").append(elapsedMs).append("ms");
      sb.append(", throughput=").append(String.format("%.1f", getRowsPerSecond())).append(" rows/sec");
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Creates a new builder for EtlResult.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a success result.
   */
  public static EtlResult success(String pipelineName, long totalRows, int batches, long elapsedMs) {
    return builder()
        .pipelineName(pipelineName)
        .totalRows(totalRows)
        .successfulBatches(batches)
        .elapsedMs(elapsedMs)
        .build();
  }

  /**
   * Creates a failure result.
   */
  public static EtlResult failure(String pipelineName, String message, long elapsedMs) {
    return builder()
        .pipelineName(pipelineName)
        .failed(true)
        .failureMessage(message)
        .elapsedMs(elapsedMs)
        .build();
  }

  /**
   * Creates a skipped result when the entire pipeline was skipped due to table completion.
   */
  public static EtlResult skipped(String pipelineName, long elapsedMs) {
    return builder()
        .pipelineName(pipelineName)
        .skippedEntirePipeline(true)
        .elapsedMs(elapsedMs)
        .build();
  }

  /**
   * Builder for EtlResult.
   */
  public static class Builder {
    private String pipelineName;
    private long totalRows;
    private int successfulBatches;
    private int failedBatches;
    private int skippedBatches;
    private long elapsedMs;
    private List<String> errors;
    private boolean failed;
    private String failureMessage;
    private boolean skippedEntirePipeline;
    private String tableLocation;
    private MaterializeConfig.Format materializeFormat;

    public Builder pipelineName(String pipelineName) {
      this.pipelineName = pipelineName;
      return this;
    }

    public Builder totalRows(long totalRows) {
      this.totalRows = totalRows;
      return this;
    }

    public Builder successfulBatches(int successfulBatches) {
      this.successfulBatches = successfulBatches;
      return this;
    }

    public Builder failedBatches(int failedBatches) {
      this.failedBatches = failedBatches;
      return this;
    }

    public Builder skippedBatches(int skippedBatches) {
      this.skippedBatches = skippedBatches;
      return this;
    }

    public Builder elapsedMs(long elapsedMs) {
      this.elapsedMs = elapsedMs;
      return this;
    }

    public Builder errors(List<String> errors) {
      this.errors = errors;
      return this;
    }

    public Builder failed(boolean failed) {
      this.failed = failed;
      return this;
    }

    public Builder failureMessage(String failureMessage) {
      this.failureMessage = failureMessage;
      return this;
    }

    public Builder skippedEntirePipeline(boolean skippedEntirePipeline) {
      this.skippedEntirePipeline = skippedEntirePipeline;
      return this;
    }

    public Builder tableLocation(String tableLocation) {
      this.tableLocation = tableLocation;
      return this;
    }

    public Builder materializeFormat(MaterializeConfig.Format materializeFormat) {
      this.materializeFormat = materializeFormat;
      return this;
    }

    public EtlResult build() {
      return new EtlResult(this);
    }
  }
}
