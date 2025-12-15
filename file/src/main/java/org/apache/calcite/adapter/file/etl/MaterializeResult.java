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

/**
 * Result of a materialization operation.
 *
 * <p>Contains statistics about the materialization:
 * <ul>
 *   <li>status - Success, skipped, or error</li>
 *   <li>rowCount - Number of rows written (-1 if unknown)</li>
 *   <li>fileCount - Number of output files created (-1 if unknown)</li>
 *   <li>elapsedMillis - Time taken for materialization</li>
 *   <li>message - Optional message (error message or reason for skip)</li>
 * </ul>
 */
public class MaterializeResult {

  /**
   * Status of the materialization operation.
   */
  public enum Status {
    /** Materialization completed successfully. */
    SUCCESS,
    /** Materialization was skipped (disabled or already up-to-date). */
    SKIPPED,
    /** Materialization failed with an error. */
    ERROR
  }

  private final Status status;
  private final long rowCount;
  private final int fileCount;
  private final long elapsedMillis;
  private final String message;

  private MaterializeResult(Status status, long rowCount, int fileCount,
      long elapsedMillis, String message) {
    this.status = status;
    this.rowCount = rowCount;
    this.fileCount = fileCount;
    this.elapsedMillis = elapsedMillis;
    this.message = message;
  }

  /**
   * Returns the status of the materialization.
   */
  public Status getStatus() {
    return status;
  }

  /**
   * Returns the number of rows written, or -1 if unknown.
   */
  public long getRowCount() {
    return rowCount;
  }

  /**
   * Returns the number of files created, or -1 if unknown.
   */
  public int getFileCount() {
    return fileCount;
  }

  /**
   * Returns the elapsed time in milliseconds.
   */
  public long getElapsedMillis() {
    return elapsedMillis;
  }

  /**
   * Returns an optional message (error message or skip reason).
   */
  public String getMessage() {
    return message;
  }

  /**
   * Returns true if the materialization was successful.
   */
  public boolean isSuccess() {
    return status == Status.SUCCESS;
  }

  /**
   * Returns true if the materialization was skipped.
   */
  public boolean isSkipped() {
    return status == Status.SKIPPED;
  }

  /**
   * Returns true if the materialization failed.
   */
  public boolean isError() {
    return status == Status.ERROR;
  }

  /**
   * Creates a successful result.
   *
   * @param rowCount Number of rows written
   * @param fileCount Number of files created
   * @param elapsedMillis Time taken in milliseconds
   * @return Success result
   */
  public static MaterializeResult success(long rowCount, int fileCount, long elapsedMillis) {
    return new MaterializeResult(Status.SUCCESS, rowCount, fileCount, elapsedMillis, null);
  }

  /**
   * Creates a skipped result.
   *
   * @param reason Reason for skipping
   * @return Skipped result
   */
  public static MaterializeResult skipped(String reason) {
    return new MaterializeResult(Status.SKIPPED, 0, 0, 0, reason);
  }

  /**
   * Creates an error result.
   *
   * @param message Error message
   * @param elapsedMillis Time taken before failure
   * @return Error result
   */
  public static MaterializeResult error(String message, long elapsedMillis) {
    return new MaterializeResult(Status.ERROR, 0, 0, elapsedMillis, message);
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("MaterializeResult{status=").append(status);
    if (rowCount >= 0) {
      sb.append(", rows=").append(rowCount);
    }
    if (fileCount >= 0) {
      sb.append(", files=").append(fileCount);
    }
    if (elapsedMillis > 0) {
      sb.append(", elapsed=").append(elapsedMillis).append("ms");
    }
    if (message != null) {
      sb.append(", message='").append(message).append("'");
    }
    sb.append("}");
    return sb.toString();
  }
}
