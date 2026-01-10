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
 * Result from the source (data fetching) phase of ETL.
 *
 * <p>Contains statistics about data fetched from HTTP, file, or other sources.
 */
public class SourceResult {

  public enum Status {
    SUCCESS,
    SKIPPED,
    ERROR
  }

  private final Status status;
  private final long recordCount;
  private final long bytesRead;
  private final long durationMs;
  private final String sourceUrl;
  private final String errorMessage;

  private SourceResult(Status status, long recordCount, long bytesRead,
      long durationMs, String sourceUrl, String errorMessage) {
    this.status = status;
    this.recordCount = recordCount;
    this.bytesRead = bytesRead;
    this.durationMs = durationMs;
    this.sourceUrl = sourceUrl;
    this.errorMessage = errorMessage;
  }

  public Status getStatus() {
    return status;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public long getBytesRead() {
    return bytesRead;
  }

  public long getDurationMs() {
    return durationMs;
  }

  public String getSourceUrl() {
    return sourceUrl;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public boolean isSuccess() {
    return status == Status.SUCCESS;
  }

  public boolean isError() {
    return status == Status.ERROR;
  }

  public static SourceResult success(long recordCount, long bytesRead,
      long durationMs, String sourceUrl) {
    return new SourceResult(Status.SUCCESS, recordCount, bytesRead,
        durationMs, sourceUrl, null);
  }

  public static SourceResult skipped(String reason) {
    return new SourceResult(Status.SKIPPED, 0, 0, 0, null, reason);
  }

  public static SourceResult error(String message, long durationMs, String sourceUrl) {
    return new SourceResult(Status.ERROR, 0, 0, durationMs, sourceUrl, message);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SourceResult{status=").append(status);
    if (status == Status.SUCCESS) {
      sb.append(", records=").append(recordCount);
      sb.append(", bytes=").append(bytesRead);
      sb.append(", duration=").append(durationMs).append("ms");
    }
    if (sourceUrl != null) {
      sb.append(", url=").append(sourceUrl);
    }
    if (errorMessage != null) {
      sb.append(", error=").append(errorMessage);
    }
    sb.append("}");
    return sb.toString();
  }
}
