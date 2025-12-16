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
package org.apache.calcite.adapter.govdata;

/**
 * Defines the types of operations supported by the government data downloader infrastructure.
 *
 * <p>These operation types control cache manifest checking behavior:
 * <ul>
 *   <li>{@link #DOWNLOAD} - Downloads raw data (sourced check: {@code cachedAt} timestamp)</li>
 *   <li>{@link #CONVERSION} - Materializes cached data to target format (materialized check: {@code materializedAt} timestamp)</li>
 *   <li>{@link #DOWNLOAD_AND_CONVERT} - Downloads and materializes in single operation
 *       (checks both sourced and materialized timestamps)</li>
 * </ul>
 *
 * <p>The two-phase approach supports:
 * <ul>
 *   <li><b>Sourced check</b> - Do I have the raw source files cached?</li>
 *   <li><b>Materialized check</b> - Have I converted them to the target format (Iceberg, Parquet, etc.)?</li>
 * </ul>
 */
public enum OperationType {
  /**
   * Download operation - fetches raw data from remote source and caches it.
   * Performs sourced check via {@code cachedAt} timestamp.
   */
  DOWNLOAD("download"),

  /**
   * Materialization operation - converts cached raw data to target output format.
   * Performs materialized check via {@code materializedAt} timestamp.
   * Target format is determined by MaterializeConfig (Iceberg, Parquet, Delta, etc.).
   */
  CONVERSION("conversion"),

  /**
   * Combined download and materialization operation - fetches data and immediately converts to target format.
   * Performs both sourced check ({@code cachedAt}) and materialized check ({@code materializedAt}).
   */
  DOWNLOAD_AND_CONVERT("download_and_convert");

  private final String value;

  OperationType(String value) {
    this.value = value;
  }

  /**
   * Returns the string representation of this operation type.
   * Used for logging and backward compatibility.
   *
   * @return the operation type string
   */
  public String getValue() {
    return value;
  }

  /**
   * Parses a string into an OperationType enum value.
   *
   * @param value the string value to parse
   * @return the corresponding OperationType
   * @throws IllegalArgumentException if the value doesn't match any operation type
   */
  public static OperationType fromValue(String value) {
    for (OperationType type : values()) {
      if (type.value.equals(value)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown operation type: " + value);
  }
}
