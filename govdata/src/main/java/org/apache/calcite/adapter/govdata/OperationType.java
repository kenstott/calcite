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
package org.apache.calcite.adapter.govdata;

/**
 * Type of cache operation for government data downloads.
 */
public enum OperationType {
  /** Downloading raw data from source API. */
  DOWNLOAD("download"),

  /** Converting raw data to Parquet format. */
  CONVERSION("conversion");

  private final String value;

  OperationType(String value) {
    this.value = value;
  }

  /**
   * Get the string value for logging/display.
   */
  public String getValue() {
    return value;
  }
}
