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
package org.apache.calcite.adapter.govdata.sec;

/**
 * Exception thrown when data fetching fails and fallback is disabled.
 */
public class DataFetchException extends RuntimeException {
  public DataFetchException(String message) {
    super(message);
  }

  public DataFetchException(String message, Throwable cause) {
    super(message, cause);
  }
}
