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
 * Exception thrown by government data adapters.
 *
 * <p>This is a common exception class used across all government data sources
 * to provide consistent error handling and messaging.
 */
public class GovDataException extends RuntimeException {

  /**
   * Creates a new GovDataException with the specified message.
   */
  public GovDataException(String message) {
    super(message);
  }

  /**
   * Creates a new GovDataException with the specified message and cause.
   */
  public GovDataException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new GovDataException with the specified cause.
   */
  public GovDataException(Throwable cause) {
    super(cause);
  }
}
