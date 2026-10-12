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
package org.apache.calcite.adapter.servicenow;

/**
 * A request to ServiceNow failed, or a response was not in the shape the adapter expects.
 *
 * <p>Nothing in the adapter catches this and carries on: an incomplete result is worse than an
 * error, because ServiceNow gives no other sign that rows are missing.
 */
public class ServiceNowException extends RuntimeException {

  /** HTTP status of the failed response, or 0 if there was none (network failure, bad shape). */
  private final int status;

  public ServiceNowException(String message) {
    this(0, message, null);
  }

  public ServiceNowException(String message, Throwable cause) {
    this(0, message, cause);
  }

  public ServiceNowException(int status, String message) {
    this(status, message, null);
  }

  public ServiceNowException(int status, String message, Throwable cause) {
    super(message, cause);
    this.status = status;
  }

  /** Returns the HTTP status of the failed response, or 0 if there was none. */
  public int getStatus() {
    return status;
  }

  /**
   * Thrown when ServiceNow answered 429 and the bounded retry budget (attempts or total wait) is
   * used up, or when the 429 carried no usable {@code Retry-After}.
   */
  public static class RateLimited extends ServiceNowException {
    public RateLimited(String message) {
      super(429, message, null);
    }
  }
}
